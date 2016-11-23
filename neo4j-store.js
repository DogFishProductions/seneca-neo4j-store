/* jslint node: true */
'use strict'

/**
 * This callback type is called `middlewareCallback` and is displayed as a global symbol.
 *
 * @callback middlewareCallback
 * @param     {Object}  [err] - Error object
 */

var _ = require('lodash')
var Request = require('request')
var Uuid = require('uuid')
var DefaultConfig = require('./config/default_config.json')
var StatementBuilder = require('./lib/statement-builder.js')

var Q = require('q')

var _storeName = 'neo4j-store'
var _actionRole = 'neo4j'
var _internals = {}

/** @function _executeCypher
 *
 *  @summary Runs a query on the graphstore and returns the result as an array.
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  cypher - The query to be answered.
 *  @param    {Object}  params - The parameters required by the query.
 *
 *  @returns  {Promise} The promise of a result.
 */
var _executeCypher = function (cypher, params) {
  var _deferred = Q.defer()
  if (_.isEmpty(cypher)) {
    _deferred.resolve([])
  }
  var _json = { statements: [ { statement: cypher, parameters: params } ] }
  var _opts = _.clone(_internals.opts.conn)
  _opts.json = _json
  var _execute = Q.nbind(Request.post, Request)

  _execute(_opts)
  .then(function (response) {
    var _body = response[1]
    if (_body) {
      var _errors = _body.errors
      var _results = _body.results
      if (_errors && !_.isEmpty(_errors)) {
        _deferred.reject(_errors)
      }
      else {
        var _answer = []
        _results.forEach(function (result) {
          var _cols = result.columns
          // check whether we have asked for a label to be returned with the results...
          var _length = _cols.length
          var _labelsReturned = true
          if ((_length <= 1) || (_cols[_length - 1].indexOf('labels') < 0)) {
            _labelsReturned = false
          }
          var _data = result.data
          if (_data) {
            _data.forEach(function (entry) {
              var _row = entry.row
              if (_row) {
                if (!_labelsReturned) {
                  // no labels returned so it's just an entity...
                  _answer.push([_row[0], ['entity']])
                }
                else {
                  _answer.push(_row)
                }
              }
            })
          }
        })
        _deferred.resolve(_answer)
      }
    }
    else {
      _deferred.resolve()
    }
  })
  .catch(function (err) {
    _deferred.reject({ err: err })
  })

  return _deferred.promise
}

/** @function _parseResult
 *
 *  @summary Parses the result returned from Neo4j.
 *
 *  Neo4j only supports storage of primitive types or arrays.  In order to store Dates or Objects we
 *  must first convert them to strings.  This means that we have to convert them back to their original form
 *  before returning them to the client.  That is what this function does.
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  result - The object to be parsed.
 *
 *  @returns  {Object} The parsed object.
 */
var _parseResult = function (result) {
  if (_.isPlainObject(result)) {
    _.mapValues(result, function (value, key) {
      if (_.isString(value)) {
        var _tests = { objPatt: /^~obj~{/, arrPatt: /^~arr~\[/ }
        _.mapValues(_tests, function (regex) {
          if (regex.test(value)) {
            try {
              result[key] = JSON.parse(value.slice(5))
            }
            catch (e) {
              // do nothing, we don't care
            }
          }
        })
      }
    })
  }

  return result
}

module.exports = function (options) {
  var _seneca = this

  var _opts = _seneca.util.deepextend(DefaultConfig, options)
  _internals = {
    name: _storeName,
    opts: _opts
  }

  var _act = Q.nbind(_seneca.act, _seneca)

  /** @function _performAction
   *
   *  @summary Generates a query and passes it to the graphstore.
   *
   *  Calls the supplied hook to generate a query and passes it to the graphstore for resolution.
   *  Performs the supplied success function if results are returned. This method should be passed
   *  the seneca instance as its context when called.
   *
   *  @since 1.0.0
   *
   *  @param    {String}  hook - The name of the query to be generated.
   *  @param    {Function}  success - The function to be called if the query returns a result.
   *  @param    {Object}  args - The original query arguments.
   *  @param    {middlewareCallback}  next - The next callback in the sequence.
   */
  var _performAction = function (hook, success, args, next) {
    var _self = this
    _act({ role: _actionRole, hook: hook, target: store.name }, args)
    .done(
      function (statementObj) {
        var _cypher = statementObj.query.statement
        var _params = statementObj.query.parameters
        var _context = {
          args: args,
          next: next,
          cypher: _cypher,
          seneca: _self
        }
        _executeCypher(_cypher, _params)
        .done(
          function (result) {
            success.call(_context, result)
          },
          function (err) {
            _seneca.log.error(_cypher, _params, err)
            return next(err, { code: statementObj.operation, tag: args.tag$, store: store.name, query: _cypher, error: err })
          }
        )
      },
      function (err) {
        _seneca.log.error('Neo4j ' + hook + ' error', err)
        return next(err, { code: err.operation, tag: args.tag$, store: store.name, error: err.error })
      }
    )
  }

  var _createResultEntity = function (result, ent, label) {
    var _props = _parseResult(result[0])
    var _label = label || result[1][0] || 'entity'
    var _canonArr = ent.canon$().split('/').reverse()
    _canonArr[0] = _label
    var _newCanon = _canonArr.reverse().join('/')
    return _seneca.make$(_newCanon, _props)
  }

  // the store interface returned to seneca
  var store = {

    // methods required by store interface
    name: _storeName,

    /** @function save
     *
     *  @summary Saves object to database.
     *
     *  Save the data as specified in the entitiy block on the arguments object.
     *
     *  @since 1.0.0
     *
     *  @param    {Object}  args - The arguments.
     *  @param    {Object}  args.ent - The object to be saved.
     *  @param    {middlewareCallback}  next - The next callback in the sequence.
     *
     *  @returns  {Object}  The saved object.
     */
    save: function (args, next) {
      // to be called with the context of _performAction (= 'this')
      var _success = function (results) {
        var _self = this
        var _ent = _createResultEntity(results[0], _self.args.ent)
        _self.seneca.log(_self.args.tag$, _self.cypher, _ent)
        return _self.next(null, _ent)
      }
      _performAction.call(_seneca, 'create_save_statement', _success, args, next)
    },

    /** @function load
     *
     *  @summary Loads a single object from the database.
     *
     *  Load first object matching query based on id.
     *
     *  @since 1.0.0
     *
     *  @param    {Object}  args - The arguments.
     *  @param    {Object}  args.q - The query parameters.
     *  @param    {Object}  args.qent - The object to be retrieved.
     *  @param    {middlewareCallback}  next - The next callback in the sequence.
     *
     *  @returns  {Object}  The retrieved object.
     */
    load: function (args, next) {
      // to be called with the context of _performAction (= 'this')
      var _success = function (results) {
        var _self = this
        if (results[0]) {
          var _ent = _createResultEntity(results[0], _self.args.qent)
          _self.seneca.log(_self.args.tag$, _self.cypher, _ent)
          return _self.next(null, _ent)
        }
        return _self.next(null, null)
      }
      _performAction.call(_seneca, 'create_load_statement', _success, args, next)
    },

    /** @function list
     *
     *  @summary Return a list of objects based on the supplied query, if no query is supplied then return all objects of that type.
     *
     *  @since 1.0.0
     *
     *  @param    {Object}  args - The arguments.
     *  @param    {Object}  args.q - The query parameters.
     *  @param    {Object}  args.qent - The object to be retrieved.
     *  @param    {middlewareCallback}  next - The next callback in the sequence.
     *
     *  @returns  {Object}  The list of objects.
     */
    list: function (args, next) {
      // to be called with the context of _performAction (= 'this')
      var _success = function (results) {
        var _self = this
        if (_self.args.q.count$) {
          return _self.next(null, [results[0][0]])
        }
        if (_self.args.q.exists$) {
          return _self.next(null, (results[0][0] > 0))
        }
        var _list = []
        results.forEach(function (result) {
          _list.push(_createResultEntity(result, _self.args.qent))
        })
        _self.seneca.log(_self.args.tag$, _self.cypher, null)
        return _self.next(null, _list)
      }
      _performAction.call(_seneca, 'create_list_statement', _success, args, next)
    },

    /** @function remove
     *
     *  @summary Delete an object based on the supplied query.
     *
     *  @since 1.0.0
     *
     *  @param    {Object}  args - The arguments.
     *  @param    {Object}  args.q - The query parameters.
     *  @param    {Object}  args.qent - The entity to be removed.
     *  @param    {middlewareCallback}  next - The next callback in the sequence.
     *
     *  @returns  {Object}  The deleted object if query contains load$, null otherwise.
     */
    remove: function (args, next) {
      var _fetch_rows_to_delete = function (args) {
        var _deferred = Q.defer()
        var _q = args.q
        var _qualifiers = ['sort$', 'skip$', 'limit$']
        // all$: if true, all matching entities are deleted and none are returned; if false only the first entry in the result set is deleted; default: false
        // load$: if true, the first matching entry (only, and if any) is the response data; if false, there is no response data; default: false
        // all$ overrides load$
        if (!_q.all$ && (!_.has(_q, 'relationship$.data') || (!_q.relationship$.data.all$))) {
          store.load(args, function (err, row) {
            if (err) {
              return _deferred.reject(err)
            }
            if (!row) {
              // return an id no-one would ever create
              var _impossibleId = '~$$$$$$$$~'
              if (_q.relationship$) {
                _q.id = _impossibleId
              }
              else {
                args.q = _impossibleId
              }
              return _deferred.resolve()
            }
            if (_q.load$) {
              args.row = row
            }
            // if we're not loading all then we must delete the first match we get back. Since this will be the full object (including id) we'll only match that one
            if (_q.relationship$) {
              _q.id = row.id
            }
            else {
              var _ids = []
              try {
                _ids.push(row.id)
              }
              catch (e) {
                // do nothing, we don't care
              }
              args.q = _ids
            }
            return _deferred.resolve()
          })
        }
        else if (!_.isEmpty(_.intersection(Object.keys(_q), _qualifiers)) || (_.has(_q, 'relationship$.data') && (!_.isEmpty(_.intersection(Object.keys(_q.relationship$.data), _qualifiers))))) {
          store.list(args, function (err, results) {
            if (err) {
              return _deferred.reject(err)
            }
            // if we're not loading all then we must delete the first match we get back. Since this will be the full object (including id) we'll only match that one
            var _ids = []
            if (!_.isArray(results)) {
              results = [results]
            }
            // load$: if true, the first matching entry (only, and if any) is the response data; if false, there is no response data; default: false
            if (_q.load$) {
              args.row = results[0]
            }
            for (var _index in results) {
              try {
                _ids.push(results[_index].id)
              }
              catch (e) {
                // do nothing, we don't care
              }
            }
            args.q = _ids
            return _deferred.resolve()
          })
        }
        else {
          _deferred.resolve()
        }
        return _deferred.promise
      }

      _fetch_rows_to_delete(args)
      .done(
        function (res) {
          // to be called with the context of _performAction (= 'this')
          var _success = function (results) {
            var _self = this
            _self.seneca.log(_self.args.tag$, _self.cypher, null)
            var _result = _self.args.row || null
            return _self.next(null, _result)
          }
          _performAction.call(_seneca, 'create_remove_statement', _success, args, next)
        },
        function (err) {
          _seneca.log.error('Neo4j create_remove_statement error', err)
          return next(err, { code: err.operation, tag: args.tag$, store: store.name, error: err })
        }
      )
    },

    close: function (args, next) {
      // do nothing as we're talking to Neo4j over http - there's no connection to open or close
      return next()
    },

    native: function (args, next) {
      // to be called with the context of _performAction (= 'this')
      var _success = function (results) {
        var _self = this
        var _list = []
        results = _.castArray(results)
        results.forEach(function (result) {
          var _name = _self.args.name$ || 'entity'
          _list.push(_self.seneca.make$(_name, _parseResult(result[0])))
        })
        _self.seneca.log(_self.args.tag$, _self.cypher, null)
        return _self.next(null, _list)
      }
      var _action = function (args, next) {
        _performAction.call(_seneca, 'handle_native_statement', _success, args, next)
      }
      next(null, { query: _action })
    },

    /** @function saveRelationship
     *
     *  @summary Creates a unique relationship between the objects.
     *
     *  The 'from' object matches the id in the supplied qent and the 'to' object(s) match the query parameters.
     *  The relationship details are contained in the 'relationship$' object in the query.  Note that this method
     *  will always return an array since it is possible to create multiple relationships in a single call depending
     *  upon the filter parameters passed in.
     *
     *  @since 1.0.0
     *
     *  @param    {Object}  args - The arguments.
     *  @param    {Object}  args.q - The query parameters.
     *  @param    {Object}  args.qent - The 'from' object.
     *  @param    {middlewareCallback}  next - The next callback in the sequence.
     *
     *  @returns  {Object}  The list of entities.
     */
    saveRelationship: function (args, next) {
      // to be called with the context of _performAction (= 'this')
      var _success = function (results) {
        var _self = this
        var _list = []
        results = _.castArray(results)
        results.forEach(function (result) {
          _list.push(_createResultEntity(result, _self.args.qent, 'relationship'))
        })
        _self.seneca.log(_self.args.tag$, _self.cypher, _list)
        return _self.next(null, _list)
      }
      _performAction.call(_seneca, 'create_save_relationship_statement', _success, args, next)
    },

    /** @function updateRelationship
     *
     *  @summary Updates a relationship between objects.
     *
     *  The 'from' object matches the id in the supplied qent and the 'to' object(s) match the query parameters.
     *  The new relationship details are contained in the 'relationship$' object in the query.
     *
     *  @since 1.0.0
     *
     *  @param    {Object}  args - The arguments.
     *  @param    {Object}  args.q - The query parameters.
     *  @param    {Object}  args.qent - The 'from' object.
     *  @param    {middlewareCallback}  next - The next callback in the sequence.
     *
     *  @returns  {Object}  The list of entities.
     */
    updateRelationship: function (args, next) {
      // to be called with the context of _performAction (= 'this')
      var _success = function (results) {
        var _self = this
        var _list = []
        results = _.castArray(results)
        results.forEach(function (result) {
          _list.push(_createResultEntity(result, _self.args.qent, 'relationship'))
        })
        _self.seneca.log(_self.args.tag$, _self.cypher, _list)
        return _self.next(null, _list)
      }
      _performAction.call(_seneca, 'create_update_relationship_statement', _success, args, next)
    }
  }

  /**
   * Initialization
   */
  var _meta = _seneca.store.init(_seneca, _opts, store)
  _internals.desc = _meta.desc

  var _entityProto
  try {
    _entityProto = _seneca.private$.exports.Entity.prototype
  }
  catch (e) {
    // do nothing, entity not assigned yet....
  }
  if (_entityProto) {
    var _resolve_id_query = function (qin, ent) {
      var q

      if ((_.isUndefined(qin) || _.isNull(qin) || _.isFunction(qin)) && ent.id != null) {
        q = {id: ent.id}
      }
      else if (_.isString(qin) || _.isNumber(qin)) {
        q = qin === '' ? null : {id: qin}
      }
      else if (_.isFunction(qin)) {
        q = null
      }
      else {
        q = qin
      }

      return q
    }

    // extend entity by adding saveRelationship$ as a method
    _entityProto.saveRelationship$ = function (qin, cb) {
      var _self = this
      var _si = _self.private$.seneca
      var _qent = _self
      var _q = _resolve_id_query(qin, _self)

      cb = (_.isFunction(qin) ? qin : cb) || _.noop

      // empty query or no relationship gives empty result
      if ((_q == null) || (_q['relationship$'] == null)) {
        return cb()
      }

      _si.act(_self.private$.entargs({ qent: _qent, q: _q, cmd: 'saveRelationship' }), cb)

      return _self
    }

    // extend entity by adding updateRelationship$ as a method
    _entityProto.updateRelationship$ = function (qin, cb) {
      var _self = this
      var _si = _self.private$.seneca
      var _qent = _self
      var _q = _resolve_id_query(qin, _self)

      cb = (_.isFunction(qin) ? qin : cb) || _.noop

      // empty query or no relationship gives empty result
      if ((_q == null) || (_q['relationship$'] == null)) {
        return cb()
      }

      _si.act(_self.private$.entargs({ qent: _qent, q: _q, cmd: 'updateRelationship' }), cb)

      return _self
    }
  }

  _seneca.add({ init: store.name, tag: _meta.tag }, function (args, next) {
    return next()
  })

  _seneca.add({ role: _actionRole, hook: 'create_load_statement' }, function (args, next) {
    // we don't want changes to q here onwards to be reflected in the calling code...
    var _q = _.cloneDeep(args.q)
    var _statement

    if (!_q.sort$ && !(_.isArray(_q) || _.isString(_q))) {
      try {
        _q.sort$ = { _id: -1 }
      }
      catch (e) {
        // do nothing
      }
    }
    if (_q.relationship$) {
      var _qent = args.qent
      // should do nothing if no relationship query provided and id not present
      if (_.isEmpty(_q.relationship$) && (_.isNull(_qent.id) || _.isUndefined(_qent.id) || _.isEmpty(_qent.id))) {
        return next(null, { query: { statement: null, parameters: null }, operation: 'load related' })
      }
      _.set(_q, 'relationship$.data.limit$', 1)
      _statement = StatementBuilder.retrieveRelatedStatement(_qent, _q)
      return next(null, { query: _statement, operation: 'load related' })
    }
    _statement = StatementBuilder.loadStatement(args.qent, args.q)
    return next(null, { query: _statement, operation: 'load' })
  })

  _seneca.add({ role: _actionRole, hook: 'create_list_statement' }, function (args, next) {
    // we don't want changes to q here onwards to be reflected in the calling code...
    var _q = _.cloneDeep(args.q)
    var _statement

    if (!_q.sort$ && !_q.count$) {
      var _newsort
      if (!_.isArray(_q)) {
        _newsort = { _id: -1 }
      }
      else {
        _newsort = { id: 1 }
      }
      try {
        _q.sort$ = _newsort
      }
      catch (e) {
        // do nothing
      }
    }
    else if (_q.sort$ && _q.count$) {
      delete _q.sort$
    }
    if (_q.relationship$) {
      _statement = StatementBuilder.retrieveRelatedStatement(args.qent, _q)
      return next(null, { query: _statement, operation: 'list related' })
    }
    _statement = StatementBuilder.listStatement(args.qent, _q)
    return next(null, { query: _statement, operation: 'list' })
  })

  _seneca.add({ role: _actionRole, hook: 'create_save_statement' }, function (args, next) {
    var _ent = args.ent
    // If the entity has an id field this is used as the primary key by the underlying database and the save is considered an update operation.
    var _shouldMerge = true
    if (options.merge !== false && _ent.merge$ === false) {
      _shouldMerge = false
    }
    if (options.merge === false && _ent.merge$ !== true) {
      _shouldMerge = false
    }

    var _update = (!!_ent.id && _shouldMerge)
    var _statement

    if (_update) {
      _statement = StatementBuilder.updateStatement(_ent)
      return next(null, { query: _statement, operation: 'update' })
    }

    // If the entity has an id$ field this is used as the primary key and the save is considered to be an insert operation using the specified key.
    if (_ent.id$) {
      _ent.id = _ent.id$
      _statement = StatementBuilder.saveStatement(_ent)
      return next(null, { query: _statement, operation: 'save' })
    }

    // If the entity does not have an id field one must be generated and the save is considered an insert operation.
    if (!_ent.id) {
      _act({ role: _actionRole, hook: 'generate_id', target: args.target })
      .done(
        function (result) {
          _ent.id = result.id
          _statement = StatementBuilder.saveStatement(_ent)
          return next(null, { query: _statement, operation: 'save' })
        },
        function (err) {
          _seneca.log.error('hook generate_id failed')
          return next(err)
        }
      )
    }
    else {
      _statement = StatementBuilder.saveStatement(_ent)
      return next(null, { query: _statement, operation: 'save' })
    }
  })

  _seneca.add({ role: _actionRole, hook: 'create_remove_statement' }, function (args, next) {
    var _q = args.q
    var _statement

    // we remove nodes based on id, so we don't need to provide a label (and in some cases we don't want to)
    var _unlabelled = _seneca.make$()

    if (_q.relationship$) {
      _statement = StatementBuilder.removeRelationshipStatement(_unlabelled, args.q)
      return next(null, { query: _statement, operation: 'remove relationship' })
    }
    _statement = StatementBuilder.removeStatement(_unlabelled, args.q)
    return next(null, { query: _statement, operation: 'remove' })
  })

  _seneca.add({ role: _actionRole, hook: 'create_save_relationship_statement' }, function (args, next) {
    var _statement = StatementBuilder.uniqueRelationshipStatement(args.qent, args.q)
    return next(null, { query: _statement, operation: 'save relationship' })
  })

  _seneca.add({ role: _actionRole, hook: 'create_update_relationship_statement' }, function (args, next) {
    var _statement = StatementBuilder.updateRelationshipStatement(args.qent, args.q)
    return next(null, { query: _statement, operation: 'update relationship' })
  })

  _seneca.add({ role: _actionRole, hook: 'handle_native_statement' }, function (args, next) {
    var _statement = { statement: args.cypher, parameters: args.parameters }
    return next(null, { query: _statement, operation: 'native' })
  })

  _seneca.add({ role: _actionRole, hook: 'generate_id', target: store.name }, function (args, next) {
    return next(null, { id: Uuid() })
  })

  return { name: store.name, tag: _meta.tag }
}
