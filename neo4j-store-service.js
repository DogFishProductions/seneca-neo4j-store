/* jslint node: true */
'use strict'

/** neo4j-store-service
 *  @summary Interface for Neo4j persistence microservice
 */
var _ = require('lodash')
var Seneca = require('seneca')

var _si = Seneca({
  default_plugins: {
    'mem-store': false
  }
})
var _actionRole = 'graphstore_role'

// TODO: load this from a file...
var _conn = {
  'conn': {
    'url': 'http://neo4j:7474/db/data/transaction/commit',
    'auth': {
      'user': 'neo4j',
      'pass': 'Paul~J4m'
    },
    'headers': {
      'accept': 'application/json; charset=UTF-8',
      'content-type': 'application/json',
      'x-stream': 'true'
    },
    'strictSSL': false
  }
}

_si.use(require('./neo4j-store.js'), _conn)
_si.ready(function (err, response) {
  if (err) {
    _si.log.error(_actionRole + '_store ready error', err)
  }

  _si.add({ role: _actionRole, hook: 'save' }, function (args, next) {
    createEntity(args).save$(function (err, new_node) {
      if (err) {
        _si.log.error(_actionRole + ' save', err)
        return next(err)
      }
      next(null, new_node)
    })
  })

  _si.add({ role: _actionRole, hook: 'load' }, function (args, next) {
    var _opts = args.ent.options || {}
    createEntity(args).load$(_opts, function (err, loaded_node) {
      if (err) {
        _si.log.error(_actionRole + ' load', err)
        return next(err)
      }
      next(null, loaded_node)
    })
  })

  _si.add({ role: _actionRole, hook: 'list' }, function (args, next) {
    var _opts = args.ent.options || {}
    createEntity(args).list$(_opts, function (err, node_list) {
      if (err) {
        _si.log.error(_actionRole + ' list', err)
        return next(err)
      }
      next(null, node_list)
    })
  })

  _si.add({ role: _actionRole, hook: 'remove' }, function (args, next) {
    var _opts = args.ent.options || {}
    createEntity(args).remove$(_opts, function (err, removed_node) {
      if (err) {
        _si.log.error(_actionRole + ' remove', err)
        return next(err)
      }
      next(null, removed_node)
    })
  })

  _si.add({ role: _actionRole, hook: 'saveRelationship' }, function (args, next) {
    var _opts = args.ent.options || {}
    createEntity(args).saveRelationship$(_opts, function (err, relationship) {
      if (err) {
        _si.log.error(_actionRole + ' saveRelationship', err)
        return next(err)
      }
      next(null, relationship)
    })
  })

  _si.add({ role: _actionRole, hook: 'updateRelationship' }, function (args, next) {
    var _opts = args.ent.options || {}
    createEntity(args).updateRelationship$(_opts, function (err, relationship) {
      if (err) {
        _si.log.error(_actionRole + ' updateRelationship', err)
        return next(err)
      }
      next(null, relationship)
    })
  })

  _si.add({ role: _actionRole, hook: 'removeRelationship' }, function (args, next) {
    var _opts = args.ent.options || {}
    createEntity(args).removeRelationship$(_opts, function (err, relationship) {
      if (err) {
        _si.log.error(_actionRole + ' removeRelationship', err)
        return next(err)
      }
      next(null, relationship)
    })
  })

  _si.add({ role: _actionRole }, function (args, next) {
    if (_.isEmpty(args.label)) {
      next(new Error('Expected entity label.'))
    }

    this.prior({ role: _actionRole }, function (err, result) {
      if (err) {
        return next(err)
      }
      next(null, result)
    })
  })

  _si.listen({ port: 9001, type: 'tcp', pin: 'role:' + _actionRole })
})

var createEntity = function (args) {
  var _ent = args.ent
  var _node = _si.make(_ent.zone, _ent.base)
  var _props = _ent.properties || {}
  var _keys = _.keys(_props)
  _keys.forEach(function (key) {
    _node[key] = _props[key]
  })
  return _node
}
