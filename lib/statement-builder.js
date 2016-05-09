/* jslint node: true */
'use strict'

var _ = require('lodash')
var GraphStore = require('./graph-util')
var OpParser = require('./operator-parser.js')

// private functions

/** @function _parseComplexSelectOperator
 *
 *  @summary Proxies a select operator to the operator parser for implementation
 *
 *  @since 1.0.0
 *
 *  @param    {string}  name - The name of the field to which the select operator is to be applied.
 *  @param    {Object}  value - The operator and the value to use for comparison.
 *
 *  @returns  {Object}  The sanitised entity.
 */
function _parseComplexSelectOperator (name, value) {
  var _operatorStm = ''

  for (var op in value) {
    var _params = value[op]
    if (!OpParser[op]) {
      throw new Error('This operator is not yet implemented: ' + op)
    }
    if (!_.isEmpty(_operatorStm)) {
      _operatorStm += ' AND '
    }
    _operatorStm += OpParser[op]('n', name, _params)
  }
  return _operatorStm
}

/** @function _handleArrayOfValues
 *
 *  @summary Deals with values contained in an array.
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  context - The execution context.
 *  @param    {Object}  context.value - The value to be handled.
 *  @param    {string}  context.whereStatement - The 'where' statement to be updated by handling the value. *
 *  @returns  {Object}  Updated context object.
 */
var _handleArrayOfValues = function (context) {
  var _whereStmt = context.whereStatement || ''
  var _value = _.clone(context.value)

  if (_.isArray(_value)) {
    if (_.isEmpty(_whereStmt)) {
      _whereStmt += ' WHERE '
    }
    _whereStmt += ' ('
    context.whereStatement = _whereStmt
    for (var _index in _value) {
      context.value = _value[_index]
      context = _parseConditions(context)
    }
    context.whereStatement += ') '
    context.handled = true
  }
  else {
    context.handled = false
  }
  return context
}

/** @function _handleConjunctionInsertion
 *
 *  @summary Determines how to join conditions together
 *
 *  Note that this requires the statement parameter to be passed by reference.
 *
 *  @since 1.0.0
 *
 *  @param    {string}  conjunction - The conjunction to be used when joining conditions together.
 *  @param    {string}  statement - The statement to be updated by handling the value.
 *
 *  @returns  {string}  The handled statement.
 */
var _handleConjunctionInsertion = function (conjunction, statement) {
  var _stmt = statement || ''

  if (_.isEmpty(_stmt)) {
    _stmt += ' WHERE '
  }
  else if (!_.endsWith(_.trimEnd(_stmt), 'WHERE') && !_.endsWith(_stmt, '(')) {
    _stmt += conjunction
  }
  return _stmt
}

/** @function _toStatementValue
 *
 *  @summary Explicitly inserts single quotes around strings
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  value - The the value to be sanitised.
 *
 *  @returns  {Object}  The sanitised entity.
 */
var _toStatementValue = function (value) {
  if (_.isString(value)) {
    value = "'" + value + "'"
  }
  return value
}

/** @function _createStateProxy
 *
 *  @summary Creates an object to encapsulate the context of the proxyValue function, so that it can be passed
 *  between invocations on recursion.
 *
 *  @since 1.0.0
 *
 *  @returns  {Object}  The state proxy Object.
 */
var _createStateProxy = function () {
  return Object.create({}, {
    type: { value: 'stateProxy' },
    fieldTemplate: { value: _.template('<%= key %>: <%= value %>') },
    whereTemplate: { value: _.template('<%= identifier %>.<%= key %> = <%= value %>') },
    noneTemplate: { value: _.template('MATCH p=(<%= identifier %>:<%= label %> <%= fields %>) <%= whereStm %> <%= stmtEnd %>') },
    // these are getters and setters rather than properties so that we can easily see the state when debugging
    identifier: {
      configurable: false,
      get: function () { return this._identifier },
      set: function (value) { this._identifier = value }
    },
    value: {
      configurable: false,
      get: function () { return this._valObj },
      set: function (value) { this._valObj = value }
    },
    conjunction: {
      configurable: false,
      get: function () { return this._conjunction },
      set: function (value) { this._conjunction = value }
    },
    whereStatement: {
      configurable: false,
      get: function () { return this._whereStm },
      set: function (value) { this._whereStm = value }
    },
    fieldsStatement: {
      configurable: false,
      get: function () { return this._fieldsStm },
      set: function (value) { this._fieldsStm = value }
    },
    conditional: {
      configurable: false,
      get: function () { return this._conditional },
      set: function (value) { this._conditional = value }
    },
    template: {
      configurable: false,
      get: function () { return this._template },
      set: function (value) { this._template = value }
    }
  })
}

/** @function _parseConditions
 *
 *  @summary Parses objects into separate conjoined condition statements
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  context - The execution context.
 *  @param    {Object}  context.identifier - The variable used to identify the return value in cypher.
 *  @param    {Object}  context.value - The value to be parsed.
 *  @param    {string}  context.conjunction - The conjunction currently in force.
 *  @param    {string}  context.whereStatement - The current 'where' statement.
 *  @param    {string}  context.fieldsStatement - The current 'fields' statement.
 *  @param    {Object}  context.template - The current template for creating the complete statement.
 *  @param    {bool}    context.conditional - Indicates whether we're currently handling a conditional statement with a conjunction.
 *
 *  @param    {Object}  The completed 'where' and 'fields' statements.
 */
var _parseConditions = function (context) {
  if (!context.type || (context.type !== 'stateProxy')) {
    var _stateProxy = _createStateProxy()
    _stateProxy.identifier = context.identifier || 'n'
    _stateProxy.value = context.value || ''
    _stateProxy.conjunction = context.conjunction || ' AND '
    _stateProxy.whereStatement = context.whereStatement || ''
    _stateProxy.fieldsStatement = context.fieldsStatement || ''
    _stateProxy.conditional = context.conditional || false
  }
  else {
    _stateProxy = context
  }

  _stateProxy = _handleArrayOfValues(_stateProxy)
  if (!_stateProxy.handled) {
    var _conjunctions = { 'or$': ' OR ', 'and$': ' AND ' }
    _.mapValues(_stateProxy.value, function (value, key) {
      var _currentConjunction = _conjunctions[key]
      if (_currentConjunction) {
        if (_stateProxy.conditional) {
          _stateProxy.whereStatement += _stateProxy.conjunction
        }
        _stateProxy.conjunction = _currentConjunction
        _stateProxy.conditional = true
        _stateProxy.value = value
        _stateProxy = _parseConditions(_stateProxy)
      }
      else {
        if (_.isPlainObject(value)) {
          _stateProxy.whereStatement = _handleConjunctionInsertion(_stateProxy.conjunction, _stateProxy.whereStatement)
          try {
            if (value['nin$']) {
              _stateProxy.template = _stateProxy.noneTemplate
            }
            _stateProxy.whereStatement += _parseComplexSelectOperator(key, value)
          }
          catch (e) {
            // if we have a conditional ('or$' or 'and$') and error will be thrown because there are no select operator implementations
            // of these conjunctions.  We have to parse this value further...
            if (_stateProxy.conditional) {
              try {
                _stateProxy.value = value
                _stateProxy = _parseConditions(_stateProxy)
              }
              catch (ex) {
                throw ex
              }
            }
            else {
              throw e
            }
          }
        }
        else if (_stateProxy.conditional) {
          _stateProxy = _handleArrayOfValues(_stateProxy)
          if (!_stateProxy.handled) {
            _stateProxy.whereStatement = _handleConjunctionInsertion(_stateProxy.conjunction, _stateProxy.whereStatement)
            _stateProxy.whereStatement += _stateProxy.whereTemplate({ identifier: _stateProxy.identifier, key: key, value: _toStatementValue(value) })
          }
        }
        else {
          if (!_.isEmpty(_stateProxy.fieldsStatement)) {
            _stateProxy.fieldsStatement += ','
          }
          _stateProxy.fieldsStatement += _stateProxy.fieldTemplate({ key: key, value: _toStatementValue(value) })
        }
      }
    })
  }
  return _stateProxy
}

/** @function _generateStatement
 *
 *  @summary Generates a cypher statement based on the supplied entity and filter
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  qent - The entity that is the object of the statement.
 *  @param    {Object}  q - The query that the statement represents.
 *  @param    {string}  stmtEnd - The return part of the statement.
 *
 *  @returns  {Object}  The cypher statement.
 */
function _generateStatement (qent, q, stmtEnd) {
  var _stm = {}
  var _label = qent.canon$({ array: true }).reverse()[0]
  var _mainTemplate = _.template('MATCH (n:<%= label %> <%= fields %>) <%= whereStm %> <%= stmtEnd %>')
  var _fieldTemplate = _.template('<%= key %>: <%= value %>')

  var _fieldsStm = ''
  var _whereStm = ''
  var _identifier = 'n'
  var _queryTemplate = _mainTemplate

  // handle the case where we're passed an array of ids...
  if (_.isArray(q)) {
    _whereStm = 'WHERE n.id IN ' + JSON.stringify(q)
  }
  // or an id string...
  else if (_.isString(q)) {
    _fieldsStm += _fieldTemplate({ key: 'id', value: _toStatementValue(q) })
  }
  // otherwise we've got an object which needs to be parsed
  else {
    var _parsed = _parseConditions({ identifier: _identifier, value: q, whereStatement: _whereStm, fieldsStatement: _fieldsStm })
    _whereStm = _parsed.whereStatement
    _fieldsStm = _parsed.fieldsStatement
    if (_parsed.template) {
      _queryTemplate = _parsed.template
    }
  }

  if (!_.isEmpty(_fieldsStm)) {
    _fieldsStm = '{ ' + _fieldsStm + '}'
  }

  _stm.statement = _queryTemplate({ identifier: _identifier, label: _label, fields: _fieldsStm, whereStm: _whereStm, stmtEnd: stmtEnd })
  return _stm
}

/** @function _parseQueryQualifier
 *
 *  @summary Parses qualifiers in the query object and modifies return statement accordingly
 *
 *  Queries can contain qualifiers sort$, skip$ and limit$ which require the return statement to be
 *  modified in order to meet these qualifications. This function performs that role.
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  q - The query to be parsed.
 *  @param    {string}  qualifier - The initial statement qualifier.
 *  @param    {string}  identifier - The identifier used in the statement.
 *
 *  @returns  {string}  The fully qualified cypher return statement.
 */
function _parseQueryQualifier (q, qualifier, identifier) {
  var _qualifier = qualifier
  // fields$: a sub-object containing the node properties to be returned
  var _fields = q.fields$
  if (_fields) {
    if (qualifier.indexOf('DELETE') < 0) {
      _qualifier = 'RETURN '
    }
    else {
      _qualifier += ' RETURN '
    }
    var _fieldsTemplate = _.template('<%= identifier %>.<%= field %> AS <%= field %>')
    for (var _index in _fields) {
      if (!_.endsWith(_.trimEnd(_qualifier), 'RETURN')) {
        _qualifier += ', '
      }
      _qualifier += _fieldsTemplate({ identifier: identifier, field: _fields[_index] })
    }
  }
  // sort$: a sub-object containing a single field, with value 1 to sort results in ascending order, and -1 to sort descending
  var _sort = q.sort$
  if (_sort) {
    var _sf = _.keys(_sort)[0]
    try {
      var _dir = _sort[_sf]
      if (_.isInteger(_dir)) {
        // _id is the actual id of the node in the database (not the field 'id')...
        if (_sf === '_id') {
          _qualifier += ' ORDER BY id(' + identifier + ')'
        }
        else {
          _qualifier += ' ORDER BY ' + identifier + '.' + _sf
        }
        if (_dir < 0) {
          _qualifier += ' DESC'
        }
      }
    }
    catch (e) {
      // ignore invalid values
    }
  }
  // skip$: an integer > 0 indicating the number of result set rows to skip
  var _skip = q.skip$
  if (_skip) {
    try {
      if (_.isInteger(_skip) && (_skip >= 0)) {
        _qualifier += ' SKIP ' + _skip
      }
    }
    catch (e) {
      // ignore invalid values
    }
  }
  // limit$: an integer > 0 indicating the maximum number of result set rows to return, dropping any additional rows
  var _limit = q.limit$
  if (_limit) {
    try {
      if (_.isInteger(_limit) && (_limit >= 0)) {
        _qualifier += ' LIMIT ' + _limit
      }
    }
    catch (e) {
      // ignore invalid values
    }
  }
  return _qualifier
}

// public functions

/** @function saveStatement
 *
 *  @summary Creates a cypher statement for saving an entity.
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  ent - The entity to be saved.
 *
 *  @returns  {String}  The cypher statement.
 */
function saveStatement (ent) {
  var _stm = {}

  var _entp = GraphStore.makeentp(ent)
  var _label = ent.canon$({ array: true }).reverse()[0]
  var _template = _.template('CREATE (n:<%= label %>{ props }) RETURN n')

  _stm.statement = _template({ 'label': _label })
  _stm.parameters = {
    'props': _entp
  }
  return _stm
}

/** @function updateStatement
 *
 *  @summary Creates a cypher statement for updating an entity.
 *
 *  Fields can be removed by setting their value to null. Fields that exist and are not included in the update statement
 *  will retain their current values.
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  ent - The entity to be updated.
 *
 *  @returns  {String}  The cypher statement.
 */
function updateStatement (ent) {
  var _stm = {}

  var _label = ent.canon$({ array: true }).reverse()[0]
  var _mainTemplate = _.template("MERGE (n:<%= label %> { id: '<%= id %>' } ) <%= update %> <%= remove %> RETURN n")
  var _updateTemplate = _.template('n.<%= field %> = {<%= field %>}')
  var _removeTemplate = _.template('n.<%= field %>')

  var _fields = _.clone(ent.data$(false))
  delete _fields.id
  var _updateStm = ''
  var _removeStm = ''
  _stm.parameters = {}
  var _field
  var _param

  for (_field in _fields) {
    _param = ent[_field]
    if (_.isNull(_param) || _.isUndefined(_param)) {
      if (_.isEmpty(_removeStm)) {
        _removeStm += 'REMOVE '
      }
      else {
        _removeStm += ','
      }
      _removeStm += _removeTemplate({ 'field': _field })
    }
    else {
      if (_.isEmpty(_updateStm)) {
        _updateStm += 'SET '
      }
      else {
        _updateStm += ','
      }
      _updateStm += _updateTemplate({ 'field': _field })
      _stm.parameters[_field.toString()] = _param
    }
  }

  _stm.statement = _mainTemplate({ 'label': _label, 'id': ent.id, 'update': _updateStm, 'remove': _removeStm })
  return _stm
}

/** @function loadStatement
 *
 *  @summary Creates a cypher statement for loading an entity based on a filter.
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  qent - The entity type to be retrieved.
 *  @param    {Object}  q - The query filter.
 *
 *  @returns  {String}  The cypher statement.
 */
function loadStatement (qent, q) {
  var _qualifier = 'RETURN n'
  q.limit$ = 1
  if (!q.sort$) {
    q.sort$ = { _id: -1 }
  }
  return _generateStatement(qent, GraphStore.sanitiseQuery(q), _parseQueryQualifier(q, _qualifier, 'n'))
}

/** @function listStatement
 *
 *  @summary Creates a cypher statement for listing entities based on a filter.
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  qent - The entity type to be retrieved.
 *  @param    {Object}  q - The query filter.
 *
 *  @returns  {String}  The cypher statement.
 */
function listStatement (qent, q) {
  var _qualifier = 'RETURN n'
  // count$: if true, return the count of matching entities; if false return the entity list; default: false
  if (q.count$) {
    _qualifier = 'RETURN COUNT(*)'
  }
  return _generateStatement(qent, GraphStore.sanitiseQuery(q), _parseQueryQualifier(q, _qualifier, 'n'))
}

/** @function removeStatement
 *
 *  @summary Creates a cypher statement for removing entities based on a filter.
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  qent - The entity type to be removed.
 *  @param    {Object}  q - The query filter.
 *
 *  @returns  {String}  The cypher statement.
 */
function removeStatement (qent, q) {
  return _generateStatement(qent, GraphStore.sanitiseQuery(q), _parseQueryQualifier(q, 'DETACH DELETE n', 'n'))
}

/** @function uniqueRelationshipStatement
 *
 *  @summary Creates a cypher statement for creating a unique relationship between nodes.
 *
 *  This function uses entities and queries slightly differently to those above.  The qent parameter now defines the label and
 *  id of the source node from which the relationship originates.  The filter is used to identify the destination node.
 *  The filter also carries the relationship data ('relationship$') including the label
 *  of the destination node and the type and associated parameters of the relationship.
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  qent - The entity which is the source of the relationship.
 *  @param    {Object}  q - The query filter.
 *
 *  @returns  {String}  The cypher statement.
 */
function uniqueRelationshipStatement (qent, q) {
  var _stm = {}
  var _labelFrom = qent.canon$({ array: true }).reverse()[0]
  var _toFilter = q
  var _relationship = _.clone(_toFilter['relationship$'])
  var _labelTo = _relationship.relatedModelName
  var _type = _relationship.type
  var _data = _relationship.data
  var _mainTemplate = _.template("MATCH (a:<%= labelFrom %>),(b:<%= labelTo %>) WHERE a.id = '<%= idFrom %>' <%= toStmt %> CREATE UNIQUE (a)-[r:<%= type %> <%= fields %>]->(b) RETURN r")
  var _filterTemplate = _.template('b.<%= key %> = <%= value %>')
  var _fieldTemplate = _.template('<%= key %>: <%= value %>')

  var _filterStm = ''
  var _fieldsStm = ''

  // use the query data fields (excluding $-properties) to filter the destination node
  _.mapValues(GraphStore.sanitiseQuery(_toFilter), function (value, key) {
    if (_.isEmpty(_filterStm)) {
      _filterStm += ' AND '
    }
    else {
      _filterStm += ', '
    }
    _filterStm += _filterTemplate({ key: key, value: _toStatementValue(value) })
  })
  // use the relationship data to create properties on the relationship
  _.mapValues(_data, function (value, key) {
    if (!_.isEmpty(_fieldsStm)) {
      _fieldsStm += ','
    }
    _fieldsStm += _fieldTemplate({ key: key, value: _toStatementValue(value) })
  })
  if (!_.isEmpty(_fieldsStm)) {
    _fieldsStm = '{ ' + _fieldsStm + ' }'
  }
  _stm.statement = _mainTemplate({ labelFrom: _labelFrom, labelTo: _labelTo, idFrom: qent.id, toStmt: _filterStm, type: _type, fields: _fieldsStm })
  return _stm
}

/** @function uniqueRelationshipStatement
 *
 *  @summary Creates a cypher statement for returning related nodes.
 *
 *  This function creates a statement that returns all the nodes that are related to the source node (defined by parameter 'qent')
 *  based on the query which filters the both relationship and the destination nodes.
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  qent - The entity which is the source of the relationship.
 *  @param    {Object}  q - The query filter.
 *
 *  @returns  {String}  The cypher statement.
 */
function retrieveRelatedStatement (qent, q) {
  var _stm = {}
  var _labelFrom = qent.canon$({ array: true }).reverse()[0]
  var _toFilter = q
  var _relationship = _.clone(_toFilter['relationship$'])
  var _labelTo = _relationship.relatedModelName
  var _type = _relationship.type
  var _data = _relationship.data
  var _mainTemplate = _.template("MATCH (a:<%= labelFrom %>)-[r:<%= type %>]->(b:<%= labelTo %>) WHERE a.id = '<%= idFrom %>' <%= filter %> RETURN b")
  var _filterTemplate = _.template('<%= identifier %>.<%= key %> = <%= value %>')

  var _filterStm = ''

  // use the query data fields (excluding $-properties) to filter the destination node
  _.mapValues(GraphStore.sanitiseQuery(_toFilter), function (value, key) {
    if (_.isEmpty(_filterStm)) {
      _filterStm += ' AND '
    }
    else {
      _filterStm += ', '
    }
    _filterStm += _filterTemplate({ identifier: 'b', key: key, value: _toStatementValue(value) })
  })
  // use the relationship data to filter properties on the relationship
  _.mapValues(GraphStore.sanitiseQuery(_data), function (value, key) {
    if (_.isEmpty(_filterStm)) {
      _filterStm += ' AND '
    }
    else {
      _filterStm += ', '
    }
    _filterStm += _filterTemplate({ identifier: 'r', key: key, value: _toStatementValue(value) })
  })
  _stm.statement = _mainTemplate({ labelFrom: _labelFrom, type: _type, labelTo: _labelTo, idFrom: qent.id, filter: _filterStm })
  return _stm
}

/** @function updateRelationshipStatement
 *
 *  @summary Creates a cypher statement for updating related nodes.
 *
 *  This function creates a statement that updates relationships from the source node (defined by parameter 'qent')
 *  based on the query which filters the the destination node(s) and defines the new relationship values.
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  qent - The entity which is the source of the relationship.
 *  @param    {Object}  q - The query filter.
 *
 *  @returns  {String}  The cypher statement.
 */
function updateRelationshipStatement (qent, q) {
  var _stm = {}
  var _labelFrom = qent.canon$({ array: true }).reverse()[0]
  var _toFilter = q
  var _relationship = _.clone(_toFilter['relationship$'])
  var _labelTo = _relationship.relatedModelName
  var _type = _relationship.type
  var _data = _relationship.data
  var _mainTemplate = _.template("MATCH (a:<%= labelFrom %>)-[r:<%= type %>]->(b:<%= labelTo %>) WHERE a.id = '<%= idFrom %>' <%= filter %> <%= update %> <%= remove %> RETURN r")
  var _filterTemplate = _.template('<%= identifier %>.<%= key %> = <%= value %>')
  var _updateTemplate = _.template('<%= identifier %>.<%= field %> = {<%= field %>}')
  var _removeTemplate = _.template('<%= identifier %>.<%= field %>')

  var _filterStm = ''
  var _updateStm = ''
  var _removeStm = ''

  // use the query data fields (excluding $-properties) to filter the destination node
  _.mapValues(GraphStore.sanitiseQuery(_toFilter), function (value, key) {
    if (_.isEmpty(_filterStm)) {
      _filterStm += ' AND '
    }
    else {
      _filterStm += ', '
    }
    _filterStm += _filterTemplate({ identifier: 'b', key: key, value: _toStatementValue(value) })
  })
  // use the relationship data to update properties on the relationship
  _.mapValues(GraphStore.sanitiseQuery(_data), function (value, key) {
    if (_.isNull(value) || _.isUndefined(value)) {
      if (_.isEmpty(_removeStm)) {
        _removeStm += 'REMOVE '
      }
      else {
        _removeStm += ','
      }
      _removeStm += _removeTemplate({ identifier: 'r', field: key })
    }
    else {
      if (_.isEmpty(_updateStm)) {
        _updateStm += 'SET '
      }
      else {
        _updateStm += ','
      }
      _updateStm += _updateTemplate({ 'field': key })
      _stm.parameters[key.toString()] = value
    }
    _updateStm += _updateTemplate({ identifier: 'r', key: key, value: value })
  })
  _stm.statement = _mainTemplate({ labelFrom: _labelFrom, type: _type, labelTo: _labelTo, idFrom: qent.id, filter: _filterStm, update: _updateStm })
  return _stm
}

function removeRelationshipStatement (qent, q) {
  var _stm = {}
  var _labelFrom = qent.canon$({ array: true }).reverse()[0]
  var _toFilter = q
  var _relationship = _.clone(_toFilter['relationship$'])
  var _labelTo = _relationship.relatedModelName
  var _type = _relationship.type
  var _data = _relationship.data
  var _mainTemplate = _.template("MATCH (a:<%= labelFrom %>)-[r:<%= type %>]->(b:<%= labelTo %>) WHERE a.id = '<%= idFrom %>' <%= filter %> DELETE r")
  var _filterTemplate = _.template("<%= identifier %>.<%= key %> = '<%= value %>'")

  var _filterStm = ''

  // use the query data fields (excluding $-properties) to filter the destination node
  _.mapValues(GraphStore.sanitiseQuery(_toFilter), function (value, key) {
    if (_.isEmpty(_filterStm)) {
      _filterStm += ' AND '
    }
    else {
      _filterStm += ', '
    }
    _filterStm += _filterTemplate({ identifier: 'r', key: key, value: _toStatementValue(value) })
  })
  // use the relationship data to update properties on the relationship
  _.mapValues(GraphStore.sanitiseQuery(_data), function (value, key) {
    if (_.isEmpty(_filterStm)) {
      _filterStm += ' AND '
    }
    else {
      _filterStm += ', '
    }
    _filterStm += _filterTemplate({ identifier: 'r', key: key, value: _toStatementValue(value) })
  })
  _stm.statement = _mainTemplate({ labelFrom: _labelFrom, type: _type, labelTo: _labelTo, idFrom: qent.id, filter: _filterStm })
  return _stm
}

module.exports.saveStatement = saveStatement
module.exports.updateStatement = updateStatement
module.exports.loadStatement = loadStatement
module.exports.listStatement = listStatement
module.exports.removeStatement = removeStatement
module.exports.uniqueRelationshipStatement = uniqueRelationshipStatement
module.exports.retrieveRelatedStatement = retrieveRelatedStatement
module.exports.updateRelationshipStatement = updateRelationshipStatement
module.exports.removeRelationshipStatement = removeRelationshipStatement
