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
    _operatorStm += OpParser[op](name, _params)
  }
  return _operatorStm
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
  var _noneTemplate = _.template('MATCH p=(n:<%= label %> <%= fields %>) <%= whereStm %> <%= stmtEnd %>')
  var _fieldTemplate = _.template('<%= key %>: <%= value %>')
  var _whereTemplate = _.template('n.<%= key %> = <%= value %>')

  var _fieldsStm = ''
  var _whereStm = ''
  var _queryTemplate = _mainTemplate
  var _conditional = false

  /** @function _handleArrayOfValues
   *
   *  @summary Deals with values contained in an array.
   *
   *  @since 1.0.0
   *
   *  @param    {Object}  value - The value to be handled.
   *  @param    {string}  conjunction - The conjunction to be used when joining conditions together.
   *
   *  @returns  {boolean}  'true' if handled, 'false' if not.
   */
  var _handleArrayOfValues = function (value, conjunction) {
    if (_.isArray(value)) {
      if (_.isEmpty(_whereStm)) {
        _whereStm += ' WHERE '
      }
      _whereStm += ' ('
      for (var _index in value) {
        _parseValues(value[_index], conjunction)
      }
      _whereStm += ') '
      return true
    }
    else {
      return false
    }
  }

  /** @function _handleConjunctionInsertion
   *
   *  @summary Determines how to join conditions together
   *
   *  @since 1.0.0
   *
   *  @param    {string}  conjunction - The conjunction to be used when joining conditions together.
   *
   *  @returns  {Object}  The sanitised entity.
   */
  var _handleConjunctionInsertion = function (conjunction) {
    if (_.isEmpty(_whereStm)) {
      _whereStm += ' WHERE '
    }
    else if (!_.endsWith(_whereStm, 'WHERE ') && !_.endsWith(_whereStm, '(')) {
      _whereStm += conjunction
    }    
  }

  /** @function _parseValues
   *
   *  @summary Parses objects into separate conjoined condition statements
   *
   *  @since 1.0.0
   *
   *  @param    {Object}  valObj - The value to be parsed.
   *  @param    {string}  conjunction - The conjunction currently in force.
   */
  var _parseValues = function (valObj, conjunction) {
    var _conjunction = conjunction || ' AND '
    if (!_handleArrayOfValues(valObj, conjunction)) {
      var _conjunctions = { 'or$': ' OR ', 'and$': ' AND ' }
      _.mapValues(valObj, function (value, key) {
        var _currentConjunction = _conjunctions[key]
        if (_currentConjunction)  {
          if (_conditional) {
            _whereStm += _conjunction
          }
          _conjunction = _currentConjunction
          _conditional = true
          _parseValues(value, _conjunction)
        }
        else {
          if (_.isPlainObject(value)) {
            _handleConjunctionInsertion(_conjunction)
            try {
              if (value['nin$']) {
                _queryTemplate = _noneTemplate
              }
              _whereStm += _parseComplexSelectOperator(key, value)
            }
            catch (e) {
              // if we have a conditional ('or$' or 'and$') and error will be thrown because there are no select operator implementations
              // of these conjunctions.  We have to parse this value further...
              if (_conditional) {
                try {
                  _parseValues(value, _conjunction)
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
          else if (_conditional) {
            if (!_handleArrayOfValues(value, conjunction)) {
              _handleConjunctionInsertion(_conjunction)
              if (_.isString(value)) {
                value = "'" + value + "'"
              }
              _whereStm += _whereTemplate({ 'key': key, 'value': value })
            }
          }
          else {
            if (!_.isEmpty(_fieldsStm)) {
              _fieldsStm += ','
            }
            if (_.isString(value)) {
              value = "'" + value + "'"
            }
            _fieldsStm += _fieldTemplate({ 'key': key, 'value': value })
          }
        }
      })
    }
  }

  // handle the case where we're passed an array of ids...
  if (_.isArray(q)) {
    _whereStm = 'WHERE n.id IN ' + JSON.stringify(q)
  }
  // or an id string...
  else if (_.isString(q)) {
    _fieldsStm += _stringTemplate({ 'key': 'id', 'value': q })
  }
  // otherwise we've got an object which needs to be parsed
  else {
    _parseValues(q)
  }
  if (!_.isEmpty(_fieldsStm)) {
    _fieldsStm = '{ ' + _fieldsStm + '}'
  }

  _stm.statement = _queryTemplate({ 'label': _label, 'fields': _fieldsStm, 'whereStm': _whereStm, 'stmtEnd': stmtEnd })
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
      if (!_.endsWith(_qualifier, 'RETURN ')) {
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

  var _key
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
    if (_.isString(value)) {
      value = "'" + value + "'"
    }
    _filterStm += _filterTemplate({ 'key': key, 'value': value })
  })
  // use the relationship data to create properties on the relationship
  _.mapValues(_data, function (value, key) {
    if (!_.isEmpty(_fieldsStm)) {
      _fieldsStm += ','
    }
    if (_.isString(value)) {
      value = "'" + value + "'"
    }
    _fieldsStm += _fieldTemplate({ 'key': key, 'value': value })
  })
  if (!_.isEmpty(_fieldsStm)) {
    _fieldsStm = '{ ' + _fieldsStm + ' }'
  }
  _stm.statement = _mainTemplate({ 'labelFrom': _labelFrom, 'labelTo': _labelTo, 'idFrom': qent.id, 'toStmt': _filterStm, 'type': _type, 'fields': _fieldsStm })
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
  var _mainTemplate = _.template("MATCH (a:<%= labelFrom %>)-[r:<%= type %>]->(b:<%= labelTo %>) WHERE a.id = '<%= idFrom %>' <%= filter %> RETURN b")
  var _filterTemplate = _.template('<%= identifier %>.<%= key %> = <%= value %>')

  var _key
  var _filterStm = ''

  // use the query data fields (excluding $-properties) to filter the destination node
  _.mapValues(GraphStore.sanitiseQuery(_toFilter), function (value, key) {
    if (_.isEmpty(_filterStm)) {
      _filterStm += ' AND '
    }
    else {
      _filterStm += ', '
    }
    if (_.isString(value)) {
      value = "'" + value + "'"
    }
    _filterStm += _filterTemplate({ 'identifier': 'b', 'key': key, 'value': value })
  })
  // use the relationship data to filter properties on the relationship
  _.mapValues(GraphStore.sanitiseQuery(_data), function (value, key) {
    if (_.isEmpty(_filterStm)) {
      _filterStm += ' AND '
    }
    else {
      _filterStm += ', '
    }
    if (_.isString(value)) {
      value = "'" + value + "'"
    }
    _filterStm += _filterTemplate({ 'identifier': 'r', 'key': key, 'value': value })
  })
  _stm.statement = _mainTemplate({ 'labelFrom': _labelFrom, 'type': _type, 'labelTo': _labelTo, 'idFrom': qent.id, 'filter': _filterStm })
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

  var _key
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
    if (_.isString(value)) {
      value = "'" + value + "'"
    }
    _filterStm += _filterTemplate({ 'identifier': 'b', 'key': key, 'value': value })
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
      _removeStm += _removeTemplate({ 'identifier': 'r', 'field': _field })
    }
    else {
      if (_.isEmpty(_updateStm)) {
        _updateStm += 'SET '
      }
      else {
        _updateStm += ','
      }
      _updateStm += _fieldTemplate({ 'field': _field })
      _stm.parameters[_field.toString()] = _param
    }
    _updateStm += _updateTemplate({ 'identifier': 'r', 'key': key, 'value': value })
  })
  _stm.statement = _mainTemplate({ 'labelFrom': _labelFrom, 'type': _type, 'labelTo': _labelTo, 'idFrom': qent.id, 'filter': _filterStm, 'update': _updateStm })
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

  var _key
  var _filterStm = ''

  // use the query data fields (excluding $-properties) to filter the destination node
  _.mapValues(GraphStore.sanitiseQuery(_toFilter), function (value, key) {
    if (_.isEmpty(_filterStm)) {
      _filterStm += ' AND '
    }
    else {
      _filterStm += ', '
    }
    if (_.isString(value)) {
      value = "'" + value + "'"
    }
    _filterStm += _filterTemplate({ 'identifier': 'r', 'key': key, 'value': value })
  })
  // use the relationship data to update properties on the relationship
  _.mapValues(GraphStore.sanitiseQuery(_data), function (value, key) {
    if (_.isEmpty(_filterStm)) {
      _filterStm += ' AND '
    }
    else {
      _filterStm += ', '
    }
    if (_.isString(value)) {
      value = "'" + value + "'"
    }
    _filterStm += _filterTemplate({ 'identifier': 'r', 'key': key, 'value': value })
  })
  _stm.statement = _mainTemplate({ 'labelFrom': _labelFrom, 'type': _type, 'labelTo': _labelTo, 'idFrom': qent.id, 'filter': _filterStm })
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
