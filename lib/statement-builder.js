/* jslint node: true */
'use strict'

var _ = require('lodash')
var GraphStore = require('./graph-util')

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

function updateStatement (ent) {
  var _stm = {}

  var _label = ent.canon$({ array: true }).reverse()[0]
  var _mainTemplate = _.template("MATCH (n:<%= label %> { id: '<%= id %>' } ) SET <%= params %> RETURN n")
  var _fieldTemplate = _.template('n.<%= field %> = {<%= field %>}')

  var _fields = _.clone(ent.data$(false))
  delete _fields.id
  var _last = _.last(_.keys(_fields))
  var _fieldsStm = ''
  _stm.parameters = {}
  var _field

  for (_field in _fields) {
    _fieldsStm += _fieldTemplate({ 'field': _field })
    _stm.parameters[_field.toString()] = ent[_field]
    if (_field !== _last) {
      _fieldsStm += ','
    }
  }
  _stm.statement = _mainTemplate({ 'label': _label, 'id': ent.id, 'params': _fieldsStm })
  return _stm
}

function generateStatement (qent, q, stmtEnd) {
  var _stm = {}
  var _label = qent.canon$({ array: true }).reverse()[0]
  var _mainTemplate = _.template('MATCH (n:<%= label %> { <%= fields %> }) <%= stmtEnd %>')
  var _fieldTemplate = _.template("<%= key %>: '<%= value %>'")

  var _key
  var _last = _.last(_.keys(q))
  var _fieldsStm = ''

  for (_key in q) {
    _fieldsStm += _fieldTemplate({ 'key': _key, 'value': q[_key] })
    if (_key !== _last) {
      _fieldsStm += ','
    }
  }
  _stm.statement = _mainTemplate({ 'label': _label, 'fields': _fieldsStm, 'stmtEnd': stmtEnd })
  return _stm
}

function loadStatement (qent, q) {
  var _qualifier = 'RETURN n'
  // sort$: a sub-object containing a single field, with value 1 to sort results in ascending order, and -1 to sort descending
  if (q.sort$) {
    var _sort = q.sort$
    var _sf = _.keys(_sort)[0]
    _qualifier += ' ORDER BY n.' + _sf
    if (_sort[_sf] < 0) {
      _qualifier += ' DESC'
    }
    delete q.sort$
  }
  // limit$: an integer > 0 indicating the maximum number of result set rows to return, dropping any additional rows
  if (q.limit$) {
    var _limit = q.limit$
    _qualifier += ' LIMIT ' + _limit
    delete q.limit$
  }
  else {
    _qualifier += ' LIMIT 1'
  }
  // skip$: an integer > 0 indicating the number of result set rows to skip
  if (q.skip$) {
    var _skip = q.skip$
    _qualifier += ' SKIP ' + _skip
    delete q.skip$
  }
  return generateStatement(qent, q, 'RETURN n LIMIT 1')
}

function listStatement (qent, q) {
  var _stmt_end = 'RETURN n'
  // count$: if true, return the count of matching entities; if false return the entity list; default: false
  if (q.count$) {
    delete q.count$
    _stmt_end = 'RETURN COUNT(*)'
  }
  return generateStatement(qent, q, _stmt_end)
}

function removeStatement (qent, q) {
  return generateStatement(qent, q, 'DETACH DELETE n')
}

function uniqueRelationshipStatement (qent, q) {
  var _stm = {}
  var _labelFrom = qent.canon$({ array: true }).reverse()[0]
  var _toFilter = q
  var _relationship = _.clone(_toFilter['relationship$'])
  delete _toFilter['relationship$']
  var _labelTo = _relationship.relatedModelName
  var _type = _relationship.type
  var _data = _relationship.data
  var _mainTemplate = _.template("MATCH (a:<%= labelFrom %>),(b:<%= labelTo %>) WHERE a.id = '<%= idFrom %>' <%= toStmt %> CREATE UNIQUE (a)-[r:<%= type %> <%= fields %>]->(b) RETURN r")
  var _filterTemplate = _.template("AND b.<%= key %> = '<%= value %>'")
  var _fieldTemplate = _.template("<%= key %>: '<%= value %>'")

  var _key
  var _last = _.last(_.keys(_data))
  var _filterStm = ''
  var _fieldsStm = ''

  for (_key in _toFilter) {
    _filterStm += _filterTemplate({ 'key': _key, 'value': _toFilter[_key] })
  }
  for (_key in _data) {
    _fieldsStm += _fieldTemplate({ 'key': _key, 'value': _data[_key] })
    if (_key !== _last) {
      _fieldsStm += ','
    }
  }
  if (!_.isEmpty(_fieldsStm)) {
    _fieldsStm = '{ ' + _fieldsStm + '}'
  }
  _stm.statement = _mainTemplate({ 'labelFrom': _labelFrom, 'labelTo': _labelTo, 'idFrom': qent.id, 'toStmt': _filterStm, 'type': _type, 'fields': _fieldsStm })
  return _stm
}

function retrieveRelatedStatement (qent, q) {
  var _stm = {}
  var _labelFrom = qent.canon$({ array: true }).reverse()[0]
  var _toFilter = q
  var _relationship = _.clone(_toFilter['relationship$'])
  delete _toFilter['relationship$']
  var _labelTo = _relationship.relatedModelName
  var _type = _relationship.type
  var _mainTemplate = _.template("MATCH (a:<%= labelFrom %>)-[r:<%= type %>]->(b:<%= labelTo %>) WHERE a.id = '<%= idFrom %>' <%= filter %> RETURN b")
  var _filterTemplate = _.template("AND r.<%= key %> = '<%= value %>'")

  var _key
  var _filterStm = ''
  for (_key in _toFilter) {
    _filterStm += _filterTemplate({ 'key': _key, 'value': _toFilter[_key] })
  }
  _stm.statement = _mainTemplate({ 'labelFrom': _labelFrom, 'type': _type, 'labelTo': _labelTo, 'idFrom': qent.id, 'filter': _filterStm })
  return _stm
}

function updateRelationshipStatement (qent, q) {
  var _stm = {}
  var _labelFrom = qent.canon$({ array: true }).reverse()[0]
  var _toFilter = q
  var _relationship = _.clone(_toFilter['relationship$'])
  delete _toFilter['relationship$']
  var _labelTo = _relationship.relatedModelName
  var _type = _relationship.type
  var _mainTemplate = _.template("MATCH (a:<%= labelFrom %>)-[r:<%= type %>]->(b:<%= labelTo %>) WHERE a.id = '<%= idFrom %>' SET <%= filter %>")
  var _filterTemplate = _.template("r.<%= key %> = '<%= value %>'")

  var _key
  var _filterStm = ''
  var _count = 0
  for (_key in _toFilter) {
    _count++
    if (_count > 1) {
      _filterStm += ', '
    }
    _filterStm += _filterTemplate({ 'key': _key, 'value': _toFilter[_key] })
  }
  _stm.statement = _mainTemplate({ 'labelFrom': _labelFrom, 'type': _type, 'labelTo': _labelTo, 'idFrom': qent.id, 'filter': _filterStm })
  return _stm
}

function removeRelationshipStatement (qent, q) {
  var _stm = {}
  var _labelFrom = qent.canon$({ array: true }).reverse()[0]
  var _toFilter = q
  var _relationship = _.clone(_toFilter['relationship$'])
  delete _toFilter['relationship$']
  var _labelTo = _relationship.relatedModelName
  var _type = _relationship.type
  var _mainTemplate = _.template("MATCH (a:<%= labelFrom %>)-[r:<%= type %>]->(b:<%= labelTo %>) WHERE a.id = '<%= idFrom %>' <%= filter %> DELETE r")
  var _filterTemplate = _.template("AND r.<%= key %> = '<%= value %>'")

  var _key
  var _filterStm = ''
  for (_key in _toFilter) {
    _filterStm += _filterTemplate({ 'key': _key, 'value': _toFilter[_key] })
  }
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
