/* jslint node: true */
'use strict'

var _ = require('lodash')
var GraphStore = require('./graph-util')
var OpParser = require('./operator-parser.js')

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
  var _mainTemplate = _.template("MERGE (n:<%= label %> { id: '<%= id %>' } ) <%= params %> <%= remove %> RETURN n")
  var _fieldTemplate = _.template('n.<%= field %> = {<%= field %>}')
  var _removeTemplate = _.template('n.<%= field %>')

  var _fields = _.clone(ent.data$(false))
  delete _fields.id
  var _fieldsStm = ''
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
      if (_.isEmpty(_fieldsStm)) {
        _fieldsStm += 'SET '
      }
      else {
        _fieldsStm += ','
      }
      _fieldsStm += _fieldTemplate({ 'field': _field })
      _stm.parameters[_field.toString()] = _param
    }
  }

  _stm.statement = _mainTemplate({ 'label': _label, 'id': ent.id, 'params': _fieldsStm, 'remove': _removeStm })
  return _stm
}

function parseComplexSelectOperator (name, value) {
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

function generateStatement (qent, q, stmtEnd) {
  var _stm = {}
  var _label = qent.canon$({ array: true }).reverse()[0]
  var _mainTemplate = _.template('MATCH (n:<%= label %> <%= fields %>) <%= whereStm %> <%= stmtEnd %>')
  var _noneTemplate = _.template('MATCH p=(n:<%= label %> <%= fields %>) <%= whereStm %> <%= stmtEnd %>')
  var _stringTemplate = _.template("<%= key %>: '<%= value %>'")
  var _fieldTemplate = _.template('<%= key %>: <%= value %>')
  var _stringWhereTemplate = _.template("n.<%= key %> = '<%= value %>'")
  var _fieldWhereTemplate = _.template('n.<%= key %> = <%= value %>')

  var _fieldsStm = ''
  var _whereStm = ''
  var _queryTemplate = _mainTemplate
  var _conditional = false

  var _parseValues = function (valObj, conjunction) {
    var _conjunction = conjunction || ' AND '
    if (_.isArray(valObj)) {
      if (_.isEmpty(_whereStm)) {
        _whereStm += ' WHERE '
      }
      _whereStm += ' ('
      for (var _index in valObj) {
        _parseValues(valObj[_index], _conjunction)
      }
      _whereStm += ') '
    }
    else {
      _.mapValues(valObj, function (value, key) {
        if (key === 'or$') {
          if (_conditional) {
            _whereStm += _conjunction
          }
          _conjunction = ' OR '
          _conditional = true
          _parseValues(value, _conjunction)
        }
        else if (key === 'and$') {
          if (_conditional) {
            _whereStm += _conjunction
          }
          _conjunction = ' AND '
          _conditional = true
          _parseValues(value, _conjunction)
        }
        else {
          if (_.isPlainObject(value)) {
            if (_.isEmpty(_whereStm)) {
              _whereStm += ' WHERE '
            }
            else if (!_.endsWith(_whereStm, 'WHERE ') && !_.endsWith(_whereStm, '(')) {
              _whereStm += _conjunction
            }
            try {
              if (value['nin$']) {
                _queryTemplate = _noneTemplate
              }
              _whereStm += parseComplexSelectOperator(key, value)
            }
            catch (e) {
              if (_conditional) {
                try {
                  _parseValues(value, _conjunction)
                }
                catch (ex) {
                  // handle this...
                }
              }
            }
          }
          else if (_conditional) {
            if (_.isArray(value)) {
              if (_.isEmpty(_whereStm)) {
                _whereStm += ' WHERE '
              }
              _whereStm += ' ('
              for (var _index in value) {
                _parseValues(value[_index], _conjunction)
              }
              _whereStm += ') '
            }
            else {
              if (_.isEmpty(_whereStm)) {
                _whereStm += ' WHERE '
              }
              else if (!_.endsWith(_whereStm, 'WHERE ') && !_.endsWith(_whereStm, '(')) {
                _whereStm += _conjunction
              }
              if (_.isString(value)) {
                _whereStm += _stringWhereTemplate({ 'key': key, 'value': value })
              }
              else {
                _whereStm += _fieldWhereTemplate({ 'key': key, 'value': value })
              }
            }
          }
          else {
            if (!_.isEmpty(_fieldsStm)) {
              _fieldsStm += ','
            }
            if (_.isString(value)) {
              _fieldsStm += _stringTemplate({ 'key': key, 'value': value })
            }
            else {
              _fieldsStm += _fieldTemplate({ 'key': key, 'value': value })
            }
          }
        }
      })
    }
  }

  if (_.isArray(q)) {
    _whereStm = 'WHERE n.id IN ' + JSON.stringify(q)
  }
  else if (_.isString(q)) {
    _fieldsStm += _stringTemplate({ 'key': 'id', 'value': q })
  }
  else {
    _parseValues(q)
  }
  if (!_.isEmpty(_fieldsStm)) {
    _fieldsStm = '{ ' + _fieldsStm + '}'
  }

  _stm.statement = _queryTemplate({ 'label': _label, 'fields': _fieldsStm, 'whereStm': _whereStm, 'stmtEnd': stmtEnd })
  return _stm
}

function parseQueryQualifier (q, qualifier) {
  var _qualifier = qualifier
  // sort$: a sub-object containing a single field, with value 1 to sort results in ascending order, and -1 to sort descending
  if (q.sort$) {
    var _sort = q.sort$
    var _sf = _.keys(_sort)[0]
    try {
      var _dir = _sort[_sf]
      if (_.isInteger(_dir)) {
        // _id is the actual id of the node in the database...
        if (_sf === '_id') {
          _qualifier += ' ORDER BY id(n)'
        }
        else {
          _qualifier += ' ORDER BY n.' + _sf
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
  if (q.skip$) {
    var _skip = q.skip$
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
  if (q.limit$) {
    var _limit = q.limit$
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

function loadStatement (qent, q) {
  var _qualifier = 'RETURN n'
  var _fields = q.fields$
  if (_fields) {
    _qualifier = 'RETURN '
    var _fieldsTemplate = _.template('n.<%= field %>')
    for (var _index in _fields) {
      if (!_.endsWith(_qualifier, 'RETURN ')) {
        _qualifier += ', '
      }
      _qualifier += _fieldsTemplate({ field: _fields[_index] })
    }
  }
  q.limit$ = 1
  if (!q.sort$) {
    q.sort$ = { _id: -1 }
  }
  return generateStatement(qent, GraphStore.sanitiseQuery(q), parseQueryQualifier(q, _qualifier))
}

function listStatement (qent, q) {
  var _qualifier = 'RETURN n'
  var _fields = q.fields$
  if (_fields) {
    _qualifier = 'RETURN '
    var _fieldsTemplate = _.template('n.<%= field %> AS <%= field %>')
    for (var _index in _fields) {
      if (!_.endsWith(_qualifier, 'RETURN ')) {
        _qualifier += ', '
      }
      _qualifier += _fieldsTemplate({ field: _fields[_index] })
    }
  }
  // count$: if true, return the count of matching entities; if false return the entity list; default: false
  if (q.count$) {
    _qualifier = 'RETURN COUNT(*)'
  }
  return generateStatement(qent, GraphStore.sanitiseQuery(q), parseQueryQualifier(q, _qualifier))
}

function removeStatement (qent, q) {
  return generateStatement(qent, GraphStore.sanitiseQuery(q), parseQueryQualifier(q, 'DETACH DELETE n'))
}

function uniqueRelationshipStatement (qent, q) {
  var _stm = {}
  var _labelFrom = qent.canon$({ array: true }).reverse()[0]
  var _toFilter = q
  var _relationship = _.clone(_toFilter['relationship$'])
  var _labelTo = _relationship.relatedModelName
  var _type = _relationship.type
  var _data = _relationship.data
  var _mainTemplate = _.template("MATCH (a:<%= labelFrom %>),(b:<%= labelTo %>) WHERE a.id = '<%= idFrom %>' <%= toStmt %> CREATE UNIQUE (a)-[r:<%= type %> <%= fields %>]->(b) RETURN r")
  var _filterTemplate = _.template("AND b.<%= key %> = '<%= value %>'")
  var _fieldTemplate = _.template("<%= key %>: '<%= value %>'")

  var _key
  var _filterStm = ''
  var _fieldsStm = ''

  for (_key in GraphStore.sanitiseQuery(_toFilter)) {
    _filterStm += _filterTemplate({ 'key': _key, 'value': _toFilter[_key] })
  }
  for (_key in _data) {
    if (!_.isEmpty(_fieldsStm)) {
      _fieldsStm += ','
    }
    _fieldsStm += _fieldTemplate({ 'key': _key, 'value': _data[_key] })
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
  var _labelTo = _relationship.relatedModelName
  var _type = _relationship.type
  var _mainTemplate = _.template("MATCH (a:<%= labelFrom %>)-[r:<%= type %>]->(b:<%= labelTo %>) WHERE a.id = '<%= idFrom %>' <%= filter %> RETURN b")
  var _filterTemplate = _.template("AND r.<%= key %> = '<%= value %>'")

  var _key
  var _filterStm = ''
  for (_key in GraphStore.sanitiseQuery(_toFilter)) {
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
  var _labelTo = _relationship.relatedModelName
  var _type = _relationship.type
  var _mainTemplate = _.template("MATCH (a:<%= labelFrom %>)-[r:<%= type %>]->(b:<%= labelTo %>) WHERE a.id = '<%= idFrom %>' SET <%= filter %>")
  var _filterTemplate = _.template("r.<%= key %> = '<%= value %>'")

  var _key
  var _filterStm = ''
  for (_key in GraphStore.sanitiseQuery(_toFilter)) {
    if (!_.isEmpty(_filterStm)) {
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
  var _labelTo = _relationship.relatedModelName
  var _type = _relationship.type
  var _mainTemplate = _.template("MATCH (a:<%= labelFrom %>)-[r:<%= type %>]->(b:<%= labelTo %>) WHERE a.id = '<%= idFrom %>' <%= filter %> DELETE r")
  var _filterTemplate = _.template("AND r.<%= key %> = '<%= value %>'")

  var _key
  var _filterStm = ''
  for (_key in GraphStore.sanitiseQuery(_toFilter)) {
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
