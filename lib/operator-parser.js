'use strict'

var _ = require('lodash')
var _operatorOtherTemplate = _.template('<%= identifier %>.<%= key %> <%= operator %> <%= value %>')
var _operatorStringTemplate = _.template("<%= identifier %>.<%= key %> <%= operator %> '<%= value %>'")
var _operatorArrayTemplate = _.template('<%= identifier %>.<%= key %> <%= operator %> [<%= value %>]')
var _noneTemplate = _.template(' NONE (x IN nodes(p) WHERE x.<%= key %> IN [<%= value %>])')

function _createStatementFragment (identifier, name, operator, params) {
  var _template = (_.isString(params) ? _operatorStringTemplate : _operatorOtherTemplate)
  return _template({ identifier: identifier, key: name, operator: operator, value: params })
}

var ne$ = function (identifier, name, params) {
  return _createStatementFragment(identifier, name, '<>', params)
}

var eq$ = function (identifier, name, params) {
  return _createStatementFragment(identifier, name, '=', params)
}

var gte$ = function (identifier, name, params) {
  return _createStatementFragment(identifier, name, '>=', params)
}

var lte$ = function (identifier, name, params) {
  return _createStatementFragment(identifier, name, '<=', params)
}

var gt$ = function (identifier, name, params) {
  return _createStatementFragment(identifier, name, '>', params)
}

var lt$ = function (identifier, name, params) {
  return _createStatementFragment(identifier, name, '<', params)
}

function _parseArray (operator, params) {
  if (!_.isArray(params)) {
    throw new Error('Operator ' + operator + ' accepts only Array as value')
  }
  var _params = []
  var _current
  for (var index in params) {
    _current = params[index]
    if (_.isString(_current)) {
      _params.push("'" + _current + "'")
    }
    else {
      _params.push(_current)
    }
  }
  return _params
}

var in$ = function (identifier, name, params) {
  var _params = _parseArray('in$', params)
  return _operatorArrayTemplate({ identifier: identifier, key: name, operator: 'IN', value: _params })
}

var nin$ = function (identifier, name, params) {
  var _params = _parseArray('nin$', params)
  return _noneTemplate({ identifier: identifier, key: name, value: _params })
}


module.exports.ne$ = ne$
module.exports.gte$ = gte$
module.exports.gt$ = gt$
module.exports.lte$ = lte$
module.exports.lt$ = lt$
module.exports.eq$ = eq$
module.exports.in$ = in$
module.exports.nin$ = nin$
