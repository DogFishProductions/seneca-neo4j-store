'use strict'

var _ = require('lodash')

var _operatorTemplate = _.template('<%= identifier %>.<%= key %> <%= operator %> { <%= value %> }')
var _noneTemplate = _.template(' NONE (x IN nodes(p) WHERE x.<%= key %> IN { <%= value %> })')

// change 'params' to 'placeholder'
function _createStatementFragment (identifier, name, operator, params) {
  return _operatorTemplate({ identifier: identifier, key: name, operator: operator, value: params })
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

var in$ = function (identifier, name, params) {
  return _createStatementFragment(identifier, name, 'IN', params)
}

var nin$ = function (identifier, name, params) {
  return _noneTemplate({ identifier: identifier, key: name, value: params })
}


module.exports.ne$ = ne$
module.exports.gte$ = gte$
module.exports.gt$ = gt$
module.exports.lte$ = lte$
module.exports.lt$ = lt$
module.exports.eq$ = eq$
module.exports.in$ = in$
module.exports.nin$ = nin$
