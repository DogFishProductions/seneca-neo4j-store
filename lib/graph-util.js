/* jslint node: true */
'use strict'

module.exports.makeentp = function (ent) {
  var _entp = {}
  var _fields = ent.fields$()

  _fields.forEach(function (field) {
    var _value = ent[field]
    _entp[field] = _value
  })

  return _entp
}
