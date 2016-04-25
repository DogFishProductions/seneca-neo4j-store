/* Base class for graph databases */
'use strict'

var util = require('util');

var _ = require('lodash');

module.exports.makeentp = function(ent) {
	var _entp = {};
	var _type = {};
	var _fields = ent.fields$();

	_fields.forEach(function(field) {
		var _value = ent[field];
		_entp[field] = _value;
	})

	return _entp;

}