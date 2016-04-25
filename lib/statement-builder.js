'use strict'

var util = require('util');

var _ = require('lodash');
var GraphStore = require('./graph-util');

function saveStatement(ent) {
	var _stm = {};

	var _entp = GraphStore.makeentp(ent);
	var _label = ent.canon$({ array: true }).reverse()[0];
	var _template = _.template("CREATE (n:<%= label %>{ props }) RETURN n");

	_stm.statement = _template( { 'label': _label } );
	_stm.parameters = {
      "props" : _entp
    };
	return _stm;
}

function updateStatement(ent) {
	var _stm = {};

	var _entp = GraphStore.makeentp(ent);
	var _label = ent.canon$({ array: true }).reverse()[0];
	var _main_template = _.template("MATCH (n:<%= label %> { id: '<%= id %>' } ) SET <%= params %> RETURN n");
	var _field_template = _.template("n.<%= field %> = {<%= field %>}");

	var _fields = _.clone(ent.data$(false));
	delete _fields.id;
	var _last = _.last(_.keys(_fields));
	var _fields_stm = "";
	_stm.parameters = {};
	var _field;

	for (_field in _fields) {
		_fields_stm += _field_template( { 'field': _field });
		_stm.parameters[_field.toString()] = ent[_field];
		if (_field != _last) {
			_fields_stm += ",";
		}
	}
	_stm.statement = _main_template( { 'label': _label, 'id': ent.id, 'params': _fields_stm } );
	return _stm;
}

function generateStatement(qent, q, stmtEnd) {
	var _stm = {};
	var _label = qent.canon$({ array: true }).reverse()[0];
	var _main_template = _.template("MATCH (n:<%= label %> { <%= fields %> }) <%= stmtEnd %>");
	var _field_template = _.template("<%= key %>: '<%= value %>'");

	var _key;
	var _last = _.last(_.keys(q));
	var _fields_stm = "";

	for (_key in q) {
		_fields_stm += _field_template( { 'key': _key, 'value': q[_key] } );
		if (_key != _last) {
			_fields_stm += ",";
		}
	}
	_stm.statement = _main_template( { 'label': _label, 'fields': _fields_stm, 'stmtEnd': stmtEnd } );
	return _stm;
}

function loadStatement(qent, q) {
	var _qualifier = "RETURN n";
	// sort$: a sub-object containing a single field, with value 1 to sort results in ascending order, and -1 to sort descending
	if (q.sort$) {
		var _sort = q.sort$;
		var _sf = _.keys(_sort)[0];
		_qualifier += " ORDER BY n." + _sf;
		if (_sort[_sf] < 0) {
			_qualifier += " DESC";
		}
		delete q.sort$;
	}
	// limit$: an integer > 0 indicating the maximum number of result set rows to return, dropping any additional rows
	if (q.limit$) {
		var _limit = q.limit$;
		_qualifier += " LIMIT " + _limit;
		delete q.limit$;
	}
	else {
		_qualifier += " LIMIT 1";
	}
	// skip$: an integer > 0 indicating the number of result set rows to skip
	if (q.skip$) {
		var _skip = q.skip$;
		_qualifier += " SKIP " + _skip;
		delete q.skip$;
	}
	return generateStatement(qent, q, "RETURN n LIMIT 1");
}

function listStatement(qent, q) {
	var _stmt_end = "RETURN n";
	// count$: if true, return the count of matching entities; if false return the entity list; default: false
	if (q.count$) {
		delete q.count$;
		_stmt_end = "RETURN COUNT(*)";
	}
	return generateStatement(qent, q, _stmt_end);
}

function removeStatement(qent, q) {
	return generateStatement(qent, q, "DETACH DELETE n");
}
/*
function constraintStatement(qent, q) {
	var _stm = {};
	var _label = qent.entity$.split('/').reverse()[0];

	_stm.statement = "CREATE CONSTRAINT ON (n:" + _label + ") ASSERT n." + _.keys(q)[0] + " IS UNIQUE";
	return _stm;
}//*/

function uniqueRelationshipStatement(ent) {
	var _stm = {};
	var _label_from = ent.canon$({ array: true }).reverse()[0];
	var _to_filter = ent.data$(false);
	delete _to_filter.id;
	var _relationship = ent["relationship~"];
	delete _to_filter["relationship~"];
	var _label_to = _relationship.relatedModelName;
	var _type = _relationship.type;
	var _data = _relationship.data;
	var _main_template = _.template("MATCH (a:<%= labelFrom %>),(b:<%= labelTo %>) WHERE a.id = '<%= idFrom %>' <%= toStmt %> CREATE UNIQUE (a)-[r:<%= type %> <%= fields %>]->(b) RETURN r");
	var _filter_template = _.template("AND b.<%= key %> = '<%= value %>'");
	var _field_template = _.template("<%= key %>: '<%= value %>'");

	var _key;
	var _last = _.last(_.keys(_data));
	var _filter_stm = "";
	var _fields_stm = "";

	for (_key in _to_filter) {
		_filter_stm += _filter_template( { 'key': _key, 'value': _to_filter[_key] } );
	}
	for (_key in _data) {
		_fields_stm += _field_template( { 'key': _key, 'value': _data[_key] } );
		if (_key != _last) {
			_fields_stm += ",";
		}
	}
	if (!_.isEmpty(_fields_stm)) {
		_fields_stm = "{ " + _fields_stm + "}";
	}
	_stm.statement = _main_template( { 'labelFrom': _label_from, 'labelTo': _label_to, 'idFrom': ent.id, 'toStmt': _filter_stm, 'type': _type,  'fields': _fields_stm } );
	return _stm;
}

function retrieveRelatedStatement(qent, q) {
	var _stm = {};
	var _label_from = qent.canon$({ array: true }).reverse()[0];
	var _to_filter = q;
	var _relationship = _.clone(_to_filter["relationship~"]);
	delete _to_filter["relationship~"];
	var _label_to = _relationship.relatedModelName;
	var _type = _relationship.type;
	var _main_template = _.template("MATCH (a:<%= labelFrom %>)-[r:<%= type %>]->(b:<%= labelTo %>) WHERE a.id = '<%= idFrom %>' <%= filter %> RETURN b");
	var _filter_template = _.template("AND r.<%= key %> = '<%= value %>'");

	var _key;
	var _filter_stm = "";
	for (_key in _to_filter) {
		_filter_stm += _filter_template( { 'key': _key, 'value': _to_filter[_key] } );
	}
	_stm.statement = _main_template( { 'labelFrom': _label_from, 'type': _type, 'labelTo': _label_to, 'idFrom': qent.id, 'filter': _filter_stm } );
	return _stm;
}

module.exports.saveStatement = saveStatement;
module.exports.updateStatement = updateStatement;
module.exports.loadStatement = loadStatement;
module.exports.listStatement = listStatement;
module.exports.removeStatement = removeStatement;
//module.exports.constraintStatement = constraintStatement;
module.exports.uniqueRelationshipStatement = uniqueRelationshipStatement;
module.exports.retrieveRelatedStatement = retrieveRelatedStatement;