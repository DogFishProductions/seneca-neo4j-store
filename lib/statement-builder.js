'use strict'

var util = require('util');

var _ = require('lodash');
var GraphStore = require('./graph-util');

function saveStatement(ent) {
	var _stm = {};

	var _entp = GraphStore.makeentp(ent);
	var _label = ent.entity$.split('/').reverse()[0];
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
	var _label = ent.entity$.split('/').reverse()[0];
	var _main_template = _.template("CREATE (n:<%= label %> { id: '<%= id %>' } ) SET <%= params %> RETURN n");

	var _fields = ent.fields$();
	var _last = _.last(_fields);
	var _params = {};
	var _fields_stm = "";
	_stm.parameters = {};

//	_stm.statement = "MATCH (n:" + _label + " { id: '" + _entp.id + "' }" + ") SET ";
	_fields.forEach(function(field) {
		var _field_template = _.template("n.<%= field %> = {<%= field %>}");
		if (field != "id") {
//			_stm.statement += "n." + field + "={" + field + "}"
			_fields_stm += _field_template( { 'field': field });
			_stm.parameters[field.toString()] = ent[field];
			if (field != _last) {
//				_stm.statement += ",";
				_fields_stm += ",";
			}
		}
	})
//	_stm.statement += " RETURN n";
	_stm.statement = _main_template( { 'label': _label, 'id': ent.id, 'params': _fields_stm } );
	console.log("statement: " + util.inspect(_stm.statemtnt));
	return _stm;
}

function generateStatement(qent, q, stmtEnd) {
	var _stm = {};
	var _label = qent.entity$.split('/').reverse()[0];

	_stm.statement = "MATCH (n:" + _label;
	var _key;
	var _last = _.last(_.keys(q));
	var _stm_end = ") " + stmtEnd;
	if (!_.isEmpty(q)) {
		_stm.statement += "{";
		_stm_end = "}" + _stm_end;
	}	
	for (_key in q) {
		_stm.statement += _key;
		_stm.statement += ":'";
		_stm.statement += q[_key];
		_stm.statement += "'";
		if (_key != _last) {
			_stm.statement += ",";
		}
	}
	_stm.statement += _stm_end;
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
	return generateStatement(qent, q, "RETURN n");
}

function removeStatement(qent, q) {
	return generateStatement(qent, q, "DETACH DELETE n");
}

function constraintStatement(qent, q) {
/*	var _stm = {};
	var _label = qent.entity$.split('/').reverse()[0];

	_stm.statement = "CREATE CONSTRAINT ON (n:" + _label + ") ASSERT n." + _.keys(q)[0] + " IS UNIQUE";
	return _stm;//*/
}

function uniqueRelationshipStatement(from, to, q) {
	// TODO: Not finished!!!
/*	"MATCH (a:Person),(b:Person) WHERE a.name = 'Node A' AND b.name = 'Node B' CREATE (a)-[r:RELTYPE { name : a.name + '<->' + b.name }]->(b) RETURN r"
	var _stm = {};
	var _label_from = from.entity$.split('/').reverse()[0];
	var _label_to = to.entity$.split('/').reverse()[0];

//	_stm.statement = _.template('MATCH (a:Person),(b:Person) WHERE a.name = 'Node A' AND b.name = 'Node B' CREATE (a)-[r:RELTYPE { name : a.name + '<->' + b.name }]->(b) RETURN r');

	_stm.statement = "MATCH (a:Person),(b:Person) WHERE ";
	_stm.statement 
	return _stm;//*/
}

module.exports.saveStatement = saveStatement;
module.exports.updateStatement = updateStatement;
module.exports.loadStatement = loadStatement;
module.exports.listStatement = listStatement;
module.exports.removeStatement = removeStatement;
module.exports.constraintStatement = constraintStatement;
module.exports.uniqueRelationshipStatement = uniqueRelationshipStatement;