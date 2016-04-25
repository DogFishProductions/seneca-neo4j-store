'use strict'

var util = require('util');

var _ = require('lodash');
var Seneca = require('seneca');
var DatabaseConfig = require('./default_config.json');

var _si = Seneca({
  default_plugins: {
    'mem-store': false
  }
});
var _action_role = 'graphstore_role';
var _cmds = [ 'save', 'load', 'list', 'remove' ];

_si.use(require('./neo4j-store.js'), { map: { '-/neo4j/-': _cmds } });
_si.ready(function(err, response) {
	if (err) {
		_seneca.log.error(_action_role + '_store ready error', err);
	}
	_si.listen( { port:9001, type: 'tcp', pin:'role:' + _action_role } );
});

var createEntity = function(args) {
	var _ent = args.ent;
	var _node = _si.make(_ent.zone, _ent.base);
	var _props = _ent.properties || {};
	var _keys = _.keys(_props);
	_keys.forEach(function(key) {
		_node[key] = _props[key];
	});
	return _node;
}

_si.add({ role: _action_role, hook: 'save' }, function(args, next) {
	createEntity(args).save$(function(err, new_node) {
		next(null, new_node);
	});
});

_si.add({ role: _action_role, hook: 'load' }, function(args, next) {
	var _opts = args.ent.options || {};
	createEntity(args).load$(_opts, function(err, loaded_node) {
		next(null, loaded_node);
	});
});

_si.add({ role: _action_role, hook: 'list' }, function(args, next) {
	var _opts = args.ent.options || {};
	createEntity(args).list$(_opts, function(err, node_list) {
		next(null, node_list);
	});
});

_si.add({ role: _action_role, hook: 'remove' }, function(args, next) {
	var _opts = args.ent.options || {};
	createEntity(args).remove$(_opts, function(err, removed_node) {
		next(null, removed_node);
	});
});

_si.add({ role: _action_role }, function(args, next) {
	if (_.isEmpty(args.label)) {
		next(new Error("Expected entity label."));
	}

	this.prior({ role: _action_role }, function(err, result) {
		if (err) {
			return next(err);
		}
		next(null, result);
	})
})