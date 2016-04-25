'use strict'

var util = require('util');

/**
 * This callback type is called `middlewareCallback` and is displayed as a global symbol.
 *
 * @callback middlewareCallback
 * @param 		{Object} 	[err] - Error object
 */

var _ = require('lodash');
var Request = require('request');
var Uuid = require('node-uuid');
var DefaultConfig = require('./default_config.json');
var StatementBuilder = require('./lib/statement-builder.js');
var GraphStore = require('./lib/graph-util');

var Q = require('q');

var Eraro = require('eraro')({
	package: 'neo4j'
});

var _store_name = 'neo4j-store';
var _action_role = 'neo4j';
var _internals = {};

var executeCypher = function(cypher,params) {
	var _deferred = Q.defer();
	var _json = { statements:[ { statement: cypher, parameters: params} ] };
	var _opts = _.clone(_internals.opts);
	_opts.json = _json;
	var _execute = Q.nbind(Request.post, Request);
	
	_execute(_opts)
	.then(function(response) {
		var _body = response[1];
		if (_body) {
			var _errors = _body.errors;
			var _results = _body.results;
			if (_errors && !_.isEmpty(_errors)) {
				_deferred.reject(_errors);
			}
			else {
				var _answer = [];
				_results.forEach(function(result) {
					var _data = result.data;
					if (_data) {
						_data.forEach(function(entry) {
							var _row = entry.row;
							if (_row) {
								_answer.push(_row[0])
							}
						})
					}
				})
				_deferred.resolve(_answer);
			}
		}
		else {
			_deferred.resolve();
		}
	})
	.catch(function(err) {
		_deferred.reject({ err: err });
	});

	return _deferred.promise;
}

module.exports = function(options) {
	var _seneca = this;

	var _opts = _seneca.util.deepextend(DefaultConfig, options);
	_internals = {
		name: _store_name,
		opts: _opts
	}

	var _act = Q.nbind(_seneca.act, _seneca);

	// the store interface returned to seneca
	var store = {

		// methods required by store interface
		name: _store_name,

    	/** @function save
		 *
		 *	@summary Saves entity to database.
		 *
		 *	Save the data as specified in the entitiy block on the arguments object.
		 *
		 *	@since 1.0.0
		 *
		 *	@param 	 	{Object}	args - The arguments.
		 *	@param 	 	{Object}	args.ent - The entity to be saved.
		 *	@param 	 	{middlewareCallback}	next - The next callback in the sequence.
		 *
		 *	@returns 	{Object}	The saved entity.
		 */
		save: function(args, next) {
			_act({ role: _action_role, hook: 'create_save_statement', target: store.name}, args)
			.done(
				function(statementObj) {
					var _cypher = statementObj.query.statement;
					var _params = statementObj.query.parameters;
					executeCypher(_cypher, _params)
					.done(
						function(result) {
							var _ent = args.ent.make$(result[0]);
							_seneca.log(args.tag$, _cypher, _ent);
							return next(null, _ent);
						},
						function(err) {
							_seneca.log.error(_cypher, _params, err);
							return next(err, { code: statementObj.operation, tag: args.tag$, store: store.name, query: _cypher, error: err });
						}
					);
				},
				function(err) {
					_seneca.log.error('Neo4j ' + statementObj.operation + ' error', err);
          			return next(err, { code: err.operation, tag: args.tag$, store: store.name, query: _cypher, error: err.error });
				}
			);
		},

		/** @function save
		 *
		 *	@summary Saves entity to database.
		 *
		 *	Load first matching item based on id.
		 *
		 *	@since 1.0.0
		 *
		 *	@param 	 	{Object}	args - The arguments.
		 *	@param 	 	{Object}	args.q - The query parameters.
		 *	@param 	 	{Object}	args.qent - The entity to be retrieved.
		 *	@param 	 	{middlewareCallback}	next - The next callback in the sequence.
		 *
		 *	@returns 	{Object}	The retrieved entity.
		 */
		load: function(args, next) {
			_act({ role: _action_role, hook: 'create_load_statement', target: store.name}, args)
			.done(
				function(statementObj) {
					var _cypher = statementObj.query.statement;
					var _params = statementObj.query.parameters;
					executeCypher(_cypher, _params)
					.done(
						function(results) {
							if (results[0]) {
								var _ent = args.qent.make$(results[0]);
								_seneca.log(args.tag$, _cypher, _ent);
								return next(null, _ent);
							}
							return next(null, []);
						},
						function(err) {
							_seneca.log.error(_cypher, _params, err);
							return next(err, { code: statementObj.operation, tag: args.tag$, store: store.name, query: _cypher, error: err });
						}
					);
				},
				function(err) {
					_seneca.log.error('Neo4j ' + statementObj.operation + ' error', err);
          			return next(err, { code: err.operation, tag: args.tag$, store: store.name, query: _cypher, error: err.error });
				}
			);
		},

		/** @function list
		 *
		 *	@summary Return a list of objects based on the supplied query, if no query is supplied then return all objects of that type.
		 *
		 *	@since 1.0.0
		 *
		 *	@param 	 	{Object}	args - The arguments.
		 *	@param 	 	{Object}	args.q - The query parameters.
		 *	@param 	 	{Object}	args.qent - The entity to be retrieved.
		 *	@param 	 	{middlewareCallback}	next - The next callback in the sequence.
		 *
		 *	@returns 	{Object}	The list of entities.
		 */
		list: function(args, next) {
			var _count = args.q.count$ || false;
			var _exists = args.q.exists$ || false;
			if (_exists) {
				_count = true;
			}
			_act({ role: _action_role, hook: 'create_list_statement', target: store.name}, args)
			.done(
				function(statementObj) {
					var _cypher = statementObj.query.statement;
					var _params = statementObj.query.parameters;
					executeCypher(_cypher, _params)
					.done(
						function(results) {
							if (_count) {
								return next(null, results);
							}
							if (_exists) {
								return next(null, (results > 0));
							}
							var _list = [];
							results.forEach(function(result) {
								_list.push(args.qent.make$(result));
							});
							_seneca.log(args.tag$, statementObj.operation, null);
							return next(null, _list);
						},
						function(err) {
							_seneca.log.error(_cypher, _params, err);
							return next(err, { code: statementObj.operation, tag: args.tag$, store: store.name, query: _cypher, error: err });
						}
					);
				},
				function(err) {
					_seneca.log.error('Neo4j ' + statementObj.operation + ' error', err);
          			return next(err, { code: err.operation, tag: args.tag$, store: store.name, query: _cypher, error: err.error });
				}
			);
		},

		/** @function remove
		 *
		 *	@summary Delete an object based on the supplied query.
		 *
		 *	@since 1.0.0
		 *
		 *	@param 	 	{Object}	args - The arguments.
		 *	@param 	 	{Object}	args.q - The query parameters.
		 *	@param 	 	{Object}	args.qent - The entity to be retrieved.
		 *	@param 	 	{middlewareCallback}	next - The next callback in the sequence.
		 *
		 *	@returns 	{Object}	The list of entities.
		 */
		remove: function(args, next) {
			var _fetch_single_row = function(args) {
				var _deferred = Q.defer();
				var _q = args.q;
				// all$: if true, all matching entities are deleted; if false only the first entry in the result set; default: false
				// load$: if true, the first matching entry (only, and if any) is the response data; if false, there is no response data; default: false
				if ((_q.load$) || (!_q.all$)) {
					store.load(args, function(err, row) {
						if (err) {
							return _deferred.reject(err);
						}
						if (!row) {
							return _deferred.resolve(-5);
						}
						if (_q.load$) {
							args.row = row;
						}
						// if we're not loading all then we must delete the first match we get back. Since this will be the full object (including id) we'll only match that one
						args.q = GraphStore.makeentp(row);
						return _deferred.resolve();
					});
				}
				else {
					delete _q.all$;
					_deferred.resolve();
				}
				return _deferred.promise;
			}

			_fetch_single_row(args)
			.then(function(res) {
				if (res == -5) {
					return next();
				}
				return _act({ role: _action_role, hook: 'create_remove_statement', target: store.name}, args);
			})
			.done(
				function(statementObj) {
					var _cypher = statementObj.query.statement;
					var _params = statementObj.query.parameters;
					executeCypher(_cypher, _params)
					.done(
						function(results) {
							_seneca.log(args.tag$, statementObj.operation, null);
							var _result = args.row || null;
							return next(null, _result);
						},
						function(err) {
							_seneca.log.error(_cypher, _params, err);
							return next(err, { code: statementObj.operation, tag: args.tag$, store: store.name, query: _cypher, error: err });
						}
					);
				},
				function(err) {
					_seneca.log.error('Neo4j ' + statementObj.operation + ' error', err);
          			return next(err, { code: err.operation, tag: args.tag$, store: store.name, query: _cypher, error: err.error });
				}
			);
		},

		close: function(args, next) {
			// do nothing as we're talking to Neo4j over http - there's no connection to open or close
		},

		native: function(args, next) {
			// TODO: Implement this as necessary
		}
	}

	/**
	 * Initialization
	 */
	var _meta = _seneca.store.init(_seneca, _opts, store);
	_internals.desc = _meta.desc;

	_seneca.add({ init: store.name, tag: _meta.tag }, function(args, next) {
		return next();
	});

	_seneca.add({ role: _action_role, hook: 'create_load_statement'}, function(args, next) {
		var _statement = StatementBuilder.loadStatement(args.qent, args.q);		
		return next(null, { query: _statement, operation: 'load' });
	});

	_seneca.add({ role: _action_role, hook: 'create_list_statement'}, function(args, next) {
		var _q = args.q;
		var _statement;

		if (_q["relationship~"]) {
			_statement = StatementBuilder.retrieveRelatedStatement(args.qent, _q);
			return next(null, { query: _statement, operation: 'list related' });
		}
		_statement = StatementBuilder.listStatement(args.qent, _q);
		return next(null, { query: _statement, operation: 'list' });
	});

	_seneca.add({ role: _action_role, hook: 'create_save_statement'}, function(args, next) {
		var _ent = args.ent;
		// If the entity has an id field this is used as the primary key by the underlying database and the save is considered an update operation.
		var _update = !!_ent.id;
		var _statement;

		if (_ent["relationship~"]) {
			_statement = StatementBuilder.uniqueRelationshipStatement(_ent);
			return next(null, { query: _statement, operation: 'save relationship' })
		}

		if (_update) {
			_statement = StatementBuilder.updateStatement(_ent);
			return next(null, { query: _statement, operation: 'update' });
		}

		// If the entity has an id$ field this is used as the primary key and the save is considered to be an insert operation using the specified key.
		if (_ent.id$) {
			_end.id = ent.id$;
			_statement = StatementBuilder.saveStatement(_ent);
			return next(null, { query: _statement, operation: 'save' });
		}
		// If the entity does not have an id field one must be generated and the save is considered an insert operation.
		_act({ role: _action_role, hook: 'generate_id', target: args.target })
		.done(
			function(result) {
				_ent.id = result.id;
				_statement = StatementBuilder.saveStatement(_ent);
				return next(null, { query: _statement, operation: 'save' });
			},
			function(err) {
				_seneca.log.error('hook generate_id failed');
				return next(err);
			}
		);
	});

	_seneca.add({ role: _action_role, hook: 'create_remove_statement'}, function(args, next) {
		var _statement = StatementBuilder.removeStatement(args.qent, args.q);
		return next(null, { query: _statement, operation: 'remove' });
	});

	_seneca.add({ role: _action_role, hook: 'generate_id', target: store.name}, function(args, next) {
		return next(null, { id: Uuid() });
	});

	return { name: store.name, tag: _meta.tag}
}