/* Copyright (c) 2010-2014 Michele Capra, MIT License */
'use strict';


var neo4j = require('neo4j');
var typeis = require('type-of-is');

var name = "neo4j-store";


module.exports = function (opts) {
    var seneca = this;
    var desc;
    var dbinst = null;
    var specifications = null;

    function createCollectionName(entity) {
        var canon = entity.canon$({object: true});
        return (canon.base ? canon.base + '_' : '') + canon.name;
    }

    function error(args, err, cb) {
        if (err) {
            seneca.log.error('entity', err, {store: name});
            return true;
        }
        return false;
    }

    function composeConnectionstring(conf) {
        var connectionString = 'http://';
        var port = ':7474';
        var password = '';
        var username = '';
        var host = 'localhost';

        if (conf.username) {
            username = conf.username;
            password = '@';
        }
        if (conf.password) {
            password = ':' + conf.password + '@';
        }
        if (conf.host) {
            host = conf.host;
        }
        if (conf.port) {
            port = ':' + conf.port;
        }

        connectionString += username + password + host + port;

        return connectionString;
    }

    function configure(spec, cb) {
        specifications = spec;

        var conf = 'string' === typeof(spec) ? null : spec;

        var dbopts = seneca.util.deepextend({
            native_parser: false,
            auto_reconnect: true,
            w: 1
        }, conf.options);


        var connectionstring = composeConnectionstring(conf);

        dbinst = new neo4j.GraphDatabase(connectionstring);

        //check the state of the connection after opening it
        seneca.log.debug('init', 'db open', dbopts);

        cb(null);
    }

    function generateUUID() {
        var d = new Date().getTime();
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
            var r = (d + Math.random() * 16) % 16 | 0;
            d = Math.floor(d / 16);
            return (c === 'x' ? r : (r & 0x7 | 0x8)).toString(16);
        });
    }


    function composeWhereClause(q) {
        var j = 0;
        var whereclause = '';
        var fieldName = '';
        if (!q.all$ || q.all$ !== true) {
            for (var field in q) {
                if (j === 0) {
                    whereclause += ' WHERE ';
                }
                if (field.toLowerCase() === 'id') {
                    fieldName = 'custom_id';
                }
                else {
                    fieldName = field;
                }

                if (typeis.string(q[field]) === 'String' || !q[field]) {
                    whereclause += ' ent.' + fieldName + ' = \'' + q[field] + '\'';
                }
                else {
                    whereclause += ' ent.' + fieldName + ' = ' + q[field];
                }

                j++;

                if (j !== 0 && j < Object.keys(q).length) {
                    whereclause += " AND ";
                }

            }

        }
        return whereclause;
    }

    function deserializeObject(entp, index) {
        for (var p in entp[index].ent.data) {
            if (typeis.string(entp[index].ent.data[p]) === 'String') {
                //everytime i try do deserialize strings
                try {
                    entp[index].ent.data[p] = JSON.parse(entp[index].ent.data[p]);
                }
                catch (exc) {
                    var temp = 0;
                }
            }
        }
    }

    function createSenecaObjectsFromNodes(entp, qent) {
        var senecaObj = null;
        var results = [];
        for (var i = 0; i < entp.length; i++) {
            deserializeObject(entp, i);

            //create seneca object
            senecaObj = qent.make$(entp[i].ent.data);
            senecaObj.id = entp[i].ent.data.custom_id;

            results.push(senecaObj);
        }
        return results;
    }

    function queryoption(q) {
        var suffix = '';

        if (!q.native$) {

            if (q.sort$) {
                // ORDER BY performance_count DESC
                var sf = Object.keys(q.sort$)[0];
                var sd = q.sort$[sf] < 0 ? 'descending' : 'ascending';
                suffix += ' ORDER BY ' + sf + ' ' + sd;
            }

            if (q.limit$) {
                // limit 25
                suffix += ' LIMIT ' + q.limit$ + '\n';
            }

            if (q.skip$) {
                // skip 3
                suffix.skip += ' SKIP ' + q.skip$ + '\n';
            }
        }

        return suffix;
    }


    var store = {
        name: name,

        /**
         * close the connection
         *
         * params
         * cb - callback
         */
        close: function (args, cb) {
            dbinst = null;
        },


        /**
         * save the data as specified in the ent attribute of the args object
         * params
         * args -
         * cb - callback
         */
        save: function (args, cb) {
            var ent = args.ent;

            var query = '';
            var collectionname = createCollectionName(ent);
            var fields = ent.fields$();


            //making the query on custom_id and not on id
            if (ent.id) {
                if (ent.custom_id) {
                    query = [
                        'MATCH ent ',
                            'WHERE ent.custom_id= \'' + ent.custom_id + '\'',
                        'RETURN ent'
                    ].join('\n');
                }
                else {
                    query = [
                        'MATCH ent ',
                            'WHERE ent.custom_id= \'' + ent.id + '\'',
                        'RETURN ent'
                    ].join('\n');
                }
            }
            else {

                if(!ent.id && ent.id$){
                    query = [
                            'create (ent:' + collectionname ,
                            '{ custom_id:\'' +ent.id$ + '\'}',
                        ')',
                        'RETURN ent'
                    ].join('\n');
                }
                else {
                    query = [
                            'create (ent:' + collectionname ,
                            '{ custom_id:\'' + generateUUID() + '\'}',
                        ')',
                        'RETURN ent'
                    ].join('\n');
                }
            }

            seneca.log.debug(args.actid$, ' - mik - query:', query.replace(/(\r\n|\n|\r)/gm, ""));

            dbinst.query(query, null, function (err, results) {

                if (!error(args, err, cb)) {
                    seneca.log.debug('save/insert', results, desc);
                    if (!results[0]) {
                        cb(null, null);
                        return;
                    }
                    //only for custom id changed by user
                    if (results[0].ent.data && ent.id && ent.id !== results[0].ent.data.custom_id) {
                        results[0].ent.data.custom_id = ent.id;
                    }

                    //serializing objects
                    fields.forEach(function (field) {
                        if (typeis.string(ent[field]) === 'Object' || typeis.string(ent[field]) === 'Array') {
                            results[0].ent.data[field] = JSON.stringify(ent[field]);
                        }
                        else {
                            results[0].ent.data[field] = ent[field];
                        }
                    });

                    //saving node with properties
                    results[0].ent.save(function (err, result) {
                        if (!error(args, err, cb)) {
                            ent.id = result.data.custom_id;
                            cb(null, ent);
                        }
                    });

                }
                else {
                    cb(null, null);
                }
            });

        },

        /**
         * load first matching item based on matching property values
         * in the q attribute of the args object
         * params
         * args -
         * cb - callback
         */
        load: function (args, cb) {
            var qent = args.qent;
            var q = args.q;

            if (qent.id) {
                qent.custom_id = qent.id;
            }

            var query = [
                'MATCH ent',
                composeWhereClause(q),
                'RETURN ent',
                queryoption(q)
            ].join('\n');

            seneca.log.debug(args.actid$, ' - mikload-  query:', query.replace(/(\r\n|\n|\r)/gm, ""));

            dbinst.query(query, null, function (err, entp) {

                if (!error(args, err, cb)) {
                    var senecaObj = null;
                    if (entp[0]) {

                        senecaObj = qent.make$(entp[0].ent.data);

                        deserializeObject(entp, 0);

                        senecaObj.id = entp[0].ent.data.custom_id;
                    }

                    seneca.log.debug('load', q, senecaObj, desc);
                    cb(null, senecaObj);
                }
            });
        },

        /**
         * return a list of object based on the supplied query, if no query is supplied
         * then all items are selected
         *
         * params
         * args -
         * cb - callback
         */
        list: function (args, cb) {
            var qent = args.qent;
            var q = args.q;


            var query = [
                    'MATCH (ent:' + createCollectionName(qent) + ') ',
                composeWhereClause(q),
                'RETURN ent',
                queryoption(q)
            ].join('\n');

            seneca.log.debug(args.actid$, 'query:', query);

            dbinst.query(query, null, function (err, entp) {
                if (!error(args, err, cb)) {
                    var results = createSenecaObjectsFromNodes(entp, qent);
                    seneca.log.debug('list', q, results, desc);
                    cb(null, results);
                }
            });
        },

        /**
         * delete an item
         *
         * params
         * args -
         * cb - callback
         */
        remove: function (args, cb) {
            var qent = args.qent;
            var q = args.q;

            var query = [
                    'MATCH (ent:' + createCollectionName(qent) + ') ',
                composeWhereClause(q),
                'delete ent'
            ].join('\n');
            seneca.log.debug(args.actid$, 'query:', query);
            dbinst.query(query, null, function (err, entp) {
                if (!error(args, err, cb)) {
                    seneca.log.debug('remove', q, [], desc);
                    cb(null, []);
                }
            });

        },

        /**
         * return the underlying native connection object
         *
         * params
         * cb - callback
         */
        native: function (args, cb) {
            if (dbinst) {
                cb(null, dbinst);
            } else {
                cb(new Error());
            }


        }
    };

    var meta = seneca.store.init(seneca, opts, store);
    desc = meta.desc;


    seneca.add({init: store.name, tag: meta.tag}, function (args, done) {
        configure(opts, function (err) {
            if (err) {
                return seneca.die('store', err, {store: store.name, desc: desc});
            }
            return done();
        });
    });


    return {name: store.name, tag: meta.tag};
};












