/* Copyright (c) 2010-2014 Michele Capra, MIT License */
"use strict";


var _ = require('underscore');
var neo4j = require('neo4j');
var seneca = require('seneca');
var typeis = require('type-of-is');

var name = "neo4j-store";

function createCollectionName(entity) {
    var canon = entity.canon$({object: true}),
        colName = (canon.base ? canon.base + '_' : '') + canon.name;

    return colName;
}


module.exports = function (opts) {
    var seneca = this;
    var desc;

    var dbinst = null;
    var specifications = null;


    function error(args, err, cb) {
        if (err) {
            seneca.log.error('entity', err, {store: name});
            return true;
        }
        else return false;
    }

    function configure(spec, cb) {
        specifications = spec;

        var conf = 'string' == typeof(spec) ? null : spec;

        var dbopts = seneca.util.deepextend({
            native_parser: false,
            auto_reconnect: true,
            w: 1
        }, conf.options);

        dbinst = new neo4j.GraphDatabase('http://localhost:7474');

        //check the state of the connection after opening it
        seneca.log.debug('init', 'db open', dbopts);

        cb(null)
    }

    function generateUUID() {
        var d = new Date().getTime();
        var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
            var r = (d + Math.random() * 16) % 16 | 0;
            d = Math.floor(d / 16);
            return (c == 'x' ? r : (r & 0x7 | 0x8)).toString(16);
        });
        return uuid;
    };


    function composeWhereClause(q) {
        var j = 0;
        var whereclause = '';
        if (!q.all$ || q.all$ !== true) {
            for (var field in q) {
                if (j === 0) {
                    whereclause += ' WHERE '
                }

                if (field.toLowerCase() === 'id') {
                    if (typeis.string(q[field]) == 'String') {
                        whereclause += ' ent.custom_id = \'' + q[field] + '\'';
                    }
                    else {
                        whereclause += ' ent.custom_id = ' + q[field];
                    }
                }
                else {
                    if (typeis.string(q[field]) == 'String') {
                        whereclause += ' ent.' + field + ' = \'' + q[field] + '\'';
                    }
                    else {
                        whereclause += ' ent.' + field + ' = ' + q[field];
                    }
                }
                j++;
                if (j !== 0 && j < Object.keys(q).length) {
                    whereclause += " AND ";
                }

            }
            ;
        }
        return whereclause;
    }

    function deserializeObject(entp,index) {
        for (var p in entp[index].ent.data) {
            if (typeis.string(entp[index].ent.data[p]) === 'String') {
                //provo sempre a deserializzare un campo in oggetto perchè non ho modo di sapere se è un json o no
                try {
                    entp[index].ent.data[p] = JSON.parse(entp[index].ent.data[p]);
                }
                catch (exc) {

                }
            }
        }
    }

    var store = {
        name: name,

        close: function (args, cb) {
            return cb();
        },

        save: function (args, cb) {
            var ent = args.ent;


            var collectionname = createCollectionName(ent);
            var fields = ent.fields$();

            //making the query on custom_id and not on id
            if (ent.id) {
                var query = [
                    'MATCH ent ',
                        'WHERE ent.custom_id= \'' + ent.custom_id + '\'',
                    'RETURN ent'
                ].join('\n');
            }
            else {
                var query = [
                        'create (ent:' + collectionname ,
                        '{ custom_id:\'' + generateUUID() + '\'}',
                    ')',
                    'RETURN ent'
                ].join('\n');
            }
            dbinst.query(query, null, function (err, results) {

                if (!error(args, err, cb)) {
                    seneca.log.debug('save/insert', results, desc)

                    //only for custom id changed by user
                    if (results[0].ent.data && ent.id$ && ent.id$ !== results[0].ent.data.custom_id) {
                        results[0].ent.data.custom_id = ent.id$;
                    }

                    //serializing objects
                    fields.forEach(function (field) {
                        if (typeis.string(ent[field]) == 'Object') {
                            results[0].ent.data[field] = JSON.stringify(ent[field])
                        }
                        else {
                            results[0].ent.data[field] = ent[field]
                        }
                    });

                    //saving node with properties
                    results[0].ent.save(function (err, result) {
                        if (!error(args, err, cb)) {
                            ent.id = result.data.custom_id;
                            cb(null, ent);
                        }
                    })

                }
                else {
                    cb(null, null)
                }
            });

        },

        load: function (args, cb) {
            var qent = args.qent;
            var q = args.q;

            qent.custom_id = qent.id;

            var query = [
                'MATCH ent',
                    'WHERE ent.custom_id = \'' + q.id + '\'',
                'RETURN ent'
            ].join('\n');

            dbinst.query(query, null, function (err, entp) {
                if (!error(args, err, cb)) {
                    var senecaObj = null;
                    if (entp[0]) {
                        senecaObj = qent.make$(entp[0].ent.data);

                        deserializeObject(entp,0);
                        senecaObj.id = entp[0].ent.data.custom_id;
                    }

                    seneca.log.debug('load', q, senecaObj, desc);
                    cb(null, senecaObj);
                }
            });
        },

        list: function (args, cb) {
            var qent = args.qent;
            var q = args.q;

            var query = [
                'MATCH (ent:' + createCollectionName(qent) + ') ',
                composeWhereClause(q),
                'RETURN ent'
            ].join('\n');

            dbinst.query(query, null, function (err, entp) {
                if (!error(args, err, cb)) {
                    var senecaObj = null;
                    var results = [];
                    for (var i = 0; i < entp.length; i++) {
                        deserializeObject(entp,i);

                        //create seneca object
                        senecaObj = qent.make$(entp[i].ent.data);
                        senecaObj.id = entp[i].ent.data.custom_id;

                        results.push(senecaObj)
                    }

                    seneca.log.debug('list', q, results, desc);
                    cb(null, results);
                }
            });
        },

        remove: function (args, cb) {
            var qent = args.qent;
            var q = args.q;

            var query = [
                'MATCH (ent:' + createCollectionName(qent) + ') ',
                composeWhereClause(q),
                'delete ent'
            ].join('\n');

            dbinst.query(query, null, function (err, entp) {
                if (!error(args, err, cb)) {
                    seneca.log.debug('remove', q, [], desc);
                    cb(null, []);
                }
            });

        },

        native: function (args, done) {
            // todo
        }
    }


    var meta = seneca.store.init(seneca, opts, store);
    desc = meta.desc;


    seneca.add({init: store.name, tag: meta.tag}, function (args, done) {
        configure(opts, function (err) {
            if (err) return seneca.die('store', err, {store: store.name, desc: desc});
            return done();
        })
    });


    return {name: store.name, tag: meta.tag}
};












