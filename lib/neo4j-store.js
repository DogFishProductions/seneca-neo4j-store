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

function metaquery(qent, q) {
    var mq = {};

    if (!q.native$) {

        if (q.sort$) {
            for (var sf in q.sort$) break;
            var sd = q.sort$[sf] < 0 ? 'descending' : 'ascending'
            mq.sort = [
                [sf, sd]
            ]
        }

        if (q.limit$) {
            mq.limit = q.limit$;
        }

        if (q.skip$) {
            mq.skip = q.skip$;
        }

        if (q.fields$) {
            mq.fields = q.fields$;
        }
    }
    else {
        mq = _.isArray(q.native$) ? q.native$[1] : mq;
    }

    return mq
}


module.exports = function (opts) {
    var seneca = this;
    var desc;

    var usenativeid = false; // opts.nativeid != undefined ? opts.nativeid: false
    var dbinst = null;
    var collmap = {};
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


    var store = {
        name: name,

        close: function (args, cb) {
            //close db conn
//            if (dbinst) {
//                dbinst.close(cb)
//            }
            //else
            return cb();
        },


        save: function (args, cb) {
            var ent = args.ent;
            var querydata = '';

            //save node
            var entp = {};
            var collectionname = createCollectionName(ent);
            var fields = ent.fields$();

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

                    fields.forEach(function (field) {
                        if (typeis.string(ent[field]) == 'Object') {
                            results[0].ent.data[field] = JSON.stringify(ent[field])
                        }
                        else {
                            results[0].ent.data[field] = ent[field]
                        }

                    });

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
            var qq = {};
            var collectionname = createCollectionName(qent);

            qent.custom_id = qent.id;

            //load a node by id
            if (usenativeid) {
                var query = [
                        'MATCH (ent:' + collectionname + ')',
                        'WHERE ent.custom_id = \'' + q.id + '\'',
                    'RETURN ent'
                ].join('\n');
            }
            else {
                var query = [
                    'MATCH ent',
                        'WHERE ent.custom_id = \'' + q.id + '\'',
                    'RETURN ent'
                ].join('\n');
            }


            dbinst.query(query, null, function (err, entp) {
                if (!error(args, err, cb)) {
                    var fent = null;
                    if (entp[0]) {
                        fent = qent.make$(entp[0].ent.data);

                        //provo a deserializzare gli oggetti
                        for (var p in entp[0].ent.data) {
                            if (typeis.string(entp[0].ent.data[p]) === 'String') {
                                //provo sempre a deserializzare un campo in oggetto perchè non ho modo di sapere se è un json o no
                                try {
                                    entp[0].ent.data[p] = JSON.parse(entp[0].ent.data[p]);
                                }
                                catch (exc) {

                                }
                            }
                        }
                        if (usenativeid) {
                            fent.id = entp[0].ent.data.custom_id;
                        }
                        fent.id = entp[0].ent.data.custom_id;
                    }

                    seneca.log.debug('load', q, fent, desc);
                    cb(null, fent);
                }
            });
        },


        list: function (args, cb) {
            var qent = args.qent;
            var q = args.q;
            var whereclause = '';
            var j = 0;
            for(var field in q) {
                if(j===0){
                   whereclause += ' WHERE '
                }

                if(field.toLowerCase()==='id'){
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
                if (j !== 0 && j<Object.keys(q).length) {
                    whereclause += " AND ";
                }

            };

            var query = [
                    'MATCH (ent:' + createCollectionName(qent) + ') ',

                whereclause,
                'RETURN ent'
            ].join('\n');

            dbinst.query(query, null, function (err, entp) {
                if (!error(args, err, cb)) {
                    var fent = null;
                    var results = [];
                    for (var i = 0; i < entp.length; i++) {
                        for (var p in entp[i].ent.data) {
                            if (typeis.string(entp[i].ent.data[p]) === 'String') {
                                //provo sempre a deserializzare un campo in oggetto perchè non ho modo di sapere se è un json o no
                                try {
                                    entp[i].ent.data[p] = JSON.parse(entp[i].ent.data[p]);
                                }
                                catch (exc) {

                                }
                            }
                        }
                        fent = qent.make$(entp[i].ent.data);
                        if (usenativeid) {
                            fent.id = entp[i].ent.data.custom_id;
                        }
                        fent.id = entp[i].ent.data.custom_id;
                        results.push(fent)
                    }

                    seneca.log.debug('load', q, results, desc);
                    cb(null, results);
                }
            });
        },


        remove: function (args, cb) {
            var qent = args.qent;
            var q = args.q;
            var whereclause = '';
            var j = 0;
            if(!q.all$ || q.all$ !== true) {
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
            var query = [
                    'MATCH (ent:' + createCollectionName(qent) + ') ',

                whereclause,
                'delete ent'
            ].join('\n');

            dbinst.query(query, null, function (err, entp) {
                if (!error(args, err, cb)) {
                    var fent = null;
                    var results = [];
                    seneca.log.debug('load', q, results, desc);
                    cb(null, results);
                }
            });

        },

        native: function (args, done) {
            dbinst.collection('seneca', function (err, coll) {
                if (!error(args, err, done)) {
                    coll.findOne({}, {}, function (err, entp) {
                        if (!error(args, err, done)) {
                            done(null, dbinst)
                        }
                        else {
                            done(err)
                        }
                    })
                }
                else {
                    done(err)
                }
            })
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












