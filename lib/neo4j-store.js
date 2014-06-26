/* Copyright (c) 2010-2014 Michele Capra, MIT License */
"use strict";


var _ = require('underscore');
var neo4j = require('neo4j');
var seneca = require('seneca');
var typeis = require('type-of-is');

var name = "neo4j-store"

function createCollectionName(entity) {
    var canon = entity.canon$({object: true}),
        colName = (canon.base ? canon.base + '_' : '') + canon.name;

    return colName;
}

function metaquery(qent, q) {
    var mq = {}

    if (!q.native$) {

        if (q.sort$) {
            for (var sf in q.sort$) break;
            var sd = q.sort$[sf] < 0 ? 'descending' : 'ascending'
            mq.sort = [
                [sf, sd]
            ]
        }

        if (q.limit$) {
            mq.limit = q.limit$
        }

        if (q.skip$) {
            mq.skip = q.skip$
        }

        if (q.fields$) {
            mq.fields = q.fields$
        }
    }
    else {
        mq = _.isArray(q.native$) ? q.native$[1] : mq
    }

    return mq
}


module.exports = function (opts) {
    var seneca = this
    var desc

    var usenativeid = false // opts.nativeid != undefined ? opts.nativeid: false
    var dbinst = null
    var collmap = {}
    var specifications = null


    function error(args, err, cb) {
        if (err) {
            seneca.log.error('entity', err, {store: name})
            return true;
        }
        else return false;
    }

    function configure(spec, cb) {
        specifications = spec

        var conf = 'string' == typeof(spec) ? null : spec

        var dbopts = seneca.util.deepextend({
            native_parser: false,
            auto_reconnect: true,
            w: 1
        }, conf.options)

        dbinst = new neo4j.GraphDatabase('http://localhost:7474');

        //check the state of the connection after opening it
        seneca.log.debug('init', 'db open', dbopts)

        cb(null)
    }


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
            var ent = args.ent
            var new_obj = true
            var querydata = '';

            //save node
            var entp = {};
            var collectionname = createCollectionName(ent)
            var fields = ent.fields$()


            if(entp.id) {
                var query = [
                    'Merge (ent:' + collectionname + " {",
                    'ent.id='+entp.id,
                    '})',
                    'RETURN ent',
                ].join('\n');
            }
            else{
                var query = [
                    'create (ent:' + collectionname + ')',
                    'RETURN ent',
                ].join('\n');
            }
            dbinst.query(query, null, function (err, results) {

                if (!error(args, err, cb)) {
                    seneca.log.debug('save/insert', results, desc)

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
                            ent.id = result.id
                            cb(null, ent);
                        }
                    })
                    //adding the new id coming from db
//                    if (usenativeid) {
//                        if (!results[0].ent.data._id) {
//                            results[0].ent.data._id = results[0].ent.id
//                        }
//
//                        ent.id = results[0].ent.data._id
//                        results[0].ent.save(function (err, result) {
//                            if (!error(args, err, cb)) {
//                                cb(null, ent);
//                            }
//                        })
//
//                    }
//                    else {
//                        ent.id = results[0].ent.id
//                        cb(null, ent)
//                    }

                }
                else {
                    cb(null, null)
                }
            });

        },

        load: function (args, cb) {
            var qent = args.qent
            var q = args.q
            var qq = {};
            var collectionname = createCollectionName(qent)

            //load a node by id
            if (usenativeid) {
                var query = [
                        'MATCH (ent:' + collectionname + ')',
                    'WHERE ent._id = {userId}',
                    'RETURN ent'
                ].join('\n');
            }
            else {
                var query = [
                    'MATCH ent',
                    'WHERE ID(ent) = {userId}',
                    'RETURN ent'
                ].join('\n');
            }

            var params = {
                userId: q
            };

            dbinst.query(query, params, function (err, entp) {
                if (!error(args, err, cb)) {
                    var fent = null;
                    if (entp[0]) {
                        fent = qent.make$(entp[0].ent.data);
                        if (usenativeid) {
                            fent.id = entp[0].ent.data._id
                        }
                        fent.id = entp[0].ent.id
                    }

                    seneca.log.debug('load', q, fent, desc)
                    cb(null, fent);
                }
            });
        },


        list: function (args, cb) {
            var qent = args.qent
            var q = args.q

            //list all nodes with of with particular tag
        },


        remove: function (args, cb) {
            var qent = args.qent
            var q = args.q

            var all = q.all$ // default false
            var load = _.isUndefined(q.load$) ? true : q.load$ // default true

            var query = [
                'MATCH (ent)',
                'WHERE ID(ent) = {userId}',
                'DELETE ent',
            ].join('\n')

            var params = {
                userId: qent.id
            };

            db.query(query, params, function (err) {
                if (!error(args, err))
                    cb(null)
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


    var meta = seneca.store.init(seneca, opts, store)
    desc = meta.desc


    seneca.add({init: store.name, tag: meta.tag}, function (args, done) {
        configure(opts, function (err) {
            if (err) return seneca.die('store', err, {store: store.name, desc: desc});
            return done();
        })
    })


    return {name: store.name, tag: meta.tag}
}












