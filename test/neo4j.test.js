/* jslint node: true */
/* Copyright (c) 2016 Paul Nebel */

'use strict'

var Seneca = require('seneca')
var Shared = require('seneca-store-test')
var Relationships = require('./neo4j.relationship.test.js')
var Fs = require('fs')

var Lab = require('lab')
var _ = require('lodash')
var lab = exports.lab = Lab.script()
var before = lab.before
var describe = lab.describe

var dbConfig
if (Fs.existsSync(__dirname + '/dbconfig.mine.js')) {
  dbConfig = require('./dbconfig.mine')
}
else {
  dbConfig = require('./dbconfig.example')
}

var si = Seneca({
  default_plugins: {
    'mem-store': false
  }
})

if (si.version >= '2.0.0') {
  si.use('entity')
}

var senecaMerge = Seneca({
  default_plugins: {
    'mem-store': false
  }
})

if (si.version >= '2.0.0') {
  senecaMerge.use('entity')
}

describe('Neo4J suite tests ', function () {
  before({}, function (done) {
    var mergeConfig = _.cloneDeep(dbConfig)
    mergeConfig.merge = false
    senecaMerge.use(require('../neo4j-store.js'), mergeConfig)
    si.use(require('../neo4j-store.js'), dbConfig)
    si.ready(done)
  })

  Shared.basictest({
    seneca: si,
    senecaMerge: senecaMerge,
    script: lab
  })

  Shared.sorttest({
    seneca: si,
    script: lab
  })

  Shared.limitstest({
    seneca: si,
    script: lab
  })

  Shared.extended({
    seneca: si,
    script: lab
  })

  Relationships.basictest({
    seneca: si,
    script: lab
  })

  Relationships.sorttest({
    seneca: si,
    script: lab
  })

  Relationships.limitstest({
    seneca: si,
    script: lab
  })

  Relationships.cyphertest({
    seneca: si,
    script: lab
  })

  Relationships.extendedtest({
    seneca: si,
    script: lab
  })
})
