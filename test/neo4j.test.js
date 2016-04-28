/* jslint node: true */
/* Copyright (c) 2016 Paul Nebel */

'use strict'

var Seneca = require('seneca')
var Shared = require('seneca-store-test')
// var Extra = require('./neo4j.ext.test.js')
var Fs = require('fs')

var Lab = require('lab')
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

describe('Neo4J suite tests ', function () {
  before({}, function (done) {
    si.use(require('../neo4j-store.js'), dbConfig)
    si.ready(done)
  })

  Shared.basictest({
    seneca: si,
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

/*
  Extra.extendTest({
    seneca: si,
    script: lab
  });//*/
})
