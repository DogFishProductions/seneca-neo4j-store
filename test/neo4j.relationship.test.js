/* Copyright (c) 2014 Richard Rodger, MIT License */
'use strict'

var Assert = require('chai').assert
var Async = require('async')
var _ = require('lodash')
var Lab = require('lab')

var reltemplate = {
  relatedNodeLabel: 'bar',
  type: 'RELATED_TO',
  data: {
    str: 'aaa',
    int: 11,
    dec: 33.33,
    bol: false,
    wen: new Date(2020, 1, 1),
    arr: [ 2, 3 ],
    obj: {
      a: 1,
      b: [2],
      c: { d: 3 }
    }
  }
}

var relverify = function (bar) {
  Assert.equal(bar.str, 'aaa')
  Assert.equal(bar.int, 11)
  Assert.equal(bar.dec, 33.33)
  Assert.equal(bar.bol, false)
  Assert.equal(_.isDate(bar.wen) ? bar.wen.toISOString() : bar.wen, new Date(2020, 1, 1).toISOString())
  Assert.equal('' + bar.arr, '' + [ 2, 3 ])
  Assert.deepEqual(bar.obj, {
    a: 1,
    b: [2],
    c: { d: 3 }
  })
}

function verify (cb, tests) {
  return function (error, out) {
    if (error) {
      return cb(error)
    }

    try {
      tests(out)
    }
    catch (ex) {
      return cb(ex)
    }

    cb()
  }
}

function clearDb (si) {
  return function clear (done) {
    Async.series([
      function clearFoo (next) {
        si.make('foo').remove$({ all$: true }, next)
      },
      function clearBar (next) {
        si.make('bar').remove$({ all$: true }, next)
      }
    ], done)
  }
}

function createEntities (si, name, data) {
  return function create (done) {
    Async.each(data, function (el, next) {
      si.make$(name, el).save$(next)
    }, done)
  }
}

function createRelationships (si, name, data, relationships) {
  return function create (done) {
    Async.each(relationships, function (el, next) {
      si.make$(name, data).saveRelationship$(el, next)
    }, done)
  }
}

function basictest (settings) {
  var si = settings.seneca
  var script = settings.script || Lab.script()

  var describe = script.describe
  var it = script.it
  var before = script.before
  var beforeEach = script.beforeEach

  describe('Relationship Basic Tests', function () {
    describe('Load', function () {
      before(clearDb(si))
      before(createEntities(si, 'foo', [{
        id$: 'source',
        p1: 'v1',
        p2: 'v2'
      }]))
      before(createEntities(si, 'bar', [{
        id$: 'sink',
        p1: 'v2',
        p2: 'z2'
      }]))
      before(createRelationships(si, 'foo', { id: 'source' }, [{ relationship$: reltemplate }]))

      it('should load a related entity', function (done) {
        var foo = si.make('foo', { id$: 'source' })
        foo.load$({ relationship$: reltemplate }, verify(done, function (bar) {
          Assert.isNotNull(bar)
          Assert.equal(bar.id, 'sink')
          Assert.equal(bar.p1, 'v2')
          Assert.equal(bar.p2, 'z2')
        }))
      })

      it('should return null for non existing relationship', function (done) {
        var foo = si.make('foo', { id: 'source' })
        var rel = {
          relatedNodeLabel: 'bar',
          type: 'DOES_NOT_EXIST',
          data: {}
        }
        foo.load$({ relationship$: rel }, verify(done, function (bar) {
          Assert.isNull(bar)
        }))
      })

      it('should return null for non existing entity', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.load$({ relationship$: reltemplate, p1: 'does-not-exist' }, verify(done, function (bar) {
          Assert.isNull(bar)
        }))
      })

      it('should support filtering', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.load$({ relationship$: reltemplate, p1: 'v2' }, verify(done, function (bar) {
          Assert.isNotNull(bar)
          Assert.equal(bar.id, 'sink')
          Assert.equal(bar.p1, 'v2')
          Assert.equal(bar.p2, 'z2')
        }))
      })

      it('should filter with AND', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.load$({ relationship$: reltemplate, p1: 'v2', p2: 'z2' }, verify(done, function (bar) {
          Assert.isNotNull(bar)
          Assert.equal(bar.id, 'sink')
          Assert.equal(bar.p1, 'v2')
          Assert.equal(bar.p2, 'z2')
        }))
      })

      it('should filter with AND 2', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.load$({ relationship$: reltemplate, p1: 'v2', p2: 'a' }, verify(done, function (bar) {
          Assert.isNull(bar)
        }))
      })

      it('should do nothing if no relationship query provided and id not present', function (done) {
        var foo = si.make('foo')
        foo.load$({ relationship$: {} }, verify(done, function (bar) {
          Assert.isNull(bar)
        }))
      })
    })

    describe('Save', function () {
      beforeEach(clearDb(si))
      beforeEach(createEntities(si, 'foo', [{
        id$: 'source',
        p1: 'v1',
        p2: 'v2',
        p3: 'v3'
      }]))
      beforeEach(createEntities(si, 'bar', [{
        id$: 'destination',
        p1: 'v1',
        p2: 'v2',
        p3: 'v3'
      }]))

      it('should save a relationship to store', function (done) {
        var bar = si.make('bar')
        bar.id$ = 'sink'
        bar.p1 = 'v1'
        bar.p2 = 'v2'
        var bam = si.make('bam')
        bam.id$ = 'unrelated'
        bam.p1 = 'v1'
        bam.p2 = 'v2'

        bar.save$(function (err, bar1) {
          Assert.isNull(err)
          var foo = si.make('foo', { p1: 'v1' })
          foo.saveRelationship$({ relationship$: reltemplate }, function (err, rel1) {
            Assert.isNull(err)
            relverify(rel1.data$(false))
            foo.list$({ relationship$: reltemplate }, verify(done, function (res) {
              Assert.lengthOf(res, 2)
            }))
          })
        })
      })

      it('should update a relationship', function (done) {
        var startTemplate = {
          relatedNodeLabel: 'bar',
          type: 'RELATED_TO',
          data: {
            str: 'to-be-updated',
            num: 5
          }
        }
        var endTemplate = {
          relatedNodeLabel: 'bar',
          type: 'RELATED_TO',
          data: {
            str: 'updated'
          }
        }
        var foo = si.make('foo')
        foo.id = 'source'

        foo.save$(function (err, foo1) {
          Assert.isNull(err)
          foo1.saveRelationship$({ relationship$: startTemplate }, function (err, rel1) {
            Assert.isNull(err)
            Assert.equal(rel1.str, 'to-be-updated')
            Assert.equal(rel1.num, 5)

            foo1.updateRelationship$({ relationship$: endTemplate }, verify(done, function (rel2) {
              Assert.equal(rel2.str, 'updated')
              Assert.equal(rel1.num, 5)
            }))
          })
        })
      })

      it('should not save modifications to relationship after save completes', function (done) {
        var foo = si.make('foo')
        foo.id = 'source'
        var rel = si.make('rel', {
          relatedNodeLabel: 'bar',
          type: 'RELATED_TO',
          data: {
            str: 'aaa'
          }
        })

        foo.save$(function (err, foo1) {
          Assert.isNull(err)
          foo1.saveRelationship$({ relationship$: rel.data$(true) }, verify(done, function (rel1) {
            // now that rel is in the database, modify the original data
            rel.data.str = 'bbb'
            Assert.equal(rel1.str, 'aaa')
          }))
        })
      })

      it('should not backport modification to saved entity to the original one', function (done) {
        var foo = si.make('foo')
        foo.id = 'source'
        var rel = si.make('rel', {
          relatedNodeLabel: 'bar',
          type: 'RELATED_TO',
          data: {
            str: 'aaa'
          }
        })

        foo.save$(function (err, foo1) {
          Assert.isNull(err)
          foo1.saveRelationship$({ relationship$: rel.data$(true) }, verify(done, function (rel1) {
            // now that rel is in the database, modify the created data
            rel1.str = 'bbb'
            Assert.equal(rel.data.str, 'aaa')
          }))
        })
      })

      it('should clear an attribute if = null', function (done) {
        var foo = si.make('foo')
        foo.id = 'source'
        var rel = {
          relatedNodeLabel: 'bar',
          type: 'RELATED_TO',
          data: {
            p1: 'a',
            p2: 'b',
            p3: 'c'
          }
        }

        foo.save$(function (err, foo1) {
          Assert.isNull(err)
          foo1.saveRelationship$({ relationship$: rel }, function (err, rel1) {
            Assert.isNull(err)
            Assert.equal(rel1.p1, rel.data.p1)
            Assert.equal(rel1.p2, rel.data.p2)
            Assert.equal(rel1.p3, rel.data.p3)
            rel.data.p1 = null
            rel.data.p2 = undefined
            foo1.updateRelationship$({ relationship$: rel }, verify(done, function (rel2) {
              Assert.notOk(rel2.p1)
              Assert.notOk(rel2.p2)
              Assert.equal(rel2.p3, rel.data.p3)
            }))
          })
        })
      })
    })

    describe('List', function () {
      before(clearDb(si))
      before(clearDb(si))
      before(createEntities(si, 'foo', [{
        id$: 'source',
        p1: 'v1',
        p2: 'v2'
      }]))
      before(createEntities(si, 'bar', [{
        id$: 'sink1',
        p1: 'v2'
      },
      {
        id$: 'sink2',
        p1: 'w2',
        p2: 5
      }]))
      before(createRelationships(si, 'foo', { id: 'source' }, [{ relationship$: reltemplate }]))


      it('should load all related elements if no params', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.list$({ relationship$: reltemplate }, verify(done, function (res) {
          Assert.lengthOf(res, 2)
        }))
      })

      it('should list entities by related entity id', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.list$({ relationship$: reltemplate, id: 'sink1' }, verify(done, function (res) {
          Assert.lengthOf(res, 1)
          Assert.equal(res[0].id, 'sink1')
          Assert.equal(res[0].p1, 'v2')
          Assert.notOk(res[0].p2)
        }))
      })

      it('should list entities by related entity integer property', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.list$({ relationship$: reltemplate, p2: 5 }, verify(done, function (res) {
          Assert.lengthOf(res, 1)
          Assert.equal(res[0].id, 'sink2')
          Assert.equal(res[0].p1, 'w2')
          Assert.equal(res[0].p2, 5)
        }))
      })

      it('should list entities by related entity string property', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.list$({ relationship$: reltemplate, p1: 'w2' }, verify(done, function (res) {
          Assert.lengthOf(res, 1)
          Assert.equal(res[0].id, 'sink2')
          Assert.equal(res[0].p1, 'w2')
          Assert.equal(res[0].p2, 5)
        }))
      })

      it('should list entities by two related entity properties', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.list$({ relationship$: reltemplate, p1: 'w2', p2: 5 }, verify(done, function (res) {
          Assert.lengthOf(res, 1)
          Assert.equal(res[0].id, 'sink2')
          Assert.equal(res[0].p1, 'w2')
          Assert.equal(res[0].p2, 5)
        }))
      })

      it('should list entities by relationship type', function (done) {
        var foo = si.make('foo', { id: 'source' })
        var bam = si.make('bam', { id$: 'bam', p1: 'a' })
        var bamTemplate = {
          relatedNodeLabel: 'bam',
          type: 'RELATIONSHIP_WITH',
          data: {
            str: 'str',
            num: 5
          }
        }
        bam.save$(function (err, bam) {
          Assert.isNull(err)
          foo.saveRelationship$({ relationship$: bamTemplate }, function (err, bam) {
            Assert.isNull(err)
            foo.list$({ relationship$: { type: 'RELATIONSHIP_WITH' } }, verify(done, function (res) {
              Assert.lengthOf(res, 1)
              Assert.equal(res[0].id, 'bam')
              Assert.equal(res[0].p1, 'a')
            }))
          })
        })
      })

      it('should list entities by relationship type and integer property', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.updateRelationship$({ relationship$: { relatedNodeLabel: 'bar', type: 'RELATED_TO', data: { int: 12 } }, p1: 'v2' }, function (err, rel) {
          Assert.isNull(err)
          foo.list$({ relationship$: { type: 'RELATED_TO', data: { int: 12 } } }, verify(done, function (res) {
            Assert.lengthOf(res, 1)
            Assert.equal(res[0].id, 'sink1')
            Assert.equal(res[0].p1, 'v2')
          }))
        })
      })

      it('should list entities by relationship type and string property', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.updateRelationship$({ relationship$: { relatedNodeLabel: 'bar', type: 'RELATED_TO', data: { str: 'ccc' } }, p1: 'v2' }, function (err, rel) {
          Assert.isNull(err)
          foo.list$({ relationship$: { type: 'RELATED_TO', data: { str: 'ccc' } } }, verify(done, function (res) {
            Assert.lengthOf(res, 1)
            Assert.equal(res[0].id, 'sink1')
            Assert.equal(res[0].p1, 'v2')
          }))
        })
      })

      it('should list entities by relationship type and two properties', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.updateRelationship$({ relationship$: { relatedNodeLabel: 'bar', type: 'RELATED_TO', data: { int: 12, str: 'ccc' } }, p1: 'v2' }, function (err, rel) {
          Assert.isNull(err)
          foo.list$({ relationship$: { type: 'RELATED_TO', data: { int: 12, str: 'ccc' } } }, verify(done, function (res) {
            Assert.lengthOf(res, 1)
            Assert.equal(res[0].id, 'sink1')
            Assert.equal(res[0].p1, 'v2')
          }))
        })
      })

      it('should return a count of entities by label', function(done) {
        var bar = si.make('bar')
        bar.list$({ count$: true }, verify(done, function(res) {
          Assert.equal(res, 2)
        }))
      })
    })

    describe('Remove', function () {
      beforeEach(clearDb(si))
      beforeEach(createEntities(si, 'foo', [{
        id$: 'source',
        p1: 'v1',
        p2: 'v2'
      }]))
      beforeEach(createEntities(si, 'bar', [{
        id$: 'sink1',
        p1: 'v2'
      },
      {
        id$: 'sink2',
        p1: 'w2',
        p2: 5
      }]))
      beforeEach(createRelationships(si, 'foo', { id: 'source' }, [{ relationship$: reltemplate }]))

      it('should delete only one relationship', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.remove$({ relationship$: reltemplate }, function (err, res) {
          Assert.isNull(err)
          Assert.notOk(res)

          foo.list$({ relationship$: reltemplate }, verify(done, function (res) {
            Assert.lengthOf(res, 1)
          }))
        })
      })

      it('should delete all relationships if all$ = true', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.remove$({ relationship$: reltemplate, all$: true }, function (err, res) {
          Assert.isNull(err)
          Assert.notOk(res)

          foo.list$({ relationship$: reltemplate }, verify(done, function (res) {
            Assert.lengthOf(res, 0)
          }))
        })
      })

      it('should delete relationships by type', function (done) {
        var foo = si.make('foo', { id: 'source' })
        var bam = si.make('bam', { id$: 'bam', p1: 'a' })
        var bamTemplate = {
          relatedNodeLabel: 'bam',
          type: 'RELATIONSHIP_WITH',
          data: {
            str: 'str',
            num: 5
          }
        }
        bam.save$(function (err, bam) {
          Assert.isNull(err)
          foo.saveRelationship$({ relationship$: bamTemplate }, function (err, rel) {
            Assert.isNull(err)
            Assert.equal(rel.str, 'str')
            Assert.equal(rel.num, 5)
            foo.list$({ relationship$: { type: 'RELATIONSHIP_WITH' } }, function (err, res0) {
              Assert.isNull(err)
              Assert.lengthOf(res0, 1)
              Assert.equal(res0[0].id, 'bam')
              Assert.equal(res0[0].p1, 'a')
              foo.remove$({ relationship$: bamTemplate }, function (err, res1) {
                Assert.isNull(err)
                Assert.notOk(res1)
                foo.list$({ relationship$: { type: 'RELATIONSHIP_WITH' } }, verify(done, function (res2) {
                  Assert.lengthOf(res2, 0)
                }))
              })
            })
          })
        })
      })

      it('should delete a relationship by type and property', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.updateRelationship$({ relationship$: { relatedNodeLabel: 'bar', type: 'RELATED_TO', data: { int: 12 } }, p1: 'v2' }, function (err, rel) {
          Assert.isNull(err)
          foo.list$({ relationship$: { type: 'RELATED_TO', data: { int: 12 } } }, function (err, res) {
            Assert.isNull(err)
            Assert.lengthOf(res, 1)
            Assert.equal(res[0].id, 'sink1')
            Assert.equal(res[0].p1, 'v2')
            foo.remove$({ relationship$: { type: 'RELATED_TO', data: { int: 12 } } }, function (err, res1) {
              Assert.isNull(err)
              Assert.notOk(res1)
              foo.list$({ relationship$: { type: 'RELATED_TO' } }, verify(done, function (res2) {
                Assert.lengthOf(res2, 1)
              }))
            })
          })
        })
      })

      it('should delete relationships filtered by type and AND', function (done) {
        var foo = si.make('foo', { id: 'source' })
        foo.updateRelationship$({ relationship$: { relatedNodeLabel: 'bar', type: 'RELATED_TO', data: { int: 12 } }, p1: 'v2' }, function (err, rel) {
          Assert.isNull(err)
          foo.list$({ relationship$: { type: 'RELATED_TO', data: { int: 12 } } }, function (err, res) {
            Assert.isNull(err)
            Assert.lengthOf(res, 1)
            Assert.equal(res[0].id, 'sink1')
            Assert.equal(res[0].p1, 'v2')
            foo.remove$({ relationship$: { type: 'RELATED_TO', data: { str: 'aaa', int: 12 } } }, function (err, res1) {
              Assert.isNull(err)
              Assert.notOk(res1)
              foo.list$({ relationship$: { type: 'RELATED_TO' } }, verify(done, function (res2) {
                Assert.lengthOf(res2, 1)
              }))
            })
          })
        })
      })
    })
  })

  return script
}

function sorttest (settings) {
  var si = settings.seneca
  var script = settings.script || Lab.script()

  var describe = script.describe
  var it = script.it
  var before = script.before

  describe('Relationship Sorting', function () {
    before(clearDb(si))
    before(createEntities(si, 'foo', [
      { p1: 'v1', p2: 'v1' }
    ]))
    // make sure this is not in alphabetical order,
    // otherwise insertion order will be similar to the order we use for tests
    // possibly leading to false positives.
    // Also, create separately to ensure they are created in the correct order, otherwise
    // unit tests will fail.
    before(createEntities(si, 'bar', [
      { p1: 'v1', p2: 'v1' }
    ]))
    before(createEntities(si, 'bar', [
      { p1: 'v2', p2: 'v3' }
    ]))
    before(createEntities(si, 'bar', [
      { p1: 'v3', p2: 'v2' }
    ]))
    before(createRelationships(si, 'foo',
      { p1: 'v1', p2: 'v1' },
      [
        {
          relationship$: {
            relatedNodeLabel: 'bar',
            type: 'RELATIONSHIP_SORT',
            data: { r1: 'v1', r2: 'v1' }
          },
          p1: 'v1',
          p2: 'v1'
        },
        {
          relationship$: {
            relatedNodeLabel: 'bar',
            type: 'RELATIONSHIP_SORT',
            data: { r1: 'v2', r2: 'v3' }
          },
          p1: 'v2',
          p2: 'v3'
        },
        {
          relationship$: {
            relatedNodeLabel: 'bar',
            type: 'RELATIONSHIP_SORT',
            data: { r1: 'v3', r2: 'v2' }
          },
          p1: 'v3',
          p2: 'v2'
        },
        {
          relationship$: {
            relatedNodeLabel: 'bar',
            type: 'RELATED_SORT',
            data: { r1: 'v4', r2: 'v7' }
          },
          p1: 'v1',
          p2: 'v1'
        },
        {
          relationship$: {
            relatedNodeLabel: 'bar',
            type: 'RELATED_SORT',
            data: { r1: 'v4', r2: 'v7' }
          },
          p1: 'v2',
          p2: 'v3'
        },
        {
          relationship$: {
            relatedNodeLabel: 'bar',
            type: 'RELATED_SORT',
            data: { r1: 'v4', r2: 'v7' }
          },
          p1: 'v3',
          p2: 'v2'
        }
      ])
    )

    describe('Load', function () {
      it('should support ascending order for relationships', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } } }, verify(done, function (foo) {
          Assert.ok(foo)
          Assert.equal(foo.p1, 'v1')
        }))
      })

      it('should support descending order for relationships', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: -1 } } } }, verify(done, function (foo) {
          Assert.ok(foo)
          Assert.equal(foo.p1, 'v3')
        }))
      })

      it('should support ascending order for related nodes', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATED_SORT' }, sort$: { p1: 1 } }, verify(done, function (foo) {
          Assert.ok(foo)
          Assert.equal(foo.p1, 'v1')
        }))
      })

      it('should support descending order for related nodes', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATED_SORT' }, sort$: { p1: -1 } }, verify(done, function (foo) {
          Assert.ok(foo)
          Assert.equal(foo.p1, 'v3')
        }))
      })

      it('should support ascending order for relationships and related nodes', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } }, sort$: { p1: 1 } }, verify(done, function (foo) {
          Assert.ok(foo)
          Assert.equal(foo.p1, 'v1')
        }))
      })

      it('should support descending order for relationships and related nodes', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: -1 } } }, sort$: { p1: -1 } }, verify(done, function (foo) {
          Assert.ok(foo)
          Assert.equal(foo.p1, 'v3')
        }))
      })

      it('should support ascending order for relationships and descending order for related nodes', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } }, sort$: { p1: -1 } }, verify(done, function (foo) {
          Assert.ok(foo)
          Assert.equal(foo.p1, 'v1')
        }))
      })

      it('should support ascending order for relationships and descending order for related nodes 2', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATED_SORT', data: { sort$: { r1: 1 } } }, sort$: { p1: -1 } }, verify(done, function (foo) {
          Assert.ok(foo)
          Assert.equal(foo.p1, 'v3')
        }))
      })

      it('should support descending order for relationships and ascending order for related nodes', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: -1 } } }, sort$: { p1: 1 } }, verify(done, function (foo) {
          Assert.ok(foo)
          Assert.equal(foo.p1, 'v3')
        }))
      })

      it('should support decending order for relationships and ascending order for related nodes 2', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATED_SORT', data: { sort$: { r1: -1 } } }, sort$: { p1: 1 } }, verify(done, function (foo) {
          Assert.ok(foo)
          Assert.equal(foo.p1, 'v1')
        }))
      })
    })

    describe('List', function () {
      it('should support ascending order for relationships', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v1')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v3')
        }))
      })

      it('should support ascending order for related nodes', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATED_SORT' }, sort$: { p1: 1 } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v1')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v3')
        }))
      })

      it('should support dscending order for relationships', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: -1 } } } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v3')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v1')
        }))
      })

      it('should support descending order for related nodes', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATED_SORT' }, sort$: { p1: -1 } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v3')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v1')
        }))
      })

      it('should support ascending order for relationships and related nodes', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } }, sort$: { p1: 1 } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v1')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v3')
        }))
      })

      it('should support ascending order for relationships and related nodes 2', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATED_SORT', data: { sort$: { r1: 1 } } }, sort$: { p1: 1 } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v1')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v3')
        }))
      })

      it('should support descending order for relationships and related nodes', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: -1 } } }, sort$: { p1: -1 } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v3')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v1')
        }))
      })

      it('should support descending order for relationships and related nodes 2', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATED_SORT', data: { sort$: { r1: -1 } } }, sort$: { p1: -1 } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v3')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v1')
        }))
      })

      it('should support ascending order for relationships and descending order for related nodes', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } }, sort$: { p1: -1 } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v1')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v3')
        }))
      })

      it('should support ascending order for relationships and descending order for related nodes 2', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATED_SORT', data: { sort$: { r1: 1 } } }, sort$: { p1: -1 } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v3')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v1')
        }))
      })

      it('should support descending order for relationships and ascending order for related nodes', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: -1 } } }, sort$: { p1: 1 } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v3')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v1')
        }))
      })

      it('should support descending order for relationships and ascending order for related nodes 2', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATED_SORT', data: { sort$: { r1: -1 } } }, sort$: { p1: 1 } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v1')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v3')
        }))
      })
    })

    describe('Remove', function () {
      it('should support ascending order for relationships', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } } }, function (err) {
          if (err) {
            return done(err)
          }

          cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } } }, verify(done, function (lst) {
            Assert.equal(lst.length, 2)
            Assert.equal(lst[0].p1, 'v2')
            Assert.equal(lst[1].p1, 'v3')
          }))
        })
      })

      it('should support descending order for relationships', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: -1 } } } }, function (err) {
          if (err) {
            return done(err)
          }

          cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: -1 } } } }, verify(done, function (lst) {
            Assert.equal(lst.length, 1)
            Assert.equal(lst[0].p1, 'v2')
          }))
        })
      })
      // it should also support ascending and descending for related nodes and for combinations of relationships and related nodes but I'm
      // not writing explicit tests for those as they are analagous to the previous set of tests which all pass.
    })
  })

  return script
}

function limitstest (settings) {
  var si = settings.seneca
  var script = settings.script || Lab.script()

  var describe = script.describe
  var it = script.it
  var before = script.before
  var beforeEach = script.beforeEach

  describe('Relationship Limits', function () {
    before(clearDb(si))
    before(createEntities(si, 'foo', [
      { p1: 'v1', p2: 'v1' }
    ]))
    // make sure this is not in alphabetical order,
    // otherwise insertion order will be similar to the order we use for tests
    // possibly leading to false positives.
    // Also, create separately to ensure they are created in the correct order, otherwise
    // unit tests will fail.
    before(createEntities(si, 'bar', [
      { p1: 'v1', p2: 'v1' }
    ]))
    before(createEntities(si, 'bar', [
      { p1: 'v2', p2: 'v3' }
    ]))
    before(createEntities(si, 'bar', [
      { p1: 'v3', p2: 'v2' }
    ]))
    before(createRelationships(si, 'foo',
      { p1: 'v1', p2: 'v1' },
      [
        {
          relationship$: {
            relatedNodeLabel: 'bar',
            type: 'RELATIONSHIP_SORT',
            data: { r1: 'v1', r2: 'v1' }
          },
          p1: 'v1',
          p2: 'v1'
        },
        {
          relationship$: {
            relatedNodeLabel: 'bar',
            type: 'RELATIONSHIP_SORT',
            data: { r1: 'v2', r2: 'v3' }
          },
          p1: 'v2',
          p2: 'v3'
        },
        {
          relationship$: {
            relatedNodeLabel: 'bar',
            type: 'RELATIONSHIP_SORT',
            data: { r1: 'v3', r2: 'v2' }
          },
          p1: 'v3',
          p2: 'v2'
        },
        {
          relationship$: {
            relatedNodeLabel: 'bar',
            type: 'RELATED_SORT',
            data: { r1: 'v4', r2: 'v7' }
          },
          p1: 'v1',
          p2: 'v1'
        },
        {
          relationship$: {
            relatedNodeLabel: 'bar',
            type: 'RELATED_SORT',
            data: { r1: 'v4', r2: 'v7' }
          },
          p1: 'v2',
          p2: 'v3'
        },
        {
          relationship$: {
            relatedNodeLabel: 'bar',
            type: 'RELATED_SORT',
            data: { r1: 'v4', r2: 'v7' }
          },
          p1: 'v3',
          p2: 'v2'
        }
      ])
    )

    // whilst sort$ can be applied to both relationships and related entities, skip$ and limit$ cannot (as this can't be expressed in cypher - skip$ and limit$
    // only accept integers as parameters, not qualified properties like sort$).  As a result, skip$ and limit$ applied to relationships will override skip$ and
    // limit$ applied to related entities.
    describe('Load', function () {
      it('should support skip and sort on relationships', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { skip$: 2, sort$: { r1: 1 } } } }, verify(done, function (bar) {
          Assert.ok(bar)
          Assert.equal(bar.p1, 'v3')
        }))
      })

      it('should support skip and sort on related nodes', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATED_SORT' }, skip$: 2, sort$: { p1: -1 } }, verify(done, function (bar) {
          Assert.ok(bar)
          Assert.equal(bar.p1, 'v1')
        }))
      })

      it('skip on relationships should override skip on related nodes', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { skip$: 2, sort$: { r1: 1 } } }, skip$: 2, sort$: { p1: -1 } }, verify(done, function (bar) {
          Assert.ok(bar)
          Assert.equal(bar.p1, 'v3')
        }))
      })

      it('should return empty array when skipping all the relationships', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { skip$: 3 } } }, verify(done, function (bar) {
          Assert.notOk(bar)
        }))
      })

      it('should return empty array when skipping all the related nodes', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATED_SORT' }, skip$: 3 }, verify(done, function (bar) {
          Assert.notOk(bar)
        }))
      })

      it('should not be influenced by limit on relationships', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { limit$: 2, sort$: { r1: 1 } } } }, verify(done, function (bar) {
          Assert.ok(bar)
          Assert.equal(bar.p1, 'v1')
        }))
      })

      it('should not be influenced by limit on related nodes', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATED_SORT', data: { sort$: { r1: 1 } } }, limit$: 2 }, verify(done, function (bar) {
          Assert.ok(bar)
          Assert.equal(bar.p1, 'v3')
        }))
      })

      it('should ignore skip < 0 on relationships', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { skip$: -1, sort$: { r1: 1 } } } }, verify(done, function (bar) {
          Assert.ok(bar)
          Assert.equal(bar.p1, 'v1')
        }))
      })

      it('should ignore skip < 0 on related nodes', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATED_SORT', data: { sort$: { r1: 1 } } }, skip$: -1 }, verify(done, function (bar) {
          Assert.ok(bar)
          Assert.equal(bar.p1, 'v3')
        }))
      })

      it('should ignore limit < 0 on relationships', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { limit$: -1, sort$: { r1: 1 } } } }, verify(done, function (bar) {
          Assert.ok(bar)
          Assert.equal(bar.p1, 'v1')
        }))
      })

      it('should ignore limit < 0 on related nodes', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATED_SORT', data: { sort$: { r1: 1 } } }, limit$: -1 }, verify(done, function (bar) {
          Assert.ok(bar)
          Assert.equal(bar.p1, 'v3')
        }))
      })

      it('should ignore invalid qualifier values on relationships', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { limit$: 'A', skip$: 'B', sort$: { r1: 1 } } } }, verify(done, function (bar) {
          Assert.ok(bar)
          Assert.equal(bar.p1, 'v1')
        }))
      })

      it('should ignore invalid qualifier values on related nodes', function (done) {
        var cl = si.make('foo')
        cl.load$({ relationship$: { type: 'RELATED_SORT', data: { sort$: { r1: 1 } } }, limit$: 'A', skip$: 'B' }, verify(done, function (bar) {
          Assert.ok(bar)
          Assert.equal(bar.p1, 'v3')
        }))
      })
    })

    describe('List', function () {
      it('should support limit, skip and sort on relationships', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { limit$: 1, skip$: 1, sort$: { r1: 1 } } } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 1)
          Assert.equal(lst[0].p1, 'v2')
        }))
      })

      it('should support limit, skip and sort on related nodes', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATED_SORT' }, limit$: 1, skip$: 1, sort$: { p1: 1 } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 1)
          Assert.equal(lst[0].p1, 'v2')
        }))
      })

      it('should return empty array when skipping all the relationships', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { limit$: 2, skip$: 3 } } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 0)
        }))
      })

      it('should return empty array when skipping all the related nodes', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATED_SORT' }, limit$: 2, skip$: 3 }, verify(done, function (lst) {
          Assert.lengthOf(lst, 0)
        }))
      })

      it('should return correct number of records if limit on relationships is too high', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { limit$: 5, skip$: 2, sort$: { r1: 1 } } } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 1)
          Assert.equal(lst[0].p1, 'v3')
        }))
      })

      it('should return correct number of records if limit on related nodes is too high', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATED_SORT' }, limit$: 5, skip$: 2, sort$: { p1: 1 } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 1)
          Assert.equal(lst[0].p1, 'v3')
        }))
      })

      it('should ignore skip < 0 on relationships', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { skip$: -1, sort$: { r1: 1 } } } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v1')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v3')
        }))
      })

      it('should ignore skip < 0 on related nodes', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATED_SORT' }, skip$: -1, sort$: { p1: 1 } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v1')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v3')
        }))
      })

      it('should ignore limit < 0 on relationships', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { limit$: -1, sort$: { r1: 1 } } } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v1')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v3')
        }))
      })

      it('should ignore limit < 0 on related nodes', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATED_SORT' }, limit$: -1, sort$: { p1: 1 } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v1')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v3')
        }))
      })

      it('should ignore invalid qualifier values on relationships', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { limit$: 'A', skip$: 'B', sort$: { r1: 1 } } } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v1')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v3')
        }))
      })

      it('should ignore invalid qualifier values on related nodes', function (done) {
        var cl = si.make('foo')
        cl.list$({ relationship$: { type: 'RELATED_SORT' }, limit$: 'A', skip$: 'B', sort$: { p1: 1 } }, verify(done, function (lst) {
          Assert.lengthOf(lst, 3)
          Assert.equal(lst[0].p1, 'v1')
          Assert.equal(lst[1].p1, 'v2')
          Assert.equal(lst[2].p1, 'v3')
        }))
      })
    })

    describe('Remove', function () {
      beforeEach(clearDb(si))
      beforeEach(createEntities(si, 'foo', [
        { p1: 'v1', p2: 'v1' }
      ]))
      // make sure this is not in alphabetical order,
      // otherwise insertion order will be similar to the order we use for tests
      // possibly leading to false positives.
      // Also, create separately to ensure they are created in the correct order, otherwise
      // unit tests will fail.
      beforeEach(createEntities(si, 'bar', [
        { p1: 'v1', p2: 'v1' }
      ]))
      beforeEach(createEntities(si, 'bar', [
        { p1: 'v2', p2: 'v3' }
      ]))
      beforeEach(createEntities(si, 'bar', [
        { p1: 'v3', p2: 'v2' }
      ]))
      beforeEach(createRelationships(si, 'foo',
        { p1: 'v1', p2: 'v1' },
        [
          {
            relationship$: {
              relatedNodeLabel: 'bar',
              type: 'RELATIONSHIP_SORT',
              data: { r1: 'v1', r2: 'v1' }
            },
            p1: 'v1',
            p2: 'v1'
          },
          {
            relationship$: {
              relatedNodeLabel: 'bar',
              type: 'RELATIONSHIP_SORT',
              data: { r1: 'v2', r2: 'v3' }
            },
            p1: 'v2',
            p2: 'v3'
          },
          {
            relationship$: {
              relatedNodeLabel: 'bar',
              type: 'RELATIONSHIP_SORT',
              data: { r1: 'v3', r2: 'v2' }
            },
            p1: 'v3',
            p2: 'v2'
          },
          {
            relationship$: {
              relatedNodeLabel: 'bar',
              type: 'RELATED_SORT',
              data: { r1: 'v4', r2: 'v7' }
            },
            p1: 'v1',
            p2: 'v1'
          },
          {
            relationship$: {
              relatedNodeLabel: 'bar',
              type: 'RELATED_SORT',
              data: { r1: 'v4', r2: 'v7' }
            },
            p1: 'v2',
            p2: 'v3'
          },
          {
            relationship$: {
              relatedNodeLabel: 'bar',
              type: 'RELATED_SORT',
              data: { r1: 'v4', r2: 'v7' }
            },
            p1: 'v3',
            p2: 'v2'
          }
        ])
      )

      it('should support limit, skip and sort on relationships', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { limit$: 1, skip$: 1, sort$: { r1: 1 } } } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 2)
            Assert.equal(lst[0].p1, 'v1')
            Assert.equal(lst[1].p1, 'v3')
          }))
        })
      })

      it('should support limit, skip and sort on related nodes', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATED_SORT' }, limit$: 1, skip$: 1, sort$: { p1: 1 } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATED_SORT' }, sort$: { p1: 1 } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 2)
            Assert.equal(lst[0].p1, 'v1')
            Assert.equal(lst[1].p1, 'v3')
          }))
        })
      })

      it('should not be impacted by limit > 1 on relationships', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { limit$: 2, skip$: 1, sort$: { r1: 1 } } } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 2)
            Assert.equal(lst[0].p1, 'v1')
            Assert.equal(lst[1].p1, 'v3')
          }))
        })
      })

      it('should not be impacted by limit > 1 on related nodes', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATED_SORT' }, limit$: 2, skip$: 1, sort$: { p1: 1 } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATED_SORT' }, sort$: { p1: 1 } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 2)
            Assert.equal(lst[0].p1, 'v1')
            Assert.equal(lst[1].p1, 'v3')
          }))
        })
      })

      it('should work with all$: true on relationships', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { all$: true, limit$: 2, skip$: 1, sort$: { r1: 1 } } } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 1)
            Assert.equal(lst[0].p1, 'v1')
          }))
        })
      })

      it('should work with all$: true on related nodes', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATED_SORT' }, all$: true, limit$: 2, skip$: 1, sort$: { p1: 1 } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATED_SORT' }, sort$: { p1: 1 } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 1)
            Assert.equal(lst[0].p1, 'v1')
          }))
        })
      })

      it('should not delete anyithing when skipping all the relationships', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { all$: true, limit$: 2, skip$: 3, sort$: { r1: 1 } } } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 3)
          }))
        })
      })

      it('should not delete anyithing when skipping all the related nodes', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATED_SORT' }, all$: true, limit$: 2, skip$: 3, sort$: { p1: 1 } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATED_SORT' }, sort$: { p1: 1 } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 3)
          }))
        })
      })

      it('should delete correct number of relationships if limit is too high', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { all$: true, limit$: 5, skip$: 2, sort$: { r1: 1 } } } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 2)
            Assert.equal(lst[0].p1, 'v1')
            Assert.equal(lst[1].p1, 'v2')
          }))
        })
      })

      it('should delete correct number of related nodes if limit is too high', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATED_SORT' }, all$: true, limit$: 5, skip$: 2, sort$: { p1: 1 } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATED_SORT' }, sort$: { p1: 1 } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 2)
            Assert.equal(lst[0].p1, 'v1')
            Assert.equal(lst[1].p1, 'v2')
          }))
        })
      })

      it('should ignore skip < 0 on relationships', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { skip$: -1, sort$: { r1: 1 } } } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 2)
            Assert.equal(lst[0].p1, 'v2')
            Assert.equal(lst[1].p1, 'v3')
          }))
        })
      })

      it('should ignore skip < 0 on related nodes', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATED_SORT' }, skip$: -1, sort$: { p1: 1 } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATED_SORT' }, sort$: { p1: 1 } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 2)
            Assert.equal(lst[0].p1, 'v2')
            Assert.equal(lst[1].p1, 'v3')
          }))
        })
      })

      it('should ignore limit < 0 on relationships', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { all$: true, limit$: -1, sort$: { r1: 1 } } } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 0)
          }))
        })
      })

      it('should ignore limit < 0 on related nodes', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATED_SORT' }, all$: true, limit$: -1, sort$: { p1: 1 } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATED_SORT' }, sort$: { p1: 1 } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 0)
          }))
        })
      })

      it('should ignore invalid qualifier values on relationships', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { limit$: 'A', skip$: 'B', sort$: { r1: 1 } } } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATIONSHIP_SORT', data: { sort$: { r1: 1 } } } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 2)
            Assert.equal(lst[0].p1, 'v2')
            Assert.equal(lst[1].p1, 'v3')
          }))
        })
      })

      it('should ignore invalid qualifier values on related nodes', function (done) {
        var cl = si.make('foo')
        cl.remove$({ relationship$: { type: 'RELATED_SORT' }, limit$: 'A', skip$: 'B', sort$: { p1: 1 } }, function (err) {
          if (err) {
            return done(err)
          }
          cl.list$({ relationship$: { type: 'RELATED_SORT' }, sort$: { p1: 1 } }, verify(done, function (lst) {
            Assert.lengthOf(lst, 2)
            Assert.equal(lst[0].p1, 'v2')
            Assert.equal(lst[1].p1, 'v3')
          }))
        })
      })
    })
  })

  return script
}

function cyphertest (settings) {
  var si = settings.seneca
  var script = settings.script || Lab.script()

  var describe = script.describe
  var before = script.before
  var it = script.it

  var Product = si.make('product')

  describe('Relationship Cypher support', function () {
    before(clearDb(si))
    before(createEntities(si, 'product', [
      {
        name: 'apple',
        price: 100
      }
    ]))
    before(createEntities(si, 'product', [
      { name:
        'pear',
        price: 200
      }
    ]))

    it('should accept a string query', function (done) {
      Product.native$({ cypher: 'MATCH (n:product) RETURN n ORDER BY n.price' }, verify(done, function (list) {
        Assert.lengthOf(list, 2)

        Assert.equal(list[0].entity$, '-/-/product')
        Assert.equal(list[0].name, 'apple')
        Assert.equal(list[0].price, 100)

        Assert.equal(list[1].entity$, '-/-/product')
        Assert.equal(list[1].name, 'pear')
        Assert.equal(list[1].price, 200)
      }))
    })

    it('should accept and array with query and parameters', function (done) {
      Product.native$({ cypher: 'MATCH (n:product) WHERE n.price >= {lower} AND n.price <= {upper} RETURN n ORDER BY n.price', parameters: { lower: 0, upper: 1000 } }, verify(done, function (list) {
        Assert.lengthOf(list, 2)

        Assert.equal(list[0].entity$, '-/-/product')
        Assert.equal(list[0].name, 'apple')
        Assert.equal(list[0].price, 100)

        Assert.equal(list[1].entity$, '-/-/product')
        Assert.equal(list[1].name, 'pear')
        Assert.equal(list[1].price, 200)
      }))
    })
  })

  return script
}

module.exports = {
  basictest: basictest,
  sorttest: sorttest,
  limitstest: limitstest,
  cyphertest: cyphertest,
  verify: verify
}
