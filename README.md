![Seneca](http://senecajs.org/files/assets/seneca-logo.png)
> A [Seneca.js][] Web plugin

# seneca-neo4j-store

[![npm version][npm-badge]][npm-url]
[![Build Status][travis-badge]][travis-url]
[![Coverage Status][coveralls-badge]][coveralls-url]
[![Dependency Status][david-badge]][david-url]

[![js-standard-style][standard-badge]][standard-style]

## Description

A storage engine that uses [neo4j][] to persist data.

If you're using this module, and need help, you can post a [github issue][].

If you are new to Seneca in general, please take a look at [senecajs.org][]. They have everything from
tutorials to sample apps to help get you up and running quickly.

### Seneca compatibility
Supports Seneca versions **1.x** and **2.x**

## Install
To install, simply use npm. You will need to install [Seneca.js][]
separately.

```
npm install seneca
npm install seneca-neo4j-store
```

## Quick Example
```js
var seneca = require('seneca')()
seneca.use('neo4j-store', {
  'conn': {
    'url': 'http://localhost:7474/db/data/transaction/commit',
    'auth': {
      'user': 'neo4j',
      'pass': 'neo4j'
    },
    'headers': {
      'accept': 'application/json; charset=UTF-8',
      'content-type': 'application/json',
      'x-stream': true
    },
    'strictSSL': true
  },
  'map': { '-/-/-': [ 'save', 'load', 'list', 'remove', 'native', 'saveRelationship', 'updateRelationship' ] },
  "merge": false
})

seneca.ready(function () {
  var apple = seneca.make$('fruit')
  apple.name  = 'Pink Lady'
  apple.price = 0.99
  apple.save$(function (err, apple) {
    console.log("apple.id = " + apple.id)
  })
})
```

## Usage
You don't use this module directly. It provides an underlying data storage engine for the Seneca entity API:

```js
var entity = seneca.make$('label')
entity.someproperty = "something"
entity.anotherproperty = 100

entity.save$(function (err, entity) { ... })
entity.load$({id: ...}, function (err, entity) { ... })
entity.list$({property: ...}, function (err, entity) { ... })
entity.remove$({id: ...}, function (err, entity) { ... })
```
Creating nodes (otherwise known as vertices) in this store is exactly the same as creating an entity in other Seneca data stores; the entity name defines the label of the node being saved/updated and it's parameters become the parameters of that node.

I tried to accommodate relationships (otherwise known as edges) within this metaphor but in the end I have had to add two further store methods in order to avoid bending that metaphor so far that it breaks:

```js
entity.saveRelationship$({relationship$: {relatedModelName: ..., type: ..., data: {id: ...}}}, p1: ...}, function (err, relationship) { ... })
entity.updateRelationship$({relationship$: {relationship$: {relatedModelName: ..., type: ..., data: {id: ...}}}, p1: ...}, function (err, relationship) { ... })
```
The reason for creating these two new store methods is that I needed a way to represent both the source and destination nodes as well as the relationship itself in a single method call.  This is no problem for `load$`, `list$` and `remove$` as these methods all accept both an entity and a query, but the `save$` method (which also acts as update with the appropriate parameters) only accepts an entity and that would mean mixing my relationship query data with node entity data in the most horrible of ways! For the sake of consistency I decided not to do this and to create these two new store methods.

In the methods shown above, the entity is the source node in the relationship and must already have been created, as must the destination node(s).  The supplied entity name and parameters are used to identify an existing node as the source of the relationship.  The query contains both the parameters used to identify the destination node(s) as well as the relationship data (contained in the `relationship$` object).

Th `relationship$` object has three possible keys:
- `relatedNodeLabel`: A string defining/identifying the label of the destination node(s)
- `type`: A string defining/identifying the relationship type
- `data`: An object containing parameters used to define/identify the relationship parameters

In the case of `saveRelationship$`, the `relationship$` object is used to define the new relationship, whereas in `updateRelationship$` it is used to identify existing relationships.  You can supply as many or as few of these parameters as you like (but beware of the costs of being unspecific!).

### Detailed Example
```js
// CREATE (a:label_1 { p1: 'v1', p2: 50 })-[r:RELTYPE { q1: 'asdf' }]->(b:label_2 { p1: 'v2' }) return r

// create the source node from which the relationship originates
var entity1 = seneca.make$('label_1')
entity1.p1 = "v1"
entity1.p2 = 50
entity1.save$(function (err, entity) { ... })

// now create the destination node to which is related to the source node
var entity2 = seneca.make$('label_2')
entity2.p1 = "v2"
entity2.save$(function (err, entity) { ... })

// now relate them together
var rel1 = { relatedNodeLabel: 'label_2', type: 'RELTYPE', data: { q1: 'asdf' } }
entity1.saveRelationship$({ relationship$: rel1, p1: 'v2' }, function (err, relationship) { ... })
// the same method signature can be used to update the relationship
```
The `relationship$` object can be used in `load$`, `list$` and `remove$`.  In the case of `load$` and `list$` the related node(s) are returned (not the relationships themselves - these are only returned on save or update).  In the case of `remove$` the relationship only is removed, not the source or destination nodes.

### Query Support
The standard Seneca query format is supported for both relationships and nodes:

- `.list$({f1:v1, f2:v2, ...})` implies pseudo-query `f1==v1 AND f2==v2, ...`.

- `.list$({f1:v1, ...}, {sort$:{field1:1}})` means sort by f1, ascending.

- `.list$({f1:v1, ...}, {sort$:{field1:-1}})` means sort by f1, descending.

- `.list$({f1:v1, ...}, {limit$:10})` means only return 10 results.

- `.list$({f1:v1, ...}, {skip$:5})` means skip the first 5.

- `.list$({f1:v1,...}, {fields$:['fd1','f2']})` means only return the listed fields.

Note: you can use `sort$`, `limit$`, `skip$` and `fields$` together in both the `relationship$` object and the related node data.  However, when including the `relationship$` object `limit$` and `skip$` can only be applied either to the relationship or to the related node but not to both together.  This is because `sort$` and `fields$` support the use of identifiers to indicate which entities the associated properties should apply to whereas `limit$` and `skip$` only have integer values.  If used together in both `relationship$` and the related object the `relationship$` values will be applied in preference to the relationship values.

Note also that, unlike other data stores, this data store does use attributes from the entity to filter queries but only in the context of the `relationship$` object.

###Native Driver

As with all seneca stores, you can access the native driver.

`entity.native$(function(err, dbInst){ cypher: ..., params: ..., name$: ...})`

The native driver takes 3 parameters:

- `cypher` - The native cypher query statement (required)

- `params` - The parameters associated with the cypher query (optional)

- `name$` - The entity name to be associated with the returned results, as in `$-/-/<name>;...`.  If not supplied, the entity name will be 'entity', as in `$-/-/entity;...` (optional)

With the dbInst object you can perform any query using [Cypher](http://docs.neo4j.org/chunked/stable/cypher-query-lang.html).

```javascript
entity.native$(function(err, dbinst){
  dbInst.query({ cypher: 'MATCH (n) RETURN n LIMIT 25' }, function (err,results){
    if(!err){
      return results;
    }
  });
})
```

### To run tests with Docker
I prefer to develop my code in Docker as it means I can be certain which versions of software are being used.  It also means that I don't have to install anything other than docker on my computer.  To this end, I've included a docker-compose.yml file that creates containers for both Neo4j (using the official Docker image) and seneca-neo4j-store (using a highly insecure image I've created purely for the purposes of local testing - don't say I didn't warn you) and runs the unit tests within Docker. Start the neo4j container first to ensure it is running before you run tests.

Build the images:
```sh
docker-compose up neo4j
docker-compose up store
```

Start the containers and run the tests (start the neo4j container first to ensure it is running before you run tests):
```sh
docker-compose start neo4j
docker-compose start store
```

Stop the containers:
```sh
docker-compose stop
```

## License
Copyright (c) 2016, Paul Nebel.
Licensed under [MIT][].

[npm-badge]: https://img.shields.io/npm/v/seneca-neo4j-store.svg
[npm-url]: https://npmjs.com/package/seneca-neo4j-store
[travis-badge]: https://travis-ci.org/DogFishProductions/seneca-neo4j-store.svg
[travis-url]: https://travis-ci.org/DogFishProductions/seneca-neo4j-store
[coveralls-badge]: https://coveralls.io/repos/github/DogFishProductions/seneca-neo4j-store/badge.svg?branch=master
[coveralls-url]: https://coveralls.io/github/DogFishProductions/seneca-neo4j-store?branch=master
[david-badge]: https://david-dm.org/DogFishProductions/seneca-neo4j-store.svg
[david-url]: https://david-dm.org/DogFishProductions/seneca-neo4j-store
[standard-badge]: https://raw.githubusercontent.com/feross/standard/master/badge.png
[standard-style]: https://github.com/feross/standard

[Neo4j]: http://neo4j.com/
[MIT]: ./LICENSE.txt
[Senecajs org]: https://github.com/senecajs/
[Seneca.js]: https://www.npmjs.com/package/seneca
[senecajs.org]: http://senecajs.org/
[github issue]: https://github.com/DogFishProductions/seneca-neo4j-store/issues
