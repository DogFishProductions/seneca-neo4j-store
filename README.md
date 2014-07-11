[![Build Status](https://travis-ci.org/piccoloaiutante/seneca-neo4j-store.png?branch=master)](https://travis-ci.org/piccoloaiutante/seneca-neo4j-store)

# seneca-neo4j-store

[![NPM](https://nodei.co/npm/seneca-neo4j-store.png)](https://nodei.co/npm/seneca-neo4j-store/)

### Seneca node.js data-storage plugin for Neo4j.

This module is a plugin for the Seneca framework. It provides a
storage engine that uses Neo4j to persist data. This module is currently under development so consider it as alpha version.

The Seneca framework provides an 
[ActiveRecord-style data storage API](http://senecajs.org/data-entities.html). 
Each supported database has a plugin, such as this one, that
provides the underlying Seneca plugin actions required for data
persistence.


### Support

If you're using this module, feel free to contact me on twitter if you
have any questions! :) [@piccoloaiutante](http://twitter.com/piccoloaiutante)

Current Version: 0.1.3

Tested on: Node 0.10.28, Seneca 0.5.17

### Quick example

```JavaScript
var seneca = require('seneca')()
seneca.use('neo4j-store',{
    host:'localhost',
    port:7474
})

seneca.ready(function(){
  var apple = seneca.make$('fruit')
  apple.name  = 'Pink Lady'
  apple.price = 0.99
  apple.save$(function(err,apple){
    console.log( "apple.id = "+apple.id  )
  })
})
```


## Install

```sh
npm install seneca
npm install seneca-neo4j-store
```


## Usage

You don't use this module directly. It provides an underlying data storage engine for the Seneca entity API:

```JavaScript
var entity = seneca.make$('typename')
entity.someproperty = "something"
entity.anotherproperty = 100

entity.save$( function(err,entity){ ... } )
entity.load$( {id: ...}, function(err,entity){ ... } )
entity.list$( {property: ...}, function(err,entity){ ... } )
entity.remove$( {id: ...}, function(err,entity){ ... } )
```


##Queries

The standard Seneca query format is supported:

- `entity.list$({field1:value1, field2:value2, ...})` implies pseudo-query field1==value1 AND field2==value2, ...
- you can only do AND queries. 
- `entity.list$({f1:v1,...},{sort$:{field1:1}})` means sort by field1, ascending
- `entity.list$({f1:v1,...},{sort$:{field1:-1}})` means sort by field1, descending
- `entity.list$({f1:v1,...},{limit$:10})` means only return 10 results
- `entity.list$({f1:v1,...},{skip$:5})` means skip the first 5
- `entity.list$({f1:v1,...},{fields$:['field1','field2']})` means only return the listed fields (avoids pulling lots of data out of the database) NOT SUPPORTED
- you can use sort$, limit$ and skip$ together


###Native Driver

As with all seneca stores, you can access the native driver, in this case since i use [node-neo4j](https://github.com/thingdom/node-neo4j), you can directly access the GraphDatabase object provided by that driver.

`entity.native$(function(err, dbinst){...})`

With the GraphDatabase object you can perform any query using [Cypher](http://docs.neo4j.org/chunked/stable/cypher-query-lang.html). Fore more informatino about node-neo4j chekout [here](http://coffeedoc.info/github/thingdom/node-neo4j/master/)

```javascript
entity.native$(function(err, dbinst){
	dbinst.query('MATCH (n) RETURN n LIMIT 25', null, function (err,results){
	  if(!err){
	    return results;
	  }
	});
})
```


## Test

```bash
npm test
```
