[![Build Status](https://travis-ci.org/piccoloaiutante/seneca-neo4j-store.png?branch=master)](https://travis-ci.org/piccoloaiutante/seneca-neo4j-store)
# seneca-neo4j-store

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

Current Version: 0.0.1

Tested on: Node 0.10.28, Seneca 0.5.17

### Quick example

```JavaScript
var seneca = require('seneca')()
seneca.use('mongo-store',{
  name:'dbname',
  host:'127.0.0.1',
  port:27017
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


### Queries

I'm working to support the standard Seneca query format.

