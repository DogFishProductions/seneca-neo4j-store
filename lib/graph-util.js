/* jslint node: true */
'use strict'

var _ = require('lodash')

/** @function makeentp
 *
 *  @summary Constructs a seneca entity from parsed data.
 *
 *  Neo4j only allows for storage of primitive types or arrays (and it's fussy about arrays).  Some types,
 *  such as Dates, must therefore be stringified before being sent to the database.
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  ent - The entity from which to create a sanitised entity.
 *
 *  @returns  {Object}  The sanitised entity.
 */
module.exports.makeentp = function (ent) {
  var _entp = {}
  var _fields
  try {
    _fields = ent.data$(false)
  }
  catch (e) {
    _fields = ent
  }

  _.mapValues(_fields, function (value, key) {
    var _parsedValue = (_.isDate(value) ? value.toISOString() : value)
    if (_.isDate(value)) {
      _parsedValue = value.toISOString()
    }
    else if (_.isPlainObject(value)) {
      // Neo4j can only store primitive types or arrays. Objects must be converted to strings for storage.
      // Prefix stringified JSON with '~obj~' so that we can identify it on return and convert back to an Object.
      _parsedValue = '~obj~' + JSON.stringify(value)
    }
    else if (_.isArray(value)) {
      // Although Neo4j can store arrays it is very particular about what types of array can be stored.
      // The easiest thing, therefore, is to convert arrays to strings too.
      // Prefix stringified JSON with '~arr~' so that we can identify it on return and convert back to an Array
      _parsedValue = '~arr~' + JSON.stringify(value)
    }
    else {
      _parsedValue = value
    }
    _entp[key] = _parsedValue
  })

  return _entp
}

/** @function sanitiseQuery
 *
 *  @summary Removes metadata from query object.
 *
 *  The query object is used to pass metadata to query consructors. This metadata consists of words ending in '$'
 *  (e.g. all$, sort$ etc.).  This metadata must be removed from the query object before being used to construct
 *  the actual query, which is what this function does.
 *
 *  @since 1.0.0
 *
 *  @param    {Object}  query - The query to be sanitised.
 *
 *  @returns  {Object}  The sanitised query.
 */
module.exports.sanitiseQuery = function (query) {
  if (_.isArray(query) || _.isString(query)) {
    return query
  }
  var _sanitised = {}
  var _patt = /\$$/
  _.mapValues(query, function (value, key) {
    if (!_patt.test(key) || (key === 'and$') || (key === 'or$')) {
      _sanitised[key] = value
    }
  })
  return _sanitised
}
