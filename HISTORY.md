<a name="1.0.10"></a>
## 1.0.10 (2016-08-01)


### Bug Fixes

* **#1:** Filters null properties and removes them from Cypher queries on load/list.([c0a6803](https://github.com/DogFishProductions/seneca-neo4j-store/commit/c0a6803)), closes [#1](https://github.com/DogFishProductions/seneca-neo4j-store/issues/1)
* **#1:** Removes qualifier from cypher statement when using count(*) in list.([ed9e487](https://github.com/DogFishProductions/seneca-neo4j-store/commit/ed9e487)), closes [#1](https://github.com/DogFishProductions/seneca-neo4j-store/issues/1)
* Fixes bug inadvertantly introduced by last fix.([64b2eae](https://github.com/DogFishProductions/seneca-neo4j-store/commit/64b2eae))
* **#2:** Retrieves nodes and relationships with apostrophes in their properties. This closes [#2](https://github.com/DogFishProductions/seneca-neo4j-store/issues/2).([46a528a](https://github.com/DogFishProductions/seneca-neo4j-store/commit/46a528a)), closes [#2](https://github.com/DogFishProductions/seneca-neo4j-store/issues/2)
* **#3:** All tests passing. Debug comments removes. This closes [#3](https://github.com/DogFishProductions/seneca-neo4j-store/issues/3).([18de3e7](https://github.com/DogFishProductions/seneca-neo4j-store/commit/18de3e7)), closes [#3](https://github.com/DogFishProductions/seneca-neo4j-store/issues/3)
* **#3:** Sets entity label correctly for all basic relationship tests. Other tests not yet checked, debug logging to be removed.([f2c4471](https://github.com/DogFishProductions/seneca-neo4j-store/commit/f2c4471))
* **#4:** Now passing all default store tests, including extended. Still contains debug statements.([7df8b35](https://github.com/DogFishProductions/seneca-neo4j-store/commit/7df8b35))
* **#4:** Successfully implements parameters for basic, sort and limit tests.([30307f4](https://github.com/DogFishProductions/seneca-neo4j-store/commit/30307f4))
* **#4:** Updates statement builder such that all values are passed as parameters. This closes [#4](https://github.com/DogFishProductions/seneca-neo4j-store/issues/4).([f230474](https://github.com/DogFishProductions/seneca-neo4j-store/commit/f230474)), closes [#4](https://github.com/DogFishProductions/seneca-neo4j-store/issues/4)
* **neo4j_microservice:** Passes all tests and extended tests except those requiring a native connection([ca19ce1](https://github.com/DogFishProductions/seneca-neo4j-store/commit/ca19ce1))


### Features

* **microservices:** Abstracts graph storage into microservice.  Passes cucumber tests.([9868480](https://github.com/DogFishProductions/seneca-neo4j-store/commit/9868480))
* **microservices:** Adding a microservices definition for Neo4j data server.([8d840ae](https://github.com/DogFishProductions/seneca-neo4j-store/commit/8d840ae))
* **native$:** Supports native cypher queries.([dcd5a09](https://github.com/DogFishProductions/seneca-neo4j-store/commit/dcd5a09))



## Changes since 2016-01-01T00:00:00.000


