Gafkalo
=======


A rewrite of `kafkalo` in Go

It is used to create topics , schemas in the Schema registry and assign permissions (using Confluent RBAC)
Additionally it is meant as a CLI/debugging tool that needs no dependencies.

It uses Sarama with a goal of producing a Go binary with no external dependencies. Sarama does not use the client properties of Confluent libraries.


Documentation
-------------

The documentation is available at https://gafkalo.readthedocs.io/

Features
--------

- Create and update topics
- Create and update Schema registry subjects and configs
- Manage client permissions using Confluent's RBAC module.
- Produce detailed plan of what it will do or a report of what it did.
- Can be used as a console Producer.
  - Can use Schema registry and Avro schemas for both key and value
- Can be used as a console Consumer
  - Can deserialize key and value from the schema registry (AVRO for now)
  - Can pretty print records
  - Allows user defined Golang template to do custom formatting of Kafka records
  - Can read from multiple topics
  - Can set multiple partition:offset explicitly to reset the consumer group to these offsets.
- Can check if a given schema is already registered in the schema registry under a given subject
- Connect CLI
  - Create connectors
  - Status check
  - Restart (heal) connectors and their failed tasks
- Pure Go. Now dependencies, and no librdkafka!
- Supports inreasing partition count


Incompatible changes
--------------------

1. Configuration for kafka itself and howto connect to componets has changed. We use sarama and it does not accept confluent/kakfa style string properties.
2. `prefixed` field renamed to `isLiteral` and logic inverted to match how Go treats default bool fields. Topics without this field keep the same behavior (defaults to PREFIXED) but topics that had it disabled now need to have `isLiteral: true`

License
-------

GPL3. Full license text in LICENSE file
