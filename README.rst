Gafkalo
=======


A rewrite of `kafkalo` in Go

It has minor differences (in its own configuration) but it is indended to be a safe replacement

It uses Sarama with a goal of producing a Go binary with no external dependencies. Sarama does not use the client properties of Confluent libraries.


Incompatible changes
--------------------

1. Configuration for kafka itself and howto connect to componets has changed. We use sarama and it does not accept confluent/kakfa style string properties.
2. `prefixed` field renamed to `isLiteral` and logic inverted to match how Go treats default bool fields. Topics without this field keep the same behavior (defaults to PREFIXED) but topics that had it disabled now need to have `isLiteral: true`


New features
------------

- Pure Go. Now dependencies, and no librdkafka!
- Supports inreasing partition count
