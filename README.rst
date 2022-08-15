Gafkalo
=======

Gafkalo is to manage a Confluent platform by managing, in YAML:
- Topics
- Subjects (Schema registry schemas)
- RBAC (using Confluent RBAC plugin. No ACL support)
- Connectors.

This allows the implementation of a GitOps pipeline to manage a cluster.

It does not support deleting topics or schemas, primarily out of an abundance of caution, though this feature may be added in later versions if needed.

Additionally, it is meant as a CLI/debugging tool that needs no dependencies.

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


License
-------

GPL3. Full license text in LICENSE file
