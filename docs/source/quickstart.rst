==========
Quickstart
==========

YAML-based workflow
-------------------

Gafkalo manages Kafka resources through YAML definitions.

1. Define resources in YAML
2. Generate a plan
3. Apply changes

Basic workflow
--------------

Create a config file:

.. code-block:: yaml

   connections:
     kafka:
       bootstrapBrokers: ["localhost:9093"]

   kafkalo:
     input_dirs:
       - "data/*.yaml"

Define a topic in ``data/topics.yaml``:

.. code-block:: yaml

   topics:
     - name: events.user.login
       partitions: 6
       replication_factor: 3
       configs:
         cleanup.policy: delete
         min.insync.replicas: 2
         retention.ms: 604800000

Generate plan:

.. code-block:: bash

   gafkalo plan --config config.yaml

Apply changes:

.. code-block:: bash

   gafkalo apply --config config.yaml

Features
--------

- Declarative YAML-based management
- Plan and apply workflow (dry-run support)
- Topics, schemas, RBAC, connectors
- CLI producer/consumer with schema support
- Topic linter for best practices
- No external dependencies (pure Go)
- GitOps ready

What it manages
---------------

**Topics**
  Create, update partitions, manage configs

**Schemas**
  Register Avro schemas, set compatibility levels

**RBAC**
  Manage Confluent RBAC rolebindings (producer, consumer, resourceowner)

**Connectors**
  Create and manage Kafka Connect connectors

Limitations
-----------

- No topic/schema deletion (by design)
- No ACL support (RBAC only)
- Sarama-based (no librdkafka)
