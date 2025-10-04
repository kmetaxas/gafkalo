=========
CLI Usage
=========

Command-line tools for producing, consuming, and inspecting Kafka.

Producer
--------

Produce messages to topics with optional schema serialization.

Basic usage
~~~~~~~~~~~

.. code-block:: bash

   gafkalo --config config.yaml produce TOPIC_NAME

Reads from stdin. One message per line.

With schema serialization:

.. code-block:: bash

   gafkalo --config config.yaml produce events.user.login \
     --serialize \
     --key-schema-file schemas/user-key.avsc \
     --value-schema-file schemas/login-event.avsc \
     --separator '|'

Input format: ``key|value``

Example:

.. code-block:: console

   {"user_id":"u123"}|{"user_id":"u123","timestamp":1234567890,"success":true}

Options
~~~~~~~

``--idempotent``
  Enable idempotent producer. Requires DeveloperWrite on Cluster resource.

``--acks``
  Acks to require. Default: ``-1`` (all).

``--separator``
  Character separating key from value. Default: none (whole input is value).

``--serialize``
  Serialize using Avro schema registry.

``--key-schema-file``
  Path to key Avro schema (.avsc).

``--value-schema-file``
  Path to value Avro schema (.avsc).

``--tombstone``
  Produce tombstone (null value). Input used as key.

``--whole-file``
  Produce entire file as single message.

Examples
~~~~~~~~

String messages:

.. code-block:: bash

   echo "hello world" | gafkalo --config config.yaml produce test-topic

With key:

.. code-block:: bash

   echo "key1:value1" | gafkalo --config config.yaml produce test-topic --separator ':'

Serialized Avro:

.. code-block:: bash

   echo '{"id":"123"}|{"name":"test","value":42}' | \
     gafkalo --config config.yaml produce events.test \
       --serialize \
       --key-schema-file key.avsc \
       --value-schema-file value.avsc \
       --separator '|'

Tombstone:

.. code-block:: bash

   echo "user123" | gafkalo --config config.yaml produce state.users --tombstone

Large file:

.. code-block:: bash

   gafkalo --config config.yaml produce test-topic --whole-file /path/to/data.json

Consumer
--------

Consume messages from topics with optional deserialization.

Basic usage
~~~~~~~~~~~

.. code-block:: bash

   gafkalo --config config.yaml consumer TOPIC_NAME

Consumes from beginning by default.

With deserialization:

.. code-block:: bash

   gafkalo --config config.yaml consumer events.user.login \
     --deserialize-key \
     --deserialize-value

Options
~~~~~~~

``--deserialize-key``
  Deserialize key using schema registry.

``--deserialize-value``
  Deserialize value using schema registry.

``--max-records``
  Stop after N records.

``--record-template``
  Path to Go template for custom formatting.

``--output-format``
  Output format: ``text``, ``json``.

Examples
~~~~~~~~

Read 10 records:

.. code-block:: bash

   gafkalo --config config.yaml consumer events.orders --max-records 10

Deserialize Avro:

.. code-block:: bash

   gafkalo --config config.yaml consumer events.orders \
     --deserialize-key \
     --deserialize-value

JSON output:

.. code-block:: bash

   gafkalo --config config.yaml consumer events.orders \
     --output-format json \
     --deserialize-value

Custom template
~~~~~~~~~~~~~~~

Create template file ``record.tpl``:

.. code-block:: go

   TOPIC: {{.Topic}} | OFFSET: {{.Offset}} | KEY: {{.Key}} | VALUE: {{.Value}}

Use it:

.. code-block:: bash

   gafkalo --config config.yaml consumer events.orders \
     --record-template record.tpl

Template context:

.. code-block:: go

   type CustomRecordTemplateContext struct {
       Topic       string
       Key         string
       Value       string
       Timestamp   time.Time
       Partition   int32
       Offset      int64
       KeySchemaID int
       ValSchemaID int
       Headers     map[string]string
   }

Headers
~~~~~~~

Headers displayed automatically:

.. code-block:: console

   Headers[[trace-id:abc123 content-type:application/json]] Topic[events.orders] Offset[42] ...

JSON format:

.. code-block:: json

   {
     "headers": {
       "trace-id": "abc123",
       "content-type": "application/json"
     },
     "topic": "events.orders",
     "offset": 42
   }

Replicator
----------

Copy topics within or across clusters.

Same cluster
~~~~~~~~~~~~

.. code-block:: bash

   gafkalo --config config.yaml replicator \
     --source-topic old-topic \
     --dest-topic new-topic

Cross-cluster
~~~~~~~~~~~~~

.. code-block:: bash

   gafkalo --config source-cluster.yaml replicator \
     --source-topic events.orders \
     --dest-topic events.orders \
     --dest-config dest-cluster.yaml

Behavior:

- Default consumer group: ``gafkalo-replicator``
- Idempotent mode enabled (requires permissions)
- Resumable (uses consumer group offsets)

Consumer groups
---------------

Manage consumer group offsets.

Reset offsets
~~~~~~~~~~~~~

.. code-block:: bash

   gafkalo --config config.yaml consumer events.orders \
     --group my-consumer-group \
     --reset-offsets \
     --partition 0:100 \
     --partition 1:200

Sets partition 0 to offset 100, partition 1 to offset 200.

Topic linting
-------------

See `topics` documentation for linting commands.

Schema utilities
----------------

See `schemas` documentation for schema CLI commands.

Connect management
------------------

See `connectors` documentation for Connect CLI commands.

Global options
--------------

``--config``
  Path to config YAML. Required for all commands.

``--verbose``
  Enable verbose logging.

``--version``
  Show version and exit.

Exit codes
----------

- ``0``: Success
- ``1``: Error

Useful for scripts:

.. code-block:: bash

   #!/bin/bash
   if ! gafkalo --config prod.yaml topic list; then
     echo "Failed to connect to Kafka"
     exit 1
   fi
