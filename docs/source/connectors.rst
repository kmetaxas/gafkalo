======================
Maintaining Connectors
======================

Manage Kafka Connect connectors via CLI and YAML.

Configuration
-------------

In ``config.yaml``:

.. code-block:: yaml

   connections:
     connect:
       url: "https://connect:8083"
       username: "connect-user"
       password: "connect-pass"
       caPath: "/path/to/ca.crt"
       skipVerify: false

CLI commands
------------

Create connector
~~~~~~~~~~~~~~~~

.. code-block:: bash

   gafkalo --config config.yaml connect create connector-definition.json

Connector definition (JSON):

.. code-block:: json

   {
     "name": "postgres-source",
     "config": {
       "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
       "tasks.max": "1",
       "connection.url": "jdbc:postgresql://db:5432/mydb",
       "connection.user": "user",
       "connection.password": "pass",
       "table.whitelist": "users,orders",
       "mode": "incrementing",
       "incrementing.column.name": "id",
       "topic.prefix": "postgres-"
     }
   }

List connectors
~~~~~~~~~~~~~~~

.. code-block:: bash

   gafkalo --config config.yaml connect list

Output:

.. code-block:: console

   +---+------------------+
   | # | CONNECTOR NAME   |
   +---+------------------+
   | 0 | postgres-source  |
   | 1 | s3-sink          |
   +---+------------------+

Describe connector
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   gafkalo --config config.yaml connect describe postgres-source

Output:

- Connector configuration (all key-value pairs)
- Task status (ID, status, worker, running state)

Delete connector
~~~~~~~~~~~~~~~~

.. code-block:: bash

   gafkalo --config config.yaml connect delete postgres-source

Heal connector
~~~~~~~~~~~~~~

Restart failed connector and tasks:

.. code-block:: bash

   gafkalo --config config.yaml connect heal postgres-source

Checks status and restarts any task not in ``RUNNING`` state.

Health check
~~~~~~~~~~~~

Check all connectors:

.. code-block:: bash

   gafkalo --config config.yaml connect health-check

Reports any connectors or tasks not in healthy state.

YAML definition
---------------

Define connectors in YAML (applied via ``gafkalo apply``):

.. code-block:: yaml

   connectors:
     - name: postgres-source
       config:
         connector.class: io.confluent.connect.jdbc.JdbcSourceConnector
         tasks.max: 1
         connection.url: jdbc:postgresql://db:5432/mydb
         connection.user: user
         connection.password: pass
         table.whitelist: users,orders
         mode: incrementing
         incrementing.column.name: id
         topic.prefix: postgres-

     - name: s3-sink
       config:
         connector.class: io.confluent.connect.s3.S3SinkConnector
         tasks.max: 3
         topics: events.orders,events.payments
         s3.bucket.name: kafka-events
         s3.region: us-east-1
         flush.size: 1000

Apply:

.. code-block:: bash

   gafkalo plan --config config.yaml
   gafkalo apply --config config.yaml

Common connector types
----------------------

JDBC Source
~~~~~~~~~~~

.. code-block:: yaml

   connectors:
     - name: mysql-source
       config:
         connector.class: io.confluent.connect.jdbc.JdbcSourceConnector
         tasks.max: 1
         connection.url: jdbc:mysql://db:3306/mydb
         connection.user: user
         connection.password: pass
         mode: timestamp+incrementing
         timestamp.column.name: updated_at
         incrementing.column.name: id
         topic.prefix: mysql-

S3 Sink
~~~~~~~

.. code-block:: yaml

   connectors:
     - name: s3-sink
       config:
         connector.class: io.confluent.connect.s3.S3SinkConnector
         tasks.max: 3
         topics.regex: events\..*
         s3.bucket.name: data-lake
         s3.region: us-west-2
         flush.size: 1000
         rotate.interval.ms: 3600000
         format.class: io.confluent.connect.s3.format.parquet.ParquetFormat

Elasticsearch Sink
~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   connectors:
     - name: elasticsearch-sink
       config:
         connector.class: io.confluent.connect.elasticsearch.ElasticsearchSinkConnector
         tasks.max: 2
         topics: events.search
         connection.url: https://es:9200
         connection.username: elastic
         connection.password: pass
         type.name: _doc
         key.ignore: false

Replicator
~~~~~~~~~~

.. code-block:: yaml

   connectors:
     - name: replicator-prod-to-dr
       config:
         connector.class: io.confluent.connect.replicator.ReplicatorSourceConnector
         tasks.max: 4
         topic.regex: events\..*
         topic.rename.format: ${topic}.replica
         src.kafka.bootstrap.servers: prod:9092
         dest.kafka.bootstrap.servers: dr:9092
         src.kafka.security.protocol: SASL_SSL
         dest.kafka.security.protocol: SASL_SSL

Best practices
--------------

1. Use ``tasks.max`` appropriate for workload
2. Enable error handling and DLQ
3. Monitor connector and task status
4. Use ``heal`` command in runbooks
5. Version connector configs in git
6. Test in dev before prod

Error handling
--------------

Enable error tolerance:

.. code-block:: yaml

   connectors:
     - name: s3-sink
       config:
         connector.class: io.confluent.connect.s3.S3SinkConnector
         errors.tolerance: all
         errors.log.enable: true
         errors.log.include.messages: true
         errors.deadletterqueue.topic.name: dlq.s3-sink
         errors.deadletterqueue.topic.replication.factor: 3

Monitoring
----------

Use ``health-check`` in monitoring:

.. code-block:: bash

   #!/bin/bash
   gafkalo --config prod.yaml connect health-check
   if [ $? -ne 0 ]; then
     alert "Connect cluster has failed connectors"
   fi

Or check specific connector:

.. code-block:: bash

   status=$(gafkalo --config prod.yaml connect describe my-connector | grep Status)
   if [[ ! "$status" =~ "RUNNING" ]]; then
     gafkalo --config prod.yaml connect heal my-connector
   fi

Troubleshooting
---------------

**Connector fails to start**

.. code-block:: bash

   gafkalo --config config.yaml connect describe connector-name

Check task status and error messages.

**Tasks in FAILED state**

.. code-block:: bash

   gafkalo --config config.yaml connect heal connector-name

**Configuration issues**

Validate connector config via Connect REST API:

.. code-block:: bash

   curl -X PUT https://connect:8083/connector-plugins/JdbcSourceConnector/config/validate \
     -H "Content-Type: application/json" \
     -d @connector-config.json

Limitations
-----------

- No connector deletion via YAML apply (use CLI)
- No custom validation
- No converter/transform validation

For complex deployments, consider Confluent Control Center or custom automation.

Security
--------

Hide sensitive config keys in output. See :ref:`config:Hiding sensitive keys`.
