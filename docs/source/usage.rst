=====
Usage
=====

Maintain topics, schemas and RBAC
---------------------------------

`gafkalo` can maintain the topics, schemas and RBAC rolebindings for a cluster.

In the configuration file you point it to the YAML definitions and it will attempt to create or update any resources needed.

It will investigate the cluster's state in order to decide if something needs creating or updating (or no action needed). 


To generate a plan (a list of changes that it will perform, without actually performing them)

.. code-block:: bash

   gafkalo plan --config myconfig.yaml

This will produce a plan output with the changes that `gafkalo` will make.

Once you are satisfied with the plan you can let `gafkalo` apply the changes:

.. code-block:: bash

  gafkalo apply --config myconfig.yaml

This command will read all the yaml files specified in the config , merge them and apply them. It will produce a report at the end.


Producer
--------

gafkalo can be used as a producer to aid in testing, development and troubleshooting.

gafkalo --config dev.yaml produce "SKATA1" --idempotent --separator=!  --serialize --value-schema-file data/schemas/schema.json --key-schema-file data/schemas/schema.json


Available options:

--idempotent
   
   Enable idempotence in the producer.
   In an RBAC enabled cluster this requires DeveloperWrite in the `Cluster` resource

--acks

   acks to require from brokers. Defaults to -1 (all).

--separator

  a character the producer will use to separate the key part from the value part of the user supplied input

--serialize
  
  serialize the input. The latest schema in the schema registry will be used. 
  Provide `--user-schema` and/or `--key-schema` if you want to produce according to a new schema (will be registered)

--key-schema

  the key schema to use (avsc file)

--value-schema

  the value schema to use (avsc file)

--tombstone

  Produce a tombstone message. The input will be used as record `key` and the value will be automatically `null`.

--whole-file

  Point to a file and produce the whole file as a single kafka record. (for example, to test big messages, etc)

Consumer
--------

gafkalo can be used as a consumer.

Example 1:
Read from SKATA1 and deserialize the value using schema registry

.. code-block:: bash

   gafkalo --config dev.yaml consumer SKATA1


The consumer by default will emit the raw record. IF the records are serialized using Schema Registry you can add the parameters `--deserialize-key` and `--deserialize-value` to deserialize key and value respectively. The consumer will then utilize the schema registry to fetch the corresponding schema , deserialize and display the deserialized output.

.. code-block:: bash

   gafkalo --config dev.yaml consumer SKATA1 --deserialize-key --deserialize-value

It is possible to define a user provided Golang template to format records as they come in. using the `--record-template` parameter.

If you only want to read a certain number of records and then stop you can add `--max-records=<number>`.

.. code-block:: bash

  $ ./gafkalo --config dev.yaml consumer SKATA2 --max-records=4
  Topic[SKATA2] Offset[5] Timestamp[2021-06-30 13:01:05.893 +0200 CEST]: Key:=, Value:asd
  Topic[SKATA2] Offset[6] Timestamp[2021-06-30 13:01:06.701 +0200 CEST]: Key:=, Value:sdf
  Topic[SKATA2] Offset[7] Timestamp[2021-06-30 13:01:07.191 +0200 CEST]: Key:=, Value:sdf
  Topic[SKATA2] Offset[8] Timestamp[2021-06-30 13:01:07.658 +0200 CEST]: Key:=, Value:sdf
  2021/06/30 13:01:07 terminating..


The template will be passed the `CustomRecordTemplateContext` context and all fields will be made available to the user to format as needed

Example:

.. code-block:: go 

   CUSTOMRECORD: {{ .Value }} The key is : {{.Key }} with Offset:{{ .Offset }}


Then produce some records and you will see:

.. code-block:: console

   $ ./gafkalo --config dev.yaml consumer SKATA2 --record-template testdata/files/recordtemplate.tpl
   CUSTOMRECORD: sdfsdf The key is :  with Offset:0
   CUSTOMRECORD: sgfdg The key is :  with Offset:1
   CUSTOMRECORD: dfgdf The key is :  with Offset:2
   CUSTOMRECORD: gdf The key is :  with Offset:3
   CUSTOMRECORD: gdfg The key is :  with Offset:4

The context passed to that template is a `CustomRecordTemplateContext` with this definition (please check source for staleness of this document):


.. code-block:: go 

   type CustomRecordTemplateContext struct {
      Topic       string
      Key         string
      Value       string
      Timestamp   time.Time
      Partition   int32
      Offset      int64
      KeySchemaID int // The schema registry ID of the Key schema
      ValSchemaID int // The Schema registry ID of the Value schema
   }



Schemas
-------

gafkalo has some schema related CLI functions. 

`schema schema-diff` can get compare a subject+version on the schema registry against a JSON file on disk, and tell if they match or give you a visual diff. Useful to identify why schema is detected as changed etc

`schema  check-exists` can check if a provided schema on disk, is already registered on the provided subject name. If it is, it will return under which version and what is the ID of the schema. 


Topics
------

`topic` command allows you to manage topics.

`describe` subcommand outputs the description of a Topic (partitions, replication, configuration).

Example:

.. code-block:: console

   ./gafkalo --config mycluster.yaml topic describe TOPIC1

   Topic: TOPIC1
   ReplicationFactor 3 , Partitions: 1
   Configs: cleanup.policy=delete, compression.type=producer, confluent.key.schema.validation=false, confluent.key.subject.name.strategy=io.confluent.kafka.serializers.subject.TopicNameStrategy, confluent.placement.constraints=, confluent.prefer.tier.fetch.ms=-1, confluent.segment.speculative.prefetch.enable=false, confluent.tier.enable=false, confluent.tier.local.hotset.bytes=-1, confluent.tier.local.hotset.ms=86400000, confluent.tier.segment.hotset.roll.min.bytes=104857600, confluent.value.schema.validation=false, confluent.value.subject.name.strategy=io.confluent.kafka.serializers.subject.TopicNameStrategy, delete.retention.ms=86400000, file.delete.delay.ms=60000, flush.messages=9223372036854775807, flush.ms=9223372036854775807, follower.replication.throttled.replicas=, index.interval.bytes=4096, leader.replication.throttled.replicas=, max.compaction.lag.ms=9223372036854775807, max.message.bytes=1048588, message.downconversion.enable=true, message.format.version=2.7-IV2, message.timestamp.difference.max.ms=9223372036854775807, message.timestamp.type=CreateTime, min.cleanable.dirty.ratio=0.5, min.compaction.lag.ms=0, min.insync.replicas=1, preallocate=false, retention.bytes=-1, retention.ms=604800000, segment.bytes=1073741824, segment.index.bytes=10485760, segment.jitter.ms=0, segment.ms=604800000, unclean.leader.election.enable=false, 
   Partition 0: Leader ID 0. Followers: [1,2]


Topic linter
------------


There is minimal support for a topic linter

The idea is to parse the topic configs and give you errors or warnings for them. For example you may have replication settings not indicated for production setup,
or a tombstone retention setting with a topic that does not use compacion (and is therefore meaningless and indicative of a possible mistake).

Running linter against YAML
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Can be run with:


.. code-block:: bash

   gafkalo plan --config myconfig.yaml lint

and will produce a report like this:

.. code-block:: console
   
   SKATA.VROMIA.LIGO has WARNING: min.insync.replicas not defined (Hint: Setting min.insync.replicas to 2 or higher will reduce chances of data-loss)
   SKATA3 has ERROR: Replication factor < 2. Possible downtime (Hint: Increase replication factor to 3)

Ideally the user should be able to define custom rules in a future version..

Running linter against Broker topics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Linter can also be run against the topics in the running brokers instead of the yaml.
This is useful in detecting misconfigurations or before running a rolling restart.

The command is `lint-broker`

.. code-block:: bash

   gafkalo --config myconfig.yaml lint-broker

.. code-block:: console

   confluent-audit-log-events has ERROR: Replication factor < 2. Possible downtime (Hint: Increase replication factor to 3)
   confluent-audit-log-events has WARNING: min.insync.replicas not defined (Hint: Setting min.insync.replicas to 2 or higher will reduce chances of data-loss)
   _confluent-metadata-auth has ERROR: Replication factor < 2. Possible downtime (Hint: Increase replication factor to 3)
   _confluent-metadata-auth has WARNING: min.insync.replicas not defined (Hint: Setting min.insync.replicas to 2 or higher will reduce chances of data-loss)


Replicator
----------

Gafkalo can be used to replicate a topic into another topic.
A usage scenario would be to copy topic A into topic B (since renaming is not supported by Kafka)

Example usage:


.. code-block:: console

   ./gafkalo --config dev.yaml replicator --source-topic SKATA1 --dest-topic SKATA2


It can also do from between two clusters. In that case the `--config` will be the source cluster and the `--dest-config=` will be the destination's cluster YAML connecton config.

.. code-block:: console

   ./gafkalo --config dev.yaml replicator --source-topic SKATA1 --dest-topic SKATA2


The replicator will default to the group ID `gafkalo-replicator`. This is in contrast to the `consumer` command (that generates random suffixes). This is done to easily support resuming the replicator process and it will use the recoded offsets to resume where it left off. 

Additionaly the replicator defaults to Idempotemt mode, so the required permissions need to be set.


Connnect CLI
------------

Gafkalo can also provide some a CLI interface to manage the Connect_ framework.

The configuration YAML needs the relevant snippet to know how to reach and authenticate to connect.

An full example with authentication and custom TLS CA

.. code-block:: YAML

  connect:
    url: "http://localhost:8083"
    username: "username"
    password: "password""
    caPath: /path/to/ca.pem
    skipVerify: false


Creating a connector:
=====================

.. code-block:: console

   $ ./gafkalo --config conf.yaml connect create  /path/to/connector_definition.json


Listing connectors:
===================

.. code-block:: console

   $ ./gafkalo --config conf.yaml connect list
   ┌───┬────────────────────┐
   │ # │ CONNECTOR NAME     │
   ├───┼────────────────────┤
   │ 0 │ replicator1jschema │
   └───┴────────────────────┘

Delete a connector
==================

.. code-block:: console

   $ ./gafkalo --config conf.yaml connect delete replicator1jschema
   Deleted connector replicator1jschema

Describe a connector: 
=====================

In this example we see a Confluent replicator connector in a simple configuration. There are no tasks running for this connector yet

.. code-block:: console

   $ ./gafkalo --config conf.yaml connect describe replicator1jschema
   
   Connector: replicator1jschema
   ┌─────────────────────────────────────────┬───────────────────────────────────────────────────────────────────────────┐
   │ CONFIG NAME                             │ CONFIG VALUE                                                              │
   ├─────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────┤
   │ connector.class                         │ io.confluent.connect.replicator.ReplicatorSourceConnector                 │
   │ errors.log.include.messages             │ true                                                                      │
   │ tasks.max                               │ 1                                                                         │
   │ topic.rename.format                     │ ${topic}.mirror                                                           │
   │ dest.kafka.security.protocol            │ PLAINTEXT                                                                 │
   │ src.consumer.interceptor.classes        │ io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor │
   │ src.kafka.client.id                     │ connector1-noschema                                                       │
   │ topic.auto.create                       │ true                                                                      │
   │ src.value.converter.schema.registry.url │ http://localhost:8081                                                     │
   │ dest.kafka.client.id                    │ connector1-noschema                                                       │
   │ errors.log.enable                       │ true                                                                      │
   │ topic.regex                             │ raw.*                                                                     │
   │ src.value.converter.schemas.enable      │ false                                                                     │
   │ value.converter.schema.registry.url     │ http://localhost:8081                                                     │
   │ src.kafka.security.protocol             │ PLAINTEXT                                                                 │
   │ dest.kafka.bootstrap.servers            │ localhost:9092                                                            │
   │ src.value.converter                     │ io.confluent.connect.json.JsonSchemaConverter                             │
   │ schema.registry.url                     │ http://localhost:8081                                                     │
   │ name                                    │ replicator1jschema                                                        │
   │ topic.preserve.partitions               │ true                                                                      │
   │ value.converter                         │ io.confluent.connect.avro.AvroConverter                                   │
   │ key.converter                           │ org.apache.kafka.connect.converters.ByteArrayConverter                    │
   │ src.kafka.bootstrap.servers             │ localhost:9095                                                            │
   └─────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────┘
   ┌────┬────────┬────────┬────────────┐
   │ ID │ STATUS │ WORKER │ IS RUNNING │
   ├────┼────────┼────────┼────────────┤
   └────┴────────┴────────┴────────────┘


Heal a failed connector
=======================

The tool can check a connector's status and restart the connector itself and any failed tasks it discovers.

.. code-block:: console

   $ ./gafkalo --config conf.yaml connect heal replicator1jschema

This will do a REST call to restart any task that does not have a status of RUNNING.

.. _Connect: https://docs.confluent.io/platform/current/connect/index.html


Do a quick health check
=======================

`gafkalo` can quickly check all connectors for health and report back any errors.
This is will provide a usefuly, brief information when all you want to know if if everything is OK

.. code-block:: console

   $ ./gafkalo --config conf.yaml connect health-check


.. _Connect: https://docs.confluent.io/platform/current/connect/index.html
