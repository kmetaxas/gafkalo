=====
Usage
=====


To generate a plan

.. code-block:: bash

   gafkalo plan --config myconfig.yaml

This will produce a plan output with the changes that `gafkalo` will make.

Once you are satisfied with the plan you can let `gafkalo` apply:

.. code-block:: bash

  gafkalo apply --config myconfig.yaml

This command will read all the yaml files specified in the config , merge them and apply them.


Producer
--------

gafkalo can be used as a producer to aid in testing, development and troubleshooting.

gafkalo --config dev.yaml produce "SKATA1" --idempotent --separator=!  --serialize --value-schema-file data/schemas/schema.json --key-schema-file data/schemas/schema.json

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
