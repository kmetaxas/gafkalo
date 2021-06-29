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

   gafkalo --config dev.yaml consumer SKATA1  --deserialize-value


It is possible to define a user provided Golang template to format records as they come in. using the `--record-template` parameter.

The template will be passed the `CustomRecordTemplateContext` context and all fields will be made available to the user to format as needed


Schemas
-------

gafkalo has some schema related CLI functions. 

`schema schema-diff` can get compare a subject+version on the schema registry against a JSON file on disk, and tell if they match or give you a visual diff. Useful to identify why schema is detected as changed etc

`schema  check-exists` can check if a provided schema on disk, is already registered on the provided subject name. If it is, it will return under which version and what is the ID of the schema. 
