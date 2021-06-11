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
