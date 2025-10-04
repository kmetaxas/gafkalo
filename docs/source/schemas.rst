===================
Maintaining Schemas
===================

Manage Avro schemas and compatibility settings via YAML.

YAML definition
---------------

Schemas are defined alongside topics:

.. code-block:: yaml

   topics:
     - name: events.user.login
       partitions: 6
       replication_factor: 3
       configs:
         cleanup.policy: delete
         min.insync.replicas: 2
       key:
         schema: "schemas/user-key.avsc"
         compatibility: BACKWARD
       value:
         schema: "schemas/login-event.avsc"
         compatibility: BACKWARD_TRANSITIVE

Schema paths:

- Relative paths are resolved from ``kafkalo.schema_dir`` in config
- Absolute paths used as-is

Config example:

.. code-block:: yaml

   kafkalo:
     input_dirs:
       - "data/*.yaml"
     schema_dir: "data/"

Compatibility modes
-------------------

Standard Confluent modes:

- ``BACKWARD``: Consumers using new schema can read old data
- ``BACKWARD_TRANSITIVE``: Backward for all versions
- ``FORWARD``: Consumers using old schema can read new data
- ``FORWARD_TRANSITIVE``: Forward for all versions
- ``FULL``: Both backward and forward
- ``FULL_TRANSITIVE``: Full for all versions
- ``NONE``: No compatibility checks

Use ``BACKWARD`` or ``BACKWARD_TRANSITIVE`` for most use cases.

Schema registry config
-----------------------

In ``config.yaml``:

.. code-block:: yaml

   connections:
     schemaregistry:
       url: "https://schema-registry:8081"
       username: "user"
       password: "pass"
       caPath: "/path/to/ca.crt"
       timeout: 10
       skipRegistryForReads: false

``skipRegistryForReads``:
  Read ``_schemas`` topic directly instead of REST API. Faster for large subject counts.
  Mutations still use REST API.

Schema file format
------------------

Standard Avro schema JSON:

.. code-block:: json

   {
     "type": "record",
     "name": "LoginEvent",
     "namespace": "com.example.events",
     "fields": [
       {"name": "user_id", "type": "string"},
       {"name": "timestamp", "type": "long"},
       {"name": "ip_address", "type": ["null", "string"], "default": null}
     ]
   }

CLI commands
------------

Check if schema exists
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   gafkalo --config config.yaml schema check-exists \
     --subject events.user.login-value \
     --schema-file schemas/login-event.avsc

Returns:

- Schema ID
- Version number
- Whether schema is registered

Compare schemas
~~~~~~~~~~~~~~~

Diff local schema against registered version:

.. code-block:: bash

   gafkalo --config config.yaml schema schema-diff \
     --subject events.user.login-value \
     --version 3 \
     --schema-file schemas/login-event.avsc

Shows visual diff if schemas differ.

Workflow
--------

1. Create/update Avro schema file
2. Reference in YAML topic definition
3. Run ``gafkalo plan`` to preview
4. Run ``gafkalo apply`` to register

Gafkalo handles:

- Registering new schemas
- Checking if schema already exists
- Setting compatibility mode
- Validating against compatibility rules

Example workflow
----------------

**Step 1: Create schema**

``schemas/user-key.avsc``:

.. code-block:: json

   {
     "type": "record",
     "name": "UserKey",
     "namespace": "com.example.keys",
     "fields": [
       {"name": "user_id", "type": "string"}
     ]
   }

``schemas/login-event.avsc``:

.. code-block:: json

   {
     "type": "record",
     "name": "LoginEvent",
     "namespace": "com.example.events",
     "fields": [
       {"name": "user_id", "type": "string"},
       {"name": "timestamp", "type": "long"},
       {"name": "success", "type": "boolean"}
     ]
   }

**Step 2: Define in YAML**

``data/topics.yaml``:

.. code-block:: yaml

   topics:
     - name: events.user.login
       partitions: 6
       replication_factor: 3
       key:
         schema: "schemas/user-key.avsc"
         compatibility: BACKWARD
       value:
         schema: "schemas/login-event.avsc"
         compatibility: BACKWARD

**Step 3: Apply**

.. code-block:: bash

   gafkalo plan --config config.yaml
   gafkalo apply --config config.yaml

Subject naming
--------------

Gafkalo uses TopicNameStrategy by default:

- Key subject: ``<topic-name>-key``
- Value subject: ``<topic-name>-value``

For ``events.user.login``:

- Key: ``events.user.login-key``
- Value: ``events.user.login-value``

Schema evolution
----------------

**Adding optional field (backward compatible)**

.. code-block:: json

   {
     "type": "record",
     "name": "LoginEvent",
     "namespace": "com.example.events",
     "fields": [
       {"name": "user_id", "type": "string"},
       {"name": "timestamp", "type": "long"},
       {"name": "success", "type": "boolean"},
       {"name": "device_type", "type": ["null", "string"], "default": null}
     ]
   }

**Removing field with default (backward compatible)**

Remove field that has default value in previous schema.

**Breaking changes**

- Removing field without default
- Changing field type
- Renaming field without alias

Test compatibility before applying.

Performance optimization
------------------------

For clusters with many subjects (1000+):

.. code-block:: yaml

   connections:
     schemaregistry:
       skipRegistryForReads: true
       timeout: 30

This consumes ``_schemas`` topic directly, building in-memory cache.

Mutations still use REST API for safety.

Best practices
--------------

1. Use ``BACKWARD`` or ``BACKWARD_TRANSITIVE`` compatibility
2. Always provide defaults for optional fields
3. Use namespaces to organize schemas
4. Version schema files in git alongside YAML
5. Test schema changes in dev before prod
6. Use ``gafkalo plan`` to preview schema registration
