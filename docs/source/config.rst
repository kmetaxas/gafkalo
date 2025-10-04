=================
App Configuration
=================

Configuration file (``config.yaml``) defines connections and app settings.

Structure
---------

.. code-block:: yaml

   connections:
     kafka: {...}
     schemaregistry: {...}
     mds: {...}
     connect: {...}

   kafkalo:
     input_dirs: [...]
     schema_dir: "..."

Kafka connection
----------------

.. code-block:: yaml

   connections:
     kafka:
       bootstrapBrokers: ["broker1:9093", "broker2:9093", "broker3:9093"]
       ssl:
         enabled: true
         caPath: "/path/to/ca.crt"
         skipVerify: false
         clientCert: "/path/to/client.crt"
         clientKey: "/path/to/client.key"
       kerberos:
         enabled: false
         serviceName: "kafka"
         realm: "EXAMPLE.COM"
         username: "user"
         password: "pass"
         keytab: "/path/to/user.keytab"

SSL/TLS
~~~~~~~

Standard SSL:

.. code-block:: yaml

   ssl:
     enabled: true
     caPath: "/path/to/ca.crt"
     skipVerify: false

Mutual TLS (mTLS):

.. code-block:: yaml

   ssl:
     enabled: true
     caPath: "/path/to/ca.crt"
     clientCert: "/path/to/client.crt"
     clientKey: "/path/to/client.key"
     skipVerify: false

Options:

- ``enabled``: Enable SSL/TLS
- ``caPath``: CA certificate (optional, uses system truststore if omitted)
- ``skipVerify``: Skip certificate verification (dev/test only)
- ``clientCert``: Client certificate for mTLS
- ``clientKey``: Client private key for mTLS

Kerberos
~~~~~~~~

.. code-block:: yaml

   kerberos:
     enabled: true
     serviceName: "kafka"
     realm: "EXAMPLE.COM"
     username: "kafkauser"
     password: "pass"

Or with keytab:

.. code-block:: yaml

   kerberos:
     enabled: true
     serviceName: "kafka"
     realm: "EXAMPLE.COM"
     username: "kafkauser"
     keytab: "/path/to/user.keytab"

If ``keytab`` is specified, password auth is disabled.

Schema Registry
---------------

.. code-block:: yaml

   connections:
     schemaregistry:
       url: "https://schema-registry:8081"
       username: "user"
       password: "pass"
       caPath: "/path/to/ca.crt"
       timeout: 10
       skipRegistryForReads: false

Options:

- ``url``: Schema registry endpoint
- ``username``: Basic auth username
- ``password``: Basic auth password
- ``caPath``: CA certificate for TLS
- ``timeout``: REST call timeout in seconds (default: 5)
- ``skipRegistryForReads``: Read ``_schemas`` topic directly (default: false)

``skipRegistryForReads``:
  Bypass REST API for read operations. Builds in-memory cache from ``_schemas`` topic.
  Useful for large subject counts (1000+). Mutations still use REST API.

Confluent MDS (RBAC)
--------------------

.. code-block:: yaml

   connections:
     mds:
       url: "https://kafka:8090"
       username: "admin"
       password: "admin-secret"
       caPath: "/path/to/ca.crt"
       schema-registry-cluster-id: "schema-registry"
       connect-cluster-id: "connect-cluster"
       ksql-cluster-id: "ksql-cluster"

Options:

- ``url``: MDS endpoint
- ``username``: MDS admin user
- ``password``: MDS admin password
- ``caPath``: CA certificate for TLS
- ``schema-registry-cluster-id``: Schema registry cluster ID (for cross-cluster bindings)
- ``connect-cluster-id``: Connect cluster ID
- ``ksql-cluster-id``: KSQL cluster ID

Cluster IDs required for rolebindings across services (e.g., schema registry permissions).

Kafka Connect
-------------

.. code-block:: yaml

   connections:
     connect:
       url: "https://connect:8083"
       username: "connect-user"
       password: "connect-pass"
       caPath: "/path/to/ca.crt"
       skipVerify: false

Options:

- ``url``: Connect REST endpoint
- ``username``: Basic auth username
- ``password``: Basic auth password
- ``caPath``: CA certificate for TLS
- ``skipVerify``: Skip certificate verification (dev/test only)

App settings
------------

.. code-block:: yaml

   kafkalo:
     input_dirs:
       - "data/*.yaml"
       - "data/team-a/*.yaml"
       - "data/team-b/topics.yaml"
     schema_dir: "data/"

``input_dirs``:
  YAML files to load. Supports glob patterns. All files merged into single config.

``schema_dir``:
  Base directory for relative schema paths. If schema path is ``schemas/user.avsc`` and ``schema_dir`` is ``data/``, final path is ``data/schemas/user.avsc``.

Encryption (SOPS)
-----------------

Gafkalo automatically decrypts SOPS-encrypted configs.

Encrypt config:

.. code-block:: bash

   sops -e config.yaml > config.enc.yaml

Use encrypted config:

.. code-block:: bash

   gafkalo --config config.enc.yaml plan

SOPS metadata required in YAML. Plaintext YAML works without SOPS.

Refer to `SOPS documentation <https://github.com/mozilla/sops>`_ for setup.

Complete example
----------------

.. code-block:: yaml

   connections:
     kafka:
       bootstrapBrokers: ["kafka1:9093", "kafka2:9093", "kafka3:9093"]
       ssl:
         enabled: true
         caPath: "/etc/kafka/ca.crt"
         clientCert: "/etc/kafka/client.crt"
         clientKey: "/etc/kafka/client.key"

     schemaregistry:
       url: "https://schema-registry:8081"
       username: "sr-user"
       password: "sr-pass"
       caPath: "/etc/kafka/ca.crt"
       timeout: 10
       skipRegistryForReads: false

     mds:
       url: "https://kafka1:8090"
       username: "mds-admin"
       password: "mds-secret"
       caPath: "/etc/kafka/ca.crt"
       schema-registry-cluster-id: "schema-registry"

     connect:
       url: "https://connect:8083"
       username: "connect-user"
       password: "connect-pass"
       caPath: "/etc/kafka/ca.crt"

   kafkalo:
     input_dirs:
       - "definitions/production/*.yaml"
       - "definitions/shared/*.yaml"
     schema_dir: "schemas/"

Environment-specific configs
----------------------------

Maintain separate configs per environment:

.. code-block:: text

   configs/
     dev.yaml
     staging.yaml
     prod.yaml

Use:

.. code-block:: bash

   gafkalo --config configs/dev.yaml plan
   gafkalo --config configs/prod.yaml plan

Best practices
--------------

1. Use SOPS for production credentials
2. Separate configs per environment
3. Use mTLS for production
4. Set appropriate timeouts for schema registry
5. Version control configs (encrypt secrets)
6. Use service accounts, not personal credentials
7. Enable ``skipRegistryForReads`` only if needed
8. Document cluster IDs in comments
