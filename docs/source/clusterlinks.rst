===========================
Managing Cluster Links
===========================

Manage Confluent Cluster Links via the CLI and GitOps (YAML).

Cluster Links are a feature of Confluent Platform and Confluent Cloud that allows you to create a connection between two Kafka clusters, enabling reliable and simple data mirroring.

Configuration
-------------

Cluster Link management requires a connection to the Confluent REST Proxy.

.. note::
  Cluster Link functionality is only available for Confluent Platform and Confluent Cloud. The REST Proxy must be enabled and configured.

In ``config.yaml``, configure the ``restproxy`` connection:

.. code-block:: yaml

   connections:
     restproxy:
       url: "http://localhost:8082" # URL of your Confluent REST Proxy
       clusterId: "lkc-abcde"      # The ID of the *destination* cluster
       username: "rest-proxy-user"
       password: "rest-proxy-pass"
       caPath: "/path/to/ca.crt"
       skipVerify: false

YAML Definition
---------------

Define cluster links in your input YAML files (applied via ``gafkalo plan/apply``).

.. code-block:: yaml

   clusterlinks:
     - name: my-source-link-to-dest
       cluster_id: lkc-12345  # ID of the *source* cluster
       configs:
         bootstrap.servers: "source-cluster:9092"
         security.protocol: "SASL_SSL"
         sasl.mechanism: "PLAIN"
         sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username='user' password='pass';
         auto.create.mirror.topics.filters: |
            { "topicFilters": [ {"name": "TROL",  "patternType": "PREFIXED",  "filterType": "INCLUDE"} ] }
         auto.create.mirror.topics.enable: "true"
     - name: another-link
       cluster_id: lkc-67890 # ID of the *source* cluster
       configs:
         bootstrap.servers: "another-cluster:9092"
         security.protocol: "SSL"

**Key Fields:**

- ``name``: A unique name for the cluster link on the destination cluster.
- ``cluster_id``: The cluster ID of the **source** cluster you are linking from.
- ``configs``: A map of configuration properties for the cluster link. These define how the destination cluster connects to the source cluster.

To apply the changes:

.. code-block:: bash

   gafkalo plan --config config.yaml
   gafkalo apply --config config.yaml

CLI Commands
------------

CLI commands for managing cluster links are available under the ``clusterlink`` subcommand.

List Cluster Links
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   gafkalo --config config.yaml clusterlink list

Create a Cluster Link
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   gafkalo --config config.yaml clusterlink create --name my-link --config-file link-definition.yaml

Where ``link-definition.yaml`` contains:

.. code-block:: yaml

    cluster_id: lkc-12345
    configs:
      bootstrap.servers: "source-cluster:9092"
      # ... other configs

Update a Cluster Link
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   gafkalo --config config.yaml clusterlink update --name my-link --config-file updated-link.yaml

This will compare the existing link's configuration with ``updated-link.yaml`` and apply any changes.

Delete a Cluster Link
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   gafkalo --config config.yaml clusterlink delete --name my-link

