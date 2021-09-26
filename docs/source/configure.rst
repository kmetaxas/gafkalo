=========
Configure
=========

configuration yaml
------------------

You need to provide a `config.yaml` file so that `kafkalo` will know how to connect and authenticate with your Kafka infrastrucure and related components (Schema registry, RBAC metadata server).

Look at sample config file included.

Config yaml structure:

.. code-block:: YAML

   ---
   # Configs to reach out all API endpoints
   connections:
     kafka:
       # The bootstrap brokers to make initial connection to. A list
       bootstrapBrokers: ["localhost:9093"]
       ssl:
         # Enable or disable SSL
         enabled: false
         # Path to a CA file to add to the truststore.
         caPath: "/home/user/ca.crt"
       kerberos:
         # Enable/Disable Kerberos authentication
         enabled: false
         # Service name of Kafka (typically 'kafka')
         serviceName: "kafka"
         # Realm to use.
         realm: "EXAMPLE.COM"
         # Username to use (username is appended to realm to form principal)
         username: "trololol"
         # Password. Only if password authentication is used
         password: "password!"
         # path to keytab file to use for authentication.
         # AFAIK sarama does not support using a ticket from the  credential cache
         keytab: "path_to.key" # Will disable user auth if specified
     schemaregistry:
       # The URL of the schema registry
       url: "http://localhost:8081"
       # Username to use to authenticate
       username: "username"
       # Password to use to authenticate
       password: "password"
       # Path a CA file to add to trust store
       caPath: "/home/user/ca.crt"
     mds:
       # URL to Confluent Metadata Service (MDS)
       url: "http://localhost:8090"
       # Username to authenticate as
       username: "username"
       # Password to use for MDS
       password: "password"
       # Schema registry cluster-id
       schema-registry-cluster-id: "schemaregistry"
       # Connect Cluster ID (if Connect rolebindings are needed)
       connect-cluster-id: "connect-cluster"
       # KSQL cluste id (if ksql rolebindings are needed)
       ksql-cluster-id: "ksql-cluster"
       # Path a CA file to add to trust store
       caPath: "/home/user/ca.crt"

   # App specific configs
   kafkalo:
     input_dirs:
       - "data/*"
       - "data/team2.yaml"
     # This path will be prepended to Schema paths that are not absolute.
     # If a schema: "somedir/schema.json" is defined, it will be treated as:
     # "data/somedir/schema.json"
     schema_dir: "data/"


You can add input dirs with glob patterns to let kafkalo know where to find your YAML definitions. 
Kafkalo will read all the input YAMLs, merge then into a single internal data structure and try to sync them.


encryption
~~~~~~~~~~

`gafkalo` will automatically try to decrypt the config file with sops_. If there no sops metadata in the yaml it will read it as plaintext, otherwise it will attempt to decrypt.

Refer to sops_ for further configuration.

sops_ is bundled as a library and there is no need to have the sops binary in the path.


.. _sops: https://github.com/mozilla/sops

input yaml
----------

Kafkalo will read YAML input file and apply the definitions to the Kafka brokers, Schema registry and Metadata service (Confluent RBAC).

A sample YAML file is as follows:


.. code-block:: YAML

   topics:
     - name: SKATA.VROMIA.POLY
       partitions: 6
       replication_factor: 1
       # Any topic configs can be added to this key
       configs:
         cleanup.policy: delete
         min.insync.replicas: 1
         retention.ms: 10000000
       key:
         # Lookup is relative to file
         schema: "schema-key.json"
         compatibility: BACKWARD
       value:
         schema: "schema.json"
         compatibility: NONE
     - name: SKATA.VROMIA.LIGO
       partitions: 6
       replication_factor: 3
       configs:
         cleanup.policy: delete
         min.insync.replicas: 1
       key:
         schema: "schema-key.json"
     - name: SKATA1
       partitions: 1
       replication_factor: 1
     - name: SKATA2
       partitions: 1
       replication_factor: 1
     - name: SKATA3
       partitions: 1
       replication_factor: 1
     - name: SKATA4
       partitions: 1
       replication_factor: 1
     - name: SKATA5
       partitions: 1
       replication_factor: 1
     - name: SKATA6
       partitions: 1
       replication_factor: 1
     - name: SKATA7
       partitions: 1
       replication_factor: 1
   # Clients configures the RBAC (Confluent MDS)
   clients:
     # principals must be in the form User:name or Group:name
     # For each principal you can have a consumer_for, producer_for or resourceowner_for
     # and the topics for each of these categories
     - principal: User:poutanaola
       consumer_for:
         # By default we will use PREFIXED. 
         # set prefixed: false to set it to LITERAL
         - topic: TOPIC1.
         - topic: TOPIC2.
           prefixed: false
       producer_for:
         - topic: TOPIC1.
       resourceowner_for:
         - topic: TOPIC4.
     - principal: Group:malakes
       consumer_for:
         - topic: TOPIC1.
         - topic: TOPIC2.
       producer_for:
         - topic: TOPIC1.
     - principal: User:produser
       producer_for:
         - topic: TOPIC1.
           # Strict mode is mean for production.
           # It will make the producer able to write the topics but read-only
           # access to the schema registry
           strict: false
      # Alllow this principal access to the following consumer groups.
      # roles can be defined but defaults to DeveloperRead
       groups:
         - name: consumer-produser-
         - name: consumer-produser-owner-
           # if not specified, roles is [DeveloperRead]
           roles: ["ResourceOwner"]
           # prefixed is true by default but can be disabled like below
           prefixed: false
     

topics
~~~~~~
For each topic under the `topics:` key define the name and the required parameters. 
The `configs:` sections is optional and defaults for the cluster will be used.

clients
~~~~~~~~~

This tools is not meant to make common tasks easy, not to make anything possible (at least, not yet)
For this reason we define rolebindings primarily by the client's function.
A client meant to be a consumer will have `consumer_for` defined and the topics it can consume from. This will automatically add the correct permissions for the schema registry. You will need to add a `group:` field to add the consumer group permisssion

For producers the `producer_for` section works the same way as the consumer
You can define a role as `strict: true` if you want to disable writing new schemas in the schema registry. Useful for production systems 


