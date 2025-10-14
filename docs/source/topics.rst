==================
Maintaining Topics
==================

Define topics in YAML and manage them declaratively.

YAML definition
---------------

.. code-block:: yaml

   topics:
     - name: events.user.signup
       partitions: 12
       replication_factor: 3
       configs:
         cleanup.policy: delete
         min.insync.replicas: 2
         retention.ms: 604800000
         compression.type: lz4

     - name: state.user.profile
       partitions: 6
       replication_factor: 3
       configs:
         cleanup.policy: compact
         min.insync.replicas: 2

Required fields:

- ``name``: Topic name
- ``partitions``: Partition count
- ``replication_factor``: Replication factor

Optional fields:

- ``configs``: Topic-level configurations (key-value pairs)

Common configurations
---------------------

**Retention**

.. code-block:: yaml

   configs:
     retention.ms: 604800000        # 7 days
     retention.bytes: 1073741824    # 1 GB

**Compaction**

.. code-block:: yaml

   configs:
     cleanup.policy: compact
     min.cleanable.dirty.ratio: 0.5
     delete.retention.ms: 86400000

**Durability**

.. code-block:: yaml

   configs:
     min.insync.replicas: 2
     unclean.leader.election.enable: false

**Compression**

.. code-block:: yaml

   configs:
     compression.type: lz4  # snappy, gzip, zstd, producer

CLI commands
------------

List topics
~~~~~~~~~~~

.. code-block:: bash

   # Plain list
   gafkalo --config config.yaml topic list

   # Table format
   gafkalo --config config.yaml topic list --output-format table

   # JSON
   gafkalo --config config.yaml topic list --output-format json

   # Filter by pattern
   gafkalo --config config.yaml topic list --pattern '^prod-'

   # Include internal topics
   gafkalo --config config.yaml topic list --show-internal

Output formats:

- ``plain``: Topic names, one per line (default)
- ``table``: Formatted table with metadata
- ``json``: Structured JSON
- ``detailed``: Human-readable detailed view

Create topic
~~~~~~~~~~~~

For ad-hoc topic creation (YAML workflow recommended for production):

.. code-block:: bash

   # Basic topic
   gafkalo --config config.yaml topic create -n my-topic

   # Production topic
   gafkalo --config config.yaml topic create -n events.orders \
     --partitions 12 \
     --replication-factor 3 \
     -c retention.ms=604800000 \
     -c min.insync.replicas=2

   # Compacted topic
   gafkalo --config config.yaml topic create -n state.users \
     --partitions 6 \
     --replication-factor 3 \
     -c cleanup.policy=compact

   # Validate only (dry-run)
   gafkalo --config config.yaml topic create -n test-topic \
     --partitions 5 \
     --validate-only

Describe topic
~~~~~~~~~~~~~~

.. code-block:: bash

   gafkalo --config config.yaml topic describe events.orders

Output shows:

- Partition count and replication factor
- All topic configurations
- Partition leaders and replicas

Change partitions
~~~~~~~~~~~~~~~~~

Modify partition count and replication factor for existing topics:

.. code-block:: bash

   # Plan changes (dry-run)
   gafkalo --config config.yaml topic partitions events.orders \
     --count 24 \
     --factor 3 \
     --plan

   # Execute changes
   gafkalo --config config.yaml topic partitions events.orders \
     --count 24 \
     --factor 3 \
     --execute

Options:

``--count``
  New partition count (must be >= current count)

``--factor``
  Replication factor for new partitions

``--plan``
  Show execution plan without applying changes (dry-run)

``--execute``
  Apply the partition changes

Limitations:

- Cannot decrease partition count (Kafka limitation)
- Must specify either ``--plan`` or ``--execute``
- Topic must exist

The plan output shows:

- Current and new partition counts
- Replication factor
- Replica assignments for new partitions

Topic linter
------------

Detect misconfigurations and anti-patterns.

Lint YAML definitions
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   gafkalo plan --config config.yaml lint

Example output:

.. code-block:: console

   events.login has WARNING: min.insync.replicas not defined (Hint: Setting min.insync.replicas to 2 or higher will reduce chances of data-loss)
   state.users has ERROR: Replication factor < 2. Possible downtime (Hint: Increase replication factor to 3)

Lint running cluster
~~~~~~~~~~~~~~~~~~~~

Check topics already deployed:

.. code-block:: bash

   gafkalo --config config.yaml lint-broker

Useful before rolling restarts or auditing production.

Best practices
--------------

**Production topics**

.. code-block:: yaml

   topics:
     - name: prod.events
       partitions: 12
       replication_factor: 3
       configs:
         min.insync.replicas: 2
         unclean.leader.election.enable: false
         retention.ms: 604800000

**State topics (compacted)**

.. code-block:: yaml

   topics:
     - name: state.users
       partitions: 6
       replication_factor: 3
       configs:
         cleanup.policy: compact
         min.insync.replicas: 2
         min.cleanable.dirty.ratio: 0.5

**Tombstone support**

.. code-block:: yaml

   topics:
     - name: state.cache
       partitions: 6
       replication_factor: 3
       configs:
         cleanup.policy: compact
         delete.retention.ms: 86400000

Updating topics
---------------

Supported changes:

- Increase partition count
- Update topic configurations
- Change replication factor for new partitions

Unsupported:

- Decrease partitions (Kafka limitation)
- Delete topics (by design)

**Declarative approach (recommended)**

Update your YAML definition:

.. code-block:: yaml

   topics:
     - name: events.orders
       partitions: 24           # Increased from 12
       replication_factor: 3
       configs:
         retention.ms: 1209600000  # Changed from 7d to 14d

Then run:

.. code-block:: bash

   gafkalo plan --config config.yaml   # Review changes
   gafkalo apply --config config.yaml  # Apply

**Imperative approach**

For ad-hoc partition changes:

.. code-block:: bash

   # Preview changes
   gafkalo --config config.yaml topic partitions events.orders \
     --count 24 \
     --factor 3 \
     --plan

   # Apply changes
   gafkalo --config config.yaml topic partitions events.orders \
     --count 24 \
     --factor 3 \
     --execute
