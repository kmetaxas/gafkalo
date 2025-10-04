==========================
Maintaining Confluent RBAC
==========================

Manage Confluent RBAC rolebindings via YAML.

No ACL support. RBAC only.

YAML definition
---------------

Define principals and their permissions:

.. code-block:: yaml

   clients:
     - principal: User:app-consumer
       consumer_for:
         - topic: events.user.
         - topic: events.orders.
       groups:
         - name: app-consumer-group-

     - principal: User:app-producer
       producer_for:
         - topic: events.user.
           strict: true

     - principal: Group:data-engineers
       consumer_for:
         - topic: events.
         - topic: state.
       producer_for:
         - topic: analytics.

     - principal: User:admin
       resourceowner_for:
         - topic: events.critical.
           isLiteral: true

Principal format
----------------

Must be prefixed:

- ``User:username``
- ``Group:groupname``

Permission types
----------------

consumer_for
~~~~~~~~~~~~

Read permission for topics and schema registry.

.. code-block:: yaml

   - principal: User:consumer-app
     consumer_for:
       - topic: events.orders.
       - topic: events.payments.
         isLiteral: false

Grants:

- DeveloperRead on topics (default: prefixed)
- DeveloperRead on schema registry subjects

producer_for
~~~~~~~~~~~~

Write permission for topics and schema registry.

.. code-block:: yaml

   - principal: User:producer-app
     producer_for:
       - topic: events.orders.
         strict: false

Grants:

- DeveloperWrite on topics (default: prefixed)
- DeveloperWrite on schema registry subjects

**Strict mode**:

.. code-block:: yaml

   producer_for:
     - topic: events.orders.
       strict: true

Grants:

- DeveloperWrite on topics
- DeveloperRead on schema registry (read-only, no new schemas)

Use for production producers to prevent accidental schema changes.

resourceowner_for
~~~~~~~~~~~~~~~~~

Full ownership of topic.

.. code-block:: yaml

   - principal: User:topic-owner
     resourceowner_for:
       - topic: events.critical.
         isLiteral: true

Grants:

- ResourceOwner role on topic

Topic pattern matching
----------------------

By default, topic names are PREFIXED:

.. code-block:: yaml

   consumer_for:
     - topic: events.user.

Matches:

- ``events.user.login``
- ``events.user.signup``
- ``events.user.anything``

Literal matching:

.. code-block:: yaml

   consumer_for:
     - topic: events.user.login
       isLiteral: true

Matches only ``events.user.login``.

Consumer groups
---------------

Grant access to consumer groups:

.. code-block:: yaml

   - principal: User:consumer-app
     consumer_for:
       - topic: events.orders.
     groups:
       - name: consumer-app-group-

Default role: ``DeveloperRead``

Custom roles:

.. code-block:: yaml

   groups:
     - name: consumer-app-group-
       roles: ["ResourceOwner"]

Disable prefix matching:

.. code-block:: yaml

   groups:
     - name: exact-group-name
       prefixed: false

MDS configuration
-----------------

In ``config.yaml``:

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

Cluster IDs required for cross-cluster rolebindings (e.g., schema registry permissions).

Complete examples
-----------------

Consumer application
~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   clients:
     - principal: User:order-processor
       consumer_for:
         - topic: events.orders.
         - topic: events.payments.
       groups:
         - name: order-processor-group-

Producer application
~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   clients:
     - principal: User:api-gateway
       producer_for:
         - topic: events.user.
         - topic: events.orders.
           strict: true

Producer with idempotence
~~~~~~~~~~~~~~~~~~~~~~~~~

Idempotent producers need cluster-level write:

.. code-block:: yaml

   clients:
     - principal: User:idempotent-producer
       producer_for:
         - topic: events.orders.
           strict: true

Note: Idempotence requires ``DeveloperWrite`` on ``Cluster`` resource. Set via Confluent Control Center or MDS API.

Stream processor (consumer + producer)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   clients:
     - principal: User:stream-processor
       consumer_for:
         - topic: events.raw.
       producer_for:
         - topic: events.processed.
       groups:
         - name: stream-processor-group-

Data engineer (broad read access)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   clients:
     - principal: Group:data-engineers
       consumer_for:
         - topic: events.
         - topic: state.
         - topic: analytics.
       groups:
         - name: analytics-

Topic owner
~~~~~~~~~~~

.. code-block:: yaml

   clients:
     - principal: User:critical-app-owner
       resourceowner_for:
         - topic: events.critical.transactions
           isLiteral: true

Workflow
--------

1. Define principals in YAML
2. Run ``gafkalo plan --config config.yaml``
3. Review rolebindings to be created
4. Run ``gafkalo apply --config config.yaml``

Gafkalo creates rolebindings via MDS REST API.

Best practices
--------------

1. Use service accounts (User:) for applications
2. Use groups (Group:) for teams
3. Enable ``strict: true`` for production producers
4. Use prefix matching for topic families
5. Use literal matching for critical topics
6. Separate read-only and write access
7. Document principals in YAML comments

Limitations
-----------

- No ACL support (RBAC only)
- No rolebinding deletion (by design)
- Predefined role combinations only (consumer, producer, resourceowner)
- No custom role definitions

For advanced use cases, use Confluent Control Center or MDS API directly.

Troubleshooting
---------------

**Principal not found**

Ensure principal exists in authentication backend (LDAP, etc.).

**Insufficient permissions**

MDS user needs ``SystemAdmin`` or ``UserAdmin`` role.

**Cluster ID mismatch**

Verify cluster IDs in config match MDS configuration:

.. code-block:: bash

   curl -u admin:password https://mds:8090/security/1.0/metadataClusterId
