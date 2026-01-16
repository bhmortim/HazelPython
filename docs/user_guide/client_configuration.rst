Client Configuration
====================

The ``ClientConfig`` class provides comprehensive configuration options
for the Hazelcast Python Client.

Basic Configuration
-------------------

.. code-block:: python

    from hazelcast import ClientConfig

    config = ClientConfig()
    config.cluster_name = "production"
    config.client_name = "my-app-client"
    config.cluster_members = ["node1:5701", "node2:5701", "node3:5701"]

Network Configuration
---------------------

Connection Settings
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    config = ClientConfig()

    # Cluster addresses
    config.cluster_members = ["192.168.1.100:5701", "192.168.1.101:5701"]

    # Connection timeout (seconds)
    config.connection_timeout = 10.0

    # Smart routing - connects to all cluster members
    config.smart_routing = True

Retry Configuration
~~~~~~~~~~~~~~~~~~~

Configure exponential backoff for connection retries:

.. code-block:: python

    from hazelcast.config import RetryConfig, ReconnectMode

    config = ClientConfig()

    # Reconnection mode
    config.connection_strategy.reconnect_mode = ReconnectMode.ON

    # Custom retry settings
    config.connection_strategy.retry = RetryConfig(
        initial_backoff=1.0,    # Initial delay in seconds
        max_backoff=30.0,       # Maximum delay
        multiplier=2.0,         # Backoff multiplier
        jitter=0.1,             # Random jitter factor (0.0 to 1.0)
    )

Security Configuration
----------------------

Username/Password Authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    config = ClientConfig()
    config.security.username = "admin"
    config.security.password = "secret"

Token-Based Authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    config = ClientConfig()
    config.security.token = "your-auth-token"

Serialization Configuration
---------------------------

.. code-block:: python

    from hazelcast.config import SerializationConfig

    config = ClientConfig()

    # Portable serialization version
    config.serialization.portable_version = 1

    # Add custom serializers
    config.serialization.add_custom_serializer(MyClass, MySerializer())

    # Add compact serializers
    config.serialization.add_compact_serializer(MyCompactSerializer())

Near Cache Configuration
------------------------

Enable client-side caching for faster reads:

.. code-block:: python

    from hazelcast.config import NearCacheConfig, EvictionPolicy, InMemoryFormat

    # Create near cache config
    nc_config = NearCacheConfig(
        name="my-map",
        max_size=10000,
        max_idle_seconds=300,
        time_to_live_seconds=600,
        eviction_policy=EvictionPolicy.LRU,
        in_memory_format=InMemoryFormat.BINARY,
        invalidate_on_change=True,
    )

    config = ClientConfig()
    config.add_near_cache(nc_config)

Loading from YAML
-----------------

Load configuration from a YAML file:

.. code-block:: python

    config = ClientConfig.from_yaml("hazelcast-client.yml")

Example YAML configuration:

.. code-block:: yaml

    hazelcast_client:
      cluster_name: production
      client_name: my-client

      network:
        addresses:
          - node1:5701
          - node2:5701
        connection_timeout: 10.0
        smart_routing: true

      connection_strategy:
        reconnect_mode: ON
        retry:
          initial_backoff: 1.0
          max_backoff: 30.0
          multiplier: 2.0

      security:
        username: admin
        password: secret

      near_caches:
        my-map:
          max_size: 10000
          eviction_policy: LRU
          invalidate_on_change: true

      labels:
        - web-tier
        - production

Client Labels
-------------

Add labels for identification and filtering:

.. code-block:: python

    config = ClientConfig()
    config.labels = ["web-tier", "region-us-east", "production"]

Configuration Reference
-----------------------

.. list-table:: Core Settings
   :widths: 25 15 60
   :header-rows: 1

   * - Property
     - Default
     - Description
   * - ``cluster_name``
     - "dev"
     - Name of the Hazelcast cluster
   * - ``client_name``
     - auto-generated
     - Unique name for this client instance
   * - ``cluster_members``
     - ["localhost:5701"]
     - List of cluster member addresses

.. list-table:: Network Settings
   :widths: 25 15 60
   :header-rows: 1

   * - Property
     - Default
     - Description
   * - ``connection_timeout``
     - 5.0
     - Connection timeout in seconds
   * - ``smart_routing``
     - True
     - Connect to all cluster members
