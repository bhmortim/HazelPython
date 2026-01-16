Configuration Reference
=======================

This guide covers all configuration options for the Hazelcast Python client.

ClientConfig
------------

The main configuration class for the client.

.. code-block:: python

   from hazelcast import ClientConfig

   config = ClientConfig()

Basic Settings
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Property
     - Default
     - Description
   * - ``cluster_name``
     - ``"dev"``
     - Name of the Hazelcast cluster to connect to
   * - ``client_name``
     - Auto-generated
     - Name for this client instance (for identification)
   * - ``labels``
     - ``[]``
     - List of labels for this client instance

Network Configuration
---------------------

Configure network settings via ``config.network``:

.. code-block:: python

   config = ClientConfig()
   
   # Cluster member addresses
   config.cluster_members = [
       "192.168.1.10:5701",
       "192.168.1.11:5701",
   ]
   
   # Connection timeout
   config.connection_timeout = 5.0  # seconds
   
   # Enable smart routing (connect to all members)
   config.smart_routing = True

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Property
     - Default
     - Description
   * - ``cluster_members``
     - ``["localhost:5701"]``
     - List of cluster member addresses
   * - ``connection_timeout``
     - ``5.0``
     - Connection timeout in seconds
   * - ``smart_routing``
     - ``True``
     - If true, connects to all cluster members

Connection Strategy
-------------------

Configure reconnection behavior:

.. code-block:: python

   from hazelcast.config import ReconnectMode, RetryConfig

   config = ClientConfig()
   
   # Async start (don't block on initial connection)
   config.connection_strategy.async_start = False
   
   # Reconnection mode
   config.connection_strategy.reconnect_mode = ReconnectMode.ON
   
   # Retry configuration
   config.connection_strategy.retry.initial_backoff = 1.0  # seconds
   config.connection_strategy.retry.max_backoff = 30.0
   config.connection_strategy.retry.multiplier = 2.0
   config.connection_strategy.retry.jitter = 0.1

**Reconnect Modes:**

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Mode
     - Description
   * - ``OFF``
     - No automatic reconnection
   * - ``ON``
     - Reconnect synchronously (blocking)
   * - ``ASYNC``
     - Reconnect asynchronously (non-blocking)

Security Configuration
----------------------

Configure authentication and TLS:

.. code-block:: python

   config = ClientConfig()
   
   # Username/password authentication
   config.security.username = "admin"
   config.security.password = "secret"
   
   # Or use token authentication
   config.security.token = "my-api-token"
   
   # TLS configuration
   config.security.tls.enabled = True
   config.security.tls.ca_file = "/path/to/ca.pem"
   config.security.tls.cert_file = "/path/to/client.pem"
   config.security.tls.key_file = "/path/to/client-key.pem"

Near Cache Configuration
------------------------

Configure client-side caching for frequently accessed data:

.. code-block:: python

   from hazelcast.config import NearCacheConfig, EvictionPolicy, InMemoryFormat

   config = ClientConfig()
   
   # Create near cache config for "users" map
   near_cache = NearCacheConfig(
       name="users",
       max_size=10000,
       time_to_live_seconds=300,  # 5 minutes
       max_idle_seconds=60,
       eviction_policy=EvictionPolicy.LRU,
       in_memory_format=InMemoryFormat.OBJECT,
       invalidate_on_change=True,
   )
   
   config.add_near_cache(near_cache)

**Eviction Policies:**

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Policy
     - Description
   * - ``LRU``
     - Least Recently Used - evicts least recently accessed entries
   * - ``LFU``
     - Least Frequently Used - evicts least frequently accessed entries
   * - ``RANDOM``
     - Randomly selects entries for eviction
   * - ``NONE``
     - No eviction (cache grows unbounded)

Serialization Configuration
---------------------------

Configure custom serialization:

.. code-block:: python

   config = ClientConfig()
   
   # Portable serialization version
   config.serialization.portable_version = 1
   
   # Add custom serializer
   config.serialization.add_custom_serializer(MyClass, MySerializer())
   
   # Add compact serializer (recommended for new projects)
   config.serialization.add_compact_serializer(MyCompactSerializer())

Statistics Configuration
------------------------

Enable client statistics for Management Center:

.. code-block:: python

   config = ClientConfig()
   config.statistics.enabled = True
   config.statistics.period_seconds = 5.0  # Publish every 5 seconds

Discovery Configuration
-----------------------

Configure automatic cluster discovery:

AWS Discovery
~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.config import DiscoveryConfig, DiscoveryStrategyType

   config = ClientConfig()
   config.discovery.enabled = True
   config.discovery.strategy_type = DiscoveryStrategyType.AWS
   config.discovery.aws = {
       "region": "us-west-2",
       "tag_key": "hazelcast-cluster",
       "tag_value": "production",
       "access_key": "AKIAIOSFODNN7EXAMPLE",
       "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
   }

Kubernetes Discovery
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   config.discovery.enabled = True
   config.discovery.strategy_type = DiscoveryStrategyType.KUBERNETES
   config.discovery.kubernetes = {
       "namespace": "hazelcast",
       "service_name": "hazelcast-service",
   }

Hazelcast Cloud (Viridian)
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   config.discovery.enabled = True
   config.discovery.strategy_type = DiscoveryStrategyType.CLOUD
   config.discovery.cloud = {
       "cluster_name": "my-cluster",
       "token": "your-api-token",
   }

Loading from YAML
-----------------

Load configuration from a YAML file:

.. code-block:: python

   config = ClientConfig.from_yaml("hazelcast-client.yml")

Example YAML file:

.. code-block:: yaml

   hazelcast_client:
     cluster_name: production
     client_name: my-python-app
     
     network:
       addresses:
         - server1:5701
         - server2:5701
       connection_timeout: 10.0
       smart_routing: true
     
     security:
       username: admin
       password: secret
       tls:
         enabled: true
         ca_file: /path/to/ca.pem
     
     near_caches:
       users:
         max_size: 10000
         time_to_live_seconds: 300
         eviction_policy: LRU
     
     statistics:
       enabled: true
       period_seconds: 5.0

Split-Brain Protection
----------------------

Configure split-brain protection for data consistency during network partitions:

.. code-block:: python

   from hazelcast.config import (
       SplitBrainProtectionConfig,
       SplitBrainProtectionOn,
       SplitBrainProtectionFunctionType,
   )

   config = ClientConfig()
   
   sbp = SplitBrainProtectionConfig(
       name="minimum-3-members",
       min_cluster_size=3,
       protect_on=SplitBrainProtectionOn.READ_WRITE,
       function_type=SplitBrainProtectionFunctionType.MEMBER_COUNT,
   )
   
   config.add_split_brain_protection(sbp)
