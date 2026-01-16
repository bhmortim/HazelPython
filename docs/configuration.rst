Configuration Reference
=======================

This page documents all configuration options for the Hazelcast Python Client.

ClientConfig
------------

The main configuration class for the Hazelcast client.

.. code-block:: python

   from hazelcast import ClientConfig

   config = ClientConfig()
   config.cluster_name = "production"
   config.client_name = "my-app-client"
   config.cluster_members = ["node1:5701", "node2:5701"]

Basic Properties
~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 25 15 15 45

   * - Property
     - Type
     - Default
     - Description
   * - ``cluster_name``
     - ``str``
     - ``"dev"``
     - Name of the Hazelcast cluster to connect to
   * - ``client_name``
     - ``str``
     - Auto-generated
     - Identifier for this client instance
   * - ``cluster_members``
     - ``List[str]``
     - ``["localhost:5701"]``
     - List of cluster member addresses
   * - ``connection_timeout``
     - ``float``
     - ``5.0``
     - Connection timeout in seconds
   * - ``smart_routing``
     - ``bool``
     - ``True``
     - Enable smart routing to partition owners
   * - ``labels``
     - ``List[str]``
     - ``[]``
     - Client labels for identification


NetworkConfig
-------------

Network-related settings:

.. code-block:: python

   from hazelcast.config import NetworkConfig

   network = NetworkConfig(
       addresses=["node1:5701", "node2:5701"],
       connection_timeout=10.0,
       smart_routing=True,
   )
   config.network = network

.. list-table::
   :header-rows: 1
   :widths: 25 15 15 45

   * - Property
     - Type
     - Default
     - Description
   * - ``addresses``
     - ``List[str]``
     - ``["localhost:5701"]``
     - List of cluster member addresses
   * - ``connection_timeout``
     - ``float``
     - ``5.0``
     - Connection timeout in seconds
   * - ``smart_routing``
     - ``bool``
     - ``True``
     - Route operations to partition owners


SecurityConfig
--------------

Authentication credentials:

.. code-block:: python

   from hazelcast.config import SecurityConfig

   # Username/password authentication
   config.security = SecurityConfig(
       username="admin",
       password="secret",
   )

   # Token-based authentication
   config.security = SecurityConfig(
       token="your-auth-token",
   )

.. list-table::
   :header-rows: 1
   :widths: 25 15 15 45

   * - Property
     - Type
     - Default
     - Description
   * - ``username``
     - ``str``
     - ``None``
     - Username for authentication
   * - ``password``
     - ``str``
     - ``None``
     - Password for authentication
   * - ``token``
     - ``str``
     - ``None``
     - Authentication token


ConnectionStrategyConfig
------------------------

Connection and reconnection settings:

.. code-block:: python

   from hazelcast.config import (
       ConnectionStrategyConfig,
       ReconnectMode,
       RetryConfig,
   )

   config.connection_strategy = ConnectionStrategyConfig(
       async_start=False,
       reconnect_mode=ReconnectMode.ON,
       retry=RetryConfig(
           initial_backoff=1.0,
           max_backoff=30.0,
           multiplier=2.0,
           jitter=0.1,
       ),
   )

ReconnectMode Options
~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Mode
     - Description
   * - ``OFF``
     - Do not reconnect after disconnection
   * - ``ON``
     - Automatically reconnect with blocking
   * - ``ASYNC``
     - Automatically reconnect without blocking

RetryConfig Options
~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 25 15 15 45

   * - Property
     - Type
     - Default
     - Description
   * - ``initial_backoff``
     - ``float``
     - ``1.0``
     - Initial retry delay in seconds
   * - ``max_backoff``
     - ``float``
     - ``30.0``
     - Maximum retry delay in seconds
   * - ``multiplier``
     - ``float``
     - ``2.0``
     - Backoff multiplier for exponential delay
   * - ``jitter``
     - ``float``
     - ``0.0``
     - Random jitter factor (0.0 to 1.0)


NearCacheConfig
---------------

Configure local caching for maps:

.. code-block:: python

   from hazelcast.config import NearCacheConfig, EvictionPolicy, InMemoryFormat

   near_cache = NearCacheConfig(
       name="users",
       max_size=10000,
       time_to_live_seconds=300,
       max_idle_seconds=60,
       eviction_policy=EvictionPolicy.LRU,
       in_memory_format=InMemoryFormat.OBJECT,
       invalidate_on_change=True,
   )
   config.add_near_cache(near_cache)

.. list-table::
   :header-rows: 1
   :widths: 25 15 15 45

   * - Property
     - Type
     - Default
     - Description
   * - ``name``
     - ``str``
     - Required
     - Name of the map to cache
   * - ``max_size``
     - ``int``
     - ``10000``
     - Maximum number of entries
   * - ``time_to_live_seconds``
     - ``int``
     - ``0``
     - Entry TTL (0 = infinite)
   * - ``max_idle_seconds``
     - ``int``
     - ``0``
     - Max idle time (0 = infinite)
   * - ``eviction_policy``
     - ``EvictionPolicy``
     - ``LRU``
     - ``LRU``, ``LFU``, ``RANDOM``, ``NONE``
   * - ``in_memory_format``
     - ``InMemoryFormat``
     - ``BINARY``
     - ``BINARY`` or ``OBJECT``
   * - ``invalidate_on_change``
     - ``bool``
     - ``True``
     - Invalidate on server-side changes


SerializationConfig
-------------------

Configure serialization:

.. code-block:: python

   from hazelcast.config import SerializationConfig

   serialization = SerializationConfig(
       portable_version=1,
       default_integer_type="INT",
   )

   # Add custom serializers
   serialization.add_portable_factory(1, MyPortableFactory())
   serialization.add_compact_serializer(MyCompactSerializer())

   config.serialization = serialization


YAML Configuration
------------------

Load configuration from a YAML file:

.. code-block:: yaml

   # hazelcast-client.yml
   hazelcast_client:
     cluster_name: production
     client_name: my-app

     network:
       addresses:
         - node1.example.com:5701
         - node2.example.com:5701
       connection_timeout: 10.0
       smart_routing: true

     security:
       username: admin
       password: secret

     connection_strategy:
       async_start: false
       reconnect_mode: ON
       retry:
         initial_backoff: 1.0
         max_backoff: 30.0
         multiplier: 2.0

     near_caches:
       users:
         max_size: 10000
         time_to_live_seconds: 300
         eviction_policy: LRU

     labels:
       - web-tier
       - production

Load in Python:

.. code-block:: python

   config = ClientConfig.from_yaml("hazelcast-client.yml")

Or from a YAML string:

.. code-block:: python

   yaml_content = """
   hazelcast_client:
     cluster_name: dev
     network:
       addresses:
         - localhost:5701
   """
   config = ClientConfig.from_yaml_string(yaml_content)


Environment Variables
---------------------

The client version can be set via environment variable:

.. code-block:: bash

   export HAZELCAST_CLIENT_VERSION=1.0.0


Complete Example
----------------

.. code-block:: python

   from hazelcast import HazelcastClient, ClientConfig
   from hazelcast.config import (
       NetworkConfig,
       SecurityConfig,
       ConnectionStrategyConfig,
       ReconnectMode,
       RetryConfig,
       NearCacheConfig,
       EvictionPolicy,
   )

   # Create comprehensive configuration
   config = ClientConfig()
   config.cluster_name = "production"
   config.client_name = "order-service"
   config.labels = ["backend", "orders"]

   # Network settings
   config.network = NetworkConfig(
       addresses=["hz1.internal:5701", "hz2.internal:5701"],
       connection_timeout=10.0,
       smart_routing=True,
   )

   # Security
   config.security = SecurityConfig(
       username="order-service",
       password="secure-password",
   )

   # Connection strategy
   config.connection_strategy = ConnectionStrategyConfig(
       async_start=False,
       reconnect_mode=ReconnectMode.ON,
       retry=RetryConfig(
           initial_backoff=1.0,
           max_backoff=60.0,
           multiplier=2.0,
           jitter=0.1,
       ),
   )

   # Near cache for frequently accessed data
   config.add_near_cache(NearCacheConfig(
       name="products",
       max_size=5000,
       time_to_live_seconds=300,
       eviction_policy=EvictionPolicy.LRU,
   ))

   # Connect
   with HazelcastClient(config) as client:
       # Client is ready to use
       pass
