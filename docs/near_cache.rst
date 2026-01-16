Near Cache
==========

Near Cache provides client-side caching of map data, dramatically reducing
network round-trips for read operations.

Configuration
-------------

Configure near cache per map in your client configuration:

.. code-block:: python

   from hazelcast import ClientConfig
   from hazelcast.config import NearCacheConfig, EvictionPolicy, InMemoryFormat

   config = ClientConfig()

   config.add_near_cache(NearCacheConfig(
       name="users",
       max_size=10000,
       time_to_live_seconds=300,      # 5 minutes TTL
       max_idle_seconds=60,           # Evict after 1 minute idle
       eviction_policy=EvictionPolicy.LRU,
       in_memory_format=InMemoryFormat.OBJECT,
       invalidate_on_change=True,
   ))


Configuration Options
---------------------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Option
     - Default
     - Description
   * - ``name``
     - Required
     - Name of the map to cache
   * - ``max_size``
     - 10000
     - Maximum number of entries in the cache
   * - ``time_to_live_seconds``
     - 0
     - Entry TTL in seconds (0 = infinite)
   * - ``max_idle_seconds``
     - 0
     - Max idle time before eviction (0 = infinite)
   * - ``eviction_policy``
     - LRU
     - ``LRU``, ``LFU``, ``RANDOM``, ``NONE``
   * - ``in_memory_format``
     - BINARY
     - ``BINARY`` or ``OBJECT``
   * - ``invalidate_on_change``
     - True
     - Invalidate on server-side changes


Eviction Policies
-----------------

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Policy
     - Description
   * - ``LRU``
     - Least Recently Used - evicts entries not accessed recently
   * - ``LFU``
     - Least Frequently Used - evicts entries accessed least often
   * - ``RANDOM``
     - Random eviction
   * - ``NONE``
     - No eviction (cache can grow unbounded up to max_size)


In-Memory Formats
-----------------

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Format
     - Description
   * - ``BINARY``
     - Store entries as serialized binary data. Lower memory footprint,
       higher CPU on access. Best for large objects.
   * - ``OBJECT``
     - Store entries as deserialized objects. Higher memory footprint,
       faster access. Best for frequently accessed data.


Usage Example
-------------

Once configured, near cache is transparent:

.. code-block:: python

   from hazelcast import HazelcastClient, ClientConfig
   from hazelcast.config import NearCacheConfig, EvictionPolicy

   config = ClientConfig()
   config.add_near_cache(NearCacheConfig(
       name="products",
       max_size=5000,
       time_to_live_seconds=300,
       eviction_policy=EvictionPolicy.LRU,
   ))

   with HazelcastClient(config) as client:
       products = client.get_map("products")

       # First read - fetches from cluster
       product = products.get("product:123")

       # Second read - served from near cache (no network call)
       product = products.get("product:123")


Invalidation
------------

When ``invalidate_on_change=True``, the near cache automatically
invalidates entries when they change on the server:

.. code-block:: python

   # Client A
   products.put("product:123", new_product_data)

   # Client B's near cache for "product:123" is invalidated
   # Next get() will fetch fresh data from the cluster


YAML Configuration
------------------

.. code-block:: yaml

   hazelcast_client:
     cluster_name: dev

     near_caches:
       products:
         max_size: 10000
         time_to_live_seconds: 300
         max_idle_seconds: 60
         eviction_policy: LRU
         in_memory_format: OBJECT
         invalidate_on_change: true

       users:
         max_size: 5000
         time_to_live_seconds: 600
         eviction_policy: LFU


Best Practices
--------------

1. **Use for read-heavy workloads**: Near cache benefits reads more than
   writes.

2. **Set appropriate TTL**: Balance freshness vs. cache hit rate.

3. **Size based on memory**: Consider JVM/Python heap when setting max_size.

4. **Enable invalidation**: Keep ``invalidate_on_change=True`` unless you
   can tolerate stale data.

5. **Choose format wisely**: Use OBJECT for hot data, BINARY for large
   or infrequently accessed data.

6. **Monitor hit rates**: Track cache effectiveness and adjust settings.
