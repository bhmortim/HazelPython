Near Cache
==========

Near Cache provides client-side caching for frequently accessed data,
reducing network round-trips and improving read performance.

Configuration
-------------

Configure near cache for a map:

.. code-block:: python

   from hazelcast import ClientConfig, HazelcastClient
   from hazelcast.config import (
       NearCacheConfig,
       EvictionPolicy,
       InMemoryFormat,
   )

   config = ClientConfig()
   
   # Create near cache configuration
   near_cache = NearCacheConfig(
       name="users",  # Map name
       max_size=10000,  # Maximum entries
       time_to_live_seconds=300,  # Entry TTL (5 minutes)
       max_idle_seconds=60,  # Max idle time
       eviction_policy=EvictionPolicy.LRU,
       in_memory_format=InMemoryFormat.OBJECT,
       invalidate_on_change=True,  # Sync with cluster changes
   )
   
   config.add_near_cache(near_cache)
   
   with HazelcastClient(config) as client:
       users = client.get_map("users")
       
       # First access - fetches from cluster
       user = users.get("user:1")
       
       # Subsequent access - served from near cache
       user = users.get("user:1")  # Fast local lookup

Configuration Options
---------------------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Property
     - Default
     - Description
   * - ``name``
     - Required
     - Map name to cache
   * - ``max_size``
     - ``10000``
     - Maximum number of entries
   * - ``time_to_live_seconds``
     - ``0``
     - Entry TTL (0 = no TTL)
   * - ``max_idle_seconds``
     - ``0``
     - Max idle time (0 = no limit)
   * - ``eviction_policy``
     - ``LRU``
     - Eviction policy (LRU, LFU, RANDOM, NONE)
   * - ``in_memory_format``
     - ``BINARY``
     - Storage format (BINARY or OBJECT)
   * - ``invalidate_on_change``
     - ``True``
     - Invalidate on cluster changes
   * - ``serialize_keys``
     - ``False``
     - Serialize keys in cache

Eviction Policies
-----------------

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Policy
     - Description
   * - ``LRU``
     - Least Recently Used - evicts entries not accessed recently
   * - ``LFU``
     - Least Frequently Used - evicts entries with lowest access count
   * - ``RANDOM``
     - Randomly selects entries for eviction
   * - ``NONE``
     - No eviction (cache grows until TTL expires entries)

In-Memory Format
----------------

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Format
     - Description
   * - ``BINARY``
     - Store serialized data (lower memory, deserialization on each read)
   * - ``OBJECT``
     - Store deserialized objects (faster reads, higher memory)

Statistics
----------

Monitor near cache performance:

.. code-block:: python

   from hazelcast.near_cache import NearCacheManager

   # Access near cache statistics
   users_map = client.get_map("users")
   
   # If using NearCacheManager directly
   manager = NearCacheManager()
   cache = manager.get("users")
   
   if cache:
       stats = cache.stats
       print(f"Hits: {stats.hits}")
       print(f"Misses: {stats.misses}")
       print(f"Hit ratio: {stats.hit_ratio:.2%}")
       print(f"Entries: {stats.entries_count}")
       print(f"Evictions: {stats.evictions}")
       print(f"Expirations: {stats.expirations}")

Invalidation
------------

Near cache entries are invalidated when:

1. **TTL Expires**: Entry exceeds ``time_to_live_seconds``
2. **Idle Timeout**: Entry not accessed within ``max_idle_seconds``
3. **Cluster Change**: Entry modified on cluster (if ``invalidate_on_change=True``)
4. **Eviction**: Cache reaches ``max_size``
5. **Manual Invalidation**: Programmatic invalidation

Manual Invalidation
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   cache = manager.get("users")
   
   # Invalidate single key
   cache.invalidate("user:1")
   
   # Invalidate multiple keys
   cache.invalidate_batch(["user:1", "user:2", "user:3"])
   
   # Invalidate all entries
   cache.invalidate_all()

Local Update Policy
-------------------

Configure how updates are handled:

.. code-block:: python

   from hazelcast.config import LocalUpdatePolicy

   near_cache = NearCacheConfig(
       name="users",
       local_update_policy=LocalUpdatePolicy.INVALIDATE,  # Default
   )
   
   # INVALIDATE: Remove from cache on local update (re-fetch on next read)
   # CACHE_ON_UPDATE: Update cache with new value immediately

Preloader
---------

Preload near cache on startup:

.. code-block:: python

   near_cache = NearCacheConfig(
       name="users",
       preloader_enabled=True,
       preloader_directory="/tmp/near-cache",
       preloader_store_initial_delay_seconds=600,
       preloader_store_interval_seconds=600,
   )

Best Practices
--------------

1. **Use for Read-Heavy Data**
   
   Near cache is most effective for frequently read, rarely modified data.

2. **Set Appropriate TTL**
   
   Balance freshness vs. performance with appropriate TTL values.

3. **Monitor Hit Ratio**
   
   A low hit ratio indicates the cache isn't effective. Consider:
   
   - Increasing ``max_size``
   - Increasing TTL
   - Checking access patterns

4. **Choose Format Based on Usage**
   
   - Use ``BINARY`` for large objects or low read frequency
   - Use ``OBJECT`` for small objects with high read frequency

5. **Enable Invalidation**
   
   Keep ``invalidate_on_change=True`` for data consistency.

6. **Size Appropriately**
   
   Set ``max_size`` based on available memory and working set size.
