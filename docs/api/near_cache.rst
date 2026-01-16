Near Cache
==========

.. module:: hazelcast.near_cache
   :synopsis: Near Cache implementation for Hazelcast client.

This module provides a client-side cache that stores frequently accessed
data locally, reducing network round-trips to the cluster.

Statistics
----------

.. autoclass:: NearCacheStats
   :members:

Cache Records
-------------

.. autoclass:: NearCacheRecord
   :members:

Eviction Strategies
-------------------

.. autoclass:: EvictionStrategy
   :members:

.. autoclass:: LRUEvictionStrategy
   :members:

.. autoclass:: LFUEvictionStrategy
   :members:

.. autoclass:: RandomEvictionStrategy
   :members:

.. autoclass:: NoneEvictionStrategy
   :members:

Near Cache
----------

.. autoclass:: NearCache
   :members:
   :special-members: __init__

Near Cache Manager
------------------

.. autoclass:: NearCacheManager
   :members:
   :special-members: __init__

Example Usage
-------------

Configure a near cache::

    from hazelcast.config import NearCacheConfig, EvictionPolicy

    config = NearCacheConfig(
        name="my-map",
        max_size=10000,
        time_to_live_seconds=300,
        eviction_policy=EvictionPolicy.LRU
    )

Using the NearCacheManager::

    from hazelcast.near_cache import NearCacheManager, NearCacheConfig

    manager = NearCacheManager()
    config = NearCacheConfig(name="users", max_size=1000)
    cache = manager.get_or_create("users", config)

    cache.put("user:1", user_data)
    data = cache.get("user:1")  # Local lookup
