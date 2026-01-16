hazelcast.proxy
===============

The ``hazelcast.proxy`` package contains distributed data structure proxies
that provide the client-side API for interacting with Hazelcast cluster data.

.. toctree::
   :maxdepth: 2
   :caption: Data Structures:

   map
   multi_map
   collections
   queue
   topic
   reliable_topic
   ringbuffer
   replicated_map
   cache
   pn_counter
   cardinality_estimator
   flake_id_generator

Package Overview
----------------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Module
     - Description
   * - :doc:`map`
     - Distributed key-value Map with rich querying and indexing
   * - :doc:`multi_map`
     - Map allowing multiple values per key
   * - :doc:`collections`
     - Distributed Set and List collections
   * - :doc:`queue`
     - Distributed FIFO queue with blocking operations
   * - :doc:`topic`
     - Publish-subscribe messaging (best-effort delivery)
   * - :doc:`reliable_topic`
     - Publish-subscribe with ordering and replay guarantees
   * - :doc:`ringbuffer`
     - Fixed-capacity circular buffer with sequence access
   * - :doc:`replicated_map`
     - Fully replicated Map for small, read-heavy datasets
   * - :doc:`cache`
     - JCache (JSR-107) compliant distributed cache
   * - :doc:`pn_counter`
     - CRDT counter for eventually consistent counting
   * - :doc:`cardinality_estimator`
     - HyperLogLog-based distinct element counter
   * - :doc:`flake_id_generator`
     - Cluster-wide unique ID generator

Choosing the Right Data Structure
---------------------------------

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Need
     - Recommended
     - Alternative
   * - Key-value storage
     - :doc:`map`
     - :doc:`replicated_map` (small, read-heavy)
   * - Multiple values per key
     - :doc:`multi_map`
     - Map with List values
   * - Unique elements
     - Set (:doc:`collections`)
     - Map with null values
   * - Ordered elements
     - List (:doc:`collections`)
     - :doc:`queue` (FIFO only)
   * - Producer-consumer
     - :doc:`queue`
     - :doc:`ringbuffer` (with replay)
   * - Pub-sub messaging
     - :doc:`topic`
     - :doc:`reliable_topic` (ordered)
   * - Event streaming
     - :doc:`ringbuffer`
     - :doc:`reliable_topic`
   * - Counting
     - :doc:`pn_counter`
     - CP AtomicLong (strong consistency)
   * - Cardinality estimation
     - :doc:`cardinality_estimator`
     - Set (exact, more memory)
   * - Unique IDs
     - :doc:`flake_id_generator`
     - CP AtomicLong (sequential)
   * - Caching with TTL
     - :doc:`cache`
     - :doc:`map` with TTL

API Reference
-------------

.. automodule:: hazelcast.proxy
   :members:
   :undoc-members:
   :show-inheritance:
