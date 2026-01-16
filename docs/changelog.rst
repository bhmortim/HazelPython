Changelog
=========

All notable changes to the Hazelcast Python Client are documented here.

Version 0.1.0 (Initial Release)
-------------------------------

Features
~~~~~~~~

* **Core Client**

  - HazelcastClient with synchronous and asynchronous APIs
  - Context manager support for automatic resource cleanup
  - Lifecycle and membership event listeners
  - Configurable connection strategy with exponential backoff

* **Distributed Data Structures**

  - Map (IMap) with entry listeners, TTL, and predicates
  - ReplicatedMap for fully replicated data
  - MultiMap for multiple values per key
  - Queue with blocking operations
  - Set and List collections
  - Ringbuffer with sequence-based access
  - Topic and ReliableTopic for pub/sub messaging
  - PNCounter (CRDT counter)
  - CardinalityEstimator (HyperLogLog)
  - FlakeIdGenerator for distributed unique ID generation
  - IExecutorService for distributed task execution

* **CP Subsystem**

  - AtomicLong and AtomicReference
  - FencedLock with fencing tokens
  - Semaphore for distributed permits
  - CountDownLatch for synchronization

* **SQL Service**

  - Execute SQL queries against cluster data
  - Parameterized queries
  - Result iteration and metadata

* **Jet Integration**

  - Pipeline building API
  - Job submission and management
  - Built-in sources and sinks

* **Transactions**

  - ACID transactions across data structures
  - ONE_PHASE and TWO_PHASE commit types
  - TransactionalMap, Set, List, Queue, MultiMap

* **Configuration**

  - Programmatic configuration via ClientConfig
  - YAML configuration file support
  - Near cache configuration
  - Security configuration (username/password, token)
  - Connection strategy with retry settings

* **Query Support**

  - Predicates for filtering (SQL, comparison, compound)
  - Aggregators (count, sum, avg, min, max, distinct)
  - Projections for attribute extraction

* **Near Cache**

  - Local caching with LRU/LFU eviction
  - TTL and max-idle expiration
  - Server-side invalidation

* **Failover**

  - Multi-cluster failover support
  - CNAME-based cluster discovery
  - Priority-based cluster selection

* **Entry Processors**

  - Execute code on map entries server-side
  - executeOnKey and executeOnKeys operations
