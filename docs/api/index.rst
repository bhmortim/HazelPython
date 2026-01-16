API Reference
=============

Complete API reference for the Hazelcast Python Client.

.. toctree::
   :maxdepth: 2

   client
   config
   data_structures
   cp
   sql
   jet
   transactions
   predicates
   aggregators
   serialization
   listeners
   exceptions


Module Index
------------

Core
~~~~

* :mod:`hazelcast` - Main package exports
* :mod:`hazelcast.client` - HazelcastClient
* :mod:`hazelcast.config` - Configuration classes

Data Structures
~~~~~~~~~~~~~~~

* :mod:`hazelcast.proxy.map` - Map
* :mod:`hazelcast.proxy.queue` - Queue
* :mod:`hazelcast.proxy.collections` - Set, List
* :mod:`hazelcast.proxy.multi_map` - MultiMap
* :mod:`hazelcast.proxy.replicated_map` - ReplicatedMap
* :mod:`hazelcast.proxy.ringbuffer` - Ringbuffer
* :mod:`hazelcast.proxy.topic` - Topic
* :mod:`hazelcast.proxy.reliable_topic` - ReliableTopic
* :mod:`hazelcast.proxy.pn_counter` - PNCounter
* :mod:`hazelcast.proxy.cardinality_estimator` - CardinalityEstimator
* :mod:`hazelcast.proxy.executor` - IExecutorService

CP Subsystem
~~~~~~~~~~~~

* :mod:`hazelcast.cp.atomic` - AtomicLong, AtomicReference
* :mod:`hazelcast.cp.sync` - FencedLock, Semaphore, CountDownLatch

SQL
~~~

* :mod:`hazelcast.sql.service` - SqlService
* :mod:`hazelcast.sql.statement` - SqlStatement

Jet
~~~

* :mod:`hazelcast.jet.service` - JetService
* :mod:`hazelcast.jet` - Pipeline, Job, JobConfig

Transactions
~~~~~~~~~~~~

* :mod:`hazelcast.transaction` - TransactionContext, TransactionOptions

Query
~~~~~

* :mod:`hazelcast.predicate` - Predicates
* :mod:`hazelcast.aggregator` - Aggregators
* :mod:`hazelcast.projection` - Projections

Other
~~~~~

* :mod:`hazelcast.exceptions` - Exception classes
* :mod:`hazelcast.listener` - Event listeners
* :mod:`hazelcast.failover` - Failover configuration
* :mod:`hazelcast.near_cache` - Near cache
