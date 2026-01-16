Hazelcast Python Client Documentation
=====================================

Welcome to the Hazelcast Python Client documentation. This client provides
a comprehensive API for connecting to Hazelcast in-memory data grid clusters
from Python applications.

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   getting_started
   configuration
   data_structures
   transactions
   cp_subsystem
   sql
   jet
   near_cache
   failover

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/index
   api/client
   api/config
   api/data_structures
   api/cp
   api/sql
   api/jet
   api/transactions
   api/predicates
   api/aggregators
   api/serialization
   api/listeners
   api/exceptions

.. toctree::
   :maxdepth: 1
   :caption: Additional Resources

   changelog
   contributing


Features
--------

* **Distributed Data Structures**: Map, Queue, Set, List, Topic, MultiMap,
  ReplicatedMap, Ringbuffer, PN Counter, Cardinality Estimator, FlakeIdGenerator
* **SQL Support**: Execute SQL queries against cluster data
* **CP Subsystem**: AtomicLong, AtomicReference, FencedLock, Semaphore, CountDownLatch
* **Jet Integration**: Submit and manage streaming/batch processing pipelines
* **Transactions**: ACID transactions across multiple data structures
* **Near Cache**: Local caching with configurable eviction and TTL
* **Failover**: Automatic failover across multiple clusters
* **Async Support**: Both synchronous and asynchronous APIs


Quick Example
-------------

.. code-block:: python

   from hazelcast import HazelcastClient, ClientConfig

   config = ClientConfig()
   config.cluster_name = "dev"
   config.cluster_members = ["localhost:5701"]

   with HazelcastClient(config) as client:
       my_map = client.get_map("my-map")
       my_map.put("key", "value")
       value = my_map.get("key")
       print(f"Retrieved: {value}")


Installation
------------

.. code-block:: bash

   pip install hazelcast-python-client


Requirements
------------

* Python 3.8 or higher
* A running Hazelcast cluster (5.x recommended)


Indices and Tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
