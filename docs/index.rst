Hazelcast Python Client Documentation
=====================================

Welcome to the **Hazelcast Python Client** documentation. This client provides
a Pythonic interface to Hazelcast, the real-time data platform for
low-latency stream processing and fast data at any scale.

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   getting_started
   installation

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   configuration
   data_structures
   cp_subsystem
   sql_jet
   near_cache
   transactions
   serialization

.. toctree::
   :maxdepth: 2
   :caption: Advanced Topics

   entry_processors
   predicates_aggregators
   listeners
   discovery

.. toctree::
   :maxdepth: 2
   :caption: Migration

   migration

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/index

Features
--------

* **Distributed Data Structures**: Maps, Queues, Sets, Lists, MultiMaps, and more
* **CP Subsystem**: Strongly consistent primitives (AtomicLong, FencedLock, Semaphore)
* **SQL Queries**: Execute SQL queries against cluster data
* **Jet Pipelines**: Stream and batch processing with Jet
* **Near Cache**: Client-side caching for reduced latency
* **Transactions**: ACID transactions across data structures
* **Cloud Discovery**: AWS, Azure, GCP, Kubernetes, and Hazelcast Cloud support

Quick Example
-------------

.. code-block:: python

   from hazelcast import HazelcastClient, ClientConfig

   # Connect to cluster
   config = ClientConfig()
   config.cluster_name = "dev"
   
   with HazelcastClient(config) as client:
       # Get a distributed map
       my_map = client.get_map("my-map")
       
       # Perform operations
       my_map.put("key", "value")
       value = my_map.get("key")
       print(f"Value: {value}")

Indices and Tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
