HazelPython Documentation
=========================

HazelPython is the official Python client for `Hazelcast <https://hazelcast.com/>`_,
an open-source distributed in-memory data store and computation platform.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   api/index

Features
--------

* **Distributed Data Structures**: Maps, Queues, Sets, Lists, MultiMaps, and more
* **CP Subsystem**: Strongly consistent primitives (AtomicLong, FencedLock, Semaphore)
* **SQL Queries**: Execute SQL queries against distributed data
* **Jet Pipelines**: Build and execute distributed stream processing pipelines
* **Near Cache**: Client-side caching for reduced latency
* **Event Listeners**: React to data changes in real-time

Quick Start
-----------

.. code-block:: python

   from hazelcast import HazelcastClient, ClientConfig

   config = ClientConfig()
   config.cluster_name = "dev"

   with HazelcastClient(config) as client:
       my_map = client.get_map("my-map")
       my_map.put("key", "value")
       value = my_map.get("key")
       print(f"Value: {value}")

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
