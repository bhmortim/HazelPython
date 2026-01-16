API Reference
=============

This section contains the complete API documentation for the Hazelcast
Python Client.

.. toctree::
   :maxdepth: 2

   client
   config
   data_structures
   predicates
   aggregators
   serialization
   sql
   jet
   transactions
   cp
   listeners
   exceptions

Core Classes
------------

.. currentmodule:: hazelcast

HazelcastClient
~~~~~~~~~~~~~~~

.. autoclass:: HazelcastClient
   :members:
   :undoc-members:
   :show-inheritance:

ClientConfig
~~~~~~~~~~~~

.. autoclass:: hazelcast.config.ClientConfig
   :members:
   :undoc-members:
   :show-inheritance:

Quick Reference
---------------

**Client Lifecycle:**

.. code-block:: python

   from hazelcast import HazelcastClient, ClientConfig

   config = ClientConfig()
   client = HazelcastClient(config)
   client.start()
   # ... use client ...
   client.shutdown()

**Data Structures:**

.. code-block:: python

   # Map
   my_map = client.get_map("my-map")
   
   # Queue
   queue = client.get_queue("my-queue")
   
   # Set
   my_set = client.get_set("my-set")
   
   # List
   my_list = client.get_list("my-list")
   
   # MultiMap
   multi_map = client.get_multi_map("my-multimap")
   
   # ReplicatedMap
   rep_map = client.get_replicated_map("my-rep-map")
   
   # Ringbuffer
   ringbuffer = client.get_ringbuffer("my-ringbuffer")
   
   # Topic
   topic = client.get_topic("my-topic")

**CP Subsystem:**

.. code-block:: python

   # AtomicLong
   counter = client.get_atomic_long("my-counter")
   
   # AtomicReference
   ref = client.get_atomic_reference("my-ref")
   
   # FencedLock
   lock = client.get_fenced_lock("my-lock")
   
   # Semaphore
   semaphore = client.get_semaphore("my-semaphore")
   
   # CountDownLatch
   latch = client.get_count_down_latch("my-latch")

**Services:**

.. code-block:: python

   # SQL
   sql = client.get_sql()
   
   # Jet
   jet = client.get_jet()
   
   # Transactions
   ctx = client.new_transaction_context()
