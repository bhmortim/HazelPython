Getting Started
===============

This guide will help you get started with the Hazelcast Python Client.

Prerequisites
-------------

Before you begin, ensure you have:

* Python 3.8 or higher installed
* A running Hazelcast cluster (or use Docker to start one)

Starting a Hazelcast Cluster
----------------------------

The easiest way to start a Hazelcast cluster for development:

.. code-block:: bash

   docker run -d -p 5701:5701 hazelcast/hazelcast

Installation
------------

Install the client using pip:

.. code-block:: bash

   pip install hazelcast-python-client

For development with additional tools:

.. code-block:: bash

   pip install hazelcast-python-client[dev]


Basic Connection
----------------

Connect to a Hazelcast cluster using the context manager pattern:

.. code-block:: python

   from hazelcast import HazelcastClient, ClientConfig

   # Create configuration
   config = ClientConfig()
   config.cluster_name = "dev"
   config.cluster_members = ["localhost:5701"]

   # Connect using context manager (recommended)
   with HazelcastClient(config) as client:
       print(f"Connected to cluster: {config.cluster_name}")
       
       # Use distributed data structures
       my_map = client.get_map("my-map")
       my_map.put("greeting", "Hello, Hazelcast!")
       
       value = my_map.get("greeting")
       print(f"Retrieved: {value}")

   # Client automatically shuts down when exiting the context


Manual Connection
-----------------

If you need more control over the client lifecycle:

.. code-block:: python

   from hazelcast import HazelcastClient, ClientConfig

   config = ClientConfig()
   config.cluster_name = "dev"

   client = HazelcastClient(config)
   client.start()

   try:
       my_map = client.get_map("my-map")
       my_map.put("key", "value")
   finally:
       client.shutdown()


Async Usage
-----------

The client supports asynchronous operations:

.. code-block:: python

   import asyncio
   from hazelcast import HazelcastClient, ClientConfig

   async def main():
       config = ClientConfig()
       config.cluster_name = "dev"

       async with HazelcastClient(config) as client:
           my_map = client.get_map("my-map")
           
           # Async operations return futures
           await my_map.put_async("key", "value")
           future = my_map.get_async("key")
           value = await future
           print(f"Retrieved: {value}")

   asyncio.run(main())


Working with Maps
-----------------

The distributed Map is the most commonly used data structure:

.. code-block:: python

   with HazelcastClient(config) as client:
       users = client.get_map("users")
       
       # Basic CRUD operations
       users.put("user:1", {"name": "Alice", "age": 30})
       users.put("user:2", {"name": "Bob", "age": 25})
       
       # Get a single value
       user = users.get("user:1")
       print(f"User: {user}")
       
       # Check existence
       exists = users.contains_key("user:1")
       
       # Get multiple values
       entries = users.get_all({"user:1", "user:2"})
       
       # Conditional operations
       users.put_if_absent("user:3", {"name": "Charlie"})
       users.replace("user:1", {"name": "Alice", "age": 31})
       
       # Remove entries
       removed = users.remove("user:2")
       
       # Clear all entries
       users.clear()


Entry Listeners
---------------

Listen to map changes in real-time:

.. code-block:: python

   from hazelcast.proxy.map import EntryListener, EntryEvent

   class MyListener(EntryListener):
       def entry_added(self, event: EntryEvent) -> None:
           print(f"Added: {event.key} = {event.value}")
       
       def entry_removed(self, event: EntryEvent) -> None:
           print(f"Removed: {event.key}")
       
       def entry_updated(self, event: EntryEvent) -> None:
           print(f"Updated: {event.key}: {event.old_value} -> {event.value}")

   with HazelcastClient(config) as client:
       my_map = client.get_map("events")
       
       # Register listener
       listener = MyListener()
       reg_id = my_map.add_entry_listener(listener, include_value=True)
       
       # Operations will trigger listener callbacks
       my_map.put("key", "value")
       my_map.put("key", "new_value")
       my_map.remove("key")
       
       # Unregister when done
       my_map.remove_entry_listener(reg_id)


Error Handling
--------------

Handle common exceptions:

.. code-block:: python

   from hazelcast import (
       HazelcastClient,
       ClientConfig,
       ClientOfflineException,
       OperationTimeoutException,
       IllegalStateException,
   )

   config = ClientConfig()
   config.cluster_members = ["localhost:5701"]

   try:
       with HazelcastClient(config) as client:
           my_map = client.get_map("my-map")
           my_map.put("key", "value")
   except ClientOfflineException:
       print("Could not connect to the cluster")
   except OperationTimeoutException:
       print("Operation timed out")
   except IllegalStateException as e:
       print(f"Client is in an invalid state: {e}")


Next Steps
----------

* :doc:`configuration` - Learn about all configuration options
* :doc:`data_structures` - Explore all distributed data structures
* :doc:`transactions` - Use ACID transactions
* :doc:`cp_subsystem` - Use CP data structures for strong consistency
* :doc:`sql` - Execute SQL queries
* :doc:`jet` - Build stream processing pipelines
