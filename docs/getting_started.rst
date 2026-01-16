Getting Started
===============

This guide walks you through connecting to a Hazelcast cluster and performing
basic operations with the Python client.

Connecting to a Cluster
-----------------------

The simplest way to connect is using default settings:

.. code-block:: python

   from hazelcast import HazelcastClient

   # Connect to localhost:5701
   client = HazelcastClient()
   
   # Use the client...
   
   # Always shutdown when done
   client.shutdown()

Using Context Manager
~~~~~~~~~~~~~~~~~~~~~

The recommended approach uses a context manager for automatic cleanup:

.. code-block:: python

   from hazelcast import HazelcastClient, ClientConfig

   config = ClientConfig()
   config.cluster_name = "dev"
   config.cluster_members = ["localhost:5701"]

   with HazelcastClient(config) as client:
       # Client automatically shuts down on exit
       my_map = client.get_map("my-map")
       my_map.put("greeting", "Hello, Hazelcast!")

Configuring the Client
----------------------

Use ``ClientConfig`` to customize connection settings:

.. code-block:: python

   from hazelcast import ClientConfig, HazelcastClient

   config = ClientConfig()
   
   # Cluster settings
   config.cluster_name = "production"
   config.cluster_members = [
       "server1.example.com:5701",
       "server2.example.com:5701",
       "server3.example.com:5701",
   ]
   
   # Connection settings
   config.connection_timeout = 10.0  # seconds
   config.smart_routing = True
   
   # Optional client name and labels
   config.client_name = "my-python-app"
   config.labels = ["env:production", "app:orders"]

   client = HazelcastClient(config)

Working with Maps
-----------------

Maps are the most commonly used data structure:

.. code-block:: python

   with HazelcastClient(config) as client:
       users = client.get_map("users")
       
       # Basic operations
       users.put("user:1", {"name": "Alice", "age": 30})
       users.put("user:2", {"name": "Bob", "age": 25})
       
       # Get a value
       user = users.get("user:1")
       print(f"User: {user}")
       
       # Check existence
       if users.contains_key("user:1"):
           print("User exists!")
       
       # Get all keys
       keys = users.key_set()
       print(f"All keys: {keys}")
       
       # Remove a value
       old_value = users.remove("user:2")

Async Operations
----------------

All operations have async variants for non-blocking execution:

.. code-block:: python

   import asyncio
   from hazelcast import HazelcastClient, ClientConfig

   async def main():
       config = ClientConfig()
       
       async with HazelcastClient(config) as client:
           my_map = client.get_map("async-map")
           
           # Async put
           await my_map.put_async("key", "value")
           
           # Async get
           value = await my_map.get_async("key")
           print(f"Value: {value}")

   asyncio.run(main())

Using Listeners
---------------

Register listeners to react to cluster events:

.. code-block:: python

   from hazelcast import HazelcastClient, ClientConfig
   from hazelcast.listener import LifecycleEvent

   def on_lifecycle_change(event: LifecycleEvent):
       print(f"Client state: {event.state.name}")

   config = ClientConfig()
   client = HazelcastClient(config)
   
   # Add lifecycle listener
   reg_id = client.add_lifecycle_listener(
       on_state_changed=on_lifecycle_change
   )
   
   # Remove listener when done
   client.remove_listener(reg_id)

Error Handling
--------------

Handle common exceptions appropriately:

.. code-block:: python

   from hazelcast import HazelcastClient, ClientConfig
   from hazelcast.exceptions import (
       ClientOfflineException,
       TimeoutException,
       HazelcastException,
   )

   config = ClientConfig()
   
   try:
       with HazelcastClient(config) as client:
           my_map = client.get_map("my-map")
           value = my_map.get("key")
   except ClientOfflineException:
       print("Cannot connect to cluster")
   except TimeoutException:
       print("Operation timed out")
   except HazelcastException as e:
       print(f"Hazelcast error: {e}")

Next Steps
----------

* :doc:`configuration` - Detailed configuration options
* :doc:`data_structures` - Working with distributed data structures
* :doc:`sql_jet` - SQL queries and stream processing
* :doc:`transactions` - ACID transactions
