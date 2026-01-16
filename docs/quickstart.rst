Quick Start Guide
=================

This guide helps you get started with the Hazelcast Python Client in minutes.

Installation
------------

Install the Hazelcast Python Client using pip:

.. code-block:: bash

    pip install hazelcast-python-client

Basic Usage
-----------

Connect to a Cluster
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from hazelcast import HazelcastClient, ClientConfig

    # Create configuration
    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    # Connect to cluster
    with HazelcastClient(config) as client:
        print(f"Connected: {client.running}")

Using Distributed Map
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from hazelcast import HazelcastClient

    with HazelcastClient() as client:
        # Get or create a distributed map
        my_map = client.get_map("my-distributed-map")

        # Put and get values
        my_map.put("key1", "value1")
        value = my_map.get("key1")
        print(f"Retrieved: {value}")

        # Use Python dict-like syntax
        my_map["key2"] = {"name": "Alice", "age": 30}
        user = my_map["key2"]

        # Check size
        print(f"Map size: {len(my_map)}")

Using Distributed Queue
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    with HazelcastClient() as client:
        queue = client.get_queue("task-queue")

        # Add items
        queue.offer("task-1")
        queue.offer("task-2")

        # Poll items (with optional timeout)
        task = queue.poll(timeout=5.0)
        print(f"Processing: {task}")

Asynchronous Operations
~~~~~~~~~~~~~~~~~~~~~~~

All operations have async variants:

.. code-block:: python

    import asyncio
    from hazelcast import HazelcastClient

    async def main():
        async with HazelcastClient() as client:
            my_map = client.get_map("async-map")

            # Async put
            await my_map.put_async("key", "value")

            # Async get
            future = my_map.get_async("key")
            value = await future

    asyncio.run(main())

Using Entry Listeners
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from hazelcast import HazelcastClient
    from hazelcast.proxy.map import EntryListener, EntryEvent

    class MyListener(EntryListener):
        def entry_added(self, event: EntryEvent) -> None:
            print(f"Added: {event.key} = {event.value}")

        def entry_removed(self, event: EntryEvent) -> None:
            print(f"Removed: {event.key}")

    with HazelcastClient() as client:
        my_map = client.get_map("listened-map")

        # Register listener
        reg_id = my_map.add_entry_listener(MyListener(), include_value=True)

        # Operations trigger listener
        my_map.put("key", "value")
        my_map.remove("key")

        # Unregister when done
        my_map.remove_entry_listener(reg_id)

Next Steps
----------

- :doc:`user_guide/client_configuration` - Detailed configuration options
- :doc:`user_guide/data_structures` - All distributed data structures
- :doc:`user_guide/sql_queries` - SQL query capabilities
- :doc:`user_guide/jet_pipelines` - Stream processing with Jet
