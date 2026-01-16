Client API
==========

HazelcastClient
---------------

.. automodule:: hazelcast.client
   :members:
   :undoc-members:
   :show-inheritance:

Client States
~~~~~~~~~~~~~

The client goes through the following states during its lifecycle:

- **INITIAL**: Client created but not started
- **STARTING**: Client is connecting to the cluster
- **CONNECTED**: Client is connected and operational
- **DISCONNECTED**: Client lost connection to cluster
- **RECONNECTING**: Client is attempting to reconnect
- **SHUTTING_DOWN**: Client is shutting down
- **SHUTDOWN**: Client has shut down

Example
~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient, ClientConfig

   # Create configuration
   config = ClientConfig()
   config.cluster_name = "dev"
   config.cluster_members = ["localhost:5701"]

   # Create and start client
   client = HazelcastClient(config)
   client.start()

   # Check state
   print(f"Client state: {client.state}")
   print(f"Is running: {client.running}")

   # Get distributed objects
   my_map = client.get_map("my-map")
   my_queue = client.get_queue("my-queue")

   # Shutdown
   client.shutdown()
