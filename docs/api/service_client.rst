Client Service
==============

.. module:: hazelcast.service.client_service
   :synopsis: Client service for connection monitoring and management.

This module provides services for monitoring and managing client
connections to the Hazelcast cluster.

Enumerations
------------

.. autoclass:: ConnectionStatus
   :members:
   :undoc-members:

Listener Interfaces
-------------------

.. autoclass:: ClientConnectionListener
   :members:

.. autoclass:: FunctionConnectionListener
   :members:
   :special-members: __init__

Data Classes
------------

.. autoclass:: ConnectionInfo
   :members:

.. autoclass:: ClusterInfo
   :members:

Client Service
--------------

.. autoclass:: ClientService
   :members:
   :special-members: __init__

Example Usage
-------------

Using the client service::

    from hazelcast.service.client_service import ClientService, ConnectionStatus

    service = ClientService()

    # Check connection status
    if service.is_connected:
        print(f"Connected to {service.cluster_info.cluster_name}")

    # Get active connections
    connections = service.get_connections()
    print(f"Active connections: {len(connections)}")

Adding connection listeners::

    from hazelcast.service.client_service import FunctionConnectionListener

    listener = FunctionConnectionListener(
        on_opened=lambda c: print(f"Connected to {c.remote_address}"),
        on_closed=lambda c, r: print(f"Disconnected: {r}")
    )
    registration_id = service.add_connection_listener(listener)
