hazelcast.cp.cp_map
===================

CP Map with strong consistency.

A ``CPMap`` is a distributed key-value store with linearizable (strongly
consistent) guarantees. Unlike the regular ``Map`` which uses AP (Available,
Partition-tolerant) semantics, ``CPMap`` uses CP (Consistent, Partition-tolerant)
semantics backed by the Raft consensus algorithm.

Use ``CPMap`` when you need guaranteed consistency for critical data like
configuration, metadata, or coordination state.

Usage Examples
--------------

Basic Operations
~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Get or create a CP Map
   cp_map = client.cp_subsystem.get_map("config")
   
   # Put a value
   cp_map.put("feature.enabled", True)
   
   # Get a value
   enabled = cp_map.get("feature.enabled")
   
   # Remove a value
   old = cp_map.remove("feature.enabled")
   
   # Check existence
   exists = cp_map.contains_key("feature.enabled")
   
   client.shutdown()

Atomic Operations
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Put if absent (atomic check-and-set)
   old = cp_map.put_if_absent("key", "value")
   if old is None:
       print("Key was absent, value was set")
   else:
       print(f"Key existed with value: {old}")
   
   # Set (unconditional put)
   cp_map.set("key", "new-value")
   
   # Compare and set
   success = cp_map.compare_and_set("key", "new-value", "updated-value")
   if success:
       print("Value was updated")
   else:
       print("Value was changed by another client")

Configuration Management
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Use CPMap for distributed configuration
   config = client.cp_subsystem.get_map("app-config")
   
   # Set configuration values
   config.put("db.host", "localhost")
   config.put("db.port", 5432)
   config.put("db.pool_size", 10)
   
   # Read configuration
   db_host = config.get("db.host")
   db_port = config.get("db.port")
   
   # Atomic configuration update
   if config.compare_and_set("db.pool_size", 10, 20):
       print("Pool size updated")

Leader Election Data
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Store leader election state
   election = client.cp_subsystem.get_map("leader-election")
   
   my_node_id = generate_node_id()
   
   # Try to become leader
   old_leader = election.put_if_absent("leader", my_node_id)
   if old_leader is None:
       print("I am the leader")
   else:
       print(f"Leader is: {old_leader}")
   
   # Leader updates last heartbeat
   election.put("leader:heartbeat", time.time())

Service Registry
~~~~~~~~~~~~~~~~

.. code-block:: python

   # Strongly consistent service registry
   registry = client.cp_subsystem.get_map("service-registry")
   
   # Register service
   service_info = {
       "host": "10.0.0.1",
       "port": 8080,
       "version": "1.2.3",
       "started_at": time.time()
   }
   registry.put("order-service:instance-1", service_info)
   
   # Discover service
   service = registry.get("order-service:instance-1")
   if service:
       connect_to_service(service["host"], service["port"])

CPMap vs Regular Map
--------------------

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Aspect
     - CPMap
     - Regular Map
   * - **Consistency**
     - Linearizable (strong)
     - Eventually consistent
   * - **Availability**
     - Requires CP quorum
     - Always available
   * - **Partition Behavior**
     - May be unavailable
     - Always accessible
   * - **Performance**
     - Slower (Raft consensus)
     - Faster
   * - **Capacity**
     - Limited (in-memory only)
     - Large (partitioned)
   * - **Use Case**
     - Config, metadata, coordination
     - Application data, caches

Configuration Options
---------------------

CP Subsystem configuration (cluster-side):

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Option
     - Default
     - Description
   * - ``cp_member_count``
     - 0
     - Number of CP members (0 = disabled)
   * - ``group_size``
     - Same as cp_member_count
     - Raft group size

Best Practices
--------------

1. **Use for Critical Data**: Reserve CPMap for data that requires
   strong consistency (config, metadata, coordination).

2. **Keep Data Small**: CPMap is not partitioned; all data lives on
   each CP member. Keep the dataset small.

3. **Handle Unavailability**: CPMap may be unavailable during network
   partitions. Implement retry logic and fallbacks.

4. **Use Regular Map for Scale**: For large datasets or high throughput,
   use the regular ``Map`` instead.

5. **Monitor CP Health**: Ensure the CP Subsystem has a healthy quorum
   for CPMap to be available.

API Reference
-------------

.. automodule:: hazelcast.cp.cp_map
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

CPMap
~~~~~

.. autoclass:: hazelcast.cp.cp_map.CPMap
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
