hazelcast.cp.atomic
===================

Atomic data structures with strong consistency.

The CP Subsystem provides strongly consistent (linearizable) atomic operations
using the Raft consensus algorithm. These primitives guarantee that operations
appear to execute instantaneously and in a total order.

AtomicLong
----------

A distributed, linearizable 64-bit counter. Unlike ``PNCounter``, ``AtomicLong``
provides strong consistency at the cost of availability during network partitions.

Usage Examples
~~~~~~~~~~~~~~

Basic Operations
^^^^^^^^^^^^^^^^

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Get or create an AtomicLong
   counter = client.cp_subsystem.get_atomic_long("counter")
   
   # Set value
   counter.set(0)
   
   # Increment/Decrement
   value = counter.increment_and_get()  # Returns 1
   value = counter.decrement_and_get()  # Returns 0
   
   # Get then modify
   old = counter.get_and_increment()  # Returns 0, counter is 1
   old = counter.get_and_add(10)      # Returns 1, counter is 11
   
   # Add arbitrary amount
   value = counter.add_and_get(5)     # Returns 16
   
   # Get current value
   current = counter.get()            # Returns 16
   
   client.shutdown()

Compare and Set
^^^^^^^^^^^^^^^

.. code-block:: python

   # Atomic compare-and-set
   success = counter.compare_and_set(16, 20)  # True, counter is 20
   success = counter.compare_and_set(16, 25)  # False, counter is still 20
   
   # Implement optimistic locking
   while True:
       current = counter.get()
       new_value = compute_new_value(current)
       if counter.compare_and_set(current, new_value):
           break

Sequence Generation
^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Generate unique sequence numbers
   sequence_gen = client.cp_subsystem.get_atomic_long("sequence-generator")
   
   def next_id():
       return sequence_gen.increment_and_get()
   
   id1 = next_id()  # 1
   id2 = next_id()  # 2
   id3 = next_id()  # 3

AtomicReference
---------------

A distributed, linearizable reference that holds any serializable object.
Provides atomic read, write, and compare-and-set operations on object references.

Usage Examples
~~~~~~~~~~~~~~

Basic Operations
^^^^^^^^^^^^^^^^

.. code-block:: python

   # Get or create an AtomicReference
   ref = client.cp_subsystem.get_atomic_reference("config")
   
   # Set value
   ref.set({"version": 1, "feature_flags": ["beta"]})
   
   # Get current value
   config = ref.get()
   
   # Check if null/None
   is_null = ref.is_null()
   
   # Set and return old value
   old = ref.get_and_set({"version": 2, "feature_flags": ["beta", "gamma"]})
   
   # Clear (set to None)
   ref.clear()

Compare and Set
^^^^^^^^^^^^^^^

.. code-block:: python

   # Atomic compare-and-set for object references
   current = ref.get()
   new_config = {**current, "version": current["version"] + 1}
   
   success = ref.compare_and_set(current, new_config)
   if success:
       print("Config updated")
   else:
       print("Concurrent modification detected")

Distributed Singleton
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Use AtomicReference for distributed singleton pattern
   leader_ref = client.cp_subsystem.get_atomic_reference("leader")
   
   my_id = generate_unique_id()
   
   # Try to become leader
   if leader_ref.compare_and_set(None, my_id):
       print("I am the leader")
       run_as_leader()
   else:
       leader = leader_ref.get()
       print(f"Leader is {leader}")

Configuration Options
---------------------

CP Subsystem must be enabled on the cluster. Configure via cluster-side settings:

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
   * - ``session_ttl_seconds``
     - 300
     - Session timeout
   * - ``session_heartbeat_interval_seconds``
     - 5
     - Session heartbeat interval

Best Practices
--------------

1. **Use for Strong Consistency**: When you need linearizable operations
   that must appear atomic across the cluster.

2. **Accept Availability Trade-off**: CP primitives may be unavailable
   during network partitions (CAP theorem).

3. **Use PNCounter for Metrics**: For counters that don't need strong
   consistency, use ``PNCounter`` for better availability.

4. **Keep References Small**: AtomicReference stores the entire object.
   For large objects, store a key and use a Map.

5. **Handle CP Exceptions**: Be prepared to handle ``CPGroupDestroyedException``
   and ``CPSubsystemException``.

API Reference
-------------

.. automodule:: hazelcast.cp.atomic
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

AtomicLong
~~~~~~~~~~

.. autoclass:: hazelcast.cp.atomic.AtomicLong
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

AtomicReference
~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.cp.atomic.AtomicReference
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
