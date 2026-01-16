hazelcast.proxy.flake_id_generator
==================================

Distributed Flake ID Generator proxy.

A ``FlakeIdGenerator`` generates cluster-wide unique, roughly time-ordered
64-bit IDs. IDs are composed of:

- **Timestamp** (41 bits): Milliseconds since a configurable epoch
- **Node ID** (10 bits): Identifies the cluster member
- **Sequence** (13 bits): Per-member sequence number

This design ensures IDs are unique across the cluster without coordination,
while maintaining rough chronological ordering.

Usage Examples
--------------

Basic Usage
~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Get or create a FlakeIdGenerator
   id_gen = client.get_flake_id_generator("order-ids")
   
   # Generate a new unique ID
   order_id = id_gen.new_id()
   print(f"New order ID: {order_id}")
   
   # Generate multiple IDs
   for _ in range(10):
       print(id_gen.new_id())
   
   client.shutdown()

Database Primary Keys
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Use for distributed primary key generation
   id_gen = client.get_flake_id_generator("user-ids")
   
   def create_user(name, email):
       user_id = id_gen.new_id()
       user = {
           "id": user_id,
           "name": name,
           "email": email,
           "created_at": time.time()
       }
       users_map.put(user_id, user)
       return user

Event Tracking
~~~~~~~~~~~~~~

.. code-block:: python

   # Generate unique event IDs
   event_id_gen = client.get_flake_id_generator("event-ids")
   
   def log_event(event_type, data):
       event_id = event_id_gen.new_id()
       event = {
           "id": event_id,
           "type": event_type,
           "data": data,
           "timestamp": time.time()
       }
       events_topic.publish(event)
       return event_id

Batch ID Generation
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Generate IDs in batches for efficiency
   id_gen = client.get_flake_id_generator("batch-ids")
   
   def create_batch(items):
       results = []
       for item in items:
           item_id = id_gen.new_id()
           results.append({"id": item_id, **item})
       return results

ID Properties
-------------

Flake IDs have several useful properties:

.. code-block:: python

   id1 = id_gen.new_id()
   time.sleep(0.001)
   id2 = id_gen.new_id()
   
   # IDs are roughly time-ordered
   assert id2 > id1  # Usually true (same node)
   
   # IDs are 64-bit positive integers
   assert 0 < id1 < 2**63
   
   # IDs are unique across the cluster
   # (guaranteed by node ID component)

Configuration Options
---------------------

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Option
     - Default
     - Description
   * - ``prefetch_count``
     - 100
     - Number of IDs to prefetch from cluster
   * - ``prefetch_validity_ms``
     - 600000
     - Validity period for prefetched IDs
   * - ``epoch_start``
     - 1514764800000
     - Custom epoch (milliseconds since Unix epoch)
   * - ``node_id_offset``
     - 0
     - Offset added to node IDs
   * - ``bits_sequence``
     - 6
     - Bits allocated to sequence number
   * - ``bits_node_id``
     - 16
     - Bits allocated to node ID

Best Practices
--------------

1. **Use for Primary Keys**: FlakeIdGenerator is ideal for generating
   database-style primary keys without coordination.

2. **Understand Time Ordering**: IDs are roughly ordered but not strictly
   monotonic across nodes. Don't rely on strict ordering.

3. **Handle Clock Skew**: Large clock differences between nodes can
   affect ordering. Use NTP to synchronize clocks.

4. **Consider Prefetch Settings**: Adjust ``prefetch_count`` based on
   your ID generation rate to balance performance and freshness.

5. **One Generator Per Entity Type**: Create separate generators for
   different entity types (users, orders, events).

When to Use FlakeIdGenerator
----------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Use Case
     - Recommendation
   * - Primary keys
     - ✅ Ideal
   * - Event IDs
     - ✅ Good
   * - Strict ordering
     - ❌ Use AtomicLong
   * - Human-readable IDs
     - ❌ Use custom scheme
   * - Sparse IDs
     - ✅ Good (64-bit space)

API Reference
-------------

.. automodule:: hazelcast.proxy.flake_id_generator
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

FlakeIdGenerator
~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.flake_id_generator.FlakeIdGenerator
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
