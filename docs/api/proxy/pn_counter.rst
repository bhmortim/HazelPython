hazelcast.proxy.pn_counter
==========================

Distributed PN Counter (CRDT counter) proxy.

A ``PNCounter`` (Positive-Negative Counter) is a CRDT (Conflict-free Replicated
Data Type) that provides eventually consistent counters across a distributed
cluster. It supports both increment and decrement operations that converge
to the correct value even under network partitions.

Key characteristics:

- **No coordination required**: Operations are local and fast
- **Eventually consistent**: All replicas converge to the same value
- **Partition tolerant**: Works correctly during network splits
- **High availability**: No single point of failure

Usage Examples
--------------

Basic Operations
~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Get or create a PN Counter
   counter = client.get_pn_counter("page-views")
   
   # Increment (returns new value)
   value = counter.increment_and_get()
   print(f"Views: {value}")
   
   # Decrement
   value = counter.decrement_and_get()
   
   # Get current value
   value = counter.get()
   
   # Add arbitrary amount
   value = counter.add_and_get(10)
   
   # Subtract arbitrary amount
   value = counter.add_and_get(-5)
   
   # Get then increment
   old_value = counter.get_and_increment()
   
   # Get then add
   old_value = counter.get_and_add(100)
   
   client.shutdown()

Web Analytics Counter
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Track page views across distributed servers
   views = client.get_pn_counter("page:home:views")
   
   # Each web server increments locally
   def on_page_view():
       views.increment_and_get()
   
   # Dashboard reads the counter
   def get_page_views():
       return views.get()

Distributed Rate Counter
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Track API requests across services
   api_calls = client.get_pn_counter("api:requests:today")
   
   # Middleware increments on each request
   def api_middleware(request):
       api_calls.increment_and_get()
       return handle_request(request)
   
   # Monitoring reads the count
   def get_request_count():
       return api_calls.get()

Inventory Counter
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Track inventory (supports both additions and removals)
   inventory = client.get_pn_counter("product:SKU123:stock")
   
   # Warehouse adds stock
   def add_inventory(quantity):
       return inventory.add_and_get(quantity)
   
   # Order processing removes stock
   def reserve_inventory(quantity):
       return inventory.add_and_get(-quantity)
   
   # Check current stock
   def get_stock():
       return inventory.get()

Understanding CRDT Convergence
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # PN Counter guarantees convergence even with concurrent operations
   # from different cluster members
   
   # Server A increments
   counter.increment_and_get()  # Returns 1
   
   # Server B (concurrently) increments
   counter.increment_and_get()  # Returns 1 (hasn't seen A's update yet)
   
   # After synchronization, both see 2
   # The counter converges to the correct value
   
   # This works because PN Counter tracks:
   # - P (Positive): increments per replica
   # - N (Negative): decrements per replica
   # Value = sum(P) - sum(N)

Vector Clock Access
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Access the vector clock for advanced use cases
   from hazelcast.proxy.pn_counter import VectorClock
   
   counter = client.get_pn_counter("my-counter")
   
   # The vector clock tracks causality
   # Useful for debugging or advanced consistency checks
   value = counter.get()
   
   # In some implementations, you can access the observed vector clock
   # to understand which replica updates have been incorporated

Configuration Options
---------------------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Option
     - Default
     - Description
   * - ``replica_count``
     - MAX_INT
     - Number of replicas to consult for reads
   * - ``statistics_enabled``
     - true
     - Enable statistics collection

Best Practices
--------------

1. **Use for Metrics**: PN Counters are ideal for metrics, analytics,
   and any counter that doesn't need strong consistency.

2. **Accept Eventual Consistency**: Reads may return slightly stale
   values. Design your application accordingly.

3. **Not for Inventory Constraints**: Don't use for hard inventory
   limits where overselling must be prevented. Use AtomicLong or
   transactions instead.

4. **Reset Requires Coordination**: There's no atomic reset. To reset,
   read the value and subtract it (may have races).

5. **Monitor Replica Lag**: In large clusters, some replicas may lag.
   Use ``replica_count`` to balance consistency vs. performance.

When to Use PN Counter vs. AtomicLong
-------------------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Aspect
     - PNCounter
     - AtomicLong (CP)
   * - **Consistency**
     - Eventually consistent
     - Strongly consistent (linearizable)
   * - **Availability**
     - Always available
     - Requires CP quorum
   * - **Performance**
     - Very fast (local operations)
     - Slower (Raft consensus)
   * - **Partition Behavior**
     - Works during partitions
     - May become unavailable
   * - **Use Case**
     - Metrics, analytics
     - Financial, inventory limits

API Reference
-------------

.. automodule:: hazelcast.proxy.pn_counter
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

PNCounterProxy
~~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.pn_counter.PNCounterProxy
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

VectorClock
~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.pn_counter.VectorClock
   :members:
   :undoc-members:
   :show-inheritance:
