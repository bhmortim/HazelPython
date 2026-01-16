hazelcast.proxy.collections
===========================

Distributed Set and List data structure proxies.

Set
---

A distributed ``Set`` is an unordered collection that does not allow duplicate
elements. It provides constant-time performance for basic operations like
``add``, ``remove``, and ``contains``.

List
----

A distributed ``List`` is an ordered collection that allows duplicate elements.
Elements can be accessed by their integer index. Lists maintain insertion order
and support positional access.

Usage Examples
--------------

Set Operations
~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Get or create a distributed Set
   my_set = client.get_set("unique-users")
   
   # Add elements
   my_set.add("user:alice")
   my_set.add("user:bob")
   my_set.add("user:alice")  # Duplicate, not added
   
   print(my_set.size())  # 2
   
   # Check membership
   exists = my_set.contains("user:alice")  # True
   exists = "user:alice" in my_set  # Pythonic alternative
   
   # Remove element
   removed = my_set.remove("user:bob")
   
   # Iterate over elements
   for user in my_set:
       print(user)
   
   # Bulk operations
   my_set.add_all(["user:charlie", "user:dave"])
   my_set.remove_all(["user:alice", "user:charlie"])
   
   # Get all elements
   all_users = my_set.get_all()
   
   client.shutdown()

List Operations
~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Get or create a distributed List
   my_list = client.get_list("task-queue")
   
   # Add elements
   my_list.add("task-1")
   my_list.add("task-2")
   my_list.add("task-1")  # Duplicates allowed
   
   print(my_list.size())  # 3
   
   # Positional access
   first = my_list.get(0)
   my_list.set(0, "updated-task-1")
   
   # Pythonic access
   first = my_list[0]
   my_list[0] = "updated-task-1"
   del my_list[2]
   
   # Insert at position
   my_list.add_at(1, "priority-task")
   
   # Find index
   index = my_list.index_of("task-2")
   last_index = my_list.last_index_of("task-2")
   
   # Sublist
   sub = my_list.sub_list(0, 2)
   
   # Iterate
   for task in my_list:
       print(task)
   
   client.shutdown()

Set Listeners
~~~~~~~~~~~~~

.. code-block:: python

   def on_item_added(event):
       print(f"Added: {event.item}")
   
   def on_item_removed(event):
       print(f"Removed: {event.item}")
   
   listener = ItemListener(
       added=on_item_added,
       removed=on_item_removed,
   )
   
   reg_id = my_set.add_listener(listener, include_value=True)
   
   # Later: remove listener
   my_set.remove_listener(reg_id)

List Listeners
~~~~~~~~~~~~~~

.. code-block:: python

   listener = ItemListener(
       added=on_item_added,
       removed=on_item_removed,
   )
   
   reg_id = my_list.add_listener(listener, include_value=True)
   my_list.remove_listener(reg_id)

Common Patterns
~~~~~~~~~~~~~~~

.. code-block:: python

   # Set: Tracking unique visitors
   visitors = client.get_set("daily-visitors")
   visitors.add(user_id)
   unique_count = visitors.size()
   
   # Set: Deduplication
   processed_ids = client.get_set("processed-orders")
   if not processed_ids.contains(order_id):
       process_order(order_id)
       processed_ids.add(order_id)
   
   # List: Task queue (simple FIFO)
   tasks = client.get_list("pending-tasks")
   tasks.add(new_task)
   if tasks.size() > 0:
       next_task = tasks.remove_at(0)
   
   # List: Maintaining history
   history = client.get_list("user-activity")
   history.add({"action": "login", "timestamp": time.time()})
   recent = history.sub_list(max(0, history.size() - 10), history.size())

Configuration Options
---------------------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Option
     - Default
     - Description
   * - ``backup_count``
     - 1
     - Number of synchronous backups
   * - ``async_backup_count``
     - 0
     - Number of asynchronous backups
   * - ``max_size``
     - 0
     - Maximum collection size (0 = unlimited)
   * - ``statistics_enabled``
     - true
     - Enable statistics collection

Best Practices
--------------

1. **Set vs List**: Use ``Set`` when uniqueness matters and order doesn't.
   Use ``List`` when you need ordering or allow duplicates.

2. **Avoid Large Lists**: Large lists can cause memory issues. Consider
   using ``Queue`` for producer-consumer patterns or ``Map`` for indexed access.

3. **Use Bulk Operations**: Prefer ``add_all()`` over multiple ``add()``
   calls to reduce network round-trips.

4. **Consider Queue for FIFO**: For true FIFO semantics with blocking
   operations, use ``Queue`` instead of ``List``.

5. **Index Lookups Are O(n)**: ``index_of()`` and ``last_index_of()``
   scan the entire list. For frequent lookups, consider a Map.

API Reference
-------------

.. automodule:: hazelcast.proxy.collections
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

SetProxy
~~~~~~~~

.. autoclass:: hazelcast.proxy.collections.SetProxy
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__, __contains__, __len__, __iter__

ListProxy
~~~~~~~~~

.. autoclass:: hazelcast.proxy.collections.ListProxy
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__, __getitem__, __setitem__, __delitem__, __contains__, __len__, __iter__
