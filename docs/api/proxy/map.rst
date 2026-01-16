hazelcast.proxy.map
===================

Distributed Map data structure proxy.

The ``Map`` is the most commonly used distributed data structure in Hazelcast.
It provides a distributed, partitioned key-value store with rich querying and
event capabilities.

Usage Examples
--------------

Basic Operations
~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Get or create a distributed map
   my_map = client.get_map("my-map")
   
   # Basic CRUD operations
   my_map.put("key1", "value1")
   value = my_map.get("key1")
   my_map.remove("key1")
   
   # Pythonic dict-like access
   my_map["key2"] = "value2"
   value = my_map["key2"]
   del my_map["key2"]
   
   # Check existence
   exists = my_map.contains_key("key1")
   exists = "key1" in my_map  # Pythonic alternative
   
   # Get size
   size = my_map.size()
   size = len(my_map)  # Pythonic alternative
   
   client.shutdown()

Bulk Operations
~~~~~~~~~~~~~~~

.. code-block:: python

   # Put multiple entries at once
   my_map.put_all({
       "user:1": {"name": "Alice", "age": 30},
       "user:2": {"name": "Bob", "age": 25},
       "user:3": {"name": "Charlie", "age": 35},
   })
   
   # Get multiple entries
   entries = my_map.get_all(["user:1", "user:2"])
   
   # Get all keys, values, or entries
   keys = my_map.key_set()
   values = my_map.values()
   entries = my_map.entry_set()

TTL and Expiration
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Put with time-to-live (entry expires after 60 seconds)
   my_map.put("session:123", session_data, ttl=60.0)
   
   # Put with max idle time (expires if not accessed for 30 seconds)
   my_map.put("cache:key", cached_value, max_idle=30.0)
   
   # Set expiration on existing entry
   my_map.set_ttl("key", 120.0)

Atomic Operations
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Put if absent (atomic check-and-set)
   old = my_map.put_if_absent("key", "value")
   
   # Replace only if key exists
   old = my_map.replace("key", "new-value")
   
   # Replace only if current value matches
   success = my_map.replace_if_same("key", "old-value", "new-value")
   
   # Remove only if value matches
   success = my_map.remove_if_same("key", "expected-value")

Locking
~~~~~~~

.. code-block:: python

   # Pessimistic locking
   my_map.lock("key")
   try:
       value = my_map.get("key")
       my_map.put("key", value + 1)
   finally:
       my_map.unlock("key")
   
   # Try lock with timeout
   if my_map.try_lock("key", timeout=5.0):
       try:
           # Critical section
           pass
       finally:
           my_map.unlock("key")
   
   # Lock with lease time (auto-unlock after 30 seconds)
   my_map.lock("key", lease_time=30.0)

Querying with Predicates
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.predicate import (
       sql, equal, greater_than, and_, between, like
   )
   
   # SQL-style predicate
   adults = my_map.values(sql("age >= 18"))
   
   # Attribute predicates
   bobs = my_map.key_set(equal("name", "Bob"))
   seniors = my_map.entry_set(greater_than("age", 60))
   
   # Combined predicates
   results = my_map.values(
       and_(
           greater_than("age", 21),
           like("name", "A%")
       )
   )
   
   # Range query
   middle_aged = my_map.values(between("age", 30, 50))

Entry Listeners
~~~~~~~~~~~~~~~

.. code-block:: python

   def on_entry_added(event):
       print(f"Added: {event.key} = {event.value}")
   
   def on_entry_updated(event):
       print(f"Updated: {event.key}: {event.old_value} -> {event.value}")
   
   def on_entry_removed(event):
       print(f"Removed: {event.key}")
   
   # Add listener for all events
   listener = EntryListener(
       added=on_entry_added,
       updated=on_entry_updated,
       removed=on_entry_removed,
   )
   
   registration_id = my_map.add_entry_listener(
       listener,
       include_value=True
   )
   
   # Add listener for specific key
   my_map.add_entry_listener(listener, key="important-key")
   
   # Add listener with predicate (only matching entries)
   my_map.add_entry_listener(
       listener,
       predicate=greater_than("priority", 5)
   )
   
   # Remove listener
   my_map.remove_entry_listener(registration_id)

Indexing for Fast Queries
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.proxy.map import IndexConfig, IndexType
   
   # Add sorted index on 'age' attribute
   my_map.add_index(IndexConfig(
       name="age-index",
       type=IndexType.SORTED,
       attributes=["age"]
   ))
   
   # Add hash index for equality queries
   my_map.add_index(IndexConfig(
       name="name-index",
       type=IndexType.HASH,
       attributes=["name"]
   ))
   
   # Composite index
   my_map.add_index(IndexConfig(
       name="location-index",
       type=IndexType.SORTED,
       attributes=["country", "city"]
   ))

Entry Processor
~~~~~~~~~~~~~~~

.. code-block:: python

   # Execute code on the server side (avoids network round-trips)
   class IncrementProcessor:
       def process(self, entry):
           entry.set_value(entry.get_value() + 1)
           return entry.get_value()
   
   # Execute on single entry
   new_value = my_map.execute_on_key("counter", IncrementProcessor())
   
   # Execute on multiple entries
   results = my_map.execute_on_keys(
       ["counter1", "counter2"],
       IncrementProcessor()
   )
   
   # Execute on all entries matching predicate
   results = my_map.execute_on_entries(
       IncrementProcessor(),
       predicate=greater_than("value", 0)
   )

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
   * - ``time_to_live_seconds``
     - 0
     - Default TTL for entries (0 = infinite)
   * - ``max_idle_seconds``
     - 0
     - Default max idle time (0 = infinite)
   * - ``eviction_policy``
     - NONE
     - Eviction policy (NONE, LRU, LFU, RANDOM)
   * - ``max_size``
     - MAX_INT
     - Maximum number of entries before eviction
   * - ``in_memory_format``
     - BINARY
     - Storage format (BINARY, OBJECT, NATIVE)

Best Practices
--------------

1. **Use Bulk Operations**: Prefer ``put_all()`` and ``get_all()`` over
   multiple single operations to reduce network round-trips.

2. **Index Frequently Queried Attributes**: Add indexes on attributes
   used in predicates to improve query performance.

3. **Use Entry Processors**: For read-modify-write operations, use
   entry processors to avoid network latency and race conditions.

4. **Set Appropriate TTL**: Use TTL or max-idle to prevent unbounded
   growth and stale data accumulation.

5. **Limit Listener Scope**: Use key-based or predicate-based listeners
   instead of listening to all events when possible.

6. **Choose Serialization Wisely**: Use Compact serialization for complex
   objects to enable server-side queries and reduce payload size.

API Reference
-------------

.. automodule:: hazelcast.proxy.map
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

MapProxy
~~~~~~~~

.. autoclass:: hazelcast.proxy.map.MapProxy
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__, __getitem__, __setitem__, __delitem__, __contains__, __len__, __iter__

EntryView
~~~~~~~~~

.. autoclass:: hazelcast.proxy.map.EntryView
   :members:
   :undoc-members:
   :show-inheritance:

EntryEvent
~~~~~~~~~~

.. autoclass:: hazelcast.proxy.map.EntryEvent
   :members:
   :undoc-members:
   :show-inheritance:

EntryListener
~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.map.EntryListener
   :members:
   :undoc-members:
   :show-inheritance:

IndexConfig
~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.map.IndexConfig
   :members:
   :undoc-members:
   :show-inheritance:

IndexType
~~~~~~~~~

.. autoclass:: hazelcast.proxy.map.IndexType
   :members:
   :undoc-members:
