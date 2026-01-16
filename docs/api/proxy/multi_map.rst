hazelcast.proxy.multi_map
=========================

Distributed MultiMap data structure proxy.

A ``MultiMap`` is a specialized map that allows multiple values to be
associated with a single key. Unlike a regular Map where each key maps
to exactly one value, a MultiMap key can map to a collection of values.

Usage Examples
--------------

Basic Operations
~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Get or create a distributed MultiMap
   multi_map = client.get_multi_map("tags")
   
   # Associate multiple values with a key
   multi_map.put("article:123", "python")
   multi_map.put("article:123", "programming")
   multi_map.put("article:123", "tutorial")
   
   # Get all values for a key
   tags = multi_map.get("article:123")
   # Returns: ["python", "programming", "tutorial"]
   
   # Check how many values a key has
   count = multi_map.value_count("article:123")  # 3
   
   # Check if key-value pair exists
   exists = multi_map.contains_entry("article:123", "python")
   
   client.shutdown()

Common Use Cases
~~~~~~~~~~~~~~~~

.. code-block:: python

   # User roles (one user, multiple roles)
   roles = client.get_multi_map("user-roles")
   roles.put("user:alice", "admin")
   roles.put("user:alice", "editor")
   roles.put("user:alice", "viewer")
   
   user_roles = roles.get("user:alice")
   
   # Product categories (one product, multiple categories)
   categories = client.get_multi_map("product-categories")
   categories.put("product:laptop", "electronics")
   categories.put("product:laptop", "computers")
   categories.put("product:laptop", "office")
   
   # Reverse lookup: find all products in a category
   # (requires application-level indexing)

Removal Operations
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Remove a specific value from a key
   removed = multi_map.remove("article:123", "tutorial")
   
   # Remove all values for a key
   values = multi_map.remove_all("article:123")
   
   # Clear the entire MultiMap
   multi_map.clear()

Querying
~~~~~~~~

.. code-block:: python

   # Get all keys
   keys = multi_map.key_set()
   
   # Get all values (from all keys)
   all_values = multi_map.values()
   
   # Get all key-value pairs
   entries = multi_map.entry_set()
   
   # Get size (total number of key-value pairs)
   size = multi_map.size()
   
   # Check if a key exists
   has_key = multi_map.contains_key("article:123")
   
   # Check if a value exists (in any key)
   has_value = multi_map.contains_value("python")

Locking
~~~~~~~

.. code-block:: python

   # Lock a key for exclusive access
   multi_map.lock("article:123")
   try:
       # Atomic read-modify-write
       current = multi_map.get("article:123")
       if "deprecated" not in current:
           multi_map.put("article:123", "deprecated")
   finally:
       multi_map.unlock("article:123")
   
   # Try lock with timeout
   if multi_map.try_lock("article:123", timeout=5.0):
       try:
           # Critical section
           pass
       finally:
           multi_map.unlock("article:123")

Entry Listeners
~~~~~~~~~~~~~~~

.. code-block:: python

   def on_entry_added(event):
       print(f"Added: {event.key} -> {event.value}")
   
   def on_entry_removed(event):
       print(f"Removed: {event.key} -> {event.value}")
   
   listener = EntryListener(
       added=on_entry_added,
       removed=on_entry_removed,
   )
   
   # Listen to all entries
   reg_id = multi_map.add_entry_listener(listener, include_value=True)
   
   # Listen to specific key
   multi_map.add_entry_listener(listener, key="article:123")
   
   # Remove listener
   multi_map.remove_entry_listener(reg_id)

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
   * - ``value_collection_type``
     - SET
     - Collection type for values (SET or LIST)
   * - ``binary``
     - false
     - Store values in binary format

Best Practices
--------------

1. **Choose Collection Type Wisely**: Use ``SET`` (default) to prevent
   duplicate values per key. Use ``LIST`` if duplicates are allowed.

2. **Use Locking for Consistency**: When performing read-modify-write
   operations, use ``lock()`` to ensure atomicity.

3. **Consider Memory Impact**: MultiMaps can grow large quickly. Monitor
   memory usage and consider eviction policies.

4. **Batch Operations**: When adding many values, consider the network
   overhead of individual ``put()`` calls.

API Reference
-------------

.. automodule:: hazelcast.proxy.multi_map
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

MultiMapProxy
~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.multi_map.MultiMapProxy
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__, __getitem__, __contains__, __len__
