hazelcast.proxy.cache
=====================

JCache (JSR-107) distributed cache proxy.

The ``Cache`` provides a JCache-compliant distributed caching implementation.
It offers standard caching operations with configurable expiry policies,
entry processors, and event listeners.

Usage Examples
--------------

Basic Operations
~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Get or create a Cache
   cache = client.get_cache("my-cache")
   
   # Basic CRUD operations
   cache.put("key1", "value1")
   value = cache.get("key1")
   cache.remove("key1")
   
   # Pythonic dict-like access
   cache["key2"] = "value2"
   value = cache["key2"]
   del cache["key2"]
   
   # Check existence
   exists = cache.contains_key("key1")
   exists = "key1" in cache  # Pythonic alternative
   
   # Get size
   size = cache.size()
   size = len(cache)  # Pythonic alternative
   
   client.shutdown()

Bulk Operations
~~~~~~~~~~~~~~~

.. code-block:: python

   # Put multiple entries
   cache.put_all({
       "user:1": {"name": "Alice", "age": 30},
       "user:2": {"name": "Bob", "age": 25},
       "user:3": {"name": "Charlie", "age": 35},
   })
   
   # Get multiple entries
   entries = cache.get_all(["user:1", "user:2"])
   
   # Remove multiple entries
   cache.remove_all(["user:1", "user:2"])
   
   # Clear entire cache
   cache.clear()

Atomic Operations
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Put if absent (atomic check-and-set)
   success = cache.put_if_absent("key", "value")
   
   # Get and put (returns old value)
   old = cache.get_and_put("key", "new-value")
   
   # Get and remove (returns removed value)
   removed = cache.get_and_remove("key")
   
   # Replace only if key exists
   success = cache.replace("key", "replacement")
   
   # Replace only if current value matches
   success = cache.replace_if_equals("key", "old-value", "new-value")
   
   # Remove only if value matches
   success = cache.remove_if_equals("key", "expected-value")

Expiry Policies
~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.proxy.cache import ExpiryPolicy, Duration
   from datetime import timedelta

   # Create expiry policy
   expiry = ExpiryPolicy(
       creation=Duration(timedelta(minutes=30)),
       access=Duration(timedelta(minutes=10)),
       update=Duration(timedelta(minutes=15)),
   )
   
   # Put with custom expiry
   cache.put("session:123", session_data, expiry_policy=expiry)
   
   # Default expiry policies
   eternal = ExpiryPolicy.ETERNAL  # Never expires
   
   # Create duration from various time units
   d1 = Duration.of_seconds(60)
   d2 = Duration.of_minutes(30)
   d3 = Duration.of_hours(24)

Entry Processor
~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.proxy.cache import CacheEntryProcessor, MutableEntry

   # Define an entry processor
   class IncrementProcessor(CacheEntryProcessor):
       def process(self, entry: MutableEntry, *args):
           current = entry.get_value() or 0
           new_value = current + 1
           entry.set_value(new_value)
           return new_value
   
   # Execute on single entry
   result = cache.invoke("counter", IncrementProcessor())
   
   # Execute on multiple entries
   results = cache.invoke_all(
       ["counter1", "counter2"],
       IncrementProcessor()
   )

Cache Listeners
~~~~~~~~~~~~~~~

.. code-block:: python

   # Define listener callbacks
   def on_created(event):
       print(f"Created: {event.key} = {event.value}")
   
   def on_updated(event):
       print(f"Updated: {event.key}: {event.old_value} -> {event.value}")
   
   def on_removed(event):
       print(f"Removed: {event.key}")
   
   def on_expired(event):
       print(f"Expired: {event.key}")
   
   # Register listener
   reg_id = cache.add_entry_listener(
       created=on_created,
       updated=on_updated,
       removed=on_removed,
       expired=on_expired,
   )
   
   # Remove listener
   cache.remove_entry_listener(reg_id)

Caching Patterns
~~~~~~~~~~~~~~~~

.. code-block:: python

   # Cache-aside pattern
   def get_user(user_id):
       # Try cache first
       user = cache.get(f"user:{user_id}")
       if user is None:
           # Cache miss: load from database
           user = database.get_user(user_id)
           if user:
               cache.put(f"user:{user_id}", user)
       return user
   
   # Read-through (if configured)
   # Cache automatically loads missing entries
   
   # Write-through (if configured)
   # Cache automatically writes to underlying store
   
   # Refresh-ahead
   def schedule_refresh(key):
       # Proactively refresh before expiry
       pass

Session Caching
~~~~~~~~~~~~~~~

.. code-block:: python

   from datetime import timedelta

   # Session cache with sliding expiration
   sessions = client.get_cache("sessions")
   
   expiry = ExpiryPolicy(
       creation=Duration.of_minutes(30),
       access=Duration.of_minutes(30),  # Reset on access
   )
   
   def create_session(session_id, user_data):
       sessions.put(session_id, user_data, expiry_policy=expiry)
   
   def get_session(session_id):
       # Access resets the expiry timer
       return sessions.get(session_id)
   
   def invalidate_session(session_id):
       sessions.remove(session_id)

Configuration Options
---------------------

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Option
     - Default
     - Description
   * - ``backup_count``
     - 1
     - Number of synchronous backups
   * - ``async_backup_count``
     - 0
     - Number of asynchronous backups
   * - ``in_memory_format``
     - BINARY
     - Storage format (BINARY or OBJECT)
   * - ``read_through``
     - false
     - Enable read-through from CacheLoader
   * - ``write_through``
     - false
     - Enable write-through to CacheWriter
   * - ``statistics_enabled``
     - false
     - Enable JCache statistics
   * - ``management_enabled``
     - false
     - Enable JCache management

Best Practices
--------------

1. **Set Expiry Policies**: Always configure appropriate expiry to
   prevent stale data and memory exhaustion.

2. **Use Entry Processors**: For read-modify-write operations, use
   entry processors to avoid network round-trips.

3. **Monitor Statistics**: Enable statistics to track hit/miss rates
   and optimize cache configuration.

4. **Consider Near Cache**: For read-heavy workloads, enable Near
   Cache for local caching on the client.

5. **Handle Cache Misses**: Always handle ``None`` returns from
   ``get()`` and implement proper fallback logic.

6. **Use Bulk Operations**: Prefer ``put_all()`` and ``get_all()``
   over multiple individual operations.

API Reference
-------------

.. automodule:: hazelcast.proxy.cache
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

Cache
~~~~~

.. autoclass:: hazelcast.proxy.cache.Cache
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__, __getitem__, __setitem__, __delitem__, __contains__, __len__

CacheConfig
~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.cache.CacheConfig
   :members:
   :undoc-members:
   :show-inheritance:

ExpiryPolicy
~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.cache.ExpiryPolicy
   :members:
   :undoc-members:
   :show-inheritance:

Duration
~~~~~~~~

.. autoclass:: hazelcast.proxy.cache.Duration
   :members:
   :undoc-members:
   :show-inheritance:

CacheEntryProcessor
~~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.cache.CacheEntryProcessor
   :members:
   :undoc-members:
   :show-inheritance:

MutableEntry
~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.cache.MutableEntry
   :members:
   :undoc-members:
   :show-inheritance:
