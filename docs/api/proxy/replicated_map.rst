hazelcast.proxy.replicated_map
==============================

Distributed ReplicatedMap data structure proxy.

A ``ReplicatedMap`` is a weakly consistent, distributed key-value store that
replicates all entries to all cluster members. Unlike a partitioned ``Map``,
every member holds the entire dataset, providing:

- **Local reads**: All reads are served from local memory
- **Weak consistency**: Updates propagate asynchronously
- **High availability**: No single point of failure

ReplicatedMap is ideal for small, read-heavy datasets like configuration,
reference data, or caches where eventual consistency is acceptable.

Usage Examples
--------------

Basic Operations
~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Get or create a ReplicatedMap
   config_map = client.get_replicated_map("config")
   
   # Basic CRUD operations
   config_map.put("app.name", "MyApp")
   config_map.put("app.version", "1.0.0")
   config_map.put("feature.enabled", True)
   
   # Get values (always local, very fast)
   name = config_map.get("app.name")
   version = config_map.get("app.version")
   
   # Remove entry
   old = config_map.remove("feature.enabled")
   
   # Check size
   size = config_map.size()
   
   client.shutdown()

Configuration Management
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Store application configuration
   config = client.get_replicated_map("app-config")
   
   # Set configuration values
   config.put_all({
       "database.host": "localhost",
       "database.port": 5432,
       "cache.ttl": 3600,
       "feature.dark_mode": True,
   })
   
   # Read configuration (fast local reads)
   def get_config(key, default=None):
       value = config.get(key)
       return value if value is not None else default
   
   db_host = get_config("database.host", "127.0.0.1")
   cache_ttl = get_config("cache.ttl", 1800)

Reference Data Cache
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Cache reference data that rarely changes
   countries = client.get_replicated_map("countries")
   
   # Load reference data once
   countries.put_all({
       "US": {"name": "United States", "currency": "USD"},
       "GB": {"name": "United Kingdom", "currency": "GBP"},
       "DE": {"name": "Germany", "currency": "EUR"},
       "JP": {"name": "Japan", "currency": "JPY"},
   })
   
   # Fast lookups from any node
   def get_country_info(code):
       return countries.get(code)

Feature Flags
~~~~~~~~~~~~~

.. code-block:: python

   # Distributed feature flags
   features = client.get_replicated_map("feature-flags")
   
   # Update feature flags
   features.put("new_checkout", True)
   features.put("dark_mode", False)
   features.put("beta_features", ["feature_a", "feature_b"])
   
   # Check feature flags (local read)
   def is_feature_enabled(feature_name):
       return features.get(feature_name) == True
   
   if is_feature_enabled("new_checkout"):
       show_new_checkout()

Entry Listeners
~~~~~~~~~~~~~~~

.. code-block:: python

   def on_entry_added(event):
       print(f"Added: {event.key} = {event.value}")
   
   def on_entry_updated(event):
       print(f"Updated: {event.key}: {event.old_value} -> {event.value}")
   
   def on_entry_removed(event):
       print(f"Removed: {event.key}")
   
   listener = EntryListener(
       added=on_entry_added,
       updated=on_entry_updated,
       removed=on_entry_removed,
   )
   
   reg_id = config_map.add_entry_listener(listener)
   
   # Remove listener when done
   config_map.remove_entry_listener(reg_id)

Querying
~~~~~~~~

.. code-block:: python

   # Get all keys
   keys = config_map.key_set()
   
   # Get all values
   values = config_map.values()
   
   # Get all entries
   entries = config_map.entry_set()
   
   # Iterate over entries
   for key, value in config_map.entry_set():
       print(f"{key}: {value}")
   
   # Check if key exists
   has_key = config_map.contains_key("app.name")
   
   # Check if value exists
   has_value = config_map.contains_value("MyApp")

Configuration Options
---------------------

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Option
     - Default
     - Description
   * - ``in_memory_format``
     - OBJECT
     - Storage format (OBJECT or BINARY)
   * - ``replication_delay_ms``
     - 100
     - Delay before replication (batching)
   * - ``concurrency_level``
     - 32
     - Internal lock striping level
   * - ``statistics_enabled``
     - true
     - Enable statistics collection
   * - ``async_fillup``
     - true
     - Async initial data load

Best Practices
--------------

1. **Keep Data Small**: ReplicatedMap stores all data on every member.
   Keep the dataset small (< 10,000 entries recommended).

2. **Use for Read-Heavy Data**: ReplicatedMap excels when reads far
   outnumber writes. All reads are local and fast.

3. **Accept Eventual Consistency**: Updates propagate asynchronously.
   Don't use for data requiring strong consistency.

4. **Consider Memory Impact**: Each member stores the entire dataset.
   Monitor memory usage as data grows.

5. **Use for Configuration**: Ideal for configuration, feature flags,
   and reference data that changes infrequently.

ReplicatedMap vs Map
--------------------

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Aspect
     - ReplicatedMap
     - Map (Partitioned)
   * - **Data Distribution**
     - Full copy on each member
     - Partitioned across members
   * - **Read Performance**
     - Very fast (local)
     - Network hop for remote partitions
   * - **Write Performance**
     - Slower (broadcasts to all)
     - Fast (single partition)
   * - **Consistency**
     - Eventual
     - Configurable
   * - **Memory Usage**
     - N x data size
     - (1 + backups) x data size
   * - **Best For**
     - Small, read-heavy data
     - Large, write-heavy data

API Reference
-------------

.. automodule:: hazelcast.proxy.replicated_map
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

ReplicatedMapProxy
~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.replicated_map.ReplicatedMapProxy
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__, __getitem__, __setitem__, __delitem__, __contains__, __len__, __iter__
