Distributed Data Structures
===========================

Hazelcast provides a rich set of distributed data structures that can be
accessed through the Python client.

Map (IMap)
----------

The distributed Map is a key-value store partitioned across the cluster.

.. code-block:: python

   my_map = client.get_map("my-map")

   # Basic operations
   my_map.put("key", "value")
   value = my_map.get("key")
   my_map.remove("key")

   # Bulk operations
   my_map.put_all({"k1": "v1", "k2": "v2"})
   entries = my_map.get_all({"k1", "k2"})

   # Conditional operations
   my_map.put_if_absent("key", "default")
   old = my_map.replace("key", "new_value")
   success = my_map.replace_if_equals("key", "expected", "new")

   # TTL support
   my_map.put("expiring", "value", ttl=300)  # Expires in 5 minutes

   # Query with predicates
   from hazelcast import SqlPredicate
   results = my_map.values(SqlPredicate("age > 30"))


ReplicatedMap
-------------

ReplicatedMap stores data on all cluster members (no partitioning).
Suitable for small, read-heavy datasets with eventual consistency.

.. code-block:: python

   rep_map = client.get_replicated_map("config")

   # Same API as Map
   rep_map.put("setting", "value")
   value = rep_map.get("setting")

   # With TTL
   rep_map.put("temp", "data", ttl=60)

   # Bulk operations
   all_entries = rep_map.entry_set()
   all_keys = rep_map.key_set()
   all_values = rep_map.values()


MultiMap
--------

MultiMap allows multiple values per key.

.. code-block:: python

   mm = client.get_multi_map("user-roles")

   # Add multiple values for same key
   mm.put("user:1", "admin")
   mm.put("user:1", "editor")
   mm.put("user:1", "viewer")

   # Get all values for a key
   roles = mm.get("user:1")  # ["admin", "editor", "viewer"]

   # Remove specific value
   mm.remove("user:1", "viewer")

   # Remove all values for key
   mm.remove_all("user:1")

   # Count values
   count = mm.value_count("user:1")


Queue
-----

Distributed blocking queue.

.. code-block:: python

   queue = client.get_queue("task-queue")

   # Add items
   queue.offer("task-1")  # Non-blocking, returns bool
   queue.put("task-2")    # Blocking

   # Retrieve items
   item = queue.poll()           # Non-blocking, returns None if empty
   item = queue.poll(timeout=5)  # Wait up to 5 seconds
   item = queue.take()           # Blocking wait

   # Peek without removing
   item = queue.peek()

   # Bulk operations
   items = queue.drain_to([])


Set
---

Distributed set that doesn't allow duplicates.

.. code-block:: python

   my_set = client.get_set("unique-ids")

   my_set.add("id-1")
   my_set.add("id-2")
   my_set.add("id-1")  # Duplicate, ignored

   size = my_set.size()  # 2
   exists = my_set.contains("id-1")

   my_set.remove("id-2")

   # Bulk operations
   my_set.add_all(["id-3", "id-4"])
   all_items = my_set.get_all()


List
----

Distributed list maintaining insertion order.

.. code-block:: python

   my_list = client.get_list("items")

   my_list.add("first")
   my_list.add("second")
   my_list.add_at(0, "new-first")

   item = my_list.get(0)
   my_list.set(1, "updated")
   my_list.remove_at(0)

   size = my_list.size()
   sublist = my_list.sub_list(0, 2)


Ringbuffer
----------

Bounded, circular data structure with sequence-based access.

.. code-block:: python

   rb = client.get_ringbuffer("events")

   # Add items
   sequence = rb.add("event-1")
   rb.add_all(["event-2", "event-3"])

   # Read items
   item = rb.read_one(sequence)

   # Batch read
   result = rb.read_many(
       start_sequence=0,
       min_count=1,
       max_count=100,
   )
   for item in result:
       print(item)

   # Capacity and size
   capacity = rb.capacity()
   size = rb.size()
   head = rb.head_sequence()
   tail = rb.tail_sequence()


Topic
-----

Publish-subscribe messaging.

.. code-block:: python

   topic = client.get_topic("notifications")

   # Subscribe to messages
   def on_message(message):
       print(f"Received: {message.message}")
       print(f"Published at: {message.publish_time}")

   reg_id = topic.add_message_listener(on_message=on_message)

   # Publish messages
   topic.publish("Hello, subscribers!")

   # Unsubscribe
   topic.remove_message_listener(reg_id)


ReliableTopic
-------------

Topic backed by a Ringbuffer for reliable delivery.

.. code-block:: python

   from hazelcast.proxy.reliable_topic import ReliableMessageListener

   class MyListener(ReliableMessageListener):
       def on_message(self, message):
           print(f"Received: {message.message}")

       def is_loss_tolerant(self):
           return False

       def is_terminal(self, error):
           return True

       def store_sequence(self, sequence):
           pass

       def retrieve_initial_sequence(self):
           return -1

   topic = client.get_reliable_topic("reliable-events")
   topic.add_message_listener(MyListener())
   topic.publish("Important message")


PN Counter
----------

CRDT Positive-Negative Counter with eventual consistency.

.. code-block:: python

   counter = client.get_pn_counter("page-views")

   # Increment
   value = counter.increment_and_get()
   value = counter.add_and_get(10)

   # Decrement
   value = counter.decrement_and_get()
   value = counter.subtract_and_get(5)

   # Get current value
   current = counter.get()


Cardinality Estimator
---------------------

Estimate unique element count using HyperLogLog algorithm.

.. code-block:: python

   estimator = client.get_cardinality_estimator("unique-visitors")

   # Add elements
   estimator.add("user-1")
   estimator.add("user-2")
   estimator.add("user-1")  # Duplicate

   # Get estimate
   count = estimator.estimate()
   print(f"Unique visitors: ~{count}")


FlakeIdGenerator
----------------

Generate cluster-wide unique IDs using Flake ID algorithm. IDs are
64-bit integers, roughly ordered by time, and generated without
coordination between cluster members.

.. code-block:: python

   id_gen = client.get_flake_id_generator("order-ids")

   # Generate a single unique ID
   order_id = id_gen.new_id()
   print(f"New order ID: {order_id}")

   # Generate multiple IDs in a batch (more efficient)
   batch = id_gen.new_id_batch(10)
   for id in batch:
       print(f"Batch ID: {id}")

Async Variants
~~~~~~~~~~~~~~

.. code-block:: python

   id_gen = client.get_flake_id_generator("async-ids")

   # Async single ID
   future = id_gen.new_id_async()
   new_id = future.result()

   # Async batch
   future = id_gen.new_id_batch_async(5)
   batch = future.result()

Configuration
~~~~~~~~~~~~~

FlakeIdGenerator can be configured with prefetch settings for
batching efficiency:

.. code-block:: python

   from hazelcast.proxy.flake_id import FlakeIdGeneratorConfig

   config = FlakeIdGeneratorConfig(
       name="my-id-gen",
       prefetch_count=100,           # IDs to prefetch
       prefetch_validity_millis=600000,  # 10 minutes validity
   )


Entry Listeners
---------------

Listen to data structure changes:

.. code-block:: python

   from hazelcast.proxy.map import EntryListener, EntryEvent

   class MapListener(EntryListener):
       def entry_added(self, event: EntryEvent):
           print(f"Added: {event.key}")

       def entry_removed(self, event: EntryEvent):
           print(f"Removed: {event.key}")

       def entry_updated(self, event: EntryEvent):
           print(f"Updated: {event.key}")

       def entry_evicted(self, event: EntryEvent):
           print(f"Evicted: {event.key}")

   my_map = client.get_map("observed-map")
   reg_id = my_map.add_entry_listener(
       MapListener(),
       include_value=True,
   )

   # Filter by key
   reg_id = my_map.add_entry_listener(
       MapListener(),
       key="specific-key",
       include_value=True,
   )

   # Filter by predicate
   from hazelcast import SqlPredicate
   reg_id = my_map.add_entry_listener(
       MapListener(),
       predicate=SqlPredicate("status = 'active'"),
       include_value=True,
   )


Item Listeners
--------------

Listen to collection changes:

.. code-block:: python

   from hazelcast.proxy.queue import ItemListener, ItemEvent, ItemEventType

   class QueueListener(ItemListener):
       def item_added(self, event: ItemEvent):
           print(f"Added: {event.item}")

       def item_removed(self, event: ItemEvent):
           print(f"Removed: {event.item}")

   queue = client.get_queue("observed-queue")
   reg_id = queue.add_item_listener(QueueListener(), include_value=True)
