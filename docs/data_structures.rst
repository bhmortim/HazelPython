Distributed Data Structures
===========================

Hazelcast provides distributed versions of common data structures. Data is
automatically partitioned across cluster members for scalability and
high availability.

Map (IMap)
----------

The distributed map is the most commonly used data structure. It provides
key-value storage with support for queries, entry processors, and listeners.

Basic Operations
~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   with HazelcastClient() as client:
       my_map = client.get_map("my-map")
       
       # Put and get
       my_map.put("key1", "value1")
       value = my_map.get("key1")
       
       # Put with TTL
       my_map.put("temp-key", "temp-value", ttl=60)  # Expires in 60 seconds
       
       # Put if absent
       my_map.put_if_absent("key1", "new-value")  # Won't overwrite
       
       # Replace
       my_map.replace("key1", "updated-value")
       my_map.replace_if_same("key1", "updated-value", "final-value")
       
       # Remove
       old = my_map.remove("key1")
       removed = my_map.remove_if_same("key2", "expected-value")
       
       # Bulk operations
       my_map.put_all({"a": 1, "b": 2, "c": 3})
       values = my_map.get_all(["a", "b", "c"])

Querying Maps
~~~~~~~~~~~~~

Use predicates to query map entries:

.. code-block:: python

   from hazelcast.predicate import (
       attr, sql, and_, or_, not_, between, paging
   )

   users = client.get_map("users")
   
   # SQL-like predicate
   adults = users.values(sql("age >= 18"))
   
   # Attribute predicates
   seniors = users.values(attr("age").greater_than_or_equal(65))
   
   # Combined predicates
   young_devs = users.values(
       and_(
           attr("age").between(20, 30),
           attr("role").equal("developer")
       )
   )
   
   # Paging predicate for large results
   paging_pred = paging(page_size=100)
   page1 = users.values(paging_pred)
   
   paging_pred.next_page()
   page2 = users.values(paging_pred)

Aggregations
~~~~~~~~~~~~

Perform server-side aggregations:

.. code-block:: python

   from hazelcast.aggregator import count, sum_, average, min_, max_

   employees = client.get_map("employees")
   
   # Count all entries
   total = employees.aggregate(count())
   
   # Sum salaries
   total_salary = employees.aggregate(sum_("salary"))
   
   # Average age
   avg_age = employees.aggregate(average("age"))
   
   # With predicate
   from hazelcast.predicate import attr
   
   dept_avg = employees.aggregate(
       average("salary"),
       predicate=attr("department").equal("Engineering")
   )

Projections
~~~~~~~~~~~

Project specific attributes:

.. code-block:: python

   from hazelcast.projection import single_attribute, multi_attribute

   users = client.get_map("users")
   
   # Get only names
   names = users.project(single_attribute("name"))
   
   # Get multiple attributes
   contacts = users.project(multi_attribute("name", "email", "phone"))

Entry Listeners
~~~~~~~~~~~~~~~

React to map changes:

.. code-block:: python

   def on_entry_added(event):
       print(f"Added: {event.key} = {event.value}")

   def on_entry_updated(event):
       print(f"Updated: {event.key}: {event.old_value} -> {event.value}")

   def on_entry_removed(event):
       print(f"Removed: {event.key}")

   users = client.get_map("users")
   
   reg_id = users.add_entry_listener(
       on_added=on_entry_added,
       on_updated=on_entry_updated,
       on_removed=on_entry_removed,
       include_value=True,
   )
   
   # Later, remove the listener
   users.remove_entry_listener(reg_id)

Queue (IQueue)
--------------

A distributed blocking queue for producer-consumer patterns:

.. code-block:: python

   queue = client.get_queue("task-queue")
   
   # Producer
   queue.offer("task-1")
   queue.offer("task-2", timeout=5.0)  # Wait up to 5 seconds
   queue.put("task-3")  # Block until space available
   
   # Consumer
   task = queue.poll()  # Non-blocking
   task = queue.poll(timeout=10.0)  # Wait up to 10 seconds
   task = queue.take()  # Block until item available
   
   # Peek without removing
   next_task = queue.peek()
   
   # Bulk operations
   tasks = queue.drain_to(max_elements=100)

Set (ISet)
----------

A distributed set that doesn't allow duplicates:

.. code-block:: python

   tags = client.get_set("tags")
   
   # Add items
   tags.add("python")
   tags.add_all(["java", "golang", "rust"])
   
   # Check membership
   has_python = tags.contains("python")
   has_all = tags.contains_all(["python", "java"])
   
   # Remove items
   tags.remove("golang")
   tags.remove_all(["java", "rust"])
   
   # Get all items
   all_tags = tags.get_all()
   
   # Size
   count = tags.size()

List (IList)
------------

A distributed list maintaining insertion order:

.. code-block:: python

   items = client.get_list("items")
   
   # Add items
   items.add("first")
   items.add("second")
   items.add_at(1, "inserted")  # Insert at index
   
   # Access by index
   first = items.get(0)
   
   # Modify
   items.set(0, "updated-first")
   items.remove_at(1)
   
   # Sublist
   subset = items.sub_list(0, 2)

MultiMap
--------

A map that allows multiple values per key:

.. code-block:: python

   user_roles = client.get_multi_map("user-roles")
   
   # Add multiple values for a key
   user_roles.put("user:1", "admin")
   user_roles.put("user:1", "editor")
   user_roles.put("user:1", "viewer")
   
   # Get all values for a key
   roles = user_roles.get("user:1")  # ["admin", "editor", "viewer"]
   
   # Remove specific value
   user_roles.remove("user:1", "viewer")
   
   # Remove all values for key
   user_roles.remove_all("user:1")
   
   # Get counts
   value_count = user_roles.value_count("user:1")
   total_size = user_roles.size()

ReplicatedMap
-------------

A map replicated to all cluster members (not partitioned):

.. code-block:: python

   config_map = client.get_replicated_map("config")
   
   # Operations are similar to IMap
   config_map.put("timeout", 30)
   config_map.put("max_retries", 3)
   
   timeout = config_map.get("timeout")
   
   # Replicated to all members - fast local reads
   # Best for small, read-heavy datasets

Ringbuffer
----------

A bounded, circular buffer with sequence-based access:

.. code-block:: python

   from hazelcast.proxy.ringbuffer import OverflowPolicy

   events = client.get_ringbuffer("events")
   
   # Add items
   seq = events.add("event-1")
   seq = events.add("event-2", overflow_policy=OverflowPolicy.OVERWRITE)
   
   # Read by sequence
   event = events.read_one(seq)
   
   # Read multiple
   items = events.read_many(
       start_sequence=0,
       min_count=1,
       max_count=100,
   )
   
   # Get capacity and size
   capacity = events.capacity()
   size = events.size()
   head_seq = events.head_sequence()
   tail_seq = events.tail_sequence()

Topic (ITopic)
--------------

Publish-subscribe messaging:

.. code-block:: python

   def on_message(message):
       print(f"Received: {message.message}")

   notifications = client.get_topic("notifications")
   
   # Subscribe
   reg_id = notifications.add_message_listener(on_message=on_message)
   
   # Publish
   notifications.publish("Hello, subscribers!")
   notifications.publish({"type": "alert", "message": "Server restarting"})
   
   # Unsubscribe
   notifications.remove_message_listener(reg_id)

ReliableTopic
~~~~~~~~~~~~~

A topic backed by a ringbuffer for reliable delivery:

.. code-block:: python

   reliable = client.get_reliable_topic("reliable-notifications")
   
   # Same API as Topic
   reg_id = reliable.add_message_listener(on_message=on_message)
   reliable.publish("Important message")

PNCounter
---------

A CRDT counter supporting increment and decrement with eventual consistency:

.. code-block:: python

   page_views = client.get_pn_counter("page-views")
   
   # Increment
   new_value = page_views.increment_and_get()
   page_views.add_and_get(5)
   
   # Decrement
   new_value = page_views.decrement_and_get()
   
   # Get current value
   current = page_views.get()
   
   # Reset
   page_views.reset()

FlakeIdGenerator
----------------

Generate cluster-wide unique, roughly time-ordered IDs:

.. code-block:: python

   id_gen = client.get_flake_id_generator("order-ids")
   
   # Generate unique ID
   order_id = id_gen.new_id()
   
   # IDs are 64-bit integers, roughly ordered by time
   print(f"New order ID: {order_id}")

CardinalityEstimator
--------------------

Estimate distinct element count using HyperLogLog:

.. code-block:: python

   visitors = client.get_cardinality_estimator("unique-visitors")
   
   # Add elements
   visitors.add("user-1")
   visitors.add("user-2")
   visitors.add("user-1")  # Duplicate
   
   # Estimate count
   estimate = visitors.estimate()
   print(f"Approximately {estimate} unique visitors")
