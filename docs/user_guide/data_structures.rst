Distributed Data Structures
===========================

The Hazelcast Python Client provides access to various distributed data
structures that are partitioned across cluster members for scalability.

IMap
----

A distributed, partitioned key-value store with rich query capabilities.

Basic Operations
~~~~~~~~~~~~~~~~

.. code-block:: python

    my_map = client.get_map("users")

    # Put and get
    my_map.put("user:1", {"name": "Alice", "age": 30})
    user = my_map.get("user:1")

    # Put with TTL (time-to-live)
    my_map.put("session:123", session_data, ttl=3600)  # 1 hour TTL

    # Conditional operations
    my_map.put_if_absent("user:2", new_user)
    my_map.replace("user:1", updated_user)
    my_map.replace_if_same("user:1", old_user, new_user)

    # Remove operations
    removed = my_map.remove("user:1")
    my_map.delete("user:2")  # No return value

    # Bulk operations
    my_map.put_all({"k1": "v1", "k2": "v2", "k3": "v3"})
    results = my_map.get_all({"k1", "k2", "k3"})

    # Collection views
    keys = my_map.key_set()
    values = my_map.values()
    entries = my_map.entry_set()

Python Dict-Like Interface
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    my_map = client.get_map("cache")

    # Dict-like access
    my_map["key"] = "value"
    value = my_map["key"]
    del my_map["key"]

    # Membership testing
    if "key" in my_map:
        print("Key exists")

    # Iteration
    for key in my_map:
        print(f"{key}: {my_map[key]}")

    # Size
    print(f"Size: {len(my_map)}")

Entry Listeners
~~~~~~~~~~~~~~~

.. code-block:: python

    from hazelcast.proxy.map import EntryListener, EntryEvent

    class CacheListener(EntryListener):
        def entry_added(self, event: EntryEvent) -> None:
            print(f"Added: {event.key}")

        def entry_updated(self, event: EntryEvent) -> None:
            print(f"Updated: {event.key}, old={event.old_value}")

        def entry_removed(self, event: EntryEvent) -> None:
            print(f"Removed: {event.key}")

        def entry_evicted(self, event: EntryEvent) -> None:
            print(f"Evicted: {event.key}")

    reg_id = my_map.add_entry_listener(
        CacheListener(),
        include_value=True,
        key="specific-key",  # Optional: listen to specific key
    )

    # Later, remove listener
    my_map.remove_entry_listener(reg_id)

Entry Processors
~~~~~~~~~~~~~~~~

Execute code on entries atomically:

.. code-block:: python

    from hazelcast.processor import EntryProcessor

    class IncrementProcessor(EntryProcessor):
        def __init__(self, delta: int):
            self.delta = delta

        def process(self, entry):
            current = entry.get_value() or 0
            entry.set_value(current + self.delta)
            return current + self.delta

    # Execute on single key
    result = my_map.execute_on_key("counter", IncrementProcessor(5))

    # Execute on multiple keys
    results = my_map.execute_on_keys({"c1", "c2", "c3"}, IncrementProcessor(1))

    # Execute on all entries
    results = my_map.execute_on_all_entries(IncrementProcessor(1))

Locking
~~~~~~~

.. code-block:: python

    my_map.lock("resource-key")
    try:
        # Critical section
        value = my_map.get("resource-key")
        my_map.put("resource-key", value + 1)
    finally:
        my_map.unlock("resource-key")

    # Try lock with timeout
    if my_map.try_lock("key", timeout=5.0):
        try:
            # ...
        finally:
            my_map.unlock("key")

IQueue
------

A distributed blocking queue for producer-consumer patterns.

.. code-block:: python

    queue = client.get_queue("task-queue")

    # Add items
    queue.offer("task-1")
    queue.put("task-2")  # Blocks if full
    queue.add("task-3")  # Throws if full

    # Poll items
    task = queue.poll()  # Returns None if empty
    task = queue.poll(timeout=5.0)  # Wait up to 5 seconds
    task = queue.take()  # Blocks until available

    # Peek without removing
    head = queue.peek()

    # Drain to list
    tasks = []
    count = queue.drain_to(tasks, max_elements=10)

    # Item listeners
    queue.add_item_listener(
        item_added=lambda e: print(f"Added: {e.item}"),
        item_removed=lambda e: print(f"Removed: {e.item}"),
    )

ISet
----

A distributed set that doesn't allow duplicates.

.. code-block:: python

    my_set = client.get_set("unique-ids")

    # Add items
    added = my_set.add("id-1")  # Returns True if new
    my_set.add_all(["id-2", "id-3", "id-4"])

    # Check membership
    exists = my_set.contains("id-1")
    all_exist = my_set.contains_all(["id-1", "id-2"])

    # Remove items
    removed = my_set.remove("id-1")
    my_set.remove_all(["id-2", "id-3"])

    # Retain only specified
    my_set.retain_all(["id-4"])

    # Size and iteration
    print(f"Size: {len(my_set)}")
    for item in my_set:
        print(item)

IList
-----

A distributed ordered list allowing duplicates.

.. code-block:: python

    my_list = client.get_list("items")

    # Add items
    my_list.add("item-1")
    my_list.add_at(0, "first-item")
    my_list.add_all(["item-2", "item-3"])

    # Access by index
    item = my_list.get(0)
    my_list.set(0, "updated-first")

    # Remove
    my_list.remove("item-1")
    removed = my_list.remove_at(0)

    # Search
    index = my_list.index_of("item-2")
    last_index = my_list.last_index_of("item-2")

    # Sublist
    sub = my_list.sub_list(1, 3)

    # Python list interface
    item = my_list[0]
    my_list[0] = "new-value"
    del my_list[0]

MultiMap
--------

A map allowing multiple values per key.

.. code-block:: python

    multi_map = client.get_multi_map("user-roles")

    # Add values
    multi_map.put("user:1", "admin")
    multi_map.put("user:1", "editor")
    multi_map.put("user:1", "viewer")

    # Get all values for key
    roles = multi_map.get("user:1")  # ["admin", "editor", "viewer"]

    # Remove specific value
    multi_map.remove("user:1", "viewer")

    # Remove all values for key
    removed = multi_map.remove_all("user:1")

    # Count values
    count = multi_map.value_count("user:1")

ReplicatedMap
-------------

A fully replicated map where all members hold all data.

.. code-block:: python

    rep_map = client.get_replicated_map("config")

    # Similar to IMap but not partitioned
    rep_map.put("timeout", 30, ttl=300)
    value = rep_map.get("timeout")

    # Good for small, read-heavy datasets
    all_config = rep_map.entry_set()

Ringbuffer
----------

A bounded circular buffer with sequence-based access.

.. code-block:: python

    from hazelcast.proxy.ringbuffer import OverflowPolicy

    rb = client.get_ringbuffer("events")

    # Add items
    seq = rb.add("event-1")
    seq = rb.add("event-2", overflow_policy=OverflowPolicy.OVERWRITE)

    # Add multiple
    last_seq = rb.add_all(["e3", "e4", "e5"])

    # Read by sequence
    item = rb.read_one(seq)

    # Read multiple
    result_set = rb.read_many(
        start_sequence=0,
        min_count=1,
        max_count=100,
    )
    for item in result_set:
        print(item)

    # Capacity info
    capacity = rb.capacity()
    size = rb.size()
    remaining = rb.remaining_capacity()
    head = rb.head_sequence()
    tail = rb.tail_sequence()

Topic
-----

Publish-subscribe messaging.

.. code-block:: python

    topic = client.get_topic("notifications")

    # Subscribe
    def on_message(msg):
        print(f"Received: {msg.message}")

    reg_id = topic.add_message_listener(on_message=on_message)

    # Publish
    topic.publish("Hello, subscribers!")

    # Unsubscribe
    topic.remove_message_listener(reg_id)

ReliableTopic
-------------

Topic backed by a ringbuffer for reliable delivery.

.. code-block:: python

    from hazelcast.proxy.reliable_topic import ReliableMessageListener

    class MyListener(ReliableMessageListener):
        def on_message(self, msg):
            print(f"Received: {msg.message}")

        def store_sequence(self, sequence):
            # Persist sequence for recovery
            pass

        def retrieve_initial_sequence(self):
            # Return stored sequence or -1 for latest
            return -1

        def is_loss_tolerant(self):
            return True

    reliable_topic = client.get_reliable_topic("events")
    reliable_topic.add_message_listener(MyListener())
    reliable_topic.publish("Reliable message")

PNCounter
---------

CRDT counter supporting increment and decrement with eventual consistency.

.. code-block:: python

    counter = client.get_pn_counter("page-views")

    # Increment/decrement
    new_val = counter.increment_and_get()
    new_val = counter.decrement_and_get()
    new_val = counter.add_and_get(10)
    new_val = counter.subtract_and_get(5)

    # Get previous value
    old_val = counter.get_and_increment()
    old_val = counter.get_and_add(10)

    # Current value
    current = counter.get()

CardinalityEstimator
--------------------

HyperLogLog-based distinct count estimation.

.. code-block:: python

    estimator = client.get_cardinality_estimator("unique-visitors")

    # Add values
    estimator.add("user-1")
    estimator.add("user-2")
    estimator.add("user-1")  # Duplicate

    # Estimate distinct count
    count = estimator.estimate().result()  # ~2
