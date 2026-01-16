hazelcast.proxy.ringbuffer
==========================

Distributed Ringbuffer data structure proxy.

A ``Ringbuffer`` is a fixed-capacity, circular data structure that overwrites
the oldest elements when full. It provides:

- **Bounded storage** with automatic eviction of old data
- **Sequence-based access** for reliable reads
- **Batch operations** for efficient data transfer
- **Replay capability** to re-read historical data

Ringbuffers are ideal for event streams, audit logs, and time-series data
where you want to keep a sliding window of recent data.

Usage Examples
--------------

Basic Operations
~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Get or create a Ringbuffer
   ringbuffer = client.get_ringbuffer("events")
   
   # Add items (returns the sequence number)
   seq1 = ringbuffer.add("event-1")
   seq2 = ringbuffer.add("event-2")
   seq3 = ringbuffer.add("event-3")
   
   print(f"Added at sequences: {seq1}, {seq2}, {seq3}")
   
   # Read single item by sequence
   item = ringbuffer.read_one(seq1)
   
   # Get capacity and size
   capacity = ringbuffer.capacity()
   size = ringbuffer.size()
   size = len(ringbuffer)  # Pythonic alternative
   
   # Get sequence bounds
   head = ringbuffer.head_sequence()  # Oldest available
   tail = ringbuffer.tail_sequence()  # Newest available
   
   client.shutdown()

Batch Operations
~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.proxy.ringbuffer import OverflowPolicy

   # Add multiple items at once
   sequences = ringbuffer.add_all(
       ["event-a", "event-b", "event-c"],
       overflow_policy=OverflowPolicy.OVERWRITE
   )
   
   # Read multiple items starting from a sequence
   result = ringbuffer.read_many(
       start_sequence=head,
       min_count=1,
       max_count=100
   )
   
   for item in result:
       print(item)
   
   # Get the next sequence to read
   next_seq = result.get_next_sequence_to_read_from()

Overflow Policies
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.proxy.ringbuffer import OverflowPolicy

   # OVERWRITE: Overwrites oldest when full (default)
   seq = ringbuffer.add("item", overflow_policy=OverflowPolicy.OVERWRITE)
   
   # FAIL: Returns -1 if buffer is full
   seq = ringbuffer.add("item", overflow_policy=OverflowPolicy.FAIL)
   if seq == -1:
       print("Ringbuffer is full!")

Event Stream Processing
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Producer: continuous event stream
   def produce_events(ringbuffer):
       for i in range(1000):
           event = {
               "id": i,
               "timestamp": time.time(),
               "data": f"payload-{i}"
           }
           ringbuffer.add(event)
           time.sleep(0.1)
   
   # Consumer: process events with cursor
   def consume_events(ringbuffer):
       sequence = ringbuffer.head_sequence()
       
       while True:
           # Read batch of events
           result = ringbuffer.read_many(
               start_sequence=sequence,
               min_count=1,
               max_count=50
           )
           
           for event in result:
               process_event(event)
           
           # Move cursor
           sequence = result.get_next_sequence_to_read_from()
           
           # If caught up, wait for more
           if result.size() == 0:
               time.sleep(0.5)

Audit Log Pattern
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Audit log that keeps last 10000 events
   audit_log = client.get_ringbuffer("audit-log")
   
   # Log events
   audit_log.add({
       "action": "user_login",
       "user_id": "user:123",
       "ip_address": "192.168.1.1",
       "timestamp": time.time()
   })
   
   # Query recent events
   tail = audit_log.tail_sequence()
   head = audit_log.head_sequence()
   
   # Read last 100 events
   start = max(head, tail - 99)
   recent_events = audit_log.read_many(start, 1, 100)
   
   for event in recent_events:
       print(f"{event['timestamp']}: {event['action']} by {event['user_id']}")

Time-Series Data
~~~~~~~~~~~~~~~~

.. code-block:: python

   # Store sensor readings
   sensor_data = client.get_ringbuffer("sensor:temperature")
   
   # Add reading
   sensor_data.add({
       "value": 23.5,
       "unit": "celsius",
       "timestamp": time.time()
   })
   
   # Get readings from last hour
   now = time.time()
   hour_ago = now - 3600
   
   result = sensor_data.read_many(
       sensor_data.head_sequence(),
       1,
       sensor_data.capacity()
   )
   
   readings = [r for r in result if r["timestamp"] >= hour_ago]
   avg_temp = sum(r["value"] for r in readings) / len(readings)

Configuration Options
---------------------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Option
     - Default
     - Description
   * - ``capacity``
     - 10000
     - Maximum number of items in the Ringbuffer
   * - ``backup_count``
     - 1
     - Number of synchronous backups
   * - ``async_backup_count``
     - 0
     - Number of asynchronous backups
   * - ``time_to_live_seconds``
     - 0
     - TTL for items (0 = infinite)
   * - ``in_memory_format``
     - BINARY
     - Storage format (BINARY or OBJECT)

Best Practices
--------------

1. **Size Appropriately**: Set capacity based on your retention needs.
   Too small loses data; too large wastes memory.

2. **Use Batch Reads**: ``read_many()`` is more efficient than
   multiple ``read_one()`` calls.

3. **Track Sequences**: Persist the last read sequence for reliable
   consumers that can resume after restarts.

4. **Choose Overflow Policy**: Use ``OVERWRITE`` for logs/streams,
   ``FAIL`` when you need backpressure.

5. **Consider ReliableTopic**: For pub-sub patterns with reliable
   delivery, use ``ReliableTopic`` which uses Ringbuffer internally.

API Reference
-------------

.. automodule:: hazelcast.proxy.ringbuffer
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

RingbufferProxy
~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.ringbuffer.RingbufferProxy
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__, __len__

ReadResultSet
~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.ringbuffer.ReadResultSet
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__, __len__, __getitem__, __iter__

Enumerations
------------

OverflowPolicy
~~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.ringbuffer.OverflowPolicy
   :members:
   :undoc-members:
