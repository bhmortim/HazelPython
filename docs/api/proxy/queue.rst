hazelcast.proxy.queue
=====================

Distributed Queue data structure proxy.

A distributed ``Queue`` provides a FIFO (First-In-First-Out) data structure
with blocking operations. It is ideal for producer-consumer patterns where
multiple producers add items and multiple consumers process them.

Usage Examples
--------------

Basic Operations
~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Get or create a distributed Queue
   queue = client.get_queue("work-queue")
   
   # Add items (non-blocking)
   queue.offer("task-1")
   queue.offer("task-2")
   queue.offer("task-3")
   
   # Remove and return head (non-blocking)
   item = queue.poll()  # Returns "task-1" or None if empty
   
   # Peek at head without removing
   head = queue.peek()  # Returns "task-2" or None if empty
   
   # Check size
   size = queue.size()
   size = len(queue)  # Pythonic alternative
   
   client.shutdown()

Blocking Operations
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Producer
   def producer(queue):
       for i in range(100):
           # Block until space available (if queue has max size)
           queue.put(f"item-{i}")
   
   # Consumer
   def consumer(queue):
       while True:
           # Block until item available
           item = queue.take()
           process(item)

Timeout Operations
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Offer with timeout (returns False if queue full after timeout)
   success = queue.offer("item", timeout=5.0)
   
   # Poll with timeout (returns None if queue empty after timeout)
   item = queue.poll(timeout=5.0)

Bulk Operations
~~~~~~~~~~~~~~~

.. code-block:: python

   # Add multiple items
   queue.add_all(["task-1", "task-2", "task-3"])
   
   # Drain up to N items into a list
   items = queue.drain_to(max_elements=10)
   
   # Drain all items
   all_items = queue.drain_to()
   
   # Get remaining capacity (if bounded)
   remaining = queue.remaining_capacity()

Queue Listeners
~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.proxy.queue import ItemListener, ItemEventType

   def on_item_added(event):
       print(f"Added: {event.item}")
       print(f"Member: {event.member}")
   
   def on_item_removed(event):
       print(f"Removed: {event.item}")
   
   listener = ItemListener(
       added=on_item_added,
       removed=on_item_removed,
   )
   
   # Register listener
   reg_id = queue.add_listener(listener, include_value=True)
   
   # Later: remove listener
   queue.remove_listener(reg_id)

Producer-Consumer Pattern
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import threading
   from hazelcast import HazelcastClient

   client = HazelcastClient()
   queue = client.get_queue("job-queue")
   
   def producer():
       for i in range(100):
           job = {"id": i, "data": f"payload-{i}"}
           queue.put(job)
           print(f"Produced job {i}")
   
   def consumer(consumer_id):
       while True:
           job = queue.take()  # Blocks until available
           print(f"Consumer {consumer_id} processing job {job['id']}")
           # Process job...
   
   # Start producer
   producer_thread = threading.Thread(target=producer)
   producer_thread.start()
   
   # Start multiple consumers
   for i in range(3):
       t = threading.Thread(target=consumer, args=(i,), daemon=True)
       t.start()
   
   producer_thread.join()

Work Distribution
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Distribute work across cluster nodes
   # Each consumer on different nodes competes for items
   
   queue = client.get_queue("distributed-work")
   
   # Add work items
   for task in tasks:
       queue.offer(task)
   
   # Workers on any node can process
   while True:
       task = queue.poll(timeout=1.0)
       if task:
           process_task(task)
       else:
           # Check if more work is coming
           if should_stop():
               break

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
     - Maximum queue size (0 = unbounded)
   * - ``empty_queue_ttl``
     - -1
     - Seconds empty queue lives before destruction (-1 = never)
   * - ``statistics_enabled``
     - true
     - Enable statistics collection

Best Practices
--------------

1. **Set Max Size**: For production, set ``max_size`` to prevent
   unbounded memory growth when consumers are slow.

2. **Use Timeouts**: Prefer ``poll(timeout)`` and ``offer(timeout)``
   over blocking ``take()`` and ``put()`` for better control.

3. **Handle Failures**: Items taken from the queue are removed.
   Implement retry logic or dead-letter queues for failed processing.

4. **Monitor Queue Depth**: Use listeners or periodic ``size()`` checks
   to detect consumer lag.

5. **Consider Ringbuffer**: For pub-sub with replay capability, use
   ``Ringbuffer`` instead of ``Queue``.

6. **Idempotent Processing**: Design consumers to handle duplicate
   processing in case of failures and retries.

API Reference
-------------

.. automodule:: hazelcast.proxy.queue
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

QueueProxy
~~~~~~~~~~

.. autoclass:: hazelcast.proxy.queue.QueueProxy
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__, __contains__, __len__, __iter__

ItemEvent
~~~~~~~~~

.. autoclass:: hazelcast.proxy.queue.ItemEvent
   :members:
   :undoc-members:
   :show-inheritance:

ItemListener
~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.queue.ItemListener
   :members:
   :undoc-members:
   :show-inheritance:

Enumerations
------------

ItemEventType
~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.queue.ItemEventType
   :members:
   :undoc-members:
