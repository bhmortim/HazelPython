CP Subsystem
============

The CP (Consensus Protocol) Subsystem provides distributed data structures
with strong consistency guarantees using the Raft consensus algorithm.

.. note::

   CP Subsystem requires a Hazelcast cluster with at least 3 members.

AtomicLong
----------

A distributed atomic counter with strong consistency.

.. code-block:: python

   counter = client.get_atomic_long("sequence-generator")

   # Set and get
   counter.set(0)
   value = counter.get()

   # Increment/decrement
   value = counter.increment_and_get()
   value = counter.get_and_increment()
   value = counter.decrement_and_get()
   value = counter.get_and_decrement()

   # Add/subtract
   value = counter.add_and_get(10)
   value = counter.get_and_add(10)
   value = counter.subtract_and_get(5)
   value = counter.get_and_subtract(5)

   # Compare and set (CAS)
   success = counter.compare_and_set(expected=10, update=20)

   # Alter with function
   counter.alter(lambda x: x * 2)
   value = counter.alter_and_get(lambda x: x + 1)
   value = counter.get_and_alter(lambda x: x - 1)

   # Apply (read-only)
   result = counter.apply(lambda x: f"Value is {x}")


AtomicReference
---------------

A distributed atomic object reference.

.. code-block:: python

   ref = client.get_atomic_reference("config")

   # Set and get
   ref.set({"version": "1.0", "enabled": True})
   config = ref.get()

   # Get and set
   old_config = ref.get_and_set({"version": "2.0"})

   # Compare and set
   current = ref.get()
   new_config = {"version": "2.1"}
   success = ref.compare_and_set(current, new_config)

   # Check contents
   has_value = ref.contains(new_config)
   is_empty = ref.is_null()

   # Clear
   ref.clear()


FencedLock
----------

A distributed lock with fencing token support for safe ownership verification.

Basic Usage
~~~~~~~~~~~

.. code-block:: python

   lock = client.get_fenced_lock("resource-lock")

   # Acquire lock
   fence_token = lock.lock()
   try:
       # Critical section
       print(f"Lock acquired with fence: {fence_token}")
   finally:
       lock.unlock()

Context Manager
~~~~~~~~~~~~~~~

.. code-block:: python

   lock = client.get_fenced_lock("resource-lock")

   with lock as fence_token:
       # Critical section
       print(f"Working with fence: {fence_token}")
   # Lock automatically released

Try Lock
~~~~~~~~

.. code-block:: python

   lock = client.get_fenced_lock("resource-lock")

   # Non-blocking
   if lock.try_lock():
       try:
           # Got the lock
           pass
       finally:
           lock.unlock()
   else:
       print("Lock not available")

   # With timeout
   if lock.try_lock(timeout=5.0):
       try:
           # Got the lock within 5 seconds
           pass
       finally:
           lock.unlock()

Lock Status
~~~~~~~~~~~

.. code-block:: python

   lock = client.get_fenced_lock("resource-lock")

   lock.lock()
   print(f"Is locked: {lock.is_locked()}")
   print(f"Is locked by me: {lock.is_locked_by_current_thread()}")
   print(f"Lock count: {lock.get_lock_count()}")

Reentrant Locking
~~~~~~~~~~~~~~~~~

FencedLock supports reentrant acquisition:

.. code-block:: python

   lock = client.get_fenced_lock("resource-lock")

   lock.lock()  # count = 1
   lock.lock()  # count = 2 (reentrant)

   lock.unlock()  # count = 1
   lock.unlock()  # count = 0 (released)


Semaphore
---------

A distributed counting semaphore.

.. code-block:: python

   sem = client.get_semaphore("connection-pool")

   # Initialize permits (only once)
   sem.init(10)  # 10 permits

   # Acquire permits
   sem.acquire(1)
   sem.acquire(2)  # Acquire multiple

   # Release permits
   sem.release(1)
   sem.release(2)

   # Try acquire (non-blocking)
   if sem.try_acquire(1):
       try:
           # Got a permit
           pass
       finally:
           sem.release(1)

   # Try acquire with timeout
   if sem.try_acquire(1, timeout=5.0):
       try:
           # Got a permit within 5 seconds
           pass
       finally:
           sem.release(1)

   # Check available permits
   available = sem.available_permits()

   # Modify permit count
   sem.reduce_permits(2)
   sem.increase_permits(5)

   # Drain all permits
   drained = sem.drain_permits()


CountDownLatch
--------------

A distributed synchronization barrier.

.. code-block:: python

   latch = client.get_count_down_latch("startup-latch")

   # Set count (typically done once by coordinator)
   latch.try_set_count(3)

   # Check count
   count = latch.get_count()

   # Count down (done by workers)
   latch.count_down()

   # Wait for count to reach zero
   latch.await_latch()  # Blocking wait
   success = latch.await_latch(timeout=30.0)  # With timeout

Example: Multi-Service Startup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import threading

   # Coordinator sets up the latch
   latch = client.get_count_down_latch("services-ready")
   latch.try_set_count(3)

   # Each service counts down when ready
   def start_database():
       # Initialize database...
       latch.count_down()

   def start_cache():
       # Initialize cache...
       latch.count_down()

   def start_message_queue():
       # Initialize MQ...
       latch.count_down()

   # Start services in threads
   threads = [
       threading.Thread(target=start_database),
       threading.Thread(target=start_cache),
       threading.Thread(target=start_message_queue),
   ]
   for t in threads:
       t.start()

   # Main thread waits for all services
   if latch.await_latch(timeout=60.0):
       print("All services started!")
   else:
       print("Timeout waiting for services")


CP Groups
---------

CP structures belong to CP groups. By default, they use the "default" group.

.. code-block:: python

   # Default group (implicitly)
   counter = client.get_atomic_long("my-counter")

   # The underlying CP group can be accessed via the proxy
   # group_id = counter.group_id


Best Practices
--------------

1. **Use FencedLock fence tokens**: Always check fence tokens when
   accessing protected resources to detect stale locks.

2. **Initialize Semaphores once**: Call ``init()`` only once per semaphore
   name, typically at application startup.

3. **Set appropriate timeouts**: Use timeouts with ``try_lock()`` and
   ``await_latch()`` to prevent deadlocks.

4. **Handle failures gracefully**: CP operations can fail if the CP
   subsystem loses quorum.

5. **Size your CP group appropriately**: Minimum 3 members, 5 or 7
   recommended for production.
