hazelcast.cp.sync
=================

Synchronization primitives with strong consistency.

The CP Subsystem provides distributed synchronization primitives with
linearizable guarantees using the Raft consensus algorithm. These are
essential for coordinating access to shared resources across a cluster.

FencedLock
----------

A distributed reentrant lock with fencing token support. The fencing token
monotonically increases with each lock acquisition, enabling detection of
stale lock holders.

Usage Examples
~~~~~~~~~~~~~~

Basic Locking
^^^^^^^^^^^^^

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Get or create a FencedLock
   lock = client.cp_subsystem.get_lock("my-lock")
   
   # Acquire lock
   fence = lock.lock()
   try:
       # Critical section
       process_shared_resource()
   finally:
       lock.unlock()

Context Manager
^^^^^^^^^^^^^^^

.. code-block:: python

   # Pythonic lock usage with context manager
   lock = client.cp_subsystem.get_lock("my-lock")
   
   with lock:
       # Critical section - auto unlock on exit
       process_shared_resource()

Try Lock with Timeout
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Try to acquire lock with timeout
   fence = lock.try_lock(timeout=5.0)
   if fence:
       try:
           # Got the lock
           process_shared_resource()
       finally:
           lock.unlock()
   else:
       print("Could not acquire lock within timeout")

Fencing Tokens
^^^^^^^^^^^^^^

.. code-block:: python

   # Use fencing token to detect stale operations
   fence = lock.lock()
   
   # Pass fence token to external systems
   external_system.write(data, fence_token=fence)
   
   # External system can reject stale writes
   # by comparing fence tokens
   
   lock.unlock()

Reentrant Locking
^^^^^^^^^^^^^^^^^

.. code-block:: python

   # FencedLock is reentrant
   lock = client.cp_subsystem.get_lock("reentrant-lock")
   
   def outer():
       with lock:
           print("Outer acquired")
           inner()  # Same thread can acquire again
   
   def inner():
       with lock:  # Succeeds (reentrant)
           print("Inner acquired")
   
   outer()

Semaphore
---------

A distributed counting semaphore for limiting concurrent access to a resource.
Supports both session-based and sessionless semantics.

Usage Examples
~~~~~~~~~~~~~~

Basic Semaphore
^^^^^^^^^^^^^^^

.. code-block:: python

   # Get or create a Semaphore
   semaphore = client.cp_subsystem.get_semaphore("connection-pool")
   
   # Initialize with permit count
   semaphore.init(10)  # Allow 10 concurrent connections
   
   # Acquire a permit
   semaphore.acquire()
   try:
       # Use the resource
       connection = create_connection()
       use_connection(connection)
   finally:
       # Release the permit
       semaphore.release()

Try Acquire with Timeout
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Try to acquire with timeout
   acquired = semaphore.try_acquire(timeout=5.0)
   if acquired:
       try:
           use_resource()
       finally:
           semaphore.release()
   else:
       print("Resource busy, try again later")

Multiple Permits
^^^^^^^^^^^^^^^^

.. code-block:: python

   # Acquire multiple permits at once
   semaphore.acquire(permits=3)
   try:
       # Using 3 resource units
       process_batch()
   finally:
       semaphore.release(permits=3)

Rate Limiting
^^^^^^^^^^^^^

.. code-block:: python

   # Use semaphore for rate limiting
   rate_limiter = client.cp_subsystem.get_semaphore("api-rate-limit")
   rate_limiter.init(100)  # 100 requests per interval
   
   def handle_request(request):
       acquired = rate_limiter.try_acquire(timeout=0)
       if not acquired:
           return {"error": "Rate limit exceeded"}, 429
       try:
           return process_request(request)
       finally:
           rate_limiter.release()
   
   # Periodic permit refresh (run in background)
   def refresh_permits():
       while True:
           time.sleep(60)  # Every minute
           current = rate_limiter.available_permits()
           rate_limiter.release(100 - current)

CountDownLatch
--------------

A distributed synchronization barrier that allows one or more threads to
wait until a set of operations completes.

Usage Examples
~~~~~~~~~~~~~~

Basic CountDownLatch
^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Get or create a CountDownLatch
   latch = client.cp_subsystem.get_count_down_latch("job-completion")
   
   # Initialize with count
   latch.try_set_count(3)  # Wait for 3 tasks
   
   # Worker threads count down
   def worker(task_id):
       process_task(task_id)
       latch.count_down()
   
   # Main thread waits
   threads = []
   for i in range(3):
       t = threading.Thread(target=worker, args=(i,))
       t.start()
       threads.append(t)
   
   # Wait for all workers to complete
   latch.await_latch(timeout=60.0)
   print("All tasks completed")

Distributed Job Coordination
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Coordinate distributed job across multiple nodes
   job_latch = client.cp_subsystem.get_count_down_latch("batch-job:123")
   job_latch.try_set_count(num_partitions)
   
   # Each worker node processes its partition
   def process_partition(partition_id):
       process(partition_id)
       job_latch.count_down()
   
   # Coordinator waits for all partitions
   if job_latch.await_latch(timeout=300.0):
       print("Batch job completed successfully")
   else:
       print("Batch job timed out")

Configuration Options
---------------------

CP Subsystem configuration (cluster-side):

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Option
     - Default
     - Description
   * - ``cp_member_count``
     - 0
     - Number of CP members (0 = disabled)
   * - ``group_size``
     - Same as cp_member_count
     - Raft group size (3, 5, or 7 recommended)
   * - ``session_ttl_seconds``
     - 300
     - Time before session expires
   * - ``session_heartbeat_interval_seconds``
     - 5
     - Heartbeat interval

Best Practices
--------------

1. **Use Fencing Tokens**: Always use the fencing token from ``FencedLock``
   when accessing external resources to prevent split-brain issues.

2. **Set Timeouts**: Always use timeouts with ``try_lock()`` and
   ``try_acquire()`` to prevent deadlocks.

3. **Release in Finally**: Always release locks and semaphores in a
   ``finally`` block or use context managers.

4. **Initialize Semaphores Once**: Call ``init()`` only once, typically
   during application startup.

5. **Monitor CP Health**: The CP Subsystem requires a quorum. Monitor
   cluster health to ensure availability.

API Reference
-------------

.. automodule:: hazelcast.cp.sync
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

FencedLock
~~~~~~~~~~

.. autoclass:: hazelcast.cp.sync.FencedLock
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__, __enter__, __exit__

Semaphore
~~~~~~~~~

.. autoclass:: hazelcast.cp.sync.Semaphore
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

CountDownLatch
~~~~~~~~~~~~~~

.. autoclass:: hazelcast.cp.sync.CountDownLatch
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
