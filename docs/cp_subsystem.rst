CP Subsystem
============

The CP (Consensus Protocol) Subsystem provides strongly consistent distributed
primitives using the Raft consensus algorithm. Unlike AP (Available/Partition-tolerant)
data structures, CP primitives guarantee linearizability.

.. note::

   The CP Subsystem requires a minimum of 3 cluster members to operate.
   Configure CP members in the Hazelcast cluster configuration.

AtomicLong
----------

A distributed atomic long counter with strong consistency:

.. code-block:: python

   from hazelcast import HazelcastClient

   with HazelcastClient() as client:
       counter = client.get_atomic_long("my-counter")
       
       # Set initial value
       counter.set(0)
       
       # Atomic increment
       new_value = counter.increment_and_get()
       old_value = counter.get_and_increment()
       
       # Add arbitrary value
       counter.add_and_get(10)
       counter.get_and_add(5)
       
       # Compare and set (CAS)
       success = counter.compare_and_set(expected=15, update=20)
       
       # Get current value
       current = counter.get()
       
       # Alter with function
       counter.alter(lambda x: x * 2)
       result = counter.alter_and_get(lambda x: x + 1)

AtomicReference
---------------

A distributed atomic reference for arbitrary objects:

.. code-block:: python

   config_ref = client.get_atomic_reference("app-config")
   
   # Set value
   config_ref.set({"version": "1.0", "features": ["a", "b"]})
   
   # Get value
   config = config_ref.get()
   
   # Compare and set
   old_config = config_ref.get()
   new_config = {**old_config, "version": "2.0"}
   success = config_ref.compare_and_set(old_config, new_config)
   
   # Clear
   config_ref.clear()
   
   # Check if null
   is_null = config_ref.is_null()

FencedLock
----------

A distributed mutex with fencing token support to prevent split-brain issues:

.. code-block:: python

   lock = client.get_fenced_lock("resource-lock")
   
   # Basic lock/unlock
   fence = lock.lock()
   try:
       # Critical section
       print(f"Lock acquired with fence token: {fence}")
       # Do protected work...
   finally:
       lock.unlock()
   
   # Context manager (recommended)
   with lock as fence:
       print(f"Lock acquired: {fence}")
       # Critical section
   
   # Try lock with timeout
   fence = lock.try_lock(timeout=5.0)
   if fence:
       try:
           # Acquired the lock
           pass
       finally:
           lock.unlock()
   else:
       print("Could not acquire lock")
   
   # Check lock status
   is_locked = lock.is_locked()
   is_mine = lock.is_locked_by_current_thread()
   lock_count = lock.get_lock_count()

**Fencing Tokens:**

Fencing tokens are monotonically increasing numbers that can be used to
detect stale lock holders. Pass the fence token to protected resources
to ensure only the current lock holder can make changes:

.. code-block:: python

   with lock as fence:
       # Use fence token when accessing protected resource
       update_resource(fence_token=fence, data=my_data)

Semaphore
---------

A distributed counting semaphore:

.. code-block:: python

   semaphore = client.get_semaphore("connection-pool")
   
   # Initialize with permits
   semaphore.init(10)  # Allow 10 concurrent connections
   
   # Acquire permit(s)
   semaphore.acquire()
   semaphore.acquire(permits=3)  # Acquire multiple
   
   # Try acquire with timeout
   acquired = semaphore.try_acquire(timeout=5.0)
   acquired = semaphore.try_acquire(permits=2, timeout=5.0)
   
   # Release permit(s)
   semaphore.release()
   semaphore.release(permits=3)
   
   # Check available permits
   available = semaphore.available_permits()
   
   # Drain all permits
   drained = semaphore.drain_permits()
   
   # Increase/decrease permits
   semaphore.increase_permits(5)
   semaphore.reduce_permits(2)

**Rate Limiting Example:**

.. code-block:: python

   rate_limiter = client.get_semaphore("api-rate-limiter")
   rate_limiter.init(100)  # 100 requests per period
   
   def make_api_call():
       if rate_limiter.try_acquire(timeout=1.0):
           try:
               # Make the API call
               pass
           finally:
               # Release after rate limit window
               # (typically done by a background task)
               pass
       else:
           raise RateLimitExceeded()

CountDownLatch
--------------

A distributed countdown latch for coordinating multiple processes:

.. code-block:: python

   latch = client.get_count_down_latch("startup-latch")
   
   # Initialize (typically by coordinator)
   success = latch.try_set_count(3)  # Wait for 3 services
   
   # Wait for countdown (blocking)
   latch.await_latch()
   latch.await_latch(timeout=30.0)  # With timeout
   
   # Count down (by each service when ready)
   latch.count_down()
   
   # Get current count
   remaining = latch.get_count()

**Coordinated Startup Example:**

.. code-block:: python

   # Coordinator
   startup_latch = client.get_count_down_latch("app-startup")
   startup_latch.try_set_count(3)
   
   print("Waiting for all services...")
   startup_latch.await_latch(timeout=60.0)
   print("All services ready!")

   # Service 1, 2, 3 (each runs this)
   startup_latch = client.get_count_down_latch("app-startup")
   
   # Do initialization...
   initialize_service()
   
   # Signal ready
   startup_latch.count_down()
   print("Service ready")

CPMap
-----

A distributed map with strong consistency (CP):

.. code-block:: python

   cp_map = client.get_cp_map("critical-config")
   
   # Basic operations (similar to IMap but strongly consistent)
   cp_map.put("key", "value")
   value = cp_map.get("key")
   
   # Compare and set
   success = cp_map.compare_and_set("key", "value", "new-value")
   
   # Put if absent
   old = cp_map.put_if_absent("key2", "value2")
   
   # Remove
   cp_map.remove("key")

.. warning::

   CPMap has lower throughput than IMap due to consensus overhead.
   Use it only when strong consistency is required.

Best Practices
--------------

1. **Use CP for Coordination, AP for Data**
   
   Use CP primitives for coordination (locks, latches, counters) and
   AP data structures (IMap, IQueue) for application data.

2. **Keep Critical Sections Short**
   
   When holding a FencedLock, complete work quickly to avoid blocking
   other processes.

3. **Use Fencing Tokens**
   
   Always use fencing tokens when the lock protects external resources
   to prevent split-brain scenarios.

4. **Handle Timeouts**
   
   Always use timeouts with try_acquire/try_lock to prevent deadlocks.

5. **Monitor CP Health**
   
   Monitor CP group health through Management Center to detect issues
   early.
