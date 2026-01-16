CP Subsystem
============

The CP Subsystem provides strongly consistent (CP in CAP terms) distributed
data structures using the Raft consensus algorithm.

.. note::

    The CP Subsystem requires at least 3 cluster members configured as
    CP members for fault tolerance.

AtomicLong
----------

A distributed atomic counter with strong consistency:

.. code-block:: python

    atomic_long = client.get_atomic_long("sequence-generator")

    # Set initial value
    atomic_long.set(0)

    # Increment/decrement
    new_val = atomic_long.increment_and_get()
    new_val = atomic_long.decrement_and_get()

    # Add/subtract
    new_val = atomic_long.add_and_get(10)
    old_val = atomic_long.get_and_add(5)

    # Get current value
    current = atomic_long.get()

    # Compare and set
    success = atomic_long.compare_and_set(expected=15, update=20)

    # Async operations
    future = atomic_long.increment_and_get_async()
    result = future.result()

AtomicReference
---------------

A distributed atomic reference for storing objects:

.. code-block:: python

    atomic_ref = client.get_atomic_reference("config")

    # Set value
    atomic_ref.set({"timeout": 30, "retries": 3})

    # Get value
    config = atomic_ref.get()

    # Compare and set
    old_config = {"timeout": 30, "retries": 3}
    new_config = {"timeout": 60, "retries": 5}
    success = atomic_ref.compare_and_set(old_config, new_config)

    # Get and set
    previous = atomic_ref.get_and_set(new_config)

    # Set if absent
    atomic_ref.set_if_absent(default_config)

    # Check if null
    is_null = atomic_ref.is_null()

    # Clear
    atomic_ref.clear()

FencedLock
----------

A distributed mutex with fencing token support:

.. code-block:: python

    lock = client.get_fenced_lock("resource-lock")

    # Acquire lock
    fence = lock.lock()
    try:
        # Critical section
        print(f"Lock acquired with fence token: {fence}")
        # ... do work ...
    finally:
        lock.unlock()

    # Try lock with timeout
    fence = lock.try_lock(timeout=5.0)
    if fence:
        try:
            # ...
        finally:
            lock.unlock()

    # Context manager
    with lock as fence:
        print(f"Fence token: {fence}")
        # Critical section

Fencing tokens prevent issues with long GC pauses or network delays:

.. code-block:: python

    fence1 = lock.lock()
    # ... client appears to hang ...
    # lock times out, another client acquires:
    fence2 = lock.lock()  # fence2 > fence1

    # Operations can validate fence tokens to ensure
    # they're working with the current lock holder

Semaphore
---------

A distributed counting semaphore:

.. code-block:: python

    semaphore = client.get_semaphore("connection-pool")

    # Initialize permits
    semaphore.init(10)

    # Acquire permits
    semaphore.acquire()  # Acquire 1
    semaphore.acquire(3)  # Acquire 3

    # Try acquire
    acquired = semaphore.try_acquire()
    acquired = semaphore.try_acquire(2, timeout=5.0)

    # Release permits
    semaphore.release()
    semaphore.release(3)

    # Check available
    available = semaphore.available_permits()

    # Drain all permits
    drained = semaphore.drain_permits()

    # Reduce permits
    semaphore.reduce_permits(2)

    # Increase permits
    semaphore.increase_permits(5)

CountDownLatch
--------------

A distributed countdown latch for synchronization:

.. code-block:: python

    latch = client.get_count_down_latch("startup-latch")

    # Initialize count
    success = latch.try_set_count(3)

    # In worker processes:
    # ... do initialization work ...
    latch.count_down()

    # In main process, wait for all workers:
    latch.await_latch()  # Blocks until count reaches 0

    # With timeout
    completed = latch.await_latch(timeout=30.0)
    if not completed:
        print("Timeout waiting for workers")

    # Check current count
    count = latch.get_count()

Use Cases
---------

Distributed Sequence Generator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    def get_next_id() -> int:
        counter = client.get_atomic_long("id-sequence")
        return counter.increment_and_get()

Distributed Rate Limiter
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    def check_rate_limit(user_id: str, limit: int) -> bool:
        semaphore = client.get_semaphore(f"rate-limit:{user_id}")

        # Initialize on first use
        if semaphore.available_permits() == 0:
            semaphore.init(limit)

        # Try to acquire a permit
        return semaphore.try_acquire(timeout=0)

Leader Election
~~~~~~~~~~~~~~~

.. code-block:: python

    def try_become_leader() -> bool:
        lock = client.get_fenced_lock("leader-lock")
        fence = lock.try_lock(timeout=0)
        return fence is not None

    def do_leader_work():
        lock = client.get_fenced_lock("leader-lock")
        with lock:
            # Only one instance executes this at a time
            process_as_leader()

Distributed Barrier
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    def distributed_barrier(participant_count: int):
        latch = client.get_count_down_latch("barrier")

        # First participant initializes
        latch.try_set_count(participant_count)

        # Each participant counts down when ready
        latch.count_down()

        # Wait for all participants
        latch.await_latch()
        print("All participants ready, proceeding...")
