"""CP Subsystem Data Structures Example.

This example demonstrates the CP (Consensus Protocol) subsystem data structures
which provide strong consistency guarantees using Raft consensus.

CP structures include:
- AtomicLong: Distributed atomic counter
- AtomicReference: Distributed atomic object reference
- FencedLock: Distributed mutex with fencing tokens
- Semaphore: Distributed counting semaphore
- CountDownLatch: Distributed synchronization barrier

Prerequisites:
    - A Hazelcast cluster with CP subsystem enabled (requires 3+ members)

Usage:
    python cp_structures.py
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from hazelcast import HazelcastClient, ClientConfig
from hazelcast.cp import (
    AtomicLong,
    AtomicReference,
    FencedLock,
    Semaphore,
    CountDownLatch,
    CPGroupId,
)


def atomic_long_example():
    """Demonstrate AtomicLong operations."""
    print("\n" + "=" * 50)
    print("AtomicLong Example")
    print("=" * 50)

    # Create an AtomicLong (standalone demo without cluster)
    group_id = CPGroupId("default")
    counter = AtomicLong("visitor-counter", group_id)

    # Basic operations
    print("\n--- Basic Operations ---")

    # Set initial value
    counter.set(0)
    print(f"Initial value: {counter.get()}")

    # Increment
    new_value = counter.increment_and_get()
    print(f"After increment_and_get(): {new_value}")

    # Get and increment
    old_value = counter.get_and_increment()
    print(f"get_and_increment() returned: {old_value}, current: {counter.get()}")

    # Add
    result = counter.add_and_get(10)
    print(f"After add_and_get(10): {result}")

    # Subtract
    result = counter.get_and_subtract(5)
    print(f"get_and_subtract(5) returned: {result}, current: {counter.get()}")

    # Compare and set
    print("\n--- Compare and Set ---")
    current = counter.get()
    success = counter.compare_and_set(current, 100)
    print(f"compare_and_set({current}, 100): {success}, current: {counter.get()}")

    # CAS with wrong expected value
    success = counter.compare_and_set(50, 200)
    print(f"compare_and_set(50, 200): {success}, current: {counter.get()}")

    # Alter with function
    print("\n--- Alter with Function ---")
    counter.alter(lambda x: x * 2)
    print(f"After alter(x * 2): {counter.get()}")

    result = counter.alter_and_get(lambda x: x + 1)
    print(f"alter_and_get(x + 1): {result}")

    result = counter.get_and_alter(lambda x: x - 50)
    print(f"get_and_alter(x - 50) returned: {result}, current: {counter.get()}")

    # Apply (read-only transformation)
    result = counter.apply(lambda x: f"Counter value is {x}")
    print(f"apply() result: {result}")


def atomic_reference_example():
    """Demonstrate AtomicReference operations."""
    print("\n" + "=" * 50)
    print("AtomicReference Example")
    print("=" * 50)

    group_id = CPGroupId("default")
    ref = AtomicReference("config-ref", group_id)

    # Set and get
    print("\n--- Basic Operations ---")

    config = {"version": "1.0", "debug": False, "max_connections": 100}
    ref.set(config)
    print(f"Set config: {ref.get()}")

    # Get and set
    old_config = ref.get_and_set({"version": "2.0", "debug": True})
    print(f"get_and_set() returned: {old_config}")
    print(f"Current config: {ref.get()}")

    # Compare and set
    print("\n--- Compare and Set ---")
    current = ref.get()
    new_config = {"version": "2.1", "debug": False, "max_connections": 200}

    success = ref.compare_and_set(current, new_config)
    print(f"compare_and_set() success: {success}")
    print(f"Current config: {ref.get()}")

    # Contains
    print("\n--- Contains Check ---")
    print(f"Contains new_config: {ref.contains(new_config)}")
    print(f"Contains old value: {ref.contains(config)}")

    # Is null
    print(f"Is null: {ref.is_null()}")

    # Clear
    ref.clear()
    print(f"After clear, is null: {ref.is_null()}")


def fenced_lock_example():
    """Demonstrate FencedLock for distributed mutual exclusion."""
    print("\n" + "=" * 50)
    print("FencedLock Example")
    print("=" * 50)

    group_id = CPGroupId("default")
    lock = FencedLock("resource-lock", group_id)

    # Basic lock/unlock
    print("\n--- Basic Lock/Unlock ---")

    fence = lock.lock()
    print(f"Lock acquired with fence token: {fence}")
    print(f"Is locked: {lock.is_locked()}")
    print(f"Is locked by current thread: {lock.is_locked_by_current_thread()}")
    print(f"Lock count: {lock.get_lock_count()}")

    lock.unlock()
    print(f"Lock released, is locked: {lock.is_locked()}")

    # Try lock with timeout
    print("\n--- Try Lock ---")

    success = lock.try_lock(timeout=1.0)
    print(f"try_lock(timeout=1.0): {success}")

    if success:
        print("Performing critical section work...")
        time.sleep(0.1)
        lock.unlock()
        print("Lock released")

    # Context manager usage
    print("\n--- Context Manager Usage ---")

    with lock as fence_token:
        print(f"Inside context manager, fence: {fence_token}")
        print(f"Lock count inside: {lock.get_lock_count()}")
        # Critical section code here

    print(f"After context manager, is locked: {lock.is_locked()}")

    # Reentrant locking
    print("\n--- Reentrant Locking ---")

    lock.lock()
    print(f"First lock, count: {lock.get_lock_count()}")

    lock.lock()
    print(f"Second lock (reentrant), count: {lock.get_lock_count()}")

    lock.unlock()
    print(f"First unlock, count: {lock.get_lock_count()}")

    lock.unlock()
    print(f"Second unlock, count: {lock.get_lock_count()}")

    # Fence token explanation
    print("\n--- Fence Token Explanation ---")
    print("""
    Fence tokens provide protection against lock "resurrection":
    - Each lock acquisition returns a monotonically increasing fence token
    - If a client holds an old fence token, it knows another client
      has acquired the lock in between
    - Use fence tokens to guard critical resources and detect stale locks
    """)


def semaphore_example():
    """Demonstrate Semaphore for controlling concurrent access."""
    print("\n" + "=" * 50)
    print("Semaphore Example")
    print("=" * 50)

    group_id = CPGroupId("default")
    sem = Semaphore("connection-pool", group_id, initial_permits=3)

    print("\n--- Basic Operations ---")
    print(f"Initial permits: {sem.available_permits()}")

    # Acquire permits
    sem.acquire(1)
    print(f"Acquired 1 permit, available: {sem.available_permits()}")

    sem.acquire(2)
    print(f"Acquired 2 more permits, available: {sem.available_permits()}")

    # Release permits
    sem.release(1)
    print(f"Released 1 permit, available: {sem.available_permits()}")

    sem.release(2)
    print(f"Released 2 permits, available: {sem.available_permits()}")

    # Try acquire
    print("\n--- Try Acquire ---")

    success = sem.try_acquire(2)
    print(f"try_acquire(2): {success}, available: {sem.available_permits()}")

    success = sem.try_acquire(5)  # More than available
    print(f"try_acquire(5): {success}, available: {sem.available_permits()}")

    sem.release(2)

    # Try acquire with timeout
    success = sem.try_acquire(1, timeout=0.5)
    print(f"try_acquire(1, timeout=0.5): {success}")
    if success:
        sem.release(1)

    # Reduce/increase permits
    print("\n--- Modify Permit Count ---")

    print(f"Current permits: {sem.available_permits()}")
    sem.reduce_permits(1)
    print(f"After reduce_permits(1): {sem.available_permits()}")

    sem.increase_permits(2)
    print(f"After increase_permits(2): {sem.available_permits()}")

    # Drain permits
    drained = sem.drain_permits()
    print(f"drain_permits() returned: {drained}, available: {sem.available_permits()}")

    # Restore for demo
    sem.increase_permits(3)

    # Simulated concurrent access
    print("\n--- Simulated Resource Pool ---")
    print("Simulating 3 permits for a connection pool with 5 workers")

    def worker(worker_id: int, semaphore: Semaphore):
        acquired = semaphore.try_acquire(1, timeout=0.1)
        if acquired:
            print(f"  Worker {worker_id}: acquired connection")
            time.sleep(0.05)  # Simulate work
            semaphore.release(1)
            print(f"  Worker {worker_id}: released connection")
        else:
            print(f"  Worker {worker_id}: no connection available")

    with ThreadPoolExecutor(max_workers=5) as executor:
        for i in range(5):
            executor.submit(worker, i, sem)

    time.sleep(0.5)  # Wait for workers


def count_down_latch_example():
    """Demonstrate CountDownLatch for synchronization."""
    print("\n" + "=" * 50)
    print("CountDownLatch Example")
    print("=" * 50)

    group_id = CPGroupId("default")
    latch = CountDownLatch("startup-latch", group_id)

    # Set count
    print("\n--- Basic Operations ---")

    success = latch.try_set_count(3)
    print(f"try_set_count(3): {success}")
    print(f"Current count: {latch.get_count()}")

    # Count down
    latch.count_down()
    print(f"After count_down(): {latch.get_count()}")

    latch.count_down()
    print(f"After count_down(): {latch.get_count()}")

    latch.count_down()
    print(f"After count_down(): {latch.get_count()}")

    # Await (non-blocking check since count is 0)
    print("\n--- Await Example ---")
    latch.try_set_count(2)
    print(f"Set count to 2")

    # Try await with timeout (will timeout since count > 0)
    result = latch.await_latch(timeout=0.1)
    print(f"await_latch(timeout=0.1): {result}")

    # Count down to 0
    latch.count_down()
    latch.count_down()
    print(f"Counted down to: {latch.get_count()}")

    # Now await should succeed
    result = latch.await_latch(timeout=0.1)
    print(f"await_latch(timeout=0.1) after count=0: {result}")

    # Simulated multi-service startup
    print("\n--- Multi-Service Startup Simulation ---")
    print("Simulating 3 services that must start before main continues")

    startup_latch = CountDownLatch("service-startup", group_id)
    startup_latch.try_set_count(3)

    def start_service(service_name: str, latch: CountDownLatch, delay: float):
        print(f"  {service_name}: starting...")
        time.sleep(delay)
        print(f"  {service_name}: started!")
        latch.count_down()

    services = [
        ("Database", 0.1),
        ("Cache", 0.15),
        ("MessageQueue", 0.2),
    ]

    with ThreadPoolExecutor(max_workers=3) as executor:
        for name, delay in services:
            executor.submit(start_service, name, startup_latch, delay)

    # Wait for all services
    print("  Main: waiting for all services...")
    startup_latch.await_latch(timeout=1.0)
    print("  Main: all services started, proceeding!")


def main():
    print("CP Subsystem Data Structures Examples")
    print("=" * 50)
    print("""
Note: These examples demonstrate CP structures standalone.
In production, CP structures require a Hazelcast cluster with
at least 3 members for the Raft consensus protocol.

CP structures provide:
- Strong consistency (linearizability)
- Partition tolerance
- Availability within the CP group
    """)

    # Run examples
    atomic_long_example()
    atomic_reference_example()
    fenced_lock_example()
    semaphore_example()
    count_down_latch_example()

    print("\n" + "=" * 50)
    print("All CP examples completed!")
    print("=" * 50)


if __name__ == "__main__":
    main()
