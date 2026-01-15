"""CP Subsystem example for the Hazelcast Python client.

This example demonstrates:
- AtomicLong operations
- AtomicReference operations
- FencedLock for distributed locking
- CountDownLatch for coordination
- Semaphore for resource control
"""

from hazelcast import HazelcastClient, ClientConfig
from hazelcast.cp import (
    AtomicLong,
    AtomicReference,
    FencedLock,
    CountDownLatch,
    Semaphore,
)


def atomic_long_example():
    """Demonstrate AtomicLong operations."""
    print("--- AtomicLong Example ---")

    counter = AtomicLong("my-counter")

    # Basic operations
    counter.set(0)
    print(f"Initial value: {counter.get()}")

    counter.increment_and_get()
    print(f"After increment: {counter.get()}")

    old_value = counter.get_and_add(10)
    print(f"get_and_add(10): old={old_value}, new={counter.get()}")

    # Compare and set
    success = counter.compare_and_set(11, 20)
    print(f"compare_and_set(11, 20): success={success}, value={counter.get()}")

    # Alter with function
    counter.alter(lambda x: x * 2)
    print(f"After alter(x*2): {counter.get()}")

    # Apply function (read-only)
    result = counter.apply(lambda x: x + 100)
    print(f"apply(x+100): result={result}, value={counter.get()}")

    counter.destroy()


def atomic_reference_example():
    """Demonstrate AtomicReference operations."""
    print("\n--- AtomicReference Example ---")

    ref = AtomicReference("my-ref")

    # Set and get
    ref.set({"name": "Alice", "score": 100})
    print(f"Initial: {ref.get()}")

    # Compare and set
    old_value = ref.get()
    success = ref.compare_and_set(old_value, {"name": "Alice", "score": 150})
    print(f"CAS success: {success}, new value: {ref.get()}")

    # Alter
    ref.alter(lambda v: {**v, "score": v["score"] + 50} if v else None)
    print(f"After alter: {ref.get()}")

    # Is null check
    print(f"Is null: {ref.is_null()}")

    ref.clear()
    print(f"After clear, is null: {ref.is_null()}")

    ref.destroy()


def fenced_lock_example():
    """Demonstrate FencedLock operations."""
    print("\n--- FencedLock Example ---")

    lock = FencedLock("my-lock")

    # Acquire lock
    fence = lock.lock()
    print(f"Lock acquired with fence: {fence}")
    print(f"Is locked: {lock.is_locked()}")
    print(f"Lock count: {lock.get_lock_count()}")

    # Re-entrant locking
    fence2 = lock.lock()
    print(f"Re-entered with fence: {fence2}")
    print(f"Lock count after re-entry: {lock.get_lock_count()}")

    # Release locks
    lock.unlock()
    lock.unlock()
    print(f"Lock count after unlocks: {lock.get_lock_count()}")

    # Context manager usage
    print("\nUsing context manager:")
    with lock as f:
        print(f"  Inside lock with fence: {f}")
        print(f"  Is locked: {lock.is_locked()}")
    print(f"  After context: is_locked={lock.is_locked()}")

    lock.destroy()


def countdown_latch_example():
    """Demonstrate CountDownLatch operations."""
    print("\n--- CountDownLatch Example ---")

    latch = CountDownLatch("my-latch")

    # Try to set count
    success = latch.try_set_count(3)
    print(f"Set count to 3: {success}")
    print(f"Current count: {latch.get_count()}")

    # Count down
    latch.count_down()
    print(f"After count_down: {latch.get_count()}")

    latch.count_down()
    print(f"After count_down: {latch.get_count()}")

    # Await with timeout
    print("Waiting with short timeout...")
    reached_zero = latch.await_(timeout=0.1)
    print(f"Reached zero: {reached_zero}")

    # Final count down
    latch.count_down()
    print(f"Final count: {latch.get_count()}")

    reached_zero = latch.await_(timeout=0.1)
    print(f"Reached zero after final count_down: {reached_zero}")

    latch.destroy()


def semaphore_example():
    """Demonstrate Semaphore operations."""
    print("\n--- Semaphore Example ---")

    sem = Semaphore("my-semaphore", initial_permits=5)

    print(f"Initial permits: {sem.available_permits()}")

    # Acquire permits
    sem.acquire(2)
    print(f"After acquire(2): {sem.available_permits()}")

    # Try acquire
    success = sem.try_acquire(3, timeout=0.1)
    print(f"try_acquire(3): {success}, available: {sem.available_permits()}")

    # Release
    sem.release(2)
    print(f"After release(2): {sem.available_permits()}")

    # Drain
    drained = sem.drain_permits()
    print(f"Drained {drained} permits, available: {sem.available_permits()}")

    # Increase permits
    sem.increase_permits(10)
    print(f"After increase(10): {sem.available_permits()}")

    sem.destroy()


def main():
    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    client = HazelcastClient(config)

    try:
        client.start()
        print(f"Connected to cluster: {config.cluster_name}\n")

        atomic_long_example()
        atomic_reference_example()
        fenced_lock_example()
        countdown_latch_example()
        semaphore_example()

    finally:
        client.shutdown()


if __name__ == "__main__":
    main()
