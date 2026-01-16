"""Integration tests for CP Subsystem primitives against a real Hazelcast cluster."""

import pytest
import time
import threading
from typing import List

from tests.integration.conftest import skip_integration, DOCKER_AVAILABLE


@skip_integration
class TestAtomicLongOperations:
    """Test AtomicLong operations."""

    def test_get_and_set(self, connected_client, unique_name):
        """Test get and set operations."""
        atomic = connected_client.get_atomic_long(unique_name)
        
        atomic.set(42)
        result = atomic.get()
        
        assert result == 42

    def test_increment_and_get(self, connected_client, unique_name):
        """Test increment_and_get operation."""
        atomic = connected_client.get_atomic_long(unique_name)
        
        atomic.set(10)
        result = atomic.increment_and_get()
        
        assert result == 11
        assert atomic.get() == 11

    def test_get_and_increment(self, connected_client, unique_name):
        """Test get_and_increment operation."""
        atomic = connected_client.get_atomic_long(unique_name)
        
        atomic.set(10)
        result = atomic.get_and_increment()
        
        assert result == 10
        assert atomic.get() == 11

    def test_decrement_and_get(self, connected_client, unique_name):
        """Test decrement_and_get operation."""
        atomic = connected_client.get_atomic_long(unique_name)
        
        atomic.set(10)
        result = atomic.decrement_and_get()
        
        assert result == 9

    def test_get_and_decrement(self, connected_client, unique_name):
        """Test get_and_decrement operation."""
        atomic = connected_client.get_atomic_long(unique_name)
        
        atomic.set(10)
        result = atomic.get_and_decrement()
        
        assert result == 10
        assert atomic.get() == 9

    def test_add_and_get(self, connected_client, unique_name):
        """Test add_and_get operation."""
        atomic = connected_client.get_atomic_long(unique_name)
        
        atomic.set(10)
        result = atomic.add_and_get(5)
        
        assert result == 15

    def test_get_and_add(self, connected_client, unique_name):
        """Test get_and_add operation."""
        atomic = connected_client.get_atomic_long(unique_name)
        
        atomic.set(10)
        result = atomic.get_and_add(5)
        
        assert result == 10
        assert atomic.get() == 15

    def test_compare_and_set_success(self, connected_client, unique_name):
        """Test successful compare_and_set."""
        atomic = connected_client.get_atomic_long(unique_name)
        
        atomic.set(10)
        result = atomic.compare_and_set(10, 20)
        
        assert result is True
        assert atomic.get() == 20

    def test_compare_and_set_failure(self, connected_client, unique_name):
        """Test failed compare_and_set."""
        atomic = connected_client.get_atomic_long(unique_name)
        
        atomic.set(10)
        result = atomic.compare_and_set(99, 20)
        
        assert result is False
        assert atomic.get() == 10

    def test_get_and_set(self, connected_client, unique_name):
        """Test get_and_set operation."""
        atomic = connected_client.get_atomic_long(unique_name)
        
        atomic.set(10)
        result = atomic.get_and_set(20)
        
        assert result == 10
        assert atomic.get() == 20


@skip_integration
class TestAtomicLongConcurrency:
    """Test concurrent AtomicLong operations."""

    def test_concurrent_increments(self, connected_client, unique_name):
        """Test concurrent increment operations."""
        atomic = connected_client.get_atomic_long(unique_name)
        atomic.set(0)
        errors: List[Exception] = []
        
        def incrementer():
            try:
                for _ in range(100):
                    atomic.increment_and_get()
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=incrementer) for _ in range(5)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert atomic.get() == 500


@skip_integration
class TestAtomicReferenceOperations:
    """Test AtomicReference operations."""

    def test_get_and_set(self, connected_client, unique_name):
        """Test get and set operations."""
        ref = connected_client.get_atomic_reference(unique_name)
        
        ref.set("initial")
        result = ref.get()
        
        assert result == "initial"

    def test_compare_and_set_success(self, connected_client, unique_name):
        """Test successful compare_and_set."""
        ref = connected_client.get_atomic_reference(unique_name)
        
        ref.set("old")
        result = ref.compare_and_set("old", "new")
        
        assert result is True
        assert ref.get() == "new"

    def test_compare_and_set_failure(self, connected_client, unique_name):
        """Test failed compare_and_set."""
        ref = connected_client.get_atomic_reference(unique_name)
        
        ref.set("old")
        result = ref.compare_and_set("wrong", "new")
        
        assert result is False
        assert ref.get() == "old"

    def test_get_and_set(self, connected_client, unique_name):
        """Test get_and_set operation."""
        ref = connected_client.get_atomic_reference(unique_name)
        
        ref.set("old")
        result = ref.get_and_set("new")
        
        assert result == "old"
        assert ref.get() == "new"

    def test_is_null(self, connected_client, unique_name):
        """Test is_null operation."""
        ref = connected_client.get_atomic_reference(unique_name)
        
        assert ref.is_null() is True
        
        ref.set("value")
        assert ref.is_null() is False

    def test_clear(self, connected_client, unique_name):
        """Test clear operation."""
        ref = connected_client.get_atomic_reference(unique_name)
        
        ref.set("value")
        ref.clear()
        
        assert ref.is_null() is True

    def test_contains_value(self, connected_client, unique_name):
        """Test contains operation."""
        ref = connected_client.get_atomic_reference(unique_name)
        
        ref.set("value")
        
        assert ref.contains("value") is True
        assert ref.contains("other") is False


@skip_integration
class TestCountDownLatchOperations:
    """Test CountDownLatch operations."""

    def test_count_down_and_await(self, connected_client, unique_name):
        """Test count_down and await operations."""
        latch = connected_client.get_count_down_latch(unique_name)
        
        latch.try_set_count(3)
        assert latch.get_count() == 3
        
        latch.count_down()
        assert latch.get_count() == 2
        
        latch.count_down()
        latch.count_down()
        
        result = latch.await_latch(timeout=1)
        assert result is True

    def test_await_timeout(self, connected_client, unique_name):
        """Test await with timeout."""
        latch = connected_client.get_count_down_latch(unique_name)
        
        latch.try_set_count(5)
        
        result = latch.await_latch(timeout=0.5)
        assert result is False

    def test_concurrent_count_down(self, connected_client, unique_name):
        """Test concurrent count_down operations."""
        latch = connected_client.get_count_down_latch(unique_name)
        latch.try_set_count(10)
        
        def counter():
            for _ in range(2):
                latch.count_down()
                time.sleep(0.1)
        
        threads = [threading.Thread(target=counter) for _ in range(5)]
        
        for t in threads:
            t.start()
        
        result = latch.await_latch(timeout=5)
        
        for t in threads:
            t.join()
        
        assert result is True
        assert latch.get_count() == 0


@skip_integration
class TestSemaphoreOperations:
    """Test Semaphore operations."""

    def test_acquire_and_release(self, connected_client, unique_name):
        """Test acquire and release operations."""
        sem = connected_client.get_semaphore(unique_name)
        
        sem.init(3)
        assert sem.available_permits() == 3
        
        sem.acquire()
        assert sem.available_permits() == 2
        
        sem.release()
        assert sem.available_permits() == 3

    def test_acquire_multiple(self, connected_client, unique_name):
        """Test acquiring multiple permits."""
        sem = connected_client.get_semaphore(unique_name)
        
        sem.init(5)
        sem.acquire(3)
        
        assert sem.available_permits() == 2

    def test_try_acquire_success(self, connected_client, unique_name):
        """Test successful try_acquire."""
        sem = connected_client.get_semaphore(unique_name)
        
        sem.init(1)
        result = sem.try_acquire()
        
        assert result is True
        assert sem.available_permits() == 0

    def test_try_acquire_failure(self, connected_client, unique_name):
        """Test failed try_acquire."""
        sem = connected_client.get_semaphore(unique_name)
        
        sem.init(0)
        result = sem.try_acquire(timeout=0.1)
        
        assert result is False

    def test_reduce_permits(self, connected_client, unique_name):
        """Test reduce_permits operation."""
        sem = connected_client.get_semaphore(unique_name)
        
        sem.init(5)
        sem.reduce_permits(2)
        
        assert sem.available_permits() == 3

    def test_increase_permits(self, connected_client, unique_name):
        """Test increase_permits operation."""
        sem = connected_client.get_semaphore(unique_name)
        
        sem.init(3)
        sem.increase_permits(2)
        
        assert sem.available_permits() == 5


@skip_integration
class TestFencedLockOperations:
    """Test FencedLock operations."""

    def test_lock_and_unlock(self, connected_client, unique_name):
        """Test lock and unlock operations."""
        lock = connected_client.get_lock(unique_name)
        
        fence = lock.lock()
        assert fence > 0
        assert lock.is_locked() is True
        
        lock.unlock(fence)
        assert lock.is_locked() is False

    def test_try_lock_success(self, connected_client, unique_name):
        """Test successful try_lock."""
        lock = connected_client.get_lock(unique_name)
        
        fence = lock.try_lock()
        assert fence > 0
        
        lock.unlock(fence)

    def test_try_lock_timeout(self, connected_client, unique_name):
        """Test try_lock with timeout when locked."""
        lock = connected_client.get_lock(unique_name)
        
        fence1 = lock.lock()
        
        def try_acquire():
            fence2 = lock.try_lock(timeout=0.5)
            return fence2
        
        thread = threading.Thread(target=try_acquire)
        thread.start()
        
        time.sleep(0.2)
        lock.unlock(fence1)
        
        thread.join()

    def test_lock_count(self, connected_client, unique_name):
        """Test lock count (reentrancy)."""
        lock = connected_client.get_lock(unique_name)
        
        fence1 = lock.lock()
        assert lock.get_lock_count() == 1
        
        fence2 = lock.lock()
        assert lock.get_lock_count() == 2
        
        lock.unlock(fence2)
        assert lock.get_lock_count() == 1
        
        lock.unlock(fence1)
        assert lock.get_lock_count() == 0


@skip_integration
class TestCPMapOperations:
    """Test CPMap operations."""

    def test_put_and_get(self, connected_client, unique_name):
        """Test put and get operations."""
        cp_map = connected_client.get_cp_map(unique_name)
        
        cp_map.put("key1", "value1")
        result = cp_map.get("key1")
        
        assert result == "value1"

    def test_put_if_absent(self, connected_client, unique_name):
        """Test put_if_absent operation."""
        cp_map = connected_client.get_cp_map(unique_name)
        
        result1 = cp_map.put_if_absent("key1", "value1")
        assert result1 is None
        
        result2 = cp_map.put_if_absent("key1", "value2")
        assert result2 == "value1"
        assert cp_map.get("key1") == "value1"

    def test_remove(self, connected_client, unique_name):
        """Test remove operation."""
        cp_map = connected_client.get_cp_map(unique_name)
        
        cp_map.put("key1", "value1")
        removed = cp_map.remove("key1")
        
        assert removed == "value1"
        assert cp_map.get("key1") is None

    def test_compare_and_set(self, connected_client, unique_name):
        """Test compare_and_set operation."""
        cp_map = connected_client.get_cp_map(unique_name)
        
        cp_map.put("key1", "old")
        
        result1 = cp_map.compare_and_set("key1", "old", "new")
        assert result1 is True
        assert cp_map.get("key1") == "new"
        
        result2 = cp_map.compare_and_set("key1", "wrong", "newer")
        assert result2 is False
        assert cp_map.get("key1") == "new"

    def test_set(self, connected_client, unique_name):
        """Test set operation."""
        cp_map = connected_client.get_cp_map(unique_name)
        
        old = cp_map.set("key1", "value1")
        assert old is None
        
        old = cp_map.set("key1", "value2")
        assert old == "value1"

    def test_delete(self, connected_client, unique_name):
        """Test delete operation."""
        cp_map = connected_client.get_cp_map(unique_name)
        
        cp_map.put("key1", "value1")
        cp_map.delete("key1")
        
        assert cp_map.get("key1") is None
