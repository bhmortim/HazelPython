"""Unit tests for hazelcast.cp.sync module."""

import pytest
import threading
import time
from concurrent.futures import Future
from unittest.mock import Mock, patch

from hazelcast.cp.sync import CountDownLatch, Semaphore, FencedLock
from hazelcast.exceptions import IllegalArgumentException, IllegalStateException


class TestCountDownLatch:
    """Tests for CountDownLatch class."""

    def test_service_name_constant(self):
        assert CountDownLatch.SERVICE_NAME == "hz:raft:countDownLatchService"

    def test_initial_count_is_zero(self):
        latch = CountDownLatch("test")
        assert latch._count == 0

    def test_try_set_count_when_zero_success(self):
        latch = CountDownLatch("test")
        result = latch.try_set_count(5)
        assert result is True
        assert latch._count == 5

    def test_try_set_count_when_non_zero_failure(self):
        latch = CountDownLatch("test")
        latch._count = 3
        result = latch.try_set_count(5)
        assert result is False
        assert latch._count == 3

    def test_try_set_count_async_success(self):
        latch = CountDownLatch("test")
        future = latch.try_set_count_async(5)
        assert future.result() is True
        assert latch._count == 5

    def test_try_set_count_async_failure(self):
        latch = CountDownLatch("test")
        latch._count = 3
        future = latch.try_set_count_async(5)
        assert future.result() is False

    def test_try_set_count_negative_raises(self):
        latch = CountDownLatch("test")
        with pytest.raises(IllegalArgumentException) as exc_info:
            latch.try_set_count(-1)
        assert "count cannot be negative" in str(exc_info.value)

    def test_try_set_count_async_negative_raises(self):
        latch = CountDownLatch("test")
        with pytest.raises(IllegalArgumentException):
            latch.try_set_count_async(-1)

    def test_get_count(self):
        latch = CountDownLatch("test")
        latch._count = 5
        assert latch.get_count() == 5

    def test_get_count_async(self):
        latch = CountDownLatch("test")
        latch._count = 5
        future = latch.get_count_async()
        assert future.result() == 5

    def test_count_down_decrements(self):
        latch = CountDownLatch("test")
        latch._count = 3
        latch.count_down()
        assert latch._count == 2

    def test_count_down_async(self):
        latch = CountDownLatch("test")
        latch._count = 3
        future = latch.count_down_async()
        assert future.result() is None
        assert latch._count == 2

    def test_count_down_at_zero_does_nothing(self):
        latch = CountDownLatch("test")
        latch._count = 0
        latch.count_down()
        assert latch._count == 0

    def test_count_down_to_zero_notifies(self):
        latch = CountDownLatch("test")
        latch._count = 1
        
        notified = threading.Event()
        
        def waiter():
            with latch._condition:
                while latch._count > 0:
                    latch._condition.wait(timeout=1.0)
            notified.set()
        
        thread = threading.Thread(target=waiter)
        thread.start()
        
        time.sleep(0.05)
        latch.count_down()
        
        thread.join(timeout=1.0)
        assert notified.is_set()

    def test_await_with_timeout_success(self):
        latch = CountDownLatch("test")
        latch._count = 0
        result = latch.await_with_timeout(1.0)
        assert result is True

    def test_await_with_timeout_async_success(self):
        latch = CountDownLatch("test")
        latch._count = 0
        future = latch.await_with_timeout_async(1.0)
        assert future.result() is True

    def test_await_with_timeout_expires(self):
        latch = CountDownLatch("test")
        latch._count = 5
        result = latch.await_with_timeout(0.05)
        assert result is False

    def test_await_with_timeout_async_expires(self):
        latch = CountDownLatch("test")
        latch._count = 5
        future = latch.await_with_timeout_async(0.05)
        assert future.result() is False

    def test_await_with_timeout_negative_raises(self):
        latch = CountDownLatch("test")
        with pytest.raises(IllegalArgumentException) as exc_info:
            latch.await_with_timeout(-1)
        assert "timeout cannot be negative" in str(exc_info.value)

    def test_await_with_timeout_async_negative_raises(self):
        latch = CountDownLatch("test")
        with pytest.raises(IllegalArgumentException):
            latch.await_with_timeout_async(-1)

    def test_await_completes_when_count_reaches_zero(self):
        latch = CountDownLatch("test")
        latch._count = 2
        
        result_holder = [None]
        
        def waiter():
            result_holder[0] = latch.await_with_timeout(2.0)
        
        thread = threading.Thread(target=waiter)
        thread.start()
        
        time.sleep(0.05)
        latch.count_down()
        time.sleep(0.05)
        latch.count_down()
        
        thread.join(timeout=2.0)
        assert result_holder[0] is True

    def test_operations_on_destroyed_proxy_get_count(self):
        latch = CountDownLatch("test")
        latch.destroy()
        with pytest.raises(IllegalStateException):
            latch.get_count()

    def test_operations_on_destroyed_proxy_count_down(self):
        latch = CountDownLatch("test")
        latch.destroy()
        with pytest.raises(IllegalStateException):
            latch.count_down()

    def test_operations_on_destroyed_proxy_try_set_count(self):
        latch = CountDownLatch("test")
        latch.destroy()
        with pytest.raises(IllegalStateException):
            latch.try_set_count(5)


class TestSemaphore:
    """Tests for Semaphore class."""

    def test_service_name_constant(self):
        assert Semaphore.SERVICE_NAME == "hz:raft:semaphoreService"

    def test_initial_permits_is_zero(self):
        sem = Semaphore("test")
        assert sem._permits == 0

    def test_init_success_when_permits_zero(self):
        sem = Semaphore("test")
        result = sem.init(10)
        assert result is True
        assert sem._permits == 10

    def test_init_failure_when_permits_non_zero(self):
        sem = Semaphore("test")
        sem._permits = 5
        result = sem.init(10)
        assert result is False
        assert sem._permits == 5

    def test_init_async_success(self):
        sem = Semaphore("test")
        future = sem.init_async(10)
        assert future.result() is True
        assert sem._permits == 10

    def test_init_async_failure(self):
        sem = Semaphore("test")
        sem._permits = 5
        future = sem.init_async(10)
        assert future.result() is False

    def test_init_negative_raises(self):
        sem = Semaphore("test")
        with pytest.raises(IllegalArgumentException) as exc_info:
            sem.init(-1)
        assert "permits cannot be negative" in str(exc_info.value)

    def test_init_async_negative_raises(self):
        sem = Semaphore("test")
        with pytest.raises(IllegalArgumentException):
            sem.init_async(-1)

    def test_acquire_decrements_permits(self):
        sem = Semaphore("test")
        sem._permits = 5
        sem.acquire(2)
        assert sem._permits == 3

    def test_acquire_async(self):
        sem = Semaphore("test")
        sem._permits = 5
        future = sem.acquire_async(2)
        assert future.result() is None
        assert sem._permits == 3

    def test_acquire_default_one_permit(self):
        sem = Semaphore("test")
        sem._permits = 5
        sem.acquire()
        assert sem._permits == 4

    def test_acquire_negative_raises(self):
        sem = Semaphore("test")
        with pytest.raises(IllegalArgumentException) as exc_info:
            sem.acquire(-1)
        assert "permits cannot be negative" in str(exc_info.value)

    def test_acquire_async_negative_raises(self):
        sem = Semaphore("test")
        with pytest.raises(IllegalArgumentException):
            sem.acquire_async(-1)

    def test_acquire_blocks_when_not_enough_permits(self):
        sem = Semaphore("test")
        sem._permits = 1
        
        acquired = threading.Event()
        
        def acquire_two():
            sem.acquire(2)
            acquired.set()
        
        thread = threading.Thread(target=acquire_two)
        thread.start()
        
        time.sleep(0.05)
        assert not acquired.is_set()
        
        sem.release(1)
        thread.join(timeout=1.0)
        assert acquired.is_set()

    def test_try_acquire_immediate_success(self):
        sem = Semaphore("test")
        sem._permits = 5
        result = sem.try_acquire(2)
        assert result is True
        assert sem._permits == 3

    def test_try_acquire_immediate_failure(self):
        sem = Semaphore("test")
        sem._permits = 1
        result = sem.try_acquire(2)
        assert result is False
        assert sem._permits == 1

    def test_try_acquire_async_success(self):
        sem = Semaphore("test")
        sem._permits = 5
        future = sem.try_acquire_async(2)
        assert future.result() is True
        assert sem._permits == 3

    def test_try_acquire_async_failure(self):
        sem = Semaphore("test")
        sem._permits = 1
        future = sem.try_acquire_async(2)
        assert future.result() is False

    def test_try_acquire_with_timeout_success(self):
        sem = Semaphore("test")
        sem._permits = 5
        result = sem.try_acquire(2, timeout=1.0)
        assert result is True
        assert sem._permits == 3

    def test_try_acquire_with_timeout_expires(self):
        sem = Semaphore("test")
        sem._permits = 1
        result = sem.try_acquire(2, timeout=0.05)
        assert result is False
        assert sem._permits == 1

    def test_try_acquire_negative_permits_raises(self):
        sem = Semaphore("test")
        with pytest.raises(IllegalArgumentException) as exc_info:
            sem.try_acquire(-1)
        assert "permits cannot be negative" in str(exc_info.value)

    def test_try_acquire_negative_timeout_raises(self):
        sem = Semaphore("test")
        with pytest.raises(IllegalArgumentException) as exc_info:
            sem.try_acquire(1, timeout=-1)
        assert "timeout cannot be negative" in str(exc_info.value)

    def test_try_acquire_async_negative_permits_raises(self):
        sem = Semaphore("test")
        with pytest.raises(IllegalArgumentException):
            sem.try_acquire_async(-1)

    def test_try_acquire_async_negative_timeout_raises(self):
        sem = Semaphore("test")
        with pytest.raises(IllegalArgumentException):
            sem.try_acquire_async(1, timeout=-1)

    def test_release_increments_permits(self):
        sem = Semaphore("test")
        sem._permits = 3
        sem.release(2)
        assert sem._permits == 5

    def test_release_async(self):
        sem = Semaphore("test")
        sem._permits = 3
        future = sem.release_async(2)
        assert future.result() is None
        assert sem._permits == 5

    def test_release_default_one_permit(self):
        sem = Semaphore("test")
        sem._permits = 3
        sem.release()
        assert sem._permits == 4

    def test_release_negative_raises(self):
        sem = Semaphore("test")
        with pytest.raises(IllegalArgumentException) as exc_info:
            sem.release(-1)
        assert "permits cannot be negative" in str(exc_info.value)

    def test_release_async_negative_raises(self):
        sem = Semaphore("test")
        with pytest.raises(IllegalArgumentException):
            sem.release_async(-1)

    def test_available_permits(self):
        sem = Semaphore("test")
        sem._permits = 7
        assert sem.available_permits() == 7

    def test_available_permits_async(self):
        sem = Semaphore("test")
        sem._permits = 7
        future = sem.available_permits_async()
        assert future.result() == 7

    def test_drain_permits(self):
        sem = Semaphore("test")
        sem._permits = 7
        drained = sem.drain_permits()
        assert drained == 7
        assert sem._permits == 0

    def test_drain_permits_async(self):
        sem = Semaphore("test")
        sem._permits = 7
        future = sem.drain_permits_async()
        assert future.result() == 7
        assert sem._permits == 0

    def test_reduce_permits(self):
        sem = Semaphore("test")
        sem._permits = 10
        sem.reduce_permits(3)
        assert sem._permits == 7

    def test_reduce_permits_async(self):
        sem = Semaphore("test")
        sem._permits = 10
        future = sem.reduce_permits_async(3)
        assert future.result() is None
        assert sem._permits == 7

    def test_reduce_permits_can_go_negative(self):
        sem = Semaphore("test")
        sem._permits = 5
        sem.reduce_permits(10)
        assert sem._permits == -5

    def test_reduce_permits_negative_raises(self):
        sem = Semaphore("test")
        with pytest.raises(IllegalArgumentException) as exc_info:
            sem.reduce_permits(-1)
        assert "reduction cannot be negative" in str(exc_info.value)

    def test_reduce_permits_async_negative_raises(self):
        sem = Semaphore("test")
        with pytest.raises(IllegalArgumentException):
            sem.reduce_permits_async(-1)

    def test_increase_permits(self):
        sem = Semaphore("test")
        sem._permits = 5
        sem.increase_permits(3)
        assert sem._permits == 8

    def test_increase_permits_async(self):
        sem = Semaphore("test")
        sem._permits = 5
        future = sem.increase_permits_async(3)
        assert future.result() is None
        assert sem._permits == 8

    def test_increase_permits_negative_raises(self):
        sem = Semaphore("test")
        with pytest.raises(IllegalArgumentException) as exc_info:
            sem.increase_permits(-1)
        assert "increase cannot be negative" in str(exc_info.value)

    def test_increase_permits_async_negative_raises(self):
        sem = Semaphore("test")
        with pytest.raises(IllegalArgumentException):
            sem.increase_permits_async(-1)

    def test_operations_on_destroyed_proxy_acquire(self):
        sem = Semaphore("test")
        sem.destroy()
        with pytest.raises(IllegalStateException):
            sem.acquire()

    def test_operations_on_destroyed_proxy_release(self):
        sem = Semaphore("test")
        sem.destroy()
        with pytest.raises(IllegalStateException):
            sem.release()

    def test_operations_on_destroyed_proxy_available_permits(self):
        sem = Semaphore("test")
        sem.destroy()
        with pytest.raises(IllegalStateException):
            sem.available_permits()


class TestFencedLock:
    """Tests for FencedLock class."""

    def test_service_name_constant(self):
        assert FencedLock.SERVICE_NAME == "hz:raft:lockService"

    def test_invalid_fence_constant(self):
        assert FencedLock.INVALID_FENCE == 0

    def test_initial_state_not_locked(self):
        lock = FencedLock("test")
        assert lock._owner_thread is None

    def test_initial_state_count_zero(self):
        lock = FencedLock("test")
        assert lock._lock_count == 0

    def test_initial_state_fence_invalid(self):
        lock = FencedLock("test")
        assert lock._fence == FencedLock.INVALID_FENCE

    def test_lock_acquires_and_returns_fence(self):
        lock = FencedLock("test")
        fence = lock.lock()
        assert fence > 0
        assert lock._owner_thread == threading.current_thread().ident
        assert lock._lock_count == 1

    def test_lock_async(self):
        lock = FencedLock("test")
        future = lock.lock_async()
        fence = future.result()
        assert fence > 0
        assert lock._lock_count == 1

    def test_lock_reentrant_increments_count_same_fence(self):
        lock = FencedLock("test")
        fence1 = lock.lock()
        fence2 = lock.lock()
        assert fence1 == fence2
        assert lock._lock_count == 2

    def test_try_lock_immediate_success(self):
        lock = FencedLock("test")
        fence = lock.try_lock()
        assert fence > 0
        assert lock._lock_count == 1

    def test_try_lock_immediate_failure_other_thread(self):
        lock = FencedLock("test")
        
        acquired = threading.Event()
        
        def holder():
            lock.lock()
            acquired.set()
            time.sleep(0.5)
            lock.unlock()
        
        thread = threading.Thread(target=holder)
        thread.start()
        acquired.wait()
        
        fence = lock.try_lock()
        assert fence == FencedLock.INVALID_FENCE
        
        thread.join()

    def test_try_lock_async_success(self):
        lock = FencedLock("test")
        future = lock.try_lock_async()
        fence = future.result()
        assert fence > 0

    def test_try_lock_with_timeout_success(self):
        lock = FencedLock("test")
        fence = lock.try_lock(timeout=1.0)
        assert fence > 0
        assert lock._lock_count == 1

    def test_try_lock_with_timeout_expires(self):
        lock = FencedLock("test")
        
        acquired = threading.Event()
        
        def holder():
            lock.lock()
            acquired.set()
            time.sleep(0.5)
            lock.unlock()
        
        thread = threading.Thread(target=holder)
        thread.start()
        acquired.wait()
        
        fence = lock.try_lock(timeout=0.05)
        assert fence == FencedLock.INVALID_FENCE
        
        thread.join()

    def test_try_lock_negative_timeout_raises(self):
        lock = FencedLock("test")
        with pytest.raises(IllegalArgumentException) as exc_info:
            lock.try_lock(timeout=-1)
        assert "timeout cannot be negative" in str(exc_info.value)

    def test_try_lock_async_negative_timeout_raises(self):
        lock = FencedLock("test")
        with pytest.raises(IllegalArgumentException):
            lock.try_lock_async(timeout=-1)

    def test_unlock_decrements_count(self):
        lock = FencedLock("test")
        lock.lock()
        lock.lock()
        assert lock._lock_count == 2
        lock.unlock()
        assert lock._lock_count == 1

    def test_unlock_async(self):
        lock = FencedLock("test")
        lock.lock()
        future = lock.unlock_async()
        assert future.result() is None
        assert lock._lock_count == 0

    def test_unlock_fully_releases_lock(self):
        lock = FencedLock("test")
        lock.lock()
        lock.unlock()
        assert lock._owner_thread is None
        assert lock._lock_count == 0
        assert lock._fence == FencedLock.INVALID_FENCE

    def test_unlock_not_owner_raises(self):
        lock = FencedLock("test")
        with pytest.raises(IllegalStateException) as exc_info:
            lock.unlock()
        assert "does not hold this lock" in str(exc_info.value)

    def test_unlock_async_not_owner_raises(self):
        lock = FencedLock("test")
        with pytest.raises(IllegalStateException):
            lock.unlock_async()

    def test_is_locked_when_locked(self):
        lock = FencedLock("test")
        lock.lock()
        assert lock.is_locked() is True

    def test_is_locked_when_not_locked(self):
        lock = FencedLock("test")
        assert lock.is_locked() is False

    def test_is_locked_async_when_locked(self):
        lock = FencedLock("test")
        lock.lock()
        future = lock.is_locked_async()
        assert future.result() is True

    def test_is_locked_async_when_not_locked(self):
        lock = FencedLock("test")
        future = lock.is_locked_async()
        assert future.result() is False

    def test_is_locked_by_current_thread_when_owner(self):
        lock = FencedLock("test")
        lock.lock()
        assert lock.is_locked_by_current_thread() is True

    def test_is_locked_by_current_thread_when_not_owner(self):
        lock = FencedLock("test")
        assert lock.is_locked_by_current_thread() is False

    def test_is_locked_by_current_thread_async(self):
        lock = FencedLock("test")
        lock.lock()
        future = lock.is_locked_by_current_thread_async()
        assert future.result() is True

    def test_get_lock_count(self):
        lock = FencedLock("test")
        lock.lock()
        lock.lock()
        assert lock.get_lock_count() == 2

    def test_get_lock_count_async(self):
        lock = FencedLock("test")
        lock.lock()
        lock.lock()
        future = lock.get_lock_count_async()
        assert future.result() == 2

    def test_get_lock_count_when_not_locked(self):
        lock = FencedLock("test")
        assert lock.get_lock_count() == 0

    def test_get_fence(self):
        lock = FencedLock("test")
        lock.lock()
        fence = lock.get_fence()
        assert fence > 0

    def test_get_fence_async(self):
        lock = FencedLock("test")
        lock.lock()
        future = lock.get_fence_async()
        assert future.result() > 0

    def test_get_fence_when_not_locked(self):
        lock = FencedLock("test")
        assert lock.get_fence() == FencedLock.INVALID_FENCE

    def test_fence_changes_on_new_acquisition(self):
        lock = FencedLock("test")
        fence1 = lock.lock()
        lock.unlock()
        fence2 = lock.lock()
        assert fence2 > fence1

    def test_operations_on_destroyed_proxy_lock(self):
        lock = FencedLock("test")
        lock.destroy()
        with pytest.raises(IllegalStateException):
            lock.lock()

    def test_operations_on_destroyed_proxy_try_lock(self):
        lock = FencedLock("test")
        lock.destroy()
        with pytest.raises(IllegalStateException):
            lock.try_lock()

    def test_operations_on_destroyed_proxy_is_locked(self):
        lock = FencedLock("test")
        lock.destroy()
        with pytest.raises(IllegalStateException):
            lock.is_locked()

    def test_operations_on_destroyed_proxy_get_fence(self):
        lock = FencedLock("test")
        lock.destroy()
        with pytest.raises(IllegalStateException):
            lock.get_fence()
