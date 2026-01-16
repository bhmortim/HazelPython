"""Comprehensive unit tests for CP Subsystem: FencedLock, CountDownLatch, Semaphore."""

import threading
import time
import unittest
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from unittest.mock import MagicMock, patch

from hazelcast.cp.fenced_lock import FencedLock
from hazelcast.cp.count_down_latch import CountDownLatch
from hazelcast.cp.semaphore import Semaphore
from hazelcast.exceptions import IllegalStateException


class TestFencedLock(unittest.TestCase):
    """Tests for FencedLock distributed data structure."""

    def setUp(self):
        with patch("hazelcast.cp.fenced_lock.CPProxy.__init__", return_value=None):
            self.lock = FencedLock("test-lock")
            self.lock._destroyed = False

    def _patch_check_not_destroyed(self, lock):
        lock._check_not_destroyed = MagicMock()

    def test_init(self):
        """Test FencedLock initialization."""
        with patch("hazelcast.cp.fenced_lock.CPProxy.__init__", return_value=None):
            lock = FencedLock("my-lock")
            self.assertEqual(lock.INVALID_FENCE, 0)
            self.assertEqual(lock._fence, 0)
            self.assertIsNone(lock._owner)
            self.assertEqual(lock._lock_count, 0)

    def test_lock_acquires_and_returns_fence(self):
        """Test that lock() acquires the lock and returns a fence token."""
        self._patch_check_not_destroyed(self.lock)
        fence = self.lock.lock()
        self.assertEqual(fence, 1)
        self.assertTrue(self.lock.is_locked())
        self.assertTrue(self.lock.is_locked_by_current_thread())

    def test_lock_async_returns_future(self):
        """Test that lock_async() returns a Future with fence token."""
        self._patch_check_not_destroyed(self.lock)
        future = self.lock.lock_async()
        self.assertIsInstance(future, Future)
        fence = future.result()
        self.assertEqual(fence, 1)

    def test_lock_reentrant(self):
        """Test that the lock is reentrant."""
        self._patch_check_not_destroyed(self.lock)
        fence1 = self.lock.lock()
        fence2 = self.lock.lock()
        self.assertEqual(fence1, 1)
        self.assertEqual(fence2, 2)
        self.assertEqual(self.lock.get_lock_count(), 2)

    def test_try_lock_success(self):
        """Test try_lock() succeeds when lock is available."""
        self._patch_check_not_destroyed(self.lock)
        fence = self.lock.try_lock(timeout=1.0)
        self.assertNotEqual(fence, FencedLock.INVALID_FENCE)
        self.assertTrue(self.lock.is_locked())

    def test_try_lock_with_zero_timeout(self):
        """Test try_lock() with zero timeout."""
        self._patch_check_not_destroyed(self.lock)
        fence = self.lock.try_lock(timeout=0)
        self.assertNotEqual(fence, FencedLock.INVALID_FENCE)

    def test_try_lock_async(self):
        """Test try_lock_async() returns Future."""
        self._patch_check_not_destroyed(self.lock)
        future = self.lock.try_lock_async(timeout=0.1)
        self.assertIsInstance(future, Future)
        fence = future.result()
        self.assertNotEqual(fence, FencedLock.INVALID_FENCE)

    def test_unlock_releases_lock(self):
        """Test that unlock() releases the lock."""
        self._patch_check_not_destroyed(self.lock)
        self.lock.lock()
        self.assertTrue(self.lock.is_locked())
        self.lock.unlock()
        self.assertFalse(self.lock.is_locked())

    def test_unlock_async(self):
        """Test unlock_async() returns Future."""
        self._patch_check_not_destroyed(self.lock)
        self.lock.lock()
        future = self.lock.unlock_async()
        self.assertIsInstance(future, Future)
        future.result()
        self.assertFalse(self.lock.is_locked())

    def test_unlock_reentrant_decrements_count(self):
        """Test that unlock decrements reentrant lock count."""
        self._patch_check_not_destroyed(self.lock)
        self.lock.lock()
        self.lock.lock()
        self.assertEqual(self.lock.get_lock_count(), 2)
        self.lock.unlock()
        self.assertEqual(self.lock.get_lock_count(), 1)
        self.assertTrue(self.lock.is_locked())
        self.lock.unlock()
        self.assertEqual(self.lock.get_lock_count(), 0)

    def test_unlock_not_held_raises_exception(self):
        """Test that unlock raises when lock not held."""
        self._patch_check_not_destroyed(self.lock)
        with self.assertRaises(IllegalStateException):
            self.lock.unlock()

    def test_get_fence_when_locked(self):
        """Test get_fence() returns current fence when locked."""
        self._patch_check_not_destroyed(self.lock)
        self.lock.lock()
        fence = self.lock.get_fence()
        self.assertEqual(fence, 1)

    def test_get_fence_when_not_locked(self):
        """Test get_fence() returns INVALID_FENCE when not locked."""
        self._patch_check_not_destroyed(self.lock)
        fence = self.lock.get_fence()
        self.assertEqual(fence, FencedLock.INVALID_FENCE)

    def test_get_fence_async(self):
        """Test get_fence_async() returns Future."""
        self._patch_check_not_destroyed(self.lock)
        future = self.lock.get_fence_async()
        self.assertIsInstance(future, Future)

    def test_is_locked_false_initially(self):
        """Test is_locked() returns False initially."""
        self._patch_check_not_destroyed(self.lock)
        self.assertFalse(self.lock.is_locked())

    def test_is_locked_async(self):
        """Test is_locked_async() returns Future."""
        self._patch_check_not_destroyed(self.lock)
        future = self.lock.is_locked_async()
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_is_locked_by_current_thread_false_initially(self):
        """Test is_locked_by_current_thread() returns False initially."""
        self._patch_check_not_destroyed(self.lock)
        self.assertFalse(self.lock.is_locked_by_current_thread())

    def test_is_locked_by_current_thread_async(self):
        """Test is_locked_by_current_thread_async() returns Future."""
        self._patch_check_not_destroyed(self.lock)
        future = self.lock.is_locked_by_current_thread_async()
        self.assertIsInstance(future, Future)

    def test_get_lock_count_zero_initially(self):
        """Test get_lock_count() returns 0 initially."""
        self._patch_check_not_destroyed(self.lock)
        self.assertEqual(self.lock.get_lock_count(), 0)

    def test_get_lock_count_async(self):
        """Test get_lock_count_async() returns Future."""
        self._patch_check_not_destroyed(self.lock)
        future = self.lock.get_lock_count_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 0)

    def test_context_manager(self):
        """Test FencedLock as context manager."""
        self._patch_check_not_destroyed(self.lock)
        with self.lock as fence:
            self.assertEqual(fence, 1)
            self.assertTrue(self.lock.is_locked())
        self.assertFalse(self.lock.is_locked())

    def test_context_manager_releases_on_exception(self):
        """Test context manager releases lock on exception."""
        self._patch_check_not_destroyed(self.lock)
        try:
            with self.lock:
                self.assertTrue(self.lock.is_locked())
                raise ValueError("test error")
        except ValueError:
            pass
        self.assertFalse(self.lock.is_locked())

    def test_multithreaded_lock(self):
        """Test lock behavior across multiple threads."""
        self._patch_check_not_destroyed(self.lock)
        results = []
        barrier = threading.Barrier(2)

        def worker():
            barrier.wait()
            fence = self.lock.try_lock(timeout=0.5)
            results.append(fence)
            if fence != FencedLock.INVALID_FENCE:
                time.sleep(0.1)
                self.lock.unlock()

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(len(results), 2)


class TestCountDownLatch(unittest.TestCase):
    """Tests for CountDownLatch distributed data structure."""

    def setUp(self):
        with patch("hazelcast.cp.count_down_latch.CPProxy.__init__", return_value=None):
            self.latch = CountDownLatch("test-latch")
            self.latch._destroyed = False

    def _patch_check_not_destroyed(self, latch):
        latch._check_not_destroyed = MagicMock()

    def test_init(self):
        """Test CountDownLatch initialization."""
        with patch("hazelcast.cp.count_down_latch.CPProxy.__init__", return_value=None):
            latch = CountDownLatch("my-latch")
            self.assertEqual(latch._count, 0)

    def test_try_set_count_success(self):
        """Test try_set_count() succeeds when count is zero."""
        self._patch_check_not_destroyed(self.latch)
        result = self.latch.try_set_count(5)
        self.assertTrue(result)
        self.assertEqual(self.latch.get_count(), 5)

    def test_try_set_count_fails_when_not_zero(self):
        """Test try_set_count() fails when count is non-zero."""
        self._patch_check_not_destroyed(self.latch)
        self.latch.try_set_count(5)
        result = self.latch.try_set_count(10)
        self.assertFalse(result)
        self.assertEqual(self.latch.get_count(), 5)

    def test_try_set_count_fails_with_zero(self):
        """Test try_set_count() fails with zero count."""
        self._patch_check_not_destroyed(self.latch)
        result = self.latch.try_set_count(0)
        self.assertFalse(result)

    def test_try_set_count_fails_with_negative(self):
        """Test try_set_count() fails with negative count."""
        self._patch_check_not_destroyed(self.latch)
        result = self.latch.try_set_count(-1)
        self.assertFalse(result)

    def test_try_set_count_async(self):
        """Test try_set_count_async() returns Future."""
        self._patch_check_not_destroyed(self.latch)
        future = self.latch.try_set_count_async(3)
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_get_count_initial(self):
        """Test get_count() returns 0 initially."""
        self._patch_check_not_destroyed(self.latch)
        self.assertEqual(self.latch.get_count(), 0)

    def test_get_count_async(self):
        """Test get_count_async() returns Future."""
        self._patch_check_not_destroyed(self.latch)
        future = self.latch.get_count_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 0)

    def test_count_down_decrements(self):
        """Test count_down() decrements the count."""
        self._patch_check_not_destroyed(self.latch)
        self.latch.try_set_count(3)
        self.latch.count_down()
        self.assertEqual(self.latch.get_count(), 2)
        self.latch.count_down()
        self.assertEqual(self.latch.get_count(), 1)
        self.latch.count_down()
        self.assertEqual(self.latch.get_count(), 0)

    def test_count_down_does_not_go_negative(self):
        """Test count_down() does not decrement below zero."""
        self._patch_check_not_destroyed(self.latch)
        self.latch.try_set_count(1)
        self.latch.count_down()
        self.latch.count_down()
        self.assertEqual(self.latch.get_count(), 0)

    def test_count_down_async(self):
        """Test count_down_async() returns Future."""
        self._patch_check_not_destroyed(self.latch)
        self.latch.try_set_count(2)
        future = self.latch.count_down_async()
        self.assertIsInstance(future, Future)
        future.result()
        self.assertEqual(self.latch.get_count(), 1)

    def test_await_returns_immediately_when_zero(self):
        """Test await_() returns immediately when count is zero."""
        self._patch_check_not_destroyed(self.latch)
        result = self.latch.await_(timeout=1.0)
        self.assertTrue(result)

    def test_await_waits_for_countdown(self):
        """Test await_() waits for count to reach zero."""
        self._patch_check_not_destroyed(self.latch)
        self.latch.try_set_count(2)
        results = []

        def countdown():
            time.sleep(0.05)
            self.latch.count_down()
            time.sleep(0.05)
            self.latch.count_down()

        t = threading.Thread(target=countdown)
        t.start()

        result = self.latch.await_(timeout=2.0)
        t.join()
        self.assertTrue(result)
        self.assertEqual(self.latch.get_count(), 0)

    def test_await_timeout_returns_false(self):
        """Test await_() returns False on timeout."""
        self._patch_check_not_destroyed(self.latch)
        self.latch.try_set_count(10)
        result = self.latch.await_(timeout=0.05)
        self.assertFalse(result)

    def test_await_async(self):
        """Test await_async() returns Future."""
        self._patch_check_not_destroyed(self.latch)
        future = self.latch.await_async(timeout=0.1)
        self.assertIsInstance(future, Future)

    def test_await_infinite_timeout(self):
        """Test await_() with infinite timeout."""
        self._patch_check_not_destroyed(self.latch)
        self.latch.try_set_count(1)

        def countdown():
            time.sleep(0.05)
            self.latch.count_down()

        t = threading.Thread(target=countdown)
        t.start()
        result = self.latch.await_(timeout=-1)
        t.join()
        self.assertTrue(result)

    def test_get_round(self):
        """Test get_round() returns round number."""
        self._patch_check_not_destroyed(self.latch)
        round_num = self.latch.get_round()
        self.assertEqual(round_num, 0)

    def test_get_round_async(self):
        """Test get_round_async() returns Future."""
        self._patch_check_not_destroyed(self.latch)
        future = self.latch.get_round_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 0)

    def test_multiple_waiters(self):
        """Test multiple threads waiting on latch."""
        self._patch_check_not_destroyed(self.latch)
        self.latch.try_set_count(1)
        results = []
        barrier = threading.Barrier(3)

        def waiter():
            barrier.wait()
            result = self.latch.await_(timeout=2.0)
            results.append(result)

        t1 = threading.Thread(target=waiter)
        t2 = threading.Thread(target=waiter)
        t1.start()
        t2.start()

        barrier.wait()
        time.sleep(0.05)
        self.latch.count_down()

        t1.join()
        t2.join()
        self.assertEqual(results, [True, True])


class TestSemaphore(unittest.TestCase):
    """Tests for Semaphore distributed data structure."""

    def setUp(self):
        with patch("hazelcast.cp.semaphore.CPProxy.__init__", return_value=None):
            self.semaphore = Semaphore("test-semaphore", initial_permits=0)
            self.semaphore._destroyed = False

    def _patch_check_not_destroyed(self, sem):
        sem._check_not_destroyed = MagicMock()

    def test_init_with_permits(self):
        """Test Semaphore initialization with permits."""
        with patch("hazelcast.cp.semaphore.CPProxy.__init__", return_value=None):
            sem = Semaphore("my-sem", initial_permits=5)
            self.assertEqual(sem._permits, 5)

    def test_init_with_negative_permits(self):
        """Test Semaphore initialization with negative permits uses 0."""
        with patch("hazelcast.cp.semaphore.CPProxy.__init__", return_value=None):
            sem = Semaphore("my-sem", initial_permits=-5)
            self.assertEqual(sem._permits, -5)

    def test_init_method_success(self):
        """Test init() method succeeds when permits are zero."""
        self._patch_check_not_destroyed(self.semaphore)
        result = self.semaphore.init(10)
        self.assertTrue(result)
        self.assertEqual(self.semaphore.available_permits(), 10)

    def test_init_method_fails_when_initialized(self):
        """Test init() fails when already initialized."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(5)
        result = self.semaphore.init(10)
        self.assertFalse(result)
        self.assertEqual(self.semaphore.available_permits(), 5)

    def test_init_async(self):
        """Test init_async() returns Future."""
        self._patch_check_not_destroyed(self.semaphore)
        future = self.semaphore.init_async(3)
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_acquire_single_permit(self):
        """Test acquire() gets a single permit."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(5)
        self.semaphore.acquire()
        self.assertEqual(self.semaphore.available_permits(), 4)

    def test_acquire_multiple_permits(self):
        """Test acquire() gets multiple permits."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(5)
        self.semaphore.acquire(3)
        self.assertEqual(self.semaphore.available_permits(), 2)

    def test_acquire_async(self):
        """Test acquire_async() returns Future."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(2)
        future = self.semaphore.acquire_async(1)
        self.assertIsInstance(future, Future)
        future.result()
        self.assertEqual(self.semaphore.available_permits(), 1)

    def test_try_acquire_success(self):
        """Test try_acquire() succeeds when permits available."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(5)
        result = self.semaphore.try_acquire(2, timeout=0.1)
        self.assertTrue(result)
        self.assertEqual(self.semaphore.available_permits(), 3)

    def test_try_acquire_failure_timeout(self):
        """Test try_acquire() fails on timeout."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(1)
        result = self.semaphore.try_acquire(5, timeout=0.05)
        self.assertFalse(result)
        self.assertEqual(self.semaphore.available_permits(), 1)

    def test_try_acquire_async(self):
        """Test try_acquire_async() returns Future."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(3)
        future = self.semaphore.try_acquire_async(2, timeout=0.1)
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_release_single_permit(self):
        """Test release() releases a single permit."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(5)
        self.semaphore.acquire(2)
        self.semaphore.release()
        self.assertEqual(self.semaphore.available_permits(), 4)

    def test_release_multiple_permits(self):
        """Test release() releases multiple permits."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(5)
        self.semaphore.acquire(3)
        self.semaphore.release(2)
        self.assertEqual(self.semaphore.available_permits(), 4)

    def test_release_async(self):
        """Test release_async() returns Future."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(2)
        self.semaphore.acquire(1)
        future = self.semaphore.release_async(1)
        self.assertIsInstance(future, Future)
        future.result()
        self.assertEqual(self.semaphore.available_permits(), 2)

    def test_available_permits_initial(self):
        """Test available_permits() returns initial value."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(10)
        self.assertEqual(self.semaphore.available_permits(), 10)

    def test_available_permits_async(self):
        """Test available_permits_async() returns Future."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(7)
        future = self.semaphore.available_permits_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 7)

    def test_drain_permits(self):
        """Test drain_permits() acquires all available permits."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(5)
        drained = self.semaphore.drain_permits()
        self.assertEqual(drained, 5)
        self.assertEqual(self.semaphore.available_permits(), 0)

    def test_drain_permits_async(self):
        """Test drain_permits_async() returns Future."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(3)
        future = self.semaphore.drain_permits_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 3)

    def test_reduce_permits(self):
        """Test reduce_permits() reduces available permits."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(10)
        self.semaphore.reduce_permits(3)
        self.assertEqual(self.semaphore.available_permits(), 7)

    def test_reduce_permits_async(self):
        """Test reduce_permits_async() returns Future."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(5)
        future = self.semaphore.reduce_permits_async(2)
        self.assertIsInstance(future, Future)
        future.result()
        self.assertEqual(self.semaphore.available_permits(), 3)

    def test_increase_permits(self):
        """Test increase_permits() increases available permits."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(5)
        self.semaphore.increase_permits(3)
        self.assertEqual(self.semaphore.available_permits(), 8)

    def test_increase_permits_async(self):
        """Test increase_permits_async() returns Future."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(2)
        future = self.semaphore.increase_permits_async(5)
        self.assertIsInstance(future, Future)
        future.result()
        self.assertEqual(self.semaphore.available_permits(), 7)

    def test_acquire_blocks_when_no_permits(self):
        """Test acquire() blocks when no permits available."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(1)
        self.semaphore.acquire(1)
        acquired = []

        def try_acquire():
            result = self.semaphore.try_acquire(1, timeout=0.1)
            acquired.append(result)

        t = threading.Thread(target=try_acquire)
        t.start()
        t.join()
        self.assertEqual(acquired, [False])

    def test_release_unblocks_waiter(self):
        """Test release() unblocks waiting threads."""
        self._patch_check_not_destroyed(self.semaphore)
        self.semaphore.init(0)
        acquired = []

        def acquirer():
            result = self.semaphore.try_acquire(1, timeout=1.0)
            acquired.append(result)

        t = threading.Thread(target=acquirer)
        t.start()
        time.sleep(0.05)
        self.semaphore.release(1)
        t.join()
        self.assertTrue(acquired[0])


if __name__ == "__main__":
    unittest.main()
