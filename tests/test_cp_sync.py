"""Unit tests for CP Subsystem Synchronization primitives."""

import threading
import time
import unittest

from hazelcast.cp.sync import CountDownLatch, Semaphore, FencedLock
from hazelcast.exceptions import IllegalStateException, IllegalArgumentException


class TestCountDownLatch(unittest.TestCase):
    """Tests for CountDownLatch."""

    def setUp(self):
        self.latch = CountDownLatch("test-latch")

    def tearDown(self):
        if not self.latch.is_destroyed:
            self.latch.destroy()

    def test_initial_count_is_zero(self):
        self.assertEqual(0, self.latch.get_count())

    def test_try_set_count(self):
        result = self.latch.try_set_count(3)
        self.assertTrue(result)
        self.assertEqual(3, self.latch.get_count())

    def test_try_set_count_fails_when_not_zero(self):
        self.latch.try_set_count(3)
        result = self.latch.try_set_count(5)
        self.assertFalse(result)
        self.assertEqual(3, self.latch.get_count())

    def test_try_set_count_negative_raises(self):
        with self.assertRaises(IllegalArgumentException):
            self.latch.try_set_count(-1)

    def test_count_down(self):
        self.latch.try_set_count(3)
        self.latch.count_down()
        self.assertEqual(2, self.latch.get_count())

    def test_count_down_to_zero(self):
        self.latch.try_set_count(1)
        self.latch.count_down()
        self.assertEqual(0, self.latch.get_count())

    def test_count_down_at_zero_stays_zero(self):
        self.latch.count_down()
        self.assertEqual(0, self.latch.get_count())

    def test_await_returns_immediately_when_zero(self):
        result = self.latch.await_()
        self.assertTrue(result)

    def test_await_with_timeout_returns_immediately_when_zero(self):
        result = self.latch.await_with_timeout(1.0)
        self.assertTrue(result)

    def test_await_with_timeout_negative_raises(self):
        with self.assertRaises(IllegalArgumentException):
            self.latch.await_with_timeout(-1.0)

    def test_await_with_timeout_expires(self):
        self.latch.try_set_count(1)
        start = time.monotonic()
        result = self.latch.await_with_timeout(0.1)
        elapsed = time.monotonic() - start
        self.assertFalse(result)
        self.assertGreaterEqual(elapsed, 0.1)

    def test_await_blocks_until_count_down(self):
        self.latch.try_set_count(1)
        results = []

        def waiter():
            results.append(self.latch.await_())

        thread = threading.Thread(target=waiter)
        thread.start()
        time.sleep(0.05)
        self.latch.count_down()
        thread.join(timeout=1.0)
        self.assertEqual([True], results)

    def test_operations_on_destroyed_raise(self):
        self.latch.destroy()
        with self.assertRaises(IllegalStateException):
            self.latch.get_count()

    def test_service_name(self):
        self.assertEqual("hz:raft:countDownLatchService", self.latch.service_name)

    def test_async_get_count(self):
        self.latch.try_set_count(5)
        future = self.latch.get_count_async()
        self.assertEqual(5, future.result())


class TestSemaphore(unittest.TestCase):
    """Tests for Semaphore."""

    def setUp(self):
        self.semaphore = Semaphore("test-semaphore")

    def tearDown(self):
        if not self.semaphore.is_destroyed:
            self.semaphore.destroy()

    def test_initial_permits_is_zero(self):
        self.assertEqual(0, self.semaphore.available_permits())

    def test_init(self):
        result = self.semaphore.init(5)
        self.assertTrue(result)
        self.assertEqual(5, self.semaphore.available_permits())

    def test_init_fails_when_not_zero(self):
        self.semaphore.init(5)
        result = self.semaphore.init(10)
        self.assertFalse(result)
        self.assertEqual(5, self.semaphore.available_permits())

    def test_init_negative_raises(self):
        with self.assertRaises(IllegalArgumentException):
            self.semaphore.init(-1)

    def test_acquire_single(self):
        self.semaphore.init(3)
        self.semaphore.acquire()
        self.assertEqual(2, self.semaphore.available_permits())

    def test_acquire_multiple(self):
        self.semaphore.init(5)
        self.semaphore.acquire(3)
        self.assertEqual(2, self.semaphore.available_permits())

    def test_acquire_negative_raises(self):
        self.semaphore.init(5)
        with self.assertRaises(IllegalArgumentException):
            self.semaphore.acquire(-1)

    def test_try_acquire_success(self):
        self.semaphore.init(3)
        result = self.semaphore.try_acquire()
        self.assertTrue(result)
        self.assertEqual(2, self.semaphore.available_permits())

    def test_try_acquire_failure(self):
        result = self.semaphore.try_acquire()
        self.assertFalse(result)

    def test_try_acquire_with_timeout_success(self):
        self.semaphore.init(1)
        result = self.semaphore.try_acquire(1, timeout=0.1)
        self.assertTrue(result)

    def test_try_acquire_with_timeout_expires(self):
        start = time.monotonic()
        result = self.semaphore.try_acquire(1, timeout=0.1)
        elapsed = time.monotonic() - start
        self.assertFalse(result)
        self.assertGreaterEqual(elapsed, 0.1)

    def test_try_acquire_negative_timeout_raises(self):
        with self.assertRaises(IllegalArgumentException):
            self.semaphore.try_acquire(1, timeout=-1.0)

    def test_release(self):
        self.semaphore.init(3)
        self.semaphore.acquire()
        self.semaphore.release()
        self.assertEqual(3, self.semaphore.available_permits())

    def test_release_multiple(self):
        self.semaphore.init(3)
        self.semaphore.release(2)
        self.assertEqual(5, self.semaphore.available_permits())

    def test_release_negative_raises(self):
        with self.assertRaises(IllegalArgumentException):
            self.semaphore.release(-1)

    def test_drain_permits(self):
        self.semaphore.init(5)
        drained = self.semaphore.drain_permits()
        self.assertEqual(5, drained)
        self.assertEqual(0, self.semaphore.available_permits())

    def test_reduce_permits(self):
        self.semaphore.init(5)
        self.semaphore.reduce_permits(3)
        self.assertEqual(2, self.semaphore.available_permits())

    def test_reduce_permits_below_zero(self):
        self.semaphore.init(2)
        self.semaphore.reduce_permits(5)
        self.assertEqual(-3, self.semaphore.available_permits())

    def test_reduce_permits_negative_raises(self):
        with self.assertRaises(IllegalArgumentException):
            self.semaphore.reduce_permits(-1)

    def test_increase_permits(self):
        self.semaphore.init(3)
        self.semaphore.increase_permits(2)
        self.assertEqual(5, self.semaphore.available_permits())

    def test_increase_permits_negative_raises(self):
        with self.assertRaises(IllegalArgumentException):
            self.semaphore.increase_permits(-1)

    def test_acquire_blocks_until_release(self):
        self.semaphore.init(0)
        results = []

        def acquirer():
            self.semaphore.acquire()
            results.append("acquired")

        thread = threading.Thread(target=acquirer)
        thread.start()
        time.sleep(0.05)
        self.semaphore.release()
        thread.join(timeout=1.0)
        self.assertEqual(["acquired"], results)

    def test_operations_on_destroyed_raise(self):
        self.semaphore.destroy()
        with self.assertRaises(IllegalStateException):
            self.semaphore.available_permits()

    def test_service_name(self):
        self.assertEqual("hz:raft:semaphoreService", self.semaphore.service_name)

    def test_async_available_permits(self):
        self.semaphore.init(7)
        future = self.semaphore.available_permits_async()
        self.assertEqual(7, future.result())


class TestFencedLock(unittest.TestCase):
    """Tests for FencedLock."""

    def setUp(self):
        self.lock = FencedLock("test-lock")

    def tearDown(self):
        if not self.lock.is_destroyed:
            try:
                while self.lock.is_locked_by_current_thread():
                    self.lock.unlock()
            except IllegalStateException:
                pass
            self.lock.destroy()

    def test_initial_state_not_locked(self):
        self.assertFalse(self.lock.is_locked())
        self.assertEqual(0, self.lock.get_lock_count())
        self.assertEqual(FencedLock.INVALID_FENCE, self.lock.get_fence())

    def test_lock_and_unlock(self):
        fence = self.lock.lock()
        self.assertTrue(fence > 0)
        self.assertTrue(self.lock.is_locked())
        self.assertTrue(self.lock.is_locked_by_current_thread())
        self.assertEqual(1, self.lock.get_lock_count())
        self.assertEqual(fence, self.lock.get_fence())

        self.lock.unlock()
        self.assertFalse(self.lock.is_locked())
        self.assertEqual(0, self.lock.get_lock_count())

    def test_reentrant_lock(self):
        fence1 = self.lock.lock()
        fence2 = self.lock.lock()
        self.assertEqual(fence1, fence2)
        self.assertEqual(2, self.lock.get_lock_count())

        self.lock.unlock()
        self.assertTrue(self.lock.is_locked())
        self.assertEqual(1, self.lock.get_lock_count())

        self.lock.unlock()
        self.assertFalse(self.lock.is_locked())

    def test_fence_increments_on_new_acquisition(self):
        fence1 = self.lock.lock()
        self.lock.unlock()

        fence2 = self.lock.lock()
        self.lock.unlock()

        self.assertGreater(fence2, fence1)

    def test_try_lock_success(self):
        fence = self.lock.try_lock()
        self.assertTrue(fence > 0)
        self.assertTrue(self.lock.is_locked())
        self.lock.unlock()

    def test_try_lock_failure(self):
        results = []

        def holder():
            self.lock.lock()
            time.sleep(0.2)
            self.lock.unlock()

        thread = threading.Thread(target=holder)
        thread.start()
        time.sleep(0.05)
        fence = self.lock.try_lock()
        results.append(fence)
        thread.join()

        self.assertEqual(FencedLock.INVALID_FENCE, results[0])

    def test_try_lock_with_timeout_success(self):
        fence = self.lock.try_lock(timeout=0.1)
        self.assertTrue(fence > 0)
        self.lock.unlock()

    def test_try_lock_with_timeout_expires(self):
        results = []

        def holder():
            self.lock.lock()
            time.sleep(0.3)
            self.lock.unlock()

        thread = threading.Thread(target=holder)
        thread.start()
        time.sleep(0.05)

        start = time.monotonic()
        fence = self.lock.try_lock(timeout=0.1)
        elapsed = time.monotonic() - start

        results.append(fence)
        thread.join()

        self.assertEqual(FencedLock.INVALID_FENCE, results[0])
        self.assertGreaterEqual(elapsed, 0.1)

    def test_try_lock_negative_timeout_raises(self):
        with self.assertRaises(IllegalArgumentException):
            self.lock.try_lock(timeout=-1.0)

    def test_unlock_without_lock_raises(self):
        with self.assertRaises(IllegalStateException):
            self.lock.unlock()

    def test_unlock_from_different_thread_raises(self):
        self.lock.lock()
        results = []

        def other_thread():
            try:
                self.lock.unlock()
                results.append("success")
            except IllegalStateException:
                results.append("error")

        thread = threading.Thread(target=other_thread)
        thread.start()
        thread.join()

        self.assertEqual(["error"], results)
        self.lock.unlock()

    def test_is_locked_by_current_thread(self):
        self.assertFalse(self.lock.is_locked_by_current_thread())
        self.lock.lock()
        self.assertTrue(self.lock.is_locked_by_current_thread())
        self.lock.unlock()
        self.assertFalse(self.lock.is_locked_by_current_thread())

    def test_lock_blocks_until_released(self):
        results = []

        def holder():
            self.lock.lock()
            results.append("locked")
            time.sleep(0.1)
            self.lock.unlock()
            results.append("unlocked")

        def waiter():
            time.sleep(0.05)
            self.lock.lock()
            results.append("waiter_locked")
            self.lock.unlock()

        t1 = threading.Thread(target=holder)
        t2 = threading.Thread(target=waiter)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(["locked", "unlocked", "waiter_locked"], results)

    def test_operations_on_destroyed_raise(self):
        self.lock.destroy()
        with self.assertRaises(IllegalStateException):
            self.lock.is_locked()

    def test_service_name(self):
        self.assertEqual("hz:raft:lockService", self.lock.service_name)

    def test_async_is_locked(self):
        future = self.lock.is_locked_async()
        self.assertFalse(future.result())
        self.lock.lock()
        future = self.lock.is_locked_async()
        self.assertTrue(future.result())
        self.lock.unlock()


class TestFencedLockFencing(unittest.TestCase):
    """Tests for FencedLock fencing token behavior."""

    def setUp(self):
        self.lock = FencedLock("test-fencing")

    def tearDown(self):
        if not self.lock.is_destroyed:
            try:
                while self.lock.is_locked_by_current_thread():
                    self.lock.unlock()
            except IllegalStateException:
                pass
            self.lock.destroy()

    def test_fence_is_monotonically_increasing(self):
        fences = []
        for _ in range(5):
            fence = self.lock.lock()
            fences.append(fence)
            self.lock.unlock()

        for i in range(1, len(fences)):
            self.assertGreater(fences[i], fences[i - 1])

    def test_fence_preserved_during_reentrant_lock(self):
        fence1 = self.lock.lock()
        fence2 = self.lock.lock()
        fence3 = self.lock.lock()

        self.assertEqual(fence1, fence2)
        self.assertEqual(fence2, fence3)

        self.lock.unlock()
        self.lock.unlock()
        self.lock.unlock()

    def test_invalid_fence_is_zero(self):
        self.assertEqual(0, FencedLock.INVALID_FENCE)
        self.assertEqual(FencedLock.INVALID_FENCE, self.lock.get_fence())


if __name__ == "__main__":
    unittest.main()
