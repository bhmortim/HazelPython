"""Tests for CP Subsystem synchronization primitives."""

import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from hazelcast.cp.sync import FencedLock, Semaphore, CountDownLatch, INVALID_FENCE
from hazelcast.proxy.base import ProxyContext
from hazelcast.exceptions import IllegalStateException


class MockInvocationService:
    """Mock invocation service for testing."""

    def __init__(self, return_value=None):
        self._return_value = return_value

    def invoke(self, invocation):
        future = Future()
        future.set_result(self._return_value)
        return future


class TestFencedLock(unittest.TestCase):
    """Tests for FencedLock proxy."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_invocation = MockInvocationService()
        self.context = ProxyContext(
            invocation_service=self.mock_invocation,
            serialization_service=None,
            partition_service=None,
        )

    def test_init(self):
        """Test FencedLock initialization."""
        lock = FencedLock("hz:raft:lockService", "test-lock", self.context)
        self.assertEqual(lock.name, "test-lock")
        self.assertEqual(lock.service_name, "hz:raft:lockService")

    def test_parse_group_id_default(self):
        """Test default CP group ID parsing."""
        lock = FencedLock("hz:raft:lockService", "lock", self.context)
        self.assertEqual(lock._group_id, "default")

    def test_parse_group_id_custom(self):
        """Test custom CP group ID parsing."""
        lock = FencedLock("hz:raft:lockService", "lock@custom", self.context)
        self.assertEqual(lock._group_id, "custom")

    def test_get_object_name_simple(self):
        """Test object name extraction without group."""
        lock = FencedLock("hz:raft:lockService", "lock", self.context)
        self.assertEqual(lock._get_object_name(), "lock")

    def test_get_object_name_with_group(self):
        """Test object name extraction with group."""
        lock = FencedLock("hz:raft:lockService", "lock@group", self.context)
        self.assertEqual(lock._get_object_name(), "lock")

    def test_get_fence_initial(self):
        """Test initial fence value is INVALID_FENCE."""
        lock = FencedLock("hz:raft:lockService", "lock", self.context)
        self.assertEqual(lock.get_fence(), INVALID_FENCE)

    def test_is_locked_by_current_thread_initial(self):
        """Test is_locked_by_current_thread returns False initially."""
        lock = FencedLock("hz:raft:lockService", "lock", self.context)
        self.assertFalse(lock.is_locked_by_current_thread())

    def test_lock_async_returns_future(self):
        """Test that lock_async returns a Future."""
        lock = FencedLock("hz:raft:lockService", "lock", self.context)
        result = lock.lock_async()
        self.assertIsInstance(result, Future)

    def test_try_lock_async_returns_future(self):
        """Test that try_lock_async returns a Future."""
        lock = FencedLock("hz:raft:lockService", "lock", self.context)
        result = lock.try_lock_async(timeout=1.0)
        self.assertIsInstance(result, Future)

    def test_is_locked_async_returns_future(self):
        """Test that is_locked_async returns a Future."""
        lock = FencedLock("hz:raft:lockService", "lock", self.context)
        result = lock.is_locked_async()
        self.assertIsInstance(result, Future)

    def test_get_lock_count_async_returns_future(self):
        """Test that get_lock_count_async returns a Future."""
        lock = FencedLock("hz:raft:lockService", "lock", self.context)
        result = lock.get_lock_count_async()
        self.assertIsInstance(result, Future)

    def test_context_manager_protocol(self):
        """Test FencedLock implements context manager protocol."""
        lock = FencedLock("hz:raft:lockService", "lock", self.context)
        self.assertTrue(hasattr(lock, "__enter__"))
        self.assertTrue(hasattr(lock, "__exit__"))


class TestSemaphore(unittest.TestCase):
    """Tests for Semaphore proxy."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_invocation = MockInvocationService()
        self.context = ProxyContext(
            invocation_service=self.mock_invocation,
            serialization_service=None,
            partition_service=None,
        )

    def test_init(self):
        """Test Semaphore initialization."""
        sem = Semaphore("hz:raft:semaphoreService", "test-sem", self.context)
        self.assertEqual(sem.name, "test-sem")
        self.assertEqual(sem.service_name, "hz:raft:semaphoreService")

    def test_parse_group_id_default(self):
        """Test default CP group ID parsing."""
        sem = Semaphore("hz:raft:semaphoreService", "sem", self.context)
        self.assertEqual(sem._group_id, "default")

    def test_parse_group_id_custom(self):
        """Test custom CP group ID parsing."""
        sem = Semaphore("hz:raft:semaphoreService", "sem@custom", self.context)
        self.assertEqual(sem._group_id, "custom")

    def test_init_async_returns_future(self):
        """Test that init_async returns a Future."""
        sem = Semaphore("hz:raft:semaphoreService", "sem", self.context)
        result = sem.init_async(5)
        self.assertIsInstance(result, Future)

    def test_init_async_rejects_negative_permits(self):
        """Test that init_async rejects negative permits."""
        sem = Semaphore("hz:raft:semaphoreService", "sem", self.context)
        result = sem.init_async(-1)
        with self.assertRaises(IllegalStateException):
            result.result()

    def test_acquire_async_returns_future(self):
        """Test that acquire_async returns a Future."""
        sem = Semaphore("hz:raft:semaphoreService", "sem", self.context)
        result = sem.acquire_async(1)
        self.assertIsInstance(result, Future)

    def test_acquire_async_rejects_zero_permits(self):
        """Test that acquire_async rejects zero permits."""
        sem = Semaphore("hz:raft:semaphoreService", "sem", self.context)
        result = sem.acquire_async(0)
        with self.assertRaises(IllegalStateException):
            result.result()

    def test_try_acquire_async_returns_future(self):
        """Test that try_acquire_async returns a Future."""
        sem = Semaphore("hz:raft:semaphoreService", "sem", self.context)
        result = sem.try_acquire_async(1, timeout=1.0)
        self.assertIsInstance(result, Future)

    def test_release_async_returns_future(self):
        """Test that release_async returns a Future."""
        sem = Semaphore("hz:raft:semaphoreService", "sem", self.context)
        result = sem.release_async(1)
        self.assertIsInstance(result, Future)

    def test_release_async_rejects_zero_permits(self):
        """Test that release_async rejects zero permits."""
        sem = Semaphore("hz:raft:semaphoreService", "sem", self.context)
        result = sem.release_async(0)
        with self.assertRaises(IllegalStateException):
            result.result()

    def test_available_permits_async_returns_future(self):
        """Test that available_permits_async returns a Future."""
        sem = Semaphore("hz:raft:semaphoreService", "sem", self.context)
        result = sem.available_permits_async()
        self.assertIsInstance(result, Future)

    def test_drain_permits_async_returns_future(self):
        """Test that drain_permits_async returns a Future."""
        sem = Semaphore("hz:raft:semaphoreService", "sem", self.context)
        result = sem.drain_permits_async()
        self.assertIsInstance(result, Future)

    def test_reduce_permits_async_returns_future(self):
        """Test that reduce_permits_async returns a Future."""
        sem = Semaphore("hz:raft:semaphoreService", "sem", self.context)
        result = sem.reduce_permits_async(1)
        self.assertIsInstance(result, Future)

    def test_reduce_permits_async_rejects_negative(self):
        """Test that reduce_permits_async rejects negative reduction."""
        sem = Semaphore("hz:raft:semaphoreService", "sem", self.context)
        result = sem.reduce_permits_async(-1)
        with self.assertRaises(IllegalStateException):
            result.result()

    def test_increase_permits_async_returns_future(self):
        """Test that increase_permits_async returns a Future."""
        sem = Semaphore("hz:raft:semaphoreService", "sem", self.context)
        result = sem.increase_permits_async(1)
        self.assertIsInstance(result, Future)

    def test_increase_permits_async_rejects_negative(self):
        """Test that increase_permits_async rejects negative increase."""
        sem = Semaphore("hz:raft:semaphoreService", "sem", self.context)
        result = sem.increase_permits_async(-1)
        with self.assertRaises(IllegalStateException):
            result.result()


class TestCountDownLatch(unittest.TestCase):
    """Tests for CountDownLatch proxy."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_invocation = MockInvocationService()
        self.context = ProxyContext(
            invocation_service=self.mock_invocation,
            serialization_service=None,
            partition_service=None,
        )

    def test_init(self):
        """Test CountDownLatch initialization."""
        latch = CountDownLatch("hz:raft:countDownLatchService", "test-latch", self.context)
        self.assertEqual(latch.name, "test-latch")
        self.assertEqual(latch.service_name, "hz:raft:countDownLatchService")

    def test_parse_group_id_default(self):
        """Test default CP group ID parsing."""
        latch = CountDownLatch("hz:raft:countDownLatchService", "latch", self.context)
        self.assertEqual(latch._group_id, "default")

    def test_parse_group_id_custom(self):
        """Test custom CP group ID parsing."""
        latch = CountDownLatch("hz:raft:countDownLatchService", "latch@custom", self.context)
        self.assertEqual(latch._group_id, "custom")

    def test_try_set_count_async_returns_future(self):
        """Test that try_set_count_async returns a Future."""
        latch = CountDownLatch("hz:raft:countDownLatchService", "latch", self.context)
        result = latch.try_set_count_async(3)
        self.assertIsInstance(result, Future)

    def test_try_set_count_async_rejects_zero(self):
        """Test that try_set_count_async rejects zero count."""
        latch = CountDownLatch("hz:raft:countDownLatchService", "latch", self.context)
        result = latch.try_set_count_async(0)
        with self.assertRaises(IllegalStateException):
            result.result()

    def test_try_set_count_async_rejects_negative(self):
        """Test that try_set_count_async rejects negative count."""
        latch = CountDownLatch("hz:raft:countDownLatchService", "latch", self.context)
        result = latch.try_set_count_async(-1)
        with self.assertRaises(IllegalStateException):
            result.result()

    def test_get_count_async_returns_future(self):
        """Test that get_count_async returns a Future."""
        latch = CountDownLatch("hz:raft:countDownLatchService", "latch", self.context)
        result = latch.get_count_async()
        self.assertIsInstance(result, Future)

    def test_count_down_async_returns_future(self):
        """Test that count_down_async returns a Future."""
        latch = CountDownLatch("hz:raft:countDownLatchService", "latch", self.context)
        result = latch.count_down_async()
        self.assertIsInstance(result, Future)

    def test_await_async_returns_future(self):
        """Test that await_async returns a Future."""
        latch = CountDownLatch("hz:raft:countDownLatchService", "latch", self.context)
        result = latch.await_async(timeout=5.0)
        self.assertIsInstance(result, Future)

    def test_await_alias(self):
        """Test that wait is an alias for await_."""
        latch = CountDownLatch("hz:raft:countDownLatchService", "latch", self.context)
        self.assertEqual(latch.wait, latch.await_)

    def test_get_round_async_returns_future(self):
        """Test that get_round_async returns a Future."""
        latch = CountDownLatch("hz:raft:countDownLatchService", "latch", self.context)
        result = latch.get_round_async()
        self.assertIsInstance(result, Future)


class TestFencedLockCodec(unittest.TestCase):
    """Tests for FencedLock protocol codec."""

    def test_encode_lock_request(self):
        """Test encoding a lock request."""
        from hazelcast.protocol.codec import FencedLockCodec

        msg = FencedLockCodec.encode_lock_request(
            "default", "lock", 12345, 67890, 11111
        )
        self.assertIsNotNone(msg)

    def test_encode_try_lock_request(self):
        """Test encoding a try_lock request."""
        from hazelcast.protocol.codec import FencedLockCodec

        msg = FencedLockCodec.encode_try_lock_request(
            "default", "lock", 12345, 67890, 11111, 1000
        )
        self.assertIsNotNone(msg)

    def test_encode_unlock_request(self):
        """Test encoding an unlock request."""
        from hazelcast.protocol.codec import FencedLockCodec

        msg = FencedLockCodec.encode_unlock_request(
            "default", "lock", 12345, 67890, 11111
        )
        self.assertIsNotNone(msg)

    def test_encode_get_lock_ownership_state_request(self):
        """Test encoding a get_lock_ownership_state request."""
        from hazelcast.protocol.codec import FencedLockCodec

        msg = FencedLockCodec.encode_get_lock_ownership_state_request("default", "lock")
        self.assertIsNotNone(msg)


class TestSemaphoreCodec(unittest.TestCase):
    """Tests for Semaphore protocol codec."""

    def test_encode_init_request(self):
        """Test encoding an init request."""
        from hazelcast.protocol.codec import SemaphoreCodec

        msg = SemaphoreCodec.encode_init_request("default", "sem", 5)
        self.assertIsNotNone(msg)

    def test_encode_acquire_request(self):
        """Test encoding an acquire request."""
        from hazelcast.protocol.codec import SemaphoreCodec

        msg = SemaphoreCodec.encode_acquire_request(
            "default", "sem", 12345, 67890, 11111, 1, 1000
        )
        self.assertIsNotNone(msg)

    def test_encode_release_request(self):
        """Test encoding a release request."""
        from hazelcast.protocol.codec import SemaphoreCodec

        msg = SemaphoreCodec.encode_release_request(
            "default", "sem", 12345, 67890, 11111, 1
        )
        self.assertIsNotNone(msg)

    def test_encode_drain_request(self):
        """Test encoding a drain request."""
        from hazelcast.protocol.codec import SemaphoreCodec

        msg = SemaphoreCodec.encode_drain_request(
            "default", "sem", 12345, 67890, 11111
        )
        self.assertIsNotNone(msg)

    def test_encode_change_request(self):
        """Test encoding a change request."""
        from hazelcast.protocol.codec import SemaphoreCodec

        msg = SemaphoreCodec.encode_change_request(
            "default", "sem", 12345, 67890, 11111, -2
        )
        self.assertIsNotNone(msg)

    def test_encode_available_permits_request(self):
        """Test encoding an available_permits request."""
        from hazelcast.protocol.codec import SemaphoreCodec

        msg = SemaphoreCodec.encode_available_permits_request("default", "sem")
        self.assertIsNotNone(msg)


class TestCountDownLatchCodec(unittest.TestCase):
    """Tests for CountDownLatch protocol codec."""

    def test_encode_try_set_count_request(self):
        """Test encoding a try_set_count request."""
        from hazelcast.protocol.codec import CountDownLatchCodec

        msg = CountDownLatchCodec.encode_try_set_count_request("default", "latch", 3)
        self.assertIsNotNone(msg)

    def test_encode_get_count_request(self):
        """Test encoding a get_count request."""
        from hazelcast.protocol.codec import CountDownLatchCodec

        msg = CountDownLatchCodec.encode_get_count_request("default", "latch")
        self.assertIsNotNone(msg)

    def test_encode_count_down_request(self):
        """Test encoding a count_down request."""
        from hazelcast.protocol.codec import CountDownLatchCodec

        msg = CountDownLatchCodec.encode_count_down_request("default", "latch", 11111, 1)
        self.assertIsNotNone(msg)

    def test_encode_await_request(self):
        """Test encoding an await request."""
        from hazelcast.protocol.codec import CountDownLatchCodec

        msg = CountDownLatchCodec.encode_await_request("default", "latch", 11111, 5000)
        self.assertIsNotNone(msg)

    def test_encode_get_round_request(self):
        """Test encoding a get_round request."""
        from hazelcast.protocol.codec import CountDownLatchCodec

        msg = CountDownLatchCodec.encode_get_round_request("default", "latch")
        self.assertIsNotNone(msg)


if __name__ == "__main__":
    unittest.main()
