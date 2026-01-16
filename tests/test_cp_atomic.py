"""Tests for CP Subsystem atomic data structures."""

import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from hazelcast.cp.atomic import AtomicLong, AtomicReference
from hazelcast.proxy.base import ProxyContext


class MockInvocationService:
    """Mock invocation service for testing."""

    def __init__(self, return_value=None):
        self._return_value = return_value

    def invoke(self, invocation):
        future = Future()
        future.set_result(self._return_value)
        return future


class TestAtomicLong(unittest.TestCase):
    """Tests for AtomicLong proxy."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_invocation = MockInvocationService()
        self.context = ProxyContext(
            invocation_service=self.mock_invocation,
            serialization_service=None,
            partition_service=None,
        )

    def test_init(self):
        """Test AtomicLong initialization."""
        atomic = AtomicLong("hz:raft:atomicLongService", "test-counter", self.context)
        self.assertEqual(atomic.name, "test-counter")
        self.assertEqual(atomic.service_name, "hz:raft:atomicLongService")

    def test_parse_group_id_default(self):
        """Test default CP group ID parsing."""
        atomic = AtomicLong("hz:raft:atomicLongService", "counter", self.context)
        self.assertEqual(atomic._group_id, "default")

    def test_parse_group_id_custom(self):
        """Test custom CP group ID parsing."""
        atomic = AtomicLong("hz:raft:atomicLongService", "counter@custom-group", self.context)
        self.assertEqual(atomic._group_id, "custom-group")

    def test_get_object_name_simple(self):
        """Test object name extraction without group."""
        atomic = AtomicLong("hz:raft:atomicLongService", "counter", self.context)
        self.assertEqual(atomic._get_object_name(), "counter")

    def test_get_object_name_with_group(self):
        """Test object name extraction with group."""
        atomic = AtomicLong("hz:raft:atomicLongService", "counter@my-group", self.context)
        self.assertEqual(atomic._get_object_name(), "counter")

    def test_get_async_returns_future(self):
        """Test that get_async returns a Future."""
        atomic = AtomicLong("hz:raft:atomicLongService", "counter", self.context)
        result = atomic.get_async()
        self.assertIsInstance(result, Future)

    def test_set_async_returns_future(self):
        """Test that set_async returns a Future."""
        atomic = AtomicLong("hz:raft:atomicLongService", "counter", self.context)
        result = atomic.set_async(42)
        self.assertIsInstance(result, Future)

    def test_compare_and_set_async_returns_future(self):
        """Test that compare_and_set_async returns a Future."""
        atomic = AtomicLong("hz:raft:atomicLongService", "counter", self.context)
        result = atomic.compare_and_set_async(0, 1)
        self.assertIsInstance(result, Future)

    def test_increment_and_get_uses_add_and_get(self):
        """Test increment_and_get delegates to add_and_get."""
        atomic = AtomicLong("hz:raft:atomicLongService", "counter", self.context)
        with patch.object(atomic, "add_and_get", return_value=1) as mock_add:
            result = atomic.increment_and_get()
            mock_add.assert_called_once_with(1)
            self.assertEqual(result, 1)

    def test_decrement_and_get_uses_add_and_get(self):
        """Test decrement_and_get delegates to add_and_get with -1."""
        atomic = AtomicLong("hz:raft:atomicLongService", "counter", self.context)
        with patch.object(atomic, "add_and_get", return_value=-1) as mock_add:
            result = atomic.decrement_and_get()
            mock_add.assert_called_once_with(-1)
            self.assertEqual(result, -1)

    def test_get_and_increment_uses_get_and_add(self):
        """Test get_and_increment delegates to get_and_add."""
        atomic = AtomicLong("hz:raft:atomicLongService", "counter", self.context)
        with patch.object(atomic, "get_and_add", return_value=0) as mock_get_add:
            result = atomic.get_and_increment()
            mock_get_add.assert_called_once_with(1)
            self.assertEqual(result, 0)

    def test_get_and_decrement_uses_get_and_add(self):
        """Test get_and_decrement delegates to get_and_add with -1."""
        atomic = AtomicLong("hz:raft:atomicLongService", "counter", self.context)
        with patch.object(atomic, "get_and_add", return_value=1) as mock_get_add:
            result = atomic.get_and_decrement()
            mock_get_add.assert_called_once_with(-1)
            self.assertEqual(result, 1)

    def test_add_and_get_async_returns_future(self):
        """Test that add_and_get_async returns a Future."""
        atomic = AtomicLong("hz:raft:atomicLongService", "counter", self.context)
        result = atomic.add_and_get_async(5)
        self.assertIsInstance(result, Future)

    def test_get_and_add_async_returns_future(self):
        """Test that get_and_add_async returns a Future."""
        atomic = AtomicLong("hz:raft:atomicLongService", "counter", self.context)
        result = atomic.get_and_add_async(5)
        self.assertIsInstance(result, Future)

    def test_get_and_set_async_returns_future(self):
        """Test that get_and_set_async returns a Future."""
        atomic = AtomicLong("hz:raft:atomicLongService", "counter", self.context)
        result = atomic.get_and_set_async(100)
        self.assertIsInstance(result, Future)


class TestAtomicReference(unittest.TestCase):
    """Tests for AtomicReference proxy."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_invocation = MockInvocationService()
        self.context = ProxyContext(
            invocation_service=self.mock_invocation,
            serialization_service=None,
            partition_service=None,
        )

    def test_init(self):
        """Test AtomicReference initialization."""
        ref = AtomicReference("hz:raft:atomicRefService", "test-ref", self.context)
        self.assertEqual(ref.name, "test-ref")
        self.assertEqual(ref.service_name, "hz:raft:atomicRefService")

    def test_parse_group_id_default(self):
        """Test default CP group ID parsing."""
        ref = AtomicReference("hz:raft:atomicRefService", "ref", self.context)
        self.assertEqual(ref._group_id, "default")

    def test_parse_group_id_custom(self):
        """Test custom CP group ID parsing."""
        ref = AtomicReference("hz:raft:atomicRefService", "ref@custom", self.context)
        self.assertEqual(ref._group_id, "custom")

    def test_get_object_name_simple(self):
        """Test object name extraction without group."""
        ref = AtomicReference("hz:raft:atomicRefService", "ref", self.context)
        self.assertEqual(ref._get_object_name(), "ref")

    def test_get_object_name_with_group(self):
        """Test object name extraction with group."""
        ref = AtomicReference("hz:raft:atomicRefService", "ref@group", self.context)
        self.assertEqual(ref._get_object_name(), "ref")

    def test_get_async_returns_future(self):
        """Test that get_async returns a Future."""
        ref = AtomicReference("hz:raft:atomicRefService", "ref", self.context)
        result = ref.get_async()
        self.assertIsInstance(result, Future)

    def test_set_async_returns_future(self):
        """Test that set_async returns a Future."""
        ref = AtomicReference("hz:raft:atomicRefService", "ref", self.context)
        result = ref.set_async({"key": "value"})
        self.assertIsInstance(result, Future)

    def test_compare_and_set_async_returns_future(self):
        """Test that compare_and_set_async returns a Future."""
        ref = AtomicReference("hz:raft:atomicRefService", "ref", self.context)
        result = ref.compare_and_set_async("old", "new")
        self.assertIsInstance(result, Future)

    def test_is_null_async_returns_future(self):
        """Test that is_null_async returns a Future."""
        ref = AtomicReference("hz:raft:atomicRefService", "ref", self.context)
        result = ref.is_null_async()
        self.assertIsInstance(result, Future)

    def test_clear_delegates_to_set_none(self):
        """Test clear calls set with None."""
        ref = AtomicReference("hz:raft:atomicRefService", "ref", self.context)
        with patch.object(ref, "set") as mock_set:
            ref.clear()
            mock_set.assert_called_once_with(None)

    def test_clear_async_delegates_to_set_async(self):
        """Test clear_async calls set_async with None."""
        ref = AtomicReference("hz:raft:atomicRefService", "ref", self.context)
        with patch.object(ref, "set_async") as mock_set_async:
            ref.clear_async()
            mock_set_async.assert_called_once_with(None)

    def test_contains_async_returns_future(self):
        """Test that contains_async returns a Future."""
        ref = AtomicReference("hz:raft:atomicRefService", "ref", self.context)
        result = ref.contains_async("test")
        self.assertIsInstance(result, Future)

    def test_get_and_set_async_returns_future(self):
        """Test that get_and_set_async returns a Future."""
        ref = AtomicReference("hz:raft:atomicRefService", "ref", self.context)
        result = ref.get_and_set_async("new_value")
        self.assertIsInstance(result, Future)


class TestAtomicLongCodec(unittest.TestCase):
    """Tests for AtomicLong protocol codec."""

    def test_encode_get_request(self):
        """Test encoding a get request."""
        from hazelcast.protocol.codec import AtomicLongCodec

        msg = AtomicLongCodec.encode_get_request("default", "counter")
        self.assertIsNotNone(msg)

    def test_encode_set_request(self):
        """Test encoding a set request."""
        from hazelcast.protocol.codec import AtomicLongCodec

        msg = AtomicLongCodec.encode_set_request("default", "counter", 42)
        self.assertIsNotNone(msg)

    def test_encode_compare_and_set_request(self):
        """Test encoding a compare-and-set request."""
        from hazelcast.protocol.codec import AtomicLongCodec

        msg = AtomicLongCodec.encode_compare_and_set_request("default", "counter", 0, 1)
        self.assertIsNotNone(msg)

    def test_encode_add_and_get_request(self):
        """Test encoding an add-and-get request."""
        from hazelcast.protocol.codec import AtomicLongCodec

        msg = AtomicLongCodec.encode_add_and_get_request("default", "counter", 10)
        self.assertIsNotNone(msg)

    def test_encode_get_and_add_request(self):
        """Test encoding a get-and-add request."""
        from hazelcast.protocol.codec import AtomicLongCodec

        msg = AtomicLongCodec.encode_get_and_add_request("default", "counter", 10)
        self.assertIsNotNone(msg)

    def test_encode_get_and_set_request(self):
        """Test encoding a get-and-set request."""
        from hazelcast.protocol.codec import AtomicLongCodec

        msg = AtomicLongCodec.encode_get_and_set_request("default", "counter", 100)
        self.assertIsNotNone(msg)


class TestAtomicReferenceCodec(unittest.TestCase):
    """Tests for AtomicReference protocol codec."""

    def test_encode_get_request(self):
        """Test encoding a get request."""
        from hazelcast.protocol.codec import AtomicReferenceCodec

        msg = AtomicReferenceCodec.encode_get_request("default", "ref")
        self.assertIsNotNone(msg)

    def test_encode_set_request(self):
        """Test encoding a set request."""
        from hazelcast.protocol.codec import AtomicReferenceCodec

        msg = AtomicReferenceCodec.encode_set_request("default", "ref", b"value")
        self.assertIsNotNone(msg)

    def test_encode_set_request_null(self):
        """Test encoding a set request with null value."""
        from hazelcast.protocol.codec import AtomicReferenceCodec

        msg = AtomicReferenceCodec.encode_set_request("default", "ref", None)
        self.assertIsNotNone(msg)

    def test_encode_compare_and_set_request(self):
        """Test encoding a compare-and-set request."""
        from hazelcast.protocol.codec import AtomicReferenceCodec

        msg = AtomicReferenceCodec.encode_compare_and_set_request(
            "default", "ref", b"old", b"new"
        )
        self.assertIsNotNone(msg)

    def test_encode_is_null_request(self):
        """Test encoding an is-null request."""
        from hazelcast.protocol.codec import AtomicReferenceCodec

        msg = AtomicReferenceCodec.encode_is_null_request("default", "ref")
        self.assertIsNotNone(msg)

    def test_encode_contains_request(self):
        """Test encoding a contains request."""
        from hazelcast.protocol.codec import AtomicReferenceCodec

        msg = AtomicReferenceCodec.encode_contains_request("default", "ref", b"value")
        self.assertIsNotNone(msg)


if __name__ == "__main__":
    unittest.main()
