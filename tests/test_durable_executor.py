"""Tests for DurableExecutorService proxy."""

import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from hazelcast.proxy.durable_executor import (
    DurableExecutorService,
    DurableExecutorServiceFuture,
    IDurableExecutorService,
)
from hazelcast.proxy.executor import Callable, Runnable, ExecutionCallback
from hazelcast.proxy.base import ProxyContext
from hazelcast.exceptions import IllegalArgumentException, IllegalStateException


class SimpleCallable(Callable):
    """Test callable that returns a value."""

    def __init__(self, value):
        self.value = value

    def call(self):
        return self.value


class SimpleRunnable(Runnable):
    """Test runnable that does nothing."""

    def run(self):
        pass


class TestDurableExecutorServiceFuture(unittest.TestCase):
    """Tests for DurableExecutorServiceFuture."""

    def test_task_id_default(self):
        """Test default task ID is -1."""
        future = DurableExecutorServiceFuture()
        self.assertEqual(-1, future.task_id)

    def test_task_id_initialization(self):
        """Test task ID can be set during initialization."""
        future = DurableExecutorServiceFuture(task_id=12345)
        self.assertEqual(12345, future.task_id)

    def test_task_id_setter(self):
        """Test task ID can be set after creation."""
        future = DurableExecutorServiceFuture()
        future.task_id = 67890
        self.assertEqual(67890, future.task_id)

    def test_inherits_from_future(self):
        """Test that DurableExecutorServiceFuture is a Future."""
        future = DurableExecutorServiceFuture()
        self.assertIsInstance(future, Future)

    def test_set_result(self):
        """Test that result can be set like a normal Future."""
        future = DurableExecutorServiceFuture(task_id=100)
        future.set_result("test_value")
        self.assertEqual("test_value", future.result())
        self.assertEqual(100, future.task_id)


class TestDurableExecutorServiceInit(unittest.TestCase):
    """Tests for DurableExecutorService initialization."""

    def test_initialization(self):
        """Test basic initialization."""
        executor = DurableExecutorService(
            "hz:impl:durableExecutorService",
            "test-executor",
            None,
        )
        self.assertEqual("test-executor", executor._name)
        self.assertFalse(executor._is_shutdown)

    def test_initialization_with_context(self):
        """Test initialization with proxy context."""
        mock_context = MagicMock(spec=ProxyContext)
        executor = DurableExecutorService(
            "hz:impl:durableExecutorService",
            "test-executor",
            mock_context,
        )
        self.assertEqual(mock_context, executor._context)


class TestDurableExecutorServiceSubmit(unittest.TestCase):
    """Tests for DurableExecutorService submit methods."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_context = MagicMock(spec=ProxyContext)
        self.mock_context.invocation_service = MagicMock()
        self.mock_context.serialization_service = MagicMock()
        self.mock_context.serialization_service.to_data.return_value = b"serialized"

        self.executor = DurableExecutorService(
            "hz:impl:durableExecutorService",
            "test-executor",
            self.mock_context,
        )

    def test_submit_returns_durable_future(self):
        """Test that submit returns a DurableExecutorServiceFuture."""
        task = SimpleCallable("result")
        future = self.executor.submit(task)
        self.assertIsInstance(future, DurableExecutorServiceFuture)

    def test_submit_when_shutdown_raises(self):
        """Test that submit raises when executor is shut down."""
        self.executor._is_shutdown = True
        task = SimpleCallable("result")

        with self.assertRaises(IllegalStateException):
            self.executor.submit(task)

    def test_submit_to_key_owner_returns_durable_future(self):
        """Test that submit_to_key_owner returns a DurableExecutorServiceFuture."""
        task = SimpleCallable("result")
        future = self.executor.submit_to_key_owner(task, "test-key")
        self.assertIsInstance(future, DurableExecutorServiceFuture)

    def test_submit_to_key_owner_with_none_key_raises(self):
        """Test that submit_to_key_owner raises with None key."""
        task = SimpleCallable("result")

        with self.assertRaises(IllegalArgumentException):
            self.executor.submit_to_key_owner(task, None)

    def test_submit_to_key_owner_when_shutdown_raises(self):
        """Test that submit_to_key_owner raises when executor is shut down."""
        self.executor._is_shutdown = True
        task = SimpleCallable("result")

        with self.assertRaises(IllegalStateException):
            self.executor.submit_to_key_owner(task, "test-key")


class TestDurableExecutorServiceRetrieve(unittest.TestCase):
    """Tests for DurableExecutorService retrieve methods."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_context = MagicMock(spec=ProxyContext)
        self.mock_context.invocation_service = MagicMock()

        self.executor = DurableExecutorService(
            "hz:impl:durableExecutorService",
            "test-executor",
            self.mock_context,
        )

    def test_retrieve_result_returns_future(self):
        """Test that retrieve_result returns a Future."""
        future = self.executor.retrieve_result(12345)
        self.assertIsInstance(future, Future)

    def test_retrieve_result_when_shutdown_raises(self):
        """Test that retrieve_result raises when executor is shut down."""
        self.executor._is_shutdown = True

        with self.assertRaises(IllegalStateException):
            self.executor.retrieve_result(12345)

    def test_dispose_result_returns_future(self):
        """Test that dispose_result returns a Future."""
        future = self.executor.dispose_result(12345)
        self.assertIsInstance(future, Future)

    def test_dispose_result_when_shutdown_raises(self):
        """Test that dispose_result raises when executor is shut down."""
        self.executor._is_shutdown = True

        with self.assertRaises(IllegalStateException):
            self.executor.dispose_result(12345)

    def test_retrieve_and_dispose_result_returns_future(self):
        """Test that retrieve_and_dispose_result returns a Future."""
        future = self.executor.retrieve_and_dispose_result(12345)
        self.assertIsInstance(future, Future)

    def test_retrieve_and_dispose_result_when_shutdown_raises(self):
        """Test that retrieve_and_dispose_result raises when executor is shut down."""
        self.executor._is_shutdown = True

        with self.assertRaises(IllegalStateException):
            self.executor.retrieve_and_dispose_result(12345)


class TestDurableExecutorServiceShutdown(unittest.TestCase):
    """Tests for DurableExecutorService shutdown."""

    def setUp(self):
        """Set up test fixtures."""
        self.executor = DurableExecutorService(
            "hz:impl:durableExecutorService",
            "test-executor",
            None,
        )

    def test_is_shutdown_initially_false(self):
        """Test that is_shutdown returns False initially."""
        self.assertFalse(self.executor.is_shutdown())

    def test_shutdown_sets_is_shutdown(self):
        """Test that shutdown sets is_shutdown to True."""
        self.executor.shutdown()
        self.assertTrue(self.executor.is_shutdown())

    def test_shutdown_idempotent(self):
        """Test that multiple shutdown calls are safe."""
        self.executor.shutdown()
        self.executor.shutdown()
        self.assertTrue(self.executor.is_shutdown())


class TestDurableExecutorServiceCallback(unittest.TestCase):
    """Tests for DurableExecutorService callback handling."""

    def test_callback_on_response(self):
        """Test that callback on_response is called on success."""
        callback = ExecutionCallback(
            on_response=MagicMock(),
            on_failure=MagicMock(),
        )

        executor = DurableExecutorService(
            "hz:impl:durableExecutorService",
            "test-executor",
            None,
        )

        future = DurableExecutorServiceFuture()
        executor._complete_with_callback(future, "result", callback)

        callback._on_response.assert_called_once_with("result")
        callback._on_failure.assert_not_called()

    def test_callback_on_failure(self):
        """Test that callback on_failure is called on error."""
        callback = ExecutionCallback(
            on_response=MagicMock(),
            on_failure=MagicMock(),
        )

        executor = DurableExecutorService(
            "hz:impl:durableExecutorService",
            "test-executor",
            None,
        )

        future = DurableExecutorServiceFuture()
        error = Exception("test error")
        executor._complete_with_callback(future, None, callback, error)

        callback._on_failure.assert_called_once_with(error)
        callback._on_response.assert_not_called()


class TestDurableExecutorServiceAlias(unittest.TestCase):
    """Tests for DurableExecutorService aliases."""

    def test_idurable_executor_service_alias(self):
        """Test that IDurableExecutorService is an alias."""
        self.assertIs(IDurableExecutorService, DurableExecutorService)


class TestUniqueIdEncoding(unittest.TestCase):
    """Tests for unique ID encoding/decoding."""

    def test_unique_id_roundtrip(self):
        """Test that partition_id and sequence roundtrip correctly."""
        partition_id = 42
        sequence = 12345

        unique_id = (partition_id << 32) | (sequence & 0xFFFFFFFF)

        decoded_sequence = unique_id & 0xFFFFFFFF
        decoded_partition_id = (unique_id >> 32) & 0xFFFFFFFF

        self.assertEqual(partition_id, decoded_partition_id)
        self.assertEqual(sequence, decoded_sequence)

    def test_unique_id_with_large_values(self):
        """Test unique ID encoding with large values."""
        partition_id = 270
        sequence = 0x7FFFFFFF

        unique_id = (partition_id << 32) | (sequence & 0xFFFFFFFF)

        decoded_sequence = unique_id & 0xFFFFFFFF
        decoded_partition_id = (unique_id >> 32) & 0xFFFFFFFF

        self.assertEqual(partition_id, decoded_partition_id)
        self.assertEqual(sequence, decoded_sequence)


if __name__ == "__main__":
    unittest.main()
