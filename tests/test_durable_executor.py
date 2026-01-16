"""Unit tests for Durable Executor Service proxy."""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future

from hazelcast.proxy.durable_executor import (
    DurableExecutorService,
    DurableExecutorServiceFuture,
    IDurableExecutorService,
)
from hazelcast.proxy.executor import Callable, Runnable, ExecutionCallback


class TestDurableExecutorServiceFuture(unittest.TestCase):
    """Tests for DurableExecutorServiceFuture class."""

    def test_future_initialization(self):
        """Test DurableExecutorServiceFuture initialization."""
        future = DurableExecutorServiceFuture()
        self.assertEqual(future.task_id, -1)

    def test_future_with_task_id(self):
        """Test DurableExecutorServiceFuture with task_id."""
        future = DurableExecutorServiceFuture(task_id=12345)
        self.assertEqual(future.task_id, 12345)

    def test_task_id_setter(self):
        """Test task_id setter."""
        future = DurableExecutorServiceFuture()
        future.task_id = 99999
        self.assertEqual(future.task_id, 99999)

    def test_future_is_instance_of_concurrent_future(self):
        """Test DurableExecutorServiceFuture is a Future subclass."""
        future = DurableExecutorServiceFuture()
        self.assertIsInstance(future, Future)

    def test_future_set_result(self):
        """Test setting result on the future."""
        future = DurableExecutorServiceFuture()
        future.set_result("test_result")
        self.assertEqual(future.result(), "test_result")


class TestDurableExecutorService(unittest.TestCase):
    """Tests for DurableExecutorService class."""

    def setUp(self):
        """Set up test fixtures."""
        self.executor = DurableExecutorService(
            service_name="hz:impl:durableExecutorService",
            name="test-durable-executor",
            context=None,
        )

    def test_submit_returns_durable_future(self):
        """Test submit returns DurableExecutorServiceFuture."""
        class TestCallable(Callable):
            def call(self):
                return 42

        future = self.executor.submit(TestCallable())
        self.assertIsInstance(future, DurableExecutorServiceFuture)

    def test_submit_with_callback(self):
        """Test submit with callback."""
        results = []

        class TestCallable(Callable):
            def call(self):
                return 42

        callback = ExecutionCallback(on_response=lambda r: results.append(r))
        future = self.executor.submit(TestCallable(), callback)
        self.assertIsInstance(future, DurableExecutorServiceFuture)

    def test_submit_to_key_owner_returns_future(self):
        """Test submit_to_key_owner returns DurableExecutorServiceFuture."""
        class TestCallable(Callable):
            def call(self):
                return 42

        future = self.executor.submit_to_key_owner(TestCallable(), "my-key")
        self.assertIsInstance(future, DurableExecutorServiceFuture)

    def test_submit_to_key_owner_with_callback(self):
        """Test submit_to_key_owner with callback."""
        results = []

        class TestCallable(Callable):
            def call(self):
                return 42

        callback = ExecutionCallback(on_response=lambda r: results.append(r))
        future = self.executor.submit_to_key_owner(TestCallable(), "key", callback)
        self.assertIsInstance(future, DurableExecutorServiceFuture)

    def test_submit_to_key_owner_none_raises(self):
        """Test submit_to_key_owner with None key raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.executor.submit_to_key_owner(TestCallable(), None)

    def test_retrieve_result_returns_future(self):
        """Test retrieve_result returns a Future."""
        unique_id = (10 << 32) | 5
        future = self.executor.retrieve_result(unique_id)
        self.assertIsInstance(future, Future)

    def test_dispose_result_returns_future(self):
        """Test dispose_result returns a Future."""
        unique_id = (10 << 32) | 5
        future = self.executor.dispose_result(unique_id)
        self.assertIsInstance(future, Future)

    def test_retrieve_and_dispose_result_returns_future(self):
        """Test retrieve_and_dispose_result returns a Future."""
        unique_id = (10 << 32) | 5
        future = self.executor.retrieve_and_dispose_result(unique_id)
        self.assertIsInstance(future, Future)

    def test_shutdown(self):
        """Test shutdown method."""
        self.assertFalse(self.executor.is_shutdown())
        self.executor.shutdown()
        self.assertTrue(self.executor.is_shutdown())

    def test_shutdown_idempotent(self):
        """Test shutdown is idempotent."""
        self.executor.shutdown()
        self.executor.shutdown()
        self.assertTrue(self.executor.is_shutdown())

    def test_submit_after_shutdown_raises(self):
        """Test submit after shutdown raises exception."""
        class TestCallable(Callable):
            def call(self):
                return 42

        self.executor.shutdown()
        from hazelcast.exceptions import IllegalStateException
        with self.assertRaises(IllegalStateException):
            self.executor.submit(TestCallable())

    def test_submit_to_key_owner_after_shutdown_raises(self):
        """Test submit_to_key_owner after shutdown raises exception."""
        class TestCallable(Callable):
            def call(self):
                return 42

        self.executor.shutdown()
        from hazelcast.exceptions import IllegalStateException
        with self.assertRaises(IllegalStateException):
            self.executor.submit_to_key_owner(TestCallable(), "key")

    def test_retrieve_result_after_shutdown_raises(self):
        """Test retrieve_result after shutdown raises exception."""
        self.executor.shutdown()
        from hazelcast.exceptions import IllegalStateException
        with self.assertRaises(IllegalStateException):
            self.executor.retrieve_result(12345)

    def test_dispose_result_after_shutdown_raises(self):
        """Test dispose_result after shutdown raises exception."""
        self.executor.shutdown()
        from hazelcast.exceptions import IllegalStateException
        with self.assertRaises(IllegalStateException):
            self.executor.dispose_result(12345)

    def test_retrieve_and_dispose_result_after_shutdown_raises(self):
        """Test retrieve_and_dispose_result after shutdown raises exception."""
        self.executor.shutdown()
        from hazelcast.exceptions import IllegalStateException
        with self.assertRaises(IllegalStateException):
            self.executor.retrieve_and_dispose_result(12345)


class TestDurableExecutorAlias(unittest.TestCase):
    """Tests for durable executor service alias."""

    def test_i_durable_executor_service_alias(self):
        """Test IDurableExecutorService alias."""
        self.assertIs(IDurableExecutorService, DurableExecutorService)


class TestUniqueIdEncoding(unittest.TestCase):
    """Tests for unique ID encoding/decoding logic."""

    def test_unique_id_encoding(self):
        """Test unique ID encodes partition_id and sequence correctly."""
        partition_id = 42
        sequence = 12345

        unique_id = (partition_id << 32) | (sequence & 0xFFFFFFFF)

        decoded_sequence = unique_id & 0xFFFFFFFF
        decoded_partition_id = (unique_id >> 32) & 0xFFFFFFFF

        self.assertEqual(decoded_partition_id, partition_id)
        self.assertEqual(decoded_sequence, sequence)

    def test_unique_id_with_large_values(self):
        """Test unique ID with large partition_id and sequence."""
        partition_id = 270
        sequence = 0xFFFFFFFE

        unique_id = (partition_id << 32) | (sequence & 0xFFFFFFFF)

        decoded_sequence = unique_id & 0xFFFFFFFF
        decoded_partition_id = (unique_id >> 32) & 0xFFFFFFFF

        self.assertEqual(decoded_partition_id, partition_id)
        self.assertEqual(decoded_sequence, sequence)


class TestDurableExecutorWithRunnable(unittest.TestCase):
    """Tests for DurableExecutorService with Runnable tasks."""

    def setUp(self):
        """Set up test fixtures."""
        self.executor = DurableExecutorService(
            service_name="hz:impl:durableExecutorService",
            name="test-durable-executor",
            context=None,
        )

    def test_submit_runnable(self):
        """Test submit with Runnable task."""
        class TestRunnable(Runnable):
            def __init__(self):
                self.executed = False

            def run(self):
                self.executed = True

        future = self.executor.submit(TestRunnable())
        self.assertIsInstance(future, DurableExecutorServiceFuture)

    def test_submit_to_key_owner_runnable(self):
        """Test submit_to_key_owner with Runnable task."""
        class TestRunnable(Runnable):
            def run(self):
                pass

        future = self.executor.submit_to_key_owner(TestRunnable(), "key")
        self.assertIsInstance(future, DurableExecutorServiceFuture)


if __name__ == "__main__":
    unittest.main()
