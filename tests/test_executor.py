"""Tests for the Distributed Executor Service."""

import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from hazelcast.proxy.executor import (
    IExecutorService,
    Callable,
    Runnable,
    ExecutionCallback,
    MultiExecutionCallback,
    Member,
)
from hazelcast.proxy.base import ProxyContext
from hazelcast.exceptions import IllegalArgumentException, IllegalStateException


class SampleCallable(Callable):
    """Sample Callable for testing."""

    def __init__(self, value: int):
        self.value = value

    def call(self) -> int:
        return self.value * 2


class SampleRunnable(Runnable):
    """Sample Runnable for testing."""

    def __init__(self, message: str):
        self.message = message
        self.executed = False

    def run(self) -> None:
        self.executed = True


class TestCallable(unittest.TestCase):
    """Tests for the Callable base class."""

    def test_callable_abstract(self):
        """Test that Callable is abstract."""
        with self.assertRaises(TypeError):
            Callable()

    def test_callable_implementation(self):
        """Test Callable implementation."""
        task = SampleCallable(5)
        result = task.call()
        self.assertEqual(10, result)


class TestRunnable(unittest.TestCase):
    """Tests for the Runnable base class."""

    def test_runnable_abstract(self):
        """Test that Runnable is abstract."""
        with self.assertRaises(TypeError):
            Runnable()

    def test_runnable_implementation(self):
        """Test Runnable implementation."""
        task = SampleRunnable("test")
        self.assertFalse(task.executed)
        task.run()
        self.assertTrue(task.executed)


class TestExecutionCallback(unittest.TestCase):
    """Tests for the ExecutionCallback class."""

    def test_on_response_called(self):
        """Test that on_response callback is invoked."""
        received = []
        callback = ExecutionCallback(on_response=lambda r: received.append(r))

        callback.on_response("result")

        self.assertEqual(["result"], received)

    def test_on_failure_called(self):
        """Test that on_failure callback is invoked."""
        errors = []
        callback = ExecutionCallback(on_failure=lambda e: errors.append(e))

        error = Exception("test error")
        callback.on_failure(error)

        self.assertEqual([error], errors)

    def test_no_callback_provided(self):
        """Test that missing callbacks don't cause errors."""
        callback = ExecutionCallback()
        callback.on_response("result")
        callback.on_failure(Exception("error"))


class TestMultiExecutionCallback(unittest.TestCase):
    """Tests for the MultiExecutionCallback class."""

    def test_on_response_called(self):
        """Test per-member response callback."""
        responses = []
        callback = MultiExecutionCallback(
            on_response=lambda m, r: responses.append((m, r))
        )

        callback.on_response("member-1", "result-1")

        self.assertEqual([("member-1", "result-1")], responses)

    def test_on_failure_called(self):
        """Test per-member failure callback."""
        failures = []
        callback = MultiExecutionCallback(
            on_failure=lambda m, e: failures.append((m, e))
        )

        error = Exception("test error")
        callback.on_failure("member-1", error)

        self.assertEqual([("member-1", error)], failures)

    def test_on_complete_called(self):
        """Test overall completion callback."""
        results = []
        callback = MultiExecutionCallback(
            on_complete=lambda r: results.append(r)
        )

        all_results = {"member-1": "result-1", "member-2": "result-2"}
        callback.on_complete(all_results)

        self.assertEqual([all_results], results)


class TestMember(unittest.TestCase):
    """Tests for the Member class."""

    def test_member_properties(self):
        """Test Member property access."""
        member = Member("uuid-123", "localhost:5701", lite_member=False)

        self.assertEqual("uuid-123", member.uuid)
        self.assertEqual("localhost:5701", member.address)
        self.assertFalse(member.lite_member)

    def test_member_equality(self):
        """Test Member equality based on UUID."""
        member1 = Member("uuid-123", "host1:5701")
        member2 = Member("uuid-123", "host2:5702")
        member3 = Member("uuid-456", "host1:5701")

        self.assertEqual(member1, member2)
        self.assertNotEqual(member1, member3)

    def test_member_hash(self):
        """Test Member hash for use in dicts and sets."""
        member1 = Member("uuid-123")
        member2 = Member("uuid-123")

        self.assertEqual(hash(member1), hash(member2))

        member_set = {member1}
        self.assertIn(member2, member_set)

    def test_member_repr(self):
        """Test Member string representation."""
        member = Member("uuid-123", "localhost:5701")

        repr_str = repr(member)

        self.assertIn("uuid-123", repr_str)
        self.assertIn("localhost:5701", repr_str)


class TestIExecutorService(unittest.TestCase):
    """Tests for the IExecutorService proxy."""

    def setUp(self):
        """Set up test fixtures."""
        self.context = MagicMock(spec=ProxyContext)
        self.context.serialization_service = None
        self.context.partition_service = None
        self.executor = IExecutorService(
            "hz:impl:executorService",
            "test-executor",
            self.context,
        )

    def test_service_name(self):
        """Test executor service name."""
        self.assertEqual("hz:impl:executorService", self.executor.service_name)
        self.assertEqual("test-executor", self.executor.name)

    def test_submit(self):
        """Test submitting a task to random member."""
        task = SampleCallable(5)

        future = self.executor.submit(task)

        self.assertIsInstance(future, Future)
        self.assertTrue(future.done())

    def test_submit_with_callback(self):
        """Test submitting a task with callback."""
        task = SampleCallable(5)
        responses = []
        callback = ExecutionCallback(on_response=lambda r: responses.append(r))

        self.executor.submit(task, callback)

        self.assertEqual([None], responses)

    def test_submit_to_member(self):
        """Test submitting a task to a specific member."""
        task = SampleCallable(5)
        member = Member("member-uuid", "localhost:5701")

        future = self.executor.submit_to_member(task, member)

        self.assertIsInstance(future, Future)
        self.assertTrue(future.done())

    def test_submit_to_member_none_raises(self):
        """Test that None member raises exception."""
        task = SampleCallable(5)

        with self.assertRaises(IllegalArgumentException):
            self.executor.submit_to_member(task, None)

    def test_submit_to_key_owner(self):
        """Test submitting a task to key owner."""
        task = SampleCallable(5)

        future = self.executor.submit_to_key_owner(task, "my-key")

        self.assertIsInstance(future, Future)
        self.assertTrue(future.done())

    def test_submit_to_key_owner_none_raises(self):
        """Test that None key raises exception."""
        task = SampleCallable(5)

        with self.assertRaises(IllegalArgumentException):
            self.executor.submit_to_key_owner(task, None)

    def test_submit_to_all_members(self):
        """Test submitting a task to all members."""
        task = SampleCallable(5)

        results = self.executor.submit_to_all_members(task)

        self.assertIsInstance(results, dict)

    def test_submit_to_all_members_with_callback(self):
        """Test submitting to all members with callback."""
        task = SampleCallable(5)
        completed = []
        callback = MultiExecutionCallback(on_complete=lambda r: completed.append(r))

        self.executor.submit_to_all_members(task, callback)

        self.assertEqual(1, len(completed))

    def test_submit_to_members(self):
        """Test submitting a task to specific members."""
        task = SampleCallable(5)
        members = [
            Member("member-1", "host1:5701"),
            Member("member-2", "host2:5701"),
        ]

        results = self.executor.submit_to_members(task, members)

        self.assertIsInstance(results, dict)

    def test_submit_to_members_empty_raises(self):
        """Test that empty members list raises exception."""
        task = SampleCallable(5)

        with self.assertRaises(IllegalArgumentException):
            self.executor.submit_to_members(task, [])

    def test_shutdown(self):
        """Test executor shutdown."""
        self.assertFalse(self.executor.is_shutdown())

        self.executor.shutdown()

        self.assertTrue(self.executor.is_shutdown())

    def test_shutdown_idempotent(self):
        """Test that multiple shutdowns are safe."""
        self.executor.shutdown()
        self.executor.shutdown()

        self.assertTrue(self.executor.is_shutdown())

    def test_submit_after_shutdown_raises(self):
        """Test that submit after shutdown raises exception."""
        task = SampleCallable(5)
        self.executor.shutdown()

        with self.assertRaises(IllegalStateException):
            self.executor.submit(task)

    def test_submit_to_member_after_shutdown_raises(self):
        """Test that submit_to_member after shutdown raises exception."""
        task = SampleCallable(5)
        member = Member("member-uuid")
        self.executor.shutdown()

        with self.assertRaises(IllegalStateException):
            self.executor.submit_to_member(task, member)

    def test_submit_to_key_owner_after_shutdown_raises(self):
        """Test that submit_to_key_owner after shutdown raises exception."""
        task = SampleCallable(5)
        self.executor.shutdown()

        with self.assertRaises(IllegalStateException):
            self.executor.submit_to_key_owner(task, "key")

    def test_submit_to_all_members_after_shutdown_raises(self):
        """Test that submit_to_all_members after shutdown raises exception."""
        task = SampleCallable(5)
        self.executor.shutdown()

        with self.assertRaises(IllegalStateException):
            self.executor.submit_to_all_members(task)

    def test_is_terminated(self):
        """Test is_terminated returns shutdown state."""
        self.assertFalse(self.executor.is_terminated())

        self.executor.shutdown()

        self.assertTrue(self.executor.is_terminated())

    def test_destroyed_executor_raises(self):
        """Test that destroyed executor raises on submit."""
        task = SampleCallable(5)
        self.executor.destroy()

        with self.assertRaises(IllegalStateException):
            self.executor.submit(task)

    def test_repr(self):
        """Test executor string representation."""
        repr_str = repr(self.executor)

        self.assertIn("test-executor", repr_str)


class TestExecutorServiceCallbackErrors(unittest.TestCase):
    """Tests for callback error handling."""

    def setUp(self):
        """Set up test fixtures."""
        self.context = MagicMock(spec=ProxyContext)
        self.context.serialization_service = None
        self.executor = IExecutorService(
            "hz:impl:executorService",
            "test-executor",
            self.context,
        )

    def test_callback_exception_does_not_fail_submit(self):
        """Test that callback exceptions don't fail the submit."""
        task = SampleCallable(5)

        def bad_callback(r):
            raise ValueError("Callback error")

        callback = ExecutionCallback(on_response=bad_callback)

        future = self.executor.submit(task, callback)

        self.assertTrue(future.done())


if __name__ == "__main__":
    unittest.main()
