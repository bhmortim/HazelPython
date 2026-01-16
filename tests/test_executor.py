"""Unit tests for Executor Service proxy."""

import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from hazelcast.proxy.executor import (
    Callable,
    Runnable,
    ExecutionCallback,
    MultiExecutionCallback,
    Member,
    MemberSelector,
    LiteMemberSelector,
    DataMemberSelector,
    IExecutorService,
)
from hazelcast.proxy.base import ProxyContext
from hazelcast.exceptions import IllegalArgumentException, IllegalStateException


class SumTask(Callable):
    """Test task that sums two numbers."""

    def __init__(self, a: int, b: int):
        self.a = a
        self.b = b

    def call(self) -> int:
        return self.a + self.b


class PrintTask(Runnable):
    """Test task that prints a message."""

    def __init__(self, message: str):
        self.message = message

    def run(self) -> None:
        print(self.message)


class TestCallable(unittest.TestCase):
    """Tests for Callable interface."""

    def test_callable_implementation(self):
        task = SumTask(3, 5)
        self.assertEqual(8, task.call())

    def test_callable_with_different_values(self):
        task = SumTask(10, 20)
        self.assertEqual(30, task.call())

    def test_callable_with_negative_values(self):
        task = SumTask(-5, 10)
        self.assertEqual(5, task.call())


class TestRunnable(unittest.TestCase):
    """Tests for Runnable interface."""

    def test_runnable_implementation(self):
        task = PrintTask("Hello")
        task.run()

    def test_runnable_does_not_return_value(self):
        task = PrintTask("Test")
        result = task.run()
        self.assertIsNone(result)


class TestMember(unittest.TestCase):
    """Tests for Member class."""

    def test_member_creation(self):
        member = Member("uuid-123", "127.0.0.1:5701", False)
        self.assertEqual("uuid-123", member.uuid)
        self.assertEqual("127.0.0.1:5701", member.address)
        self.assertFalse(member.lite_member)

    def test_member_lite_member(self):
        member = Member("uuid-456", lite_member=True)
        self.assertTrue(member.lite_member)

    def test_member_equality(self):
        member1 = Member("uuid-123")
        member2 = Member("uuid-123")
        member3 = Member("uuid-456")

        self.assertEqual(member1, member2)
        self.assertNotEqual(member1, member3)

    def test_member_hash(self):
        member1 = Member("uuid-123")
        member2 = Member("uuid-123")

        self.assertEqual(hash(member1), hash(member2))
        members_set = {member1, member2}
        self.assertEqual(1, len(members_set))

    def test_member_repr(self):
        member = Member("uuid-123", "localhost:5701")
        repr_str = repr(member)
        self.assertIn("uuid-123", repr_str)
        self.assertIn("localhost:5701", repr_str)


class TestExecutionCallback(unittest.TestCase):
    """Tests for ExecutionCallback class."""

    def test_callback_on_response(self):
        results = []
        callback = ExecutionCallback(
            on_response=lambda r: results.append(r)
        )
        callback.on_response("result")
        self.assertEqual(["result"], results)

    def test_callback_on_failure(self):
        errors = []
        callback = ExecutionCallback(
            on_failure=lambda e: errors.append(e)
        )
        error = Exception("test error")
        callback.on_failure(error)
        self.assertEqual([error], errors)

    def test_callback_with_no_handlers(self):
        callback = ExecutionCallback()
        callback.on_response("result")
        callback.on_failure(Exception("error"))


class TestMultiExecutionCallback(unittest.TestCase):
    """Tests for MultiExecutionCallback class."""

    def test_multi_callback_on_response(self):
        results = []
        callback = MultiExecutionCallback(
            on_response=lambda uuid, r: results.append((uuid, r))
        )
        callback.on_response("member-1", "result-1")
        callback.on_response("member-2", "result-2")
        self.assertEqual([("member-1", "result-1"), ("member-2", "result-2")], results)

    def test_multi_callback_on_failure(self):
        errors = []
        callback = MultiExecutionCallback(
            on_failure=lambda uuid, e: errors.append((uuid, e))
        )
        error = Exception("test error")
        callback.on_failure("member-1", error)
        self.assertEqual([("member-1", error)], errors)

    def test_multi_callback_on_complete(self):
        final_results = []
        callback = MultiExecutionCallback(
            on_complete=lambda r: final_results.append(r)
        )
        all_results = {"member-1": "result-1", "member-2": "result-2"}
        callback.on_complete(all_results)
        self.assertEqual([all_results], final_results)


class TestMemberSelector(unittest.TestCase):
    """Tests for MemberSelector implementations."""

    def test_lite_member_selector(self):
        selector = LiteMemberSelector()
        data_member = Member("uuid-1", lite_member=False)
        lite_member = Member("uuid-2", lite_member=True)

        self.assertFalse(selector.select(data_member))
        self.assertTrue(selector.select(lite_member))

    def test_data_member_selector(self):
        selector = DataMemberSelector()
        data_member = Member("uuid-1", lite_member=False)
        lite_member = Member("uuid-2", lite_member=True)

        self.assertTrue(selector.select(data_member))
        self.assertFalse(selector.select(lite_member))


class TestIExecutorService(unittest.TestCase):
    """Tests for IExecutorService proxy."""

    def setUp(self):
        self.context = MagicMock(spec=ProxyContext)
        self.context.invocation_service = MagicMock()
        self.context.serialization_service = MagicMock()
        self.context.partition_service = MagicMock()

        self.executor = IExecutorService(
            "hz:impl:executorService",
            "test-executor",
            self.context,
        )

    def test_executor_creation(self):
        self.assertEqual("test-executor", self.executor.name)
        self.assertEqual("hz:impl:executorService", self.executor.service_name)

    def test_submit_validates_shutdown(self):
        self.executor.shutdown()
        task = SumTask(1, 2)

        with self.assertRaises(IllegalStateException):
            self.executor.submit(task)

    def test_submit_to_member_validates_member_not_none(self):
        task = SumTask(1, 2)

        with self.assertRaises(IllegalArgumentException):
            self.executor.submit_to_member(task, None)

    def test_submit_to_key_owner_validates_key_not_none(self):
        task = SumTask(1, 2)

        with self.assertRaises(IllegalArgumentException):
            self.executor.submit_to_key_owner(task, None)

    def test_submit_to_members_validates_members_not_empty(self):
        task = SumTask(1, 2)

        with self.assertRaises(IllegalArgumentException):
            self.executor.submit_to_members(task, [])

    def test_shutdown(self):
        self.assertFalse(self.executor.is_shutdown())
        self.executor.shutdown()
        self.assertTrue(self.executor.is_shutdown())

    def test_shutdown_idempotent(self):
        self.executor.shutdown()
        self.executor.shutdown()
        self.assertTrue(self.executor.is_shutdown())

    def test_is_terminated_after_shutdown(self):
        self.executor.shutdown()
        self.assertTrue(self.executor.is_terminated())

    def test_submit_returns_future(self):
        task = SumTask(1, 2)
        future = self.executor.submit(task)
        self.assertIsInstance(future, Future)

    def test_submit_to_member_returns_future(self):
        task = SumTask(1, 2)
        member = Member("uuid-123")
        future = self.executor.submit_to_member(task, member)
        self.assertIsInstance(future, Future)

    def test_submit_to_key_owner_returns_future(self):
        task = SumTask(1, 2)
        future = self.executor.submit_to_key_owner(task, "my-key")
        self.assertIsInstance(future, Future)

    def test_submit_to_all_members_returns_dict(self):
        task = SumTask(1, 2)
        results = self.executor.submit_to_all_members(task)
        self.assertIsInstance(results, dict)

    def test_submit_to_members_returns_dict(self):
        task = SumTask(1, 2)
        member1 = Member("uuid-1")
        member2 = Member("uuid-2")
        results = self.executor.submit_to_members(task, [member1, member2])
        self.assertIsInstance(results, dict)

    def test_add_and_remove_member(self):
        member = Member("uuid-123", "localhost:5701")
        self.executor._add_member(member)
        self.assertIn(member, self.executor._get_members())

        self.executor._remove_member("uuid-123")
        self.assertNotIn(member, self.executor._get_members())

    def test_select_members_with_selector(self):
        data_member = Member("uuid-1", lite_member=False)
        lite_member = Member("uuid-2", lite_member=True)

        self.executor._add_member(data_member)
        self.executor._add_member(lite_member)

        selector = DataMemberSelector()
        selected = self.executor._select_members(selector)

        self.assertEqual(1, len(selected))
        self.assertIn(data_member, selected)

    def test_submit_to_member_with_selector(self):
        data_member = Member("uuid-1", lite_member=False)
        self.executor._add_member(data_member)

        task = SumTask(1, 2)
        selector = DataMemberSelector()

        future = self.executor.submit_to_member_with_selector(task, selector)
        self.assertIsInstance(future, Future)

    def test_submit_to_member_with_selector_no_match(self):
        lite_member = Member("uuid-1", lite_member=True)
        self.executor._add_member(lite_member)

        task = SumTask(1, 2)
        selector = DataMemberSelector()

        with self.assertRaises(IllegalArgumentException):
            self.executor.submit_to_member_with_selector(task, selector)

    def test_submit_to_all_members_with_selector(self):
        data_member1 = Member("uuid-1", lite_member=False)
        data_member2 = Member("uuid-2", lite_member=False)
        lite_member = Member("uuid-3", lite_member=True)

        self.executor._add_member(data_member1)
        self.executor._add_member(data_member2)
        self.executor._add_member(lite_member)

        task = SumTask(1, 2)
        selector = DataMemberSelector()

        results = self.executor.submit_to_all_members_with_selector(task, selector)
        self.assertEqual(2, len(results))
        self.assertIn(data_member1, results)
        self.assertIn(data_member2, results)

    def test_destroy_marks_executor_destroyed(self):
        self.executor.destroy()
        self.assertTrue(self.executor.is_destroyed)

    def test_operations_after_destroy_raise_exception(self):
        self.executor.destroy()
        task = SumTask(1, 2)

        with self.assertRaises(IllegalStateException):
            self.executor.submit(task)


class TestExecutionCallbackIntegration(unittest.TestCase):
    """Integration tests for execution callbacks."""

    def setUp(self):
        self.context = MagicMock(spec=ProxyContext)
        self.context.invocation_service = None
        self.context.serialization_service = None
        self.context.partition_service = None

        self.executor = IExecutorService(
            "hz:impl:executorService",
            "test-executor",
            self.context,
        )

    def test_callback_invoked_on_submit(self):
        results = []
        callback = ExecutionCallback(
            on_response=lambda r: results.append(("success", r)),
            on_failure=lambda e: results.append(("failure", e)),
        )

        task = SumTask(1, 2)
        self.executor.submit(task, callback)

        self.assertEqual(1, len(results))
        self.assertEqual("success", results[0][0])

    def test_multi_callback_on_complete_invoked(self):
        complete_results = []
        callback = MultiExecutionCallback(
            on_complete=lambda r: complete_results.append(r)
        )

        task = SumTask(1, 2)
        self.executor.submit_to_all_members(task, callback)

        self.assertEqual(1, len(complete_results))


class TestExecutorServiceRepr(unittest.TestCase):
    """Tests for string representation."""

    def test_executor_repr(self):
        executor = IExecutorService("hz:impl:executorService", "my-executor")
        repr_str = repr(executor)
        self.assertIn("IExecutorService", repr_str)
        self.assertIn("my-executor", repr_str)


if __name__ == "__main__":
    unittest.main()
