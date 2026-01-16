"""Unit tests for Executor Service proxy."""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future

from hazelcast.proxy.executor import (
    IExecutorService,
    ExecutorService,
    ExecutorServiceProxy,
    MemberSelector,
    LiteMemberSelector,
    DataMemberSelector,
    Callable,
    Runnable,
    ExecutionCallback,
    MultiExecutionCallback,
    Member,
)


class TestMember(unittest.TestCase):
    """Tests for Member class."""

    def test_member_initialization(self):
        """Test Member initialization."""
        member = Member(uuid="test-uuid", address="localhost:5701", lite_member=False)
        self.assertEqual(member.uuid, "test-uuid")
        self.assertEqual(member.address, "localhost:5701")
        self.assertFalse(member.lite_member)

    def test_member_with_defaults(self):
        """Test Member with default values."""
        member = Member(uuid="test-uuid")
        self.assertEqual(member.uuid, "test-uuid")
        self.assertIsNone(member.address)
        self.assertFalse(member.lite_member)

    def test_member_lite_member(self):
        """Test lite member flag."""
        member = Member(uuid="lite-uuid", lite_member=True)
        self.assertTrue(member.lite_member)

    def test_member_repr(self):
        """Test Member __repr__."""
        member = Member(uuid="test-uuid", address="localhost:5701")
        repr_str = repr(member)
        self.assertIn("test-uuid", repr_str)
        self.assertIn("localhost:5701", repr_str)

    def test_member_equality(self):
        """Test Member equality based on uuid."""
        member1 = Member(uuid="same-uuid", address="addr1")
        member2 = Member(uuid="same-uuid", address="addr2")
        member3 = Member(uuid="different-uuid", address="addr1")
        self.assertEqual(member1, member2)
        self.assertNotEqual(member1, member3)

    def test_member_equality_with_non_member(self):
        """Test Member equality with non-Member object."""
        member = Member(uuid="test-uuid")
        self.assertNotEqual(member, "test-uuid")
        self.assertNotEqual(member, None)

    def test_member_hash(self):
        """Test Member hash based on uuid."""
        member1 = Member(uuid="same-uuid")
        member2 = Member(uuid="same-uuid")
        self.assertEqual(hash(member1), hash(member2))


class TestMemberSelectors(unittest.TestCase):
    """Tests for MemberSelector implementations."""

    def test_lite_member_selector_selects_lite(self):
        """Test LiteMemberSelector selects lite members."""
        selector = LiteMemberSelector()
        lite_member = Member(uuid="lite", lite_member=True)
        data_member = Member(uuid="data", lite_member=False)
        self.assertTrue(selector.select(lite_member))
        self.assertFalse(selector.select(data_member))

    def test_data_member_selector_selects_data(self):
        """Test DataMemberSelector selects data members."""
        selector = DataMemberSelector()
        lite_member = Member(uuid="lite", lite_member=True)
        data_member = Member(uuid="data", lite_member=False)
        self.assertFalse(selector.select(lite_member))
        self.assertTrue(selector.select(data_member))


class TestCallableAndRunnable(unittest.TestCase):
    """Tests for Callable and Runnable abstract classes."""

    def test_callable_is_abstract(self):
        """Test Callable.call is abstract."""
        with self.assertRaises(TypeError):
            Callable()

    def test_runnable_is_abstract(self):
        """Test Runnable.run is abstract."""
        with self.assertRaises(TypeError):
            Runnable()

    def test_callable_subclass(self):
        """Test Callable can be subclassed."""
        class MyCallable(Callable):
            def call(self):
                return 42

        task = MyCallable()
        self.assertEqual(task.call(), 42)

    def test_runnable_subclass(self):
        """Test Runnable can be subclassed."""
        class MyRunnable(Runnable):
            def __init__(self):
                self.executed = False

            def run(self):
                self.executed = True

        task = MyRunnable()
        task.run()
        self.assertTrue(task.executed)


class TestExecutionCallback(unittest.TestCase):
    """Tests for ExecutionCallback class."""

    def test_callback_on_response(self):
        """Test on_response callback."""
        results = []
        callback = ExecutionCallback(
            on_response=lambda r: results.append(r),
        )
        callback.on_response("result")
        self.assertEqual(results, ["result"])

    def test_callback_on_failure(self):
        """Test on_failure callback."""
        errors = []
        callback = ExecutionCallback(
            on_failure=lambda e: errors.append(e),
        )
        error = Exception("test error")
        callback.on_failure(error)
        self.assertEqual(errors, [error])

    def test_callback_without_handlers(self):
        """Test callback without handlers doesn't raise."""
        callback = ExecutionCallback()
        callback.on_response("result")
        callback.on_failure(Exception("error"))


class TestMultiExecutionCallback(unittest.TestCase):
    """Tests for MultiExecutionCallback class."""

    def test_multi_callback_on_response(self):
        """Test on_response callback."""
        results = []
        callback = MultiExecutionCallback(
            on_response=lambda uuid, r: results.append((uuid, r)),
        )
        callback.on_response("member-1", "result")
        self.assertEqual(results, [("member-1", "result")])

    def test_multi_callback_on_failure(self):
        """Test on_failure callback."""
        errors = []
        callback = MultiExecutionCallback(
            on_failure=lambda uuid, e: errors.append((uuid, e)),
        )
        error = Exception("test error")
        callback.on_failure("member-1", error)
        self.assertEqual(errors, [("member-1", error)])

    def test_multi_callback_on_complete(self):
        """Test on_complete callback."""
        completed = []
        callback = MultiExecutionCallback(
            on_complete=lambda r: completed.append(r),
        )
        callback.on_complete({"m1": "r1", "m2": "r2"})
        self.assertEqual(completed, [{"m1": "r1", "m2": "r2"}])

    def test_multi_callback_without_handlers(self):
        """Test multi callback without handlers doesn't raise."""
        callback = MultiExecutionCallback()
        callback.on_response("member", "result")
        callback.on_failure("member", Exception("error"))
        callback.on_complete({})


class TestIExecutorService(unittest.TestCase):
    """Tests for IExecutorService class."""

    def setUp(self):
        """Set up test fixtures."""
        self.executor = IExecutorService(
            service_name="hz:impl:executorService",
            name="test-executor",
            context=None,
        )

    def test_submit_returns_future(self):
        """Test submit returns a Future."""
        class TestCallable(Callable):
            def call(self):
                return 42

        future = self.executor.submit(TestCallable())
        self.assertIsInstance(future, Future)

    def test_submit_to_member_returns_future(self):
        """Test submit_to_member returns a Future."""
        class TestCallable(Callable):
            def call(self):
                return 42

        member = Member(uuid="test-uuid")
        future = self.executor.submit_to_member(TestCallable(), member)
        self.assertIsInstance(future, Future)

    def test_submit_to_member_none_raises(self):
        """Test submit_to_member with None member raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.executor.submit_to_member(TestCallable(), None)

    def test_submit_to_key_owner_returns_future(self):
        """Test submit_to_key_owner returns a Future."""
        class TestCallable(Callable):
            def call(self):
                return 42

        future = self.executor.submit_to_key_owner(TestCallable(), "my-key")
        self.assertIsInstance(future, Future)

    def test_submit_to_key_owner_none_raises(self):
        """Test submit_to_key_owner with None key raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.executor.submit_to_key_owner(TestCallable(), None)

    def test_submit_to_all_members_returns_dict(self):
        """Test submit_to_all_members returns a dictionary."""
        class TestCallable(Callable):
            def call(self):
                return 42

        result = self.executor.submit_to_all_members(TestCallable())
        self.assertIsInstance(result, dict)

    def test_submit_to_members_returns_dict(self):
        """Test submit_to_members returns a dictionary."""
        class TestCallable(Callable):
            def call(self):
                return 42

        members = [Member(uuid="m1"), Member(uuid="m2")]
        self.executor._members = {"m1": members[0], "m2": members[1]}
        result = self.executor.submit_to_members(TestCallable(), members)
        self.assertIsInstance(result, dict)

    def test_submit_to_members_empty_raises(self):
        """Test submit_to_members with empty list raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.executor.submit_to_members(TestCallable(), [])

    def test_submit_to_member_with_selector(self):
        """Test submit_to_member_with_selector."""
        class TestCallable(Callable):
            def call(self):
                return 42

        member = Member(uuid="data-member", lite_member=False)
        self.executor._members = {"data-member": member}
        selector = DataMemberSelector()
        future = self.executor.submit_to_member_with_selector(TestCallable(), selector)
        self.assertIsInstance(future, Future)

    def test_submit_to_member_with_selector_no_match_raises(self):
        """Test submit_to_member_with_selector with no matching member raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        selector = DataMemberSelector()
        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.executor.submit_to_member_with_selector(TestCallable(), selector)

    def test_submit_to_all_members_with_selector(self):
        """Test submit_to_all_members_with_selector."""
        class TestCallable(Callable):
            def call(self):
                return 42

        member = Member(uuid="data-member", lite_member=False)
        self.executor._members = {"data-member": member}
        selector = DataMemberSelector()
        result = self.executor.submit_to_all_members_with_selector(TestCallable(), selector)
        self.assertIsInstance(result, dict)

    def test_submit_to_all_members_with_selector_no_match_raises(self):
        """Test submit_to_all_members_with_selector with no match raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        selector = DataMemberSelector()
        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.executor.submit_to_all_members_with_selector(TestCallable(), selector)

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

    def test_is_terminated(self):
        """Test is_terminated method."""
        self.assertFalse(self.executor.is_terminated())
        self.executor.shutdown()
        self.assertTrue(self.executor.is_terminated())

    def test_submit_after_shutdown_raises(self):
        """Test submit after shutdown raises exception."""
        class TestCallable(Callable):
            def call(self):
                return 42

        self.executor.shutdown()
        from hazelcast.exceptions import IllegalStateException
        with self.assertRaises(IllegalStateException):
            self.executor.submit(TestCallable())

    def test_add_member(self):
        """Test _add_member method."""
        member = Member(uuid="test-uuid")
        self.executor._add_member(member)
        self.assertIn("test-uuid", self.executor._members)

    def test_remove_member(self):
        """Test _remove_member method."""
        member = Member(uuid="test-uuid")
        self.executor._add_member(member)
        self.executor._remove_member("test-uuid")
        self.assertNotIn("test-uuid", self.executor._members)

    def test_remove_nonexistent_member(self):
        """Test _remove_member for nonexistent member doesn't raise."""
        self.executor._remove_member("nonexistent")

    def test_get_members(self):
        """Test _get_members method."""
        member1 = Member(uuid="m1")
        member2 = Member(uuid="m2")
        self.executor._add_member(member1)
        self.executor._add_member(member2)
        members = self.executor._get_members()
        self.assertEqual(len(members), 2)


class TestExecutorAliases(unittest.TestCase):
    """Tests for executor service aliases."""

    def test_executor_service_alias(self):
        """Test ExecutorService alias."""
        self.assertIs(ExecutorService, IExecutorService)

    def test_executor_service_proxy_alias(self):
        """Test ExecutorServiceProxy alias."""
        self.assertIs(ExecutorServiceProxy, IExecutorService)


if __name__ == "__main__":
    unittest.main()
