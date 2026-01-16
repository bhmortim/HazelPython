"""Unit tests for Scheduled Executor Service proxy."""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future
import uuid

from hazelcast.proxy.scheduled_executor import (
    IScheduledExecutorService,
    ScheduledExecutorService,
    ScheduledExecutorServiceProxy,
    TimeUnit,
    ScheduledTaskHandler,
    IScheduledFuture,
)
from hazelcast.proxy.executor import Callable, Runnable


class TestTimeUnit(unittest.TestCase):
    """Tests for TimeUnit enum."""

    def test_time_unit_values(self):
        """Test TimeUnit values are defined."""
        self.assertEqual(TimeUnit.NANOSECONDS.value, "NANOSECONDS")
        self.assertEqual(TimeUnit.MICROSECONDS.value, "MICROSECONDS")
        self.assertEqual(TimeUnit.MILLISECONDS.value, "MILLISECONDS")
        self.assertEqual(TimeUnit.SECONDS.value, "SECONDS")
        self.assertEqual(TimeUnit.MINUTES.value, "MINUTES")
        self.assertEqual(TimeUnit.HOURS.value, "HOURS")
        self.assertEqual(TimeUnit.DAYS.value, "DAYS")

    def test_to_millis_nanoseconds(self):
        """Test nanoseconds to millis conversion."""
        result = TimeUnit.NANOSECONDS.to_millis(1_000_000)
        self.assertEqual(result, 1)

    def test_to_millis_microseconds(self):
        """Test microseconds to millis conversion."""
        result = TimeUnit.MICROSECONDS.to_millis(1000)
        self.assertEqual(result, 1)

    def test_to_millis_milliseconds(self):
        """Test milliseconds to millis conversion."""
        result = TimeUnit.MILLISECONDS.to_millis(100)
        self.assertEqual(result, 100)

    def test_to_millis_seconds(self):
        """Test seconds to millis conversion."""
        result = TimeUnit.SECONDS.to_millis(5)
        self.assertEqual(result, 5000)

    def test_to_millis_minutes(self):
        """Test minutes to millis conversion."""
        result = TimeUnit.MINUTES.to_millis(2)
        self.assertEqual(result, 120000)

    def test_to_millis_hours(self):
        """Test hours to millis conversion."""
        result = TimeUnit.HOURS.to_millis(1)
        self.assertEqual(result, 3600000)

    def test_to_millis_days(self):
        """Test days to millis conversion."""
        result = TimeUnit.DAYS.to_millis(1)
        self.assertEqual(result, 86400000)


class TestScheduledTaskHandler(unittest.TestCase):
    """Tests for ScheduledTaskHandler class."""

    def test_partition_based_handler(self):
        """Test partition-based handler initialization."""
        handler = ScheduledTaskHandler(
            scheduler_name="my-scheduler",
            task_name="task-1",
            partition_id=42,
        )
        self.assertEqual(handler.scheduler_name, "my-scheduler")
        self.assertEqual(handler.task_name, "task-1")
        self.assertEqual(handler.partition_id, 42)
        self.assertIsNone(handler.member_uuid)
        self.assertTrue(handler.is_partition_based)

    def test_member_based_handler(self):
        """Test member-based handler initialization."""
        member_uuid = uuid.uuid4()
        handler = ScheduledTaskHandler(
            scheduler_name="my-scheduler",
            task_name="task-1",
            member_uuid=member_uuid,
        )
        self.assertEqual(handler.scheduler_name, "my-scheduler")
        self.assertEqual(handler.task_name, "task-1")
        self.assertEqual(handler.partition_id, -1)
        self.assertEqual(handler.member_uuid, member_uuid)
        self.assertFalse(handler.is_partition_based)

    def test_to_urn_partition_based(self):
        """Test to_urn for partition-based handler."""
        handler = ScheduledTaskHandler(
            scheduler_name="sched",
            task_name="task",
            partition_id=10,
        )
        urn = handler.to_urn()
        self.assertEqual(urn, "urn:hzScheduledTask:sched:10:task")

    def test_to_urn_member_based(self):
        """Test to_urn for member-based handler."""
        member_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
        handler = ScheduledTaskHandler(
            scheduler_name="sched",
            task_name="task",
            member_uuid=member_uuid,
        )
        urn = handler.to_urn()
        self.assertIn("sched", urn)
        self.assertIn("task", urn)

    def test_from_urn_partition_based(self):
        """Test from_urn for partition-based handler."""
        urn = "urn:hzScheduledTask:sched:10:task"
        handler = ScheduledTaskHandler.from_urn(urn)
        self.assertEqual(handler.scheduler_name, "sched")
        self.assertEqual(handler.task_name, "task")
        self.assertEqual(handler.partition_id, 10)
        self.assertIsNone(handler.member_uuid)

    def test_from_urn_member_based(self):
        """Test from_urn for member-based handler."""
        member_uuid = "12345678-1234-5678-1234-567812345678"
        urn = f"urn:hzScheduledTask:sched:{member_uuid}:task"
        handler = ScheduledTaskHandler.from_urn(urn)
        self.assertEqual(handler.scheduler_name, "sched")
        self.assertEqual(handler.task_name, "task")
        self.assertEqual(handler.member_uuid, uuid.UUID(member_uuid))

    def test_from_urn_invalid_format(self):
        """Test from_urn with invalid format raises."""
        with self.assertRaises(ValueError):
            ScheduledTaskHandler.from_urn("invalid:urn")

    def test_repr(self):
        """Test handler __repr__."""
        handler = ScheduledTaskHandler("sched", "task", partition_id=5)
        repr_str = repr(handler)
        self.assertIn("ScheduledTaskHandler", repr_str)

    def test_equality(self):
        """Test handler equality."""
        handler1 = ScheduledTaskHandler("sched", "task", partition_id=5)
        handler2 = ScheduledTaskHandler("sched", "task", partition_id=5)
        handler3 = ScheduledTaskHandler("sched", "task", partition_id=6)
        self.assertEqual(handler1, handler2)
        self.assertNotEqual(handler1, handler3)

    def test_equality_with_non_handler(self):
        """Test handler equality with non-handler object."""
        handler = ScheduledTaskHandler("sched", "task", partition_id=5)
        self.assertNotEqual(handler, "not a handler")
        self.assertNotEqual(handler, None)

    def test_hash(self):
        """Test handler hash."""
        handler1 = ScheduledTaskHandler("sched", "task", partition_id=5)
        handler2 = ScheduledTaskHandler("sched", "task", partition_id=5)
        self.assertEqual(hash(handler1), hash(handler2))


class TestIScheduledFuture(unittest.TestCase):
    """Tests for IScheduledFuture class."""

    def test_future_initialization(self):
        """Test IScheduledFuture initialization."""
        handler = ScheduledTaskHandler("sched", "task", partition_id=0)
        future = IScheduledFuture(handler, context=None)
        self.assertEqual(future.handler, handler)

    def test_get_without_context(self):
        """Test get without context returns None."""
        handler = ScheduledTaskHandler("sched", "task", partition_id=0)
        future = IScheduledFuture(handler, context=None)
        result = future.get()
        self.assertIsNone(result)

    def test_get_delay_without_context(self):
        """Test get_delay without context returns 0."""
        handler = ScheduledTaskHandler("sched", "task", partition_id=0)
        future = IScheduledFuture(handler, context=None)
        delay = future.get_delay()
        self.assertEqual(delay, 0)

    def test_cancel_without_context(self):
        """Test cancel without context marks as cancelled."""
        handler = ScheduledTaskHandler("sched", "task", partition_id=0)
        future = IScheduledFuture(handler, context=None)
        result = future.cancel()
        self.assertTrue(result)
        self.assertTrue(future.is_cancelled())

    def test_cancel_already_cancelled(self):
        """Test cancel on already cancelled future returns False."""
        handler = ScheduledTaskHandler("sched", "task", partition_id=0)
        future = IScheduledFuture(handler, context=None)
        future.cancel()
        result = future.cancel()
        self.assertFalse(result)

    def test_is_cancelled_without_context(self):
        """Test is_cancelled without context."""
        handler = ScheduledTaskHandler("sched", "task", partition_id=0)
        future = IScheduledFuture(handler, context=None)
        self.assertFalse(future.is_cancelled())

    def test_is_done_without_context(self):
        """Test is_done without context."""
        handler = ScheduledTaskHandler("sched", "task", partition_id=0)
        future = IScheduledFuture(handler, context=None)
        self.assertFalse(future.is_done())

    def test_is_done_after_cancel(self):
        """Test is_done after cancel returns True."""
        handler = ScheduledTaskHandler("sched", "task", partition_id=0)
        future = IScheduledFuture(handler, context=None)
        future.cancel()
        self.assertTrue(future.is_done())

    def test_dispose_without_context(self):
        """Test dispose without context doesn't raise."""
        handler = ScheduledTaskHandler("sched", "task", partition_id=0)
        future = IScheduledFuture(handler, context=None)
        future.dispose()

    def test_repr(self):
        """Test IScheduledFuture __repr__."""
        handler = ScheduledTaskHandler("sched", "task", partition_id=0)
        future = IScheduledFuture(handler, context=None)
        repr_str = repr(future)
        self.assertIn("IScheduledFuture", repr_str)
        self.assertIn("pending", repr_str)


class TestIScheduledExecutorService(unittest.TestCase):
    """Tests for IScheduledExecutorService class."""

    def setUp(self):
        """Set up test fixtures."""
        self.scheduler = IScheduledExecutorService(
            service_name="hz:impl:scheduledExecutorService",
            name="test-scheduler",
            context=None,
        )

    def test_service_name_constant(self):
        """Test SERVICE_NAME is defined correctly."""
        self.assertEqual(
            IScheduledExecutorService.SERVICE_NAME,
            "hz:impl:scheduledExecutorService",
        )

    def test_schedule_returns_future(self):
        """Test schedule returns IScheduledFuture."""
        class TestCallable(Callable):
            def call(self):
                return 42

        future = self.scheduler.schedule(TestCallable(), 10, TimeUnit.SECONDS)
        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_on_member_returns_future(self):
        """Test schedule_on_member returns IScheduledFuture."""
        from hazelcast.proxy.executor import Member

        class TestCallable(Callable):
            def call(self):
                return 42

        member = Member(uuid="test-uuid")
        future = self.scheduler.schedule_on_member(
            TestCallable(), member, 10, TimeUnit.SECONDS
        )
        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_on_member_none_raises(self):
        """Test schedule_on_member with None member raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.scheduler.schedule_on_member(TestCallable(), None, 10)

    def test_schedule_on_key_owner_returns_future(self):
        """Test schedule_on_key_owner returns IScheduledFuture."""
        class TestCallable(Callable):
            def call(self):
                return 42

        future = self.scheduler.schedule_on_key_owner(
            TestCallable(), "my-key", 10, TimeUnit.SECONDS
        )
        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_on_key_owner_none_raises(self):
        """Test schedule_on_key_owner with None key raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.scheduler.schedule_on_key_owner(TestCallable(), None, 10)

    def test_schedule_at_fixed_rate_returns_future(self):
        """Test schedule_at_fixed_rate returns IScheduledFuture."""
        class TestCallable(Callable):
            def call(self):
                return 42

        future = self.scheduler.schedule_at_fixed_rate(
            TestCallable(), 0, 5, TimeUnit.SECONDS
        )
        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_at_fixed_rate_invalid_period_raises(self):
        """Test schedule_at_fixed_rate with invalid period raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.scheduler.schedule_at_fixed_rate(TestCallable(), 0, 0)

    def test_schedule_on_member_at_fixed_rate(self):
        """Test schedule_on_member_at_fixed_rate."""
        from hazelcast.proxy.executor import Member

        class TestCallable(Callable):
            def call(self):
                return 42

        member = Member(uuid="test-uuid")
        future = self.scheduler.schedule_on_member_at_fixed_rate(
            TestCallable(), member, 0, 5, TimeUnit.SECONDS
        )
        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_on_member_at_fixed_rate_none_member_raises(self):
        """Test schedule_on_member_at_fixed_rate with None member raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.scheduler.schedule_on_member_at_fixed_rate(
                TestCallable(), None, 0, 5
            )

    def test_schedule_on_member_at_fixed_rate_invalid_period_raises(self):
        """Test schedule_on_member_at_fixed_rate with invalid period raises."""
        from hazelcast.proxy.executor import Member

        class TestCallable(Callable):
            def call(self):
                return 42

        member = Member(uuid="test-uuid")
        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.scheduler.schedule_on_member_at_fixed_rate(
                TestCallable(), member, 0, 0
            )

    def test_schedule_on_key_owner_at_fixed_rate(self):
        """Test schedule_on_key_owner_at_fixed_rate."""
        class TestCallable(Callable):
            def call(self):
                return 42

        future = self.scheduler.schedule_on_key_owner_at_fixed_rate(
            TestCallable(), "my-key", 0, 5, TimeUnit.SECONDS
        )
        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_on_key_owner_at_fixed_rate_none_key_raises(self):
        """Test schedule_on_key_owner_at_fixed_rate with None key raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.scheduler.schedule_on_key_owner_at_fixed_rate(
                TestCallable(), None, 0, 5
            )

    def test_schedule_on_key_owner_at_fixed_rate_invalid_period_raises(self):
        """Test schedule_on_key_owner_at_fixed_rate with invalid period raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.scheduler.schedule_on_key_owner_at_fixed_rate(
                TestCallable(), "key", 0, 0
            )

    def test_schedule_with_fixed_delay_returns_future(self):
        """Test schedule_with_fixed_delay returns IScheduledFuture."""
        class TestCallable(Callable):
            def call(self):
                return 42

        future = self.scheduler.schedule_with_fixed_delay(
            TestCallable(), 0, 5, TimeUnit.SECONDS
        )
        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_with_fixed_delay_invalid_delay_raises(self):
        """Test schedule_with_fixed_delay with invalid delay raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.scheduler.schedule_with_fixed_delay(TestCallable(), 0, 0)

    def test_schedule_on_member_with_fixed_delay(self):
        """Test schedule_on_member_with_fixed_delay."""
        from hazelcast.proxy.executor import Member

        class TestCallable(Callable):
            def call(self):
                return 42

        member = Member(uuid="test-uuid")
        future = self.scheduler.schedule_on_member_with_fixed_delay(
            TestCallable(), member, 0, 5, TimeUnit.SECONDS
        )
        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_on_member_with_fixed_delay_none_member_raises(self):
        """Test schedule_on_member_with_fixed_delay with None member raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.scheduler.schedule_on_member_with_fixed_delay(
                TestCallable(), None, 0, 5
            )

    def test_schedule_on_member_with_fixed_delay_invalid_delay_raises(self):
        """Test schedule_on_member_with_fixed_delay with invalid delay raises."""
        from hazelcast.proxy.executor import Member

        class TestCallable(Callable):
            def call(self):
                return 42

        member = Member(uuid="test-uuid")
        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.scheduler.schedule_on_member_with_fixed_delay(
                TestCallable(), member, 0, 0
            )

    def test_schedule_on_key_owner_with_fixed_delay(self):
        """Test schedule_on_key_owner_with_fixed_delay."""
        class TestCallable(Callable):
            def call(self):
                return 42

        future = self.scheduler.schedule_on_key_owner_with_fixed_delay(
            TestCallable(), "my-key", 0, 5, TimeUnit.SECONDS
        )
        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_on_key_owner_with_fixed_delay_none_key_raises(self):
        """Test schedule_on_key_owner_with_fixed_delay with None key raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.scheduler.schedule_on_key_owner_with_fixed_delay(
                TestCallable(), None, 0, 5
            )

    def test_schedule_on_key_owner_with_fixed_delay_invalid_delay_raises(self):
        """Test schedule_on_key_owner_with_fixed_delay with invalid delay raises."""
        class TestCallable(Callable):
            def call(self):
                return 42

        from hazelcast.exceptions import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.scheduler.schedule_on_key_owner_with_fixed_delay(
                TestCallable(), "key", 0, 0
            )

    def test_get_scheduled_returns_future(self):
        """Test get_scheduled returns IScheduledFuture."""
        handler = ScheduledTaskHandler("test-scheduler", "task", partition_id=0)
        future = self.scheduler.get_scheduled(handler)
        self.assertIsInstance(future, IScheduledFuture)

    def test_get_all_scheduled_returns_dict(self):
        """Test get_all_scheduled returns dictionary."""
        result = self.scheduler.get_all_scheduled()
        self.assertIsInstance(result, dict)

    def test_shutdown(self):
        """Test shutdown method."""
        self.assertFalse(self.scheduler.is_shutdown())
        self.scheduler.shutdown()
        self.assertTrue(self.scheduler.is_shutdown())

    def test_shutdown_idempotent(self):
        """Test shutdown is idempotent."""
        self.scheduler.shutdown()
        self.scheduler.shutdown()
        self.assertTrue(self.scheduler.is_shutdown())

    def test_schedule_after_shutdown_raises(self):
        """Test schedule after shutdown raises exception."""
        class TestCallable(Callable):
            def call(self):
                return 42

        self.scheduler.shutdown()
        from hazelcast.exceptions import IllegalStateException
        with self.assertRaises(IllegalStateException):
            self.scheduler.schedule(TestCallable(), 10)


class TestScheduledExecutorAliases(unittest.TestCase):
    """Tests for scheduled executor service aliases."""

    def test_scheduled_executor_service_alias(self):
        """Test ScheduledExecutorService alias."""
        self.assertIs(ScheduledExecutorService, IScheduledExecutorService)

    def test_scheduled_executor_service_proxy_alias(self):
        """Test ScheduledExecutorServiceProxy alias."""
        self.assertIs(ScheduledExecutorServiceProxy, IScheduledExecutorService)


if __name__ == "__main__":
    unittest.main()
