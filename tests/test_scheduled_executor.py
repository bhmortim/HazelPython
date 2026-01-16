"""Unit tests for IScheduledExecutorService."""

import unittest
import uuid
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from hazelcast.proxy.scheduled_executor import (
    IScheduledExecutorService,
    IScheduledFuture,
    ScheduledTaskHandler,
    TimeUnit,
)
from hazelcast.proxy.executor import Callable, Runnable, Member
from hazelcast.proxy.base import ProxyContext


class SimpleCallable(Callable):
    """Simple callable for testing."""

    def __init__(self, value: int = 42):
        self.value = value

    def call(self):
        return self.value


class SimpleRunnable(Runnable):
    """Simple runnable for testing."""

    def __init__(self, message: str = "executed"):
        self.message = message

    def run(self):
        pass


class TestTimeUnit(unittest.TestCase):
    """Tests for TimeUnit enumeration."""

    def test_nanoseconds_to_millis(self):
        result = TimeUnit.NANOSECONDS.to_millis(5_000_000)
        self.assertEqual(5, result)

    def test_microseconds_to_millis(self):
        result = TimeUnit.MICROSECONDS.to_millis(5_000)
        self.assertEqual(5, result)

    def test_milliseconds_to_millis(self):
        result = TimeUnit.MILLISECONDS.to_millis(5000)
        self.assertEqual(5000, result)

    def test_seconds_to_millis(self):
        result = TimeUnit.SECONDS.to_millis(5)
        self.assertEqual(5000, result)

    def test_minutes_to_millis(self):
        result = TimeUnit.MINUTES.to_millis(2)
        self.assertEqual(120_000, result)

    def test_hours_to_millis(self):
        result = TimeUnit.HOURS.to_millis(1)
        self.assertEqual(3_600_000, result)

    def test_days_to_millis(self):
        result = TimeUnit.DAYS.to_millis(1)
        self.assertEqual(86_400_000, result)


class TestScheduledTaskHandler(unittest.TestCase):
    """Tests for ScheduledTaskHandler."""

    def test_partition_based_handler(self):
        handler = ScheduledTaskHandler("scheduler", "task1", partition_id=5)

        self.assertEqual("scheduler", handler.scheduler_name)
        self.assertEqual("task1", handler.task_name)
        self.assertEqual(5, handler.partition_id)
        self.assertIsNone(handler.member_uuid)
        self.assertTrue(handler.is_partition_based)

    def test_member_based_handler(self):
        member_uuid = uuid.uuid4()
        handler = ScheduledTaskHandler("scheduler", "task2", member_uuid=member_uuid)

        self.assertEqual("scheduler", handler.scheduler_name)
        self.assertEqual("task2", handler.task_name)
        self.assertEqual(-1, handler.partition_id)
        self.assertEqual(member_uuid, handler.member_uuid)
        self.assertFalse(handler.is_partition_based)

    def test_to_urn_partition_based(self):
        handler = ScheduledTaskHandler("my-scheduler", "my-task", partition_id=10)
        urn = handler.to_urn()

        self.assertEqual("urn:hzScheduledTask:my-scheduler:10:my-task", urn)

    def test_to_urn_member_based(self):
        member_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
        handler = ScheduledTaskHandler("my-scheduler", "my-task", member_uuid=member_uuid)
        urn = handler.to_urn()

        self.assertEqual(
            "urn:hzScheduledTask:my-scheduler:12345678-1234-5678-1234-567812345678:my-task",
            urn,
        )

    def test_from_urn_partition_based(self):
        urn = "urn:hzScheduledTask:my-scheduler:10:my-task"
        handler = ScheduledTaskHandler.from_urn(urn)

        self.assertEqual("my-scheduler", handler.scheduler_name)
        self.assertEqual("my-task", handler.task_name)
        self.assertEqual(10, handler.partition_id)
        self.assertIsNone(handler.member_uuid)

    def test_from_urn_member_based(self):
        urn = "urn:hzScheduledTask:my-scheduler:12345678-1234-5678-1234-567812345678:my-task"
        handler = ScheduledTaskHandler.from_urn(urn)

        self.assertEqual("my-scheduler", handler.scheduler_name)
        self.assertEqual("my-task", handler.task_name)
        self.assertEqual(-1, handler.partition_id)
        self.assertEqual(
            uuid.UUID("12345678-1234-5678-1234-567812345678"),
            handler.member_uuid,
        )

    def test_from_urn_invalid_format(self):
        with self.assertRaises(ValueError):
            ScheduledTaskHandler.from_urn("invalid:urn")

    def test_equality(self):
        handler1 = ScheduledTaskHandler("scheduler", "task", partition_id=5)
        handler2 = ScheduledTaskHandler("scheduler", "task", partition_id=5)
        handler3 = ScheduledTaskHandler("scheduler", "task", partition_id=6)

        self.assertEqual(handler1, handler2)
        self.assertNotEqual(handler1, handler3)

    def test_hash(self):
        handler1 = ScheduledTaskHandler("scheduler", "task", partition_id=5)
        handler2 = ScheduledTaskHandler("scheduler", "task", partition_id=5)

        self.assertEqual(hash(handler1), hash(handler2))


class TestIScheduledFuture(unittest.TestCase):
    """Tests for IScheduledFuture."""

    def setUp(self):
        self.handler = ScheduledTaskHandler("scheduler", "task", partition_id=5)
        self.context = MagicMock(spec=ProxyContext)
        self.context.invocation_service = None
        self.context.serialization_service = None

    def test_handler_property(self):
        future = IScheduledFuture(self.handler, self.context)
        self.assertEqual(self.handler, future.handler)

    def test_is_cancelled_initial(self):
        future = IScheduledFuture(self.handler, self.context)
        self.assertFalse(future.is_cancelled())

    def test_is_done_initial(self):
        future = IScheduledFuture(self.handler, self.context)
        self.assertFalse(future.is_done())

    def test_cancel_without_invocation_service(self):
        future = IScheduledFuture(self.handler, self.context)
        result = future.cancel()

        self.assertTrue(result)
        self.assertTrue(future.is_cancelled())

    def test_get_without_invocation_service(self):
        future = IScheduledFuture(self.handler, self.context)
        result = future.get()

        self.assertIsNone(result)

    def test_get_delay_without_invocation_service(self):
        future = IScheduledFuture(self.handler, self.context)
        delay = future.get_delay(TimeUnit.MILLISECONDS)

        self.assertEqual(0, delay)

    def test_dispose_without_invocation_service(self):
        future = IScheduledFuture(self.handler, self.context)
        future.dispose()

    def test_repr(self):
        future = IScheduledFuture(self.handler, self.context)
        repr_str = repr(future)

        self.assertIn("IScheduledFuture", repr_str)
        self.assertIn("pending", repr_str)


class TestIScheduledExecutorService(unittest.TestCase):
    """Tests for IScheduledExecutorService."""

    def setUp(self):
        self.context = MagicMock(spec=ProxyContext)
        self.context.invocation_service = None
        self.context.serialization_service = None
        self.context.partition_service = None
        self.service = IScheduledExecutorService(
            "hz:impl:scheduledExecutorService",
            "test-scheduler",
            self.context,
        )

    def test_schedule_returns_future(self):
        task = SimpleCallable(100)
        future = self.service.schedule(task, 10, TimeUnit.SECONDS)

        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_on_member(self):
        task = SimpleCallable(100)
        member = Member(str(uuid.uuid4()))
        future = self.service.schedule_on_member(task, member, 5, TimeUnit.SECONDS)

        self.assertIsInstance(future, IScheduledFuture)
        self.assertEqual(member.uuid, str(future.handler.member_uuid))

    def test_schedule_on_member_none_raises(self):
        task = SimpleCallable(100)

        with self.assertRaises(Exception) as ctx:
            self.service.schedule_on_member(task, None, 5, TimeUnit.SECONDS)

        self.assertIn("Member cannot be None", str(ctx.exception))

    def test_schedule_on_key_owner(self):
        task = SimpleCallable(100)
        future = self.service.schedule_on_key_owner(task, "my-key", 5, TimeUnit.SECONDS)

        self.assertIsInstance(future, IScheduledFuture)
        self.assertTrue(future.handler.is_partition_based)

    def test_schedule_on_key_owner_none_raises(self):
        task = SimpleCallable(100)

        with self.assertRaises(Exception) as ctx:
            self.service.schedule_on_key_owner(task, None, 5, TimeUnit.SECONDS)

        self.assertIn("Key cannot be None", str(ctx.exception))

    def test_schedule_at_fixed_rate(self):
        task = SimpleRunnable()
        future = self.service.schedule_at_fixed_rate(
            task, 0, 5, TimeUnit.SECONDS
        )

        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_at_fixed_rate_invalid_period(self):
        task = SimpleRunnable()

        with self.assertRaises(Exception) as ctx:
            self.service.schedule_at_fixed_rate(task, 0, 0, TimeUnit.SECONDS)

        self.assertIn("Period must be positive", str(ctx.exception))

    def test_schedule_at_fixed_rate_negative_period(self):
        task = SimpleRunnable()

        with self.assertRaises(Exception) as ctx:
            self.service.schedule_at_fixed_rate(task, 0, -1, TimeUnit.SECONDS)

        self.assertIn("Period must be positive", str(ctx.exception))

    def test_schedule_with_fixed_delay(self):
        task = SimpleRunnable()
        future = self.service.schedule_with_fixed_delay(
            task, 0, 5, TimeUnit.SECONDS
        )

        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_with_fixed_delay_invalid_delay(self):
        task = SimpleRunnable()

        with self.assertRaises(Exception) as ctx:
            self.service.schedule_with_fixed_delay(task, 0, 0, TimeUnit.SECONDS)

        self.assertIn("Delay must be positive", str(ctx.exception))

    def test_schedule_on_member_at_fixed_rate(self):
        task = SimpleRunnable()
        member = Member(str(uuid.uuid4()))
        future = self.service.schedule_on_member_at_fixed_rate(
            task, member, 0, 5, TimeUnit.SECONDS
        )

        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_on_member_at_fixed_rate_none_member(self):
        task = SimpleRunnable()

        with self.assertRaises(Exception) as ctx:
            self.service.schedule_on_member_at_fixed_rate(
                task, None, 0, 5, TimeUnit.SECONDS
            )

        self.assertIn("Member cannot be None", str(ctx.exception))

    def test_schedule_on_member_at_fixed_rate_invalid_period(self):
        task = SimpleRunnable()
        member = Member(str(uuid.uuid4()))

        with self.assertRaises(Exception) as ctx:
            self.service.schedule_on_member_at_fixed_rate(
                task, member, 0, 0, TimeUnit.SECONDS
            )

        self.assertIn("Period must be positive", str(ctx.exception))

    def test_schedule_on_key_owner_at_fixed_rate(self):
        task = SimpleRunnable()
        future = self.service.schedule_on_key_owner_at_fixed_rate(
            task, "my-key", 0, 5, TimeUnit.SECONDS
        )

        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_on_key_owner_at_fixed_rate_none_key(self):
        task = SimpleRunnable()

        with self.assertRaises(Exception) as ctx:
            self.service.schedule_on_key_owner_at_fixed_rate(
                task, None, 0, 5, TimeUnit.SECONDS
            )

        self.assertIn("Key cannot be None", str(ctx.exception))

    def test_schedule_on_member_with_fixed_delay(self):
        task = SimpleRunnable()
        member = Member(str(uuid.uuid4()))
        future = self.service.schedule_on_member_with_fixed_delay(
            task, member, 0, 5, TimeUnit.SECONDS
        )

        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_on_member_with_fixed_delay_none_member(self):
        task = SimpleRunnable()

        with self.assertRaises(Exception) as ctx:
            self.service.schedule_on_member_with_fixed_delay(
                task, None, 0, 5, TimeUnit.SECONDS
            )

        self.assertIn("Member cannot be None", str(ctx.exception))

    def test_schedule_on_key_owner_with_fixed_delay(self):
        task = SimpleRunnable()
        future = self.service.schedule_on_key_owner_with_fixed_delay(
            task, "my-key", 0, 5, TimeUnit.SECONDS
        )

        self.assertIsInstance(future, IScheduledFuture)

    def test_schedule_on_key_owner_with_fixed_delay_none_key(self):
        task = SimpleRunnable()

        with self.assertRaises(Exception) as ctx:
            self.service.schedule_on_key_owner_with_fixed_delay(
                task, None, 0, 5, TimeUnit.SECONDS
            )

        self.assertIn("Key cannot be None", str(ctx.exception))

    def test_get_scheduled(self):
        handler = ScheduledTaskHandler("test-scheduler", "task1", partition_id=3)
        future = self.service.get_scheduled(handler)

        self.assertIsInstance(future, IScheduledFuture)
        self.assertEqual(handler, future.handler)

    def test_get_all_scheduled_empty(self):
        result = self.service.get_all_scheduled()

        self.assertEqual({}, result)

    def test_shutdown(self):
        self.assertFalse(self.service.is_shutdown())

        self.service.shutdown()

        self.assertTrue(self.service.is_shutdown())

    def test_shutdown_idempotent(self):
        self.service.shutdown()
        self.service.shutdown()

        self.assertTrue(self.service.is_shutdown())

    def test_schedule_after_shutdown_raises(self):
        self.service.shutdown()

        with self.assertRaises(Exception) as ctx:
            self.service.schedule(SimpleCallable(), 10, TimeUnit.SECONDS)

        self.assertIn("shut down", str(ctx.exception))

    def test_schedule_at_fixed_rate_after_shutdown_raises(self):
        self.service.shutdown()

        with self.assertRaises(Exception) as ctx:
            self.service.schedule_at_fixed_rate(
                SimpleRunnable(), 0, 5, TimeUnit.SECONDS
            )

        self.assertIn("shut down", str(ctx.exception))

    def test_schedule_with_fixed_delay_after_shutdown_raises(self):
        self.service.shutdown()

        with self.assertRaises(Exception) as ctx:
            self.service.schedule_with_fixed_delay(
                SimpleRunnable(), 0, 5, TimeUnit.SECONDS
            )

        self.assertIn("shut down", str(ctx.exception))

    def test_destroyed_raises_on_schedule(self):
        self.service.destroy()

        with self.assertRaises(Exception) as ctx:
            self.service.schedule(SimpleCallable(), 10, TimeUnit.SECONDS)

        self.assertIn("destroyed", str(ctx.exception))

    def test_service_name(self):
        self.assertEqual("hz:impl:scheduledExecutorService", self.service.service_name)

    def test_name(self):
        self.assertEqual("test-scheduler", self.service.name)


class TestScheduledExecutorServiceAliases(unittest.TestCase):
    """Tests for module-level aliases."""

    def test_scheduled_executor_service_alias(self):
        from hazelcast.proxy.scheduled_executor import ScheduledExecutorService
        self.assertIs(ScheduledExecutorService, IScheduledExecutorService)

    def test_scheduled_executor_service_proxy_alias(self):
        from hazelcast.proxy.scheduled_executor import ScheduledExecutorServiceProxy
        self.assertIs(ScheduledExecutorServiceProxy, IScheduledExecutorService)


if __name__ == "__main__":
    unittest.main()
