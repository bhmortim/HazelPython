"""Distributed Scheduled Executor Service proxy implementation."""

import time
import uuid as uuid_module
from abc import ABC, abstractmethod
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from enum import Enum
from typing import Any, Callable as CallableType, Dict, List, Optional, Union, TYPE_CHECKING

from hazelcast.proxy.base import Proxy, ProxyContext
from hazelcast.proxy.executor import Callable, Runnable, Member
from hazelcast.logging import get_logger

if TYPE_CHECKING:
    from hazelcast.protocol.client_message import ClientMessage

_logger = get_logger("scheduled_executor")


class TimeUnit(Enum):
    """Time units for scheduling delays and periods."""

    NANOSECONDS = "NANOSECONDS"
    MICROSECONDS = "MICROSECONDS"
    MILLISECONDS = "MILLISECONDS"
    SECONDS = "SECONDS"
    MINUTES = "MINUTES"
    HOURS = "HOURS"
    DAYS = "DAYS"

    def to_millis(self, duration: float) -> int:
        """Convert the given duration to milliseconds.

        Args:
            duration: The duration in this time unit.

        Returns:
            The duration in milliseconds.
        """
        conversions = {
            TimeUnit.NANOSECONDS: lambda d: int(d / 1_000_000),
            TimeUnit.MICROSECONDS: lambda d: int(d / 1_000),
            TimeUnit.MILLISECONDS: lambda d: int(d),
            TimeUnit.SECONDS: lambda d: int(d * 1_000),
            TimeUnit.MINUTES: lambda d: int(d * 60_000),
            TimeUnit.HOURS: lambda d: int(d * 3_600_000),
            TimeUnit.DAYS: lambda d: int(d * 86_400_000),
        }
        return conversions[self](duration)


class ScheduledTaskHandler:
    """Handler identifying a scheduled task.

    Contains information needed to interact with a scheduled task,
    including its name, the scheduler it belongs to, and its location
    (partition or member).

    Attributes:
        scheduler_name: Name of the scheduler service.
        task_name: Unique name of the scheduled task.
        partition_id: Partition ID if task is partition-based (-1 otherwise).
        member_uuid: Member UUID if task is member-based (None otherwise).
    """

    def __init__(
        self,
        scheduler_name: str,
        task_name: str,
        partition_id: int = -1,
        member_uuid: Optional[uuid_module.UUID] = None,
    ):
        self._scheduler_name = scheduler_name
        self._task_name = task_name
        self._partition_id = partition_id
        self._member_uuid = member_uuid

    @property
    def scheduler_name(self) -> str:
        """Get the scheduler service name."""
        return self._scheduler_name

    @property
    def task_name(self) -> str:
        """Get the task name."""
        return self._task_name

    @property
    def partition_id(self) -> int:
        """Get the partition ID (-1 if member-based)."""
        return self._partition_id

    @property
    def member_uuid(self) -> Optional[uuid_module.UUID]:
        """Get the member UUID (None if partition-based)."""
        return self._member_uuid

    @property
    def is_partition_based(self) -> bool:
        """Check if task is assigned to a partition."""
        return self._partition_id >= 0

    def to_urn(self) -> str:
        """Convert handler to URN string representation."""
        if self.is_partition_based:
            return f"urn:hzScheduledTask:{self._scheduler_name}:{self._partition_id}:{self._task_name}"
        return f"urn:hzScheduledTask:{self._scheduler_name}:{self._member_uuid}:{self._task_name}"

    @classmethod
    def from_urn(cls, urn: str) -> "ScheduledTaskHandler":
        """Create handler from URN string."""
        parts = urn.split(":")
        if len(parts) < 5:
            raise ValueError(f"Invalid URN format: {urn}")

        scheduler_name = parts[2]
        location = parts[3]
        task_name = parts[4]

        try:
            partition_id = int(location)
            return cls(scheduler_name, task_name, partition_id=partition_id)
        except ValueError:
            member_uuid = uuid_module.UUID(location)
            return cls(scheduler_name, task_name, member_uuid=member_uuid)

    def __repr__(self) -> str:
        return f"ScheduledTaskHandler({self.to_urn()!r})"

    def __eq__(self, other) -> bool:
        if isinstance(other, ScheduledTaskHandler):
            return (
                self._scheduler_name == other._scheduler_name
                and self._task_name == other._task_name
                and self._partition_id == other._partition_id
                and self._member_uuid == other._member_uuid
            )
        return False

    def __hash__(self) -> int:
        return hash((self._scheduler_name, self._task_name, self._partition_id, self._member_uuid))


class IScheduledFuture:
    """A delayed result of a scheduled task.

    Represents a scheduled task that can be queried for completion status,
    remaining delay, and result. Supports cancellation.

    Example:
        >>> future = scheduler.schedule(my_task, 10, TimeUnit.SECONDS)
        >>> delay = future.get_delay(TimeUnit.MILLISECONDS)
        >>> print(f"Task will run in {delay}ms")
        >>> result = future.get()
    """

    def __init__(
        self,
        handler: ScheduledTaskHandler,
        context: Optional[ProxyContext] = None,
    ):
        self._handler = handler
        self._context = context
        self._result_future: Optional[Future] = None
        self._cancelled = False
        self._done = False

    @property
    def handler(self) -> ScheduledTaskHandler:
        """Get the task handler."""
        return self._handler

    def get(self, timeout: Optional[float] = None) -> Any:
        """Get the result of the scheduled task.

        Waits if necessary for the task to complete, and retrieves
        its result.

        Args:
            timeout: Maximum time to wait in seconds. None means wait forever.

        Returns:
            The task result.

        Raises:
            TimeoutError: If timeout is reached before task completes.
            CancellationException: If the task was cancelled.
        """
        if self._cancelled:
            from hazelcast.exceptions import IllegalStateException
            raise IllegalStateException("Task was cancelled")

        if self._context is None or self._context.invocation_service is None:
            return None

        from hazelcast.protocol.codec import ScheduledExecutorServiceCodec
        request = ScheduledExecutorServiceCodec.encode_get_result_request(
            self._handler.scheduler_name,
            self._handler.task_name,
            self._handler.partition_id,
            self._handler.member_uuid,
        )

        from hazelcast.invocation import Invocation
        invocation = Invocation(request, timeout=timeout or 120.0)
        result_future = self._context.invocation_service.invoke(invocation)

        try:
            response = result_future.result(timeout=timeout)
            result_data = ScheduledExecutorServiceCodec.decode_get_result_response(response)
            self._done = True
            if result_data and self._context.serialization_service:
                return self._context.serialization_service.to_object(result_data)
            return result_data
        except FutureTimeoutError:
            raise TimeoutError(f"Timed out waiting for result after {timeout}s")

    def get_delay(self, unit: TimeUnit = TimeUnit.MILLISECONDS) -> int:
        """Get the remaining delay before the task executes.

        Args:
            unit: The time unit for the returned delay.

        Returns:
            The remaining delay in the specified time unit.
        """
        if self._context is None or self._context.invocation_service is None:
            return 0

        from hazelcast.protocol.codec import ScheduledExecutorServiceCodec
        request = ScheduledExecutorServiceCodec.encode_get_delay_request(
            self._handler.scheduler_name,
            self._handler.task_name,
            self._handler.partition_id,
            self._handler.member_uuid,
        )

        from hazelcast.invocation import Invocation
        invocation = Invocation(request)
        result_future = self._context.invocation_service.invoke(invocation)

        response = result_future.result()
        delay_nanos = ScheduledExecutorServiceCodec.decode_get_delay_response(response)

        if unit == TimeUnit.NANOSECONDS:
            return delay_nanos
        elif unit == TimeUnit.MICROSECONDS:
            return delay_nanos // 1_000
        elif unit == TimeUnit.MILLISECONDS:
            return delay_nanos // 1_000_000
        elif unit == TimeUnit.SECONDS:
            return delay_nanos // 1_000_000_000
        elif unit == TimeUnit.MINUTES:
            return delay_nanos // 60_000_000_000
        elif unit == TimeUnit.HOURS:
            return delay_nanos // 3_600_000_000_000
        elif unit == TimeUnit.DAYS:
            return delay_nanos // 86_400_000_000_000
        return delay_nanos

    def cancel(self, may_interrupt_if_running: bool = False) -> bool:
        """Attempt to cancel the scheduled task.

        Args:
            may_interrupt_if_running: If True, the task may be interrupted
                even if it's currently running.

        Returns:
            True if the task was successfully cancelled.
        """
        if self._cancelled or self._done:
            return False

        if self._context is None or self._context.invocation_service is None:
            self._cancelled = True
            return True

        from hazelcast.protocol.codec import ScheduledExecutorServiceCodec
        request = ScheduledExecutorServiceCodec.encode_cancel_request(
            self._handler.scheduler_name,
            self._handler.task_name,
            self._handler.partition_id,
            self._handler.member_uuid,
            may_interrupt_if_running,
        )

        from hazelcast.invocation import Invocation
        invocation = Invocation(request)
        result_future = self._context.invocation_service.invoke(invocation)

        response = result_future.result()
        self._cancelled = ScheduledExecutorServiceCodec.decode_cancel_response(response)
        return self._cancelled

    def is_cancelled(self) -> bool:
        """Check if the task has been cancelled.

        Returns:
            True if the task was cancelled before completion.
        """
        if self._cancelled:
            return True

        if self._context is None or self._context.invocation_service is None:
            return self._cancelled

        from hazelcast.protocol.codec import ScheduledExecutorServiceCodec
        request = ScheduledExecutorServiceCodec.encode_is_cancelled_request(
            self._handler.scheduler_name,
            self._handler.task_name,
            self._handler.partition_id,
            self._handler.member_uuid,
        )

        from hazelcast.invocation import Invocation
        invocation = Invocation(request)
        result_future = self._context.invocation_service.invoke(invocation)

        response = result_future.result()
        self._cancelled = ScheduledExecutorServiceCodec.decode_is_cancelled_response(response)
        return self._cancelled

    def is_done(self) -> bool:
        """Check if the task has completed.

        A task is done if it has completed normally, was cancelled,
        or threw an exception.

        Returns:
            True if the task has completed.
        """
        if self._done or self._cancelled:
            return True

        if self._context is None or self._context.invocation_service is None:
            return self._done

        from hazelcast.protocol.codec import ScheduledExecutorServiceCodec
        request = ScheduledExecutorServiceCodec.encode_is_done_request(
            self._handler.scheduler_name,
            self._handler.task_name,
            self._handler.partition_id,
            self._handler.member_uuid,
        )

        from hazelcast.invocation import Invocation
        invocation = Invocation(request)
        result_future = self._context.invocation_service.invoke(invocation)

        response = result_future.result()
        self._done = ScheduledExecutorServiceCodec.decode_is_done_response(response)
        return self._done

    def dispose(self) -> None:
        """Dispose of the scheduled task resources.

        Releases server-side resources associated with this task.
        After disposal, the task cannot be queried or cancelled.
        """
        if self._context is None or self._context.invocation_service is None:
            return

        from hazelcast.protocol.codec import ScheduledExecutorServiceCodec
        request = ScheduledExecutorServiceCodec.encode_dispose_request(
            self._handler.scheduler_name,
            self._handler.task_name,
            self._handler.partition_id,
            self._handler.member_uuid,
        )

        from hazelcast.invocation import Invocation
        invocation = Invocation(request)
        self._context.invocation_service.invoke(invocation)

    def __repr__(self) -> str:
        status = "cancelled" if self._cancelled else ("done" if self._done else "pending")
        return f"IScheduledFuture(handler={self._handler!r}, status={status})"


class IScheduledExecutorService(Proxy):
    """Distributed scheduled executor service for delayed and periodic task execution.

    Provides functionality to schedule tasks for one-time or repeated execution
    after a given delay. Tasks can be scheduled on specific members, on the
    owner of a key (for data locality), or on any available member.

    Unlike `IExecutorService`, tasks scheduled with this service can have
    their execution delayed and can be configured to run repeatedly at
    fixed rate or with fixed delay.

    Example:
        >>> scheduler = client.get_scheduled_executor_service("my-scheduler")
        >>>
        >>> # Schedule a one-time task after 10 seconds
        >>> future = scheduler.schedule(my_task, 10, TimeUnit.SECONDS)
        >>>
        >>> # Schedule a task at fixed rate (every 5 seconds)
        >>> future = scheduler.schedule_at_fixed_rate(
        ...     my_task, 0, 5, TimeUnit.SECONDS
        ... )
        >>>
        >>> # Get all scheduled tasks
        >>> all_futures = scheduler.get_all_scheduled()
    """

    SERVICE_NAME = "hz:impl:scheduledExecutorService"

    def __init__(
        self,
        service_name: str,
        name: str,
        context: Optional[ProxyContext] = None,
    ):
        super().__init__(service_name, name, context)
        self._is_shutdown = False
        self._task_counter = 0

    def _generate_task_name(self) -> str:
        """Generate a unique task name."""
        self._task_counter += 1
        return f"{self._name}:{uuid_module.uuid4().hex[:8]}:{self._task_counter}"

    def schedule(
        self,
        task: Union[Callable, Runnable],
        delay: float,
        unit: TimeUnit = TimeUnit.SECONDS,
    ) -> IScheduledFuture:
        """Schedule a task for one-time execution after a delay.

        Args:
            task: The Callable or Runnable to execute.
            delay: The delay before execution.
            unit: The time unit for the delay.

        Returns:
            An IScheduledFuture representing the scheduled task.

        Raises:
            IllegalStateException: If the executor is shut down.

        Example:
            >>> future = scheduler.schedule(my_task, 30, TimeUnit.SECONDS)
            >>> result = future.get()  # Blocks until task completes
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        delay_ms = unit.to_millis(delay)
        return self._schedule_on_partition(task, delay_ms, period_ms=0, fixed_rate=False)

    def schedule_on_member(
        self,
        task: Union[Callable, Runnable],
        member: Member,
        delay: float,
        unit: TimeUnit = TimeUnit.SECONDS,
    ) -> IScheduledFuture:
        """Schedule a task for execution on a specific member after a delay.

        Args:
            task: The Callable or Runnable to execute.
            member: The target member.
            delay: The delay before execution.
            unit: The time unit for the delay.

        Returns:
            An IScheduledFuture representing the scheduled task.

        Raises:
            IllegalArgumentException: If member is None.
            IllegalStateException: If the executor is shut down.
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        if member is None:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Member cannot be None")

        delay_ms = unit.to_millis(delay)
        return self._schedule_on_member(task, member, delay_ms, period_ms=0, fixed_rate=False)

    def schedule_on_key_owner(
        self,
        task: Union[Callable, Runnable],
        key: Any,
        delay: float,
        unit: TimeUnit = TimeUnit.SECONDS,
    ) -> IScheduledFuture:
        """Schedule a task for execution on the owner of the specified key.

        This is useful for data locality - the task runs on the member
        that owns the partition containing the key.

        Args:
            task: The Callable or Runnable to execute.
            key: The key whose owner will execute the task.
            delay: The delay before execution.
            unit: The time unit for the delay.

        Returns:
            An IScheduledFuture representing the scheduled task.

        Raises:
            IllegalArgumentException: If key is None.
            IllegalStateException: If the executor is shut down.
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        if key is None:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Key cannot be None")

        delay_ms = unit.to_millis(delay)
        key_data = self._to_data(key)
        partition_id = self._get_partition_id(key_data)
        return self._schedule_on_partition(
            task, delay_ms, period_ms=0, fixed_rate=False, partition_id=partition_id
        )

    def schedule_at_fixed_rate(
        self,
        task: Union[Callable, Runnable],
        initial_delay: float,
        period: float,
        unit: TimeUnit = TimeUnit.SECONDS,
    ) -> IScheduledFuture:
        """Schedule a task for repeated execution at a fixed rate.

        The task will first execute after initial_delay, then repeatedly
        with the given period. If any execution takes longer than the
        period, subsequent executions may start late but will not
        execute concurrently.

        Args:
            task: The Callable or Runnable to execute.
            initial_delay: The delay before first execution.
            period: The period between successive executions.
            unit: The time unit for delays.

        Returns:
            An IScheduledFuture representing the scheduled task.

        Raises:
            IllegalArgumentException: If period is <= 0.
            IllegalStateException: If the executor is shut down.

        Example:
            >>> # Run every 5 seconds, starting immediately
            >>> future = scheduler.schedule_at_fixed_rate(
            ...     heartbeat_task, 0, 5, TimeUnit.SECONDS
            ... )
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        if period <= 0:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Period must be positive")

        initial_delay_ms = unit.to_millis(initial_delay)
        period_ms = unit.to_millis(period)
        return self._schedule_on_partition(task, initial_delay_ms, period_ms, fixed_rate=True)

    def schedule_on_member_at_fixed_rate(
        self,
        task: Union[Callable, Runnable],
        member: Member,
        initial_delay: float,
        period: float,
        unit: TimeUnit = TimeUnit.SECONDS,
    ) -> IScheduledFuture:
        """Schedule a task for repeated execution on a specific member at a fixed rate.

        Args:
            task: The Callable or Runnable to execute.
            member: The target member.
            initial_delay: The delay before first execution.
            period: The period between successive executions.
            unit: The time unit for delays.

        Returns:
            An IScheduledFuture representing the scheduled task.

        Raises:
            IllegalArgumentException: If member is None or period <= 0.
            IllegalStateException: If the executor is shut down.
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        if member is None:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Member cannot be None")

        if period <= 0:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Period must be positive")

        initial_delay_ms = unit.to_millis(initial_delay)
        period_ms = unit.to_millis(period)
        return self._schedule_on_member(task, member, initial_delay_ms, period_ms, fixed_rate=True)

    def schedule_on_key_owner_at_fixed_rate(
        self,
        task: Union[Callable, Runnable],
        key: Any,
        initial_delay: float,
        period: float,
        unit: TimeUnit = TimeUnit.SECONDS,
    ) -> IScheduledFuture:
        """Schedule a task for repeated execution on the key owner at a fixed rate.

        Args:
            task: The Callable or Runnable to execute.
            key: The key whose owner will execute the task.
            initial_delay: The delay before first execution.
            period: The period between successive executions.
            unit: The time unit for delays.

        Returns:
            An IScheduledFuture representing the scheduled task.
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        if key is None:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Key cannot be None")

        if period <= 0:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Period must be positive")

        initial_delay_ms = unit.to_millis(initial_delay)
        period_ms = unit.to_millis(period)
        key_data = self._to_data(key)
        partition_id = self._get_partition_id(key_data)
        return self._schedule_on_partition(
            task, initial_delay_ms, period_ms, fixed_rate=True, partition_id=partition_id
        )

    def schedule_with_fixed_delay(
        self,
        task: Union[Callable, Runnable],
        initial_delay: float,
        delay: float,
        unit: TimeUnit = TimeUnit.SECONDS,
    ) -> IScheduledFuture:
        """Schedule a task for repeated execution with a fixed delay between completions.

        The task will first execute after initial_delay, then repeatedly
        with the given delay between the end of one execution and the
        start of the next.

        Args:
            task: The Callable or Runnable to execute.
            initial_delay: The delay before first execution.
            delay: The delay between end of one execution and start of next.
            unit: The time unit for delays.

        Returns:
            An IScheduledFuture representing the scheduled task.

        Raises:
            IllegalArgumentException: If delay is <= 0.
            IllegalStateException: If the executor is shut down.

        Example:
            >>> # Run with 5 second gaps between executions
            >>> future = scheduler.schedule_with_fixed_delay(
            ...     cleanup_task, 0, 5, TimeUnit.SECONDS
            ... )
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        if delay <= 0:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Delay must be positive")

        initial_delay_ms = unit.to_millis(initial_delay)
        delay_ms = unit.to_millis(delay)
        return self._schedule_on_partition(task, initial_delay_ms, delay_ms, fixed_rate=False)

    def schedule_on_member_with_fixed_delay(
        self,
        task: Union[Callable, Runnable],
        member: Member,
        initial_delay: float,
        delay: float,
        unit: TimeUnit = TimeUnit.SECONDS,
    ) -> IScheduledFuture:
        """Schedule a task for repeated execution on a specific member with fixed delay.

        Args:
            task: The Callable or Runnable to execute.
            member: The target member.
            initial_delay: The delay before first execution.
            delay: The delay between end of one execution and start of next.
            unit: The time unit for delays.

        Returns:
            An IScheduledFuture representing the scheduled task.
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        if member is None:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Member cannot be None")

        if delay <= 0:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Delay must be positive")

        initial_delay_ms = unit.to_millis(initial_delay)
        delay_ms = unit.to_millis(delay)
        return self._schedule_on_member(task, member, initial_delay_ms, delay_ms, fixed_rate=False)

    def schedule_on_key_owner_with_fixed_delay(
        self,
        task: Union[Callable, Runnable],
        key: Any,
        initial_delay: float,
        delay: float,
        unit: TimeUnit = TimeUnit.SECONDS,
    ) -> IScheduledFuture:
        """Schedule a task for repeated execution on the key owner with fixed delay.

        Args:
            task: The Callable or Runnable to execute.
            key: The key whose owner will execute the task.
            initial_delay: The delay before first execution.
            delay: The delay between end of one execution and start of next.
            unit: The time unit for delays.

        Returns:
            An IScheduledFuture representing the scheduled task.
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        if key is None:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Key cannot be None")

        if delay <= 0:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Delay must be positive")

        initial_delay_ms = unit.to_millis(initial_delay)
        delay_ms = unit.to_millis(delay)
        key_data = self._to_data(key)
        partition_id = self._get_partition_id(key_data)
        return self._schedule_on_partition(
            task, initial_delay_ms, delay_ms, fixed_rate=False, partition_id=partition_id
        )

    def get_scheduled(self, handler: ScheduledTaskHandler) -> IScheduledFuture:
        """Get a scheduled future for an existing task.

        Args:
            handler: The task handler.

        Returns:
            An IScheduledFuture for the specified task.
        """
        self._check_not_destroyed()
        return IScheduledFuture(handler, self._context)

    def get_all_scheduled(self) -> Dict[Member, List[IScheduledFuture]]:
        """Get all scheduled tasks across all members.

        Returns:
            A dictionary mapping members to their scheduled futures.

        Example:
            >>> all_tasks = scheduler.get_all_scheduled()
            >>> for member, futures in all_tasks.items():
            ...     print(f"Member {member}: {len(futures)} tasks")
        """
        self._check_not_destroyed()

        if self._context is None or self._context.invocation_service is None:
            return {}

        from hazelcast.protocol.codec import ScheduledExecutorServiceCodec
        request = ScheduledExecutorServiceCodec.encode_get_all_scheduled_futures_request(
            self._name
        )

        from hazelcast.invocation import Invocation
        invocation = Invocation(request)
        result_future = self._context.invocation_service.invoke(invocation)

        response = result_future.result()
        handlers = ScheduledExecutorServiceCodec.decode_get_all_scheduled_futures_response(
            response
        )

        result: Dict[Member, List[IScheduledFuture]] = {}
        for handler_name, partition_id, member_uuid in handlers:
            handler = ScheduledTaskHandler(
                self._name,
                handler_name,
                partition_id=partition_id,
                member_uuid=member_uuid,
            )
            future = IScheduledFuture(handler, self._context)

            if member_uuid:
                member = Member(str(member_uuid))
            else:
                member = Member(f"partition-{partition_id}")

            if member not in result:
                result[member] = []
            result[member].append(future)

        return result

    def shutdown(self) -> None:
        """Initiate an orderly shutdown of the scheduler.

        Previously scheduled tasks are executed, but no new tasks
        will be accepted.
        """
        self._check_not_destroyed()
        if self._is_shutdown:
            return

        _logger.debug("Shutting down scheduled executor service: %s", self._name)

        if self._context is not None and self._context.invocation_service is not None:
            from hazelcast.protocol.codec import ScheduledExecutorServiceCodec
            request = ScheduledExecutorServiceCodec.encode_shutdown_request(self._name)

            from hazelcast.invocation import Invocation
            invocation = Invocation(request)
            self._context.invocation_service.invoke(invocation)

        self._is_shutdown = True

    def is_shutdown(self) -> bool:
        """Check if this scheduler has been shut down.

        Returns:
            True if the scheduler has been shut down.
        """
        return self._is_shutdown

    def _check_not_shutdown(self) -> None:
        """Raise an exception if the scheduler is shut down."""
        if self._is_shutdown:
            from hazelcast.exceptions import IllegalStateException
            raise IllegalStateException(
                f"Scheduled executor service {self._name} has been shut down"
            )

    def _schedule_on_partition(
        self,
        task: Union[Callable, Runnable],
        initial_delay_ms: int,
        period_ms: int,
        fixed_rate: bool,
        partition_id: int = -1,
    ) -> IScheduledFuture:
        """Schedule a task on a partition."""
        task_name = self._generate_task_name()
        task_data = self._to_data(task)

        if partition_id < 0:
            partition_id = hash(task_name) % 271

        if self._context is None or self._context.invocation_service is None:
            handler = ScheduledTaskHandler(self._name, task_name, partition_id=partition_id)
            return IScheduledFuture(handler, self._context)

        from hazelcast.protocol.codec import ScheduledExecutorServiceCodec
        request = ScheduledExecutorServiceCodec.encode_submit_to_partition_request(
            self._name,
            task_data,
            partition_id,
            initial_delay_ms,
            period_ms,
            fixed_rate,
            auto_disposable=True,
        )

        from hazelcast.invocation import Invocation
        invocation = Invocation(request, partition_id=partition_id)
        result_future = self._context.invocation_service.invoke(invocation)

        try:
            response = result_future.result()
            handler_name, resp_partition_id, _ = (
                ScheduledExecutorServiceCodec.decode_submit_response(response)
            )
            handler = ScheduledTaskHandler(
                self._name,
                handler_name or task_name,
                partition_id=resp_partition_id if resp_partition_id >= 0 else partition_id,
            )
        except Exception:
            handler = ScheduledTaskHandler(self._name, task_name, partition_id=partition_id)

        return IScheduledFuture(handler, self._context)

    def _schedule_on_member(
        self,
        task: Union[Callable, Runnable],
        member: Member,
        initial_delay_ms: int,
        period_ms: int,
        fixed_rate: bool,
    ) -> IScheduledFuture:
        """Schedule a task on a specific member."""
        task_name = self._generate_task_name()
        task_data = self._to_data(task)
        member_uuid = uuid_module.UUID(member.uuid) if isinstance(member.uuid, str) else member.uuid

        if self._context is None or self._context.invocation_service is None:
            handler = ScheduledTaskHandler(self._name, task_name, member_uuid=member_uuid)
            return IScheduledFuture(handler, self._context)

        from hazelcast.protocol.codec import ScheduledExecutorServiceCodec
        request = ScheduledExecutorServiceCodec.encode_submit_to_member_request(
            self._name,
            task_data,
            member_uuid,
            initial_delay_ms,
            period_ms,
            fixed_rate,
            auto_disposable=True,
        )

        from hazelcast.invocation import Invocation
        invocation = Invocation(request)
        result_future = self._context.invocation_service.invoke(invocation)

        try:
            response = result_future.result()
            handler_name, _, _ = ScheduledExecutorServiceCodec.decode_submit_response(response)
            handler = ScheduledTaskHandler(
                self._name,
                handler_name or task_name,
                member_uuid=member_uuid,
            )
        except Exception:
            handler = ScheduledTaskHandler(self._name, task_name, member_uuid=member_uuid)

        return IScheduledFuture(handler, self._context)


ScheduledExecutorService = IScheduledExecutorService
ScheduledExecutorServiceProxy = IScheduledExecutorService
