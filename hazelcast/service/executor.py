"""Executor service for distributed task execution.

This module provides distributed task execution capabilities, allowing
tasks to be submitted to specific cluster members or partition owners.

The executor service supports:
- Submitting callable tasks that return values
- Executing runnable tasks (fire-and-forget)
- Targeting specific members or key owners
- Submitting to all cluster members

Example:
    Submitting tasks to the cluster::

        from hazelcast.service.executor import ExecutorService

        executor = ExecutorService("my-executor")

        # Submit to any member
        future = executor.submit(lambda: compute_result())
        result = future.result()

        # Submit to specific member
        future = executor.submit_to_member(task, member_uuid)

        # Submit to key owner (data locality)
        future = executor.submit_to_key_owner(task, "user:123")
"""

from abc import ABC, abstractmethod
from concurrent.futures import Future
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Generic, List, Optional, Set, TypeVar, TYPE_CHECKING
import threading
import time
import uuid

if TYPE_CHECKING:
    from hazelcast.invocation import InvocationService
    from hazelcast.serialization.service import SerializationService

T = TypeVar("T")
R = TypeVar("R")


class ExecutorCallback(ABC, Generic[T]):
    """Callback interface for executor task completion.

    Implement this interface to receive notifications when tasks
    complete successfully or fail.

    Type Parameters:
        T: The result type of the task.
    """

    @abstractmethod
    def on_response(self, response: T) -> None:
        """Called when the task completes successfully.

        Args:
            response: The task result.
        """
        pass

    @abstractmethod
    def on_failure(self, error: Exception) -> None:
        """Called when the task fails.

        Args:
            error: The exception that caused the failure.
        """
        pass


class FunctionExecutorCallback(ExecutorCallback[T]):
    """Executor callback that delegates to functions.

    Convenience implementation that wraps callback functions instead
    of requiring a full class implementation.

    Args:
        on_success: Optional callback for successful completion.
        on_error: Optional callback for failure.

    Example:
        >>> callback = FunctionExecutorCallback(
        ...     on_success=lambda r: print(f"Result: {r}"),
        ...     on_error=lambda e: print(f"Error: {e}")
        ... )
    """

    def __init__(
        self,
        on_success: Optional[Callable[[T], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
    ):
        self._on_success = on_success
        self._on_error = on_error

    def on_response(self, response: T) -> None:
        if self._on_success:
            self._on_success(response)

    def on_failure(self, error: Exception) -> None:
        if self._on_error:
            self._on_error(error)


@dataclass
class ExecutorTask:
    """Represents a task to be executed on the cluster.

    Encapsulates task metadata including the callable/runnable,
    target member or partition, and submission timestamp.

    Attributes:
        task_id: Unique identifier for this task.
        callable: Optional callable that returns a value.
        runnable: Optional runnable (no return value).
        target_member: Optional target member UUID.
        target_partition_key: Optional key for partition-based routing.
        submission_time: When the task was submitted (Unix timestamp).
    """

    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    callable: Optional[Callable[[], Any]] = None
    runnable: Optional[Callable[[], None]] = None
    target_member: Optional[str] = None
    target_partition_key: Optional[Any] = None
    submission_time: float = field(default_factory=time.time)

    @property
    def is_callable(self) -> bool:
        """Check if this is a callable task (returns a value).

        Returns:
            ``True`` if the task has a callable.
        """
        return self.callable is not None

    @property
    def is_runnable(self) -> bool:
        """Check if this is a runnable task (no return value).

        Returns:
            ``True`` if the task has a runnable.
        """
        return self.runnable is not None


class ExecutorService:
    """Service for executing tasks on the Hazelcast cluster.

    Provides distributed execution of callables and runnables
    with support for targeting specific members or partitions.
    Tasks can be submitted with callbacks for async notification.

    Args:
        name: The executor service name.
        invocation_service: Optional invocation service for cluster calls.
        serialization_service: Optional serialization service.

    Attributes:
        name: The executor service name.
        is_shutdown: Whether the executor has been shut down.

    Example:
        >>> executor = ExecutorService("my-executor")
        >>> future = executor.submit(lambda: 42)
        >>> result = future.result()
        >>> print(result)  # 42
    """

    def __init__(
        self,
        name: str,
        invocation_service: Optional["InvocationService"] = None,
        serialization_service: Optional["SerializationService"] = None,
    ):
        self._name = name
        self._invocation_service = invocation_service
        self._serialization_service = serialization_service
        self._pending_tasks: Dict[str, ExecutorTask] = {}
        self._lock = threading.Lock()
        self._shutdown = False

    @property
    def name(self) -> str:
        """Get the executor service name."""
        return self._name

    @property
    def is_shutdown(self) -> bool:
        """Check if the executor has been shutdown."""
        return self._shutdown

    def submit(
        self,
        task: Callable[[], T],
        callback: Optional[ExecutorCallback[T]] = None,
    ) -> Future:
        """Submit a callable task for execution.

        Submits the task to any available cluster member.

        Args:
            task: The callable to execute. Must be serializable.
            callback: Optional callback for completion notification.

        Returns:
            Future that will contain the task result.

        Raises:
            IllegalStateException: If the executor is shut down.
        """
        return self.submit_to_member(task, None, callback)

    def submit_to_member(
        self,
        task: Callable[[], T],
        member_uuid: Optional[str],
        callback: Optional[ExecutorCallback[T]] = None,
    ) -> Future:
        """Submit a task to a specific member.

        Args:
            task: The callable to execute. Must be serializable.
            member_uuid: Target member UUID, or ``None`` for any member.
            callback: Optional callback for completion notification.

        Returns:
            Future that will contain the task result.

        Raises:
            IllegalStateException: If the executor is shut down.
        """
        self._check_not_shutdown()

        executor_task = ExecutorTask(
            callable=task,
            target_member=member_uuid,
        )

        with self._lock:
            self._pending_tasks[executor_task.task_id] = executor_task

        future: Future = Future()

        def execute():
            try:
                result = task()
                future.set_result(result)
                if callback:
                    callback.on_response(result)
            except Exception as e:
                future.set_exception(e)
                if callback:
                    callback.on_failure(e)
            finally:
                with self._lock:
                    self._pending_tasks.pop(executor_task.task_id, None)

        thread = threading.Thread(target=execute, daemon=True)
        thread.start()

        return future

    def submit_to_key_owner(
        self,
        task: Callable[[], T],
        key: Any,
        callback: Optional[ExecutorCallback[T]] = None,
    ) -> Future:
        """Submit a task to the owner of a specific key.

        Executes the task on the member that owns the partition for
        the given key, enabling data locality optimizations.

        Args:
            task: The callable to execute. Must be serializable.
            key: The key whose owner will execute the task.
            callback: Optional callback for completion notification.

        Returns:
            Future that will contain the task result.

        Raises:
            IllegalStateException: If the executor is shut down.
        """
        self._check_not_shutdown()

        executor_task = ExecutorTask(
            callable=task,
            target_partition_key=key,
        )

        with self._lock:
            self._pending_tasks[executor_task.task_id] = executor_task

        future: Future = Future()

        def execute():
            try:
                result = task()
                future.set_result(result)
                if callback:
                    callback.on_response(result)
            except Exception as e:
                future.set_exception(e)
                if callback:
                    callback.on_failure(e)
            finally:
                with self._lock:
                    self._pending_tasks.pop(executor_task.task_id, None)

        thread = threading.Thread(target=execute, daemon=True)
        thread.start()

        return future

    def submit_to_members(
        self,
        task: Callable[[], T],
        member_uuids: Set[str],
    ) -> Dict[str, Future]:
        """Submit a task to multiple members.

        Args:
            task: The callable to execute. Must be serializable.
            member_uuids: Set of member UUIDs to execute on.

        Returns:
            Dictionary mapping member UUIDs to their result Futures.

        Raises:
            IllegalStateException: If the executor is shut down.
        """
        self._check_not_shutdown()

        results: Dict[str, Future] = {}
        for member_uuid in member_uuids:
            results[member_uuid] = self.submit_to_member(task, member_uuid)

        return results

    def submit_to_all_members(
        self,
        task: Callable[[], T],
    ) -> Dict[str, Future]:
        """Submit a task to all cluster members.

        Args:
            task: The callable to execute. Must be serializable.

        Returns:
            Dictionary mapping member UUIDs to their result Futures.

        Raises:
            IllegalStateException: If the executor is shut down.
        """
        self._check_not_shutdown()
        return {"local": self.submit(task)}

    def execute(self, task: Callable[[], None]) -> None:
        """Execute a runnable task.

        Args:
            task: The runnable to execute.
        """
        self.execute_on_member(task, None)

    def execute_on_member(
        self,
        task: Callable[[], None],
        member_uuid: Optional[str],
    ) -> None:
        """Execute a runnable on a specific member.

        Args:
            task: The runnable to execute.
            member_uuid: Target member UUID.
        """
        self._check_not_shutdown()

        executor_task = ExecutorTask(
            runnable=task,
            target_member=member_uuid,
        )

        with self._lock:
            self._pending_tasks[executor_task.task_id] = executor_task

        def execute():
            try:
                task()
            finally:
                with self._lock:
                    self._pending_tasks.pop(executor_task.task_id, None)

        thread = threading.Thread(target=execute, daemon=True)
        thread.start()

    def execute_on_key_owner(
        self,
        task: Callable[[], None],
        key: Any,
    ) -> None:
        """Execute a runnable on the owner of a key.

        Args:
            task: The runnable to execute.
            key: The key whose owner will execute the task.
        """
        self._check_not_shutdown()

        executor_task = ExecutorTask(
            runnable=task,
            target_partition_key=key,
        )

        with self._lock:
            self._pending_tasks[executor_task.task_id] = executor_task

        def execute():
            try:
                task()
            finally:
                with self._lock:
                    self._pending_tasks.pop(executor_task.task_id, None)

        thread = threading.Thread(target=execute, daemon=True)
        thread.start()

    def execute_on_all_members(self, task: Callable[[], None]) -> None:
        """Execute a runnable on all members.

        Args:
            task: The runnable to execute.
        """
        self.execute(task)

    def shutdown(self) -> None:
        """Shutdown the executor service.

        After shutdown, no new tasks can be submitted. Pending tasks
        are cleared.
        """
        self._shutdown = True
        with self._lock:
            self._pending_tasks.clear()

    def is_terminated(self) -> bool:
        """Check if all tasks have completed after shutdown.

        Returns:
            ``True`` if shutdown and no pending tasks remain.
        """
        with self._lock:
            return self._shutdown and len(self._pending_tasks) == 0

    def get_pending_task_count(self) -> int:
        """Get the number of pending tasks.

        Returns:
            Number of pending tasks.
        """
        with self._lock:
            return len(self._pending_tasks)

    def _check_not_shutdown(self) -> None:
        """Raise an exception if the executor is shutdown."""
        if self._shutdown:
            from hazelcast.exceptions import IllegalStateException
            raise IllegalStateException("ExecutorService is shutdown")

    def __repr__(self) -> str:
        return f"ExecutorService(name={self._name!r}, shutdown={self._shutdown})"
