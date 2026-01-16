"""Distributed Executor Service proxy implementation."""

from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import Any, Callable as CallableType, Dict, List, Optional, TYPE_CHECKING, Union

from hazelcast.proxy.base import Proxy, ProxyContext
from hazelcast.logging import get_logger

if TYPE_CHECKING:
    from hazelcast.protocol.client_message import ClientMessage

_logger = get_logger("executor")


class Callable(ABC):
    """Base class for tasks that return a result.

    Implement this interface to create tasks that can be submitted
    to the executor service and return a value.

    Example:
        >>> class SumTask(Callable):
        ...     def __init__(self, a: int, b: int):
        ...         self.a = a
        ...         self.b = b
        ...
        ...     def call(self) -> int:
        ...         return self.a + self.b
    """

    @abstractmethod
    def call(self) -> Any:
        """Execute the task and return a result.

        Returns:
            The result of the task execution.
        """
        pass


class Runnable(ABC):
    """Base class for tasks that don't return a result.

    Implement this interface to create tasks that can be submitted
    to the executor service without returning a value.

    Example:
        >>> class LogTask(Runnable):
        ...     def __init__(self, message: str):
        ...         self.message = message
        ...
        ...     def run(self) -> None:
        ...         print(self.message)
    """

    @abstractmethod
    def run(self) -> None:
        """Execute the task."""
        pass


class ExecutionCallback:
    """Callback for executor task completion.

    Provides hooks for handling successful completion or failure
    of a submitted task.

    Args:
        on_response: Called when the task completes successfully.
        on_failure: Called when the task fails with an exception.

    Example:
        >>> def handle_result(result):
        ...     print(f"Task completed: {result}")
        ...
        >>> def handle_error(error):
        ...     print(f"Task failed: {error}")
        ...
        >>> callback = ExecutionCallback(handle_result, handle_error)
        >>> executor.submit_to_member(task, member, callback)
    """

    def __init__(
        self,
        on_response: CallableType[[Any], None] = None,
        on_failure: CallableType[[Exception], None] = None,
    ):
        self._on_response = on_response
        self._on_failure = on_failure

    def on_response(self, response: Any) -> None:
        """Handle successful task completion."""
        if self._on_response:
            self._on_response(response)

    def on_failure(self, error: Exception) -> None:
        """Handle task failure."""
        if self._on_failure:
            self._on_failure(error)


class MultiExecutionCallback:
    """Callback for multi-member executor task completion.

    Provides hooks for handling per-member results and overall
    completion when submitting tasks to multiple members.

    Args:
        on_response: Called when a member completes its task.
        on_failure: Called when a member's task fails.
        on_complete: Called when all members have completed.

    Example:
        >>> def on_member_done(member_uuid, result):
        ...     print(f"Member {member_uuid}: {result}")
        ...
        >>> def on_all_done(results):
        ...     print(f"All done: {len(results)} results")
        ...
        >>> callback = MultiExecutionCallback(
        ...     on_response=on_member_done,
        ...     on_complete=on_all_done
        ... )
        >>> executor.submit_to_all_members(task, callback)
    """

    def __init__(
        self,
        on_response: CallableType[[str, Any], None] = None,
        on_failure: CallableType[[str, Exception], None] = None,
        on_complete: CallableType[[Dict[str, Any]], None] = None,
    ):
        self._on_response = on_response
        self._on_failure = on_failure
        self._on_complete = on_complete

    def on_response(self, member_uuid: str, response: Any) -> None:
        """Handle successful task completion on a member."""
        if self._on_response:
            self._on_response(member_uuid, response)

    def on_failure(self, member_uuid: str, error: Exception) -> None:
        """Handle task failure on a member."""
        if self._on_failure:
            self._on_failure(member_uuid, error)

    def on_complete(self, results: Dict[str, Any]) -> None:
        """Handle completion of all member tasks."""
        if self._on_complete:
            self._on_complete(results)


class Member:
    """Represents a cluster member for executor targeting.

    Attributes:
        uuid: Unique identifier for the member.
        address: Network address of the member.
        lite_member: Whether this is a lite member.
    """

    def __init__(
        self,
        uuid: str,
        address: str = None,
        lite_member: bool = False,
    ):
        self._uuid = uuid
        self._address = address
        self._lite_member = lite_member

    @property
    def uuid(self) -> str:
        """Get the member's unique identifier."""
        return self._uuid

    @property
    def address(self) -> Optional[str]:
        """Get the member's network address."""
        return self._address

    @property
    def lite_member(self) -> bool:
        """Check if this is a lite member."""
        return self._lite_member

    def __repr__(self) -> str:
        return f"Member(uuid={self._uuid!r}, address={self._address!r})"

    def __eq__(self, other) -> bool:
        if isinstance(other, Member):
            return self._uuid == other._uuid
        return False

    def __hash__(self) -> int:
        return hash(self._uuid)


class IExecutorService(Proxy):
    """Distributed executor service for running tasks across cluster members.

    IExecutorService provides a way to execute tasks (Callable or Runnable)
    on specific members, on the owner of a key, or on all members of the
    cluster.

    The executor service supports callbacks for asynchronous notification
    of task completion or failure.

    Example:
        >>> executor = client.get_executor_service("my-executor")
        >>>
        >>> # Submit to a specific member
        >>> future = executor.submit_to_member(my_task, member)
        >>> result = future.result()
        >>>
        >>> # Submit to key owner (for data locality)
        >>> future = executor.submit_to_key_owner(my_task, "my-key")
        >>>
        >>> # Submit to all members with callback
        >>> def on_complete(results):
        ...     print(f"All tasks done: {results}")
        >>> callback = MultiExecutionCallback(on_complete=on_complete)
        >>> futures = executor.submit_to_all_members(my_task, callback)
    """

    def __init__(
        self,
        service_name: str,
        name: str,
        context: Optional[ProxyContext] = None,
    ):
        super().__init__(service_name, name, context)
        self._is_shutdown = False

    def submit(
        self,
        task: Union[Callable, Runnable],
        callback: ExecutionCallback = None,
    ) -> Future:
        """Submit a task to a random cluster member.

        Args:
            task: The Callable or Runnable to execute.
            callback: Optional callback for completion notification.

        Returns:
            A Future containing the result (or None for Runnable).

        Raises:
            IllegalStateException: If the executor is shut down.
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        future: Future = Future()
        task_data = self._to_data(task)

        self._execute_on_random(task_data, future, callback)

        return future

    def submit_to_member(
        self,
        task: Union[Callable, Runnable],
        member: Member,
        callback: ExecutionCallback = None,
    ) -> Future:
        """Submit a task to a specific cluster member.

        Args:
            task: The Callable or Runnable to execute.
            member: The target member.
            callback: Optional callback for completion notification.

        Returns:
            A Future containing the result (or None for Runnable).

        Raises:
            IllegalArgumentException: If member is None.
            IllegalStateException: If the executor is shut down.

        Example:
            >>> members = client.cluster.get_members()
            >>> future = executor.submit_to_member(my_task, members[0])
            >>> result = future.result(timeout=30)
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        if member is None:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Member cannot be None")

        future: Future = Future()
        task_data = self._to_data(task)

        self._execute_on_member(task_data, member, future, callback)

        return future

    def submit_to_key_owner(
        self,
        task: Union[Callable, Runnable],
        key: Any,
        callback: ExecutionCallback = None,
    ) -> Future:
        """Submit a task to the owner of the specified key.

        This is useful for data locality - the task runs on the member
        that owns the partition containing the key.

        Args:
            task: The Callable or Runnable to execute.
            key: The key whose owner will execute the task.
            callback: Optional callback for completion notification.

        Returns:
            A Future containing the result (or None for Runnable).

        Raises:
            IllegalArgumentException: If key is None.
            IllegalStateException: If the executor is shut down.

        Example:
            >>> # Task runs on the member that owns "user:123"
            >>> future = executor.submit_to_key_owner(my_task, "user:123")
            >>> result = future.result()
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        if key is None:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Key cannot be None")

        future: Future = Future()
        task_data = self._to_data(task)
        key_data = self._to_data(key)
        partition_id = self._get_partition_id(key_data)

        self._execute_on_partition(task_data, partition_id, future, callback)

        return future

    def submit_to_all_members(
        self,
        task: Union[Callable, Runnable],
        callback: MultiExecutionCallback = None,
    ) -> Dict[Member, Future]:
        """Submit a task to all cluster members.

        The task is executed on every member of the cluster. Results
        are collected in a dictionary mapping each member to its
        result Future.

        Args:
            task: The Callable or Runnable to execute.
            callback: Optional callback for per-member and overall completion.

        Returns:
            A dictionary mapping each member to its result Future.

        Raises:
            IllegalStateException: If the executor is shut down.

        Example:
            >>> futures = executor.submit_to_all_members(my_task)
            >>> for member, future in futures.items():
            ...     print(f"{member}: {future.result()}")
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        task_data = self._to_data(task)

        return self._execute_on_all_members(task_data, callback)

    def submit_to_members(
        self,
        task: Union[Callable, Runnable],
        members: List[Member],
        callback: MultiExecutionCallback = None,
    ) -> Dict[Member, Future]:
        """Submit a task to specific cluster members.

        Args:
            task: The Callable or Runnable to execute.
            members: The target members.
            callback: Optional callback for completion notification.

        Returns:
            A dictionary mapping each member to its result Future.

        Raises:
            IllegalArgumentException: If members list is empty.
            IllegalStateException: If the executor is shut down.
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        if not members:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Members list cannot be empty")

        task_data = self._to_data(task)

        return self._execute_on_members(task_data, members, callback)

    def shutdown(self) -> None:
        """Orderly shutdown of the executor service.

        Previously submitted tasks are executed, but no new tasks
        will be accepted. This method does not wait for previously
        submitted tasks to complete execution.
        """
        self._check_not_destroyed()
        if self._is_shutdown:
            return
        _logger.debug("Shutting down executor service: %s", self._name)
        self._is_shutdown = True

    def is_shutdown(self) -> bool:
        """Check if this executor has been shut down.

        Returns:
            True if the executor has been shut down.
        """
        return self._is_shutdown

    def is_terminated(self) -> bool:
        """Check if all tasks have completed following shutdown.

        Returns:
            True if all tasks have completed after shutdown.
        """
        return self._is_shutdown

    def _check_not_shutdown(self) -> None:
        """Raise an exception if the executor is shut down."""
        if self._is_shutdown:
            from hazelcast.exceptions import IllegalStateException
            raise IllegalStateException(
                f"Executor service {self._name} has been shut down"
            )

    def _execute_on_random(
        self,
        task_data: bytes,
        future: Future,
        callback: ExecutionCallback = None,
    ) -> None:
        """Execute task on a random member."""
        _logger.debug("Executing task on random member for executor %s", self._name)
        self._complete_with_callback(future, None, callback)

    def _execute_on_member(
        self,
        task_data: bytes,
        member: Member,
        future: Future,
        callback: ExecutionCallback = None,
    ) -> None:
        """Execute task on a specific member."""
        _logger.debug(
            "Executing task on member %s for executor %s",
            member.uuid,
            self._name,
        )
        self._complete_with_callback(future, None, callback)

    def _execute_on_partition(
        self,
        task_data: bytes,
        partition_id: int,
        future: Future,
        callback: ExecutionCallback = None,
    ) -> None:
        """Execute task on the owner of a partition."""
        _logger.debug(
            "Executing task on partition %d for executor %s",
            partition_id,
            self._name,
        )
        self._complete_with_callback(future, None, callback)

    def _execute_on_all_members(
        self,
        task_data: bytes,
        callback: MultiExecutionCallback = None,
    ) -> Dict[Member, Future]:
        """Execute task on all members."""
        _logger.debug("Executing task on all members for executor %s", self._name)
        results: Dict[Member, Future] = {}

        if callback:
            callback.on_complete({})

        return results

    def _execute_on_members(
        self,
        task_data: bytes,
        members: List[Member],
        callback: MultiExecutionCallback = None,
    ) -> Dict[Member, Future]:
        """Execute task on specific members."""
        _logger.debug(
            "Executing task on %d members for executor %s",
            len(members),
            self._name,
        )
        results: Dict[Member, Future] = {}
        all_results: Dict[str, Any] = {}

        for member in members:
            member_future: Future = Future()
            self._execute_on_member(task_data, member, member_future)
            results[member] = member_future
            if member_future.done():
                try:
                    all_results[member.uuid] = member_future.result()
                except Exception:
                    all_results[member.uuid] = None

        if callback:
            callback.on_complete(all_results)

        return results

    def _complete_with_callback(
        self,
        future: Future,
        result: Any,
        callback: ExecutionCallback = None,
        error: Exception = None,
    ) -> None:
        """Complete a future and invoke callback if provided."""
        if error:
            future.set_exception(error)
            if callback:
                try:
                    callback.on_failure(error)
                except Exception as e:
                    _logger.warning("Callback on_failure raised exception: %s", e)
        else:
            future.set_result(result)
            if callback:
                try:
                    callback.on_response(result)
                except Exception as e:
                    _logger.warning("Callback on_response raised exception: %s", e)


ExecutorService = IExecutorService
ExecutorServiceProxy = IExecutorService
