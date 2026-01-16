"""Distributed Executor Service proxy implementation."""

import uuid as uuid_module
from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import Any, Callable as CallableType, Dict, List, Optional, Set, TYPE_CHECKING, Union

from hazelcast.proxy.base import Proxy, ProxyContext
from hazelcast.logging import get_logger

if TYPE_CHECKING:
    from hazelcast.protocol.client_message import ClientMessage

_logger = get_logger("executor")


class MemberSelector(ABC):
    """Interface for selecting cluster members for task execution.

    Implement this interface to filter which members should be
    considered for task execution.

    Example:
        >>> class DataMemberSelector(MemberSelector):
        ...     def select(self, member: Member) -> bool:
        ...         return not member.lite_member
    """

    @abstractmethod
    def select(self, member: "Member") -> bool:
        """Determine if the given member should be selected.

        Args:
            member: The cluster member to evaluate.

        Returns:
            True if the member should be selected for task execution.
        """
        pass


class LiteMemberSelector(MemberSelector):
    """Selects only lite members."""

    def select(self, member: "Member") -> bool:
        return member.lite_member


class DataMemberSelector(MemberSelector):
    """Selects only data (non-lite) members."""

    def select(self, member: "Member") -> bool:
        return not member.lite_member


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
        self._members: Dict[str, Member] = {}

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

    def submit_to_member_with_selector(
        self,
        task: Union[Callable, Runnable],
        member_selector: MemberSelector,
        callback: ExecutionCallback = None,
    ) -> Future:
        """Submit a task to a member selected by the given selector.

        Args:
            task: The Callable or Runnable to execute.
            member_selector: The selector to filter eligible members.
            callback: Optional callback for completion notification.

        Returns:
            A Future containing the result (or None for Runnable).

        Raises:
            IllegalArgumentException: If no member matches the selector.
            IllegalStateException: If the executor is shut down.

        Example:
            >>> selector = DataMemberSelector()
            >>> future = executor.submit_to_member_with_selector(my_task, selector)
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        selected_members = self._select_members(member_selector)
        if not selected_members:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("No member matches the selector")

        member = next(iter(selected_members))
        return self.submit_to_member(task, member, callback)

    def submit_to_all_members_with_selector(
        self,
        task: Union[Callable, Runnable],
        member_selector: MemberSelector,
        callback: MultiExecutionCallback = None,
    ) -> Dict[Member, Future]:
        """Submit a task to all members matching the selector.

        Args:
            task: The Callable or Runnable to execute.
            member_selector: The selector to filter eligible members.
            callback: Optional callback for completion notification.

        Returns:
            A dictionary mapping each selected member to its result Future.

        Raises:
            IllegalArgumentException: If no member matches the selector.
            IllegalStateException: If the executor is shut down.

        Example:
            >>> selector = DataMemberSelector()
            >>> futures = executor.submit_to_all_members_with_selector(my_task, selector)
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        selected_members = self._select_members(member_selector)
        if not selected_members:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("No member matches the selector")

        return self.submit_to_members(task, list(selected_members), callback)

    def _select_members(self, selector: MemberSelector) -> Set[Member]:
        """Select members using the given selector."""
        return {m for m in self._members.values() if selector.select(m)}

    def _get_members(self) -> List[Member]:
        """Get all known cluster members."""
        return list(self._members.values())

    def _add_member(self, member: Member) -> None:
        """Add a member to the known members list."""
        self._members[member.uuid] = member

    def _remove_member(self, member_uuid: str) -> None:
        """Remove a member from the known members list."""
        self._members.pop(member_uuid, None)

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

        if self._context is None or self._context.invocation_service is None:
            self._complete_with_callback(future, None, callback)
            return

        partition_id = hash(uuid_module.uuid4()) % 271

        from hazelcast.protocol.codec import ExecutorServiceCodec
        request = ExecutorServiceCodec.encode_submit_to_partition_request(
            self._name, task_data, partition_id
        )

        def handle_response(response):
            result_data = ExecutorServiceCodec.decode_submit_response(response)
            return self._to_object(result_data) if result_data else None

        invoke_future = self._invoke_on_partition(request, partition_id, handle_response)
        self._chain_future(invoke_future, future, callback)

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

        if self._context is None or self._context.invocation_service is None:
            self._complete_with_callback(future, None, callback)
            return

        from hazelcast.protocol.codec import ExecutorServiceCodec
        member_uuid = uuid_module.UUID(member.uuid) if isinstance(member.uuid, str) else member.uuid
        request = ExecutorServiceCodec.encode_submit_to_member_request(
            self._name, task_data, member_uuid
        )

        def handle_response(response):
            result_data = ExecutorServiceCodec.decode_submit_response(response)
            return self._to_object(result_data) if result_data else None

        invoke_future = self._invoke(request, handle_response)
        self._chain_future(invoke_future, future, callback)

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

        if self._context is None or self._context.invocation_service is None:
            self._complete_with_callback(future, None, callback)
            return

        from hazelcast.protocol.codec import ExecutorServiceCodec
        request = ExecutorServiceCodec.encode_submit_to_partition_request(
            self._name, task_data, partition_id
        )

        def handle_response(response):
            result_data = ExecutorServiceCodec.decode_submit_response(response)
            return self._to_object(result_data) if result_data else None

        invoke_future = self._invoke_on_partition(request, partition_id, handle_response)
        self._chain_future(invoke_future, future, callback)

    def _execute_on_all_members(
        self,
        task_data: bytes,
        callback: MultiExecutionCallback = None,
    ) -> Dict[Member, Future]:
        """Execute task on all members."""
        _logger.debug("Executing task on all members for executor %s", self._name)

        members = self._get_members()
        if not members:
            if callback:
                callback.on_complete({})
            return {}

        return self._execute_on_members(task_data, members, callback)

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
        pending_count = [len(members)]

        def on_member_complete(member: Member, member_future: Future) -> None:
            try:
                result = member_future.result()
                all_results[member.uuid] = result
                if callback:
                    try:
                        callback.on_response(member.uuid, result)
                    except Exception as e:
                        _logger.warning("Callback on_response raised exception: %s", e)
            except Exception as e:
                all_results[member.uuid] = None
                if callback:
                    try:
                        callback.on_failure(member.uuid, e)
                    except Exception as ex:
                        _logger.warning("Callback on_failure raised exception: %s", ex)
            finally:
                pending_count[0] -= 1
                if pending_count[0] == 0 and callback:
                    try:
                        callback.on_complete(all_results)
                    except Exception as e:
                        _logger.warning("Callback on_complete raised exception: %s", e)

        for member in members:
            member_future: Future = Future()
            results[member] = member_future

            def make_callback(m: Member, f: Future):
                return lambda _: on_member_complete(m, f)

            self._execute_on_member(task_data, member, member_future)
            member_future.add_done_callback(make_callback(member, member_future))

        return results

    def _chain_future(
        self,
        source: Future,
        target: Future,
        callback: ExecutionCallback = None,
    ) -> None:
        """Chain a source future to a target future with optional callback."""
        def on_done(f: Future) -> None:
            try:
                result = f.result()
                target.set_result(result)
                if callback:
                    try:
                        callback.on_response(result)
                    except Exception as e:
                        _logger.warning("Callback on_response raised exception: %s", e)
            except Exception as e:
                target.set_exception(e)
                if callback:
                    try:
                        callback.on_failure(e)
                    except Exception as ex:
                        _logger.warning("Callback on_failure raised exception: %s", ex)

        source.add_done_callback(on_done)

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
