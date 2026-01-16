"""Durable Executor Service proxy implementation.

Provides a distributed executor service that durably stores task results
on the cluster, allowing retrieval and disposal of results at a later time.
"""

import uuid as uuid_module
from concurrent.futures import Future
from typing import Any, Optional, TYPE_CHECKING, Union

from hazelcast.proxy.base import Proxy, ProxyContext
from hazelcast.proxy.executor import Callable, Runnable, ExecutionCallback
from hazelcast.logging import get_logger

if TYPE_CHECKING:
    pass

_logger = get_logger("durable_executor")


class DurableExecutorServiceFuture(Future):
    """Future that holds the task ID for durable executor results.

    Extends the standard Future to include a unique task ID that can be
    used to retrieve or dispose of the result later.

    Attributes:
        task_id: The unique identifier for the submitted task.
    """

    def __init__(self, task_id: int = -1):
        super().__init__()
        self._task_id = task_id

    @property
    def task_id(self) -> int:
        """Get the unique task identifier."""
        return self._task_id

    @task_id.setter
    def task_id(self, value: int) -> None:
        """Set the unique task identifier."""
        self._task_id = value


class DurableExecutorService(Proxy):
    """Durable distributed executor service for running tasks with persistent results.

    DurableExecutorService provides a way to execute tasks (Callable or Runnable)
    on cluster members with the guarantee that task results are stored durably.
    Results can be retrieved at a later time using the unique task ID, even after
    client reconnection.

    Unlike the regular IExecutorService, the durable executor stores task results
    in a replicated manner across the cluster, making them available for retrieval
    until explicitly disposed.

    Example:
        >>> executor = client.get_durable_executor_service("my-durable-executor")
        >>>
        >>> # Submit a task and get the future with task ID
        >>> future = executor.submit(my_task)
        >>> task_id = future.task_id
        >>> result = future.result()
        >>>
        >>> # Or retrieve result later using task ID
        >>> result = executor.retrieve_result(task_id)
        >>>
        >>> # Dispose the result when no longer needed
        >>> executor.dispose_result(task_id)
    """

    def __init__(
        self,
        service_name: str,
        name: str,
        context: Optional[ProxyContext] = None,
    ):
        super().__init__(service_name, name, context)
        self._is_shutdown = False
        self._partition_count = 271

    def submit(
        self,
        task: Union[Callable, Runnable],
        callback: ExecutionCallback = None,
    ) -> DurableExecutorServiceFuture:
        """Submit a task to a random cluster member.

        The task is executed on a member determined by consistent hashing.
        The result is stored durably and can be retrieved later using
        the task ID from the returned future.

        Args:
            task: The Callable or Runnable to execute.
            callback: Optional callback for completion notification.

        Returns:
            A DurableExecutorServiceFuture containing the result and task ID.

        Raises:
            IllegalStateException: If the executor is shut down.

        Example:
            >>> future = executor.submit(my_task)
            >>> task_id = future.task_id
            >>> result = future.result(timeout=30)
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        future = DurableExecutorServiceFuture()
        task_data = self._to_data(task)

        partition_id = hash(uuid_module.uuid4()) % self._partition_count

        self._execute_on_partition(task_data, partition_id, future, callback)

        return future

    def submit_to_key_owner(
        self,
        task: Union[Callable, Runnable],
        key: Any,
        callback: ExecutionCallback = None,
    ) -> DurableExecutorServiceFuture:
        """Submit a task to the owner of the specified key.

        This is useful for data locality - the task runs on the member
        that owns the partition containing the key. The result is stored
        durably and can be retrieved later.

        Args:
            task: The Callable or Runnable to execute.
            key: The key whose owner will execute the task.
            callback: Optional callback for completion notification.

        Returns:
            A DurableExecutorServiceFuture containing the result and task ID.

        Raises:
            IllegalArgumentException: If key is None.
            IllegalStateException: If the executor is shut down.

        Example:
            >>> future = executor.submit_to_key_owner(my_task, "user:123")
            >>> result = future.result()
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        if key is None:
            from hazelcast.exceptions import IllegalArgumentException
            raise IllegalArgumentException("Key cannot be None")

        future = DurableExecutorServiceFuture()
        task_data = self._to_data(task)
        key_data = self._to_data(key)
        partition_id = self._get_partition_id(key_data)

        self._execute_on_partition(task_data, partition_id, future, callback)

        return future

    def retrieve_result(self, unique_id: int) -> Future:
        """Retrieve the result of a previously submitted task.

        Fetches the durably stored result for a task identified by
        its unique ID. The result remains stored after retrieval.

        Args:
            unique_id: The unique task identifier from a previous submission.

        Returns:
            A Future containing the task result.

        Raises:
            IllegalStateException: If the executor is shut down.
            StaleTaskIdException: If the task ID is no longer valid.

        Example:
            >>> # Submit and save task ID
            >>> future = executor.submit(my_task)
            >>> task_id = future.task_id
            >>>
            >>> # Later, retrieve the result
            >>> result_future = executor.retrieve_result(task_id)
            >>> result = result_future.result()
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        future: Future = Future()

        if self._context is None or self._context.invocation_service is None:
            future.set_result(None)
            return future

        sequence = unique_id & 0xFFFFFFFF
        partition_id = (unique_id >> 32) & 0xFFFFFFFF

        from hazelcast.protocol.codec import DurableExecutorServiceCodec
        request = DurableExecutorServiceCodec.encode_retrieve_result_request(
            self._name, partition_id, sequence
        )

        def handle_response(response):
            result_data = DurableExecutorServiceCodec.decode_retrieve_result_response(response)
            return self._to_object(result_data) if result_data else None

        invoke_future = self._invoke_on_partition(request, partition_id, handle_response)
        self._chain_future(invoke_future, future)

        return future

    def dispose_result(self, unique_id: int) -> Future:
        """Dispose the result of a previously submitted task.

        Removes the durably stored result for a task identified by
        its unique ID. After disposal, the result cannot be retrieved.

        Args:
            unique_id: The unique task identifier from a previous submission.

        Returns:
            A Future that completes when the result is disposed.

        Raises:
            IllegalStateException: If the executor is shut down.

        Example:
            >>> # Dispose result when no longer needed
            >>> dispose_future = executor.dispose_result(task_id)
            >>> dispose_future.result()  # Wait for disposal
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        future: Future = Future()

        if self._context is None or self._context.invocation_service is None:
            future.set_result(None)
            return future

        sequence = unique_id & 0xFFFFFFFF
        partition_id = (unique_id >> 32) & 0xFFFFFFFF

        from hazelcast.protocol.codec import DurableExecutorServiceCodec
        request = DurableExecutorServiceCodec.encode_dispose_result_request(
            self._name, partition_id, sequence
        )

        invoke_future = self._invoke_on_partition(request, partition_id, lambda _: None)
        self._chain_future(invoke_future, future)

        return future

    def retrieve_and_dispose_result(self, unique_id: int) -> Future:
        """Retrieve and dispose the result in a single operation.

        Fetches the durably stored result and then disposes it atomically.
        This is more efficient than calling retrieve_result() followed by
        dispose_result() when you only need to read the result once.

        Args:
            unique_id: The unique task identifier from a previous submission.

        Returns:
            A Future containing the task result.

        Raises:
            IllegalStateException: If the executor is shut down.
            StaleTaskIdException: If the task ID is no longer valid.

        Example:
            >>> result_future = executor.retrieve_and_dispose_result(task_id)
            >>> result = result_future.result()
            >>> # Result is now disposed and cannot be retrieved again
        """
        self._check_not_destroyed()
        self._check_not_shutdown()

        future: Future = Future()

        if self._context is None or self._context.invocation_service is None:
            future.set_result(None)
            return future

        sequence = unique_id & 0xFFFFFFFF
        partition_id = (unique_id >> 32) & 0xFFFFFFFF

        from hazelcast.protocol.codec import DurableExecutorServiceCodec
        request = DurableExecutorServiceCodec.encode_retrieve_and_dispose_result_request(
            self._name, partition_id, sequence
        )

        def handle_response(response):
            result_data = DurableExecutorServiceCodec.decode_retrieve_and_dispose_result_response(
                response
            )
            return self._to_object(result_data) if result_data else None

        invoke_future = self._invoke_on_partition(request, partition_id, handle_response)
        self._chain_future(invoke_future, future)

        return future

    def shutdown(self) -> None:
        """Orderly shutdown of the durable executor service.

        Previously submitted tasks are executed, but no new tasks
        will be accepted. This method does not wait for previously
        submitted tasks to complete execution. Task results that
        are already stored remain available for retrieval.
        """
        self._check_not_destroyed()
        if self._is_shutdown:
            return
        _logger.debug("Shutting down durable executor service: %s", self._name)
        self._is_shutdown = True

        if self._context is not None and self._context.invocation_service is not None:
            from hazelcast.protocol.codec import DurableExecutorServiceCodec
            request = DurableExecutorServiceCodec.encode_shutdown_request(self._name)
            self._invoke(request, lambda _: None)

    def is_shutdown(self) -> bool:
        """Check if this executor has been shut down.

        Returns:
            True if the executor has been shut down.
        """
        return self._is_shutdown

    def _check_not_shutdown(self) -> None:
        """Raise an exception if the executor is shut down."""
        if self._is_shutdown:
            from hazelcast.exceptions import IllegalStateException
            raise IllegalStateException(
                f"Durable executor service {self._name} has been shut down"
            )

    def _execute_on_partition(
        self,
        task_data: bytes,
        partition_id: int,
        future: DurableExecutorServiceFuture,
        callback: ExecutionCallback = None,
    ) -> None:
        """Execute task on a specific partition."""
        _logger.debug(
            "Executing task on partition %d for durable executor %s",
            partition_id,
            self._name,
        )

        if self._context is None or self._context.invocation_service is None:
            future.task_id = -1
            self._complete_with_callback(future, None, callback)
            return

        from hazelcast.protocol.codec import DurableExecutorServiceCodec
        request = DurableExecutorServiceCodec.encode_submit_to_partition_request(
            self._name, task_data, partition_id
        )

        def handle_response(response):
            result_data, sequence = DurableExecutorServiceCodec.decode_submit_response(response)
            unique_id = (partition_id << 32) | (sequence & 0xFFFFFFFF)
            future.task_id = unique_id
            return self._to_object(result_data) if result_data else None

        invoke_future = self._invoke_on_partition(request, partition_id, handle_response)
        self._chain_future(invoke_future, future, callback)

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


IDurableExecutorService = DurableExecutorService
