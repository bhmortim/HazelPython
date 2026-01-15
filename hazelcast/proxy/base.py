"""Base proxy classes for distributed data structures."""

import logging
from abc import ABC, abstractmethod
from concurrent.futures import Future
from typing import Any, Callable, Optional, TYPE_CHECKING

from hazelcast.logging import get_logger

if TYPE_CHECKING:
    from hazelcast.invocation import InvocationService
    from hazelcast.protocol.client_message import ClientMessage

_logger = get_logger("proxy")


class DistributedObject(ABC):
    """Base interface for all distributed objects."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Get the name of this distributed object."""
        pass

    @property
    @abstractmethod
    def service_name(self) -> str:
        """Get the service name for this distributed object."""
        pass

    @abstractmethod
    def destroy(self) -> None:
        """Destroy this distributed object."""
        pass

    @abstractmethod
    async def destroy_async(self) -> None:
        """Destroy this distributed object asynchronously."""
        pass


class ProxyContext:
    """Context holding services needed by proxies."""

    def __init__(
        self,
        invocation_service: "InvocationService",
        serialization_service: Any = None,
        partition_service: Any = None,
        listener_service: Any = None,
    ):
        self._invocation_service = invocation_service
        self._serialization_service = serialization_service
        self._partition_service = partition_service
        self._listener_service = listener_service

    @property
    def invocation_service(self) -> "InvocationService":
        return self._invocation_service

    @property
    def serialization_service(self) -> Any:
        return self._serialization_service

    @property
    def partition_service(self) -> Any:
        return self._partition_service

    @property
    def listener_service(self) -> Any:
        return self._listener_service


class Proxy(DistributedObject):
    """Abstract base class for distributed object proxies."""

    def __init__(
        self,
        service_name: str,
        name: str,
        context: Optional[ProxyContext] = None,
    ):
        self._service_name = service_name
        self._name = name
        self._context = context
        self._destroyed = False
        _logger.debug("Proxy created: service=%s, name=%s", service_name, name)

    @property
    def name(self) -> str:
        return self._name

    @property
    def service_name(self) -> str:
        return self._service_name

    @property
    def is_destroyed(self) -> bool:
        return self._destroyed

    def _check_not_destroyed(self) -> None:
        """Raise an exception if this proxy has been destroyed."""
        if self._destroyed:
            from hazelcast.exceptions import IllegalStateException
            raise IllegalStateException(
                f"Distributed object {self._name} has been destroyed"
            )

    def destroy(self) -> None:
        """Destroy this distributed object."""
        self._check_not_destroyed()
        self._destroyed = True
        _logger.debug("Proxy destroyed: service=%s, name=%s", self._service_name, self._name)
        self._on_destroy()

    async def destroy_async(self) -> None:
        """Destroy this distributed object asynchronously."""
        self._check_not_destroyed()
        self._destroyed = True
        _logger.debug("Proxy destroyed: service=%s, name=%s", self._service_name, self._name)
        await self._on_destroy_async()

    def _on_destroy(self) -> None:
        """Called when the proxy is destroyed. Override in subclasses."""
        pass

    async def _on_destroy_async(self) -> None:
        """Called when the proxy is destroyed asynchronously."""
        pass

    def _invoke(
        self,
        request: "ClientMessage",
        response_handler: Optional[Callable[["ClientMessage"], Any]] = None,
    ) -> Future:
        """Invoke a request and return a future for the response.

        Args:
            request: The request message to send.
            response_handler: Optional function to process the response.

        Returns:
            A Future that will contain the result.
        """
        self._check_not_destroyed()

        if self._context is None or self._context.invocation_service is None:
            future: Future = Future()
            future.set_result(None)
            return future

        from hazelcast.invocation import Invocation
        invocation = Invocation(request)
        result_future = self._context.invocation_service.invoke(invocation)

        if response_handler is None:
            return result_future

        processed_future: Future = Future()

        def on_complete(f: Future) -> None:
            try:
                response = f.result()
                processed = response_handler(response)
                processed_future.set_result(processed)
            except Exception as e:
                processed_future.set_exception(e)

        result_future.add_done_callback(on_complete)
        return processed_future

    def _invoke_on_partition(
        self,
        request: "ClientMessage",
        partition_id: int,
        response_handler: Optional[Callable[["ClientMessage"], Any]] = None,
    ) -> Future:
        """Invoke a request on a specific partition.

        Args:
            request: The request message to send.
            partition_id: The partition ID to target.
            response_handler: Optional function to process the response.

        Returns:
            A Future that will contain the result.
        """
        self._check_not_destroyed()

        if self._context is None or self._context.invocation_service is None:
            future: Future = Future()
            future.set_result(None)
            return future

        from hazelcast.invocation import Invocation
        invocation = Invocation(request, partition_id=partition_id)
        result_future = self._context.invocation_service.invoke(invocation)

        if response_handler is None:
            return result_future

        processed_future: Future = Future()

        def on_complete(f: Future) -> None:
            try:
                response = f.result()
                processed = response_handler(response)
                processed_future.set_result(processed)
            except Exception as e:
                processed_future.set_exception(e)

        result_future.add_done_callback(on_complete)
        return processed_future

    def _to_data(self, obj: Any) -> bytes:
        """Serialize an object to binary data."""
        if self._context and self._context.serialization_service:
            return self._context.serialization_service.to_data(obj)
        if obj is None:
            return b""
        if isinstance(obj, bytes):
            return obj
        return str(obj).encode("utf-8")

    def _to_object(self, data: bytes) -> Any:
        """Deserialize binary data to an object."""
        if self._context and self._context.serialization_service:
            return self._context.serialization_service.to_object(data)
        if not data:
            return None
        return data

    def _get_partition_id(self, key: Any) -> int:
        """Get the partition ID for a key."""
        if self._context and self._context.partition_service:
            return self._context.partition_service.get_partition_id(key)
        return hash(key) % 271

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self._name!r})"

    def __str__(self) -> str:
        return self.__repr__()
