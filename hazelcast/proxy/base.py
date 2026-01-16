"""Base proxy classes for distributed data structures.

This module provides the foundational classes for all distributed object
proxies in the Hazelcast Python Client. These classes handle common
functionality like service invocation, serialization, and lifecycle
management.

Classes:
    DistributedObject: Abstract base interface for all distributed objects.
    ProxyContext: Container for services needed by proxies.
    Proxy: Base implementation class for distributed object proxies.

Example:
    Creating a custom proxy::

        from hazelcast.proxy.base import Proxy, ProxyContext

        class MyCustomProxy(Proxy):
            SERVICE_NAME = "hz:impl:myService"

            def __init__(self, name: str, context: ProxyContext = None):
                super().__init__(self.SERVICE_NAME, name, context)

            def my_operation(self) -> str:
                self._check_not_destroyed()
                return "result"
"""

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
    """Abstract base interface for all distributed objects.

    This interface defines the common contract for all distributed
    objects managed by the Hazelcast cluster. Every distributed data
    structure (Map, Queue, Set, etc.) implements this interface.

    Distributed objects are identified by their service name and
    object name, forming a unique key within the cluster.

    Attributes:
        name: The unique name of this distributed object within its service.
        service_name: The service identifier (e.g., "hz:impl:mapService").
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Get the name of this distributed object.

        Returns:
            The unique name identifying this object within its service.
        """
        pass

    @property
    @abstractmethod
    def service_name(self) -> str:
        """Get the service name for this distributed object.

        The service name identifies the type of distributed object
        (e.g., Map, Queue, Set).

        Returns:
            The service identifier string.
        """
        pass

    @abstractmethod
    def destroy(self) -> None:
        """Destroy this distributed object.

        Destroys this object cluster-wide. Clears all data and releases
        all resources for this object. After destruction, any operation
        on this object will raise an exception.

        Warning:
            This operation is irreversible. All data in this distributed
            object will be permanently deleted.

        Raises:
            IllegalStateException: If the object is already destroyed.
        """
        pass

    @abstractmethod
    async def destroy_async(self) -> None:
        """Destroy this distributed object asynchronously.

        Async version of :meth:`destroy`.

        Raises:
            IllegalStateException: If the object is already destroyed.
        """
        pass


class ProxyContext:
    """Context holding services needed by proxies.

    ProxyContext provides proxies with access to the client's internal
    services for request invocation, serialization, partition routing,
    and listener management.

    This class is typically created by the HazelcastClient and passed
    to proxies during their creation.

    Args:
        invocation_service: Service for sending requests to the cluster.
        serialization_service: Service for object serialization.
        partition_service: Service for partition routing.
        listener_service: Service for managing event listeners.

    Example:
        >>> context = ProxyContext(
        ...     invocation_service=inv_service,
        ...     serialization_service=ser_service,
        ...     partition_service=part_service,
        ...     listener_service=listener_service,
        ... )
    """

    def __init__(
        self,
        invocation_service: "InvocationService",
        serialization_service: Any = None,
        partition_service: Any = None,
        listener_service: Any = None,
    ):
        """Initialize the ProxyContext with required services.

        Args:
            invocation_service: Service for sending requests to the cluster.
            serialization_service: Service for object serialization.
            partition_service: Service for partition routing.
            listener_service: Service for managing event listeners.
        """
        self._invocation_service = invocation_service
        self._serialization_service = serialization_service
        self._partition_service = partition_service
        self._listener_service = listener_service

    @property
    def invocation_service(self) -> "InvocationService":
        """Get the invocation service.

        Returns:
            The service used for sending requests to cluster members.
        """
        return self._invocation_service

    @property
    def serialization_service(self) -> Any:
        """Get the serialization service.

        Returns:
            The service used for serializing and deserializing objects.
        """
        return self._serialization_service

    @property
    def partition_service(self) -> Any:
        """Get the partition service.

        Returns:
            The service used for determining partition ownership.
        """
        return self._partition_service

    @property
    def listener_service(self) -> Any:
        """Get the listener service.

        Returns:
            The service used for managing event listeners.
        """
        return self._listener_service


class Proxy(DistributedObject):
    """Abstract base class for distributed object proxies.

    Proxy is the base implementation for all distributed data structure
    proxies. It provides common functionality for service invocation,
    serialization, and lifecycle management.

    Subclasses should implement specific operations for their data
    structure type while leveraging the base class for common operations.

    Args:
        service_name: The service identifier for this proxy type.
        name: The unique name for this distributed object.
        context: The proxy context providing access to client services.

    Attributes:
        name: The name of this distributed object.
        service_name: The service identifier.
        is_destroyed: Whether this proxy has been destroyed.

    Example:
        >>> class MyProxy(Proxy):
        ...     SERVICE_NAME = "hz:impl:myService"
        ...
        ...     def my_method(self):
        ...         self._check_not_destroyed()
        ...         # Implementation
    """

    def __init__(
        self,
        service_name: str,
        name: str,
        context: Optional[ProxyContext] = None,
    ):
        """Initialize the proxy.

        Args:
            service_name: The service identifier for this proxy type.
            name: The unique name for this distributed object.
            context: The proxy context providing access to client services.
        """
        self._service_name = service_name
        self._name = name
        self._context = context
        self._destroyed = False
        _logger.debug("Proxy created: service=%s, name=%s", service_name, name)

    @property
    def name(self) -> str:
        """Get the name of this distributed object.

        Returns:
            The unique name identifying this object.
        """
        return self._name

    @property
    def service_name(self) -> str:
        """Get the service name for this proxy.

        Returns:
            The service identifier string.
        """
        return self._service_name

    @property
    def is_destroyed(self) -> bool:
        """Check if this proxy has been destroyed.

        Returns:
            True if the proxy has been destroyed, False otherwise.
        """
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
