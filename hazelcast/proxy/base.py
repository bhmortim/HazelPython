"""Base classes and context for Hazelcast proxies."""

from concurrent.futures import Future
from typing import Any, Callable, Optional


class ProxyContext:
    """Context providing services needed by proxies.

    Attributes:
        invocation_service: Service for sending requests to the cluster.
        serialization_service: Service for serializing/deserializing data.
    """

    def __init__(
        self,
        invocation_service: Any = None,
        serialization_service: Any = None,
    ):
        self.invocation_service = invocation_service
        self.serialization_service = serialization_service


class Proxy:
    """Base class for all Hazelcast distributed object proxies.

    Provides common functionality for serialization, invocation,
    and lifecycle management.

    Attributes:
        service_name: The name of the Hazelcast service.
        name: The name of this distributed object.
    """

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

    @property
    def service_name(self) -> str:
        """Get the service name."""
        return self._service_name

    @property
    def name(self) -> str:
        """Get the name of this distributed object."""
        return self._name

    def _check_not_destroyed(self) -> None:
        """Check if this proxy has been destroyed."""
        if self._destroyed:
            from hazelcast.exceptions import IllegalStateException

            raise IllegalStateException(
                f"Distributed object '{self._name}' has been destroyed"
            )

    def _to_data(self, obj: Any) -> bytes:
        """Serialize an object to bytes."""
        if obj is None:
            return b""
        if isinstance(obj, bytes):
            return obj
        if self._context and self._context.serialization_service:
            return self._context.serialization_service.to_data(obj)
        return str(obj).encode("utf-8")

    def _to_object(self, data: Any) -> Any:
        """Deserialize bytes to an object."""
        if data is None:
            return None
        if isinstance(data, bytes) and len(data) == 0:
            return None
        if self._context and self._context.serialization_service:
            return self._context.serialization_service.to_object(data)
        if isinstance(data, bytes):
            try:
                return data.decode("utf-8")
            except UnicodeDecodeError:
                return data
        return data

    def _invoke(
        self,
        request: Any,
        response_handler: Optional[Callable[[Any], Any]] = None,
    ) -> Future:
        """Invoke a request on the cluster.

        Args:
            request: The request message to send.
            response_handler: Optional function to process the response.

        Returns:
            A Future containing the result.
        """
        future: Future = Future()

        if self._context and self._context.invocation_service:
            try:
                result_future = self._context.invocation_service.invoke(request)

                def handle_result(f):
                    try:
                        response = f.result()
                        if response_handler:
                            result = response_handler(response)
                        else:
                            result = response
                        future.set_result(result)
                    except Exception as e:
                        future.set_exception(e)

                result_future.add_done_callback(handle_result)
            except Exception as e:
                future.set_exception(e)
        else:
            if response_handler:
                future.set_result(response_handler(None))
            else:
                future.set_result(None)

        return future

    def destroy(self) -> None:
        """Destroy this distributed object.

        After destruction, any operation on this proxy will raise
        an IllegalStateException.
        """
        if self._destroyed:
            return

        self._destroyed = True
        self._on_destroy()

    def _on_destroy(self) -> None:
        """Called when the proxy is destroyed.

        Subclasses can override this to perform cleanup.
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self._name!r})"
