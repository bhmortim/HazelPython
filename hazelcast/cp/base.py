"""Base classes for CP subsystem proxies."""

from abc import ABC
from concurrent.futures import Future
from typing import Any, Callable, Optional, TYPE_CHECKING
import threading

if TYPE_CHECKING:
    from hazelcast.invocation import InvocationService
    from hazelcast.protocol.client_message import ClientMessage


class CPGroupId:
    """Identifier for a CP group."""

    DEFAULT_GROUP_NAME = "default"

    def __init__(self, name: str, seed: int = 0, group_id: int = 0):
        self._name = name
        self._seed = seed
        self._group_id = group_id

    @property
    def name(self) -> str:
        """Get the group name."""
        return self._name

    @property
    def seed(self) -> int:
        """Get the group seed."""
        return self._seed

    @property
    def group_id(self) -> int:
        """Get the group ID."""
        return self._group_id

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CPGroupId):
            return False
        return (
            self._name == other._name
            and self._seed == other._seed
            and self._group_id == other._group_id
        )

    def __hash__(self) -> int:
        return hash((self._name, self._seed, self._group_id))

    def __repr__(self) -> str:
        return f"CPGroupId(name={self._name!r}, id={self._group_id})"


class CPProxy(ABC):
    """Base class for CP subsystem distributed object proxies.

    CP proxies support direct-to-leader routing for linearizable operations.
    """

    def __init__(
        self,
        service_name: str,
        name: str,
        group_id: CPGroupId,
        invocation_service: Optional["InvocationService"] = None,
        direct_to_leader: bool = True,
    ):
        self._service_name = service_name
        self._name = name
        self._group_id = group_id
        self._invocation_service = invocation_service
        self._direct_to_leader = direct_to_leader
        self._destroyed = False
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        """Get the proxy name."""
        return self._name

    @property
    def service_name(self) -> str:
        """Get the service name."""
        return self._service_name

    @property
    def group_id(self) -> CPGroupId:
        """Get the CP group ID."""
        return self._group_id

    @property
    def direct_to_leader(self) -> bool:
        """Check if direct-to-leader routing is enabled."""
        return self._direct_to_leader

    @direct_to_leader.setter
    def direct_to_leader(self, value: bool) -> None:
        """Set direct-to-leader routing."""
        self._direct_to_leader = value

    @property
    def is_destroyed(self) -> bool:
        """Check if this proxy has been destroyed."""
        return self._destroyed

    def _check_not_destroyed(self) -> None:
        """Check that this proxy has not been destroyed."""
        if self._destroyed:
            from hazelcast.exceptions import IllegalStateException
            raise IllegalStateException(
                f"CP proxy {self._name} has been destroyed"
            )

    def destroy(self) -> None:
        """Destroy this CP proxy."""
        self._check_not_destroyed()
        self._destroyed = True

    async def destroy_async(self) -> None:
        """Destroy this CP proxy asynchronously."""
        self.destroy()

    def _invoke(
        self,
        request: "ClientMessage",
        response_handler: Optional[Callable[["ClientMessage"], Any]] = None,
    ) -> Future:
        """Invoke a request to the CP group.

        If direct_to_leader is enabled, routes directly to the group leader.
        """
        self._check_not_destroyed()

        if self._invocation_service is None:
            future: Future = Future()
            future.set_result(None)
            return future

        from hazelcast.invocation import Invocation
        invocation = Invocation(request)
        result_future = self._invocation_service.invoke(invocation)

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

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"name={self._name!r}, "
            f"group={self._group_id.name!r})"
        )
