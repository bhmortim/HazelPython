"""Hazelcast client implementation."""

import asyncio
import threading
import uuid
from enum import Enum
from typing import Callable, List, Optional, TYPE_CHECKING

from hazelcast.config import ClientConfig
from hazelcast.exceptions import (
    ClientOfflineException,
    IllegalStateException,
    HazelcastException,
)
from hazelcast.listener import (
    LifecycleState,
    LifecycleEvent,
    LifecycleListener,
    FunctionLifecycleListener,
    MembershipListener,
    FunctionMembershipListener,
    MembershipEvent,
    DistributedObjectListener,
    ListenerService,
)
from hazelcast.auth import AuthenticationService


class ClientState(Enum):
    """Internal client state for the state machine."""

    INITIAL = "INITIAL"
    STARTING = "STARTING"
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    RECONNECTING = "RECONNECTING"
    SHUTTING_DOWN = "SHUTTING_DOWN"
    SHUTDOWN = "SHUTDOWN"


_VALID_TRANSITIONS = {
    ClientState.INITIAL: {ClientState.STARTING, ClientState.SHUTDOWN},
    ClientState.STARTING: {ClientState.CONNECTED, ClientState.DISCONNECTED, ClientState.SHUTTING_DOWN},
    ClientState.CONNECTED: {ClientState.DISCONNECTED, ClientState.SHUTTING_DOWN},
    ClientState.DISCONNECTED: {ClientState.RECONNECTING, ClientState.SHUTTING_DOWN, ClientState.SHUTDOWN},
    ClientState.RECONNECTING: {ClientState.CONNECTED, ClientState.DISCONNECTED, ClientState.SHUTTING_DOWN},
    ClientState.SHUTTING_DOWN: {ClientState.SHUTDOWN},
    ClientState.SHUTDOWN: set(),
}


class HazelcastClient:
    """Hazelcast Python Client.

    The main entry point for connecting to a Hazelcast cluster.
    Supports both synchronous and asynchronous operations.
    """

    def __init__(self, config: ClientConfig = None):
        """Initialize the Hazelcast client.

        Args:
            config: Client configuration. If None, default configuration is used.
        """
        self._config = config or ClientConfig()
        self._state = ClientState.INITIAL
        self._state_lock = threading.Lock()

        self._client_uuid = str(uuid.uuid4())
        self._client_name = self._config.client_name or f"hz.client_{self._client_uuid[:8]}"

        self._listener_service = ListenerService()
        self._auth_service = AuthenticationService.from_config(self._config.security)

        self._connection_manager = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None

    @property
    def config(self) -> ClientConfig:
        """Get the client configuration."""
        return self._config

    @property
    def name(self) -> str:
        """Get the client name."""
        return self._client_name

    @property
    def uuid(self) -> str:
        """Get the client UUID."""
        return self._client_uuid

    @property
    def labels(self) -> List[str]:
        """Get the client labels."""
        return self._config.labels

    @property
    def state(self) -> ClientState:
        """Get the current client state."""
        with self._state_lock:
            return self._state

    @property
    def running(self) -> bool:
        """Check if the client is running and connected."""
        return self._state in (ClientState.CONNECTED, ClientState.RECONNECTING)

    @property
    def lifecycle_service(self) -> ListenerService:
        """Get the listener service for registering listeners."""
        return self._listener_service

    def _transition_state(self, new_state: ClientState) -> bool:
        """Transition to a new state if valid.

        Args:
            new_state: The target state.

        Returns:
            True if transition was successful.

        Raises:
            IllegalStateException: If the transition is not valid.
        """
        with self._state_lock:
            current = self._state
            valid_targets = _VALID_TRANSITIONS.get(current, set())

            if new_state not in valid_targets:
                raise IllegalStateException(
                    f"Invalid state transition: {current.value} -> {new_state.value}"
                )

            self._state = new_state

        lifecycle_state = self._map_to_lifecycle_state(new_state)
        if lifecycle_state:
            previous_lifecycle = self._map_to_lifecycle_state(current)
            event = LifecycleEvent(lifecycle_state, previous_lifecycle)
            self._listener_service.fire_lifecycle_event(event)

        return True

    def _map_to_lifecycle_state(self, client_state: ClientState) -> Optional[LifecycleState]:
        """Map internal client state to lifecycle state for events."""
        mapping = {
            ClientState.STARTING: LifecycleState.STARTING,
            ClientState.CONNECTED: LifecycleState.CONNECTED,
            ClientState.DISCONNECTED: LifecycleState.DISCONNECTED,
            ClientState.SHUTTING_DOWN: LifecycleState.SHUTTING_DOWN,
            ClientState.SHUTDOWN: LifecycleState.SHUTDOWN,
        }
        return mapping.get(client_state)

    def add_lifecycle_listener(
        self,
        listener: LifecycleListener = None,
        on_state_changed: Callable[[LifecycleEvent], None] = None,
    ) -> str:
        """Add a lifecycle listener.

        Args:
            listener: A LifecycleListener instance, or
            on_state_changed: A callback function for state changes.

        Returns:
            Registration ID for removing the listener.
        """
        if listener is None and on_state_changed is not None:
            listener = FunctionLifecycleListener(on_state_changed)
        elif listener is None:
            raise ValueError("Either listener or on_state_changed must be provided")

        return self._listener_service.add_lifecycle_listener(listener)

    def add_membership_listener(
        self,
        listener: MembershipListener = None,
        on_member_added: Callable[[MembershipEvent], None] = None,
        on_member_removed: Callable[[MembershipEvent], None] = None,
    ) -> str:
        """Add a membership listener.

        Args:
            listener: A MembershipListener instance, or
            on_member_added: Callback for member added events.
            on_member_removed: Callback for member removed events.

        Returns:
            Registration ID for removing the listener.
        """
        if listener is None:
            listener = FunctionMembershipListener(on_member_added, on_member_removed)

        return self._listener_service.add_membership_listener(listener)

    def add_distributed_object_listener(
        self,
        listener: DistributedObjectListener,
    ) -> str:
        """Add a distributed object listener.

        Args:
            listener: A DistributedObjectListener instance.

        Returns:
            Registration ID for removing the listener.
        """
        return self._listener_service.add_distributed_object_listener(listener)

    def remove_listener(self, registration_id: str) -> bool:
        """Remove a registered listener.

        Args:
            registration_id: The registration ID returned when adding the listener.

        Returns:
            True if the listener was removed.
        """
        return self._listener_service.remove_listener(registration_id)

    def start(self) -> "HazelcastClient":
        """Start the client and connect to the cluster synchronously.

        Returns:
            This client instance.

        Raises:
            IllegalStateException: If the client is not in INITIAL state.
            ClientOfflineException: If connection fails.
        """
        self._transition_state(ClientState.STARTING)

        try:
            self._start_internal()
            self._transition_state(ClientState.CONNECTED)
        except Exception as e:
            self._transition_state(ClientState.DISCONNECTED)
            raise ClientOfflineException(f"Failed to connect: {e}")

        return self

    def _start_internal(self) -> None:
        """Internal startup logic."""
        pass

    def connect(self) -> "HazelcastClient":
        """Alias for start() - connect to the cluster.

        Returns:
            This client instance.
        """
        return self.start()

    async def start_async(self) -> "HazelcastClient":
        """Start the client and connect to the cluster asynchronously.

        Returns:
            This client instance.
        """
        self._transition_state(ClientState.STARTING)

        try:
            await self._start_async_internal()
            self._transition_state(ClientState.CONNECTED)
        except Exception as e:
            self._transition_state(ClientState.DISCONNECTED)
            raise ClientOfflineException(f"Failed to connect: {e}")

        return self

    async def _start_async_internal(self) -> None:
        """Internal async startup logic."""
        pass

    def shutdown(self) -> None:
        """Shutdown the client and disconnect from the cluster."""
        current_state = self.state

        if current_state == ClientState.SHUTDOWN:
            return

        if current_state == ClientState.SHUTTING_DOWN:
            return

        try:
            self._transition_state(ClientState.SHUTTING_DOWN)
        except IllegalStateException:
            return

        try:
            self._shutdown_internal()
        finally:
            self._transition_state(ClientState.SHUTDOWN)
            self._listener_service.clear()

    def _shutdown_internal(self) -> None:
        """Internal shutdown logic."""
        pass

    async def shutdown_async(self) -> None:
        """Shutdown the client asynchronously."""
        current_state = self.state

        if current_state in (ClientState.SHUTDOWN, ClientState.SHUTTING_DOWN):
            return

        try:
            self._transition_state(ClientState.SHUTTING_DOWN)
        except IllegalStateException:
            return

        try:
            await self._shutdown_async_internal()
        finally:
            self._transition_state(ClientState.SHUTDOWN)
            self._listener_service.clear()

    async def _shutdown_async_internal(self) -> None:
        """Internal async shutdown logic."""
        pass

    def __enter__(self) -> "HazelcastClient":
        """Enter context manager - starts the client."""
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager - shuts down the client."""
        self.shutdown()

    async def __aenter__(self) -> "HazelcastClient":
        """Async context manager entry - starts the client."""
        return await self.start_async()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit - shuts down the client."""
        await self.shutdown_async()

    def __repr__(self) -> str:
        return (
            f"HazelcastClient(name={self._client_name!r}, "
            f"state={self._state.value}, "
            f"cluster={self._config.cluster_name!r})"
        )
