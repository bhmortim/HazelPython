"""Client service for connection monitoring and management.

This module provides services for monitoring and managing client
connections to the Hazelcast cluster. It tracks connection status,
maintains connection metadata, and notifies listeners of connection
events.

Example:
    Using the client service::

        from hazelcast.service.client_service import ClientService, ConnectionStatus

        service = ClientService()

        # Check connection status
        if service.is_connected:
            print(f"Connected to {service.cluster_info.cluster_name}")

        # Get active connections
        connections = service.get_connections()
        print(f"Active connections: {len(connections)}")

        # Add connection listener
        listener = FunctionConnectionListener(
            on_opened=lambda c: print(f"Connected to {c.remote_address}"),
            on_closed=lambda c, r: print(f"Disconnected: {r}")
        )
        service.add_connection_listener(listener)
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Optional, Set, TYPE_CHECKING
import threading
import time

if TYPE_CHECKING:
    from hazelcast.network.connection import Connection
    from hazelcast.listener import MemberInfo


class ConnectionStatus(Enum):
    """Status of the client connection to the cluster.

    Attributes:
        DISCONNECTED: Not connected to any cluster member.
        CONNECTING: Attempting to establish connection.
        CONNECTED: Successfully connected to the cluster.
        RECONNECTING: Lost connection, attempting to reconnect.
    """

    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"


class ClientConnectionListener(ABC):
    """Listener for client connection events.

    Implement this interface to receive notifications when connections
    are opened or closed.
    """

    @abstractmethod
    def on_connection_opened(self, connection: "Connection") -> None:
        """Called when a new connection is established.

        Args:
            connection: The newly opened connection.
        """
        pass

    @abstractmethod
    def on_connection_closed(self, connection: "Connection", reason: str) -> None:
        """Called when a connection is closed.

        Args:
            connection: The closed connection.
            reason: Description of why the connection was closed.
        """
        pass


class FunctionConnectionListener(ClientConnectionListener):
    """Connection listener that delegates to functions.

    Convenience implementation that wraps callback functions instead
    of requiring a full class implementation.

    Args:
        on_opened: Optional callback for connection opened events.
        on_closed: Optional callback for connection closed events.

    Example:
        >>> listener = FunctionConnectionListener(
        ...     on_opened=lambda c: print(f"Connected: {c.remote_address}"),
        ...     on_closed=lambda c, r: print(f"Closed: {r}")
        ... )
    """

    def __init__(
        self,
        on_opened: Optional[Callable[["Connection"], None]] = None,
        on_closed: Optional[Callable[["Connection", str], None]] = None,
    ):
        self._on_opened = on_opened
        self._on_closed = on_closed

    def on_connection_opened(self, connection: "Connection") -> None:
        if self._on_opened:
            self._on_opened(connection)

    def on_connection_closed(self, connection: "Connection", reason: str) -> None:
        if self._on_closed:
            self._on_closed(connection, reason)


@dataclass
class ConnectionInfo:
    """Information about a connection.

    Stores metadata about an individual connection to a cluster member.

    Attributes:
        connection_id: Unique identifier for this connection.
        remote_address: Remote (host, port) tuple.
        local_address: Local (host, port) tuple.
        member_uuid: UUID of the connected member.
        connected_time: When the connection was established.
        last_read_time: Timestamp of last data read.
        last_write_time: Timestamp of last data write.
        is_alive: Whether the connection is still active.
    """

    connection_id: int
    remote_address: Optional[tuple] = None
    local_address: Optional[tuple] = None
    member_uuid: Optional[str] = None
    connected_time: float = field(default_factory=time.time)
    last_read_time: float = 0.0
    last_write_time: float = 0.0
    is_alive: bool = True


@dataclass
class ClusterInfo:
    """Information about the connected cluster.

    Stores metadata about the Hazelcast cluster the client is connected to.

    Attributes:
        cluster_uuid: Unique identifier of the cluster.
        cluster_name: Name of the cluster.
        member_count: Number of members in the cluster.
        partition_count: Number of partitions in the cluster.
        version: Cluster version string.
    """

    cluster_uuid: Optional[str] = None
    cluster_name: str = ""
    member_count: int = 0
    partition_count: int = 0
    version: str = ""


class ClientService:
    """Service for monitoring client connections and cluster state.

    Provides methods to query connection status, active connections,
    and cluster information. Also manages connection event listeners.

    Attributes:
        status: Current connection status.
        is_connected: Whether the client is connected.
        connection_count: Number of active connections.
        cluster_info: Information about the connected cluster.

    Example:
        >>> service = ClientService()
        >>> if service.is_connected:
        ...     print(f"Connected to {service.cluster_info.cluster_name}")
        ...     print(f"Active connections: {service.connection_count}")
    """

    def __init__(self):
        """Initialize the client service."""
        self._status = ConnectionStatus.DISCONNECTED
        self._connections: Dict[int, ConnectionInfo] = {}
        self._cluster_info = ClusterInfo()
        self._listeners: Dict[str, ClientConnectionListener] = {}
        self._lock = threading.RLock()
        self._last_connection_time: Optional[float] = None
        self._total_connections_opened = 0
        self._total_connections_closed = 0

    @property
    def status(self) -> ConnectionStatus:
        """Get the current connection status."""
        with self._lock:
            return self._status

    @property
    def is_connected(self) -> bool:
        """Check if the client is connected to the cluster."""
        return self._status == ConnectionStatus.CONNECTED

    @property
    def connection_count(self) -> int:
        """Get the number of active connections."""
        with self._lock:
            return len([c for c in self._connections.values() if c.is_alive])

    @property
    def cluster_info(self) -> ClusterInfo:
        """Get information about the connected cluster."""
        with self._lock:
            return self._cluster_info

    def set_status(self, status: ConnectionStatus) -> None:
        """Set the connection status.

        Args:
            status: The new connection status.
        """
        with self._lock:
            self._status = status

    def get_connections(self) -> List[ConnectionInfo]:
        """Get information about all active connections.

        Returns:
            List of connection information objects.
        """
        with self._lock:
            return [c for c in self._connections.values() if c.is_alive]

    def get_connection(self, connection_id: int) -> Optional[ConnectionInfo]:
        """Get information about a specific connection.

        Args:
            connection_id: The connection ID.

        Returns:
            Connection information, or None if not found.
        """
        with self._lock:
            return self._connections.get(connection_id)

    def add_connection(self, connection: "Connection") -> None:
        """Register a new connection.

        Updates internal state and notifies listeners.

        Args:
            connection: The connection to register.
        """
        info = ConnectionInfo(
            connection_id=connection.connection_id,
            remote_address=connection.remote_address,
            local_address=connection.local_address,
            member_uuid=connection.member_uuid,
            last_read_time=connection.last_read_time,
            last_write_time=connection.last_write_time,
            is_alive=connection.is_alive,
        )

        with self._lock:
            self._connections[connection.connection_id] = info
            self._total_connections_opened += 1
            self._last_connection_time = time.time()

            if self._status != ConnectionStatus.CONNECTED:
                self._status = ConnectionStatus.CONNECTED

        self._notify_connection_opened(connection)

    def remove_connection(self, connection: "Connection", reason: str) -> None:
        """Unregister a connection.

        Updates internal state and notifies listeners.

        Args:
            connection: The connection to unregister.
            reason: Description of why the connection was removed.
        """
        with self._lock:
            info = self._connections.get(connection.connection_id)
            if info:
                info.is_alive = False
            self._total_connections_closed += 1

            if self.connection_count == 0:
                self._status = ConnectionStatus.DISCONNECTED

        self._notify_connection_closed(connection, reason)

    def update_cluster_info(
        self,
        cluster_uuid: Optional[str] = None,
        cluster_name: Optional[str] = None,
        member_count: Optional[int] = None,
        partition_count: Optional[int] = None,
        version: Optional[str] = None,
    ) -> None:
        """Update cluster information.

        Args:
            cluster_uuid: The cluster UUID.
            cluster_name: The cluster name.
            member_count: Number of members.
            partition_count: Number of partitions.
            version: Cluster version.
        """
        with self._lock:
            if cluster_uuid is not None:
                self._cluster_info.cluster_uuid = cluster_uuid
            if cluster_name is not None:
                self._cluster_info.cluster_name = cluster_name
            if member_count is not None:
                self._cluster_info.member_count = member_count
            if partition_count is not None:
                self._cluster_info.partition_count = partition_count
            if version is not None:
                self._cluster_info.version = version

    def add_connection_listener(
        self,
        listener: ClientConnectionListener,
    ) -> str:
        """Add a connection listener.

        Args:
            listener: The listener to receive connection events.

        Returns:
            Registration ID for removing the listener later.
        """
        import uuid
        registration_id = str(uuid.uuid4())
        with self._lock:
            self._listeners[registration_id] = listener
        return registration_id

    def remove_connection_listener(self, registration_id: str) -> bool:
        """Remove a connection listener.

        Args:
            registration_id: The registration ID.

        Returns:
            True if removed, False if not found.
        """
        with self._lock:
            return self._listeners.pop(registration_id, None) is not None

    def _notify_connection_opened(self, connection: "Connection") -> None:
        """Notify listeners of a new connection."""
        with self._lock:
            listeners = list(self._listeners.values())

        for listener in listeners:
            try:
                listener.on_connection_opened(connection)
            except Exception:
                pass

    def _notify_connection_closed(
        self, connection: "Connection", reason: str
    ) -> None:
        """Notify listeners of a closed connection."""
        with self._lock:
            listeners = list(self._listeners.values())

        for listener in listeners:
            try:
                listener.on_connection_closed(connection, reason)
            except Exception:
                pass

    def get_statistics(self) -> Dict[str, any]:
        """Get connection statistics.

        Returns:
            Dictionary containing connection status, counts, and
            cluster information.
        """
        with self._lock:
            return {
                "status": self._status.value,
                "active_connections": self.connection_count,
                "total_connections_opened": self._total_connections_opened,
                "total_connections_closed": self._total_connections_closed,
                "last_connection_time": self._last_connection_time,
                "cluster_uuid": self._cluster_info.cluster_uuid,
                "cluster_name": self._cluster_info.cluster_name,
            }

    def reset(self) -> None:
        """Reset the service state.

        Clears all connections, listeners, and cluster information.
        """
        with self._lock:
            self._status = ConnectionStatus.DISCONNECTED
            self._connections.clear()
            self._cluster_info = ClusterInfo()
            self._listeners.clear()

    def __repr__(self) -> str:
        return (
            f"ClientService(status={self._status.value}, "
            f"connections={self.connection_count})"
        )
