"""Connection management with routing modes and reconnect logic."""

import asyncio
import logging
import random
import threading
import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Callable, Dict, List, Optional, TYPE_CHECKING

from hazelcast.network.address import Address, AddressHelper
from hazelcast.network.connection import Connection, ConnectionState
from hazelcast.network.failure_detector import FailureDetector, HeartbeatManager
from hazelcast.network.ssl_config import SSLConfig
from hazelcast.network.socket_interceptor import SocketInterceptor
from hazelcast.exceptions import (
    ClientOfflineException,
    HazelcastException,
    IllegalStateException,
)
from hazelcast.logging import get_logger

_logger = get_logger("connection.manager")

if TYPE_CHECKING:
    from hazelcast.config import RetryConfig, ReconnectMode


class RoutingMode(Enum):
    """Connection routing modes."""

    ALL_MEMBERS = "ALL_MEMBERS"
    SINGLE_MEMBER = "SINGLE_MEMBER"
    MULTI_MEMBER = "MULTI_MEMBER"


class LoadBalancer(ABC):
    """Interface for load balancing across connections."""

    @abstractmethod
    def init(self, connections: List[Connection]) -> None:
        """Initialize the load balancer with available connections."""
        pass

    @abstractmethod
    def next(self) -> Optional[Connection]:
        """Get the next connection to use."""
        pass

    @abstractmethod
    def can_get_next(self) -> bool:
        """Check if there is a connection available."""
        pass


class RoundRobinLoadBalancer(LoadBalancer):
    """Round-robin load balancer."""

    def __init__(self):
        self._connections: List[Connection] = []
        self._index = 0
        self._lock = threading.Lock()

    def init(self, connections: List[Connection]) -> None:
        with self._lock:
            self._connections = [c for c in connections if c.is_alive]
            self._index = 0

    def next(self) -> Optional[Connection]:
        with self._lock:
            if not self._connections:
                return None

            alive = [c for c in self._connections if c.is_alive]
            if not alive:
                return None

            self._index = self._index % len(alive)
            connection = alive[self._index]
            self._index = (self._index + 1) % len(alive)
            return connection

    def can_get_next(self) -> bool:
        with self._lock:
            return any(c.is_alive for c in self._connections)


class RandomLoadBalancer(LoadBalancer):
    """Random load balancer."""

    def __init__(self):
        self._connections: List[Connection] = []
        self._lock = threading.Lock()

    def init(self, connections: List[Connection]) -> None:
        with self._lock:
            self._connections = [c for c in connections if c.is_alive]

    def next(self) -> Optional[Connection]:
        with self._lock:
            alive = [c for c in self._connections if c.is_alive]
            if not alive:
                return None
            return random.choice(alive)

    def can_get_next(self) -> bool:
        with self._lock:
            return any(c.is_alive for c in self._connections)


class ConnectionManager:
    """Manages connections to Hazelcast cluster members."""

    def __init__(
        self,
        addresses: List[str],
        routing_mode: RoutingMode = RoutingMode.ALL_MEMBERS,
        connection_timeout: float = 5.0,
        reconnect_mode: "ReconnectMode" = None,
        retry_config: "RetryConfig" = None,
        ssl_config: Optional[SSLConfig] = None,
        socket_interceptor: Optional[SocketInterceptor] = None,
        load_balancer: Optional[LoadBalancer] = None,
        heartbeat_interval: float = 5.0,
        heartbeat_timeout: float = 60.0,
    ):
        self._addresses = AddressHelper.parse_list(addresses)
        self._routing_mode = routing_mode
        self._connection_timeout = connection_timeout
        self._reconnect_mode = reconnect_mode
        self._retry_config = retry_config
        self._ssl_config = ssl_config
        self._socket_interceptor = socket_interceptor

        self._load_balancer = load_balancer or RoundRobinLoadBalancer()
        self._connections: Dict[int, Connection] = {}
        self._address_connections: Dict[Address, Connection] = {}
        self._connection_id_counter = 0
        self._lock = threading.Lock()
        self._running = False
        self._reconnect_task: Optional[asyncio.Task] = None

        self._failure_detector = FailureDetector(
            heartbeat_interval=heartbeat_interval,
            heartbeat_timeout=heartbeat_timeout,
        )
        self._heartbeat_manager: Optional[HeartbeatManager] = None
        self._on_connection_opened: Optional[Callable[[Connection], None]] = None
        self._on_connection_closed: Optional[Callable[[Connection, str], None]] = None
        self._message_callback: Optional[Callable] = None
        self._pending_addresses: List[Address] = []

    @property
    def routing_mode(self) -> RoutingMode:
        return self._routing_mode

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def connection_count(self) -> int:
        with self._lock:
            return len([c for c in self._connections.values() if c.is_alive])

    def set_connection_listener(
        self,
        on_opened: Callable[[Connection], None],
        on_closed: Callable[[Connection, str], None],
    ) -> None:
        """Set connection lifecycle listeners."""
        self._on_connection_opened = on_opened
        self._on_connection_closed = on_closed

    def set_message_callback(self, callback: Callable) -> None:
        """Set the callback for received messages."""
        self._message_callback = callback

    async def start(self) -> None:
        """Start the connection manager and connect to the cluster."""
        if self._running:
            return

        _logger.info("Starting connection manager with routing_mode=%s", self._routing_mode.value)
        self._running = True

        self._heartbeat_manager = HeartbeatManager(
            failure_detector=self._failure_detector,
            send_heartbeat=self._send_heartbeat,
            on_connection_failed=self._on_heartbeat_failure,
        )

        await self._connect_to_cluster()

        if self.connection_count == 0:
            self._running = False
            _logger.error("Failed to connect to any cluster member")
            raise ClientOfflineException("Could not connect to any cluster member")

        _logger.info("Connected to cluster with %d connection(s)", self.connection_count)
        self._heartbeat_manager.start()

        if self._reconnect_mode and self._reconnect_mode.value != "OFF":
            _logger.debug("Starting reconnection background task")
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def shutdown(self) -> None:
        """Shutdown the connection manager."""
        _logger.info("Shutting down connection manager")
        self._running = False

        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
            self._reconnect_task = None

        if self._heartbeat_manager:
            await self._heartbeat_manager.stop()

        with self._lock:
            connections = list(self._connections.values())

        _logger.debug("Closing %d connection(s)", len(connections))
        for connection in connections:
            await connection.close("Client shutdown")

        with self._lock:
            self._connections.clear()
            self._address_connections.clear()

        _logger.info("Connection manager shutdown complete")

    async def _connect_to_cluster(self) -> None:
        """Attempt to connect to cluster members."""
        addresses = AddressHelper.get_possible_addresses(self._addresses)

        if self._routing_mode == RoutingMode.SINGLE_MEMBER:
            for address in addresses:
                try:
                    await self._connect_to_address(address)
                    return
                except Exception:
                    continue
        else:
            tasks = [self._connect_to_address(addr) for addr in addresses]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self._pending_addresses.append(addresses[i])

    async def _connect_to_address(self, address: Address) -> Connection:
        """Connect to a specific address."""
        with self._lock:
            if address in self._address_connections:
                existing = self._address_connections[address]
                if existing.is_alive:
                    _logger.debug("Reusing existing connection to %s", address)
                    return existing

            self._connection_id_counter += 1
            connection_id = self._connection_id_counter

        _logger.debug("Connecting to %s (connection_id=%d)", address, connection_id)

        connection = Connection(
            address=address,
            connection_id=connection_id,
            connection_timeout=self._connection_timeout,
            ssl_config=self._ssl_config,
            socket_interceptor=self._socket_interceptor,
        )

        if self._message_callback:
            connection.set_message_callback(self._message_callback)

        await connection.connect()

        with self._lock:
            self._connections[connection_id] = connection
            self._address_connections[address] = connection

        if self._heartbeat_manager:
            self._heartbeat_manager.add_connection(connection)

        self._load_balancer.init(list(self._connections.values()))

        _logger.info("Connection established to %s (connection_id=%d)", address, connection_id)

        if self._on_connection_opened:
            self._on_connection_opened(connection)

        connection.start_reading()
        return connection

    def get_connection(self, partition_id: int = -1) -> Optional[Connection]:
        """Get a connection for sending a request.

        Args:
            partition_id: Optional partition ID for routing.

        Returns:
            A connection or None if not available.
        """
        if self._routing_mode == RoutingMode.SINGLE_MEMBER:
            with self._lock:
                for conn in self._connections.values():
                    if conn.is_alive:
                        return conn
            return None

        return self._load_balancer.next()

    def get_connection_for_address(self, address: Address) -> Optional[Connection]:
        """Get a connection to a specific address."""
        with self._lock:
            conn = self._address_connections.get(address)
            if conn and conn.is_alive:
                return conn
        return None

    def get_all_connections(self) -> List[Connection]:
        """Get all active connections."""
        with self._lock:
            return [c for c in self._connections.values() if c.is_alive]

    async def _reconnect_loop(self) -> None:
        """Background task for reconnecting to failed members."""
        while self._running:
            try:
                backoff = self._calculate_backoff(0)
                await asyncio.sleep(backoff)

                if not self._running:
                    break

                await self._try_reconnect()

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _try_reconnect(self) -> None:
        """Attempt to reconnect to disconnected members."""
        if self._routing_mode == RoutingMode.SINGLE_MEMBER:
            if self.connection_count > 0:
                return

        addresses_to_try = list(self._pending_addresses)
        self._pending_addresses.clear()

        with self._lock:
            for address in self._addresses:
                if address not in self._address_connections:
                    addresses_to_try.append(address)
                elif not self._address_connections[address].is_alive:
                    addresses_to_try.append(address)

        attempt = 0
        for address in addresses_to_try:
            try:
                await self._connect_to_address(address)
            except Exception:
                backoff = self._calculate_backoff(attempt)
                attempt += 1
                await asyncio.sleep(min(backoff, 1.0))
                self._pending_addresses.append(address)

    def _calculate_backoff(self, attempt: int) -> float:
        """Calculate backoff duration with jitter.

        Args:
            attempt: The attempt number (0-based).

        Returns:
            Backoff duration in seconds.
        """
        if self._retry_config is None:
            return min(1.0 * (2 ** attempt), 30.0)

        initial = self._retry_config.initial_backoff
        multiplier = self._retry_config.multiplier
        max_backoff = self._retry_config.max_backoff
        jitter = self._retry_config.jitter

        backoff = initial * (multiplier ** attempt)
        backoff = min(backoff, max_backoff)

        if jitter > 0:
            jitter_amount = backoff * jitter * random.random()
            backoff = backoff + jitter_amount

        return backoff

    def _send_heartbeat(self, connection: Connection) -> None:
        """Send a heartbeat to a connection."""
        pass

    def _on_heartbeat_failure(self, connection: Connection, reason: str) -> None:
        """Handle heartbeat failure."""
        asyncio.create_task(self._close_connection(connection, reason))

    async def _close_connection(self, connection: Connection, reason: str) -> None:
        """Close a connection and notify listeners."""
        _logger.info(
            "Closing connection %d to %s: %s",
            connection.connection_id,
            connection.address,
            reason,
        )

        if self._heartbeat_manager:
            self._heartbeat_manager.remove_connection(connection)

        await connection.close(reason)

        with self._lock:
            self._connections.pop(connection.connection_id, None)
            if (
                connection.address in self._address_connections
                and self._address_connections[connection.address] is connection
            ):
                del self._address_connections[connection.address]

        self._load_balancer.init(list(self._connections.values()))

        if self._on_connection_closed:
            self._on_connection_closed(connection, reason)

        self._pending_addresses.append(connection.address)

    def on_connection_closed(self, connection: Connection, reason: str) -> None:
        """Handle connection closure (external notification)."""
        asyncio.create_task(self._close_connection(connection, reason))
