"""Heartbeat and failure detection mechanisms."""

import asyncio
import time
from typing import Callable, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from hazelcast.network.connection import Connection


class FailureDetector:
    """Detects connection failures based on heartbeat timeouts."""

    def __init__(
        self,
        heartbeat_interval: float = 5.0,
        heartbeat_timeout: float = 60.0,
    ):
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout
        self._last_heartbeat: Dict[int, float] = {}

    @property
    def heartbeat_interval(self) -> float:
        return self._heartbeat_interval

    @heartbeat_interval.setter
    def heartbeat_interval(self, value: float) -> None:
        self._heartbeat_interval = value

    @property
    def heartbeat_timeout(self) -> float:
        return self._heartbeat_timeout

    @heartbeat_timeout.setter
    def heartbeat_timeout(self, value: float) -> None:
        self._heartbeat_timeout = value

    def register_connection(self, connection: "Connection") -> None:
        """Register a connection for failure detection."""
        self._last_heartbeat[connection.connection_id] = time.time()

    def unregister_connection(self, connection: "Connection") -> None:
        """Unregister a connection from failure detection."""
        self._last_heartbeat.pop(connection.connection_id, None)

    def on_heartbeat_received(self, connection: "Connection") -> None:
        """Record a heartbeat received from a connection."""
        self._last_heartbeat[connection.connection_id] = time.time()

    def is_alive(self, connection: "Connection") -> bool:
        """Check if a connection is considered alive.

        Args:
            connection: The connection to check.

        Returns:
            True if the connection is alive, False otherwise.
        """
        if not connection.is_alive:
            return False

        last_read = connection.last_read_time
        if last_read == 0:
            return True

        elapsed = time.time() - last_read
        return elapsed < self._heartbeat_timeout

    def needs_heartbeat(self, connection: "Connection") -> bool:
        """Check if a connection needs a heartbeat.

        Args:
            connection: The connection to check.

        Returns:
            True if a heartbeat should be sent.
        """
        if not connection.is_alive:
            return False

        last_write = connection.last_write_time
        if last_write == 0:
            return True

        elapsed = time.time() - last_write
        return elapsed >= self._heartbeat_interval

    def get_suspect_connections(
        self, connections: Dict[int, "Connection"]
    ) -> list:
        """Get connections that might be failing.

        Args:
            connections: Dictionary of connection_id to Connection.

        Returns:
            List of connections that haven't responded within timeout.
        """
        suspects = []
        current_time = time.time()

        for conn_id, connection in connections.items():
            if not connection.is_alive:
                continue

            last_read = connection.last_read_time
            if last_read > 0 and (current_time - last_read) > self._heartbeat_timeout:
                suspects.append(connection)

        return suspects


class HeartbeatManager:
    """Manages heartbeat sending and failure detection."""

    def __init__(
        self,
        failure_detector: FailureDetector,
        send_heartbeat: Callable[["Connection"], None],
        on_connection_failed: Callable[["Connection", str], None],
    ):
        self._failure_detector = failure_detector
        self._send_heartbeat = send_heartbeat
        self._on_connection_failed = on_connection_failed
        self._connections: Dict[int, "Connection"] = {}
        self._running = False
        self._task: Optional[asyncio.Task] = None

    @property
    def failure_detector(self) -> FailureDetector:
        return self._failure_detector

    def add_connection(self, connection: "Connection") -> None:
        """Add a connection to be managed."""
        self._connections[connection.connection_id] = connection
        self._failure_detector.register_connection(connection)

    def remove_connection(self, connection: "Connection") -> None:
        """Remove a connection from management."""
        self._connections.pop(connection.connection_id, None)
        self._failure_detector.unregister_connection(connection)

    def start(self) -> None:
        """Start the heartbeat manager."""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self) -> None:
        """Stop the heartbeat manager."""
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _heartbeat_loop(self) -> None:
        """Background task for sending heartbeats and checking failures."""
        interval = self._failure_detector.heartbeat_interval

        while self._running:
            try:
                await asyncio.sleep(interval)

                if not self._running:
                    break

                await self._check_connections()

            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _check_connections(self) -> None:
        """Check all connections and send heartbeats as needed."""
        connections = list(self._connections.values())

        for connection in connections:
            if not connection.is_alive:
                continue

            if not self._failure_detector.is_alive(connection):
                self._on_connection_failed(
                    connection,
                    f"Heartbeat timeout: no response for "
                    f"{self._failure_detector.heartbeat_timeout}s",
                )
                continue

            if self._failure_detector.needs_heartbeat(connection):
                try:
                    self._send_heartbeat(connection)
                except Exception:
                    pass
