"""Heartbeat and failure detection mechanisms."""

import asyncio
import os
import platform
import socket
import struct
import time
from collections import deque
from math import exp, log, sqrt
from typing import Callable, Deque, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from hazelcast.network.connection import Connection


def _can_use_raw_sockets() -> bool:
    """Check if the current process can use raw sockets for ICMP.

    Returns:
        True if raw sockets are available, False otherwise.
    """
    if platform.system() == "Windows":
        return True

    if os.geteuid() == 0:
        return True

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)
        sock.close()
        return True
    except (PermissionError, OSError):
        pass

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_ICMP)
        sock.close()
        return True
    except (PermissionError, OSError):
        pass

    return False


def _calculate_checksum(data: bytes) -> int:
    """Calculate ICMP checksum."""
    if len(data) % 2:
        data += b'\x00'

    checksum = 0
    for i in range(0, len(data), 2):
        word = (data[i] << 8) + data[i + 1]
        checksum += word

    checksum = (checksum >> 16) + (checksum & 0xFFFF)
    checksum += checksum >> 16
    return ~checksum & 0xFFFF


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


class PingFailureDetector:
    """Failure detector using ICMP ping for host reachability checks."""

    ICMP_ECHO_REQUEST = 8
    ICMP_ECHO_REPLY = 0

    def __init__(
        self,
        ping_timeout: float = 5.0,
        ping_interval: float = 10.0,
        max_failures: int = 3,
    ):
        self._ping_timeout = ping_timeout
        self._ping_interval = ping_interval
        self._max_failures = max_failures
        self._failure_counts: Dict[str, int] = {}
        self._last_ping_times: Dict[str, float] = {}
        self._sequence_number = 0
        self._can_ping = _can_use_raw_sockets()

    @property
    def is_available(self) -> bool:
        """Check if ping-based detection is available."""
        return self._can_ping

    @property
    def ping_timeout(self) -> float:
        return self._ping_timeout

    @property
    def ping_interval(self) -> float:
        return self._ping_interval

    @property
    def max_failures(self) -> int:
        return self._max_failures

    def register_host(self, host: str) -> None:
        """Register a host for ping monitoring."""
        self._failure_counts[host] = 0
        self._last_ping_times[host] = 0.0

    def unregister_host(self, host: str) -> None:
        """Unregister a host from ping monitoring."""
        self._failure_counts.pop(host, None)
        self._last_ping_times.pop(host, None)

    def needs_ping(self, host: str) -> bool:
        """Check if a host needs to be pinged."""
        if not self._can_ping:
            return False

        last_ping = self._last_ping_times.get(host, 0.0)
        if last_ping == 0.0:
            return True

        return (time.time() - last_ping) >= self._ping_interval

    async def ping(self, host: str) -> Tuple[bool, float]:
        """Send an ICMP ping to a host.

        Args:
            host: The host to ping.

        Returns:
            Tuple of (success, latency_ms). Latency is -1 on failure.
        """
        if not self._can_ping:
            return False, -1.0

        self._last_ping_times[host] = time.time()

        try:
            return await self._send_ping(host)
        except Exception:
            self._record_failure(host)
            return False, -1.0

    async def _send_ping(self, host: str) -> Tuple[bool, float]:
        """Send ICMP echo request and wait for reply."""
        loop = asyncio.get_event_loop()

        try:
            try:
                sock = socket.socket(
                    socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP
                )
            except (PermissionError, OSError):
                sock = socket.socket(
                    socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_ICMP
                )

            sock.setblocking(False)
            sock.settimeout(self._ping_timeout)

            self._sequence_number = (self._sequence_number + 1) % 65536
            packet_id = os.getpid() & 0xFFFF
            sequence = self._sequence_number

            header = struct.pack(
                "!BBHHH",
                self.ICMP_ECHO_REQUEST,
                0,
                0,
                packet_id,
                sequence,
            )
            data = b"hazelcast-ping"
            checksum = _calculate_checksum(header + data)
            header = struct.pack(
                "!BBHHH",
                self.ICMP_ECHO_REQUEST,
                0,
                checksum,
                packet_id,
                sequence,
            )
            packet = header + data

            try:
                dest_addr = socket.gethostbyname(host)
            except socket.gaierror:
                sock.close()
                self._record_failure(host)
                return False, -1.0

            start_time = time.time()

            await loop.sock_sendto(sock, packet, (dest_addr, 0))

            try:
                response, addr = await asyncio.wait_for(
                    loop.sock_recvfrom(sock, 1024),
                    timeout=self._ping_timeout,
                )
            except asyncio.TimeoutError:
                sock.close()
                self._record_failure(host)
                return False, -1.0

            end_time = time.time()
            latency_ms = (end_time - start_time) * 1000

            sock.close()
            self._record_success(host)
            return True, latency_ms

        except Exception:
            self._record_failure(host)
            return False, -1.0

    def _record_success(self, host: str) -> None:
        """Record a successful ping."""
        self._failure_counts[host] = 0

    def _record_failure(self, host: str) -> None:
        """Record a failed ping."""
        current = self._failure_counts.get(host, 0)
        self._failure_counts[host] = current + 1

    def is_host_suspect(self, host: str) -> bool:
        """Check if a host is suspected to be failing."""
        failures = self._failure_counts.get(host, 0)
        return failures >= self._max_failures

    def get_failure_count(self, host: str) -> int:
        """Get the current failure count for a host."""
        return self._failure_counts.get(host, 0)


class PhiAccrualFailureDetector:
    """Phi Accrual Failure Detector for adaptive failure detection.

    This detector uses statistical analysis of heartbeat inter-arrival times
    to compute a suspicion level (phi) that increases over time when no
    heartbeats are received.
    """

    def __init__(
        self,
        threshold: float = 8.0,
        max_sample_size: int = 200,
        min_std_deviation_ms: float = 100.0,
        acceptable_heartbeat_pause_ms: float = 0.0,
        first_heartbeat_estimate_ms: float = 500.0,
    ):
        self._threshold = threshold
        self._max_sample_size = max_sample_size
        self._min_std_deviation_ms = min_std_deviation_ms
        self._acceptable_heartbeat_pause_ms = acceptable_heartbeat_pause_ms
        self._first_heartbeat_estimate_ms = first_heartbeat_estimate_ms

        self._heartbeat_history: Dict[int, "HeartbeatHistory"] = {}

    @property
    def threshold(self) -> float:
        return self._threshold

    @threshold.setter
    def threshold(self, value: float) -> None:
        self._threshold = value

    def register_connection(self, connection_id: int) -> None:
        """Register a connection for phi accrual detection."""
        self._heartbeat_history[connection_id] = HeartbeatHistory(
            max_sample_size=self._max_sample_size,
            first_heartbeat_estimate_ms=self._first_heartbeat_estimate_ms,
        )

    def unregister_connection(self, connection_id: int) -> None:
        """Unregister a connection from phi accrual detection."""
        self._heartbeat_history.pop(connection_id, None)

    def heartbeat(self, connection_id: int, timestamp_ms: Optional[float] = None) -> None:
        """Record a heartbeat for a connection."""
        history = self._heartbeat_history.get(connection_id)
        if history is None:
            return

        if timestamp_ms is None:
            timestamp_ms = time.time() * 1000

        history.add(timestamp_ms)

    def phi(self, connection_id: int, timestamp_ms: Optional[float] = None) -> float:
        """Calculate the phi value for a connection.

        Args:
            connection_id: The connection to check.
            timestamp_ms: Optional timestamp in milliseconds.

        Returns:
            The phi value. Higher values indicate higher suspicion.
        """
        history = self._heartbeat_history.get(connection_id)
        if history is None or history.is_empty:
            return 0.0

        if timestamp_ms is None:
            timestamp_ms = time.time() * 1000

        last_timestamp = history.last_timestamp
        if last_timestamp is None:
            return 0.0

        time_diff = timestamp_ms - last_timestamp

        mean = history.mean + self._acceptable_heartbeat_pause_ms
        std_dev = max(history.std_deviation, self._min_std_deviation_ms)

        return self._calculate_phi(time_diff, mean, std_dev)

    def _calculate_phi(self, time_diff: float, mean: float, std_dev: float) -> float:
        """Calculate phi using the cumulative distribution function."""
        if time_diff <= 0:
            return 0.0

        y = (time_diff - mean) / std_dev
        e = exp(-y * (1.5976 + 0.070566 * y * y))

        if time_diff > mean:
            return -1.0 * (log(e / (1.0 + e)) / log(10))
        else:
            return -1.0 * (log(1.0 - 1.0 / (1.0 + e)) / log(10))

    def is_available(self, connection_id: int, timestamp_ms: Optional[float] = None) -> bool:
        """Check if a connection is considered available.

        Args:
            connection_id: The connection to check.
            timestamp_ms: Optional timestamp in milliseconds.

        Returns:
            True if the connection is available (phi < threshold).
        """
        return self.phi(connection_id, timestamp_ms) < self._threshold


class HeartbeatHistory:
    """Stores heartbeat inter-arrival times for phi calculation."""

    def __init__(
        self,
        max_sample_size: int = 200,
        first_heartbeat_estimate_ms: float = 500.0,
    ):
        self._max_sample_size = max_sample_size
        self._intervals: Deque[float] = deque(maxlen=max_sample_size)
        self._last_timestamp: Optional[float] = None
        self._sum: float = 0.0
        self._squared_sum: float = 0.0

        if first_heartbeat_estimate_ms > 0:
            std_dev = first_heartbeat_estimate_ms / 4.0
            self._add_interval(first_heartbeat_estimate_ms - std_dev)
            self._add_interval(first_heartbeat_estimate_ms + std_dev)

    @property
    def is_empty(self) -> bool:
        return len(self._intervals) == 0

    @property
    def last_timestamp(self) -> Optional[float]:
        return self._last_timestamp

    @property
    def mean(self) -> float:
        if len(self._intervals) == 0:
            return 0.0
        return self._sum / len(self._intervals)

    @property
    def variance(self) -> float:
        n = len(self._intervals)
        if n == 0:
            return 0.0
        mean = self.mean
        return (self._squared_sum / n) - (mean * mean)

    @property
    def std_deviation(self) -> float:
        return sqrt(max(0.0, self.variance))

    def add(self, timestamp_ms: float) -> None:
        """Add a heartbeat timestamp."""
        if self._last_timestamp is not None:
            interval = timestamp_ms - self._last_timestamp
            if interval > 0:
                self._add_interval(interval)

        self._last_timestamp = timestamp_ms

    def _add_interval(self, interval: float) -> None:
        """Add an interval to the history."""
        if len(self._intervals) >= self._max_sample_size:
            oldest = self._intervals[0]
            self._sum -= oldest
            self._squared_sum -= oldest * oldest

        self._intervals.append(interval)
        self._sum += interval
        self._squared_sum += interval * interval


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
