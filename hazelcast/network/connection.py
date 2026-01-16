"""Async socket connection handling."""

import asyncio
import logging
import socket
import ssl
import struct
import time
from enum import Enum
from typing import Callable, Optional, TYPE_CHECKING

from hazelcast.protocol.client_message import ClientMessage, SIZE_OF_FRAME_LENGTH_AND_FLAGS
from hazelcast.exceptions import (
    HazelcastException,
    TargetDisconnectedException,
)
from hazelcast.logging import get_logger

_logger = get_logger("connection")

if TYPE_CHECKING:
    from hazelcast.network.address import Address
    from hazelcast.network.ssl_config import SSLConfig
    from hazelcast.network.socket_interceptor import SocketInterceptor


class ConnectionState(Enum):
    """Connection lifecycle states."""

    CREATED = "CREATED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    AUTHENTICATED = "AUTHENTICATED"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"


class Connection:
    """Represents a connection to a Hazelcast cluster member."""

    READ_BUFFER_SIZE = 8192

    def __init__(
        self,
        address: "Address",
        connection_id: int,
        connection_timeout: float = 5.0,
        ssl_config: Optional["SSLConfig"] = None,
        socket_interceptor: Optional["SocketInterceptor"] = None,
        tcp_no_delay: bool = True,
        socket_keep_alive: bool = True,
        socket_send_buffer_size: Optional[int] = None,
        socket_receive_buffer_size: Optional[int] = None,
        socket_linger_seconds: Optional[int] = None,
    ):
        self._address = address
        self._connection_id = connection_id
        self._connection_timeout = connection_timeout
        self._ssl_config = ssl_config
        self._socket_interceptor = socket_interceptor
        self._tcp_no_delay = tcp_no_delay
        self._socket_keep_alive = socket_keep_alive
        self._socket_send_buffer_size = socket_send_buffer_size
        self._socket_receive_buffer_size = socket_receive_buffer_size
        self._socket_linger_seconds = socket_linger_seconds

        self._state = ConnectionState.CREATED
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._read_buffer = bytearray()
        self._last_read_time = 0.0
        self._last_write_time = 0.0
        self._close_reason: Optional[str] = None
        self._close_cause: Optional[Exception] = None
        self._message_callback: Optional[Callable[[ClientMessage], None]] = None
        self._read_task: Optional[asyncio.Task] = None
        self._remote_address: Optional[tuple] = None
        self._local_address: Optional[tuple] = None
        self._member_uuid: Optional[str] = None

    @property
    def address(self) -> "Address":
        return self._address

    @property
    def connection_id(self) -> int:
        return self._connection_id

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def is_alive(self) -> bool:
        return self._state in (ConnectionState.CONNECTED, ConnectionState.AUTHENTICATED)

    @property
    def last_read_time(self) -> float:
        return self._last_read_time

    @property
    def last_write_time(self) -> float:
        return self._last_write_time

    @property
    def remote_address(self) -> Optional[tuple]:
        return self._remote_address

    @property
    def local_address(self) -> Optional[tuple]:
        return self._local_address

    @property
    def member_uuid(self) -> Optional[str]:
        return self._member_uuid

    @member_uuid.setter
    def member_uuid(self, value: Optional[str]) -> None:
        self._member_uuid = value

    def set_message_callback(
        self, callback: Callable[[ClientMessage], None]
    ) -> None:
        """Set the callback for received messages."""
        self._message_callback = callback

    async def connect(self) -> None:
        """Establish the connection asynchronously."""
        if self._state != ConnectionState.CREATED:
            raise HazelcastException(
                f"Cannot connect: connection is in state {self._state.value}"
            )

        self._state = ConnectionState.CONNECTING
        _logger.debug("Connection %d: connecting to %s", self._connection_id, self._address)

        try:
            resolved = self._address.resolve()
            if not resolved:
                _logger.error("Connection %d: could not resolve address %s", self._connection_id, self._address)
                raise HazelcastException(
                    f"Could not resolve address: {self._address}"
                )

            host, port = resolved[0]
            ssl_context = None

            if self._ssl_config and self._ssl_config.enabled:
                _logger.debug("Connection %d: using SSL/TLS", self._connection_id)
                ssl_context = self._ssl_config.create_ssl_context()

            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(
                    host,
                    port,
                    ssl=ssl_context,
                ),
                timeout=self._connection_timeout,
            )

            sock = self._writer.get_extra_info("socket")
            if sock:
                self._apply_socket_options(sock)
                self._remote_address = sock.getpeername()
                self._local_address = sock.getsockname()

                if self._socket_interceptor:
                    self._socket_interceptor.on_connect(sock, self._remote_address)

            self._state = ConnectionState.CONNECTED
            self._last_read_time = time.time()
            self._last_write_time = time.time()

            _logger.debug(
                "Connection %d: established to %s (local=%s)",
                self._connection_id,
                self._remote_address,
                self._local_address,
            )

        except asyncio.TimeoutError:
            self._state = ConnectionState.CLOSED
            _logger.warning(
                "Connection %d: timeout after %.1fs to %s",
                self._connection_id,
                self._connection_timeout,
                self._address,
            )
            raise HazelcastException(
                f"Connection timeout to {self._address}"
            )
        except Exception as e:
            self._state = ConnectionState.CLOSED
            _logger.error("Connection %d: failed to connect to %s: %s", self._connection_id, self._address, e)
            raise HazelcastException(
                f"Failed to connect to {self._address}: {e}"
            )

    def _apply_socket_options(self, sock: socket.socket) -> None:
        """Apply configured socket options to the socket."""
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, int(self._tcp_no_delay))
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, int(self._socket_keep_alive))

            if self._socket_send_buffer_size is not None:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self._socket_send_buffer_size)

            if self._socket_receive_buffer_size is not None:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self._socket_receive_buffer_size)

            if self._socket_linger_seconds is not None:
                sock.setsockopt(
                    socket.SOL_SOCKET,
                    socket.SO_LINGER,
                    struct.pack("ii", 1, self._socket_linger_seconds),
                )

            _logger.debug(
                "Connection %d: applied socket options (tcp_no_delay=%s, keep_alive=%s)",
                self._connection_id,
                self._tcp_no_delay,
                self._socket_keep_alive,
            )
        except OSError as e:
            _logger.warning("Connection %d: failed to set socket options: %s", self._connection_id, e)

    def start_reading(self) -> None:
        """Start the background read task."""
        if self._read_task is None and self.is_alive:
            self._read_task = asyncio.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        """Background task for reading messages."""
        _logger.debug("Connection %d: read loop started", self._connection_id)
        try:
            while self.is_alive and self._reader:
                data = await self._reader.read(self.READ_BUFFER_SIZE)
                if not data:
                    _logger.debug("Connection %d: remote closed connection", self._connection_id)
                    await self.close("Connection closed by remote")
                    break

                self._last_read_time = time.time()
                self._read_buffer.extend(data)
                await self._process_buffer()

        except asyncio.CancelledError:
            _logger.debug("Connection %d: read loop cancelled", self._connection_id)
        except Exception as e:
            _logger.error("Connection %d: read error: %s", self._connection_id, e)
            await self.close(f"Read error: {e}", e)

    async def _process_buffer(self) -> None:
        """Process accumulated data and extract complete messages."""
        while len(self._read_buffer) >= SIZE_OF_FRAME_LENGTH_AND_FLAGS:
            frame_length = struct.unpack_from("<I", self._read_buffer, 0)[0]

            if len(self._read_buffer) < frame_length:
                break

            message_end = self._find_message_end()
            if message_end is None:
                break

            message_data = bytes(self._read_buffer[:message_end])
            del self._read_buffer[:message_end]

            message = ClientMessage.from_bytes(message_data)
            if self._message_callback:
                self._message_callback(message)

    def _find_message_end(self) -> Optional[int]:
        """Find the end of a complete message in the buffer."""
        offset = 0
        while offset < len(self._read_buffer):
            if offset + SIZE_OF_FRAME_LENGTH_AND_FLAGS > len(self._read_buffer):
                return None

            frame_length = struct.unpack_from("<I", self._read_buffer, offset)[0]
            flags = struct.unpack_from("<H", self._read_buffer, offset + 4)[0]

            if offset + frame_length > len(self._read_buffer):
                return None

            offset += frame_length

            from hazelcast.protocol.client_message import END_FLAG
            if flags & END_FLAG:
                return offset

        return None

    async def send(self, message: ClientMessage) -> None:
        """Send a message over the connection.

        Args:
            message: The message to send.
        """
        if not self.is_alive:
            raise TargetDisconnectedException(
                f"Connection {self._connection_id} is not alive"
            )

        if self._writer is None:
            raise TargetDisconnectedException("Writer not initialized")

        try:
            data = message.to_bytes()
            self._writer.write(data)
            await self._writer.drain()
            self._last_write_time = time.time()
            _logger.debug(
                "Connection %d: sent message (correlation_id=%d, size=%d)",
                self._connection_id,
                message.get_correlation_id(),
                len(data),
            )
        except Exception as e:
            _logger.error("Connection %d: write error: %s", self._connection_id, e)
            await self.close(f"Write error: {e}", e)
            raise TargetDisconnectedException(f"Failed to send: {e}")

    def send_sync(self, message: ClientMessage) -> None:
        """Synchronous send (schedules async send)."""
        if asyncio.get_event_loop().is_running():
            asyncio.create_task(self.send(message))
        else:
            asyncio.get_event_loop().run_until_complete(self.send(message))

    async def close(
        self, reason: str = "Connection closed", cause: Exception = None
    ) -> None:
        """Close the connection.

        Args:
            reason: Reason for closing.
            cause: Optional exception that caused the close.
        """
        if self._state in (ConnectionState.CLOSING, ConnectionState.CLOSED):
            return

        _logger.debug("Connection %d: closing (%s)", self._connection_id, reason)
        self._state = ConnectionState.CLOSING
        self._close_reason = reason
        self._close_cause = cause

        if self._read_task:
            self._read_task.cancel()
            try:
                await self._read_task
            except asyncio.CancelledError:
                pass
            self._read_task = None

        if self._writer:
            sock = self._writer.get_extra_info("socket")
            if sock and self._socket_interceptor:
                self._socket_interceptor.on_disconnect(sock)

            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None

        self._reader = None
        self._state = ConnectionState.CLOSED

    def mark_authenticated(self) -> None:
        """Mark the connection as authenticated."""
        if self._state == ConnectionState.CONNECTED:
            self._state = ConnectionState.AUTHENTICATED

    def __str__(self) -> str:
        return (
            f"Connection[id={self._connection_id}, "
            f"address={self._address}, "
            f"state={self._state.value}]"
        )

    def __repr__(self) -> str:
        return self.__str__()
