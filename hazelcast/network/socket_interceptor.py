"""Socket interceptor interface for custom socket modifications."""

import socket
from abc import ABC, abstractmethod
from typing import Optional


class SocketInterceptor(ABC):
    """Interface for intercepting socket operations.

    Implementations can modify sockets before they are used for connections,
    enabling custom configurations such as socket options, proxying, or logging.
    """

    @abstractmethod
    def intercept(self, sock: socket.socket) -> socket.socket:
        """Intercept and optionally modify a socket.

        Args:
            sock: The socket to intercept.

        Returns:
            The modified socket (may be the same instance or a new one).
        """
        pass

    def on_connect(self, sock: socket.socket, address: tuple) -> None:
        """Called after a successful connection.

        Args:
            sock: The connected socket.
            address: The (host, port) tuple of the remote address.
        """
        pass

    def on_disconnect(self, sock: socket.socket) -> None:
        """Called before a socket is closed.

        Args:
            sock: The socket being closed.
        """
        pass


class NoOpSocketInterceptor(SocketInterceptor):
    """A socket interceptor that performs no modifications."""

    def intercept(self, sock: socket.socket) -> socket.socket:
        return sock


class SocketOptionsInterceptor(SocketInterceptor):
    """Socket interceptor that sets common socket options."""

    def __init__(
        self,
        tcp_nodelay: bool = True,
        keep_alive: bool = True,
        receive_buffer_size: Optional[int] = None,
        send_buffer_size: Optional[int] = None,
    ):
        self._tcp_nodelay = tcp_nodelay
        self._keep_alive = keep_alive
        self._receive_buffer_size = receive_buffer_size
        self._send_buffer_size = send_buffer_size

    def intercept(self, sock: socket.socket) -> socket.socket:
        if self._tcp_nodelay:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        if self._keep_alive:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        if self._receive_buffer_size is not None:
            sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_RCVBUF, self._receive_buffer_size
            )

        if self._send_buffer_size is not None:
            sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_SNDBUF, self._send_buffer_size
            )

        return sock
