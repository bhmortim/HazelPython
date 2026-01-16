"""Unit tests for hazelcast.network.connection module."""

import asyncio
import socket
import struct
from unittest.mock import Mock, MagicMock, patch, AsyncMock

import pytest

from hazelcast.network.connection import Connection, ConnectionState


class MockAddress:
    """Mock Address class for testing."""

    def __init__(self, host: str = "127.0.0.1", port: int = 5701):
        self._host = host
        self._port = port

    def resolve(self):
        return [(self._host, self._port)]

    def __str__(self):
        return f"{self._host}:{self._port}"


class TestConnectionSocketOptions:
    """Tests for Connection socket options."""

    def test_default_socket_options(self):
        """Test default socket option values."""
        address = MockAddress()
        conn = Connection(address, connection_id=1)

        assert conn._tcp_no_delay is True
        assert conn._socket_keep_alive is True
        assert conn._socket_send_buffer_size is None
        assert conn._socket_receive_buffer_size is None
        assert conn._socket_linger_seconds is None

    def test_custom_socket_options(self):
        """Test custom socket option configuration."""
        address = MockAddress()
        conn = Connection(
            address,
            connection_id=1,
            tcp_no_delay=False,
            socket_keep_alive=False,
            socket_send_buffer_size=65536,
            socket_receive_buffer_size=131072,
            socket_linger_seconds=5,
        )

        assert conn._tcp_no_delay is False
        assert conn._socket_keep_alive is False
        assert conn._socket_send_buffer_size == 65536
        assert conn._socket_receive_buffer_size == 131072
        assert conn._socket_linger_seconds == 5

    def test_apply_socket_options_basic(self):
        """Test applying basic socket options."""
        address = MockAddress()
        conn = Connection(
            address,
            connection_id=1,
            tcp_no_delay=True,
            socket_keep_alive=True,
        )

        mock_socket = Mock(spec=socket.socket)
        conn._apply_socket_options(mock_socket)

        mock_socket.setsockopt.assert_any_call(
            socket.IPPROTO_TCP, socket.TCP_NODELAY, 1
        )
        mock_socket.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1
        )

    def test_apply_socket_options_disabled(self):
        """Test applying disabled socket options."""
        address = MockAddress()
        conn = Connection(
            address,
            connection_id=1,
            tcp_no_delay=False,
            socket_keep_alive=False,
        )

        mock_socket = Mock(spec=socket.socket)
        conn._apply_socket_options(mock_socket)

        mock_socket.setsockopt.assert_any_call(
            socket.IPPROTO_TCP, socket.TCP_NODELAY, 0
        )
        mock_socket.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_KEEPALIVE, 0
        )

    def test_apply_socket_options_buffer_sizes(self):
        """Test applying buffer size socket options."""
        address = MockAddress()
        conn = Connection(
            address,
            connection_id=1,
            socket_send_buffer_size=65536,
            socket_receive_buffer_size=131072,
        )

        mock_socket = Mock(spec=socket.socket)
        conn._apply_socket_options(mock_socket)

        mock_socket.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_SNDBUF, 65536
        )
        mock_socket.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_RCVBUF, 131072
        )

    def test_apply_socket_options_no_buffer_sizes(self):
        """Test that buffer sizes are not set when None."""
        address = MockAddress()
        conn = Connection(
            address,
            connection_id=1,
            socket_send_buffer_size=None,
            socket_receive_buffer_size=None,
        )

        mock_socket = Mock(spec=socket.socket)
        conn._apply_socket_options(mock_socket)

        calls = mock_socket.setsockopt.call_args_list
        for call in calls:
            args = call[0]
            assert args[1] not in (socket.SO_SNDBUF, socket.SO_RCVBUF)

    def test_apply_socket_options_linger(self):
        """Test applying SO_LINGER socket option."""
        address = MockAddress()
        conn = Connection(
            address,
            connection_id=1,
            socket_linger_seconds=5,
        )

        mock_socket = Mock(spec=socket.socket)
        conn._apply_socket_options(mock_socket)

        expected_linger = struct.pack("ii", 1, 5)
        mock_socket.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_LINGER, expected_linger
        )

    def test_apply_socket_options_linger_zero(self):
        """Test applying SO_LINGER with zero seconds (immediate close)."""
        address = MockAddress()
        conn = Connection(
            address,
            connection_id=1,
            socket_linger_seconds=0,
        )

        mock_socket = Mock(spec=socket.socket)
        conn._apply_socket_options(mock_socket)

        expected_linger = struct.pack("ii", 1, 0)
        mock_socket.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_LINGER, expected_linger
        )

    def test_apply_socket_options_no_linger(self):
        """Test that SO_LINGER is not set when None."""
        address = MockAddress()
        conn = Connection(
            address,
            connection_id=1,
            socket_linger_seconds=None,
        )

        mock_socket = Mock(spec=socket.socket)
        conn._apply_socket_options(mock_socket)

        calls = mock_socket.setsockopt.call_args_list
        for call in calls:
            args = call[0]
            assert args[1] != socket.SO_LINGER

    def test_apply_socket_options_handles_os_error(self):
        """Test that OSError during socket option application is handled."""
        address = MockAddress()
        conn = Connection(address, connection_id=1)

        mock_socket = Mock(spec=socket.socket)
        mock_socket.setsockopt.side_effect = OSError("Permission denied")

        conn._apply_socket_options(mock_socket)

    @pytest.mark.asyncio
    async def test_connect_applies_socket_options(self):
        """Test that connect() applies socket options."""
        address = MockAddress()
        conn = Connection(
            address,
            connection_id=1,
            tcp_no_delay=True,
            socket_keep_alive=True,
            socket_send_buffer_size=32768,
        )

        mock_socket = Mock(spec=socket.socket)
        mock_socket.getpeername.return_value = ("127.0.0.1", 5701)
        mock_socket.getsockname.return_value = ("127.0.0.1", 54321)

        mock_writer = Mock()
        mock_writer.get_extra_info.return_value = mock_socket

        mock_reader = Mock()

        with patch("asyncio.open_connection", new_callable=AsyncMock) as mock_open:
            with patch("asyncio.wait_for", new_callable=AsyncMock) as mock_wait:
                mock_wait.return_value = (mock_reader, mock_writer)

                await conn.connect()

                mock_socket.setsockopt.assert_any_call(
                    socket.IPPROTO_TCP, socket.TCP_NODELAY, 1
                )
                mock_socket.setsockopt.assert_any_call(
                    socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1
                )
                mock_socket.setsockopt.assert_any_call(
                    socket.SOL_SOCKET, socket.SO_SNDBUF, 32768
                )


class TestConnectionState:
    """Tests for Connection state management."""

    def test_initial_state(self):
        """Test initial connection state."""
        address = MockAddress()
        conn = Connection(address, connection_id=1)
        assert conn.state == ConnectionState.CREATED
        assert conn.is_alive is False

    def test_connection_properties(self):
        """Test connection property accessors."""
        address = MockAddress()
        conn = Connection(
            address,
            connection_id=42,
            connection_timeout=10.0,
        )

        assert conn.address == address
        assert conn.connection_id == 42
        assert conn._connection_timeout == 10.0
