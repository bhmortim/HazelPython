"""Unit tests for network layer with mocked sockets."""

import asyncio
import socket
import ssl
import struct
import unittest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from hazelcast.network.address import Address, AddressHelper
from hazelcast.network.ssl_config import SSLConfig, SSLProtocol
from hazelcast.network.socket_interceptor import (
    SocketInterceptor,
    NoOpSocketInterceptor,
    SocketOptionsInterceptor,
)
from hazelcast.network.connection import Connection, ConnectionState
from hazelcast.network.failure_detector import FailureDetector, HeartbeatManager
from hazelcast.network.connection_manager import (
    ConnectionManager,
    RoutingMode,
    LoadBalancer,
    RoundRobinLoadBalancer,
    RandomLoadBalancer,
)


class TestAddress(unittest.TestCase):
    """Tests for Address class."""

    def test_default_port(self):
        addr = Address("localhost")
        self.assertEqual(addr.host, "localhost")
        self.assertEqual(addr.port, 5701)

    def test_custom_port(self):
        addr = Address("192.168.1.1", 5702)
        self.assertEqual(addr.host, "192.168.1.1")
        self.assertEqual(addr.port, 5702)

    def test_str_representation(self):
        addr = Address("localhost", 5701)
        self.assertEqual(str(addr), "localhost:5701")

    def test_equality(self):
        addr1 = Address("localhost", 5701)
        addr2 = Address("localhost", 5701)
        addr3 = Address("localhost", 5702)
        self.assertEqual(addr1, addr2)
        self.assertNotEqual(addr1, addr3)

    def test_hash(self):
        addr1 = Address("localhost", 5701)
        addr2 = Address("localhost", 5701)
        self.assertEqual(hash(addr1), hash(addr2))

    @patch("socket.getaddrinfo")
    def test_resolve(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 5701)),
            (socket.AF_INET6, socket.SOCK_STREAM, 0, "", ("::1", 5701)),
        ]
        addr = Address("localhost", 5701)
        resolved = addr.resolve()
        self.assertEqual(len(resolved), 2)
        self.assertIn(("127.0.0.1", 5701), resolved)

    def test_invalidate_cache(self):
        addr = Address("localhost", 5701)
        addr._resolved_addresses = [("127.0.0.1", 5701)]
        addr.invalidate_cache()
        self.assertIsNone(addr._resolved_addresses)


class TestAddressHelper(unittest.TestCase):
    """Tests for AddressHelper class."""

    def test_parse_simple_host(self):
        addr = AddressHelper.parse("localhost")
        self.assertEqual(addr.host, "localhost")
        self.assertEqual(addr.port, 5701)

    def test_parse_host_port(self):
        addr = AddressHelper.parse("localhost:5702")
        self.assertEqual(addr.host, "localhost")
        self.assertEqual(addr.port, 5702)

    def test_parse_ipv6_with_brackets(self):
        addr = AddressHelper.parse("[::1]:5703")
        self.assertEqual(addr.host, "::1")
        self.assertEqual(addr.port, 5703)

    def test_parse_ipv6_without_port(self):
        addr = AddressHelper.parse("[::1]")
        self.assertEqual(addr.host, "::1")
        self.assertEqual(addr.port, 5701)

    def test_parse_list(self):
        addresses = AddressHelper.parse_list(["host1:5701", "host2:5702"])
        self.assertEqual(len(addresses), 2)
        self.assertEqual(addresses[0].host, "host1")
        self.assertEqual(addresses[1].port, 5702)

    def test_get_possible_addresses(self):
        addresses = [Address("localhost", 5701)]
        expanded = AddressHelper.get_possible_addresses(addresses, port_range=3)
        self.assertEqual(len(expanded), 3)
        ports = [a.port for a in expanded]
        self.assertIn(5701, ports)
        self.assertIn(5702, ports)
        self.assertIn(5703, ports)


class TestSSLConfig(unittest.TestCase):
    """Tests for SSLConfig class."""

    def test_default_config(self):
        config = SSLConfig()
        self.assertFalse(config.enabled)
        self.assertEqual(config.protocol, SSLProtocol.TLS)
        self.assertTrue(config.check_hostname)
        self.assertEqual(config.verify_mode, ssl.CERT_REQUIRED)

    def test_enabled_config(self):
        config = SSLConfig(enabled=True)
        self.assertTrue(config.enabled)

    def test_create_ssl_context(self):
        config = SSLConfig(
            enabled=True,
            check_hostname=False,
            verify_mode=ssl.CERT_NONE,
        )
        context = config.create_ssl_context()
        self.assertIsInstance(context, ssl.SSLContext)
        self.assertFalse(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_NONE)

    def test_from_dict(self):
        data = {
            "enabled": True,
            "protocol": "TLSv1.2",
            "check_hostname": False,
            "verify_mode": "CERT_NONE",
        }
        config = SSLConfig.from_dict(data)
        self.assertTrue(config.enabled)
        self.assertEqual(config.protocol, SSLProtocol.TLSv1_2)
        self.assertFalse(config.check_hostname)
        self.assertEqual(config.verify_mode, ssl.CERT_NONE)


class TestSocketInterceptor(unittest.TestCase):
    """Tests for SocketInterceptor classes."""

    def test_noop_interceptor(self):
        interceptor = NoOpSocketInterceptor()
        mock_socket = MagicMock()
        result = interceptor.intercept(mock_socket)
        self.assertEqual(result, mock_socket)

    def test_socket_options_interceptor(self):
        interceptor = SocketOptionsInterceptor(
            tcp_nodelay=True,
            keep_alive=True,
            receive_buffer_size=65536,
            send_buffer_size=65536,
        )
        mock_socket = MagicMock()
        result = interceptor.intercept(mock_socket)
        self.assertEqual(result, mock_socket)
        mock_socket.setsockopt.assert_any_call(
            socket.IPPROTO_TCP, socket.TCP_NODELAY, 1
        )
        mock_socket.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1
        )


class TestConnectionState(unittest.TestCase):
    """Tests for ConnectionState enum."""

    def test_states(self):
        self.assertEqual(ConnectionState.CREATED.value, "CREATED")
        self.assertEqual(ConnectionState.CONNECTING.value, "CONNECTING")
        self.assertEqual(ConnectionState.CONNECTED.value, "CONNECTED")
        self.assertEqual(ConnectionState.AUTHENTICATED.value, "AUTHENTICATED")
        self.assertEqual(ConnectionState.CLOSING.value, "CLOSING")
        self.assertEqual(ConnectionState.CLOSED.value, "CLOSED")


class TestConnection(unittest.TestCase):
    """Tests for Connection class."""

    def test_initial_state(self):
        addr = Address("localhost", 5701)
        conn = Connection(addr, connection_id=1)
        self.assertEqual(conn.state, ConnectionState.CREATED)
        self.assertEqual(conn.connection_id, 1)
        self.assertFalse(conn.is_alive)

    def test_str_representation(self):
        addr = Address("localhost", 5701)
        conn = Connection(addr, connection_id=1)
        result = str(conn)
        self.assertIn("Connection", result)
        self.assertIn("id=1", result)
        self.assertIn("localhost:5701", result)


class TestConnectionAsync(unittest.IsolatedAsyncioTestCase):
    """Async tests for Connection class."""

    async def test_connect_failure(self):
        addr = Address("nonexistent.invalid", 5701)
        conn = Connection(addr, connection_id=1, connection_timeout=0.1)

        with self.assertRaises(Exception):
            await conn.connect()

        self.assertEqual(conn.state, ConnectionState.CLOSED)

    @patch("asyncio.open_connection")
    async def test_connect_success(self, mock_open_connection):
        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_writer.get_extra_info = MagicMock(return_value=None)
        mock_open_connection.return_value = (mock_reader, mock_writer)

        addr = Address("localhost", 5701)
        conn = Connection(addr, connection_id=1)

        await conn.connect()

        self.assertEqual(conn.state, ConnectionState.CONNECTED)
        self.assertTrue(conn.is_alive)

    @patch("asyncio.open_connection")
    async def test_close(self, mock_open_connection):
        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_writer.get_extra_info = MagicMock(return_value=None)
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        mock_open_connection.return_value = (mock_reader, mock_writer)

        addr = Address("localhost", 5701)
        conn = Connection(addr, connection_id=1)
        await conn.connect()
        await conn.close("Test close")

        self.assertEqual(conn.state, ConnectionState.CLOSED)
        self.assertFalse(conn.is_alive)


class TestFailureDetector(unittest.TestCase):
    """Tests for FailureDetector class."""

    def test_default_config(self):
        detector = FailureDetector()
        self.assertEqual(detector.heartbeat_interval, 5.0)
        self.assertEqual(detector.heartbeat_timeout, 60.0)

    def test_custom_config(self):
        detector = FailureDetector(
            heartbeat_interval=10.0,
            heartbeat_timeout=120.0,
        )
        self.assertEqual(detector.heartbeat_interval, 10.0)
        self.assertEqual(detector.heartbeat_timeout, 120.0)

    def test_register_connection(self):
        detector = FailureDetector()
        mock_conn = MagicMock()
        mock_conn.connection_id = 1
        detector.register_connection(mock_conn)
        self.assertIn(1, detector._last_heartbeat)

    def test_unregister_connection(self):
        detector = FailureDetector()
        mock_conn = MagicMock()
        mock_conn.connection_id = 1
        detector.register_connection(mock_conn)
        detector.unregister_connection(mock_conn)
        self.assertNotIn(1, detector._last_heartbeat)


class TestLoadBalancer(unittest.TestCase):
    """Tests for LoadBalancer implementations."""

    def test_round_robin_empty(self):
        lb = RoundRobinLoadBalancer()
        lb.init([])
        self.assertIsNone(lb.next())
        self.assertFalse(lb.can_get_next())

    def test_round_robin_single(self):
        lb = RoundRobinLoadBalancer()
        mock_conn = MagicMock()
        mock_conn.is_alive = True
        lb.init([mock_conn])
        self.assertEqual(lb.next(), mock_conn)
        self.assertTrue(lb.can_get_next())

    def test_round_robin_multiple(self):
        lb = RoundRobinLoadBalancer()
        conn1 = MagicMock()
        conn1.is_alive = True
        conn2 = MagicMock()
        conn2.is_alive = True
        lb.init([conn1, conn2])

        first = lb.next()
        second = lb.next()
        self.assertIn(first, [conn1, conn2])
        self.assertIn(second, [conn1, conn2])

    def test_random_empty(self):
        lb = RandomLoadBalancer()
        lb.init([])
        self.assertIsNone(lb.next())
        self.assertFalse(lb.can_get_next())

    def test_random_single(self):
        lb = RandomLoadBalancer()
        mock_conn = MagicMock()
        mock_conn.is_alive = True
        lb.init([mock_conn])
        self.assertEqual(lb.next(), mock_conn)
        self.assertTrue(lb.can_get_next())


class TestRoutingMode(unittest.TestCase):
    """Tests for RoutingMode enum."""

    def test_modes(self):
        self.assertEqual(RoutingMode.ALL_MEMBERS.value, "ALL_MEMBERS")
        self.assertEqual(RoutingMode.SINGLE_MEMBER.value, "SINGLE_MEMBER")
        self.assertEqual(RoutingMode.MULTI_MEMBER.value, "MULTI_MEMBER")


class TestConnectionManager(unittest.TestCase):
    """Tests for ConnectionManager class."""

    def test_initial_state(self):
        manager = ConnectionManager(addresses=["localhost:5701"])
        self.assertFalse(manager.is_running)
        self.assertEqual(manager.connection_count, 0)
        self.assertEqual(manager.routing_mode, RoutingMode.ALL_MEMBERS)

    def test_custom_routing_mode(self):
        manager = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )
        self.assertEqual(manager.routing_mode, RoutingMode.SINGLE_MEMBER)

    def test_get_connection_no_connections(self):
        manager = ConnectionManager(addresses=["localhost:5701"])
        self.assertIsNone(manager.get_connection())

    def test_get_all_connections_empty(self):
        manager = ConnectionManager(addresses=["localhost:5701"])
        self.assertEqual(manager.get_all_connections(), [])


class TestConnectionManagerAsync(unittest.IsolatedAsyncioTestCase):
    """Async tests for ConnectionManager class."""

    async def test_shutdown_without_start(self):
        manager = ConnectionManager(addresses=["localhost:5701"])
        await manager.shutdown()
        self.assertFalse(manager.is_running)

    @patch("hazelcast.network.connection.Connection.connect")
    async def test_start_success(self, mock_connect):
        mock_connect.return_value = None

        manager = ConnectionManager(
            addresses=["localhost:5701"],
            connection_timeout=0.1,
        )

        with patch.object(
            Connection, "is_alive", new_callable=PropertyMock
        ) as mock_alive:
            mock_alive.return_value = True
            await manager.start()

        self.assertTrue(manager.is_running)
        await manager.shutdown()


class TestBackoffCalculation(unittest.TestCase):
    """Tests for backoff calculation logic."""

    def test_default_backoff(self):
        manager = ConnectionManager(addresses=["localhost:5701"])
        backoff = manager._calculate_backoff(0)
        self.assertGreater(backoff, 0)
        self.assertLessEqual(backoff, 30.0)

    def test_exponential_growth(self):
        manager = ConnectionManager(addresses=["localhost:5701"])
        backoff0 = manager._calculate_backoff(0)
        backoff1 = manager._calculate_backoff(1)
        backoff2 = manager._calculate_backoff(2)
        self.assertLess(backoff0, backoff1)
        self.assertLess(backoff1, backoff2)

    def test_max_backoff_cap(self):
        manager = ConnectionManager(addresses=["localhost:5701"])
        backoff = manager._calculate_backoff(100)
        self.assertLessEqual(backoff, 30.0)


if __name__ == "__main__":
    unittest.main()
