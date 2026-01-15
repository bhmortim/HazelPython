"""Unit tests for hazelcast.network module."""

import pytest
import socket
import ssl
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

from hazelcast.network.address import Address, AddressHelper
from hazelcast.network.ssl_config import SSLConfig, SSLProtocol
from hazelcast.network.socket_interceptor import (
    SocketInterceptor,
    NoOpSocketInterceptor,
    SocketOptionsInterceptor,
)
from hazelcast.network.failure_detector import (
    FailureDetector,
    PingFailureDetector,
    PhiAccrualFailureDetector,
    HeartbeatHistory,
)
from hazelcast.network.connection import Connection, ConnectionState


class TestAddress:
    """Tests for Address class."""

    def test_init_default_port(self):
        addr = Address("localhost")
        assert addr.host == "localhost"
        assert addr.port == 5701

    def test_init_custom_port(self):
        addr = Address("192.168.1.1", 5702)
        assert addr.host == "192.168.1.1"
        assert addr.port == 5702

    def test_str(self):
        addr = Address("localhost", 5701)
        assert str(addr) == "localhost:5701"

    def test_repr(self):
        addr = Address("localhost", 5701)
        assert repr(addr) == "Address('localhost', 5701)"

    def test_equality(self):
        addr1 = Address("localhost", 5701)
        addr2 = Address("localhost", 5701)
        addr3 = Address("localhost", 5702)
        addr4 = Address("other", 5701)
        assert addr1 == addr2
        assert addr1 != addr3
        assert addr1 != addr4
        assert addr1 != "not an address"

    def test_hash(self):
        addr1 = Address("localhost", 5701)
        addr2 = Address("localhost", 5701)
        assert hash(addr1) == hash(addr2)
        addresses = {addr1, addr2}
        assert len(addresses) == 1

    def test_resolve_localhost(self):
        addr = Address("localhost", 5701)
        resolved = addr.resolve()
        assert len(resolved) > 0
        assert all(r[1] == 5701 for r in resolved)

    def test_resolve_caches_result(self):
        addr = Address("localhost", 5701)
        resolved1 = addr.resolve()
        resolved2 = addr.resolve()
        assert resolved1 is resolved2

    def test_invalidate_cache(self):
        addr = Address("localhost", 5701)
        addr.resolve()
        addr.invalidate_cache()
        assert addr._resolved_addresses is None

    def test_resolve_invalid_host(self):
        addr = Address("invalid.host.that.does.not.exist.example", 5701)
        resolved = addr.resolve()
        assert resolved == [("invalid.host.that.does.not.exist.example", 5701)]


class TestAddressHelper:
    """Tests for AddressHelper class."""

    def test_parse_host_only(self):
        addr = AddressHelper.parse("localhost")
        assert addr.host == "localhost"
        assert addr.port == Address.DEFAULT_PORT

    def test_parse_host_port(self):
        addr = AddressHelper.parse("localhost:5702")
        assert addr.host == "localhost"
        assert addr.port == 5702

    def test_parse_with_whitespace(self):
        addr = AddressHelper.parse("  localhost:5701  ")
        assert addr.host == "localhost"
        assert addr.port == 5701

    def test_parse_ipv6_bracket(self):
        addr = AddressHelper.parse("[::1]:5701")
        assert addr.host == "::1"
        assert addr.port == 5701

    def test_parse_ipv6_bracket_no_port(self):
        addr = AddressHelper.parse("[::1]")
        assert addr.host == "::1"
        assert addr.port == Address.DEFAULT_PORT

    def test_parse_ipv6_no_bracket(self):
        addr = AddressHelper.parse("::1")
        assert addr.host == "::1"
        assert addr.port == Address.DEFAULT_PORT

    def test_parse_invalid_port(self):
        addr = AddressHelper.parse("localhost:invalid")
        assert addr.host == "localhost:invalid"
        assert addr.port == Address.DEFAULT_PORT

    def test_parse_list(self):
        addresses = AddressHelper.parse_list(["host1:5701", "host2:5702", "host3"])
        assert len(addresses) == 3
        assert addresses[0].host == "host1"
        assert addresses[0].port == 5701
        assert addresses[1].host == "host2"
        assert addresses[1].port == 5702
        assert addresses[2].host == "host3"
        assert addresses[2].port == 5701

    def test_get_possible_addresses(self):
        base = [Address("localhost", 5701)]
        possible = AddressHelper.get_possible_addresses(base, port_range=3)
        assert len(possible) == 3
        assert Address("localhost", 5701) in possible
        assert Address("localhost", 5702) in possible
        assert Address("localhost", 5703) in possible

    def test_get_possible_addresses_deduplication(self):
        base = [Address("localhost", 5701), Address("localhost", 5702)]
        possible = AddressHelper.get_possible_addresses(base, port_range=2)
        hosts = [a.host for a in possible]
        ports = [a.port for a in possible]
        assert len(set(zip(hosts, ports))) == len(possible)


class TestSSLConfig:
    """Tests for SSLConfig class."""

    def test_default_values(self):
        config = SSLConfig()
        assert config.enabled is False
        assert config.protocol == SSLProtocol.TLS
        assert config.check_hostname is True
        assert config.verify_mode == ssl.CERT_REQUIRED

    def test_enabled(self):
        config = SSLConfig(enabled=True)
        assert config.enabled is True

    def test_all_properties(self):
        config = SSLConfig(
            enabled=True,
            cafile="/path/to/ca.crt",
            capath="/path/to/certs",
            certfile="/path/to/cert.crt",
            keyfile="/path/to/key.pem",
            keyfile_password="secret",
            protocol=SSLProtocol.TLSv1_2,
            check_hostname=False,
            verify_mode=ssl.CERT_OPTIONAL,
            ciphers="HIGH:!aNULL",
        )
        assert config.cafile == "/path/to/ca.crt"
        assert config.capath == "/path/to/certs"
        assert config.certfile == "/path/to/cert.crt"
        assert config.keyfile == "/path/to/key.pem"
        assert config.keyfile_password == "secret"
        assert config.protocol == SSLProtocol.TLSv1_2
        assert config.check_hostname is False
        assert config.verify_mode == ssl.CERT_OPTIONAL
        assert config.ciphers == "HIGH:!aNULL"

    def test_setters(self):
        config = SSLConfig()
        config.enabled = True
        config.cafile = "/ca.crt"
        config.protocol = SSLProtocol.TLSv1_3
        assert config.enabled is True
        assert config.cafile == "/ca.crt"
        assert config.protocol == SSLProtocol.TLSv1_3

    def test_create_ssl_context(self):
        config = SSLConfig(enabled=True, verify_mode=ssl.CERT_NONE)
        config.check_hostname = False
        context = config.create_ssl_context()
        assert isinstance(context, ssl.SSLContext)

    def test_from_dict(self):
        data = {
            "enabled": True,
            "protocol": "TLSv1.2",
            "check_hostname": False,
            "verify_mode": "CERT_NONE",
        }
        config = SSLConfig.from_dict(data)
        assert config.enabled is True
        assert config.protocol == SSLProtocol.TLSv1_2
        assert config.check_hostname is False
        assert config.verify_mode == ssl.CERT_NONE

    def test_from_dict_invalid_protocol(self):
        data = {"protocol": "INVALID"}
        config = SSLConfig.from_dict(data)
        assert config.protocol == SSLProtocol.TLS


class TestSSLProtocol:
    """Tests for SSLProtocol enum."""

    def test_values(self):
        assert SSLProtocol.TLS.value == "TLS"
        assert SSLProtocol.TLSv1_2.value == "TLSv1.2"
        assert SSLProtocol.TLSv1_3.value == "TLSv1.3"


class TestNoOpSocketInterceptor:
    """Tests for NoOpSocketInterceptor."""

    def test_intercept_returns_same_socket(self):
        interceptor = NoOpSocketInterceptor()
        mock_socket = MagicMock(spec=socket.socket)
        result = interceptor.intercept(mock_socket)
        assert result is mock_socket


class TestSocketOptionsInterceptor:
    """Tests for SocketOptionsInterceptor."""

    def test_default_options(self):
        interceptor = SocketOptionsInterceptor()
        mock_socket = MagicMock(spec=socket.socket)
        result = interceptor.intercept(mock_socket)
        assert result is mock_socket
        mock_socket.setsockopt.assert_any_call(
            socket.IPPROTO_TCP, socket.TCP_NODELAY, 1
        )
        mock_socket.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1
        )

    def test_custom_buffer_sizes(self):
        interceptor = SocketOptionsInterceptor(
            receive_buffer_size=65536,
            send_buffer_size=32768,
        )
        mock_socket = MagicMock(spec=socket.socket)
        interceptor.intercept(mock_socket)
        mock_socket.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_RCVBUF, 65536
        )
        mock_socket.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_SNDBUF, 32768
        )

    def test_disabled_options(self):
        interceptor = SocketOptionsInterceptor(
            tcp_nodelay=False,
            keep_alive=False,
        )
        mock_socket = MagicMock(spec=socket.socket)
        interceptor.intercept(mock_socket)
        calls = mock_socket.setsockopt.call_args_list
        assert not any(
            call[0][1] == socket.TCP_NODELAY for call in calls
        )


class TestFailureDetector:
    """Tests for FailureDetector class."""

    def test_default_values(self):
        fd = FailureDetector()
        assert fd.heartbeat_interval == 5.0
        assert fd.heartbeat_timeout == 60.0

    def test_custom_values(self):
        fd = FailureDetector(heartbeat_interval=10.0, heartbeat_timeout=120.0)
        assert fd.heartbeat_interval == 10.0
        assert fd.heartbeat_timeout == 120.0

    def test_register_connection(self, mock_connection):
        fd = FailureDetector()
        fd.register_connection(mock_connection)
        assert mock_connection.connection_id in fd._last_heartbeat

    def test_unregister_connection(self, mock_connection):
        fd = FailureDetector()
        fd.register_connection(mock_connection)
        fd.unregister_connection(mock_connection)
        assert mock_connection.connection_id not in fd._last_heartbeat

    def test_on_heartbeat_received(self, mock_connection):
        fd = FailureDetector()
        fd.register_connection(mock_connection)
        initial = fd._last_heartbeat[mock_connection.connection_id]
        fd.on_heartbeat_received(mock_connection)
        assert fd._last_heartbeat[mock_connection.connection_id] >= initial

    def test_is_alive_dead_connection(self, mock_connection):
        fd = FailureDetector()
        mock_connection.is_alive = False
        assert fd.is_alive(mock_connection) is False

    def test_is_alive_no_read(self, mock_connection):
        fd = FailureDetector()
        mock_connection.is_alive = True
        mock_connection.last_read_time = 0
        assert fd.is_alive(mock_connection) is True

    def test_needs_heartbeat_dead_connection(self, mock_connection):
        fd = FailureDetector()
        mock_connection.is_alive = False
        assert fd.needs_heartbeat(mock_connection) is False

    def test_needs_heartbeat_no_write(self, mock_connection):
        fd = FailureDetector()
        mock_connection.is_alive = True
        mock_connection.last_write_time = 0
        assert fd.needs_heartbeat(mock_connection) is True


class TestPingFailureDetector:
    """Tests for PingFailureDetector class."""

    def test_default_values(self):
        pfd = PingFailureDetector()
        assert pfd.ping_timeout == 5.0
        assert pfd.ping_interval == 10.0
        assert pfd.max_failures == 3

    def test_register_host(self):
        pfd = PingFailureDetector()
        pfd.register_host("192.168.1.1")
        assert "192.168.1.1" in pfd._failure_counts
        assert pfd._failure_counts["192.168.1.1"] == 0

    def test_unregister_host(self):
        pfd = PingFailureDetector()
        pfd.register_host("192.168.1.1")
        pfd.unregister_host("192.168.1.1")
        assert "192.168.1.1" not in pfd._failure_counts

    def test_is_host_suspect(self):
        pfd = PingFailureDetector()
        pfd.register_host("192.168.1.1")
        assert pfd.is_host_suspect("192.168.1.1") is False
        pfd._failure_counts["192.168.1.1"] = 3
        assert pfd.is_host_suspect("192.168.1.1") is True

    def test_get_failure_count(self):
        pfd = PingFailureDetector()
        pfd.register_host("192.168.1.1")
        assert pfd.get_failure_count("192.168.1.1") == 0
        pfd._record_failure("192.168.1.1")
        assert pfd.get_failure_count("192.168.1.1") == 1

    def test_record_success_resets_count(self):
        pfd = PingFailureDetector()
        pfd.register_host("192.168.1.1")
        pfd._failure_counts["192.168.1.1"] = 2
        pfd._record_success("192.168.1.1")
        assert pfd._failure_counts["192.168.1.1"] == 0


class TestPhiAccrualFailureDetector:
    """Tests for PhiAccrualFailureDetector class."""

    def test_default_values(self):
        pafd = PhiAccrualFailureDetector()
        assert pafd.threshold == 8.0

    def test_register_connection(self):
        pafd = PhiAccrualFailureDetector()
        pafd.register_connection(1)
        assert 1 in pafd._heartbeat_history

    def test_unregister_connection(self):
        pafd = PhiAccrualFailureDetector()
        pafd.register_connection(1)
        pafd.unregister_connection(1)
        assert 1 not in pafd._heartbeat_history

    def test_heartbeat(self):
        pafd = PhiAccrualFailureDetector()
        pafd.register_connection(1)
        pafd.heartbeat(1, timestamp_ms=1000)
        pafd.heartbeat(1, timestamp_ms=1500)
        history = pafd._heartbeat_history[1]
        assert history.last_timestamp == 1500

    def test_phi_unregistered_connection(self):
        pafd = PhiAccrualFailureDetector()
        phi = pafd.phi(999)
        assert phi == 0.0

    def test_phi_empty_history(self):
        pafd = PhiAccrualFailureDetector()
        pafd._heartbeat_history[1] = HeartbeatHistory(
            first_heartbeat_estimate_ms=0
        )
        phi = pafd.phi(1)
        assert phi == 0.0

    def test_is_available(self):
        pafd = PhiAccrualFailureDetector(threshold=8.0)
        pafd.register_connection(1)
        pafd.heartbeat(1, timestamp_ms=1000)
        pafd.heartbeat(1, timestamp_ms=1500)
        assert pafd.is_available(1, timestamp_ms=1600) is True


class TestHeartbeatHistory:
    """Tests for HeartbeatHistory class."""

    def test_empty_history(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        assert history.is_empty is True
        assert history.last_timestamp is None

    def test_with_initial_estimate(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=500)
        assert history.is_empty is False
        assert len(history._intervals) == 2

    def test_add_timestamp(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        history.add(1000)
        assert history.last_timestamp == 1000
        history.add(1500)
        assert history.last_timestamp == 1500
        assert len(history._intervals) == 1

    def test_mean(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        history.add(0)
        history.add(100)
        history.add(200)
        history.add(300)
        assert history.mean == 100.0

    def test_std_deviation(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        history.add(0)
        history.add(100)
        history.add(200)
        history.add(300)
        assert history.std_deviation >= 0


class TestConnectionState:
    """Tests for ConnectionState enum."""

    def test_values(self):
        assert ConnectionState.CREATED.value == "CREATED"
        assert ConnectionState.CONNECTING.value == "CONNECTING"
        assert ConnectionState.CONNECTED.value == "CONNECTED"
        assert ConnectionState.AUTHENTICATED.value == "AUTHENTICATED"
        assert ConnectionState.CLOSING.value == "CLOSING"
        assert ConnectionState.CLOSED.value == "CLOSED"


class TestConnection:
    """Tests for Connection class."""

    def test_init(self, address):
        conn = Connection(address, connection_id=1)
        assert conn.address is address
        assert conn.connection_id == 1
        assert conn.state == ConnectionState.CREATED
        assert conn.is_alive is False

    def test_is_alive_states(self, address):
        conn = Connection(address, connection_id=1)
        assert conn.is_alive is False

        conn._state = ConnectionState.CONNECTED
        assert conn.is_alive is True

        conn._state = ConnectionState.AUTHENTICATED
        assert conn.is_alive is True

        conn._state = ConnectionState.CLOSING
        assert conn.is_alive is False

    def test_member_uuid(self, address):
        conn = Connection(address, connection_id=1)
        assert conn.member_uuid is None
        conn.member_uuid = "test-uuid"
        assert conn.member_uuid == "test-uuid"

    def test_mark_authenticated(self, address):
        conn = Connection(address, connection_id=1)
        conn._state = ConnectionState.CONNECTED
        conn.mark_authenticated()
        assert conn.state == ConnectionState.AUTHENTICATED

    def test_str_repr(self, address):
        conn = Connection(address, connection_id=1)
        s = str(conn)
        assert "Connection" in s
        assert "localhost" in s
