"""Unit tests for hazelcast.network module (address, connection, ssl_config, socket_interceptor)."""

import asyncio
import socket
import ssl
import struct
import time
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest

from hazelcast.network.address import Address, AddressHelper
from hazelcast.network.connection import Connection, ConnectionState
from hazelcast.network.ssl_config import (
    CertificateVerifyMode,
    DEFAULT_CIPHER_SUITES,
    TlsConfig,
    TlsProtocol,
)
from hazelcast.network.socket_interceptor import (
    NoOpSocketInterceptor,
    SocketInterceptor,
    SocketOptionsInterceptor,
)
from hazelcast.network.failure_detector import (
    FailureDetector,
    PingFailureDetector,
    PhiAccrualFailureDetector,
    HeartbeatHistory,
    HeartbeatManager,
    _can_use_raw_sockets,
    _calculate_checksum,
)
from hazelcast.network.connection_manager import (
    RoutingMode,
    RoundRobinLoadBalancer,
    RandomLoadBalancer,
    ConnectionManager,
)
from hazelcast.exceptions import (
    ClientOfflineException,
    ConfigurationException,
    HazelcastException,
    TargetDisconnectedException,
)


class TestAddress:
    """Tests for Address class."""

    def test_init_default_port(self):
        addr = Address("localhost")
        assert addr.host == "localhost"
        assert addr.port == 5701

    def test_init_custom_port(self):
        addr = Address("localhost", 5702)
        assert addr.host == "localhost"
        assert addr.port == 5702

    def test_host_property(self):
        addr = Address("192.168.1.1", 5701)
        assert addr.host == "192.168.1.1"

    def test_port_property(self):
        addr = Address("localhost", 5703)
        assert addr.port == 5703

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

    def test_equality_with_non_address(self):
        addr = Address("localhost", 5701)
        assert addr != "localhost:5701"
        assert addr != 5701
        assert addr != None

    def test_hash(self):
        addr1 = Address("localhost", 5701)
        addr2 = Address("localhost", 5701)
        addr3 = Address("localhost", 5702)

        assert hash(addr1) == hash(addr2)
        assert hash(addr1) != hash(addr3)

    def test_hash_in_set(self):
        addr1 = Address("localhost", 5701)
        addr2 = Address("localhost", 5701)
        addr3 = Address("localhost", 5702)

        s = {addr1, addr2, addr3}
        assert len(s) == 2

    @patch("socket.getaddrinfo")
    def test_resolve_success(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 5701)),
            (socket.AF_INET6, socket.SOCK_STREAM, 0, "", ("::1", 5701, 0, 0)),
        ]

        addr = Address("localhost", 5701)
        resolved = addr.resolve()

        assert len(resolved) == 2
        assert ("127.0.0.1", 5701) in resolved
        assert ("::1", 5701) in resolved

    @patch("socket.getaddrinfo")
    def test_resolve_deduplicates(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 5701)),
            (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 5701)),
        ]

        addr = Address("localhost", 5701)
        resolved = addr.resolve()

        assert len(resolved) == 1

    @patch("socket.getaddrinfo")
    def test_resolve_caches_result(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 5701)),
        ]

        addr = Address("localhost", 5701)
        resolved1 = addr.resolve()
        resolved2 = addr.resolve()

        assert resolved1 is resolved2
        mock_getaddrinfo.assert_called_once()

    @patch("socket.getaddrinfo")
    def test_resolve_failure(self, mock_getaddrinfo):
        mock_getaddrinfo.side_effect = socket.gaierror("DNS lookup failed")

        addr = Address("nonexistent.invalid", 5701)
        resolved = addr.resolve()

        assert resolved == [("nonexistent.invalid", 5701)]

    def test_invalidate_cache(self):
        addr = Address("localhost", 5701)
        addr._resolved_addresses = [("127.0.0.1", 5701)]
        addr.invalidate_cache()
        assert addr._resolved_addresses is None


class TestAddressHelper:
    """Tests for AddressHelper class."""

    def test_parse_simple_host(self):
        addr = AddressHelper.parse("localhost")
        assert addr.host == "localhost"
        assert addr.port == 5701

    def test_parse_host_with_port(self):
        addr = AddressHelper.parse("localhost:5702")
        assert addr.host == "localhost"
        assert addr.port == 5702

    def test_parse_ip_address(self):
        addr = AddressHelper.parse("192.168.1.1:5701")
        assert addr.host == "192.168.1.1"
        assert addr.port == 5701

    def test_parse_ipv6_bracketed(self):
        addr = AddressHelper.parse("[::1]:5701")
        assert addr.host == "::1"
        assert addr.port == 5701

    def test_parse_ipv6_bracketed_no_port(self):
        addr = AddressHelper.parse("[::1]")
        assert addr.host == "::1"
        assert addr.port == 5701

    def test_parse_ipv6_unbracketed(self):
        addr = AddressHelper.parse("::1")
        assert addr.host == "::1"
        assert addr.port == 5701

    def test_parse_strips_whitespace(self):
        addr = AddressHelper.parse("  localhost:5701  ")
        assert addr.host == "localhost"
        assert addr.port == 5701

    def test_parse_invalid_port(self):
        addr = AddressHelper.parse("localhost:invalid")
        assert addr.host == "localhost:invalid"
        assert addr.port == 5701

    def test_parse_list(self):
        addresses = AddressHelper.parse_list([
            "host1:5701",
            "host2:5702",
            "host3",
        ])
        assert len(addresses) == 3
        assert addresses[0].host == "host1"
        assert addresses[0].port == 5701
        assert addresses[1].host == "host2"
        assert addresses[1].port == 5702
        assert addresses[2].host == "host3"
        assert addresses[2].port == 5701

    def test_parse_list_empty(self):
        addresses = AddressHelper.parse_list([])
        assert addresses == []

    def test_get_possible_addresses_default_range(self):
        base = [Address("host1", 5701)]
        possible = AddressHelper.get_possible_addresses(base)

        assert len(possible) == 3
        assert Address("host1", 5701) in possible
        assert Address("host1", 5702) in possible
        assert Address("host1", 5703) in possible

    def test_get_possible_addresses_custom_range(self):
        base = [Address("host1", 5701)]
        possible = AddressHelper.get_possible_addresses(base, port_range=5)

        assert len(possible) == 5

    def test_get_possible_addresses_deduplicates(self):
        base = [
            Address("host1", 5701),
            Address("host1", 5702),
        ]
        possible = AddressHelper.get_possible_addresses(base, port_range=2)

        assert len(possible) == 3

    def test_get_possible_addresses_multiple_hosts(self):
        base = [
            Address("host1", 5701),
            Address("host2", 5701),
        ]
        possible = AddressHelper.get_possible_addresses(base, port_range=2)

        assert len(possible) == 4


class TestTlsProtocol:
    """Tests for TlsProtocol enum."""

    def test_tls_1_0(self):
        assert TlsProtocol.TLS_1_0.value == "TLSv1"

    def test_tls_1_1(self):
        assert TlsProtocol.TLS_1_1.value == "TLSv1.1"

    def test_tls_1_2(self):
        assert TlsProtocol.TLS_1_2.value == "TLSv1.2"

    def test_tls_1_3(self):
        assert TlsProtocol.TLS_1_3.value == "TLSv1.3"


class TestCertificateVerifyMode:
    """Tests for CertificateVerifyMode enum."""

    def test_none(self):
        assert CertificateVerifyMode.NONE.value == "NONE"

    def test_optional(self):
        assert CertificateVerifyMode.OPTIONAL.value == "OPTIONAL"

    def test_required(self):
        assert CertificateVerifyMode.REQUIRED.value == "REQUIRED"


class TestTlsConfig:
    """Tests for TlsConfig class."""

    def test_init_defaults(self):
        config = TlsConfig()
        assert config.enabled is False
        assert config.ca_cert_path is None
        assert config.client_cert_path is None
        assert config.client_key_path is None
        assert config.verify_mode == CertificateVerifyMode.REQUIRED
        assert config.check_hostname is True
        assert config.protocol == TlsProtocol.TLS_1_2

    def test_init_enabled_no_ca_required_raises(self):
        with pytest.raises(ConfigurationException):
            TlsConfig(
                enabled=True,
                verify_mode=CertificateVerifyMode.REQUIRED,
            )

    def test_init_enabled_with_ca(self):
        config = TlsConfig(
            enabled=True,
            ca_cert_path="/path/to/ca.pem",
        )
        assert config.enabled is True
        assert config.ca_cert_path == "/path/to/ca.pem"

    def test_init_client_cert_without_key_raises(self):
        with pytest.raises(ConfigurationException):
            TlsConfig(
                enabled=True,
                ca_cert_path="/path/to/ca.pem",
                client_cert_path="/path/to/client.pem",
            )

    def test_init_client_key_without_cert_raises(self):
        with pytest.raises(ConfigurationException):
            TlsConfig(
                enabled=True,
                ca_cert_path="/path/to/ca.pem",
                client_key_path="/path/to/client-key.pem",
            )

    def test_init_mutual_tls(self):
        config = TlsConfig(
            enabled=True,
            ca_cert_path="/path/to/ca.pem",
            client_cert_path="/path/to/client.pem",
            client_key_path="/path/to/client-key.pem",
        )
        assert config.mutual_tls_enabled is True

    def test_init_verify_mode_none_no_ca_required(self):
        config = TlsConfig(
            enabled=True,
            verify_mode=CertificateVerifyMode.NONE,
        )
        assert config.enabled is True

    def test_enabled_setter_triggers_validation(self):
        config = TlsConfig()
        with pytest.raises(ConfigurationException):
            config.enabled = True

    def test_ca_cert_path_setter(self):
        config = TlsConfig()
        config.ca_cert_path = "/path/to/ca.pem"
        assert config.ca_cert_path == "/path/to/ca.pem"

    def test_client_cert_path_setter(self):
        config = TlsConfig()
        config.client_cert_path = "/path/to/client.pem"
        assert config.client_cert_path == "/path/to/client.pem"

    def test_client_key_path_setter(self):
        config = TlsConfig()
        config.client_key_path = "/path/to/client-key.pem"
        assert config.client_key_path == "/path/to/client-key.pem"

    def test_client_key_password_setter(self):
        config = TlsConfig()
        config.client_key_password = "secret"
        assert config.client_key_password == "secret"

    def test_verify_mode_setter(self):
        config = TlsConfig()
        config.verify_mode = CertificateVerifyMode.OPTIONAL
        assert config.verify_mode == CertificateVerifyMode.OPTIONAL

    def test_check_hostname_setter(self):
        config = TlsConfig()
        config.check_hostname = False
        assert config.check_hostname is False

    def test_cipher_suites_setter(self):
        config = TlsConfig()
        config.cipher_suites = ["TLS_AES_256_GCM_SHA384"]
        assert config.cipher_suites == ["TLS_AES_256_GCM_SHA384"]

    def test_protocol_setter(self):
        config = TlsConfig()
        config.protocol = TlsProtocol.TLS_1_3
        assert config.protocol == TlsProtocol.TLS_1_3

    def test_mutual_tls_enabled_false(self):
        config = TlsConfig()
        assert config.mutual_tls_enabled is False

    def test_create_ssl_context_not_enabled_raises(self):
        config = TlsConfig()
        with pytest.raises(ConfigurationException):
            config.create_ssl_context()

    def test_create_ssl_context_basic(self):
        config = TlsConfig(
            enabled=True,
            verify_mode=CertificateVerifyMode.NONE,
        )
        context = config.create_ssl_context()
        assert isinstance(context, ssl.SSLContext)
        assert context.verify_mode == ssl.CERT_NONE

    def test_create_ssl_context_verify_optional(self):
        config = TlsConfig(
            enabled=True,
            verify_mode=CertificateVerifyMode.OPTIONAL,
        )
        context = config.create_ssl_context()
        assert context.verify_mode == ssl.CERT_OPTIONAL

    def test_create_ssl_context_check_hostname_disabled_for_none(self):
        config = TlsConfig(
            enabled=True,
            verify_mode=CertificateVerifyMode.NONE,
            check_hostname=True,
        )
        context = config.create_ssl_context()
        assert context.check_hostname is False

    def test_from_dict_minimal(self):
        data = {"enabled": False}
        config = TlsConfig.from_dict(data)
        assert config.enabled is False

    def test_from_dict_full(self):
        data = {
            "enabled": True,
            "ca_cert_path": "/path/to/ca.pem",
            "client_cert_path": "/path/to/client.pem",
            "client_key_path": "/path/to/client-key.pem",
            "client_key_password": "secret",
            "verify_mode": "OPTIONAL",
            "check_hostname": False,
            "cipher_suites": ["TLS_AES_256_GCM_SHA384"],
            "protocol": "TLSv1.3",
        }
        config = TlsConfig.from_dict(data)
        assert config.enabled is True
        assert config.ca_cert_path == "/path/to/ca.pem"
        assert config.verify_mode == CertificateVerifyMode.OPTIONAL
        assert config.protocol == TlsProtocol.TLS_1_3

    def test_from_dict_invalid_verify_mode(self):
        data = {"enabled": False, "verify_mode": "INVALID"}
        with pytest.raises(ConfigurationException):
            TlsConfig.from_dict(data)

    def test_repr(self):
        config = TlsConfig(
            enabled=True,
            ca_cert_path="/path/to/ca.pem",
            verify_mode=CertificateVerifyMode.REQUIRED,
        )
        repr_str = repr(config)
        assert "enabled=True" in repr_str
        assert "mutual_tls=False" in repr_str
        assert "verify_mode=REQUIRED" in repr_str

    def test_default_cipher_suites(self):
        assert len(DEFAULT_CIPHER_SUITES) > 0
        assert "TLS_AES_256_GCM_SHA384" in DEFAULT_CIPHER_SUITES


class TestNoOpSocketInterceptor:
    """Tests for NoOpSocketInterceptor class."""

    def test_intercept_returns_same_socket(self):
        interceptor = NoOpSocketInterceptor()
        mock_socket = MagicMock(spec=socket.socket)
        result = interceptor.intercept(mock_socket)
        assert result is mock_socket

    def test_on_connect_does_nothing(self):
        interceptor = NoOpSocketInterceptor()
        mock_socket = MagicMock(spec=socket.socket)
        interceptor.on_connect(mock_socket, ("127.0.0.1", 5701))

    def test_on_disconnect_does_nothing(self):
        interceptor = NoOpSocketInterceptor()
        mock_socket = MagicMock(spec=socket.socket)
        interceptor.on_disconnect(mock_socket)


class TestSocketOptionsInterceptor:
    """Tests for SocketOptionsInterceptor class."""

    def test_init_defaults(self):
        interceptor = SocketOptionsInterceptor()
        assert interceptor._tcp_nodelay is True
        assert interceptor._keep_alive is True
        assert interceptor._receive_buffer_size is None
        assert interceptor._send_buffer_size is None

    def test_init_custom_values(self):
        interceptor = SocketOptionsInterceptor(
            tcp_nodelay=False,
            keep_alive=False,
            receive_buffer_size=8192,
            send_buffer_size=16384,
        )
        assert interceptor._tcp_nodelay is False
        assert interceptor._keep_alive is False
        assert interceptor._receive_buffer_size == 8192
        assert interceptor._send_buffer_size == 16384

    def test_intercept_sets_tcp_nodelay(self):
        interceptor = SocketOptionsInterceptor(tcp_nodelay=True, keep_alive=False)
        mock_socket = MagicMock(spec=socket.socket)

        result = interceptor.intercept(mock_socket)

        mock_socket.setsockopt.assert_any_call(
            socket.IPPROTO_TCP, socket.TCP_NODELAY, 1
        )
        assert result is mock_socket

    def test_intercept_sets_keep_alive(self):
        interceptor = SocketOptionsInterceptor(tcp_nodelay=False, keep_alive=True)
        mock_socket = MagicMock(spec=socket.socket)

        interceptor.intercept(mock_socket)

        mock_socket.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1
        )

    def test_intercept_sets_receive_buffer_size(self):
        interceptor = SocketOptionsInterceptor(
            tcp_nodelay=False, keep_alive=False, receive_buffer_size=8192
        )
        mock_socket = MagicMock(spec=socket.socket)

        interceptor.intercept(mock_socket)

        mock_socket.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_RCVBUF, 8192
        )

    def test_intercept_sets_send_buffer_size(self):
        interceptor = SocketOptionsInterceptor(
            tcp_nodelay=False, keep_alive=False, send_buffer_size=16384
        )
        mock_socket = MagicMock(spec=socket.socket)

        interceptor.intercept(mock_socket)

        mock_socket.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_SNDBUF, 16384
        )

    def test_intercept_no_buffer_sizes(self):
        interceptor = SocketOptionsInterceptor(tcp_nodelay=False, keep_alive=False)
        mock_socket = MagicMock(spec=socket.socket)

        interceptor.intercept(mock_socket)

        calls = mock_socket.setsockopt.call_args_list
        for call in calls:
            assert call[0][1] not in (socket.SO_RCVBUF, socket.SO_SNDBUF)


class TestConnectionState:
    """Tests for ConnectionState enum."""

    def test_states(self):
        assert ConnectionState.CREATED.value == "CREATED"
        assert ConnectionState.CONNECTING.value == "CONNECTING"
        assert ConnectionState.CONNECTED.value == "CONNECTED"
        assert ConnectionState.AUTHENTICATED.value == "AUTHENTICATED"
        assert ConnectionState.CLOSING.value == "CLOSING"
        assert ConnectionState.CLOSED.value == "CLOSED"


class TestConnection:
    """Tests for Connection class."""

    def test_init_defaults(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)

        assert conn.address is addr
        assert conn.connection_id == 1
        assert conn.state == ConnectionState.CREATED
        assert conn.is_alive is False
        assert conn.last_read_time == 0.0
        assert conn.last_write_time == 0.0
        assert conn.remote_address is None
        assert conn.local_address is None
        assert conn.member_uuid is None

    def test_init_custom_values(self):
        addr = Address("localhost", 5701)
        conn = Connection(
            address=addr,
            connection_id=1,
            connection_timeout=10.0,
            tcp_no_delay=False,
            socket_keep_alive=False,
            socket_send_buffer_size=8192,
            socket_receive_buffer_size=16384,
            socket_linger_seconds=5,
        )

        assert conn._connection_timeout == 10.0
        assert conn._tcp_no_delay is False
        assert conn._socket_keep_alive is False
        assert conn._socket_send_buffer_size == 8192
        assert conn._socket_receive_buffer_size == 16384
        assert conn._socket_linger_seconds == 5

    def test_is_alive_created(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        assert conn.is_alive is False

    def test_is_alive_connected(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED
        assert conn.is_alive is True

    def test_is_alive_authenticated(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.AUTHENTICATED
        assert conn.is_alive is True

    def test_is_alive_closing(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CLOSING
        assert conn.is_alive is False

    def test_is_alive_closed(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CLOSED
        assert conn.is_alive is False

    def test_member_uuid_setter(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn.member_uuid = "test-uuid"
        assert conn.member_uuid == "test-uuid"

    def test_set_message_callback(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        callback = MagicMock()
        conn.set_message_callback(callback)
        assert conn._message_callback is callback

    def test_mark_authenticated(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED
        conn.mark_authenticated()
        assert conn.state == ConnectionState.AUTHENTICATED

    def test_mark_authenticated_wrong_state(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn.mark_authenticated()
        assert conn.state == ConnectionState.CREATED

    def test_str(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        s = str(conn)
        assert "Connection" in s
        assert "id=1" in s
        assert "localhost:5701" in s
        assert "CREATED" in s

    def test_repr(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        assert repr(conn) == str(conn)

    @pytest.mark.asyncio
    async def test_connect_wrong_state(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED

        with pytest.raises(HazelcastException):
            await conn.connect()

    @pytest.mark.asyncio
    async def test_connect_unresolvable_address(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)

        with patch.object(addr, "resolve", return_value=[]):
            with pytest.raises(HazelcastException):
                await conn.connect()

    @pytest.mark.asyncio
    async def test_connect_timeout(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1, connection_timeout=0.001)

        with patch.object(addr, "resolve", return_value=[("127.0.0.1", 5701)]):
            with patch("asyncio.open_connection", side_effect=asyncio.TimeoutError()):
                with pytest.raises(HazelcastException) as exc_info:
                    await conn.connect()
                assert "timeout" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_connect_failure(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)

        with patch.object(addr, "resolve", return_value=[("127.0.0.1", 5701)]):
            with patch("asyncio.open_connection", side_effect=ConnectionRefusedError()):
                with pytest.raises(HazelcastException):
                    await conn.connect()

    @pytest.mark.asyncio
    async def test_connect_success(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)

        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_socket = MagicMock()
        mock_socket.getpeername.return_value = ("127.0.0.1", 5701)
        mock_socket.getsockname.return_value = ("127.0.0.1", 12345)
        mock_writer.get_extra_info.return_value = mock_socket

        with patch.object(addr, "resolve", return_value=[("127.0.0.1", 5701)]):
            with patch("asyncio.open_connection", return_value=(mock_reader, mock_writer)):
                await conn.connect()

        assert conn.state == ConnectionState.CONNECTED
        assert conn.remote_address == ("127.0.0.1", 5701)
        assert conn.local_address == ("127.0.0.1", 12345)

    @pytest.mark.asyncio
    async def test_connect_with_ssl(self):
        addr = Address("localhost", 5701)
        ssl_config = MagicMock()
        ssl_config.enabled = True
        ssl_config.create_ssl_context.return_value = MagicMock(spec=ssl.SSLContext)

        conn = Connection(address=addr, connection_id=1, ssl_config=ssl_config)

        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_writer.get_extra_info.return_value = None

        with patch.object(addr, "resolve", return_value=[("127.0.0.1", 5701)]):
            with patch("asyncio.open_connection", return_value=(mock_reader, mock_writer)) as mock_open:
                await conn.connect()

                call_kwargs = mock_open.call_args[1]
                assert "ssl" in call_kwargs

    @pytest.mark.asyncio
    async def test_connect_with_socket_interceptor(self):
        addr = Address("localhost", 5701)
        interceptor = MagicMock(spec=SocketInterceptor)

        conn = Connection(address=addr, connection_id=1, socket_interceptor=interceptor)

        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_socket = MagicMock()
        mock_socket.getpeername.return_value = ("127.0.0.1", 5701)
        mock_socket.getsockname.return_value = ("127.0.0.1", 12345)
        mock_writer.get_extra_info.return_value = mock_socket

        with patch.object(addr, "resolve", return_value=[("127.0.0.1", 5701)]):
            with patch("asyncio.open_connection", return_value=(mock_reader, mock_writer)):
                await conn.connect()

        interceptor.on_connect.assert_called_once()

    def test_apply_socket_options(self):
        addr = Address("localhost", 5701)
        conn = Connection(
            address=addr,
            connection_id=1,
            tcp_no_delay=True,
            socket_keep_alive=True,
            socket_send_buffer_size=8192,
            socket_receive_buffer_size=16384,
            socket_linger_seconds=5,
        )

        mock_socket = MagicMock(spec=socket.socket)
        conn._apply_socket_options(mock_socket)

        calls = mock_socket.setsockopt.call_args_list
        assert len(calls) == 5

    def test_apply_socket_options_minimal(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)

        mock_socket = MagicMock(spec=socket.socket)
        conn._apply_socket_options(mock_socket)

        calls = mock_socket.setsockopt.call_args_list
        assert len(calls) == 2

    def test_apply_socket_options_error_caught(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)

        mock_socket = MagicMock(spec=socket.socket)
        mock_socket.setsockopt.side_effect = OSError("Socket error")

        conn._apply_socket_options(mock_socket)

    @pytest.mark.asyncio
    async def test_send_not_alive(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        message = MagicMock()

        with pytest.raises(TargetDisconnectedException):
            await conn.send(message)

    @pytest.mark.asyncio
    async def test_send_no_writer(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED
        message = MagicMock()

        with pytest.raises(TargetDisconnectedException):
            await conn.send(message)

    @pytest.mark.asyncio
    async def test_send_success(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED

        mock_writer = MagicMock()
        mock_writer.drain = AsyncMock()
        conn._writer = mock_writer

        message = MagicMock()
        message.to_bytes.return_value = b"test data"
        message.get_correlation_id.return_value = 123

        await conn.send(message)

        mock_writer.write.assert_called_once_with(b"test data")
        mock_writer.drain.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_write_error(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED

        mock_writer = MagicMock()
        mock_writer.write.side_effect = Exception("Write failed")
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        conn._writer = mock_writer

        message = MagicMock()
        message.to_bytes.return_value = b"test data"

        with pytest.raises(TargetDisconnectedException):
            await conn.send(message)

    def test_send_sync_in_running_loop(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED

        mock_writer = MagicMock()
        conn._writer = mock_writer

        message = MagicMock()

        with patch("asyncio.get_event_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_loop.is_running.return_value = True
            mock_get_loop.return_value = mock_loop

            with patch("asyncio.create_task"):
                conn.send_sync(message)

    @pytest.mark.asyncio
    async def test_close_already_closing(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CLOSING

        await conn.close("Test")
        assert conn.state == ConnectionState.CLOSING

    @pytest.mark.asyncio
    async def test_close_already_closed(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CLOSED

        await conn.close("Test")
        assert conn.state == ConnectionState.CLOSED

    @pytest.mark.asyncio
    async def test_close_with_read_task(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED

        async def dummy_task():
            await asyncio.sleep(10)

        conn._read_task = asyncio.create_task(dummy_task())

        await conn.close("Test")

        assert conn.state == ConnectionState.CLOSED
        assert conn._read_task is None

    @pytest.mark.asyncio
    async def test_close_with_writer(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED

        mock_writer = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        mock_writer.get_extra_info.return_value = None
        conn._writer = mock_writer

        await conn.close("Test")

        mock_writer.close.assert_called_once()
        assert conn._writer is None

    @pytest.mark.asyncio
    async def test_close_with_socket_interceptor(self):
        addr = Address("localhost", 5701)
        interceptor = MagicMock(spec=SocketInterceptor)
        conn = Connection(address=addr, connection_id=1, socket_interceptor=interceptor)
        conn._state = ConnectionState.CONNECTED

        mock_socket = MagicMock()
        mock_writer = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        mock_writer.get_extra_info.return_value = mock_socket
        conn._writer = mock_writer

        await conn.close("Test")

        interceptor.on_disconnect.assert_called_once_with(mock_socket)

    def test_start_reading(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED

        with patch("asyncio.create_task") as mock_create:
            conn.start_reading()
            mock_create.assert_called_once()

    def test_start_reading_not_alive(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)

        with patch("asyncio.create_task") as mock_create:
            conn.start_reading()
            mock_create.assert_not_called()

    def test_start_reading_already_started(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED
        conn._read_task = MagicMock()

        with patch("asyncio.create_task") as mock_create:
            conn.start_reading()
            mock_create.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_loop_processes_data(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED

        mock_reader = AsyncMock()
        mock_reader.read.side_effect = [b"", None]
        conn._reader = mock_reader

        await conn._read_loop()

        assert conn.state == ConnectionState.CLOSED

    @pytest.mark.asyncio
    async def test_read_loop_handles_exception(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED

        mock_reader = AsyncMock()
        mock_reader.read.side_effect = Exception("Read error")
        conn._reader = mock_reader

        mock_writer = MagicMock()
        mock_writer.close = MagicMock()
        mock_writer.wait_closed = AsyncMock()
        mock_writer.get_extra_info.return_value = None
        conn._writer = mock_writer

        await conn._read_loop()

        assert conn.state == ConnectionState.CLOSED

    @pytest.mark.asyncio
    async def test_read_loop_cancelled(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED

        mock_reader = AsyncMock()
        mock_reader.read.side_effect = asyncio.CancelledError()
        conn._reader = mock_reader

        await conn._read_loop()

    @pytest.mark.asyncio
    async def test_process_buffer_incomplete_frame_header(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._read_buffer = bytearray(b"\x00\x01\x02")

        await conn._process_buffer()

        assert len(conn._read_buffer) == 3

    @pytest.mark.asyncio
    async def test_process_buffer_incomplete_frame_body(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._read_buffer = bytearray(struct.pack("<I", 100) + b"\x00\x00")

        await conn._process_buffer()

        assert len(conn._read_buffer) == 6

    def test_find_message_end_insufficient_header(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._read_buffer = bytearray(b"\x00\x01\x02")

        result = conn._find_message_end()

        assert result is None

    def test_find_message_end_incomplete_frame(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        frame_length = 100
        conn._read_buffer = bytearray(struct.pack("<I", frame_length) + b"\x00\x00")

        result = conn._find_message_end()

        assert result is None

    def test_find_message_end_complete_unfragmented(self):
        from hazelcast.protocol.client_message import END_FLAG
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        frame_length = 10
        flags = END_FLAG
        conn._read_buffer = bytearray(
            struct.pack("<I", frame_length) +
            struct.pack("<H", flags) +
            b"\x00" * (frame_length - 6)
        )

        result = conn._find_message_end()

        assert result == frame_length

    def test_find_message_end_no_end_flag(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        frame_length = 10
        flags = 0
        conn._read_buffer = bytearray(
            struct.pack("<I", frame_length) +
            struct.pack("<H", flags) +
            b"\x00" * (frame_length - 6)
        )

        result = conn._find_message_end()

        assert result is None

    def test_send_sync_not_running_loop(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED

        mock_writer = MagicMock()
        conn._writer = mock_writer

        message = MagicMock()

        with patch("asyncio.get_event_loop") as mock_get_loop:
            mock_loop = MagicMock()
            mock_loop.is_running.return_value = False
            mock_loop.run_until_complete = MagicMock()
            mock_get_loop.return_value = mock_loop

            conn.send_sync(message)

            mock_loop.run_until_complete.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_updates_last_write_time(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED
        conn._last_write_time = 0.0

        mock_writer = MagicMock()
        mock_writer.drain = AsyncMock()
        conn._writer = mock_writer

        message = MagicMock()
        message.to_bytes.return_value = b"test data"
        message.get_correlation_id.return_value = 123

        await conn.send(message)

        assert conn._last_write_time > 0.0

    @pytest.mark.asyncio
    async def test_close_with_cause(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED

        cause = Exception("Test cause")
        await conn.close("Test reason", cause)

        assert conn._close_reason == "Test reason"
        assert conn._close_cause is cause

    @pytest.mark.asyncio
    async def test_close_writer_exception_caught(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)
        conn._state = ConnectionState.CONNECTED

        mock_writer = MagicMock()
        mock_writer.close.side_effect = Exception("Close failed")
        mock_writer.get_extra_info.return_value = None
        conn._writer = mock_writer

        await conn.close("Test")

        assert conn.state == ConnectionState.CLOSED

    @pytest.mark.asyncio
    async def test_connect_no_ssl_context_when_disabled(self):
        addr = Address("localhost", 5701)
        ssl_config = MagicMock()
        ssl_config.enabled = False

        conn = Connection(address=addr, connection_id=1, ssl_config=ssl_config)

        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_writer.get_extra_info.return_value = None

        with patch.object(addr, "resolve", return_value=[("127.0.0.1", 5701)]):
            with patch("asyncio.open_connection", return_value=(mock_reader, mock_writer)) as mock_open:
                await conn.connect()

                call_kwargs = mock_open.call_args[1]
                assert call_kwargs.get("ssl") is None

    @pytest.mark.asyncio
    async def test_connect_no_socket_from_writer(self):
        addr = Address("localhost", 5701)
        conn = Connection(address=addr, connection_id=1)

        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_writer.get_extra_info.return_value = None

        with patch.object(addr, "resolve", return_value=[("127.0.0.1", 5701)]):
            with patch("asyncio.open_connection", return_value=(mock_reader, mock_writer)):
                await conn.connect()

        assert conn.state == ConnectionState.CONNECTED
        assert conn.remote_address is None
        assert conn.local_address is None

    @pytest.mark.asyncio
    async def test_connect_ssl_handshake_failure(self):
        addr = Address("localhost", 5701)
        ssl_config = MagicMock()
        ssl_config.enabled = True
        ssl_config.create_ssl_context.return_value = MagicMock(spec=ssl.SSLContext)

        conn = Connection(address=addr, connection_id=1, ssl_config=ssl_config)

        with patch.object(addr, "resolve", return_value=[("127.0.0.1", 5701)]):
            with patch("asyncio.open_connection", side_effect=ssl.SSLError("handshake failed")):
                with pytest.raises(HazelcastException):
                    await conn.connect()

        assert conn.state == ConnectionState.CLOSED

    @pytest.mark.asyncio
    async def test_connect_ssl_certificate_verify_failed(self):
        addr = Address("localhost", 5701)
        ssl_config = MagicMock()
        ssl_config.enabled = True
        ssl_config.create_ssl_context.return_value = MagicMock(spec=ssl.SSLContext)

        conn = Connection(address=addr, connection_id=1, ssl_config=ssl_config)

        with patch.object(addr, "resolve", return_value=[("127.0.0.1", 5701)]):
            with patch("asyncio.open_connection", side_effect=ssl.SSLCertVerificationError("certificate verify failed")):
                with pytest.raises(HazelcastException):
                    await conn.connect()

        assert conn.state == ConnectionState.CLOSED


class TestFailureDetector:
    """Tests for FailureDetector class."""

    def test_init_defaults(self):
        fd = FailureDetector()
        assert fd.heartbeat_interval == 5.0
        assert fd.heartbeat_timeout == 60.0

    def test_init_custom_values(self):
        fd = FailureDetector(heartbeat_interval=10.0, heartbeat_timeout=120.0)
        assert fd.heartbeat_interval == 10.0
        assert fd.heartbeat_timeout == 120.0

    def test_heartbeat_interval_property_getter_setter(self):
        fd = FailureDetector()
        fd.heartbeat_interval = 15.0
        assert fd.heartbeat_interval == 15.0

    def test_heartbeat_timeout_property_getter_setter(self):
        fd = FailureDetector()
        fd.heartbeat_timeout = 90.0
        assert fd.heartbeat_timeout == 90.0

    def test_register_connection(self):
        fd = FailureDetector()
        conn = MagicMock()
        conn.connection_id = 1

        with patch("time.time", return_value=1000.0):
            fd.register_connection(conn)

        assert fd._last_heartbeat[1] == 1000.0

    def test_unregister_connection(self):
        fd = FailureDetector()
        conn = MagicMock()
        conn.connection_id = 1
        fd._last_heartbeat[1] = 1000.0

        fd.unregister_connection(conn)

        assert 1 not in fd._last_heartbeat

    def test_unregister_nonexistent_connection(self):
        fd = FailureDetector()
        conn = MagicMock()
        conn.connection_id = 999

        fd.unregister_connection(conn)

    def test_on_heartbeat_received(self):
        fd = FailureDetector()
        conn = MagicMock()
        conn.connection_id = 1

        with patch("time.time", return_value=2000.0):
            fd.on_heartbeat_received(conn)

        assert fd._last_heartbeat[1] == 2000.0

    def test_is_alive_dead_connection(self):
        fd = FailureDetector()
        conn = MagicMock()
        conn.is_alive = False

        assert fd.is_alive(conn) is False

    def test_is_alive_no_last_read(self):
        fd = FailureDetector()
        conn = MagicMock()
        conn.is_alive = True
        conn.last_read_time = 0

        assert fd.is_alive(conn) is True

    def test_is_alive_within_timeout(self):
        fd = FailureDetector(heartbeat_timeout=60.0)
        conn = MagicMock()
        conn.is_alive = True
        conn.last_read_time = 950.0

        with patch("time.time", return_value=1000.0):
            assert fd.is_alive(conn) is True

    def test_is_alive_past_timeout(self):
        fd = FailureDetector(heartbeat_timeout=60.0)
        conn = MagicMock()
        conn.is_alive = True
        conn.last_read_time = 900.0

        with patch("time.time", return_value=1000.0):
            assert fd.is_alive(conn) is False

    def test_needs_heartbeat_dead_connection(self):
        fd = FailureDetector()
        conn = MagicMock()
        conn.is_alive = False

        assert fd.needs_heartbeat(conn) is False

    def test_needs_heartbeat_no_last_write(self):
        fd = FailureDetector()
        conn = MagicMock()
        conn.is_alive = True
        conn.last_write_time = 0

        assert fd.needs_heartbeat(conn) is True

    def test_needs_heartbeat_within_interval(self):
        fd = FailureDetector(heartbeat_interval=5.0)
        conn = MagicMock()
        conn.is_alive = True
        conn.last_write_time = 998.0

        with patch("time.time", return_value=1000.0):
            assert fd.needs_heartbeat(conn) is False

    def test_needs_heartbeat_past_interval(self):
        fd = FailureDetector(heartbeat_interval=5.0)
        conn = MagicMock()
        conn.is_alive = True
        conn.last_write_time = 990.0

        with patch("time.time", return_value=1000.0):
            assert fd.needs_heartbeat(conn) is True

    def test_get_suspect_connections_empty(self):
        fd = FailureDetector(heartbeat_timeout=60.0)
        conn = MagicMock()
        conn.is_alive = True
        conn.last_read_time = 950.0

        with patch("time.time", return_value=1000.0):
            suspects = fd.get_suspect_connections({1: conn})

        assert len(suspects) == 0

    def test_get_suspect_connections_with_suspects(self):
        fd = FailureDetector(heartbeat_timeout=60.0)
        conn = MagicMock()
        conn.is_alive = True
        conn.last_read_time = 900.0

        with patch("time.time", return_value=1000.0):
            suspects = fd.get_suspect_connections({1: conn})

        assert len(suspects) == 1
        assert suspects[0] is conn

    def test_get_suspect_connections_skips_dead(self):
        fd = FailureDetector(heartbeat_timeout=60.0)
        conn = MagicMock()
        conn.is_alive = False
        conn.last_read_time = 900.0

        with patch("time.time", return_value=1000.0):
            suspects = fd.get_suspect_connections({1: conn})

        assert len(suspects) == 0


class TestPingFailureDetector:
    """Tests for PingFailureDetector class."""

    def test_init_defaults(self):
        pfd = PingFailureDetector()
        assert pfd.ping_timeout == 5.0
        assert pfd.ping_interval == 10.0
        assert pfd.max_failures == 3

    def test_init_custom_values(self):
        pfd = PingFailureDetector(
            ping_timeout=10.0, ping_interval=20.0, max_failures=5
        )
        assert pfd.ping_timeout == 10.0
        assert pfd.ping_interval == 20.0
        assert pfd.max_failures == 5

    def test_is_available_property(self):
        pfd = PingFailureDetector()
        pfd._can_ping = True
        assert pfd.is_available is True

        pfd._can_ping = False
        assert pfd.is_available is False

    def test_ping_timeout_property(self):
        pfd = PingFailureDetector(ping_timeout=7.0)
        assert pfd.ping_timeout == 7.0

    def test_ping_interval_property(self):
        pfd = PingFailureDetector(ping_interval=15.0)
        assert pfd.ping_interval == 15.0

    def test_max_failures_property(self):
        pfd = PingFailureDetector(max_failures=10)
        assert pfd.max_failures == 10

    def test_register_host(self):
        pfd = PingFailureDetector()
        pfd.register_host("192.168.1.1")

        assert pfd._failure_counts["192.168.1.1"] == 0
        assert pfd._last_ping_times["192.168.1.1"] == 0.0

    def test_unregister_host(self):
        pfd = PingFailureDetector()
        pfd._failure_counts["192.168.1.1"] = 2
        pfd._last_ping_times["192.168.1.1"] = 1000.0

        pfd.unregister_host("192.168.1.1")

        assert "192.168.1.1" not in pfd._failure_counts
        assert "192.168.1.1" not in pfd._last_ping_times

    def test_unregister_nonexistent_host(self):
        pfd = PingFailureDetector()
        pfd.unregister_host("nonexistent")

    def test_needs_ping_not_available(self):
        pfd = PingFailureDetector()
        pfd._can_ping = False

        assert pfd.needs_ping("192.168.1.1") is False

    def test_needs_ping_never_pinged(self):
        pfd = PingFailureDetector()
        pfd._can_ping = True
        pfd._last_ping_times["192.168.1.1"] = 0.0

        assert pfd.needs_ping("192.168.1.1") is True

    def test_needs_ping_within_interval(self):
        pfd = PingFailureDetector(ping_interval=10.0)
        pfd._can_ping = True
        pfd._last_ping_times["192.168.1.1"] = 995.0

        with patch("time.time", return_value=1000.0):
            assert pfd.needs_ping("192.168.1.1") is False

    def test_needs_ping_past_interval(self):
        pfd = PingFailureDetector(ping_interval=10.0)
        pfd._can_ping = True
        pfd._last_ping_times["192.168.1.1"] = 985.0

        with patch("time.time", return_value=1000.0):
            assert pfd.needs_ping("192.168.1.1") is True

    @pytest.mark.asyncio
    async def test_ping_not_available(self):
        pfd = PingFailureDetector()
        pfd._can_ping = False

        success, latency = await pfd.ping("192.168.1.1")

        assert success is False
        assert latency == -1.0

    def test_is_host_suspect_below_max(self):
        pfd = PingFailureDetector(max_failures=3)
        pfd._failure_counts["192.168.1.1"] = 2

        assert pfd.is_host_suspect("192.168.1.1") is False

    def test_is_host_suspect_at_max(self):
        pfd = PingFailureDetector(max_failures=3)
        pfd._failure_counts["192.168.1.1"] = 3

        assert pfd.is_host_suspect("192.168.1.1") is True

    def test_get_failure_count_registered(self):
        pfd = PingFailureDetector()
        pfd._failure_counts["192.168.1.1"] = 5

        assert pfd.get_failure_count("192.168.1.1") == 5

    def test_get_failure_count_unregistered(self):
        pfd = PingFailureDetector()

        assert pfd.get_failure_count("unknown") == 0

    def test_record_success_resets_failures(self):
        pfd = PingFailureDetector()
        pfd._failure_counts["192.168.1.1"] = 5

        pfd._record_success("192.168.1.1")

        assert pfd._failure_counts["192.168.1.1"] == 0

    def test_record_failure_increments(self):
        pfd = PingFailureDetector()
        pfd._failure_counts["192.168.1.1"] = 2

        pfd._record_failure("192.168.1.1")

        assert pfd._failure_counts["192.168.1.1"] == 3


class TestPhiAccrualFailureDetector:
    """Tests for PhiAccrualFailureDetector class."""

    def test_init_defaults(self):
        phi = PhiAccrualFailureDetector()
        assert phi.threshold == 8.0
        assert phi._max_sample_size == 200
        assert phi._min_std_deviation_ms == 100.0
        assert phi._acceptable_heartbeat_pause_ms == 0.0
        assert phi._first_heartbeat_estimate_ms == 500.0

    def test_init_custom_values(self):
        phi = PhiAccrualFailureDetector(
            threshold=10.0,
            max_sample_size=100,
            min_std_deviation_ms=50.0,
            acceptable_heartbeat_pause_ms=100.0,
            first_heartbeat_estimate_ms=1000.0,
        )
        assert phi.threshold == 10.0
        assert phi._max_sample_size == 100
        assert phi._min_std_deviation_ms == 50.0
        assert phi._acceptable_heartbeat_pause_ms == 100.0
        assert phi._first_heartbeat_estimate_ms == 1000.0

    def test_threshold_property_getter_setter(self):
        phi = PhiAccrualFailureDetector()
        phi.threshold = 12.0
        assert phi.threshold == 12.0

    def test_register_connection(self):
        phi = PhiAccrualFailureDetector()
        phi.register_connection(1)

        assert 1 in phi._heartbeat_history
        assert isinstance(phi._heartbeat_history[1], HeartbeatHistory)

    def test_unregister_connection(self):
        phi = PhiAccrualFailureDetector()
        phi.register_connection(1)
        phi.unregister_connection(1)

        assert 1 not in phi._heartbeat_history

    def test_unregister_nonexistent(self):
        phi = PhiAccrualFailureDetector()
        phi.unregister_connection(999)

    def test_heartbeat_unregistered_connection(self):
        phi = PhiAccrualFailureDetector()
        phi.heartbeat(999, 1000.0)

    def test_heartbeat_registered_connection(self):
        phi = PhiAccrualFailureDetector()
        phi.register_connection(1)

        phi.heartbeat(1, 1000.0)

        assert phi._heartbeat_history[1].last_timestamp == 1000.0

    def test_heartbeat_with_custom_timestamp(self):
        phi = PhiAccrualFailureDetector()
        phi.register_connection(1)

        phi.heartbeat(1, timestamp_ms=5000.0)

        assert phi._heartbeat_history[1].last_timestamp == 5000.0

    def test_phi_unregistered_connection(self):
        phi = PhiAccrualFailureDetector()

        assert phi.phi(999) == 0.0

    def test_phi_empty_history(self):
        phi = PhiAccrualFailureDetector(first_heartbeat_estimate_ms=0)
        phi.register_connection(1)

        assert phi.phi(1) == 0.0

    def test_phi_no_last_timestamp(self):
        phi = PhiAccrualFailureDetector(first_heartbeat_estimate_ms=0)
        phi.register_connection(1)
        phi._heartbeat_history[1]._intervals.append(100.0)

        assert phi.phi(1) == 0.0

    def test_phi_low_for_recent_heartbeat(self):
        phi = PhiAccrualFailureDetector()
        phi.register_connection(1)

        phi.heartbeat(1, 1000.0)
        phi.heartbeat(1, 1500.0)

        result = phi.phi(1, 1600.0)
        assert result < phi.threshold

    def test_phi_high_for_old_heartbeat(self):
        phi = PhiAccrualFailureDetector()
        phi.register_connection(1)

        phi.heartbeat(1, 1000.0)
        phi.heartbeat(1, 1500.0)

        result = phi.phi(1, 100000.0)
        assert result > phi.threshold

    def test_is_available_below_threshold(self):
        phi = PhiAccrualFailureDetector()
        phi.register_connection(1)

        phi.heartbeat(1, 1000.0)
        phi.heartbeat(1, 1500.0)

        assert phi.is_available(1, 1600.0) is True

    def test_is_available_above_threshold(self):
        phi = PhiAccrualFailureDetector()
        phi.register_connection(1)

        phi.heartbeat(1, 1000.0)
        phi.heartbeat(1, 1500.0)

        assert phi.is_available(1, 100000.0) is False

    def test_calculate_phi_zero_time_diff(self):
        phi = PhiAccrualFailureDetector()

        result = phi._calculate_phi(0.0, 500.0, 100.0)
        assert result == 0.0

    def test_calculate_phi_positive_diff_above_mean(self):
        phi = PhiAccrualFailureDetector()

        result = phi._calculate_phi(1000.0, 500.0, 100.0)
        assert result > 0.0

    def test_calculate_phi_positive_diff_below_mean(self):
        phi = PhiAccrualFailureDetector()

        result = phi._calculate_phi(300.0, 500.0, 100.0)
        assert result >= 0.0


class TestHeartbeatHistory:
    """Tests for HeartbeatHistory class."""

    def test_init_defaults(self):
        hh = HeartbeatHistory()
        assert hh._max_sample_size == 200
        assert len(hh._intervals) == 2

    def test_init_no_initial_estimate(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        assert len(hh._intervals) == 0

    def test_is_empty_with_initial_estimates(self):
        hh = HeartbeatHistory()
        assert hh.is_empty is False

    def test_is_empty_no_estimates(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        assert hh.is_empty is True

    def test_last_timestamp_none_initially(self):
        hh = HeartbeatHistory()
        assert hh.last_timestamp is None

    def test_last_timestamp_after_add(self):
        hh = HeartbeatHistory()
        hh.add(1000.0)
        assert hh.last_timestamp == 1000.0

    def test_mean_empty(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        assert hh.mean == 0.0

    def test_mean_with_intervals(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        hh._add_interval(100.0)
        hh._add_interval(200.0)
        hh._add_interval(300.0)
        assert hh.mean == 200.0

    def test_variance_empty(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        assert hh.variance == 0.0

    def test_variance_with_intervals(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        hh._add_interval(100.0)
        hh._add_interval(200.0)
        hh._add_interval(300.0)
        expected_variance = ((100-200)**2 + (200-200)**2 + (300-200)**2) / 3
        assert abs(hh.variance - expected_variance) < 0.001

    def test_std_deviation(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        hh._add_interval(100.0)
        hh._add_interval(200.0)
        hh._add_interval(300.0)
        from math import sqrt
        expected_std = sqrt(hh.variance)
        assert abs(hh.std_deviation - expected_std) < 0.001

    def test_add_first_timestamp(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        initial_len = len(hh._intervals)
        hh.add(1000.0)
        assert hh.last_timestamp == 1000.0
        assert len(hh._intervals) == initial_len

    def test_add_subsequent_timestamp(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        hh.add(1000.0)
        hh.add(1500.0)
        assert 500.0 in hh._intervals

    def test_add_negative_interval_ignored(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        hh.add(1500.0)
        initial_len = len(hh._intervals)
        hh.add(1000.0)
        assert len(hh._intervals) == initial_len

    def test_add_interval_rolling_window(self):
        hh = HeartbeatHistory(max_sample_size=3, first_heartbeat_estimate_ms=0)
        hh._add_interval(100.0)
        hh._add_interval(200.0)
        hh._add_interval(300.0)
        hh._add_interval(400.0)

        assert len(hh._intervals) == 3
        assert 100.0 not in hh._intervals
        assert 400.0 in hh._intervals


class TestHeartbeatManager:
    """Tests for HeartbeatManager class."""

    def test_init(self):
        fd = FailureDetector()
        send_hb = MagicMock()
        on_fail = MagicMock()

        hm = HeartbeatManager(fd, send_hb, on_fail)

        assert hm._failure_detector is fd
        assert hm._send_heartbeat is send_hb
        assert hm._on_connection_failed is on_fail

    def test_failure_detector_property(self):
        fd = FailureDetector()
        hm = HeartbeatManager(fd, MagicMock(), MagicMock())

        assert hm.failure_detector is fd

    def test_add_connection(self):
        fd = FailureDetector()
        hm = HeartbeatManager(fd, MagicMock(), MagicMock())

        conn = MagicMock()
        conn.connection_id = 1

        hm.add_connection(conn)

        assert 1 in hm._connections
        assert 1 in fd._last_heartbeat

    def test_remove_connection(self):
        fd = FailureDetector()
        hm = HeartbeatManager(fd, MagicMock(), MagicMock())

        conn = MagicMock()
        conn.connection_id = 1
        hm._connections[1] = conn
        fd._last_heartbeat[1] = 1000.0

        hm.remove_connection(conn)

        assert 1 not in hm._connections
        assert 1 not in fd._last_heartbeat

    def test_start_creates_task(self):
        fd = FailureDetector()
        hm = HeartbeatManager(fd, MagicMock(), MagicMock())

        with patch("asyncio.create_task") as mock_create:
            hm.start()

            assert hm._running is True
            mock_create.assert_called_once()

    def test_start_already_running(self):
        fd = FailureDetector()
        hm = HeartbeatManager(fd, MagicMock(), MagicMock())
        hm._running = True

        with patch("asyncio.create_task") as mock_create:
            hm.start()

            mock_create.assert_not_called()

    @pytest.mark.asyncio
    async def test_stop_cancels_task(self):
        fd = FailureDetector()
        hm = HeartbeatManager(fd, MagicMock(), MagicMock())

        async def dummy():
            await asyncio.sleep(10)

        hm._running = True
        hm._task = asyncio.create_task(dummy())

        await hm.stop()

        assert hm._running is False
        assert hm._task is None

    @pytest.mark.asyncio
    async def test_stop_not_running(self):
        fd = FailureDetector()
        hm = HeartbeatManager(fd, MagicMock(), MagicMock())

        await hm.stop()

        assert hm._running is False


class TestCanUseRawSockets:
    """Tests for _can_use_raw_sockets function."""

    def test_can_use_raw_sockets_windows(self):
        with patch("platform.system", return_value="Windows"):
            assert _can_use_raw_sockets() is True

    def test_can_use_raw_sockets_root_user(self):
        with patch("platform.system", return_value="Linux"):
            with patch("os.geteuid", return_value=0):
                assert _can_use_raw_sockets() is True

    def test_can_use_raw_sockets_raw_socket_available(self):
        mock_sock = MagicMock()

        with patch("platform.system", return_value="Linux"):
            with patch("os.geteuid", return_value=1000):
                with patch("socket.socket", return_value=mock_sock):
                    assert _can_use_raw_sockets() is True
                    mock_sock.close.assert_called()

    def test_can_use_raw_sockets_dgram_fallback(self):
        mock_sock = MagicMock()

        def socket_side_effect(family, sock_type, proto):
            if sock_type == socket.SOCK_RAW:
                raise PermissionError()
            return mock_sock

        with patch("platform.system", return_value="Linux"):
            with patch("os.geteuid", return_value=1000):
                with patch("socket.socket", side_effect=socket_side_effect):
                    assert _can_use_raw_sockets() is True

    def test_can_use_raw_sockets_none_available(self):
        with patch("platform.system", return_value="Linux"):
            with patch("os.geteuid", return_value=1000):
                with patch("socket.socket", side_effect=PermissionError()):
                    assert _can_use_raw_sockets() is False


class TestCalculateChecksum:
    """Tests for _calculate_checksum function."""

    def test_calculate_checksum_even_length(self):
        data = b"\x08\x00\x00\x00\x00\x01\x00\x01"
        checksum = _calculate_checksum(data)
        assert isinstance(checksum, int)
        assert 0 <= checksum <= 0xFFFF

    def test_calculate_checksum_odd_length(self):
        data = b"\x08\x00\x00\x00\x00\x01\x00"
        checksum = _calculate_checksum(data)
        assert isinstance(checksum, int)
        assert 0 <= checksum <= 0xFFFF


class TestRoutingMode:
    """Tests for RoutingMode enum."""

    def test_all_members_value(self):
        assert RoutingMode.ALL_MEMBERS.value == "ALL_MEMBERS"

    def test_single_member_value(self):
        assert RoutingMode.SINGLE_MEMBER.value == "SINGLE_MEMBER"

    def test_multi_member_value(self):
        assert RoutingMode.MULTI_MEMBER.value == "MULTI_MEMBER"


class TestRoundRobinLoadBalancer:
    """Tests for RoundRobinLoadBalancer class."""

    def test_init(self):
        lb = RoundRobinLoadBalancer()
        assert lb._connections == []
        assert lb._index == 0

    def test_init_with_connections(self):
        lb = RoundRobinLoadBalancer()
        conn1 = MagicMock()
        conn1.is_alive = True
        conn2 = MagicMock()
        conn2.is_alive = True

        lb.init([conn1, conn2])

        assert len(lb._connections) == 2

    def test_next_empty_connections(self):
        lb = RoundRobinLoadBalancer()
        assert lb.next() is None

    def test_next_no_alive_connections(self):
        lb = RoundRobinLoadBalancer()
        conn = MagicMock()
        conn.is_alive = False
        lb._connections = [conn]

        assert lb.next() is None

    def test_next_round_robin(self):
        lb = RoundRobinLoadBalancer()
        conn1 = MagicMock()
        conn1.is_alive = True
        conn2 = MagicMock()
        conn2.is_alive = True
        conn3 = MagicMock()
        conn3.is_alive = True

        lb.init([conn1, conn2, conn3])

        assert lb.next() is conn1
        assert lb.next() is conn2
        assert lb.next() is conn3
        assert lb.next() is conn1

    def test_next_skips_dead_connections(self):
        lb = RoundRobinLoadBalancer()
        conn1 = MagicMock()
        conn1.is_alive = False
        conn2 = MagicMock()
        conn2.is_alive = True

        lb.init([conn1, conn2])

        assert lb.next() is conn2

    def test_can_get_next_empty(self):
        lb = RoundRobinLoadBalancer()
        assert lb.can_get_next() is False

    def test_can_get_next_all_dead(self):
        lb = RoundRobinLoadBalancer()
        conn = MagicMock()
        conn.is_alive = False
        lb._connections = [conn]

        assert lb.can_get_next() is False

    def test_can_get_next_some_alive(self):
        lb = RoundRobinLoadBalancer()
        conn = MagicMock()
        conn.is_alive = True
        lb._connections = [conn]

        assert lb.can_get_next() is True


class TestRandomLoadBalancer:
    """Tests for RandomLoadBalancer class."""

    def test_init(self):
        lb = RandomLoadBalancer()
        assert lb._connections == []

    def test_init_with_connections(self):
        lb = RandomLoadBalancer()
        conn1 = MagicMock()
        conn1.is_alive = True
        conn2 = MagicMock()
        conn2.is_alive = True

        lb.init([conn1, conn2])

        assert len(lb._connections) == 2

    def test_next_empty_connections(self):
        lb = RandomLoadBalancer()
        assert lb.next() is None

    def test_next_no_alive_connections(self):
        lb = RandomLoadBalancer()
        conn = MagicMock()
        conn.is_alive = False
        lb._connections = [conn]

        assert lb.next() is None

    def test_next_returns_alive_connection(self):
        lb = RandomLoadBalancer()
        conn1 = MagicMock()
        conn1.is_alive = True
        conn2 = MagicMock()
        conn2.is_alive = True

        lb.init([conn1, conn2])

        result = lb.next()
        assert result in [conn1, conn2]

    def test_can_get_next_empty(self):
        lb = RandomLoadBalancer()
        assert lb.can_get_next() is False

    def test_can_get_next_all_dead(self):
        lb = RandomLoadBalancer()
        conn = MagicMock()
        conn.is_alive = False
        lb._connections = [conn]

        assert lb.can_get_next() is False

    def test_can_get_next_some_alive(self):
        lb = RandomLoadBalancer()
        conn = MagicMock()
        conn.is_alive = True
        lb._connections = [conn]

        assert lb.can_get_next() is True


class TestConnectionManager:
    """Tests for ConnectionManager class."""

    def test_init_defaults(self):
        cm = ConnectionManager(addresses=["localhost:5701"])

        assert cm._routing_mode == RoutingMode.ALL_MEMBERS
        assert cm._connection_timeout == 5.0
        assert cm._running is False

    def test_init_custom_values(self):
        cm = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
            connection_timeout=10.0,
            heartbeat_interval=10.0,
            heartbeat_timeout=120.0,
        )

        assert cm._routing_mode == RoutingMode.SINGLE_MEMBER
        assert cm._connection_timeout == 10.0

    def test_routing_mode_property(self):
        cm = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.MULTI_MEMBER,
        )

        assert cm.routing_mode == RoutingMode.MULTI_MEMBER

    def test_is_running_property(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        assert cm.is_running is False

    def test_connection_count_empty(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        assert cm.connection_count == 0

    def test_connection_count_with_connections(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        conn1 = MagicMock()
        conn1.is_alive = True
        conn2 = MagicMock()
        conn2.is_alive = False

        cm._connections = {1: conn1, 2: conn2}

        assert cm.connection_count == 1

    def test_set_connection_listener(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        on_opened = MagicMock()
        on_closed = MagicMock()

        cm.set_connection_listener(on_opened, on_closed)

        assert cm._on_connection_opened is on_opened
        assert cm._on_connection_closed is on_closed

    def test_set_message_callback(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        callback = MagicMock()

        cm.set_message_callback(callback)

        assert cm._message_callback is callback

    @pytest.mark.asyncio
    async def test_start_already_running(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        cm._running = True

        await cm.start()

    @pytest.mark.asyncio
    async def test_start_no_connections_raises(self):
        cm = ConnectionManager(addresses=["nonexistent:5701"])

        with patch.object(cm, "_connect_to_cluster", new_callable=AsyncMock):
            with pytest.raises(ClientOfflineException):
                await cm.start()

    @pytest.mark.asyncio
    async def test_shutdown(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        cm._running = True

        conn = MagicMock()
        conn.close = AsyncMock()
        cm._connections = {1: conn}

        hm = MagicMock()
        hm.stop = AsyncMock()
        cm._heartbeat_manager = hm

        await cm.shutdown()

        assert cm._running is False
        conn.close.assert_called_once()
        hm.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_not_running(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        await cm.shutdown()

    def test_get_connection_single_member_mode(self):
        cm = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )

        conn = MagicMock()
        conn.is_alive = True
        cm._connections = {1: conn}

        result = cm.get_connection()
        assert result is conn

    def test_get_connection_load_balanced(self):
        cm = ConnectionManager(addresses=["localhost:5701"])

        conn = MagicMock()
        conn.is_alive = True
        cm._load_balancer.init([conn])

        result = cm.get_connection()
        assert result is conn

    def test_get_connection_for_address_exists(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        addr = Address("localhost", 5701)

        conn = MagicMock()
        conn.is_alive = True
        cm._address_connections[addr] = conn

        result = cm.get_connection_for_address(addr)
        assert result is conn

    def test_get_connection_for_address_not_exists(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        addr = Address("localhost", 5701)

        result = cm.get_connection_for_address(addr)
        assert result is None

    def test_get_connection_for_address_dead(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        addr = Address("localhost", 5701)

        conn = MagicMock()
        conn.is_alive = False
        cm._address_connections[addr] = conn

        result = cm.get_connection_for_address(addr)
        assert result is None

    def test_get_all_connections(self):
        cm = ConnectionManager(addresses=["localhost:5701"])

        conn1 = MagicMock()
        conn1.is_alive = True
        conn2 = MagicMock()
        conn2.is_alive = False
        conn3 = MagicMock()
        conn3.is_alive = True

        cm._connections = {1: conn1, 2: conn2, 3: conn3}

        result = cm.get_all_connections()
        assert len(result) == 2
        assert conn1 in result
        assert conn3 in result

    def test_calculate_backoff_no_retry_config(self):
        cm = ConnectionManager(addresses=["localhost:5701"])

        backoff0 = cm._calculate_backoff(0)
        backoff1 = cm._calculate_backoff(1)
        backoff2 = cm._calculate_backoff(2)

        assert backoff0 == 1.0
        assert backoff1 == 2.0
        assert backoff2 == 4.0

    def test_calculate_backoff_with_retry_config(self):
        retry_config = MagicMock()
        retry_config.initial_backoff = 0.5
        retry_config.multiplier = 2.0
        retry_config.max_backoff = 10.0
        retry_config.jitter = 0.0

        cm = ConnectionManager(
            addresses=["localhost:5701"],
            retry_config=retry_config,
        )

        backoff = cm._calculate_backoff(0)
        assert backoff == 0.5

    def test_calculate_backoff_with_jitter(self):
        retry_config = MagicMock()
        retry_config.initial_backoff = 1.0
        retry_config.multiplier = 1.0
        retry_config.max_backoff = 10.0
        retry_config.jitter = 0.5

        cm = ConnectionManager(
            addresses=["localhost:5701"],
            retry_config=retry_config,
        )

        with patch("random.random", return_value=1.0):
            backoff = cm._calculate_backoff(0)
            assert backoff == 1.5

    @pytest.mark.asyncio
    async def test_connect_to_address_reuses_existing(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        addr = Address("localhost", 5701)

        conn = MagicMock()
        conn.is_alive = True
        cm._address_connections[addr] = conn

        result = await cm._connect_to_address(addr)
        assert result is conn

    @pytest.mark.asyncio
    async def test_connect_to_address_new_connection(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        addr = Address("localhost", 5701)

        mock_conn = MagicMock()
        mock_conn.connect = AsyncMock()
        mock_conn.start_reading = MagicMock()
        mock_conn.set_message_callback = MagicMock()
        mock_conn.connection_id = 1

        with patch("hazelcast.network.connection_manager.Connection", return_value=mock_conn):
            result = await cm._connect_to_address(addr)

        assert result is mock_conn
        mock_conn.connect.assert_called_once()

    def test_on_connection_closed(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        conn = MagicMock()
        conn.connection_id = 1
        conn.address = Address("localhost", 5701)

        with patch("asyncio.create_task"):
            cm.on_connection_closed(conn, "test reason")

    def test_on_heartbeat_failure(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        conn = MagicMock()
        conn.connection_id = 1
        conn.address = Address("localhost", 5701)

        with patch("asyncio.create_task"):
            cm._on_heartbeat_failure(conn, "timeout")

    def test_send_heartbeat(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        conn = MagicMock()
        conn.connection_id = 1
        conn.send_sync = MagicMock()

        cm._send_heartbeat(conn)

        conn.send_sync.assert_called_once()

    def test_send_heartbeat_failure(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        conn = MagicMock()
        conn.connection_id = 1
        conn.send_sync = MagicMock(side_effect=Exception("send failed"))

        cm._send_heartbeat(conn)
