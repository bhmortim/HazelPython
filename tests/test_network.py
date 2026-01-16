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
from hazelcast.exceptions import ConfigurationException, HazelcastException, TargetDisconnectedException


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
