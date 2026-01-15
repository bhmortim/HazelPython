"""Unit tests for security components."""

import ssl
import socket
import unittest
from unittest.mock import Mock, patch, MagicMock

from hazelcast.network.ssl_config import SSLConfig, SSLProtocol
from hazelcast.network.socket_interceptor import (
    NoOpSocketInterceptor,
    SocketOptionsInterceptor,
)
from hazelcast.auth import (
    CredentialsType,
    UsernamePasswordCredentials,
    TokenCredentials,
    KerberosCredentials,
    CustomCredentials,
    StaticCredentialsFactory,
    CallableCredentialsFactory,
    AuthenticationService,
)
from hazelcast.config import SecurityConfig


class TestSSLProtocol(unittest.TestCase):
    """Tests for SSLProtocol enum."""

    def test_protocol_values(self):
        self.assertEqual(SSLProtocol.TLS.value, "TLS")
        self.assertEqual(SSLProtocol.TLSv1.value, "TLSv1")
        self.assertEqual(SSLProtocol.TLSv1_1.value, "TLSv1.1")
        self.assertEqual(SSLProtocol.TLSv1_2.value, "TLSv1.2")
        self.assertEqual(SSLProtocol.TLSv1_3.value, "TLSv1.3")


class TestSSLConfig(unittest.TestCase):
    """Tests for SSLConfig class."""

    def test_default_values(self):
        config = SSLConfig()
        self.assertFalse(config.enabled)
        self.assertIsNone(config.cafile)
        self.assertIsNone(config.capath)
        self.assertIsNone(config.certfile)
        self.assertIsNone(config.keyfile)
        self.assertIsNone(config.keyfile_password)
        self.assertEqual(config.protocol, SSLProtocol.TLS)
        self.assertTrue(config.check_hostname)
        self.assertEqual(config.verify_mode, ssl.CERT_REQUIRED)
        self.assertIsNone(config.ciphers)

    def test_custom_values(self):
        config = SSLConfig(
            enabled=True,
            cafile="/path/to/ca.pem",
            capath="/path/to/certs",
            certfile="/path/to/cert.pem",
            keyfile="/path/to/key.pem",
            keyfile_password="secret",
            protocol=SSLProtocol.TLSv1_3,
            check_hostname=False,
            verify_mode=ssl.CERT_OPTIONAL,
            ciphers="HIGH:!aNULL",
        )
        self.assertTrue(config.enabled)
        self.assertEqual(config.cafile, "/path/to/ca.pem")
        self.assertEqual(config.capath, "/path/to/certs")
        self.assertEqual(config.certfile, "/path/to/cert.pem")
        self.assertEqual(config.keyfile, "/path/to/key.pem")
        self.assertEqual(config.keyfile_password, "secret")
        self.assertEqual(config.protocol, SSLProtocol.TLSv1_3)
        self.assertFalse(config.check_hostname)
        self.assertEqual(config.verify_mode, ssl.CERT_OPTIONAL)
        self.assertEqual(config.ciphers, "HIGH:!aNULL")

    def test_property_setters(self):
        config = SSLConfig()

        config.enabled = True
        self.assertTrue(config.enabled)

        config.cafile = "/ca.pem"
        self.assertEqual(config.cafile, "/ca.pem")

        config.capath = "/certs"
        self.assertEqual(config.capath, "/certs")

        config.certfile = "/cert.pem"
        self.assertEqual(config.certfile, "/cert.pem")

        config.keyfile = "/key.pem"
        self.assertEqual(config.keyfile, "/key.pem")

        config.keyfile_password = "pass"
        self.assertEqual(config.keyfile_password, "pass")

        config.protocol = SSLProtocol.TLSv1_2
        self.assertEqual(config.protocol, SSLProtocol.TLSv1_2)

        config.check_hostname = False
        self.assertFalse(config.check_hostname)

        config.verify_mode = ssl.CERT_NONE
        self.assertEqual(config.verify_mode, ssl.CERT_NONE)

        config.ciphers = "AES256"
        self.assertEqual(config.ciphers, "AES256")

    def test_from_dict_defaults(self):
        config = SSLConfig.from_dict({})
        self.assertFalse(config.enabled)
        self.assertEqual(config.protocol, SSLProtocol.TLS)
        self.assertEqual(config.verify_mode, ssl.CERT_REQUIRED)

    def test_from_dict_custom(self):
        data = {
            "enabled": True,
            "cafile": "/ca.pem",
            "capath": "/certs",
            "certfile": "/cert.pem",
            "keyfile": "/key.pem",
            "keyfile_password": "secret",
            "protocol": "TLSv1.3",
            "check_hostname": False,
            "verify_mode": "CERT_OPTIONAL",
            "ciphers": "HIGH",
        }
        config = SSLConfig.from_dict(data)
        self.assertTrue(config.enabled)
        self.assertEqual(config.cafile, "/ca.pem")
        self.assertEqual(config.capath, "/certs")
        self.assertEqual(config.certfile, "/cert.pem")
        self.assertEqual(config.keyfile, "/key.pem")
        self.assertEqual(config.keyfile_password, "secret")
        self.assertEqual(config.protocol, SSLProtocol.TLSv1_3)
        self.assertFalse(config.check_hostname)
        self.assertEqual(config.verify_mode, ssl.CERT_OPTIONAL)
        self.assertEqual(config.ciphers, "HIGH")

    def test_from_dict_invalid_protocol(self):
        config = SSLConfig.from_dict({"protocol": "INVALID"})
        self.assertEqual(config.protocol, SSLProtocol.TLS)

    def test_from_dict_invalid_verify_mode(self):
        config = SSLConfig.from_dict({"verify_mode": "INVALID"})
        self.assertEqual(config.verify_mode, ssl.CERT_REQUIRED)

    @patch("ssl.SSLContext")
    def test_create_ssl_context_basic(self, mock_context_class):
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context

        config = SSLConfig(enabled=True, verify_mode=ssl.CERT_NONE)
        config.create_ssl_context()

        mock_context_class.assert_called_once()

    @patch("ssl.SSLContext")
    def test_create_ssl_context_with_ca(self, mock_context_class):
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context

        config = SSLConfig(
            enabled=True,
            cafile="/ca.pem",
            verify_mode=ssl.CERT_REQUIRED,
        )
        config.create_ssl_context()

        mock_context.load_verify_locations.assert_called_once_with(
            cafile="/ca.pem",
            capath=None,
        )

    @patch("ssl.SSLContext")
    def test_create_ssl_context_with_cert(self, mock_context_class):
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context

        config = SSLConfig(
            enabled=True,
            certfile="/cert.pem",
            keyfile="/key.pem",
            keyfile_password="secret",
            verify_mode=ssl.CERT_NONE,
        )
        config.create_ssl_context()

        mock_context.load_cert_chain.assert_called_once_with(
            certfile="/cert.pem",
            keyfile="/key.pem",
            password="secret",
        )

    @patch("ssl.SSLContext")
    def test_create_ssl_context_with_ciphers(self, mock_context_class):
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context

        config = SSLConfig(
            enabled=True,
            ciphers="HIGH:!aNULL",
            verify_mode=ssl.CERT_NONE,
        )
        config.create_ssl_context()

        mock_context.set_ciphers.assert_called_once_with("HIGH:!aNULL")


class TestSocketInterceptor(unittest.TestCase):
    """Tests for SocketInterceptor classes."""

    def test_noop_interceptor(self):
        interceptor = NoOpSocketInterceptor()
        mock_sock = Mock(spec=socket.socket)

        result = interceptor.intercept(mock_sock)

        self.assertIs(result, mock_sock)

    def test_socket_options_interceptor_defaults(self):
        interceptor = SocketOptionsInterceptor()
        mock_sock = Mock(spec=socket.socket)

        result = interceptor.intercept(mock_sock)

        self.assertIs(result, mock_sock)
        mock_sock.setsockopt.assert_any_call(
            socket.IPPROTO_TCP, socket.TCP_NODELAY, 1
        )
        mock_sock.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1
        )

    def test_socket_options_interceptor_disabled(self):
        interceptor = SocketOptionsInterceptor(
            tcp_nodelay=False,
            keep_alive=False,
        )
        mock_sock = Mock(spec=socket.socket)

        result = interceptor.intercept(mock_sock)

        self.assertIs(result, mock_sock)
        mock_sock.setsockopt.assert_not_called()

    def test_socket_options_interceptor_buffer_sizes(self):
        interceptor = SocketOptionsInterceptor(
            tcp_nodelay=False,
            keep_alive=False,
            receive_buffer_size=65536,
            send_buffer_size=32768,
        )
        mock_sock = Mock(spec=socket.socket)

        interceptor.intercept(mock_sock)

        mock_sock.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_RCVBUF, 65536
        )
        mock_sock.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_SNDBUF, 32768
        )

    def test_socket_interceptor_callbacks(self):
        interceptor = NoOpSocketInterceptor()
        mock_sock = Mock(spec=socket.socket)

        interceptor.on_connect(mock_sock, ("localhost", 5701))
        interceptor.on_disconnect(mock_sock)


class TestCredentials(unittest.TestCase):
    """Tests for Credentials classes."""

    def test_username_password_credentials(self):
        creds = UsernamePasswordCredentials("admin", "secret")

        self.assertEqual(creds.credentials_type, CredentialsType.USERNAME_PASSWORD)
        self.assertEqual(creds.username, "admin")
        self.assertEqual(creds.password, "secret")

        data = creds.to_dict()
        self.assertEqual(data["type"], "USERNAME_PASSWORD")
        self.assertEqual(data["username"], "admin")
        self.assertEqual(data["password"], "secret")

        self.assertIn("admin", repr(creds))
        self.assertNotIn("secret", repr(creds))

    def test_token_credentials(self):
        creds = TokenCredentials("my-token-123")

        self.assertEqual(creds.credentials_type, CredentialsType.TOKEN)
        self.assertEqual(creds.token, "my-token-123")

        data = creds.to_dict()
        self.assertEqual(data["type"], "TOKEN")
        self.assertEqual(data["token"], "my-token-123")

        self.assertNotIn("my-token-123", repr(creds))
        self.assertIn("***", repr(creds))

    def test_kerberos_credentials(self):
        creds = KerberosCredentials(
            service_principal="hazelcast/server@REALM",
            spn="hazelcast",
            keytab_path="/etc/keytab",
        )

        self.assertEqual(creds.credentials_type, CredentialsType.KERBEROS)
        self.assertEqual(creds.service_principal, "hazelcast/server@REALM")
        self.assertEqual(creds.spn, "hazelcast")
        self.assertEqual(creds.keytab_path, "/etc/keytab")

        data = creds.to_dict()
        self.assertEqual(data["type"], "KERBEROS")
        self.assertEqual(data["service_principal"], "hazelcast/server@REALM")

    def test_custom_credentials(self):
        custom_data = {"key1": "value1", "key2": 123}
        creds = CustomCredentials(custom_data)

        self.assertEqual(creds.credentials_type, CredentialsType.CUSTOM)
        self.assertEqual(creds.data, custom_data)

        data = creds.to_dict()
        self.assertEqual(data["type"], "CUSTOM")
        self.assertEqual(data["data"], custom_data)


class TestCredentialsFactory(unittest.TestCase):
    """Tests for CredentialsFactory classes."""

    def test_static_credentials_factory(self):
        creds = UsernamePasswordCredentials("user", "pass")
        factory = StaticCredentialsFactory(creds)

        result = factory.create_credentials()

        self.assertIs(result, creds)

    def test_callable_credentials_factory(self):
        call_count = [0]

        def create_creds():
            call_count[0] += 1
            return TokenCredentials(f"token-{call_count[0]}")

        factory = CallableCredentialsFactory(create_creds)

        creds1 = factory.create_credentials()
        creds2 = factory.create_credentials()

        self.assertEqual(creds1.token, "token-1")
        self.assertEqual(creds2.token, "token-2")
        self.assertEqual(call_count[0], 2)


class TestAuthenticationService(unittest.TestCase):
    """Tests for AuthenticationService class."""

    def test_initial_state(self):
        service = AuthenticationService()

        self.assertFalse(service.is_authenticated)
        self.assertIsNone(service.cluster_uuid)
        self.assertIsNone(service.member_uuid)
        self.assertIsNone(service.get_credentials())

    def test_with_static_credentials(self):
        creds = UsernamePasswordCredentials("admin", "pass")
        service = AuthenticationService(credentials=creds)

        result = service.get_credentials()

        self.assertIs(result, creds)

    def test_with_credentials_factory(self):
        creds = TokenCredentials("token")
        factory = StaticCredentialsFactory(creds)
        service = AuthenticationService(credentials_factory=factory)

        result = service.get_credentials()

        self.assertIs(result, creds)

    def test_factory_takes_precedence(self):
        static_creds = UsernamePasswordCredentials("user", "pass")
        factory_creds = TokenCredentials("token")
        factory = StaticCredentialsFactory(factory_creds)

        service = AuthenticationService(
            credentials=static_creds,
            credentials_factory=factory,
        )

        result = service.get_credentials()

        self.assertIs(result, factory_creds)

    def test_set_credentials(self):
        service = AuthenticationService()
        creds = UsernamePasswordCredentials("user", "pass")

        service.set_credentials(creds)

        self.assertIs(service.get_credentials(), creds)

    def test_set_credentials_clears_factory(self):
        factory = StaticCredentialsFactory(TokenCredentials("token"))
        service = AuthenticationService(credentials_factory=factory)

        new_creds = UsernamePasswordCredentials("user", "pass")
        service.set_credentials(new_creds)

        self.assertIs(service.get_credentials(), new_creds)

    def test_set_credentials_factory(self):
        service = AuthenticationService()
        creds = TokenCredentials("token")
        factory = StaticCredentialsFactory(creds)

        service.set_credentials_factory(factory)

        self.assertIs(service.get_credentials(), creds)

    def test_set_credentials_factory_clears_credentials(self):
        creds = UsernamePasswordCredentials("user", "pass")
        service = AuthenticationService(credentials=creds)

        factory_creds = TokenCredentials("token")
        factory = StaticCredentialsFactory(factory_creds)
        service.set_credentials_factory(factory)

        self.assertIs(service.get_credentials(), factory_creds)

    def test_mark_authenticated(self):
        service = AuthenticationService()

        service.mark_authenticated("cluster-123", "member-456")

        self.assertTrue(service.is_authenticated)
        self.assertEqual(service.cluster_uuid, "cluster-123")
        self.assertEqual(service.member_uuid, "member-456")

    def test_reset(self):
        service = AuthenticationService()
        service.mark_authenticated("cluster-123", "member-456")

        service.reset()

        self.assertFalse(service.is_authenticated)
        self.assertIsNone(service.cluster_uuid)
        self.assertIsNone(service.member_uuid)

    def test_from_config_with_token(self):
        security_config = SecurityConfig(token="my-token")

        service = AuthenticationService.from_config(security_config)
        creds = service.get_credentials()

        self.assertIsInstance(creds, TokenCredentials)
        self.assertEqual(creds.token, "my-token")

    def test_from_config_with_username_password(self):
        security_config = SecurityConfig(username="admin", password="secret")

        service = AuthenticationService.from_config(security_config)
        creds = service.get_credentials()

        self.assertIsInstance(creds, UsernamePasswordCredentials)
        self.assertEqual(creds.username, "admin")
        self.assertEqual(creds.password, "secret")

    def test_from_config_with_username_only(self):
        security_config = SecurityConfig(username="admin")

        service = AuthenticationService.from_config(security_config)
        creds = service.get_credentials()

        self.assertIsInstance(creds, UsernamePasswordCredentials)
        self.assertEqual(creds.username, "admin")
        self.assertEqual(creds.password, "")

    def test_from_config_empty(self):
        security_config = SecurityConfig()

        service = AuthenticationService.from_config(security_config)

        self.assertIsNone(service.get_credentials())


class TestSecurityConfig(unittest.TestCase):
    """Tests for SecurityConfig class."""

    def test_default_values(self):
        config = SecurityConfig()

        self.assertIsNone(config.username)
        self.assertIsNone(config.password)
        self.assertIsNone(config.token)
        self.assertFalse(config.is_configured)

    def test_username_password(self):
        config = SecurityConfig(username="admin", password="secret")

        self.assertEqual(config.username, "admin")
        self.assertEqual(config.password, "secret")
        self.assertTrue(config.is_configured)

    def test_token(self):
        config = SecurityConfig(token="my-token")

        self.assertEqual(config.token, "my-token")
        self.assertTrue(config.is_configured)

    def test_property_setters(self):
        config = SecurityConfig()

        config.username = "user"
        config.password = "pass"
        config.token = "token"

        self.assertEqual(config.username, "user")
        self.assertEqual(config.password, "pass")
        self.assertEqual(config.token, "token")

    def test_from_dict(self):
        data = {
            "username": "admin",
            "password": "secret",
            "token": "my-token",
        }

        config = SecurityConfig.from_dict(data)

        self.assertEqual(config.username, "admin")
        self.assertEqual(config.password, "secret")
        self.assertEqual(config.token, "my-token")


class TestSecurityModuleImports(unittest.TestCase):
    """Tests for hazelcast.security module imports."""

    def test_ssl_imports(self):
        from hazelcast.security import SSLConfig, SSLProtocol
        self.assertIsNotNone(SSLConfig)
        self.assertIsNotNone(SSLProtocol)

    def test_socket_interceptor_imports(self):
        from hazelcast.security import (
            SocketInterceptor,
            NoOpSocketInterceptor,
            SocketOptionsInterceptor,
        )
        self.assertIsNotNone(SocketInterceptor)
        self.assertIsNotNone(NoOpSocketInterceptor)
        self.assertIsNotNone(SocketOptionsInterceptor)

    def test_auth_imports(self):
        from hazelcast.security import (
            Credentials,
            CredentialsType,
            UsernamePasswordCredentials,
            TokenCredentials,
            KerberosCredentials,
            CustomCredentials,
            CredentialsFactory,
            StaticCredentialsFactory,
            CallableCredentialsFactory,
            AuthenticationService,
        )
        self.assertIsNotNone(Credentials)
        self.assertIsNotNone(CredentialsType)
        self.assertIsNotNone(UsernamePasswordCredentials)
        self.assertIsNotNone(TokenCredentials)
        self.assertIsNotNone(KerberosCredentials)
        self.assertIsNotNone(CustomCredentials)
        self.assertIsNotNone(CredentialsFactory)
        self.assertIsNotNone(StaticCredentialsFactory)
        self.assertIsNotNone(CallableCredentialsFactory)
        self.assertIsNotNone(AuthenticationService)


if __name__ == "__main__":
    unittest.main()
