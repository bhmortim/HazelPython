"""Tests for security and authentication."""

import pytest

from hazelcast.auth import (
    CredentialsType,
    Credentials,
    UsernamePasswordCredentials,
    TokenCredentials,
    KerberosCredentials,
    LdapCredentials,
    CustomCredentials,
    CredentialsFactory,
    StaticCredentialsFactory,
    CallableCredentialsFactory,
    AuthenticationService,
)
from hazelcast.config import SecurityConfig
from hazelcast.network.ssl_config import (
    TlsConfig,
    TlsProtocol,
    CertificateVerifyMode,
    DEFAULT_CIPHER_SUITES,
)
from hazelcast.exceptions import ConfigurationException


class TestUsernamePasswordCredentials:
    """Tests for UsernamePasswordCredentials."""

    def test_creation(self):
        creds = UsernamePasswordCredentials("user", "pass")
        assert creds.username == "user"
        assert creds.password == "pass"
        assert creds.credentials_type == CredentialsType.USERNAME_PASSWORD

    def test_to_dict(self):
        creds = UsernamePasswordCredentials("admin", "secret")
        result = creds.to_dict()
        assert result["type"] == "USERNAME_PASSWORD"
        assert result["username"] == "admin"
        assert result["password"] == "secret"

    def test_repr(self):
        creds = UsernamePasswordCredentials("user", "pass")
        assert "user" in repr(creds)
        assert "pass" not in repr(creds)


class TestTokenCredentials:
    """Tests for TokenCredentials."""

    def test_creation(self):
        creds = TokenCredentials("my-token-123")
        assert creds.token == "my-token-123"
        assert creds.credentials_type == CredentialsType.TOKEN

    def test_to_dict(self):
        creds = TokenCredentials("secret-token")
        result = creds.to_dict()
        assert result["type"] == "TOKEN"
        assert result["token"] == "secret-token"

    def test_repr_hides_token(self):
        creds = TokenCredentials("secret-token")
        assert "secret-token" not in repr(creds)
        assert "***" in repr(creds)


class TestKerberosCredentials:
    """Tests for KerberosCredentials."""

    def test_creation_minimal(self):
        creds = KerberosCredentials("hazelcast/server@REALM")
        assert creds.service_principal == "hazelcast/server@REALM"
        assert creds.spn is None
        assert creds.keytab_path is None
        assert creds.credentials_type == CredentialsType.KERBEROS

    def test_creation_full(self):
        creds = KerberosCredentials(
            service_principal="hazelcast/server@REALM",
            spn="hazelcast",
            keytab_path="/etc/keytabs/hazelcast.keytab",
        )
        assert creds.service_principal == "hazelcast/server@REALM"
        assert creds.spn == "hazelcast"
        assert creds.keytab_path == "/etc/keytabs/hazelcast.keytab"

    def test_to_dict(self):
        creds = KerberosCredentials(
            service_principal="hz/host@DOMAIN",
            spn="hz",
            keytab_path="/path/to/keytab",
        )
        result = creds.to_dict()
        assert result["type"] == "KERBEROS"
        assert result["service_principal"] == "hz/host@DOMAIN"
        assert result["spn"] == "hz"
        assert result["keytab_path"] == "/path/to/keytab"


class TestLdapCredentials:
    """Tests for LdapCredentials."""

    def test_creation_minimal(self):
        creds = LdapCredentials("user@domain.com", "password")
        assert creds.username == "user@domain.com"
        assert creds.password == "password"
        assert creds.base_dn is None
        assert creds.user_filter is None
        assert creds.url is None
        assert creds.credentials_type == CredentialsType.LDAP

    def test_creation_full(self):
        creds = LdapCredentials(
            username="cn=admin,dc=example,dc=com",
            password="secret",
            base_dn="dc=example,dc=com",
            user_filter="(uid={0})",
            url="ldaps://ldap.example.com:636",
        )
        assert creds.username == "cn=admin,dc=example,dc=com"
        assert creds.password == "secret"
        assert creds.base_dn == "dc=example,dc=com"
        assert creds.user_filter == "(uid={0})"
        assert creds.url == "ldaps://ldap.example.com:636"

    def test_to_dict(self):
        creds = LdapCredentials(
            username="user",
            password="pass",
            base_dn="dc=test,dc=com",
        )
        result = creds.to_dict()
        assert result["type"] == "LDAP"
        assert result["username"] == "user"
        assert result["password"] == "pass"
        assert result["base_dn"] == "dc=test,dc=com"

    def test_repr(self):
        creds = LdapCredentials("user", "pass", base_dn="dc=test")
        r = repr(creds)
        assert "user" in r
        assert "dc=test" in r
        assert "pass" not in r


class TestCustomCredentials:
    """Tests for CustomCredentials."""

    def test_creation(self):
        data = {"key": "value", "number": 42}
        creds = CustomCredentials(data)
        assert creds.data == data
        assert creds.credentials_type == CredentialsType.CUSTOM

    def test_to_dict(self):
        data = {"custom_field": "custom_value"}
        creds = CustomCredentials(data)
        result = creds.to_dict()
        assert result["type"] == "CUSTOM"
        assert result["data"] == data


class TestCredentialsFactory:
    """Tests for credential factories."""

    def test_static_factory(self):
        creds = TokenCredentials("static-token")
        factory = StaticCredentialsFactory(creds)
        assert factory.create_credentials() is creds
        assert factory.create_credentials() is creds

    def test_callable_factory(self):
        call_count = [0]

        def create():
            call_count[0] += 1
            return TokenCredentials(f"token-{call_count[0]}")

        factory = CallableCredentialsFactory(create)
        creds1 = factory.create_credentials()
        creds2 = factory.create_credentials()

        assert creds1.token == "token-1"
        assert creds2.token == "token-2"
        assert call_count[0] == 2


class TestAuthenticationService:
    """Tests for AuthenticationService."""

    def test_initial_state(self):
        service = AuthenticationService()
        assert not service.is_authenticated
        assert service.cluster_uuid is None
        assert service.member_uuid is None

    def test_with_static_credentials(self):
        creds = UsernamePasswordCredentials("user", "pass")
        service = AuthenticationService(credentials=creds)
        assert service.get_credentials() is creds

    def test_with_factory(self):
        creds = TokenCredentials("token")
        factory = StaticCredentialsFactory(creds)
        service = AuthenticationService(credentials_factory=factory)
        assert service.get_credentials() is creds

    def test_factory_takes_precedence(self):
        static_creds = UsernamePasswordCredentials("static", "pass")
        factory_creds = TokenCredentials("factory")
        factory = StaticCredentialsFactory(factory_creds)

        service = AuthenticationService(
            credentials=static_creds,
            credentials_factory=factory,
        )
        assert service.get_credentials() is factory_creds

    def test_set_credentials(self):
        service = AuthenticationService()
        creds = TokenCredentials("new-token")
        service.set_credentials(creds)
        assert service.get_credentials() is creds

    def test_set_credentials_clears_factory(self):
        factory = StaticCredentialsFactory(TokenCredentials("factory"))
        service = AuthenticationService(credentials_factory=factory)

        new_creds = UsernamePasswordCredentials("user", "pass")
        service.set_credentials(new_creds)
        assert service.get_credentials() is new_creds

    def test_set_factory(self):
        service = AuthenticationService()
        creds = TokenCredentials("token")
        factory = StaticCredentialsFactory(creds)
        service.set_credentials_factory(factory)
        assert service.get_credentials() is creds

    def test_mark_authenticated(self):
        service = AuthenticationService()
        service.mark_authenticated("cluster-uuid-123", "member-uuid-456")

        assert service.is_authenticated
        assert service.cluster_uuid == "cluster-uuid-123"
        assert service.member_uuid == "member-uuid-456"

    def test_reset(self):
        service = AuthenticationService()
        service.mark_authenticated("cluster", "member")
        service.reset()

        assert not service.is_authenticated
        assert service.cluster_uuid is None
        assert service.member_uuid is None

    def test_from_config_with_token(self):
        config = SecurityConfig(token="config-token")
        service = AuthenticationService.from_config(config)
        creds = service.get_credentials()

        assert isinstance(creds, TokenCredentials)
        assert creds.token == "config-token"

    def test_from_config_with_username(self):
        config = SecurityConfig(username="admin", password="secret")
        service = AuthenticationService.from_config(config)
        creds = service.get_credentials()

        assert isinstance(creds, UsernamePasswordCredentials)
        assert creds.username == "admin"
        assert creds.password == "secret"

    def test_from_config_empty(self):
        config = SecurityConfig()
        service = AuthenticationService.from_config(config)
        assert service.get_credentials() is None


class TestTlsConfig:
    """Tests for TlsConfig."""

    def test_defaults(self):
        config = TlsConfig()
        assert not config.enabled
        assert config.ca_cert_path is None
        assert config.client_cert_path is None
        assert config.client_key_path is None
        assert config.verify_mode == CertificateVerifyMode.REQUIRED
        assert config.check_hostname is True
        assert config.protocol == TlsProtocol.TLS_1_2
        assert not config.mutual_tls_enabled

    def test_basic_tls(self):
        config = TlsConfig(
            enabled=True,
            ca_cert_path="/path/to/ca.pem",
        )
        assert config.enabled
        assert config.ca_cert_path == "/path/to/ca.pem"
        assert not config.mutual_tls_enabled

    def test_mutual_tls(self):
        config = TlsConfig(
            enabled=True,
            ca_cert_path="/path/to/ca.pem",
            client_cert_path="/path/to/client.pem",
            client_key_path="/path/to/client-key.pem",
        )
        assert config.mutual_tls_enabled

    def test_validation_client_key_without_cert(self):
        with pytest.raises(ConfigurationException) as exc_info:
            TlsConfig(
                enabled=True,
                ca_cert_path="/path/to/ca.pem",
                client_key_path="/path/to/key.pem",
            )
        assert "client_cert_path is required" in str(exc_info.value)

    def test_validation_client_cert_without_key(self):
        with pytest.raises(ConfigurationException) as exc_info:
            TlsConfig(
                enabled=True,
                ca_cert_path="/path/to/ca.pem",
                client_cert_path="/path/to/cert.pem",
            )
        assert "client_key_path is required" in str(exc_info.value)

    def test_validation_required_without_ca(self):
        with pytest.raises(ConfigurationException) as exc_info:
            TlsConfig(
                enabled=True,
                verify_mode=CertificateVerifyMode.REQUIRED,
            )
        assert "ca_cert_path is required" in str(exc_info.value)

    def test_no_validation_when_disabled(self):
        config = TlsConfig(
            enabled=False,
            client_key_path="/key.pem",
        )
        assert not config.enabled

    def test_cipher_suites(self):
        custom_ciphers = ["ECDHE-RSA-AES256-GCM-SHA384"]
        config = TlsConfig(
            enabled=True,
            ca_cert_path="/ca.pem",
            cipher_suites=custom_ciphers,
        )
        assert config.cipher_suites == custom_ciphers

    def test_default_cipher_suites(self):
        config = TlsConfig()
        assert config.cipher_suites == DEFAULT_CIPHER_SUITES

    def test_protocol_versions(self):
        for protocol in TlsProtocol:
            config = TlsConfig(
                enabled=True,
                ca_cert_path="/ca.pem",
                protocol=protocol,
            )
            assert config.protocol == protocol

    def test_verify_modes(self):
        config = TlsConfig(
            enabled=True,
            verify_mode=CertificateVerifyMode.NONE,
        )
        assert config.verify_mode == CertificateVerifyMode.NONE

        config = TlsConfig(
            enabled=True,
            ca_cert_path="/ca.pem",
            verify_mode=CertificateVerifyMode.OPTIONAL,
        )
        assert config.verify_mode == CertificateVerifyMode.OPTIONAL

    def test_from_dict_minimal(self):
        data = {"enabled": True, "ca_cert_path": "/ca.pem"}
        config = TlsConfig.from_dict(data)
        assert config.enabled
        assert config.ca_cert_path == "/ca.pem"

    def test_from_dict_full(self):
        data = {
            "enabled": True,
            "ca_cert_path": "/ca.pem",
            "client_cert_path": "/client.pem",
            "client_key_path": "/client-key.pem",
            "client_key_password": "secret",
            "verify_mode": "OPTIONAL",
            "check_hostname": False,
            "cipher_suites": ["ECDHE-RSA-AES256-GCM-SHA384"],
            "protocol": "TLSv1.3",
        }
        config = TlsConfig.from_dict(data)
        assert config.enabled
        assert config.ca_cert_path == "/ca.pem"
        assert config.client_cert_path == "/client.pem"
        assert config.client_key_path == "/client-key.pem"
        assert config.client_key_password == "secret"
        assert config.verify_mode == CertificateVerifyMode.OPTIONAL
        assert config.check_hostname is False
        assert config.cipher_suites == ["ECDHE-RSA-AES256-GCM-SHA384"]
        assert config.protocol == TlsProtocol.TLS_1_3

    def test_from_dict_invalid_verify_mode(self):
        data = {"enabled": True, "verify_mode": "INVALID"}
        with pytest.raises(ConfigurationException) as exc_info:
            TlsConfig.from_dict(data)
        assert "Invalid verify_mode" in str(exc_info.value)

    def test_repr(self):
        config = TlsConfig(
            enabled=True,
            ca_cert_path="/ca.pem",
            client_cert_path="/client.pem",
            client_key_path="/client-key.pem",
        )
        r = repr(config)
        assert "enabled=True" in r
        assert "mutual_tls=True" in r


class TestSecurityConfigWithTls:
    """Tests for SecurityConfig with TLS."""

    def test_default_tls(self):
        config = SecurityConfig()
        assert config.tls is not None
        assert not config.tls_enabled

    def test_with_tls(self):
        tls = TlsConfig(enabled=True, ca_cert_path="/ca.pem")
        config = SecurityConfig(username="user", password="pass", tls=tls)
        assert config.tls_enabled
        assert config.tls.ca_cert_path == "/ca.pem"

    def test_from_dict_with_tls(self):
        data = {
            "username": "admin",
            "password": "secret",
            "tls": {
                "enabled": True,
                "ca_cert_path": "/ca.pem",
                "verify_mode": "REQUIRED",
            },
        }
        config = SecurityConfig.from_dict(data)
        assert config.username == "admin"
        assert config.tls_enabled
        assert config.tls.ca_cert_path == "/ca.pem"

    def test_from_dict_without_tls(self):
        data = {"username": "user"}
        config = SecurityConfig.from_dict(data)
        assert not config.tls_enabled
