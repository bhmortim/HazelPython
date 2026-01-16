"""Unit tests for hazelcast.auth and hazelcast.network.ssl_config modules."""

import pytest
import ssl
from unittest.mock import MagicMock, patch

from hazelcast.auth import (
    Credentials,
    CredentialsType,
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
from hazelcast.network.ssl_config import (
    TlsConfig,
    TlsProtocol,
    CertificateVerifyMode,
    DEFAULT_CIPHER_SUITES,
)
from hazelcast.exceptions import ConfigurationException


class TestUsernamePasswordCredentials:
    """Tests for UsernamePasswordCredentials."""

    def test_create(self):
        creds = UsernamePasswordCredentials("admin", "secret")
        assert creds.username == "admin"
        assert creds.password == "secret"

    def test_credentials_type(self):
        creds = UsernamePasswordCredentials("u", "p")
        assert creds.credentials_type == CredentialsType.USERNAME_PASSWORD

    def test_to_dict(self):
        creds = UsernamePasswordCredentials("user", "pass")
        d = creds.to_dict()
        assert d["type"] == "USERNAME_PASSWORD"
        assert d["username"] == "user"
        assert d["password"] == "pass"

    def test_repr(self):
        creds = UsernamePasswordCredentials("admin", "secret")
        repr_str = repr(creds)
        assert "UsernamePasswordCredentials" in repr_str
        assert "admin" in repr_str
        assert "secret" not in repr_str  # Password should not be in repr


class TestTokenCredentials:
    """Tests for TokenCredentials."""

    def test_create(self):
        creds = TokenCredentials("my-token-123")
        assert creds.token == "my-token-123"

    def test_credentials_type(self):
        creds = TokenCredentials("tok")
        assert creds.credentials_type == CredentialsType.TOKEN

    def test_to_dict(self):
        creds = TokenCredentials("abc")
        d = creds.to_dict()
        assert d["type"] == "TOKEN"
        assert d["token"] == "abc"

    def test_repr_hides_token(self):
        creds = TokenCredentials("secret-token")
        repr_str = repr(creds)
        assert "TokenCredentials" in repr_str
        assert "secret-token" not in repr_str
        assert "***" in repr_str


class TestKerberosCredentials:
    """Tests for KerberosCredentials."""

    def test_create_basic(self):
        creds = KerberosCredentials("hazelcast/server@REALM.COM")
        assert creds.service_principal == "hazelcast/server@REALM.COM"
        assert creds.spn is None
        assert creds.keytab_path is None

    def test_create_with_all_options(self):
        creds = KerberosCredentials(
            service_principal="hz/server@REALM",
            spn="hz/target@REALM",
            keytab_path="/etc/keytabs/hz.keytab",
        )
        assert creds.service_principal == "hz/server@REALM"
        assert creds.spn == "hz/target@REALM"
        assert creds.keytab_path == "/etc/keytabs/hz.keytab"

    def test_credentials_type(self):
        creds = KerberosCredentials("sp")
        assert creds.credentials_type == CredentialsType.KERBEROS

    def test_to_dict(self):
        creds = KerberosCredentials("sp@REALM", spn="target", keytab_path="/path")
        d = creds.to_dict()
        assert d["type"] == "KERBEROS"
        assert d["service_principal"] == "sp@REALM"
        assert d["spn"] == "target"
        assert d["keytab_path"] == "/path"

    def test_repr(self):
        creds = KerberosCredentials("hz@REALM")
        assert "KerberosCredentials" in repr(creds)
        assert "hz@REALM" in repr(creds)


class TestLdapCredentials:
    """Tests for LdapCredentials."""

    def test_create_basic(self):
        creds = LdapCredentials("jdoe", "password")
        assert creds.username == "jdoe"
        assert creds.password == "password"

    def test_create_with_all_options(self):
        creds = LdapCredentials(
            username="cn=admin,dc=example,dc=com",
            password="secret",
            base_dn="ou=users,dc=example,dc=com",
            user_filter="(uid={0})",
            url="ldap://ldap.example.com:389",
        )
        assert creds.base_dn == "ou=users,dc=example,dc=com"
        assert creds.user_filter == "(uid={0})"
        assert creds.url == "ldap://ldap.example.com:389"

    def test_credentials_type(self):
        creds = LdapCredentials("u", "p")
        assert creds.credentials_type == CredentialsType.LDAP

    def test_to_dict(self):
        creds = LdapCredentials("user", "pass", base_dn="dn", url="ldap://host")
        d = creds.to_dict()
        assert d["type"] == "LDAP"
        assert d["username"] == "user"
        assert d["base_dn"] == "dn"
        assert d["url"] == "ldap://host"

    def test_repr(self):
        creds = LdapCredentials("admin", "pass", base_dn="dc=test")
        repr_str = repr(creds)
        assert "LdapCredentials" in repr_str
        assert "admin" in repr_str
        assert "dc=test" in repr_str


class TestCustomCredentials:
    """Tests for CustomCredentials."""

    def test_create(self):
        data = {"api_key": "abc", "tenant": "xyz"}
        creds = CustomCredentials(data)
        assert creds.data == data

    def test_credentials_type(self):
        creds = CustomCredentials({})
        assert creds.credentials_type == CredentialsType.CUSTOM

    def test_to_dict(self):
        data = {"key": "value"}
        creds = CustomCredentials(data)
        d = creds.to_dict()
        assert d["type"] == "CUSTOM"
        assert d["data"] == data

    def test_repr(self):
        creds = CustomCredentials({"x": 1})
        assert "CustomCredentials" in repr(creds)


class TestStaticCredentialsFactory:
    """Tests for StaticCredentialsFactory."""

    def test_returns_same_credentials(self):
        creds = UsernamePasswordCredentials("u", "p")
        factory = StaticCredentialsFactory(creds)
        
        result1 = factory.create_credentials()
        result2 = factory.create_credentials()
        
        assert result1 is creds
        assert result2 is creds


class TestCallableCredentialsFactory:
    """Tests for CallableCredentialsFactory."""

    def test_calls_function(self):
        call_count = [0]
        
        def get_creds():
            call_count[0] += 1
            return TokenCredentials(f"token-{call_count[0]}")
        
        factory = CallableCredentialsFactory(get_creds)
        
        creds1 = factory.create_credentials()
        creds2 = factory.create_credentials()
        
        assert creds1.token == "token-1"
        assert creds2.token == "token-2"
        assert call_count[0] == 2


class TestAuthenticationService:
    """Tests for AuthenticationService."""

    def test_initial_state(self):
        service = AuthenticationService()
        assert service.is_authenticated is False
        assert service.cluster_uuid is None
        assert service.member_uuid is None

    def test_with_credentials(self):
        creds = UsernamePasswordCredentials("admin", "pass")
        service = AuthenticationService(credentials=creds)
        
        result = service.get_credentials()
        assert result is creds

    def test_with_factory(self):
        creds = TokenCredentials("tok")
        factory = StaticCredentialsFactory(creds)
        service = AuthenticationService(credentials_factory=factory)
        
        result = service.get_credentials()
        assert result is creds

    def test_factory_takes_precedence(self):
        static_creds = UsernamePasswordCredentials("static", "pass")
        factory_creds = TokenCredentials("factory")
        factory = StaticCredentialsFactory(factory_creds)
        
        service = AuthenticationService(
            credentials=static_creds,
            credentials_factory=factory,
        )
        
        result = service.get_credentials()
        assert result is factory_creds

    def test_set_credentials(self):
        service = AuthenticationService()
        creds = UsernamePasswordCredentials("u", "p")
        
        service.set_credentials(creds)
        
        assert service.get_credentials() is creds

    def test_set_credentials_clears_factory(self):
        factory = StaticCredentialsFactory(TokenCredentials("tok"))
        service = AuthenticationService(credentials_factory=factory)
        
        new_creds = UsernamePasswordCredentials("u", "p")
        service.set_credentials(new_creds)
        
        assert service.get_credentials() is new_creds

    def test_set_credentials_factory(self):
        service = AuthenticationService(
            credentials=UsernamePasswordCredentials("u", "p")
        )
        
        factory = StaticCredentialsFactory(TokenCredentials("tok"))
        service.set_credentials_factory(factory)
        
        assert service.get_credentials().credentials_type == CredentialsType.TOKEN

    def test_mark_authenticated(self):
        service = AuthenticationService()
        
        service.mark_authenticated("cluster-123", "member-456")
        
        assert service.is_authenticated is True
        assert service.cluster_uuid == "cluster-123"
        assert service.member_uuid == "member-456"

    def test_reset(self):
        service = AuthenticationService()
        service.mark_authenticated("c", "m")
        
        service.reset()
        
        assert service.is_authenticated is False
        assert service.cluster_uuid is None
        assert service.member_uuid is None

    def test_from_config_with_username(self):
        config = MagicMock()
        config.token = None
        config.username = "admin"
        config.password = "secret"
        
        service = AuthenticationService.from_config(config)
        
        creds = service.get_credentials()
        assert isinstance(creds, UsernamePasswordCredentials)
        assert creds.username == "admin"

    def test_from_config_with_token(self):
        config = MagicMock()
        config.token = "my-token"
        config.username = None
        
        service = AuthenticationService.from_config(config)
        
        creds = service.get_credentials()
        assert isinstance(creds, TokenCredentials)
        assert creds.token == "my-token"

    def test_from_config_token_priority(self):
        config = MagicMock()
        config.token = "tok"
        config.username = "user"
        config.password = "pass"
        
        service = AuthenticationService.from_config(config)
        
        creds = service.get_credentials()
        assert isinstance(creds, TokenCredentials)


class TestTlsConfig:
    """Tests for TlsConfig."""

    def test_defaults(self):
        config = TlsConfig()
        assert config.enabled is False
        assert config.ca_cert_path is None
        assert config.verify_mode == CertificateVerifyMode.REQUIRED
        assert config.check_hostname is True
        assert config.protocol == TlsProtocol.TLS_1_2

    def test_basic_tls(self):
        config = TlsConfig(
            enabled=True,
            ca_cert_path="/path/to/ca.pem",
        )
        assert config.enabled is True
        assert config.ca_cert_path == "/path/to/ca.pem"

    def test_mutual_tls(self):
        config = TlsConfig(
            enabled=True,
            ca_cert_path="/ca.pem",
            client_cert_path="/client.pem",
            client_key_path="/client-key.pem",
        )
        assert config.mutual_tls_enabled is True

    def test_mutual_tls_requires_both(self):
        with pytest.raises(ConfigurationException):
            TlsConfig(
                enabled=True,
                ca_cert_path="/ca.pem",
                client_cert_path="/client.pem",
                # Missing client_key_path
            )
        
        with pytest.raises(ConfigurationException):
            TlsConfig(
                enabled=True,
                ca_cert_path="/ca.pem",
                client_key_path="/key.pem",
                # Missing client_cert_path
            )

    def test_required_verify_needs_ca(self):
        with pytest.raises(ConfigurationException):
            TlsConfig(
                enabled=True,
                verify_mode=CertificateVerifyMode.REQUIRED,
                # Missing ca_cert_path
            )

    def test_optional_verify_without_ca(self):
        config = TlsConfig(
            enabled=True,
            verify_mode=CertificateVerifyMode.OPTIONAL,
        )
        assert config.verify_mode == CertificateVerifyMode.OPTIONAL

    def test_none_verify_without_ca(self):
        config = TlsConfig(
            enabled=True,
            verify_mode=CertificateVerifyMode.NONE,
        )
        assert config.verify_mode == CertificateVerifyMode.NONE

    def test_cipher_suites(self):
        config = TlsConfig()
        assert len(config.cipher_suites) > 0
        assert config.cipher_suites == DEFAULT_CIPHER_SUITES

    def test_custom_cipher_suites(self):
        config = TlsConfig(cipher_suites=["TLS_AES_256_GCM_SHA384"])
        assert config.cipher_suites == ["TLS_AES_256_GCM_SHA384"]

    def test_protocol_versions(self):
        for protocol in TlsProtocol:
            config = TlsConfig(
                enabled=True,
                verify_mode=CertificateVerifyMode.NONE,
                protocol=protocol,
            )
            assert config.protocol == protocol

    def test_setters(self):
        config = TlsConfig()
        config.ca_cert_path = "/new/ca.pem"
        config.check_hostname = False
        config.cipher_suites = ["SUITE1"]
        
        assert config.ca_cert_path == "/new/ca.pem"
        assert config.check_hostname is False
        assert config.cipher_suites == ["SUITE1"]

    def test_enable_setter_validates(self):
        config = TlsConfig()
        config.verify_mode = CertificateVerifyMode.NONE
        config.enabled = True  # Should work with NONE verify
        
        config.enabled = False
        config.verify_mode = CertificateVerifyMode.REQUIRED
        
        with pytest.raises(ConfigurationException):
            config.enabled = True  # Should fail without ca_cert

    def test_from_dict(self):
        config = TlsConfig.from_dict({
            "enabled": True,
            "ca_cert_path": "/ca.pem",
            "verify_mode": "REQUIRED",
            "check_hostname": False,
            "protocol": "TLSv1.3",
        })
        assert config.enabled is True
        assert config.ca_cert_path == "/ca.pem"
        assert config.check_hostname is False
        assert config.protocol == TlsProtocol.TLS_1_3

    def test_from_dict_invalid_verify_mode(self):
        with pytest.raises(ConfigurationException):
            TlsConfig.from_dict({"verify_mode": "INVALID"})

    def test_repr(self):
        config = TlsConfig(enabled=True, verify_mode=CertificateVerifyMode.NONE)
        repr_str = repr(config)
        assert "TlsConfig" in repr_str
        assert "enabled=True" in repr_str

    @pytest.mark.skipif(not hasattr(ssl, 'TLSVersion'), reason="Requires Python 3.7+")
    def test_create_ssl_context_disabled(self):
        config = TlsConfig(enabled=False)
        with pytest.raises(ConfigurationException):
            config.create_ssl_context()
