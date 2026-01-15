"""Unit tests for hazelcast.auth module."""

import pytest

from hazelcast.auth import (
    CredentialsType,
    Credentials,
    UsernamePasswordCredentials,
    TokenCredentials,
    KerberosCredentials,
    CustomCredentials,
    CredentialsFactory,
    StaticCredentialsFactory,
    CallableCredentialsFactory,
    AuthenticationService,
)
from hazelcast.config import SecurityConfig


class TestCredentialsType:
    """Tests for CredentialsType enum."""

    def test_values(self):
        assert CredentialsType.USERNAME_PASSWORD.value == "USERNAME_PASSWORD"
        assert CredentialsType.TOKEN.value == "TOKEN"
        assert CredentialsType.KERBEROS.value == "KERBEROS"
        assert CredentialsType.CUSTOM.value == "CUSTOM"


class TestUsernamePasswordCredentials:
    """Tests for UsernamePasswordCredentials class."""

    def test_init(self):
        creds = UsernamePasswordCredentials("user", "pass")
        assert creds.username == "user"
        assert creds.password == "pass"
        assert creds.credentials_type == CredentialsType.USERNAME_PASSWORD

    def test_to_dict(self):
        creds = UsernamePasswordCredentials("user", "pass")
        d = creds.to_dict()
        assert d["type"] == "USERNAME_PASSWORD"
        assert d["username"] == "user"
        assert d["password"] == "pass"

    def test_repr(self):
        creds = UsernamePasswordCredentials("user", "pass")
        r = repr(creds)
        assert "user" in r
        assert "pass" not in r


class TestTokenCredentials:
    """Tests for TokenCredentials class."""

    def test_init(self):
        creds = TokenCredentials("my-token")
        assert creds.token == "my-token"
        assert creds.credentials_type == CredentialsType.TOKEN

    def test_to_dict(self):
        creds = TokenCredentials("my-token")
        d = creds.to_dict()
        assert d["type"] == "TOKEN"
        assert d["token"] == "my-token"

    def test_repr_hides_token(self):
        creds = TokenCredentials("secret-token")
        r = repr(creds)
        assert "secret-token" not in r
        assert "***" in r


class TestKerberosCredentials:
    """Tests for KerberosCredentials class."""

    def test_init(self):
        creds = KerberosCredentials("hazelcast/service")
        assert creds.service_principal == "hazelcast/service"
        assert creds.spn is None
        assert creds.keytab_path is None
        assert creds.credentials_type == CredentialsType.KERBEROS

    def test_full_init(self):
        creds = KerberosCredentials(
            "hazelcast/service",
            spn="hz@REALM",
            keytab_path="/etc/keytab",
        )
        assert creds.spn == "hz@REALM"
        assert creds.keytab_path == "/etc/keytab"

    def test_to_dict(self):
        creds = KerberosCredentials("principal", spn="spn")
        d = creds.to_dict()
        assert d["type"] == "KERBEROS"
        assert d["service_principal"] == "principal"
        assert d["spn"] == "spn"


class TestCustomCredentials:
    """Tests for CustomCredentials class."""

    def test_init(self):
        data = {"key": "value", "number": 42}
        creds = CustomCredentials(data)
        assert creds.data == data
        assert creds.credentials_type == CredentialsType.CUSTOM

    def test_to_dict(self):
        data = {"custom": "data"}
        creds = CustomCredentials(data)
        d = creds.to_dict()
        assert d["type"] == "CUSTOM"
        assert d["data"] == data


class TestStaticCredentialsFactory:
    """Tests for StaticCredentialsFactory class."""

    def test_create_credentials(self):
        creds = UsernamePasswordCredentials("user", "pass")
        factory = StaticCredentialsFactory(creds)
        result = factory.create_credentials()
        assert result is creds


class TestCallableCredentialsFactory:
    """Tests for CallableCredentialsFactory class."""

    def test_create_credentials(self):
        call_count = [0]

        def factory_fn():
            call_count[0] += 1
            return TokenCredentials(f"token-{call_count[0]}")

        factory = CallableCredentialsFactory(factory_fn)
        result1 = factory.create_credentials()
        result2 = factory.create_credentials()
        assert result1.token == "token-1"
        assert result2.token == "token-2"


class TestAuthenticationService:
    """Tests for AuthenticationService class."""

    def test_init_no_credentials(self):
        service = AuthenticationService()
        assert service.is_authenticated is False
        assert service.get_credentials() is None

    def test_init_with_credentials(self):
        creds = UsernamePasswordCredentials("user", "pass")
        service = AuthenticationService(credentials=creds)
        result = service.get_credentials()
        assert result is creds

    def test_init_with_factory(self):
        creds = TokenCredentials("token")
        factory = StaticCredentialsFactory(creds)
        service = AuthenticationService(credentials_factory=factory)
        result = service.get_credentials()
        assert result is creds

    def test_factory_takes_precedence(self):
        static_creds = UsernamePasswordCredentials("user", "pass")
        factory_creds = TokenCredentials("token")
        factory = StaticCredentialsFactory(factory_creds)
        service = AuthenticationService(
            credentials=static_creds,
            credentials_factory=factory,
        )
        result = service.get_credentials()
        assert result is factory_creds

    def test_set_credentials(self):
        service = AuthenticationService()
        creds = UsernamePasswordCredentials("user", "pass")
        service.set_credentials(creds)
        assert service.get_credentials() is creds

    def test_set_credentials_clears_factory(self):
        factory = StaticCredentialsFactory(TokenCredentials("token"))
        service = AuthenticationService(credentials_factory=factory)
        new_creds = UsernamePasswordCredentials("user", "pass")
        service.set_credentials(new_creds)
        assert service.get_credentials() is new_creds

    def test_set_credentials_factory(self):
        service = AuthenticationService()
        factory = StaticCredentialsFactory(TokenCredentials("token"))
        service.set_credentials_factory(factory)
        assert service.get_credentials().credentials_type == CredentialsType.TOKEN

    def test_mark_authenticated(self):
        service = AuthenticationService()
        service.mark_authenticated("cluster-uuid", "member-uuid")
        assert service.is_authenticated is True
        assert service.cluster_uuid == "cluster-uuid"
        assert service.member_uuid == "member-uuid"

    def test_reset(self):
        service = AuthenticationService()
        service.mark_authenticated("cluster", "member")
        service.reset()
        assert service.is_authenticated is False
        assert service.cluster_uuid is None
        assert service.member_uuid is None

    def test_from_config_token(self):
        config = SecurityConfig(token="my-token")
        service = AuthenticationService.from_config(config)
        creds = service.get_credentials()
        assert isinstance(creds, TokenCredentials)
        assert creds.token == "my-token"

    def test_from_config_username_password(self):
        config = SecurityConfig(username="user", password="pass")
        service = AuthenticationService.from_config(config)
        creds = service.get_credentials()
        assert isinstance(creds, UsernamePasswordCredentials)
        assert creds.username == "user"
        assert creds.password == "pass"

    def test_from_config_no_credentials(self):
        config = SecurityConfig()
        service = AuthenticationService.from_config(config)
        assert service.get_credentials() is None
