"""Authentication support for Hazelcast client."""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Callable, Dict, Optional, Any


class CredentialsType(Enum):
    """Type of credentials used for authentication."""

    USERNAME_PASSWORD = "USERNAME_PASSWORD"
    TOKEN = "TOKEN"
    KERBEROS = "KERBEROS"
    LDAP = "LDAP"
    CUSTOM = "CUSTOM"


class Credentials(ABC):
    """Base class for authentication credentials."""

    @property
    @abstractmethod
    def credentials_type(self) -> CredentialsType:
        """Get the type of credentials."""
        pass

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert credentials to a dictionary for serialization."""
        pass


class UsernamePasswordCredentials(Credentials):
    """Username and password based credentials."""

    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password

    @property
    def credentials_type(self) -> CredentialsType:
        return CredentialsType.USERNAME_PASSWORD

    @property
    def username(self) -> str:
        return self._username

    @property
    def password(self) -> str:
        return self._password

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.credentials_type.value,
            "username": self._username,
            "password": self._password,
        }

    def __repr__(self) -> str:
        return f"UsernamePasswordCredentials(username={self._username!r})"


class TokenCredentials(Credentials):
    """Token-based credentials."""

    def __init__(self, token: str):
        self._token = token

    @property
    def credentials_type(self) -> CredentialsType:
        return CredentialsType.TOKEN

    @property
    def token(self) -> str:
        return self._token

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.credentials_type.value,
            "token": self._token,
        }

    def __repr__(self) -> str:
        return "TokenCredentials(token=***)"


class KerberosCredentials(Credentials):
    """Kerberos-based credentials."""

    def __init__(
        self,
        service_principal: str,
        spn: Optional[str] = None,
        keytab_path: Optional[str] = None,
    ):
        self._service_principal = service_principal
        self._spn = spn
        self._keytab_path = keytab_path

    @property
    def credentials_type(self) -> CredentialsType:
        return CredentialsType.KERBEROS

    @property
    def service_principal(self) -> str:
        return self._service_principal

    @property
    def spn(self) -> Optional[str]:
        return self._spn

    @property
    def keytab_path(self) -> Optional[str]:
        return self._keytab_path

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.credentials_type.value,
            "service_principal": self._service_principal,
            "spn": self._spn,
            "keytab_path": self._keytab_path,
        }

    def __repr__(self) -> str:
        return f"KerberosCredentials(service_principal={self._service_principal!r})"


class LdapCredentials(Credentials):
    """LDAP-based credentials for directory service authentication."""

    def __init__(
        self,
        username: str,
        password: str,
        base_dn: Optional[str] = None,
        user_filter: Optional[str] = None,
        url: Optional[str] = None,
    ):
        self._username = username
        self._password = password
        self._base_dn = base_dn
        self._user_filter = user_filter
        self._url = url

    @property
    def credentials_type(self) -> CredentialsType:
        return CredentialsType.LDAP

    @property
    def username(self) -> str:
        return self._username

    @property
    def password(self) -> str:
        return self._password

    @property
    def base_dn(self) -> Optional[str]:
        return self._base_dn

    @property
    def user_filter(self) -> Optional[str]:
        return self._user_filter

    @property
    def url(self) -> Optional[str]:
        return self._url

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.credentials_type.value,
            "username": self._username,
            "password": self._password,
            "base_dn": self._base_dn,
            "user_filter": self._user_filter,
            "url": self._url,
        }

    def __repr__(self) -> str:
        return f"LdapCredentials(username={self._username!r}, base_dn={self._base_dn!r})"


class CustomCredentials(Credentials):
    """Custom credentials with arbitrary data."""

    def __init__(self, data: Dict[str, Any]):
        self._data = data

    @property
    def credentials_type(self) -> CredentialsType:
        return CredentialsType.CUSTOM

    @property
    def data(self) -> Dict[str, Any]:
        return self._data

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.credentials_type.value,
            "data": self._data,
        }

    def __repr__(self) -> str:
        return f"CustomCredentials(data={self._data!r})"


class CredentialsFactory(ABC):
    """Factory interface for creating credentials."""

    @abstractmethod
    def create_credentials(self) -> Credentials:
        """Create and return credentials for authentication."""
        pass


class StaticCredentialsFactory(CredentialsFactory):
    """Factory that returns pre-configured static credentials."""

    def __init__(self, credentials: Credentials):
        self._credentials = credentials

    def create_credentials(self) -> Credentials:
        return self._credentials


class CallableCredentialsFactory(CredentialsFactory):
    """Factory that uses a callable to create credentials."""

    def __init__(self, factory_fn: Callable[[], Credentials]):
        self._factory_fn = factory_fn

    def create_credentials(self) -> Credentials:
        return self._factory_fn()


class AuthenticationService:
    """Service for managing client authentication."""

    def __init__(
        self,
        credentials: Optional[Credentials] = None,
        credentials_factory: Optional[CredentialsFactory] = None,
    ):
        self._credentials = credentials
        self._credentials_factory = credentials_factory
        self._authenticated = False
        self._cluster_uuid: Optional[str] = None
        self._member_uuid: Optional[str] = None

    @property
    def is_authenticated(self) -> bool:
        return self._authenticated

    @property
    def cluster_uuid(self) -> Optional[str]:
        return self._cluster_uuid

    @property
    def member_uuid(self) -> Optional[str]:
        return self._member_uuid

    def get_credentials(self) -> Optional[Credentials]:
        """Get credentials for authentication.

        Returns credentials from factory if set, otherwise static credentials.
        """
        if self._credentials_factory:
            return self._credentials_factory.create_credentials()
        return self._credentials

    def set_credentials(self, credentials: Credentials) -> None:
        """Set static credentials."""
        self._credentials = credentials
        self._credentials_factory = None

    def set_credentials_factory(self, factory: CredentialsFactory) -> None:
        """Set credentials factory."""
        self._credentials_factory = factory
        self._credentials = None

    def mark_authenticated(
        self, cluster_uuid: str, member_uuid: str
    ) -> None:
        """Mark the client as authenticated."""
        self._authenticated = True
        self._cluster_uuid = cluster_uuid
        self._member_uuid = member_uuid

    def reset(self) -> None:
        """Reset authentication state."""
        self._authenticated = False
        self._cluster_uuid = None
        self._member_uuid = None

    @classmethod
    def from_config(cls, security_config) -> "AuthenticationService":
        """Create AuthenticationService from SecurityConfig."""
        credentials = None

        if security_config.token:
            credentials = TokenCredentials(security_config.token)
        elif security_config.username:
            credentials = UsernamePasswordCredentials(
                security_config.username,
                security_config.password or "",
            )

        return cls(credentials=credentials)
