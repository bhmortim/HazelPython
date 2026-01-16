"""Authentication support for Hazelcast client.

This module provides authentication credentials and services for connecting
to secured Hazelcast clusters. It supports multiple authentication mechanisms
including username/password, token-based, Kerberos, LDAP, and custom credentials.

The authentication flow involves:
1. Creating appropriate credentials (e.g., ``UsernamePasswordCredentials``)
2. Configuring them in ``SecurityConfig`` or using a ``CredentialsFactory``
3. The client automatically uses these credentials during connection

Example:
    Username/password authentication::

        from hazelcast.auth import UsernamePasswordCredentials
        from hazelcast.config import ClientConfig

        config = ClientConfig()
        config.security.username = "admin"
        config.security.password = "secret"

    Token-based authentication::

        from hazelcast.auth import TokenCredentials, AuthenticationService

        credentials = TokenCredentials("my-auth-token")
        auth_service = AuthenticationService(credentials=credentials)

    Custom credentials factory::

        from hazelcast.auth import (
            CallableCredentialsFactory,
            UsernamePasswordCredentials
        )

        def get_credentials():
            # Fetch credentials dynamically
            return UsernamePasswordCredentials("user", "pass")

        factory = CallableCredentialsFactory(get_credentials)
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Callable, Dict, Optional, Any


class CredentialsType(Enum):
    """Type of credentials used for authentication.

    Attributes:
        USERNAME_PASSWORD: Standard username and password authentication.
        TOKEN: Token-based authentication (e.g., JWT, OAuth).
        KERBEROS: Kerberos/SPNEGO authentication for enterprise environments.
        LDAP: LDAP directory service authentication.
        CUSTOM: Custom authentication with arbitrary data.
    """

    USERNAME_PASSWORD = "USERNAME_PASSWORD"
    TOKEN = "TOKEN"
    KERBEROS = "KERBEROS"
    LDAP = "LDAP"
    CUSTOM = "CUSTOM"


class Credentials(ABC):
    """Base class for authentication credentials.

    All credential types must inherit from this class and implement
    the required abstract methods for type identification and serialization.

    Subclasses:
        - :class:`UsernamePasswordCredentials`: Username/password auth
        - :class:`TokenCredentials`: Token-based auth
        - :class:`KerberosCredentials`: Kerberos/SPNEGO auth
        - :class:`LdapCredentials`: LDAP directory auth
        - :class:`CustomCredentials`: Custom auth data
    """

    @property
    @abstractmethod
    def credentials_type(self) -> CredentialsType:
        """Get the type of credentials.

        Returns:
            The credentials type enum value.
        """
        pass

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert credentials to a dictionary for serialization.

        Returns:
            Dictionary representation of the credentials suitable
            for transmission to the server.
        """
        pass


class UsernamePasswordCredentials(Credentials):
    """Username and password based credentials.

    Standard authentication using a username and password pair.
    This is the most common authentication method for Hazelcast clusters.

    Args:
        username: The username for authentication.
        password: The password for authentication.

    Attributes:
        username: The authentication username.
        password: The authentication password.

    Example:
        >>> creds = UsernamePasswordCredentials("admin", "secret123")
        >>> print(creds.username)
        admin
    """

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
    """Token-based credentials for authentication.

    Used for token-based authentication mechanisms such as JWT, OAuth,
    or custom token systems. The token is passed directly to the server
    for validation.

    Args:
        token: The authentication token string.

    Attributes:
        token: The authentication token.

    Example:
        >>> creds = TokenCredentials("eyJhbGciOiJIUzI1NiIs...")
        >>> print(creds.credentials_type)
        CredentialsType.TOKEN
    """

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
    """Kerberos-based credentials for enterprise authentication.

    Used for Kerberos/SPNEGO authentication in enterprise environments.
    Supports service principal names and keytab files for automated
    authentication without user interaction.

    Args:
        service_principal: The Kerberos service principal name
            (e.g., "hazelcast/server@REALM.COM").
        spn: Optional Service Principal Name for the target service.
            If not provided, derived from service_principal.
        keytab_path: Optional path to the keytab file containing
            the service key. Required for non-interactive authentication.

    Attributes:
        service_principal: The Kerberos service principal.
        spn: The target service principal name.
        keytab_path: Path to the keytab file.

    Example:
        >>> creds = KerberosCredentials(
        ...     service_principal="hazelcast/server@EXAMPLE.COM",
        ...     keytab_path="/etc/security/keytabs/hazelcast.keytab"
        ... )
    """

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
    """LDAP-based credentials for directory service authentication.

    Used for authenticating against LDAP directory services such as
    Active Directory or OpenLDAP. Supports configurable base DN,
    user filters, and LDAP server URL.

    Args:
        username: The LDAP username or distinguished name (DN).
        password: The LDAP password.
        base_dn: Optional base distinguished name for user search
            (e.g., "ou=users,dc=example,dc=com").
        user_filter: Optional LDAP filter for finding users
            (e.g., "(uid={0})").
        url: Optional LDAP server URL (e.g., "ldap://ldap.example.com:389").

    Attributes:
        username: The LDAP username.
        password: The LDAP password.
        base_dn: The base DN for user search.
        user_filter: The LDAP user filter.
        url: The LDAP server URL.

    Example:
        >>> creds = LdapCredentials(
        ...     username="jdoe",
        ...     password="secret",
        ...     base_dn="ou=users,dc=example,dc=com",
        ...     url="ldap://ldap.example.com:389"
        ... )
    """

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
    """Custom credentials with arbitrary data.

    Used for custom authentication mechanisms that don't fit the standard
    credential types. The data dictionary can contain any authentication
    information required by a custom security provider.

    Args:
        data: Dictionary containing custom authentication data.

    Attributes:
        data: The custom authentication data dictionary.

    Example:
        >>> creds = CustomCredentials({
        ...     "api_key": "abc123",
        ...     "tenant_id": "tenant-001",
        ...     "signature": "xyz789"
        ... })
    """

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
    """Factory interface for creating credentials dynamically.

    Implement this interface to provide credentials that may change
    over time or need to be fetched from external sources (e.g., vault,
    environment variables, or configuration service).

    Example:
        >>> class VaultCredentialsFactory(CredentialsFactory):
        ...     def create_credentials(self) -> Credentials:
        ...         # Fetch from vault
        ...         secret = vault_client.get_secret("hazelcast")
        ...         return UsernamePasswordCredentials(
        ...             secret["username"],
        ...             secret["password"]
        ...         )
    """

    @abstractmethod
    def create_credentials(self) -> Credentials:
        """Create and return credentials for authentication.

        Called each time credentials are needed, allowing for
        dynamic credential refresh.

        Returns:
            Credentials instance for authentication.
        """
        pass


class StaticCredentialsFactory(CredentialsFactory):
    """Factory that returns pre-configured static credentials.

    A simple factory that always returns the same credentials instance.
    Useful when credentials don't change during the client lifetime.

    Args:
        credentials: The credentials to return on each call.

    Example:
        >>> creds = UsernamePasswordCredentials("admin", "secret")
        >>> factory = StaticCredentialsFactory(creds)
        >>> factory.create_credentials()  # Always returns same creds
    """

    def __init__(self, credentials: Credentials):
        self._credentials = credentials

    def create_credentials(self) -> Credentials:
        """Return the pre-configured credentials.

        Returns:
            The static credentials instance.
        """
        return self._credentials


class CallableCredentialsFactory(CredentialsFactory):
    """Factory that uses a callable to create credentials.

    Delegates credential creation to a provided function, allowing
    for flexible credential sourcing without subclassing.

    Args:
        factory_fn: A callable that returns a Credentials instance.

    Example:
        >>> def get_rotating_credentials():
        ...     token = fetch_new_token()
        ...     return TokenCredentials(token)
        ...
        >>> factory = CallableCredentialsFactory(get_rotating_credentials)
    """

    def __init__(self, factory_fn: Callable[[], Credentials]):
        self._factory_fn = factory_fn

    def create_credentials(self) -> Credentials:
        """Create credentials by invoking the factory function.

        Returns:
            Credentials instance from the factory function.
        """
        return self._factory_fn()


class AuthenticationService:
    """Service for managing client authentication.

    Handles credential management and authentication state for the
    Hazelcast client. Supports both static credentials and dynamic
    credential factories.

    Args:
        credentials: Optional static credentials for authentication.
        credentials_factory: Optional factory for dynamic credentials.
            If provided, takes precedence over static credentials.

    Attributes:
        is_authenticated: Whether the client is currently authenticated.
        cluster_uuid: UUID of the authenticated cluster.
        member_uuid: UUID of the member the client authenticated with.

    Example:
        >>> from hazelcast.auth import AuthenticationService, UsernamePasswordCredentials
        >>> creds = UsernamePasswordCredentials("admin", "secret")
        >>> auth_service = AuthenticationService(credentials=creds)
        >>> auth_service.get_credentials()
        UsernamePasswordCredentials(username='admin')
    """

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

        Returns credentials from the factory if set, otherwise returns
        the static credentials.

        Returns:
            Credentials instance, or ``None`` if no credentials configured.
        """
        if self._credentials_factory:
            return self._credentials_factory.create_credentials()
        return self._credentials

    def set_credentials(self, credentials: Credentials) -> None:
        """Set static credentials.

        Replaces any existing credentials or factory with the provided
        static credentials.

        Args:
            credentials: The credentials to use for authentication.
        """
        self._credentials = credentials
        self._credentials_factory = None

    def set_credentials_factory(self, factory: CredentialsFactory) -> None:
        """Set a credentials factory.

        Replaces any existing credentials or factory. The factory will
        be called each time credentials are needed.

        Args:
            factory: The credentials factory to use.
        """
        self._credentials_factory = factory
        self._credentials = None

    def mark_authenticated(
        self, cluster_uuid: str, member_uuid: str
    ) -> None:
        """Mark the client as authenticated.

        Called internally when authentication succeeds.

        Args:
            cluster_uuid: The UUID of the authenticated cluster.
            member_uuid: The UUID of the member that authenticated.
        """
        self._authenticated = True
        self._cluster_uuid = cluster_uuid
        self._member_uuid = member_uuid

    def reset(self) -> None:
        """Reset authentication state.

        Clears the authenticated flag and cluster/member UUIDs.
        Used when disconnecting or reconnecting.
        """
        self._authenticated = False
        self._cluster_uuid = None
        self._member_uuid = None

    @classmethod
    def from_config(cls, security_config) -> "AuthenticationService":
        """Create AuthenticationService from SecurityConfig.

        Factory method that creates an AuthenticationService with
        credentials derived from a SecurityConfig object.

        Args:
            security_config: SecurityConfig containing authentication settings.

        Returns:
            Configured AuthenticationService instance.
        """
        credentials = None

        if security_config.token:
            credentials = TokenCredentials(security_config.token)
        elif security_config.username:
            credentials = UsernamePasswordCredentials(
                security_config.username,
                security_config.password or "",
            )

        return cls(credentials=credentials)
