"""TLS/SSL configuration for secure Hazelcast client connections."""

import ssl
from enum import Enum
from typing import List, Optional

from hazelcast.exceptions import ConfigurationException


class TlsProtocol(Enum):
    """TLS protocol versions."""

    TLS_1_0 = "TLSv1"
    TLS_1_1 = "TLSv1.1"
    TLS_1_2 = "TLSv1.2"
    TLS_1_3 = "TLSv1.3"


class CertificateVerifyMode(Enum):
    """Certificate verification mode."""

    NONE = "NONE"
    OPTIONAL = "OPTIONAL"
    REQUIRED = "REQUIRED"


DEFAULT_CIPHER_SUITES = [
    "TLS_AES_256_GCM_SHA384",
    "TLS_CHACHA20_POLY1305_SHA256",
    "TLS_AES_128_GCM_SHA256",
    "ECDHE-ECDSA-AES256-GCM-SHA384",
    "ECDHE-RSA-AES256-GCM-SHA384",
    "ECDHE-ECDSA-AES128-GCM-SHA256",
    "ECDHE-RSA-AES128-GCM-SHA256",
]


class TlsConfig:
    """TLS/SSL configuration for secure connections.

    Supports mutual TLS (mTLS), certificate validation, cipher suite
    configuration, and protocol version selection.

    Attributes:
        enabled: Whether TLS is enabled.
        ca_cert_path: Path to CA certificate file for server verification.
        client_cert_path: Path to client certificate for mutual TLS.
        client_key_path: Path to client private key for mutual TLS.
        client_key_password: Password for encrypted client private key.
        verify_mode: Certificate verification mode.
        check_hostname: Whether to verify server hostname.
        cipher_suites: List of allowed cipher suites.
        protocol: Minimum TLS protocol version.

    Example:
        Basic TLS::

            tls = TlsConfig(
                enabled=True,
                ca_cert_path="/path/to/ca.pem",
            )

        Mutual TLS::

            tls = TlsConfig(
                enabled=True,
                ca_cert_path="/path/to/ca.pem",
                client_cert_path="/path/to/client.pem",
                client_key_path="/path/to/client-key.pem",
            )
    """

    def __init__(
        self,
        enabled: bool = False,
        ca_cert_path: Optional[str] = None,
        client_cert_path: Optional[str] = None,
        client_key_path: Optional[str] = None,
        client_key_password: Optional[str] = None,
        verify_mode: CertificateVerifyMode = CertificateVerifyMode.REQUIRED,
        check_hostname: bool = True,
        cipher_suites: Optional[List[str]] = None,
        protocol: TlsProtocol = TlsProtocol.TLS_1_2,
    ):
        self._enabled = enabled
        self._ca_cert_path = ca_cert_path
        self._client_cert_path = client_cert_path
        self._client_key_path = client_key_path
        self._client_key_password = client_key_password
        self._verify_mode = verify_mode
        self._check_hostname = check_hostname
        self._cipher_suites = cipher_suites or DEFAULT_CIPHER_SUITES.copy()
        self._protocol = protocol

        if enabled:
            self._validate()

    def _validate(self) -> None:
        """Validate TLS configuration."""
        if self._client_cert_path and not self._client_key_path:
            raise ConfigurationException(
                "client_key_path is required when client_cert_path is set"
            )
        if self._client_key_path and not self._client_cert_path:
            raise ConfigurationException(
                "client_cert_path is required when client_key_path is set"
            )
        if self._verify_mode == CertificateVerifyMode.REQUIRED and not self._ca_cert_path:
            raise ConfigurationException(
                "ca_cert_path is required when verify_mode is REQUIRED"
            )

    @property
    def enabled(self) -> bool:
        """Get whether TLS is enabled."""
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool) -> None:
        self._enabled = value
        if value:
            self._validate()

    @property
    def ca_cert_path(self) -> Optional[str]:
        """Get the CA certificate path."""
        return self._ca_cert_path

    @ca_cert_path.setter
    def ca_cert_path(self, value: Optional[str]) -> None:
        self._ca_cert_path = value
        if self._enabled:
            self._validate()

    @property
    def client_cert_path(self) -> Optional[str]:
        """Get the client certificate path for mutual TLS."""
        return self._client_cert_path

    @client_cert_path.setter
    def client_cert_path(self, value: Optional[str]) -> None:
        self._client_cert_path = value
        if self._enabled:
            self._validate()

    @property
    def client_key_path(self) -> Optional[str]:
        """Get the client private key path for mutual TLS."""
        return self._client_key_path

    @client_key_path.setter
    def client_key_path(self, value: Optional[str]) -> None:
        self._client_key_path = value
        if self._enabled:
            self._validate()

    @property
    def client_key_password(self) -> Optional[str]:
        """Get the client private key password."""
        return self._client_key_password

    @client_key_password.setter
    def client_key_password(self, value: Optional[str]) -> None:
        self._client_key_password = value

    @property
    def verify_mode(self) -> CertificateVerifyMode:
        """Get the certificate verification mode."""
        return self._verify_mode

    @verify_mode.setter
    def verify_mode(self, value: CertificateVerifyMode) -> None:
        self._verify_mode = value
        if self._enabled:
            self._validate()

    @property
    def check_hostname(self) -> bool:
        """Get whether hostname verification is enabled."""
        return self._check_hostname

    @check_hostname.setter
    def check_hostname(self, value: bool) -> None:
        self._check_hostname = value

    @property
    def cipher_suites(self) -> List[str]:
        """Get the list of allowed cipher suites."""
        return self._cipher_suites

    @cipher_suites.setter
    def cipher_suites(self, value: List[str]) -> None:
        self._cipher_suites = value

    @property
    def protocol(self) -> TlsProtocol:
        """Get the minimum TLS protocol version."""
        return self._protocol

    @protocol.setter
    def protocol(self, value: TlsProtocol) -> None:
        self._protocol = value

    @property
    def mutual_tls_enabled(self) -> bool:
        """Check if mutual TLS is configured."""
        return bool(self._client_cert_path and self._client_key_path)

    def create_ssl_context(self) -> ssl.SSLContext:
        """Create an SSL context from this configuration.

        Returns:
            Configured SSLContext instance.

        Raises:
            ConfigurationException: If configuration is invalid.
        """
        if not self._enabled:
            raise ConfigurationException("TLS is not enabled")

        protocol_map = {
            TlsProtocol.TLS_1_0: ssl.TLSVersion.TLSv1,
            TlsProtocol.TLS_1_1: ssl.TLSVersion.TLSv1_1,
            TlsProtocol.TLS_1_2: ssl.TLSVersion.TLSv1_2,
            TlsProtocol.TLS_1_3: ssl.TLSVersion.TLSv1_3,
        }

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = protocol_map.get(
            self._protocol, ssl.TLSVersion.TLSv1_2
        )

        verify_map = {
            CertificateVerifyMode.NONE: ssl.CERT_NONE,
            CertificateVerifyMode.OPTIONAL: ssl.CERT_OPTIONAL,
            CertificateVerifyMode.REQUIRED: ssl.CERT_REQUIRED,
        }
        context.verify_mode = verify_map.get(self._verify_mode, ssl.CERT_REQUIRED)
        context.check_hostname = (
            self._check_hostname
            and self._verify_mode != CertificateVerifyMode.NONE
        )

        if self._ca_cert_path:
            context.load_verify_locations(cafile=self._ca_cert_path)

        if self._client_cert_path and self._client_key_path:
            context.load_cert_chain(
                certfile=self._client_cert_path,
                keyfile=self._client_key_path,
                password=self._client_key_password,
            )

        if self._cipher_suites:
            try:
                context.set_ciphers(":".join(self._cipher_suites))
            except ssl.SSLError:
                pass

        return context

    @classmethod
    def from_dict(cls, data: dict) -> "TlsConfig":
        """Create TlsConfig from a dictionary.

        Args:
            data: Dictionary containing TLS configuration.

        Returns:
            TlsConfig instance.
        """
        verify_mode_str = data.get("verify_mode", "REQUIRED")
        try:
            verify_mode = CertificateVerifyMode(verify_mode_str.upper())
        except ValueError:
            raise ConfigurationException(f"Invalid verify_mode: {verify_mode_str}")

        protocol_str = data.get("protocol", "TLSv1.2")
        protocol = TlsProtocol.TLS_1_2
        for p in TlsProtocol:
            if p.value == protocol_str:
                protocol = p
                break

        return cls(
            enabled=data.get("enabled", False),
            ca_cert_path=data.get("ca_cert_path"),
            client_cert_path=data.get("client_cert_path"),
            client_key_path=data.get("client_key_path"),
            client_key_password=data.get("client_key_password"),
            verify_mode=verify_mode,
            check_hostname=data.get("check_hostname", True),
            cipher_suites=data.get("cipher_suites"),
            protocol=protocol,
        )

    def __repr__(self) -> str:
        return (
            f"TlsConfig(enabled={self._enabled}, "
            f"mutual_tls={self.mutual_tls_enabled}, "
            f"verify_mode={self._verify_mode.value})"
        )
