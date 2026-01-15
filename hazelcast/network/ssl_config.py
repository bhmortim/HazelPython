"""TLS/SSL configuration for secure connections."""

import ssl
from enum import Enum
from typing import Optional


class SSLProtocol(Enum):
    """SSL/TLS protocol versions."""

    TLS = "TLS"
    TLSv1 = "TLSv1"
    TLSv1_1 = "TLSv1.1"
    TLSv1_2 = "TLSv1.2"
    TLSv1_3 = "TLSv1.3"


class SSLConfig:
    """Configuration for SSL/TLS connections."""

    def __init__(
        self,
        enabled: bool = False,
        cafile: Optional[str] = None,
        capath: Optional[str] = None,
        certfile: Optional[str] = None,
        keyfile: Optional[str] = None,
        keyfile_password: Optional[str] = None,
        protocol: SSLProtocol = SSLProtocol.TLS,
        check_hostname: bool = True,
        verify_mode: ssl.VerifyMode = ssl.CERT_REQUIRED,
        ciphers: Optional[str] = None,
    ):
        self._enabled = enabled
        self._cafile = cafile
        self._capath = capath
        self._certfile = certfile
        self._keyfile = keyfile
        self._keyfile_password = keyfile_password
        self._protocol = protocol
        self._check_hostname = check_hostname
        self._verify_mode = verify_mode
        self._ciphers = ciphers

    @property
    def enabled(self) -> bool:
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool) -> None:
        self._enabled = value

    @property
    def cafile(self) -> Optional[str]:
        return self._cafile

    @cafile.setter
    def cafile(self, value: Optional[str]) -> None:
        self._cafile = value

    @property
    def capath(self) -> Optional[str]:
        return self._capath

    @capath.setter
    def capath(self, value: Optional[str]) -> None:
        self._capath = value

    @property
    def certfile(self) -> Optional[str]:
        return self._certfile

    @certfile.setter
    def certfile(self, value: Optional[str]) -> None:
        self._certfile = value

    @property
    def keyfile(self) -> Optional[str]:
        return self._keyfile

    @keyfile.setter
    def keyfile(self, value: Optional[str]) -> None:
        self._keyfile = value

    @property
    def keyfile_password(self) -> Optional[str]:
        return self._keyfile_password

    @keyfile_password.setter
    def keyfile_password(self, value: Optional[str]) -> None:
        self._keyfile_password = value

    @property
    def protocol(self) -> SSLProtocol:
        return self._protocol

    @protocol.setter
    def protocol(self, value: SSLProtocol) -> None:
        self._protocol = value

    @property
    def check_hostname(self) -> bool:
        return self._check_hostname

    @check_hostname.setter
    def check_hostname(self, value: bool) -> None:
        self._check_hostname = value

    @property
    def verify_mode(self) -> ssl.VerifyMode:
        return self._verify_mode

    @verify_mode.setter
    def verify_mode(self, value: ssl.VerifyMode) -> None:
        self._verify_mode = value

    @property
    def ciphers(self) -> Optional[str]:
        return self._ciphers

    @ciphers.setter
    def ciphers(self, value: Optional[str]) -> None:
        self._ciphers = value

    def create_ssl_context(self) -> ssl.SSLContext:
        """Create an SSL context from this configuration.

        Returns:
            Configured SSLContext.
        """
        protocol_map = {
            SSLProtocol.TLS: ssl.PROTOCOL_TLS_CLIENT,
            SSLProtocol.TLSv1_2: ssl.PROTOCOL_TLS_CLIENT,
            SSLProtocol.TLSv1_3: ssl.PROTOCOL_TLS_CLIENT,
        }

        ssl_protocol = protocol_map.get(self._protocol, ssl.PROTOCOL_TLS_CLIENT)
        context = ssl.SSLContext(ssl_protocol)

        context.check_hostname = self._check_hostname
        context.verify_mode = self._verify_mode

        if self._protocol == SSLProtocol.TLSv1_2:
            context.minimum_version = ssl.TLSVersion.TLSv1_2
        elif self._protocol == SSLProtocol.TLSv1_3:
            context.minimum_version = ssl.TLSVersion.TLSv1_3

        if self._cafile or self._capath:
            context.load_verify_locations(
                cafile=self._cafile,
                capath=self._capath,
            )
        elif self._verify_mode != ssl.CERT_NONE:
            context.load_default_certs()

        if self._certfile:
            context.load_cert_chain(
                certfile=self._certfile,
                keyfile=self._keyfile,
                password=self._keyfile_password,
            )

        if self._ciphers:
            context.set_ciphers(self._ciphers)

        return context

    @classmethod
    def from_dict(cls, data: dict) -> "SSLConfig":
        """Create SSLConfig from a dictionary."""
        protocol_str = data.get("protocol", "TLS")
        try:
            protocol = SSLProtocol(protocol_str)
        except ValueError:
            protocol = SSLProtocol.TLS

        verify_mode_str = data.get("verify_mode", "CERT_REQUIRED")
        verify_mode_map = {
            "CERT_NONE": ssl.CERT_NONE,
            "CERT_OPTIONAL": ssl.CERT_OPTIONAL,
            "CERT_REQUIRED": ssl.CERT_REQUIRED,
        }
        verify_mode = verify_mode_map.get(verify_mode_str, ssl.CERT_REQUIRED)

        return cls(
            enabled=data.get("enabled", False),
            cafile=data.get("cafile"),
            capath=data.get("capath"),
            certfile=data.get("certfile"),
            keyfile=data.get("keyfile"),
            keyfile_password=data.get("keyfile_password"),
            protocol=protocol,
            check_hostname=data.get("check_hostname", True),
            verify_mode=verify_mode,
            ciphers=data.get("ciphers"),
        )
