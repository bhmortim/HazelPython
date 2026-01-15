"""Security components for Hazelcast client.

This module provides a unified interface to security-related functionality
including SSL/TLS configuration, authentication, and socket interception.
"""

from hazelcast.network.ssl_config import SSLConfig, SSLProtocol
from hazelcast.network.socket_interceptor import (
    SocketInterceptor,
    NoOpSocketInterceptor,
    SocketOptionsInterceptor,
)
from hazelcast.auth import (
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

__all__ = [
    "SSLConfig",
    "SSLProtocol",
    "SocketInterceptor",
    "NoOpSocketInterceptor",
    "SocketOptionsInterceptor",
    "Credentials",
    "CredentialsType",
    "UsernamePasswordCredentials",
    "TokenCredentials",
    "KerberosCredentials",
    "CustomCredentials",
    "CredentialsFactory",
    "StaticCredentialsFactory",
    "CallableCredentialsFactory",
    "AuthenticationService",
]
