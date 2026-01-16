"""Base classes and context for Hazelcast proxies."""

from typing import Any, Optional


class ProxyContext:
    """Context providing services needed by proxies.

    Attributes:
        invocation_service: Service for sending requests to the cluster.
        serialization_service: Service for serializing/deserializing data.
    """

    def __init__(
        self,
        invocation_service: Any = None,
        serialization_service: Any = None,
    ):
        self.invocation_service = invocation_service
        self.serialization_service = serialization_service
