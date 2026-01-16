"""Base classes for cloud discovery strategies."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Dict, Any


@dataclass
class DiscoveryNode:
    """Represents a discovered Hazelcast cluster member.

    Attributes:
        private_address: Private IP address of the member.
        public_address: Public IP address (if available).
        port: Hazelcast port number.
        properties: Additional node properties/metadata.
    """

    private_address: str
    port: int = 5701
    public_address: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.properties is None:
            self.properties = {}

    @property
    def address(self) -> str:
        """Get the address string in host:port format."""
        return f"{self.private_address}:{self.port}"

    @property
    def public_address_str(self) -> Optional[str]:
        """Get the public address string in host:port format."""
        if self.public_address:
            return f"{self.public_address}:{self.port}"
        return None


class DiscoveryStrategy(ABC):
    """Abstract base class for cloud discovery strategies.

    Implementations should discover Hazelcast cluster members
    from various cloud providers or orchestration platforms.
    """

    def __init__(self, properties: Optional[Dict[str, Any]] = None):
        """Initialize the discovery strategy.

        Args:
            properties: Provider-specific configuration properties.
        """
        self._properties = properties or {}
        self._started = False

    @property
    def is_started(self) -> bool:
        """Check if the discovery strategy has been started."""
        return self._started

    def start(self) -> None:
        """Start the discovery strategy.

        Called when the client begins the discovery process.
        Implementations can perform initialization here.
        """
        self._started = True

    def stop(self) -> None:
        """Stop the discovery strategy.

        Called when the client shuts down.
        Implementations should clean up resources here.
        """
        self._started = False

    @abstractmethod
    def discover_nodes(self) -> List[DiscoveryNode]:
        """Discover cluster member nodes.

        Returns:
            List of discovered nodes.

        Raises:
            DiscoveryException: If discovery fails.
        """
        pass

    def get_known_addresses(self) -> List[str]:
        """Get addresses of discovered nodes.

        Returns:
            List of address strings in host:port format.
        """
        nodes = self.discover_nodes()
        return [node.address for node in nodes]


class DiscoveryException(Exception):
    """Exception raised when discovery fails."""

    pass
