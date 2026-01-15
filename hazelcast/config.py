"""Hazelcast client configuration."""

from typing import List, Optional


class ClientConfig:
    """Configuration for the Hazelcast client."""

    def __init__(self):
        """Initialize client configuration with defaults."""
        self._cluster_name: str = "dev"
        self._cluster_members: List[str] = ["localhost:5701"]
        self._connection_timeout: float = 5.0
        self._retry_initial_backoff: float = 1.0
        self._retry_max_backoff: float = 30.0
        self._retry_multiplier: float = 2.0
        self._smart_routing: bool = True
        self._credentials: Optional[dict] = None

    @property
    def cluster_name(self) -> str:
        """Get the cluster name."""
        return self._cluster_name

    @cluster_name.setter
    def cluster_name(self, value: str) -> None:
        """Set the cluster name."""
        self._cluster_name = value

    @property
    def cluster_members(self) -> List[str]:
        """Get the list of cluster member addresses."""
        return self._cluster_members

    @cluster_members.setter
    def cluster_members(self, value: List[str]) -> None:
        """Set the list of cluster member addresses."""
        self._cluster_members = value

    @property
    def connection_timeout(self) -> float:
        """Get the connection timeout in seconds."""
        return self._connection_timeout

    @connection_timeout.setter
    def connection_timeout(self, value: float) -> None:
        """Set the connection timeout in seconds."""
        self._connection_timeout = value

    @property
    def smart_routing(self) -> bool:
        """Get whether smart routing is enabled."""
        return self._smart_routing

    @smart_routing.setter
    def smart_routing(self, value: bool) -> None:
        """Set whether smart routing is enabled."""
        self._smart_routing = value

    @property
    def credentials(self) -> Optional[dict]:
        """Get the authentication credentials."""
        return self._credentials

    @credentials.setter
    def credentials(self, value: Optional[dict]) -> None:
        """Set the authentication credentials."""
        self._credentials = value
