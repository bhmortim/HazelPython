"""Failover configuration for high availability."""

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, TYPE_CHECKING
import socket
import threading
import time

if TYPE_CHECKING:
    from hazelcast.config import ClientConfig


@dataclass
class ClusterConfig:
    """Configuration for a single cluster in a failover setup."""

    cluster_name: str
    addresses: List[str] = field(default_factory=list)
    priority: int = 0

    def __post_init__(self):
        if not self.addresses:
            self.addresses = ["localhost:5701"]


class CNAMEResolver:
    """Resolves cluster addresses using DNS CNAME records.

    Enables dynamic discovery of cluster members through DNS,
    allowing for dynamic cluster membership changes without
    client reconfiguration.
    """

    def __init__(
        self,
        dns_name: str,
        port: int = 5701,
        refresh_interval: float = 60.0,
    ):
        self._dns_name = dns_name
        self._port = port
        self._refresh_interval = refresh_interval
        self._resolved_addresses: List[str] = []
        self._last_refresh: float = 0.0
        self._lock = threading.Lock()

    @property
    def dns_name(self) -> str:
        """Get the DNS name being resolved."""
        return self._dns_name

    @property
    def port(self) -> int:
        """Get the port used for resolved addresses."""
        return self._port

    def resolve(self) -> List[str]:
        """Resolve the DNS name to addresses.

        Returns:
            List of resolved addresses in "host:port" format.
        """
        current_time = time.time()

        with self._lock:
            if (
                current_time - self._last_refresh < self._refresh_interval
                and self._resolved_addresses
            ):
                return list(self._resolved_addresses)

        try:
            _, _, addresses = socket.gethostbyname_ex(self._dns_name)
            resolved = [f"{addr}:{self._port}" for addr in addresses]

            with self._lock:
                self._resolved_addresses = resolved
                self._last_refresh = current_time

            return resolved

        except socket.gaierror:
            with self._lock:
                return list(self._resolved_addresses) if self._resolved_addresses else []

    def refresh(self) -> List[str]:
        """Force refresh the resolved addresses.

        Returns:
            List of newly resolved addresses.
        """
        with self._lock:
            self._last_refresh = 0.0
        return self.resolve()


class FailoverConfig:
    """Configuration for client failover across multiple clusters.

    Supports automatic failover between clusters when the primary
    cluster becomes unavailable, with configurable retry counts
    and CNAME-based dynamic discovery.
    """

    DEFAULT_TRY_COUNT = 3

    def __init__(
        self,
        try_count: int = DEFAULT_TRY_COUNT,
        clusters: Optional[List[ClusterConfig]] = None,
    ):
        """Initialize failover configuration.

        Args:
            try_count: Maximum number of connection attempts per cluster.
            clusters: List of cluster configurations in priority order.
        """
        self._try_count = try_count
        self._clusters: List[ClusterConfig] = clusters or []
        self._cname_resolvers: Dict[str, CNAMEResolver] = {}
        self._current_cluster_index = 0
        self._lock = threading.Lock()

    @property
    def try_count(self) -> int:
        """Get the maximum try count per cluster."""
        return self._try_count

    @try_count.setter
    def try_count(self, value: int) -> None:
        """Set the maximum try count."""
        if value < 1:
            raise ValueError("try_count must be at least 1")
        self._try_count = value

    @property
    def clusters(self) -> List[ClusterConfig]:
        """Get the list of cluster configurations."""
        return list(self._clusters)

    @property
    def cluster_count(self) -> int:
        """Get the number of configured clusters."""
        return len(self._clusters)

    @property
    def current_cluster_index(self) -> int:
        """Get the current cluster index."""
        with self._lock:
            return self._current_cluster_index

    def add_cluster(
        self,
        cluster_name: str,
        addresses: List[str],
        priority: int = 0,
    ) -> "FailoverConfig":
        """Add a cluster configuration.

        Args:
            cluster_name: Name of the cluster.
            addresses: List of member addresses.
            priority: Priority (lower is higher priority).

        Returns:
            This config for chaining.
        """
        config = ClusterConfig(
            cluster_name=cluster_name,
            addresses=addresses,
            priority=priority,
        )
        self._clusters.append(config)
        self._clusters.sort(key=lambda c: c.priority)
        return self

    def add_cname_cluster(
        self,
        cluster_name: str,
        dns_name: str,
        port: int = 5701,
        priority: int = 0,
        refresh_interval: float = 60.0,
    ) -> "FailoverConfig":
        """Add a cluster with CNAME-based discovery.

        Args:
            cluster_name: Name of the cluster.
            dns_name: DNS name for dynamic resolution.
            port: Port for resolved addresses.
            priority: Priority (lower is higher priority).
            refresh_interval: DNS refresh interval in seconds.

        Returns:
            This config for chaining.
        """
        resolver = CNAMEResolver(dns_name, port, refresh_interval)
        self._cname_resolvers[cluster_name] = resolver

        config = ClusterConfig(
            cluster_name=cluster_name,
            addresses=[],
            priority=priority,
        )
        self._clusters.append(config)
        self._clusters.sort(key=lambda c: c.priority)
        return self

    def get_current_cluster(self) -> Optional[ClusterConfig]:
        """Get the current cluster configuration.

        Returns:
            Current cluster config, or None if no clusters configured.
        """
        with self._lock:
            if not self._clusters:
                return None
            return self._clusters[self._current_cluster_index]

    def get_cluster_addresses(self, cluster_name: str) -> List[str]:
        """Get addresses for a cluster.

        Resolves CNAME if configured, otherwise returns static addresses.

        Args:
            cluster_name: Name of the cluster.

        Returns:
            List of addresses for the cluster.
        """
        if cluster_name in self._cname_resolvers:
            return self._cname_resolvers[cluster_name].resolve()

        for cluster in self._clusters:
            if cluster.cluster_name == cluster_name:
                return list(cluster.addresses)

        return []

    def switch_to_next_cluster(self) -> Optional[ClusterConfig]:
        """Switch to the next cluster in the failover list.

        Returns:
            The next cluster config, or None if no more clusters.
        """
        with self._lock:
            if not self._clusters:
                return None

            self._current_cluster_index = (
                self._current_cluster_index + 1
            ) % len(self._clusters)

            return self._clusters[self._current_cluster_index]

    def reset(self) -> None:
        """Reset to the first cluster."""
        with self._lock:
            self._current_cluster_index = 0

    def to_client_config(self, cluster_index: int = 0) -> "ClientConfig":
        """Create a ClientConfig for a specific cluster.

        Args:
            cluster_index: Index of the cluster to configure.

        Returns:
            A ClientConfig for the specified cluster.
        """
        from hazelcast.config import ClientConfig

        if not self._clusters or cluster_index >= len(self._clusters):
            return ClientConfig()

        cluster = self._clusters[cluster_index]
        addresses = self.get_cluster_addresses(cluster.cluster_name)

        config = ClientConfig()
        config.cluster_name = cluster.cluster_name
        config.cluster_members = addresses if addresses else cluster.addresses

        return config

    @classmethod
    def from_configs(cls, configs: List["ClientConfig"]) -> "FailoverConfig":
        """Create a FailoverConfig from multiple ClientConfigs.

        Args:
            configs: List of client configurations.

        Returns:
            A FailoverConfig with the clusters configured.
        """
        failover = cls()
        for i, config in enumerate(configs):
            failover.add_cluster(
                cluster_name=config.cluster_name,
                addresses=config.cluster_members,
                priority=i,
            )
        return failover

    def __repr__(self) -> str:
        cluster_names = [c.cluster_name for c in self._clusters]
        return (
            f"FailoverConfig(try_count={self._try_count}, "
            f"clusters={cluster_names})"
        )
