"""Failover configuration for high availability.

This module provides failover configuration for Hazelcast clients to
support high availability across multiple clusters. When the primary
cluster becomes unavailable, the client can automatically switch to
a backup cluster.

Key features:
- Multiple cluster configurations with priority ordering
- CNAME-based dynamic cluster discovery via DNS
- Configurable retry counts per cluster
- Automatic cluster switching on failure

Example:
    Basic failover configuration::

        from hazelcast.failover import FailoverConfig

        failover = FailoverConfig(try_count=3)
        failover.add_cluster("primary", ["node1:5701", "node2:5701"])
        failover.add_cluster("backup", ["backup1:5701"], priority=1)

    CNAME-based discovery::

        failover = FailoverConfig()
        failover.add_cname_cluster(
            "production",
            dns_name="hazelcast.example.com",
            port=5701,
            refresh_interval=60.0
        )
"""

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, TYPE_CHECKING
import socket
import threading
import time

if TYPE_CHECKING:
    from hazelcast.config import ClientConfig


@dataclass
class ClusterConfig:
    """Configuration for a single cluster in a failover setup.

    Represents the connection details for one Hazelcast cluster
    in a multi-cluster failover configuration.

    Attributes:
        cluster_name: Name of the cluster (must match server configuration).
        addresses: List of member addresses in "host:port" format.
        priority: Priority for failover ordering (lower = higher priority).

    Example:
        >>> config = ClusterConfig(
        ...     cluster_name="production",
        ...     addresses=["node1:5701", "node2:5701"],
        ...     priority=0
        ... )
    """

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
    client reconfiguration. The resolver caches results and
    periodically refreshes them.

    Args:
        dns_name: The DNS name to resolve (e.g., "hazelcast.example.com").
        port: The port to use for resolved addresses. Defaults to 5701.
        refresh_interval: How often to refresh DNS in seconds. Defaults to 60.

    Attributes:
        dns_name: The DNS name being resolved.
        port: The port used for resolved addresses.

    Example:
        >>> resolver = CNAMEResolver(
        ...     dns_name="hazelcast.example.com",
        ...     port=5701,
        ...     refresh_interval=30.0
        ... )
        >>> addresses = resolver.resolve()
        >>> print(addresses)
        ['192.168.1.10:5701', '192.168.1.11:5701']
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

        Uses cached results if the refresh interval hasn't elapsed.
        On DNS failure, returns previously cached addresses.

        Returns:
            List of resolved addresses in "host:port" format.

        Example:
            >>> resolver = CNAMEResolver("cluster.example.com")
            >>> addresses = resolver.resolve()
            ['10.0.0.1:5701', '10.0.0.2:5701']
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

        Ignores the refresh interval and immediately performs a new
        DNS lookup.

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

    The failover mechanism works as follows:
    1. Client attempts to connect to the current cluster
    2. On failure, retries up to ``try_count`` times
    3. If all retries fail, switches to the next cluster
    4. Process repeats through all configured clusters

    Attributes:
        try_count: Maximum connection attempts per cluster.
        clusters: List of configured clusters.
        cluster_count: Number of configured clusters.
        current_cluster_index: Index of the current cluster.

    Example:
        >>> failover = FailoverConfig(try_count=3)
        >>> failover.add_cluster("primary", ["node1:5701", "node2:5701"])
        >>> failover.add_cluster("dr-site", ["dr1:5701", "dr2:5701"], priority=1)
        >>> current = failover.get_current_cluster()
        >>> print(current.cluster_name)
        'primary'
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
                Defaults to 3.
            clusters: Optional list of pre-configured clusters. Clusters
                are sorted by priority (lower priority values first).
        """
        self._try_count = try_count
        self._clusters: List[ClusterConfig] = clusters or []
        self._cname_resolvers: Dict[str, CNAMEResolver] = {}
        self._current_cluster_index = 0
        self._lock = threading.Lock()

    @property
    def try_count(self) -> int:
        """Get the maximum try count per cluster.

        Returns:
            Number of connection attempts before switching clusters.
        """
        return self._try_count

    @try_count.setter
    def try_count(self, value: int) -> None:
        """Set the maximum try count.

        Args:
            value: Number of attempts (must be at least 1).

        Raises:
            ValueError: If value is less than 1.
        """
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

        Adds a static cluster configuration with known member addresses.
        Clusters are automatically sorted by priority.

        Args:
            cluster_name: Name of the cluster (must match server config).
            addresses: List of member addresses in "host:port" format.
            priority: Priority for failover ordering. Lower values have
                higher priority and are tried first. Defaults to 0.

        Returns:
            This config instance for method chaining.

        Example:
            >>> failover = FailoverConfig()
            >>> failover.add_cluster("primary", ["node1:5701"])
            ...        .add_cluster("backup", ["backup1:5701"], priority=1)
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

        Adds a cluster that discovers member addresses dynamically
        via DNS resolution. This allows the cluster membership to
        change without client reconfiguration.

        Args:
            cluster_name: Name of the cluster (must match server config).
            dns_name: DNS name to resolve for member addresses
                (e.g., "hazelcast.example.com").
            port: Port to use for resolved addresses. Defaults to 5701.
            priority: Priority for failover ordering. Defaults to 0.
            refresh_interval: How often to refresh DNS in seconds.
                Defaults to 60.

        Returns:
            This config instance for method chaining.

        Example:
            >>> failover = FailoverConfig()
            >>> failover.add_cname_cluster(
            ...     "production",
            ...     dns_name="hz.prod.example.com",
            ...     refresh_interval=30.0
            ... )
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

        Returns the cluster that the client should currently connect to.

        Returns:
            Current cluster config, or ``None`` if no clusters configured.
        """
        with self._lock:
            if not self._clusters:
                return None
            return self._clusters[self._current_cluster_index]

    def get_cluster_addresses(self, cluster_name: str) -> List[str]:
        """Get addresses for a cluster.

        For CNAME-based clusters, performs DNS resolution. For static
        clusters, returns the configured addresses.

        Args:
            cluster_name: Name of the cluster.

        Returns:
            List of addresses in "host:port" format, or empty list
            if the cluster is not found.
        """
        if cluster_name in self._cname_resolvers:
            return self._cname_resolvers[cluster_name].resolve()

        for cluster in self._clusters:
            if cluster.cluster_name == cluster_name:
                return list(cluster.addresses)

        return []

    def switch_to_next_cluster(self) -> Optional[ClusterConfig]:
        """Switch to the next cluster in the failover list.

        Cycles through clusters in priority order. After the last
        cluster, wraps back to the first.

        Returns:
            The next cluster config, or ``None`` if no clusters configured.
        """
        with self._lock:
            if not self._clusters:
                return None

            self._current_cluster_index = (
                self._current_cluster_index + 1
            ) % len(self._clusters)

            return self._clusters[self._current_cluster_index]

    def reset(self) -> None:
        """Reset to the first (highest priority) cluster.

        Resets the current cluster index to 0, causing the next
        connection attempt to use the highest priority cluster.
        """
        with self._lock:
            self._current_cluster_index = 0

    def to_client_config(self, cluster_index: int = 0) -> "ClientConfig":
        """Create a ClientConfig for a specific cluster.

        Generates a client configuration suitable for connecting to
        one of the failover clusters.

        Args:
            cluster_index: Index of the cluster to configure.
                Defaults to 0 (highest priority).

        Returns:
            ClientConfig configured for the specified cluster.
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

        Factory method that creates a failover configuration from
        a list of individual client configurations. Each config
        becomes a cluster with priority based on its position.

        Args:
            configs: List of ClientConfig instances. Earlier configs
                have higher priority.

        Returns:
            FailoverConfig with all clusters configured.

        Example:
            >>> config1 = ClientConfig()
            >>> config1.cluster_name = "primary"
            >>> config2 = ClientConfig()
            >>> config2.cluster_name = "backup"
            >>> failover = FailoverConfig.from_configs([config1, config2])
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
