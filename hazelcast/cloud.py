"""Hazelcast Cloud (Viridian) discovery and connection support."""

import json
import ssl
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

from hazelcast.discovery.base import DiscoveryStrategy, DiscoveryNode, DiscoveryException


_CLOUD_URL_BASE = "https://api.viridian.hazelcast.com"
_CLOUD_URL_PATH = "/cluster/discovery"
_DEFAULT_TIMEOUT = 10.0


@dataclass
class CloudConfig:
    """Configuration for Hazelcast Cloud (Viridian) discovery.

    Attributes:
        token: API discovery token from Hazelcast Cloud console.
        cluster_id: Optional cluster ID for filtering.
        cloud_url: Base URL for the Viridian API (defaults to production).
        connection_timeout: HTTP connection timeout in seconds.
        tls_enabled: Whether TLS is enabled for cluster connections.
        tls_ca_path: Path to CA certificate file for TLS verification.
        tls_cert_path: Path to client certificate file for mutual TLS.
        tls_key_path: Path to client private key file for mutual TLS.
        tls_password: Password for encrypted private key.

    Example:
        Basic cloud configuration::

            config = CloudConfig(
                token="your-discovery-token",
            )

        With custom TLS settings::

            config = CloudConfig(
                token="your-discovery-token",
                tls_enabled=True,
                tls_ca_path="/path/to/ca.pem",
            )
    """

    token: str
    cluster_id: Optional[str] = None
    cloud_url: str = _CLOUD_URL_BASE
    connection_timeout: float = _DEFAULT_TIMEOUT
    tls_enabled: bool = True
    tls_ca_path: Optional[str] = None
    tls_cert_path: Optional[str] = None
    tls_key_path: Optional[str] = None
    tls_password: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.token:
            raise ValueError("Cloud discovery token is required")

    @classmethod
    def from_dict(cls, data: dict) -> "CloudConfig":
        """Create CloudConfig from a dictionary.

        Args:
            data: Configuration dictionary.

        Returns:
            CloudConfig instance.
        """
        return cls(
            token=data.get("token", ""),
            cluster_id=data.get("cluster_id"),
            cloud_url=data.get("cloud_url", _CLOUD_URL_BASE),
            connection_timeout=data.get("connection_timeout", _DEFAULT_TIMEOUT),
            tls_enabled=data.get("tls_enabled", True),
            tls_ca_path=data.get("tls_ca_path"),
            tls_cert_path=data.get("tls_cert_path"),
            tls_key_path=data.get("tls_key_path"),
            tls_password=data.get("tls_password"),
            properties=data.get("properties", {}),
        )

    @property
    def discovery_url(self) -> str:
        """Get the full discovery API URL."""
        base = self.cloud_url.rstrip("/")
        return f"{base}{_CLOUD_URL_PATH}"


class HazelcastCloudDiscovery(DiscoveryStrategy):
    """Discovery strategy for Hazelcast Cloud (Viridian) clusters.

    This strategy discovers cluster members by querying the Hazelcast
    Viridian API using a discovery token. It supports automatic TLS
    configuration for secure cluster connections.

    Example:
        Using directly::

            config = CloudConfig(token="your-token")
            discovery = HazelcastCloudDiscovery(config)
            discovery.start()
            nodes = discovery.discover_nodes()

        Via client configuration::

            client_config = ClientConfig()
            client_config.discovery.enabled = True
            client_config.discovery.strategy_type = DiscoveryStrategyType.CLOUD
            client_config.discovery.cloud = {"token": "your-token"}
    """

    def __init__(self, config: CloudConfig):
        """Initialize the Hazelcast Cloud discovery strategy.

        Args:
            config: Cloud configuration with discovery token.
        """
        super().__init__(config.properties)
        self._config = config
        self._ssl_context: Optional[ssl.SSLContext] = None
        self._cached_nodes: List[DiscoveryNode] = []

    @property
    def config(self) -> CloudConfig:
        """Get the cloud configuration."""
        return self._config

    @property
    def ssl_context(self) -> Optional[ssl.SSLContext]:
        """Get the SSL context for cluster connections."""
        return self._ssl_context

    def start(self) -> None:
        """Start the discovery strategy and initialize TLS if enabled."""
        super().start()
        if self._config.tls_enabled:
            self._ssl_context = self._create_ssl_context()

    def stop(self) -> None:
        """Stop the discovery strategy and clean up resources."""
        super().stop()
        self._ssl_context = None
        self._cached_nodes = []

    def _create_ssl_context(self) -> ssl.SSLContext:
        """Create SSL context for secure connections.

        Returns:
            Configured SSL context.
        """
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

        if self._config.tls_ca_path:
            context.load_verify_locations(cafile=self._config.tls_ca_path)

        if self._config.tls_cert_path and self._config.tls_key_path:
            context.load_cert_chain(
                certfile=self._config.tls_cert_path,
                keyfile=self._config.tls_key_path,
                password=self._config.tls_password,
            )

        return context

    def discover_nodes(self) -> List[DiscoveryNode]:
        """Discover cluster member nodes from Hazelcast Cloud.

        Queries the Viridian API to retrieve the list of cluster
        member addresses for the configured discovery token.

        Returns:
            List of discovered nodes.

        Raises:
            DiscoveryException: If the API request fails.
        """
        if not self._started:
            raise DiscoveryException("Discovery strategy has not been started")

        try:
            response_data = self._fetch_cluster_info()
            nodes = self._parse_nodes(response_data)
            self._cached_nodes = nodes
            return nodes
        except DiscoveryException:
            raise
        except Exception as e:
            raise DiscoveryException(f"Failed to discover cloud nodes: {e}") from e

    def _fetch_cluster_info(self) -> dict:
        """Fetch cluster information from Viridian API.

        Returns:
            Parsed JSON response from the API.

        Raises:
            DiscoveryException: If the request fails.
        """
        url = self._config.discovery_url
        headers = {
            "Authorization": f"Bearer {self._config.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        request = urllib.request.Request(url, headers=headers, method="GET")

        api_ssl_context = ssl.create_default_context()

        try:
            with urllib.request.urlopen(
                request,
                timeout=self._config.connection_timeout,
                context=api_ssl_context,
            ) as response:
                data = response.read().decode("utf-8")
                return json.loads(data)
        except urllib.error.HTTPError as e:
            error_body = ""
            try:
                error_body = e.read().decode("utf-8")
            except Exception:
                pass
            raise DiscoveryException(
                f"Cloud API request failed with status {e.code}: {error_body}"
            ) from e
        except urllib.error.URLError as e:
            raise DiscoveryException(
                f"Cloud API request failed: {e.reason}"
            ) from e
        except json.JSONDecodeError as e:
            raise DiscoveryException(
                f"Failed to parse cloud API response: {e}"
            ) from e

    def _parse_nodes(self, response_data: dict) -> List[DiscoveryNode]:
        """Parse discovery nodes from API response.

        Args:
            response_data: Parsed JSON response.

        Returns:
            List of DiscoveryNode instances.
        """
        nodes = []

        members = response_data.get("members", [])
        if not members and "privateAddress" in response_data:
            members = [response_data]

        for member in members:
            private_addr = member.get("privateAddress", "")
            public_addr = member.get("publicAddress")
            port = member.get("port", 5701)

            if ":" in private_addr:
                host, port_str = private_addr.rsplit(":", 1)
                private_addr = host
                try:
                    port = int(port_str)
                except ValueError:
                    pass

            if public_addr and ":" in public_addr:
                public_addr = public_addr.rsplit(":", 1)[0]

            if private_addr:
                node = DiscoveryNode(
                    private_address=private_addr,
                    port=port,
                    public_address=public_addr,
                    properties={
                        "cluster_id": response_data.get("clusterId"),
                        "cluster_name": response_data.get("clusterName"),
                        "cloud_provider": response_data.get("cloudProvider"),
                        "region": response_data.get("region"),
                    },
                )
                nodes.append(node)

        return nodes

    def get_known_addresses(self) -> List[str]:
        """Get addresses of discovered nodes.

        Returns:
            List of address strings in host:port format.
        """
        if not self._cached_nodes:
            self._cached_nodes = self.discover_nodes()
        return [node.address for node in self._cached_nodes]

    def get_tls_config(self) -> Optional[Dict[str, Any]]:
        """Get TLS configuration for cluster connections.

        Returns:
            Dictionary with TLS settings, or None if TLS is disabled.
        """
        if not self._config.tls_enabled:
            return None

        return {
            "enabled": True,
            "ca_path": self._config.tls_ca_path,
            "cert_path": self._config.tls_cert_path,
            "key_path": self._config.tls_key_path,
            "password": self._config.tls_password,
        }
