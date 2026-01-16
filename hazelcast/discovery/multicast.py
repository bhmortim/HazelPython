"""Multicast discovery strategy for Hazelcast cluster member discovery."""

import socket
import struct
import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

from hazelcast.discovery.base import DiscoveryStrategy, DiscoveryNode, DiscoveryException


@dataclass
class MulticastConfig:
    """Configuration for multicast discovery.

    Attributes:
        group: Multicast group address.
        port: Multicast port number.
        timeout_seconds: Discovery timeout in seconds.
        ttl: Time-to-live for multicast packets.
        loopback_enabled: Whether to receive own multicast packets.
        trusted_interfaces: List of trusted network interfaces.
    """

    group: str = "224.2.2.3"
    port: int = 54327
    timeout_seconds: float = 3.0
    ttl: int = 32
    loopback_enabled: bool = False
    trusted_interfaces: List[str] = field(default_factory=list)

    def __post_init__(self):
        self._validate()

    def _validate(self) -> None:
        if not self.group:
            raise ValueError("Multicast group address cannot be empty")
        if not self._is_multicast_address(self.group):
            raise ValueError(
                f"Invalid multicast address: {self.group}. "
                "Must be in range 224.0.0.0 to 239.255.255.255"
            )
        if not (1 <= self.port <= 65535):
            raise ValueError(f"Invalid port: {self.port}. Must be between 1 and 65535")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not (0 <= self.ttl <= 255):
            raise ValueError(f"Invalid TTL: {self.ttl}. Must be between 0 and 255")

    @staticmethod
    def _is_multicast_address(address: str) -> bool:
        try:
            parts = address.split(".")
            if len(parts) != 4:
                return False
            first_octet = int(parts[0])
            return 224 <= first_octet <= 239
        except (ValueError, AttributeError):
            return False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MulticastConfig":
        """Create MulticastConfig from a dictionary."""
        return cls(
            group=data.get("group", "224.2.2.3"),
            port=data.get("port", 54327),
            timeout_seconds=data.get("timeout_seconds", 3.0),
            ttl=data.get("ttl", 32),
            loopback_enabled=data.get("loopback_enabled", False),
            trusted_interfaces=data.get("trusted_interfaces", []),
        )


class MulticastDiscoveryStrategy(DiscoveryStrategy):
    """Discovery strategy using UDP multicast.

    Discovers Hazelcast cluster members by sending multicast discovery
    requests and listening for responses from cluster members.

    The protocol uses a simple request/response pattern:
    - Client sends a discovery request to the multicast group
    - Cluster members respond with their address information
    """

    DISCOVERY_REQUEST = b"HZ-DISCOVERY-REQUEST"
    DISCOVERY_RESPONSE_PREFIX = b"HZ-DISCOVERY-RESPONSE:"
    HAZELCAST_DEFAULT_PORT = 5701

    def __init__(
        self,
        config: Optional[MulticastConfig] = None,
        properties: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the multicast discovery strategy.

        Args:
            config: Multicast configuration. Uses defaults if not provided.
            properties: Additional properties (for compatibility).
        """
        super().__init__(properties)
        self._config = config or MulticastConfig()
        self._discovered_nodes: List[DiscoveryNode] = []
        self._lock = threading.Lock()
        self._socket: Optional[socket.socket] = None

    @property
    def config(self) -> MulticastConfig:
        """Get the multicast configuration."""
        return self._config

    def start(self) -> None:
        """Start the discovery strategy."""
        super().start()
        self._discovered_nodes = []

    def stop(self) -> None:
        """Stop the discovery strategy and clean up resources."""
        super().stop()
        self._close_socket()
        with self._lock:
            self._discovered_nodes = []

    def _close_socket(self) -> None:
        """Close the multicast socket if open."""
        if self._socket:
            try:
                self._socket.close()
            except OSError:
                pass
            self._socket = None

    def _create_socket(self) -> socket.socket:
        """Create and configure a multicast socket."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        if hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass

        sock.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_MULTICAST_TTL,
            struct.pack("b", self._config.ttl),
        )

        loopback = 1 if self._config.loopback_enabled else 0
        sock.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_MULTICAST_LOOP,
            struct.pack("b", loopback),
        )

        sock.settimeout(self._config.timeout_seconds)

        return sock

    def discover_nodes(self) -> List[DiscoveryNode]:
        """Discover cluster member nodes via multicast.

        Sends a discovery request to the multicast group and collects
        responses from cluster members.

        Returns:
            List of discovered nodes.

        Raises:
            DiscoveryException: If discovery fails due to network issues.
        """
        if not self._started:
            raise DiscoveryException("Discovery strategy has not been started")

        discovered: List[DiscoveryNode] = []
        seen_addresses: set = set()

        try:
            self._socket = self._create_socket()
            self._send_discovery_request()
            discovered = self._collect_responses(seen_addresses)
        except OSError as e:
            raise DiscoveryException(f"Multicast discovery failed: {e}")
        finally:
            self._close_socket()

        with self._lock:
            self._discovered_nodes = discovered

        return discovered

    def _send_discovery_request(self) -> None:
        """Send a discovery request to the multicast group."""
        if not self._socket:
            return
        self._socket.sendto(
            self.DISCOVERY_REQUEST,
            (self._config.group, self._config.port),
        )

    def _collect_responses(self, seen_addresses: set) -> List[DiscoveryNode]:
        """Collect discovery responses within the timeout period."""
        discovered: List[DiscoveryNode] = []
        start_time = time.monotonic()
        remaining_time = self._config.timeout_seconds

        while remaining_time > 0:
            try:
                if self._socket:
                    self._socket.settimeout(remaining_time)
                    data, addr = self._socket.recvfrom(1024)
                    node = self._parse_response(data, addr)
                    if node and node.address not in seen_addresses:
                        seen_addresses.add(node.address)
                        discovered.append(node)
            except socket.timeout:
                break
            except OSError:
                break

            elapsed = time.monotonic() - start_time
            remaining_time = self._config.timeout_seconds - elapsed

        return discovered

    def _parse_response(
        self, data: bytes, sender_addr: tuple
    ) -> Optional[DiscoveryNode]:
        """Parse a discovery response.

        Args:
            data: Raw response data.
            sender_addr: Address tuple (ip, port) of the sender.

        Returns:
            DiscoveryNode if valid response, None otherwise.
        """
        if not data.startswith(self.DISCOVERY_RESPONSE_PREFIX):
            return None

        try:
            payload = data[len(self.DISCOVERY_RESPONSE_PREFIX):].decode("utf-8")
            parts = payload.split(":")

            if len(parts) >= 2:
                host = parts[0]
                port = int(parts[1])
            elif len(parts) == 1 and parts[0]:
                host = parts[0]
                port = self.HAZELCAST_DEFAULT_PORT
            else:
                host = sender_addr[0]
                port = self.HAZELCAST_DEFAULT_PORT

            if not self._is_trusted_address(host):
                return None

            public_address = None
            if len(parts) >= 4:
                public_address = parts[2] if parts[2] else None

            properties = {}
            if len(parts) >= 5:
                try:
                    props_str = ":".join(parts[4:])
                    if props_str:
                        for prop in props_str.split(","):
                            if "=" in prop:
                                k, v = prop.split("=", 1)
                                properties[k.strip()] = v.strip()
                except (ValueError, IndexError):
                    pass

            return DiscoveryNode(
                private_address=host,
                port=port,
                public_address=public_address,
                properties=properties,
            )
        except (ValueError, UnicodeDecodeError):
            return None

    def _is_trusted_address(self, address: str) -> bool:
        """Check if an address is trusted.

        Args:
            address: IP address to check.

        Returns:
            True if trusted or no trust filter configured.
        """
        if not self._config.trusted_interfaces:
            return True
        return address in self._config.trusted_interfaces

    def get_known_addresses(self) -> List[str]:
        """Get addresses of discovered nodes.

        Returns:
            List of address strings in host:port format.
        """
        with self._lock:
            return [node.address for node in self._discovered_nodes]
