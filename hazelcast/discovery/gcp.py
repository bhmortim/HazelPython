"""GCP discovery strategy for Compute Engine instances."""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

from hazelcast.discovery.base import DiscoveryStrategy, DiscoveryNode, DiscoveryException


@dataclass
class GcpConfig:
    """Configuration for GCP discovery.

    Attributes:
        project: GCP project ID.
        zone: Compute Engine zone (e.g., 'us-central1-a').
        region: Compute Engine region for regional discovery.
        label_key: Filter by label key.
        label_value: Filter by label value.
        service_account_json: Path to service account JSON key file.
        use_public_ip: Whether to use public IP addresses.
        hz_port: Hazelcast port on discovered instances.
    """

    project: Optional[str] = None
    zone: Optional[str] = None
    region: Optional[str] = None
    label_key: Optional[str] = None
    label_value: Optional[str] = None
    service_account_json: Optional[str] = None
    use_public_ip: bool = False
    hz_port: int = 5701
    filters: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> "GcpConfig":
        """Create GcpConfig from a dictionary."""
        return cls(
            project=data.get("project"),
            zone=data.get("zone"),
            region=data.get("region"),
            label_key=data.get("label_key"),
            label_value=data.get("label_value"),
            service_account_json=data.get("service_account_json"),
            use_public_ip=data.get("use_public_ip", False),
            hz_port=data.get("hz_port", 5701),
            filters=data.get("filters", {}),
        )


class GcpDiscoveryStrategy(DiscoveryStrategy):
    """GCP discovery strategy for Compute Engine instances.

    Discovers Hazelcast cluster members running on Google Compute Engine.
    Supports filtering by zones, regions, and labels.

    Example:
        Zone-based discovery::

            config = GcpConfig(
                project="my-project",
                zone="us-central1-a",
                label_key="hazelcast-cluster",
                label_value="production",
            )
            strategy = GcpDiscoveryStrategy(config)
            nodes = strategy.discover_nodes()

        Region-based discovery::

            config = GcpConfig(
                project="my-project",
                region="us-central1",
            )
            strategy = GcpDiscoveryStrategy(config)
            nodes = strategy.discover_nodes()
    """

    def __init__(self, config: Optional[GcpConfig] = None):
        """Initialize GCP discovery strategy.

        Args:
            config: GCP configuration options.
        """
        super().__init__()
        self._config = config or GcpConfig()
        self._compute = None

    @property
    def config(self) -> GcpConfig:
        """Get the GCP configuration."""
        return self._config

    def start(self) -> None:
        """Start the GCP discovery strategy and initialize clients."""
        super().start()
        self._initialize_client()

    def stop(self) -> None:
        """Stop the GCP discovery strategy."""
        super().stop()
        self._compute = None

    def _initialize_client(self) -> None:
        """Initialize the Google Compute Engine client."""
        try:
            from google.cloud import compute_v1
            from google.oauth2 import service_account
        except ImportError:
            raise DiscoveryException(
                "google-cloud-compute is required for GCP discovery. "
                "Install with: pip install google-cloud-compute"
            )

        try:
            credentials = None
            if self._config.service_account_json:
                credentials = service_account.Credentials.from_service_account_file(
                    self._config.service_account_json
                )

            self._compute = compute_v1.InstancesClient(credentials=credentials)

        except Exception as e:
            raise DiscoveryException(f"Failed to initialize GCP client: {e}")

    def discover_nodes(self) -> List[DiscoveryNode]:
        """Discover Hazelcast nodes on GCP.

        Returns:
            List of discovered nodes.

        Raises:
            DiscoveryException: If discovery fails.
        """
        if not self._started:
            self.start()

        if not self._config.project:
            raise DiscoveryException("project is required for GCP discovery")

        if self._config.zone:
            return self._discover_zone_instances()
        elif self._config.region:
            return self._discover_region_instances()
        else:
            return self._discover_all_instances()

    def _discover_zone_instances(self) -> List[DiscoveryNode]:
        """Discover instances in a specific zone."""
        try:
            request = {"project": self._config.project, "zone": self._config.zone}

            filter_expr = self._build_filter()
            if filter_expr:
                request["filter"] = filter_expr

            instances = self._compute.list(request=request)

        except Exception as e:
            raise DiscoveryException(f"Failed to list GCP instances: {e}")

        return self._instances_to_nodes(instances)

    def _discover_region_instances(self) -> List[DiscoveryNode]:
        """Discover instances in all zones of a region."""
        try:
            from google.cloud import compute_v1

            zones_client = compute_v1.ZonesClient()
            zones = zones_client.list(project=self._config.project)

            region_zones = [
                z.name
                for z in zones
                if z.name.startswith(self._config.region)
            ]

        except Exception as e:
            raise DiscoveryException(f"Failed to list zones: {e}")

        all_nodes = []
        for zone in region_zones:
            try:
                request = {"project": self._config.project, "zone": zone}

                filter_expr = self._build_filter()
                if filter_expr:
                    request["filter"] = filter_expr

                instances = self._compute.list(request=request)
                all_nodes.extend(self._instances_to_nodes(instances))

            except Exception:
                continue

        return all_nodes

    def _discover_all_instances(self) -> List[DiscoveryNode]:
        """Discover instances across all zones."""
        try:
            from google.cloud import compute_v1

            aggregated_client = compute_v1.InstancesClient()
            request = {"project": self._config.project}

            filter_expr = self._build_filter()
            if filter_expr:
                request["filter"] = filter_expr

            aggregated_list = aggregated_client.aggregated_list(request=request)

        except Exception as e:
            raise DiscoveryException(f"Failed to list all GCP instances: {e}")

        all_nodes = []
        for zone, response in aggregated_list:
            if response.instances:
                all_nodes.extend(self._instances_to_nodes(response.instances))

        return all_nodes

    def _build_filter(self) -> Optional[str]:
        """Build a GCP filter expression."""
        filters = ["status = RUNNING"]

        if self._config.label_key and self._config.label_value:
            filters.append(
                f"labels.{self._config.label_key} = {self._config.label_value}"
            )

        for key, value in self._config.filters.items():
            filters.append(f"{key} = {value}")

        return " AND ".join(filters) if filters else None

    def _instances_to_nodes(self, instances) -> List[DiscoveryNode]:
        """Convert GCP instances to DiscoveryNodes."""
        nodes = []
        for instance in instances:
            node = self._instance_to_node(instance)
            if node:
                nodes.append(node)
        return nodes

    def _instance_to_node(self, instance) -> Optional[DiscoveryNode]:
        """Convert a GCP instance to a DiscoveryNode."""
        for interface in instance.network_interfaces:
            private_ip = interface.network_i_p
            if not private_ip:
                continue

            public_ip = None
            if self._config.use_public_ip and interface.access_configs:
                for access_config in interface.access_configs:
                    if access_config.nat_i_p:
                        public_ip = access_config.nat_i_p
                        break

            labels = dict(instance.labels) if instance.labels else {}

            return DiscoveryNode(
                private_address=private_ip,
                public_address=public_ip,
                port=self._config.hz_port,
                properties={
                    "instance_id": str(instance.id),
                    "instance_name": instance.name,
                    "zone": instance.zone.split("/")[-1] if instance.zone else None,
                    "machine_type": instance.machine_type.split("/")[-1]
                    if instance.machine_type
                    else None,
                    "labels": labels,
                },
            )

        return None
