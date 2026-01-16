"""Azure discovery strategy for Virtual Machines and Scale Sets."""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any

from hazelcast.discovery.base import DiscoveryStrategy, DiscoveryNode, DiscoveryException


@dataclass
class AzureConfig:
    """Configuration for Azure discovery.

    Attributes:
        tenant_id: Azure Active Directory tenant ID.
        client_id: Azure AD application (client) ID.
        client_secret: Azure AD application secret.
        subscription_id: Azure subscription ID.
        resource_group: Resource group name to search.
        scale_set: Virtual Machine Scale Set name (optional).
        tag_key: Filter by tag key.
        tag_value: Filter by tag value.
        hz_port: Hazelcast port on discovered instances.
        use_public_ip: Whether to use public IP addresses.
        instance_metadata_enabled: Use Azure Instance Metadata Service.
    """

    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    subscription_id: Optional[str] = None
    resource_group: Optional[str] = None
    scale_set: Optional[str] = None
    tag_key: Optional[str] = None
    tag_value: Optional[str] = None
    hz_port: int = 5701
    use_public_ip: bool = False
    instance_metadata_enabled: bool = True

    @classmethod
    def from_dict(cls, data: dict) -> "AzureConfig":
        """Create AzureConfig from a dictionary."""
        return cls(
            tenant_id=data.get("tenant_id"),
            client_id=data.get("client_id"),
            client_secret=data.get("client_secret"),
            subscription_id=data.get("subscription_id"),
            resource_group=data.get("resource_group"),
            scale_set=data.get("scale_set"),
            tag_key=data.get("tag_key"),
            tag_value=data.get("tag_value"),
            hz_port=data.get("hz_port", 5701),
            use_public_ip=data.get("use_public_ip", False),
            instance_metadata_enabled=data.get("instance_metadata_enabled", True),
        )


class AzureDiscoveryStrategy(DiscoveryStrategy):
    """Azure discovery strategy for VMs and Scale Sets.

    Discovers Hazelcast cluster members running on Azure Virtual Machines
    or Virtual Machine Scale Sets. Supports filtering by resource groups,
    scale sets, and tags.

    Example:
        Discovery with service principal::

            config = AzureConfig(
                tenant_id="your-tenant-id",
                client_id="your-client-id",
                client_secret="your-secret",
                subscription_id="your-subscription-id",
                resource_group="hazelcast-rg",
                tag_key="hazelcast-cluster",
                tag_value="production",
            )
            strategy = AzureDiscoveryStrategy(config)
            nodes = strategy.discover_nodes()

        Scale Set discovery::

            config = AzureConfig(
                subscription_id="your-subscription-id",
                resource_group="hazelcast-rg",
                scale_set="hazelcast-vmss",
            )
            strategy = AzureDiscoveryStrategy(config)
            nodes = strategy.discover_nodes()
    """

    def __init__(self, config: Optional[AzureConfig] = None):
        """Initialize Azure discovery strategy.

        Args:
            config: Azure configuration options.
        """
        super().__init__()
        self._config = config or AzureConfig()
        self._compute_client = None
        self._network_client = None
        self._credential = None

    @property
    def config(self) -> AzureConfig:
        """Get the Azure configuration."""
        return self._config

    def start(self) -> None:
        """Start the Azure discovery strategy and initialize clients."""
        super().start()
        self._initialize_clients()

    def stop(self) -> None:
        """Stop the Azure discovery strategy."""
        super().stop()
        self._compute_client = None
        self._network_client = None
        self._credential = None

    def _initialize_clients(self) -> None:
        """Initialize Azure SDK clients."""
        try:
            from azure.identity import ClientSecretCredential, DefaultAzureCredential
            from azure.mgmt.compute import ComputeManagementClient
            from azure.mgmt.network import NetworkManagementClient
        except ImportError:
            raise DiscoveryException(
                "azure-identity, azure-mgmt-compute, and azure-mgmt-network "
                "are required for Azure discovery. Install with: "
                "pip install azure-identity azure-mgmt-compute azure-mgmt-network"
            )

        try:
            if (
                self._config.tenant_id
                and self._config.client_id
                and self._config.client_secret
            ):
                self._credential = ClientSecretCredential(
                    tenant_id=self._config.tenant_id,
                    client_id=self._config.client_id,
                    client_secret=self._config.client_secret,
                )
            else:
                self._credential = DefaultAzureCredential()

            if not self._config.subscription_id:
                raise DiscoveryException("subscription_id is required for Azure discovery")

            self._compute_client = ComputeManagementClient(
                self._credential,
                self._config.subscription_id,
            )
            self._network_client = NetworkManagementClient(
                self._credential,
                self._config.subscription_id,
            )

        except Exception as e:
            raise DiscoveryException(f"Failed to initialize Azure clients: {e}")

    def discover_nodes(self) -> List[DiscoveryNode]:
        """Discover Hazelcast nodes on Azure.

        Returns:
            List of discovered nodes.

        Raises:
            DiscoveryException: If discovery fails.
        """
        if not self._started:
            self.start()

        if self._config.scale_set:
            return self._discover_scale_set_nodes()
        return self._discover_vm_nodes()

    def _discover_vm_nodes(self) -> List[DiscoveryNode]:
        """Discover standalone Virtual Machines."""
        try:
            if self._config.resource_group:
                vms = self._compute_client.virtual_machines.list(
                    self._config.resource_group
                )
            else:
                vms = self._compute_client.virtual_machines.list_all()
        except Exception as e:
            raise DiscoveryException(f"Failed to list Azure VMs: {e}")

        nodes = []
        for vm in vms:
            if self._matches_tags(vm.tags):
                node = self._vm_to_node(vm)
                if node:
                    nodes.append(node)

        return nodes

    def _discover_scale_set_nodes(self) -> List[DiscoveryNode]:
        """Discover Virtual Machine Scale Set instances."""
        if not self._config.resource_group:
            raise DiscoveryException(
                "resource_group is required for Scale Set discovery"
            )

        try:
            instances = self._compute_client.virtual_machine_scale_set_vms.list(
                self._config.resource_group,
                self._config.scale_set,
            )
        except Exception as e:
            raise DiscoveryException(f"Failed to list Scale Set instances: {e}")

        nodes = []
        for instance in instances:
            node = self._scale_set_instance_to_node(instance)
            if node:
                nodes.append(node)

        return nodes

    def _matches_tags(self, tags: Optional[Dict[str, str]]) -> bool:
        """Check if tags match the configured filter."""
        if not self._config.tag_key:
            return True
        if not tags:
            return False
        return tags.get(self._config.tag_key) == self._config.tag_value

    def _vm_to_node(self, vm) -> Optional[DiscoveryNode]:
        """Convert an Azure VM to a DiscoveryNode."""
        try:
            resource_group = vm.id.split("/")[4]

            for nic_ref in vm.network_profile.network_interfaces:
                nic_name = nic_ref.id.split("/")[-1]
                nic = self._network_client.network_interfaces.get(
                    resource_group, nic_name
                )

                for ip_config in nic.ip_configurations:
                    private_ip = ip_config.private_ip_address
                    if not private_ip:
                        continue

                    public_ip = None
                    if (
                        self._config.use_public_ip
                        and ip_config.public_ip_address
                    ):
                        pub_ip_name = ip_config.public_ip_address.id.split("/")[-1]
                        pub_ip = self._network_client.public_ip_addresses.get(
                            resource_group, pub_ip_name
                        )
                        public_ip = pub_ip.ip_address

                    return DiscoveryNode(
                        private_address=private_ip,
                        public_address=public_ip,
                        port=self._config.hz_port,
                        properties={
                            "vm_id": vm.vm_id,
                            "vm_name": vm.name,
                            "location": vm.location,
                            "tags": vm.tags or {},
                        },
                    )

        except Exception:
            pass

        return None

    def _scale_set_instance_to_node(self, instance) -> Optional[DiscoveryNode]:
        """Convert a Scale Set instance to a DiscoveryNode."""
        try:
            nics = (
                self._compute_client
                .virtual_machine_scale_set_network_interfaces
                .list_virtual_machine_scale_set_vm_network_interfaces(
                    self._config.resource_group,
                    self._config.scale_set,
                    instance.instance_id,
                )
            )

            for nic in nics:
                for ip_config in nic.ip_configurations:
                    private_ip = ip_config.private_ip_address
                    if private_ip:
                        return DiscoveryNode(
                            private_address=private_ip,
                            port=self._config.hz_port,
                            properties={
                                "instance_id": instance.instance_id,
                                "vm_name": instance.name,
                                "scale_set": self._config.scale_set,
                            },
                        )

        except Exception:
            pass

        return None
