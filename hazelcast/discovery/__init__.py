"""Cloud discovery strategies for Hazelcast cluster member discovery."""

from hazelcast.discovery.base import DiscoveryStrategy, DiscoveryNode
from hazelcast.discovery.aws import AwsDiscoveryStrategy, AwsConfig
from hazelcast.discovery.azure import AzureDiscoveryStrategy, AzureConfig
from hazelcast.discovery.gcp import GcpDiscoveryStrategy, GcpConfig
from hazelcast.discovery.kubernetes import KubernetesDiscoveryStrategy, KubernetesConfig
from hazelcast.cloud import HazelcastCloudDiscovery, CloudConfig

__all__ = [
    "DiscoveryStrategy",
    "DiscoveryNode",
    "AwsDiscoveryStrategy",
    "AwsConfig",
    "AzureDiscoveryStrategy",
    "AzureConfig",
    "GcpDiscoveryStrategy",
    "GcpConfig",
    "KubernetesDiscoveryStrategy",
    "KubernetesConfig",
    "HazelcastCloudDiscovery",
    "CloudConfig",
]
