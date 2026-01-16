"""Unit tests for cloud discovery strategies."""

import unittest
from unittest.mock import Mock, MagicMock, patch

from hazelcast.discovery.base import DiscoveryNode, DiscoveryStrategy, DiscoveryException
from hazelcast.discovery.aws import AwsDiscoveryStrategy, AwsConfig
from hazelcast.discovery.azure import AzureDiscoveryStrategy, AzureConfig
from hazelcast.discovery.gcp import GcpDiscoveryStrategy, GcpConfig
from hazelcast.discovery.kubernetes import KubernetesDiscoveryStrategy, KubernetesConfig
from hazelcast.config import DiscoveryConfig, DiscoveryStrategyType
from hazelcast.exceptions import ConfigurationException


class TestDiscoveryNode(unittest.TestCase):
    """Tests for DiscoveryNode dataclass."""

    def test_basic_node(self):
        node = DiscoveryNode(private_address="10.0.0.1")
        self.assertEqual(node.private_address, "10.0.0.1")
        self.assertEqual(node.port, 5701)
        self.assertIsNone(node.public_address)
        self.assertEqual(node.properties, {})

    def test_node_with_all_fields(self):
        node = DiscoveryNode(
            private_address="10.0.0.1",
            port=5702,
            public_address="54.1.2.3",
            properties={"instance_id": "i-123"},
        )
        self.assertEqual(node.private_address, "10.0.0.1")
        self.assertEqual(node.port, 5702)
        self.assertEqual(node.public_address, "54.1.2.3")
        self.assertEqual(node.properties["instance_id"], "i-123")

    def test_address_property(self):
        node = DiscoveryNode(private_address="10.0.0.1", port=5702)
        self.assertEqual(node.address, "10.0.0.1:5702")

    def test_public_address_str(self):
        node = DiscoveryNode(
            private_address="10.0.0.1",
            port=5702,
            public_address="54.1.2.3",
        )
        self.assertEqual(node.public_address_str, "54.1.2.3:5702")

    def test_public_address_str_none(self):
        node = DiscoveryNode(private_address="10.0.0.1")
        self.assertIsNone(node.public_address_str)


class TestAwsConfig(unittest.TestCase):
    """Tests for AwsConfig."""

    def test_default_values(self):
        config = AwsConfig()
        self.assertIsNone(config.access_key)
        self.assertIsNone(config.secret_key)
        self.assertEqual(config.region, "us-east-1")
        self.assertEqual(config.hz_port, 5701)
        self.assertFalse(config.use_public_ip)
        self.assertFalse(config.use_ecs)

    def test_from_dict(self):
        data = {
            "access_key": "AKIATEST",
            "secret_key": "secret123",
            "region": "us-west-2",
            "tag_key": "cluster",
            "tag_value": "prod",
            "hz_port": 5702,
            "use_public_ip": True,
        }
        config = AwsConfig.from_dict(data)
        self.assertEqual(config.access_key, "AKIATEST")
        self.assertEqual(config.secret_key, "secret123")
        self.assertEqual(config.region, "us-west-2")
        self.assertEqual(config.tag_key, "cluster")
        self.assertEqual(config.tag_value, "prod")
        self.assertEqual(config.hz_port, 5702)
        self.assertTrue(config.use_public_ip)

    def test_ecs_config(self):
        data = {
            "use_ecs": True,
            "ecs_cluster": "my-cluster",
            "ecs_service": "hazelcast",
        }
        config = AwsConfig.from_dict(data)
        self.assertTrue(config.use_ecs)
        self.assertEqual(config.ecs_cluster, "my-cluster")
        self.assertEqual(config.ecs_service, "hazelcast")


class TestAwsDiscoveryStrategy(unittest.TestCase):
    """Tests for AwsDiscoveryStrategy."""

    def test_init_without_config(self):
        strategy = AwsDiscoveryStrategy()
        self.assertIsNotNone(strategy.config)
        self.assertFalse(strategy.is_started)

    def test_init_with_config(self):
        config = AwsConfig(region="eu-west-1")
        strategy = AwsDiscoveryStrategy(config)
        self.assertEqual(strategy.config.region, "eu-west-1")

    def test_requires_boto3(self):
        strategy = AwsDiscoveryStrategy()
        with patch.dict("sys.modules", {"boto3": None}):
            with self.assertRaises(DiscoveryException) as ctx:
                strategy._initialize_clients()
            self.assertIn("boto3", str(ctx.exception))

    @patch("hazelcast.discovery.aws.boto3")
    def test_discover_ec2_nodes(self, mock_boto3):
        mock_session = MagicMock()
        mock_ec2 = MagicMock()
        mock_session.client.return_value = mock_ec2
        mock_boto3.Session.return_value = mock_session

        mock_ec2.describe_instances.return_value = {
            "Reservations": [
                {
                    "Instances": [
                        {
                            "PrivateIpAddress": "10.0.0.1",
                            "PublicIpAddress": "54.1.2.3",
                            "InstanceId": "i-123",
                            "InstanceType": "t2.micro",
                            "Placement": {"AvailabilityZone": "us-east-1a"},
                            "Tags": [{"Key": "Name", "Value": "hz-1"}],
                        },
                        {
                            "PrivateIpAddress": "10.0.0.2",
                            "InstanceId": "i-456",
                            "InstanceType": "t2.micro",
                            "Placement": {"AvailabilityZone": "us-east-1b"},
                            "Tags": [],
                        },
                    ]
                }
            ]
        }

        config = AwsConfig(tag_key="cluster", tag_value="test")
        strategy = AwsDiscoveryStrategy(config)
        nodes = strategy.discover_nodes()

        self.assertEqual(len(nodes), 2)
        self.assertEqual(nodes[0].private_address, "10.0.0.1")
        self.assertEqual(nodes[0].public_address, "54.1.2.3")
        self.assertEqual(nodes[0].properties["instance_id"], "i-123")
        self.assertEqual(nodes[1].private_address, "10.0.0.2")
        self.assertIsNone(nodes[1].public_address)

    @patch("hazelcast.discovery.aws.boto3")
    def test_discover_ecs_nodes(self, mock_boto3):
        mock_session = MagicMock()
        mock_ec2 = MagicMock()
        mock_ecs = MagicMock()
        mock_session.client.side_effect = lambda svc: mock_ecs if svc == "ecs" else mock_ec2
        mock_boto3.Session.return_value = mock_session

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {"taskArns": ["arn:aws:ecs:task/123"]}
        ]
        mock_ecs.get_paginator.return_value = mock_paginator

        mock_ecs.describe_tasks.return_value = {
            "tasks": [
                {
                    "taskArn": "arn:aws:ecs:task/123",
                    "taskDefinitionArn": "arn:aws:ecs:task-def/hz:1",
                    "containers": [
                        {
                            "name": "hazelcast",
                            "networkInterfaces": [
                                {"privateIpv4Address": "10.0.1.1"}
                            ],
                        }
                    ],
                }
            ]
        }

        config = AwsConfig(use_ecs=True, ecs_cluster="test-cluster")
        strategy = AwsDiscoveryStrategy(config)
        nodes = strategy.discover_nodes()

        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].private_address, "10.0.1.1")


class TestAzureConfig(unittest.TestCase):
    """Tests for AzureConfig."""

    def test_default_values(self):
        config = AzureConfig()
        self.assertIsNone(config.tenant_id)
        self.assertIsNone(config.subscription_id)
        self.assertEqual(config.hz_port, 5701)
        self.assertFalse(config.use_public_ip)

    def test_from_dict(self):
        data = {
            "tenant_id": "tenant-123",
            "client_id": "client-456",
            "client_secret": "secret",
            "subscription_id": "sub-789",
            "resource_group": "hazelcast-rg",
            "tag_key": "cluster",
            "tag_value": "prod",
        }
        config = AzureConfig.from_dict(data)
        self.assertEqual(config.tenant_id, "tenant-123")
        self.assertEqual(config.client_id, "client-456")
        self.assertEqual(config.subscription_id, "sub-789")
        self.assertEqual(config.resource_group, "hazelcast-rg")


class TestAzureDiscoveryStrategy(unittest.TestCase):
    """Tests for AzureDiscoveryStrategy."""

    def test_init_without_config(self):
        strategy = AzureDiscoveryStrategy()
        self.assertIsNotNone(strategy.config)
        self.assertFalse(strategy.is_started)

    def test_requires_subscription_id(self):
        strategy = AzureDiscoveryStrategy()
        with patch.dict(
            "sys.modules",
            {
                "azure.identity": MagicMock(),
                "azure.mgmt.compute": MagicMock(),
                "azure.mgmt.network": MagicMock(),
            },
        ):
            with self.assertRaises(DiscoveryException) as ctx:
                strategy._initialize_clients()
            self.assertIn("subscription_id", str(ctx.exception))


class TestGcpConfig(unittest.TestCase):
    """Tests for GcpConfig."""

    def test_default_values(self):
        config = GcpConfig()
        self.assertIsNone(config.project)
        self.assertIsNone(config.zone)
        self.assertEqual(config.hz_port, 5701)
        self.assertFalse(config.use_public_ip)

    def test_from_dict(self):
        data = {
            "project": "my-project",
            "zone": "us-central1-a",
            "label_key": "cluster",
            "label_value": "prod",
            "hz_port": 5702,
        }
        config = GcpConfig.from_dict(data)
        self.assertEqual(config.project, "my-project")
        self.assertEqual(config.zone, "us-central1-a")
        self.assertEqual(config.label_key, "cluster")
        self.assertEqual(config.hz_port, 5702)


class TestGcpDiscoveryStrategy(unittest.TestCase):
    """Tests for GcpDiscoveryStrategy."""

    def test_init_without_config(self):
        strategy = GcpDiscoveryStrategy()
        self.assertIsNotNone(strategy.config)
        self.assertFalse(strategy.is_started)

    def test_requires_project(self):
        strategy = GcpDiscoveryStrategy()
        strategy._started = True
        strategy._compute = MagicMock()

        with self.assertRaises(DiscoveryException) as ctx:
            strategy.discover_nodes()
        self.assertIn("project", str(ctx.exception))


class TestKubernetesConfig(unittest.TestCase):
    """Tests for KubernetesConfig."""

    def test_default_values(self):
        config = KubernetesConfig()
        self.assertIsNone(config.namespace)
        self.assertIsNone(config.service_name)
        self.assertEqual(config.hz_port, 5701)
        self.assertTrue(config.in_cluster)

    def test_from_dict(self):
        data = {
            "namespace": "hazelcast",
            "service_name": "hazelcast-svc",
            "hz_port": 5702,
            "in_cluster": False,
        }
        config = KubernetesConfig.from_dict(data)
        self.assertEqual(config.namespace, "hazelcast")
        self.assertEqual(config.service_name, "hazelcast-svc")
        self.assertEqual(config.hz_port, 5702)
        self.assertFalse(config.in_cluster)


class TestKubernetesDiscoveryStrategy(unittest.TestCase):
    """Tests for KubernetesDiscoveryStrategy."""

    def test_init_without_config(self):
        strategy = KubernetesDiscoveryStrategy()
        self.assertIsNotNone(strategy.config)
        self.assertFalse(strategy.is_started)

    def test_requires_discovery_method(self):
        config = KubernetesConfig()
        strategy = KubernetesDiscoveryStrategy(config)
        strategy._started = True
        strategy._v1 = MagicMock()
        strategy._namespace = "default"

        with self.assertRaises(DiscoveryException) as ctx:
            strategy.discover_nodes()
        self.assertIn("No discovery method", str(ctx.exception))

    @patch("hazelcast.discovery.kubernetes.client")
    @patch("hazelcast.discovery.kubernetes.config")
    def test_discover_service_endpoints(self, mock_k8s_config, mock_client):
        mock_v1 = MagicMock()
        mock_client.CoreV1Api.return_value = mock_v1

        mock_endpoints = MagicMock()
        mock_subset = MagicMock()
        mock_address = MagicMock()
        mock_address.ip = "10.0.0.1"
        mock_address.target_ref = MagicMock()
        mock_address.target_ref.name = "pod-1"
        mock_address.node_name = "node-1"
        mock_subset.addresses = [mock_address]
        mock_subset.ports = []
        mock_endpoints.subsets = [mock_subset]
        mock_v1.read_namespaced_endpoints.return_value = mock_endpoints

        config = KubernetesConfig(
            namespace="hazelcast",
            service_name="hazelcast-svc",
            in_cluster=False,
        )
        strategy = KubernetesDiscoveryStrategy(config)
        strategy._started = True
        strategy._v1 = mock_v1
        strategy._namespace = "hazelcast"

        nodes = strategy._discover_service_endpoints()

        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].private_address, "10.0.0.1")
        self.assertEqual(nodes[0].properties["pod_name"], "pod-1")


class TestDiscoveryConfig(unittest.TestCase):
    """Tests for DiscoveryConfig."""

    def test_default_values(self):
        config = DiscoveryConfig()
        self.assertFalse(config.enabled)
        self.assertIsNone(config.strategy_type)

    def test_aws_auto_sets_type(self):
        config = DiscoveryConfig()
        config.aws = {"region": "us-west-2"}
        self.assertEqual(config.strategy_type, DiscoveryStrategyType.AWS)

    def test_azure_auto_sets_type(self):
        config = DiscoveryConfig()
        config.azure = {"subscription_id": "sub-123"}
        self.assertEqual(config.strategy_type, DiscoveryStrategyType.AZURE)

    def test_gcp_auto_sets_type(self):
        config = DiscoveryConfig()
        config.gcp = {"project": "my-project"}
        self.assertEqual(config.strategy_type, DiscoveryStrategyType.GCP)

    def test_kubernetes_auto_sets_type(self):
        config = DiscoveryConfig()
        config.kubernetes = {"service_name": "hazelcast"}
        self.assertEqual(config.strategy_type, DiscoveryStrategyType.KUBERNETES)

    def test_from_dict(self):
        data = {
            "enabled": True,
            "strategy_type": "AWS",
            "aws": {"region": "us-west-2", "tag_key": "cluster"},
        }
        config = DiscoveryConfig.from_dict(data)
        self.assertTrue(config.enabled)
        self.assertEqual(config.strategy_type, DiscoveryStrategyType.AWS)
        self.assertEqual(config.aws["region"], "us-west-2")

    def test_from_dict_invalid_strategy(self):
        data = {"enabled": True, "strategy_type": "INVALID"}
        with self.assertRaises(ConfigurationException):
            DiscoveryConfig.from_dict(data)

    def test_create_strategy_disabled(self):
        config = DiscoveryConfig(enabled=False)
        with self.assertRaises(ConfigurationException) as ctx:
            config.create_strategy()
        self.assertIn("not enabled", str(ctx.exception))

    def test_create_strategy_no_type(self):
        config = DiscoveryConfig(enabled=True)
        with self.assertRaises(ConfigurationException) as ctx:
            config.create_strategy()
        self.assertIn("type is not set", str(ctx.exception))

    @patch("hazelcast.discovery.aws.boto3")
    def test_create_aws_strategy(self, mock_boto3):
        mock_boto3.Session.return_value = MagicMock()

        config = DiscoveryConfig(enabled=True)
        config.strategy_type = DiscoveryStrategyType.AWS
        config.aws = {"region": "us-west-2"}

        strategy = config.create_strategy()
        self.assertIsInstance(strategy, AwsDiscoveryStrategy)
        self.assertEqual(strategy.config.region, "us-west-2")


class TestClientConfigDiscovery(unittest.TestCase):
    """Tests for ClientConfig discovery integration."""

    def test_default_discovery(self):
        from hazelcast.config import ClientConfig

        config = ClientConfig()
        self.assertIsNotNone(config.discovery)
        self.assertFalse(config.discovery.enabled)

    def test_from_dict_with_discovery(self):
        from hazelcast.config import ClientConfig

        data = {
            "cluster_name": "test",
            "discovery": {
                "enabled": True,
                "strategy_type": "KUBERNETES",
                "kubernetes": {
                    "namespace": "hazelcast",
                    "service_name": "hazelcast-svc",
                },
            },
        }
        config = ClientConfig.from_dict(data)
        self.assertTrue(config.discovery.enabled)
        self.assertEqual(
            config.discovery.strategy_type, DiscoveryStrategyType.KUBERNETES
        )
        self.assertEqual(config.discovery.kubernetes["service_name"], "hazelcast-svc")


if __name__ == "__main__":
    unittest.main()
