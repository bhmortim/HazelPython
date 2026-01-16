"""Tests for Hazelcast Cloud (Viridian) discovery."""

import json
import ssl
import unittest
from unittest import mock

from hazelcast.cloud import (
    CloudConfig,
    HazelcastCloudDiscovery,
    _CLOUD_URL_BASE,
    _CLOUD_URL_PATH,
)
from hazelcast.discovery.base import DiscoveryException, DiscoveryNode
from hazelcast.config import DiscoveryConfig, DiscoveryStrategyType


class TestCloudConfig(unittest.TestCase):
    """Tests for CloudConfig dataclass."""

    def test_default_values(self):
        config = CloudConfig(token="test-token")

        self.assertEqual(config.token, "test-token")
        self.assertIsNone(config.cluster_id)
        self.assertEqual(config.cloud_url, _CLOUD_URL_BASE)
        self.assertEqual(config.connection_timeout, 10.0)
        self.assertTrue(config.tls_enabled)
        self.assertIsNone(config.tls_ca_path)
        self.assertIsNone(config.tls_cert_path)
        self.assertIsNone(config.tls_key_path)
        self.assertIsNone(config.tls_password)

    def test_token_required(self):
        with self.assertRaises(ValueError) as ctx:
            CloudConfig(token="")

        self.assertIn("token is required", str(ctx.exception))

    def test_custom_values(self):
        config = CloudConfig(
            token="my-token",
            cluster_id="cluster-123",
            cloud_url="https://custom.api.com",
            connection_timeout=30.0,
            tls_enabled=False,
            tls_ca_path="/path/to/ca.pem",
        )

        self.assertEqual(config.token, "my-token")
        self.assertEqual(config.cluster_id, "cluster-123")
        self.assertEqual(config.cloud_url, "https://custom.api.com")
        self.assertEqual(config.connection_timeout, 30.0)
        self.assertFalse(config.tls_enabled)
        self.assertEqual(config.tls_ca_path, "/path/to/ca.pem")

    def test_discovery_url(self):
        config = CloudConfig(token="test-token")
        expected_url = f"{_CLOUD_URL_BASE}{_CLOUD_URL_PATH}"
        self.assertEqual(config.discovery_url, expected_url)

    def test_discovery_url_strips_trailing_slash(self):
        config = CloudConfig(token="test-token", cloud_url="https://api.example.com/")
        self.assertEqual(config.discovery_url, "https://api.example.com/cluster/discovery")

    def test_from_dict(self):
        data = {
            "token": "dict-token",
            "cluster_id": "cluster-456",
            "cloud_url": "https://test.api.com",
            "tls_enabled": False,
        }

        config = CloudConfig.from_dict(data)

        self.assertEqual(config.token, "dict-token")
        self.assertEqual(config.cluster_id, "cluster-456")
        self.assertEqual(config.cloud_url, "https://test.api.com")
        self.assertFalse(config.tls_enabled)

    def test_from_dict_defaults(self):
        data = {"token": "minimal-token"}
        config = CloudConfig.from_dict(data)

        self.assertEqual(config.token, "minimal-token")
        self.assertEqual(config.cloud_url, _CLOUD_URL_BASE)
        self.assertTrue(config.tls_enabled)


class TestHazelcastCloudDiscovery(unittest.TestCase):
    """Tests for HazelcastCloudDiscovery strategy."""

    def setUp(self):
        self.config = CloudConfig(token="test-token-123")
        self.discovery = HazelcastCloudDiscovery(self.config)

    def test_init(self):
        self.assertEqual(self.discovery.config, self.config)
        self.assertFalse(self.discovery.is_started)
        self.assertIsNone(self.discovery.ssl_context)

    def test_start_creates_ssl_context(self):
        self.discovery.start()

        self.assertTrue(self.discovery.is_started)
        self.assertIsNotNone(self.discovery.ssl_context)
        self.assertIsInstance(self.discovery.ssl_context, ssl.SSLContext)

    def test_start_without_tls(self):
        config = CloudConfig(token="test-token", tls_enabled=False)
        discovery = HazelcastCloudDiscovery(config)
        discovery.start()

        self.assertTrue(discovery.is_started)
        self.assertIsNone(discovery.ssl_context)

    def test_stop_clears_state(self):
        self.discovery.start()
        self.discovery.stop()

        self.assertFalse(self.discovery.is_started)
        self.assertIsNone(self.discovery.ssl_context)

    def test_discover_nodes_not_started(self):
        with self.assertRaises(DiscoveryException) as ctx:
            self.discovery.discover_nodes()

        self.assertIn("not been started", str(ctx.exception))

    @mock.patch("hazelcast.cloud.urllib.request.urlopen")
    def test_discover_nodes_success(self, mock_urlopen):
        mock_response = mock.MagicMock()
        mock_response.read.return_value = json.dumps({
            "clusterId": "cluster-abc",
            "clusterName": "my-cluster",
            "members": [
                {"privateAddress": "10.0.0.1", "port": 5701},
                {"privateAddress": "10.0.0.2", "port": 5701, "publicAddress": "203.0.113.2"},
            ],
        }).encode("utf-8")
        mock_response.__enter__ = mock.MagicMock(return_value=mock_response)
        mock_response.__exit__ = mock.MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        self.discovery.start()
        nodes = self.discovery.discover_nodes()

        self.assertEqual(len(nodes), 2)
        self.assertEqual(nodes[0].private_address, "10.0.0.1")
        self.assertEqual(nodes[0].port, 5701)
        self.assertEqual(nodes[1].private_address, "10.0.0.2")
        self.assertEqual(nodes[1].public_address, "203.0.113.2")

    @mock.patch("hazelcast.cloud.urllib.request.urlopen")
    def test_discover_nodes_parses_address_with_port(self, mock_urlopen):
        mock_response = mock.MagicMock()
        mock_response.read.return_value = json.dumps({
            "members": [
                {"privateAddress": "10.0.0.1:5702"},
            ],
        }).encode("utf-8")
        mock_response.__enter__ = mock.MagicMock(return_value=mock_response)
        mock_response.__exit__ = mock.MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        self.discovery.start()
        nodes = self.discovery.discover_nodes()

        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].private_address, "10.0.0.1")
        self.assertEqual(nodes[0].port, 5702)

    @mock.patch("hazelcast.cloud.urllib.request.urlopen")
    def test_discover_nodes_http_error(self, mock_urlopen):
        import urllib.error

        error_response = mock.MagicMock()
        error_response.read.return_value = b"Unauthorized"
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url="https://api.test.com",
            code=401,
            msg="Unauthorized",
            hdrs={},
            fp=error_response,
        )

        self.discovery.start()

        with self.assertRaises(DiscoveryException) as ctx:
            self.discovery.discover_nodes()

        self.assertIn("401", str(ctx.exception))

    @mock.patch("hazelcast.cloud.urllib.request.urlopen")
    def test_discover_nodes_network_error(self, mock_urlopen):
        import urllib.error

        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")

        self.discovery.start()

        with self.assertRaises(DiscoveryException) as ctx:
            self.discovery.discover_nodes()

        self.assertIn("Connection refused", str(ctx.exception))

    @mock.patch("hazelcast.cloud.urllib.request.urlopen")
    def test_get_known_addresses(self, mock_urlopen):
        mock_response = mock.MagicMock()
        mock_response.read.return_value = json.dumps({
            "members": [
                {"privateAddress": "10.0.0.1", "port": 5701},
                {"privateAddress": "10.0.0.2", "port": 5702},
            ],
        }).encode("utf-8")
        mock_response.__enter__ = mock.MagicMock(return_value=mock_response)
        mock_response.__exit__ = mock.MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        self.discovery.start()
        addresses = self.discovery.get_known_addresses()

        self.assertEqual(addresses, ["10.0.0.1:5701", "10.0.0.2:5702"])

    def test_get_tls_config_enabled(self):
        config = CloudConfig(
            token="test-token",
            tls_enabled=True,
            tls_ca_path="/path/to/ca.pem",
            tls_cert_path="/path/to/cert.pem",
            tls_key_path="/path/to/key.pem",
        )
        discovery = HazelcastCloudDiscovery(config)

        tls_config = discovery.get_tls_config()

        self.assertIsNotNone(tls_config)
        self.assertTrue(tls_config["enabled"])
        self.assertEqual(tls_config["ca_path"], "/path/to/ca.pem")
        self.assertEqual(tls_config["cert_path"], "/path/to/cert.pem")
        self.assertEqual(tls_config["key_path"], "/path/to/key.pem")

    def test_get_tls_config_disabled(self):
        config = CloudConfig(token="test-token", tls_enabled=False)
        discovery = HazelcastCloudDiscovery(config)

        tls_config = discovery.get_tls_config()

        self.assertIsNone(tls_config)


class TestDiscoveryConfigCloudIntegration(unittest.TestCase):
    """Tests for DiscoveryConfig cloud integration."""

    def test_cloud_property_sets_strategy_type(self):
        config = DiscoveryConfig(enabled=True)
        config.cloud = {"token": "test-token"}

        self.assertEqual(config.strategy_type, DiscoveryStrategyType.CLOUD)

    def test_create_strategy_cloud(self):
        config = DiscoveryConfig(enabled=True)
        config.cloud = {"token": "test-token"}

        strategy = config.create_strategy()

        self.assertIsInstance(strategy, HazelcastCloudDiscovery)
        self.assertEqual(strategy.config.token, "test-token")

    def test_from_dict_with_cloud(self):
        data = {
            "enabled": True,
            "strategy_type": "CLOUD",
            "cloud": {
                "token": "dict-token",
                "cluster_id": "cluster-789",
            },
        }

        config = DiscoveryConfig.from_dict(data)

        self.assertTrue(config.enabled)
        self.assertEqual(config.strategy_type, DiscoveryStrategyType.CLOUD)
        self.assertEqual(config.cloud["token"], "dict-token")
        self.assertEqual(config.cloud["cluster_id"], "cluster-789")


class TestHazelcastCloudDiscoveryIntegration(unittest.TestCase):
    """Integration test scaffolding for Hazelcast Cloud.

    These tests are skipped by default and require a valid
    Hazelcast Cloud token to run. Set the HZ_CLOUD_TOKEN
    environment variable to enable them.
    """

    @unittest.skip("Integration test - requires HZ_CLOUD_TOKEN environment variable")
    def test_real_discovery(self):
        import os

        token = os.environ.get("HZ_CLOUD_TOKEN")
        if not token:
            self.skipTest("HZ_CLOUD_TOKEN not set")

        config = CloudConfig(token=token)
        discovery = HazelcastCloudDiscovery(config)
        discovery.start()

        try:
            nodes = discovery.discover_nodes()
            self.assertGreater(len(nodes), 0)
            for node in nodes:
                self.assertIsInstance(node, DiscoveryNode)
                self.assertTrue(node.private_address)
        finally:
            discovery.stop()


if __name__ == "__main__":
    unittest.main()
