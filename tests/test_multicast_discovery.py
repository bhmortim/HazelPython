"""Tests for multicast discovery strategy."""

import socket
import struct
import threading
import time
import unittest
from unittest.mock import Mock, patch, MagicMock

from hazelcast.discovery.multicast import (
    MulticastConfig,
    MulticastDiscoveryStrategy,
)
from hazelcast.discovery.base import DiscoveryException, DiscoveryNode
from hazelcast.config import DiscoveryConfig, DiscoveryStrategyType


class TestMulticastConfig(unittest.TestCase):
    """Tests for MulticastConfig."""

    def test_default_values(self):
        config = MulticastConfig()
        self.assertEqual(config.group, "224.2.2.3")
        self.assertEqual(config.port, 54327)
        self.assertEqual(config.timeout_seconds, 3.0)
        self.assertEqual(config.ttl, 32)
        self.assertFalse(config.loopback_enabled)
        self.assertEqual(config.trusted_interfaces, [])

    def test_custom_values(self):
        config = MulticastConfig(
            group="224.0.0.1",
            port=5701,
            timeout_seconds=5.0,
            ttl=64,
            loopback_enabled=True,
            trusted_interfaces=["192.168.1.1"],
        )
        self.assertEqual(config.group, "224.0.0.1")
        self.assertEqual(config.port, 5701)
        self.assertEqual(config.timeout_seconds, 5.0)
        self.assertEqual(config.ttl, 64)
        self.assertTrue(config.loopback_enabled)
        self.assertEqual(config.trusted_interfaces, ["192.168.1.1"])

    def test_invalid_multicast_address_too_low(self):
        with self.assertRaises(ValueError) as ctx:
            MulticastConfig(group="223.255.255.255")
        self.assertIn("Invalid multicast address", str(ctx.exception))

    def test_invalid_multicast_address_too_high(self):
        with self.assertRaises(ValueError) as ctx:
            MulticastConfig(group="240.0.0.1")
        self.assertIn("Invalid multicast address", str(ctx.exception))

    def test_invalid_multicast_address_unicast(self):
        with self.assertRaises(ValueError) as ctx:
            MulticastConfig(group="192.168.1.1")
        self.assertIn("Invalid multicast address", str(ctx.exception))

    def test_invalid_multicast_address_empty(self):
        with self.assertRaises(ValueError) as ctx:
            MulticastConfig(group="")
        self.assertIn("cannot be empty", str(ctx.exception))

    def test_invalid_port_zero(self):
        with self.assertRaises(ValueError) as ctx:
            MulticastConfig(port=0)
        self.assertIn("Invalid port", str(ctx.exception))

    def test_invalid_port_negative(self):
        with self.assertRaises(ValueError) as ctx:
            MulticastConfig(port=-1)
        self.assertIn("Invalid port", str(ctx.exception))

    def test_invalid_port_too_high(self):
        with self.assertRaises(ValueError) as ctx:
            MulticastConfig(port=65536)
        self.assertIn("Invalid port", str(ctx.exception))

    def test_invalid_timeout_zero(self):
        with self.assertRaises(ValueError) as ctx:
            MulticastConfig(timeout_seconds=0)
        self.assertIn("timeout_seconds must be positive", str(ctx.exception))

    def test_invalid_timeout_negative(self):
        with self.assertRaises(ValueError) as ctx:
            MulticastConfig(timeout_seconds=-1)
        self.assertIn("timeout_seconds must be positive", str(ctx.exception))

    def test_invalid_ttl_negative(self):
        with self.assertRaises(ValueError) as ctx:
            MulticastConfig(ttl=-1)
        self.assertIn("Invalid TTL", str(ctx.exception))

    def test_invalid_ttl_too_high(self):
        with self.assertRaises(ValueError) as ctx:
            MulticastConfig(ttl=256)
        self.assertIn("Invalid TTL", str(ctx.exception))

    def test_from_dict_defaults(self):
        config = MulticastConfig.from_dict({})
        self.assertEqual(config.group, "224.2.2.3")
        self.assertEqual(config.port, 54327)

    def test_from_dict_custom(self):
        config = MulticastConfig.from_dict({
            "group": "224.1.1.1",
            "port": 12345,
            "timeout_seconds": 10.0,
            "ttl": 128,
            "loopback_enabled": True,
            "trusted_interfaces": ["10.0.0.1", "10.0.0.2"],
        })
        self.assertEqual(config.group, "224.1.1.1")
        self.assertEqual(config.port, 12345)
        self.assertEqual(config.timeout_seconds, 10.0)
        self.assertEqual(config.ttl, 128)
        self.assertTrue(config.loopback_enabled)
        self.assertEqual(config.trusted_interfaces, ["10.0.0.1", "10.0.0.2"])

    def test_valid_multicast_addresses(self):
        valid_addresses = ["224.0.0.0", "224.0.0.1", "239.255.255.255", "225.1.2.3"]
        for addr in valid_addresses:
            config = MulticastConfig(group=addr)
            self.assertEqual(config.group, addr)


class TestMulticastDiscoveryStrategy(unittest.TestCase):
    """Tests for MulticastDiscoveryStrategy."""

    def test_init_default_config(self):
        strategy = MulticastDiscoveryStrategy()
        self.assertIsNotNone(strategy.config)
        self.assertEqual(strategy.config.group, "224.2.2.3")
        self.assertFalse(strategy.is_started)

    def test_init_custom_config(self):
        config = MulticastConfig(group="224.1.1.1", port=5000)
        strategy = MulticastDiscoveryStrategy(config)
        self.assertEqual(strategy.config.group, "224.1.1.1")
        self.assertEqual(strategy.config.port, 5000)

    def test_start_stop_lifecycle(self):
        strategy = MulticastDiscoveryStrategy()
        self.assertFalse(strategy.is_started)

        strategy.start()
        self.assertTrue(strategy.is_started)

        strategy.stop()
        self.assertFalse(strategy.is_started)

    def test_discover_nodes_not_started(self):
        strategy = MulticastDiscoveryStrategy()
        with self.assertRaises(DiscoveryException) as ctx:
            strategy.discover_nodes()
        self.assertIn("not been started", str(ctx.exception))

    @patch("hazelcast.discovery.multicast.socket.socket")
    def test_discover_nodes_timeout(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket
        mock_socket.recvfrom.side_effect = socket.timeout()

        config = MulticastConfig(timeout_seconds=0.1)
        strategy = MulticastDiscoveryStrategy(config)
        strategy.start()

        nodes = strategy.discover_nodes()

        self.assertEqual(nodes, [])
        mock_socket.sendto.assert_called_once()

    @patch("hazelcast.discovery.multicast.socket.socket")
    def test_discover_nodes_success(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket

        response = b"HZ-DISCOVERY-RESPONSE:192.168.1.10:5701"
        mock_socket.recvfrom.side_effect = [
            (response, ("192.168.1.10", 54327)),
            socket.timeout(),
        ]

        config = MulticastConfig(timeout_seconds=0.1)
        strategy = MulticastDiscoveryStrategy(config)
        strategy.start()

        nodes = strategy.discover_nodes()

        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].private_address, "192.168.1.10")
        self.assertEqual(nodes[0].port, 5701)

    @patch("hazelcast.discovery.multicast.socket.socket")
    def test_discover_multiple_nodes(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket

        responses = [
            (b"HZ-DISCOVERY-RESPONSE:192.168.1.10:5701", ("192.168.1.10", 54327)),
            (b"HZ-DISCOVERY-RESPONSE:192.168.1.11:5701", ("192.168.1.11", 54327)),
            (b"HZ-DISCOVERY-RESPONSE:192.168.1.12:5702", ("192.168.1.12", 54327)),
            socket.timeout(),
        ]
        mock_socket.recvfrom.side_effect = responses

        config = MulticastConfig(timeout_seconds=0.1)
        strategy = MulticastDiscoveryStrategy(config)
        strategy.start()

        nodes = strategy.discover_nodes()

        self.assertEqual(len(nodes), 3)
        addresses = {node.address for node in nodes}
        self.assertIn("192.168.1.10:5701", addresses)
        self.assertIn("192.168.1.11:5701", addresses)
        self.assertIn("192.168.1.12:5702", addresses)

    @patch("hazelcast.discovery.multicast.socket.socket")
    def test_discover_nodes_deduplication(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket

        responses = [
            (b"HZ-DISCOVERY-RESPONSE:192.168.1.10:5701", ("192.168.1.10", 54327)),
            (b"HZ-DISCOVERY-RESPONSE:192.168.1.10:5701", ("192.168.1.10", 54327)),
            socket.timeout(),
        ]
        mock_socket.recvfrom.side_effect = responses

        config = MulticastConfig(timeout_seconds=0.1)
        strategy = MulticastDiscoveryStrategy(config)
        strategy.start()

        nodes = strategy.discover_nodes()

        self.assertEqual(len(nodes), 1)

    @patch("hazelcast.discovery.multicast.socket.socket")
    def test_discover_nodes_invalid_response(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket

        responses = [
            (b"INVALID-RESPONSE", ("192.168.1.10", 54327)),
            (b"HZ-DISCOVERY-RESPONSE:192.168.1.11:5701", ("192.168.1.11", 54327)),
            socket.timeout(),
        ]
        mock_socket.recvfrom.side_effect = responses

        config = MulticastConfig(timeout_seconds=0.1)
        strategy = MulticastDiscoveryStrategy(config)
        strategy.start()

        nodes = strategy.discover_nodes()

        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].private_address, "192.168.1.11")

    @patch("hazelcast.discovery.multicast.socket.socket")
    def test_discover_nodes_trusted_interfaces(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket

        responses = [
            (b"HZ-DISCOVERY-RESPONSE:192.168.1.10:5701", ("192.168.1.10", 54327)),
            (b"HZ-DISCOVERY-RESPONSE:192.168.1.11:5701", ("192.168.1.11", 54327)),
            socket.timeout(),
        ]
        mock_socket.recvfrom.side_effect = responses

        config = MulticastConfig(
            timeout_seconds=0.1,
            trusted_interfaces=["192.168.1.10"],
        )
        strategy = MulticastDiscoveryStrategy(config)
        strategy.start()

        nodes = strategy.discover_nodes()

        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].private_address, "192.168.1.10")

    @patch("hazelcast.discovery.multicast.socket.socket")
    def test_discover_nodes_default_port(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket

        response = b"HZ-DISCOVERY-RESPONSE:192.168.1.10"
        mock_socket.recvfrom.side_effect = [
            (response, ("192.168.1.10", 54327)),
            socket.timeout(),
        ]

        config = MulticastConfig(timeout_seconds=0.1)
        strategy = MulticastDiscoveryStrategy(config)
        strategy.start()

        nodes = strategy.discover_nodes()

        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].port, 5701)

    @patch("hazelcast.discovery.multicast.socket.socket")
    def test_discover_nodes_with_public_address(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket

        response = b"HZ-DISCOVERY-RESPONSE:192.168.1.10:5701:203.0.113.10:"
        mock_socket.recvfrom.side_effect = [
            (response, ("192.168.1.10", 54327)),
            socket.timeout(),
        ]

        config = MulticastConfig(timeout_seconds=0.1)
        strategy = MulticastDiscoveryStrategy(config)
        strategy.start()

        nodes = strategy.discover_nodes()

        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].private_address, "192.168.1.10")
        self.assertEqual(nodes[0].public_address, "203.0.113.10")

    @patch("hazelcast.discovery.multicast.socket.socket")
    def test_get_known_addresses(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket

        responses = [
            (b"HZ-DISCOVERY-RESPONSE:192.168.1.10:5701", ("192.168.1.10", 54327)),
            (b"HZ-DISCOVERY-RESPONSE:192.168.1.11:5702", ("192.168.1.11", 54327)),
            socket.timeout(),
        ]
        mock_socket.recvfrom.side_effect = responses

        config = MulticastConfig(timeout_seconds=0.1)
        strategy = MulticastDiscoveryStrategy(config)
        strategy.start()
        strategy.discover_nodes()

        addresses = strategy.get_known_addresses()

        self.assertEqual(len(addresses), 2)
        self.assertIn("192.168.1.10:5701", addresses)
        self.assertIn("192.168.1.11:5702", addresses)

    @patch("hazelcast.discovery.multicast.socket.socket")
    def test_socket_configuration(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket
        mock_socket.recvfrom.side_effect = socket.timeout()

        config = MulticastConfig(ttl=64, loopback_enabled=True, timeout_seconds=0.1)
        strategy = MulticastDiscoveryStrategy(config)
        strategy.start()
        strategy.discover_nodes()

        mock_socket.setsockopt.assert_any_call(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
        )
        mock_socket.setsockopt.assert_any_call(
            socket.IPPROTO_IP,
            socket.IP_MULTICAST_TTL,
            struct.pack("b", 64),
        )
        mock_socket.setsockopt.assert_any_call(
            socket.IPPROTO_IP,
            socket.IP_MULTICAST_LOOP,
            struct.pack("b", 1),
        )

    @patch("hazelcast.discovery.multicast.socket.socket")
    def test_discover_nodes_os_error(self, mock_socket_class):
        mock_socket = MagicMock()
        mock_socket_class.return_value = mock_socket
        mock_socket.sendto.side_effect = OSError("Network unreachable")

        config = MulticastConfig(timeout_seconds=0.1)
        strategy = MulticastDiscoveryStrategy(config)
        strategy.start()

        with self.assertRaises(DiscoveryException) as ctx:
            strategy.discover_nodes()
        self.assertIn("Multicast discovery failed", str(ctx.exception))


class TestDiscoveryConfigMulticast(unittest.TestCase):
    """Tests for multicast integration with DiscoveryConfig."""

    def test_multicast_property(self):
        config = DiscoveryConfig(enabled=True)
        config.multicast = {"group": "224.1.1.1", "port": 5000}

        self.assertEqual(config.multicast["group"], "224.1.1.1")
        self.assertEqual(config.strategy_type, DiscoveryStrategyType.MULTICAST)

    def test_create_multicast_strategy(self):
        config = DiscoveryConfig(enabled=True)
        config.multicast = {"group": "224.1.1.1", "port": 5000, "timeout_seconds": 2.0}

        strategy = config.create_strategy()

        self.assertIsInstance(strategy, MulticastDiscoveryStrategy)
        self.assertEqual(strategy.config.group, "224.1.1.1")
        self.assertEqual(strategy.config.port, 5000)
        self.assertEqual(strategy.config.timeout_seconds, 2.0)

    def test_from_dict_with_multicast(self):
        data = {
            "enabled": True,
            "strategy_type": "MULTICAST",
            "multicast": {
                "group": "224.2.2.2",
                "port": 12345,
            },
        }
        config = DiscoveryConfig.from_dict(data)

        self.assertTrue(config.enabled)
        self.assertEqual(config.strategy_type, DiscoveryStrategyType.MULTICAST)
        self.assertEqual(config.multicast["group"], "224.2.2.2")


if __name__ == "__main__":
    unittest.main()
