"""Tests for Management Center integration."""

import time
import threading
import unittest
from unittest.mock import Mock, patch

from hazelcast.config import StatisticsConfig, ClientConfig
from hazelcast.management import (
    ClientStatistics,
    ManagementCenterService,
)
from hazelcast.metrics import MetricsRegistry


class TestStatisticsConfig(unittest.TestCase):
    """Tests for StatisticsConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = StatisticsConfig()

        self.assertFalse(config.enabled)
        self.assertEqual(3.0, config.period_seconds)

    def test_custom_values(self):
        """Test custom configuration values."""
        config = StatisticsConfig(enabled=True, period_seconds=5.0)

        self.assertTrue(config.enabled)
        self.assertEqual(5.0, config.period_seconds)

    def test_invalid_period_raises(self):
        """Test that invalid period raises ConfigurationException."""
        from hazelcast.exceptions import ConfigurationException

        with self.assertRaises(ConfigurationException):
            StatisticsConfig(period_seconds=0)

        with self.assertRaises(ConfigurationException):
            StatisticsConfig(period_seconds=-1.0)

    def test_from_dict(self):
        """Test creating config from dictionary."""
        data = {
            "enabled": True,
            "period_seconds": 10.0,
        }
        config = StatisticsConfig.from_dict(data)

        self.assertTrue(config.enabled)
        self.assertEqual(10.0, config.period_seconds)

    def test_from_dict_defaults(self):
        """Test from_dict with empty dictionary."""
        config = StatisticsConfig.from_dict({})

        self.assertFalse(config.enabled)
        self.assertEqual(3.0, config.period_seconds)

    def test_property_setters(self):
        """Test property setters."""
        config = StatisticsConfig()

        config.enabled = True
        self.assertTrue(config.enabled)

        config.period_seconds = 7.5
        self.assertEqual(7.5, config.period_seconds)


class TestClientStatistics(unittest.TestCase):
    """Tests for ClientStatistics."""

    def test_default_values(self):
        """Test default statistics values."""
        stats = ClientStatistics()

        self.assertEqual(0.0, stats.timestamp)
        self.assertEqual("", stats.client_name)
        self.assertEqual(0, stats.connections_active)
        self.assertEqual(0, stats.invocations_total)
        self.assertEqual({}, stats.near_cache_stats)
        self.assertEqual({}, stats.custom_metrics)

    def test_to_dict(self):
        """Test converting statistics to dictionary."""
        stats = ClientStatistics(
            timestamp=1234567890.123,
            client_name="test-client",
            client_uuid="test-uuid",
            cluster_name="dev",
            uptime_seconds=100.5,
            connections_active=3,
            invocations_total=1000,
            invocations_failed=5,
        )

        result = stats.to_dict()

        self.assertEqual(1234567890.123, result["timestamp"])
        self.assertEqual("test-client", result["client"]["name"])
        self.assertEqual("test-uuid", result["client"]["uuid"])
        self.assertEqual("dev", result["client"]["cluster_name"])
        self.assertEqual(100.5, result["client"]["uptime_seconds"])
        self.assertEqual(3, result["connections"]["active"])
        self.assertEqual(1000, result["invocations"]["total"])
        self.assertEqual(5, result["invocations"]["failed"])

    def test_to_key_value_string(self):
        """Test converting statistics to key=value format."""
        stats = ClientStatistics(
            timestamp=1000.0,
            client_name="test-client",
            client_uuid="uuid-123",
            cluster_name="dev",
            connections_active=2,
            invocations_total=50,
        )

        kv_string = stats.to_key_value_string()

        self.assertIn("client.name=test-client", kv_string)
        self.assertIn("client.uuid=uuid-123", kv_string)
        self.assertIn("clusterName=dev", kv_string)
        self.assertIn("connections.active=2", kv_string)
        self.assertIn("invocations.total=50", kv_string)

    def test_to_key_value_string_with_near_cache(self):
        """Test key=value format includes near cache stats."""
        stats = ClientStatistics(
            client_name="test",
            near_cache_stats={
                "my-map": {"hits": 100, "misses": 20, "entries": 50, "evictions": 5}
            },
        )

        kv_string = stats.to_key_value_string()

        self.assertIn("nc.my-map.hits=100", kv_string)
        self.assertIn("nc.my-map.misses=20", kv_string)
        self.assertIn("nc.my-map.entries=50", kv_string)
        self.assertIn("nc.my-map.evictions=5", kv_string)

    def test_to_key_value_string_with_custom_metrics(self):
        """Test key=value format includes custom metrics."""
        stats = ClientStatistics(
            client_name="test",
            custom_metrics={"my.gauge": 42.5, "my.counter": 100.0},
        )

        kv_string = stats.to_key_value_string()

        self.assertIn("custom.my.gauge=42.5", kv_string)
        self.assertIn("custom.my.counter=100.0", kv_string)


class TestManagementCenterService(unittest.TestCase):
    """Tests for ManagementCenterService."""

    def setUp(self):
        """Set up test fixtures."""
        self.registry = MetricsRegistry()
        self.config = StatisticsConfig(enabled=True, period_seconds=0.1)

    def tearDown(self):
        """Clean up resources."""
        self.registry.reset()

    def test_init(self):
        """Test service initialization."""
        service = ManagementCenterService(
            config=self.config,
            metrics_registry=self.registry,
            client_name="test-client",
            client_uuid="test-uuid",
            cluster_name="dev",
        )

        self.assertFalse(service.running)
        self.assertIsNone(service.last_published)
        self.assertEqual(0, service.publish_count)

    def test_start_stop(self):
        """Test starting and stopping the service."""
        service = ManagementCenterService(
            config=self.config,
            metrics_registry=self.registry,
        )

        service.start()
        self.assertTrue(service.running)

        service.shutdown()
        self.assertFalse(service.running)

    def test_start_disabled(self):
        """Test that start does nothing when disabled."""
        config = StatisticsConfig(enabled=False)
        service = ManagementCenterService(
            config=config,
            metrics_registry=self.registry,
        )

        service.start()
        self.assertFalse(service.running)

    def test_collect_statistics(self):
        """Test collecting statistics from registry."""
        self.registry.record_connection_opened()
        self.registry.record_connection_opened()
        self.registry.record_invocation_start()
        self.registry.record_invocation_end(15.0, success=True)

        service = ManagementCenterService(
            config=self.config,
            metrics_registry=self.registry,
            client_name="test-client",
            cluster_name="dev",
        )

        stats = service.collect_statistics()

        self.assertEqual("test-client", stats.client_name)
        self.assertEqual("dev", stats.cluster_name)
        self.assertEqual(2, stats.connections_active)
        self.assertEqual(1, stats.invocations_total)
        self.assertEqual(1, stats.invocations_completed)
        self.assertGreater(stats.timestamp, 0)

    def test_collect_statistics_with_near_cache(self):
        """Test collecting near cache statistics."""
        self.registry.record_near_cache_hit("my-map")
        self.registry.record_near_cache_hit("my-map")
        self.registry.record_near_cache_miss("my-map")

        service = ManagementCenterService(
            config=self.config,
            metrics_registry=self.registry,
        )

        stats = service.collect_statistics()

        self.assertIn("my-map", stats.near_cache_stats)
        nc_stats = stats.near_cache_stats["my-map"]
        self.assertEqual(2, nc_stats["hits"])
        self.assertEqual(1, nc_stats["misses"])

    def test_publish_callback(self):
        """Test that publish callback is invoked."""
        callback = Mock(return_value=True)

        service = ManagementCenterService(
            config=self.config,
            metrics_registry=self.registry,
            publish_callback=callback,
        )

        result = service.publish_statistics()

        self.assertTrue(result)
        callback.assert_called_once()
        self.assertEqual(1, service.publish_count)

    def test_publish_callback_failure(self):
        """Test handling of publish callback failure."""
        callback = Mock(return_value=False)

        service = ManagementCenterService(
            config=self.config,
            metrics_registry=self.registry,
            publish_callback=callback,
        )

        result = service.publish_statistics()

        self.assertFalse(result)
        self.assertEqual(0, service.publish_count)
        self.assertEqual(1, service.publish_failures)

    def test_statistics_listener(self):
        """Test statistics listener is called on publish."""
        listener = Mock()

        service = ManagementCenterService(
            config=self.config,
            metrics_registry=self.registry,
        )
        service.add_statistics_listener(listener)

        service.publish_statistics()

        listener.assert_called_once()
        args = listener.call_args[0]
        self.assertIsInstance(args[0], ClientStatistics)

    def test_remove_statistics_listener(self):
        """Test removing a statistics listener."""
        listener = Mock()

        service = ManagementCenterService(
            config=self.config,
            metrics_registry=self.registry,
        )
        service.add_statistics_listener(listener)

        result = service.remove_statistics_listener(listener)
        self.assertTrue(result)

        service.publish_statistics()
        listener.assert_not_called()

    def test_remove_nonexistent_listener(self):
        """Test removing a listener that was never added."""
        listener = Mock()

        service = ManagementCenterService(
            config=self.config,
            metrics_registry=self.registry,
        )

        result = service.remove_statistics_listener(listener)
        self.assertFalse(result)

    def test_periodic_collection(self):
        """Test that statistics are collected periodically."""
        collected = []

        def on_stats(stats):
            collected.append(stats)

        service = ManagementCenterService(
            config=self.config,
            metrics_registry=self.registry,
        )
        service.add_statistics_listener(on_stats)

        service.start()
        time.sleep(0.35)
        service.shutdown()

        self.assertGreaterEqual(len(collected), 2)

    def test_get_statistics_summary(self):
        """Test getting service status summary."""
        service = ManagementCenterService(
            config=self.config,
            metrics_registry=self.registry,
        )

        service.publish_statistics()
        summary = service.get_statistics_summary()

        self.assertTrue(summary["enabled"])
        self.assertFalse(summary["running"])
        self.assertEqual(0.1, summary["period_seconds"])
        self.assertEqual(1, summary["publish_count"])
        self.assertEqual(0, summary["publish_failures"])
        self.assertIsNotNone(summary["last_published"])

    def test_repr(self):
        """Test string representation."""
        service = ManagementCenterService(
            config=self.config,
            metrics_registry=self.registry,
        )

        repr_str = repr(service)

        self.assertIn("ManagementCenterService", repr_str)
        self.assertIn("enabled=True", repr_str)
        self.assertIn("running=False", repr_str)

    def test_double_start(self):
        """Test that starting twice is safe."""
        service = ManagementCenterService(
            config=self.config,
            metrics_registry=self.registry,
        )

        service.start()
        service.start()

        self.assertTrue(service.running)
        service.shutdown()

    def test_double_shutdown(self):
        """Test that shutting down twice is safe."""
        service = ManagementCenterService(
            config=self.config,
            metrics_registry=self.registry,
        )

        service.start()
        service.shutdown()
        service.shutdown()

        self.assertFalse(service.running)


class TestClientConfigStatistics(unittest.TestCase):
    """Tests for statistics in ClientConfig."""

    def test_default_statistics_config(self):
        """Test that ClientConfig has default statistics config."""
        config = ClientConfig()

        self.assertIsNotNone(config.statistics)
        self.assertFalse(config.statistics.enabled)
        self.assertEqual(3.0, config.statistics.period_seconds)

    def test_set_statistics_config(self):
        """Test setting custom statistics config."""
        config = ClientConfig()
        stats_config = StatisticsConfig(enabled=True, period_seconds=5.0)

        config.statistics = stats_config

        self.assertTrue(config.statistics.enabled)
        self.assertEqual(5.0, config.statistics.period_seconds)

    def test_from_dict_with_statistics(self):
        """Test ClientConfig.from_dict includes statistics."""
        data = {
            "cluster_name": "test",
            "statistics": {
                "enabled": True,
                "period_seconds": 10.0,
            },
        }

        config = ClientConfig.from_dict(data)

        self.assertTrue(config.statistics.enabled)
        self.assertEqual(10.0, config.statistics.period_seconds)


if __name__ == "__main__":
    unittest.main()
