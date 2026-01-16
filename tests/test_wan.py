"""Tests for WAN replication support."""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future

from hazelcast.wan import (
    WanReplicationRef,
    WanSyncState,
    WanPublisherState,
    WanSyncResult,
    MergePolicy,
)
from hazelcast.config import WanReplicationConfig
from hazelcast.exceptions import ConfigurationException


class TestWanReplicationRef(unittest.TestCase):
    """Tests for WanReplicationRef class."""

    def test_create_with_defaults(self):
        """Test creating WanReplicationRef with default values."""
        ref = WanReplicationRef("my-wan")

        self.assertEqual("my-wan", ref.name)
        self.assertEqual(MergePolicy.PASS_THROUGH.value, ref.merge_policy_class_name)
        self.assertFalse(ref.republishing_enabled)
        self.assertEqual([], ref.filters)

    def test_create_with_all_options(self):
        """Test creating WanReplicationRef with all options."""
        ref = WanReplicationRef(
            name="custom-wan",
            merge_policy_class_name=MergePolicy.LATEST_UPDATE.value,
            republishing_enabled=True,
            filters=["com.example.Filter1", "com.example.Filter2"],
        )

        self.assertEqual("custom-wan", ref.name)
        self.assertEqual(MergePolicy.LATEST_UPDATE.value, ref.merge_policy_class_name)
        self.assertTrue(ref.republishing_enabled)
        self.assertEqual(
            ["com.example.Filter1", "com.example.Filter2"],
            ref.filters,
        )

    def test_empty_name_raises_exception(self):
        """Test that empty name raises ConfigurationException."""
        with self.assertRaises(ConfigurationException) as ctx:
            WanReplicationRef("")

        self.assertIn("name cannot be empty", str(ctx.exception))

    def test_set_name_to_empty_raises_exception(self):
        """Test that setting name to empty raises exception."""
        ref = WanReplicationRef("valid-name")

        with self.assertRaises(ConfigurationException):
            ref.name = ""

    def test_add_filter(self):
        """Test adding filters with method chaining."""
        ref = WanReplicationRef("my-wan")

        result = ref.add_filter("com.example.Filter1")
        result.add_filter("com.example.Filter2")

        self.assertIs(ref, result)
        self.assertEqual(
            ["com.example.Filter1", "com.example.Filter2"],
            ref.filters,
        )

    def test_from_dict(self):
        """Test creating WanReplicationRef from dictionary."""
        data = {
            "name": "dict-wan",
            "merge_policy_class_name": MergePolicy.HIGHER_HITS.value,
            "republishing_enabled": True,
            "filters": ["com.example.MyFilter"],
        }

        ref = WanReplicationRef.from_dict(data)

        self.assertEqual("dict-wan", ref.name)
        self.assertEqual(MergePolicy.HIGHER_HITS.value, ref.merge_policy_class_name)
        self.assertTrue(ref.republishing_enabled)
        self.assertEqual(["com.example.MyFilter"], ref.filters)

    def test_from_dict_with_defaults(self):
        """Test from_dict uses defaults for missing values."""
        data = {"name": "minimal-wan"}

        ref = WanReplicationRef.from_dict(data)

        self.assertEqual("minimal-wan", ref.name)
        self.assertEqual(MergePolicy.PASS_THROUGH.value, ref.merge_policy_class_name)
        self.assertFalse(ref.republishing_enabled)
        self.assertEqual([], ref.filters)

    def test_to_dict(self):
        """Test converting WanReplicationRef to dictionary."""
        ref = WanReplicationRef(
            name="test-wan",
            merge_policy_class_name=MergePolicy.PUT_IF_ABSENT.value,
            republishing_enabled=True,
            filters=["filter1"],
        )

        result = ref.to_dict()

        self.assertEqual("test-wan", result["name"])
        self.assertEqual(MergePolicy.PUT_IF_ABSENT.value, result["merge_policy_class_name"])
        self.assertTrue(result["republishing_enabled"])
        self.assertEqual(["filter1"], result["filters"])

    def test_repr(self):
        """Test string representation."""
        ref = WanReplicationRef("test-wan", republishing_enabled=True)

        repr_str = repr(ref)

        self.assertIn("test-wan", repr_str)
        self.assertIn("WanReplicationRef", repr_str)
        self.assertIn("republishing=True", repr_str)

    def test_equality(self):
        """Test equality comparison."""
        ref1 = WanReplicationRef(
            "wan",
            merge_policy_class_name=MergePolicy.LATEST_UPDATE.value,
        )
        ref2 = WanReplicationRef(
            "wan",
            merge_policy_class_name=MergePolicy.LATEST_UPDATE.value,
        )
        ref3 = WanReplicationRef("different-wan")

        self.assertEqual(ref1, ref2)
        self.assertNotEqual(ref1, ref3)
        self.assertNotEqual(ref1, "not a ref")

    def test_hash(self):
        """Test that WanReplicationRef is hashable."""
        ref1 = WanReplicationRef("wan1")
        ref2 = WanReplicationRef("wan1")
        ref3 = WanReplicationRef("wan2")

        ref_set = {ref1, ref2, ref3}

        self.assertEqual(2, len(ref_set))

    def test_set_properties(self):
        """Test setting properties after creation."""
        ref = WanReplicationRef("initial")

        ref.name = "updated"
        ref.merge_policy_class_name = MergePolicy.LATEST_ACCESS.value
        ref.republishing_enabled = True
        ref.filters = ["new-filter"]

        self.assertEqual("updated", ref.name)
        self.assertEqual(MergePolicy.LATEST_ACCESS.value, ref.merge_policy_class_name)
        self.assertTrue(ref.republishing_enabled)
        self.assertEqual(["new-filter"], ref.filters)


class TestWanSyncState(unittest.TestCase):
    """Tests for WanSyncState enum."""

    def test_states(self):
        """Test all sync states exist."""
        self.assertEqual("IN_PROGRESS", WanSyncState.IN_PROGRESS.value)
        self.assertEqual("COMPLETED", WanSyncState.COMPLETED.value)
        self.assertEqual("FAILED", WanSyncState.FAILED.value)


class TestWanPublisherState(unittest.TestCase):
    """Tests for WanPublisherState enum."""

    def test_states(self):
        """Test all publisher states exist."""
        self.assertEqual("REPLICATING", WanPublisherState.REPLICATING.value)
        self.assertEqual("PAUSED", WanPublisherState.PAUSED.value)
        self.assertEqual("STOPPED", WanPublisherState.STOPPED.value)


class TestMergePolicy(unittest.TestCase):
    """Tests for MergePolicy enum."""

    def test_policies(self):
        """Test all merge policies have correct class names."""
        self.assertIn("PassThroughMergePolicy", MergePolicy.PASS_THROUGH.value)
        self.assertIn("PutIfAbsentMergePolicy", MergePolicy.PUT_IF_ABSENT.value)
        self.assertIn("HigherHitsMergePolicy", MergePolicy.HIGHER_HITS.value)
        self.assertIn("LatestUpdateMergePolicy", MergePolicy.LATEST_UPDATE.value)
        self.assertIn("LatestAccessMergePolicy", MergePolicy.LATEST_ACCESS.value)
        self.assertIn("ExpirationTimeMergePolicy", MergePolicy.EXPIRATION_TIME.value)


class TestWanSyncResult(unittest.TestCase):
    """Tests for WanSyncResult class."""

    def test_create_with_defaults(self):
        """Test creating WanSyncResult with defaults."""
        result = WanSyncResult()

        self.assertEqual(WanSyncState.IN_PROGRESS, result.state)
        self.assertEqual(-1, result.partition_id)
        self.assertEqual(0, result.entries_synced)
        self.assertFalse(result.is_completed)
        self.assertFalse(result.is_failed)

    def test_completed_result(self):
        """Test completed sync result."""
        result = WanSyncResult(
            state=WanSyncState.COMPLETED,
            partition_id=5,
            entries_synced=1000,
        )

        self.assertTrue(result.is_completed)
        self.assertFalse(result.is_failed)
        self.assertEqual(5, result.partition_id)
        self.assertEqual(1000, result.entries_synced)

    def test_failed_result(self):
        """Test failed sync result."""
        result = WanSyncResult(state=WanSyncState.FAILED)

        self.assertFalse(result.is_completed)
        self.assertTrue(result.is_failed)

    def test_repr(self):
        """Test string representation."""
        result = WanSyncResult(
            state=WanSyncState.COMPLETED,
            entries_synced=500,
        )

        repr_str = repr(result)

        self.assertIn("COMPLETED", repr_str)
        self.assertIn("500", repr_str)


class TestWanReplicationConfig(unittest.TestCase):
    """Tests for WanReplicationConfig from config module."""

    def test_create_with_defaults(self):
        """Test creating config with default values."""
        config = WanReplicationConfig()

        self.assertEqual("dev", config.cluster_name)
        self.assertEqual([], config.endpoints)
        self.assertEqual(10000, config.queue_capacity)
        self.assertEqual(500, config.batch_size)
        self.assertEqual(1000, config.batch_max_delay_millis)
        self.assertEqual(60000, config.response_timeout_millis)
        self.assertEqual("ACK_ON_OPERATION_COMPLETE", config.acknowledge_type)

    def test_create_with_custom_values(self):
        """Test creating config with custom values."""
        config = WanReplicationConfig(
            cluster_name="target-cluster",
            endpoints=["target1:5701", "target2:5701"],
            queue_capacity=20000,
            batch_size=1000,
            batch_max_delay_millis=2000,
            response_timeout_millis=120000,
            acknowledge_type="ACK_ON_RECEIPT",
        )

        self.assertEqual("target-cluster", config.cluster_name)
        self.assertEqual(["target1:5701", "target2:5701"], config.endpoints)
        self.assertEqual(20000, config.queue_capacity)
        self.assertEqual(1000, config.batch_size)
        self.assertEqual(2000, config.batch_max_delay_millis)
        self.assertEqual(120000, config.response_timeout_millis)
        self.assertEqual("ACK_ON_RECEIPT", config.acknowledge_type)

    def test_invalid_acknowledge_type(self):
        """Test that invalid acknowledge type raises exception."""
        with self.assertRaises(ConfigurationException):
            WanReplicationConfig(acknowledge_type="INVALID")

    def test_invalid_queue_capacity(self):
        """Test that invalid queue capacity raises exception."""
        with self.assertRaises(ConfigurationException):
            WanReplicationConfig(queue_capacity=0)

    def test_invalid_batch_size(self):
        """Test that invalid batch size raises exception."""
        with self.assertRaises(ConfigurationException):
            WanReplicationConfig(batch_size=-1)

    def test_add_endpoint(self):
        """Test adding endpoints with method chaining."""
        config = WanReplicationConfig()

        result = config.add_endpoint("host1:5701")
        result.add_endpoint("host2:5701")

        self.assertIs(config, result)
        self.assertEqual(["host1:5701", "host2:5701"], config.endpoints)

    def test_from_dict(self):
        """Test creating config from dictionary."""
        data = {
            "cluster_name": "remote-cluster",
            "endpoints": ["remote:5701"],
            "queue_capacity": 5000,
            "batch_size": 250,
        }

        config = WanReplicationConfig.from_dict(data)

        self.assertEqual("remote-cluster", config.cluster_name)
        self.assertEqual(["remote:5701"], config.endpoints)
        self.assertEqual(5000, config.queue_capacity)
        self.assertEqual(250, config.batch_size)

    def test_empty_cluster_name_raises_exception(self):
        """Test that empty cluster name raises exception."""
        with self.assertRaises(ConfigurationException):
            WanReplicationConfig(cluster_name="")


class TestMapProxyWanMethods(unittest.TestCase):
    """Tests for WAN-aware methods in MapProxy."""

    def setUp(self):
        """Set up test fixtures."""
        self.wan_ref = WanReplicationRef("test-wan")

    @patch("hazelcast.proxy.map.MapCodec")
    def test_put_wan_invalidates_near_cache(self, mock_codec):
        """Test that put_wan invalidates near cache."""
        from hazelcast.proxy.map import MapProxy

        mock_near_cache = MagicMock()
        mock_context = MagicMock()
        mock_context.serialization_service.to_data.side_effect = lambda x: x
        mock_context.serialization_service.to_object.side_effect = lambda x: x

        proxy = MapProxy("test-map", mock_context, mock_near_cache)
        proxy._destroyed = False

        future = Future()
        future.set_result(MagicMock())
        proxy._invoke = MagicMock(return_value=future)
        mock_codec.encode_put_wan_request.return_value = MagicMock()
        mock_codec.decode_put_wan_response.return_value = None

        proxy.put_wan("key", "value", self.wan_ref)

        mock_near_cache.invalidate.assert_called_once_with("key")

    @patch("hazelcast.proxy.map.MapCodec")
    def test_remove_wan_invalidates_near_cache(self, mock_codec):
        """Test that remove_wan invalidates near cache."""
        from hazelcast.proxy.map import MapProxy

        mock_near_cache = MagicMock()
        mock_context = MagicMock()
        mock_context.serialization_service.to_data.side_effect = lambda x: x
        mock_context.serialization_service.to_object.side_effect = lambda x: x

        proxy = MapProxy("test-map", mock_context, mock_near_cache)
        proxy._destroyed = False

        future = Future()
        future.set_result(MagicMock())
        proxy._invoke = MagicMock(return_value=future)
        mock_codec.encode_remove_wan_request.return_value = MagicMock()
        mock_codec.decode_remove_wan_response.return_value = None

        proxy.remove_wan("key", self.wan_ref)

        mock_near_cache.invalidate.assert_called_once_with("key")


if __name__ == "__main__":
    unittest.main()
