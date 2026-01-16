"""Integration tests for MultiMap and ReplicatedMap against a real Hazelcast cluster."""

import pytest
import time
import threading
from typing import List

from tests.integration.conftest import skip_integration, DOCKER_AVAILABLE


@skip_integration
class TestMultiMapBasicOperations:
    """Test basic MultiMap operations."""

    def test_put_and_get(self, connected_client, unique_name):
        """Test put and get operations."""
        mm = connected_client.get_multi_map(unique_name)
        
        mm.put("key1", "value1")
        mm.put("key1", "value2")
        mm.put("key1", "value3")
        
        values = mm.get("key1")
        assert len(values) == 3
        assert set(values) == {"value1", "value2", "value3"}

    def test_put_returns_true_for_new(self, connected_client, unique_name):
        """Test put returns True for new value."""
        mm = connected_client.get_multi_map(unique_name)
        
        result1 = mm.put("key1", "value1")
        assert result1 is True

    def test_remove_single_value(self, connected_client, unique_name):
        """Test removing a single value."""
        mm = connected_client.get_multi_map(unique_name)
        
        mm.put("key1", "value1")
        mm.put("key1", "value2")
        
        result = mm.remove("key1", "value1")
        assert result is True
        
        values = mm.get("key1")
        assert values == ["value2"]

    def test_remove_all_values(self, connected_client, unique_name):
        """Test removing all values for a key."""
        mm = connected_client.get_multi_map(unique_name)
        
        mm.put("key1", "value1")
        mm.put("key1", "value2")
        
        removed = mm.remove_all("key1")
        assert len(removed) == 2
        assert mm.get("key1") == []

    def test_contains_key(self, connected_client, unique_name):
        """Test contains_key operation."""
        mm = connected_client.get_multi_map(unique_name)
        
        mm.put("key1", "value1")
        
        assert mm.contains_key("key1") is True
        assert mm.contains_key("nonexistent") is False

    def test_contains_value(self, connected_client, unique_name):
        """Test contains_value operation."""
        mm = connected_client.get_multi_map(unique_name)
        
        mm.put("key1", "value1")
        
        assert mm.contains_value("value1") is True
        assert mm.contains_value("nonexistent") is False

    def test_contains_entry(self, connected_client, unique_name):
        """Test contains_entry operation."""
        mm = connected_client.get_multi_map(unique_name)
        
        mm.put("key1", "value1")
        
        assert mm.contains_entry("key1", "value1") is True
        assert mm.contains_entry("key1", "wrong") is False

    def test_size_and_value_count(self, connected_client, unique_name):
        """Test size and value_count operations."""
        mm = connected_client.get_multi_map(unique_name)
        
        mm.put("key1", "value1")
        mm.put("key1", "value2")
        mm.put("key2", "value3")
        
        assert mm.size() == 3
        assert mm.value_count("key1") == 2
        assert mm.value_count("key2") == 1

    def test_key_set(self, connected_client, unique_name):
        """Test key_set operation."""
        mm = connected_client.get_multi_map(unique_name)
        
        mm.put("key1", "value1")
        mm.put("key2", "value2")
        
        keys = mm.key_set()
        assert set(keys) == {"key1", "key2"}

    def test_values(self, connected_client, unique_name):
        """Test values operation."""
        mm = connected_client.get_multi_map(unique_name)
        
        mm.put("key1", "value1")
        mm.put("key1", "value2")
        mm.put("key2", "value3")
        
        values = mm.values()
        assert set(values) == {"value1", "value2", "value3"}

    def test_entry_set(self, connected_client, unique_name):
        """Test entry_set operation."""
        mm = connected_client.get_multi_map(unique_name)
        
        mm.put("key1", "value1")
        mm.put("key1", "value2")
        
        entries = mm.entry_set()
        assert len(entries) == 2

    def test_clear(self, connected_client, unique_name):
        """Test clear operation."""
        mm = connected_client.get_multi_map(unique_name)
        
        mm.put("key1", "value1")
        mm.put("key2", "value2")
        mm.clear()
        
        assert mm.size() == 0


@skip_integration
class TestReplicatedMapBasicOperations:
    """Test basic ReplicatedMap operations."""

    def test_put_and_get(self, connected_client, unique_name):
        """Test put and get operations."""
        rm = connected_client.get_replicated_map(unique_name)
        
        rm.put("key1", "value1")
        result = rm.get("key1")
        
        assert result == "value1"

    def test_put_all(self, connected_client, unique_name):
        """Test put_all operation."""
        rm = connected_client.get_replicated_map(unique_name)
        
        entries = {"a": 1, "b": 2, "c": 3}
        rm.put_all(entries)
        
        assert rm.size() == 3
        for key, value in entries.items():
            assert rm.get(key) == value

    def test_remove(self, connected_client, unique_name):
        """Test remove operation."""
        rm = connected_client.get_replicated_map(unique_name)
        
        rm.put("key1", "value1")
        removed = rm.remove("key1")
        
        assert removed == "value1"
        assert rm.get("key1") is None

    def test_contains_key(self, connected_client, unique_name):
        """Test contains_key operation."""
        rm = connected_client.get_replicated_map(unique_name)
        
        rm.put("key1", "value1")
        
        assert rm.contains_key("key1") is True
        assert rm.contains_key("nonexistent") is False

    def test_contains_value(self, connected_client, unique_name):
        """Test contains_value operation."""
        rm = connected_client.get_replicated_map(unique_name)
        
        rm.put("key1", "value1")
        
        assert rm.contains_value("value1") is True
        assert rm.contains_value("nonexistent") is False

    def test_size_and_is_empty(self, connected_client, unique_name):
        """Test size and is_empty operations."""
        rm = connected_client.get_replicated_map(unique_name)
        
        assert rm.is_empty() is True
        assert rm.size() == 0
        
        rm.put("key1", "value1")
        
        assert rm.is_empty() is False
        assert rm.size() == 1

    def test_key_set(self, connected_client, unique_name):
        """Test key_set operation."""
        rm = connected_client.get_replicated_map(unique_name)
        
        rm.put("a", 1)
        rm.put("b", 2)
        
        keys = rm.key_set()
        assert set(keys) == {"a", "b"}

    def test_values(self, connected_client, unique_name):
        """Test values operation."""
        rm = connected_client.get_replicated_map(unique_name)
        
        rm.put("a", 1)
        rm.put("b", 2)
        
        values = rm.values()
        assert set(values) == {1, 2}

    def test_entry_set(self, connected_client, unique_name):
        """Test entry_set operation."""
        rm = connected_client.get_replicated_map(unique_name)
        
        rm.put("a", 1)
        rm.put("b", 2)
        
        entries = rm.entry_set()
        assert len(entries) == 2

    def test_clear(self, connected_client, unique_name):
        """Test clear operation."""
        rm = connected_client.get_replicated_map(unique_name)
        
        rm.put("a", 1)
        rm.put("b", 2)
        rm.clear()
        
        assert rm.size() == 0


@skip_integration
class TestReplicatedMapReplication:
    """Test ReplicatedMap replication behavior."""

    def test_replication_across_clients(self, hazelcast_cluster, cluster_client_config):
        """Test data is replicated and visible to multiple clients."""
        from hazelcast.client import HazelcastClient
        import uuid
        
        map_name = f"replicated-test-{uuid.uuid4().hex[:8]}"
        
        client1 = HazelcastClient(cluster_client_config)
        client1.start()
        
        client2 = HazelcastClient(cluster_client_config)
        client2.start()
        
        try:
            rm1 = client1.get_replicated_map(map_name)
            rm2 = client2.get_replicated_map(map_name)
            
            rm1.put("key1", "value1")
            time.sleep(0.5)
            
            result = rm2.get("key1")
            assert result == "value1"
        finally:
            client1.shutdown()
            client2.shutdown()


@skip_integration
class TestMultiMapListeners:
    """Test MultiMap entry listeners."""

    def test_entry_added_listener(self, connected_client, unique_name):
        """Test entry added listener."""
        mm = connected_client.get_multi_map(unique_name)
        events: List[dict] = []
        
        def on_added(event):
            events.append({"key": event.key, "value": event.value})
        
        mm.add_entry_listener(on_added=on_added)
        mm.put("key1", "value1")
        
        time.sleep(0.5)
        assert len(events) >= 1


@skip_integration
class TestReplicatedMapListeners:
    """Test ReplicatedMap entry listeners."""

    def test_entry_added_listener(self, connected_client, unique_name):
        """Test entry added listener."""
        rm = connected_client.get_replicated_map(unique_name)
        events: List[dict] = []
        
        def on_added(event):
            events.append({"key": event.key, "value": event.value})
        
        rm.add_entry_listener(on_added=on_added)
        rm.put("key1", "value1")
        
        time.sleep(0.5)
        assert len(events) >= 1
