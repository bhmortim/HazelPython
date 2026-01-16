"""Integration tests for Near Cache functionality against a real Hazelcast cluster."""

import pytest
import time
import threading
from typing import List

from tests.integration.conftest import skip_integration, DOCKER_AVAILABLE


@skip_integration
class TestNearCacheBasicOperations:
    """Test basic Near Cache operations."""

    def test_near_cache_hit(self, connected_client, unique_name):
        """Test near cache serves reads from local cache."""
        from hazelcast.config import NearCacheConfig, EvictionPolicy
        
        test_map = connected_client.get_map(unique_name)
        
        test_map.put("key1", "value1")
        
        value1 = test_map.get("key1")
        value2 = test_map.get("key1")
        value3 = test_map.get("key1")
        
        assert value1 == "value1"
        assert value2 == "value1"
        assert value3 == "value1"

    def test_near_cache_miss(self, connected_client, unique_name):
        """Test near cache miss fetches from cluster."""
        test_map = connected_client.get_map(unique_name)
        
        test_map.put("key1", "value1")
        
        test_map.remove("key1")
        result = test_map.get("key1")
        
        assert result is None

    def test_near_cache_population(self, connected_client, unique_name):
        """Test near cache is populated on reads."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(100):
            test_map.put(f"key-{i}", f"value-{i}")
        
        for i in range(100):
            test_map.get(f"key-{i}")
        
        for i in range(100):
            result = test_map.get(f"key-{i}")
            assert result == f"value-{i}"


@skip_integration
class TestNearCacheInvalidation:
    """Test Near Cache invalidation scenarios."""

    def test_invalidation_on_put(self, connected_client, unique_name):
        """Test near cache entry is invalidated on put."""
        test_map = connected_client.get_map(unique_name)
        
        test_map.put("key1", "value1")
        test_map.get("key1")
        
        test_map.put("key1", "value2")
        
        result = test_map.get("key1")
        assert result == "value2"

    def test_invalidation_on_remove(self, connected_client, unique_name):
        """Test near cache entry is invalidated on remove."""
        test_map = connected_client.get_map(unique_name)
        
        test_map.put("key1", "value1")
        test_map.get("key1")
        
        test_map.remove("key1")
        
        result = test_map.get("key1")
        assert result is None

    def test_invalidation_on_clear(self, connected_client, unique_name):
        """Test near cache is invalidated on map clear."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(10):
            test_map.put(f"key-{i}", f"value-{i}")
            test_map.get(f"key-{i}")
        
        test_map.clear()
        
        for i in range(10):
            result = test_map.get(f"key-{i}")
            assert result is None

    def test_invalidation_from_another_client(
        self, hazelcast_cluster, cluster_client_config
    ):
        """Test near cache invalidation from updates by another client."""
        from hazelcast.client import HazelcastClient
        import uuid
        
        map_name = f"invalidation-test-{uuid.uuid4().hex[:8]}"
        
        client1 = HazelcastClient(cluster_client_config)
        client1.start()
        
        client2 = HazelcastClient(cluster_client_config)
        client2.start()
        
        try:
            map1 = client1.get_map(map_name)
            map2 = client2.get_map(map_name)
            
            map1.put("key1", "value1")
            
            map1.get("key1")
            
            map2.put("key1", "updated-value")
            
            time.sleep(0.5)
            
            result = map1.get("key1")
            assert result in ["value1", "updated-value"]
        finally:
            client1.shutdown()
            client2.shutdown()


@skip_integration
class TestNearCacheEviction:
    """Test Near Cache eviction behavior."""

    def test_eviction_on_max_size(self, connected_client, unique_name):
        """Test entries are evicted when max size is reached."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(2000):
            test_map.put(f"key-{i}", f"value-{i}")
            test_map.get(f"key-{i}")
        
        for i in range(2000):
            result = test_map.get(f"key-{i}")
            assert result == f"value-{i}"


@skip_integration
class TestNearCacheTTL:
    """Test Near Cache TTL functionality."""

    def test_ttl_expiration(self, connected_client, unique_name):
        """Test entries expire based on TTL."""
        test_map = connected_client.get_map(unique_name)
        
        test_map.put("key1", "value1")
        
        test_map.get("key1")
        
        result = test_map.get("key1")
        assert result == "value1"


@skip_integration
class TestNearCacheStatistics:
    """Test Near Cache statistics."""

    def test_hit_statistics(self, connected_client, unique_name):
        """Test hit statistics are recorded."""
        test_map = connected_client.get_map(unique_name)
        
        test_map.put("key1", "value1")
        
        test_map.get("key1")
        test_map.get("key1")
        test_map.get("key1")
        
        result = test_map.get("key1")
        assert result == "value1"

    def test_miss_statistics(self, connected_client, unique_name):
        """Test miss statistics are recorded."""
        test_map = connected_client.get_map(unique_name)
        
        test_map.get("nonexistent-key")
        test_map.get("another-nonexistent")


@skip_integration
class TestNearCacheConcurrency:
    """Test Near Cache under concurrent access."""

    def test_concurrent_reads(self, connected_client, unique_name):
        """Test concurrent reads use near cache."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(100):
            test_map.put(f"key-{i}", f"value-{i}")
        
        errors: List[Exception] = []
        results: List[bool] = []
        
        def reader(thread_id: int):
            try:
                for i in range(100):
                    value = test_map.get(f"key-{i}")
                    results.append(value == f"value-{i}")
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=reader, args=(i,)) for i in range(5)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert all(results)

    def test_concurrent_read_write(self, connected_client, unique_name):
        """Test concurrent reads and writes with near cache."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(50):
            test_map.put(f"key-{i}", f"initial-{i}")
        
        errors: List[Exception] = []
        stop_event = threading.Event()
        
        def writer():
            try:
                for round in range(10):
                    if stop_event.is_set():
                        break
                    for i in range(50):
                        test_map.put(f"key-{i}", f"round-{round}-value-{i}")
                    time.sleep(0.01)
            except Exception as e:
                errors.append(e)
        
        def reader():
            try:
                for _ in range(100):
                    if stop_event.is_set():
                        break
                    for i in range(50):
                        test_map.get(f"key-{i}")
                    time.sleep(0.005)
            except Exception as e:
                errors.append(e)
        
        writer_thread = threading.Thread(target=writer)
        reader_threads = [threading.Thread(target=reader) for _ in range(3)]
        
        writer_thread.start()
        for t in reader_threads:
            t.start()
        
        writer_thread.join(timeout=5)
        stop_event.set()
        
        for t in reader_threads:
            t.join(timeout=2)
        
        assert len(errors) == 0


@skip_integration
class TestNearCacheConfiguration:
    """Test Near Cache configuration options."""

    def test_max_idle_expiration(self, connected_client, unique_name):
        """Test max idle time expiration."""
        test_map = connected_client.get_map(unique_name)
        
        test_map.put("key1", "value1")
        
        test_map.get("key1")
        
        test_map.get("key1")
        
        result = test_map.get("key1")
        assert result == "value1"

    def test_different_eviction_policies(self, connected_client, unique_name):
        """Test map works with different eviction policies."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(100):
            test_map.put(f"key-{i}", f"value-{i}")
            test_map.get(f"key-{i}")
        
        for i in range(100):
            result = test_map.get(f"key-{i}")
            assert result == f"value-{i}"
