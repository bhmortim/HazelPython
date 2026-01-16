"""Unit tests for hazelcast.near_cache module."""

import pytest
import time
from unittest.mock import MagicMock, patch

from hazelcast.near_cache import (
    NearCache,
    NearCacheManager,
    NearCacheStats,
    NearCacheRecord,
    EvictionStrategy,
    LRUEvictionStrategy,
    LFUEvictionStrategy,
    RandomEvictionStrategy,
    NoneEvictionStrategy,
    _create_eviction_strategy,
)
from hazelcast.config import NearCacheConfig, EvictionPolicy, InMemoryFormat


class TestNearCacheStats:
    """Tests for NearCacheStats."""

    def test_defaults(self):
        stats = NearCacheStats()
        assert stats.hits == 0
        assert stats.misses == 0
        assert stats.evictions == 0
        assert stats.expirations == 0
        assert stats.invalidations == 0
        assert stats.entries_count == 0

    def test_hit_ratio_empty(self):
        stats = NearCacheStats()
        assert stats.hit_ratio == 0.0
        assert stats.miss_ratio == 0.0

    def test_hit_ratio(self):
        stats = NearCacheStats(hits=80, misses=20)
        assert stats.hit_ratio == 0.8
        assert stats.miss_ratio == 0.2

    def test_to_dict(self):
        stats = NearCacheStats(hits=10, misses=5, evictions=2)
        d = stats.to_dict()
        assert d["hits"] == 10
        assert d["misses"] == 5
        assert d["evictions"] == 2
        assert "hit_ratio" in d
        assert "miss_ratio" in d


class TestNearCacheRecord:
    """Tests for NearCacheRecord."""

    def test_create(self):
        record = NearCacheRecord(value="test")
        assert record.value == "test"
        assert record.access_count == 0
        assert record.ttl_seconds == 0
        assert record.max_idle_seconds == 0

    def test_not_expired_by_default(self):
        record = NearCacheRecord(value="test")
        assert record.is_expired() is False

    def test_ttl_expiration(self):
        record = NearCacheRecord(
            value="test",
            ttl_seconds=1,
            creation_time=time.time() - 2,
        )
        assert record.is_expired() is True

    def test_max_idle_expiration(self):
        record = NearCacheRecord(
            value="test",
            max_idle_seconds=1,
            last_access_time=time.time() - 2,
        )
        assert record.is_expired() is True

    def test_record_access(self):
        record = NearCacheRecord(value="test")
        old_time = record.last_access_time
        
        time.sleep(0.01)
        record.record_access()
        
        assert record.access_count == 1
        assert record.last_access_time >= old_time


class TestEvictionStrategies:
    """Tests for eviction strategies."""

    def test_lru_strategy(self):
        strategy = LRUEvictionStrategy()
        now = time.time()
        records = {
            "key1": NearCacheRecord(value=1, last_access_time=now - 100),
            "key2": NearCacheRecord(value=2, last_access_time=now - 50),
            "key3": NearCacheRecord(value=3, last_access_time=now),
        }
        
        evict_key = strategy.select_for_eviction(records)
        assert evict_key == "key1"

    def test_lru_empty(self):
        strategy = LRUEvictionStrategy()
        assert strategy.select_for_eviction({}) is None

    def test_lfu_strategy(self):
        strategy = LFUEvictionStrategy()
        records = {
            "key1": NearCacheRecord(value=1, access_count=10),
            "key2": NearCacheRecord(value=2, access_count=1),
            "key3": NearCacheRecord(value=3, access_count=5),
        }
        
        evict_key = strategy.select_for_eviction(records)
        assert evict_key == "key2"

    def test_lfu_empty(self):
        strategy = LFUEvictionStrategy()
        assert strategy.select_for_eviction({}) is None

    def test_random_strategy(self):
        strategy = RandomEvictionStrategy()
        records = {
            "key1": NearCacheRecord(value=1),
            "key2": NearCacheRecord(value=2),
        }
        
        evict_key = strategy.select_for_eviction(records)
        assert evict_key in records

    def test_random_empty(self):
        strategy = RandomEvictionStrategy()
        assert strategy.select_for_eviction({}) is None

    def test_none_strategy(self):
        strategy = NoneEvictionStrategy()
        records = {"key1": NearCacheRecord(value=1)}
        
        assert strategy.select_for_eviction(records) is None

    @pytest.mark.parametrize("policy,expected_type", [
        (EvictionPolicy.LRU, LRUEvictionStrategy),
        (EvictionPolicy.LFU, LFUEvictionStrategy),
        (EvictionPolicy.RANDOM, RandomEvictionStrategy),
        (EvictionPolicy.NONE, NoneEvictionStrategy),
    ])
    def test_create_eviction_strategy(self, policy, expected_type):
        strategy = _create_eviction_strategy(policy)
        assert isinstance(strategy, expected_type)


class TestNearCache:
    """Tests for NearCache."""

    @pytest.fixture
    def config(self):
        return NearCacheConfig(
            name="test-cache",
            max_size=100,
            time_to_live_seconds=0,
            max_idle_seconds=0,
        )

    @pytest.fixture
    def cache(self, config):
        return NearCache(config)

    def test_create(self, cache):
        assert cache.name == "test-cache"
        assert cache.size == 0

    def test_put_and_get(self, cache):
        cache.put("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_get_miss(self, cache):
        assert cache.get("nonexistent") is None

    def test_get_updates_stats(self, cache):
        cache.put("key", "value")
        cache.get("key")  # hit
        cache.get("missing")  # miss
        
        stats = cache.stats
        assert stats.hits == 1
        assert stats.misses == 1

    def test_remove(self, cache):
        cache.put("key", "value")
        removed = cache.remove("key")
        
        assert removed == "value"
        assert cache.get("key") is None

    def test_remove_nonexistent(self, cache):
        assert cache.remove("missing") is None

    def test_invalidate(self, cache):
        cache.put("key", "value")
        cache.invalidate("key")
        
        assert cache.get("key") is None
        assert cache.stats.invalidations == 1

    def test_invalidate_all(self, cache):
        cache.put("k1", "v1")
        cache.put("k2", "v2")
        cache.invalidate_all()
        
        assert cache.size == 0
        assert cache.stats.invalidations == 2

    def test_clear(self, cache):
        cache.put("k1", "v1")
        cache.put("k2", "v2")
        cache.clear()
        
        assert cache.size == 0

    def test_contains(self, cache):
        cache.put("key", "value")
        assert cache.contains("key") is True
        assert cache.contains("missing") is False

    def test_eviction(self):
        config = NearCacheConfig(name="small", max_size=3)
        cache = NearCache(config)
        
        cache.put("k1", "v1")
        cache.put("k2", "v2")
        cache.put("k3", "v3")
        cache.put("k4", "v4")
        
        assert cache.size <= 3

    def test_ttl_expiration(self):
        config = NearCacheConfig(
            name="ttl-cache",
            time_to_live_seconds=1,
        )
        cache = NearCache(config)
        
        cache.put("key", "value")
        time.sleep(1.1)
        
        assert cache.get("key") is None
        assert cache.stats.misses == 1

    def test_add_invalidation_listener(self, cache):
        events = []
        
        def listener(key):
            events.append(key)
        
        reg_id = cache.add_invalidation_listener(listener)
        cache.put("key", "value")
        cache.invalidate("key")
        
        assert len(events) == 1
        assert events[0] == "key"
        assert reg_id is not None

    def test_remove_invalidation_listener(self, cache):
        events = []
        reg_id = cache.add_invalidation_listener(lambda k: events.append(k))
        
        removed = cache.remove_invalidation_listener(reg_id)
        assert removed is True
        
        cache.put("key", "value")
        cache.invalidate("key")
        assert len(events) == 0

    def test_remove_unknown_listener(self, cache):
        assert cache.remove_invalidation_listener("unknown") is False

    def test_do_expiration(self):
        config = NearCacheConfig(name="exp", time_to_live_seconds=1)
        cache = NearCache(config)
        
        cache.put("k1", "v1")
        cache.put("k2", "v2")
        time.sleep(1.1)
        
        expired = cache.do_expiration()
        assert expired == 2
        assert cache.size == 0

    def test_invalidate_batch(self, cache):
        cache.put("k1", "v1")
        cache.put("k2", "v2")
        cache.put("k3", "v3")
        
        count = cache.invalidate_batch(["k1", "k3", "k5"])
        
        assert count == 2
        assert cache.get("k2") == "v2"

    def test_get_stats_snapshot(self, cache):
        cache.put("k", "v")
        cache.get("k")
        
        snapshot = cache.get_stats_snapshot()
        
        assert snapshot.hits == 1
        assert snapshot.entries_count == 1


class TestNearCacheManager:
    """Tests for NearCacheManager."""

    @pytest.fixture
    def manager(self):
        return NearCacheManager()

    def test_get_or_create(self, manager):
        config = NearCacheConfig(name="map1", max_size=100)
        cache = manager.get_or_create("map1", config)
        
        assert cache is not None
        assert cache.name == "map1"

    def test_get_or_create_returns_same(self, manager):
        config = NearCacheConfig(name="map1")
        cache1 = manager.get_or_create("map1", config)
        cache2 = manager.get_or_create("map1", config)
        
        assert cache1 is cache2

    def test_get_existing(self, manager):
        config = NearCacheConfig(name="map1")
        manager.get_or_create("map1", config)
        
        cache = manager.get("map1")
        assert cache is not None

    def test_get_nonexistent(self, manager):
        assert manager.get("missing") is None

    def test_destroy(self, manager):
        config = NearCacheConfig(name="map1")
        cache = manager.get_or_create("map1", config)
        cache.put("k", "v")
        
        manager.destroy("map1")
        
        assert manager.get("map1") is None

    def test_destroy_all(self, manager):
        manager.get_or_create("map1", NearCacheConfig(name="map1"))
        manager.get_or_create("map2", NearCacheConfig(name="map2"))
        
        manager.destroy_all()
        
        assert len(manager.list_all()) == 0

    def test_list_all(self, manager):
        manager.get_or_create("map1", NearCacheConfig(name="map1"))
        cache2 = manager.get_or_create("map2", NearCacheConfig(name="map2"))
        cache2.put("k", "v")
        cache2.get("k")
        
        stats = manager.list_all()
        
        assert "map1" in stats
        assert "map2" in stats
        assert stats["map2"].hits == 1
