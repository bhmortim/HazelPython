"""Unit tests for hazelcast.near_cache module."""

import pytest
import time
from unittest.mock import MagicMock

from hazelcast.near_cache import (
    NearCache,
    NearCacheManager,
    NearCacheRecord,
    NearCacheStats,
    LRUEvictionStrategy,
    LFUEvictionStrategy,
    RandomEvictionStrategy,
    NoneEvictionStrategy,
    _create_eviction_strategy,
)
from hazelcast.config import NearCacheConfig, EvictionPolicy, InMemoryFormat


class TestNearCacheStats:
    """Tests for NearCacheStats class."""

    def test_default_values(self):
        stats = NearCacheStats()
        assert stats.hits == 0
        assert stats.misses == 0
        assert stats.evictions == 0
        assert stats.expirations == 0
        assert stats.invalidations == 0
        assert stats.entries_count == 0

    def test_hit_ratio_no_accesses(self):
        stats = NearCacheStats()
        assert stats.hit_ratio == 0.0

    def test_hit_ratio_with_accesses(self):
        stats = NearCacheStats(hits=80, misses=20)
        assert stats.hit_ratio == 0.8

    def test_miss_ratio(self):
        stats = NearCacheStats(hits=80, misses=20)
        assert stats.miss_ratio == 0.2

    def test_to_dict(self):
        stats = NearCacheStats(hits=10, misses=5)
        d = stats.to_dict()
        assert d["hits"] == 10
        assert d["misses"] == 5
        assert "hit_ratio" in d


class TestNearCacheRecord:
    """Tests for NearCacheRecord class."""

    def test_init(self):
        record = NearCacheRecord(value="test")
        assert record.value == "test"
        assert record.access_count == 0
        assert record.ttl_seconds == 0
        assert record.max_idle_seconds == 0

    def test_is_expired_no_expiry(self):
        record = NearCacheRecord(value="test")
        assert record.is_expired() is False

    def test_is_expired_ttl(self):
        record = NearCacheRecord(
            value="test",
            ttl_seconds=1,
        )
        record.creation_time = time.time() - 2
        assert record.is_expired() is True

    def test_is_expired_max_idle(self):
        record = NearCacheRecord(
            value="test",
            max_idle_seconds=1,
        )
        record.last_access_time = time.time() - 2
        assert record.is_expired() is True

    def test_record_access(self):
        record = NearCacheRecord(value="test")
        initial_count = record.access_count
        record.record_access()
        assert record.access_count == initial_count + 1


class TestEvictionStrategies:
    """Tests for eviction strategy classes."""

    def test_lru_strategy(self):
        strategy = LRUEvictionStrategy()
        records = {
            "key1": NearCacheRecord(value="v1"),
            "key2": NearCacheRecord(value="v2"),
        }
        records["key1"].last_access_time = time.time() - 100
        records["key2"].last_access_time = time.time()
        key = strategy.select_for_eviction(records)
        assert key == "key1"

    def test_lru_strategy_empty(self):
        strategy = LRUEvictionStrategy()
        assert strategy.select_for_eviction({}) is None

    def test_lfu_strategy(self):
        strategy = LFUEvictionStrategy()
        records = {
            "key1": NearCacheRecord(value="v1"),
            "key2": NearCacheRecord(value="v2"),
        }
        records["key1"].access_count = 10
        records["key2"].access_count = 1
        key = strategy.select_for_eviction(records)
        assert key == "key2"

    def test_lfu_strategy_empty(self):
        strategy = LFUEvictionStrategy()
        assert strategy.select_for_eviction({}) is None

    def test_random_strategy(self):
        strategy = RandomEvictionStrategy()
        records = {
            "key1": NearCacheRecord(value="v1"),
            "key2": NearCacheRecord(value="v2"),
        }
        key = strategy.select_for_eviction(records)
        assert key in ["key1", "key2"]

    def test_random_strategy_empty(self):
        strategy = RandomEvictionStrategy()
        assert strategy.select_for_eviction({}) is None

    def test_none_strategy(self):
        strategy = NoneEvictionStrategy()
        records = {
            "key1": NearCacheRecord(value="v1"),
        }
        assert strategy.select_for_eviction(records) is None

    def test_create_eviction_strategy(self):
        assert isinstance(_create_eviction_strategy(EvictionPolicy.LRU), LRUEvictionStrategy)
        assert isinstance(_create_eviction_strategy(EvictionPolicy.LFU), LFUEvictionStrategy)
        assert isinstance(_create_eviction_strategy(EvictionPolicy.RANDOM), RandomEvictionStrategy)
        assert isinstance(_create_eviction_strategy(EvictionPolicy.NONE), NoneEvictionStrategy)


class TestNearCache:
    """Tests for NearCache class."""

    def test_init(self, near_cache_config):
        cache = NearCache(near_cache_config)
        assert cache.name == near_cache_config.name
        assert cache.size == 0

    def test_put_and_get(self, near_cache_config):
        cache = NearCache(near_cache_config)
        cache.put("key1", "value1")
        assert cache.get("key1") == "value1"
        assert cache.size == 1

    def test_get_miss(self, near_cache_config):
        cache = NearCache(near_cache_config)
        assert cache.get("nonexistent") is None
        assert cache.stats.misses == 1

    def test_get_hit(self, near_cache_config):
        cache = NearCache(near_cache_config)
        cache.put("key", "value")
        cache.get("key")
        assert cache.stats.hits == 1

    def test_remove(self, near_cache_config):
        cache = NearCache(near_cache_config)
        cache.put("key", "value")
        result = cache.remove("key")
        assert result == "value"
        assert cache.size == 0

    def test_remove_nonexistent(self, near_cache_config):
        cache = NearCache(near_cache_config)
        result = cache.remove("nonexistent")
        assert result is None

    def test_invalidate(self, near_cache_config):
        cache = NearCache(near_cache_config)
        cache.put("key", "value")
        cache.invalidate("key")
        assert cache.get("key") is None
        assert cache.stats.invalidations == 1

    def test_invalidate_all(self, near_cache_config):
        cache = NearCache(near_cache_config)
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        cache.invalidate_all()
        assert cache.size == 0
        assert cache.stats.invalidations == 2

    def test_clear(self, near_cache_config):
        cache = NearCache(near_cache_config)
        cache.put("key", "value")
        cache.clear()
        assert cache.size == 0

    def test_contains(self, near_cache_config):
        cache = NearCache(near_cache_config)
        cache.put("key", "value")
        assert cache.contains("key") is True
        assert cache.contains("nonexistent") is False

    def test_eviction(self):
        config = NearCacheConfig(
            name="test",
            max_size=2,
            eviction_policy=EvictionPolicy.LRU,
        )
        cache = NearCache(config)
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        cache.put("key3", "value3")
        assert cache.size <= 2
        assert cache.stats.evictions >= 1

    def test_expiration_ttl(self):
        config = NearCacheConfig(
            name="test",
            max_size=100,
            time_to_live_seconds=1,
        )
        cache = NearCache(config)
        cache.put("key", "value")
        cache._records["key"].creation_time = time.time() - 2
        assert cache.get("key") is None
        assert cache.stats.expirations == 1

    def test_expiration_max_idle(self):
        config = NearCacheConfig(
            name="test",
            max_size=100,
            max_idle_seconds=1,
        )
        cache = NearCache(config)
        cache.put("key", "value")
        cache._records["key"].last_access_time = time.time() - 2
        assert cache.get("key") is None

    def test_do_expiration(self):
        config = NearCacheConfig(
            name="test",
            max_size=100,
            time_to_live_seconds=1,
        )
        cache = NearCache(config)
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        for record in cache._records.values():
            record.creation_time = time.time() - 2
        expired = cache.do_expiration()
        assert expired == 2
        assert cache.size == 0

    def test_add_invalidation_listener(self, near_cache_config):
        cache = NearCache(near_cache_config)
        invalidated = []
        reg_id = cache.add_invalidation_listener(lambda k: invalidated.append(k))
        cache.put("key", "value")
        cache.invalidate("key")
        assert "key" in invalidated
        assert cache.remove_invalidation_listener(reg_id) is True

    def test_remove_invalidation_listener_nonexistent(self, near_cache_config):
        cache = NearCache(near_cache_config)
        assert cache.remove_invalidation_listener("nonexistent") is False

    def test_stats_property(self, near_cache_config):
        cache = NearCache(near_cache_config)
        cache.put("key", "value")
        stats = cache.stats
        assert stats.entries_count == 1


class TestNearCacheManager:
    """Tests for NearCacheManager class."""

    def test_get_or_create(self, near_cache_config):
        manager = NearCacheManager()
        cache1 = manager.get_or_create("map1", near_cache_config)
        cache2 = manager.get_or_create("map1", near_cache_config)
        assert cache1 is cache2

    def test_get_nonexistent(self):
        manager = NearCacheManager()
        assert manager.get("nonexistent") is None

    def test_get_existing(self, near_cache_config):
        manager = NearCacheManager()
        created = manager.get_or_create("map1", near_cache_config)
        found = manager.get("map1")
        assert found is created

    def test_destroy(self, near_cache_config):
        manager = NearCacheManager()
        manager.get_or_create("map1", near_cache_config)
        manager.destroy("map1")
        assert manager.get("map1") is None

    def test_destroy_all(self, near_cache_config):
        manager = NearCacheManager()
        manager.get_or_create("map1", near_cache_config)
        manager.get_or_create("map2", near_cache_config)
        manager.destroy_all()
        assert manager.get("map1") is None
        assert manager.get("map2") is None

    def test_list_all(self, near_cache_config):
        manager = NearCacheManager()
        manager.get_or_create("map1", near_cache_config)
        manager.get_or_create("map2", near_cache_config)
        all_stats = manager.list_all()
        assert "map1" in all_stats
        assert "map2" in all_stats
