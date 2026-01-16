"""Unit tests for hazelcast/near_cache.py"""

import time
import unittest
from unittest.mock import MagicMock, patch

from hazelcast.near_cache import (
    NearCacheStats,
    NearCacheRecord,
    EvictionStrategy,
    LRUEvictionStrategy,
    LFUEvictionStrategy,
    RandomEvictionStrategy,
    NoneEvictionStrategy,
    _create_eviction_strategy,
    NearCache,
    NearCacheManager,
)
from hazelcast.config import EvictionPolicy, InMemoryFormat


def create_mock_config(
    name="test-cache",
    max_size=100,
    time_to_live_seconds=0,
    max_idle_seconds=0,
    eviction_policy=EvictionPolicy.LRU,
    in_memory_format=InMemoryFormat.OBJECT,
):
    config = MagicMock()
    config.name = name
    config.max_size = max_size
    config.time_to_live_seconds = time_to_live_seconds
    config.max_idle_seconds = max_idle_seconds
    config.eviction_policy = eviction_policy
    config.in_memory_format = in_memory_format
    return config


class TestNearCacheStats(unittest.TestCase):
    def test_default_values(self):
        stats = NearCacheStats()
        self.assertEqual(stats.hits, 0)
        self.assertEqual(stats.misses, 0)
        self.assertEqual(stats.evictions, 0)
        self.assertEqual(stats.expirations, 0)
        self.assertEqual(stats.invalidations, 0)
        self.assertEqual(stats.entries_count, 0)
        self.assertEqual(stats.owned_entry_memory_cost, 0)

    def test_hit_ratio_zero_total(self):
        stats = NearCacheStats()
        self.assertEqual(stats.hit_ratio, 0.0)

    def test_hit_ratio_with_data(self):
        stats = NearCacheStats(hits=75, misses=25)
        self.assertEqual(stats.hit_ratio, 0.75)

    def test_miss_ratio_zero_total(self):
        stats = NearCacheStats()
        self.assertEqual(stats.miss_ratio, 0.0)

    def test_miss_ratio_with_data(self):
        stats = NearCacheStats(hits=60, misses=40)
        self.assertEqual(stats.miss_ratio, 0.40)

    def test_to_dict(self):
        stats = NearCacheStats(hits=10, misses=5, evictions=2)
        result = stats.to_dict()
        self.assertEqual(result["hits"], 10)
        self.assertEqual(result["misses"], 5)
        self.assertEqual(result["evictions"], 2)
        self.assertIn("hit_ratio", result)
        self.assertIn("miss_ratio", result)
        self.assertIn("creation_time", result)


class TestNearCacheRecord(unittest.TestCase):
    def test_init(self):
        record = NearCacheRecord(value="test")
        self.assertEqual(record.value, "test")
        self.assertEqual(record.access_count, 0)
        self.assertEqual(record.ttl_seconds, 0)
        self.assertEqual(record.max_idle_seconds, 0)

    def test_is_expired_no_ttl(self):
        record = NearCacheRecord(value="test")
        self.assertFalse(record.is_expired())

    def test_is_expired_ttl_not_exceeded(self):
        record = NearCacheRecord(value="test", ttl_seconds=60)
        self.assertFalse(record.is_expired())

    def test_is_expired_ttl_exceeded(self):
        record = NearCacheRecord(value="test", ttl_seconds=0)
        record.ttl_seconds = 1
        record.creation_time = time.time() - 2
        self.assertTrue(record.is_expired())

    def test_is_expired_max_idle_not_exceeded(self):
        record = NearCacheRecord(value="test", max_idle_seconds=60)
        self.assertFalse(record.is_expired())

    def test_is_expired_max_idle_exceeded(self):
        record = NearCacheRecord(value="test", max_idle_seconds=1)
        record.last_access_time = time.time() - 2
        self.assertTrue(record.is_expired())

    def test_record_access(self):
        record = NearCacheRecord(value="test")
        old_time = record.last_access_time
        time.sleep(0.01)
        record.record_access()
        self.assertEqual(record.access_count, 1)
        self.assertGreater(record.last_access_time, old_time)


class TestEvictionStrategyABC(unittest.TestCase):
    def test_is_abstract(self):
        from abc import ABC
        self.assertTrue(issubclass(EvictionStrategy, ABC))

    def test_cannot_instantiate(self):
        with self.assertRaises(TypeError):
            EvictionStrategy()


class TestLRUEvictionStrategy(unittest.TestCase):
    def test_select_for_eviction_empty(self):
        strategy = LRUEvictionStrategy()
        result = strategy.select_for_eviction({})
        self.assertIsNone(result)

    def test_select_for_eviction(self):
        strategy = LRUEvictionStrategy()
        old_record = NearCacheRecord(value="old")
        old_record.last_access_time = time.time() - 100
        new_record = NearCacheRecord(value="new")
        records = {"old_key": old_record, "new_key": new_record}
        result = strategy.select_for_eviction(records)
        self.assertEqual(result, "old_key")


class TestLFUEvictionStrategy(unittest.TestCase):
    def test_select_for_eviction_empty(self):
        strategy = LFUEvictionStrategy()
        result = strategy.select_for_eviction({})
        self.assertIsNone(result)

    def test_select_for_eviction(self):
        strategy = LFUEvictionStrategy()
        low_record = NearCacheRecord(value="low")
        low_record.access_count = 1
        high_record = NearCacheRecord(value="high")
        high_record.access_count = 100
        records = {"low": low_record, "high": high_record}
        result = strategy.select_for_eviction(records)
        self.assertEqual(result, "low")


class TestRandomEvictionStrategy(unittest.TestCase):
    def test_select_for_eviction_empty(self):
        strategy = RandomEvictionStrategy()
        result = strategy.select_for_eviction({})
        self.assertIsNone(result)

    def test_select_for_eviction(self):
        strategy = RandomEvictionStrategy()
        records = {
            "a": NearCacheRecord(value="a"),
            "b": NearCacheRecord(value="b"),
        }
        with patch("random.choice", return_value="a"):
            result = strategy.select_for_eviction(records)
            self.assertEqual(result, "a")


class TestNoneEvictionStrategy(unittest.TestCase):
    def test_select_for_eviction_always_none(self):
        strategy = NoneEvictionStrategy()
        records = {"a": NearCacheRecord(value="a")}
        result = strategy.select_for_eviction(records)
        self.assertIsNone(result)

    def test_select_for_eviction_empty(self):
        strategy = NoneEvictionStrategy()
        result = strategy.select_for_eviction({})
        self.assertIsNone(result)


class TestCreateEvictionStrategy(unittest.TestCase):
    def test_lru(self):
        strategy = _create_eviction_strategy(EvictionPolicy.LRU)
        self.assertIsInstance(strategy, LRUEvictionStrategy)

    def test_lfu(self):
        strategy = _create_eviction_strategy(EvictionPolicy.LFU)
        self.assertIsInstance(strategy, LFUEvictionStrategy)

    def test_random(self):
        strategy = _create_eviction_strategy(EvictionPolicy.RANDOM)
        self.assertIsInstance(strategy, RandomEvictionStrategy)

    def test_none(self):
        strategy = _create_eviction_strategy(EvictionPolicy.NONE)
        self.assertIsInstance(strategy, NoneEvictionStrategy)


class TestNearCache(unittest.TestCase):
    def test_init(self):
        config = create_mock_config()
        cache = NearCache(config)
        self.assertEqual(cache.name, "test-cache")
        self.assertEqual(cache.config, config)
        self.assertEqual(cache.size, 0)

    def test_name_property(self):
        config = create_mock_config(name="my-cache")
        cache = NearCache(config)
        self.assertEqual(cache.name, "my-cache")

    def test_config_property(self):
        config = create_mock_config()
        cache = NearCache(config)
        self.assertIs(cache.config, config)

    def test_stats_property(self):
        config = create_mock_config()
        cache = NearCache(config)
        stats = cache.stats
        self.assertIsInstance(stats, NearCacheStats)

    def test_size_property(self):
        config = create_mock_config()
        cache = NearCache(config)
        self.assertEqual(cache.size, 0)
        cache.put("key", "value")
        self.assertEqual(cache.size, 1)

    def test_get_miss(self):
        config = create_mock_config()
        cache = NearCache(config)
        result = cache.get("nonexistent")
        self.assertIsNone(result)
        self.assertEqual(cache.stats.misses, 1)

    def test_get_hit(self):
        config = create_mock_config()
        cache = NearCache(config)
        cache.put("key", "value")
        result = cache.get("key")
        self.assertEqual(result, "value")
        self.assertEqual(cache.stats.hits, 1)

    def test_get_expired_entry(self):
        config = create_mock_config(time_to_live_seconds=1)
        cache = NearCache(config)
        cache.put("key", "value")
        cache._records["key"].creation_time = time.time() - 2
        result = cache.get("key")
        self.assertIsNone(result)
        self.assertEqual(cache.stats.misses, 1)
        self.assertEqual(cache.stats.expirations, 1)

    def test_put(self):
        config = create_mock_config()
        cache = NearCache(config)
        cache.put("key", "value")
        self.assertEqual(cache.size, 1)
        self.assertEqual(cache.get("key"), "value")

    def test_put_with_ttl(self):
        config = create_mock_config(time_to_live_seconds=60)
        cache = NearCache(config)
        cache.put("key", "value")
        record = cache._records["key"]
        self.assertEqual(record.ttl_seconds, 60)

    def test_put_triggers_eviction(self):
        config = create_mock_config(max_size=2)
        cache = NearCache(config)
        cache.put("a", "1")
        cache.put("b", "2")
        cache.put("c", "3")
        self.assertEqual(cache.size, 2)
        self.assertEqual(cache.stats.evictions, 1)

    def test_remove(self):
        config = create_mock_config()
        cache = NearCache(config)
        cache.put("key", "value")
        result = cache.remove("key")
        self.assertEqual(result, "value")
        self.assertEqual(cache.size, 0)

    def test_remove_not_found(self):
        config = create_mock_config()
        cache = NearCache(config)
        result = cache.remove("nonexistent")
        self.assertIsNone(result)

    def test_invalidate(self):
        config = create_mock_config()
        cache = NearCache(config)
        cache.put("key", "value")
        cache.invalidate("key")
        self.assertEqual(cache.size, 0)
        self.assertEqual(cache.stats.invalidations, 1)

    def test_invalidate_not_found(self):
        config = create_mock_config()
        cache = NearCache(config)
        cache.invalidate("nonexistent")
        self.assertEqual(cache.stats.invalidations, 0)

    def test_invalidate_notifies_listeners(self):
        config = create_mock_config()
        cache = NearCache(config)
        cache.put("key", "value")
        listener = MagicMock()
        cache.add_invalidation_listener(listener)
        cache.invalidate("key")
        listener.assert_called_once_with("key")

    def test_invalidate_all(self):
        config = create_mock_config()
        cache = NearCache(config)
        cache.put("a", "1")
        cache.put("b", "2")
        cache.invalidate_all()
        self.assertEqual(cache.size, 0)
        self.assertEqual(cache.stats.invalidations, 2)

    def test_clear(self):
        config = create_mock_config()
        cache = NearCache(config)
        cache.put("a", "1")
        cache.put("b", "2")
        cache.clear()
        self.assertEqual(cache.size, 0)

    def test_contains_true(self):
        config = create_mock_config()
        cache = NearCache(config)
        cache.put("key", "value")
        self.assertTrue(cache.contains("key"))

    def test_contains_false(self):
        config = create_mock_config()
        cache = NearCache(config)
        self.assertFalse(cache.contains("nonexistent"))

    def test_contains_expired(self):
        config = create_mock_config(time_to_live_seconds=1)
        cache = NearCache(config)
        cache.put("key", "value")
        cache._records["key"].creation_time = time.time() - 2
        self.assertFalse(cache.contains("key"))
        self.assertEqual(cache.stats.expirations, 1)

    def test_add_invalidation_listener(self):
        config = create_mock_config()
        cache = NearCache(config)
        listener = MagicMock()
        reg_id = cache.add_invalidation_listener(listener)
        self.assertIsInstance(reg_id, str)

    def test_remove_invalidation_listener(self):
        config = create_mock_config()
        cache = NearCache(config)
        listener = MagicMock()
        reg_id = cache.add_invalidation_listener(listener)
        result = cache.remove_invalidation_listener(reg_id)
        self.assertTrue(result)

    def test_remove_invalidation_listener_not_found(self):
        config = create_mock_config()
        cache = NearCache(config)
        result = cache.remove_invalidation_listener("unknown")
        self.assertFalse(result)

    def test_do_expiration(self):
        config = create_mock_config(time_to_live_seconds=1)
        cache = NearCache(config)
        cache.put("a", "1")
        cache.put("b", "2")
        cache._records["a"].creation_time = time.time() - 2
        count = cache.do_expiration()
        self.assertEqual(count, 1)
        self.assertEqual(cache.size, 1)
        self.assertEqual(cache.stats.expirations, 1)

    def test_invalidate_batch(self):
        config = create_mock_config()
        cache = NearCache(config)
        cache.put("a", "1")
        cache.put("b", "2")
        cache.put("c", "3")
        count = cache.invalidate_batch(["a", "b", "d"])
        self.assertEqual(count, 2)
        self.assertEqual(cache.size, 1)
        self.assertEqual(cache.stats.invalidations, 2)

    def test_get_stats_snapshot(self):
        config = create_mock_config()
        cache = NearCache(config)
        cache.put("key", "value")
        cache.get("key")
        snapshot = cache.get_stats_snapshot()
        self.assertEqual(snapshot.hits, 1)
        self.assertEqual(snapshot.entries_count, 1)


class TestNearCacheWithSerialization(unittest.TestCase):
    def test_binary_format_serialization(self):
        config = create_mock_config(in_memory_format=InMemoryFormat.BINARY)
        mock_ser = MagicMock()
        mock_ser.to_data.return_value = b"serialized"
        mock_ser.to_object.return_value = "deserialized"
        cache = NearCache(config, mock_ser)
        cache.put("key", "value")
        mock_ser.to_data.assert_called_once_with("value")

    def test_binary_format_deserialization(self):
        config = create_mock_config(in_memory_format=InMemoryFormat.BINARY)
        mock_ser = MagicMock()
        mock_ser.to_data.return_value = b"serialized"
        mock_ser.to_object.return_value = "deserialized"
        cache = NearCache(config, mock_ser)
        cache.put("key", "value")
        result = cache.get("key")
        self.assertEqual(result, "deserialized")


class TestNearCacheManager(unittest.TestCase):
    def test_init(self):
        manager = NearCacheManager()
        self.assertEqual(len(manager._caches), 0)

    def test_init_with_serialization_service(self):
        mock_ser = MagicMock()
        manager = NearCacheManager(mock_ser)
        self.assertEqual(manager._serialization_service, mock_ser)

    def test_get_or_create_creates_new(self):
        manager = NearCacheManager()
        config = create_mock_config(name="test")
        cache = manager.get_or_create("test", config)
        self.assertIsNotNone(cache)
        self.assertEqual(cache.name, "test")

    def test_get_or_create_returns_existing(self):
        manager = NearCacheManager()
        config = create_mock_config(name="test")
        cache1 = manager.get_or_create("test", config)
        cache2 = manager.get_or_create("test", config)
        self.assertIs(cache1, cache2)

    def test_get_found(self):
        manager = NearCacheManager()
        config = create_mock_config(name="test")
        manager.get_or_create("test", config)
        cache = manager.get("test")
        self.assertIsNotNone(cache)

    def test_get_not_found(self):
        manager = NearCacheManager()
        result = manager.get("nonexistent")
        self.assertIsNone(result)

    def test_destroy(self):
        manager = NearCacheManager()
        config = create_mock_config(name="test")
        cache = manager.get_or_create("test", config)
        cache.put("key", "value")
        manager.destroy("test")
        self.assertIsNone(manager.get("test"))

    def test_destroy_not_found(self):
        manager = NearCacheManager()
        manager.destroy("nonexistent")

    def test_destroy_all(self):
        manager = NearCacheManager()
        config1 = create_mock_config(name="cache1")
        config2 = create_mock_config(name="cache2")
        manager.get_or_create("cache1", config1)
        manager.get_or_create("cache2", config2)
        manager.destroy_all()
        self.assertIsNone(manager.get("cache1"))
        self.assertIsNone(manager.get("cache2"))

    def test_list_all(self):
        manager = NearCacheManager()
        config1 = create_mock_config(name="cache1")
        config2 = create_mock_config(name="cache2")
        manager.get_or_create("cache1", config1)
        manager.get_or_create("cache2", config2)
        result = manager.list_all()
        self.assertIn("cache1", result)
        self.assertIn("cache2", result)
        self.assertIsInstance(result["cache1"], NearCacheStats)


if __name__ == "__main__":
    unittest.main()
