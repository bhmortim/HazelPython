"""Tests for Near Cache implementation."""

import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from hazelcast.config import EvictionPolicy, InMemoryFormat, NearCacheConfig
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


class TestNearCacheRecord(unittest.TestCase):
    """Tests for NearCacheRecord."""

    def test_record_not_expired_without_ttl(self):
        record = NearCacheRecord(value="test", ttl_seconds=0, max_idle_seconds=0)
        self.assertFalse(record.is_expired())

    def test_record_expired_by_ttl(self):
        record = NearCacheRecord(value="test", ttl_seconds=1, max_idle_seconds=0)
        record.creation_time = time.time() - 2
        self.assertTrue(record.is_expired())

    def test_record_not_expired_within_ttl(self):
        record = NearCacheRecord(value="test", ttl_seconds=60, max_idle_seconds=0)
        self.assertFalse(record.is_expired())

    def test_record_expired_by_max_idle(self):
        record = NearCacheRecord(value="test", ttl_seconds=0, max_idle_seconds=1)
        record.last_access_time = time.time() - 2
        self.assertTrue(record.is_expired())

    def test_record_not_expired_within_max_idle(self):
        record = NearCacheRecord(value="test", ttl_seconds=0, max_idle_seconds=60)
        self.assertFalse(record.is_expired())

    def test_record_access(self):
        record = NearCacheRecord(value="test")
        old_time = record.last_access_time
        old_count = record.access_count
        time.sleep(0.01)
        record.record_access()
        self.assertGreater(record.last_access_time, old_time)
        self.assertEqual(record.access_count, old_count + 1)


class TestEvictionStrategies(unittest.TestCase):
    """Tests for eviction strategies."""

    def test_lru_eviction_selects_oldest_access(self):
        strategy = LRUEvictionStrategy()
        records = {
            "key1": NearCacheRecord(value="v1"),
            "key2": NearCacheRecord(value="v2"),
            "key3": NearCacheRecord(value="v3"),
        }
        records["key1"].last_access_time = 100
        records["key2"].last_access_time = 50
        records["key3"].last_access_time = 150

        selected = strategy.select_for_eviction(records)
        self.assertEqual(selected, "key2")

    def test_lru_eviction_empty_records(self):
        strategy = LRUEvictionStrategy()
        self.assertIsNone(strategy.select_for_eviction({}))

    def test_lfu_eviction_selects_least_accessed(self):
        strategy = LFUEvictionStrategy()
        records = {
            "key1": NearCacheRecord(value="v1"),
            "key2": NearCacheRecord(value="v2"),
            "key3": NearCacheRecord(value="v3"),
        }
        records["key1"].access_count = 10
        records["key2"].access_count = 2
        records["key3"].access_count = 5

        selected = strategy.select_for_eviction(records)
        self.assertEqual(selected, "key2")

    def test_lfu_eviction_empty_records(self):
        strategy = LFUEvictionStrategy()
        self.assertIsNone(strategy.select_for_eviction({}))

    def test_random_eviction_selects_something(self):
        strategy = RandomEvictionStrategy()
        records = {
            "key1": NearCacheRecord(value="v1"),
            "key2": NearCacheRecord(value="v2"),
        }
        selected = strategy.select_for_eviction(records)
        self.assertIn(selected, ["key1", "key2"])

    def test_random_eviction_empty_records(self):
        strategy = RandomEvictionStrategy()
        self.assertIsNone(strategy.select_for_eviction({}))

    def test_none_eviction_returns_none(self):
        strategy = NoneEvictionStrategy()
        records = {"key1": NearCacheRecord(value="v1")}
        self.assertIsNone(strategy.select_for_eviction(records))

    def test_create_eviction_strategy_lru(self):
        strategy = _create_eviction_strategy(EvictionPolicy.LRU)
        self.assertIsInstance(strategy, LRUEvictionStrategy)

    def test_create_eviction_strategy_lfu(self):
        strategy = _create_eviction_strategy(EvictionPolicy.LFU)
        self.assertIsInstance(strategy, LFUEvictionStrategy)

    def test_create_eviction_strategy_random(self):
        strategy = _create_eviction_strategy(EvictionPolicy.RANDOM)
        self.assertIsInstance(strategy, RandomEvictionStrategy)

    def test_create_eviction_strategy_none(self):
        strategy = _create_eviction_strategy(EvictionPolicy.NONE)
        self.assertIsInstance(strategy, NoneEvictionStrategy)


class TestNearCacheStats(unittest.TestCase):
    """Tests for NearCacheStats."""

    def test_hit_ratio_zero_when_no_access(self):
        stats = NearCacheStats()
        self.assertEqual(stats.hit_ratio, 0.0)

    def test_hit_ratio_calculation(self):
        stats = NearCacheStats(hits=75, misses=25)
        self.assertEqual(stats.hit_ratio, 0.75)

    def test_miss_ratio_calculation(self):
        stats = NearCacheStats(hits=75, misses=25)
        self.assertEqual(stats.miss_ratio, 0.25)

    def test_to_dict(self):
        stats = NearCacheStats(hits=10, misses=5, evictions=2)
        d = stats.to_dict()
        self.assertEqual(d["hits"], 10)
        self.assertEqual(d["misses"], 5)
        self.assertEqual(d["evictions"], 2)
        self.assertIn("hit_ratio", d)
        self.assertIn("miss_ratio", d)


class TestNearCache(unittest.TestCase):
    """Tests for NearCache core functionality."""

    def setUp(self):
        self.config = NearCacheConfig(
            name="test-cache",
            max_size=100,
            eviction_policy=EvictionPolicy.LRU,
        )
        self.cache = NearCache(self.config)

    def test_put_and_get(self):
        self.cache.put("key1", "value1")
        result = self.cache.get("key1")
        self.assertEqual(result, "value1")

    def test_get_nonexistent_key(self):
        result = self.cache.get("nonexistent")
        self.assertIsNone(result)

    def test_get_updates_stats_hit(self):
        self.cache.put("key1", "value1")
        self.cache.get("key1")
        self.assertEqual(self.cache.stats.hits, 1)
        self.assertEqual(self.cache.stats.misses, 0)

    def test_get_updates_stats_miss(self):
        self.cache.get("nonexistent")
        self.assertEqual(self.cache.stats.hits, 0)
        self.assertEqual(self.cache.stats.misses, 1)

    def test_remove(self):
        self.cache.put("key1", "value1")
        removed = self.cache.remove("key1")
        self.assertEqual(removed, "value1")
        self.assertIsNone(self.cache.get("key1"))

    def test_remove_nonexistent(self):
        removed = self.cache.remove("nonexistent")
        self.assertIsNone(removed)

    def test_contains(self):
        self.cache.put("key1", "value1")
        self.assertTrue(self.cache.contains("key1"))
        self.assertFalse(self.cache.contains("nonexistent"))

    def test_size(self):
        self.assertEqual(self.cache.size, 0)
        self.cache.put("key1", "value1")
        self.assertEqual(self.cache.size, 1)
        self.cache.put("key2", "value2")
        self.assertEqual(self.cache.size, 2)

    def test_clear(self):
        self.cache.put("key1", "value1")
        self.cache.put("key2", "value2")
        self.cache.clear()
        self.assertEqual(self.cache.size, 0)

    def test_invalidate(self):
        self.cache.put("key1", "value1")
        self.cache.invalidate("key1")
        self.assertIsNone(self.cache.get("key1"))
        self.assertEqual(self.cache.stats.invalidations, 1)

    def test_invalidate_nonexistent(self):
        self.cache.invalidate("nonexistent")
        self.assertEqual(self.cache.stats.invalidations, 0)

    def test_invalidate_all(self):
        self.cache.put("key1", "value1")
        self.cache.put("key2", "value2")
        self.cache.invalidate_all()
        self.assertEqual(self.cache.size, 0)
        self.assertEqual(self.cache.stats.invalidations, 2)

    def test_invalidate_batch(self):
        self.cache.put("key1", "value1")
        self.cache.put("key2", "value2")
        self.cache.put("key3", "value3")
        count = self.cache.invalidate_batch(["key1", "key3", "nonexistent"])
        self.assertEqual(count, 2)
        self.assertEqual(self.cache.size, 1)
        self.assertIsNotNone(self.cache.get("key2"))

    def test_name_property(self):
        self.assertEqual(self.cache.name, "test-cache")

    def test_config_property(self):
        self.assertEqual(self.cache.config, self.config)


class TestNearCacheEviction(unittest.TestCase):
    """Tests for Near Cache eviction behavior."""

    def test_lru_eviction_when_full(self):
        config = NearCacheConfig(
            name="test",
            max_size=3,
            eviction_policy=EvictionPolicy.LRU,
        )
        cache = NearCache(config)

        cache.put("key1", "value1")
        time.sleep(0.01)
        cache.put("key2", "value2")
        time.sleep(0.01)
        cache.put("key3", "value3")

        cache.get("key1")
        cache.get("key2")

        cache.put("key4", "value4")

        self.assertEqual(cache.size, 3)
        self.assertIsNone(cache.get("key3"))
        self.assertIsNotNone(cache.get("key1"))
        self.assertIsNotNone(cache.get("key2"))

    def test_lfu_eviction_when_full(self):
        config = NearCacheConfig(
            name="test",
            max_size=3,
            eviction_policy=EvictionPolicy.LFU,
        )
        cache = NearCache(config)

        cache.put("key1", "value1")
        cache.put("key2", "value2")
        cache.put("key3", "value3")

        for _ in range(5):
            cache.get("key1")
        for _ in range(3):
            cache.get("key2")
        cache.get("key3")

        cache.put("key4", "value4")

        self.assertEqual(cache.size, 3)
        self.assertIsNone(cache.get("key3"))

    def test_eviction_updates_stats(self):
        config = NearCacheConfig(
            name="test",
            max_size=2,
            eviction_policy=EvictionPolicy.LRU,
        )
        cache = NearCache(config)

        cache.put("key1", "value1")
        cache.put("key2", "value2")
        cache.put("key3", "value3")

        self.assertEqual(cache.stats.evictions, 1)


class TestNearCacheExpiration(unittest.TestCase):
    """Tests for Near Cache expiration behavior."""

    def test_ttl_expiration(self):
        config = NearCacheConfig(
            name="test",
            max_size=100,
            time_to_live_seconds=1,
        )
        cache = NearCache(config)

        cache.put("key1", "value1")
        self.assertIsNotNone(cache.get("key1"))

        time.sleep(1.1)
        self.assertIsNone(cache.get("key1"))
        self.assertEqual(cache.stats.expirations, 1)

    def test_max_idle_expiration(self):
        config = NearCacheConfig(
            name="test",
            max_size=100,
            max_idle_seconds=1,
        )
        cache = NearCache(config)

        cache.put("key1", "value1")
        self.assertIsNotNone(cache.get("key1"))

        time.sleep(1.1)
        self.assertIsNone(cache.get("key1"))
        self.assertEqual(cache.stats.expirations, 1)

    def test_max_idle_reset_on_access(self):
        config = NearCacheConfig(
            name="test",
            max_size=100,
            max_idle_seconds=1,
        )
        cache = NearCache(config)

        cache.put("key1", "value1")
        time.sleep(0.5)
        self.assertIsNotNone(cache.get("key1"))
        time.sleep(0.5)
        self.assertIsNotNone(cache.get("key1"))
        time.sleep(0.5)
        self.assertIsNotNone(cache.get("key1"))

    def test_do_expiration(self):
        config = NearCacheConfig(
            name="test",
            max_size=100,
            time_to_live_seconds=1,
        )
        cache = NearCache(config)

        cache.put("key1", "value1")
        cache.put("key2", "value2")

        time.sleep(1.1)

        expired_count = cache.do_expiration()
        self.assertEqual(expired_count, 2)
        self.assertEqual(cache.size, 0)

    def test_contains_checks_expiration(self):
        config = NearCacheConfig(
            name="test",
            max_size=100,
            time_to_live_seconds=1,
        )
        cache = NearCache(config)

        cache.put("key1", "value1")
        self.assertTrue(cache.contains("key1"))

        time.sleep(1.1)
        self.assertFalse(cache.contains("key1"))


class TestNearCacheInvalidationListeners(unittest.TestCase):
    """Tests for invalidation listener functionality."""

    def test_add_invalidation_listener(self):
        config = NearCacheConfig(name="test", max_size=100)
        cache = NearCache(config)

        events = []
        def listener(key):
            events.append(key)

        reg_id = cache.add_invalidation_listener(listener)
        self.assertIsNotNone(reg_id)

        cache.put("key1", "value1")
        cache.invalidate("key1")

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0], "key1")

    def test_remove_invalidation_listener(self):
        config = NearCacheConfig(name="test", max_size=100)
        cache = NearCache(config)

        events = []
        def listener(key):
            events.append(key)

        reg_id = cache.add_invalidation_listener(listener)
        cache.put("key1", "value1")
        cache.invalidate("key1")
        self.assertEqual(len(events), 1)

        result = cache.remove_invalidation_listener(reg_id)
        self.assertTrue(result)

        cache.put("key2", "value2")
        cache.invalidate("key2")
        self.assertEqual(len(events), 1)

    def test_remove_nonexistent_listener(self):
        config = NearCacheConfig(name="test", max_size=100)
        cache = NearCache(config)

        result = cache.remove_invalidation_listener("nonexistent-id")
        self.assertFalse(result)


class TestNearCacheInMemoryFormat(unittest.TestCase):
    """Tests for in-memory format handling."""

    def test_object_format(self):
        config = NearCacheConfig(
            name="test",
            max_size=100,
            in_memory_format=InMemoryFormat.OBJECT,
        )
        cache = NearCache(config)

        obj = {"name": "test", "value": 123}
        cache.put("key1", obj)
        result = cache.get("key1")
        self.assertEqual(result, obj)

    def test_binary_format_with_serialization_service(self):
        config = NearCacheConfig(
            name="test",
            max_size=100,
            in_memory_format=InMemoryFormat.BINARY,
        )

        mock_service = MagicMock()
        mock_service.to_data.return_value = b"serialized"
        mock_service.to_object.return_value = "deserialized"

        cache = NearCache(config, serialization_service=mock_service)

        cache.put("key1", "value1")
        mock_service.to_data.assert_called_with("value1")

        result = cache.get("key1")
        mock_service.to_object.assert_called_with(b"serialized")
        self.assertEqual(result, "deserialized")


class TestNearCacheThreadSafety(unittest.TestCase):
    """Tests for thread-safety of Near Cache operations."""

    def test_concurrent_put_get(self):
        config = NearCacheConfig(name="test", max_size=1000)
        cache = NearCache(config)

        errors = []

        def worker(thread_id):
            try:
                for i in range(100):
                    key = f"key-{thread_id}-{i}"
                    cache.put(key, f"value-{thread_id}-{i}")
                    result = cache.get(key)
                    if result is None:
                        pass
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0)

    def test_concurrent_eviction(self):
        config = NearCacheConfig(
            name="test",
            max_size=50,
            eviction_policy=EvictionPolicy.LRU,
        )
        cache = NearCache(config)

        errors = []

        def worker(thread_id):
            try:
                for i in range(100):
                    cache.put(f"key-{thread_id}-{i}", f"value-{thread_id}-{i}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0)
        self.assertLessEqual(cache.size, 50)


class TestNearCacheManager(unittest.TestCase):
    """Tests for NearCacheManager."""

    def test_get_or_create(self):
        manager = NearCacheManager()
        config = NearCacheConfig(name="map1", max_size=100)

        cache1 = manager.get_or_create("map1", config)
        cache2 = manager.get_or_create("map1", config)

        self.assertIs(cache1, cache2)

    def test_get_existing(self):
        manager = NearCacheManager()
        config = NearCacheConfig(name="map1", max_size=100)

        manager.get_or_create("map1", config)
        cache = manager.get("map1")

        self.assertIsNotNone(cache)

    def test_get_nonexistent(self):
        manager = NearCacheManager()
        cache = manager.get("nonexistent")
        self.assertIsNone(cache)

    def test_destroy(self):
        manager = NearCacheManager()
        config = NearCacheConfig(name="map1", max_size=100)

        cache = manager.get_or_create("map1", config)
        cache.put("key1", "value1")

        manager.destroy("map1")

        self.assertIsNone(manager.get("map1"))

    def test_destroy_all(self):
        manager = NearCacheManager()

        for i in range(5):
            config = NearCacheConfig(name=f"map{i}", max_size=100)
            cache = manager.get_or_create(f"map{i}", config)
            cache.put("key", "value")

        manager.destroy_all()

        for i in range(5):
            self.assertIsNone(manager.get(f"map{i}"))

    def test_list_all(self):
        manager = NearCacheManager()

        for i in range(3):
            config = NearCacheConfig(name=f"map{i}", max_size=100)
            cache = manager.get_or_create(f"map{i}", config)
            cache.put("key", "value")

        stats = manager.list_all()

        self.assertEqual(len(stats), 3)
        for name in ["map0", "map1", "map2"]:
            self.assertIn(name, stats)
            self.assertIsInstance(stats[name], NearCacheStats)


class TestNearCacheGetStatsSnapshot(unittest.TestCase):
    """Tests for stats snapshot functionality."""

    def test_get_stats_snapshot(self):
        config = NearCacheConfig(name="test", max_size=100)
        cache = NearCache(config)

        cache.put("key1", "value1")
        cache.put("key2", "value2")
        cache.get("key1")
        cache.get("key1")
        cache.get("nonexistent")

        snapshot = cache.get_stats_snapshot()

        self.assertEqual(snapshot.hits, 2)
        self.assertEqual(snapshot.misses, 1)
        self.assertEqual(snapshot.entries_count, 2)


if __name__ == "__main__":
    unittest.main()
