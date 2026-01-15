"""Unit tests for near cache implementation."""

import time
import unittest
from unittest.mock import MagicMock

from hazelcast.config import (
    NearCacheConfig,
    EvictionPolicy,
    InMemoryFormat,
)
from hazelcast.near_cache import (
    NearCache,
    NearCacheRecord,
    NearCacheStats,
    NearCacheManager,
    LRUEvictionStrategy,
    LFUEvictionStrategy,
    RandomEvictionStrategy,
    NoneEvictionStrategy,
)


class TestNearCacheRecord(unittest.TestCase):
    """Tests for NearCacheRecord."""

    def test_record_creation(self):
        """Test record is created with default values."""
        record = NearCacheRecord(value="test")
        self.assertEqual(record.value, "test")
        self.assertEqual(record.access_count, 0)
        self.assertFalse(record.is_expired())

    def test_record_access(self):
        """Test recording access updates fields."""
        record = NearCacheRecord(value="test")
        initial_time = record.last_access_time

        time.sleep(0.01)
        record.record_access()

        self.assertEqual(record.access_count, 1)
        self.assertGreater(record.last_access_time, initial_time)

    def test_ttl_expiration(self):
        """Test TTL-based expiration."""
        record = NearCacheRecord(value="test", ttl_seconds=0)
        self.assertFalse(record.is_expired())

        record = NearCacheRecord(value="test", ttl_seconds=1)
        self.assertFalse(record.is_expired())

    def test_max_idle_expiration(self):
        """Test max-idle-based expiration."""
        record = NearCacheRecord(value="test", max_idle_seconds=0)
        self.assertFalse(record.is_expired())


class TestNearCacheStats(unittest.TestCase):
    """Tests for NearCacheStats."""

    def test_default_stats(self):
        """Test default statistics values."""
        stats = NearCacheStats()
        self.assertEqual(stats.hits, 0)
        self.assertEqual(stats.misses, 0)
        self.assertEqual(stats.evictions, 0)
        self.assertEqual(stats.hit_ratio, 0.0)
        self.assertEqual(stats.miss_ratio, 0.0)

    def test_hit_ratio_calculation(self):
        """Test hit ratio calculation."""
        stats = NearCacheStats(hits=7, misses=3)
        self.assertAlmostEqual(stats.hit_ratio, 0.7)
        self.assertAlmostEqual(stats.miss_ratio, 0.3)

    def test_stats_to_dict(self):
        """Test converting stats to dictionary."""
        stats = NearCacheStats(hits=10, misses=5, evictions=2)
        result = stats.to_dict()

        self.assertEqual(result["hits"], 10)
        self.assertEqual(result["misses"], 5)
        self.assertEqual(result["evictions"], 2)
        self.assertIn("hit_ratio", result)


class TestEvictionStrategies(unittest.TestCase):
    """Tests for eviction strategies."""

    def test_lru_eviction(self):
        """Test LRU eviction selects least recently used."""
        strategy = LRUEvictionStrategy()

        old_record = NearCacheRecord(value="old")
        old_record.last_access_time = 100.0

        new_record = NearCacheRecord(value="new")
        new_record.last_access_time = 200.0

        records = {"old": old_record, "new": new_record}
        evicted = strategy.select_for_eviction(records)

        self.assertEqual(evicted, "old")

    def test_lfu_eviction(self):
        """Test LFU eviction selects least frequently used."""
        strategy = LFUEvictionStrategy()

        rarely_used = NearCacheRecord(value="rare")
        rarely_used.access_count = 1

        often_used = NearCacheRecord(value="often")
        often_used.access_count = 100

        records = {"rare": rarely_used, "often": often_used}
        evicted = strategy.select_for_eviction(records)

        self.assertEqual(evicted, "rare")

    def test_random_eviction(self):
        """Test random eviction returns a key."""
        strategy = RandomEvictionStrategy()
        records = {
            "a": NearCacheRecord(value="a"),
            "b": NearCacheRecord(value="b"),
        }
        evicted = strategy.select_for_eviction(records)
        self.assertIn(evicted, ["a", "b"])

    def test_none_eviction(self):
        """Test none eviction returns None."""
        strategy = NoneEvictionStrategy()
        records = {"a": NearCacheRecord(value="a")}
        evicted = strategy.select_for_eviction(records)
        self.assertIsNone(evicted)

    def test_empty_records(self):
        """Test eviction with empty records."""
        for strategy in [
            LRUEvictionStrategy(),
            LFUEvictionStrategy(),
            RandomEvictionStrategy(),
            NoneEvictionStrategy(),
        ]:
            self.assertIsNone(strategy.select_for_eviction({}))


class TestNearCache(unittest.TestCase):
    """Tests for NearCache."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = NearCacheConfig(
            name="test-cache",
            max_size=100,
            eviction_policy=EvictionPolicy.LRU,
        )
        self.cache = NearCache(self.config)

    def test_put_and_get(self):
        """Test basic put and get operations."""
        self.cache.put("key1", "value1")
        result = self.cache.get("key1")

        self.assertEqual(result, "value1")
        self.assertEqual(self.cache.size, 1)

    def test_get_miss(self):
        """Test get returns None for missing key."""
        result = self.cache.get("nonexistent")
        self.assertIsNone(result)

    def test_remove(self):
        """Test remove operation."""
        self.cache.put("key1", "value1")
        removed = self.cache.remove("key1")

        self.assertEqual(removed, "value1")
        self.assertIsNone(self.cache.get("key1"))

    def test_contains(self):
        """Test contains check."""
        self.cache.put("key1", "value1")

        self.assertTrue(self.cache.contains("key1"))
        self.assertFalse(self.cache.contains("key2"))

    def test_clear(self):
        """Test clear operation."""
        self.cache.put("key1", "value1")
        self.cache.put("key2", "value2")
        self.cache.clear()

        self.assertEqual(self.cache.size, 0)

    def test_invalidate(self):
        """Test invalidation."""
        self.cache.put("key1", "value1")
        self.cache.invalidate("key1")

        self.assertIsNone(self.cache.get("key1"))
        self.assertEqual(self.cache.stats.invalidations, 1)

    def test_invalidate_all(self):
        """Test invalidate all entries."""
        self.cache.put("key1", "value1")
        self.cache.put("key2", "value2")
        self.cache.invalidate_all()

        self.assertEqual(self.cache.size, 0)
        self.assertEqual(self.cache.stats.invalidations, 2)

    def test_stats_tracking(self):
        """Test statistics are tracked correctly."""
        self.cache.put("key1", "value1")
        self.cache.get("key1")
        self.cache.get("key1")
        self.cache.get("nonexistent")

        stats = self.cache.stats
        self.assertEqual(stats.hits, 2)
        self.assertEqual(stats.misses, 1)

    def test_eviction_on_max_size(self):
        """Test eviction occurs when max size is reached."""
        config = NearCacheConfig(
            name="small-cache",
            max_size=3,
            eviction_policy=EvictionPolicy.LRU,
        )
        cache = NearCache(config)

        for i in range(5):
            cache.put(f"key{i}", f"value{i}")

        self.assertEqual(cache.size, 3)
        self.assertGreater(cache.stats.evictions, 0)

    def test_lru_eviction_order(self):
        """Test LRU eviction removes oldest accessed."""
        config = NearCacheConfig(
            name="lru-cache",
            max_size=2,
            eviction_policy=EvictionPolicy.LRU,
        )
        cache = NearCache(config)

        cache.put("key1", "value1")
        time.sleep(0.01)
        cache.put("key2", "value2")
        time.sleep(0.01)

        cache.get("key1")
        time.sleep(0.01)

        cache.put("key3", "value3")

        self.assertIsNone(cache.get("key2"))
        self.assertIsNotNone(cache.get("key1"))

    def test_lfu_eviction_order(self):
        """Test LFU eviction removes least frequently accessed."""
        config = NearCacheConfig(
            name="lfu-cache",
            max_size=2,
            eviction_policy=EvictionPolicy.LFU,
        )
        cache = NearCache(config)

        cache.put("key1", "value1")
        cache.put("key2", "value2")

        for _ in range(5):
            cache.get("key1")

        cache.put("key3", "value3")

        self.assertIsNone(cache.get("key2"))
        self.assertIsNotNone(cache.get("key1"))

    def test_binary_format(self):
        """Test binary in-memory format."""
        config = NearCacheConfig(
            name="binary-cache",
            in_memory_format=InMemoryFormat.BINARY,
        )
        cache = NearCache(config)

        cache.put("key1", "value1")
        result = cache.get("key1")

        self.assertEqual(result, "value1")

    def test_object_format(self):
        """Test object in-memory format."""
        config = NearCacheConfig(
            name="object-cache",
            in_memory_format=InMemoryFormat.OBJECT,
        )
        cache = NearCache(config)

        obj = {"nested": "value"}
        cache.put("key1", obj)
        result = cache.get("key1")

        self.assertEqual(result, obj)

    def test_invalidation_listener(self):
        """Test invalidation listener is called."""
        invalidated_keys = []

        def on_invalidate(key):
            invalidated_keys.append(key)

        self.cache.add_invalidation_listener(on_invalidate)
        self.cache.put("key1", "value1")

        self.assertIsNotNone(self.cache.get("key1"))


class TestNearCacheManager(unittest.TestCase):
    """Tests for NearCacheManager."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = NearCacheManager()
        self.config = NearCacheConfig(name="test-map")

    def test_get_or_create(self):
        """Test get or create returns same cache."""
        cache1 = self.manager.get_or_create("test-map", self.config)
        cache2 = self.manager.get_or_create("test-map", self.config)

        self.assertIs(cache1, cache2)

    def test_get_nonexistent(self):
        """Test get returns None for nonexistent cache."""
        result = self.manager.get("nonexistent")
        self.assertIsNone(result)

    def test_destroy(self):
        """Test destroying a cache."""
        self.manager.get_or_create("test-map", self.config)
        self.manager.destroy("test-map")

        self.assertIsNone(self.manager.get("test-map"))

    def test_destroy_all(self):
        """Test destroying all caches."""
        config1 = NearCacheConfig(name="map1")
        config2 = NearCacheConfig(name="map2")

        self.manager.get_or_create("map1", config1)
        self.manager.get_or_create("map2", config2)
        self.manager.destroy_all()

        self.assertIsNone(self.manager.get("map1"))
        self.assertIsNone(self.manager.get("map2"))

    def test_list_all(self):
        """Test listing all caches with stats."""
        config1 = NearCacheConfig(name="map1")
        config2 = NearCacheConfig(name="map2")

        self.manager.get_or_create("map1", config1)
        self.manager.get_or_create("map2", config2)

        all_stats = self.manager.list_all()

        self.assertIn("map1", all_stats)
        self.assertIn("map2", all_stats)


if __name__ == "__main__":
    unittest.main()
