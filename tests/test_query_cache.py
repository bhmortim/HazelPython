"""Tests for query cache functionality."""

import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from hazelcast.config import QueryCacheConfig, EvictionPolicy, InMemoryFormat
from hazelcast.query_cache import (
    QueryCache,
    QueryCacheEvent,
    QueryCacheEventType,
    QueryCacheListener,
    LocalIndex,
)
from hazelcast.exceptions import IllegalStateException


class MockMapProxy:
    """Mock MapProxy for testing query cache."""

    def __init__(self):
        self._entries = {}
        self._listeners = {}
        self._listener_id_counter = 0

    def entry_set(self, predicate=None):
        return set(self._entries.items())

    def add_entry_listener(self, listener, include_value=True, predicate=None, key=None):
        self._listener_id_counter += 1
        listener_id = f"listener-{self._listener_id_counter}"
        self._listeners[listener_id] = listener
        return listener_id

    def remove_entry_listener(self, registration_id):
        return self._listeners.pop(registration_id, None) is not None

    def put(self, key, value):
        old_value = self._entries.get(key)
        self._entries[key] = value
        self._notify_listeners(key, value, old_value)
        return old_value

    def remove(self, key):
        value = self._entries.pop(key, None)
        if value is not None:
            self._notify_remove(key, value)
        return value

    def _notify_listeners(self, key, value, old_value):
        event = MagicMock()
        event.key = key
        event.value = value
        event.old_value = old_value

        for listener in self._listeners.values():
            if old_value is None:
                listener.entry_added(event)
            else:
                listener.entry_updated(event)

    def _notify_remove(self, key, value):
        event = MagicMock()
        event.key = key
        event.value = None
        event.old_value = value

        for listener in self._listeners.values():
            listener.entry_removed(event)


class TestLocalIndex(unittest.TestCase):
    """Tests for LocalIndex class."""

    def test_add_dict_entry(self):
        """Test adding dict entries to index."""
        index = LocalIndex("status")
        index.add("key1", {"status": "active", "name": "Alice"})
        index.add("key2", {"status": "active", "name": "Bob"})
        index.add("key3", {"status": "inactive", "name": "Charlie"})

        active_keys = index.get_keys("active")
        self.assertEqual(active_keys, {"key1", "key2"})

        inactive_keys = index.get_keys("inactive")
        self.assertEqual(inactive_keys, {"key3"})

    def test_add_object_entry(self):
        """Test adding object entries to index."""
        index = LocalIndex("status")

        class User:
            def __init__(self, status):
                self.status = status

        index.add("key1", User("active"))
        index.add("key2", User("inactive"))

        self.assertEqual(index.get_keys("active"), {"key1"})
        self.assertEqual(index.get_keys("inactive"), {"key2"})

    def test_remove_entry(self):
        """Test removing entries from index."""
        index = LocalIndex("status")
        index.add("key1", {"status": "active"})
        index.add("key2", {"status": "active"})

        self.assertEqual(len(index.get_keys("active")), 2)

        index.remove("key1", {"status": "active"})
        self.assertEqual(index.get_keys("active"), {"key2"})

    def test_update_entry(self):
        """Test updating entries in index."""
        index = LocalIndex("status")
        index.add("key1", {"status": "active"})

        self.assertEqual(index.get_keys("active"), {"key1"})
        self.assertEqual(index.get_keys("inactive"), set())

        index.update("key1", {"status": "active"}, {"status": "inactive"})

        self.assertEqual(index.get_keys("active"), set())
        self.assertEqual(index.get_keys("inactive"), {"key1"})

    def test_clear(self):
        """Test clearing the index."""
        index = LocalIndex("status")
        index.add("key1", {"status": "active"})
        index.add("key2", {"status": "inactive"})

        index.clear()

        self.assertEqual(index.get_keys("active"), set())
        self.assertEqual(index.get_keys("inactive"), set())

    def test_get_keys_nonexistent(self):
        """Test getting keys for nonexistent value."""
        index = LocalIndex("status")
        self.assertEqual(index.get_keys("nonexistent"), set())

    def test_none_value(self):
        """Test handling None values."""
        index = LocalIndex("status")
        index.add("key1", None)
        self.assertEqual(index.get_keys(None), set())


class TestQueryCache(unittest.TestCase):
    """Tests for QueryCache class."""

    def setUp(self):
        """Set up test fixtures."""
        self.map_proxy = MockMapProxy()
        self.map_proxy._entries = {
            "key1": {"name": "Alice", "status": "active"},
            "key2": {"name": "Bob", "status": "active"},
            "key3": {"name": "Charlie", "status": "inactive"},
        }
        self.config = QueryCacheConfig(name="test-cache")

    def test_create_with_populate(self):
        """Test creating query cache with population."""
        self.config.populate = True
        cache = QueryCache("test", self.map_proxy, self.config)

        self.assertEqual(cache.size(), 3)
        self.assertIn("key1", cache)
        self.assertIn("key2", cache)
        self.assertIn("key3", cache)

    def test_create_without_populate(self):
        """Test creating query cache without population."""
        self.config.populate = False
        cache = QueryCache("test", self.map_proxy, self.config)

        self.assertEqual(cache.size(), 0)

    def test_get_existing_key(self):
        """Test getting an existing key."""
        cache = QueryCache("test", self.map_proxy, self.config)

        value = cache.get("key1")
        self.assertEqual(value["name"], "Alice")

    def test_get_nonexistent_key(self):
        """Test getting a nonexistent key."""
        cache = QueryCache("test", self.map_proxy, self.config)

        value = cache.get("nonexistent")
        self.assertIsNone(value)

    def test_contains_key(self):
        """Test contains_key method."""
        cache = QueryCache("test", self.map_proxy, self.config)

        self.assertTrue(cache.contains_key("key1"))
        self.assertFalse(cache.contains_key("nonexistent"))

    def test_contains_value(self):
        """Test contains_value method."""
        cache = QueryCache("test", self.map_proxy, self.config)

        alice = {"name": "Alice", "status": "active"}
        self.assertTrue(cache.contains_value(alice))

    def test_is_empty(self):
        """Test is_empty method."""
        self.map_proxy._entries = {}
        cache = QueryCache("test", self.map_proxy, self.config)

        self.assertTrue(cache.is_empty())

        self.map_proxy._entries = {"key1": "value1"}
        cache2 = QueryCache("test2", self.map_proxy, self.config)
        self.assertFalse(cache2.is_empty())

    def test_key_set(self):
        """Test key_set method."""
        cache = QueryCache("test", self.map_proxy, self.config)

        keys = cache.key_set()
        self.assertEqual(keys, {"key1", "key2", "key3"})

    def test_values(self):
        """Test values method."""
        cache = QueryCache("test", self.map_proxy, self.config)

        values = cache.values()
        self.assertEqual(len(values), 3)

    def test_entry_set(self):
        """Test entry_set method."""
        cache = QueryCache("test", self.map_proxy, self.config)

        entries = cache.entry_set()
        self.assertEqual(len(entries), 3)

    def test_get_all(self):
        """Test get_all method."""
        cache = QueryCache("test", self.map_proxy, self.config)

        result = cache.get_all({"key1", "key2", "nonexistent"})
        self.assertEqual(len(result), 2)
        self.assertIn("key1", result)
        self.assertIn("key2", result)
        self.assertNotIn("nonexistent", result)

    def test_add_index(self):
        """Test adding a local index."""
        cache = QueryCache("test", self.map_proxy, self.config)

        cache.add_index("status")
        result = cache.get_by_index("status", "active")

        self.assertEqual(len(result), 2)
        self.assertIn("key1", result)
        self.assertIn("key2", result)

    def test_get_by_index_not_indexed(self):
        """Test get_by_index with non-indexed attribute."""
        cache = QueryCache("test", self.map_proxy, self.config)

        with self.assertRaises(ValueError):
            cache.get_by_index("not_indexed", "value")

    def test_clear(self):
        """Test clearing the cache."""
        cache = QueryCache("test", self.map_proxy, self.config)
        self.assertEqual(cache.size(), 3)

        cache.clear()
        self.assertEqual(cache.size(), 0)

    def test_destroy(self):
        """Test destroying the cache."""
        cache = QueryCache("test", self.map_proxy, self.config)
        cache.destroy()

        self.assertTrue(cache.is_destroyed)
        with self.assertRaises(IllegalStateException):
            cache.get("key1")

    def test_statistics(self):
        """Test getting cache statistics."""
        cache = QueryCache("test", self.map_proxy, self.config)

        cache.get("key1")
        cache.get("key1")
        cache.get("nonexistent")

        stats = cache.get_statistics()

        self.assertEqual(stats["name"], "test")
        self.assertEqual(stats["size"], 3)
        self.assertEqual(stats["hits"], 2)
        self.assertEqual(stats["misses"], 1)
        self.assertAlmostEqual(stats["hit_rate"], 2/3)

    def test_entry_listener(self):
        """Test entry listener functionality."""
        cache = QueryCache("test", self.map_proxy, self.config)

        events = []

        def on_added(event):
            events.append(("added", event.key))

        def on_removed(event):
            events.append(("removed", event.key))

        def on_updated(event):
            events.append(("updated", event.key))

        reg_id = cache.add_entry_listener(
            on_added=on_added,
            on_removed=on_removed,
            on_updated=on_updated,
        )

        cache._add_entry_internal("new_key", {"name": "New"})
        self.assertEqual(events[-1], ("added", "new_key"))

        cache._add_entry_internal("new_key", {"name": "Updated"})
        self.assertEqual(events[-1], ("updated", "new_key"))

        cache._remove_entry_internal("new_key")
        self.assertEqual(events[-1], ("removed", "new_key"))

        self.assertTrue(cache.remove_entry_listener(reg_id))

    def test_eviction_lru(self):
        """Test LRU eviction policy."""
        self.config.eviction_max_size = 2
        self.config.eviction_policy = EvictionPolicy.LRU
        self.config.populate = False

        cache = QueryCache("test", self.map_proxy, self.config)

        cache._add_entry_internal("key1", "value1")
        cache._add_entry_internal("key2", "value2")
        self.assertEqual(cache.size(), 2)

        cache._add_entry_internal("key3", "value3")
        self.assertEqual(cache.size(), 2)
        self.assertFalse(cache.contains_key("key1"))

    def test_dunder_methods(self):
        """Test Python dunder methods."""
        cache = QueryCache("test", self.map_proxy, self.config)

        self.assertEqual(len(cache), 3)
        self.assertIn("key1", cache)
        self.assertEqual(cache["key1"]["name"], "Alice")

        keys = list(cache)
        self.assertEqual(len(keys), 3)

    def test_try_recover(self):
        """Test cache recovery."""
        cache = QueryCache("test", self.map_proxy, self.config)
        cache.clear()
        self.assertEqual(cache.size(), 0)

        result = cache.try_recover()
        self.assertTrue(result)
        self.assertEqual(cache.size(), 3)

    def test_thread_safety(self):
        """Test thread safety of cache operations."""
        cache = QueryCache("test", self.map_proxy, self.config)
        errors = []

        def reader():
            try:
                for _ in range(100):
                    cache.get("key1")
                    cache.size()
                    cache.contains_key("key2")
            except Exception as e:
                errors.append(e)

        def writer():
            try:
                for i in range(100):
                    cache._add_entry_internal(f"new_{i}", {"value": i})
                    cache._remove_entry_internal(f"new_{i}")
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=reader),
            threading.Thread(target=reader),
            threading.Thread(target=writer),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0)


class TestQueryCacheEvent(unittest.TestCase):
    """Tests for QueryCacheEvent class."""

    def test_event_properties(self):
        """Test event property access."""
        event = QueryCacheEvent(
            QueryCacheEventType.ENTRY_ADDED,
            key="key1",
            value="new_value",
            old_value="old_value",
        )

        self.assertEqual(event.event_type, QueryCacheEventType.ENTRY_ADDED)
        self.assertEqual(event.key, "key1")
        self.assertEqual(event.value, "new_value")
        self.assertEqual(event.old_value, "old_value")

    def test_event_repr(self):
        """Test event string representation."""
        event = QueryCacheEvent(QueryCacheEventType.ENTRY_REMOVED, key="key1")
        repr_str = repr(event)

        self.assertIn("ENTRY_REMOVED", repr_str)
        self.assertIn("key1", repr_str)


class TestQueryCacheListener(unittest.TestCase):
    """Tests for QueryCacheListener class."""

    def test_default_implementation(self):
        """Test default listener implementation does nothing."""
        listener = QueryCacheListener()
        event = QueryCacheEvent(QueryCacheEventType.ENTRY_ADDED, "key")

        listener.entry_added(event)
        listener.entry_removed(event)
        listener.entry_updated(event)


class TestQueryCacheConfig(unittest.TestCase):
    """Tests for QueryCacheConfig integration."""

    def test_config_from_dict(self):
        """Test creating config from dictionary."""
        data = {
            "predicate": "status = 'active'",
            "include_value": True,
            "populate": True,
            "eviction_max_size": 5000,
            "eviction_policy": "LRU",
            "in_memory_format": "OBJECT",
        }

        config = QueryCacheConfig.from_dict("test-cache", data)

        self.assertEqual(config.name, "test-cache")
        self.assertEqual(config.predicate, "status = 'active'")
        self.assertTrue(config.include_value)
        self.assertTrue(config.populate)
        self.assertEqual(config.eviction_max_size, 5000)
        self.assertEqual(config.eviction_policy, EvictionPolicy.LRU)
        self.assertEqual(config.in_memory_format, InMemoryFormat.OBJECT)

    def test_config_defaults(self):
        """Test config default values."""
        config = QueryCacheConfig(name="test")

        self.assertEqual(config.name, "test")
        self.assertIsNone(config.predicate)
        self.assertTrue(config.include_value)
        self.assertTrue(config.populate)
        self.assertEqual(config.eviction_max_size, 10000)
        self.assertEqual(config.eviction_policy, EvictionPolicy.LRU)


if __name__ == "__main__":
    unittest.main()
