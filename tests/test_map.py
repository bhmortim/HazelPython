"""Tests for Map proxy operations."""

import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from hazelcast.config import EvictionPolicy, NearCacheConfig
from hazelcast.near_cache import NearCache
from hazelcast.proxy.base import ProxyContext
from hazelcast.proxy.map import (
    EntryEvent,
    EntryListener,
    MapProxy,
    _NearCacheInvalidationListener,
)


class TestMapProxyBasic(unittest.TestCase):
    """Basic tests for MapProxy without context."""

    def test_create_proxy(self):
        proxy = MapProxy("test-map")
        self.assertEqual(proxy.name, "test-map")
        self.assertEqual(proxy.service_name, "hz:impl:mapService")

    def test_proxy_repr(self):
        proxy = MapProxy("test-map")
        self.assertIn("MapProxy", repr(proxy))
        self.assertIn("test-map", repr(proxy))


class TestMapProxyWithNearCache(unittest.TestCase):
    """Tests for MapProxy with near cache integration."""

    def setUp(self):
        self.near_cache_config = NearCacheConfig(
            name="test-map",
            max_size=100,
            eviction_policy=EvictionPolicy.LRU,
            invalidate_on_change=False,
        )
        self.near_cache = NearCache(self.near_cache_config)
        self.proxy = MapProxy("test-map", near_cache=self.near_cache)

    def test_near_cache_property(self):
        self.assertIs(self.proxy.near_cache, self.near_cache)

    def test_set_near_cache(self):
        proxy = MapProxy("test-map")
        self.assertIsNone(proxy.near_cache)

        proxy.set_near_cache(self.near_cache)
        self.assertIs(proxy.near_cache, self.near_cache)


class TestEntryEvent(unittest.TestCase):
    """Tests for EntryEvent."""

    def test_entry_event_properties(self):
        event = EntryEvent(
            event_type=EntryEvent.ADDED,
            key="test-key",
            value="test-value",
            old_value="old-value",
        )

        self.assertEqual(event.event_type, EntryEvent.ADDED)
        self.assertEqual(event.key, "test-key")
        self.assertEqual(event.value, "test-value")
        self.assertEqual(event.old_value, "old-value")

    def test_entry_event_types(self):
        self.assertEqual(EntryEvent.ADDED, 1)
        self.assertEqual(EntryEvent.REMOVED, 2)
        self.assertEqual(EntryEvent.UPDATED, 4)
        self.assertEqual(EntryEvent.EVICTED, 8)
        self.assertEqual(EntryEvent.EXPIRED, 16)


class TestEntryListener(unittest.TestCase):
    """Tests for EntryListener."""

    def test_default_methods_do_nothing(self):
        listener = EntryListener()
        event = EntryEvent(EntryEvent.ADDED, "key", "value")

        listener.entry_added(event)
        listener.entry_removed(event)
        listener.entry_updated(event)
        listener.entry_evicted(event)
        listener.entry_expired(event)
        listener.map_evicted(event)
        listener.map_cleared(event)


class TestNearCacheInvalidationListener(unittest.TestCase):
    """Tests for _NearCacheInvalidationListener."""

    def setUp(self):
        self.near_cache_config = NearCacheConfig(name="test", max_size=100)
        self.near_cache = NearCache(self.near_cache_config)
        self.listener = _NearCacheInvalidationListener(self.near_cache)

    def test_entry_added_invalidates(self):
        self.near_cache.put("key1", "value1")
        event = EntryEvent(EntryEvent.ADDED, "key1")

        self.listener.entry_added(event)

        self.assertIsNone(self.near_cache.get("key1"))

    def test_entry_removed_invalidates(self):
        self.near_cache.put("key1", "value1")
        event = EntryEvent(EntryEvent.REMOVED, "key1")

        self.listener.entry_removed(event)

        self.assertIsNone(self.near_cache.get("key1"))

    def test_entry_updated_invalidates(self):
        self.near_cache.put("key1", "value1")
        event = EntryEvent(EntryEvent.UPDATED, "key1")

        self.listener.entry_updated(event)

        self.assertIsNone(self.near_cache.get("key1"))

    def test_entry_evicted_invalidates(self):
        self.near_cache.put("key1", "value1")
        event = EntryEvent(EntryEvent.EVICTED, "key1")

        self.listener.entry_evicted(event)

        self.assertIsNone(self.near_cache.get("key1"))

    def test_entry_expired_invalidates(self):
        self.near_cache.put("key1", "value1")
        event = EntryEvent(EntryEvent.EXPIRED, "key1")

        self.listener.entry_expired(event)

        self.assertIsNone(self.near_cache.get("key1"))

    def test_map_evicted_invalidates_all(self):
        self.near_cache.put("key1", "value1")
        self.near_cache.put("key2", "value2")
        event = EntryEvent(EntryEvent.EVICT_ALL, "")

        self.listener.map_evicted(event)

        self.assertEqual(self.near_cache.size, 0)

    def test_map_cleared_invalidates_all(self):
        self.near_cache.put("key1", "value1")
        self.near_cache.put("key2", "value2")
        event = EntryEvent(EntryEvent.CLEAR_ALL, "")

        self.listener.map_cleared(event)

        self.assertEqual(self.near_cache.size, 0)


class TestMapProxyDunderMethods(unittest.TestCase):
    """Tests for MapProxy dunder methods."""

    def test_len_returns_size(self):
        mock_context = MagicMock(spec=ProxyContext)
        mock_context.invocation_service = MagicMock()

        future = Future()
        future.set_result(5)
        mock_context.invocation_service.invoke.return_value = future

        proxy = MapProxy("test-map", context=mock_context)

        with patch.object(proxy, 'size', return_value=5):
            self.assertEqual(len(proxy), 5)

    def test_contains_calls_contains_key(self):
        proxy = MapProxy("test-map")
        with patch.object(proxy, 'contains_key', return_value=True):
            self.assertTrue("key" in proxy)

    def test_getitem_calls_get(self):
        proxy = MapProxy("test-map")
        with patch.object(proxy, 'get', return_value="value"):
            self.assertEqual(proxy["key"], "value")

    def test_setitem_calls_put(self):
        proxy = MapProxy("test-map")
        with patch.object(proxy, 'put') as mock_put:
            proxy["key"] = "value"
            mock_put.assert_called_once_with("key", "value")

    def test_delitem_calls_remove(self):
        proxy = MapProxy("test-map")
        with patch.object(proxy, 'remove') as mock_remove:
            del proxy["key"]
            mock_remove.assert_called_once_with("key")

    def test_iter_calls_key_set(self):
        proxy = MapProxy("test-map")
        with patch.object(proxy, 'key_set', return_value={"k1", "k2"}):
            keys = list(iter(proxy))
            self.assertEqual(set(keys), {"k1", "k2"})


if __name__ == "__main__":
    unittest.main()
