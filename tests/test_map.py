"""Tests for Map proxy operations."""

import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch, PropertyMock

from hazelcast.config import EvictionPolicy, NearCacheConfig
from hazelcast.near_cache import NearCache
from hazelcast.proxy.base import ProxyContext
from hazelcast.proxy.map import (
    EntryEvent,
    EntryListener,
    EntryView,
    IndexConfig,
    IndexType,
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


class TestIndexConfig(unittest.TestCase):
    """Tests for IndexConfig."""

    def test_index_config_defaults(self):
        config = IndexConfig(["name"])
        self.assertEqual(config.attributes, ["name"])
        self.assertEqual(config.type, IndexType.SORTED)
        self.assertIsNone(config.name)

    def test_index_config_with_type(self):
        config = IndexConfig(["age"], index_type=IndexType.HASH)
        self.assertEqual(config.attributes, ["age"])
        self.assertEqual(config.type, IndexType.HASH)

    def test_index_config_with_name(self):
        config = IndexConfig(["value"], name="my-index")
        self.assertEqual(config.name, "my-index")

    def test_index_config_multiple_attributes(self):
        config = IndexConfig(["first_name", "last_name"])
        self.assertEqual(len(config.attributes), 2)
        self.assertIn("first_name", config.attributes)
        self.assertIn("last_name", config.attributes)


class TestIndexType(unittest.TestCase):
    """Tests for IndexType constants."""

    def test_index_type_values(self):
        self.assertEqual(IndexType.SORTED, 0)
        self.assertEqual(IndexType.HASH, 1)
        self.assertEqual(IndexType.BITMAP, 2)


class TestEntryView(unittest.TestCase):
    """Tests for EntryView."""

    def test_entry_view_properties(self):
        view = EntryView(
            key="test-key",
            value="test-value",
            cost=100,
            creation_time=1000,
            expiration_time=2000,
            hits=5,
            last_access_time=1500,
            last_stored_time=1200,
            last_update_time=1400,
            version=3,
            ttl=1000,
            max_idle=500,
        )

        self.assertEqual(view.key, "test-key")
        self.assertEqual(view.value, "test-value")
        self.assertEqual(view.cost, 100)
        self.assertEqual(view.creation_time, 1000)
        self.assertEqual(view.expiration_time, 2000)
        self.assertEqual(view.hits, 5)
        self.assertEqual(view.last_access_time, 1500)
        self.assertEqual(view.last_stored_time, 1200)
        self.assertEqual(view.last_update_time, 1400)
        self.assertEqual(view.version, 3)
        self.assertEqual(view.ttl, 1000)
        self.assertEqual(view.max_idle, 500)

    def test_entry_view_defaults(self):
        view = EntryView(key="k", value="v")
        self.assertEqual(view.cost, 0)
        self.assertEqual(view.hits, 0)
        self.assertEqual(view.version, 0)

    def test_entry_view_repr(self):
        view = EntryView(key="k", value="v", hits=10, version=2)
        repr_str = repr(view)
        self.assertIn("EntryView", repr_str)
        self.assertIn("k", repr_str)
        self.assertIn("v", repr_str)
        self.assertIn("hits=10", repr_str)
        self.assertIn("version=2", repr_str)


class TestMapProxySetTtl(unittest.TestCase):
    """Tests for MapProxy set_ttl method."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.mock_context.invocation_service = MagicMock()
        self.mock_context.serialization_service = MagicMock()
        self.mock_context.serialization_service.to_data.return_value = b"key_data"
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_set_ttl_calls_async(self):
        future = Future()
        future.set_result(True)

        with patch.object(self.proxy, 'set_ttl_async', return_value=future) as mock:
            result = self.proxy.set_ttl("key", 60.0)
            mock.assert_called_once_with("key", 60.0)
            self.assertTrue(result)

    def test_set_ttl_invalidates_near_cache(self):
        near_cache = MagicMock()
        self.proxy._near_cache = near_cache

        future = Future()
        future.set_result(True)

        with patch.object(self.proxy, '_invoke', return_value=future):
            with patch.object(self.proxy, '_check_not_destroyed'):
                self.proxy.set_ttl_async("key", 30.0)


class TestMapProxyGetEntryView(unittest.TestCase):
    """Tests for MapProxy get_entry_view method."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_get_entry_view_calls_async(self):
        view = EntryView(key="k", value="v", hits=5)
        future = Future()
        future.set_result(view)

        with patch.object(self.proxy, 'get_entry_view_async', return_value=future) as mock:
            result = self.proxy.get_entry_view("key")
            mock.assert_called_once_with("key")
            self.assertEqual(result.key, "k")
            self.assertEqual(result.hits, 5)

    def test_get_entry_view_returns_none(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'get_entry_view_async', return_value=future):
            result = self.proxy.get_entry_view("nonexistent")
            self.assertIsNone(result)


class TestMapProxySubmitToKey(unittest.TestCase):
    """Tests for MapProxy submit_to_key method."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_submit_to_key_returns_future(self):
        future = Future()
        future.set_result("result")

        mock_processor = MagicMock()

        with patch.object(self.proxy, '_invoke', return_value=future):
            with patch.object(self.proxy, '_check_not_destroyed'):
                with patch.object(self.proxy, '_to_data', return_value=b"data"):
                    result_future = self.proxy.submit_to_key("key", mock_processor)
                    self.assertIsInstance(result_future, Future)

    def test_submit_to_key_invalidates_near_cache(self):
        near_cache = MagicMock()
        self.proxy._near_cache = near_cache

        future = Future()
        future.set_result("result")

        mock_processor = MagicMock()

        with patch.object(self.proxy, '_invoke', return_value=future):
            with patch.object(self.proxy, '_check_not_destroyed'):
                with patch.object(self.proxy, '_to_data', return_value=b"data"):
                    self.proxy.submit_to_key("key", mock_processor)
                    near_cache.invalidate.assert_called_once_with("key")


class TestMapProxyAddIndex(unittest.TestCase):
    """Tests for MapProxy add_index method."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_add_index_calls_async(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'add_index_async', return_value=future) as mock:
            self.proxy.add_index(["name"])
            mock.assert_called_once_with(["name"], IndexType.SORTED, None)

    def test_add_index_with_type(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'add_index_async', return_value=future) as mock:
            self.proxy.add_index(["age"], index_type=IndexType.HASH)
            mock.assert_called_once_with(["age"], IndexType.HASH, None)

    def test_add_index_with_name(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'add_index_async', return_value=future) as mock:
            self.proxy.add_index(["value"], name="my-index")
            mock.assert_called_once_with(["value"], IndexType.SORTED, "my-index")


class TestMapProxyInterceptors(unittest.TestCase):
    """Tests for MapProxy interceptor methods."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_add_interceptor_calls_async(self):
        future = Future()
        future.set_result("reg-id-123")

        mock_interceptor = MagicMock()

        with patch.object(self.proxy, 'add_interceptor_async', return_value=future) as mock:
            result = self.proxy.add_interceptor(mock_interceptor)
            mock.assert_called_once_with(mock_interceptor)
            self.assertEqual(result, "reg-id-123")

    def test_remove_interceptor_calls_async(self):
        future = Future()
        future.set_result(True)

        with patch.object(self.proxy, 'remove_interceptor_async', return_value=future) as mock:
            result = self.proxy.remove_interceptor("reg-id-123")
            mock.assert_called_once_with("reg-id-123")
            self.assertTrue(result)

    def test_remove_interceptor_not_found(self):
        future = Future()
        future.set_result(False)

        with patch.object(self.proxy, 'remove_interceptor_async', return_value=future):
            result = self.proxy.remove_interceptor("nonexistent")
            self.assertFalse(result)


class TestMapProxyExecuteOnEntriesWithPredicate(unittest.TestCase):
    """Tests for MapProxy execute_on_entries with predicate support."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_execute_on_entries_without_predicate(self):
        future = Future()
        future.set_result({"k1": "r1", "k2": "r2"})

        mock_processor = MagicMock()

        with patch.object(self.proxy, 'execute_on_entries_async', return_value=future) as mock:
            result = self.proxy.execute_on_entries(mock_processor)
            mock.assert_called_once_with(mock_processor, None)
            self.assertEqual(len(result), 2)

    def test_execute_on_entries_with_predicate(self):
        future = Future()
        future.set_result({"k1": "r1"})

        mock_processor = MagicMock()
        mock_predicate = MagicMock()

        with patch.object(self.proxy, 'execute_on_entries_async', return_value=future) as mock:
            result = self.proxy.execute_on_entries(mock_processor, mock_predicate)
            mock.assert_called_once_with(mock_processor, mock_predicate)
            self.assertEqual(len(result), 1)

    def test_execute_on_entries_async_with_predicate_invalidates_near_cache(self):
        near_cache = MagicMock()
        self.proxy._near_cache = near_cache

        future = Future()
        future.set_result({})

        mock_processor = MagicMock()
        mock_predicate = MagicMock()

        with patch.object(self.proxy, '_invoke', return_value=future):
            with patch.object(self.proxy, '_check_not_destroyed'):
                with patch.object(self.proxy, '_to_data', return_value=b"data"):
                    self.proxy.execute_on_entries_async(mock_processor, mock_predicate)
                    near_cache.invalidate_all.assert_called_once()


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


class TestMapProxyExecuteOnKey(unittest.TestCase):
    """Tests for MapProxy execute_on_key method."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_execute_on_key_calls_async(self):
        future = Future()
        future.set_result("result")

        mock_processor = MagicMock()

        with patch.object(self.proxy, 'execute_on_key_async', return_value=future) as mock:
            result = self.proxy.execute_on_key("key", mock_processor)
            mock.assert_called_once_with("key", mock_processor)
            self.assertEqual(result, "result")


class TestMapProxyExecuteOnKeys(unittest.TestCase):
    """Tests for MapProxy execute_on_keys method."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_execute_on_keys_calls_async(self):
        future = Future()
        future.set_result({"k1": "r1", "k2": "r2"})

        mock_processor = MagicMock()

        with patch.object(self.proxy, 'execute_on_keys_async', return_value=future) as mock:
            result = self.proxy.execute_on_keys({"k1", "k2"}, mock_processor)
            mock.assert_called_once()
            self.assertEqual(len(result), 2)

    def test_execute_on_keys_empty_set(self):
        future = Future()
        future.set_result({})

        mock_processor = MagicMock()

        with patch.object(self.proxy, 'execute_on_keys_async', return_value=future):
            result = self.proxy.execute_on_keys(set(), mock_processor)
            self.assertEqual(result, {})


class TestMapProxyLocking(unittest.TestCase):
    """Tests for MapProxy locking methods."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_lock_calls_async(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'lock_async', return_value=future) as mock:
            self.proxy.lock("key")
            mock.assert_called_once_with("key", -1)

    def test_lock_with_ttl(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'lock_async', return_value=future) as mock:
            self.proxy.lock("key", ttl=30.0)
            mock.assert_called_once_with("key", 30.0)

    def test_try_lock_success(self):
        future = Future()
        future.set_result(True)

        with patch.object(self.proxy, 'try_lock_async', return_value=future):
            result = self.proxy.try_lock("key")
            self.assertTrue(result)

    def test_try_lock_failure(self):
        future = Future()
        future.set_result(False)

        with patch.object(self.proxy, 'try_lock_async', return_value=future):
            result = self.proxy.try_lock("key")
            self.assertFalse(result)

    def test_unlock_calls_async(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'unlock_async', return_value=future) as mock:
            self.proxy.unlock("key")
            mock.assert_called_once_with("key")

    def test_is_locked(self):
        future = Future()
        future.set_result(True)

        with patch.object(self.proxy, 'is_locked_async', return_value=future):
            result = self.proxy.is_locked("key")
            self.assertTrue(result)

    def test_force_unlock_calls_async(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'force_unlock_async', return_value=future) as mock:
            self.proxy.force_unlock("key")
            mock.assert_called_once_with("key")


class TestMapProxyEviction(unittest.TestCase):
    """Tests for MapProxy eviction methods."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_evict_calls_async(self):
        future = Future()
        future.set_result(True)

        with patch.object(self.proxy, 'evict_async', return_value=future) as mock:
            result = self.proxy.evict("key")
            mock.assert_called_once_with("key")
            self.assertTrue(result)

    def test_evict_all_calls_async(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'evict_all_async', return_value=future) as mock:
            self.proxy.evict_all()
            mock.assert_called_once()

    def test_evict_invalidates_near_cache(self):
        near_cache = MagicMock()
        self.proxy._near_cache = near_cache

        future = Future()
        future.set_result(True)

        with patch.object(self.proxy, '_invoke', return_value=future):
            with patch.object(self.proxy, '_check_not_destroyed'):
                with patch.object(self.proxy, '_to_data', return_value=b"data"):
                    self.proxy.evict_async("key")
                    near_cache.invalidate.assert_called_once_with("key")


class TestMapProxyBulkOperations(unittest.TestCase):
    """Tests for MapProxy bulk operations."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_get_all_calls_async(self):
        future = Future()
        future.set_result({"k1": "v1", "k2": "v2"})

        with patch.object(self.proxy, 'get_all_async', return_value=future) as mock:
            result = self.proxy.get_all({"k1", "k2"})
            mock.assert_called_once()
            self.assertEqual(len(result), 2)

    def test_put_all_calls_async(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'put_all_async', return_value=future) as mock:
            self.proxy.put_all({"k1": "v1", "k2": "v2"})
            mock.assert_called_once()


class TestMapProxyCollectionViews(unittest.TestCase):
    """Tests for MapProxy collection view methods."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_key_set_calls_async(self):
        future = Future()
        future.set_result({"k1", "k2"})

        with patch.object(self.proxy, 'key_set_async', return_value=future) as mock:
            result = self.proxy.key_set()
            mock.assert_called_once_with(None)
            self.assertEqual(len(result), 2)

    def test_values_calls_async(self):
        future = Future()
        future.set_result(["v1", "v2"])

        with patch.object(self.proxy, 'values_async', return_value=future) as mock:
            result = self.proxy.values()
            mock.assert_called_once_with(None)
            self.assertEqual(len(result), 2)

    def test_entry_set_calls_async(self):
        future = Future()
        future.set_result({("k1", "v1"), ("k2", "v2")})

        with patch.object(self.proxy, 'entry_set_async', return_value=future) as mock:
            result = self.proxy.entry_set()
            mock.assert_called_once_with(None)
            self.assertEqual(len(result), 2)


class TestMapProxyMiscOperations(unittest.TestCase):
    """Tests for miscellaneous MapProxy operations."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_flush_calls_async(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'flush_async', return_value=future) as mock:
            self.proxy.flush()
            mock.assert_called_once()

    def test_load_all_calls_async(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'load_all_async', return_value=future) as mock:
            self.proxy.load_all()
            mock.assert_called_once_with(None, True)

    def test_load_all_with_keys(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'load_all_async', return_value=future) as mock:
            self.proxy.load_all(keys={"k1", "k2"}, replace_existing=False)
            mock.assert_called_once()

    def test_is_empty_true(self):
        future = Future()
        future.set_result(True)

        with patch.object(self.proxy, 'is_empty_async', return_value=future):
            result = self.proxy.is_empty()
            self.assertTrue(result)

    def test_is_empty_false(self):
        future = Future()
        future.set_result(False)

        with patch.object(self.proxy, 'is_empty_async', return_value=future):
            result = self.proxy.is_empty()
            self.assertFalse(result)

    def test_contains_value(self):
        future = Future()
        future.set_result(True)

        with patch.object(self.proxy, 'contains_value_async', return_value=future):
            result = self.proxy.contains_value("value")
            self.assertTrue(result)


class TestMapProxyReplace(unittest.TestCase):
    """Tests for MapProxy replace methods."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_replace_calls_async(self):
        future = Future()
        future.set_result("old_value")

        with patch.object(self.proxy, 'replace_async', return_value=future) as mock:
            result = self.proxy.replace("key", "new_value")
            mock.assert_called_once_with("key", "new_value")
            self.assertEqual(result, "old_value")

    def test_replace_if_same_success(self):
        future = Future()
        future.set_result(True)

        with patch.object(self.proxy, 'replace_if_same_async', return_value=future):
            result = self.proxy.replace_if_same("key", "old", "new")
            self.assertTrue(result)

    def test_replace_if_same_failure(self):
        future = Future()
        future.set_result(False)

        with patch.object(self.proxy, 'replace_if_same_async', return_value=future):
            result = self.proxy.replace_if_same("key", "wrong", "new")
            self.assertFalse(result)


class TestMapProxyPutIfAbsent(unittest.TestCase):
    """Tests for MapProxy put_if_absent method."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_put_if_absent_key_exists(self):
        future = Future()
        future.set_result("existing_value")

        with patch.object(self.proxy, 'put_if_absent_async', return_value=future):
            result = self.proxy.put_if_absent("key", "new_value")
            self.assertEqual(result, "existing_value")

    def test_put_if_absent_key_not_exists(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'put_if_absent_async', return_value=future):
            result = self.proxy.put_if_absent("key", "value")
            self.assertIsNone(result)


class TestMapProxySet(unittest.TestCase):
    """Tests for MapProxy set method."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_set_calls_async(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'set_async', return_value=future) as mock:
            self.proxy.set("key", "value")
            mock.assert_called_once_with("key", "value", -1)

    def test_set_with_ttl(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'set_async', return_value=future) as mock:
            self.proxy.set("key", "value", ttl=60.0)
            mock.assert_called_once_with("key", "value", 60.0)


class TestMapProxyDelete(unittest.TestCase):
    """Tests for MapProxy delete method."""

    def setUp(self):
        self.mock_context = MagicMock(spec=ProxyContext)
        self.proxy = MapProxy("test-map", context=self.mock_context)
        self.proxy._destroyed = False

    def test_delete_calls_async(self):
        future = Future()
        future.set_result(None)

        with patch.object(self.proxy, 'delete_async', return_value=future) as mock:
            self.proxy.delete("key")
            mock.assert_called_once_with("key")


class TestReferenceIdGenerator(unittest.TestCase):
    """Tests for _ReferenceIdGenerator."""

    def test_generates_unique_ids(self):
        from hazelcast.proxy.map import _ReferenceIdGenerator

        generator = _ReferenceIdGenerator()
        ids = [generator.next_id() for _ in range(100)]

        self.assertEqual(len(ids), len(set(ids)))

    def test_ids_are_sequential(self):
        from hazelcast.proxy.map import _ReferenceIdGenerator

        generator = _ReferenceIdGenerator()
        id1 = generator.next_id()
        id2 = generator.next_id()
        id3 = generator.next_id()

        self.assertEqual(id2, id1 + 1)
        self.assertEqual(id3, id2 + 1)


if __name__ == "__main__":
    unittest.main()
