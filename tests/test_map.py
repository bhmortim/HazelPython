"""Unit tests for Map proxy."""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future

from hazelcast.proxy.map import (
    MapProxy,
    IndexType,
    IndexConfig,
    EntryView,
    EntryEvent,
    EntryListener,
    _ReferenceIdGenerator,
)


class TestIndexType(unittest.TestCase):
    """Tests for IndexType class."""

    def test_index_type_constants(self):
        """Test IndexType constants are defined correctly."""
        self.assertEqual(IndexType.SORTED, 0)
        self.assertEqual(IndexType.HASH, 1)
        self.assertEqual(IndexType.BITMAP, 2)


class TestIndexConfig(unittest.TestCase):
    """Tests for IndexConfig class."""

    def test_index_config_initialization(self):
        """Test IndexConfig initialization."""
        config = IndexConfig(
            attributes=["name", "age"],
            index_type=IndexType.HASH,
            name="my-index",
        )
        self.assertEqual(config.name, "my-index")
        self.assertEqual(config.type, IndexType.HASH)
        self.assertEqual(config.attributes, ["name", "age"])

    def test_index_config_defaults(self):
        """Test IndexConfig with default values."""
        config = IndexConfig(attributes=["field"])
        self.assertIsNone(config.name)
        self.assertEqual(config.type, IndexType.SORTED)
        self.assertEqual(config.attributes, ["field"])


class TestEntryView(unittest.TestCase):
    """Tests for EntryView class."""

    def test_entry_view_initialization(self):
        """Test EntryView initialization with all parameters."""
        view = EntryView(
            key="test_key",
            value="test_value",
            cost=100,
            creation_time=1000,
            expiration_time=2000,
            hits=5,
            last_access_time=1500,
            last_stored_time=1200,
            last_update_time=1400,
            version=3,
            ttl=60000,
            max_idle=30000,
        )
        self.assertEqual(view.key, "test_key")
        self.assertEqual(view.value, "test_value")
        self.assertEqual(view.cost, 100)
        self.assertEqual(view.creation_time, 1000)
        self.assertEqual(view.expiration_time, 2000)
        self.assertEqual(view.hits, 5)
        self.assertEqual(view.last_access_time, 1500)
        self.assertEqual(view.last_stored_time, 1200)
        self.assertEqual(view.last_update_time, 1400)
        self.assertEqual(view.version, 3)
        self.assertEqual(view.ttl, 60000)
        self.assertEqual(view.max_idle, 30000)

    def test_entry_view_defaults(self):
        """Test EntryView with default values."""
        view = EntryView(key="key", value="value")
        self.assertEqual(view.key, "key")
        self.assertEqual(view.value, "value")
        self.assertEqual(view.cost, 0)
        self.assertEqual(view.creation_time, 0)
        self.assertEqual(view.hits, 0)
        self.assertEqual(view.version, 0)

    def test_entry_view_repr(self):
        """Test EntryView __repr__."""
        view = EntryView(key="k", value="v", hits=10, version=2)
        repr_str = repr(view)
        self.assertIn("EntryView", repr_str)
        self.assertIn("k", repr_str)
        self.assertIn("v", repr_str)
        self.assertIn("10", repr_str)
        self.assertIn("2", repr_str)


class TestEntryEvent(unittest.TestCase):
    """Tests for EntryEvent class."""

    def test_event_type_constants(self):
        """Test event type constants are defined correctly."""
        self.assertEqual(EntryEvent.ADDED, 1)
        self.assertEqual(EntryEvent.REMOVED, 2)
        self.assertEqual(EntryEvent.UPDATED, 4)
        self.assertEqual(EntryEvent.EVICTED, 8)
        self.assertEqual(EntryEvent.EXPIRED, 16)
        self.assertEqual(EntryEvent.EVICT_ALL, 32)
        self.assertEqual(EntryEvent.CLEAR_ALL, 64)
        self.assertEqual(EntryEvent.MERGED, 128)
        self.assertEqual(EntryEvent.INVALIDATION, 256)
        self.assertEqual(EntryEvent.LOADED, 512)

    def test_event_initialization(self):
        """Test EntryEvent initialization."""
        event = EntryEvent(
            event_type=EntryEvent.ADDED,
            key="test_key",
            value="test_value",
            old_value="old_value",
            merging_value="merge_value",
            member="member1",
        )
        self.assertEqual(event.event_type, EntryEvent.ADDED)
        self.assertEqual(event.key, "test_key")
        self.assertEqual(event.value, "test_value")
        self.assertEqual(event.old_value, "old_value")
        self.assertEqual(event.merging_value, "merge_value")
        self.assertEqual(event.member, "member1")

    def test_event_with_defaults(self):
        """Test EntryEvent with default values."""
        event = EntryEvent(event_type=EntryEvent.REMOVED, key="key")
        self.assertEqual(event.event_type, EntryEvent.REMOVED)
        self.assertEqual(event.key, "key")
        self.assertIsNone(event.value)
        self.assertIsNone(event.old_value)
        self.assertIsNone(event.merging_value)
        self.assertIsNone(event.member)


class TestEntryListener(unittest.TestCase):
    """Tests for EntryListener class."""

    def test_listener_methods_exist(self):
        """Test that listener has all required methods."""
        listener = EntryListener()
        self.assertTrue(hasattr(listener, "entry_added"))
        self.assertTrue(hasattr(listener, "entry_removed"))
        self.assertTrue(hasattr(listener, "entry_updated"))
        self.assertTrue(hasattr(listener, "entry_evicted"))
        self.assertTrue(hasattr(listener, "entry_expired"))
        self.assertTrue(hasattr(listener, "map_evicted"))
        self.assertTrue(hasattr(listener, "map_cleared"))

    def test_listener_methods_are_callable(self):
        """Test that listener methods can be called without error."""
        listener = EntryListener()
        event = EntryEvent(EntryEvent.ADDED, "key", "value")
        listener.entry_added(event)
        listener.entry_removed(event)
        listener.entry_updated(event)
        listener.entry_evicted(event)
        listener.entry_expired(event)
        listener.map_evicted(event)
        listener.map_cleared(event)


class TestReferenceIdGenerator(unittest.TestCase):
    """Tests for _ReferenceIdGenerator class."""

    def test_generator_initialization(self):
        """Test generator starts at 0."""
        gen = _ReferenceIdGenerator()
        self.assertEqual(gen._counter, 0)

    def test_next_id_increments(self):
        """Test next_id increments counter."""
        gen = _ReferenceIdGenerator()
        id1 = gen.next_id()
        id2 = gen.next_id()
        id3 = gen.next_id()
        self.assertEqual(id1, 1)
        self.assertEqual(id2, 2)
        self.assertEqual(id3, 3)

    def test_thread_safety(self):
        """Test generator is thread-safe."""
        import threading
        gen = _ReferenceIdGenerator()
        ids = []
        lock = threading.Lock()

        def get_ids():
            for _ in range(100):
                id_val = gen.next_id()
                with lock:
                    ids.append(id_val)

        threads = [threading.Thread(target=get_ids) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(ids), 500)
        self.assertEqual(len(set(ids)), 500)


class TestMapProxy(unittest.TestCase):
    """Tests for MapProxy class."""

    def setUp(self):
        """Set up test fixtures."""
        self.proxy = MapProxy(
            name="test-map",
            context=None,
        )

    def test_service_name_constant(self):
        """Test SERVICE_NAME is defined correctly."""
        self.assertEqual(MapProxy.SERVICE_NAME, "hz:impl:mapService")

    def test_initialization(self):
        """Test MapProxy initialization."""
        self.assertEqual(self.proxy._name, "test-map")
        self.assertIsNone(self.proxy._near_cache)
        self.assertIsInstance(self.proxy._entry_listeners, dict)
        self.assertIsInstance(self.proxy._query_caches, dict)

    def test_near_cache_property(self):
        """Test near_cache property."""
        self.assertIsNone(self.proxy.near_cache)

    def test_put_async_returns_future(self):
        """Test put_async returns a Future."""
        future = self.proxy.put_async("key", "value")
        self.assertIsInstance(future, Future)

    def test_put_with_ttl(self):
        """Test put with TTL parameter."""
        future = self.proxy.put_async("key", "value", ttl=60)
        self.assertIsInstance(future, Future)

    def test_get_async_returns_future(self):
        """Test get_async returns a Future."""
        future = self.proxy.get_async("key")
        self.assertIsInstance(future, Future)

    def test_remove_async_returns_future(self):
        """Test remove_async returns a Future."""
        future = self.proxy.remove_async("key")
        self.assertIsInstance(future, Future)

    def test_delete_async_returns_future(self):
        """Test delete_async returns a Future."""
        future = self.proxy.delete_async("key")
        self.assertIsInstance(future, Future)

    def test_contains_key_async_returns_future(self):
        """Test contains_key_async returns a Future."""
        future = self.proxy.contains_key_async("key")
        self.assertIsInstance(future, Future)

    def test_contains_value_async_returns_future(self):
        """Test contains_value_async returns a Future."""
        future = self.proxy.contains_value_async("value")
        self.assertIsInstance(future, Future)

    def test_put_if_absent_async_returns_future(self):
        """Test put_if_absent_async returns a Future."""
        future = self.proxy.put_if_absent_async("key", "value")
        self.assertIsInstance(future, Future)

    def test_put_if_absent_with_ttl(self):
        """Test put_if_absent_async with TTL."""
        future = self.proxy.put_if_absent_async("key", "value", ttl=60)
        self.assertIsInstance(future, Future)

    def test_replace_async_returns_future(self):
        """Test replace_async returns a Future."""
        future = self.proxy.replace_async("key", "value")
        self.assertIsInstance(future, Future)

    def test_replace_if_same_async_returns_future(self):
        """Test replace_if_same_async returns a Future."""
        future = self.proxy.replace_if_same_async("key", "old", "new")
        self.assertIsInstance(future, Future)

    def test_set_async_returns_future(self):
        """Test set_async returns a Future."""
        future = self.proxy.set_async("key", "value")
        self.assertIsInstance(future, Future)

    def test_set_with_ttl(self):
        """Test set_async with TTL."""
        future = self.proxy.set_async("key", "value", ttl=60)
        self.assertIsInstance(future, Future)

    def test_get_all_async_empty_keys(self):
        """Test get_all_async with empty keys returns empty dict."""
        future = self.proxy.get_all_async(set())
        self.assertEqual(future.result(), {})

    def test_get_all_async_returns_future(self):
        """Test get_all_async returns a Future."""
        future = self.proxy.get_all_async({"key1", "key2"})
        self.assertIsInstance(future, Future)

    def test_put_all_async_empty_entries(self):
        """Test put_all_async with empty entries."""
        future = self.proxy.put_all_async({})
        self.assertIsNone(future.result())

    def test_put_all_async_returns_future(self):
        """Test put_all_async returns a Future."""
        future = self.proxy.put_all_async({"key1": "value1"})
        self.assertIsInstance(future, Future)

    def test_size_async_returns_future(self):
        """Test size_async returns a Future."""
        future = self.proxy.size_async()
        self.assertIsInstance(future, Future)

    def test_is_empty_async_returns_future(self):
        """Test is_empty_async returns a Future."""
        future = self.proxy.is_empty_async()
        self.assertIsInstance(future, Future)

    def test_clear_async_returns_future(self):
        """Test clear_async returns a Future."""
        future = self.proxy.clear_async()
        self.assertIsInstance(future, Future)

    def test_key_set_async_returns_future(self):
        """Test key_set_async returns a Future."""
        future = self.proxy.key_set_async()
        self.assertIsInstance(future, Future)

    def test_values_async_returns_future(self):
        """Test values_async returns a Future."""
        future = self.proxy.values_async()
        self.assertIsInstance(future, Future)

    def test_entry_set_async_returns_future(self):
        """Test entry_set_async returns a Future."""
        future = self.proxy.entry_set_async()
        self.assertIsInstance(future, Future)

    def test_add_entry_listener_returns_registration_id(self):
        """Test add_entry_listener returns a registration ID."""
        listener = EntryListener()
        reg_id = self.proxy.add_entry_listener(listener)
        self.assertIsInstance(reg_id, str)
        self.assertTrue(len(reg_id) > 0)

    def test_add_entry_listener_with_key(self):
        """Test add_entry_listener with specific key."""
        listener = EntryListener()
        reg_id = self.proxy.add_entry_listener(listener, key="specific_key")
        self.assertIsInstance(reg_id, str)

    def test_add_entry_listener_without_value(self):
        """Test add_entry_listener with include_value=False."""
        listener = EntryListener()
        reg_id = self.proxy.add_entry_listener(listener, include_value=False)
        self.assertIsInstance(reg_id, str)

    def test_remove_entry_listener_returns_true_for_existing(self):
        """Test remove_entry_listener returns True for existing listener."""
        listener = EntryListener()
        reg_id = self.proxy.add_entry_listener(listener)
        result = self.proxy.remove_entry_listener(reg_id)
        self.assertTrue(result)

    def test_remove_entry_listener_returns_false_for_nonexistent(self):
        """Test remove_entry_listener returns False for nonexistent listener."""
        result = self.proxy.remove_entry_listener("nonexistent-id")
        self.assertFalse(result)

    def test_aggregate_async_returns_future(self):
        """Test aggregate_async returns a Future."""
        future = self.proxy.aggregate_async(MagicMock())
        self.assertIsInstance(future, Future)

    def test_project_async_returns_future(self):
        """Test project_async returns a Future."""
        future = self.proxy.project_async(MagicMock())
        self.assertIsInstance(future, Future)

    def test_execute_on_key_async_returns_future(self):
        """Test execute_on_key_async returns a Future."""
        future = self.proxy.execute_on_key_async("key", MagicMock())
        self.assertIsInstance(future, Future)

    def test_execute_on_keys_async_empty_keys(self):
        """Test execute_on_keys_async with empty keys."""
        future = self.proxy.execute_on_keys_async(set(), MagicMock())
        self.assertEqual(future.result(), {})

    def test_execute_on_keys_async_returns_future(self):
        """Test execute_on_keys_async returns a Future."""
        future = self.proxy.execute_on_keys_async({"key1"}, MagicMock())
        self.assertIsInstance(future, Future)

    def test_execute_on_entries_async_returns_future(self):
        """Test execute_on_entries_async returns a Future."""
        future = self.proxy.execute_on_entries_async(MagicMock())
        self.assertIsInstance(future, Future)

    def test_execute_on_entries_async_with_predicate(self):
        """Test execute_on_entries_async with predicate."""
        future = self.proxy.execute_on_entries_async(MagicMock(), predicate=MagicMock())
        self.assertIsInstance(future, Future)

    def test_execute_on_all_entries_async_returns_future(self):
        """Test execute_on_all_entries_async returns a Future."""
        future = self.proxy.execute_on_all_entries_async(MagicMock())
        self.assertIsInstance(future, Future)

    def test_submit_to_key_returns_future(self):
        """Test submit_to_key returns a Future."""
        future = self.proxy.submit_to_key("key", MagicMock())
        self.assertIsInstance(future, Future)

    def test_lock_async_returns_future(self):
        """Test lock_async returns a Future."""
        future = self.proxy.lock_async("key")
        self.assertIsInstance(future, Future)

    def test_lock_with_ttl(self):
        """Test lock_async with TTL."""
        future = self.proxy.lock_async("key", ttl=60)
        self.assertIsInstance(future, Future)

    def test_try_lock_async_returns_future(self):
        """Test try_lock_async returns a Future."""
        future = self.proxy.try_lock_async("key")
        self.assertIsInstance(future, Future)

    def test_try_lock_with_timeout_and_ttl(self):
        """Test try_lock_async with timeout and TTL."""
        future = self.proxy.try_lock_async("key", timeout=5, ttl=60)
        self.assertIsInstance(future, Future)

    def test_unlock_async_returns_future(self):
        """Test unlock_async returns a Future."""
        future = self.proxy.unlock_async("key")
        self.assertIsInstance(future, Future)

    def test_is_locked_async_returns_future(self):
        """Test is_locked_async returns a Future."""
        future = self.proxy.is_locked_async("key")
        self.assertIsInstance(future, Future)

    def test_force_unlock_async_returns_future(self):
        """Test force_unlock_async returns a Future."""
        future = self.proxy.force_unlock_async("key")
        self.assertIsInstance(future, Future)

    def test_evict_async_returns_future(self):
        """Test evict_async returns a Future."""
        future = self.proxy.evict_async("key")
        self.assertIsInstance(future, Future)

    def test_evict_all_async_returns_future(self):
        """Test evict_all_async returns a Future."""
        future = self.proxy.evict_all_async()
        self.assertIsInstance(future, Future)

    def test_flush_async_returns_future(self):
        """Test flush_async returns a Future."""
        future = self.proxy.flush_async()
        self.assertIsInstance(future, Future)

    def test_load_all_async_returns_future(self):
        """Test load_all_async returns a Future."""
        future = self.proxy.load_all_async()
        self.assertIsInstance(future, Future)

    def test_load_all_async_with_keys(self):
        """Test load_all_async with specific keys."""
        future = self.proxy.load_all_async(keys={"key1", "key2"})
        self.assertIsInstance(future, Future)

    def test_load_all_async_with_replace_existing(self):
        """Test load_all_async with replace_existing=False."""
        future = self.proxy.load_all_async(replace_existing=False)
        self.assertIsInstance(future, Future)

    def test_is_loaded_async_returns_future(self):
        """Test is_loaded_async returns a Future."""
        future = self.proxy.is_loaded_async()
        self.assertIsInstance(future, Future)

    def test_set_ttl_async_returns_future(self):
        """Test set_ttl_async returns a Future."""
        future = self.proxy.set_ttl_async("key", 60)
        self.assertIsInstance(future, Future)

    def test_get_entry_view_async_returns_future(self):
        """Test get_entry_view_async returns a Future."""
        future = self.proxy.get_entry_view_async("key")
        self.assertIsInstance(future, Future)

    def test_get_query_cache_creates_cache(self):
        """Test get_query_cache creates a new query cache."""
        from hazelcast.query_cache import QueryCache
        cache = self.proxy.get_query_cache("test-cache")
        self.assertIsInstance(cache, QueryCache)

    def test_get_query_cache_returns_existing(self):
        """Test get_query_cache returns existing cache."""
        cache1 = self.proxy.get_query_cache("test-cache")
        cache2 = self.proxy.get_query_cache("test-cache")
        self.assertIs(cache1, cache2)

    def test_destroy_query_cache_returns_true(self):
        """Test destroy_query_cache returns True for existing cache."""
        self.proxy.get_query_cache("test-cache")
        result = self.proxy.destroy_query_cache("test-cache")
        self.assertTrue(result)

    def test_destroy_query_cache_returns_false(self):
        """Test destroy_query_cache returns False for nonexistent cache."""
        result = self.proxy.destroy_query_cache("nonexistent")
        self.assertFalse(result)

    def test_add_index_async_returns_future(self):
        """Test add_index_async returns a Future."""
        future = self.proxy.add_index_async(["field"])
        self.assertIsInstance(future, Future)

    def test_add_index_with_type_and_name(self):
        """Test add_index_async with index type and name."""
        future = self.proxy.add_index_async(
            ["field"], index_type=IndexType.HASH, name="my-index"
        )
        self.assertIsInstance(future, Future)

    def test_add_interceptor_async_returns_future(self):
        """Test add_interceptor_async returns a Future."""
        future = self.proxy.add_interceptor_async(MagicMock())
        self.assertIsInstance(future, Future)

    def test_remove_interceptor_async_returns_future(self):
        """Test remove_interceptor_async returns a Future."""
        future = self.proxy.remove_interceptor_async("reg-id")
        self.assertIsInstance(future, Future)

    def test_get_event_journal_reader(self):
        """Test get_event_journal_reader returns a reader."""
        from hazelcast.event_journal import EventJournalReader
        reader = self.proxy.get_event_journal_reader()
        self.assertIsInstance(reader, EventJournalReader)

    def test_read_from_event_journal_async_returns_future(self):
        """Test read_from_event_journal_async returns a Future."""
        future = self.proxy.read_from_event_journal_async(0)
        self.assertIsInstance(future, Future)

    def test_put_wan_async_returns_future(self):
        """Test put_wan_async returns a Future."""
        from hazelcast.wan import WanReplicationRef
        wan_ref = WanReplicationRef("test-wan")
        future = self.proxy.put_wan_async("key", "value", wan_ref)
        self.assertIsInstance(future, Future)

    def test_remove_wan_async_returns_future(self):
        """Test remove_wan_async returns a Future."""
        from hazelcast.wan import WanReplicationRef
        wan_ref = WanReplicationRef("test-wan")
        future = self.proxy.remove_wan_async("key", wan_ref)
        self.assertIsInstance(future, Future)

    def test_wan_sync_async_returns_future(self):
        """Test wan_sync_async returns a Future."""
        from hazelcast.wan import WanReplicationRef
        wan_ref = WanReplicationRef("test-wan")
        future = self.proxy.wan_sync_async(wan_ref)
        self.assertIsInstance(future, Future)

    def test_len_returns_size(self):
        """Test __len__ returns size."""
        self.assertEqual(len(self.proxy), 0)

    def test_contains_uses_contains_key(self):
        """Test __contains__ uses contains_key."""
        self.assertFalse("key" in self.proxy)

    def test_getitem_uses_get(self):
        """Test __getitem__ uses get."""
        result = self.proxy["key"]
        self.assertIsNone(result)

    def test_setitem_uses_put(self):
        """Test __setitem__ uses put."""
        self.proxy["key"] = "value"

    def test_delitem_uses_remove(self):
        """Test __delitem__ uses remove."""
        del self.proxy["key"]

    def test_iter_returns_keys(self):
        """Test __iter__ returns iterator over keys."""
        result = list(iter(self.proxy))
        self.assertEqual(result, [])


class TestMapProxyWithNearCache(unittest.TestCase):
    """Tests for MapProxy with near cache."""

    def setUp(self):
        """Set up test fixtures."""
        self.near_cache = MagicMock()
        self.near_cache.config = MagicMock()
        self.near_cache.config.invalidate_on_change = False
        self.proxy = MapProxy(
            name="test-map",
            context=None,
            near_cache=self.near_cache,
        )

    def test_near_cache_property(self):
        """Test near_cache property returns cache."""
        self.assertEqual(self.proxy.near_cache, self.near_cache)

    def test_get_uses_near_cache(self):
        """Test get checks near cache first."""
        self.near_cache.get.return_value = "cached_value"
        result = self.proxy.get("key")
        self.assertEqual(result, "cached_value")
        self.near_cache.get.assert_called_with("key")

    def test_put_invalidates_near_cache(self):
        """Test put invalidates near cache."""
        self.proxy.put_async("key", "value")
        self.near_cache.invalidate.assert_called_with("key")

    def test_remove_invalidates_near_cache(self):
        """Test remove invalidates near cache."""
        self.proxy.remove_async("key")
        self.near_cache.invalidate.assert_called_with("key")

    def test_clear_invalidates_all(self):
        """Test clear invalidates all near cache entries."""
        self.proxy.clear_async()
        self.near_cache.invalidate_all.assert_called()


class TestMapProxyLoadAll(unittest.TestCase):
    """Tests for MapProxy load_all with callback."""

    def setUp(self):
        """Set up test fixtures."""
        self.proxy = MapProxy(
            name="test-map",
            context=None,
        )

    def test_load_all_with_callback_success(self):
        """Test load_all invokes callback on success."""
        results = []

        def callback(count, error):
            results.append((count, error))

        self.proxy.load_all(callback=callback)
        self.assertEqual(len(results), 1)
        self.assertIsNone(results[0][1])

    def test_load_all_with_keys_and_callback(self):
        """Test load_all with specific keys invokes callback."""
        results = []

        def callback(count, error):
            results.append((count, error))

        keys = {"key1", "key2"}
        self.proxy.load_all(keys=keys, callback=callback)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], 2)


if __name__ == "__main__":
    unittest.main()
