"""Tests for Query Cache and Continuous Query Cache implementations."""

import threading
import time
import uuid
import unittest
from unittest.mock import MagicMock, patch

from hazelcast.query_cache import (
    QueryCache,
    QueryCacheEvent,
    QueryCacheEventType,
    QueryCacheListener,
    LocalIndex,
    ContinuousQueryCache,
    ContinuousQueryCacheConfig,
    ContinuousQueryCacheEvent,
    ContinuousQueryCacheListener,
)
from hazelcast.protocol.codec import (
    ContinuousQueryCacheCodec,
    CQC_CREATE_WITH_VALUE,
    CQC_CREATE,
    CQC_SET_READ_CURSOR,
    CQC_ADD_LISTENER,
    CQC_DESTROY,
    CQC_FETCH,
    CQC_SIZE,
    CQC_MADE_PUBLISHABLE,
)


class TestQueryCacheEvent(unittest.TestCase):
    """Tests for QueryCacheEvent."""

    def test_event_properties(self):
        """Test event properties are accessible."""
        event = QueryCacheEvent(
            QueryCacheEventType.ENTRY_ADDED,
            key="test_key",
            value="test_value",
            old_value=None,
        )
        self.assertEqual(event.event_type, QueryCacheEventType.ENTRY_ADDED)
        self.assertEqual(event.key, "test_key")
        self.assertEqual(event.value, "test_value")
        self.assertIsNone(event.old_value)

    def test_event_with_old_value(self):
        """Test event with old value for updates."""
        event = QueryCacheEvent(
            QueryCacheEventType.ENTRY_UPDATED,
            key="key1",
            value="new_value",
            old_value="old_value",
        )
        self.assertEqual(event.event_type, QueryCacheEventType.ENTRY_UPDATED)
        self.assertEqual(event.old_value, "old_value")

    def test_event_repr(self):
        """Test event string representation."""
        event = QueryCacheEvent(QueryCacheEventType.ENTRY_REMOVED, key="k1")
        repr_str = repr(event)
        self.assertIn("ENTRY_REMOVED", repr_str)
        self.assertIn("k1", repr_str)


class TestLocalIndex(unittest.TestCase):
    """Tests for LocalIndex."""

    def test_add_and_get(self):
        """Test adding entries and getting keys."""
        index = LocalIndex("status")
        index.add("key1", {"status": "active"})
        index.add("key2", {"status": "active"})
        index.add("key3", {"status": "inactive"})

        active_keys = index.get_keys("active")
        self.assertEqual(active_keys, {"key1", "key2"})

        inactive_keys = index.get_keys("inactive")
        self.assertEqual(inactive_keys, {"key3"})

    def test_remove(self):
        """Test removing entries from index."""
        index = LocalIndex("type")
        index.add("k1", {"type": "A"})
        index.add("k2", {"type": "A"})

        index.remove("k1", {"type": "A"})
        keys = index.get_keys("A")
        self.assertEqual(keys, {"k2"})

    def test_update(self):
        """Test updating entries in index."""
        index = LocalIndex("status")
        index.add("k1", {"status": "pending"})

        index.update("k1", {"status": "pending"}, {"status": "completed"})

        pending = index.get_keys("pending")
        completed = index.get_keys("completed")

        self.assertEqual(pending, set())
        self.assertEqual(completed, {"k1"})

    def test_clear(self):
        """Test clearing the index."""
        index = LocalIndex("attr")
        index.add("k1", {"attr": "val"})
        index.add("k2", {"attr": "val"})

        index.clear()
        keys = index.get_keys("val")
        self.assertEqual(keys, set())

    def test_object_attribute(self):
        """Test indexing object attributes."""
        index = LocalIndex("name")

        class Person:
            def __init__(self, name):
                self.name = name

        index.add("p1", Person("Alice"))
        index.add("p2", Person("Bob"))
        index.add("p3", Person("Alice"))

        alice_keys = index.get_keys("Alice")
        self.assertEqual(alice_keys, {"p1", "p3"})

    def test_none_value(self):
        """Test handling None values."""
        index = LocalIndex("attr")
        index.add("k1", None)
        keys = index.get_keys(None)
        self.assertEqual(keys, set())


class TestQueryCacheListener(unittest.TestCase):
    """Tests for QueryCacheListener."""

    def test_default_methods_do_nothing(self):
        """Test default listener methods don't raise errors."""
        listener = QueryCacheListener()
        event = QueryCacheEvent(QueryCacheEventType.ENTRY_ADDED, key="k")

        listener.entry_added(event)
        listener.entry_removed(event)
        listener.entry_updated(event)


class TestQueryCache(unittest.TestCase):
    """Tests for QueryCache."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_map = MagicMock()
        self.mock_map.entry_set.return_value = set()
        self.mock_map.add_entry_listener.return_value = "listener-id"

        self.mock_config = MagicMock()
        self.mock_config.populate = False
        self.mock_config.include_value = True
        self.mock_config.eviction_max_size = 1000
        self.mock_config.eviction_policy = MagicMock()
        self.mock_config.eviction_policy.name = "NONE"

    def test_basic_operations(self):
        """Test basic cache operations."""
        cache = QueryCache(
            name="test-cache",
            map_proxy=self.mock_map,
            config=self.mock_config,
        )

        self.assertEqual(cache.name, "test-cache")
        self.assertEqual(cache.size(), 0)
        self.assertTrue(cache.is_empty())

    def test_add_and_get(self):
        """Test adding and getting entries."""
        cache = QueryCache(
            name="cache",
            map_proxy=self.mock_map,
            config=self.mock_config,
        )

        cache._add_entry_internal("key1", "value1", fire_event=False)
        cache._add_entry_internal("key2", "value2", fire_event=False)

        self.assertEqual(cache.get("key1"), "value1")
        self.assertEqual(cache.get("key2"), "value2")
        self.assertIsNone(cache.get("key3"))

    def test_contains(self):
        """Test contains_key and contains_value."""
        cache = QueryCache(
            name="cache",
            map_proxy=self.mock_map,
            config=self.mock_config,
        )

        cache._add_entry_internal("k", "v", fire_event=False)

        self.assertTrue(cache.contains_key("k"))
        self.assertFalse(cache.contains_key("other"))
        self.assertTrue(cache.contains_value("v"))
        self.assertFalse(cache.contains_value("other"))

    def test_key_set_values_entry_set(self):
        """Test key_set, values, and entry_set."""
        cache = QueryCache(
            name="cache",
            map_proxy=self.mock_map,
            config=self.mock_config,
        )

        cache._add_entry_internal("a", 1, fire_event=False)
        cache._add_entry_internal("b", 2, fire_event=False)

        self.assertEqual(cache.key_set(), {"a", "b"})
        self.assertEqual(set(cache.values()), {1, 2})
        self.assertEqual(cache.entry_set(), {("a", 1), ("b", 2)})

    def test_get_all(self):
        """Test get_all method."""
        cache = QueryCache(
            name="cache",
            map_proxy=self.mock_map,
            config=self.mock_config,
        )

        cache._add_entry_internal("k1", "v1", fire_event=False)
        cache._add_entry_internal("k2", "v2", fire_event=False)
        cache._add_entry_internal("k3", "v3", fire_event=False)

        result = cache.get_all({"k1", "k3", "k4"})
        self.assertEqual(result, {"k1": "v1", "k3": "v3"})

    def test_indexing(self):
        """Test local indexing."""
        cache = QueryCache(
            name="cache",
            map_proxy=self.mock_map,
            config=self.mock_config,
        )

        cache._add_entry_internal("u1", {"dept": "eng"}, fire_event=False)
        cache._add_entry_internal("u2", {"dept": "sales"}, fire_event=False)
        cache._add_entry_internal("u3", {"dept": "eng"}, fire_event=False)

        cache.add_index("dept")

        eng_users = cache.get_by_index("dept", "eng")
        self.assertEqual(set(eng_users.keys()), {"u1", "u3"})

    def test_index_not_found(self):
        """Test accessing non-existent index raises error."""
        cache = QueryCache(
            name="cache",
            map_proxy=self.mock_map,
            config=self.mock_config,
        )

        with self.assertRaises(ValueError):
            cache.get_by_index("nonexistent", "value")

    def test_listener_registration(self):
        """Test listener registration and removal."""
        cache = QueryCache(
            name="cache",
            map_proxy=self.mock_map,
            config=self.mock_config,
        )

        events = []

        def on_added(event):
            events.append(("added", event.key))

        reg_id = cache.add_entry_listener(on_added=on_added)
        self.assertIsNotNone(reg_id)

        cache._add_entry_internal("k", "v")
        self.assertEqual(events, [("added", "k")])

        removed = cache.remove_entry_listener(reg_id)
        self.assertTrue(removed)

        removed_again = cache.remove_entry_listener(reg_id)
        self.assertFalse(removed_again)

    def test_clear(self):
        """Test clearing the cache."""
        cache = QueryCache(
            name="cache",
            map_proxy=self.mock_map,
            config=self.mock_config,
        )

        cache._add_entry_internal("k1", "v1", fire_event=False)
        cache._add_entry_internal("k2", "v2", fire_event=False)

        cache.clear()
        self.assertEqual(cache.size(), 0)

    def test_destroy(self):
        """Test destroying the cache."""
        cache = QueryCache(
            name="cache",
            map_proxy=self.mock_map,
            config=self.mock_config,
        )

        cache._add_entry_internal("k", "v", fire_event=False)
        cache.destroy()

        self.assertTrue(cache.is_destroyed)

        with self.assertRaises(Exception):
            cache.get("k")

    def test_statistics(self):
        """Test cache statistics."""
        cache = QueryCache(
            name="stats-cache",
            map_proxy=self.mock_map,
            config=self.mock_config,
        )

        cache._add_entry_internal("k1", "v1", fire_event=False)
        cache.get("k1")
        cache.get("nonexistent")

        stats = cache.get_statistics()
        self.assertEqual(stats["name"], "stats-cache")
        self.assertEqual(stats["size"], 1)
        self.assertEqual(stats["hits"], 1)
        self.assertEqual(stats["misses"], 1)
        self.assertEqual(stats["hit_rate"], 0.5)

    def test_dunder_methods(self):
        """Test special methods (__len__, __contains__, etc.)."""
        cache = QueryCache(
            name="cache",
            map_proxy=self.mock_map,
            config=self.mock_config,
        )

        cache._add_entry_internal("key", "value", fire_event=False)

        self.assertEqual(len(cache), 1)
        self.assertTrue("key" in cache)
        self.assertEqual(cache["key"], "value")

        keys = list(cache)
        self.assertEqual(keys, ["key"])

    def test_try_recover(self):
        """Test recovery method."""
        self.mock_map.entry_set.return_value = {("k", "v")}

        cache = QueryCache(
            name="cache",
            map_proxy=self.mock_map,
            config=self.mock_config,
        )

        result = cache.try_recover()
        self.assertTrue(result)


class TestContinuousQueryCacheConfig(unittest.TestCase):
    """Tests for ContinuousQueryCacheConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ContinuousQueryCacheConfig(name="test")

        self.assertEqual(config.name, "test")
        self.assertIsNone(config.predicate)
        self.assertEqual(config.batch_size, 1)
        self.assertEqual(config.buffer_size, 16)
        self.assertEqual(config.delay_seconds, 0)
        self.assertTrue(config.populate)
        self.assertFalse(config.coalesce)
        self.assertTrue(config.include_value)

    def test_custom_config(self):
        """Test custom configuration values."""
        config = ContinuousQueryCacheConfig(
            name="custom",
            predicate="active = true",
            batch_size=10,
            buffer_size=32,
            delay_seconds=5,
            populate=False,
            coalesce=True,
            include_value=False,
        )

        self.assertEqual(config.batch_size, 10)
        self.assertEqual(config.buffer_size, 32)
        self.assertTrue(config.coalesce)
        self.assertFalse(config.populate)


class TestContinuousQueryCacheEvent(unittest.TestCase):
    """Tests for ContinuousQueryCacheEvent."""

    def test_event_properties(self):
        """Test event properties including sequence."""
        event = ContinuousQueryCacheEvent(
            QueryCacheEventType.ENTRY_ADDED,
            key="k",
            value="v",
            sequence=42,
        )

        self.assertEqual(event.event_type, QueryCacheEventType.ENTRY_ADDED)
        self.assertEqual(event.key, "k")
        self.assertEqual(event.value, "v")
        self.assertEqual(event.sequence, 42)

    def test_event_repr(self):
        """Test event string representation includes sequence."""
        event = ContinuousQueryCacheEvent(
            QueryCacheEventType.ENTRY_UPDATED,
            key="key1",
            sequence=100,
        )
        repr_str = repr(event)
        self.assertIn("ENTRY_UPDATED", repr_str)
        self.assertIn("100", repr_str)


class TestContinuousQueryCacheListener(unittest.TestCase):
    """Tests for ContinuousQueryCacheListener."""

    def test_default_methods(self):
        """Test default listener methods don't raise."""
        listener = ContinuousQueryCacheListener()
        event = ContinuousQueryCacheEvent(QueryCacheEventType.ENTRY_ADDED, key="k")

        listener.entry_added(event)
        listener.entry_removed(event)
        listener.entry_updated(event)


class TestContinuousQueryCache(unittest.TestCase):
    """Tests for ContinuousQueryCache."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_map = MagicMock()
        self.config = ContinuousQueryCacheConfig(name="cqc-test")

    def test_basic_properties(self):
        """Test basic cache properties."""
        cqc = ContinuousQueryCache(
            name="my-cqc",
            map_name="my-map",
            map_proxy=self.mock_map,
            config=self.config,
            publisher_id=uuid.UUID("12345678-1234-5678-1234-567812345678"),
        )

        self.assertEqual(cqc.name, "my-cqc")
        self.assertEqual(cqc.map_name, "my-map")
        self.assertFalse(cqc.is_destroyed)

    def test_add_and_get(self):
        """Test adding and getting entries."""
        cqc = ContinuousQueryCache(
            name="cqc",
            map_name="map",
            map_proxy=self.mock_map,
            config=self.config,
        )

        cqc._add_entry_internal("k1", "v1", fire_event=False)
        cqc._add_entry_internal("k2", "v2", fire_event=False)

        self.assertEqual(cqc.get("k1"), "v1")
        self.assertEqual(cqc.get("k2"), "v2")
        self.assertIsNone(cqc.get("k3"))

    def test_sequence_tracking(self):
        """Test sequence number tracking."""
        cqc = ContinuousQueryCache(
            name="cqc",
            map_name="map",
            map_proxy=self.mock_map,
            config=self.config,
        )

        self.assertEqual(cqc.sequence, 0)

        cqc._add_entry_internal("k1", "v1", sequence=10, fire_event=False)
        self.assertEqual(cqc.sequence, 10)

        cqc._add_entry_internal("k2", "v2", sequence=5, fire_event=False)
        self.assertEqual(cqc.sequence, 10)

        cqc._add_entry_internal("k3", "v3", sequence=20, fire_event=False)
        self.assertEqual(cqc.sequence, 20)

    def test_set_read_cursor(self):
        """Test setting read cursor."""
        cqc = ContinuousQueryCache(
            name="cqc",
            map_name="map",
            map_proxy=self.mock_map,
            config=self.config,
        )

        result = cqc.set_read_cursor(100)
        self.assertTrue(result)
        self.assertEqual(cqc.sequence, 100)

    def test_made_publishable(self):
        """Test making cache publishable."""
        cqc = ContinuousQueryCache(
            name="cqc",
            map_name="map",
            map_proxy=self.mock_map,
            config=self.config,
        )

        result = cqc.made_publishable()
        self.assertTrue(result)

    def test_listener_with_sequence(self):
        """Test listener receives events with sequence numbers."""
        cqc = ContinuousQueryCache(
            name="cqc",
            map_name="map",
            map_proxy=self.mock_map,
            config=self.config,
        )

        events = []

        def on_added(event):
            events.append((event.key, event.sequence))

        cqc.add_entry_listener(on_added=on_added)
        cqc._add_entry_internal("k1", "v1", sequence=42)

        self.assertEqual(events, [("k1", 42)])

    def test_get_all(self):
        """Test get_all returns dictionary."""
        cqc = ContinuousQueryCache(
            name="cqc",
            map_name="map",
            map_proxy=self.mock_map,
            config=self.config,
        )

        cqc._add_entry_internal("a", 1, fire_event=False)
        cqc._add_entry_internal("b", 2, fire_event=False)

        all_entries = cqc.get_all()
        self.assertEqual(all_entries, {"a": 1, "b": 2})

    def test_fetch(self):
        """Test fetch method."""
        cqc = ContinuousQueryCache(
            name="cqc",
            map_name="map",
            map_proxy=self.mock_map,
            config=self.config,
        )

        cqc._add_entry_internal("k1", "v1", fire_event=False)
        cqc._add_entry_internal("k2", "v2", fire_event=False)
        cqc._add_entry_internal("k3", "v3", fire_event=False)

        entries = cqc.fetch(batch_size=2)
        self.assertEqual(len(entries), 2)

    def test_statistics(self):
        """Test cache statistics."""
        cqc = ContinuousQueryCache(
            name="stats-cqc",
            map_name="stats-map",
            map_proxy=self.mock_map,
            config=self.config,
        )

        cqc._add_entry_internal("k", "v", sequence=50, fire_event=False)
        cqc.made_publishable()

        stats = cqc.get_statistics()
        self.assertEqual(stats["name"], "stats-cqc")
        self.assertEqual(stats["map_name"], "stats-map")
        self.assertEqual(stats["size"], 1)
        self.assertEqual(stats["sequence"], 50)
        self.assertTrue(stats["publishable"])

    def test_destroy(self):
        """Test destroying the cache."""
        cqc = ContinuousQueryCache(
            name="cqc",
            map_name="map",
            map_proxy=self.mock_map,
            config=self.config,
        )

        cqc._add_entry_internal("k", "v", fire_event=False)
        cqc.destroy()

        self.assertTrue(cqc.is_destroyed)
        self.assertEqual(cqc.size(), 0)

        with self.assertRaises(Exception):
            cqc.get("k")

    def test_dunder_methods(self):
        """Test special methods."""
        cqc = ContinuousQueryCache(
            name="cqc",
            map_name="map",
            map_proxy=self.mock_map,
            config=self.config,
        )

        cqc._add_entry_internal("key", "value", fire_event=False)

        self.assertEqual(len(cqc), 1)
        self.assertTrue("key" in cqc)
        self.assertEqual(cqc["key"], "value")

    def test_repr(self):
        """Test string representation."""
        cqc = ContinuousQueryCache(
            name="test-cqc",
            map_name="test-map",
            map_proxy=self.mock_map,
            config=self.config,
        )

        repr_str = repr(cqc)
        self.assertIn("test-cqc", repr_str)
        self.assertIn("test-map", repr_str)


class TestContinuousQueryCacheCodec(unittest.TestCase):
    """Tests for ContinuousQueryCacheCodec."""

    def test_protocol_constants(self):
        """Test protocol constants are defined correctly."""
        self.assertEqual(CQC_CREATE_WITH_VALUE, 0x016000)
        self.assertEqual(CQC_CREATE, 0x016100)
        self.assertEqual(CQC_SET_READ_CURSOR, 0x016200)
        self.assertEqual(CQC_ADD_LISTENER, 0x016300)
        self.assertEqual(CQC_DESTROY, 0x016400)
        self.assertEqual(CQC_FETCH, 0x016500)
        self.assertEqual(CQC_SIZE, 0x016600)
        self.assertEqual(CQC_MADE_PUBLISHABLE, 0x016700)

    def test_encode_create_with_value_request(self):
        """Test encoding CreateWithValue request."""
        msg = ContinuousQueryCacheCodec.encode_create_with_value_request(
            map_name="test-map",
            cache_name="test-cache",
            predicate_data=b"predicate",
            batch_size=10,
            buffer_size=16,
            delay_seconds=5,
            populate=True,
            coalesce=False,
        )

        self.assertIsNotNone(msg)

    def test_encode_create_request(self):
        """Test encoding Create request."""
        msg = ContinuousQueryCacheCodec.encode_create_request(
            map_name="map",
            cache_name="cache",
            predicate_data=None,
            batch_size=1,
            buffer_size=8,
            delay_seconds=0,
            populate=True,
            coalesce=True,
        )

        self.assertIsNotNone(msg)

    def test_encode_set_read_cursor_request(self):
        """Test encoding SetReadCursor request."""
        publisher_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        msg = ContinuousQueryCacheCodec.encode_set_read_cursor_request(
            map_name="map",
            cache_name="cache",
            publisher_id=publisher_id,
            sequence=100,
        )

        self.assertIsNotNone(msg)

    def test_encode_add_listener_request(self):
        """Test encoding AddListener request."""
        msg = ContinuousQueryCacheCodec.encode_add_listener_request(
            map_name="map",
            cache_name="cache",
            local_only=False,
        )

        self.assertIsNotNone(msg)

    def test_encode_destroy_request(self):
        """Test encoding Destroy request."""
        publisher_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        msg = ContinuousQueryCacheCodec.encode_destroy_request(
            map_name="map",
            cache_name="cache",
            publisher_id=publisher_id,
        )

        self.assertIsNotNone(msg)

    def test_encode_fetch_request(self):
        """Test encoding Fetch request."""
        publisher_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        msg = ContinuousQueryCacheCodec.encode_fetch_request(
            map_name="map",
            cache_name="cache",
            publisher_id=publisher_id,
            sequence=0,
            batch_size=100,
        )

        self.assertIsNotNone(msg)

    def test_encode_size_request(self):
        """Test encoding Size request."""
        msg = ContinuousQueryCacheCodec.encode_size_request(
            map_name="map",
            cache_name="cache",
        )

        self.assertIsNotNone(msg)

    def test_encode_made_publishable_request(self):
        """Test encoding MadePublishable request."""
        publisher_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        msg = ContinuousQueryCacheCodec.encode_made_publishable_request(
            map_name="map",
            cache_name="cache",
            publisher_id=publisher_id,
        )

        self.assertIsNotNone(msg)


class TestConcurrentAccess(unittest.TestCase):
    """Tests for concurrent access to query caches."""

    def test_concurrent_reads_and_writes(self):
        """Test concurrent read/write operations on QueryCache."""
        mock_map = MagicMock()
        mock_map.entry_set.return_value = set()
        mock_map.add_entry_listener.return_value = "listener-id"

        mock_config = MagicMock()
        mock_config.populate = False
        mock_config.include_value = True
        mock_config.eviction_max_size = 10000
        mock_config.eviction_policy = MagicMock()
        mock_config.eviction_policy.name = "NONE"

        cache = QueryCache(
            name="concurrent-test",
            map_proxy=mock_map,
            config=mock_config,
        )

        errors = []
        num_operations = 100

        def writer():
            try:
                for i in range(num_operations):
                    cache._add_entry_internal(f"key{i}", f"value{i}", fire_event=False)
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(num_operations):
                    cache.get("key50")
                    cache.key_set()
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=writer),
            threading.Thread(target=reader),
            threading.Thread(target=reader),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])

    def test_concurrent_cqc_operations(self):
        """Test concurrent operations on ContinuousQueryCache."""
        mock_map = MagicMock()
        config = ContinuousQueryCacheConfig(name="concurrent-cqc")

        cqc = ContinuousQueryCache(
            name="cqc",
            map_name="map",
            map_proxy=mock_map,
            config=config,
        )

        errors = []

        def add_entries():
            try:
                for i in range(50):
                    cqc._add_entry_internal(f"k{i}", f"v{i}", sequence=i, fire_event=False)
            except Exception as e:
                errors.append(e)

        def read_entries():
            try:
                for _ in range(50):
                    cqc.get_all()
                    cqc.sequence
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=add_entries),
            threading.Thread(target=read_entries),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
