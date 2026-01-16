"""Unit tests for hazelcast.proxy.map module."""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from concurrent.futures import Future

from hazelcast.proxy.map import (
    MapProxy,
    EntryView,
    EntryEvent,
    EntryListener,
    IndexType,
    IndexConfig,
    _ReferenceIdGenerator,
    _NearCacheInvalidationListener,
)
from hazelcast.exceptions import IllegalStateException


class TestIndexType:
    """Tests for IndexType constants."""

    def test_values(self):
        assert IndexType.SORTED == 0
        assert IndexType.HASH == 1
        assert IndexType.BITMAP == 2


class TestIndexConfig:
    """Tests for IndexConfig."""

    def test_create(self):
        config = IndexConfig(attributes=["name", "age"])
        assert config.attributes == ["name", "age"]
        assert config.type == IndexType.SORTED
        assert config.name is None

    def test_create_with_options(self):
        config = IndexConfig(
            attributes=["status"],
            index_type=IndexType.HASH,
            name="status_idx",
        )
        assert config.name == "status_idx"
        assert config.type == IndexType.HASH
        assert config.attributes == ["status"]


class TestEntryView:
    """Tests for EntryView."""

    def test_create(self):
        view = EntryView(
            key="k1",
            value="v1",
            hits=5,
            version=2,
        )
        assert view.key == "k1"
        assert view.value == "v1"
        assert view.hits == 5
        assert view.version == 2

    def test_defaults(self):
        view = EntryView(key="k", value="v")
        assert view.cost == 0
        assert view.creation_time == 0
        assert view.expiration_time == 0
        assert view.hits == 0
        assert view.last_access_time == 0
        assert view.last_stored_time == 0
        assert view.last_update_time == 0
        assert view.version == 0
        assert view.ttl == 0
        assert view.max_idle == 0

    def test_repr(self):
        view = EntryView(key="k", value="v", hits=3, version=1)
        repr_str = repr(view)
        assert "EntryView" in repr_str
        assert "k" in repr_str
        assert "v" in repr_str
        assert "hits=3" in repr_str


class TestEntryEvent:
    """Tests for EntryEvent."""

    def test_event_types(self):
        assert EntryEvent.ADDED == 1
        assert EntryEvent.REMOVED == 2
        assert EntryEvent.UPDATED == 4
        assert EntryEvent.EVICTED == 8
        assert EntryEvent.EXPIRED == 16
        assert EntryEvent.CLEAR_ALL == 64

    def test_create(self):
        event = EntryEvent(
            event_type=EntryEvent.ADDED,
            key="k1",
            value="v1",
        )
        assert event.event_type == EntryEvent.ADDED
        assert event.key == "k1"
        assert event.value == "v1"

    def test_with_old_value(self):
        event = EntryEvent(
            event_type=EntryEvent.UPDATED,
            key="k",
            value="new",
            old_value="old",
        )
        assert event.old_value == "old"
        assert event.value == "new"

    def test_with_merging_value(self):
        event = EntryEvent(
            event_type=EntryEvent.MERGED,
            key="k",
            merging_value="merged",
        )
        assert event.merging_value == "merged"

    def test_with_member(self):
        member = MagicMock()
        event = EntryEvent(EntryEvent.ADDED, "k", member=member)
        assert event.member is member


class TestEntryListener:
    """Tests for EntryListener."""

    def test_methods_exist(self):
        listener = EntryListener()
        
        # All methods should exist and do nothing by default
        event = MagicMock()
        listener.entry_added(event)
        listener.entry_removed(event)
        listener.entry_updated(event)
        listener.entry_evicted(event)
        listener.entry_expired(event)
        listener.map_evicted(event)
        listener.map_cleared(event)

    def test_subclass(self):
        events = []
        
        class MyListener(EntryListener):
            def entry_added(self, event):
                events.append(("added", event))
        
        listener = MyListener()
        event = EntryEvent(EntryEvent.ADDED, "k", "v")
        listener.entry_added(event)
        
        assert len(events) == 1
        assert events[0][0] == "added"


class TestReferenceIdGenerator:
    """Tests for _ReferenceIdGenerator."""

    def test_generates_increasing_ids(self):
        gen = _ReferenceIdGenerator()
        id1 = gen.next_id()
        id2 = gen.next_id()
        id3 = gen.next_id()
        
        assert id1 == 1
        assert id2 == 2
        assert id3 == 3

    def test_thread_safety(self):
        import threading
        
        gen = _ReferenceIdGenerator()
        ids = []
        
        def generate():
            for _ in range(100):
                ids.append(gen.next_id())
        
        threads = [threading.Thread(target=generate) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # All IDs should be unique
        assert len(ids) == len(set(ids))


class TestNearCacheInvalidationListener:
    """Tests for _NearCacheInvalidationListener."""

    @pytest.fixture
    def near_cache(self):
        return MagicMock()

    @pytest.fixture
    def listener(self, near_cache):
        return _NearCacheInvalidationListener(near_cache)

    def test_entry_added_invalidates(self, listener, near_cache):
        event = EntryEvent(EntryEvent.ADDED, "key1")
        listener.entry_added(event)
        near_cache.invalidate.assert_called_once_with("key1")

    def test_entry_removed_invalidates(self, listener, near_cache):
        event = EntryEvent(EntryEvent.REMOVED, "key2")
        listener.entry_removed(event)
        near_cache.invalidate.assert_called_once_with("key2")

    def test_entry_updated_invalidates(self, listener, near_cache):
        event = EntryEvent(EntryEvent.UPDATED, "key3")
        listener.entry_updated(event)
        near_cache.invalidate.assert_called_once_with("key3")

    def test_entry_evicted_invalidates(self, listener, near_cache):
        event = EntryEvent(EntryEvent.EVICTED, "key4")
        listener.entry_evicted(event)
        near_cache.invalidate.assert_called_once_with("key4")

    def test_entry_expired_invalidates(self, listener, near_cache):
        event = EntryEvent(EntryEvent.EXPIRED, "key5")
        listener.entry_expired(event)
        near_cache.invalidate.assert_called_once_with("key5")

    def test_map_evicted_invalidates_all(self, listener, near_cache):
        event = EntryEvent(EntryEvent.EVICT_ALL, None)
        listener.map_evicted(event)
        near_cache.invalidate_all.assert_called_once()

    def test_map_cleared_invalidates_all(self, listener, near_cache):
        event = EntryEvent(EntryEvent.CLEAR_ALL, None)
        listener.map_cleared(event)
        near_cache.invalidate_all.assert_called_once()


class TestMapProxy:
    """Tests for MapProxy."""

    @pytest.fixture
    def mock_context(self):
        context = MagicMock()
        context.serialization_service = MagicMock()
        return context

    @pytest.fixture
    def map_proxy(self, mock_context):
        proxy = MapProxy("test-map", mock_context)
        proxy._destroyed = False
        return proxy

    def test_service_name(self):
        assert MapProxy.SERVICE_NAME == "hz:impl:mapService"

    def test_name(self, map_proxy):
        assert map_proxy.name == "test-map"

    def test_near_cache_none_by_default(self, map_proxy):
        assert map_proxy.near_cache is None

    def test_set_near_cache(self, map_proxy):
        near_cache = MagicMock()
        near_cache.config.invalidate_on_change = False
        
        map_proxy.set_near_cache(near_cache)
        
        assert map_proxy.near_cache is near_cache

    def test_dunder_len(self, map_proxy):
        map_proxy.size = MagicMock(return_value=5)
        assert len(map_proxy) == 5

    def test_dunder_contains(self, map_proxy):
        map_proxy.contains_key = MagicMock(return_value=True)
        assert "key" in map_proxy

    def test_dunder_getitem(self, map_proxy):
        map_proxy.get = MagicMock(return_value="value")
        assert map_proxy["key"] == "value"

    def test_dunder_setitem(self, map_proxy):
        map_proxy.put = MagicMock()
        map_proxy["key"] = "value"
        map_proxy.put.assert_called_once_with("key", "value")

    def test_dunder_delitem(self, map_proxy):
        map_proxy.remove = MagicMock()
        del map_proxy["key"]
        map_proxy.remove.assert_called_once_with("key")

    def test_dunder_iter(self, map_proxy):
        map_proxy.key_set = MagicMock(return_value={"k1", "k2"})
        keys = list(map_proxy)
        assert set(keys) == {"k1", "k2"}
