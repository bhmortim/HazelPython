"""Unit tests for hazelcast.proxy.multi_map module."""

import pytest
from concurrent.futures import Future

from hazelcast.proxy.multi_map import MultiMapProxy
from hazelcast.proxy.map import EntryEvent, EntryListener
from hazelcast.exceptions import IllegalStateException


class TestMultiMapProxy:
    """Tests for MultiMapProxy class."""

    def test_init(self):
        mm = MultiMapProxy("test-mm")
        assert mm.name == "test-mm"
        assert mm.service_name == "hz:impl:multiMapService"

    def test_put(self):
        mm = MultiMapProxy("test-mm")
        result = mm.put("key", "value")
        assert result is True

    def test_put_async(self):
        mm = MultiMapProxy("test-mm")
        future = mm.put_async("key", "value")
        assert isinstance(future, Future)
        assert future.result() is True

    def test_get(self):
        mm = MultiMapProxy("test-mm")
        result = mm.get("key")
        assert result == []

    def test_remove(self):
        mm = MultiMapProxy("test-mm")
        result = mm.remove("key", "value")
        assert result is False

    def test_remove_all(self):
        mm = MultiMapProxy("test-mm")
        result = mm.remove_all("key")
        assert result == []

    def test_contains_key(self):
        mm = MultiMapProxy("test-mm")
        result = mm.contains_key("key")
        assert result is False

    def test_contains_value(self):
        mm = MultiMapProxy("test-mm")
        result = mm.contains_value("value")
        assert result is False

    def test_contains_entry(self):
        mm = MultiMapProxy("test-mm")
        result = mm.contains_entry("key", "value")
        assert result is False

    def test_size(self):
        mm = MultiMapProxy("test-mm")
        assert mm.size() == 0

    def test_value_count(self):
        mm = MultiMapProxy("test-mm")
        assert mm.value_count("key") == 0

    def test_clear(self):
        mm = MultiMapProxy("test-mm")
        mm.clear()

    def test_key_set(self):
        mm = MultiMapProxy("test-mm")
        result = mm.key_set()
        assert result == set()

    def test_values(self):
        mm = MultiMapProxy("test-mm")
        result = mm.values()
        assert result == []

    def test_entry_set(self):
        mm = MultiMapProxy("test-mm")
        result = mm.entry_set()
        assert result == set()

    def test_len(self):
        mm = MultiMapProxy("test-mm")
        assert len(mm) == 0

    def test_contains_operator(self):
        mm = MultiMapProxy("test-mm")
        assert ("key" in mm) is False

    def test_getitem(self):
        mm = MultiMapProxy("test-mm")
        result = mm["key"]
        assert result == []

    def test_lock(self):
        mm = MultiMapProxy("test-mm")
        mm.lock("key")

    def test_lock_with_ttl(self):
        mm = MultiMapProxy("test-mm")
        mm.lock("key", ttl=10.0)

    def test_try_lock(self):
        mm = MultiMapProxy("test-mm")
        result = mm.try_lock("key")
        assert result is True

    def test_try_lock_with_timeout(self):
        mm = MultiMapProxy("test-mm")
        result = mm.try_lock("key", timeout=1.0, ttl=10.0)
        assert result is True

    def test_unlock(self):
        mm = MultiMapProxy("test-mm")
        mm.unlock("key")

    def test_is_locked(self):
        mm = MultiMapProxy("test-mm")
        result = mm.is_locked("key")
        assert result is False

    def test_force_unlock(self):
        mm = MultiMapProxy("test-mm")
        mm.force_unlock("key")

    def test_add_entry_listener_with_listener(self):
        mm = MultiMapProxy("test-mm")

        class TestListener(EntryListener):
            def entry_added(self, event):
                pass

            def entry_removed(self, event):
                pass

        reg_id = mm.add_entry_listener(listener=TestListener())
        assert reg_id is not None

    def test_add_entry_listener_with_callbacks(self):
        mm = MultiMapProxy("test-mm")
        reg_id = mm.add_entry_listener(
            entry_added=lambda e: None,
            entry_removed=lambda e: None,
        )
        assert reg_id is not None

    def test_add_entry_listener_no_args_raises(self):
        mm = MultiMapProxy("test-mm")
        with pytest.raises(ValueError):
            mm.add_entry_listener()

    def test_remove_entry_listener(self):
        mm = MultiMapProxy("test-mm")
        reg_id = mm.add_entry_listener(entry_added=lambda e: None)
        assert mm.remove_entry_listener(reg_id) is True
        assert mm.remove_entry_listener(reg_id) is False

    def test_notify_entry_added(self):
        mm = MultiMapProxy("test-mm")
        events = []
        mm.add_entry_listener(entry_added=lambda e: events.append(e))
        mm._notify_entry_added("key", "value")
        assert len(events) == 1

    def test_notify_entry_removed(self):
        mm = MultiMapProxy("test-mm")
        events = []
        mm.add_entry_listener(entry_removed=lambda e: events.append(e))
        mm._notify_entry_removed("key", "value")
        assert len(events) == 1

    def test_notify_map_cleared(self):
        mm = MultiMapProxy("test-mm")
        events = []
        mm.add_entry_listener(map_cleared=lambda e: events.append(e))
        mm._notify_map_cleared()
        assert len(events) == 1

    def test_destroyed_operations_raise(self):
        mm = MultiMapProxy("test-mm")
        mm.destroy()
        with pytest.raises(IllegalStateException):
            mm.put("key", "value")
