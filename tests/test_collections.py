"""Unit tests for hazelcast.proxy.collections module."""

import pytest
from concurrent.futures import Future

from hazelcast.proxy.collections import SetProxy, ListProxy
from hazelcast.proxy.queue import ItemEvent, ItemEventType, ItemListener
from hazelcast.exceptions import IllegalStateException


class TestSetProxy:
    """Tests for SetProxy class."""

    def test_init(self):
        set_proxy = SetProxy("test-set")
        assert set_proxy.name == "test-set"
        assert set_proxy.service_name == "hz:impl:setService"

    def test_add(self):
        set_proxy = SetProxy("test-set")
        result = set_proxy.add("item")
        assert result is True

    def test_add_async(self):
        set_proxy = SetProxy("test-set")
        future = set_proxy.add_async("item")
        assert isinstance(future, Future)
        assert future.result() is True

    def test_remove(self):
        set_proxy = SetProxy("test-set")
        result = set_proxy.remove("item")
        assert result is False

    def test_contains(self):
        set_proxy = SetProxy("test-set")
        result = set_proxy.contains("item")
        assert result is False

    def test_size(self):
        set_proxy = SetProxy("test-set")
        assert set_proxy.size() == 0

    def test_is_empty(self):
        set_proxy = SetProxy("test-set")
        assert set_proxy.is_empty() is True

    def test_clear(self):
        set_proxy = SetProxy("test-set")
        set_proxy.clear()

    def test_get_all(self):
        set_proxy = SetProxy("test-set")
        result = set_proxy.get_all()
        assert result == []

    def test_len(self):
        set_proxy = SetProxy("test-set")
        assert len(set_proxy) == 0

    def test_contains_operator(self):
        set_proxy = SetProxy("test-set")
        assert ("item" in set_proxy) is False

    def test_iter(self):
        set_proxy = SetProxy("test-set")
        items = list(set_proxy)
        assert items == []

    def test_add_item_listener_with_listener(self):
        set_proxy = SetProxy("test-set")

        class TestListener(ItemListener):
            def item_added(self, event):
                pass

            def item_removed(self, event):
                pass

        reg_id = set_proxy.add_item_listener(listener=TestListener())
        assert reg_id is not None

    def test_add_item_listener_with_callbacks(self):
        set_proxy = SetProxy("test-set")
        events = []
        reg_id = set_proxy.add_item_listener(
            item_added=lambda e: events.append(e)
        )
        assert reg_id is not None

    def test_add_item_listener_no_args_raises(self):
        set_proxy = SetProxy("test-set")
        with pytest.raises(ValueError):
            set_proxy.add_item_listener()

    def test_remove_item_listener(self):
        set_proxy = SetProxy("test-set")
        reg_id = set_proxy.add_item_listener(item_added=lambda e: None)
        assert set_proxy.remove_item_listener(reg_id) is True
        assert set_proxy.remove_item_listener(reg_id) is False

    def test_notify_item_added(self):
        set_proxy = SetProxy("test-set")
        events = []
        set_proxy.add_item_listener(item_added=lambda e: events.append(e))
        set_proxy._notify_item_added("test-item")
        assert len(events) == 1
        assert events[0].item == "test-item"
        assert events[0].event_type == ItemEventType.ADDED

    def test_notify_item_removed(self):
        set_proxy = SetProxy("test-set")
        events = []
        set_proxy.add_item_listener(item_removed=lambda e: events.append(e))
        set_proxy._notify_item_removed("test-item")
        assert len(events) == 1
        assert events[0].item == "test-item"
        assert events[0].event_type == ItemEventType.REMOVED

    def test_destroyed_operations_raise(self):
        set_proxy = SetProxy("test-set")
        set_proxy.destroy()
        with pytest.raises(IllegalStateException):
            set_proxy.add("item")


class TestListProxy:
    """Tests for ListProxy class."""

    def test_init(self):
        list_proxy = ListProxy("test-list")
        assert list_proxy.name == "test-list"
        assert list_proxy.service_name == "hz:impl:listService"

    def test_add(self):
        list_proxy = ListProxy("test-list")
        result = list_proxy.add("item")
        assert result is True

    def test_add_at(self):
        list_proxy = ListProxy("test-list")
        list_proxy.add_at(0, "item")

    def test_get(self):
        list_proxy = ListProxy("test-list")
        result = list_proxy.get(0)
        assert result is None

    def test_set(self):
        list_proxy = ListProxy("test-list")
        result = list_proxy.set(0, "item")
        assert result is None

    def test_remove(self):
        list_proxy = ListProxy("test-list")
        result = list_proxy.remove("item")
        assert result is False

    def test_remove_at(self):
        list_proxy = ListProxy("test-list")
        result = list_proxy.remove_at(0)
        assert result is None

    def test_contains(self):
        list_proxy = ListProxy("test-list")
        result = list_proxy.contains("item")
        assert result is False

    def test_index_of(self):
        list_proxy = ListProxy("test-list")
        result = list_proxy.index_of("item")
        assert result == -1

    def test_sub_list(self):
        list_proxy = ListProxy("test-list")
        result = list_proxy.sub_list(0, 10)
        assert result == []

    def test_size(self):
        list_proxy = ListProxy("test-list")
        assert list_proxy.size() == 0

    def test_is_empty(self):
        list_proxy = ListProxy("test-list")
        assert list_proxy.is_empty() is True

    def test_clear(self):
        list_proxy = ListProxy("test-list")
        list_proxy.clear()

    def test_get_all(self):
        list_proxy = ListProxy("test-list")
        result = list_proxy.get_all()
        assert result == []

    def test_len(self):
        list_proxy = ListProxy("test-list")
        assert len(list_proxy) == 0

    def test_contains_operator(self):
        list_proxy = ListProxy("test-list")
        assert ("item" in list_proxy) is False

    def test_getitem(self):
        list_proxy = ListProxy("test-list")
        result = list_proxy[0]
        assert result is None

    def test_setitem(self):
        list_proxy = ListProxy("test-list")
        list_proxy[0] = "value"

    def test_delitem(self):
        list_proxy = ListProxy("test-list")
        del list_proxy[0]

    def test_iter(self):
        list_proxy = ListProxy("test-list")
        items = list(list_proxy)
        assert items == []

    def test_add_item_listener(self):
        list_proxy = ListProxy("test-list")
        reg_id = list_proxy.add_item_listener(item_added=lambda e: None)
        assert reg_id is not None

    def test_remove_item_listener(self):
        list_proxy = ListProxy("test-list")
        reg_id = list_proxy.add_item_listener(item_added=lambda e: None)
        assert list_proxy.remove_item_listener(reg_id) is True
