"""Unit tests for Collection proxies (Set and List)."""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future

from hazelcast.proxy.collections import (
    SetProxy,
    ListProxy,
    _CallbackItemListener,
)
from hazelcast.proxy.queue import ItemEvent, ItemEventType, ItemListener


class TestCallbackItemListener(unittest.TestCase):
    """Tests for _CallbackItemListener class."""

    def test_callback_item_added(self):
        """Test item_added callback is invoked."""
        results = []
        listener = _CallbackItemListener(
            item_added=lambda e: results.append(("added", e))
        )
        event = ItemEvent(ItemEventType.ADDED, "item")
        listener.item_added(event)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], "added")

    def test_callback_item_removed(self):
        """Test item_removed callback is invoked."""
        results = []
        listener = _CallbackItemListener(
            item_removed=lambda e: results.append(("removed", e))
        )
        event = ItemEvent(ItemEventType.REMOVED, "item")
        listener.item_removed(event)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], "removed")

    def test_callback_none_handlers(self):
        """Test listener with None handlers doesn't raise."""
        listener = _CallbackItemListener()
        event = ItemEvent(ItemEventType.ADDED, "item")
        listener.item_added(event)
        listener.item_removed(event)


class TestSetProxy(unittest.TestCase):
    """Tests for SetProxy class."""

    def setUp(self):
        """Set up test fixtures."""
        self.proxy = SetProxy(
            name="test-set",
            context=None,
        )

    def test_service_name_constant(self):
        """Test SERVICE_NAME is defined correctly."""
        self.assertEqual(SetProxy.SERVICE_NAME, "hz:impl:setService")

    def test_initialization(self):
        """Test SetProxy initialization."""
        self.assertEqual(self.proxy._name, "test-set")
        self.assertIsInstance(self.proxy._item_listeners, dict)

    def test_add_returns_bool(self):
        """Test add returns a boolean."""
        result = self.proxy.add("item")
        self.assertTrue(result)

    def test_add_async_returns_future(self):
        """Test add_async returns a Future."""
        future = self.proxy.add_async("item")
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_remove_returns_bool(self):
        """Test remove returns a boolean."""
        result = self.proxy.remove("item")
        self.assertFalse(result)

    def test_remove_async_returns_future(self):
        """Test remove_async returns a Future."""
        future = self.proxy.remove_async("item")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_contains_returns_bool(self):
        """Test contains returns a boolean."""
        result = self.proxy.contains("item")
        self.assertFalse(result)

    def test_contains_async_returns_future(self):
        """Test contains_async returns a Future."""
        future = self.proxy.contains_async("item")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_size_returns_int(self):
        """Test size returns an integer."""
        result = self.proxy.size()
        self.assertEqual(result, 0)

    def test_size_async_returns_future(self):
        """Test size_async returns a Future."""
        future = self.proxy.size_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 0)

    def test_is_empty_returns_bool(self):
        """Test is_empty returns a boolean."""
        result = self.proxy.is_empty()
        self.assertTrue(result)

    def test_is_empty_async_returns_future(self):
        """Test is_empty_async returns a Future."""
        future = self.proxy.is_empty_async()
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_clear_completes(self):
        """Test clear completes without error."""
        self.proxy.clear()

    def test_clear_async_returns_future(self):
        """Test clear_async returns a Future."""
        future = self.proxy.clear_async()
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_get_all_returns_list(self):
        """Test get_all returns a list."""
        result = self.proxy.get_all()
        self.assertEqual(result, [])

    def test_get_all_async_returns_future(self):
        """Test get_all_async returns a Future."""
        future = self.proxy.get_all_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), [])

    def test_add_item_listener_with_listener(self):
        """Test add_item_listener with listener object."""
        listener = ItemListener()
        reg_id = self.proxy.add_item_listener(listener=listener)
        self.assertIsInstance(reg_id, str)

    def test_add_item_listener_with_callbacks(self):
        """Test add_item_listener with callbacks."""
        reg_id = self.proxy.add_item_listener(
            item_added=lambda e: None,
            item_removed=lambda e: None,
        )
        self.assertIsInstance(reg_id, str)

    def test_add_item_listener_with_include_value_false(self):
        """Test add_item_listener with include_value=False."""
        listener = ItemListener()
        reg_id = self.proxy.add_item_listener(listener=listener, include_value=False)
        self.assertIsInstance(reg_id, str)

    def test_add_item_listener_raises_without_listener_or_callbacks(self):
        """Test add_item_listener raises without listener or callbacks."""
        with self.assertRaises(ValueError):
            self.proxy.add_item_listener()

    def test_remove_item_listener_returns_true_for_existing(self):
        """Test remove_item_listener returns True for existing listener."""
        listener = ItemListener()
        reg_id = self.proxy.add_item_listener(listener=listener)
        result = self.proxy.remove_item_listener(reg_id)
        self.assertTrue(result)

    def test_remove_item_listener_returns_false_for_nonexistent(self):
        """Test remove_item_listener returns False for nonexistent listener."""
        result = self.proxy.remove_item_listener("nonexistent-id")
        self.assertFalse(result)

    def test_notify_item_added(self):
        """Test _notify_item_added calls listener."""
        results = []
        listener = ItemListener()
        listener.item_added = lambda e: results.append(e)
        self.proxy._item_listeners["test-id"] = (listener, True)
        self.proxy._notify_item_added("test-item")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].item, "test-item")

    def test_notify_item_added_without_value(self):
        """Test _notify_item_added with include_value=False."""
        results = []
        listener = ItemListener()
        listener.item_added = lambda e: results.append(e)
        self.proxy._item_listeners["test-id"] = (listener, False)
        self.proxy._notify_item_added("test-item")
        self.assertEqual(len(results), 1)
        self.assertIsNone(results[0].item)

    def test_notify_item_removed(self):
        """Test _notify_item_removed calls listener."""
        results = []
        listener = ItemListener()
        listener.item_removed = lambda e: results.append(e)
        self.proxy._item_listeners["test-id"] = (listener, True)
        self.proxy._notify_item_removed("test-item")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].item, "test-item")

    def test_len_returns_size(self):
        """Test __len__ returns size."""
        self.assertEqual(len(self.proxy), 0)

    def test_contains_dunder(self):
        """Test __contains__ uses contains."""
        self.assertFalse("item" in self.proxy)

    def test_iter_returns_items(self):
        """Test __iter__ returns iterator over items."""
        result = list(iter(self.proxy))
        self.assertEqual(result, [])


class TestListProxy(unittest.TestCase):
    """Tests for ListProxy class."""

    def setUp(self):
        """Set up test fixtures."""
        self.proxy = ListProxy(
            name="test-list",
            context=None,
        )

    def test_service_name_constant(self):
        """Test SERVICE_NAME is defined correctly."""
        self.assertEqual(ListProxy.SERVICE_NAME, "hz:impl:listService")

    def test_initialization(self):
        """Test ListProxy initialization."""
        self.assertEqual(self.proxy._name, "test-list")
        self.assertIsInstance(self.proxy._item_listeners, dict)

    def test_add_returns_bool(self):
        """Test add returns a boolean."""
        result = self.proxy.add("item")
        self.assertTrue(result)

    def test_add_async_returns_future(self):
        """Test add_async returns a Future."""
        future = self.proxy.add_async("item")
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_add_at_completes(self):
        """Test add_at completes without error."""
        self.proxy.add_at(0, "item")

    def test_add_at_async_returns_future(self):
        """Test add_at_async returns a Future."""
        future = self.proxy.add_at_async(0, "item")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_get_returns_value(self):
        """Test get returns value at index."""
        result = self.proxy.get(0)
        self.assertIsNone(result)

    def test_get_async_returns_future(self):
        """Test get_async returns a Future."""
        future = self.proxy.get_async(0)
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_set_returns_old_value(self):
        """Test set returns old value."""
        result = self.proxy.set(0, "new_item")
        self.assertIsNone(result)

    def test_set_async_returns_future(self):
        """Test set_async returns a Future."""
        future = self.proxy.set_async(0, "new_item")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_remove_returns_bool(self):
        """Test remove returns a boolean."""
        result = self.proxy.remove("item")
        self.assertFalse(result)

    def test_remove_async_returns_future(self):
        """Test remove_async returns a Future."""
        future = self.proxy.remove_async("item")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_remove_at_returns_value(self):
        """Test remove_at returns removed value."""
        result = self.proxy.remove_at(0)
        self.assertIsNone(result)

    def test_remove_at_async_returns_future(self):
        """Test remove_at_async returns a Future."""
        future = self.proxy.remove_at_async(0)
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_contains_returns_bool(self):
        """Test contains returns a boolean."""
        result = self.proxy.contains("item")
        self.assertFalse(result)

    def test_contains_async_returns_future(self):
        """Test contains_async returns a Future."""
        future = self.proxy.contains_async("item")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_index_of_returns_int(self):
        """Test index_of returns index or -1."""
        result = self.proxy.index_of("item")
        self.assertEqual(result, -1)

    def test_index_of_async_returns_future(self):
        """Test index_of_async returns a Future."""
        future = self.proxy.index_of_async("item")
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), -1)

    def test_sub_list_returns_list(self):
        """Test sub_list returns a list."""
        result = self.proxy.sub_list(0, 1)
        self.assertEqual(result, [])

    def test_sub_list_async_returns_future(self):
        """Test sub_list_async returns a Future."""
        future = self.proxy.sub_list_async(0, 1)
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), [])

    def test_size_returns_int(self):
        """Test size returns an integer."""
        result = self.proxy.size()
        self.assertEqual(result, 0)

    def test_size_async_returns_future(self):
        """Test size_async returns a Future."""
        future = self.proxy.size_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 0)

    def test_is_empty_returns_bool(self):
        """Test is_empty returns a boolean."""
        result = self.proxy.is_empty()
        self.assertTrue(result)

    def test_is_empty_async_returns_future(self):
        """Test is_empty_async returns a Future."""
        future = self.proxy.is_empty_async()
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_clear_completes(self):
        """Test clear completes without error."""
        self.proxy.clear()

    def test_clear_async_returns_future(self):
        """Test clear_async returns a Future."""
        future = self.proxy.clear_async()
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_get_all_returns_list(self):
        """Test get_all returns a list."""
        result = self.proxy.get_all()
        self.assertEqual(result, [])

    def test_get_all_async_returns_future(self):
        """Test get_all_async returns a Future."""
        future = self.proxy.get_all_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), [])

    def test_add_item_listener_with_listener(self):
        """Test add_item_listener with listener object."""
        listener = ItemListener()
        reg_id = self.proxy.add_item_listener(listener=listener)
        self.assertIsInstance(reg_id, str)

    def test_add_item_listener_with_callbacks(self):
        """Test add_item_listener with callbacks."""
        reg_id = self.proxy.add_item_listener(
            item_added=lambda e: None,
            item_removed=lambda e: None,
        )
        self.assertIsInstance(reg_id, str)

    def test_add_item_listener_raises_without_listener_or_callbacks(self):
        """Test add_item_listener raises without listener or callbacks."""
        with self.assertRaises(ValueError):
            self.proxy.add_item_listener()

    def test_remove_item_listener_returns_true_for_existing(self):
        """Test remove_item_listener returns True for existing listener."""
        listener = ItemListener()
        reg_id = self.proxy.add_item_listener(listener=listener)
        result = self.proxy.remove_item_listener(reg_id)
        self.assertTrue(result)

    def test_remove_item_listener_returns_false_for_nonexistent(self):
        """Test remove_item_listener returns False for nonexistent listener."""
        result = self.proxy.remove_item_listener("nonexistent-id")
        self.assertFalse(result)

    def test_notify_item_added(self):
        """Test _notify_item_added calls listener."""
        results = []
        listener = ItemListener()
        listener.item_added = lambda e: results.append(e)
        self.proxy._item_listeners["test-id"] = (listener, True)
        self.proxy._notify_item_added("test-item")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].item, "test-item")

    def test_notify_item_removed(self):
        """Test _notify_item_removed calls listener."""
        results = []
        listener = ItemListener()
        listener.item_removed = lambda e: results.append(e)
        self.proxy._item_listeners["test-id"] = (listener, True)
        self.proxy._notify_item_removed("test-item")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].item, "test-item")

    def test_len_returns_size(self):
        """Test __len__ returns size."""
        self.assertEqual(len(self.proxy), 0)

    def test_contains_dunder(self):
        """Test __contains__ uses contains."""
        self.assertFalse("item" in self.proxy)

    def test_getitem_uses_get(self):
        """Test __getitem__ uses get."""
        result = self.proxy[0]
        self.assertIsNone(result)

    def test_setitem_uses_set(self):
        """Test __setitem__ uses set."""
        self.proxy[0] = "value"

    def test_delitem_uses_remove_at(self):
        """Test __delitem__ uses remove_at."""
        del self.proxy[0]

    def test_iter_returns_items(self):
        """Test __iter__ returns iterator over items."""
        result = list(iter(self.proxy))
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
