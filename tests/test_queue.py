"""Unit tests for Queue proxy."""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future

from hazelcast.proxy.queue import (
    QueueProxy,
    ItemEventType,
    ItemEvent,
    ItemListener,
    _CallbackItemListener,
)


class TestItemEventType(unittest.TestCase):
    """Tests for ItemEventType enum."""

    def test_event_type_values(self):
        """Test ItemEventType values are defined correctly."""
        self.assertEqual(ItemEventType.ADDED.value, 1)
        self.assertEqual(ItemEventType.REMOVED.value, 2)


class TestItemEvent(unittest.TestCase):
    """Tests for ItemEvent class."""

    def test_event_initialization(self):
        """Test ItemEvent initialization."""
        event = ItemEvent(
            event_type=ItemEventType.ADDED,
            item="test_item",
            member="member1",
            name="test-queue",
        )
        self.assertEqual(event.event_type, ItemEventType.ADDED)
        self.assertEqual(event.item, "test_item")
        self.assertEqual(event.member, "member1")
        self.assertEqual(event.name, "test-queue")

    def test_event_with_defaults(self):
        """Test ItemEvent with default values."""
        event = ItemEvent(event_type=ItemEventType.REMOVED)
        self.assertEqual(event.event_type, ItemEventType.REMOVED)
        self.assertIsNone(event.item)
        self.assertIsNone(event.member)
        self.assertEqual(event.name, "")

    def test_event_repr(self):
        """Test ItemEvent __repr__."""
        event = ItemEvent(ItemEventType.ADDED, item="test")
        repr_str = repr(event)
        self.assertIn("ItemEvent", repr_str)
        self.assertIn("ADDED", repr_str)
        self.assertIn("test", repr_str)


class TestItemListener(unittest.TestCase):
    """Tests for ItemListener class."""

    def test_listener_methods_exist(self):
        """Test that listener has all required methods."""
        listener = ItemListener()
        self.assertTrue(hasattr(listener, "item_added"))
        self.assertTrue(hasattr(listener, "item_removed"))

    def test_listener_methods_are_callable(self):
        """Test that listener methods can be called without error."""
        listener = ItemListener()
        event = ItemEvent(ItemEventType.ADDED, "item")
        listener.item_added(event)
        listener.item_removed(event)


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


class TestQueueProxy(unittest.TestCase):
    """Tests for QueueProxy class."""

    def setUp(self):
        """Set up test fixtures."""
        self.proxy = QueueProxy(
            name="test-queue",
            context=None,
        )

    def test_service_name_constant(self):
        """Test SERVICE_NAME is defined correctly."""
        self.assertEqual(QueueProxy.SERVICE_NAME, "hz:impl:queueService")

    def test_initialization(self):
        """Test QueueProxy initialization."""
        self.assertEqual(self.proxy._name, "test-queue")
        self.assertIsInstance(self.proxy._item_listeners, dict)

    def test_add_async_returns_future(self):
        """Test add_async returns a Future."""
        future = self.proxy.add_async("item")
        self.assertIsInstance(future, Future)

    def test_offer_async_returns_future(self):
        """Test offer_async returns a Future."""
        future = self.proxy.offer_async("item")
        self.assertIsInstance(future, Future)

    def test_offer_async_with_timeout(self):
        """Test offer_async with timeout."""
        future = self.proxy.offer_async("item", timeout=5)
        self.assertIsInstance(future, Future)

    def test_put_async_returns_future(self):
        """Test put_async returns a Future."""
        future = self.proxy.put_async("item")
        self.assertIsInstance(future, Future)

    def test_poll_async_returns_future(self):
        """Test poll_async returns a Future."""
        future = self.proxy.poll_async()
        self.assertIsInstance(future, Future)

    def test_poll_async_with_timeout(self):
        """Test poll_async with timeout."""
        future = self.proxy.poll_async(timeout=5)
        self.assertIsInstance(future, Future)

    def test_take_async_returns_future(self):
        """Test take_async returns a Future."""
        future = self.proxy.take_async()
        self.assertIsInstance(future, Future)

    def test_peek_async_returns_future(self):
        """Test peek_async returns a Future."""
        future = self.proxy.peek_async()
        self.assertIsInstance(future, Future)

    def test_remove_async_returns_future(self):
        """Test remove_async returns a Future."""
        future = self.proxy.remove_async("item")
        self.assertIsInstance(future, Future)

    def test_contains_async_returns_future(self):
        """Test contains_async returns a Future."""
        future = self.proxy.contains_async("item")
        self.assertIsInstance(future, Future)

    def test_contains_all_async_returns_future(self):
        """Test contains_all_async returns a Future."""
        future = self.proxy.contains_all_async(["item1", "item2"])
        self.assertIsInstance(future, Future)

    def test_drain_to_async_returns_future(self):
        """Test drain_to_async returns a Future."""
        target = []
        future = self.proxy.drain_to_async(target)
        self.assertIsInstance(future, Future)

    def test_drain_to_async_with_max_elements(self):
        """Test drain_to_async with max_elements."""
        target = []
        future = self.proxy.drain_to_async(target, max_elements=5)
        self.assertIsInstance(future, Future)

    def test_size_async_returns_future(self):
        """Test size_async returns a Future."""
        future = self.proxy.size_async()
        self.assertIsInstance(future, Future)

    def test_is_empty_async_returns_future(self):
        """Test is_empty_async returns a Future."""
        future = self.proxy.is_empty_async()
        self.assertIsInstance(future, Future)

    def test_remaining_capacity_async_returns_future(self):
        """Test remaining_capacity_async returns a Future."""
        future = self.proxy.remaining_capacity_async()
        self.assertIsInstance(future, Future)

    def test_clear_async_returns_future(self):
        """Test clear_async returns a Future."""
        future = self.proxy.clear_async()
        self.assertIsInstance(future, Future)

    def test_get_all_async_returns_future(self):
        """Test get_all_async returns a Future."""
        future = self.proxy.get_all_async()
        self.assertIsInstance(future, Future)

    def test_add_item_listener_with_listener(self):
        """Test add_item_listener with listener object."""
        listener = ItemListener()
        reg_id = self.proxy.add_item_listener(listener=listener)
        self.assertIsInstance(reg_id, str)
        self.assertTrue(len(reg_id) > 0)

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
        self.proxy._item_listeners["test-id"] = (listener, True, None)
        self.proxy._notify_item_added("test-item")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].item, "test-item")

    def test_notify_item_added_without_value(self):
        """Test _notify_item_added with include_value=False."""
        results = []
        listener = ItemListener()
        listener.item_added = lambda e: results.append(e)
        self.proxy._item_listeners["test-id"] = (listener, False, None)
        self.proxy._notify_item_added("test-item")
        self.assertEqual(len(results), 1)
        self.assertIsNone(results[0].item)

    def test_notify_item_removed(self):
        """Test _notify_item_removed calls listener."""
        results = []
        listener = ItemListener()
        listener.item_removed = lambda e: results.append(e)
        self.proxy._item_listeners["test-id"] = (listener, True, None)
        self.proxy._notify_item_removed("test-item")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].item, "test-item")

    def test_len_returns_size(self):
        """Test __len__ returns size."""
        self.assertEqual(len(self.proxy), 0)

    def test_contains_uses_contains(self):
        """Test __contains__ uses contains."""
        self.assertFalse("item" in self.proxy)

    def test_iter_returns_items(self):
        """Test __iter__ returns iterator over items."""
        result = list(iter(self.proxy))
        self.assertEqual(result, [])


class TestQueueProxySyncMethods(unittest.TestCase):
    """Tests for QueueProxy synchronous methods."""

    def setUp(self):
        """Set up test fixtures."""
        self.proxy = QueueProxy(
            name="test-queue",
            context=None,
        )

    def test_add_calls_async(self):
        """Test add calls add_async."""
        result = self.proxy.add("item")
        self.assertIsInstance(result, bool)

    def test_offer_calls_async(self):
        """Test offer calls offer_async."""
        result = self.proxy.offer("item")
        self.assertIsInstance(result, bool)

    def test_put_calls_async(self):
        """Test put calls put_async."""
        self.proxy.put("item")

    def test_poll_calls_async(self):
        """Test poll calls poll_async."""
        result = self.proxy.poll()
        self.assertIsNone(result)

    def test_take_calls_async(self):
        """Test take calls take_async."""
        result = self.proxy.take()
        self.assertIsNone(result)

    def test_peek_calls_async(self):
        """Test peek calls peek_async."""
        result = self.proxy.peek()
        self.assertIsNone(result)

    def test_remove_calls_async(self):
        """Test remove calls remove_async."""
        result = self.proxy.remove("item")
        self.assertIsInstance(result, bool)

    def test_contains_calls_async(self):
        """Test contains calls contains_async."""
        result = self.proxy.contains("item")
        self.assertIsInstance(result, bool)

    def test_contains_all_calls_async(self):
        """Test contains_all calls contains_all_async."""
        result = self.proxy.contains_all(["item1", "item2"])
        self.assertIsInstance(result, bool)

    def test_drain_to_calls_async(self):
        """Test drain_to calls drain_to_async."""
        target = []
        result = self.proxy.drain_to(target)
        self.assertIsInstance(result, int)

    def test_size_calls_async(self):
        """Test size calls size_async."""
        result = self.proxy.size()
        self.assertIsInstance(result, int)

    def test_is_empty_calls_async(self):
        """Test is_empty calls is_empty_async."""
        result = self.proxy.is_empty()
        self.assertIsInstance(result, bool)

    def test_remaining_capacity_calls_async(self):
        """Test remaining_capacity calls remaining_capacity_async."""
        result = self.proxy.remaining_capacity()
        self.assertIsInstance(result, int)

    def test_clear_calls_async(self):
        """Test clear calls clear_async."""
        self.proxy.clear()

    def test_get_all_calls_async(self):
        """Test get_all calls get_all_async."""
        result = self.proxy.get_all()
        self.assertIsInstance(result, list)


if __name__ == "__main__":
    unittest.main()
