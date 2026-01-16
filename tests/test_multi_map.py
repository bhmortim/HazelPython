"""Unit tests for MultiMap proxy."""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future

from hazelcast.proxy.multi_map import (
    MultiMapProxy,
    _CallbackEntryListener,
)
from hazelcast.proxy.map import EntryEvent, EntryListener


class TestCallbackEntryListener(unittest.TestCase):
    """Tests for _CallbackEntryListener class."""

    def test_callback_entry_added(self):
        """Test entry_added callback is invoked."""
        results = []
        listener = _CallbackEntryListener(
            entry_added=lambda e: results.append(("added", e))
        )
        event = MagicMock()
        listener.entry_added(event)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], "added")

    def test_callback_entry_removed(self):
        """Test entry_removed callback is invoked."""
        results = []
        listener = _CallbackEntryListener(
            entry_removed=lambda e: results.append(("removed", e))
        )
        event = MagicMock()
        listener.entry_removed(event)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], "removed")

    def test_callback_map_cleared(self):
        """Test map_cleared callback is invoked."""
        results = []
        listener = _CallbackEntryListener(
            map_cleared=lambda e: results.append(("cleared", e))
        )
        event = MagicMock()
        listener.map_cleared(event)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][0], "cleared")

    def test_callback_none_handlers(self):
        """Test listener with None handlers doesn't raise."""
        listener = _CallbackEntryListener()
        event = MagicMock()
        listener.entry_added(event)
        listener.entry_removed(event)
        listener.map_cleared(event)


class TestMultiMapProxy(unittest.TestCase):
    """Tests for MultiMapProxy class."""

    def setUp(self):
        """Set up test fixtures."""
        self.proxy = MultiMapProxy(
            name="test-multimap",
            context=None,
        )

    def test_service_name_constant(self):
        """Test SERVICE_NAME is defined correctly."""
        self.assertEqual(MultiMapProxy.SERVICE_NAME, "hz:impl:multiMapService")

    def test_initialization(self):
        """Test MultiMapProxy initialization."""
        self.assertEqual(self.proxy._name, "test-multimap")
        self.assertIsInstance(self.proxy._entry_listeners, dict)

    def test_put_returns_bool(self):
        """Test put returns a boolean."""
        result = self.proxy.put("key", "value")
        self.assertTrue(result)

    def test_put_async_returns_future(self):
        """Test put_async returns a Future."""
        future = self.proxy.put_async("key", "value")
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_get_returns_collection(self):
        """Test get returns a collection."""
        result = self.proxy.get("key")
        self.assertEqual(result, [])

    def test_get_async_returns_future(self):
        """Test get_async returns a Future."""
        future = self.proxy.get_async("key")
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), [])

    def test_remove_returns_bool(self):
        """Test remove returns a boolean."""
        result = self.proxy.remove("key", "value")
        self.assertFalse(result)

    def test_remove_async_returns_future(self):
        """Test remove_async returns a Future."""
        future = self.proxy.remove_async("key", "value")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_remove_all_returns_collection(self):
        """Test remove_all returns a collection."""
        result = self.proxy.remove_all("key")
        self.assertEqual(result, [])

    def test_remove_all_async_returns_future(self):
        """Test remove_all_async returns a Future."""
        future = self.proxy.remove_all_async("key")
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), [])

    def test_contains_key_returns_bool(self):
        """Test contains_key returns a boolean."""
        result = self.proxy.contains_key("key")
        self.assertFalse(result)

    def test_contains_key_async_returns_future(self):
        """Test contains_key_async returns a Future."""
        future = self.proxy.contains_key_async("key")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_contains_value_returns_bool(self):
        """Test contains_value returns a boolean."""
        result = self.proxy.contains_value("value")
        self.assertFalse(result)

    def test_contains_value_async_returns_future(self):
        """Test contains_value_async returns a Future."""
        future = self.proxy.contains_value_async("value")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_contains_entry_returns_bool(self):
        """Test contains_entry returns a boolean."""
        result = self.proxy.contains_entry("key", "value")
        self.assertFalse(result)

    def test_contains_entry_async_returns_future(self):
        """Test contains_entry_async returns a Future."""
        future = self.proxy.contains_entry_async("key", "value")
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

    def test_value_count_returns_int(self):
        """Test value_count returns an integer."""
        result = self.proxy.value_count("key")
        self.assertEqual(result, 0)

    def test_value_count_async_returns_future(self):
        """Test value_count_async returns a Future."""
        future = self.proxy.value_count_async("key")
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 0)

    def test_clear_completes(self):
        """Test clear completes without error."""
        self.proxy.clear()

    def test_clear_async_returns_future(self):
        """Test clear_async returns a Future."""
        future = self.proxy.clear_async()
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_key_set_returns_set(self):
        """Test key_set returns a set."""
        result = self.proxy.key_set()
        self.assertEqual(result, set())

    def test_key_set_async_returns_future(self):
        """Test key_set_async returns a Future."""
        future = self.proxy.key_set_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), set())

    def test_values_returns_collection(self):
        """Test values returns a collection."""
        result = self.proxy.values()
        self.assertEqual(result, [])

    def test_values_async_returns_future(self):
        """Test values_async returns a Future."""
        future = self.proxy.values_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), [])

    def test_entry_set_returns_set(self):
        """Test entry_set returns a set."""
        result = self.proxy.entry_set()
        self.assertEqual(result, set())

    def test_entry_set_async_returns_future(self):
        """Test entry_set_async returns a Future."""
        future = self.proxy.entry_set_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), set())

    def test_add_entry_listener_with_listener(self):
        """Test add_entry_listener with listener object."""
        listener = EntryListener()
        reg_id = self.proxy.add_entry_listener(listener=listener)
        self.assertIsInstance(reg_id, str)

    def test_add_entry_listener_with_callbacks(self):
        """Test add_entry_listener with callbacks."""
        reg_id = self.proxy.add_entry_listener(
            entry_added=lambda e: None,
            entry_removed=lambda e: None,
            map_cleared=lambda e: None,
        )
        self.assertIsInstance(reg_id, str)

    def test_add_entry_listener_with_key(self):
        """Test add_entry_listener with specific key."""
        listener = EntryListener()
        reg_id = self.proxy.add_entry_listener(listener=listener, key="specific_key")
        self.assertIsInstance(reg_id, str)

    def test_add_entry_listener_with_include_value_false(self):
        """Test add_entry_listener with include_value=False."""
        listener = EntryListener()
        reg_id = self.proxy.add_entry_listener(listener=listener, include_value=False)
        self.assertIsInstance(reg_id, str)

    def test_add_entry_listener_raises_without_listener_or_callbacks(self):
        """Test add_entry_listener raises without listener or callbacks."""
        with self.assertRaises(ValueError):
            self.proxy.add_entry_listener()

    def test_remove_entry_listener_returns_true_for_existing(self):
        """Test remove_entry_listener returns True for existing listener."""
        listener = EntryListener()
        reg_id = self.proxy.add_entry_listener(listener=listener)
        result = self.proxy.remove_entry_listener(reg_id)
        self.assertTrue(result)

    def test_remove_entry_listener_returns_false_for_nonexistent(self):
        """Test remove_entry_listener returns False for nonexistent listener."""
        result = self.proxy.remove_entry_listener("nonexistent-id")
        self.assertFalse(result)

    def test_notify_entry_added(self):
        """Test _notify_entry_added calls listener."""
        results = []
        listener = EntryListener()
        listener.entry_added = lambda e: results.append(e)
        self.proxy._entry_listeners["test-id"] = (listener, True)
        self.proxy._notify_entry_added("key", "value")
        self.assertEqual(len(results), 1)

    def test_notify_entry_removed(self):
        """Test _notify_entry_removed calls listener."""
        results = []
        listener = EntryListener()
        listener.entry_removed = lambda e: results.append(e)
        self.proxy._entry_listeners["test-id"] = (listener, True)
        self.proxy._notify_entry_removed("key", "value")
        self.assertEqual(len(results), 1)

    def test_notify_map_cleared(self):
        """Test _notify_map_cleared calls listener."""
        results = []
        listener = EntryListener()
        listener.map_cleared = lambda e: results.append(e)
        self.proxy._entry_listeners["test-id"] = (listener, True)
        self.proxy._notify_map_cleared()
        self.assertEqual(len(results), 1)

    def test_lock_completes(self):
        """Test lock completes without error."""
        self.proxy.lock("key")

    def test_lock_async_returns_future(self):
        """Test lock_async returns a Future."""
        future = self.proxy.lock_async("key")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_lock_with_ttl(self):
        """Test lock with TTL."""
        future = self.proxy.lock_async("key", ttl=60)
        self.assertIsInstance(future, Future)

    def test_try_lock_returns_bool(self):
        """Test try_lock returns a boolean."""
        result = self.proxy.try_lock("key")
        self.assertTrue(result)

    def test_try_lock_async_returns_future(self):
        """Test try_lock_async returns a Future."""
        future = self.proxy.try_lock_async("key")
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_try_lock_with_timeout_and_ttl(self):
        """Test try_lock with timeout and TTL."""
        future = self.proxy.try_lock_async("key", timeout=5, ttl=60)
        self.assertIsInstance(future, Future)

    def test_unlock_completes(self):
        """Test unlock completes without error."""
        self.proxy.unlock("key")

    def test_unlock_async_returns_future(self):
        """Test unlock_async returns a Future."""
        future = self.proxy.unlock_async("key")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_is_locked_returns_bool(self):
        """Test is_locked returns a boolean."""
        result = self.proxy.is_locked("key")
        self.assertFalse(result)

    def test_is_locked_async_returns_future(self):
        """Test is_locked_async returns a Future."""
        future = self.proxy.is_locked_async("key")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_force_unlock_completes(self):
        """Test force_unlock completes without error."""
        self.proxy.force_unlock("key")

    def test_force_unlock_async_returns_future(self):
        """Test force_unlock_async returns a Future."""
        future = self.proxy.force_unlock_async("key")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_len_returns_size(self):
        """Test __len__ returns size."""
        self.assertEqual(len(self.proxy), 0)

    def test_contains_uses_contains_key(self):
        """Test __contains__ uses contains_key."""
        self.assertFalse("key" in self.proxy)

    def test_getitem_uses_get(self):
        """Test __getitem__ uses get."""
        result = self.proxy["key"]
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
