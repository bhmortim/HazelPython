"""Tests for ReplicatedMap proxy."""

import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from hazelcast.proxy.replicated_map import (
    ReplicatedMapProxy,
    EntryEvent,
    EntryListener,
)
from hazelcast.proxy.base import ProxyContext


class TestReplicatedMapProxy(unittest.TestCase):
    """Test cases for ReplicatedMapProxy."""

    def setUp(self):
        """Set up test fixtures."""
        self.context = MagicMock(spec=ProxyContext)
        self.proxy = ReplicatedMapProxy(
            "hz:impl:replicatedMapService",
            "test-replicated-map",
            self.context,
        )

    def tearDown(self):
        """Clean up after tests."""
        self.proxy._destroyed = True

    def test_put_returns_none_for_new_key(self):
        """Test that put returns None for a new key."""
        result = self.proxy.put("key1", "value1")
        self.assertIsNone(result)

    def test_put_with_ttl(self):
        """Test put with TTL parameter."""
        result = self.proxy.put("key1", "value1", ttl=60)
        self.assertIsNone(result)

    def test_put_async_returns_future(self):
        """Test that put_async returns a Future."""
        future = self.proxy.put_async("key1", "value1")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_get_returns_none_for_missing_key(self):
        """Test that get returns None for non-existent key."""
        result = self.proxy.get("nonexistent")
        self.assertIsNone(result)

    def test_get_async_returns_future(self):
        """Test that get_async returns a Future."""
        future = self.proxy.get_async("key1")
        self.assertIsInstance(future, Future)

    def test_remove_returns_none_for_missing_key(self):
        """Test that remove returns None for non-existent key."""
        result = self.proxy.remove("nonexistent")
        self.assertIsNone(result)

    def test_remove_async_returns_future(self):
        """Test that remove_async returns a Future."""
        future = self.proxy.remove_async("key1")
        self.assertIsInstance(future, Future)

    def test_contains_key_returns_false_for_missing_key(self):
        """Test that contains_key returns False for non-existent key."""
        result = self.proxy.contains_key("nonexistent")
        self.assertFalse(result)

    def test_contains_key_async_returns_future(self):
        """Test that contains_key_async returns a Future."""
        future = self.proxy.contains_key_async("key1")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_contains_value_returns_false(self):
        """Test that contains_value returns False for non-existent value."""
        result = self.proxy.contains_value("nonexistent")
        self.assertFalse(result)

    def test_key_set_returns_empty_set(self):
        """Test that key_set returns an empty set."""
        result = self.proxy.key_set()
        self.assertEqual(result, set())

    def test_key_set_async_returns_future(self):
        """Test that key_set_async returns a Future."""
        future = self.proxy.key_set_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), set())

    def test_values_returns_empty_list(self):
        """Test that values returns an empty list."""
        result = self.proxy.values()
        self.assertEqual(result, [])

    def test_values_async_returns_future(self):
        """Test that values_async returns a Future."""
        future = self.proxy.values_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), [])

    def test_entry_set_returns_empty_set(self):
        """Test that entry_set returns an empty set."""
        result = self.proxy.entry_set()
        self.assertEqual(result, set())

    def test_entry_set_async_returns_future(self):
        """Test that entry_set_async returns a Future."""
        future = self.proxy.entry_set_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), set())

    def test_size_returns_zero(self):
        """Test that size returns 0 for empty map."""
        result = self.proxy.size()
        self.assertEqual(result, 0)

    def test_size_async_returns_future(self):
        """Test that size_async returns a Future."""
        future = self.proxy.size_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 0)

    def test_is_empty_returns_true(self):
        """Test that is_empty returns True for empty map."""
        result = self.proxy.is_empty()
        self.assertTrue(result)

    def test_is_empty_async_returns_future(self):
        """Test that is_empty_async returns a Future."""
        future = self.proxy.is_empty_async()
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_clear(self):
        """Test that clear completes without error."""
        self.proxy.clear()

    def test_clear_async_returns_future(self):
        """Test that clear_async returns a Future."""
        future = self.proxy.clear_async()
        self.assertIsInstance(future, Future)

    def test_put_all(self):
        """Test that put_all completes without error."""
        self.proxy.put_all({"key1": "value1", "key2": "value2"})

    def test_put_all_async_returns_future(self):
        """Test that put_all_async returns a Future."""
        future = self.proxy.put_all_async({"key1": "value1"})
        self.assertIsInstance(future, Future)


class TestReplicatedMapEntryListener(unittest.TestCase):
    """Test cases for ReplicatedMap entry listeners."""

    def setUp(self):
        """Set up test fixtures."""
        self.context = MagicMock(spec=ProxyContext)
        self.proxy = ReplicatedMapProxy(
            "hz:impl:replicatedMapService",
            "test-replicated-map",
            self.context,
        )

    def tearDown(self):
        """Clean up after tests."""
        self.proxy._destroyed = True

    def test_add_entry_listener_returns_registration_id(self):
        """Test that add_entry_listener returns a registration ID."""
        listener = EntryListener()
        reg_id = self.proxy.add_entry_listener(listener)
        self.assertIsInstance(reg_id, str)
        self.assertTrue(len(reg_id) > 0)

    def test_add_entry_listener_to_key(self):
        """Test add_entry_listener_to_key."""
        listener = EntryListener()
        reg_id = self.proxy.add_entry_listener_to_key(listener, "specific_key")
        self.assertIsInstance(reg_id, str)

    def test_add_entry_listener_with_predicate(self):
        """Test add_entry_listener_with_predicate."""
        listener = EntryListener()
        predicate = MagicMock()
        reg_id = self.proxy.add_entry_listener_with_predicate(listener, predicate)
        self.assertIsInstance(reg_id, str)

    def test_remove_entry_listener_success(self):
        """Test successful removal of entry listener."""
        listener = EntryListener()
        reg_id = self.proxy.add_entry_listener(listener)
        result = self.proxy.remove_entry_listener(reg_id)
        self.assertTrue(result)

    def test_remove_entry_listener_not_found(self):
        """Test removal of non-existent listener."""
        result = self.proxy.remove_entry_listener("nonexistent-id")
        self.assertFalse(result)

    def test_multiple_listeners(self):
        """Test adding and removing multiple listeners."""
        listener1 = EntryListener()
        listener2 = EntryListener()
        
        reg_id1 = self.proxy.add_entry_listener(listener1)
        reg_id2 = self.proxy.add_entry_listener(listener2)
        
        self.assertNotEqual(reg_id1, reg_id2)
        
        self.assertTrue(self.proxy.remove_entry_listener(reg_id1))
        self.assertTrue(self.proxy.remove_entry_listener(reg_id2))


class TestReplicatedMapDunderMethods(unittest.TestCase):
    """Test cases for ReplicatedMap dunder methods."""

    def setUp(self):
        """Set up test fixtures."""
        self.context = MagicMock(spec=ProxyContext)
        self.proxy = ReplicatedMapProxy(
            "hz:impl:replicatedMapService",
            "test-replicated-map",
            self.context,
        )

    def tearDown(self):
        """Clean up after tests."""
        self.proxy._destroyed = True

    def test_len(self):
        """Test __len__ method."""
        self.assertEqual(len(self.proxy), 0)

    def test_contains(self):
        """Test __contains__ method."""
        self.assertFalse("key1" in self.proxy)

    def test_getitem(self):
        """Test __getitem__ method."""
        result = self.proxy["key1"]
        self.assertIsNone(result)

    def test_setitem(self):
        """Test __setitem__ method."""
        self.proxy["key1"] = "value1"

    def test_delitem(self):
        """Test __delitem__ method."""
        del self.proxy["key1"]

    def test_iter(self):
        """Test __iter__ method."""
        keys = list(self.proxy)
        self.assertEqual(keys, [])


class TestEntryEvent(unittest.TestCase):
    """Test cases for EntryEvent."""

    def test_entry_event_properties(self):
        """Test EntryEvent properties."""
        event = EntryEvent(
            event_type=EntryEvent.ADDED,
            key="test_key",
            value="test_value",
            old_value="old_value",
            member=None,
        )
        
        self.assertEqual(event.event_type, EntryEvent.ADDED)
        self.assertEqual(event.key, "test_key")
        self.assertEqual(event.value, "test_value")
        self.assertEqual(event.old_value, "old_value")
        self.assertIsNone(event.member)

    def test_entry_event_types(self):
        """Test EntryEvent type constants."""
        self.assertEqual(EntryEvent.ADDED, 1)
        self.assertEqual(EntryEvent.REMOVED, 2)
        self.assertEqual(EntryEvent.UPDATED, 4)
        self.assertEqual(EntryEvent.EVICTED, 8)


class TestEntryListener(unittest.TestCase):
    """Test cases for EntryListener."""

    def test_entry_listener_default_methods(self):
        """Test that EntryListener default methods don't raise."""
        listener = EntryListener()
        event = EntryEvent(EntryEvent.ADDED, "key", "value")
        
        listener.entry_added(event)
        listener.entry_removed(event)
        listener.entry_updated(event)
        listener.entry_evicted(event)
        listener.map_cleared(event)


class TestReplicatedMapDestroyedState(unittest.TestCase):
    """Test cases for destroyed state handling."""

    def setUp(self):
        """Set up test fixtures."""
        self.context = MagicMock(spec=ProxyContext)
        self.proxy = ReplicatedMapProxy(
            "hz:impl:replicatedMapService",
            "test-replicated-map",
            self.context,
        )

    def test_operations_raise_when_destroyed(self):
        """Test that operations raise when proxy is destroyed."""
        self.proxy._destroyed = True
        
        from hazelcast.exceptions import IllegalStateException
        
        with self.assertRaises(IllegalStateException):
            self.proxy.put("key", "value")
        
        with self.assertRaises(IllegalStateException):
            self.proxy.get("key")
        
        with self.assertRaises(IllegalStateException):
            self.proxy.remove("key")
        
        with self.assertRaises(IllegalStateException):
            self.proxy.size()


if __name__ == "__main__":
    unittest.main()
