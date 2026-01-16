"""Unit tests for ReplicatedMap proxy."""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future

from hazelcast.proxy.replicated_map import (
    ReplicatedMapProxy,
    EntryEvent,
    EntryListener,
)


class TestEntryEvent(unittest.TestCase):
    """Tests for EntryEvent class."""

    def test_event_type_constants(self):
        """Test event type constants are defined correctly."""
        self.assertEqual(EntryEvent.ADDED, 1)
        self.assertEqual(EntryEvent.REMOVED, 2)
        self.assertEqual(EntryEvent.UPDATED, 4)
        self.assertEqual(EntryEvent.EVICTED, 8)

    def test_event_initialization(self):
        """Test EntryEvent initialization."""
        event = EntryEvent(
            event_type=EntryEvent.ADDED,
            key="test_key",
            value="test_value",
            old_value="old_value",
            member="member1",
        )
        self.assertEqual(event.event_type, EntryEvent.ADDED)
        self.assertEqual(event.key, "test_key")
        self.assertEqual(event.value, "test_value")
        self.assertEqual(event.old_value, "old_value")
        self.assertEqual(event.member, "member1")

    def test_event_with_defaults(self):
        """Test EntryEvent with default values."""
        event = EntryEvent(event_type=EntryEvent.REMOVED, key="key")
        self.assertEqual(event.event_type, EntryEvent.REMOVED)
        self.assertEqual(event.key, "key")
        self.assertIsNone(event.value)
        self.assertIsNone(event.old_value)
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
        self.assertTrue(hasattr(listener, "map_cleared"))

    def test_listener_methods_are_callable(self):
        """Test that listener methods can be called without error."""
        listener = EntryListener()
        event = EntryEvent(EntryEvent.ADDED, "key", "value")
        listener.entry_added(event)
        listener.entry_removed(event)
        listener.entry_updated(event)
        listener.entry_evicted(event)
        listener.map_cleared(event)


class TestReplicatedMapProxy(unittest.TestCase):
    """Tests for ReplicatedMapProxy class."""

    def setUp(self):
        """Set up test fixtures."""
        self.proxy = ReplicatedMapProxy(
            service_name="hz:impl:replicatedMapService",
            name="test-replicated-map",
            context=None,
        )

    def test_service_name_constant(self):
        """Test SERVICE_NAME is defined correctly."""
        self.assertEqual(
            ReplicatedMapProxy.SERVICE_NAME,
            "hz:impl:replicatedMapService",
        )

    def test_put_returns_none_without_context(self):
        """Test put returns None when no context is available."""
        result = self.proxy.put("key", "value")
        self.assertIsNone(result)

    def test_put_with_ttl(self):
        """Test put with TTL parameter."""
        result = self.proxy.put("key", "value", ttl=60)
        self.assertIsNone(result)

    def test_put_async_returns_future(self):
        """Test put_async returns a Future."""
        future = self.proxy.put_async("key", "value")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_get_returns_none_without_context(self):
        """Test get returns None when no context is available."""
        result = self.proxy.get("key")
        self.assertIsNone(result)

    def test_get_async_returns_future(self):
        """Test get_async returns a Future."""
        future = self.proxy.get_async("key")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_remove_returns_none_without_context(self):
        """Test remove returns None when no context is available."""
        result = self.proxy.remove("key")
        self.assertIsNone(result)

    def test_remove_async_returns_future(self):
        """Test remove_async returns a Future."""
        future = self.proxy.remove_async("key")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_contains_key_returns_false_without_context(self):
        """Test contains_key returns False when no context is available."""
        result = self.proxy.contains_key("key")
        self.assertFalse(result)

    def test_contains_key_async_returns_future(self):
        """Test contains_key_async returns a Future."""
        future = self.proxy.contains_key_async("key")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_contains_value_returns_false_without_context(self):
        """Test contains_value returns False when no context is available."""
        result = self.proxy.contains_value("value")
        self.assertFalse(result)

    def test_contains_value_async_returns_future(self):
        """Test contains_value_async returns a Future."""
        future = self.proxy.contains_value_async("value")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_key_set_returns_empty_set_without_context(self):
        """Test key_set returns empty set when no context is available."""
        result = self.proxy.key_set()
        self.assertEqual(result, set())

    def test_key_set_async_returns_future(self):
        """Test key_set_async returns a Future."""
        future = self.proxy.key_set_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), set())

    def test_values_returns_empty_list_without_context(self):
        """Test values returns empty list when no context is available."""
        result = self.proxy.values()
        self.assertEqual(result, [])

    def test_values_async_returns_future(self):
        """Test values_async returns a Future."""
        future = self.proxy.values_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), [])

    def test_entry_set_returns_empty_set_without_context(self):
        """Test entry_set returns empty set when no context is available."""
        result = self.proxy.entry_set()
        self.assertEqual(result, set())

    def test_entry_set_async_returns_future(self):
        """Test entry_set_async returns a Future."""
        future = self.proxy.entry_set_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), set())

    def test_size_returns_zero_without_context(self):
        """Test size returns 0 when no context is available."""
        result = self.proxy.size()
        self.assertEqual(result, 0)

    def test_size_async_returns_future(self):
        """Test size_async returns a Future."""
        future = self.proxy.size_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 0)

    def test_is_empty_returns_true_without_context(self):
        """Test is_empty returns True when no context is available."""
        result = self.proxy.is_empty()
        self.assertTrue(result)

    def test_is_empty_async_returns_future(self):
        """Test is_empty_async returns a Future."""
        future = self.proxy.is_empty_async()
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_clear_completes_without_error(self):
        """Test clear completes without error."""
        self.proxy.clear()

    def test_clear_async_returns_future(self):
        """Test clear_async returns a Future."""
        future = self.proxy.clear_async()
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_put_all_completes_without_error(self):
        """Test put_all completes without error."""
        self.proxy.put_all({"key1": "value1", "key2": "value2"})

    def test_put_all_async_returns_future(self):
        """Test put_all_async returns a Future."""
        future = self.proxy.put_all_async({"key1": "value1"})
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

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

    def test_add_entry_listener_with_predicate(self):
        """Test add_entry_listener with predicate."""
        listener = EntryListener()
        reg_id = self.proxy.add_entry_listener(listener, predicate="some_predicate")
        self.assertIsInstance(reg_id, str)

    def test_add_entry_listener_to_key(self):
        """Test add_entry_listener_to_key method."""
        listener = EntryListener()
        reg_id = self.proxy.add_entry_listener_to_key(listener, "my_key")
        self.assertIsInstance(reg_id, str)

    def test_add_entry_listener_with_predicate_method(self):
        """Test add_entry_listener_with_predicate method."""
        listener = EntryListener()
        reg_id = self.proxy.add_entry_listener_with_predicate(listener, "predicate")
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


class TestReplicatedMapAlias(unittest.TestCase):
    """Tests for ReplicatedMap alias."""

    def test_replicated_map_alias_exists(self):
        """Test that ReplicatedMap alias points to ReplicatedMapProxy."""
        from hazelcast.proxy.replicated_map import ReplicatedMap
        self.assertIs(ReplicatedMap, ReplicatedMapProxy)


if __name__ == "__main__":
    unittest.main()
