"""Unit tests for the MultiMap proxy."""

import unittest
from concurrent.futures import Future

from hazelcast.proxy.multi_map import MultiMapProxy
from hazelcast.proxy.map import EntryEvent, EntryListener
from hazelcast.proxy.base import ProxyContext


class TestMultiMapProxy(unittest.TestCase):
    """Tests for MultiMapProxy class."""

    def setUp(self):
        self.mmap: MultiMapProxy[str, str] = MultiMapProxy("test-multimap")

    def test_service_name(self):
        self.assertEqual("hz:impl:multiMapService", self.mmap.service_name)

    def test_name(self):
        self.assertEqual("test-multimap", self.mmap.name)

    def test_put(self):
        result = self.mmap.put("key1", "value1")
        self.assertTrue(result)

    def test_put_async(self):
        future = self.mmap.put_async("key1", "value1")
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_get(self):
        result = self.mmap.get("key1")
        self.assertEqual([], result)

    def test_get_async(self):
        future = self.mmap.get_async("key1")
        self.assertIsInstance(future, Future)
        self.assertEqual([], future.result())

    def test_remove(self):
        result = self.mmap.remove("key1", "value1")
        self.assertFalse(result)

    def test_remove_async(self):
        future = self.mmap.remove_async("key1", "value1")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_remove_all(self):
        result = self.mmap.remove_all("key1")
        self.assertEqual([], result)

    def test_remove_all_async(self):
        future = self.mmap.remove_all_async("key1")
        self.assertIsInstance(future, Future)
        self.assertEqual([], future.result())

    def test_contains_key(self):
        result = self.mmap.contains_key("key1")
        self.assertFalse(result)

    def test_contains_key_async(self):
        future = self.mmap.contains_key_async("key1")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_contains_value(self):
        result = self.mmap.contains_value("value1")
        self.assertFalse(result)

    def test_contains_value_async(self):
        future = self.mmap.contains_value_async("value1")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_contains_entry(self):
        result = self.mmap.contains_entry("key1", "value1")
        self.assertFalse(result)

    def test_contains_entry_async(self):
        future = self.mmap.contains_entry_async("key1", "value1")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_size(self):
        result = self.mmap.size()
        self.assertEqual(0, result)

    def test_size_async(self):
        future = self.mmap.size_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(0, future.result())

    def test_value_count(self):
        result = self.mmap.value_count("key1")
        self.assertEqual(0, result)

    def test_value_count_async(self):
        future = self.mmap.value_count_async("key1")
        self.assertIsInstance(future, Future)
        self.assertEqual(0, future.result())

    def test_clear(self):
        self.mmap.clear()

    def test_clear_async(self):
        future = self.mmap.clear_async()
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_key_set(self):
        result = self.mmap.key_set()
        self.assertEqual(set(), result)

    def test_key_set_async(self):
        future = self.mmap.key_set_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(set(), future.result())

    def test_values(self):
        result = self.mmap.values()
        self.assertEqual([], result)

    def test_values_async(self):
        future = self.mmap.values_async()
        self.assertIsInstance(future, Future)
        self.assertEqual([], future.result())

    def test_entry_set(self):
        result = self.mmap.entry_set()
        self.assertEqual(set(), result)

    def test_entry_set_async(self):
        future = self.mmap.entry_set_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(set(), future.result())

    def test_len(self):
        self.assertEqual(0, len(self.mmap))

    def test_contains_dunder(self):
        self.assertFalse("key1" in self.mmap)

    def test_getitem(self):
        result = self.mmap["key1"]
        self.assertEqual([], result)

    def test_repr(self):
        repr_str = repr(self.mmap)
        self.assertIn("MultiMapProxy", repr_str)
        self.assertIn("test-multimap", repr_str)


class TestMultiMapProxyLocking(unittest.TestCase):
    """Tests for MultiMapProxy locking functionality."""

    def setUp(self):
        self.mmap: MultiMapProxy[str, str] = MultiMapProxy("test-multimap")

    def test_lock(self):
        self.mmap.lock("key1")

    def test_lock_with_ttl(self):
        self.mmap.lock("key1", ttl=10.0)

    def test_lock_async(self):
        future = self.mmap.lock_async("key1")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_try_lock(self):
        result = self.mmap.try_lock("key1")
        self.assertTrue(result)

    def test_try_lock_with_timeout(self):
        result = self.mmap.try_lock("key1", timeout=1.0)
        self.assertTrue(result)

    def test_try_lock_with_ttl(self):
        result = self.mmap.try_lock("key1", timeout=1.0, ttl=10.0)
        self.assertTrue(result)

    def test_try_lock_async(self):
        future = self.mmap.try_lock_async("key1")
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_unlock(self):
        self.mmap.unlock("key1")

    def test_unlock_async(self):
        future = self.mmap.unlock_async("key1")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_is_locked(self):
        result = self.mmap.is_locked("key1")
        self.assertFalse(result)

    def test_is_locked_async(self):
        future = self.mmap.is_locked_async("key1")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_force_unlock(self):
        self.mmap.force_unlock("key1")

    def test_force_unlock_async(self):
        future = self.mmap.force_unlock_async("key1")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())


class TestMultiMapProxyDestroyed(unittest.TestCase):
    """Tests for destroyed MultiMapProxy behavior."""

    def setUp(self):
        self.mmap: MultiMapProxy[str, str] = MultiMapProxy("test-multimap")
        self.mmap.destroy()

    def test_is_destroyed(self):
        self.assertTrue(self.mmap.is_destroyed)

    def test_put_raises(self):
        with self.assertRaises(Exception):
            self.mmap.put("key1", "value1")

    def test_get_raises(self):
        with self.assertRaises(Exception):
            self.mmap.get("key1")

    def test_remove_raises(self):
        with self.assertRaises(Exception):
            self.mmap.remove("key1", "value1")

    def test_size_raises(self):
        with self.assertRaises(Exception):
            self.mmap.size()

    def test_contains_key_raises(self):
        with self.assertRaises(Exception):
            self.mmap.contains_key("key1")

    def test_clear_raises(self):
        with self.assertRaises(Exception):
            self.mmap.clear()


class TestMultiMapProxyEntryListener(unittest.TestCase):
    """Tests for MultiMapProxy entry listener functionality."""

    def setUp(self):
        self.mmap: MultiMapProxy[str, str] = MultiMapProxy("test-multimap")
        self.events: list[EntryEvent[str, str]] = []

    def test_add_entry_listener_with_listener_object(self):
        class TestListener(EntryListener[str, str]):
            def __init__(self, events: list):
                self.events = events

            def entry_added(self, event: EntryEvent[str, str]) -> None:
                self.events.append(event)

        listener = TestListener(self.events)
        reg_id = self.mmap.add_entry_listener(listener=listener)
        self.assertIsNotNone(reg_id)
        self.assertIsInstance(reg_id, str)

    def test_add_entry_listener_with_callback(self):
        def on_added(event: EntryEvent[str, str]) -> None:
            self.events.append(event)

        reg_id = self.mmap.add_entry_listener(entry_added=on_added)
        self.assertIsNotNone(reg_id)

    def test_add_entry_listener_with_multiple_callbacks(self):
        added_events: list[EntryEvent[str, str]] = []
        removed_events: list[EntryEvent[str, str]] = []

        def on_added(event: EntryEvent[str, str]) -> None:
            added_events.append(event)

        def on_removed(event: EntryEvent[str, str]) -> None:
            removed_events.append(event)

        reg_id = self.mmap.add_entry_listener(
            entry_added=on_added, entry_removed=on_removed
        )
        self.assertIsNotNone(reg_id)

    def test_add_entry_listener_no_args_raises(self):
        with self.assertRaises(ValueError):
            self.mmap.add_entry_listener()

    def test_remove_entry_listener(self):
        def on_added(event: EntryEvent[str, str]) -> None:
            pass

        reg_id = self.mmap.add_entry_listener(entry_added=on_added)
        result = self.mmap.remove_entry_listener(reg_id)
        self.assertTrue(result)

    def test_remove_entry_listener_not_found(self):
        result = self.mmap.remove_entry_listener("non-existent-id")
        self.assertFalse(result)

    def test_remove_entry_listener_twice(self):
        def on_added(event: EntryEvent[str, str]) -> None:
            pass

        reg_id = self.mmap.add_entry_listener(entry_added=on_added)
        self.assertTrue(self.mmap.remove_entry_listener(reg_id))
        self.assertFalse(self.mmap.remove_entry_listener(reg_id))

    def test_notify_entry_added(self):
        added_keys: list[str] = []

        def on_added(event: EntryEvent[str, str]) -> None:
            added_keys.append(event.key)

        self.mmap.add_entry_listener(entry_added=on_added)
        self.mmap._notify_entry_added("key1", "value1")
        self.assertEqual(["key1"], added_keys)

    def test_notify_entry_added_with_value(self):
        received_events: list[EntryEvent[str, str]] = []

        def on_added(event: EntryEvent[str, str]) -> None:
            received_events.append(event)

        self.mmap.add_entry_listener(entry_added=on_added, include_value=True)
        self.mmap._notify_entry_added("key1", "value1")
        self.assertEqual(1, len(received_events))
        self.assertEqual("key1", received_events[0].key)
        self.assertEqual("value1", received_events[0].value)

    def test_notify_entry_added_without_value(self):
        received_events: list[EntryEvent[str, str]] = []

        def on_added(event: EntryEvent[str, str]) -> None:
            received_events.append(event)

        self.mmap.add_entry_listener(entry_added=on_added, include_value=False)
        self.mmap._notify_entry_added("key1", "value1")
        self.assertEqual(1, len(received_events))
        self.assertEqual("key1", received_events[0].key)
        self.assertIsNone(received_events[0].value)

    def test_notify_entry_removed(self):
        removed_keys: list[str] = []

        def on_removed(event: EntryEvent[str, str]) -> None:
            removed_keys.append(event.key)

        self.mmap.add_entry_listener(entry_removed=on_removed)
        self.mmap._notify_entry_removed("key1", "value1")
        self.assertEqual(["key1"], removed_keys)

    def test_notify_map_cleared(self):
        cleared_count = [0]

        def on_cleared(event: EntryEvent[str, str]) -> None:
            cleared_count[0] += 1

        self.mmap.add_entry_listener(map_cleared=on_cleared)
        self.mmap._notify_map_cleared()
        self.assertEqual(1, cleared_count[0])

    def test_multiple_listeners(self):
        count1 = [0]
        count2 = [0]

        def listener1(event: EntryEvent[str, str]) -> None:
            count1[0] += 1

        def listener2(event: EntryEvent[str, str]) -> None:
            count2[0] += 1

        self.mmap.add_entry_listener(entry_added=listener1)
        self.mmap.add_entry_listener(entry_added=listener2)
        self.mmap._notify_entry_added("key1", "value1")
        self.assertEqual(1, count1[0])
        self.assertEqual(1, count2[0])

    def test_add_listener_on_destroyed_raises(self):
        self.mmap.destroy()
        with self.assertRaises(Exception):
            self.mmap.add_entry_listener(entry_added=lambda e: None)

    def test_entry_event_type_added(self):
        received_events: list[EntryEvent[str, str]] = []

        def on_added(event: EntryEvent[str, str]) -> None:
            received_events.append(event)

        self.mmap.add_entry_listener(entry_added=on_added)
        self.mmap._notify_entry_added("key1", "value1")
        self.assertEqual(EntryEvent.ADDED, received_events[0].event_type)

    def test_entry_event_type_removed(self):
        received_events: list[EntryEvent[str, str]] = []

        def on_removed(event: EntryEvent[str, str]) -> None:
            received_events.append(event)

        self.mmap.add_entry_listener(entry_removed=on_removed)
        self.mmap._notify_entry_removed("key1", "value1")
        self.assertEqual(EntryEvent.REMOVED, received_events[0].event_type)

    def test_entry_event_type_clear_all(self):
        received_events: list[EntryEvent[str, str]] = []

        def on_cleared(event: EntryEvent[str, str]) -> None:
            received_events.append(event)

        self.mmap.add_entry_listener(map_cleared=on_cleared)
        self.mmap._notify_map_cleared()
        self.assertEqual(EntryEvent.CLEAR_ALL, received_events[0].event_type)


class TestMultiMapProxyWithContext(unittest.TestCase):
    """Tests for MultiMapProxy with ProxyContext."""

    def test_create_with_context(self):
        context = ProxyContext(invocation_service=None)
        mmap: MultiMapProxy[str, str] = MultiMapProxy("test-multimap", context=context)
        self.assertEqual("test-multimap", mmap.name)


class TestEntryEventFromMap(unittest.TestCase):
    """Tests for EntryEvent class used by MultiMap."""

    def test_event_type(self):
        event: EntryEvent[str, str] = EntryEvent(EntryEvent.ADDED, "key1", "value1")
        self.assertEqual(EntryEvent.ADDED, event.event_type)

    def test_key(self):
        event: EntryEvent[str, str] = EntryEvent(EntryEvent.ADDED, "key1", "value1")
        self.assertEqual("key1", event.key)

    def test_value(self):
        event: EntryEvent[str, str] = EntryEvent(EntryEvent.ADDED, "key1", "value1")
        self.assertEqual("value1", event.value)

    def test_old_value(self):
        event: EntryEvent[str, str] = EntryEvent(
            EntryEvent.UPDATED, "key1", "new_value", old_value="old_value"
        )
        self.assertEqual("old_value", event.old_value)

    def test_event_types(self):
        self.assertEqual(1, EntryEvent.ADDED)
        self.assertEqual(2, EntryEvent.REMOVED)
        self.assertEqual(4, EntryEvent.UPDATED)
        self.assertEqual(64, EntryEvent.CLEAR_ALL)


if __name__ == "__main__":
    unittest.main()
