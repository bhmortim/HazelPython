"""Unit tests for the Set and List collection proxies."""

import unittest
from concurrent.futures import Future

from hazelcast.proxy.collections import ListProxy, SetProxy
from hazelcast.proxy.queue import ItemEvent, ItemEventType, ItemListener
from hazelcast.proxy.base import ProxyContext


class TestSetProxy(unittest.TestCase):
    """Tests for SetProxy class."""

    def setUp(self):
        self.set: SetProxy[str] = SetProxy("test-set")

    def test_service_name(self):
        self.assertEqual("hz:impl:setService", self.set.service_name)

    def test_name(self):
        self.assertEqual("test-set", self.set.name)

    def test_add(self):
        result = self.set.add("item1")
        self.assertTrue(result)

    def test_add_async(self):
        future = self.set.add_async("item1")
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_remove(self):
        result = self.set.remove("item1")
        self.assertFalse(result)

    def test_remove_async(self):
        future = self.set.remove_async("item1")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_contains(self):
        result = self.set.contains("item1")
        self.assertFalse(result)

    def test_contains_async(self):
        future = self.set.contains_async("item1")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_size(self):
        result = self.set.size()
        self.assertEqual(0, result)

    def test_size_async(self):
        future = self.set.size_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(0, future.result())

    def test_is_empty(self):
        result = self.set.is_empty()
        self.assertTrue(result)

    def test_is_empty_async(self):
        future = self.set.is_empty_async()
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_clear(self):
        self.set.clear()

    def test_clear_async(self):
        future = self.set.clear_async()
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_get_all(self):
        result = self.set.get_all()
        self.assertEqual([], result)

    def test_get_all_async(self):
        future = self.set.get_all_async()
        self.assertIsInstance(future, Future)
        self.assertEqual([], future.result())

    def test_len(self):
        self.assertEqual(0, len(self.set))

    def test_contains_dunder(self):
        self.assertFalse("item1" in self.set)

    def test_iter(self):
        items = list(self.set)
        self.assertEqual([], items)

    def test_repr(self):
        repr_str = repr(self.set)
        self.assertIn("SetProxy", repr_str)
        self.assertIn("test-set", repr_str)


class TestSetProxyDestroyed(unittest.TestCase):
    """Tests for destroyed SetProxy behavior."""

    def setUp(self):
        self.set: SetProxy[str] = SetProxy("test-set")
        self.set.destroy()

    def test_is_destroyed(self):
        self.assertTrue(self.set.is_destroyed)

    def test_add_raises(self):
        with self.assertRaises(Exception):
            self.set.add("item1")

    def test_remove_raises(self):
        with self.assertRaises(Exception):
            self.set.remove("item1")

    def test_size_raises(self):
        with self.assertRaises(Exception):
            self.set.size()


class TestSetProxyItemListener(unittest.TestCase):
    """Tests for SetProxy item listener functionality."""

    def setUp(self):
        self.set: SetProxy[str] = SetProxy("test-set")
        self.events: list[ItemEvent[str]] = []

    def test_add_item_listener_with_listener_object(self):
        class TestListener(ItemListener[str]):
            def __init__(self, events: list):
                self.events = events

            def item_added(self, event: ItemEvent[str]) -> None:
                self.events.append(event)

        listener = TestListener(self.events)
        reg_id = self.set.add_item_listener(listener=listener)
        self.assertIsNotNone(reg_id)
        self.assertIsInstance(reg_id, str)

    def test_add_item_listener_with_callback(self):
        def on_added(event: ItemEvent[str]) -> None:
            self.events.append(event)

        reg_id = self.set.add_item_listener(item_added=on_added)
        self.assertIsNotNone(reg_id)

    def test_add_item_listener_no_args_raises(self):
        with self.assertRaises(ValueError):
            self.set.add_item_listener()

    def test_remove_item_listener(self):
        def on_added(event: ItemEvent[str]) -> None:
            pass

        reg_id = self.set.add_item_listener(item_added=on_added)
        result = self.set.remove_item_listener(reg_id)
        self.assertTrue(result)

    def test_remove_item_listener_not_found(self):
        result = self.set.remove_item_listener("non-existent-id")
        self.assertFalse(result)

    def test_notify_item_added(self):
        added_items: list[str] = []

        def on_added(event: ItemEvent[str]) -> None:
            if event.item:
                added_items.append(event.item)

        self.set.add_item_listener(item_added=on_added)
        self.set._notify_item_added("test-item")
        self.assertEqual(["test-item"], added_items)

    def test_notify_item_removed(self):
        removed_items: list[str] = []

        def on_removed(event: ItemEvent[str]) -> None:
            if event.item:
                removed_items.append(event.item)

        self.set.add_item_listener(item_removed=on_removed)
        self.set._notify_item_removed("test-item")
        self.assertEqual(["test-item"], removed_items)

    def test_notify_without_value(self):
        received_events: list[ItemEvent[str]] = []

        def on_added(event: ItemEvent[str]) -> None:
            received_events.append(event)

        self.set.add_item_listener(item_added=on_added, include_value=False)
        self.set._notify_item_added("test-item")
        self.assertEqual(1, len(received_events))
        self.assertIsNone(received_events[0].item)

    def test_add_listener_on_destroyed_raises(self):
        self.set.destroy()
        with self.assertRaises(Exception):
            self.set.add_item_listener(item_added=lambda e: None)


class TestListProxy(unittest.TestCase):
    """Tests for ListProxy class."""

    def setUp(self):
        self.list: ListProxy[str] = ListProxy("test-list")

    def test_service_name(self):
        self.assertEqual("hz:impl:listService", self.list.service_name)

    def test_name(self):
        self.assertEqual("test-list", self.list.name)

    def test_add(self):
        result = self.list.add("item1")
        self.assertTrue(result)

    def test_add_async(self):
        future = self.list.add_async("item1")
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_add_at(self):
        self.list.add_at(0, "item1")

    def test_add_at_async(self):
        future = self.list.add_at_async(0, "item1")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_get(self):
        result = self.list.get(0)
        self.assertIsNone(result)

    def test_get_async(self):
        future = self.list.get_async(0)
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_set(self):
        result = self.list.set(0, "item1")
        self.assertIsNone(result)

    def test_set_async(self):
        future = self.list.set_async(0, "item1")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_remove(self):
        result = self.list.remove("item1")
        self.assertFalse(result)

    def test_remove_async(self):
        future = self.list.remove_async("item1")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_remove_at(self):
        result = self.list.remove_at(0)
        self.assertIsNone(result)

    def test_remove_at_async(self):
        future = self.list.remove_at_async(0)
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_contains(self):
        result = self.list.contains("item1")
        self.assertFalse(result)

    def test_contains_async(self):
        future = self.list.contains_async("item1")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_index_of(self):
        result = self.list.index_of("item1")
        self.assertEqual(-1, result)

    def test_index_of_async(self):
        future = self.list.index_of_async("item1")
        self.assertIsInstance(future, Future)
        self.assertEqual(-1, future.result())

    def test_sub_list(self):
        result = self.list.sub_list(0, 5)
        self.assertEqual([], result)

    def test_sub_list_async(self):
        future = self.list.sub_list_async(0, 5)
        self.assertIsInstance(future, Future)
        self.assertEqual([], future.result())

    def test_size(self):
        result = self.list.size()
        self.assertEqual(0, result)

    def test_size_async(self):
        future = self.list.size_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(0, future.result())

    def test_is_empty(self):
        result = self.list.is_empty()
        self.assertTrue(result)

    def test_is_empty_async(self):
        future = self.list.is_empty_async()
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_clear(self):
        self.list.clear()

    def test_clear_async(self):
        future = self.list.clear_async()
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_get_all(self):
        result = self.list.get_all()
        self.assertEqual([], result)

    def test_get_all_async(self):
        future = self.list.get_all_async()
        self.assertIsInstance(future, Future)
        self.assertEqual([], future.result())

    def test_len(self):
        self.assertEqual(0, len(self.list))

    def test_contains_dunder(self):
        self.assertFalse("item1" in self.list)

    def test_getitem(self):
        result = self.list[0]
        self.assertIsNone(result)

    def test_setitem(self):
        self.list[0] = "item1"

    def test_delitem(self):
        del self.list[0]

    def test_iter(self):
        items = list(self.list)
        self.assertEqual([], items)

    def test_repr(self):
        repr_str = repr(self.list)
        self.assertIn("ListProxy", repr_str)
        self.assertIn("test-list", repr_str)


class TestListProxyDestroyed(unittest.TestCase):
    """Tests for destroyed ListProxy behavior."""

    def setUp(self):
        self.list: ListProxy[str] = ListProxy("test-list")
        self.list.destroy()

    def test_is_destroyed(self):
        self.assertTrue(self.list.is_destroyed)

    def test_add_raises(self):
        with self.assertRaises(Exception):
            self.list.add("item1")

    def test_get_raises(self):
        with self.assertRaises(Exception):
            self.list.get(0)

    def test_remove_at_raises(self):
        with self.assertRaises(Exception):
            self.list.remove_at(0)

    def test_size_raises(self):
        with self.assertRaises(Exception):
            self.list.size()


class TestListProxyItemListener(unittest.TestCase):
    """Tests for ListProxy item listener functionality."""

    def setUp(self):
        self.list: ListProxy[str] = ListProxy("test-list")
        self.events: list[ItemEvent[str]] = []

    def test_add_item_listener_with_listener_object(self):
        class TestListener(ItemListener[str]):
            def __init__(self, events: list):
                self.events = events

            def item_added(self, event: ItemEvent[str]) -> None:
                self.events.append(event)

        listener = TestListener(self.events)
        reg_id = self.list.add_item_listener(listener=listener)
        self.assertIsNotNone(reg_id)
        self.assertIsInstance(reg_id, str)

    def test_add_item_listener_with_callback(self):
        def on_added(event: ItemEvent[str]) -> None:
            self.events.append(event)

        reg_id = self.list.add_item_listener(item_added=on_added)
        self.assertIsNotNone(reg_id)

    def test_add_item_listener_with_multiple_callbacks(self):
        added_events: list[ItemEvent[str]] = []
        removed_events: list[ItemEvent[str]] = []

        def on_added(event: ItemEvent[str]) -> None:
            added_events.append(event)

        def on_removed(event: ItemEvent[str]) -> None:
            removed_events.append(event)

        reg_id = self.list.add_item_listener(
            item_added=on_added, item_removed=on_removed
        )
        self.assertIsNotNone(reg_id)

    def test_add_item_listener_no_args_raises(self):
        with self.assertRaises(ValueError):
            self.list.add_item_listener()

    def test_remove_item_listener(self):
        def on_added(event: ItemEvent[str]) -> None:
            pass

        reg_id = self.list.add_item_listener(item_added=on_added)
        result = self.list.remove_item_listener(reg_id)
        self.assertTrue(result)

    def test_remove_item_listener_not_found(self):
        result = self.list.remove_item_listener("non-existent-id")
        self.assertFalse(result)

    def test_remove_item_listener_twice(self):
        def on_added(event: ItemEvent[str]) -> None:
            pass

        reg_id = self.list.add_item_listener(item_added=on_added)
        self.assertTrue(self.list.remove_item_listener(reg_id))
        self.assertFalse(self.list.remove_item_listener(reg_id))

    def test_notify_item_added(self):
        added_items: list[str] = []

        def on_added(event: ItemEvent[str]) -> None:
            if event.item:
                added_items.append(event.item)

        self.list.add_item_listener(item_added=on_added)
        self.list._notify_item_added("test-item")
        self.assertEqual(["test-item"], added_items)

    def test_notify_item_removed(self):
        removed_items: list[str] = []

        def on_removed(event: ItemEvent[str]) -> None:
            if event.item:
                removed_items.append(event.item)

        self.list.add_item_listener(item_removed=on_removed)
        self.list._notify_item_removed("test-item")
        self.assertEqual(["test-item"], removed_items)

    def test_notify_without_value(self):
        received_events: list[ItemEvent[str]] = []

        def on_added(event: ItemEvent[str]) -> None:
            received_events.append(event)

        self.list.add_item_listener(item_added=on_added, include_value=False)
        self.list._notify_item_added("test-item")
        self.assertEqual(1, len(received_events))
        self.assertIsNone(received_events[0].item)

    def test_multiple_listeners(self):
        count1 = [0]
        count2 = [0]

        def listener1(event: ItemEvent[str]) -> None:
            count1[0] += 1

        def listener2(event: ItemEvent[str]) -> None:
            count2[0] += 1

        self.list.add_item_listener(item_added=listener1)
        self.list.add_item_listener(item_added=listener2)
        self.list._notify_item_added("test-item")
        self.assertEqual(1, count1[0])
        self.assertEqual(1, count2[0])

    def test_add_listener_on_destroyed_raises(self):
        self.list.destroy()
        with self.assertRaises(Exception):
            self.list.add_item_listener(item_added=lambda e: None)


class TestCollectionsWithContext(unittest.TestCase):
    """Tests for collection proxies with ProxyContext."""

    def test_set_with_context(self):
        context = ProxyContext(invocation_service=None)
        s: SetProxy[str] = SetProxy("test-set", context=context)
        self.assertEqual("test-set", s.name)

    def test_list_with_context(self):
        context = ProxyContext(invocation_service=None)
        lst: ListProxy[str] = ListProxy("test-list", context=context)
        self.assertEqual("test-list", lst.name)


if __name__ == "__main__":
    unittest.main()
