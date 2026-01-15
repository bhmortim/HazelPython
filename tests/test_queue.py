"""Unit tests for the Queue proxy."""

import unittest
from concurrent.futures import Future

from hazelcast.proxy.queue import (
    ItemEvent,
    ItemEventType,
    ItemListener,
    QueueProxy,
)
from hazelcast.proxy.base import ProxyContext


class TestItemEvent(unittest.TestCase):
    """Tests for ItemEvent class."""

    def test_event_type(self):
        event = ItemEvent(ItemEventType.ADDED, "item1")
        self.assertEqual(ItemEventType.ADDED, event.event_type)

    def test_item(self):
        event = ItemEvent(ItemEventType.ADDED, "item1")
        self.assertEqual("item1", event.item)

    def test_item_none(self):
        event = ItemEvent(ItemEventType.REMOVED)
        self.assertIsNone(event.item)

    def test_member(self):
        member = {"id": "member1"}
        event = ItemEvent(ItemEventType.ADDED, "item1", member=member)
        self.assertEqual(member, event.member)

    def test_name(self):
        event = ItemEvent(ItemEventType.ADDED, "item1", name="test-queue")
        self.assertEqual("test-queue", event.name)

    def test_repr(self):
        event = ItemEvent(ItemEventType.ADDED, "item1")
        repr_str = repr(event)
        self.assertIn("ADDED", repr_str)
        self.assertIn("item1", repr_str)


class TestItemEventType(unittest.TestCase):
    """Tests for ItemEventType enum."""

    def test_added_value(self):
        self.assertEqual(1, ItemEventType.ADDED.value)

    def test_removed_value(self):
        self.assertEqual(2, ItemEventType.REMOVED.value)


class TestItemListener(unittest.TestCase):
    """Tests for ItemListener class."""

    def test_default_item_added(self):
        listener = ItemListener()
        event = ItemEvent(ItemEventType.ADDED, "item")
        listener.item_added(event)

    def test_default_item_removed(self):
        listener = ItemListener()
        event = ItemEvent(ItemEventType.REMOVED, "item")
        listener.item_removed(event)


class TestQueueProxy(unittest.TestCase):
    """Tests for QueueProxy class."""

    def setUp(self):
        self.queue: QueueProxy[str] = QueueProxy("test-queue")

    def test_service_name(self):
        self.assertEqual("hz:impl:queueService", self.queue.service_name)

    def test_name(self):
        self.assertEqual("test-queue", self.queue.name)

    def test_add(self):
        result = self.queue.add("item1")
        self.assertTrue(result)

    def test_add_async(self):
        future = self.queue.add_async("item1")
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_offer(self):
        result = self.queue.offer("item1")
        self.assertTrue(result)

    def test_offer_with_timeout(self):
        result = self.queue.offer("item1", timeout=1.0)
        self.assertTrue(result)

    def test_offer_async(self):
        future = self.queue.offer_async("item1")
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_put(self):
        self.queue.put("item1")

    def test_put_async(self):
        future = self.queue.put_async("item1")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_poll(self):
        result = self.queue.poll()
        self.assertIsNone(result)

    def test_poll_with_timeout(self):
        result = self.queue.poll(timeout=0.1)
        self.assertIsNone(result)

    def test_poll_async(self):
        future = self.queue.poll_async()
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_take(self):
        result = self.queue.take()
        self.assertIsNone(result)

    def test_take_async(self):
        future = self.queue.take_async()
        self.assertIsInstance(future, Future)

    def test_peek(self):
        result = self.queue.peek()
        self.assertIsNone(result)

    def test_peek_async(self):
        future = self.queue.peek_async()
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_remove(self):
        result = self.queue.remove("item1")
        self.assertFalse(result)

    def test_remove_async(self):
        future = self.queue.remove_async("item1")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_contains(self):
        result = self.queue.contains("item1")
        self.assertFalse(result)

    def test_contains_async(self):
        future = self.queue.contains_async("item1")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_contains_all(self):
        result = self.queue.contains_all(["item1", "item2"])
        self.assertFalse(result)

    def test_contains_all_async(self):
        future = self.queue.contains_all_async(["item1", "item2"])
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_size(self):
        result = self.queue.size()
        self.assertEqual(0, result)

    def test_size_async(self):
        future = self.queue.size_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(0, future.result())

    def test_is_empty(self):
        result = self.queue.is_empty()
        self.assertTrue(result)

    def test_is_empty_async(self):
        future = self.queue.is_empty_async()
        self.assertIsInstance(future, Future)
        self.assertTrue(future.result())

    def test_remaining_capacity(self):
        result = self.queue.remaining_capacity()
        self.assertEqual(0, result)

    def test_remaining_capacity_async(self):
        future = self.queue.remaining_capacity_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(0, future.result())

    def test_clear(self):
        self.queue.clear()

    def test_clear_async(self):
        future = self.queue.clear_async()
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_drain_to(self):
        target: list[str] = []
        result = self.queue.drain_to(target)
        self.assertEqual(0, result)

    def test_drain_to_with_max(self):
        target: list[str] = []
        result = self.queue.drain_to(target, max_elements=5)
        self.assertEqual(0, result)

    def test_drain_to_async(self):
        target: list[str] = []
        future = self.queue.drain_to_async(target)
        self.assertIsInstance(future, Future)
        self.assertEqual(0, future.result())

    def test_get_all(self):
        result = self.queue.get_all()
        self.assertEqual([], result)

    def test_get_all_async(self):
        future = self.queue.get_all_async()
        self.assertIsInstance(future, Future)
        self.assertEqual([], future.result())

    def test_len(self):
        self.assertEqual(0, len(self.queue))

    def test_contains_dunder(self):
        self.assertFalse("item1" in self.queue)

    def test_iter(self):
        items = list(self.queue)
        self.assertEqual([], items)

    def test_repr(self):
        repr_str = repr(self.queue)
        self.assertIn("QueueProxy", repr_str)
        self.assertIn("test-queue", repr_str)


class TestQueueProxyDestroyed(unittest.TestCase):
    """Tests for destroyed QueueProxy behavior."""

    def setUp(self):
        self.queue: QueueProxy[str] = QueueProxy("test-queue")
        self.queue.destroy()

    def test_is_destroyed(self):
        self.assertTrue(self.queue.is_destroyed)

    def test_add_raises(self):
        with self.assertRaises(Exception):
            self.queue.add("item1")

    def test_offer_raises(self):
        with self.assertRaises(Exception):
            self.queue.offer("item1")

    def test_poll_raises(self):
        with self.assertRaises(Exception):
            self.queue.poll()

    def test_size_raises(self):
        with self.assertRaises(Exception):
            self.queue.size()


class TestQueueProxyItemListener(unittest.TestCase):
    """Tests for QueueProxy item listener functionality."""

    def setUp(self):
        self.queue: QueueProxy[str] = QueueProxy("test-queue")
        self.events: list[ItemEvent[str]] = []

    def test_add_item_listener_with_listener_object(self):
        class TestListener(ItemListener[str]):
            def __init__(self, events: list):
                self.events = events

            def item_added(self, event: ItemEvent[str]) -> None:
                self.events.append(event)

        listener = TestListener(self.events)
        reg_id = self.queue.add_item_listener(listener=listener)
        self.assertIsNotNone(reg_id)
        self.assertIsInstance(reg_id, str)

    def test_add_item_listener_with_callback(self):
        def on_added(event: ItemEvent[str]) -> None:
            self.events.append(event)

        reg_id = self.queue.add_item_listener(item_added=on_added)
        self.assertIsNotNone(reg_id)

    def test_add_item_listener_with_multiple_callbacks(self):
        added_events: list[ItemEvent[str]] = []
        removed_events: list[ItemEvent[str]] = []

        def on_added(event: ItemEvent[str]) -> None:
            added_events.append(event)

        def on_removed(event: ItemEvent[str]) -> None:
            removed_events.append(event)

        reg_id = self.queue.add_item_listener(
            item_added=on_added, item_removed=on_removed
        )
        self.assertIsNotNone(reg_id)

    def test_add_item_listener_no_args_raises(self):
        with self.assertRaises(ValueError):
            self.queue.add_item_listener()

    def test_remove_item_listener(self):
        def on_added(event: ItemEvent[str]) -> None:
            pass

        reg_id = self.queue.add_item_listener(item_added=on_added)
        result = self.queue.remove_item_listener(reg_id)
        self.assertTrue(result)

    def test_remove_item_listener_not_found(self):
        result = self.queue.remove_item_listener("non-existent-id")
        self.assertFalse(result)

    def test_remove_item_listener_twice(self):
        def on_added(event: ItemEvent[str]) -> None:
            pass

        reg_id = self.queue.add_item_listener(item_added=on_added)
        self.assertTrue(self.queue.remove_item_listener(reg_id))
        self.assertFalse(self.queue.remove_item_listener(reg_id))

    def test_notify_item_added(self):
        added_items: list[str] = []

        def on_added(event: ItemEvent[str]) -> None:
            if event.item:
                added_items.append(event.item)

        self.queue.add_item_listener(item_added=on_added)
        self.queue._notify_item_added("test-item")
        self.assertEqual(["test-item"], added_items)

    def test_notify_item_removed(self):
        removed_items: list[str] = []

        def on_removed(event: ItemEvent[str]) -> None:
            if event.item:
                removed_items.append(event.item)

        self.queue.add_item_listener(item_removed=on_removed)
        self.queue._notify_item_removed("test-item")
        self.assertEqual(["test-item"], removed_items)

    def test_notify_without_value(self):
        received_events: list[ItemEvent[str]] = []

        def on_added(event: ItemEvent[str]) -> None:
            received_events.append(event)

        self.queue.add_item_listener(item_added=on_added, include_value=False)
        self.queue._notify_item_added("test-item")
        self.assertEqual(1, len(received_events))
        self.assertIsNone(received_events[0].item)

    def test_multiple_listeners(self):
        count1 = [0]
        count2 = [0]

        def listener1(event: ItemEvent[str]) -> None:
            count1[0] += 1

        def listener2(event: ItemEvent[str]) -> None:
            count2[0] += 1

        self.queue.add_item_listener(item_added=listener1)
        self.queue.add_item_listener(item_added=listener2)
        self.queue._notify_item_added("test-item")
        self.assertEqual(1, count1[0])
        self.assertEqual(1, count2[0])

    def test_add_listener_on_destroyed_raises(self):
        self.queue.destroy()
        with self.assertRaises(Exception):
            self.queue.add_item_listener(item_added=lambda e: None)


class TestQueueProxyWithContext(unittest.TestCase):
    """Tests for QueueProxy with ProxyContext."""

    def test_create_with_context(self):
        context = ProxyContext(invocation_service=None)
        queue: QueueProxy[str] = QueueProxy("test-queue", context=context)
        self.assertEqual("test-queue", queue.name)


if __name__ == "__main__":
    unittest.main()
