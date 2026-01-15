"""Unit tests for hazelcast.proxy.queue module."""

import pytest
from concurrent.futures import Future

from hazelcast.proxy.queue import (
    QueueProxy,
    ItemEvent,
    ItemEventType,
    ItemListener,
)
from hazelcast.exceptions import IllegalStateException


class TestItemEventType:
    """Tests for ItemEventType enum."""

    def test_values(self):
        assert ItemEventType.ADDED.value == 1
        assert ItemEventType.REMOVED.value == 2


class TestItemEvent:
    """Tests for ItemEvent class."""

    def test_init(self):
        event = ItemEvent(ItemEventType.ADDED, item="test")
        assert event.event_type == ItemEventType.ADDED
        assert event.item == "test"
        assert event.member is None
        assert event.name == ""

    def test_full_init(self):
        member = object()
        event = ItemEvent(
            ItemEventType.REMOVED,
            item="item",
            member=member,
            name="my-queue",
        )
        assert event.event_type == ItemEventType.REMOVED
        assert event.item == "item"
        assert event.member is member
        assert event.name == "my-queue"

    def test_repr(self):
        event = ItemEvent(ItemEventType.ADDED, item="test")
        r = repr(event)
        assert "ADDED" in r
        assert "test" in r


class TestItemListener:
    """Tests for ItemListener class."""

    def test_default_methods(self):
        listener = ItemListener()
        event = ItemEvent(ItemEventType.ADDED)
        listener.item_added(event)
        listener.item_removed(event)


class TestQueueProxy:
    """Tests for QueueProxy class."""

    def test_init(self):
        queue = QueueProxy("test-queue")
        assert queue.name == "test-queue"
        assert queue.service_name == "hz:impl:queueService"

    def test_add(self):
        queue = QueueProxy("test-queue")
        result = queue.add("item")
        assert result is True

    def test_add_async(self):
        queue = QueueProxy("test-queue")
        future = queue.add_async("item")
        assert isinstance(future, Future)
        assert future.result() is True

    def test_offer(self):
        queue = QueueProxy("test-queue")
        result = queue.offer("item")
        assert result is True

    def test_offer_with_timeout(self):
        queue = QueueProxy("test-queue")
        result = queue.offer("item", timeout=5.0)
        assert result is True

    def test_put(self):
        queue = QueueProxy("test-queue")
        queue.put("item")

    def test_poll(self):
        queue = QueueProxy("test-queue")
        result = queue.poll()
        assert result is None

    def test_poll_with_timeout(self):
        queue = QueueProxy("test-queue")
        result = queue.poll(timeout=1.0)
        assert result is None

    def test_take(self):
        queue = QueueProxy("test-queue")
        result = queue.take()
        assert result is None

    def test_peek(self):
        queue = QueueProxy("test-queue")
        result = queue.peek()
        assert result is None

    def test_remove(self):
        queue = QueueProxy("test-queue")
        result = queue.remove("item")
        assert result is False

    def test_contains(self):
        queue = QueueProxy("test-queue")
        result = queue.contains("item")
        assert result is False

    def test_contains_all(self):
        queue = QueueProxy("test-queue")
        result = queue.contains_all(["item1", "item2"])
        assert result is False

    def test_drain_to(self):
        queue = QueueProxy("test-queue")
        target = []
        result = queue.drain_to(target)
        assert result == 0

    def test_drain_to_with_max(self):
        queue = QueueProxy("test-queue")
        target = []
        result = queue.drain_to(target, max_elements=5)
        assert result == 0

    def test_size(self):
        queue = QueueProxy("test-queue")
        assert queue.size() == 0

    def test_is_empty(self):
        queue = QueueProxy("test-queue")
        assert queue.is_empty() is True

    def test_remaining_capacity(self):
        queue = QueueProxy("test-queue")
        assert queue.remaining_capacity() == 0

    def test_clear(self):
        queue = QueueProxy("test-queue")
        queue.clear()

    def test_get_all(self):
        queue = QueueProxy("test-queue")
        result = queue.get_all()
        assert result == []

    def test_len(self):
        queue = QueueProxy("test-queue")
        assert len(queue) == 0

    def test_contains_operator(self):
        queue = QueueProxy("test-queue")
        assert ("item" in queue) is False

    def test_iter(self):
        queue = QueueProxy("test-queue")
        items = list(queue)
        assert items == []

    def test_add_item_listener_with_listener(self):
        queue = QueueProxy("test-queue")

        class TestListener(ItemListener):
            def item_added(self, event):
                pass

            def item_removed(self, event):
                pass

        reg_id = queue.add_item_listener(listener=TestListener())
        assert reg_id is not None

    def test_add_item_listener_with_callbacks(self):
        queue = QueueProxy("test-queue")
        reg_id = queue.add_item_listener(
            item_added=lambda e: None,
            item_removed=lambda e: None,
        )
        assert reg_id is not None

    def test_add_item_listener_include_value(self):
        queue = QueueProxy("test-queue")
        events = []
        queue.add_item_listener(
            item_added=lambda e: events.append(e),
            include_value=True,
        )
        queue._notify_item_added("test")
        assert events[0].item == "test"

    def test_add_item_listener_exclude_value(self):
        queue = QueueProxy("test-queue")
        events = []
        queue.add_item_listener(
            item_added=lambda e: events.append(e),
            include_value=False,
        )
        queue._notify_item_added("test")
        assert events[0].item is None

    def test_add_item_listener_no_args_raises(self):
        queue = QueueProxy("test-queue")
        with pytest.raises(ValueError):
            queue.add_item_listener()

    def test_remove_item_listener(self):
        queue = QueueProxy("test-queue")
        reg_id = queue.add_item_listener(item_added=lambda e: None)
        assert queue.remove_item_listener(reg_id) is True
        assert queue.remove_item_listener(reg_id) is False

    def test_notify_item_added(self):
        queue = QueueProxy("test-queue")
        events = []
        queue.add_item_listener(item_added=lambda e: events.append(e))
        queue._notify_item_added("test-item")
        assert len(events) == 1
        assert events[0].event_type == ItemEventType.ADDED

    def test_notify_item_removed(self):
        queue = QueueProxy("test-queue")
        events = []
        queue.add_item_listener(item_removed=lambda e: events.append(e))
        queue._notify_item_removed("test-item")
        assert len(events) == 1
        assert events[0].event_type == ItemEventType.REMOVED

    def test_destroyed_operations_raise(self):
        queue = QueueProxy("test-queue")
        queue.destroy()
        with pytest.raises(IllegalStateException):
            queue.add("item")
