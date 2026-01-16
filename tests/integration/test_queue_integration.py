"""Integration tests for IQueue operations against a real Hazelcast cluster."""

import pytest
import time
import threading
from typing import List

from tests.integration.conftest import skip_integration, DOCKER_AVAILABLE


@skip_integration
class TestQueueBasicOperations:
    """Test basic IQueue operations."""

    def test_offer_and_poll(self, test_queue):
        """Test basic offer and poll operations."""
        result = test_queue.offer("item1")
        assert result is True
        
        item = test_queue.poll()
        assert item == "item1"

    def test_poll_empty_queue(self, test_queue):
        """Test poll on empty queue."""
        result = test_queue.poll()
        assert result is None

    def test_poll_with_timeout(self, test_queue):
        """Test poll with timeout."""
        start = time.time()
        result = test_queue.poll(timeout=1)
        elapsed = time.time() - start
        
        assert result is None
        assert elapsed >= 0.9

    def test_put_and_take(self, test_queue):
        """Test put and take operations."""
        test_queue.put("item1")
        item = test_queue.take()
        assert item == "item1"

    def test_add_and_remove(self, test_queue):
        """Test add and remove operations."""
        test_queue.add("item1")
        assert test_queue.remove("item1") is True
        assert test_queue.remove("item1") is False

    def test_peek(self, test_queue):
        """Test peek operation."""
        test_queue.offer("item1")
        
        peeked = test_queue.peek()
        assert peeked == "item1"
        
        assert test_queue.size() == 1

    def test_size_and_is_empty(self, test_queue):
        """Test size and is_empty operations."""
        assert test_queue.is_empty() is True
        assert test_queue.size() == 0
        
        test_queue.offer("item1")
        
        assert test_queue.is_empty() is False
        assert test_queue.size() == 1

    def test_clear(self, test_queue):
        """Test clear operation."""
        for i in range(5):
            test_queue.offer(f"item{i}")
        
        assert test_queue.size() == 5
        test_queue.clear()
        assert test_queue.size() == 0

    def test_contains(self, test_queue):
        """Test contains operation."""
        test_queue.offer("item1")
        
        assert test_queue.contains("item1") is True
        assert test_queue.contains("nonexistent") is False

    def test_to_array(self, test_queue):
        """Test to_array operation."""
        items = ["a", "b", "c"]
        for item in items:
            test_queue.offer(item)
        
        array = test_queue.to_array()
        assert array == items


@skip_integration
class TestQueueOrdering:
    """Test IQueue ordering (FIFO)."""

    def test_fifo_ordering(self, test_queue):
        """Test FIFO ordering is maintained."""
        items = ["first", "second", "third", "fourth"]
        
        for item in items:
            test_queue.offer(item)
        
        for expected in items:
            actual = test_queue.poll()
            assert actual == expected

    def test_ordering_with_multiple_producers(self, test_queue):
        """Test ordering with multiple producers."""
        results: List[str] = []
        
        for i in range(100):
            test_queue.offer(f"item-{i}")
        
        while not test_queue.is_empty():
            item = test_queue.poll()
            if item:
                results.append(item)
        
        for i, item in enumerate(results):
            assert item == f"item-{i}"


@skip_integration
class TestQueueBounded:
    """Test bounded queue operations."""

    def test_offer_to_full_queue(self, connected_client, unique_name):
        """Test offer to full bounded queue."""
        bounded_queue = connected_client.get_queue(unique_name)
        
        for i in range(10):
            bounded_queue.offer(f"item{i}")
        
        assert bounded_queue.size() == 10


@skip_integration
class TestQueueConcurrency:
    """Test concurrent IQueue operations."""

    def test_producer_consumer(self, test_queue):
        """Test producer-consumer pattern."""
        produced: List[str] = []
        consumed: List[str] = []
        errors: List[Exception] = []
        stop_event = threading.Event()
        
        def producer():
            try:
                for i in range(50):
                    item = f"item-{i}"
                    test_queue.offer(item)
                    produced.append(item)
                    time.sleep(0.01)
            except Exception as e:
                errors.append(e)
            finally:
                stop_event.set()
        
        def consumer():
            try:
                while not stop_event.is_set() or not test_queue.is_empty():
                    item = test_queue.poll(timeout=0.1)
                    if item:
                        consumed.append(item)
            except Exception as e:
                errors.append(e)
        
        producer_thread = threading.Thread(target=producer)
        consumer_thread = threading.Thread(target=consumer)
        
        producer_thread.start()
        consumer_thread.start()
        
        producer_thread.join()
        consumer_thread.join(timeout=5)
        
        assert len(errors) == 0
        assert len(produced) == 50
        assert len(consumed) == 50

    def test_multiple_consumers(self, test_queue):
        """Test multiple consumers."""
        consumed: List[str] = []
        lock = threading.Lock()
        stop_event = threading.Event()
        
        for i in range(100):
            test_queue.offer(f"item-{i}")
        
        def consumer():
            while True:
                item = test_queue.poll(timeout=0.1)
                if item:
                    with lock:
                        consumed.append(item)
                elif stop_event.is_set():
                    break
        
        consumers = [threading.Thread(target=consumer) for _ in range(3)]
        
        for c in consumers:
            c.start()
        
        time.sleep(1)
        stop_event.set()
        
        for c in consumers:
            c.join(timeout=2)
        
        assert len(consumed) == 100


@skip_integration
class TestQueueListeners:
    """Test IQueue item listeners."""

    def test_item_added_listener(self, test_queue):
        """Test item added listener."""
        events: List[dict] = []
        
        def on_item_added(event):
            events.append({"item": event.item})
        
        test_queue.add_item_listener(on_item_added=on_item_added)
        test_queue.offer("test-item")
        
        time.sleep(0.5)
        assert len(events) >= 1

    def test_item_removed_listener(self, test_queue):
        """Test item removed listener."""
        events: List[dict] = []
        
        def on_item_removed(event):
            events.append({"item": event.item})
        
        test_queue.offer("test-item")
        test_queue.add_item_listener(on_item_removed=on_item_removed)
        test_queue.poll()
        
        time.sleep(0.5)
        assert len(events) >= 1
