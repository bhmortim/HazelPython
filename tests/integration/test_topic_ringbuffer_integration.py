"""Integration tests for ITopic and Ringbuffer operations against a real Hazelcast cluster."""

import pytest
import time
import threading
from typing import List

from tests.integration.conftest import skip_integration, DOCKER_AVAILABLE


@skip_integration
class TestTopicBasicOperations:
    """Test basic ITopic operations."""

    def test_publish_and_receive(self, connected_client, unique_name):
        """Test basic publish and receive."""
        topic = connected_client.get_topic(unique_name)
        received: List[str] = []
        
        def listener(message):
            received.append(message.message)
        
        topic.add_listener(listener)
        topic.publish("test-message")
        
        time.sleep(0.5)
        assert len(received) >= 1
        assert "test-message" in received

    def test_multiple_subscribers(self, connected_client, unique_name):
        """Test multiple subscribers receive messages."""
        topic = connected_client.get_topic(unique_name)
        received1: List[str] = []
        received2: List[str] = []
        
        def listener1(message):
            received1.append(message.message)
        
        def listener2(message):
            received2.append(message.message)
        
        topic.add_listener(listener1)
        topic.add_listener(listener2)
        topic.publish("broadcast")
        
        time.sleep(0.5)
        assert "broadcast" in received1
        assert "broadcast" in received2

    def test_remove_listener(self, connected_client, unique_name):
        """Test removing a listener."""
        topic = connected_client.get_topic(unique_name)
        received: List[str] = []
        
        def listener(message):
            received.append(message.message)
        
        reg_id = topic.add_listener(listener)
        topic.publish("first")
        time.sleep(0.3)
        
        topic.remove_listener(reg_id)
        topic.publish("second")
        time.sleep(0.3)
        
        assert "first" in received
        assert "second" not in received

    def test_publish_various_types(self, connected_client, unique_name):
        """Test publishing various data types."""
        topic = connected_client.get_topic(unique_name)
        received: List = []
        
        def listener(message):
            received.append(message.message)
        
        topic.add_listener(listener)
        
        topic.publish("string")
        topic.publish(42)
        topic.publish({"key": "value"})
        topic.publish([1, 2, 3])
        
        time.sleep(0.5)
        assert len(received) >= 4


@skip_integration
class TestTopicConcurrency:
    """Test concurrent ITopic operations."""

    def test_concurrent_publishers(self, connected_client, unique_name):
        """Test concurrent publishers."""
        topic = connected_client.get_topic(unique_name)
        received: List[str] = []
        lock = threading.Lock()
        
        def listener(message):
            with lock:
                received.append(message.message)
        
        topic.add_listener(listener)
        
        def publisher(thread_id: int):
            for i in range(10):
                topic.publish(f"thread-{thread_id}-msg-{i}")
        
        threads = [threading.Thread(target=publisher, args=(i,)) for i in range(5)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        time.sleep(1)
        assert len(received) == 50


@skip_integration
class TestRingbufferBasicOperations:
    """Test basic Ringbuffer operations."""

    def test_add_and_read_one(self, connected_client, unique_name):
        """Test add and read_one operations."""
        rb = connected_client.get_ringbuffer(unique_name)
        
        seq = rb.add("item1")
        assert seq >= 0
        
        item = rb.read_one(seq)
        assert item == "item1"

    def test_add_all(self, connected_client, unique_name):
        """Test add_all operation."""
        rb = connected_client.get_ringbuffer(unique_name)
        
        last_seq = rb.add_all(["a", "b", "c"])
        assert last_seq >= 2
        
        assert rb.read_one(last_seq - 2) == "a"
        assert rb.read_one(last_seq - 1) == "b"
        assert rb.read_one(last_seq) == "c"

    def test_size_and_capacity(self, connected_client, unique_name):
        """Test size and capacity operations."""
        rb = connected_client.get_ringbuffer(unique_name)
        
        assert rb.size() == 0
        
        rb.add("item1")
        rb.add("item2")
        
        assert rb.size() == 2
        assert rb.capacity() > 0

    def test_head_and_tail_sequence(self, connected_client, unique_name):
        """Test head_sequence and tail_sequence."""
        rb = connected_client.get_ringbuffer(unique_name)
        
        rb.add("a")
        rb.add("b")
        rb.add("c")
        
        head = rb.head_sequence()
        tail = rb.tail_sequence()
        
        assert tail >= head
        assert tail - head + 1 == rb.size()

    def test_remaining_capacity(self, connected_client, unique_name):
        """Test remaining_capacity operation."""
        rb = connected_client.get_ringbuffer(unique_name)
        
        initial_capacity = rb.remaining_capacity()
        rb.add("item")
        
        assert rb.remaining_capacity() == initial_capacity - 1

    def test_read_many(self, connected_client, unique_name):
        """Test read_many operation."""
        rb = connected_client.get_ringbuffer(unique_name)
        
        items = ["a", "b", "c", "d", "e"]
        for item in items:
            rb.add(item)
        
        start_seq = rb.head_sequence()
        result = rb.read_many(start_seq, 1, 5)
        
        assert len(result) >= 1
        assert result[0] == "a"


@skip_integration
class TestRingbufferOverflow:
    """Test Ringbuffer overflow behavior."""

    def test_overflow_removes_oldest(self, connected_client, unique_name):
        """Test that overflow removes oldest items."""
        rb = connected_client.get_ringbuffer(unique_name)
        
        capacity = rb.capacity()
        
        for i in range(capacity + 5):
            rb.add(f"item-{i}")
        
        assert rb.size() <= capacity
        
        head_seq = rb.head_sequence()
        first_item = rb.read_one(head_seq)
        assert "item-" in first_item


@skip_integration
class TestRingbufferConcurrency:
    """Test concurrent Ringbuffer operations."""

    def test_concurrent_adds(self, connected_client, unique_name):
        """Test concurrent add operations."""
        rb = connected_client.get_ringbuffer(unique_name)
        errors: List[Exception] = []
        
        def writer(thread_id: int):
            try:
                for i in range(50):
                    rb.add(f"thread-{thread_id}-item-{i}")
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=writer, args=(i,)) for i in range(5)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert rb.size() == min(250, rb.capacity())

    def test_concurrent_read_write(self, connected_client, unique_name):
        """Test concurrent read and write operations."""
        rb = connected_client.get_ringbuffer(unique_name)
        errors: List[Exception] = []
        read_count = [0]
        stop_event = threading.Event()
        
        def writer():
            try:
                for i in range(100):
                    rb.add(f"item-{i}")
                    time.sleep(0.01)
            except Exception as e:
                errors.append(e)
            finally:
                stop_event.set()
        
        def reader():
            try:
                while not stop_event.is_set():
                    try:
                        if rb.size() > 0:
                            seq = rb.head_sequence()
                            rb.read_one(seq)
                            read_count[0] += 1
                    except Exception:
                        pass
                    time.sleep(0.01)
            except Exception as e:
                errors.append(e)
        
        writer_thread = threading.Thread(target=writer)
        reader_thread = threading.Thread(target=reader)
        
        writer_thread.start()
        reader_thread.start()
        
        writer_thread.join()
        reader_thread.join(timeout=2)
        
        assert len(errors) == 0
        assert read_count[0] > 0
