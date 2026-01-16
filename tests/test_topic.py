"""Tests for Topic distributed data structure proxy."""

import pytest
from concurrent.futures import Future

from hazelcast.proxy.topic import (
    TopicProxy,
    TopicMessage,
    MessageListener,
    LocalTopicStats,
)


class TestTopicMessage:
    """Tests for TopicMessage."""

    def test_create_message(self):
        """Test creating a topic message."""
        msg = TopicMessage("hello", 12345)
        assert msg.message == "hello"
        assert msg.publish_time == 12345
        assert msg.publishing_member is None

    def test_message_with_member(self):
        """Test creating a message with member info."""
        member = {"uuid": "test-uuid"}
        msg = TopicMessage("hello", 12345, member)
        assert msg.publishing_member == member


class TestMessageListener:
    """Tests for MessageListener."""

    def test_default_on_message(self):
        """Test default on_message implementation does nothing."""
        listener = MessageListener()
        msg = TopicMessage("test", 0)
        listener.on_message(msg)  # Should not raise


class CustomListener(MessageListener[str]):
    """Test listener that records messages."""

    def __init__(self):
        self.messages = []

    def on_message(self, message: TopicMessage[str]) -> None:
        self.messages.append(message)


class TestTopicProxy:
    """Tests for TopicProxy."""

    def test_create_topic(self):
        """Test creating a topic."""
        topic = TopicProxy("test-topic")
        assert topic.name == "test-topic"
        assert topic.service_name == "hz:impl:topicService"

    def test_publish(self):
        """Test publishing a message."""
        topic = TopicProxy("test-topic")
        topic.publish("hello")  # Should not raise

    def test_publish_async(self):
        """Test async publish."""
        topic = TopicProxy("test-topic")
        future = topic.publish_async("hello")
        assert isinstance(future, Future)
        assert future.result() is None

    def test_add_message_listener_with_listener(self):
        """Test adding a message listener with listener object."""
        topic = TopicProxy[str]("test-topic")
        listener = CustomListener()
        reg_id = topic.add_message_listener(listener=listener)
        assert reg_id is not None
        assert len(reg_id) > 0

    def test_add_message_listener_with_callback(self):
        """Test adding a message listener with callback function."""
        topic = TopicProxy[str]("test-topic")
        messages = []
        reg_id = topic.add_message_listener(
            on_message=lambda msg: messages.append(msg)
        )
        assert reg_id is not None

    def test_add_message_listener_no_args_raises(self):
        """Test adding listener without args raises ValueError."""
        topic = TopicProxy("test-topic")
        with pytest.raises(ValueError):
            topic.add_message_listener()

    def test_remove_message_listener(self):
        """Test removing a message listener."""
        topic = TopicProxy[str]("test-topic")
        listener = CustomListener()
        reg_id = topic.add_message_listener(listener=listener)
        result = topic.remove_message_listener(reg_id)
        assert result is True

    def test_remove_nonexistent_listener(self):
        """Test removing a nonexistent listener returns False."""
        topic = TopicProxy("test-topic")
        result = topic.remove_message_listener("nonexistent-id")
        assert result is False

    def test_publish_notifies_listeners(self):
        """Test that publish notifies all registered listeners."""
        topic = TopicProxy[str]("test-topic")
        listener = CustomListener()
        topic.add_message_listener(listener=listener)
        topic.publish("hello")
        topic.publish("world")
        assert len(listener.messages) == 2
        assert listener.messages[0].message == "hello"
        assert listener.messages[1].message == "world"

    def test_publish_notifies_callback_listeners(self):
        """Test that publish notifies callback listeners."""
        topic = TopicProxy[str]("test-topic")
        messages = []
        topic.add_message_listener(on_message=lambda msg: messages.append(msg.message))
        topic.publish("test")
        assert messages == ["test"]

    def test_multiple_listeners(self):
        """Test multiple listeners receive messages."""
        topic = TopicProxy[str]("test-topic")
        listener1 = CustomListener()
        listener2 = CustomListener()
        topic.add_message_listener(listener=listener1)
        topic.add_message_listener(listener=listener2)
        topic.publish("msg")
        assert len(listener1.messages) == 1
        assert len(listener2.messages) == 1

    def test_removed_listener_not_notified(self):
        """Test removed listener doesn't receive messages."""
        topic = TopicProxy[str]("test-topic")
        listener = CustomListener()
        reg_id = topic.add_message_listener(listener=listener)
        topic.publish("before")
        topic.remove_message_listener(reg_id)
        topic.publish("after")
        assert len(listener.messages) == 1
        assert listener.messages[0].message == "before"

    def test_get_local_topic_stats(self):
        """Test getting local topic stats."""
        topic = TopicProxy("test-topic")
        stats = topic.get_local_topic_stats()
        assert isinstance(stats, LocalTopicStats)

    def test_destroy(self):
        """Test destroying the topic."""
        topic = TopicProxy("test-topic")
        topic.destroy()
        assert topic.is_destroyed

    def test_operations_after_destroy(self):
        """Test operations fail after destroy."""
        from hazelcast.exceptions import IllegalStateException
        topic = TopicProxy("test-topic")
        topic.destroy()
        with pytest.raises(IllegalStateException):
            topic.publish("msg")

    def test_listener_exception_doesnt_break_delivery(self):
        """Test that listener exception doesn't break delivery to others."""
        topic = TopicProxy[str]("test-topic")

        class BadListener(MessageListener[str]):
            def on_message(self, msg):
                raise RuntimeError("Intentional error")

        good_listener = CustomListener()
        topic.add_message_listener(listener=BadListener())
        topic.add_message_listener(listener=good_listener)
        topic.publish("test")
        assert len(good_listener.messages) == 1


class TestLocalTopicStats:
    """Tests for LocalTopicStats."""

    def test_create_stats(self):
        """Test creating local topic stats."""
        stats = LocalTopicStats()
        assert stats.publish_operation_count == 0
        assert stats.receive_operation_count == 0

    def test_stats_with_values(self):
        """Test creating stats with custom values."""
        stats = LocalTopicStats(publish_operation_count=5, receive_operation_count=10)
        assert stats.publish_operation_count == 5
        assert stats.receive_operation_count == 10
