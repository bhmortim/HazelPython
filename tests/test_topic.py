"""Unit tests for Topic proxy."""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future

from hazelcast.proxy.topic import (
    TopicProxy,
    TopicMessage,
    MessageListener,
    LocalTopicStats,
)


class TestTopicMessage(unittest.TestCase):
    """Tests for TopicMessage class."""

    def test_message_initialization(self):
        """Test TopicMessage initialization."""
        message = TopicMessage(
            message="test_message",
            publish_time=1234567890,
            publishing_member="member1",
        )
        self.assertEqual(message.message, "test_message")
        self.assertEqual(message.publish_time, 1234567890)
        self.assertEqual(message.publishing_member, "member1")

    def test_message_with_defaults(self):
        """Test TopicMessage with default values."""
        message = TopicMessage(message="test", publish_time=0)
        self.assertEqual(message.message, "test")
        self.assertEqual(message.publish_time, 0)
        self.assertIsNone(message.publishing_member)


class TestMessageListener(unittest.TestCase):
    """Tests for MessageListener class."""

    def test_listener_method_exists(self):
        """Test that listener has on_message method."""
        listener = MessageListener()
        self.assertTrue(hasattr(listener, "on_message"))

    def test_listener_method_is_callable(self):
        """Test that on_message can be called without error."""
        listener = MessageListener()
        message = TopicMessage("test", 0)
        listener.on_message(message)


class TestLocalTopicStats(unittest.TestCase):
    """Tests for LocalTopicStats class."""

    def test_stats_initialization(self):
        """Test LocalTopicStats initialization."""
        stats = LocalTopicStats(
            publish_operation_count=10,
            receive_operation_count=20,
        )
        self.assertEqual(stats.publish_operation_count, 10)
        self.assertEqual(stats.receive_operation_count, 20)

    def test_stats_defaults(self):
        """Test LocalTopicStats with default values."""
        stats = LocalTopicStats()
        self.assertEqual(stats.publish_operation_count, 0)
        self.assertEqual(stats.receive_operation_count, 0)


class TestTopicProxy(unittest.TestCase):
    """Tests for TopicProxy class."""

    def setUp(self):
        """Set up test fixtures."""
        self.proxy = TopicProxy(
            name="test-topic",
            context=None,
        )

    def test_service_name_constant(self):
        """Test SERVICE_NAME is defined correctly."""
        self.assertEqual(TopicProxy.SERVICE_NAME, "hz:impl:topicService")

    def test_initialization(self):
        """Test TopicProxy initialization."""
        self.assertEqual(self.proxy._name, "test-topic")
        self.assertIsInstance(self.proxy._message_listeners, dict)

    def test_publish_completes(self):
        """Test publish completes without error."""
        self.proxy.publish("message")

    def test_publish_async_returns_future(self):
        """Test publish_async returns a Future."""
        future = self.proxy.publish_async("message")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_publish_notifies_listeners(self):
        """Test publish notifies all listeners."""
        results = []

        class TestListener(MessageListener):
            def on_message(self, msg):
                results.append(msg.message)

        listener = TestListener()
        self.proxy.add_message_listener(listener=listener)
        self.proxy.publish("test_message")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], "test_message")

    def test_notify_listeners_handles_exceptions(self):
        """Test _notify_listeners handles listener exceptions."""
        class FailingListener(MessageListener):
            def on_message(self, msg):
                raise Exception("Listener error")

        listener = FailingListener()
        self.proxy.add_message_listener(listener=listener)
        self.proxy.publish("message")

    def test_add_message_listener_with_listener(self):
        """Test add_message_listener with listener object."""
        listener = MessageListener()
        reg_id = self.proxy.add_message_listener(listener=listener)
        self.assertIsInstance(reg_id, str)
        self.assertTrue(len(reg_id) > 0)

    def test_add_message_listener_with_callback(self):
        """Test add_message_listener with on_message callback."""
        reg_id = self.proxy.add_message_listener(on_message=lambda m: None)
        self.assertIsInstance(reg_id, str)

    def test_add_message_listener_raises_without_listener_or_callback(self):
        """Test add_message_listener raises without listener or callback."""
        with self.assertRaises(ValueError):
            self.proxy.add_message_listener()

    def test_remove_message_listener_returns_true_for_existing(self):
        """Test remove_message_listener returns True for existing listener."""
        listener = MessageListener()
        reg_id = self.proxy.add_message_listener(listener=listener)
        result = self.proxy.remove_message_listener(reg_id)
        self.assertTrue(result)

    def test_remove_message_listener_returns_false_for_nonexistent(self):
        """Test remove_message_listener returns False for nonexistent listener."""
        result = self.proxy.remove_message_listener("nonexistent-id")
        self.assertFalse(result)

    def test_get_local_topic_stats(self):
        """Test get_local_topic_stats returns LocalTopicStats."""
        stats = self.proxy.get_local_topic_stats()
        self.assertIsInstance(stats, LocalTopicStats)


class TestTopicProxyMultipleListeners(unittest.TestCase):
    """Tests for TopicProxy with multiple listeners."""

    def setUp(self):
        """Set up test fixtures."""
        self.proxy = TopicProxy(
            name="test-topic",
            context=None,
        )
        self.results = []

    def test_multiple_listeners_receive_message(self):
        """Test multiple listeners all receive the message."""
        listener1_results = []
        listener2_results = []

        class Listener1(MessageListener):
            def on_message(self, msg):
                listener1_results.append(msg.message)

        class Listener2(MessageListener):
            def on_message(self, msg):
                listener2_results.append(msg.message)

        self.proxy.add_message_listener(listener=Listener1())
        self.proxy.add_message_listener(listener=Listener2())
        self.proxy.publish("test")

        self.assertEqual(len(listener1_results), 1)
        self.assertEqual(len(listener2_results), 1)

    def test_removed_listener_does_not_receive_message(self):
        """Test removed listener does not receive message."""
        results = []

        class TestListener(MessageListener):
            def on_message(self, msg):
                results.append(msg.message)

        reg_id = self.proxy.add_message_listener(listener=TestListener())
        self.proxy.remove_message_listener(reg_id)
        self.proxy.publish("test")

        self.assertEqual(len(results), 0)


if __name__ == "__main__":
    unittest.main()
