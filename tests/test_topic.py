"""Unit tests for hazelcast.proxy.topic module."""

import pytest
from concurrent.futures import Future

from hazelcast.proxy.topic import (
    TopicProxy,
    TopicMessage,
    MessageListener,
    LocalTopicStats,
)
from hazelcast.proxy.reliable_topic import (
    ReliableTopicProxy,
    ReliableTopicConfig,
    ReliableMessageListener,
    TopicOverloadPolicy,
    StaleSequenceException,
)
from hazelcast.exceptions import IllegalStateException, ConfigurationException


class TestTopicMessage:
    """Tests for TopicMessage class."""

    def test_init(self):
        msg = TopicMessage("hello", publish_time=12345)
        assert msg.message == "hello"
        assert msg.publish_time == 12345
        assert msg.publishing_member is None

    def test_with_member(self):
        member = object()
        msg = TopicMessage("hello", publish_time=12345, publishing_member=member)
        assert msg.publishing_member is member


class TestMessageListener:
    """Tests for MessageListener class."""

    def test_default_on_message(self):
        listener = MessageListener()
        msg = TopicMessage("test", 0)
        listener.on_message(msg)


class TestLocalTopicStats:
    """Tests for LocalTopicStats class."""

    def test_default_values(self):
        stats = LocalTopicStats()
        assert stats.publish_operation_count == 0
        assert stats.receive_operation_count == 0

    def test_custom_values(self):
        stats = LocalTopicStats(
            publish_operation_count=10,
            receive_operation_count=5,
        )
        assert stats.publish_operation_count == 10
        assert stats.receive_operation_count == 5


class TestTopicProxy:
    """Tests for TopicProxy class."""

    def test_init(self):
        topic = TopicProxy("test-topic")
        assert topic.name == "test-topic"
        assert topic.service_name == "hz:impl:topicService"

    def test_publish(self):
        topic = TopicProxy("test-topic")
        topic.publish("message")

    def test_publish_async(self):
        topic = TopicProxy("test-topic")
        future = topic.publish_async("message")
        assert isinstance(future, Future)
        assert future.result() is None

    def test_add_message_listener_with_listener(self):
        topic = TopicProxy("test-topic")

        class TestListener(MessageListener):
            def on_message(self, message):
                pass

        reg_id = topic.add_message_listener(listener=TestListener())
        assert reg_id is not None

    def test_add_message_listener_with_callback(self):
        topic = TopicProxy("test-topic")
        messages = []
        reg_id = topic.add_message_listener(
            on_message=lambda m: messages.append(m)
        )
        assert reg_id is not None

    def test_add_message_listener_no_args_raises(self):
        topic = TopicProxy("test-topic")
        with pytest.raises(ValueError):
            topic.add_message_listener()

    def test_remove_message_listener(self):
        topic = TopicProxy("test-topic")
        reg_id = topic.add_message_listener(on_message=lambda m: None)
        assert topic.remove_message_listener(reg_id) is True
        assert topic.remove_message_listener(reg_id) is False

    def test_get_local_topic_stats(self):
        topic = TopicProxy("test-topic")
        stats = topic.get_local_topic_stats()
        assert isinstance(stats, LocalTopicStats)

    def test_destroyed_operations_raise(self):
        topic = TopicProxy("test-topic")
        topic.destroy()
        with pytest.raises(IllegalStateException):
            topic.publish("message")


class TestReliableTopicConfig:
    """Tests for ReliableTopicConfig class."""

    def test_default_values(self):
        config = ReliableTopicConfig()
        assert config.read_batch_size == 10
        assert config.overload_policy == TopicOverloadPolicy.BLOCK

    def test_custom_values(self):
        config = ReliableTopicConfig(
            read_batch_size=20,
            overload_policy=TopicOverloadPolicy.DISCARD_OLDEST,
        )
        assert config.read_batch_size == 20
        assert config.overload_policy == TopicOverloadPolicy.DISCARD_OLDEST

    def test_invalid_batch_size(self):
        with pytest.raises(ConfigurationException):
            ReliableTopicConfig(read_batch_size=0)

        with pytest.raises(ConfigurationException):
            ReliableTopicConfig(read_batch_size=-1)

    def test_setters(self):
        config = ReliableTopicConfig()
        config.read_batch_size = 50
        config.overload_policy = TopicOverloadPolicy.ERROR
        assert config.read_batch_size == 50
        assert config.overload_policy == TopicOverloadPolicy.ERROR


class TestTopicOverloadPolicy:
    """Tests for TopicOverloadPolicy enum."""

    def test_values(self):
        assert TopicOverloadPolicy.DISCARD_OLDEST.value == "DISCARD_OLDEST"
        assert TopicOverloadPolicy.DISCARD_NEWEST.value == "DISCARD_NEWEST"
        assert TopicOverloadPolicy.BLOCK.value == "BLOCK"
        assert TopicOverloadPolicy.ERROR.value == "ERROR"


class TestReliableMessageListener:
    """Tests for ReliableMessageListener class."""

    def test_default_methods(self):
        listener = ReliableMessageListener()
        listener.store_sequence(0)
        assert listener.retrieve_initial_sequence() == -1
        assert listener.is_loss_tolerant() is False
        assert listener.is_terminal(Exception()) is True
        assert listener.on_stale_sequence(100) == 100


class TestStaleSequenceException:
    """Tests for StaleSequenceException class."""

    def test_init(self):
        exc = StaleSequenceException("test message", head_sequence=100)
        assert "test message" in str(exc)
        assert exc.head_sequence == 100

    def test_default_head_sequence(self):
        exc = StaleSequenceException("test")
        assert exc.head_sequence == -1


class TestReliableTopicProxy:
    """Tests for ReliableTopicProxy class."""

    def test_init(self):
        topic = ReliableTopicProxy("test-reliable-topic")
        assert topic.name == "test-reliable-topic"
        assert topic.service_name == "hz:impl:reliableTopicService"

    def test_init_with_config(self):
        config = ReliableTopicConfig(read_batch_size=50)
        topic = ReliableTopicProxy("test", config=config)
        assert topic.config.read_batch_size == 50

    def test_publish(self):
        topic = ReliableTopicProxy("test")
        topic.publish("message")

    def test_publish_async(self):
        topic = ReliableTopicProxy("test")
        future = topic.publish_async("message")
        assert isinstance(future, Future)

    def test_add_message_listener(self):
        topic = ReliableTopicProxy("test")
        reg_id = topic.add_message_listener(on_message=lambda m: None)
        assert reg_id is not None

    def test_add_message_listener_with_reliable_listener(self):
        topic = ReliableTopicProxy("test")

        class TestListener(ReliableMessageListener):
            def on_message(self, message):
                pass

        reg_id = topic.add_message_listener(listener=TestListener())
        assert reg_id is not None

    def test_remove_message_listener(self):
        topic = ReliableTopicProxy("test")
        reg_id = topic.add_message_listener(on_message=lambda m: None)
        assert topic.remove_message_listener(reg_id) is True
        assert topic.remove_message_listener(reg_id) is False

    def test_handle_stale_sequence_no_listener(self):
        topic = ReliableTopicProxy("test")
        with pytest.raises(StaleSequenceException):
            topic._handle_stale_sequence("nonexistent", 0, 100)

    def test_handle_stale_sequence_loss_tolerant(self):
        topic = ReliableTopicProxy("test")

        class LossTolerantListener(ReliableMessageListener):
            def on_message(self, message):
                pass

            def is_loss_tolerant(self):
                return True

            def on_stale_sequence(self, head_seq):
                return head_seq

        reg_id = topic.add_message_listener(listener=LossTolerantListener())
        new_seq = topic._handle_stale_sequence(reg_id, 0, 100)
        assert new_seq == 100

    def test_handle_stale_sequence_not_loss_tolerant(self):
        topic = ReliableTopicProxy("test")

        class StrictListener(ReliableMessageListener):
            def on_message(self, message):
                pass

            def is_loss_tolerant(self):
                return False

        reg_id = topic.add_message_listener(listener=StrictListener())
        with pytest.raises(StaleSequenceException):
            topic._handle_stale_sequence(reg_id, 0, 100)
