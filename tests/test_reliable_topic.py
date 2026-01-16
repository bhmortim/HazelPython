"""Tests for ReliableTopic distributed data structure proxy."""

import pytest
from concurrent.futures import Future

from hazelcast.proxy.reliable_topic import (
    ReliableTopicProxy,
    ReliableTopicConfig,
    ReliableMessageListener,
    TopicOverloadPolicy,
    StaleSequenceException,
    _MessageRunner,
)
from hazelcast.proxy.topic import TopicMessage, MessageListener


class TestTopicOverloadPolicy:
    """Tests for TopicOverloadPolicy enum."""

    def test_discard_oldest(self):
        """Test DISCARD_OLDEST policy."""
        assert TopicOverloadPolicy.DISCARD_OLDEST.value == "DISCARD_OLDEST"

    def test_discard_newest(self):
        """Test DISCARD_NEWEST policy."""
        assert TopicOverloadPolicy.DISCARD_NEWEST.value == "DISCARD_NEWEST"

    def test_block(self):
        """Test BLOCK policy."""
        assert TopicOverloadPolicy.BLOCK.value == "BLOCK"

    def test_error(self):
        """Test ERROR policy."""
        assert TopicOverloadPolicy.ERROR.value == "ERROR"


class TestReliableTopicConfig:
    """Tests for ReliableTopicConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ReliableTopicConfig()
        assert config.read_batch_size == 10
        assert config.overload_policy == TopicOverloadPolicy.BLOCK

    def test_custom_config(self):
        """Test custom configuration values."""
        config = ReliableTopicConfig(
            read_batch_size=20,
            overload_policy=TopicOverloadPolicy.DISCARD_OLDEST,
        )
        assert config.read_batch_size == 20
        assert config.overload_policy == TopicOverloadPolicy.DISCARD_OLDEST

    def test_invalid_batch_size_raises(self):
        """Test that invalid batch size raises exception."""
        from hazelcast.exceptions import ConfigurationException
        with pytest.raises(ConfigurationException):
            ReliableTopicConfig(read_batch_size=0)

    def test_set_read_batch_size(self):
        """Test setting read_batch_size property."""
        config = ReliableTopicConfig()
        config.read_batch_size = 25
        assert config.read_batch_size == 25

    def test_set_overload_policy(self):
        """Test setting overload_policy property."""
        config = ReliableTopicConfig()
        config.overload_policy = TopicOverloadPolicy.ERROR
        assert config.overload_policy == TopicOverloadPolicy.ERROR


class TestStaleSequenceException:
    """Tests for StaleSequenceException."""

    def test_create_exception(self):
        """Test creating a stale sequence exception."""
        exc = StaleSequenceException("Test message", head_sequence=100)
        assert str(exc) == "Test message"
        assert exc.head_sequence == 100

    def test_default_head_sequence(self):
        """Test default head sequence is -1."""
        exc = StaleSequenceException("Test")
        assert exc.head_sequence == -1


class CustomReliableListener(ReliableMessageListener[str]):
    """Test listener that records messages and sequences."""

    def __init__(self, loss_tolerant=False, initial_seq=-1):
        self.messages = []
        self.stored_sequence = -1
        self._loss_tolerant = loss_tolerant
        self._initial_seq = initial_seq
        self.terminal_errors = []

    def on_message(self, message: TopicMessage[str]) -> None:
        self.messages.append(message)

    def store_sequence(self, sequence: int) -> None:
        self.stored_sequence = sequence

    def retrieve_initial_sequence(self) -> int:
        return self._initial_seq

    def is_loss_tolerant(self) -> bool:
        return self._loss_tolerant

    def is_terminal(self, error: Exception) -> bool:
        self.terminal_errors.append(error)
        return True

    def on_stale_sequence(self, head_sequence: int) -> int:
        return head_sequence


class TestReliableMessageListener:
    """Tests for ReliableMessageListener."""

    def test_default_store_sequence(self):
        """Test default store_sequence does nothing."""
        listener = ReliableMessageListener()
        listener.store_sequence(100)  # Should not raise

    def test_default_retrieve_initial_sequence(self):
        """Test default retrieve_initial_sequence returns -1."""
        listener = ReliableMessageListener()
        assert listener.retrieve_initial_sequence() == -1

    def test_default_is_loss_tolerant(self):
        """Test default is_loss_tolerant returns False."""
        listener = ReliableMessageListener()
        assert listener.is_loss_tolerant() is False

    def test_default_is_terminal(self):
        """Test default is_terminal returns True."""
        listener = ReliableMessageListener()
        assert listener.is_terminal(Exception("test")) is True

    def test_default_on_stale_sequence(self):
        """Test default on_stale_sequence returns head sequence."""
        listener = ReliableMessageListener()
        assert listener.on_stale_sequence(50) == 50


class TestReliableTopicProxy:
    """Tests for ReliableTopicProxy."""

    def test_create_reliable_topic(self):
        """Test creating a reliable topic."""
        topic = ReliableTopicProxy("test-reliable-topic")
        assert topic.name == "test-reliable-topic"
        assert topic.service_name == "hz:impl:reliableTopicService"

    def test_create_with_config(self):
        """Test creating reliable topic with custom config."""
        config = ReliableTopicConfig(read_batch_size=50)
        topic = ReliableTopicProxy("test-topic", config=config)
        assert topic.config.read_batch_size == 50

    def test_default_config(self):
        """Test default config is created if none provided."""
        topic = ReliableTopicProxy("test-topic")
        assert isinstance(topic.config, ReliableTopicConfig)

    def test_publish(self):
        """Test publishing a message."""
        topic = ReliableTopicProxy("test-topic")
        topic.publish("hello")  # Should not raise

    def test_publish_async(self):
        """Test async publish."""
        topic = ReliableTopicProxy("test-topic")
        future = topic.publish_async("hello")
        assert isinstance(future, Future)
        assert future.result() is None

    def test_add_message_listener_with_listener(self):
        """Test adding a message listener."""
        topic = ReliableTopicProxy[str]("test-topic")
        listener = CustomReliableListener()
        reg_id = topic.add_message_listener(listener=listener)
        assert reg_id is not None
        assert len(reg_id) > 0

    def test_add_message_listener_with_callback(self):
        """Test adding a listener with callback."""
        topic = ReliableTopicProxy[str]("test-topic")
        messages = []
        reg_id = topic.add_message_listener(
            on_message=lambda msg: messages.append(msg)
        )
        assert reg_id is not None

    def test_add_message_listener_no_args_raises(self):
        """Test adding listener without args raises ValueError."""
        topic = ReliableTopicProxy("test-topic")
        with pytest.raises(ValueError):
            topic.add_message_listener()

    def test_remove_message_listener(self):
        """Test removing a message listener."""
        topic = ReliableTopicProxy[str]("test-topic")
        listener = CustomReliableListener()
        reg_id = topic.add_message_listener(listener=listener)
        result = topic.remove_message_listener(reg_id)
        assert result is True

    def test_remove_nonexistent_listener(self):
        """Test removing nonexistent listener returns False."""
        topic = ReliableTopicProxy("test-topic")
        result = topic.remove_message_listener("nonexistent")
        assert result is False

    def test_publish_delivers_to_listeners(self):
        """Test that publish delivers messages to listeners."""
        topic = ReliableTopicProxy[str]("test-topic")
        listener = CustomReliableListener()
        topic.add_message_listener(listener=listener)
        topic.publish("message1")
        topic.publish("message2")
        assert len(listener.messages) == 2
        assert listener.messages[0].message == "message1"
        assert listener.messages[1].message == "message2"

    def test_reliable_listener_initial_sequence(self):
        """Test reliable listener with custom initial sequence."""
        topic = ReliableTopicProxy[str]("test-topic")
        listener = CustomReliableListener(initial_seq=100)
        topic.add_message_listener(listener=listener)
        # Verify the runner was started with correct sequence
        reg_id = list(topic._listener_runners.keys())[0]
        runner = topic._listener_runners[reg_id]
        assert runner.sequence == 100

    def test_removed_listener_not_notified(self):
        """Test removed listener doesn't receive messages."""
        topic = ReliableTopicProxy[str]("test-topic")
        listener = CustomReliableListener()
        reg_id = topic.add_message_listener(listener=listener)
        topic.publish("before")
        topic.remove_message_listener(reg_id)
        topic.publish("after")
        assert len(listener.messages) == 1

    def test_handle_stale_sequence_loss_tolerant(self):
        """Test handling stale sequence with loss-tolerant listener."""
        topic = ReliableTopicProxy[str]("test-topic")
        listener = CustomReliableListener(loss_tolerant=True)
        reg_id = topic.add_message_listener(listener=listener)
        new_seq = topic._handle_stale_sequence(reg_id, 10, 50)
        assert new_seq == 50

    def test_handle_stale_sequence_not_loss_tolerant(self):
        """Test handling stale sequence with non-loss-tolerant listener."""
        topic = ReliableTopicProxy[str]("test-topic")
        listener = CustomReliableListener(loss_tolerant=False)
        reg_id = topic.add_message_listener(listener=listener)
        with pytest.raises(StaleSequenceException):
            topic._handle_stale_sequence(reg_id, 10, 50)

    def test_handle_stale_sequence_no_listener(self):
        """Test handling stale sequence with no listener."""
        topic = ReliableTopicProxy("test-topic")
        with pytest.raises(StaleSequenceException):
            topic._handle_stale_sequence("nonexistent", 10, 50)

    def test_handle_stale_sequence_regular_listener(self):
        """Test handling stale sequence with regular MessageListener."""
        topic = ReliableTopicProxy[str]("test-topic")

        class SimpleListener(MessageListener[str]):
            def on_message(self, msg):
                pass

        reg_id = topic.add_message_listener(listener=SimpleListener())
        with pytest.raises(StaleSequenceException):
            topic._handle_stale_sequence(reg_id, 10, 50)

    def test_destroy(self):
        """Test destroying the reliable topic."""
        topic = ReliableTopicProxy[str]("test-topic")
        listener = CustomReliableListener()
        topic.add_message_listener(listener=listener)
        topic._on_destroy()
        assert len(topic._message_listeners) == 0
        assert len(topic._listener_runners) == 0

    def test_destroy_async(self):
        """Test async destroy."""
        import asyncio

        async def test():
            topic = ReliableTopicProxy[str]("test-topic")
            listener = CustomReliableListener()
            topic.add_message_listener(listener=listener)
            await topic._on_destroy_async()
            assert len(topic._message_listeners) == 0

        asyncio.run(test())

    def test_operations_after_destroy(self):
        """Test operations fail after destroy."""
        from hazelcast.exceptions import IllegalStateException
        topic = ReliableTopicProxy("test-topic")
        topic.destroy()
        with pytest.raises(IllegalStateException):
            topic.publish("msg")


class TestMessageRunner:
    """Tests for _MessageRunner."""

    def test_create_runner(self):
        """Test creating a message runner."""
        listener = CustomReliableListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner("reg-1", listener, 0, config)
        assert runner.sequence == 0
        assert runner.is_cancelled is False

    def test_cancel_runner(self):
        """Test cancelling a runner."""
        listener = CustomReliableListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner("reg-1", listener, 0, config)
        runner.cancel()
        assert runner.is_cancelled is True

    def test_process_message(self):
        """Test processing a message."""
        listener = CustomReliableListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner("reg-1", listener, 0, config)
        msg = TopicMessage("test", 12345)
        runner.process_message(msg)
        assert len(listener.messages) == 1
        assert listener.stored_sequence == 1

    def test_process_message_cancelled(self):
        """Test processing message when cancelled does nothing."""
        listener = CustomReliableListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner("reg-1", listener, 0, config)
        runner.cancel()
        msg = TopicMessage("test", 12345)
        runner.process_message(msg)
        assert len(listener.messages) == 0

    def test_process_message_increments_sequence(self):
        """Test that processing messages increments sequence."""
        listener = CustomReliableListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner("reg-1", listener, 5, config)
        msg1 = TopicMessage("test1", 12345)
        msg2 = TopicMessage("test2", 12346)
        runner.process_message(msg1)
        runner.process_message(msg2)
        assert runner.sequence == 7
        assert listener.stored_sequence == 7

    def test_process_message_stores_sequence_for_reliable_listener(self):
        """Test sequence is stored for reliable listeners."""
        listener = CustomReliableListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner("reg-1", listener, 10, config)
        msg = TopicMessage("test", 12345)
        runner.process_message(msg)
        assert listener.stored_sequence == 11

    def test_set_sequence(self):
        """Test setting sequence property."""
        listener = CustomReliableListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner("reg-1", listener, 0, config)
        runner.sequence = 50
        assert runner.sequence == 50
        assert listener.stored_sequence == 50

    def test_process_message_error_cancels_runner(self):
        """Test that error in listener cancels runner."""

        class ErrorListener(ReliableMessageListener[str]):
            def on_message(self, msg):
                raise RuntimeError("Test error")

            def is_terminal(self, error):
                return True

        listener = ErrorListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner("reg-1", listener, 0, config)
        msg = TopicMessage("test", 12345)
        with pytest.raises(RuntimeError):
            runner.process_message(msg)
        assert runner.is_cancelled is True

    def test_process_message_error_non_reliable_listener(self):
        """Test error handling for non-reliable listener."""

        class ErrorListener(MessageListener[str]):
            def on_message(self, msg):
                raise RuntimeError("Test error")

        listener = ErrorListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner("reg-1", listener, 0, config)
        msg = TopicMessage("test", 12345)
        with pytest.raises(RuntimeError):
            runner.process_message(msg)
        assert runner.is_cancelled is True
