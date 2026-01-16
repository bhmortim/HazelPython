"""Unit tests for Reliable Topic proxy."""

import unittest
from unittest.mock import MagicMock, patch
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


class TestStaleSequenceException(unittest.TestCase):
    """Tests for StaleSequenceException class."""

    def test_exception_initialization(self):
        """Test StaleSequenceException initialization."""
        exc = StaleSequenceException("Test message", head_sequence=100)
        self.assertEqual(str(exc), "Test message")
        self.assertEqual(exc.head_sequence, 100)

    def test_exception_default_head_sequence(self):
        """Test StaleSequenceException with default head_sequence."""
        exc = StaleSequenceException("Test message")
        self.assertEqual(exc.head_sequence, -1)


class TestTopicOverloadPolicy(unittest.TestCase):
    """Tests for TopicOverloadPolicy enum."""

    def test_policy_values(self):
        """Test TopicOverloadPolicy values are defined correctly."""
        self.assertEqual(TopicOverloadPolicy.DISCARD_OLDEST.value, "DISCARD_OLDEST")
        self.assertEqual(TopicOverloadPolicy.DISCARD_NEWEST.value, "DISCARD_NEWEST")
        self.assertEqual(TopicOverloadPolicy.BLOCK.value, "BLOCK")
        self.assertEqual(TopicOverloadPolicy.ERROR.value, "ERROR")


class TestReliableTopicConfig(unittest.TestCase):
    """Tests for ReliableTopicConfig class."""

    def test_config_initialization(self):
        """Test ReliableTopicConfig initialization."""
        config = ReliableTopicConfig(
            read_batch_size=20,
            overload_policy=TopicOverloadPolicy.DISCARD_OLDEST,
        )
        self.assertEqual(config.read_batch_size, 20)
        self.assertEqual(config.overload_policy, TopicOverloadPolicy.DISCARD_OLDEST)

    def test_config_defaults(self):
        """Test ReliableTopicConfig with default values."""
        config = ReliableTopicConfig()
        self.assertEqual(config.read_batch_size, 10)
        self.assertEqual(config.overload_policy, TopicOverloadPolicy.BLOCK)

    def test_config_invalid_read_batch_size_raises(self):
        """Test ReliableTopicConfig raises for invalid read_batch_size."""
        with self.assertRaises(Exception):
            ReliableTopicConfig(read_batch_size=0)

    def test_config_read_batch_size_setter(self):
        """Test read_batch_size setter."""
        config = ReliableTopicConfig()
        config.read_batch_size = 50
        self.assertEqual(config.read_batch_size, 50)

    def test_config_read_batch_size_setter_invalid_raises(self):
        """Test read_batch_size setter raises for invalid value."""
        config = ReliableTopicConfig()
        with self.assertRaises(Exception):
            config.read_batch_size = -1

    def test_config_overload_policy_setter(self):
        """Test overload_policy setter."""
        config = ReliableTopicConfig()
        config.overload_policy = TopicOverloadPolicy.ERROR
        self.assertEqual(config.overload_policy, TopicOverloadPolicy.ERROR)


class TestReliableMessageListener(unittest.TestCase):
    """Tests for ReliableMessageListener class."""

    def test_listener_methods_exist(self):
        """Test that listener has all required methods."""
        listener = ReliableMessageListener()
        self.assertTrue(hasattr(listener, "on_message"))
        self.assertTrue(hasattr(listener, "store_sequence"))
        self.assertTrue(hasattr(listener, "retrieve_initial_sequence"))
        self.assertTrue(hasattr(listener, "is_loss_tolerant"))
        self.assertTrue(hasattr(listener, "is_terminal"))
        self.assertTrue(hasattr(listener, "on_stale_sequence"))

    def test_store_sequence_default(self):
        """Test store_sequence default implementation."""
        listener = ReliableMessageListener()
        listener.store_sequence(100)

    def test_retrieve_initial_sequence_default(self):
        """Test retrieve_initial_sequence returns -1 by default."""
        listener = ReliableMessageListener()
        self.assertEqual(listener.retrieve_initial_sequence(), -1)

    def test_is_loss_tolerant_default(self):
        """Test is_loss_tolerant returns False by default."""
        listener = ReliableMessageListener()
        self.assertFalse(listener.is_loss_tolerant())

    def test_is_terminal_default(self):
        """Test is_terminal returns True by default."""
        listener = ReliableMessageListener()
        self.assertTrue(listener.is_terminal(Exception("test")))

    def test_on_stale_sequence_default(self):
        """Test on_stale_sequence returns head_sequence by default."""
        listener = ReliableMessageListener()
        result = listener.on_stale_sequence(100)
        self.assertEqual(result, 100)


class TestMessageRunner(unittest.TestCase):
    """Tests for _MessageRunner class."""

    def test_runner_initialization(self):
        """Test _MessageRunner initialization."""
        listener = MessageListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner(
            registration_id="test-id",
            listener=listener,
            initial_sequence=5,
            config=config,
        )
        self.assertEqual(runner.sequence, 5)
        self.assertFalse(runner.is_cancelled)

    def test_runner_cancel(self):
        """Test _MessageRunner cancel."""
        listener = MessageListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner("test-id", listener, 0, config)
        runner.cancel()
        self.assertTrue(runner.is_cancelled)

    def test_runner_process_message(self):
        """Test _MessageRunner process_message."""
        results = []

        class TestListener(MessageListener):
            def on_message(self, msg):
                results.append(msg.message)

        config = ReliableTopicConfig()
        runner = _MessageRunner("test-id", TestListener(), 0, config)
        message = TopicMessage("test", 0)
        runner.process_message(message)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], "test")

    def test_runner_process_message_increments_sequence(self):
        """Test _MessageRunner process_message increments sequence."""
        listener = MessageListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner("test-id", listener, 0, config)
        message = TopicMessage("test", 0)
        runner.process_message(message)
        self.assertEqual(runner.sequence, 1)

    def test_runner_process_message_cancelled_does_nothing(self):
        """Test _MessageRunner process_message does nothing when cancelled."""
        results = []

        class TestListener(MessageListener):
            def on_message(self, msg):
                results.append(msg.message)

        config = ReliableTopicConfig()
        runner = _MessageRunner("test-id", TestListener(), 0, config)
        runner.cancel()
        message = TopicMessage("test", 0)
        runner.process_message(message)
        self.assertEqual(len(results), 0)

    def test_runner_sequence_setter_stores_for_reliable_listener(self):
        """Test sequence setter calls store_sequence for ReliableMessageListener."""
        stored_sequences = []

        class TestListener(ReliableMessageListener):
            def store_sequence(self, seq):
                stored_sequences.append(seq)

        config = ReliableTopicConfig()
        runner = _MessageRunner("test-id", TestListener(), 0, config)
        runner.sequence = 10
        self.assertEqual(stored_sequences, [10])

    def test_runner_process_message_exception_cancels_for_terminal(self):
        """Test _MessageRunner cancels on terminal exception."""
        class FailingListener(MessageListener):
            def on_message(self, msg):
                raise Exception("Test error")

        config = ReliableTopicConfig()
        runner = _MessageRunner("test-id", FailingListener(), 0, config)
        message = TopicMessage("test", 0)
        with self.assertRaises(Exception):
            runner.process_message(message)
        self.assertTrue(runner.is_cancelled)


class TestReliableTopicProxy(unittest.TestCase):
    """Tests for ReliableTopicProxy class."""

    def setUp(self):
        """Set up test fixtures."""
        self.proxy = ReliableTopicProxy(
            name="test-reliable-topic",
            context=None,
        )

    def test_service_name_constant(self):
        """Test SERVICE_NAME is defined correctly."""
        self.assertEqual(
            ReliableTopicProxy.SERVICE_NAME,
            "hz:impl:reliableTopicService",
        )

    def test_initialization(self):
        """Test ReliableTopicProxy initialization."""
        self.assertEqual(self.proxy._name, "test-reliable-topic")
        self.assertIsInstance(self.proxy._config, ReliableTopicConfig)
        self.assertIsInstance(self.proxy._message_listeners, dict)
        self.assertIsInstance(self.proxy._listener_runners, dict)

    def test_initialization_with_custom_config(self):
        """Test ReliableTopicProxy initialization with custom config."""
        config = ReliableTopicConfig(read_batch_size=50)
        proxy = ReliableTopicProxy(
            name="test",
            context=None,
            config=config,
        )
        self.assertEqual(proxy.config.read_batch_size, 50)

    def test_config_property(self):
        """Test config property returns configuration."""
        config = self.proxy.config
        self.assertIsInstance(config, ReliableTopicConfig)

    def test_publish_completes(self):
        """Test publish completes without error."""
        self.proxy.publish("message")

    def test_publish_async_returns_future(self):
        """Test publish_async returns a Future."""
        future = self.proxy.publish_async("message")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_publish_delivers_to_runners(self):
        """Test publish delivers message to runners."""
        results = []

        class TestListener(MessageListener):
            def on_message(self, msg):
                results.append(msg.message)

        self.proxy.add_message_listener(listener=TestListener())
        self.proxy.publish("test_message")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], "test_message")

    def test_add_message_listener_with_listener(self):
        """Test add_message_listener with listener object."""
        listener = MessageListener()
        reg_id = self.proxy.add_message_listener(listener=listener)
        self.assertIsInstance(reg_id, str)

    def test_add_message_listener_with_callback(self):
        """Test add_message_listener with on_message callback."""
        reg_id = self.proxy.add_message_listener(on_message=lambda m: None)
        self.assertIsInstance(reg_id, str)

    def test_add_message_listener_raises_without_listener_or_callback(self):
        """Test add_message_listener raises without listener or callback."""
        with self.assertRaises(ValueError):
            self.proxy.add_message_listener()

    def test_add_message_listener_creates_runner(self):
        """Test add_message_listener creates a runner."""
        listener = MessageListener()
        reg_id = self.proxy.add_message_listener(listener=listener)
        self.assertIn(reg_id, self.proxy._listener_runners)

    def test_add_message_listener_with_reliable_listener_uses_initial_sequence(self):
        """Test add_message_listener uses initial sequence from ReliableMessageListener."""
        class TestListener(ReliableMessageListener):
            def retrieve_initial_sequence(self):
                return 100

        reg_id = self.proxy.add_message_listener(listener=TestListener())
        runner = self.proxy._listener_runners[reg_id]
        self.assertEqual(runner.sequence, 100)

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

    def test_remove_message_listener_stops_runner(self):
        """Test remove_message_listener stops the runner."""
        listener = MessageListener()
        reg_id = self.proxy.add_message_listener(listener=listener)
        self.proxy.remove_message_listener(reg_id)
        self.assertNotIn(reg_id, self.proxy._listener_runners)

    def test_handle_stale_sequence_raises_for_nonexistent_listener(self):
        """Test _handle_stale_sequence raises for nonexistent listener."""
        with self.assertRaises(StaleSequenceException):
            self.proxy._handle_stale_sequence("nonexistent", 0, 100)

    def test_handle_stale_sequence_raises_for_non_loss_tolerant(self):
        """Test _handle_stale_sequence raises for non-loss-tolerant listener."""
        listener = ReliableMessageListener()
        reg_id = self.proxy.add_message_listener(listener=listener)
        with self.assertRaises(StaleSequenceException):
            self.proxy._handle_stale_sequence(reg_id, 0, 100)

    def test_handle_stale_sequence_returns_new_sequence_for_loss_tolerant(self):
        """Test _handle_stale_sequence returns new sequence for loss-tolerant listener."""
        class TolerantListener(ReliableMessageListener):
            def is_loss_tolerant(self):
                return True

            def on_stale_sequence(self, head_seq):
                return head_seq + 10

        reg_id = self.proxy.add_message_listener(listener=TolerantListener())
        result = self.proxy._handle_stale_sequence(reg_id, 0, 100)
        self.assertEqual(result, 110)

    def test_handle_stale_sequence_raises_for_regular_listener(self):
        """Test _handle_stale_sequence raises for regular MessageListener."""
        listener = MessageListener()
        reg_id = self.proxy.add_message_listener(listener=listener)
        with self.assertRaises(StaleSequenceException):
            self.proxy._handle_stale_sequence(reg_id, 0, 100)

    def test_on_destroy_stops_all_runners(self):
        """Test _on_destroy stops all runners."""
        listener1 = MessageListener()
        listener2 = MessageListener()
        self.proxy.add_message_listener(listener=listener1)
        self.proxy.add_message_listener(listener=listener2)
        self.proxy._on_destroy()
        self.assertEqual(len(self.proxy._listener_runners), 0)
        self.assertEqual(len(self.proxy._message_listeners), 0)

    def test_on_destroy_async_stops_all_runners(self):
        """Test _on_destroy_async stops all runners."""
        import asyncio

        listener = MessageListener()
        self.proxy.add_message_listener(listener=listener)

        async def run_test():
            await self.proxy._on_destroy_async()

        asyncio.get_event_loop().run_until_complete(run_test())
        self.assertEqual(len(self.proxy._listener_runners), 0)


if __name__ == "__main__":
    unittest.main()
