"""Unit tests for Topic and ReliableTopic proxies."""

import unittest
from unittest.mock import MagicMock

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
    _MessageRunner,
)


class TestTopicMessage(unittest.TestCase):
    """Tests for TopicMessage class."""

    def test_message_properties(self):
        msg = TopicMessage(message="hello", publish_time=12345, publishing_member="member1")
        self.assertEqual("hello", msg.message)
        self.assertEqual(12345, msg.publish_time)
        self.assertEqual("member1", msg.publishing_member)

    def test_message_without_member(self):
        msg = TopicMessage(message="test", publish_time=100)
        self.assertEqual("test", msg.message)
        self.assertEqual(100, msg.publish_time)
        self.assertIsNone(msg.publishing_member)


class TestMessageListener(unittest.TestCase):
    """Tests for MessageListener interface."""

    def test_default_on_message(self):
        listener = MessageListener()
        msg = TopicMessage(message="test", publish_time=0)
        listener.on_message(msg)


class TestLocalTopicStats(unittest.TestCase):
    """Tests for LocalTopicStats class."""

    def test_default_stats(self):
        stats = LocalTopicStats()
        self.assertEqual(0, stats.publish_operation_count)
        self.assertEqual(0, stats.receive_operation_count)

    def test_custom_stats(self):
        stats = LocalTopicStats(publish_operation_count=10, receive_operation_count=20)
        self.assertEqual(10, stats.publish_operation_count)
        self.assertEqual(20, stats.receive_operation_count)


class TestTopicProxy(unittest.TestCase):
    """Tests for TopicProxy class."""

    def setUp(self):
        self.topic = TopicProxy("test-topic")

    def test_name(self):
        self.assertEqual("test-topic", self.topic.name)

    def test_service_name(self):
        self.assertEqual("hz:impl:topicService", self.topic.service_name)

    def test_publish(self):
        self.topic.publish("message")

    def test_publish_async(self):
        future = self.topic.publish_async("message")
        self.assertIsNone(future.result())

    def test_add_message_listener_with_instance(self):
        listener = MessageListener()
        reg_id = self.topic.add_message_listener(listener=listener)
        self.assertIsNotNone(reg_id)
        self.assertIn(reg_id, self.topic._message_listeners)

    def test_add_message_listener_with_callback(self):
        messages = []

        def on_message(msg):
            messages.append(msg)

        reg_id = self.topic.add_message_listener(on_message=on_message)
        self.assertIsNotNone(reg_id)

    def test_add_message_listener_requires_argument(self):
        with self.assertRaises(ValueError):
            self.topic.add_message_listener()

    def test_remove_message_listener(self):
        listener = MessageListener()
        reg_id = self.topic.add_message_listener(listener=listener)
        self.assertTrue(self.topic.remove_message_listener(reg_id))
        self.assertNotIn(reg_id, self.topic._message_listeners)

    def test_remove_nonexistent_listener(self):
        self.assertFalse(self.topic.remove_message_listener("nonexistent"))

    def test_get_local_topic_stats(self):
        stats = self.topic.get_local_topic_stats()
        self.assertIsInstance(stats, LocalTopicStats)

    def test_destroy(self):
        self.topic.destroy()
        self.assertTrue(self.topic.is_destroyed)

    def test_publish_after_destroy_raises(self):
        self.topic.destroy()
        with self.assertRaises(Exception):
            self.topic.publish("message")


class TestTopicOverloadPolicy(unittest.TestCase):
    """Tests for TopicOverloadPolicy enum."""

    def test_all_policies_exist(self):
        self.assertEqual("DISCARD_OLDEST", TopicOverloadPolicy.DISCARD_OLDEST.value)
        self.assertEqual("DISCARD_NEWEST", TopicOverloadPolicy.DISCARD_NEWEST.value)
        self.assertEqual("BLOCK", TopicOverloadPolicy.BLOCK.value)
        self.assertEqual("ERROR", TopicOverloadPolicy.ERROR.value)


class TestReliableTopicConfig(unittest.TestCase):
    """Tests for ReliableTopicConfig class."""

    def test_default_config(self):
        config = ReliableTopicConfig()
        self.assertEqual(10, config.read_batch_size)
        self.assertEqual(TopicOverloadPolicy.BLOCK, config.overload_policy)

    def test_custom_config(self):
        config = ReliableTopicConfig(
            read_batch_size=50,
            overload_policy=TopicOverloadPolicy.DISCARD_OLDEST,
        )
        self.assertEqual(50, config.read_batch_size)
        self.assertEqual(TopicOverloadPolicy.DISCARD_OLDEST, config.overload_policy)

    def test_invalid_read_batch_size(self):
        from hazelcast.exceptions import ConfigurationException

        with self.assertRaises(ConfigurationException):
            ReliableTopicConfig(read_batch_size=0)

        with self.assertRaises(ConfigurationException):
            ReliableTopicConfig(read_batch_size=-1)

    def test_set_read_batch_size(self):
        config = ReliableTopicConfig()
        config.read_batch_size = 25
        self.assertEqual(25, config.read_batch_size)

    def test_set_overload_policy(self):
        config = ReliableTopicConfig()
        config.overload_policy = TopicOverloadPolicy.ERROR
        self.assertEqual(TopicOverloadPolicy.ERROR, config.overload_policy)


class TestReliableMessageListener(unittest.TestCase):
    """Tests for ReliableMessageListener interface."""

    def test_default_retrieve_initial_sequence(self):
        listener = ReliableMessageListener()
        self.assertEqual(-1, listener.retrieve_initial_sequence())

    def test_default_is_loss_tolerant(self):
        listener = ReliableMessageListener()
        self.assertFalse(listener.is_loss_tolerant())

    def test_default_is_terminal(self):
        listener = ReliableMessageListener()
        self.assertTrue(listener.is_terminal(Exception("test")))

    def test_default_on_stale_sequence(self):
        listener = ReliableMessageListener()
        self.assertEqual(100, listener.on_stale_sequence(100))

    def test_store_sequence(self):
        listener = ReliableMessageListener()
        listener.store_sequence(42)


class TestStaleSequenceException(unittest.TestCase):
    """Tests for StaleSequenceException class."""

    def test_exception_message(self):
        exc = StaleSequenceException("test message", head_sequence=50)
        self.assertIn("test message", str(exc))
        self.assertEqual(50, exc.head_sequence)

    def test_default_head_sequence(self):
        exc = StaleSequenceException("test")
        self.assertEqual(-1, exc.head_sequence)


class TestReliableTopicProxy(unittest.TestCase):
    """Tests for ReliableTopicProxy class."""

    def setUp(self):
        self.topic = ReliableTopicProxy("reliable-test-topic")

    def test_name(self):
        self.assertEqual("reliable-test-topic", self.topic.name)

    def test_service_name(self):
        self.assertEqual("hz:impl:reliableTopicService", self.topic.service_name)

    def test_default_config(self):
        self.assertEqual(10, self.topic.config.read_batch_size)
        self.assertEqual(TopicOverloadPolicy.BLOCK, self.topic.config.overload_policy)

    def test_custom_config(self):
        config = ReliableTopicConfig(
            read_batch_size=100,
            overload_policy=TopicOverloadPolicy.DISCARD_OLDEST,
        )
        topic = ReliableTopicProxy("custom-topic", config=config)
        self.assertEqual(100, topic.config.read_batch_size)
        self.assertEqual(TopicOverloadPolicy.DISCARD_OLDEST, topic.config.overload_policy)

    def test_publish(self):
        self.topic.publish("message")

    def test_publish_async(self):
        future = self.topic.publish_async("message")
        self.assertIsNone(future.result())

    def test_add_message_listener_with_instance(self):
        listener = MessageListener()
        reg_id = self.topic.add_message_listener(listener=listener)
        self.assertIsNotNone(reg_id)
        self.assertIn(reg_id, self.topic._message_listeners)

    def test_add_message_listener_with_reliable_listener(self):
        listener = ReliableMessageListener()
        reg_id = self.topic.add_message_listener(listener=listener)
        self.assertIsNotNone(reg_id)
        self.assertIn(reg_id, self.topic._listener_runners)

    def test_add_message_listener_with_callback(self):
        messages = []

        def on_message(msg):
            messages.append(msg)

        reg_id = self.topic.add_message_listener(on_message=on_message)
        self.assertIsNotNone(reg_id)

    def test_add_message_listener_requires_argument(self):
        with self.assertRaises(ValueError):
            self.topic.add_message_listener()

    def test_remove_message_listener(self):
        listener = MessageListener()
        reg_id = self.topic.add_message_listener(listener=listener)
        self.assertTrue(self.topic.remove_message_listener(reg_id))
        self.assertNotIn(reg_id, self.topic._message_listeners)

    def test_remove_nonexistent_listener(self):
        self.assertFalse(self.topic.remove_message_listener("nonexistent"))

    def test_destroy_stops_runners(self):
        listener = ReliableMessageListener()
        reg_id = self.topic.add_message_listener(listener=listener)
        self.topic.destroy()
        self.assertNotIn(reg_id, self.topic._listener_runners)
        self.assertEqual(0, len(self.topic._message_listeners))

    def test_handle_stale_sequence_with_loss_tolerant_listener(self):
        class LossTolerantListener(ReliableMessageListener):
            def is_loss_tolerant(self):
                return True

            def on_stale_sequence(self, head_sequence):
                return head_sequence + 5

        listener = LossTolerantListener()
        reg_id = self.topic.add_message_listener(listener=listener)

        new_seq = self.topic._handle_stale_sequence(reg_id, 10, 100)
        self.assertEqual(105, new_seq)

    def test_handle_stale_sequence_with_non_loss_tolerant_listener(self):
        listener = ReliableMessageListener()
        reg_id = self.topic.add_message_listener(listener=listener)

        with self.assertRaises(StaleSequenceException) as ctx:
            self.topic._handle_stale_sequence(reg_id, 10, 100)

        self.assertEqual(100, ctx.exception.head_sequence)

    def test_handle_stale_sequence_with_basic_listener(self):
        listener = MessageListener()
        reg_id = self.topic.add_message_listener(listener=listener)

        with self.assertRaises(StaleSequenceException):
            self.topic._handle_stale_sequence(reg_id, 10, 100)

    def test_handle_stale_sequence_unknown_listener(self):
        with self.assertRaises(StaleSequenceException):
            self.topic._handle_stale_sequence("unknown", 10, 100)


class TestMessageRunner(unittest.TestCase):
    """Tests for _MessageRunner internal class."""

    def test_basic_runner(self):
        listener = MessageListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner(
            registration_id="test-id",
            listener=listener,
            initial_sequence=0,
            config=config,
        )
        self.assertEqual(0, runner.sequence)
        self.assertFalse(runner.is_cancelled)

    def test_cancel_runner(self):
        listener = MessageListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner(
            registration_id="test-id",
            listener=listener,
            initial_sequence=0,
            config=config,
        )
        runner.cancel()
        self.assertTrue(runner.is_cancelled)

    def test_process_message(self):
        received = []

        class TestListener(MessageListener):
            def on_message(self, msg):
                received.append(msg.message)

        listener = TestListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner(
            registration_id="test-id",
            listener=listener,
            initial_sequence=0,
            config=config,
        )

        msg = TopicMessage(message="hello", publish_time=100)
        runner.process_message(msg)

        self.assertEqual(["hello"], received)
        self.assertEqual(1, runner.sequence)

    def test_process_message_when_cancelled(self):
        received = []

        class TestListener(MessageListener):
            def on_message(self, msg):
                received.append(msg.message)

        listener = TestListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner(
            registration_id="test-id",
            listener=listener,
            initial_sequence=0,
            config=config,
        )
        runner.cancel()

        msg = TopicMessage(message="hello", publish_time=100)
        runner.process_message(msg)

        self.assertEqual([], received)

    def test_process_message_stores_sequence_for_reliable_listener(self):
        stored_sequences = []

        class TestReliableListener(ReliableMessageListener):
            def on_message(self, msg):
                pass

            def store_sequence(self, sequence):
                stored_sequences.append(sequence)

        listener = TestReliableListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner(
            registration_id="test-id",
            listener=listener,
            initial_sequence=0,
            config=config,
        )

        msg = TopicMessage(message="hello", publish_time=100)
        runner.process_message(msg)

        self.assertIn(1, stored_sequences)

    def test_process_message_with_terminal_error(self):
        class FailingListener(MessageListener):
            def on_message(self, msg):
                raise ValueError("Test error")

        listener = FailingListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner(
            registration_id="test-id",
            listener=listener,
            initial_sequence=0,
            config=config,
        )

        msg = TopicMessage(message="hello", publish_time=100)

        with self.assertRaises(ValueError):
            runner.process_message(msg)

        self.assertTrue(runner.is_cancelled)

    def test_process_message_with_non_terminal_error(self):
        class NonTerminalListener(ReliableMessageListener):
            def on_message(self, msg):
                raise ValueError("Test error")

            def is_terminal(self, error):
                return False

        listener = NonTerminalListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner(
            registration_id="test-id",
            listener=listener,
            initial_sequence=0,
            config=config,
        )

        msg = TopicMessage(message="hello", publish_time=100)
        runner.process_message(msg)
        self.assertFalse(runner.is_cancelled)

    def test_set_sequence_stores_for_reliable_listener(self):
        stored_sequences = []

        class TestReliableListener(ReliableMessageListener):
            def store_sequence(self, sequence):
                stored_sequences.append(sequence)

        listener = TestReliableListener()
        config = ReliableTopicConfig()
        runner = _MessageRunner(
            registration_id="test-id",
            listener=listener,
            initial_sequence=0,
            config=config,
        )

        runner.sequence = 50
        self.assertEqual(50, runner.sequence)
        self.assertIn(50, stored_sequences)


if __name__ == "__main__":
    unittest.main()
