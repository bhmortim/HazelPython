"""Tests for Event Journal functionality."""

import unittest
from unittest.mock import MagicMock, patch

from hazelcast.config import EventJournalConfig
from hazelcast.event_journal import (
    EventJournalEvent,
    EventJournalInitialSubscriberState,
    EventJournalReader,
    EventType,
    ReadResultSet,
)
from hazelcast.exceptions import ConfigurationException


class TestEventJournalConfig(unittest.TestCase):
    """Tests for EventJournalConfig."""

    def test_default_values(self):
        config = EventJournalConfig()
        self.assertFalse(config.enabled)
        self.assertEqual(10000, config.capacity)
        self.assertEqual(0, config.time_to_live_seconds)

    def test_custom_values(self):
        config = EventJournalConfig(
            enabled=True,
            capacity=50000,
            time_to_live_seconds=3600,
        )
        self.assertTrue(config.enabled)
        self.assertEqual(50000, config.capacity)
        self.assertEqual(3600, config.time_to_live_seconds)

    def test_capacity_validation(self):
        with self.assertRaises(ConfigurationException):
            EventJournalConfig(capacity=0)
        with self.assertRaises(ConfigurationException):
            EventJournalConfig(capacity=-1)

    def test_ttl_validation(self):
        with self.assertRaises(ConfigurationException):
            EventJournalConfig(time_to_live_seconds=-1)

    def test_from_dict(self):
        data = {
            "enabled": True,
            "capacity": 25000,
            "time_to_live_seconds": 1800,
        }
        config = EventJournalConfig.from_dict(data)
        self.assertTrue(config.enabled)
        self.assertEqual(25000, config.capacity)
        self.assertEqual(1800, config.time_to_live_seconds)

    def test_from_dict_defaults(self):
        config = EventJournalConfig.from_dict({})
        self.assertFalse(config.enabled)
        self.assertEqual(10000, config.capacity)
        self.assertEqual(0, config.time_to_live_seconds)

    def test_setters(self):
        config = EventJournalConfig()
        config.enabled = True
        config.capacity = 20000
        config.time_to_live_seconds = 600
        self.assertTrue(config.enabled)
        self.assertEqual(20000, config.capacity)
        self.assertEqual(600, config.time_to_live_seconds)


class TestEventType(unittest.TestCase):
    """Tests for EventType enum."""

    def test_event_types(self):
        self.assertEqual(1, EventType.ADDED)
        self.assertEqual(2, EventType.REMOVED)
        self.assertEqual(4, EventType.UPDATED)
        self.assertEqual(8, EventType.EVICTED)
        self.assertEqual(16, EventType.EXPIRED)
        self.assertEqual(32, EventType.LOADED)


class TestEventJournalEvent(unittest.TestCase):
    """Tests for EventJournalEvent."""

    def test_added_event(self):
        event = EventJournalEvent(
            sequence=1,
            key="key1",
            event_type=EventType.ADDED,
            new_value="value1",
        )
        self.assertEqual(1, event.sequence)
        self.assertEqual("key1", event.key)
        self.assertEqual(EventType.ADDED, event.event_type)
        self.assertEqual("value1", event.new_value)
        self.assertIsNone(event.old_value)
        self.assertTrue(event.is_added)
        self.assertFalse(event.is_removed)
        self.assertFalse(event.is_updated)

    def test_removed_event(self):
        event = EventJournalEvent(
            sequence=2,
            key="key1",
            event_type=EventType.REMOVED,
            old_value="value1",
        )
        self.assertTrue(event.is_removed)
        self.assertFalse(event.is_added)
        self.assertEqual("value1", event.old_value)
        self.assertIsNone(event.new_value)

    def test_updated_event(self):
        event = EventJournalEvent(
            sequence=3,
            key="key1",
            event_type=EventType.UPDATED,
            old_value="old",
            new_value="new",
        )
        self.assertTrue(event.is_updated)
        self.assertEqual("old", event.old_value)
        self.assertEqual("new", event.new_value)

    def test_evicted_event(self):
        event = EventJournalEvent(
            sequence=4,
            key="key1",
            event_type=EventType.EVICTED,
        )
        self.assertTrue(event.is_evicted)

    def test_expired_event(self):
        event = EventJournalEvent(
            sequence=5,
            key="key1",
            event_type=EventType.EXPIRED,
        )
        self.assertTrue(event.is_expired)

    def test_loaded_event(self):
        event = EventJournalEvent(
            sequence=6,
            key="key1",
            event_type=EventType.LOADED,
            new_value="loaded_value",
        )
        self.assertTrue(event.is_loaded)


class TestEventJournalInitialSubscriberState(unittest.TestCase):
    """Tests for EventJournalInitialSubscriberState."""

    def test_state_creation(self):
        state = EventJournalInitialSubscriberState(
            oldest_sequence=10,
            newest_sequence=100,
        )
        self.assertEqual(10, state.oldest_sequence)
        self.assertEqual(100, state.newest_sequence)

    def test_event_count(self):
        state = EventJournalInitialSubscriberState(
            oldest_sequence=10,
            newest_sequence=19,
        )
        self.assertEqual(10, state.event_count)

    def test_event_count_empty(self):
        state = EventJournalInitialSubscriberState(
            oldest_sequence=10,
            newest_sequence=9,
        )
        self.assertEqual(0, state.event_count)

    def test_event_count_single(self):
        state = EventJournalInitialSubscriberState(
            oldest_sequence=5,
            newest_sequence=5,
        )
        self.assertEqual(1, state.event_count)


class TestReadResultSet(unittest.TestCase):
    """Tests for ReadResultSet."""

    def test_empty_result_set(self):
        result = ReadResultSet(read_count=0, items=[], next_sequence=0)
        self.assertEqual(0, len(result))
        self.assertEqual(0, result.size)
        self.assertEqual(0, result.read_count)

    def test_result_set_with_items(self):
        events = [
            EventJournalEvent(sequence=0, key="k1", event_type=EventType.ADDED, new_value="v1"),
            EventJournalEvent(sequence=1, key="k2", event_type=EventType.ADDED, new_value="v2"),
        ]
        result = ReadResultSet(read_count=2, items=events, next_sequence=2)
        self.assertEqual(2, len(result))
        self.assertEqual(2, result.size)
        self.assertEqual(2, result.next_sequence)

    def test_result_set_iteration(self):
        events = [
            EventJournalEvent(sequence=0, key="k1", event_type=EventType.ADDED),
            EventJournalEvent(sequence=1, key="k2", event_type=EventType.REMOVED),
        ]
        result = ReadResultSet(read_count=2, items=events, next_sequence=2)
        
        keys = [e.key for e in result]
        self.assertEqual(["k1", "k2"], keys)

    def test_result_set_indexing(self):
        events = [
            EventJournalEvent(sequence=0, key="k1", event_type=EventType.ADDED),
            EventJournalEvent(sequence=1, key="k2", event_type=EventType.REMOVED),
        ]
        result = ReadResultSet(read_count=2, items=events, next_sequence=2)
        
        self.assertEqual("k1", result[0].key)
        self.assertEqual("k2", result[1].key)


class TestEventJournalReader(unittest.TestCase):
    """Tests for EventJournalReader."""

    def setUp(self):
        self.mock_proxy = MagicMock()
        self.mock_proxy._check_not_destroyed = MagicMock()
        self.reader = EventJournalReader(self.mock_proxy)

    def test_initial_state(self):
        self.assertEqual(self.mock_proxy, self.reader.proxy)
        self.assertEqual(0, self.reader.current_sequence)
        self.assertFalse(self.reader.is_subscribed)
        self.assertIsNone(self.reader.initial_state)

    def test_set_current_sequence(self):
        self.reader.current_sequence = 100
        self.assertEqual(100, self.reader.current_sequence)

    def test_set_negative_sequence_raises(self):
        with self.assertRaises(ValueError):
            self.reader.current_sequence = -1

    def test_subscribe(self):
        state = self.reader.subscribe()
        self.assertTrue(self.reader.is_subscribed)
        self.assertIsNotNone(self.reader.initial_state)
        self.assertEqual(0, state.oldest_sequence)
        self.assertEqual(-1, state.newest_sequence)

    def test_read_from_event_journal_validates_params(self):
        with self.assertRaises(ValueError) as ctx:
            self.reader.read_from_event_journal(start_sequence=-1)
        self.assertIn("start_sequence", str(ctx.exception))

        with self.assertRaises(ValueError) as ctx:
            self.reader.read_from_event_journal(start_sequence=0, min_size=-1)
        self.assertIn("min_size", str(ctx.exception))

        with self.assertRaises(ValueError) as ctx:
            self.reader.read_from_event_journal(start_sequence=0, max_size=0)
        self.assertIn("max_size", str(ctx.exception))

        with self.assertRaises(ValueError) as ctx:
            self.reader.read_from_event_journal(start_sequence=0, min_size=10, max_size=5)
        self.assertIn("min_size cannot be greater than max_size", str(ctx.exception))

    def test_read_from_event_journal_returns_result_set(self):
        result = self.reader.read_from_event_journal(start_sequence=0, max_size=10)
        self.assertIsInstance(result, ReadResultSet)
        self.assertEqual(0, result.read_count)
        self.assertEqual(0, result.next_sequence)

    def test_read_many(self):
        self.reader.current_sequence = 5
        result = self.reader.read_many(max_size=10)
        self.assertIsInstance(result, ReadResultSet)

    def test_read_many_updates_sequence(self):
        self.reader.current_sequence = 0
        result = self.reader.read_many(max_size=10)
        self.assertEqual(result.next_sequence, self.reader.current_sequence)

    def test_reset_to_oldest_without_subscribe_raises(self):
        with self.assertRaises(RuntimeError) as ctx:
            self.reader.reset_to_oldest()
        self.assertIn("subscribe()", str(ctx.exception))

    def test_reset_to_newest_without_subscribe_raises(self):
        with self.assertRaises(RuntimeError) as ctx:
            self.reader.reset_to_newest()
        self.assertIn("subscribe()", str(ctx.exception))

    def test_reset_to_oldest(self):
        self.reader.subscribe()
        self.reader._initial_state = EventJournalInitialSubscriberState(
            oldest_sequence=10,
            newest_sequence=100,
        )
        self.reader.reset_to_oldest()
        self.assertEqual(10, self.reader.current_sequence)

    def test_reset_to_newest(self):
        self.reader.subscribe()
        self.reader._initial_state = EventJournalInitialSubscriberState(
            oldest_sequence=10,
            newest_sequence=100,
        )
        self.reader.reset_to_newest()
        self.assertEqual(100, self.reader.current_sequence)


class TestMapProxyEventJournal(unittest.TestCase):
    """Tests for MapProxy event journal methods."""

    def test_get_event_journal_reader(self):
        from hazelcast.proxy.map import MapProxy
        
        mock_context = MagicMock()
        map_proxy = MapProxy("test-map", mock_context)
        map_proxy._destroyed = False
        
        reader = map_proxy.get_event_journal_reader()
        self.assertIsInstance(reader, EventJournalReader)
        self.assertEqual(map_proxy, reader.proxy)

    def test_read_from_event_journal(self):
        from hazelcast.proxy.map import MapProxy
        
        mock_context = MagicMock()
        map_proxy = MapProxy("test-map", mock_context)
        map_proxy._destroyed = False
        
        result = map_proxy.read_from_event_journal(start_sequence=0, max_size=10)
        self.assertIsInstance(result, ReadResultSet)


if __name__ == "__main__":
    unittest.main()
