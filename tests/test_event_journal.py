"""Unit tests for Event Journal functionality."""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future

from hazelcast.event_journal import (
    EventType,
    EventJournalEvent,
    EventJournalState,
    ReadResultSet,
    EventJournalReader,
)
from hazelcast.protocol.codec import (
    MapEventJournalCodec,
    CacheEventJournalCodec,
    MAP_EVENT_JOURNAL_SUBSCRIBE,
    MAP_EVENT_JOURNAL_READ,
    CACHE_EVENT_JOURNAL_SUBSCRIBE,
    CACHE_EVENT_JOURNAL_READ,
)


class TestEventType(unittest.TestCase):
    """Tests for EventType enum."""

    def test_event_types(self):
        """Test event type values."""
        self.assertEqual(EventType.ADDED, 1)
        self.assertEqual(EventType.REMOVED, 2)
        self.assertEqual(EventType.UPDATED, 4)
        self.assertEqual(EventType.EVICTED, 8)
        self.assertEqual(EventType.EXPIRED, 16)
        self.assertEqual(EventType.MERGED, 32)
        self.assertEqual(EventType.LOADED, 64)


class TestEventJournalEvent(unittest.TestCase):
    """Tests for EventJournalEvent class."""

    def test_added_event(self):
        """Test creation of an added event."""
        event = EventJournalEvent(
            event_type=EventType.ADDED,
            key="key1",
            new_value="value1",
            sequence=100,
        )
        
        self.assertEqual(event.event_type, EventType.ADDED)
        self.assertEqual(event.key, "key1")
        self.assertEqual(event.new_value, "value1")
        self.assertIsNone(event.old_value)
        self.assertEqual(event.sequence, 100)
        self.assertTrue(event.is_added)
        self.assertFalse(event.is_removed)
        self.assertFalse(event.is_updated)

    def test_updated_event(self):
        """Test creation of an updated event."""
        event = EventJournalEvent(
            event_type=EventType.UPDATED,
            key="key1",
            new_value="new_value",
            old_value="old_value",
            sequence=101,
        )
        
        self.assertEqual(event.event_type, EventType.UPDATED)
        self.assertEqual(event.key, "key1")
        self.assertEqual(event.new_value, "new_value")
        self.assertEqual(event.old_value, "old_value")
        self.assertTrue(event.is_updated)
        self.assertFalse(event.is_added)

    def test_removed_event(self):
        """Test creation of a removed event."""
        event = EventJournalEvent(
            event_type=EventType.REMOVED,
            key="key1",
            old_value="removed_value",
        )
        
        self.assertTrue(event.is_removed)
        self.assertFalse(event.is_added)
        self.assertEqual(event.old_value, "removed_value")
        self.assertIsNone(event.new_value)

    def test_evicted_event(self):
        """Test creation of an evicted event."""
        event = EventJournalEvent(
            event_type=EventType.EVICTED,
            key="key1",
        )
        
        self.assertTrue(event.is_evicted)
        self.assertFalse(event.is_expired)

    def test_expired_event(self):
        """Test creation of an expired event."""
        event = EventJournalEvent(
            event_type=EventType.EXPIRED,
            key="key1",
        )
        
        self.assertTrue(event.is_expired)
        self.assertFalse(event.is_evicted)

    def test_repr(self):
        """Test string representation."""
        event = EventJournalEvent(
            event_type=EventType.ADDED,
            key="key1",
            new_value="value1",
            sequence=100,
        )
        
        repr_str = repr(event)
        self.assertIn("ADDED", repr_str)
        self.assertIn("key1", repr_str)
        self.assertIn("value1", repr_str)
        self.assertIn("100", repr_str)


class TestEventJournalState(unittest.TestCase):
    """Tests for EventJournalState class."""

    def test_state_properties(self):
        """Test state property access."""
        state = EventJournalState(oldest_sequence=10, newest_sequence=100)
        
        self.assertEqual(state.oldest_sequence, 10)
        self.assertEqual(state.newest_sequence, 100)
        self.assertFalse(state.is_empty)

    def test_empty_state(self):
        """Test empty journal state."""
        state = EventJournalState(oldest_sequence=0, newest_sequence=-1)
        
        self.assertTrue(state.is_empty)

    def test_repr(self):
        """Test string representation."""
        state = EventJournalState(oldest_sequence=10, newest_sequence=100)
        
        repr_str = repr(state)
        self.assertIn("10", repr_str)
        self.assertIn("100", repr_str)


class TestReadResultSet(unittest.TestCase):
    """Tests for ReadResultSet class."""

    def test_result_set_properties(self):
        """Test result set property access."""
        events = [
            EventJournalEvent(EventType.ADDED, "key1", "value1"),
            EventJournalEvent(EventType.UPDATED, "key2", "value2", "old_value2"),
        ]
        result = ReadResultSet(events=events, read_count=2, next_sequence=102)
        
        self.assertEqual(result.read_count, 2)
        self.assertEqual(result.next_sequence, 102)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.events, events)

    def test_iteration(self):
        """Test iterating over result set."""
        events = [
            EventJournalEvent(EventType.ADDED, "key1", "value1"),
            EventJournalEvent(EventType.ADDED, "key2", "value2"),
        ]
        result = ReadResultSet(events=events, read_count=2, next_sequence=102)
        
        collected = list(result)
        self.assertEqual(len(collected), 2)
        self.assertEqual(collected[0].key, "key1")
        self.assertEqual(collected[1].key, "key2")

    def test_indexing(self):
        """Test index access."""
        events = [
            EventJournalEvent(EventType.ADDED, "key1", "value1"),
            EventJournalEvent(EventType.ADDED, "key2", "value2"),
        ]
        result = ReadResultSet(events=events, read_count=2, next_sequence=102)
        
        self.assertEqual(result[0].key, "key1")
        self.assertEqual(result[1].key, "key2")

    def test_repr(self):
        """Test string representation."""
        result = ReadResultSet(events=[], read_count=0, next_sequence=100)
        
        repr_str = repr(result)
        self.assertIn("count=0", repr_str)
        self.assertIn("next_seq=100", repr_str)


class TestMapEventJournalCodec(unittest.TestCase):
    """Tests for MapEventJournalCodec."""

    def test_encode_subscribe_request(self):
        """Test encoding subscribe request."""
        msg = MapEventJournalCodec.encode_subscribe_request("test-map")
        
        self.assertIsNotNone(msg)
        frames = list(msg.frames)
        self.assertGreater(len(frames), 0)

    def test_encode_read_request(self):
        """Test encoding read request."""
        msg = MapEventJournalCodec.encode_read_request(
            name="test-map",
            start_sequence=100,
            min_size=1,
            max_size=50,
        )
        
        self.assertIsNotNone(msg)
        frames = list(msg.frames)
        self.assertGreater(len(frames), 0)

    def test_encode_read_request_with_filter(self):
        """Test encoding read request with filter."""
        filter_data = b"serialized_predicate"
        msg = MapEventJournalCodec.encode_read_request(
            name="test-map",
            start_sequence=100,
            min_size=1,
            max_size=50,
            filter_data=filter_data,
        )
        
        self.assertIsNotNone(msg)


class TestCacheEventJournalCodec(unittest.TestCase):
    """Tests for CacheEventJournalCodec."""

    def test_encode_subscribe_request(self):
        """Test encoding subscribe request."""
        msg = CacheEventJournalCodec.encode_subscribe_request("test-cache")
        
        self.assertIsNotNone(msg)
        frames = list(msg.frames)
        self.assertGreater(len(frames), 0)

    def test_encode_read_request(self):
        """Test encoding read request."""
        msg = CacheEventJournalCodec.encode_read_request(
            name="test-cache",
            start_sequence=100,
            min_size=1,
            max_size=50,
        )
        
        self.assertIsNotNone(msg)
        frames = list(msg.frames)
        self.assertGreater(len(frames), 0)

    def test_encode_read_request_with_filter(self):
        """Test encoding read request with filter."""
        filter_data = b"serialized_predicate"
        msg = CacheEventJournalCodec.encode_read_request(
            name="test-cache",
            start_sequence=100,
            min_size=1,
            max_size=50,
            filter_data=filter_data,
        )
        
        self.assertIsNotNone(msg)


class TestEventJournalReader(unittest.TestCase):
    """Tests for EventJournalReader class."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_proxy = MagicMock()
        self.mock_proxy._name = "test-map"
        self.mock_proxy._check_not_destroyed = MagicMock()
        self.mock_proxy._to_data = MagicMock(return_value=b"serialized")
        self.mock_proxy._to_object = MagicMock(side_effect=lambda x: x.decode() if x else None)

    def test_reader_creation_for_map(self):
        """Test creating reader for map proxy."""
        reader = EventJournalReader(self.mock_proxy)
        
        self.assertFalse(reader._is_cache)

    def test_reader_creation_for_cache(self):
        """Test creating reader for cache proxy."""
        self.mock_proxy.config = MagicMock()
        self.mock_proxy.config.key_type = str
        
        reader = EventJournalReader(self.mock_proxy)
        
        self.assertTrue(reader._is_cache)

    def test_subscribe_async(self):
        """Test async subscription."""
        future = Future()
        future.set_result(None)
        self.mock_proxy._invoke = MagicMock(return_value=future)
        
        reader = EventJournalReader(self.mock_proxy)
        result_future = reader.subscribe_async()
        
        self.mock_proxy._check_not_destroyed.assert_called_once()
        self.mock_proxy._invoke.assert_called_once()

    def test_read_many_validation(self):
        """Test parameter validation for read_many."""
        reader = EventJournalReader(self.mock_proxy)
        
        with self.assertRaises(ValueError):
            reader.read_many_async(start_sequence=0, min_size=-1, max_size=10)
        
        with self.assertRaises(ValueError):
            reader.read_many_async(start_sequence=0, min_size=20, max_size=10)
        
        with self.assertRaises(ValueError):
            reader.read_many_async(start_sequence=0, min_size=1, max_size=2000)

    def test_read_many_async(self):
        """Test async read_many."""
        future = Future()
        future.set_result(None)
        self.mock_proxy._invoke = MagicMock(return_value=future)
        
        reader = EventJournalReader(self.mock_proxy)
        result_future = reader.read_many_async(
            start_sequence=100,
            min_size=1,
            max_size=50,
        )
        
        self.mock_proxy._check_not_destroyed.assert_called_once()
        self.mock_proxy._invoke.assert_called_once()

    def test_deserialize_events(self):
        """Test event deserialization."""
        reader = EventJournalReader(self.mock_proxy)
        
        events_data = [
            (EventType.ADDED, b"key1", b"value1", None),
            (EventType.UPDATED, b"key2", b"new_value", b"old_value"),
        ]
        sequences = [100, 101]
        
        events = reader._deserialize_events(events_data, sequences)
        
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].event_type, EventType.ADDED)
        self.assertEqual(events[0].sequence, 100)
        self.assertEqual(events[1].event_type, EventType.UPDATED)
        self.assertEqual(events[1].sequence, 101)


class TestProtocolConstants(unittest.TestCase):
    """Tests for protocol message type constants."""

    def test_map_event_journal_constants(self):
        """Test Map Event Journal protocol constants."""
        self.assertEqual(MAP_EVENT_JOURNAL_SUBSCRIBE, 0x014100)
        self.assertEqual(MAP_EVENT_JOURNAL_READ, 0x014200)

    def test_cache_event_journal_constants(self):
        """Test Cache Event Journal protocol constants."""
        self.assertEqual(CACHE_EVENT_JOURNAL_SUBSCRIBE, 0x131600)
        self.assertEqual(CACHE_EVENT_JOURNAL_READ, 0x131700)


if __name__ == "__main__":
    unittest.main()
