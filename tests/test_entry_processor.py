"""Tests for Entry Processor functionality."""

import pytest
from concurrent.futures import Future
from typing import Any, Dict, Optional

from hazelcast.processor import (
    EntryProcessor,
    EntryProcessorEntry,
    SimpleEntryProcessorEntry,
)
from hazelcast.proxy.map import MapProxy


class IncrementProcessor(EntryProcessor[str, int]):
    """Entry processor that increments integer values."""

    def __init__(self, increment: int = 1):
        self._increment = increment

    def process(self, entry: EntryProcessorEntry[str, int]) -> int:
        current = entry.get_value() or 0
        new_value = current + self._increment
        entry.set_value(new_value)
        return current


class GetValueProcessor(EntryProcessor[str, Any]):
    """Entry processor that returns the current value without modification."""

    def process(self, entry: EntryProcessorEntry[str, Any]) -> Any:
        return entry.get_value()


class SetValueProcessor(EntryProcessor[str, Any]):
    """Entry processor that sets a fixed value."""

    def __init__(self, value: Any):
        self._value = value

    def process(self, entry: EntryProcessorEntry[str, Any]) -> Any:
        old_value = entry.get_value()
        entry.set_value(self._value)
        return old_value


class DeleteProcessor(EntryProcessor[str, Any]):
    """Entry processor that deletes the entry by setting value to None."""

    def process(self, entry: EntryProcessorEntry[str, Any]) -> Any:
        old_value = entry.get_value()
        entry.set_value(None)
        return old_value


class TestSimpleEntryProcessorEntry:
    """Tests for SimpleEntryProcessorEntry."""

    def test_get_key(self):
        entry = SimpleEntryProcessorEntry("test-key", "test-value")
        assert entry.get_key() == "test-key"

    def test_get_value(self):
        entry = SimpleEntryProcessorEntry("key", "value")
        assert entry.get_value() == "value"

    def test_get_value_none(self):
        entry = SimpleEntryProcessorEntry("key")
        assert entry.get_value() is None

    def test_set_value(self):
        entry = SimpleEntryProcessorEntry("key", "old")
        result = entry.set_value("new")
        assert result == "new"
        assert entry.get_value() == "new"


class TestIncrementProcessor:
    """Tests for IncrementProcessor."""

    def test_increment_existing_value(self):
        entry = SimpleEntryProcessorEntry("counter", 10)
        processor = IncrementProcessor(5)

        result = processor.process(entry)

        assert result == 10
        assert entry.get_value() == 15

    def test_increment_none_value(self):
        entry = SimpleEntryProcessorEntry("counter", None)
        processor = IncrementProcessor(1)

        result = processor.process(entry)

        assert result == 0
        assert entry.get_value() == 1

    def test_default_increment(self):
        entry = SimpleEntryProcessorEntry("counter", 5)
        processor = IncrementProcessor()

        processor.process(entry)

        assert entry.get_value() == 6


class TestGetValueProcessor:
    """Tests for GetValueProcessor."""

    def test_get_existing_value(self):
        entry = SimpleEntryProcessorEntry("key", "my-value")
        processor = GetValueProcessor()

        result = processor.process(entry)

        assert result == "my-value"
        assert entry.get_value() == "my-value"

    def test_get_none_value(self):
        entry = SimpleEntryProcessorEntry("key")
        processor = GetValueProcessor()

        result = processor.process(entry)

        assert result is None


class TestSetValueProcessor:
    """Tests for SetValueProcessor."""

    def test_set_value_returns_old(self):
        entry = SimpleEntryProcessorEntry("key", "old-value")
        processor = SetValueProcessor("new-value")

        result = processor.process(entry)

        assert result == "old-value"
        assert entry.get_value() == "new-value"

    def test_set_value_on_none(self):
        entry = SimpleEntryProcessorEntry("key")
        processor = SetValueProcessor("new-value")

        result = processor.process(entry)

        assert result is None
        assert entry.get_value() == "new-value"


class TestDeleteProcessor:
    """Tests for DeleteProcessor."""

    def test_delete_existing_entry(self):
        entry = SimpleEntryProcessorEntry("key", "value")
        processor = DeleteProcessor()

        result = processor.process(entry)

        assert result == "value"
        assert entry.get_value() is None


class TestMapProxyEntryProcessor:
    """Tests for MapProxy entry processor methods."""

    def test_execute_on_key_returns_future(self):
        map_proxy = MapProxy("test-map")
        processor = IncrementProcessor()

        result = map_proxy.execute_on_key_async("key", processor)

        assert isinstance(result, Future)

    def test_execute_on_key_sync(self):
        map_proxy = MapProxy("test-map")
        processor = IncrementProcessor()

        result = map_proxy.execute_on_key("key", processor)

        assert result is None

    def test_execute_on_keys_returns_future(self):
        map_proxy = MapProxy("test-map")
        processor = IncrementProcessor()

        result = map_proxy.execute_on_keys_async({"key1", "key2"}, processor)

        assert isinstance(result, Future)

    def test_execute_on_keys_sync(self):
        map_proxy = MapProxy("test-map")
        processor = IncrementProcessor()

        result = map_proxy.execute_on_keys({"key1", "key2"}, processor)

        assert result == {}

    def test_execute_on_entries_returns_future(self):
        map_proxy = MapProxy("test-map")
        processor = IncrementProcessor()

        result = map_proxy.execute_on_entries_async(processor)

        assert isinstance(result, Future)

    def test_execute_on_entries_sync(self):
        map_proxy = MapProxy("test-map")
        processor = IncrementProcessor()

        result = map_proxy.execute_on_entries(processor)

        assert result == {}

    def test_execute_on_all_entries_returns_future(self):
        map_proxy = MapProxy("test-map")
        processor = IncrementProcessor()

        result = map_proxy.execute_on_all_entries_async(processor)

        assert isinstance(result, Future)

    def test_execute_on_all_entries_sync(self):
        map_proxy = MapProxy("test-map")
        processor = IncrementProcessor()

        result = map_proxy.execute_on_all_entries(processor)

        assert result == {}
