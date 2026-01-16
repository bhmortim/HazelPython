"""Integration tests for IMap operations against a real Hazelcast cluster."""

import pytest
import time
import threading
from typing import Dict, List

from tests.integration.conftest import skip_integration, DOCKER_AVAILABLE


@skip_integration
class TestMapBasicOperations:
    """Test basic IMap CRUD operations."""

    def test_put_and_get(self, test_map):
        """Test basic put and get operations."""
        test_map.put("key1", "value1")
        result = test_map.get("key1")
        assert result == "value1"

    def test_put_all(self, test_map):
        """Test put_all operation."""
        entries = {f"key{i}": f"value{i}" for i in range(10)}
        test_map.put_all(entries)
        
        assert test_map.size() == 10
        for key, value in entries.items():
            assert test_map.get(key) == value

    def test_remove(self, test_map):
        """Test remove operation."""
        test_map.put("key1", "value1")
        removed = test_map.remove("key1")
        
        assert removed == "value1"
        assert test_map.get("key1") is None

    def test_contains_key(self, test_map):
        """Test contains_key operation."""
        test_map.put("key1", "value1")
        
        assert test_map.contains_key("key1") is True
        assert test_map.contains_key("nonexistent") is False

    def test_contains_value(self, test_map):
        """Test contains_value operation."""
        test_map.put("key1", "value1")
        
        assert test_map.contains_value("value1") is True
        assert test_map.contains_value("nonexistent") is False

    def test_size_and_is_empty(self, test_map):
        """Test size and is_empty operations."""
        assert test_map.is_empty() is True
        assert test_map.size() == 0
        
        test_map.put("key1", "value1")
        
        assert test_map.is_empty() is False
        assert test_map.size() == 1

    def test_clear(self, test_map):
        """Test clear operation."""
        for i in range(10):
            test_map.put(f"key{i}", f"value{i}")
        
        assert test_map.size() == 10
        test_map.clear()
        assert test_map.size() == 0

    def test_key_set(self, test_map):
        """Test key_set operation."""
        entries = {"a": 1, "b": 2, "c": 3}
        test_map.put_all(entries)
        
        keys = test_map.key_set()
        assert set(keys) == {"a", "b", "c"}

    def test_values(self, test_map):
        """Test values operation."""
        entries = {"a": 1, "b": 2, "c": 3}
        test_map.put_all(entries)
        
        values = test_map.values()
        assert set(values) == {1, 2, 3}

    def test_entry_set(self, test_map):
        """Test entry_set operation."""
        entries = {"a": 1, "b": 2}
        test_map.put_all(entries)
        
        entry_set = test_map.entry_set()
        assert len(entry_set) == 2


@skip_integration
class TestMapAtomicOperations:
    """Test atomic IMap operations."""

    def test_put_if_absent(self, test_map):
        """Test put_if_absent operation."""
        result1 = test_map.put_if_absent("key1", "value1")
        assert result1 is None
        
        result2 = test_map.put_if_absent("key1", "value2")
        assert result2 == "value1"
        assert test_map.get("key1") == "value1"

    def test_replace(self, test_map):
        """Test replace operation."""
        test_map.put("key1", "value1")
        
        old = test_map.replace("key1", "value2")
        assert old == "value1"
        assert test_map.get("key1") == "value2"

    def test_replace_if_same(self, test_map):
        """Test conditional replace operation."""
        test_map.put("key1", "value1")
        
        result1 = test_map.replace_if_same("key1", "value1", "value2")
        assert result1 is True
        assert test_map.get("key1") == "value2"
        
        result2 = test_map.replace_if_same("key1", "wrong", "value3")
        assert result2 is False
        assert test_map.get("key1") == "value2"

    def test_remove_if_same(self, test_map):
        """Test conditional remove operation."""
        test_map.put("key1", "value1")
        
        result1 = test_map.remove_if_same("key1", "wrong")
        assert result1 is False
        assert test_map.contains_key("key1") is True
        
        result2 = test_map.remove_if_same("key1", "value1")
        assert result2 is True
        assert test_map.contains_key("key1") is False


@skip_integration
class TestMapTTL:
    """Test IMap TTL (Time-To-Live) functionality."""

    def test_put_with_ttl(self, test_map):
        """Test put with TTL."""
        test_map.put("key1", "value1", ttl=1)
        
        assert test_map.get("key1") == "value1"
        time.sleep(2)
        assert test_map.get("key1") is None

    def test_set_ttl(self, test_map):
        """Test setting TTL on existing entry."""
        test_map.put("key1", "value1")
        test_map.set_ttl("key1", 1)
        
        assert test_map.get("key1") == "value1"
        time.sleep(2)
        assert test_map.get("key1") is None


@skip_integration
class TestMapConcurrency:
    """Test concurrent IMap operations."""

    def test_concurrent_puts(self, test_map):
        """Test concurrent put operations."""
        errors: List[Exception] = []
        
        def worker(thread_id: int):
            try:
                for i in range(50):
                    key = f"thread-{thread_id}-key-{i}"
                    test_map.put(key, f"value-{i}")
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert test_map.size() == 250

    def test_concurrent_read_write(self, test_map):
        """Test concurrent read and write operations."""
        test_map.put("counter", 0)
        errors: List[Exception] = []
        
        def writer():
            try:
                for i in range(100):
                    test_map.put("counter", i)
            except Exception as e:
                errors.append(e)
        
        def reader():
            try:
                for _ in range(100):
                    test_map.get("counter")
            except Exception as e:
                errors.append(e)
        
        threads = [
            threading.Thread(target=writer),
            threading.Thread(target=reader),
            threading.Thread(target=reader),
        ]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0


@skip_integration
class TestMapListeners:
    """Test IMap entry listeners."""

    def test_entry_added_listener(self, test_map):
        """Test entry added listener."""
        events: List[dict] = []
        
        def on_added(event):
            events.append({"type": "added", "key": event.key, "value": event.value})
        
        test_map.add_entry_listener(on_added=on_added)
        test_map.put("key1", "value1")
        
        time.sleep(0.5)
        assert len(events) >= 1
        assert any(e["key"] == "key1" for e in events)

    def test_entry_removed_listener(self, test_map):
        """Test entry removed listener."""
        events: List[dict] = []
        
        def on_removed(event):
            events.append({"type": "removed", "key": event.key})
        
        test_map.put("key1", "value1")
        test_map.add_entry_listener(on_removed=on_removed)
        test_map.remove("key1")
        
        time.sleep(0.5)
        assert len(events) >= 1


@skip_integration
class TestMapDataTypes:
    """Test IMap with various data types."""

    def test_integer_values(self, test_map):
        """Test map with integer values."""
        test_map.put("int", 42)
        assert test_map.get("int") == 42

    def test_float_values(self, test_map):
        """Test map with float values."""
        test_map.put("float", 3.14159)
        result = test_map.get("float")
        assert abs(result - 3.14159) < 0.0001

    def test_boolean_values(self, test_map):
        """Test map with boolean values."""
        test_map.put("bool_true", True)
        test_map.put("bool_false", False)
        
        assert test_map.get("bool_true") is True
        assert test_map.get("bool_false") is False

    def test_list_values(self, test_map):
        """Test map with list values."""
        test_map.put("list", [1, 2, 3, "four"])
        result = test_map.get("list")
        assert result == [1, 2, 3, "four"]

    def test_dict_values(self, test_map):
        """Test map with dict values."""
        test_map.put("dict", {"nested": {"key": "value"}})
        result = test_map.get("dict")
        assert result == {"nested": {"key": "value"}}

    def test_none_value(self, test_map):
        """Test map with None value."""
        test_map.put("none", None)
        assert test_map.get("none") is None
        assert test_map.contains_key("none") is True
