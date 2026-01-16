"""Integration tests for Map operations with real cluster."""

import pytest
import time
import threading
from typing import Dict, List, Set

from tests.integration.conftest import skip_integration, DOCKER_AVAILABLE


@skip_integration
class TestMapBasicOperations:
    """Test basic Map operations against real cluster."""

    def test_put_and_get(self, connected_client):
        """Test basic put and get operations."""
        test_map = connected_client.get_map("basic-ops-map")

        test_map.put("string-key", "string-value")
        assert test_map.get("string-key") == "string-value"

        test_map.put("int-key", 42)
        assert test_map.get("int-key") == 42

        test_map.put("dict-key", {"nested": "value", "count": 10})
        result = test_map.get("dict-key")
        assert result["nested"] == "value"
        assert result["count"] == 10

    def test_remove(self, connected_client):
        """Test remove operation."""
        test_map = connected_client.get_map("remove-test-map")

        test_map.put("to-remove", "value")
        assert test_map.contains_key("to-remove")

        removed = test_map.remove("to-remove")
        assert removed == "value"
        assert not test_map.contains_key("to-remove")

    def test_delete(self, connected_client):
        """Test delete operation (no return value)."""
        test_map = connected_client.get_map("delete-test-map")

        test_map.put("to-delete", "value")
        test_map.delete("to-delete")
        assert not test_map.contains_key("to-delete")

    def test_contains_key(self, connected_client):
        """Test contains_key operation."""
        test_map = connected_client.get_map("contains-test-map")

        assert not test_map.contains_key("missing-key")
        test_map.put("existing-key", "value")
        assert test_map.contains_key("existing-key")

    def test_contains_value(self, connected_client):
        """Test contains_value operation."""
        test_map = connected_client.get_map("contains-value-map")

        test_map.put("key1", "unique-value")
        assert test_map.contains_value("unique-value")
        assert not test_map.contains_value("non-existent-value")

    def test_size_and_is_empty(self, connected_client):
        """Test size and is_empty operations."""
        test_map = connected_client.get_map("size-test-map")

        assert test_map.is_empty()
        assert test_map.size() == 0

        test_map.put("key1", "value1")
        test_map.put("key2", "value2")

        assert not test_map.is_empty()
        assert test_map.size() == 2

    def test_clear(self, connected_client):
        """Test clear operation."""
        test_map = connected_client.get_map("clear-test-map")

        for i in range(10):
            test_map.put(f"key-{i}", f"value-{i}")

        assert test_map.size() == 10

        test_map.clear()

        assert test_map.is_empty()
        assert test_map.size() == 0


@skip_integration
class TestMapBulkOperations:
    """Test bulk Map operations."""

    def test_put_all(self, connected_client):
        """Test put_all operation."""
        test_map = connected_client.get_map("put-all-map")

        entries = {f"bulk-key-{i}": f"bulk-value-{i}" for i in range(50)}
        test_map.put_all(entries)

        assert test_map.size() == 50
        for key, expected_value in entries.items():
            assert test_map.get(key) == expected_value

    def test_get_all(self, connected_client):
        """Test get_all operation."""
        test_map = connected_client.get_map("get-all-map")

        for i in range(20):
            test_map.put(f"key-{i}", f"value-{i}")

        keys_to_get = {f"key-{i}" for i in range(10)}
        results = test_map.get_all(keys_to_get)

        assert len(results) == 10
        for i in range(10):
            assert results[f"key-{i}"] == f"value-{i}"

    def test_key_set(self, connected_client):
        """Test key_set operation."""
        test_map = connected_client.get_map("key-set-map")

        expected_keys = {f"ks-key-{i}" for i in range(15)}
        for key in expected_keys:
            test_map.put(key, "value")

        keys = test_map.key_set()
        assert keys == expected_keys

    def test_values(self, connected_client):
        """Test values operation."""
        test_map = connected_client.get_map("values-map")

        for i in range(10):
            test_map.put(f"key-{i}", f"value-{i}")

        values = test_map.values()
        assert len(values) == 10
        for i in range(10):
            assert f"value-{i}" in values

    def test_entry_set(self, connected_client):
        """Test entry_set operation."""
        test_map = connected_client.get_map("entry-set-map")

        expected_entries = {(f"es-key-{i}", f"es-value-{i}") for i in range(10)}
        for key, value in expected_entries:
            test_map.put(key, value)

        entries = test_map.entry_set()
        assert entries == expected_entries


@skip_integration
class TestMapConditionalOperations:
    """Test conditional Map operations."""

    def test_put_if_absent(self, connected_client):
        """Test put_if_absent operation."""
        test_map = connected_client.get_map("put-if-absent-map")

        result = test_map.put_if_absent("new-key", "first-value")
        assert result is None
        assert test_map.get("new-key") == "first-value"

        result = test_map.put_if_absent("new-key", "second-value")
        assert result == "first-value"
        assert test_map.get("new-key") == "first-value"

    def test_replace(self, connected_client):
        """Test replace operation."""
        test_map = connected_client.get_map("replace-map")

        result = test_map.replace("missing-key", "value")
        assert result is None

        test_map.put("existing-key", "old-value")
        result = test_map.replace("existing-key", "new-value")
        assert result == "old-value"
        assert test_map.get("existing-key") == "new-value"

    def test_replace_if_same(self, connected_client):
        """Test replace_if_same operation."""
        test_map = connected_client.get_map("replace-if-same-map")

        test_map.put("key", "original")

        result = test_map.replace_if_same("key", "wrong", "new")
        assert result is False
        assert test_map.get("key") == "original"

        result = test_map.replace_if_same("key", "original", "updated")
        assert result is True
        assert test_map.get("key") == "updated"


@skip_integration
class TestMapTTL:
    """Test Map TTL (Time To Live) operations."""

    def test_put_with_ttl(self, connected_client):
        """Test put with TTL."""
        test_map = connected_client.get_map("ttl-map")

        test_map.put("ttl-key", "ttl-value", ttl=2.0)
        assert test_map.get("ttl-key") == "ttl-value"

        time.sleep(3)
        assert test_map.get("ttl-key") is None

    def test_set_with_ttl(self, connected_client):
        """Test set with TTL."""
        test_map = connected_client.get_map("set-ttl-map")

        test_map.set("set-ttl-key", "set-ttl-value", ttl=2.0)
        assert test_map.get("set-ttl-key") == "set-ttl-value"

        time.sleep(3)
        assert test_map.get("set-ttl-key") is None


@skip_integration
class TestMapLocking:
    """Test Map locking operations."""

    def test_lock_and_unlock(self, connected_client):
        """Test lock and unlock operations."""
        test_map = connected_client.get_map("lock-map")

        test_map.put("lock-key", "value")

        test_map.lock("lock-key")
        assert test_map.is_locked("lock-key")

        test_map.unlock("lock-key")
        assert not test_map.is_locked("lock-key")

    def test_try_lock(self, connected_client):
        """Test try_lock operation."""
        test_map = connected_client.get_map("try-lock-map")

        test_map.put("try-lock-key", "value")

        result = test_map.try_lock("try-lock-key", timeout=1.0)
        assert result is True

        test_map.unlock("try-lock-key")

    def test_force_unlock(self, connected_client):
        """Test force_unlock operation."""
        test_map = connected_client.get_map("force-unlock-map")

        test_map.put("force-key", "value")
        test_map.lock("force-key")
        assert test_map.is_locked("force-key")

        test_map.force_unlock("force-key")
        assert not test_map.is_locked("force-key")


@skip_integration
class TestMapConcurrency:
    """Test Map concurrent access patterns."""

    def test_concurrent_put_operations(self, connected_client):
        """Test concurrent put operations from multiple threads."""
        test_map = connected_client.get_map("concurrent-put-map")
        errors: List[Exception] = []
        thread_count = 10
        ops_per_thread = 100

        def worker(thread_id: int):
            try:
                for i in range(ops_per_thread):
                    key = f"t{thread_id}-k{i}"
                    test_map.put(key, f"value-{thread_id}-{i}")
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=worker, args=(i,))
            for i in range(thread_count)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0, f"Errors: {errors}"
        assert test_map.size() == thread_count * ops_per_thread

    def test_concurrent_get_operations(self, connected_client):
        """Test concurrent get operations from multiple threads."""
        test_map = connected_client.get_map("concurrent-get-map")

        for i in range(100):
            test_map.put(f"key-{i}", f"value-{i}")

        errors: List[Exception] = []
        results: List[bool] = []
        lock = threading.Lock()

        def reader():
            try:
                for i in range(100):
                    value = test_map.get(f"key-{i}")
                    with lock:
                        results.append(value == f"value-{i}")
            except Exception as e:
                with lock:
                    errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(10)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert all(results)


@skip_integration
class TestMapDunderMethods:
    """Test Map dunder method integration."""

    def test_len(self, connected_client):
        """Test __len__ method."""
        test_map = connected_client.get_map("len-map")

        assert len(test_map) == 0

        for i in range(5):
            test_map.put(f"key-{i}", f"value-{i}")

        assert len(test_map) == 5

    def test_contains(self, connected_client):
        """Test __contains__ method."""
        test_map = connected_client.get_map("contains-dunder-map")

        test_map.put("present-key", "value")

        assert "present-key" in test_map
        assert "absent-key" not in test_map

    def test_getitem(self, connected_client):
        """Test __getitem__ method."""
        test_map = connected_client.get_map("getitem-map")

        test_map.put("bracket-key", "bracket-value")
        assert test_map["bracket-key"] == "bracket-value"

    def test_setitem(self, connected_client):
        """Test __setitem__ method."""
        test_map = connected_client.get_map("setitem-map")

        test_map["assigned-key"] = "assigned-value"
        assert test_map.get("assigned-key") == "assigned-value"

    def test_delitem(self, connected_client):
        """Test __delitem__ method."""
        test_map = connected_client.get_map("delitem-map")

        test_map.put("delete-key", "delete-value")
        del test_map["delete-key"]
        assert not test_map.contains_key("delete-key")

    def test_iter(self, connected_client):
        """Test __iter__ method."""
        test_map = connected_client.get_map("iter-map")

        expected_keys = {f"iter-key-{i}" for i in range(10)}
        for key in expected_keys:
            test_map.put(key, "value")

        iterated_keys = set(test_map)
        assert iterated_keys == expected_keys
