"""Comprehensive unit tests for CP Subsystem CPMap."""

import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from hazelcast.cp.cp_map import CPMap


class TestCPMap(unittest.TestCase):
    """Tests for CPMap distributed data structure."""

    def setUp(self):
        with patch("hazelcast.cp.cp_map.Proxy.__init__", return_value=None):
            self.cp_map = CPMap("hz:raft:mapService", "test-map")
            self.cp_map._name = "test-map"
            self.cp_map._invoke = MagicMock()
            self.cp_map._to_data = MagicMock(side_effect=lambda x: f"data:{x}" if x else None)
            self.cp_map._to_object = MagicMock(side_effect=lambda x: x)

    def _mock_invoke_result(self, result):
        """Create a mock future with result."""
        future = Future()
        future.set_result(result)
        self.cp_map._invoke.return_value = future

    def test_parse_group_id_with_at_symbol(self):
        """Test _parse_group_id() extracts group from name."""
        with patch("hazelcast.cp.cp_map.Proxy.__init__", return_value=None):
            cp_map = CPMap("service", "map@mygroup")
            cp_map._name = "map@mygroup"
            self.assertEqual(cp_map._parse_group_id("map@mygroup"), "mygroup")

    def test_parse_group_id_without_at_symbol(self):
        """Test _parse_group_id() returns default when no group specified."""
        self.assertEqual(self.cp_map._parse_group_id("map"), "default")

    def test_get_object_name_with_at_symbol(self):
        """Test _get_object_name() extracts name without group suffix."""
        with patch("hazelcast.cp.cp_map.Proxy.__init__", return_value=None):
            cp_map = CPMap("service", "map@mygroup")
            cp_map._name = "map@mygroup"
            self.assertEqual(cp_map._get_object_name(), "map")

    def test_get_object_name_without_at_symbol(self):
        """Test _get_object_name() returns full name when no group suffix."""
        self.assertEqual(self.cp_map._get_object_name(), "test-map")

    def test_get_existing_key(self):
        """Test get() returns value for existing key."""
        self._mock_invoke_result("value1")
        result = self.cp_map.get("key1")
        self.assertEqual(result, "value1")
        self.cp_map._invoke.assert_called_once()

    def test_get_nonexistent_key(self):
        """Test get() returns None for nonexistent key."""
        self._mock_invoke_result(None)
        result = self.cp_map.get("nonexistent")
        self.assertIsNone(result)

    def test_get_async(self):
        """Test get_async() returns Future."""
        self._mock_invoke_result("async-value")
        future = self.cp_map.get_async("key")
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), "async-value")

    def test_put_returns_old_value(self):
        """Test put() returns previous value."""
        self._mock_invoke_result("old-value")
        result = self.cp_map.put("key", "new-value")
        self.assertEqual(result, "old-value")

    def test_put_returns_none_for_new_key(self):
        """Test put() returns None for new key."""
        self._mock_invoke_result(None)
        result = self.cp_map.put("new-key", "value")
        self.assertIsNone(result)

    def test_put_async(self):
        """Test put_async() returns Future."""
        self._mock_invoke_result("previous")
        future = self.cp_map.put_async("key", "value")
        self.assertIsInstance(future, Future)

    def test_set(self):
        """Test set() sets value without returning old value."""
        self._mock_invoke_result(None)
        self.cp_map.set("key", "value")
        self.cp_map._invoke.assert_called_once()

    def test_set_async(self):
        """Test set_async() returns Future."""
        self._mock_invoke_result(None)
        future = self.cp_map.set_async("key", "value")
        self.assertIsInstance(future, Future)

    def test_remove_returns_value(self):
        """Test remove() returns removed value."""
        self._mock_invoke_result("removed-value")
        result = self.cp_map.remove("key")
        self.assertEqual(result, "removed-value")

    def test_remove_returns_none_for_nonexistent(self):
        """Test remove() returns None for nonexistent key."""
        self._mock_invoke_result(None)
        result = self.cp_map.remove("nonexistent")
        self.assertIsNone(result)

    def test_remove_async(self):
        """Test remove_async() returns Future."""
        self._mock_invoke_result("removed")
        future = self.cp_map.remove_async("key")
        self.assertIsInstance(future, Future)

    def test_delete(self):
        """Test delete() removes key without returning value."""
        self._mock_invoke_result(None)
        self.cp_map.delete("key")
        self.cp_map._invoke.assert_called_once()

    def test_delete_async(self):
        """Test delete_async() returns Future."""
        self._mock_invoke_result(None)
        future = self.cp_map.delete_async("key")
        self.assertIsInstance(future, Future)

    def test_compare_and_set_success(self):
        """Test compare_and_set() returns True on success."""
        self._mock_invoke_result(True)
        result = self.cp_map.compare_and_set("key", "expected", "update")
        self.assertTrue(result)

    def test_compare_and_set_failure(self):
        """Test compare_and_set() returns False on failure."""
        self._mock_invoke_result(False)
        result = self.cp_map.compare_and_set("key", "expected", "update")
        self.assertFalse(result)

    def test_compare_and_set_with_none_expected(self):
        """Test compare_and_set() with None expected value (insert if absent)."""
        self._mock_invoke_result(True)
        result = self.cp_map.compare_and_set("key", None, "new-value")
        self.assertTrue(result)

    def test_compare_and_set_with_none_update(self):
        """Test compare_and_set() with None update value (conditional delete)."""
        self._mock_invoke_result(True)
        result = self.cp_map.compare_and_set("key", "expected", None)
        self.assertTrue(result)

    def test_compare_and_set_async(self):
        """Test compare_and_set_async() returns Future."""
        self._mock_invoke_result(True)
        future = self.cp_map.compare_and_set_async("key", "old", "new")
        self.assertIsInstance(future, Future)

    def test_multiple_operations(self):
        """Test sequence of operations."""
        self._mock_invoke_result(None)
        self.cp_map.set("key1", "value1")

        self._mock_invoke_result("value1")
        result = self.cp_map.get("key1")
        self.assertEqual(result, "value1")

        self._mock_invoke_result("value1")
        old = self.cp_map.put("key1", "value2")
        self.assertEqual(old, "value1")

    def test_get_with_complex_key(self):
        """Test get() with complex key type."""
        self._mock_invoke_result("complex-value")
        result = self.cp_map.get(("tuple", "key"))
        self.assertEqual(result, "complex-value")

    def test_put_with_complex_value(self):
        """Test put() with complex value type."""
        self._mock_invoke_result(None)
        complex_value = {"nested": {"data": [1, 2, 3]}}
        self.cp_map.put("key", complex_value)
        self.cp_map._invoke.assert_called()

    def test_group_name_parsing_various_formats(self):
        """Test group name parsing with various name formats."""
        test_cases = [
            ("simple", "default", "simple"),
            ("name@group", "group", "name"),
            ("multi@part@group", "part@group", "multi"),
            ("@onlygroup", "onlygroup", ""),
        ]

        for full_name, expected_group, expected_name in test_cases:
            with patch("hazelcast.cp.cp_map.Proxy.__init__", return_value=None):
                cp_map = CPMap("service", full_name)
                cp_map._name = full_name
                self.assertEqual(cp_map._parse_group_id(full_name), expected_group)
                self.assertEqual(cp_map._get_object_name(), expected_name)


class TestCPMapEdgeCases(unittest.TestCase):
    """Edge case tests for CPMap."""

    def setUp(self):
        with patch("hazelcast.cp.cp_map.Proxy.__init__", return_value=None):
            self.cp_map = CPMap("hz:raft:mapService", "edge-case-map")
            self.cp_map._name = "edge-case-map"
            self.cp_map._invoke = MagicMock()
            self.cp_map._to_data = MagicMock(side_effect=lambda x: f"data:{x}" if x else None)
            self.cp_map._to_object = MagicMock(side_effect=lambda x: x)

    def _mock_invoke_result(self, result):
        """Create a mock future with result."""
        future = Future()
        future.set_result(result)
        self.cp_map._invoke.return_value = future

    def test_get_empty_string_key(self):
        """Test get() with empty string key."""
        self._mock_invoke_result("value")
        result = self.cp_map.get("")
        self.assertEqual(result, "value")

    def test_put_empty_string_value(self):
        """Test put() with empty string value."""
        self._mock_invoke_result(None)
        self.cp_map.put("key", "")
        self.cp_map._invoke.assert_called_once()

    def test_get_unicode_key(self):
        """Test get() with unicode key."""
        self._mock_invoke_result("unicode-value")
        result = self.cp_map.get("key")
        self.assertEqual(result, "unicode-value")

    def test_put_large_value(self):
        """Test put() with large value."""
        self._mock_invoke_result(None)
        large_value = "x" * 10000
        self.cp_map.put("key", large_value)
        self.cp_map._invoke.assert_called_once()

    def test_compare_and_set_both_none(self):
        """Test compare_and_set() with both expected and update None."""
        self._mock_invoke_result(True)
        result = self.cp_map.compare_and_set("key", None, None)
        self.assertTrue(result)

    def test_sequential_compare_and_set(self):
        """Test sequential compare_and_set operations."""
        self._mock_invoke_result(True)
        result1 = self.cp_map.compare_and_set("key", None, "v1")
        self.assertTrue(result1)

        self._mock_invoke_result(True)
        result2 = self.cp_map.compare_and_set("key", "v1", "v2")
        self.assertTrue(result2)

        self._mock_invoke_result(False)
        result3 = self.cp_map.compare_and_set("key", "v1", "v3")
        self.assertFalse(result3)


class TestCPMapConcurrency(unittest.TestCase):
    """Concurrency-related tests for CPMap."""

    def setUp(self):
        with patch("hazelcast.cp.cp_map.Proxy.__init__", return_value=None):
            self.cp_map = CPMap("hz:raft:mapService", "concurrent-map")
            self.cp_map._name = "concurrent-map"
            self.cp_map._invoke = MagicMock()
            self.cp_map._to_data = MagicMock(side_effect=lambda x: f"data:{x}" if x else None)
            self.cp_map._to_object = MagicMock(side_effect=lambda x: x)

    def test_async_operations_return_futures(self):
        """Test that all async operations return Future instances."""
        future = Future()
        future.set_result(None)
        self.cp_map._invoke.return_value = future

        async_methods = [
            lambda: self.cp_map.get_async("key"),
            lambda: self.cp_map.put_async("key", "value"),
            lambda: self.cp_map.set_async("key", "value"),
            lambda: self.cp_map.remove_async("key"),
            lambda: self.cp_map.delete_async("key"),
            lambda: self.cp_map.compare_and_set_async("key", "old", "new"),
        ]

        for method in async_methods:
            result = method()
            self.assertIsInstance(result, Future)

    def test_optimistic_locking_pattern(self):
        """Test optimistic locking pattern with compare_and_set."""
        call_count = [0]

        def mock_cas(*args, **kwargs):
            call_count[0] += 1
            future = Future()
            future.set_result(call_count[0] >= 3)
            return future

        self.cp_map._invoke = mock_cas

        max_retries = 5
        success = False
        for _ in range(max_retries):
            if self.cp_map.compare_and_set("key", "old", "new"):
                success = True
                break

        self.assertTrue(success)
        self.assertEqual(call_count[0], 3)


if __name__ == "__main__":
    unittest.main()
