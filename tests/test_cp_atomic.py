"""Comprehensive unit tests for CP Subsystem AtomicLong and AtomicReference."""

import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch, PropertyMock

from hazelcast.cp.atomic import AtomicLong, AtomicReference


class MockClientMessage:
    """Mock client message for testing."""
    pass


class TestAtomicLong(unittest.TestCase):
    """Tests for AtomicLong distributed data structure."""

    def setUp(self):
        with patch("hazelcast.cp.atomic.Proxy.__init__", return_value=None):
            self.atomic = AtomicLong("hz:raft:atomicLongService", "test-counter")
            self.atomic._name = "test-counter"
            self.atomic._invoke = MagicMock()
            self.atomic._to_data = MagicMock(side_effect=lambda x: f"data:{x}")
            self.atomic._to_object = MagicMock(side_effect=lambda x: x)

    def _mock_invoke_result(self, result):
        """Create a mock future with result."""
        future = Future()
        future.set_result(result)
        self.atomic._invoke.return_value = future

    def test_parse_group_id_with_at_symbol(self):
        """Test _parse_group_id() extracts group from name."""
        with patch("hazelcast.cp.atomic.Proxy.__init__", return_value=None):
            atomic = AtomicLong("service", "counter@mygroup")
            atomic._name = "counter@mygroup"
            self.assertEqual(atomic._parse_group_id("counter@mygroup"), "mygroup")

    def test_parse_group_id_without_at_symbol(self):
        """Test _parse_group_id() returns default when no group specified."""
        self.assertEqual(self.atomic._parse_group_id("counter"), "default")

    def test_get_object_name_with_at_symbol(self):
        """Test _get_object_name() extracts name without group suffix."""
        with patch("hazelcast.cp.atomic.Proxy.__init__", return_value=None):
            atomic = AtomicLong("service", "counter@mygroup")
            atomic._name = "counter@mygroup"
            self.assertEqual(atomic._get_object_name(), "counter")

    def test_get_object_name_without_at_symbol(self):
        """Test _get_object_name() returns full name when no group suffix."""
        self.assertEqual(self.atomic._get_object_name(), "test-counter")

    def test_get(self):
        """Test get() returns current value."""
        self._mock_invoke_result(42)
        result = self.atomic.get()
        self.assertEqual(result, 42)
        self.atomic._invoke.assert_called_once()

    def test_get_async(self):
        """Test get_async() returns Future."""
        self._mock_invoke_result(100)
        future = self.atomic.get_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 100)

    def test_set(self):
        """Test set() sets value."""
        self._mock_invoke_result(None)
        self.atomic.set(99)
        self.atomic._invoke.assert_called_once()

    def test_set_async(self):
        """Test set_async() returns Future."""
        self._mock_invoke_result(None)
        future = self.atomic.set_async(50)
        self.assertIsInstance(future, Future)

    def test_get_and_set(self):
        """Test get_and_set() returns old value."""
        self._mock_invoke_result(10)
        result = self.atomic.get_and_set(20)
        self.assertEqual(result, 10)

    def test_get_and_set_async(self):
        """Test get_and_set_async() returns Future."""
        self._mock_invoke_result(5)
        future = self.atomic.get_and_set_async(15)
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 5)

    def test_compare_and_set_success(self):
        """Test compare_and_set() returns True on success."""
        self._mock_invoke_result(True)
        result = self.atomic.compare_and_set(10, 20)
        self.assertTrue(result)

    def test_compare_and_set_failure(self):
        """Test compare_and_set() returns False on failure."""
        self._mock_invoke_result(False)
        result = self.atomic.compare_and_set(10, 20)
        self.assertFalse(result)

    def test_compare_and_set_async(self):
        """Test compare_and_set_async() returns Future."""
        self._mock_invoke_result(True)
        future = self.atomic.compare_and_set_async(5, 10)
        self.assertIsInstance(future, Future)

    def test_increment_and_get(self):
        """Test increment_and_get() increments and returns new value."""
        self._mock_invoke_result(11)
        result = self.atomic.increment_and_get()
        self.assertEqual(result, 11)

    def test_increment_and_get_async(self):
        """Test increment_and_get_async() returns Future."""
        self._mock_invoke_result(6)
        future = self.atomic.increment_and_get_async()
        self.assertIsInstance(future, Future)

    def test_decrement_and_get(self):
        """Test decrement_and_get() decrements and returns new value."""
        self._mock_invoke_result(9)
        result = self.atomic.decrement_and_get()
        self.assertEqual(result, 9)

    def test_decrement_and_get_async(self):
        """Test decrement_and_get_async() returns Future."""
        self._mock_invoke_result(4)
        future = self.atomic.decrement_and_get_async()
        self.assertIsInstance(future, Future)

    def test_get_and_increment(self):
        """Test get_and_increment() returns old value."""
        self._mock_invoke_result(10)
        result = self.atomic.get_and_increment()
        self.assertEqual(result, 10)

    def test_get_and_increment_async(self):
        """Test get_and_increment_async() returns Future."""
        self._mock_invoke_result(5)
        future = self.atomic.get_and_increment_async()
        self.assertIsInstance(future, Future)

    def test_get_and_decrement(self):
        """Test get_and_decrement() returns old value."""
        self._mock_invoke_result(10)
        result = self.atomic.get_and_decrement()
        self.assertEqual(result, 10)

    def test_get_and_decrement_async(self):
        """Test get_and_decrement_async() returns Future."""
        self._mock_invoke_result(5)
        future = self.atomic.get_and_decrement_async()
        self.assertIsInstance(future, Future)

    def test_add_and_get(self):
        """Test add_and_get() adds delta and returns new value."""
        self._mock_invoke_result(15)
        result = self.atomic.add_and_get(5)
        self.assertEqual(result, 15)

    def test_add_and_get_negative_delta(self):
        """Test add_and_get() with negative delta."""
        self._mock_invoke_result(5)
        result = self.atomic.add_and_get(-5)
        self.assertEqual(result, 5)

    def test_add_and_get_async(self):
        """Test add_and_get_async() returns Future."""
        self._mock_invoke_result(20)
        future = self.atomic.add_and_get_async(10)
        self.assertIsInstance(future, Future)

    def test_get_and_add(self):
        """Test get_and_add() returns old value."""
        self._mock_invoke_result(10)
        result = self.atomic.get_and_add(5)
        self.assertEqual(result, 10)

    def test_get_and_add_async(self):
        """Test get_and_add_async() returns Future."""
        self._mock_invoke_result(15)
        future = self.atomic.get_and_add_async(5)
        self.assertIsInstance(future, Future)

    def test_alter(self):
        """Test alter() applies function."""
        self._mock_invoke_result(None)
        mock_function = MagicMock()
        self.atomic.alter(mock_function)
        self.atomic._invoke.assert_called_once()

    def test_alter_async(self):
        """Test alter_async() returns Future."""
        self._mock_invoke_result(None)
        future = self.atomic.alter_async(lambda x: x * 2)
        self.assertIsInstance(future, Future)

    def test_alter_and_get(self):
        """Test alter_and_get() returns new value."""
        self._mock_invoke_result(20)
        result = self.atomic.alter_and_get(lambda x: x * 2)
        self.assertEqual(result, 20)

    def test_alter_and_get_async(self):
        """Test alter_and_get_async() returns Future."""
        self._mock_invoke_result(30)
        future = self.atomic.alter_and_get_async(lambda x: x * 3)
        self.assertIsInstance(future, Future)

    def test_get_and_alter(self):
        """Test get_and_alter() returns old value."""
        self._mock_invoke_result(10)
        result = self.atomic.get_and_alter(lambda x: x * 2)
        self.assertEqual(result, 10)

    def test_get_and_alter_async(self):
        """Test get_and_alter_async() returns Future."""
        self._mock_invoke_result(15)
        future = self.atomic.get_and_alter_async(lambda x: x + 5)
        self.assertIsInstance(future, Future)

    def test_apply(self):
        """Test apply() returns function result."""
        self._mock_invoke_result("result")
        result = self.atomic.apply(lambda x: str(x))
        self.assertEqual(result, "result")

    def test_apply_async(self):
        """Test apply_async() returns Future."""
        self._mock_invoke_result("applied")
        future = self.atomic.apply_async(lambda x: f"value:{x}")
        self.assertIsInstance(future, Future)

    def test_apply_async_with_none_result(self):
        """Test apply_async() handles None result."""
        self._mock_invoke_result(None)
        future = self.atomic.apply_async(lambda x: None)
        self.assertIsNone(future.result())


class TestAtomicReference(unittest.TestCase):
    """Tests for AtomicReference distributed data structure."""

    def setUp(self):
        with patch("hazelcast.cp.atomic.Proxy.__init__", return_value=None):
            self.ref = AtomicReference("hz:raft:atomicRefService", "test-ref")
            self.ref._name = "test-ref"
            self.ref._invoke = MagicMock()
            self.ref._to_data = MagicMock(side_effect=lambda x: f"data:{x}" if x else None)
            self.ref._to_object = MagicMock(side_effect=lambda x: x)

    def _mock_invoke_result(self, result):
        """Create a mock future with result."""
        future = Future()
        future.set_result(result)
        self.ref._invoke.return_value = future

    def test_parse_group_id_with_at_symbol(self):
        """Test _parse_group_id() extracts group from name."""
        with patch("hazelcast.cp.atomic.Proxy.__init__", return_value=None):
            ref = AtomicReference("service", "ref@mygroup")
            ref._name = "ref@mygroup"
            self.assertEqual(ref._parse_group_id("ref@mygroup"), "mygroup")

    def test_parse_group_id_without_at_symbol(self):
        """Test _parse_group_id() returns default when no group specified."""
        self.assertEqual(self.ref._parse_group_id("ref"), "default")

    def test_get_object_name_with_at_symbol(self):
        """Test _get_object_name() extracts name without group suffix."""
        with patch("hazelcast.cp.atomic.Proxy.__init__", return_value=None):
            ref = AtomicReference("service", "ref@mygroup")
            ref._name = "ref@mygroup"
            self.assertEqual(ref._get_object_name(), "ref")

    def test_get_object_name_without_at_symbol(self):
        """Test _get_object_name() returns full name when no group suffix."""
        self.assertEqual(self.ref._get_object_name(), "test-ref")

    def test_get(self):
        """Test get() returns current value."""
        self._mock_invoke_result({"key": "value"})
        result = self.ref.get()
        self.assertEqual(result, {"key": "value"})

    def test_get_returns_none(self):
        """Test get() returns None when not set."""
        self._mock_invoke_result(None)
        result = self.ref.get()
        self.assertIsNone(result)

    def test_get_async(self):
        """Test get_async() returns Future."""
        self._mock_invoke_result("test-value")
        future = self.ref.get_async()
        self.assertIsInstance(future, Future)

    def test_set(self):
        """Test set() sets value."""
        self._mock_invoke_result(None)
        self.ref.set({"new": "value"})
        self.ref._invoke.assert_called_once()

    def test_set_none(self):
        """Test set() with None value."""
        self._mock_invoke_result(None)
        self.ref.set(None)
        self.ref._invoke.assert_called_once()

    def test_set_async(self):
        """Test set_async() returns Future."""
        self._mock_invoke_result(None)
        future = self.ref.set_async("new-value")
        self.assertIsInstance(future, Future)

    def test_get_and_set(self):
        """Test get_and_set() returns old value."""
        self._mock_invoke_result("old-value")
        result = self.ref.get_and_set("new-value")
        self.assertEqual(result, "old-value")

    def test_get_and_set_async(self):
        """Test get_and_set_async() returns Future."""
        self._mock_invoke_result("previous")
        future = self.ref.get_and_set_async("next")
        self.assertIsInstance(future, Future)

    def test_compare_and_set_success(self):
        """Test compare_and_set() returns True on success."""
        self._mock_invoke_result(True)
        result = self.ref.compare_and_set("expected", "update")
        self.assertTrue(result)

    def test_compare_and_set_failure(self):
        """Test compare_and_set() returns False on failure."""
        self._mock_invoke_result(False)
        result = self.ref.compare_and_set("expected", "update")
        self.assertFalse(result)

    def test_compare_and_set_with_none_expected(self):
        """Test compare_and_set() with None expected value."""
        self._mock_invoke_result(True)
        result = self.ref.compare_and_set(None, "new-value")
        self.assertTrue(result)

    def test_compare_and_set_with_none_update(self):
        """Test compare_and_set() with None update value."""
        self._mock_invoke_result(True)
        result = self.ref.compare_and_set("expected", None)
        self.assertTrue(result)

    def test_compare_and_set_async(self):
        """Test compare_and_set_async() returns Future."""
        self._mock_invoke_result(True)
        future = self.ref.compare_and_set_async("old", "new")
        self.assertIsInstance(future, Future)

    def test_is_null_true(self):
        """Test is_null() returns True when value is None."""
        self._mock_invoke_result(True)
        result = self.ref.is_null()
        self.assertTrue(result)

    def test_is_null_false(self):
        """Test is_null() returns False when value is set."""
        self._mock_invoke_result(False)
        result = self.ref.is_null()
        self.assertFalse(result)

    def test_is_null_async(self):
        """Test is_null_async() returns Future."""
        self._mock_invoke_result(True)
        future = self.ref.is_null_async()
        self.assertIsInstance(future, Future)

    def test_clear(self):
        """Test clear() sets value to None."""
        self._mock_invoke_result(None)
        self.ref.clear()
        self.ref._invoke.assert_called_once()

    def test_clear_async(self):
        """Test clear_async() returns Future."""
        self._mock_invoke_result(None)
        future = self.ref.clear_async()
        self.assertIsInstance(future, Future)

    def test_contains_true(self):
        """Test contains() returns True when values match."""
        self._mock_invoke_result(True)
        result = self.ref.contains("value")
        self.assertTrue(result)

    def test_contains_false(self):
        """Test contains() returns False when values differ."""
        self._mock_invoke_result(False)
        result = self.ref.contains("other-value")
        self.assertFalse(result)

    def test_contains_with_none(self):
        """Test contains() with None value."""
        self._mock_invoke_result(True)
        result = self.ref.contains(None)
        self.assertTrue(result)

    def test_contains_async(self):
        """Test contains_async() returns Future."""
        self._mock_invoke_result(True)
        future = self.ref.contains_async("value")
        self.assertIsInstance(future, Future)

    def test_alter(self):
        """Test alter() applies function."""
        self._mock_invoke_result(None)
        self.ref.alter(lambda x: x.upper() if x else None)
        self.ref._invoke.assert_called_once()

    def test_alter_async(self):
        """Test alter_async() returns Future."""
        self._mock_invoke_result(None)
        future = self.ref.alter_async(lambda x: f"modified:{x}")
        self.assertIsInstance(future, Future)

    def test_alter_and_get(self):
        """Test alter_and_get() returns new value."""
        self._mock_invoke_result("MODIFIED")
        result = self.ref.alter_and_get(lambda x: x.upper())
        self.assertEqual(result, "MODIFIED")

    def test_alter_and_get_async(self):
        """Test alter_and_get_async() returns Future."""
        self._mock_invoke_result("new-value")
        future = self.ref.alter_and_get_async(lambda x: "new-value")
        self.assertIsInstance(future, Future)

    def test_get_and_alter(self):
        """Test get_and_alter() returns old value."""
        self._mock_invoke_result("old-value")
        result = self.ref.get_and_alter(lambda x: "new-value")
        self.assertEqual(result, "old-value")

    def test_get_and_alter_async(self):
        """Test get_and_alter_async() returns Future."""
        self._mock_invoke_result("previous")
        future = self.ref.get_and_alter_async(lambda x: "next")
        self.assertIsInstance(future, Future)

    def test_apply(self):
        """Test apply() returns function result."""
        self._mock_invoke_result(42)
        result = self.ref.apply(lambda x: len(x) if x else 0)
        self.assertEqual(result, 42)

    def test_apply_async(self):
        """Test apply_async() returns Future."""
        self._mock_invoke_result("computed")
        future = self.ref.apply_async(lambda x: f"result:{x}")
        self.assertIsInstance(future, Future)

    def test_apply_async_with_none_result(self):
        """Test apply_async() handles None result."""
        self._mock_invoke_result(None)
        future = self.ref.apply_async(lambda x: None)
        self.assertIsNone(future.result())


if __name__ == "__main__":
    unittest.main()
