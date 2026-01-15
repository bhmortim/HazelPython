"""Unit tests for CP Subsystem Atomic data structures."""

import unittest

from hazelcast.cp.atomic import AtomicLong, AtomicReference
from hazelcast.exceptions import IllegalStateException, IllegalArgumentException


class TestAtomicLong(unittest.TestCase):
    """Tests for AtomicLong."""

    def setUp(self):
        self.atomic = AtomicLong("test-atomic-long")

    def tearDown(self):
        if not self.atomic.is_destroyed:
            self.atomic.destroy()

    def test_initial_value_is_zero(self):
        self.assertEqual(0, self.atomic.get())

    def test_set_and_get(self):
        self.atomic.set(42)
        self.assertEqual(42, self.atomic.get())

    def test_get_and_set(self):
        self.atomic.set(10)
        previous = self.atomic.get_and_set(20)
        self.assertEqual(10, previous)
        self.assertEqual(20, self.atomic.get())

    def test_compare_and_set_success(self):
        self.atomic.set(5)
        result = self.atomic.compare_and_set(5, 10)
        self.assertTrue(result)
        self.assertEqual(10, self.atomic.get())

    def test_compare_and_set_failure(self):
        self.atomic.set(5)
        result = self.atomic.compare_and_set(3, 10)
        self.assertFalse(result)
        self.assertEqual(5, self.atomic.get())

    def test_get_and_add(self):
        self.atomic.set(10)
        previous = self.atomic.get_and_add(5)
        self.assertEqual(10, previous)
        self.assertEqual(15, self.atomic.get())

    def test_add_and_get(self):
        self.atomic.set(10)
        result = self.atomic.add_and_get(5)
        self.assertEqual(15, result)
        self.assertEqual(15, self.atomic.get())

    def test_get_and_increment(self):
        self.atomic.set(10)
        previous = self.atomic.get_and_increment()
        self.assertEqual(10, previous)
        self.assertEqual(11, self.atomic.get())

    def test_increment_and_get(self):
        self.atomic.set(10)
        result = self.atomic.increment_and_get()
        self.assertEqual(11, result)
        self.assertEqual(11, self.atomic.get())

    def test_get_and_decrement(self):
        self.atomic.set(10)
        previous = self.atomic.get_and_decrement()
        self.assertEqual(10, previous)
        self.assertEqual(9, self.atomic.get())

    def test_decrement_and_get(self):
        self.atomic.set(10)
        result = self.atomic.decrement_and_get()
        self.assertEqual(9, result)
        self.assertEqual(9, self.atomic.get())

    def test_alter(self):
        self.atomic.set(10)
        self.atomic.alter(lambda x: x * 2)
        self.assertEqual(20, self.atomic.get())

    def test_alter_with_none_function_raises(self):
        with self.assertRaises(IllegalArgumentException):
            self.atomic.alter(None)

    def test_alter_and_get(self):
        self.atomic.set(10)
        result = self.atomic.alter_and_get(lambda x: x * 2)
        self.assertEqual(20, result)
        self.assertEqual(20, self.atomic.get())

    def test_get_and_alter(self):
        self.atomic.set(10)
        previous = self.atomic.get_and_alter(lambda x: x * 2)
        self.assertEqual(10, previous)
        self.assertEqual(20, self.atomic.get())

    def test_apply(self):
        self.atomic.set(10)
        result = self.atomic.apply(lambda x: x * 3)
        self.assertEqual(30, result)
        self.assertEqual(10, self.atomic.get())

    def test_apply_with_none_function_raises(self):
        with self.assertRaises(IllegalArgumentException):
            self.atomic.apply(None)

    def test_async_get(self):
        self.atomic.set(42)
        future = self.atomic.get_async()
        self.assertEqual(42, future.result())

    def test_async_set(self):
        future = self.atomic.set_async(100)
        future.result()
        self.assertEqual(100, self.atomic.get())

    def test_operations_on_destroyed_raise(self):
        self.atomic.destroy()
        with self.assertRaises(IllegalStateException):
            self.atomic.get()

    def test_service_name(self):
        self.assertEqual("hz:raft:atomicLongService", self.atomic.service_name)


class TestAtomicReference(unittest.TestCase):
    """Tests for AtomicReference."""

    def setUp(self):
        self.atomic = AtomicReference("test-atomic-ref")

    def tearDown(self):
        if not self.atomic.is_destroyed:
            self.atomic.destroy()

    def test_initial_value_is_none(self):
        self.assertIsNone(self.atomic.get())
        self.assertTrue(self.atomic.is_null())

    def test_set_and_get(self):
        self.atomic.set("hello")
        self.assertEqual("hello", self.atomic.get())
        self.assertFalse(self.atomic.is_null())

    def test_get_and_set(self):
        self.atomic.set("old")
        previous = self.atomic.get_and_set("new")
        self.assertEqual("old", previous)
        self.assertEqual("new", self.atomic.get())

    def test_clear(self):
        self.atomic.set("value")
        self.atomic.clear()
        self.assertIsNone(self.atomic.get())
        self.assertTrue(self.atomic.is_null())

    def test_contains_true(self):
        self.atomic.set("test")
        self.assertTrue(self.atomic.contains("test"))

    def test_contains_false(self):
        self.atomic.set("test")
        self.assertFalse(self.atomic.contains("other"))

    def test_contains_none(self):
        self.assertTrue(self.atomic.contains(None))
        self.atomic.set("test")
        self.assertFalse(self.atomic.contains(None))

    def test_compare_and_set_success(self):
        self.atomic.set("old")
        result = self.atomic.compare_and_set("old", "new")
        self.assertTrue(result)
        self.assertEqual("new", self.atomic.get())

    def test_compare_and_set_failure(self):
        self.atomic.set("old")
        result = self.atomic.compare_and_set("wrong", "new")
        self.assertFalse(result)
        self.assertEqual("old", self.atomic.get())

    def test_compare_and_set_with_none(self):
        result = self.atomic.compare_and_set(None, "value")
        self.assertTrue(result)
        self.assertEqual("value", self.atomic.get())

    def test_alter(self):
        self.atomic.set("hello")
        self.atomic.alter(lambda x: x.upper() if x else x)
        self.assertEqual("HELLO", self.atomic.get())

    def test_alter_with_none_function_raises(self):
        with self.assertRaises(IllegalArgumentException):
            self.atomic.alter(None)

    def test_alter_and_get(self):
        self.atomic.set("hello")
        result = self.atomic.alter_and_get(lambda x: x.upper() if x else x)
        self.assertEqual("HELLO", result)
        self.assertEqual("HELLO", self.atomic.get())

    def test_get_and_alter(self):
        self.atomic.set("hello")
        previous = self.atomic.get_and_alter(lambda x: x.upper() if x else x)
        self.assertEqual("hello", previous)
        self.assertEqual("HELLO", self.atomic.get())

    def test_apply(self):
        self.atomic.set("hello")
        result = self.atomic.apply(lambda x: len(x) if x else 0)
        self.assertEqual(5, result)
        self.assertEqual("hello", self.atomic.get())

    def test_apply_with_none_function_raises(self):
        with self.assertRaises(IllegalArgumentException):
            self.atomic.apply(None)

    def test_set_complex_object(self):
        obj = {"key": "value", "number": 42}
        self.atomic.set(obj)
        self.assertEqual(obj, self.atomic.get())

    def test_async_get(self):
        self.atomic.set("async-value")
        future = self.atomic.get_async()
        self.assertEqual("async-value", future.result())

    def test_async_is_null(self):
        future = self.atomic.is_null_async()
        self.assertTrue(future.result())

    def test_operations_on_destroyed_raise(self):
        self.atomic.destroy()
        with self.assertRaises(IllegalStateException):
            self.atomic.get()

    def test_service_name(self):
        self.assertEqual("hz:raft:atomicRefService", self.atomic.service_name)


class TestAtomicLongNegativeValues(unittest.TestCase):
    """Tests for AtomicLong with negative values."""

    def setUp(self):
        self.atomic = AtomicLong("test-negative")

    def tearDown(self):
        if not self.atomic.is_destroyed:
            self.atomic.destroy()

    def test_set_negative(self):
        self.atomic.set(-100)
        self.assertEqual(-100, self.atomic.get())

    def test_add_negative(self):
        self.atomic.set(10)
        result = self.atomic.add_and_get(-15)
        self.assertEqual(-5, result)

    def test_decrement_from_zero(self):
        result = self.atomic.decrement_and_get()
        self.assertEqual(-1, result)


if __name__ == "__main__":
    unittest.main()
