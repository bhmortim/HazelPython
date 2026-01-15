"""Unit tests for the PNCounter proxy."""

import unittest

from hazelcast.proxy.pn_counter import PNCounterProxy


class TestPNCounterProxy(unittest.TestCase):
    """Tests for PNCounterProxy."""

    def setUp(self):
        self.counter = PNCounterProxy("test-counter")

    def tearDown(self):
        if not self.counter.is_destroyed:
            self.counter.destroy()

    def test_initial_value_is_zero(self):
        self.assertEqual(0, self.counter.get())

    def test_get_async(self):
        future = self.counter.get_async()
        self.assertEqual(0, future.result())

    def test_get_and_add(self):
        previous = self.counter.get_and_add(5)
        self.assertEqual(0, previous)
        self.assertEqual(5, self.counter.get())

    def test_get_and_add_negative(self):
        self.counter.add_and_get(10)
        previous = self.counter.get_and_add(-3)
        self.assertEqual(10, previous)
        self.assertEqual(7, self.counter.get())

    def test_get_and_add_async(self):
        future = self.counter.get_and_add_async(7)
        self.assertEqual(0, future.result())
        self.assertEqual(7, self.counter.get())

    def test_add_and_get(self):
        result = self.counter.add_and_get(10)
        self.assertEqual(10, result)
        self.assertEqual(10, self.counter.get())

    def test_add_and_get_multiple(self):
        self.counter.add_and_get(5)
        result = self.counter.add_and_get(3)
        self.assertEqual(8, result)

    def test_add_and_get_async(self):
        future = self.counter.add_and_get_async(15)
        self.assertEqual(15, future.result())
        self.assertEqual(15, self.counter.get())

    def test_get_and_subtract(self):
        self.counter.add_and_get(20)
        previous = self.counter.get_and_subtract(5)
        self.assertEqual(20, previous)
        self.assertEqual(15, self.counter.get())

    def test_get_and_subtract_async(self):
        self.counter.add_and_get(10)
        future = self.counter.get_and_subtract_async(3)
        self.assertEqual(10, future.result())
        self.assertEqual(7, self.counter.get())

    def test_subtract_and_get(self):
        self.counter.add_and_get(25)
        result = self.counter.subtract_and_get(10)
        self.assertEqual(15, result)
        self.assertEqual(15, self.counter.get())

    def test_subtract_and_get_async(self):
        self.counter.add_and_get(30)
        future = self.counter.subtract_and_get_async(5)
        self.assertEqual(25, future.result())

    def test_get_and_increment(self):
        previous = self.counter.get_and_increment()
        self.assertEqual(0, previous)
        self.assertEqual(1, self.counter.get())

    def test_get_and_increment_async(self):
        future = self.counter.get_and_increment_async()
        self.assertEqual(0, future.result())
        self.assertEqual(1, self.counter.get())

    def test_increment_and_get(self):
        result = self.counter.increment_and_get()
        self.assertEqual(1, result)
        self.assertEqual(1, self.counter.get())

    def test_increment_and_get_multiple(self):
        self.counter.increment_and_get()
        self.counter.increment_and_get()
        result = self.counter.increment_and_get()
        self.assertEqual(3, result)

    def test_increment_and_get_async(self):
        future = self.counter.increment_and_get_async()
        self.assertEqual(1, future.result())

    def test_get_and_decrement(self):
        self.counter.add_and_get(5)
        previous = self.counter.get_and_decrement()
        self.assertEqual(5, previous)
        self.assertEqual(4, self.counter.get())

    def test_get_and_decrement_async(self):
        self.counter.add_and_get(3)
        future = self.counter.get_and_decrement_async()
        self.assertEqual(3, future.result())
        self.assertEqual(2, self.counter.get())

    def test_decrement_and_get(self):
        self.counter.add_and_get(10)
        result = self.counter.decrement_and_get()
        self.assertEqual(9, result)

    def test_decrement_and_get_async(self):
        self.counter.add_and_get(7)
        future = self.counter.decrement_and_get_async()
        self.assertEqual(6, future.result())

    def test_reset(self):
        self.counter.add_and_get(100)
        self.counter.reset()
        self.assertEqual(0, self.counter.get())

    def test_negative_values(self):
        self.counter.subtract_and_get(5)
        self.assertEqual(-5, self.counter.get())

    def test_service_name(self):
        self.assertEqual("hz:impl:PNCounterService", self.counter.service_name)

    def test_name(self):
        self.assertEqual("test-counter", self.counter.name)

    def test_destroyed_counter_raises(self):
        self.counter.destroy()
        with self.assertRaises(Exception):
            self.counter.get()

    def test_repr(self):
        repr_str = repr(self.counter)
        self.assertIn("PNCounterProxy", repr_str)
        self.assertIn("test-counter", repr_str)


class TestPNCounterCRDTBehavior(unittest.TestCase):
    """Tests for CRDT-specific behavior."""

    def test_concurrent_increments_converge(self):
        counter1 = PNCounterProxy("shared-counter")
        counter2 = PNCounterProxy("shared-counter")

        counter1.increment_and_get()
        counter2.increment_and_get()

        self.assertEqual(1, counter1.get())
        self.assertEqual(1, counter2.get())

        counter1.destroy()
        counter2.destroy()

    def test_mixed_operations(self):
        counter = PNCounterProxy("mixed-ops")

        counter.add_and_get(10)
        counter.subtract_and_get(3)
        counter.increment_and_get()
        counter.decrement_and_get()

        self.assertEqual(7, counter.get())
        counter.destroy()


if __name__ == "__main__":
    unittest.main()
