"""Unit tests for the Ringbuffer proxy."""

import unittest
from concurrent.futures import Future

from hazelcast.proxy.ringbuffer import (
    OverflowPolicy,
    ReadResultSet,
    RingbufferProxy,
)


class TestOverflowPolicy(unittest.TestCase):
    """Tests for OverflowPolicy enum."""

    def test_overwrite_value(self):
        self.assertEqual(OverflowPolicy.OVERWRITE.value, 0)

    def test_fail_value(self):
        self.assertEqual(OverflowPolicy.FAIL.value, 1)


class TestRingbufferProxy(unittest.TestCase):
    """Tests for RingbufferProxy class."""

    def setUp(self):
        self.ringbuffer = RingbufferProxy("test-ringbuffer", capacity=5)

    def test_init_with_name(self):
        rb = RingbufferProxy("my-buffer")
        self.assertEqual(rb.name, "my-buffer")
        self.assertEqual(rb.service_name, "hz:impl:ringbufferService")

    def test_init_with_custom_capacity(self):
        rb = RingbufferProxy("buffer", capacity=100)
        self.assertEqual(rb.capacity(), 100)

    def test_init_with_zero_capacity_uses_minimum(self):
        rb = RingbufferProxy("buffer", capacity=0)
        self.assertEqual(rb.capacity(), 1)

    def test_capacity(self):
        self.assertEqual(self.ringbuffer.capacity(), 5)

    def test_capacity_async(self):
        future = self.ringbuffer.capacity_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 5)

    def test_initial_size_is_zero(self):
        self.assertEqual(self.ringbuffer.size(), 0)

    def test_initial_tail_sequence_is_minus_one(self):
        self.assertEqual(self.ringbuffer.tail_sequence(), -1)

    def test_initial_head_sequence_is_zero(self):
        self.assertEqual(self.ringbuffer.head_sequence(), 0)

    def test_initial_remaining_capacity(self):
        self.assertEqual(self.ringbuffer.remaining_capacity(), 5)

    def test_add_single_item(self):
        seq = self.ringbuffer.add("item1")
        self.assertEqual(seq, 0)
        self.assertEqual(self.ringbuffer.size(), 1)
        self.assertEqual(self.ringbuffer.tail_sequence(), 0)

    def test_add_multiple_items(self):
        self.ringbuffer.add("item1")
        seq = self.ringbuffer.add("item2")
        self.assertEqual(seq, 1)
        self.assertEqual(self.ringbuffer.size(), 2)

    def test_add_updates_remaining_capacity(self):
        self.ringbuffer.add("item1")
        self.assertEqual(self.ringbuffer.remaining_capacity(), 4)

    def test_add_async(self):
        future = self.ringbuffer.add_async("item1")
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 0)

    def test_add_with_overwrite_when_full(self):
        for i in range(5):
            self.ringbuffer.add(f"item{i}")

        seq = self.ringbuffer.add("new_item", OverflowPolicy.OVERWRITE)
        self.assertEqual(seq, 5)
        self.assertEqual(self.ringbuffer.size(), 5)
        self.assertEqual(self.ringbuffer.head_sequence(), 1)
        self.assertEqual(self.ringbuffer.tail_sequence(), 5)

    def test_add_with_fail_when_full(self):
        for i in range(5):
            self.ringbuffer.add(f"item{i}")

        seq = self.ringbuffer.add("new_item", OverflowPolicy.FAIL)
        self.assertEqual(seq, RingbufferProxy.SEQUENCE_UNAVAILABLE)
        self.assertEqual(self.ringbuffer.size(), 5)

    def test_add_all_empty_list(self):
        seq = self.ringbuffer.add_all([])
        self.assertEqual(seq, -1)
        self.assertEqual(self.ringbuffer.size(), 0)

    def test_add_all_single_item(self):
        seq = self.ringbuffer.add_all(["item1"])
        self.assertEqual(seq, 0)
        self.assertEqual(self.ringbuffer.size(), 1)

    def test_add_all_multiple_items(self):
        seq = self.ringbuffer.add_all(["item1", "item2", "item3"])
        self.assertEqual(seq, 2)
        self.assertEqual(self.ringbuffer.size(), 3)

    def test_add_all_with_overwrite_when_full(self):
        self.ringbuffer.add_all(["a", "b", "c", "d", "e"])
        seq = self.ringbuffer.add_all(["f", "g"], OverflowPolicy.OVERWRITE)
        self.assertEqual(seq, 6)
        self.assertEqual(self.ringbuffer.size(), 5)
        self.assertEqual(self.ringbuffer.head_sequence(), 2)

    def test_add_all_with_fail_when_insufficient_capacity(self):
        self.ringbuffer.add_all(["a", "b", "c"])
        seq = self.ringbuffer.add_all(["d", "e", "f"], OverflowPolicy.FAIL)
        self.assertEqual(seq, RingbufferProxy.SEQUENCE_UNAVAILABLE)
        self.assertEqual(self.ringbuffer.size(), 3)

    def test_add_all_async(self):
        future = self.ringbuffer.add_all_async(["item1", "item2"])
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 1)

    def test_read_one(self):
        self.ringbuffer.add("item1")
        item = self.ringbuffer.read_one(0)
        self.assertEqual(item, "item1")

    def test_read_one_multiple_items(self):
        self.ringbuffer.add_all(["a", "b", "c"])
        self.assertEqual(self.ringbuffer.read_one(0), "a")
        self.assertEqual(self.ringbuffer.read_one(1), "b")
        self.assertEqual(self.ringbuffer.read_one(2), "c")

    def test_read_one_stale_sequence_raises(self):
        for i in range(7):
            self.ringbuffer.add(f"item{i}")

        with self.assertRaises(Exception) as context:
            self.ringbuffer.read_one(0)
        self.assertIn("older than head", str(context.exception))

    def test_read_one_beyond_tail_raises(self):
        self.ringbuffer.add("item1")
        with self.assertRaises(Exception) as context:
            self.ringbuffer.read_one(5)
        self.assertIn("beyond tail", str(context.exception))

    def test_read_one_empty_buffer_raises(self):
        with self.assertRaises(Exception) as context:
            self.ringbuffer.read_one(0)
        self.assertIn("beyond tail", str(context.exception))

    def test_read_one_async(self):
        self.ringbuffer.add("item1")
        future = self.ringbuffer.read_one_async(0)
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), "item1")

    def test_read_many(self):
        self.ringbuffer.add_all(["a", "b", "c", "d", "e"])
        result = self.ringbuffer.read_many(0, 1, 3)
        self.assertEqual(len(result), 3)
        self.assertEqual(list(result), ["a", "b", "c"])

    def test_read_many_from_middle(self):
        self.ringbuffer.add_all(["a", "b", "c", "d", "e"])
        result = self.ringbuffer.read_many(2, 1, 2)
        self.assertEqual(list(result), ["c", "d"])

    def test_read_many_with_filter(self):
        self.ringbuffer.add_all([1, 2, 3, 4, 5])
        result = self.ringbuffer.read_many(0, 1, 5, lambda x: x % 2 == 0)
        self.assertEqual(list(result), [2, 4])

    def test_read_many_read_count(self):
        self.ringbuffer.add_all(["a", "b", "c"])
        result = self.ringbuffer.read_many(0, 1, 3)
        self.assertEqual(result.read_count, 3)

    def test_read_many_next_sequence(self):
        self.ringbuffer.add_all(["a", "b", "c"])
        result = self.ringbuffer.read_many(0, 1, 2)
        self.assertEqual(result.next_sequence_to_read, 2)

    def test_read_many_stale_sequence_raises(self):
        for i in range(7):
            self.ringbuffer.add(f"item{i}")

        with self.assertRaises(Exception) as context:
            self.ringbuffer.read_many(0, 1, 3)
        self.assertIn("older than head", str(context.exception))

    def test_read_many_negative_min_count_raises(self):
        with self.assertRaises(Exception) as context:
            self.ringbuffer.read_many(0, -1, 3)
        self.assertIn("non-negative", str(context.exception))

    def test_read_many_max_less_than_min_raises(self):
        with self.assertRaises(Exception) as context:
            self.ringbuffer.read_many(0, 5, 3)
        self.assertIn("max_count", str(context.exception))

    def test_read_many_async(self):
        self.ringbuffer.add_all(["a", "b", "c"])
        future = self.ringbuffer.read_many_async(0, 1, 2)
        self.assertIsInstance(future, Future)
        result = future.result()
        self.assertEqual(len(result), 2)

    def test_len(self):
        self.assertEqual(len(self.ringbuffer), 0)
        self.ringbuffer.add_all(["a", "b", "c"])
        self.assertEqual(len(self.ringbuffer), 3)

    def test_destroy(self):
        self.ringbuffer.destroy()
        self.assertTrue(self.ringbuffer.is_destroyed)

    def test_operations_after_destroy_raise(self):
        self.ringbuffer.destroy()
        with self.assertRaises(Exception):
            self.ringbuffer.add("item")

    def test_repr(self):
        self.assertIn("test-ringbuffer", repr(self.ringbuffer))


class TestReadResultSet(unittest.TestCase):
    """Tests for ReadResultSet class."""

    def test_items(self):
        result = ReadResultSet(["a", "b", "c"], 3, 3)
        self.assertEqual(result.items, ["a", "b", "c"])

    def test_read_count(self):
        result = ReadResultSet(["a", "b"], 2, 2)
        self.assertEqual(result.read_count, 2)

    def test_next_sequence_to_read(self):
        result = ReadResultSet(["a", "b"], 2, 5)
        self.assertEqual(result.next_sequence_to_read, 5)

    def test_get_sequence_with_sequences(self):
        result = ReadResultSet(["a", "b", "c"], 3, 6, [3, 4, 5])
        self.assertEqual(result.get_sequence(0), 3)
        self.assertEqual(result.get_sequence(1), 4)
        self.assertEqual(result.get_sequence(2), 5)

    def test_get_sequence_without_sequences(self):
        result = ReadResultSet(["a", "b", "c"], 3, 3)
        self.assertEqual(result.get_sequence(0), 0)
        self.assertEqual(result.get_sequence(1), 1)
        self.assertEqual(result.get_sequence(2), 2)

    def test_get_sequence_out_of_range(self):
        result = ReadResultSet(["a", "b"], 2, 2)
        with self.assertRaises(IndexError):
            result.get_sequence(5)

    def test_len(self):
        result = ReadResultSet(["a", "b", "c"], 3, 3)
        self.assertEqual(len(result), 3)

    def test_getitem(self):
        result = ReadResultSet(["a", "b", "c"], 3, 3)
        self.assertEqual(result[0], "a")
        self.assertEqual(result[1], "b")
        self.assertEqual(result[2], "c")

    def test_iter(self):
        result = ReadResultSet(["a", "b", "c"], 3, 3)
        self.assertEqual(list(result), ["a", "b", "c"])

    def test_repr(self):
        result = ReadResultSet(["a", "b"], 2, 2)
        self.assertIn("ReadResultSet", repr(result))
        self.assertIn("read_count=2", repr(result))


if __name__ == "__main__":
    unittest.main()
