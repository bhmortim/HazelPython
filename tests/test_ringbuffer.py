"""Unit tests for Ringbuffer proxy."""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future

from hazelcast.proxy.ringbuffer import (
    RingbufferProxy,
    OverflowPolicy,
    ReadResultSet,
)


class TestOverflowPolicy(unittest.TestCase):
    """Tests for OverflowPolicy enum."""

    def test_overflow_policy_values(self):
        """Test OverflowPolicy values are defined correctly."""
        self.assertEqual(OverflowPolicy.OVERWRITE.value, 0)
        self.assertEqual(OverflowPolicy.FAIL.value, 1)


class TestReadResultSet(unittest.TestCase):
    """Tests for ReadResultSet class."""

    def test_result_set_initialization(self):
        """Test ReadResultSet initialization."""
        items = ["item1", "item2", "item3"]
        result_set = ReadResultSet(
            items=items,
            read_count=3,
            next_sequence_to_read=10,
            sequences=[7, 8, 9],
        )
        self.assertEqual(result_set.items, items)
        self.assertEqual(result_set.read_count, 3)
        self.assertEqual(result_set.next_sequence_to_read, 10)

    def test_result_set_defaults(self):
        """Test ReadResultSet with default values."""
        result_set = ReadResultSet(items=[], read_count=0, next_sequence_to_read=0)
        self.assertEqual(result_set.items, [])
        self.assertEqual(result_set.read_count, 0)
        self.assertEqual(result_set.next_sequence_to_read, 0)

    def test_get_sequence_with_sequences(self):
        """Test get_sequence with explicit sequences."""
        result_set = ReadResultSet(
            items=["a", "b", "c"],
            read_count=3,
            next_sequence_to_read=10,
            sequences=[7, 8, 9],
        )
        self.assertEqual(result_set.get_sequence(0), 7)
        self.assertEqual(result_set.get_sequence(1), 8)
        self.assertEqual(result_set.get_sequence(2), 9)

    def test_get_sequence_without_sequences(self):
        """Test get_sequence calculates from next_sequence."""
        result_set = ReadResultSet(
            items=["a", "b", "c"],
            read_count=3,
            next_sequence_to_read=10,
        )
        self.assertEqual(result_set.get_sequence(0), 7)
        self.assertEqual(result_set.get_sequence(1), 8)
        self.assertEqual(result_set.get_sequence(2), 9)

    def test_get_sequence_out_of_range(self):
        """Test get_sequence raises IndexError for out of range."""
        result_set = ReadResultSet(items=["a"], read_count=1, next_sequence_to_read=1)
        with self.assertRaises(IndexError):
            result_set.get_sequence(5)

    def test_len_returns_item_count(self):
        """Test __len__ returns number of items."""
        result_set = ReadResultSet(items=["a", "b"], read_count=2, next_sequence_to_read=2)
        self.assertEqual(len(result_set), 2)

    def test_getitem_returns_item(self):
        """Test __getitem__ returns item at index."""
        result_set = ReadResultSet(items=["a", "b"], read_count=2, next_sequence_to_read=2)
        self.assertEqual(result_set[0], "a")
        self.assertEqual(result_set[1], "b")

    def test_iter_returns_items(self):
        """Test __iter__ returns iterator over items."""
        result_set = ReadResultSet(items=["a", "b"], read_count=2, next_sequence_to_read=2)
        self.assertEqual(list(result_set), ["a", "b"])

    def test_repr(self):
        """Test ReadResultSet __repr__."""
        result_set = ReadResultSet(items=["a"], read_count=1, next_sequence_to_read=1)
        repr_str = repr(result_set)
        self.assertIn("ReadResultSet", repr_str)
        self.assertIn("read_count=1", repr_str)


class TestRingbufferProxy(unittest.TestCase):
    """Tests for RingbufferProxy class."""

    def setUp(self):
        """Set up test fixtures."""
        self.proxy = RingbufferProxy(
            name="test-ringbuffer",
            context=None,
        )

    def test_service_name_constant(self):
        """Test SERVICE_NAME is defined correctly."""
        self.assertEqual(RingbufferProxy.SERVICE_NAME, "hz:impl:ringbufferService")

    def test_sequence_unavailable_constant(self):
        """Test SEQUENCE_UNAVAILABLE is defined correctly."""
        self.assertEqual(RingbufferProxy.SEQUENCE_UNAVAILABLE, -1)

    def test_default_capacity_constant(self):
        """Test DEFAULT_CAPACITY is defined correctly."""
        self.assertEqual(RingbufferProxy.DEFAULT_CAPACITY, 10000)

    def test_initialization_default_capacity(self):
        """Test RingbufferProxy initialization with default capacity."""
        self.assertEqual(self.proxy._ring_capacity, RingbufferProxy.DEFAULT_CAPACITY)
        self.assertEqual(self.proxy._head_seq, 0)
        self.assertEqual(self.proxy._tail_seq, -1)

    def test_initialization_custom_capacity(self):
        """Test RingbufferProxy initialization with custom capacity."""
        proxy = RingbufferProxy(name="test", context=None, capacity=100)
        self.assertEqual(proxy._ring_capacity, 100)

    def test_initialization_minimum_capacity(self):
        """Test RingbufferProxy ensures minimum capacity of 1."""
        proxy = RingbufferProxy(name="test", context=None, capacity=0)
        self.assertEqual(proxy._ring_capacity, 1)

    def test_capacity_returns_int(self):
        """Test capacity returns an integer."""
        result = self.proxy.capacity()
        self.assertEqual(result, RingbufferProxy.DEFAULT_CAPACITY)

    def test_capacity_async_returns_future(self):
        """Test capacity_async returns a Future."""
        future = self.proxy.capacity_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), RingbufferProxy.DEFAULT_CAPACITY)

    def test_size_empty_returns_zero(self):
        """Test size returns 0 for empty ringbuffer."""
        result = self.proxy.size()
        self.assertEqual(result, 0)

    def test_size_async_returns_future(self):
        """Test size_async returns a Future."""
        future = self.proxy.size_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 0)

    def test_tail_sequence_empty_returns_negative_one(self):
        """Test tail_sequence returns -1 for empty ringbuffer."""
        result = self.proxy.tail_sequence()
        self.assertEqual(result, -1)

    def test_tail_sequence_async_returns_future(self):
        """Test tail_sequence_async returns a Future."""
        future = self.proxy.tail_sequence_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), -1)

    def test_head_sequence_empty_returns_zero(self):
        """Test head_sequence returns 0 for empty ringbuffer."""
        result = self.proxy.head_sequence()
        self.assertEqual(result, 0)

    def test_head_sequence_async_returns_future(self):
        """Test head_sequence_async returns a Future."""
        future = self.proxy.head_sequence_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 0)

    def test_remaining_capacity_empty(self):
        """Test remaining_capacity for empty ringbuffer."""
        result = self.proxy.remaining_capacity()
        self.assertEqual(result, RingbufferProxy.DEFAULT_CAPACITY)

    def test_remaining_capacity_async_returns_future(self):
        """Test remaining_capacity_async returns a Future."""
        future = self.proxy.remaining_capacity_async()
        self.assertIsInstance(future, Future)

    def test_add_returns_sequence(self):
        """Test add returns the sequence number."""
        result = self.proxy.add("item")
        self.assertEqual(result, 0)

    def test_add_async_returns_future(self):
        """Test add_async returns a Future."""
        future = self.proxy.add_async("item")
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), 0)

    def test_add_increments_sequence(self):
        """Test add increments sequence for each item."""
        seq1 = self.proxy.add("item1")
        seq2 = self.proxy.add("item2")
        seq3 = self.proxy.add("item3")
        self.assertEqual(seq1, 0)
        self.assertEqual(seq2, 1)
        self.assertEqual(seq3, 2)

    def test_add_overwrite_policy(self):
        """Test add with OVERWRITE policy when buffer is full."""
        proxy = RingbufferProxy(name="test", context=None, capacity=3)
        proxy.add("item1")
        proxy.add("item2")
        proxy.add("item3")
        seq = proxy.add("item4", OverflowPolicy.OVERWRITE)
        self.assertEqual(seq, 3)
        self.assertEqual(proxy._head_seq, 1)

    def test_add_fail_policy_when_full(self):
        """Test add with FAIL policy returns -1 when buffer is full."""
        proxy = RingbufferProxy(name="test", context=None, capacity=3)
        proxy.add("item1")
        proxy.add("item2")
        proxy.add("item3")
        seq = proxy.add("item4", OverflowPolicy.FAIL)
        self.assertEqual(seq, -1)

    def test_add_all_returns_last_sequence(self):
        """Test add_all returns the last sequence number."""
        result = self.proxy.add_all(["item1", "item2", "item3"])
        self.assertEqual(result, 2)

    def test_add_all_async_returns_future(self):
        """Test add_all_async returns a Future."""
        future = self.proxy.add_all_async(["item1", "item2"])
        self.assertIsInstance(future, Future)

    def test_add_all_empty_list(self):
        """Test add_all with empty list returns current tail."""
        result = self.proxy.add_all([])
        self.assertEqual(result, -1)

    def test_add_all_fail_policy_when_full(self):
        """Test add_all with FAIL policy returns -1 when would overflow."""
        proxy = RingbufferProxy(name="test", context=None, capacity=2)
        seq = proxy.add_all(["item1", "item2", "item3"], OverflowPolicy.FAIL)
        self.assertEqual(seq, -1)

    def test_read_one_returns_item(self):
        """Test read_one returns item at sequence."""
        self.proxy.add("item1")
        result = self.proxy.read_one(0)
        self.assertEqual(result, "item1")

    def test_read_one_async_returns_future(self):
        """Test read_one_async returns a Future."""
        self.proxy.add("item1")
        future = self.proxy.read_one_async(0)
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), "item1")

    def test_read_one_stale_sequence_raises(self):
        """Test read_one raises for stale sequence."""
        proxy = RingbufferProxy(name="test", context=None, capacity=2)
        proxy.add("item1")
        proxy.add("item2")
        proxy.add("item3")
        from hazelcast.proxy.ringbuffer import StaleSequenceException
        with self.assertRaises(StaleSequenceException):
            proxy.read_one(0)

    def test_read_one_beyond_tail_raises(self):
        """Test read_one raises for sequence beyond tail."""
        self.proxy.add("item1")
        from hazelcast.proxy.ringbuffer import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.proxy.read_one(10)

    def test_read_one_empty_buffer_raises(self):
        """Test read_one raises for empty buffer."""
        from hazelcast.proxy.ringbuffer import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.proxy.read_one(0)

    def test_read_many_returns_result_set(self):
        """Test read_many returns ReadResultSet."""
        self.proxy.add_all(["item1", "item2", "item3"])
        result = self.proxy.read_many(0, 1, 3)
        self.assertIsInstance(result, ReadResultSet)
        self.assertEqual(result.read_count, 3)

    def test_read_many_async_returns_future(self):
        """Test read_many_async returns a Future."""
        self.proxy.add("item1")
        future = self.proxy.read_many_async(0, 1, 10)
        self.assertIsInstance(future, Future)

    def test_read_many_with_filter(self):
        """Test read_many with filter predicate."""
        self.proxy.add_all([1, 2, 3, 4, 5])
        result = self.proxy.read_many(0, 1, 10, filter_predicate=lambda x: x > 2)
        self.assertEqual(result.items, [3, 4, 5])

    def test_read_many_negative_min_count_raises(self):
        """Test read_many raises for negative min_count."""
        from hazelcast.proxy.ringbuffer import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.proxy.read_many(0, -1, 10)

    def test_read_many_max_less_than_min_raises(self):
        """Test read_many raises when max_count < min_count."""
        from hazelcast.proxy.ringbuffer import IllegalArgumentException
        with self.assertRaises(IllegalArgumentException):
            self.proxy.read_many(0, 5, 2)

    def test_read_many_stale_sequence_raises(self):
        """Test read_many raises for stale start sequence."""
        proxy = RingbufferProxy(name="test", context=None, capacity=2)
        proxy.add_all(["item1", "item2", "item3"])
        from hazelcast.proxy.ringbuffer import StaleSequenceException
        with self.assertRaises(StaleSequenceException):
            proxy.read_many(0, 1, 10)

    def test_len_returns_size(self):
        """Test __len__ returns size."""
        self.assertEqual(len(self.proxy), 0)
        self.proxy.add("item1")
        self.assertEqual(len(self.proxy), 1)


class TestRingbufferProxyWithItems(unittest.TestCase):
    """Tests for RingbufferProxy with items added."""

    def setUp(self):
        """Set up test fixtures with items."""
        self.proxy = RingbufferProxy(
            name="test-ringbuffer",
            context=None,
            capacity=5,
        )
        self.proxy.add_all(["item1", "item2", "item3"])

    def test_size_after_adds(self):
        """Test size returns correct count after adds."""
        self.assertEqual(self.proxy.size(), 3)

    def test_tail_sequence_after_adds(self):
        """Test tail_sequence returns last sequence."""
        self.assertEqual(self.proxy.tail_sequence(), 2)

    def test_head_sequence_before_overflow(self):
        """Test head_sequence before overflow."""
        self.assertEqual(self.proxy.head_sequence(), 0)

    def test_remaining_capacity_after_adds(self):
        """Test remaining_capacity after adds."""
        self.assertEqual(self.proxy.remaining_capacity(), 2)

    def test_read_one_multiple_items(self):
        """Test read_one for multiple items."""
        self.assertEqual(self.proxy.read_one(0), "item1")
        self.assertEqual(self.proxy.read_one(1), "item2")
        self.assertEqual(self.proxy.read_one(2), "item3")


if __name__ == "__main__":
    unittest.main()
