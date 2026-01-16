"""Tests for Ringbuffer distributed data structure proxy."""

import pytest
from concurrent.futures import Future

from hazelcast.proxy.ringbuffer import (
    RingbufferProxy,
    OverflowPolicy,
    ReadResultSet,
    IllegalArgumentException,
    StaleSequenceException,
)


class TestRingbufferProxy:
    """Tests for RingbufferProxy."""

    def test_create_ringbuffer(self):
        """Test creating a ringbuffer."""
        rb = RingbufferProxy("test-rb")
        assert rb.name == "test-rb"
        assert rb.service_name == "hz:impl:ringbufferService"

    def test_capacity(self):
        """Test capacity retrieval."""
        rb = RingbufferProxy("test-rb", capacity=100)
        assert rb.capacity() == 100

    def test_capacity_async(self):
        """Test async capacity retrieval."""
        rb = RingbufferProxy("test-rb", capacity=50)
        future = rb.capacity_async()
        assert isinstance(future, Future)
        assert future.result() == 50

    def test_default_capacity(self):
        """Test default capacity."""
        rb = RingbufferProxy("test-rb")
        assert rb.capacity() == RingbufferProxy.DEFAULT_CAPACITY

    def test_minimum_capacity(self):
        """Test minimum capacity is enforced."""
        rb = RingbufferProxy("test-rb", capacity=0)
        assert rb.capacity() >= 1

    def test_initial_size_is_zero(self):
        """Test initial size is zero."""
        rb = RingbufferProxy("test-rb")
        assert rb.size() == 0

    def test_size_async(self):
        """Test async size retrieval."""
        rb = RingbufferProxy("test-rb")
        future = rb.size_async()
        assert isinstance(future, Future)
        assert future.result() == 0

    def test_initial_tail_sequence(self):
        """Test initial tail sequence is -1."""
        rb = RingbufferProxy("test-rb")
        assert rb.tail_sequence() == -1

    def test_tail_sequence_async(self):
        """Test async tail sequence retrieval."""
        rb = RingbufferProxy("test-rb")
        future = rb.tail_sequence_async()
        assert isinstance(future, Future)
        assert future.result() == -1

    def test_initial_head_sequence(self):
        """Test initial head sequence is 0."""
        rb = RingbufferProxy("test-rb")
        assert rb.head_sequence() == 0

    def test_head_sequence_async(self):
        """Test async head sequence retrieval."""
        rb = RingbufferProxy("test-rb")
        future = rb.head_sequence_async()
        assert isinstance(future, Future)
        assert future.result() == 0

    def test_initial_remaining_capacity(self):
        """Test initial remaining capacity equals capacity."""
        rb = RingbufferProxy("test-rb", capacity=100)
        assert rb.remaining_capacity() == 100

    def test_remaining_capacity_async(self):
        """Test async remaining capacity retrieval."""
        rb = RingbufferProxy("test-rb", capacity=50)
        future = rb.remaining_capacity_async()
        assert isinstance(future, Future)
        assert future.result() == 50

    def test_add_single_item(self):
        """Test adding a single item."""
        rb = RingbufferProxy("test-rb", capacity=10)
        seq = rb.add("item1")
        assert seq == 0
        assert rb.size() == 1
        assert rb.tail_sequence() == 0

    def test_add_async(self):
        """Test async add."""
        rb = RingbufferProxy("test-rb", capacity=10)
        future = rb.add_async("item1")
        assert isinstance(future, Future)
        assert future.result() == 0

    def test_add_multiple_items(self):
        """Test adding multiple items sequentially."""
        rb = RingbufferProxy("test-rb", capacity=10)
        seq1 = rb.add("item1")
        seq2 = rb.add("item2")
        seq3 = rb.add("item3")
        assert seq1 == 0
        assert seq2 == 1
        assert seq3 == 2
        assert rb.size() == 3

    def test_add_with_overwrite_policy(self):
        """Test add with overwrite overflow policy."""
        rb = RingbufferProxy("test-rb", capacity=3)
        rb.add("item1", OverflowPolicy.OVERWRITE)
        rb.add("item2", OverflowPolicy.OVERWRITE)
        rb.add("item3", OverflowPolicy.OVERWRITE)
        seq = rb.add("item4", OverflowPolicy.OVERWRITE)
        assert seq == 3
        assert rb.size() == 3
        assert rb.head_sequence() == 1

    def test_add_with_fail_policy_when_full(self):
        """Test add with fail overflow policy when buffer is full."""
        rb = RingbufferProxy("test-rb", capacity=2)
        rb.add("item1")
        rb.add("item2")
        seq = rb.add("item3", OverflowPolicy.FAIL)
        assert seq == RingbufferProxy.SEQUENCE_UNAVAILABLE

    def test_add_with_fail_policy_when_not_full(self):
        """Test add with fail overflow policy when buffer has space."""
        rb = RingbufferProxy("test-rb", capacity=3)
        seq = rb.add("item1", OverflowPolicy.FAIL)
        assert seq == 0

    def test_add_all(self):
        """Test adding multiple items at once."""
        rb = RingbufferProxy("test-rb", capacity=10)
        seq = rb.add_all(["item1", "item2", "item3"])
        assert seq == 2
        assert rb.size() == 3

    def test_add_all_async(self):
        """Test async add all."""
        rb = RingbufferProxy("test-rb", capacity=10)
        future = rb.add_all_async(["item1", "item2"])
        assert isinstance(future, Future)
        assert future.result() == 1

    def test_add_all_empty_list(self):
        """Test adding empty list."""
        rb = RingbufferProxy("test-rb", capacity=10)
        seq = rb.add_all([])
        assert seq == -1
        assert rb.size() == 0

    def test_add_all_with_overwrite(self):
        """Test add all with overwrite policy."""
        rb = RingbufferProxy("test-rb", capacity=3)
        rb.add_all(["a", "b", "c"])
        seq = rb.add_all(["d", "e"], OverflowPolicy.OVERWRITE)
        assert seq == 4
        assert rb.size() == 3
        assert rb.head_sequence() == 2

    def test_add_all_with_fail_policy_when_insufficient_space(self):
        """Test add all with fail policy when not enough space."""
        rb = RingbufferProxy("test-rb", capacity=3)
        rb.add("item1")
        rb.add("item2")
        seq = rb.add_all(["a", "b", "c"], OverflowPolicy.FAIL)
        assert seq == RingbufferProxy.SEQUENCE_UNAVAILABLE

    def test_read_one(self):
        """Test reading a single item."""
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.add("item1")
        rb.add("item2")
        assert rb.read_one(0) == "item1"
        assert rb.read_one(1) == "item2"

    def test_read_one_async(self):
        """Test async read one."""
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.add("item1")
        future = rb.read_one_async(0)
        assert isinstance(future, Future)
        assert future.result() == "item1"

    def test_read_one_stale_sequence(self):
        """Test reading a stale sequence raises exception."""
        rb = RingbufferProxy("test-rb", capacity=2)
        rb.add("item1")
        rb.add("item2")
        rb.add("item3")  # This overwrites item1
        with pytest.raises(StaleSequenceException):
            rb.read_one(0)

    def test_read_one_beyond_tail(self):
        """Test reading beyond tail raises exception."""
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.add("item1")
        with pytest.raises(IllegalArgumentException):
            rb.read_one(5)

    def test_read_one_empty_buffer(self):
        """Test reading from empty buffer raises exception."""
        rb = RingbufferProxy("test-rb", capacity=10)
        with pytest.raises(IllegalArgumentException):
            rb.read_one(0)

    def test_read_many(self):
        """Test reading multiple items."""
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.add_all(["a", "b", "c", "d", "e"])
        result = rb.read_many(0, 1, 3)
        assert len(result) == 3
        assert result.items == ["a", "b", "c"]
        assert result.read_count == 3

    def test_read_many_async(self):
        """Test async read many."""
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.add_all(["a", "b", "c"])
        future = rb.read_many_async(0, 1, 2)
        assert isinstance(future, Future)
        result = future.result()
        assert len(result) == 2

    def test_read_many_with_filter(self):
        """Test read many with filter predicate."""
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.add_all([1, 2, 3, 4, 5])
        result = rb.read_many(0, 1, 5, filter_predicate=lambda x: x > 2)
        assert result.items == [3, 4, 5]

    def test_read_many_stale_sequence(self):
        """Test read many with stale sequence raises exception."""
        rb = RingbufferProxy("test-rb", capacity=2)
        rb.add_all(["a", "b", "c", "d"])
        with pytest.raises(StaleSequenceException):
            rb.read_many(0, 1, 2)

    def test_read_many_invalid_min_count(self):
        """Test read many with negative min_count raises exception."""
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.add("item")
        with pytest.raises(IllegalArgumentException):
            rb.read_many(0, -1, 5)

    def test_read_many_max_less_than_min(self):
        """Test read many with max_count < min_count raises exception."""
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.add("item")
        with pytest.raises(IllegalArgumentException):
            rb.read_many(0, 5, 2)

    def test_len(self):
        """Test __len__ method."""
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.add_all(["a", "b", "c"])
        assert len(rb) == 3

    def test_remaining_capacity_after_adds(self):
        """Test remaining capacity decreases after adds."""
        rb = RingbufferProxy("test-rb", capacity=5)
        rb.add_all(["a", "b"])
        assert rb.remaining_capacity() == 3

    def test_destroy(self):
        """Test destroying the ringbuffer."""
        rb = RingbufferProxy("test-rb")
        rb.add("item")
        rb.destroy()
        assert rb.is_destroyed

    def test_operations_after_destroy(self):
        """Test operations fail after destroy."""
        from hazelcast.exceptions import IllegalStateException
        rb = RingbufferProxy("test-rb")
        rb.destroy()
        with pytest.raises(IllegalStateException):
            rb.add("item")


class TestReadResultSet:
    """Tests for ReadResultSet."""

    def test_create_result_set(self):
        """Test creating a result set."""
        rs = ReadResultSet(["a", "b", "c"], 3, 3)
        assert rs.items == ["a", "b", "c"]
        assert rs.read_count == 3
        assert rs.next_sequence_to_read == 3

    def test_len(self):
        """Test __len__ method."""
        rs = ReadResultSet(["a", "b"], 2, 2)
        assert len(rs) == 2

    def test_getitem(self):
        """Test __getitem__ method."""
        rs = ReadResultSet(["a", "b", "c"], 3, 3)
        assert rs[0] == "a"
        assert rs[1] == "b"
        assert rs[2] == "c"

    def test_iter(self):
        """Test __iter__ method."""
        rs = ReadResultSet(["a", "b", "c"], 3, 3)
        assert list(rs) == ["a", "b", "c"]

    def test_get_sequence(self):
        """Test get_sequence method."""
        rs = ReadResultSet(["a", "b"], 2, 5, [3, 4])
        assert rs.get_sequence(0) == 3
        assert rs.get_sequence(1) == 4

    def test_get_sequence_without_sequences(self):
        """Test get_sequence without explicit sequences."""
        rs = ReadResultSet(["a", "b"], 2, 4)
        assert rs.get_sequence(0) == 2
        assert rs.get_sequence(1) == 3

    def test_get_sequence_out_of_range(self):
        """Test get_sequence with out of range index."""
        rs = ReadResultSet(["a"], 1, 1)
        with pytest.raises(IndexError):
            rs.get_sequence(5)

    def test_repr(self):
        """Test __repr__ method."""
        rs = ReadResultSet(["a", "b"], 2, 2)
        repr_str = repr(rs)
        assert "ReadResultSet" in repr_str
        assert "['a', 'b']" in repr_str


class TestOverflowPolicy:
    """Tests for OverflowPolicy enum."""

    def test_overwrite_value(self):
        """Test OVERWRITE enum value."""
        assert OverflowPolicy.OVERWRITE.value == 0

    def test_fail_value(self):
        """Test FAIL enum value."""
        assert OverflowPolicy.FAIL.value == 1
