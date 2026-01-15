"""Unit tests for hazelcast.proxy.ringbuffer module."""

import pytest
from concurrent.futures import Future

from hazelcast.proxy.ringbuffer import (
    RingbufferProxy,
    ReadResultSet,
    OverflowPolicy,
)
from hazelcast.exceptions import IllegalStateException


class TestOverflowPolicy:
    """Tests for OverflowPolicy enum."""

    def test_values(self):
        assert OverflowPolicy.OVERWRITE.value == 0
        assert OverflowPolicy.FAIL.value == 1


class TestRingbufferProxy:
    """Tests for RingbufferProxy class."""

    def test_init(self):
        rb = RingbufferProxy("test-rb")
        assert rb.name == "test-rb"
        assert rb.service_name == "hz:impl:ringbufferService"

    def test_init_with_capacity(self):
        rb = RingbufferProxy("test-rb", capacity=100)
        assert rb.capacity() == 100

    def test_capacity(self):
        rb = RingbufferProxy("test-rb", capacity=500)
        assert rb.capacity() == 500

    def test_size_empty(self):
        rb = RingbufferProxy("test-rb")
        assert rb.size() == 0

    def test_tail_sequence_empty(self):
        rb = RingbufferProxy("test-rb")
        assert rb.tail_sequence() == -1

    def test_head_sequence_empty(self):
        rb = RingbufferProxy("test-rb")
        assert rb.head_sequence() == 0

    def test_remaining_capacity(self):
        rb = RingbufferProxy("test-rb", capacity=100)
        assert rb.remaining_capacity() == 100

    def test_add_returns_sequence(self):
        rb = RingbufferProxy("test-rb", capacity=10)
        seq = rb.add("item1")
        assert seq == 0

    def test_add_increments_sequence(self):
        rb = RingbufferProxy("test-rb", capacity=10)
        seq1 = rb.add("item1")
        seq2 = rb.add("item2")
        assert seq2 == seq1 + 1

    def test_add_async(self):
        rb = RingbufferProxy("test-rb", capacity=10)
        future = rb.add_async("item")
        assert isinstance(future, Future)
        assert future.result() == 0

    def test_add_with_overwrite_policy(self):
        rb = RingbufferProxy("test-rb", capacity=2)
        rb.add("item1")
        rb.add("item2")
        seq = rb.add("item3", OverflowPolicy.OVERWRITE)
        assert seq == 2
        assert rb.head_sequence() == 1

    def test_add_with_fail_policy_when_full(self):
        rb = RingbufferProxy("test-rb", capacity=2)
        rb.add("item1")
        rb.add("item2")
        seq = rb.add("item3", OverflowPolicy.FAIL)
        assert seq == RingbufferProxy.SEQUENCE_UNAVAILABLE

    def test_add_all(self):
        rb = RingbufferProxy("test-rb", capacity=10)
        seq = rb.add_all(["a", "b", "c"])
        assert seq == 2
        assert rb.size() == 3

    def test_add_all_empty(self):
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.add("existing")
        seq = rb.add_all([])
        assert seq == 0

    def test_add_all_with_fail_policy(self):
        rb = RingbufferProxy("test-rb", capacity=2)
        rb.add("item1")
        seq = rb.add_all(["a", "b"], OverflowPolicy.FAIL)
        assert seq == RingbufferProxy.SEQUENCE_UNAVAILABLE

    def test_read_one(self):
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.add("item1")
        rb.add("item2")
        result = rb.read_one(0)
        assert result == "item1"
        result = rb.read_one(1)
        assert result == "item2"

    def test_read_one_stale_sequence(self):
        rb = RingbufferProxy("test-rb", capacity=2)
        rb.add("a")
        rb.add("b")
        rb.add("c")
        with pytest.raises(Exception) as exc_info:
            rb.read_one(0)
        assert "stale" in str(exc_info.value).lower()

    def test_read_one_beyond_tail(self):
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.add("item")
        with pytest.raises(Exception) as exc_info:
            rb.read_one(100)
        assert "beyond" in str(exc_info.value).lower()

    def test_read_one_empty_buffer(self):
        rb = RingbufferProxy("test-rb", capacity=10)
        with pytest.raises(Exception):
            rb.read_one(0)

    def test_read_many(self):
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.add_all(["a", "b", "c", "d", "e"])
        result = rb.read_many(0, 1, 3)
        assert len(result) == 3
        assert result[0] == "a"
        assert result[1] == "b"
        assert result[2] == "c"

    def test_read_many_returns_result_set(self):
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.add_all(["a", "b"])
        result = rb.read_many(0, 1, 10)
        assert isinstance(result, ReadResultSet)
        assert result.read_count == 2

    def test_read_many_invalid_min_count(self):
        rb = RingbufferProxy("test-rb", capacity=10)
        with pytest.raises(Exception):
            rb.read_many(0, -1, 10)

    def test_read_many_invalid_max_count(self):
        rb = RingbufferProxy("test-rb", capacity=10)
        with pytest.raises(Exception):
            rb.read_many(0, 10, 5)

    def test_read_many_stale_sequence(self):
        rb = RingbufferProxy("test-rb", capacity=2)
        rb.add_all(["a", "b", "c"])
        with pytest.raises(Exception):
            rb.read_many(0, 1, 10)

    def test_len(self):
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.add_all(["a", "b", "c"])
        assert len(rb) == 3

    def test_destroyed_operations_raise(self):
        rb = RingbufferProxy("test-rb", capacity=10)
        rb.destroy()
        with pytest.raises(IllegalStateException):
            rb.add("item")


class TestReadResultSet:
    """Tests for ReadResultSet class."""

    def test_init(self):
        rs = ReadResultSet(["a", "b", "c"], read_count=3, next_sequence_to_read=3)
        assert rs.items == ["a", "b", "c"]
        assert rs.read_count == 3
        assert rs.next_sequence_to_read == 3

    def test_len(self):
        rs = ReadResultSet(["a", "b"], read_count=2, next_sequence_to_read=2)
        assert len(rs) == 2

    def test_getitem(self):
        rs = ReadResultSet(["a", "b", "c"], read_count=3, next_sequence_to_read=3)
        assert rs[0] == "a"
        assert rs[1] == "b"
        assert rs[2] == "c"

    def test_iter(self):
        rs = ReadResultSet(["a", "b"], read_count=2, next_sequence_to_read=2)
        items = list(rs)
        assert items == ["a", "b"]

    def test_get_sequence(self):
        rs = ReadResultSet(
            ["a", "b", "c"],
            read_count=3,
            next_sequence_to_read=103,
            sequences=[100, 101, 102],
        )
        assert rs.get_sequence(0) == 100
        assert rs.get_sequence(1) == 101
        assert rs.get_sequence(2) == 102

    def test_get_sequence_without_sequences(self):
        rs = ReadResultSet(["a", "b"], read_count=2, next_sequence_to_read=2)
        assert rs.get_sequence(0) == 0
        assert rs.get_sequence(1) == 1

    def test_get_sequence_out_of_range(self):
        rs = ReadResultSet(["a"], read_count=1, next_sequence_to_read=1)
        with pytest.raises(IndexError):
            rs.get_sequence(10)

    def test_repr(self):
        rs = ReadResultSet(["a", "b"], read_count=2, next_sequence_to_read=2)
        r = repr(rs)
        assert "ReadResultSet" in r
        assert "read_count=2" in r
