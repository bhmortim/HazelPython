"""Unit tests for PNCounter proxy."""

import pytest
import threading
from concurrent.futures import Future

from hazelcast.proxy.pn_counter import (
    PNCounterProxy,
    PNCounter,
    VectorClock,
    SERVICE_NAME_PN_COUNTER,
)


class TestVectorClock:
    """Test VectorClock functionality."""

    def test_initialization_empty(self):
        """Test empty vector clock initialization."""
        clock = VectorClock()

        assert clock.timestamps == {}

    def test_initialization_with_timestamps(self):
        """Test vector clock initialization with timestamps."""
        timestamps = {"replica-1": 5, "replica-2": 3}
        clock = VectorClock(timestamps)

        assert clock.get_replica_timestamp("replica-1") == 5
        assert clock.get_replica_timestamp("replica-2") == 3

    def test_get_missing_replica(self):
        """Test getting timestamp for missing replica."""
        clock = VectorClock()

        assert clock.get_replica_timestamp("missing") == 0

    def test_set_replica_timestamp(self):
        """Test setting replica timestamp."""
        clock = VectorClock()

        clock.set_replica_timestamp("replica-1", 10)
        assert clock.get_replica_timestamp("replica-1") == 10

        clock.set_replica_timestamp("replica-1", 15)
        assert clock.get_replica_timestamp("replica-1") == 15

    def test_is_after_empty(self):
        """Test is_after with empty clocks."""
        clock1 = VectorClock()
        clock2 = VectorClock()

        assert not clock1.is_after(clock2)

    def test_is_after_strictly_greater(self):
        """Test is_after when one clock is strictly after."""
        clock1 = VectorClock({"replica-1": 5, "replica-2": 3})
        clock2 = VectorClock({"replica-1": 3, "replica-2": 2})

        assert clock1.is_after(clock2)
        assert not clock2.is_after(clock1)

    def test_is_after_concurrent(self):
        """Test is_after for concurrent clocks."""
        clock1 = VectorClock({"replica-1": 5, "replica-2": 2})
        clock2 = VectorClock({"replica-1": 3, "replica-2": 4})

        assert not clock1.is_after(clock2)
        assert not clock2.is_after(clock1)

    def test_merge(self):
        """Test merging two vector clocks."""
        clock1 = VectorClock({"replica-1": 5, "replica-2": 3})
        clock2 = VectorClock({"replica-1": 3, "replica-2": 7, "replica-3": 2})

        clock1.merge(clock2)

        assert clock1.get_replica_timestamp("replica-1") == 5
        assert clock1.get_replica_timestamp("replica-2") == 7
        assert clock1.get_replica_timestamp("replica-3") == 2

    def test_entry_set(self):
        """Test getting entries as list."""
        clock = VectorClock({"a": 1, "b": 2})
        entries = clock.entry_set()

        assert len(entries) == 2
        assert ("a", 1) in entries
        assert ("b", 2) in entries

    def test_clear(self):
        """Test clearing the clock."""
        clock = VectorClock({"replica-1": 5})
        clock.clear()

        assert clock.timestamps == {}

    def test_repr(self):
        """Test string representation."""
        clock = VectorClock({"r1": 1})
        repr_str = repr(clock)

        assert "VectorClock" in repr_str
        assert "r1" in repr_str


class TestPNCounterProxy:
    """Test PNCounterProxy functionality."""

    def test_initialization(self):
        """Test proxy initialization."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")

        assert counter.name == "test-counter"
        assert counter.service_name == SERVICE_NAME_PN_COUNTER
        assert counter.get() == 0

    def test_increment_and_get(self):
        """Test increment_and_get operation."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")

        result = counter.increment_and_get()
        assert result == 1

        result = counter.increment_and_get()
        assert result == 2

    def test_decrement_and_get(self):
        """Test decrement_and_get operation."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")
        counter.add_and_get(5)

        result = counter.decrement_and_get()
        assert result == 4

        result = counter.decrement_and_get()
        assert result == 3

    def test_add_and_get(self):
        """Test add_and_get operation."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")

        result = counter.add_and_get(10)
        assert result == 10

        result = counter.add_and_get(5)
        assert result == 15

    def test_add_and_get_negative(self):
        """Test add_and_get with negative delta."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")
        counter.add_and_get(10)

        result = counter.add_and_get(-3)
        assert result == 7

    def test_subtract_and_get(self):
        """Test subtract_and_get operation."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")
        counter.add_and_get(10)

        result = counter.subtract_and_get(3)
        assert result == 7

    def test_get_and_add(self):
        """Test get_and_add operation."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")
        counter.add_and_get(5)

        prev = counter.get_and_add(10)
        assert prev == 5
        assert counter.get() == 15

    def test_get_and_subtract(self):
        """Test get_and_subtract operation."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")
        counter.add_and_get(10)

        prev = counter.get_and_subtract(3)
        assert prev == 10
        assert counter.get() == 7

    def test_get_and_increment(self):
        """Test get_and_increment operation."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")
        counter.add_and_get(5)

        prev = counter.get_and_increment()
        assert prev == 5
        assert counter.get() == 6

    def test_get_and_decrement(self):
        """Test get_and_decrement operation."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")
        counter.add_and_get(5)

        prev = counter.get_and_decrement()
        assert prev == 5
        assert counter.get() == 4

    def test_reset(self):
        """Test reset operation."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")
        counter.add_and_get(100)
        counter.observed_clock.set_replica_timestamp("r1", 5)

        counter.reset()

        assert counter.get() == 0
        assert counter.observed_clock.timestamps == {}
        assert counter._current_target_replica is None

    def test_get_replica_count(self):
        """Test get_replica_count operation."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")

        count = counter.get_replica_count()
        assert count >= 1

    def test_async_operations(self):
        """Test async operations return futures."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")

        future = counter.get_async()
        assert isinstance(future, Future)
        assert future.result() == 0

        future = counter.increment_and_get_async()
        assert isinstance(future, Future)
        assert future.result() == 1

        future = counter.add_and_get_async(5)
        assert isinstance(future, Future)
        assert future.result() == 6

    def test_destroyed_proxy(self):
        """Test operations on destroyed proxy."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")
        counter.destroy()

        with pytest.raises(Exception):
            counter.get()

    def test_concurrent_increments(self):
        """Test concurrent increment operations."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")
        errors = []

        def increment_many():
            try:
                for _ in range(100):
                    counter.increment_and_get()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=increment_many) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert counter.get() == 1000

    def test_update_observed_clock(self):
        """Test _update_observed_clock method."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")

        new_clock = VectorClock({"replica-1": 5, "replica-2": 3})
        counter._update_observed_clock(new_clock)

        assert counter.observed_clock.get_replica_timestamp("replica-1") == 5
        assert counter.observed_clock.get_replica_timestamp("replica-2") == 3


class TestPNCounterAlias:
    """Test PNCounter alias."""

    def test_alias_is_proxy_class(self):
        """Test that PNCounter is an alias for the proxy."""
        assert PNCounter is PNCounterProxy

    def test_alias_instantiation(self):
        """Test instantiation through alias."""
        counter = PNCounter(SERVICE_NAME_PN_COUNTER, "alias-test")
        assert isinstance(counter, PNCounterProxy)


class TestPNCounterEdgeCases:
    """Test edge cases for PNCounter."""

    def test_negative_values(self):
        """Test counter going negative."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")

        counter.decrement_and_get()
        counter.decrement_and_get()

        assert counter.get() == -2

    def test_large_values(self):
        """Test counter with large values."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")

        large_value = 10 ** 15
        result = counter.add_and_get(large_value)

        assert result == large_value

    def test_many_operations(self):
        """Test many sequential operations."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")

        for i in range(1000):
            counter.increment_and_get()

        for i in range(500):
            counter.decrement_and_get()

        assert counter.get() == 500

    def test_alternating_operations(self):
        """Test alternating increment/decrement."""
        counter = PNCounterProxy(SERVICE_NAME_PN_COUNTER, "test-counter")

        for i in range(100):
            counter.increment_and_get()
            counter.decrement_and_get()

        assert counter.get() == 0
