"""Unit tests for hazelcast.proxy.pn_counter module."""

import pytest
from concurrent.futures import Future

from hazelcast.proxy.pn_counter import PNCounterProxy
from hazelcast.exceptions import IllegalStateException


class TestPNCounterProxy:
    """Tests for PNCounterProxy class."""

    def test_init(self):
        counter = PNCounterProxy("test-counter")
        assert counter.name == "test-counter"
        assert counter.service_name == "hz:impl:PNCounterService"

    def test_get_initial_value(self):
        counter = PNCounterProxy("test-counter")
        assert counter.get() == 0

    def test_get_async(self):
        counter = PNCounterProxy("test-counter")
        future = counter.get_async()
        assert isinstance(future, Future)
        assert future.result() == 0

    def test_get_and_add(self):
        counter = PNCounterProxy("test-counter")
        prev = counter.get_and_add(5)
        assert prev == 0
        assert counter.get() == 5

    def test_get_and_add_negative(self):
        counter = PNCounterProxy("test-counter")
        counter.get_and_add(10)
        prev = counter.get_and_add(-3)
        assert prev == 10
        assert counter.get() == 7

    def test_get_and_add_async(self):
        counter = PNCounterProxy("test-counter")
        future = counter.get_and_add_async(5)
        assert isinstance(future, Future)
        assert future.result() == 0

    def test_add_and_get(self):
        counter = PNCounterProxy("test-counter")
        result = counter.add_and_get(5)
        assert result == 5
        assert counter.get() == 5

    def test_add_and_get_async(self):
        counter = PNCounterProxy("test-counter")
        future = counter.add_and_get_async(10)
        assert isinstance(future, Future)
        assert future.result() == 10

    def test_get_and_subtract(self):
        counter = PNCounterProxy("test-counter")
        counter.add_and_get(10)
        prev = counter.get_and_subtract(3)
        assert prev == 10
        assert counter.get() == 7

    def test_get_and_subtract_async(self):
        counter = PNCounterProxy("test-counter")
        counter.add_and_get(10)
        future = counter.get_and_subtract_async(3)
        assert isinstance(future, Future)
        assert future.result() == 10

    def test_subtract_and_get(self):
        counter = PNCounterProxy("test-counter")
        counter.add_and_get(10)
        result = counter.subtract_and_get(3)
        assert result == 7

    def test_subtract_and_get_async(self):
        counter = PNCounterProxy("test-counter")
        counter.add_and_get(10)
        future = counter.subtract_and_get_async(3)
        assert isinstance(future, Future)
        assert future.result() == 7

    def test_get_and_increment(self):
        counter = PNCounterProxy("test-counter")
        prev = counter.get_and_increment()
        assert prev == 0
        assert counter.get() == 1

    def test_get_and_increment_async(self):
        counter = PNCounterProxy("test-counter")
        future = counter.get_and_increment_async()
        assert isinstance(future, Future)
        assert future.result() == 0

    def test_increment_and_get(self):
        counter = PNCounterProxy("test-counter")
        result = counter.increment_and_get()
        assert result == 1

    def test_increment_and_get_async(self):
        counter = PNCounterProxy("test-counter")
        future = counter.increment_and_get_async()
        assert isinstance(future, Future)
        assert future.result() == 1

    def test_get_and_decrement(self):
        counter = PNCounterProxy("test-counter")
        counter.add_and_get(5)
        prev = counter.get_and_decrement()
        assert prev == 5
        assert counter.get() == 4

    def test_get_and_decrement_async(self):
        counter = PNCounterProxy("test-counter")
        counter.add_and_get(5)
        future = counter.get_and_decrement_async()
        assert isinstance(future, Future)
        assert future.result() == 5

    def test_decrement_and_get(self):
        counter = PNCounterProxy("test-counter")
        counter.add_and_get(5)
        result = counter.decrement_and_get()
        assert result == 4

    def test_decrement_and_get_async(self):
        counter = PNCounterProxy("test-counter")
        counter.add_and_get(5)
        future = counter.decrement_and_get_async()
        assert isinstance(future, Future)
        assert future.result() == 4

    def test_reset(self):
        counter = PNCounterProxy("test-counter")
        counter.add_and_get(100)
        counter.reset()
        assert counter.get() == 0
        assert counter._observed_clock == {}
        assert counter._current_target_replica is None

    def test_multiple_operations(self):
        counter = PNCounterProxy("test-counter")
        counter.increment_and_get()
        counter.increment_and_get()
        counter.add_and_get(10)
        counter.decrement_and_get()
        counter.subtract_and_get(5)
        assert counter.get() == 7

    def test_destroyed_operations_raise(self):
        counter = PNCounterProxy("test-counter")
        counter.destroy()
        with pytest.raises(IllegalStateException):
            counter.get()
