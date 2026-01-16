"""Unit tests for hazelcast.cp.atomic module."""

import pytest
from concurrent.futures import Future
from unittest.mock import Mock

from hazelcast.cp.atomic import AtomicLong, AtomicReference
from hazelcast.exceptions import IllegalArgumentException, IllegalStateException


class TestAtomicLong:
    """Tests for AtomicLong class."""

    def test_service_name_constant(self):
        assert AtomicLong.SERVICE_NAME == "hz:raft:atomicLongService"

    def test_initial_value(self):
        atomic = AtomicLong("test")
        assert atomic._value == 0

    def test_get(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        assert atomic.get() == 42

    def test_get_async(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        future = atomic.get_async()
        assert isinstance(future, Future)
        assert future.result() == 42

    def test_set(self):
        atomic = AtomicLong("test")
        atomic.set(100)
        assert atomic._value == 100

    def test_set_async(self):
        atomic = AtomicLong("test")
        future = atomic.set_async(100)
        assert isinstance(future, Future)
        assert future.result() is None
        assert atomic._value == 100

    def test_get_and_set(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        previous = atomic.get_and_set(100)
        assert previous == 42
        assert atomic._value == 100

    def test_get_and_set_async(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        future = atomic.get_and_set_async(100)
        assert future.result() == 42
        assert atomic._value == 100

    def test_compare_and_set_success(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        result = atomic.compare_and_set(42, 100)
        assert result is True
        assert atomic._value == 100

    def test_compare_and_set_failure(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        result = atomic.compare_and_set(0, 100)
        assert result is False
        assert atomic._value == 42

    def test_compare_and_set_async_success(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        future = atomic.compare_and_set_async(42, 100)
        assert future.result() is True
        assert atomic._value == 100

    def test_compare_and_set_async_failure(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        future = atomic.compare_and_set_async(0, 100)
        assert future.result() is False
        assert atomic._value == 42

    def test_get_and_add(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        previous = atomic.get_and_add(10)
        assert previous == 42
        assert atomic._value == 52

    def test_get_and_add_async(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        future = atomic.get_and_add_async(10)
        assert future.result() == 42
        assert atomic._value == 52

    def test_get_and_add_negative(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        previous = atomic.get_and_add(-10)
        assert previous == 42
        assert atomic._value == 32

    def test_add_and_get(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        result = atomic.add_and_get(10)
        assert result == 52
        assert atomic._value == 52

    def test_add_and_get_async(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        future = atomic.add_and_get_async(10)
        assert future.result() == 52
        assert atomic._value == 52

    def test_get_and_increment(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        previous = atomic.get_and_increment()
        assert previous == 42
        assert atomic._value == 43

    def test_get_and_increment_async(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        future = atomic.get_and_increment_async()
        assert future.result() == 42
        assert atomic._value == 43

    def test_increment_and_get(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        result = atomic.increment_and_get()
        assert result == 43
        assert atomic._value == 43

    def test_increment_and_get_async(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        future = atomic.increment_and_get_async()
        assert future.result() == 43
        assert atomic._value == 43

    def test_get_and_decrement(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        previous = atomic.get_and_decrement()
        assert previous == 42
        assert atomic._value == 41

    def test_get_and_decrement_async(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        future = atomic.get_and_decrement_async()
        assert future.result() == 42
        assert atomic._value == 41

    def test_decrement_and_get(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        result = atomic.decrement_and_get()
        assert result == 41
        assert atomic._value == 41

    def test_decrement_and_get_async(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        future = atomic.decrement_and_get_async()
        assert future.result() == 41
        assert atomic._value == 41

    def test_alter(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        atomic.alter(lambda x: x * 2)
        assert atomic._value == 84

    def test_alter_async(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        future = atomic.alter_async(lambda x: x * 2)
        assert future.result() is None
        assert atomic._value == 84

    def test_alter_with_none_function(self):
        atomic = AtomicLong("test")
        with pytest.raises(IllegalArgumentException) as exc_info:
            atomic.alter(None)
        assert "function cannot be None" in str(exc_info.value)

    def test_alter_async_with_none_function(self):
        atomic = AtomicLong("test")
        with pytest.raises(IllegalArgumentException) as exc_info:
            atomic.alter_async(None)
        assert "function cannot be None" in str(exc_info.value)

    def test_alter_and_get(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        result = atomic.alter_and_get(lambda x: x * 2)
        assert result == 84
        assert atomic._value == 84

    def test_alter_and_get_async(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        future = atomic.alter_and_get_async(lambda x: x * 2)
        assert future.result() == 84
        assert atomic._value == 84

    def test_alter_and_get_with_none_function(self):
        atomic = AtomicLong("test")
        with pytest.raises(IllegalArgumentException):
            atomic.alter_and_get(None)

    def test_get_and_alter(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        previous = atomic.get_and_alter(lambda x: x * 2)
        assert previous == 42
        assert atomic._value == 84

    def test_get_and_alter_async(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        future = atomic.get_and_alter_async(lambda x: x * 2)
        assert future.result() == 42
        assert atomic._value == 84

    def test_get_and_alter_with_none_function(self):
        atomic = AtomicLong("test")
        with pytest.raises(IllegalArgumentException):
            atomic.get_and_alter(None)

    def test_apply(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        result = atomic.apply(lambda x: x * 2)
        assert result == 84
        assert atomic._value == 42

    def test_apply_async(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        future = atomic.apply_async(lambda x: x * 2)
        assert future.result() == 84
        assert atomic._value == 42

    def test_apply_with_none_function(self):
        atomic = AtomicLong("test")
        with pytest.raises(IllegalArgumentException) as exc_info:
            atomic.apply(None)
        assert "function cannot be None" in str(exc_info.value)

    def test_apply_async_with_none_function(self):
        atomic = AtomicLong("test")
        with pytest.raises(IllegalArgumentException):
            atomic.apply_async(None)

    def test_apply_returns_different_type(self):
        atomic = AtomicLong("test")
        atomic._value = 42
        result = atomic.apply(lambda x: f"value is {x}")
        assert result == "value is 42"

    def test_operations_on_destroyed_proxy_get(self):
        atomic = AtomicLong("test")
        atomic.destroy()
        with pytest.raises(IllegalStateException):
            atomic.get()

    def test_operations_on_destroyed_proxy_set(self):
        atomic = AtomicLong("test")
        atomic.destroy()
        with pytest.raises(IllegalStateException):
            atomic.set(100)

    def test_operations_on_destroyed_proxy_compare_and_set(self):
        atomic = AtomicLong("test")
        atomic.destroy()
        with pytest.raises(IllegalStateException):
            atomic.compare_and_set(0, 100)

    def test_operations_on_destroyed_proxy_increment(self):
        atomic = AtomicLong("test")
        atomic.destroy()
        with pytest.raises(IllegalStateException):
            atomic.increment_and_get()

    def test_operations_on_destroyed_proxy_alter(self):
        atomic = AtomicLong("test")
        atomic.destroy()
        with pytest.raises(IllegalStateException):
            atomic.alter(lambda x: x + 1)


class TestAtomicReference:
    """Tests for AtomicReference class."""

    def test_service_name_constant(self):
        assert AtomicReference.SERVICE_NAME == "hz:raft:atomicRefService"

    def test_initial_value_is_none(self):
        ref = AtomicReference("test")
        assert ref._value is None

    def test_get(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        assert ref.get() == "hello"

    def test_get_async(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        future = ref.get_async()
        assert isinstance(future, Future)
        assert future.result() == "hello"

    def test_set_string(self):
        ref = AtomicReference("test")
        ref.set("hello")
        assert ref._value == "hello"

    def test_set_dict(self):
        ref = AtomicReference("test")
        value = {"key": "value"}
        ref.set(value)
        assert ref._value == value

    def test_set_list(self):
        ref = AtomicReference("test")
        value = [1, 2, 3]
        ref.set(value)
        assert ref._value == value

    def test_set_none(self):
        ref = AtomicReference("test")
        ref._value = "something"
        ref.set(None)
        assert ref._value is None

    def test_set_async(self):
        ref = AtomicReference("test")
        future = ref.set_async("hello")
        assert future.result() is None
        assert ref._value == "hello"

    def test_get_and_set(self):
        ref = AtomicReference("test")
        ref._value = "old"
        previous = ref.get_and_set("new")
        assert previous == "old"
        assert ref._value == "new"

    def test_get_and_set_async(self):
        ref = AtomicReference("test")
        ref._value = "old"
        future = ref.get_and_set_async("new")
        assert future.result() == "old"
        assert ref._value == "new"

    def test_is_null_when_none(self):
        ref = AtomicReference("test")
        assert ref.is_null() is True

    def test_is_null_when_has_value(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        assert ref.is_null() is False

    def test_is_null_async_when_none(self):
        ref = AtomicReference("test")
        future = ref.is_null_async()
        assert future.result() is True

    def test_is_null_async_when_has_value(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        future = ref.is_null_async()
        assert future.result() is False

    def test_clear(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        ref.clear()
        assert ref._value is None

    def test_clear_async(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        future = ref.clear_async()
        assert future.result() is None
        assert ref._value is None

    def test_contains_matching_value(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        assert ref.contains("hello") is True

    def test_contains_non_matching_value(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        assert ref.contains("world") is False

    def test_contains_none_when_none(self):
        ref = AtomicReference("test")
        assert ref.contains(None) is True

    def test_contains_none_when_has_value(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        assert ref.contains(None) is False

    def test_contains_async_matching(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        future = ref.contains_async("hello")
        assert future.result() is True

    def test_contains_async_non_matching(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        future = ref.contains_async("world")
        assert future.result() is False

    def test_compare_and_set_success(self):
        ref = AtomicReference("test")
        ref._value = "old"
        result = ref.compare_and_set("old", "new")
        assert result is True
        assert ref._value == "new"

    def test_compare_and_set_failure(self):
        ref = AtomicReference("test")
        ref._value = "old"
        result = ref.compare_and_set("wrong", "new")
        assert result is False
        assert ref._value == "old"

    def test_compare_and_set_with_none(self):
        ref = AtomicReference("test")
        result = ref.compare_and_set(None, "new")
        assert result is True
        assert ref._value == "new"

    def test_compare_and_set_async_success(self):
        ref = AtomicReference("test")
        ref._value = "old"
        future = ref.compare_and_set_async("old", "new")
        assert future.result() is True
        assert ref._value == "new"

    def test_compare_and_set_async_failure(self):
        ref = AtomicReference("test")
        ref._value = "old"
        future = ref.compare_and_set_async("wrong", "new")
        assert future.result() is False
        assert ref._value == "old"

    def test_alter(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        ref.alter(lambda x: x.upper())
        assert ref._value == "HELLO"

    def test_alter_async(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        future = ref.alter_async(lambda x: x.upper())
        assert future.result() is None
        assert ref._value == "HELLO"

    def test_alter_with_none_function(self):
        ref = AtomicReference("test")
        with pytest.raises(IllegalArgumentException) as exc_info:
            ref.alter(None)
        assert "function cannot be None" in str(exc_info.value)

    def test_alter_async_with_none_function(self):
        ref = AtomicReference("test")
        with pytest.raises(IllegalArgumentException):
            ref.alter_async(None)

    def test_alter_and_get(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        result = ref.alter_and_get(lambda x: x.upper())
        assert result == "HELLO"
        assert ref._value == "HELLO"

    def test_alter_and_get_async(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        future = ref.alter_and_get_async(lambda x: x.upper())
        assert future.result() == "HELLO"
        assert ref._value == "HELLO"

    def test_alter_and_get_with_none_function(self):
        ref = AtomicReference("test")
        with pytest.raises(IllegalArgumentException):
            ref.alter_and_get(None)

    def test_get_and_alter(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        previous = ref.get_and_alter(lambda x: x.upper())
        assert previous == "hello"
        assert ref._value == "HELLO"

    def test_get_and_alter_async(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        future = ref.get_and_alter_async(lambda x: x.upper())
        assert future.result() == "hello"
        assert ref._value == "HELLO"

    def test_get_and_alter_with_none_function(self):
        ref = AtomicReference("test")
        with pytest.raises(IllegalArgumentException):
            ref.get_and_alter(None)

    def test_apply(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        result = ref.apply(lambda x: len(x))
        assert result == 5
        assert ref._value == "hello"

    def test_apply_async(self):
        ref = AtomicReference("test")
        ref._value = "hello"
        future = ref.apply_async(lambda x: len(x))
        assert future.result() == 5
        assert ref._value == "hello"

    def test_apply_with_none_function(self):
        ref = AtomicReference("test")
        with pytest.raises(IllegalArgumentException) as exc_info:
            ref.apply(None)
        assert "function cannot be None" in str(exc_info.value)

    def test_apply_async_with_none_function(self):
        ref = AtomicReference("test")
        with pytest.raises(IllegalArgumentException):
            ref.apply_async(None)

    def test_operations_on_destroyed_proxy_get(self):
        ref = AtomicReference("test")
        ref.destroy()
        with pytest.raises(IllegalStateException):
            ref.get()

    def test_operations_on_destroyed_proxy_set(self):
        ref = AtomicReference("test")
        ref.destroy()
        with pytest.raises(IllegalStateException):
            ref.set("value")

    def test_operations_on_destroyed_proxy_is_null(self):
        ref = AtomicReference("test")
        ref.destroy()
        with pytest.raises(IllegalStateException):
            ref.is_null()

    def test_operations_on_destroyed_proxy_clear(self):
        ref = AtomicReference("test")
        ref.destroy()
        with pytest.raises(IllegalStateException):
            ref.clear()

    def test_operations_on_destroyed_proxy_compare_and_set(self):
        ref = AtomicReference("test")
        ref.destroy()
        with pytest.raises(IllegalStateException):
            ref.compare_and_set(None, "value")

    def test_operations_on_destroyed_proxy_alter(self):
        ref = AtomicReference("test")
        ref.destroy()
        with pytest.raises(IllegalStateException):
            ref.alter(lambda x: x)
