"""CP Subsystem Atomic data structures."""

from concurrent.futures import Future
from typing import Any, Callable, Optional, TypeVar

from hazelcast.proxy.base import Proxy, ProxyContext
from hazelcast.exceptions import IllegalArgumentException

T = TypeVar("T")


class AtomicLong(Proxy):
    """CP Subsystem AtomicLong.

    A linearizable, distributed long value that supports atomic operations.
    Uses the Raft consensus algorithm for strong consistency guarantees.
    """

    SERVICE_NAME = "hz:raft:atomicLongService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)
        self._value: int = 0

    def get(self) -> int:
        """Get the current value.

        Returns:
            The current value.
        """
        return self.get_async().result()

    def get_async(self) -> Future:
        """Get the current value asynchronously.

        Returns:
            A Future that will contain the current value.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(self._value)
        return future

    def set(self, value: int) -> None:
        """Set the value.

        Args:
            value: The new value.
        """
        self.set_async(value).result()

    def set_async(self, value: int) -> Future:
        """Set the value asynchronously.

        Args:
            value: The new value.

        Returns:
            A Future that completes when the operation is done.
        """
        self._check_not_destroyed()
        self._value = value
        future: Future = Future()
        future.set_result(None)
        return future

    def get_and_set(self, value: int) -> int:
        """Set the value and return the previous value.

        Args:
            value: The new value.

        Returns:
            The previous value.
        """
        return self.get_and_set_async(value).result()

    def get_and_set_async(self, value: int) -> Future:
        """Set the value asynchronously and return the previous value.

        Args:
            value: The new value.

        Returns:
            A Future that will contain the previous value.
        """
        self._check_not_destroyed()
        previous = self._value
        self._value = value
        future: Future = Future()
        future.set_result(previous)
        return future

    def compare_and_set(self, expected: int, update: int) -> bool:
        """Atomically set the value if it equals the expected value.

        Args:
            expected: The expected current value.
            update: The new value to set.

        Returns:
            True if successful, False if the current value was not equal to expected.
        """
        return self.compare_and_set_async(expected, update).result()

    def compare_and_set_async(self, expected: int, update: int) -> Future:
        """Atomically set the value if it equals the expected value.

        Args:
            expected: The expected current value.
            update: The new value to set.

        Returns:
            A Future that will contain True if successful.
        """
        self._check_not_destroyed()
        future: Future = Future()
        if self._value == expected:
            self._value = update
            future.set_result(True)
        else:
            future.set_result(False)
        return future

    def get_and_add(self, delta: int) -> int:
        """Add a delta and return the previous value.

        Args:
            delta: The value to add.

        Returns:
            The previous value.
        """
        return self.get_and_add_async(delta).result()

    def get_and_add_async(self, delta: int) -> Future:
        """Add a delta asynchronously and return the previous value.

        Args:
            delta: The value to add.

        Returns:
            A Future that will contain the previous value.
        """
        self._check_not_destroyed()
        previous = self._value
        self._value += delta
        future: Future = Future()
        future.set_result(previous)
        return future

    def add_and_get(self, delta: int) -> int:
        """Add a delta and return the new value.

        Args:
            delta: The value to add.

        Returns:
            The new value.
        """
        return self.add_and_get_async(delta).result()

    def add_and_get_async(self, delta: int) -> Future:
        """Add a delta asynchronously and return the new value.

        Args:
            delta: The value to add.

        Returns:
            A Future that will contain the new value.
        """
        self._check_not_destroyed()
        self._value += delta
        future: Future = Future()
        future.set_result(self._value)
        return future

    def get_and_increment(self) -> int:
        """Increment and return the previous value.

        Returns:
            The previous value.
        """
        return self.get_and_add(1)

    def get_and_increment_async(self) -> Future:
        """Increment asynchronously and return the previous value.

        Returns:
            A Future that will contain the previous value.
        """
        return self.get_and_add_async(1)

    def increment_and_get(self) -> int:
        """Increment and return the new value.

        Returns:
            The new value.
        """
        return self.add_and_get(1)

    def increment_and_get_async(self) -> Future:
        """Increment asynchronously and return the new value.

        Returns:
            A Future that will contain the new value.
        """
        return self.add_and_get_async(1)

    def get_and_decrement(self) -> int:
        """Decrement and return the previous value.

        Returns:
            The previous value.
        """
        return self.get_and_add(-1)

    def get_and_decrement_async(self) -> Future:
        """Decrement asynchronously and return the previous value.

        Returns:
            A Future that will contain the previous value.
        """
        return self.get_and_add_async(-1)

    def decrement_and_get(self) -> int:
        """Decrement and return the new value.

        Returns:
            The new value.
        """
        return self.add_and_get(-1)

    def decrement_and_get_async(self) -> Future:
        """Decrement asynchronously and return the new value.

        Returns:
            A Future that will contain the new value.
        """
        return self.add_and_get_async(-1)

    def alter(self, function: Callable[[int], int]) -> None:
        """Alter the value by applying a function.

        Args:
            function: A function that takes the current value and returns a new value.
        """
        self.alter_async(function).result()

    def alter_async(self, function: Callable[[int], int]) -> Future:
        """Alter the value asynchronously by applying a function.

        Args:
            function: A function that takes the current value and returns a new value.

        Returns:
            A Future that completes when the operation is done.
        """
        self._check_not_destroyed()
        if function is None:
            raise IllegalArgumentException("function cannot be None")
        self._value = function(self._value)
        future: Future = Future()
        future.set_result(None)
        return future

    def alter_and_get(self, function: Callable[[int], int]) -> int:
        """Alter the value and return the new value.

        Args:
            function: A function that takes the current value and returns a new value.

        Returns:
            The new value.
        """
        return self.alter_and_get_async(function).result()

    def alter_and_get_async(self, function: Callable[[int], int]) -> Future:
        """Alter the value asynchronously and return the new value.

        Args:
            function: A function that takes the current value and returns a new value.

        Returns:
            A Future that will contain the new value.
        """
        self._check_not_destroyed()
        if function is None:
            raise IllegalArgumentException("function cannot be None")
        self._value = function(self._value)
        future: Future = Future()
        future.set_result(self._value)
        return future

    def get_and_alter(self, function: Callable[[int], int]) -> int:
        """Alter the value and return the previous value.

        Args:
            function: A function that takes the current value and returns a new value.

        Returns:
            The previous value.
        """
        return self.get_and_alter_async(function).result()

    def get_and_alter_async(self, function: Callable[[int], int]) -> Future:
        """Alter the value asynchronously and return the previous value.

        Args:
            function: A function that takes the current value and returns a new value.

        Returns:
            A Future that will contain the previous value.
        """
        self._check_not_destroyed()
        if function is None:
            raise IllegalArgumentException("function cannot be None")
        previous = self._value
        self._value = function(self._value)
        future: Future = Future()
        future.set_result(previous)
        return future

    def apply(self, function: Callable[[int], T]) -> T:
        """Apply a function to the value and return the result.

        Args:
            function: A function that takes the current value and returns a result.

        Returns:
            The result of applying the function.
        """
        return self.apply_async(function).result()

    def apply_async(self, function: Callable[[int], T]) -> Future:
        """Apply a function to the value asynchronously.

        Args:
            function: A function that takes the current value and returns a result.

        Returns:
            A Future that will contain the result.
        """
        self._check_not_destroyed()
        if function is None:
            raise IllegalArgumentException("function cannot be None")
        result = function(self._value)
        future: Future = Future()
        future.set_result(result)
        return future


class AtomicReference(Proxy):
    """CP Subsystem AtomicReference.

    A linearizable, distributed reference that supports atomic operations.
    Uses the Raft consensus algorithm for strong consistency guarantees.
    """

    SERVICE_NAME = "hz:raft:atomicRefService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)
        self._value: Any = None

    def get(self) -> Any:
        """Get the current value.

        Returns:
            The current value.
        """
        return self.get_async().result()

    def get_async(self) -> Future:
        """Get the current value asynchronously.

        Returns:
            A Future that will contain the current value.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(self._value)
        return future

    def set(self, value: Any) -> None:
        """Set the value.

        Args:
            value: The new value.
        """
        self.set_async(value).result()

    def set_async(self, value: Any) -> Future:
        """Set the value asynchronously.

        Args:
            value: The new value.

        Returns:
            A Future that completes when the operation is done.
        """
        self._check_not_destroyed()
        self._value = value
        future: Future = Future()
        future.set_result(None)
        return future

    def get_and_set(self, value: Any) -> Any:
        """Set the value and return the previous value.

        Args:
            value: The new value.

        Returns:
            The previous value.
        """
        return self.get_and_set_async(value).result()

    def get_and_set_async(self, value: Any) -> Future:
        """Set the value asynchronously and return the previous value.

        Args:
            value: The new value.

        Returns:
            A Future that will contain the previous value.
        """
        self._check_not_destroyed()
        previous = self._value
        self._value = value
        future: Future = Future()
        future.set_result(previous)
        return future

    def is_null(self) -> bool:
        """Check if the value is None.

        Returns:
            True if the value is None.
        """
        return self.is_null_async().result()

    def is_null_async(self) -> Future:
        """Check if the value is None asynchronously.

        Returns:
            A Future that will contain True if the value is None.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(self._value is None)
        return future

    def clear(self) -> None:
        """Set the value to None."""
        self.clear_async().result()

    def clear_async(self) -> Future:
        """Set the value to None asynchronously.

        Returns:
            A Future that completes when the operation is done.
        """
        self._check_not_destroyed()
        self._value = None
        future: Future = Future()
        future.set_result(None)
        return future

    def contains(self, value: Any) -> bool:
        """Check if the current value equals the given value.

        Args:
            value: The value to compare.

        Returns:
            True if the current value equals the given value.
        """
        return self.contains_async(value).result()

    def contains_async(self, value: Any) -> Future:
        """Check if the current value equals the given value asynchronously.

        Args:
            value: The value to compare.

        Returns:
            A Future that will contain True if the values are equal.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(self._value == value)
        return future

    def compare_and_set(self, expected: Any, update: Any) -> bool:
        """Atomically set the value if it equals the expected value.

        Args:
            expected: The expected current value.
            update: The new value to set.

        Returns:
            True if successful, False if the current value was not equal to expected.
        """
        return self.compare_and_set_async(expected, update).result()

    def compare_and_set_async(self, expected: Any, update: Any) -> Future:
        """Atomically set the value if it equals the expected value.

        Args:
            expected: The expected current value.
            update: The new value to set.

        Returns:
            A Future that will contain True if successful.
        """
        self._check_not_destroyed()
        future: Future = Future()
        if self._value == expected:
            self._value = update
            future.set_result(True)
        else:
            future.set_result(False)
        return future

    def alter(self, function: Callable[[Any], Any]) -> None:
        """Alter the value by applying a function.

        Args:
            function: A function that takes the current value and returns a new value.
        """
        self.alter_async(function).result()

    def alter_async(self, function: Callable[[Any], Any]) -> Future:
        """Alter the value asynchronously by applying a function.

        Args:
            function: A function that takes the current value and returns a new value.

        Returns:
            A Future that completes when the operation is done.
        """
        self._check_not_destroyed()
        if function is None:
            raise IllegalArgumentException("function cannot be None")
        self._value = function(self._value)
        future: Future = Future()
        future.set_result(None)
        return future

    def alter_and_get(self, function: Callable[[Any], Any]) -> Any:
        """Alter the value and return the new value.

        Args:
            function: A function that takes the current value and returns a new value.

        Returns:
            The new value.
        """
        return self.alter_and_get_async(function).result()

    def alter_and_get_async(self, function: Callable[[Any], Any]) -> Future:
        """Alter the value asynchronously and return the new value.

        Args:
            function: A function that takes the current value and returns a new value.

        Returns:
            A Future that will contain the new value.
        """
        self._check_not_destroyed()
        if function is None:
            raise IllegalArgumentException("function cannot be None")
        self._value = function(self._value)
        future: Future = Future()
        future.set_result(self._value)
        return future

    def get_and_alter(self, function: Callable[[Any], Any]) -> Any:
        """Alter the value and return the previous value.

        Args:
            function: A function that takes the current value and returns a new value.

        Returns:
            The previous value.
        """
        return self.get_and_alter_async(function).result()

    def get_and_alter_async(self, function: Callable[[Any], Any]) -> Future:
        """Alter the value asynchronously and return the previous value.

        Args:
            function: A function that takes the current value and returns a new value.

        Returns:
            A Future that will contain the previous value.
        """
        self._check_not_destroyed()
        if function is None:
            raise IllegalArgumentException("function cannot be None")
        previous = self._value
        self._value = function(self._value)
        future: Future = Future()
        future.set_result(previous)
        return future

    def apply(self, function: Callable[[Any], T]) -> T:
        """Apply a function to the value and return the result.

        Args:
            function: A function that takes the current value and returns a result.

        Returns:
            The result of applying the function.
        """
        return self.apply_async(function).result()

    def apply_async(self, function: Callable[[Any], T]) -> Future:
        """Apply a function to the value asynchronously.

        Args:
            function: A function that takes the current value and returns a result.

        Returns:
            A Future that will contain the result.
        """
        self._check_not_destroyed()
        if function is None:
            raise IllegalArgumentException("function cannot be None")
        result = function(self._value)
        future: Future = Future()
        future.set_result(result)
        return future
