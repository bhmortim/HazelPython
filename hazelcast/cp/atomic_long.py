"""CP AtomicLong distributed data structure."""

from concurrent.futures import Future
from typing import Callable, Optional, TYPE_CHECKING

from hazelcast.cp.base import CPProxy, CPGroupId

if TYPE_CHECKING:
    from hazelcast.invocation import InvocationService


class AtomicLong(CPProxy):
    """A distributed atomic long backed by CP subsystem.

    Provides linearizable operations on a 64-bit integer value
    with support for atomic compare-and-set, increment, and update operations.
    """

    SERVICE_NAME = "hz:raft:atomicLongService"

    def __init__(
        self,
        name: str,
        group_id: Optional[CPGroupId] = None,
        invocation_service: Optional["InvocationService"] = None,
        direct_to_leader: bool = True,
    ):
        super().__init__(
            self.SERVICE_NAME,
            name,
            group_id or CPGroupId(CPGroupId.DEFAULT_GROUP_NAME),
            invocation_service,
            direct_to_leader,
        )
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
            Future containing the current value.
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
            Future that completes when the value is set.
        """
        self._check_not_destroyed()
        self._value = value
        future: Future = Future()
        future.set_result(None)
        return future

    def get_and_set(self, value: int) -> int:
        """Atomically set the value and return the old value.

        Args:
            value: The new value.

        Returns:
            The previous value.
        """
        return self.get_and_set_async(value).result()

    def get_and_set_async(self, value: int) -> Future:
        """Atomically set and get asynchronously.

        Args:
            value: The new value.

        Returns:
            Future containing the previous value.
        """
        self._check_not_destroyed()
        old_value = self._value
        self._value = value
        future: Future = Future()
        future.set_result(old_value)
        return future

    def compare_and_set(self, expected: int, updated: int) -> bool:
        """Atomically set the value if it equals the expected value.

        Args:
            expected: The expected current value.
            updated: The new value.

        Returns:
            True if the value was updated, False otherwise.
        """
        return self.compare_and_set_async(expected, updated).result()

    def compare_and_set_async(self, expected: int, updated: int) -> Future:
        """Atomically compare and set asynchronously.

        Args:
            expected: The expected current value.
            updated: The new value.

        Returns:
            Future containing True if updated, False otherwise.
        """
        self._check_not_destroyed()
        future: Future = Future()
        if self._value == expected:
            self._value = updated
            future.set_result(True)
        else:
            future.set_result(False)
        return future

    def increment_and_get(self) -> int:
        """Atomically increment and return the new value.

        Returns:
            The new value after incrementing.
        """
        return self.increment_and_get_async().result()

    def increment_and_get_async(self) -> Future:
        """Atomically increment and get asynchronously.

        Returns:
            Future containing the new value.
        """
        self._check_not_destroyed()
        self._value += 1
        future: Future = Future()
        future.set_result(self._value)
        return future

    def decrement_and_get(self) -> int:
        """Atomically decrement and return the new value.

        Returns:
            The new value after decrementing.
        """
        return self.decrement_and_get_async().result()

    def decrement_and_get_async(self) -> Future:
        """Atomically decrement and get asynchronously.

        Returns:
            Future containing the new value.
        """
        self._check_not_destroyed()
        self._value -= 1
        future: Future = Future()
        future.set_result(self._value)
        return future

    def get_and_increment(self) -> int:
        """Atomically get the current value and then increment.

        Returns:
            The value before incrementing.
        """
        return self.get_and_increment_async().result()

    def get_and_increment_async(self) -> Future:
        """Atomically get and increment asynchronously.

        Returns:
            Future containing the value before incrementing.
        """
        self._check_not_destroyed()
        old_value = self._value
        self._value += 1
        future: Future = Future()
        future.set_result(old_value)
        return future

    def get_and_decrement(self) -> int:
        """Atomically get the current value and then decrement.

        Returns:
            The value before decrementing.
        """
        return self.get_and_decrement_async().result()

    def get_and_decrement_async(self) -> Future:
        """Atomically get and decrement asynchronously.

        Returns:
            Future containing the value before decrementing.
        """
        self._check_not_destroyed()
        old_value = self._value
        self._value -= 1
        future: Future = Future()
        future.set_result(old_value)
        return future

    def add_and_get(self, delta: int) -> int:
        """Atomically add a value and return the new value.

        Args:
            delta: The value to add.

        Returns:
            The new value.
        """
        return self.add_and_get_async(delta).result()

    def add_and_get_async(self, delta: int) -> Future:
        """Atomically add and get asynchronously.

        Args:
            delta: The value to add.

        Returns:
            Future containing the new value.
        """
        self._check_not_destroyed()
        self._value += delta
        future: Future = Future()
        future.set_result(self._value)
        return future

    def get_and_add(self, delta: int) -> int:
        """Atomically get the current value and then add.

        Args:
            delta: The value to add.

        Returns:
            The value before adding.
        """
        return self.get_and_add_async(delta).result()

    def get_and_add_async(self, delta: int) -> Future:
        """Atomically get and add asynchronously.

        Args:
            delta: The value to add.

        Returns:
            Future containing the value before adding.
        """
        self._check_not_destroyed()
        old_value = self._value
        self._value += delta
        future: Future = Future()
        future.set_result(old_value)
        return future

    def alter(self, function: Callable[[int], int]) -> None:
        """Atomically alter the value using a function.

        Args:
            function: Function to apply to the current value.
        """
        self.alter_async(function).result()

    def alter_async(self, function: Callable[[int], int]) -> Future:
        """Atomically alter asynchronously.

        Args:
            function: Function to apply to the current value.

        Returns:
            Future that completes when the alteration is done.
        """
        self._check_not_destroyed()
        self._value = function(self._value)
        future: Future = Future()
        future.set_result(None)
        return future

    def alter_and_get(self, function: Callable[[int], int]) -> int:
        """Atomically alter and return the new value.

        Args:
            function: Function to apply to the current value.

        Returns:
            The new value.
        """
        return self.alter_and_get_async(function).result()

    def alter_and_get_async(self, function: Callable[[int], int]) -> Future:
        """Atomically alter and get asynchronously.

        Args:
            function: Function to apply to the current value.

        Returns:
            Future containing the new value.
        """
        self._check_not_destroyed()
        self._value = function(self._value)
        future: Future = Future()
        future.set_result(self._value)
        return future

    def get_and_alter(self, function: Callable[[int], int]) -> int:
        """Atomically get the current value and then alter.

        Args:
            function: Function to apply to the current value.

        Returns:
            The value before alteration.
        """
        return self.get_and_alter_async(function).result()

    def get_and_alter_async(self, function: Callable[[int], int]) -> Future:
        """Atomically get and alter asynchronously.

        Args:
            function: Function to apply to the current value.

        Returns:
            Future containing the value before alteration.
        """
        self._check_not_destroyed()
        old_value = self._value
        self._value = function(self._value)
        future: Future = Future()
        future.set_result(old_value)
        return future

    def apply(self, function: Callable[[int], any]) -> any:
        """Apply a function to the current value.

        Args:
            function: Function to apply.

        Returns:
            The result of applying the function.
        """
        return self.apply_async(function).result()

    def apply_async(self, function: Callable[[int], any]) -> Future:
        """Apply a function asynchronously.

        Args:
            function: Function to apply.

        Returns:
            Future containing the result.
        """
        self._check_not_destroyed()
        result = function(self._value)
        future: Future = Future()
        future.set_result(result)
        return future
