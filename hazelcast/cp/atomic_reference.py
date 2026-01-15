"""CP AtomicReference distributed data structure."""

from concurrent.futures import Future
from typing import Any, Callable, Generic, Optional, TypeVar, TYPE_CHECKING

from hazelcast.cp.base import CPProxy, CPGroupId

if TYPE_CHECKING:
    from hazelcast.invocation import InvocationService

T = TypeVar("T")


class AtomicReference(CPProxy, Generic[T]):
    """A distributed atomic reference backed by CP subsystem.

    Provides linearizable operations on an object reference
    with support for atomic compare-and-set and update operations.
    """

    SERVICE_NAME = "hz:raft:atomicRefService"

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
        self._value: Optional[T] = None

    def get(self) -> Optional[T]:
        """Get the current value.

        Returns:
            The current value, or None if not set.
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

    def set(self, value: Optional[T]) -> None:
        """Set the value.

        Args:
            value: The new value.
        """
        self.set_async(value).result()

    def set_async(self, value: Optional[T]) -> Future:
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

    def get_and_set(self, value: Optional[T]) -> Optional[T]:
        """Atomically set the value and return the old value.

        Args:
            value: The new value.

        Returns:
            The previous value.
        """
        return self.get_and_set_async(value).result()

    def get_and_set_async(self, value: Optional[T]) -> Future:
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

    def compare_and_set(self, expected: Optional[T], updated: Optional[T]) -> bool:
        """Atomically set the value if it equals the expected value.

        Args:
            expected: The expected current value.
            updated: The new value.

        Returns:
            True if the value was updated, False otherwise.
        """
        return self.compare_and_set_async(expected, updated).result()

    def compare_and_set_async(
        self, expected: Optional[T], updated: Optional[T]
    ) -> Future:
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

    def is_null(self) -> bool:
        """Check if the current value is None.

        Returns:
            True if the value is None.
        """
        return self.is_null_async().result()

    def is_null_async(self) -> Future:
        """Check if null asynchronously.

        Returns:
            Future containing True if value is None.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(self._value is None)
        return future

    def clear(self) -> None:
        """Clear the value (set to None)."""
        self.clear_async().result()

    def clear_async(self) -> Future:
        """Clear the value asynchronously.

        Returns:
            Future that completes when cleared.
        """
        self._check_not_destroyed()
        self._value = None
        future: Future = Future()
        future.set_result(None)
        return future

    def contains(self, value: Optional[T]) -> bool:
        """Check if the current value equals the given value.

        Args:
            value: The value to check.

        Returns:
            True if equal.
        """
        return self.contains_async(value).result()

    def contains_async(self, value: Optional[T]) -> Future:
        """Check if contains asynchronously.

        Args:
            value: The value to check.

        Returns:
            Future containing True if equal.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(self._value == value)
        return future

    def alter(self, function: Callable[[Optional[T]], Optional[T]]) -> None:
        """Atomically alter the value using a function.

        Args:
            function: Function to apply to the current value.
        """
        self.alter_async(function).result()

    def alter_async(
        self, function: Callable[[Optional[T]], Optional[T]]
    ) -> Future:
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

    def alter_and_get(
        self, function: Callable[[Optional[T]], Optional[T]]
    ) -> Optional[T]:
        """Atomically alter and return the new value.

        Args:
            function: Function to apply to the current value.

        Returns:
            The new value.
        """
        return self.alter_and_get_async(function).result()

    def alter_and_get_async(
        self, function: Callable[[Optional[T]], Optional[T]]
    ) -> Future:
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

    def get_and_alter(
        self, function: Callable[[Optional[T]], Optional[T]]
    ) -> Optional[T]:
        """Atomically get the current value and then alter.

        Args:
            function: Function to apply to the current value.

        Returns:
            The value before alteration.
        """
        return self.get_and_alter_async(function).result()

    def get_and_alter_async(
        self, function: Callable[[Optional[T]], Optional[T]]
    ) -> Future:
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

    def apply(self, function: Callable[[Optional[T]], Any]) -> Any:
        """Apply a function to the current value.

        Args:
            function: Function to apply.

        Returns:
            The result of applying the function.
        """
        return self.apply_async(function).result()

    def apply_async(self, function: Callable[[Optional[T]], Any]) -> Future:
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
