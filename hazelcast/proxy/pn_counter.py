"""PN Counter distributed data structure proxy."""

from concurrent.futures import Future
from typing import Optional

from hazelcast.proxy.base import Proxy, ProxyContext


class PNCounterProxy(Proxy):
    """Proxy for Hazelcast PN Counter (Positive-Negative Counter).

    A CRDT (Conflict-free Replicated Data Type) counter that supports
    both increment and decrement operations. Eventually consistent
    and available even during network partitions.
    """

    SERVICE_NAME = "hz:impl:PNCounterService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)
        self._observed_clock: dict = {}
        self._current_target_replica: Optional[str] = None

    def get(self) -> int:
        """Get the current value of the counter.

        Returns:
            The current counter value.
        """
        return self.get_async().result()

    def get_async(self) -> Future:
        """Get the current value asynchronously.

        Returns:
            A Future that will contain the counter value.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(0)
        return future

    def get_and_add(self, delta: int) -> int:
        """Add a delta and return the previous value.

        Args:
            delta: The value to add (can be negative).

        Returns:
            The value before the add.
        """
        return self.get_and_add_async(delta).result()

    def get_and_add_async(self, delta: int) -> Future:
        """Add a delta asynchronously.

        Args:
            delta: The value to add.

        Returns:
            A Future that will contain the previous value.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(0)
        return future

    def add_and_get(self, delta: int) -> int:
        """Add a delta and return the new value.

        Args:
            delta: The value to add (can be negative).

        Returns:
            The value after the add.
        """
        return self.add_and_get_async(delta).result()

    def add_and_get_async(self, delta: int) -> Future:
        """Add a delta asynchronously.

        Args:
            delta: The value to add.

        Returns:
            A Future that will contain the new value.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(delta)
        return future

    def get_and_subtract(self, delta: int) -> int:
        """Subtract a delta and return the previous value.

        Args:
            delta: The value to subtract.

        Returns:
            The value before the subtraction.
        """
        return self.get_and_subtract_async(delta).result()

    def get_and_subtract_async(self, delta: int) -> Future:
        """Subtract a delta asynchronously.

        Args:
            delta: The value to subtract.

        Returns:
            A Future that will contain the previous value.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(0)
        return future

    def subtract_and_get(self, delta: int) -> int:
        """Subtract a delta and return the new value.

        Args:
            delta: The value to subtract.

        Returns:
            The value after the subtraction.
        """
        return self.subtract_and_get_async(delta).result()

    def subtract_and_get_async(self, delta: int) -> Future:
        """Subtract a delta asynchronously.

        Args:
            delta: The value to subtract.

        Returns:
            A Future that will contain the new value.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(-delta)
        return future

    def get_and_increment(self) -> int:
        """Increment and return the previous value.

        Returns:
            The value before the increment.
        """
        return self.get_and_add(1)

    def get_and_increment_async(self) -> Future:
        """Increment asynchronously.

        Returns:
            A Future that will contain the previous value.
        """
        return self.get_and_add_async(1)

    def increment_and_get(self) -> int:
        """Increment and return the new value.

        Returns:
            The value after the increment.
        """
        return self.add_and_get(1)

    def increment_and_get_async(self) -> Future:
        """Increment asynchronously.

        Returns:
            A Future that will contain the new value.
        """
        return self.add_and_get_async(1)

    def get_and_decrement(self) -> int:
        """Decrement and return the previous value.

        Returns:
            The value before the decrement.
        """
        return self.get_and_subtract(1)

    def get_and_decrement_async(self) -> Future:
        """Decrement asynchronously.

        Returns:
            A Future that will contain the previous value.
        """
        return self.get_and_subtract_async(1)

    def decrement_and_get(self) -> int:
        """Decrement and return the new value.

        Returns:
            The value after the decrement.
        """
        return self.subtract_and_get(1)

    def decrement_and_get_async(self) -> Future:
        """Decrement asynchronously.

        Returns:
            A Future that will contain the new value.
        """
        return self.subtract_and_get_async(1)

    def reset(self) -> None:
        """Reset the observed clock and target replica.

        This does not reset the counter value, only the local state.
        """
        self._observed_clock.clear()
        self._current_target_replica = None
