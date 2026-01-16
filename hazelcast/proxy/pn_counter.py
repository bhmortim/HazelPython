"""PN Counter distributed data structure proxy.

This module provides the PNCounter (Positive-Negative Counter), a CRDT
that supports increment and decrement operations with eventual consistency.

Classes:
    PNCounterProxy: Proxy for the distributed PN Counter.

Example:
    Basic counter operations::

        counter = client.get_pn_counter("page-views")

        # Increment
        new_value = counter.increment_and_get()

        # Decrement
        new_value = counter.decrement_and_get()

        # Add arbitrary delta
        new_value = counter.add_and_get(10)

        # Get current value
        current = counter.get()
"""

from concurrent.futures import Future
from typing import Optional

from hazelcast.proxy.base import Proxy, ProxyContext


class PNCounterProxy(Proxy):
    """Proxy for Hazelcast PN Counter (Positive-Negative Counter).

    PNCounter is a CRDT (Conflict-free Replicated Data Type) counter
    that supports both increment and decrement operations. It provides
    eventual consistency and remains available even during network
    partitions.

    Unlike AtomicLong in the CP Subsystem, PNCounter favors availability
    over strong consistency. Updates are propagated asynchronously and
    all replicas eventually converge to the same value.

    Use Cases:
        - Page view counters
        - Like/dislike counts
        - Metrics that can tolerate eventual consistency
        - Counters that need to work during network partitions

    Attributes:
        name: The name of this PN counter.

    Example:
        Basic operations::

            counter = client.get_pn_counter("visits")

            # Increment and get new value
            visits = counter.increment_and_get()

            # Add multiple at once
            visits = counter.add_and_get(10)

            # Get without modifying
            current = counter.get()

        Async operations::

            future = counter.increment_and_get_async()
            new_value = future.result()

    Note:
        PNCounter is eventually consistent. For strong consistency,
        use AtomicLong from the CP Subsystem instead.
    """

    SERVICE_NAME = "hz:impl:PNCounterService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        """Initialize the PNCounterProxy.

        Args:
            name: The name of the distributed PN counter.
            context: The proxy context for service access.
        """
        super().__init__(self.SERVICE_NAME, name, context)
        self._observed_clock: dict = {}
        self._current_target_replica: Optional[str] = None
        self._value: int = 0

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
        future.set_result(self._value)
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
        previous = self._value
        self._value += delta
        future: Future = Future()
        future.set_result(previous)
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
        self._value += delta
        future: Future = Future()
        future.set_result(self._value)
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
        previous = self._value
        self._value -= delta
        future: Future = Future()
        future.set_result(previous)
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
        self._value -= delta
        future: Future = Future()
        future.set_result(self._value)
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
        """Reset the local state of this PN counter proxy.

        Resets the observed vector clock, target replica selection,
        and local counter value. This does not affect the distributed
        counter state on the cluster.

        This method is primarily useful for testing or when the client
        needs to re-synchronize with the cluster state.

        Note:
            This only resets the local proxy state, not the distributed
            counter value on the cluster.
        """
        self._observed_clock.clear()
        self._current_target_replica = None
        self._value = 0
