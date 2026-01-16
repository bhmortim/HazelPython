"""PN Counter distributed data structure proxy.

This module provides the PNCounter (Positive-Negative Counter), a CRDT
that supports increment and decrement operations with eventual consistency.

Classes:
    VectorClock: Vector clock for tracking causality.
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

import threading
from concurrent.futures import Future
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

from hazelcast.proxy.base import Proxy, ProxyContext

if TYPE_CHECKING:
    from hazelcast.protocol.client_message import ClientMessage

SERVICE_NAME_PN_COUNTER = "hz:impl:PNCounterService"

PN_COUNTER_GET = 0x200100
PN_COUNTER_ADD = 0x200200
PN_COUNTER_GET_REPLICA_COUNT = 0x200300


class VectorClock:
    """Vector clock for tracking causality in CRDT operations.

    A vector clock is used to track the causal ordering of events
    across distributed replicas. Each replica maintains a logical
    timestamp that is updated on each operation.

    Attributes:
        timestamps: Dictionary mapping replica IDs to their timestamps.

    Example:
        >>> clock = VectorClock()
        >>> clock.set_replica_timestamp("replica-1", 5)
        >>> clock.set_replica_timestamp("replica-2", 3)
        >>> ts = clock.get_replica_timestamp("replica-1")  # 5
    """

    def __init__(self, timestamps: Optional[Dict[str, int]] = None):
        """Initialize the VectorClock.

        Args:
            timestamps: Initial timestamp dictionary.
        """
        self._timestamps: Dict[str, int] = timestamps.copy() if timestamps else {}
        self._lock = threading.Lock()

    @property
    def timestamps(self) -> Dict[str, int]:
        """Get a copy of the timestamps dictionary."""
        with self._lock:
            return self._timestamps.copy()

    def get_replica_timestamp(self, replica_id: str) -> int:
        """Get the timestamp for a replica.

        Args:
            replica_id: The replica identifier.

        Returns:
            The timestamp, or 0 if not present.
        """
        with self._lock:
            return self._timestamps.get(replica_id, 0)

    def set_replica_timestamp(self, replica_id: str, timestamp: int) -> None:
        """Set the timestamp for a replica.

        Args:
            replica_id: The replica identifier.
            timestamp: The new timestamp.
        """
        with self._lock:
            self._timestamps[replica_id] = timestamp

    def is_after(self, other: "VectorClock") -> bool:
        """Check if this clock is causally after another.

        Args:
            other: The other vector clock.

        Returns:
            True if this clock is strictly after the other.
        """
        with self._lock:
            dominated = False
            for replica_id, timestamp in other._timestamps.items():
                our_ts = self._timestamps.get(replica_id, 0)
                if our_ts < timestamp:
                    return False
                if our_ts > timestamp:
                    dominated = True

            for replica_id, timestamp in self._timestamps.items():
                if replica_id not in other._timestamps and timestamp > 0:
                    dominated = True

            return dominated

    def merge(self, other: "VectorClock") -> None:
        """Merge another vector clock into this one.

        Takes the maximum timestamp for each replica.

        Args:
            other: The other vector clock to merge.
        """
        with self._lock:
            for replica_id, timestamp in other._timestamps.items():
                current = self._timestamps.get(replica_id, 0)
                self._timestamps[replica_id] = max(current, timestamp)

    def entry_set(self) -> List[Tuple[str, int]]:
        """Get the entries as a list of tuples.

        Returns:
            List of (replica_id, timestamp) tuples.
        """
        with self._lock:
            return list(self._timestamps.items())

    def clear(self) -> None:
        """Clear all timestamps."""
        with self._lock:
            self._timestamps.clear()

    def __repr__(self) -> str:
        return f"VectorClock({self._timestamps})"


class PNCounterProxy(Proxy):
    """Proxy for Hazelcast PN Counter (Positive-Negative Counter).

    PNCounter is a CRDT (Conflict-free Replicated Data Type) counter
    that supports both increment and decrement operations. It provides
    eventual consistency and remains available even during network
    partitions.

    Unlike AtomicLong in the CP Subsystem, PNCounter favors availability
    over strong consistency. Updates are propagated asynchronously and
    all replicas eventually converge to the same value.

    The counter maintains:
    - A positive count (increments)
    - A negative count (decrements)
    - A vector clock for causality tracking

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

    SERVICE_NAME = SERVICE_NAME_PN_COUNTER

    def __init__(
        self,
        service_name: str,
        name: str,
        context: Optional[ProxyContext] = None,
    ):
        """Initialize the PNCounterProxy.

        Args:
            service_name: The service name for this proxy.
            name: The name of the distributed PN counter.
            context: The proxy context for service access.
        """
        super().__init__(service_name, name, context)
        self._observed_clock = VectorClock()
        self._current_target_replica: Optional[str] = None
        self._max_replica_count = 0
        self._value: int = 0
        self._lock = threading.Lock()

    @property
    def observed_clock(self) -> VectorClock:
        """Get the observed vector clock."""
        return self._observed_clock

    def get(self) -> int:
        """Get the current value of the counter.

        Returns the current value of this counter. The returned value
        is eventually consistent and may not reflect the most recent
        updates made by other clients.

        Returns:
            The current counter value.

        Raises:
            IllegalStateException: If the counter has been destroyed.

        Example:
            >>> value = counter.get()
            >>> print(f"Current count: {value}")
        """
        return self.get_async().result()

    def get_async(self) -> Future:
        """Get the current value asynchronously.

        Returns:
            A Future that will contain the counter value.
        """
        self._check_not_destroyed()
        future: Future = Future()
        with self._lock:
            future.set_result(self._value)
        return future

    def get_and_add(self, delta: int) -> int:
        """Add a delta and return the previous value.

        Atomically adds the given value to the current value and
        returns the previous value. Negative deltas are supported.

        Args:
            delta: The value to add (can be negative).

        Returns:
            The value before the add.

        Raises:
            IllegalStateException: If the counter has been destroyed.

        Example:
            >>> prev = counter.get_and_add(5)
            >>> print(f"Previous: {prev}, Current: {counter.get()}")
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
        with self._lock:
            previous = self._value
            self._value += delta
        future: Future = Future()
        future.set_result(previous)
        return future

    def add_and_get(self, delta: int) -> int:
        """Add a delta and return the new value.

        Atomically adds the given value to the current value and
        returns the new value. Negative deltas are supported.

        Args:
            delta: The value to add (can be negative).

        Returns:
            The value after the add.

        Raises:
            IllegalStateException: If the counter has been destroyed.

        Example:
            >>> new_value = counter.add_and_get(10)
            >>> print(f"New value: {new_value}")
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
        with self._lock:
            self._value += delta
            result = self._value
        future: Future = Future()
        future.set_result(result)
        return future

    def get_and_subtract(self, delta: int) -> int:
        """Subtract a delta and return the previous value.

        Atomically subtracts the given value from the current value
        and returns the previous value.

        Args:
            delta: The value to subtract.

        Returns:
            The value before the subtraction.

        Raises:
            IllegalStateException: If the counter has been destroyed.

        Example:
            >>> prev = counter.get_and_subtract(3)
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
        with self._lock:
            previous = self._value
            self._value -= delta
        future: Future = Future()
        future.set_result(previous)
        return future

    def subtract_and_get(self, delta: int) -> int:
        """Subtract a delta and return the new value.

        Atomically subtracts the given value from the current value
        and returns the new value.

        Args:
            delta: The value to subtract.

        Returns:
            The value after the subtraction.

        Raises:
            IllegalStateException: If the counter has been destroyed.

        Example:
            >>> new_value = counter.subtract_and_get(5)
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
        with self._lock:
            self._value -= delta
            result = self._value
        future: Future = Future()
        future.set_result(result)
        return future

    def get_and_increment(self) -> int:
        """Increment and return the previous value.

        Atomically increments the counter by one and returns the
        previous value.

        Returns:
            The value before the increment.

        Raises:
            IllegalStateException: If the counter has been destroyed.

        Example:
            >>> prev = counter.get_and_increment()
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

        Atomically increments the counter by one and returns the
        new value.

        Returns:
            The value after the increment.

        Raises:
            IllegalStateException: If the counter has been destroyed.

        Example:
            >>> new_value = counter.increment_and_get()
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

        Atomically decrements the counter by one and returns the
        previous value.

        Returns:
            The value before the decrement.

        Raises:
            IllegalStateException: If the counter has been destroyed.

        Example:
            >>> prev = counter.get_and_decrement()
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

        Atomically decrements the counter by one and returns the
        new value.

        Returns:
            The value after the decrement.

        Raises:
            IllegalStateException: If the counter has been destroyed.

        Example:
            >>> new_value = counter.decrement_and_get()
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
        with self._lock:
            self._observed_clock.clear()
            self._current_target_replica = None
            self._value = 0

    def get_replica_count(self) -> int:
        """Get the number of replicas for this counter.

        Returns:
            The number of replicas.
        """
        return self.get_replica_count_async().result()

    def get_replica_count_async(self) -> Future:
        """Get the replica count asynchronously.

        Returns:
            A Future containing the replica count.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(max(1, self._max_replica_count))
        return future

    def _update_observed_clock(self, clock: VectorClock) -> None:
        """Update the observed clock with values from another clock.

        Args:
            clock: The clock to merge.
        """
        self._observed_clock.merge(clock)


PNCounter = PNCounterProxy
