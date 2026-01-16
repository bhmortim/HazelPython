"""Ringbuffer distributed data structure proxy."""

from concurrent.futures import Future
from enum import Enum
from threading import RLock
from typing import Any, Dict, Generic, List, Optional, TypeVar

from hazelcast.proxy.base import Proxy, ProxyContext

try:
    from hazelcast.exceptions import (
        IllegalArgumentException,
        StaleSequenceException,
    )
except ImportError:
    class IllegalArgumentException(Exception):
        """Raised when an illegal argument is passed."""
        pass

    class StaleSequenceException(Exception):
        """Raised when a sequence is older than the head sequence."""
        pass

E = TypeVar("E")


class OverflowPolicy(Enum):
    """Overflow policy for ringbuffer when full.

    Determines the behavior when adding items to a full ringbuffer.

    Attributes:
        OVERWRITE: Overwrite the oldest items when the buffer is full.
        FAIL: Fail the add operation when the buffer is full.

    Example:
        >>> rb.add("item", OverflowPolicy.OVERWRITE)
    """

    OVERWRITE = 0
    FAIL = 1


class RingbufferProxy(Proxy, Generic[E]):
    """Proxy for Hazelcast Ringbuffer distributed data structure.

    A ringbuffer is a bounded data structure with a fixed capacity.
    Items are stored with sequence numbers that increase monotonically.
    When the buffer is full, the overflow policy determines behavior.

    Ringbuffers are ideal for:
    - Event sourcing and audit logs
    - Reliable messaging (backing ReliableTopic)
    - Time-series data with bounded retention

    Type Parameters:
        E: The element type stored in the ringbuffer.

    Attributes:
        name: The name of this distributed ringbuffer.

    Example:
        Basic ringbuffer usage::

            rb = client.get_ringbuffer("events")
            seq = rb.add("event-1")
            seq = rb.add("event-2")
            item = rb.read_one(seq)
            print(f"Read: {item}")
    """

    SERVICE_NAME = "hz:impl:ringbufferService"

    SEQUENCE_UNAVAILABLE = -1
    DEFAULT_CAPACITY = 10000

    def __init__(
        self,
        name: str,
        context: Optional[ProxyContext] = None,
        capacity: int = DEFAULT_CAPACITY,
    ):
        super().__init__(self.SERVICE_NAME, name, context)
        self._ring_capacity = max(1, capacity)
        self._items: Dict[int, E] = {}
        self._head_seq = 0
        self._tail_seq = -1
        self._lock = RLock()

    def capacity(self) -> int:
        """Get the capacity of the ringbuffer.

        Returns:
            The capacity.
        """
        return self.capacity_async().result()

    def capacity_async(self) -> Future:
        """Get the capacity asynchronously.

        Returns:
            A Future that will contain the capacity.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(self._ring_capacity)
        return future

    def size(self) -> int:
        """Get the number of items in the ringbuffer.

        Returns:
            The number of items.
        """
        return self.size_async().result()

    def size_async(self) -> Future:
        """Get the size asynchronously.

        Returns:
            A Future that will contain the size.
        """
        self._check_not_destroyed()
        future: Future = Future()
        with self._lock:
            if self._tail_seq < 0:
                future.set_result(0)
            else:
                future.set_result(self._tail_seq - self._head_seq + 1)
        return future

    def tail_sequence(self) -> int:
        """Get the sequence of the tail (last item).

        Returns:
            The tail sequence, or -1 if empty.
        """
        return self.tail_sequence_async().result()

    def tail_sequence_async(self) -> Future:
        """Get the tail sequence asynchronously.

        Returns:
            A Future that will contain the tail sequence.
        """
        self._check_not_destroyed()
        future: Future = Future()
        with self._lock:
            future.set_result(self._tail_seq)
        return future

    def head_sequence(self) -> int:
        """Get the sequence of the head (oldest item).

        Returns:
            The head sequence, or 0 if empty.
        """
        return self.head_sequence_async().result()

    def head_sequence_async(self) -> Future:
        """Get the head sequence asynchronously.

        Returns:
            A Future that will contain the head sequence.
        """
        self._check_not_destroyed()
        future: Future = Future()
        with self._lock:
            future.set_result(self._head_seq)
        return future

    def remaining_capacity(self) -> int:
        """Get the remaining capacity.

        Returns:
            The remaining capacity.
        """
        return self.remaining_capacity_async().result()

    def remaining_capacity_async(self) -> Future:
        """Get the remaining capacity asynchronously.

        Returns:
            A Future that will contain the remaining capacity.
        """
        self._check_not_destroyed()
        future: Future = Future()
        with self._lock:
            current_size = 0 if self._tail_seq < 0 else (self._tail_seq - self._head_seq + 1)
            future.set_result(self._ring_capacity - current_size)
        return future

    def add(self, item: E, overflow_policy: OverflowPolicy = OverflowPolicy.OVERWRITE) -> int:
        """Add an item to the ringbuffer.

        Args:
            item: The item to add. Must be serializable.
            overflow_policy: Policy when the buffer is full. Defaults to OVERWRITE.

        Returns:
            The sequence number of the added item, or -1 if the buffer is
            full and FAIL policy is used.

        Raises:
            IllegalStateException: If the ringbuffer has been destroyed.

        Example:
            >>> seq = rb.add("my-event", OverflowPolicy.OVERWRITE)
            >>> print(f"Added at sequence: {seq}")
        """
        return self.add_async(item, overflow_policy).result()

    def add_async(self, item: E, overflow_policy: OverflowPolicy = OverflowPolicy.OVERWRITE) -> Future:
        """Add an item asynchronously.

        Args:
            item: The item to add.
            overflow_policy: Policy when the buffer is full.

        Returns:
            A Future that will contain the sequence number.
        """
        self._check_not_destroyed()
        future: Future = Future()

        with self._lock:
            current_size = 0 if self._tail_seq < 0 else (self._tail_seq - self._head_seq + 1)

            if current_size >= self._ring_capacity:
                if overflow_policy == OverflowPolicy.FAIL:
                    future.set_result(self.SEQUENCE_UNAVAILABLE)
                    return future
                old_seq = self._head_seq
                if old_seq in self._items:
                    del self._items[old_seq]
                self._head_seq += 1

            self._tail_seq += 1
            new_seq = self._tail_seq
            self._items[new_seq] = item
            future.set_result(new_seq)

        return future

    def add_all(
        self,
        items: List[E],
        overflow_policy: OverflowPolicy = OverflowPolicy.OVERWRITE,
    ) -> int:
        """Add multiple items to the ringbuffer.

        Args:
            items: The items to add.
            overflow_policy: Policy when the buffer is full.

        Returns:
            The sequence number of the last added item, or -1 if failed.
        """
        return self.add_all_async(items, overflow_policy).result()

    def add_all_async(
        self,
        items: List[E],
        overflow_policy: OverflowPolicy = OverflowPolicy.OVERWRITE,
    ) -> Future:
        """Add multiple items asynchronously.

        Args:
            items: The items to add.
            overflow_policy: Policy when the buffer is full.

        Returns:
            A Future that will contain the sequence number of the last item.
        """
        self._check_not_destroyed()
        future: Future = Future()

        if not items:
            future.set_result(self._tail_seq)
            return future

        with self._lock:
            current_size = 0 if self._tail_seq < 0 else (self._tail_seq - self._head_seq + 1)
            items_to_add = len(items)

            if overflow_policy == OverflowPolicy.FAIL:
                if current_size + items_to_add > self._ring_capacity:
                    future.set_result(self.SEQUENCE_UNAVAILABLE)
                    return future

            for item in items:
                current_size = 0 if self._tail_seq < 0 else (self._tail_seq - self._head_seq + 1)
                if current_size >= self._ring_capacity:
                    old_seq = self._head_seq
                    if old_seq in self._items:
                        del self._items[old_seq]
                    self._head_seq += 1

                self._tail_seq += 1
                self._items[self._tail_seq] = item

            future.set_result(self._tail_seq)

        return future

    def read_one(self, sequence: int) -> E:
        """Read a single item at the given sequence.

        Args:
            sequence: The sequence number to read.

        Returns:
            The item at the specified sequence.

        Raises:
            StaleSequenceException: If the sequence is older than the head
                (data has been overwritten).
            IllegalArgumentException: If the sequence is beyond the tail
                (data doesn't exist yet).
            IllegalStateException: If the ringbuffer has been destroyed.

        Example:
            >>> item = rb.read_one(100)
            >>> print(f"Event at seq 100: {item}")
        """
        return self.read_one_async(sequence).result()

    def read_one_async(self, sequence: int) -> Future:
        """Read a single item asynchronously.

        Args:
            sequence: The sequence to read.

        Returns:
            A Future that will contain the item.
        """
        self._check_not_destroyed()
        future: Future = Future()

        with self._lock:
            if self._tail_seq < 0:
                future.set_exception(
                    IllegalArgumentException(f"Sequence {sequence} is beyond tail -1")
                )
                return future

            if sequence < self._head_seq:
                future.set_exception(
                    StaleSequenceException(
                        f"Sequence {sequence} is older than head {self._head_seq}"
                    )
                )
                return future

            if sequence > self._tail_seq:
                future.set_exception(
                    IllegalArgumentException(
                        f"Sequence {sequence} is beyond tail {self._tail_seq}"
                    )
                )
                return future

            future.set_result(self._items.get(sequence))

        return future

    def read_many(
        self,
        start_sequence: int,
        min_count: int,
        max_count: int,
        filter_predicate: Any = None,
    ) -> "ReadResultSet[E]":
        """Read multiple items starting from a sequence.

        Reads a batch of items from the ringbuffer for efficient
        bulk retrieval.

        Args:
            start_sequence: The sequence number to start reading from.
            min_count: Minimum number of items to read (blocks until available).
            max_count: Maximum number of items to read in one batch.
            filter_predicate: Optional predicate to filter items.

        Returns:
            A ReadResultSet containing the items and metadata.

        Raises:
            StaleSequenceException: If start_sequence is older than head.
            IllegalArgumentException: If min_count < 0 or max_count < min_count.
            IllegalStateException: If the ringbuffer has been destroyed.

        Example:
            >>> result = rb.read_many(0, 1, 100)
            >>> for item in result:
            ...     print(item)
            >>> print(f"Next sequence: {result.next_sequence_to_read}")
        """
        return self.read_many_async(start_sequence, min_count, max_count, filter_predicate).result()

    def read_many_async(
        self,
        start_sequence: int,
        min_count: int,
        max_count: int,
        filter_predicate: Any = None,
    ) -> Future:
        """Read multiple items asynchronously.

        Args:
            start_sequence: The sequence to start reading from.
            min_count: Minimum number of items to read.
            max_count: Maximum number of items to read.
            filter_predicate: Optional predicate to filter items.

        Returns:
            A Future that will contain a ReadResultSet.
        """
        self._check_not_destroyed()
        future: Future = Future()

        if min_count < 0:
            future.set_exception(
                IllegalArgumentException("min_count must be non-negative")
            )
            return future

        if max_count < min_count:
            future.set_exception(
                IllegalArgumentException("max_count must be >= min_count")
            )
            return future

        with self._lock:
            if start_sequence < self._head_seq:
                future.set_exception(
                    StaleSequenceException(
                        f"Sequence {start_sequence} is older than head {self._head_seq}"
                    )
                )
                return future

            items: List[E] = []
            sequences: List[int] = []
            current_seq = start_sequence

            while len(items) < max_count and current_seq <= self._tail_seq:
                item = self._items.get(current_seq)
                if item is not None:
                    if filter_predicate is None or filter_predicate(item):
                        items.append(item)
                        sequences.append(current_seq)
                current_seq += 1

            read_count = len(items)
            next_seq = start_sequence + read_count if items else start_sequence
            result_set = ReadResultSet(items, read_count, next_seq, sequences)
            future.set_result(result_set)

        return future

    def __len__(self) -> int:
        return self.size()


class ReadResultSet(Generic[E]):
    """Result set from a ringbuffer read_many operation.

    Contains the items read from the ringbuffer along with metadata
    for pagination and sequence tracking.

    Type Parameters:
        E: The element type.

    Attributes:
        items: The list of items read.
        read_count: The number of items read.
        next_sequence_to_read: The next sequence to continue reading from.

    Example:
        >>> result = rb.read_many(0, 1, 100)
        >>> print(f"Read {result.read_count} items")
        >>> for item in result:
        ...     print(item)
    """

    def __init__(
        self,
        items: List[E],
        read_count: int,
        next_sequence_to_read: int,
        sequences: Optional[List[int]] = None,
    ):
        self._items = items
        self._read_count = read_count
        self._next_sequence = next_sequence_to_read
        self._sequences = sequences or []

    @property
    def items(self) -> List[E]:
        return self._items

    @property
    def read_count(self) -> int:
        return self._read_count

    @property
    def next_sequence_to_read(self) -> int:
        return self._next_sequence

    def get_sequence(self, index: int) -> int:
        """Get the sequence number of an item.

        Args:
            index: The index in the result set.

        Returns:
            The sequence number.

        Raises:
            IndexError: If index is out of range.
        """
        if self._sequences and 0 <= index < len(self._sequences):
            return self._sequences[index]
        if 0 <= index < self._read_count:
            return self._next_sequence - self._read_count + index
        raise IndexError(f"Index {index} out of range")

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, index: int) -> E:
        return self._items[index]

    def __iter__(self):
        return iter(self._items)

    def __repr__(self) -> str:
        return f"ReadResultSet(items={self._items}, read_count={self._read_count})"
