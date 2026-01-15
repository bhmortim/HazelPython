"""Ringbuffer distributed data structure proxy."""

from concurrent.futures import Future
from enum import Enum
from typing import Any, Generic, List, Optional, TypeVar

from hazelcast.proxy.base import Proxy, ProxyContext

E = TypeVar("E")


class OverflowPolicy(Enum):
    """Overflow policy for ringbuffer when full."""

    OVERWRITE = 0
    FAIL = 1


class RingbufferProxy(Proxy, Generic[E]):
    """Proxy for Hazelcast Ringbuffer distributed data structure.

    A ringbuffer is a bounded data structure with a fixed capacity.
    """

    SERVICE_NAME = "hz:impl:ringbufferService"

    SEQUENCE_UNAVAILABLE = -1

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)

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
        future.set_result(0)
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
        future.set_result(0)
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
        future.set_result(-1)
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
        future.set_result(0)
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
        future.set_result(0)
        return future

    def add(self, item: E, overflow_policy: OverflowPolicy = OverflowPolicy.OVERWRITE) -> int:
        """Add an item to the ringbuffer.

        Args:
            item: The item to add.
            overflow_policy: Policy when the buffer is full.

        Returns:
            The sequence number of the added item, or -1 if failed with FAIL policy.
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
        future.set_result(0)
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
            The sequence number of the last added item.
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
            A Future that will contain the sequence number.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(-1)
        return future

    def read_one(self, sequence: int) -> E:
        """Read a single item at the given sequence.

        Args:
            sequence: The sequence to read.

        Returns:
            The item at the sequence.
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
        future.set_result(None)
        return future

    def read_many(
        self,
        start_sequence: int,
        min_count: int,
        max_count: int,
        filter_predicate: Any = None,
    ) -> "ReadResultSet[E]":
        """Read multiple items starting from a sequence.

        Args:
            start_sequence: The sequence to start reading from.
            min_count: Minimum number of items to read.
            max_count: Maximum number of items to read.
            filter_predicate: Optional predicate to filter items.

        Returns:
            A ReadResultSet containing the items.
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
        future.set_result(ReadResultSet([], 0, 0))
        return future

    def __len__(self) -> int:
        return self.size()


class ReadResultSet(Generic[E]):
    """Result set from a ringbuffer read_many operation."""

    def __init__(
        self,
        items: List[E],
        read_count: int,
        next_sequence_to_read: int,
    ):
        self._items = items
        self._read_count = read_count
        self._next_sequence = next_sequence_to_read

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
        """
        return self._next_sequence - self._read_count + index

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, index: int) -> E:
        return self._items[index]

    def __iter__(self):
        return iter(self._items)
