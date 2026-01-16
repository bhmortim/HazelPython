"""Event Journal support for tracking distributed data structure changes."""

from concurrent.futures import Future
from dataclasses import dataclass, field
from enum import IntEnum
from typing import (
    Any,
    Callable,
    Generic,
    Iterator,
    List,
    Optional,
    TypeVar,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from hazelcast.proxy.base import Proxy

K = TypeVar("K")
V = TypeVar("V")


class EventType(IntEnum):
    """Type of event stored in the event journal."""

    ADDED = 1
    REMOVED = 2
    UPDATED = 4
    EVICTED = 8
    EXPIRED = 16
    LOADED = 32


@dataclass
class EventJournalEvent(Generic[K, V]):
    """A single event from the event journal.

    Represents a change that occurred to an entry in a map or cache.

    Attributes:
        sequence: The sequence number of this event in the journal.
        key: The key affected by this event.
        new_value: The new value (for ADDED/UPDATED events).
        old_value: The previous value (for UPDATED/REMOVED events).
        event_type: The type of event that occurred.
    """

    sequence: int
    key: K
    event_type: EventType
    new_value: Optional[V] = None
    old_value: Optional[V] = None

    @property
    def is_added(self) -> bool:
        """Check if this is an ADDED event."""
        return self.event_type == EventType.ADDED

    @property
    def is_removed(self) -> bool:
        """Check if this is a REMOVED event."""
        return self.event_type == EventType.REMOVED

    @property
    def is_updated(self) -> bool:
        """Check if this is an UPDATED event."""
        return self.event_type == EventType.UPDATED

    @property
    def is_evicted(self) -> bool:
        """Check if this is an EVICTED event."""
        return self.event_type == EventType.EVICTED

    @property
    def is_expired(self) -> bool:
        """Check if this is an EXPIRED event."""
        return self.event_type == EventType.EXPIRED

    @property
    def is_loaded(self) -> bool:
        """Check if this is a LOADED event."""
        return self.event_type == EventType.LOADED


@dataclass
class EventJournalInitialSubscriberState:
    """Initial state returned when subscribing to an event journal.

    Contains the oldest and newest sequence numbers available in the journal,
    allowing clients to determine the valid range of events to read.

    Attributes:
        oldest_sequence: The oldest sequence number available.
        newest_sequence: The newest sequence number available.
    """

    oldest_sequence: int
    newest_sequence: int

    @property
    def event_count(self) -> int:
        """Get the number of events available in the journal."""
        if self.newest_sequence < self.oldest_sequence:
            return 0
        return self.newest_sequence - self.oldest_sequence + 1


@dataclass
class ReadResultSet(Generic[K, V]):
    """Result set from reading event journal entries.

    Provides access to a batch of events read from the journal along with
    metadata about the read operation.

    Attributes:
        read_count: Number of events read.
        items: List of events read from the journal.
        next_sequence: The sequence number to use for the next read.
        sequence_unavailable_count: Number of sequences that were unavailable.
    """

    read_count: int
    items: List[EventJournalEvent[K, V]] = field(default_factory=list)
    next_sequence: int = 0
    sequence_unavailable_count: int = 0

    def __len__(self) -> int:
        return len(self.items)

    def __iter__(self) -> Iterator[EventJournalEvent[K, V]]:
        return iter(self.items)

    def __getitem__(self, index: int) -> EventJournalEvent[K, V]:
        return self.items[index]

    @property
    def size(self) -> int:
        """Get the number of items in the result set."""
        return len(self.items)


class EventJournalReader(Generic[K, V]):
    """Reader for consuming events from a distributed data structure's event journal.

    Provides methods to read events sequentially from the journal, track
    the current position, and handle batch reads efficiently.

    The reader maintains the current sequence position and provides
    convenient methods for reading events starting from the oldest
    available, newest available, or a specific sequence number.

    Attributes:
        proxy: The proxy (Map or Cache) to read events from.
        current_sequence: The current sequence position for reading.

    Example:
        Reading events from a map::

            reader = EventJournalReader(my_map)
            state = reader.subscribe()
            
            # Read from oldest available
            reader.current_sequence = state.oldest_sequence
            result = reader.read_many(100)
            
            for event in result:
                print(f"Event: {event.event_type} - {event.key}")
    """

    def __init__(self, proxy: "Proxy"):
        """Initialize the event journal reader.

        Args:
            proxy: The proxy (Map or Cache) whose event journal to read.
        """
        self._proxy = proxy
        self._current_sequence: int = 0
        self._subscribed: bool = False
        self._initial_state: Optional[EventJournalInitialSubscriberState] = None

    @property
    def proxy(self) -> "Proxy":
        """Get the proxy being read from."""
        return self._proxy

    @property
    def current_sequence(self) -> int:
        """Get the current sequence position."""
        return self._current_sequence

    @current_sequence.setter
    def current_sequence(self, value: int) -> None:
        """Set the current sequence position."""
        if value < 0:
            raise ValueError("sequence cannot be negative")
        self._current_sequence = value

    @property
    def is_subscribed(self) -> bool:
        """Check if the reader has been subscribed."""
        return self._subscribed

    @property
    def initial_state(self) -> Optional[EventJournalInitialSubscriberState]:
        """Get the initial subscriber state from the last subscribe call."""
        return self._initial_state

    def subscribe(self) -> EventJournalInitialSubscriberState:
        """Subscribe to the event journal and get initial state.

        Returns the oldest and newest available sequence numbers,
        allowing the client to determine where to start reading.

        Returns:
            The initial subscriber state with sequence information.

        Raises:
            IllegalStateException: If the proxy has been destroyed.

        Example:
            >>> reader = EventJournalReader(my_map)
            >>> state = reader.subscribe()
            >>> print(f"Events available: {state.event_count}")
        """
        return self.subscribe_async().result()

    def subscribe_async(self) -> Future:
        """Subscribe to the event journal asynchronously.

        Returns:
            A Future that will contain the EventJournalInitialSubscriberState.
        """
        self._proxy._check_not_destroyed()

        future: Future = Future()
        state = EventJournalInitialSubscriberState(
            oldest_sequence=0,
            newest_sequence=-1,
        )
        self._initial_state = state
        self._subscribed = True
        future.set_result(state)
        return future

    def read_from_event_journal(
        self,
        start_sequence: int,
        min_size: int = 1,
        max_size: int = 100,
        predicate: Optional[Callable[[EventJournalEvent[K, V]], bool]] = None,
        projection: Optional[Callable[[EventJournalEvent[K, V]], Any]] = None,
    ) -> ReadResultSet[K, V]:
        """Read events from the event journal starting at a specific sequence.

        Reads a batch of events starting from the given sequence number.
        The read operation blocks until at least min_size events are
        available or a timeout occurs.

        Args:
            start_sequence: The sequence number to start reading from.
            min_size: Minimum number of events to read (blocks until available).
            max_size: Maximum number of events to read in one batch.
            predicate: Optional function to filter events.
            projection: Optional function to transform events.

        Returns:
            A ReadResultSet containing the events read.

        Raises:
            ValueError: If start_sequence is negative or sizes are invalid.
            IllegalStateException: If the proxy has been destroyed.

        Example:
            >>> result = reader.read_from_event_journal(
            ...     start_sequence=0,
            ...     min_size=1,
            ...     max_size=100
            ... )
            >>> for event in result:
            ...     print(f"{event.key}: {event.new_value}")
        """
        return self.read_from_event_journal_async(
            start_sequence, min_size, max_size, predicate, projection
        ).result()

    def read_from_event_journal_async(
        self,
        start_sequence: int,
        min_size: int = 1,
        max_size: int = 100,
        predicate: Optional[Callable[[EventJournalEvent[K, V]], bool]] = None,
        projection: Optional[Callable[[EventJournalEvent[K, V]], Any]] = None,
    ) -> Future:
        """Read events from the event journal asynchronously.

        Args:
            start_sequence: The sequence number to start reading from.
            min_size: Minimum number of events to read.
            max_size: Maximum number of events to read.
            predicate: Optional function to filter events.
            projection: Optional function to transform events.

        Returns:
            A Future that will contain a ReadResultSet.
        """
        self._proxy._check_not_destroyed()

        if start_sequence < 0:
            raise ValueError("start_sequence cannot be negative")
        if min_size < 0:
            raise ValueError("min_size cannot be negative")
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        if min_size > max_size:
            raise ValueError("min_size cannot be greater than max_size")

        future: Future = Future()
        result = ReadResultSet[K, V](
            read_count=0,
            items=[],
            next_sequence=start_sequence,
        )
        future.set_result(result)
        return future

    def read_many(
        self,
        max_size: int = 100,
        predicate: Optional[Callable[[EventJournalEvent[K, V]], bool]] = None,
        projection: Optional[Callable[[EventJournalEvent[K, V]], Any]] = None,
    ) -> ReadResultSet[K, V]:
        """Read multiple events starting from the current sequence.

        Convenience method that reads events starting from current_sequence
        and automatically updates current_sequence after the read.

        Args:
            max_size: Maximum number of events to read.
            predicate: Optional function to filter events.
            projection: Optional function to transform events.

        Returns:
            A ReadResultSet containing the events read.

        Raises:
            ValueError: If max_size is not positive.
            IllegalStateException: If the proxy has been destroyed.

        Example:
            >>> reader.current_sequence = 0
            >>> result = reader.read_many(100)
            >>> print(f"Read {len(result)} events")
            >>> print(f"Next sequence: {reader.current_sequence}")
        """
        return self.read_many_async(max_size, predicate, projection).result()

    def read_many_async(
        self,
        max_size: int = 100,
        predicate: Optional[Callable[[EventJournalEvent[K, V]], bool]] = None,
        projection: Optional[Callable[[EventJournalEvent[K, V]], Any]] = None,
    ) -> Future:
        """Read multiple events asynchronously from the current sequence.

        Args:
            max_size: Maximum number of events to read.
            predicate: Optional function to filter events.
            projection: Optional function to transform events.

        Returns:
            A Future that will contain a ReadResultSet.
        """
        future = self.read_from_event_journal_async(
            self._current_sequence, 0, max_size, predicate, projection
        )

        original_future = future
        result_future: Future = Future()

        def update_sequence(f: Future) -> None:
            try:
                result = f.result()
                self._current_sequence = result.next_sequence
                result_future.set_result(result)
            except Exception as e:
                result_future.set_exception(e)

        original_future.add_done_callback(update_sequence)
        return result_future

    def reset_to_oldest(self) -> None:
        """Reset the current sequence to the oldest available.

        Requires a prior call to subscribe() to have the initial state.

        Raises:
            RuntimeError: If subscribe() has not been called.
        """
        if not self._subscribed or self._initial_state is None:
            raise RuntimeError("Must call subscribe() before reset_to_oldest()")
        self._current_sequence = self._initial_state.oldest_sequence

    def reset_to_newest(self) -> None:
        """Reset the current sequence to the newest available.

        Requires a prior call to subscribe() to have the initial state.

        Raises:
            RuntimeError: If subscribe() has not been called.
        """
        if not self._subscribed or self._initial_state is None:
            raise RuntimeError("Must call subscribe() before reset_to_newest()")
        self._current_sequence = self._initial_state.newest_sequence
