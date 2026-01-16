"""Event Journal support for Hazelcast Map and Cache.

Event journals provide a stream of change events for distributed data
structures. They are useful for CDC (Change Data Capture) patterns and
event-driven architectures.

Example:
    Reading from a map's event journal::

        reader = my_map.get_event_journal_reader()
        state = reader.subscribe()
        
        # Read events from the oldest available sequence
        result = reader.read_many(
            start_sequence=state.oldest_sequence,
            min_size=1,
            max_size=100
        )
        
        for event in result:
            if event.is_added:
                print(f"Added: {event.key} = {event.new_value}")
            elif event.is_updated:
                print(f"Updated: {event.key}: {event.old_value} -> {event.new_value}")
            elif event.is_removed:
                print(f"Removed: {event.key}")
"""

from concurrent.futures import Future
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
    from hazelcast.proxy.map import MapProxy
    from hazelcast.proxy.cache import Cache
    from hazelcast.protocol.client_message import ClientMessage

K = TypeVar("K")
V = TypeVar("V")


class EventType(IntEnum):
    """Type of event in the event journal."""

    ADDED = 1
    REMOVED = 2
    UPDATED = 4
    EVICTED = 8
    EXPIRED = 16
    MERGED = 32
    LOADED = 64


class EventJournalEvent(Generic[K, V]):
    """A single event from an event journal.

    Represents a change that occurred to an entry in a Map or Cache.

    Attributes:
        event_type: The type of event (ADDED, REMOVED, UPDATED, etc.).
        key: The key of the affected entry.
        new_value: The new value after the event (None for removals).
        old_value: The old value before the event (None for additions).
        sequence: The sequence number of this event in the journal.

    Example:
        >>> for event in result:
        ...     if event.is_added:
        ...         print(f"New entry: {event.key} = {event.new_value}")
        ...     elif event.is_updated:
        ...         print(f"Changed: {event.old_value} -> {event.new_value}")
    """

    def __init__(
        self,
        event_type: int,
        key: K,
        new_value: Optional[V] = None,
        old_value: Optional[V] = None,
        sequence: int = 0,
    ):
        self._event_type = event_type
        self._key = key
        self._new_value = new_value
        self._old_value = old_value
        self._sequence = sequence

    @property
    def event_type(self) -> int:
        """Get the event type."""
        return self._event_type

    @property
    def key(self) -> K:
        """Get the key of the affected entry."""
        return self._key

    @property
    def new_value(self) -> Optional[V]:
        """Get the new value after the event."""
        return self._new_value

    @property
    def old_value(self) -> Optional[V]:
        """Get the old value before the event."""
        return self._old_value

    @property
    def sequence(self) -> int:
        """Get the sequence number of this event."""
        return self._sequence

    @property
    def is_added(self) -> bool:
        """Check if this is an addition event."""
        return self._event_type == EventType.ADDED

    @property
    def is_removed(self) -> bool:
        """Check if this is a removal event."""
        return self._event_type == EventType.REMOVED

    @property
    def is_updated(self) -> bool:
        """Check if this is an update event."""
        return self._event_type == EventType.UPDATED

    @property
    def is_evicted(self) -> bool:
        """Check if this is an eviction event."""
        return self._event_type == EventType.EVICTED

    @property
    def is_expired(self) -> bool:
        """Check if this is an expiration event."""
        return self._event_type == EventType.EXPIRED

    def __repr__(self) -> str:
        type_name = EventType(self._event_type).name if self._event_type in EventType._value2member_map_ else str(self._event_type)
        return (
            f"EventJournalEvent(type={type_name}, key={self._key!r}, "
            f"new_value={self._new_value!r}, old_value={self._old_value!r}, "
            f"sequence={self._sequence})"
        )


class EventJournalState:
    """State information about an event journal subscription.

    Contains sequence information needed to read from the journal.

    Attributes:
        oldest_sequence: The oldest available sequence in the journal.
        newest_sequence: The newest sequence in the journal.
    """

    def __init__(self, oldest_sequence: int, newest_sequence: int):
        self._oldest_sequence = oldest_sequence
        self._newest_sequence = newest_sequence

    @property
    def oldest_sequence(self) -> int:
        """Get the oldest available sequence number."""
        return self._oldest_sequence

    @property
    def newest_sequence(self) -> int:
        """Get the newest sequence number."""
        return self._newest_sequence

    @property
    def is_empty(self) -> bool:
        """Check if the journal is empty."""
        return self._newest_sequence < self._oldest_sequence

    def __repr__(self) -> str:
        return f"EventJournalState(oldest={self._oldest_sequence}, newest={self._newest_sequence})"


class ReadResultSet(Generic[K, V]):
    """Result set from reading an event journal.

    Contains the events read and metadata for pagination.

    Attributes:
        events: List of events read from the journal.
        read_count: Number of events read.
        next_sequence: The next sequence number to read from.

    Example:
        >>> result = reader.read_many(start_sequence=0, max_size=100)
        >>> for event in result:
        ...     process(event)
        >>> # Continue reading from where we left off
        >>> result = reader.read_many(start_sequence=result.next_sequence, max_size=100)
    """

    def __init__(
        self,
        events: List[EventJournalEvent[K, V]],
        read_count: int,
        next_sequence: int,
    ):
        self._events = events
        self._read_count = read_count
        self._next_sequence = next_sequence

    @property
    def events(self) -> List[EventJournalEvent[K, V]]:
        """Get the list of events."""
        return self._events

    @property
    def read_count(self) -> int:
        """Get the number of events read."""
        return self._read_count

    @property
    def next_sequence(self) -> int:
        """Get the next sequence to read from."""
        return self._next_sequence

    def __len__(self) -> int:
        return len(self._events)

    def __iter__(self) -> Iterator[EventJournalEvent[K, V]]:
        return iter(self._events)

    def __getitem__(self, index: int) -> EventJournalEvent[K, V]:
        return self._events[index]

    def __repr__(self) -> str:
        return f"ReadResultSet(count={self._read_count}, next_seq={self._next_sequence})"


class EventJournalReader(Generic[K, V]):
    """Reader for event journals on Map and Cache data structures.

    Provides methods to subscribe to and read events from an event journal.
    The event journal must be enabled in the Hazelcast server configuration.

    Type Parameters:
        K: The key type of the data structure.
        V: The value type of the data structure.

    Example:
        >>> reader = my_map.get_event_journal_reader()
        >>> state = reader.subscribe()
        >>> print(f"Journal has sequences {state.oldest_sequence} to {state.newest_sequence}")
        >>> 
        >>> result = reader.read_many(
        ...     start_sequence=state.oldest_sequence,
        ...     min_size=1,
        ...     max_size=100
        ... )
        >>> 
        >>> for event in result:
        ...     print(f"{event.event_type}: {event.key}")
    """

    def __init__(self, proxy: Any):
        """Initialize the event journal reader.

        Args:
            proxy: The Map or Cache proxy to read events from.
        """
        self._proxy = proxy
        self._is_cache = hasattr(proxy, 'config') and hasattr(proxy.config, 'key_type')

    def subscribe(self) -> EventJournalState:
        """Subscribe to the event journal and get initial state.

        Returns the oldest and newest sequence numbers available in
        the journal. Use these to determine where to start reading.

        Returns:
            EventJournalState with sequence information.

        Raises:
            IllegalStateException: If the proxy has been destroyed.

        Example:
            >>> state = reader.subscribe()
            >>> if not state.is_empty:
            ...     print(f"Events available from {state.oldest_sequence}")
        """
        return self.subscribe_async().result()

    def subscribe_async(self) -> Future:
        """Subscribe to the event journal asynchronously.

        Returns:
            A Future that will contain the EventJournalState.
        """
        self._proxy._check_not_destroyed()

        if self._is_cache:
            from hazelcast.protocol.codec import CacheEventJournalCodec
            request = CacheEventJournalCodec.encode_subscribe_request(self._proxy._name)

            def handle_response(response: "ClientMessage") -> EventJournalState:
                oldest, newest = CacheEventJournalCodec.decode_subscribe_response(response)
                return EventJournalState(oldest, newest)
        else:
            from hazelcast.protocol.codec import MapEventJournalCodec
            request = MapEventJournalCodec.encode_subscribe_request(self._proxy._name)

            def handle_response(response: "ClientMessage") -> EventJournalState:
                oldest, newest = MapEventJournalCodec.decode_subscribe_response(response)
                return EventJournalState(oldest, newest)

        return self._proxy._invoke(request, handle_response)

    def read_many(
        self,
        start_sequence: int,
        min_size: int = 1,
        max_size: int = 100,
        filter_predicate: Optional[Any] = None,
    ) -> ReadResultSet[K, V]:
        """Read multiple events from the event journal.

        Reads up to max_size events starting from the given sequence.
        The call will block until at least min_size events are available
        or a timeout occurs.

        Args:
            start_sequence: The sequence number to start reading from.
            min_size: Minimum number of events to read (1-1000).
            max_size: Maximum number of events to read (1-1000).
            filter_predicate: Optional predicate to filter events.

        Returns:
            A ReadResultSet containing the events and next sequence.

        Raises:
            ValueError: If min_size > max_size or values out of range.
            IllegalStateException: If the proxy has been destroyed.

        Example:
            >>> result = reader.read_many(
            ...     start_sequence=0,
            ...     min_size=1,
            ...     max_size=100
            ... )
            >>> for event in result:
            ...     print(event)
            >>> # Continue from next sequence
            >>> next_result = reader.read_many(result.next_sequence)
        """
        return self.read_many_async(start_sequence, min_size, max_size, filter_predicate).result()

    def read_many_async(
        self,
        start_sequence: int,
        min_size: int = 1,
        max_size: int = 100,
        filter_predicate: Optional[Any] = None,
    ) -> Future:
        """Read multiple events asynchronously.

        Args:
            start_sequence: The sequence number to start reading from.
            min_size: Minimum number of events to read.
            max_size: Maximum number of events to read.
            filter_predicate: Optional predicate to filter events.

        Returns:
            A Future that will contain a ReadResultSet.
        """
        self._proxy._check_not_destroyed()

        if min_size < 0:
            raise ValueError("min_size must be non-negative")
        if max_size < min_size:
            raise ValueError("max_size must be >= min_size")
        if max_size > 1000:
            raise ValueError("max_size must be <= 1000")

        filter_data = None
        if filter_predicate is not None:
            filter_data = self._proxy._to_data(filter_predicate)

        if self._is_cache:
            from hazelcast.protocol.codec import CacheEventJournalCodec
            request = CacheEventJournalCodec.encode_read_request(
                self._proxy._name, start_sequence, min_size, max_size, filter_data
            )

            def handle_response(response: "ClientMessage") -> ReadResultSet[K, V]:
                read_count, next_seq, events_data, sequences = CacheEventJournalCodec.decode_read_response(response)
                events = self._deserialize_events(events_data, sequences)
                return ReadResultSet(events, read_count, next_seq)
        else:
            from hazelcast.protocol.codec import MapEventJournalCodec
            request = MapEventJournalCodec.encode_read_request(
                self._proxy._name, start_sequence, min_size, max_size, filter_data
            )

            def handle_response(response: "ClientMessage") -> ReadResultSet[K, V]:
                read_count, next_seq, events_data, sequences = MapEventJournalCodec.decode_read_response(response)
                events = self._deserialize_events(events_data, sequences)
                return ReadResultSet(events, read_count, next_seq)

        return self._proxy._invoke(request, handle_response)

    def read_from_event_journal_async(
        self,
        start_sequence: int,
        min_size: int = 1,
        max_size: int = 100,
    ) -> Future:
        """Read from event journal asynchronously (compatibility method).

        This method provides compatibility with the MapProxy interface.

        Args:
            start_sequence: The sequence number to start reading from.
            min_size: Minimum number of events to read.
            max_size: Maximum number of events to read.

        Returns:
            A Future that will contain a ReadResultSet.
        """
        return self.read_many_async(start_sequence, min_size, max_size)

    def _deserialize_events(
        self,
        events_data: List[tuple],
        sequences: List[int],
    ) -> List[EventJournalEvent[K, V]]:
        """Deserialize event data into EventJournalEvent objects."""
        events = []
        for i, (event_type, key_data, value_data, old_value_data) in enumerate(events_data):
            key = self._proxy._to_object(key_data) if key_data else None
            new_value = self._proxy._to_object(value_data) if value_data else None
            old_value = self._proxy._to_object(old_value_data) if old_value_data else None
            sequence = sequences[i] if i < len(sequences) else 0

            event = EventJournalEvent(
                event_type=event_type,
                key=key,
                new_value=new_value,
                old_value=old_value,
                sequence=sequence,
            )
            events.append(event)
        return events
