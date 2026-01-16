"""Query cache for continuous queries on Hazelcast maps."""

import threading
import uuid as uuid_module
from collections import OrderedDict
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from hazelcast.proxy.map import MapProxy
    from hazelcast.config import QueryCacheConfig

K = TypeVar("K")
V = TypeVar("V")


class QueryCacheEventType(Enum):
    """Type of query cache event."""

    ENTRY_ADDED = "ENTRY_ADDED"
    ENTRY_REMOVED = "ENTRY_REMOVED"
    ENTRY_UPDATED = "ENTRY_UPDATED"


class QueryCacheEvent(Generic[K, V]):
    """Event fired when query cache contents change.

    Attributes:
        event_type: The type of event.
        key: The key affected.
        value: The new value (for add/update events).
        old_value: The previous value (for update/remove events).
    """

    def __init__(
        self,
        event_type: QueryCacheEventType,
        key: K,
        value: Optional[V] = None,
        old_value: Optional[V] = None,
    ):
        self._event_type = event_type
        self._key = key
        self._value = value
        self._old_value = old_value

    @property
    def event_type(self) -> QueryCacheEventType:
        """Get the event type."""
        return self._event_type

    @property
    def key(self) -> K:
        """Get the affected key."""
        return self._key

    @property
    def value(self) -> Optional[V]:
        """Get the new value."""
        return self._value

    @property
    def old_value(self) -> Optional[V]:
        """Get the previous value."""
        return self._old_value

    def __repr__(self) -> str:
        return (
            f"QueryCacheEvent(type={self._event_type.value}, "
            f"key={self._key!r})"
        )


class QueryCacheListener(Generic[K, V]):
    """Listener for query cache events.

    Implement this class to receive notifications when the
    query cache contents change.

    Example:
        >>> class MyListener(QueryCacheListener):
        ...     def entry_added(self, event):
        ...         print(f"Added: {event.key}")
        ...
        >>> query_cache.add_entry_listener(MyListener())
    """

    def entry_added(self, event: QueryCacheEvent[K, V]) -> None:
        """Called when an entry is added to the query cache."""
        pass

    def entry_removed(self, event: QueryCacheEvent[K, V]) -> None:
        """Called when an entry is removed from the query cache."""
        pass

    def entry_updated(self, event: QueryCacheEvent[K, V]) -> None:
        """Called when an entry is updated in the query cache."""
        pass


class LocalIndex(Generic[K, V]):
    """Local index for fast attribute-based lookups in query cache.

    Indexes allow O(1) lookup of entries by attribute value instead
    of scanning all entries.

    Attributes:
        attribute: The attribute name being indexed.
    """

    def __init__(self, attribute: str):
        self._attribute = attribute
        self._index: Dict[Any, Set[K]] = {}
        self._lock = threading.RLock()

    @property
    def attribute(self) -> str:
        """Get the indexed attribute name."""
        return self._attribute

    def add(self, key: K, value: V) -> None:
        """Add an entry to the index."""
        attr_value = self._get_attribute_value(value)
        if attr_value is None:
            return

        with self._lock:
            if attr_value not in self._index:
                self._index[attr_value] = set()
            self._index[attr_value].add(key)

    def remove(self, key: K, value: V) -> None:
        """Remove an entry from the index."""
        attr_value = self._get_attribute_value(value)
        if attr_value is None:
            return

        with self._lock:
            if attr_value in self._index:
                self._index[attr_value].discard(key)
                if not self._index[attr_value]:
                    del self._index[attr_value]

    def update(self, key: K, old_value: V, new_value: V) -> None:
        """Update an entry in the index."""
        self.remove(key, old_value)
        self.add(key, new_value)

    def get_keys(self, attr_value: Any) -> Set[K]:
        """Get all keys with the given attribute value."""
        with self._lock:
            return self._index.get(attr_value, set()).copy()

    def clear(self) -> None:
        """Clear the index."""
        with self._lock:
            self._index.clear()

    def _get_attribute_value(self, value: V) -> Any:
        """Extract the attribute value from an entry value."""
        if value is None:
            return None

        if isinstance(value, dict):
            return value.get(self._attribute)

        return getattr(value, self._attribute, None)


class QueryCache(Generic[K, V]):
    """A continuous query cache for a Hazelcast map.

    Query cache maintains a local copy of map entries that match a
    predicate. The cache is automatically updated when the underlying
    map changes, providing a consistent local view of filtered data.

    Features:
        - Predicate-based filtering of map entries
        - Local indexing for fast attribute lookups
        - Automatic synchronization with the source map
        - Configurable eviction policies
        - Event listeners for cache changes

    Attributes:
        name: The name of this query cache.
        predicate: The predicate used to filter entries.

    Example:
        Create a query cache for active users::

            from hazelcast.config import QueryCacheConfig

            config = QueryCacheConfig(name="active-users")
            config.predicate = "active = true"
            config.include_value = True

            query_cache = my_map.get_query_cache("active-users", config)

            # Local operations (no network calls)
            active_users = query_cache.values()
            count = len(query_cache)

            # Add local index for fast lookups
            query_cache.add_index("department")
            engineering = query_cache.get_by_index("department", "Engineering")
    """

    def __init__(
        self,
        name: str,
        map_proxy: "MapProxy[K, V]",
        config: "QueryCacheConfig",
        predicate: Optional[Any] = None,
    ):
        """Initialize the query cache.

        Args:
            name: Name of the query cache.
            map_proxy: The parent map proxy.
            config: Query cache configuration.
            predicate: Optional predicate for filtering entries.
        """
        self._name = name
        self._map_proxy = map_proxy
        self._config = config
        self._predicate = predicate

        self._entries: OrderedDict[K, V] = OrderedDict()
        self._lock = threading.RLock()

        self._indexes: Dict[str, LocalIndex[K, V]] = {}
        self._listeners: Dict[str, QueryCacheListener[K, V]] = {}
        self._listener_lock = threading.Lock()

        self._destroyed = False
        self._map_listener_id: Optional[str] = None

        self._hits = 0
        self._misses = 0

        if config.populate:
            self._populate()

        self._setup_map_listener()

    @property
    def name(self) -> str:
        """Get the name of this query cache."""
        return self._name

    @property
    def predicate(self) -> Optional[Any]:
        """Get the predicate used to filter entries."""
        return self._predicate

    @property
    def config(self) -> "QueryCacheConfig":
        """Get the query cache configuration."""
        return self._config

    @property
    def is_destroyed(self) -> bool:
        """Check if this query cache has been destroyed."""
        return self._destroyed

    def size(self) -> int:
        """Get the number of entries in the cache.

        Returns:
            The number of cached entries.
        """
        with self._lock:
            return len(self._entries)

    def get(self, key: K) -> Optional[V]:
        """Get an entry from the query cache.

        This is a local operation - no network call is made.

        Args:
            key: The key to look up.

        Returns:
            The value if found in the cache, None otherwise.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> value = query_cache.get("user:123")
            >>> if value:
            ...     print(f"Found: {value}")
        """
        self._check_destroyed()

        with self._lock:
            value = self._entries.get(key)
            if value is not None:
                self._hits += 1
                self._entries.move_to_end(key)
            else:
                self._misses += 1
            return value

    def contains_key(self, key: K) -> bool:
        """Check if the cache contains a key.

        Args:
            key: The key to check.

        Returns:
            True if the key is in the cache.

        Raises:
            IllegalStateException: If the cache has been destroyed.
        """
        self._check_destroyed()

        with self._lock:
            return key in self._entries

    def contains_value(self, value: V) -> bool:
        """Check if the cache contains a value.

        Args:
            value: The value to check.

        Returns:
            True if the value is in the cache.

        Raises:
            IllegalStateException: If the cache has been destroyed.
        """
        self._check_destroyed()

        with self._lock:
            return value in self._entries.values()

    def is_empty(self) -> bool:
        """Check if the cache is empty.

        Returns:
            True if the cache has no entries.
        """
        return self.size() == 0

    def key_set(self) -> Set[K]:
        """Get all keys in the cache.

        Returns:
            A set of all keys.

        Raises:
            IllegalStateException: If the cache has been destroyed.
        """
        self._check_destroyed()

        with self._lock:
            return set(self._entries.keys())

    def values(self) -> List[V]:
        """Get all values in the cache.

        Returns:
            A list of all values.

        Raises:
            IllegalStateException: If the cache has been destroyed.
        """
        self._check_destroyed()

        with self._lock:
            return list(self._entries.values())

    def entry_set(self) -> Set[Tuple[K, V]]:
        """Get all entries in the cache.

        Returns:
            A set of (key, value) tuples.

        Raises:
            IllegalStateException: If the cache has been destroyed.
        """
        self._check_destroyed()

        with self._lock:
            return set(self._entries.items())

    def get_all(self, keys: Set[K]) -> Dict[K, V]:
        """Get multiple entries from the cache.

        Args:
            keys: The keys to look up.

        Returns:
            A dictionary of found key-value pairs.

        Raises:
            IllegalStateException: If the cache has been destroyed.
        """
        self._check_destroyed()

        result = {}
        with self._lock:
            for key in keys:
                if key in self._entries:
                    result[key] = self._entries[key]
                    self._hits += 1
                    self._entries.move_to_end(key)
                else:
                    self._misses += 1
        return result

    def try_recover(self) -> bool:
        """Attempt to recover the query cache by repopulating from the map.

        This is useful if the cache may have become inconsistent due to
        network issues or missed events.

        Returns:
            True if recovery was successful, False otherwise.

        Raises:
            IllegalStateException: If the cache has been destroyed.
        """
        self._check_destroyed()

        try:
            self._populate()
            return True
        except Exception:
            return False

    def add_index(self, attribute: str) -> None:
        """Add a local index on an attribute for faster lookups.

        Indexes enable O(1) lookup of entries by attribute value using
        the get_by_index() method.

        Args:
            attribute: The attribute name to index. For dict values,
                this is the key name. For objects, this is the
                attribute name.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> query_cache.add_index("status")
            >>> query_cache.add_index("department")
            >>> # Now lookups by status or department are O(1)
        """
        self._check_destroyed()

        with self._lock:
            if attribute in self._indexes:
                return

            index: LocalIndex[K, V] = LocalIndex(attribute)
            for key, value in self._entries.items():
                index.add(key, value)

            self._indexes[attribute] = index

    def get_by_index(self, attribute: str, value: Any) -> Dict[K, V]:
        """Get entries by indexed attribute value.

        This provides fast O(1) lookup when an index exists on the
        attribute.

        Args:
            attribute: The indexed attribute name.
            value: The attribute value to search for.

        Returns:
            A dictionary of matching entries.

        Raises:
            ValueError: If the attribute is not indexed.
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> query_cache.add_index("status")
            >>> active = query_cache.get_by_index("status", "active")
            >>> for key, user in active.items():
            ...     print(f"{key}: {user}")
        """
        self._check_destroyed()

        with self._lock:
            if attribute not in self._indexes:
                raise ValueError(f"Attribute '{attribute}' is not indexed")

            index = self._indexes[attribute]
            keys = index.get_keys(value)

            return {k: self._entries[k] for k in keys if k in self._entries}

    def add_entry_listener(
        self,
        listener: QueryCacheListener[K, V] = None,
        on_added: Callable[[QueryCacheEvent[K, V]], None] = None,
        on_removed: Callable[[QueryCacheEvent[K, V]], None] = None,
        on_updated: Callable[[QueryCacheEvent[K, V]], None] = None,
    ) -> str:
        """Add a listener for query cache events.

        Args:
            listener: A QueryCacheListener instance, or
            on_added: Callback for entry added events.
            on_removed: Callback for entry removed events.
            on_updated: Callback for entry updated events.

        Returns:
            Registration ID for removing the listener.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> def on_change(event):
            ...     print(f"{event.event_type}: {event.key}")
            ...
            >>> reg_id = query_cache.add_entry_listener(
            ...     on_added=on_change,
            ...     on_removed=on_change,
            ... )
        """
        self._check_destroyed()

        if listener is None:
            listener = _FunctionQueryCacheListener(on_added, on_removed, on_updated)

        listener_id = str(uuid_module.uuid4())

        with self._listener_lock:
            self._listeners[listener_id] = listener

        return listener_id

    def remove_entry_listener(self, registration_id: str) -> bool:
        """Remove a query cache listener.

        Args:
            registration_id: The registration ID from add_entry_listener.

        Returns:
            True if the listener was removed, False if not found.
        """
        with self._listener_lock:
            return self._listeners.pop(registration_id, None) is not None

    def clear(self) -> None:
        """Clear all entries from the query cache.

        Note: This only clears the local cache, not the underlying map.
        The cache will be repopulated as new events arrive.

        Raises:
            IllegalStateException: If the cache has been destroyed.
        """
        self._check_destroyed()

        with self._lock:
            self._entries.clear()
            for index in self._indexes.values():
                index.clear()

    def destroy(self) -> None:
        """Destroy this query cache and release resources.

        After destruction, the query cache cannot be used. Call
        get_query_cache() on the map to create a new instance.
        """
        if self._destroyed:
            return

        self._destroyed = True

        if self._map_listener_id is not None:
            try:
                self._map_proxy.remove_entry_listener(self._map_listener_id)
            except Exception:
                pass
            self._map_listener_id = None

        with self._lock:
            self._entries.clear()
            self._indexes.clear()

        with self._listener_lock:
            self._listeners.clear()

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about this query cache.

        Returns:
            A dictionary containing:
                - name: Cache name
                - size: Number of entries
                - hits: Number of cache hits
                - misses: Number of cache misses
                - hit_rate: Ratio of hits to total requests
                - index_count: Number of local indexes
                - listener_count: Number of registered listeners

        Example:
            >>> stats = query_cache.get_statistics()
            >>> print(f"Hit rate: {stats['hit_rate']:.2%}")
        """
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0.0

            return {
                "name": self._name,
                "size": len(self._entries),
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
                "index_count": len(self._indexes),
                "listener_count": len(self._listeners),
            }

    def _populate(self) -> None:
        """Populate the cache from the underlying map."""
        entries = self._map_proxy.entry_set(self._predicate)

        with self._lock:
            self._entries.clear()
            for key, value in entries:
                self._add_entry_internal(key, value, fire_event=False)

    def _setup_map_listener(self) -> None:
        """Set up the entry listener on the underlying map."""
        from hazelcast.proxy.map import EntryListener

        listener = _QueryCacheMapListener(self)
        self._map_listener_id = self._map_proxy.add_entry_listener(
            listener,
            include_value=self._config.include_value,
            predicate=self._predicate,
        )

    def _add_entry_internal(
        self, key: K, value: V, fire_event: bool = True
    ) -> None:
        """Add an entry to the cache internally."""
        with self._lock:
            old_value = self._entries.get(key)

            if len(self._entries) >= self._config.eviction_max_size:
                self._evict()

            self._entries[key] = value

            for index in self._indexes.values():
                if old_value is not None:
                    index.update(key, old_value, value)
                else:
                    index.add(key, value)

        if fire_event:
            if old_value is not None:
                self._fire_event(
                    QueryCacheEventType.ENTRY_UPDATED, key, value, old_value
                )
            else:
                self._fire_event(QueryCacheEventType.ENTRY_ADDED, key, value)

    def _remove_entry_internal(
        self, key: K, fire_event: bool = True
    ) -> Optional[V]:
        """Remove an entry from the cache internally."""
        with self._lock:
            value = self._entries.pop(key, None)

            if value is not None:
                for index in self._indexes.values():
                    index.remove(key, value)

        if fire_event and value is not None:
            self._fire_event(QueryCacheEventType.ENTRY_REMOVED, key, old_value=value)

        return value

    def _evict(self) -> None:
        """Evict entries according to the eviction policy."""
        from hazelcast.config import EvictionPolicy

        policy = self._config.eviction_policy

        if policy == EvictionPolicy.NONE:
            return

        if policy == EvictionPolicy.LRU:
            if self._entries:
                oldest_key = next(iter(self._entries))
                self._remove_entry_internal(oldest_key, fire_event=True)

        elif policy == EvictionPolicy.LFU:
            if self._entries:
                oldest_key = next(iter(self._entries))
                self._remove_entry_internal(oldest_key, fire_event=True)

        elif policy == EvictionPolicy.RANDOM:
            if self._entries:
                import random

                key = random.choice(list(self._entries.keys()))
                self._remove_entry_internal(key, fire_event=True)

    def _fire_event(
        self,
        event_type: QueryCacheEventType,
        key: K,
        value: Optional[V] = None,
        old_value: Optional[V] = None,
    ) -> None:
        """Fire a query cache event to all listeners."""
        event = QueryCacheEvent(event_type, key, value, old_value)

        with self._listener_lock:
            listeners = list(self._listeners.values())

        for listener in listeners:
            try:
                if event_type == QueryCacheEventType.ENTRY_ADDED:
                    listener.entry_added(event)
                elif event_type == QueryCacheEventType.ENTRY_REMOVED:
                    listener.entry_removed(event)
                elif event_type == QueryCacheEventType.ENTRY_UPDATED:
                    listener.entry_updated(event)
            except Exception:
                pass

    def _check_destroyed(self) -> None:
        """Check if the cache has been destroyed."""
        if self._destroyed:
            from hazelcast.exceptions import IllegalStateException

            raise IllegalStateException("Query cache has been destroyed")

    def __len__(self) -> int:
        return self.size()

    def __contains__(self, key: K) -> bool:
        return self.contains_key(key)

    def __getitem__(self, key: K) -> Optional[V]:
        return self.get(key)

    def __iter__(self) -> Iterator[K]:
        return iter(self.key_set())

    def __repr__(self) -> str:
        return f"QueryCache(name={self._name!r}, size={self.size()})"


class _FunctionQueryCacheListener(QueryCacheListener[K, V]):
    """Query cache listener that delegates to callback functions."""

    def __init__(
        self,
        on_added: Callable[[QueryCacheEvent], None] = None,
        on_removed: Callable[[QueryCacheEvent], None] = None,
        on_updated: Callable[[QueryCacheEvent], None] = None,
    ):
        self._on_added = on_added
        self._on_removed = on_removed
        self._on_updated = on_updated

    def entry_added(self, event: QueryCacheEvent) -> None:
        if self._on_added:
            self._on_added(event)

    def entry_removed(self, event: QueryCacheEvent) -> None:
        if self._on_removed:
            self._on_removed(event)

    def entry_updated(self, event: QueryCacheEvent) -> None:
        if self._on_updated:
            self._on_updated(event)


class _QueryCacheMapListener:
    """Internal listener for map events to update the query cache."""

    def __init__(self, query_cache: QueryCache):
        self._query_cache = query_cache

    def entry_added(self, event) -> None:
        if event.value is not None:
            self._query_cache._add_entry_internal(event.key, event.value)

    def entry_removed(self, event) -> None:
        self._query_cache._remove_entry_internal(event.key)

    def entry_updated(self, event) -> None:
        if event.value is not None:
            self._query_cache._add_entry_internal(event.key, event.value)
        else:
            self._query_cache._remove_entry_internal(event.key)

    def entry_evicted(self, event) -> None:
        self._query_cache._remove_entry_internal(event.key)

    def entry_expired(self, event) -> None:
        self._query_cache._remove_entry_internal(event.key)

    def map_evicted(self, event) -> None:
        self._query_cache.clear()

    def map_cleared(self, event) -> None:
        self._query_cache.clear()
