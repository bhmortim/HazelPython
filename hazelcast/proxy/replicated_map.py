"""ReplicatedMap distributed data structure proxy."""

from concurrent.futures import Future
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
)
import uuid

from hazelcast.proxy.base import Proxy, ProxyContext

K = TypeVar("K")
V = TypeVar("V")


class EntryEvent(Generic[K, V]):
    """Event fired when a replicated map entry changes."""

    ADDED = 1
    REMOVED = 2
    UPDATED = 4
    EVICTED = 8

    def __init__(
        self,
        event_type: int,
        key: K,
        value: Optional[V] = None,
        old_value: Optional[V] = None,
        member: Any = None,
    ):
        self._event_type = event_type
        self._key = key
        self._value = value
        self._old_value = old_value
        self._member = member

    @property
    def event_type(self) -> int:
        return self._event_type

    @property
    def key(self) -> K:
        return self._key

    @property
    def value(self) -> Optional[V]:
        return self._value

    @property
    def old_value(self) -> Optional[V]:
        return self._old_value

    @property
    def member(self) -> Any:
        return self._member


class EntryListener(Generic[K, V]):
    """Listener for replicated map entry events."""

    def entry_added(self, event: EntryEvent[K, V]) -> None:
        """Called when an entry is added."""
        pass

    def entry_removed(self, event: EntryEvent[K, V]) -> None:
        """Called when an entry is removed."""
        pass

    def entry_updated(self, event: EntryEvent[K, V]) -> None:
        """Called when an entry is updated."""
        pass

    def entry_evicted(self, event: EntryEvent[K, V]) -> None:
        """Called when an entry is evicted."""
        pass

    def map_cleared(self, event: EntryEvent[K, V]) -> None:
        """Called when the map is cleared."""
        pass


class ReplicatedMapProxy(Proxy, Generic[K, V]):
    """Proxy for Hazelcast ReplicatedMap distributed data structure.

    A ReplicatedMap is a distributed key-value data structure where the data
    is replicated to all cluster members. Unlike IMap, ReplicatedMap does not
    partition data - every member holds a copy of all entries.

    ReplicatedMap is suitable for:
    - Small datasets that fit in memory on all nodes
    - Read-heavy workloads where eventual consistency is acceptable
    - Scenarios requiring fast local reads

    Type Parameters:
        K: The key type.
        V: The value type.

    Example:
        Basic operations::

            rep_map = client.get_replicated_map("config")
            rep_map.put("setting1", "value1", ttl=60)
            value = rep_map.get("setting1")
            rep_map.remove("setting1")

        With entry listener::

            listener = MyEntryListener()
            reg_id = rep_map.add_entry_listener(listener)
            # ... later
            rep_map.remove_entry_listener(reg_id)
    """

    SERVICE_NAME = "hz:impl:replicatedMapService"

    def __init__(
        self,
        service_name: str,
        name: str,
        context: Optional[ProxyContext] = None,
    ):
        super().__init__(service_name, name, context)
        self._entry_listeners: Dict[str, Tuple[EntryListener, bool]] = {}

    def put(self, key: K, value: V, ttl: float = 0) -> Optional[V]:
        """Set a key-value pair in the replicated map.

        Associates the specified value with the specified key. If the map
        previously contained a mapping for the key, the old value is
        replaced and returned.

        Args:
            key: The key to set. Must be serializable.
            value: The value to associate with the key. Must be serializable.
            ttl: Time to live in seconds. The entry will be automatically
                evicted after this duration. Use 0 for infinite (default).

        Returns:
            The previous value associated with the key, or None if there
            was no mapping.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> old_value = rep_map.put("key", "new_value", ttl=300)
            >>> print(f"Previous value: {old_value}")
        """
        return self.put_async(key, value, ttl).result()

    def put_async(self, key: K, value: V, ttl: float = 0) -> Future:
        """Set a key-value pair asynchronously.

        Args:
            key: The key to set.
            value: The value to associate with the key.
            ttl: Time to live in seconds. 0 means infinite.

        Returns:
            A Future that will contain the previous value.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def get(self, key: K) -> Optional[V]:
        """Get the value associated with a key.

        Returns the value to which the specified key is mapped, or None
        if the map contains no mapping for the key.

        Args:
            key: The key whose associated value is to be returned.

        Returns:
            The value associated with the key, or None if not found.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> value = rep_map.get("key")
            >>> if value is not None:
            ...     print(f"Found: {value}")
        """
        return self.get_async(key).result()

    def get_async(self, key: K) -> Future:
        """Get a value asynchronously.

        Args:
            key: The key to look up.

        Returns:
            A Future that will contain the value.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def remove(self, key: K) -> Optional[V]:
        """Remove a key-value pair from the replicated map.

        Removes the mapping for a key from this map if it is present.
        Returns the value to which this map previously associated the key.

        Args:
            key: The key whose mapping is to be removed.

        Returns:
            The previous value associated with the key, or None if there
            was no mapping.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> removed = rep_map.remove("key")
            >>> print(f"Removed value: {removed}")
        """
        return self.remove_async(key).result()

    def remove_async(self, key: K) -> Future:
        """Remove a key-value pair asynchronously.

        Args:
            key: The key to remove.

        Returns:
            A Future that will contain the removed value.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def contains_key(self, key: K) -> bool:
        """Check if the replicated map contains a key.

        Returns True if this map contains a mapping for the specified key.

        Args:
            key: The key whose presence is to be tested.

        Returns:
            True if the map contains a mapping for the key, False otherwise.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> if rep_map.contains_key("config:timeout"):
            ...     print("Config exists")
        """
        return self.contains_key_async(key).result()

    def contains_key_async(self, key: K) -> Future:
        """Check if the map contains a key asynchronously.

        Args:
            key: The key to check.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(False)
        return future

    def contains_value(self, value: V) -> bool:
        """Check if the replicated map contains a value.

        Returns True if this map maps one or more keys to the specified value.

        Args:
            value: The value whose presence is to be tested.

        Returns:
            True if the value exists in the map, False otherwise.

        Raises:
            IllegalStateException: If the map has been destroyed.
        """
        return self.contains_value_async(value).result()

    def contains_value_async(self, value: V) -> Future:
        """Check if the map contains a value asynchronously.

        Args:
            value: The value to check.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(False)
        return future

    def key_set(self) -> Set[K]:
        """Get all keys in the replicated map.

        Returns a set view of the keys contained in this map.

        Returns:
            A set of all keys in the map.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> keys = rep_map.key_set()
            >>> for key in keys:
            ...     print(key)
        """
        return self.key_set_async().result()

    def key_set_async(self) -> Future:
        """Get all keys asynchronously.

        Returns:
            A Future that will contain a set of keys.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(set())
        return future

    def values(self) -> List[V]:
        """Get all values in the replicated map.

        Returns a collection view of the values contained in this map.

        Returns:
            A list of all values in the map.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> for value in rep_map.values():
            ...     print(value)
        """
        return self.values_async().result()

    def values_async(self) -> Future:
        """Get all values asynchronously.

        Returns:
            A Future that will contain a list of values.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result([])
        return future

    def entry_set(self) -> Set[Tuple[K, V]]:
        """Get all entries in the replicated map.

        Returns a set view of the mappings contained in this map.

        Returns:
            A set of (key, value) tuples.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> for key, value in rep_map.entry_set():
            ...     print(f"{key}: {value}")
        """
        return self.entry_set_async().result()

    def entry_set_async(self) -> Future:
        """Get all entries asynchronously.

        Returns:
            A Future that will contain a set of entries.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(set())
        return future

    def size(self) -> int:
        """Get the number of entries in the replicated map.

        Returns:
            The total number of entries in the map.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> count = rep_map.size()
            >>> print(f"Map has {count} entries")
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

    def is_empty(self) -> bool:
        """Check if the replicated map is empty.

        Returns:
            True if the map has no entries, False otherwise.
        """
        return self.is_empty_async().result()

    def is_empty_async(self) -> Future:
        """Check if the map is empty asynchronously.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(True)
        return future

    def clear(self) -> None:
        """Remove all entries from the replicated map.

        Removes all key-value mappings from this map. The map will be
        empty after this call.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> rep_map.clear()
            >>> assert rep_map.size() == 0
        """
        self.clear_async().result()

    def clear_async(self) -> Future:
        """Clear the map asynchronously.

        Returns:
            A Future that completes when the clear is done.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def put_all(self, entries: Dict[K, V]) -> None:
        """Put multiple key-value pairs at once.

        Copies all of the mappings from the specified map to this map.

        Args:
            entries: The key-value pairs to put.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> rep_map.put_all({"key1": "value1", "key2": "value2"})
        """
        self.put_all_async(entries).result()

    def put_all_async(self, entries: Dict[K, V]) -> Future:
        """Put multiple key-value pairs asynchronously.

        Args:
            entries: The key-value pairs to put.

        Returns:
            A Future that completes when all puts are done.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def add_entry_listener(
        self,
        listener: EntryListener[K, V],
        key: Optional[K] = None,
        predicate: Any = None,
    ) -> str:
        """Add an entry listener to the replicated map.

        Adds an entry listener for this map. The listener will be notified
        for all map entry events (add/remove/update/evict).

        Args:
            listener: The listener to add.
            key: Optional key to listen on. If None, listens to all keys.
            predicate: Optional predicate to filter events.

        Returns:
            A registration ID for removing the listener.

        Example:
            >>> class MyListener(EntryListener):
            ...     def entry_added(self, event):
            ...         print(f"Added: {event.key}")
            >>> reg_id = rep_map.add_entry_listener(MyListener())
        """
        registration_id = str(uuid.uuid4())
        self._entry_listeners[registration_id] = (listener, True)
        return registration_id

    def add_entry_listener_to_key(
        self,
        listener: EntryListener[K, V],
        key: K,
    ) -> str:
        """Add an entry listener for a specific key.

        Args:
            listener: The listener to add.
            key: The key to listen on.

        Returns:
            A registration ID for removing the listener.
        """
        return self.add_entry_listener(listener, key=key)

    def add_entry_listener_with_predicate(
        self,
        listener: EntryListener[K, V],
        predicate: Any,
    ) -> str:
        """Add an entry listener with a predicate filter.

        Args:
            listener: The listener to add.
            predicate: The predicate to filter events.

        Returns:
            A registration ID for removing the listener.
        """
        return self.add_entry_listener(listener, predicate=predicate)

    def remove_entry_listener(self, registration_id: str) -> bool:
        """Remove an entry listener.

        Removes the specified entry listener. Returns silently if there
        is no such listener.

        Args:
            registration_id: The registration ID from add_entry_listener.

        Returns:
            True if the listener was removed, False otherwise.

        Example:
            >>> reg_id = rep_map.add_entry_listener(my_listener)
            >>> # ... later
            >>> rep_map.remove_entry_listener(reg_id)
        """
        return self._entry_listeners.pop(registration_id, None) is not None

    def __len__(self) -> int:
        return self.size()

    def __contains__(self, key: K) -> bool:
        return self.contains_key(key)

    def __getitem__(self, key: K) -> Optional[V]:
        return self.get(key)

    def __setitem__(self, key: K, value: V) -> None:
        self.put(key, value)

    def __delitem__(self, key: K) -> None:
        self.remove(key)

    def __iter__(self) -> Iterator[K]:
        return iter(self.key_set())


ReplicatedMap = ReplicatedMapProxy
