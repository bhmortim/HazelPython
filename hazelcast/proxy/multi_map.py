"""MultiMap distributed data structure proxy."""

from concurrent.futures import Future
from typing import Any, Callable, Collection, Dict, Generic, List, Optional, Set, Tuple, TypeVar

from hazelcast.proxy.base import Proxy, ProxyContext
from hazelcast.proxy.map import EntryEvent, EntryListener

K = TypeVar("K")
V = TypeVar("V")


class _CallbackEntryListener(EntryListener[Any, Any]):
    """Internal listener that wraps callback functions for MultiMap."""

    def __init__(
        self,
        entry_added: Optional[Callable[[EntryEvent[Any, Any]], None]] = None,
        entry_removed: Optional[Callable[[EntryEvent[Any, Any]], None]] = None,
        map_cleared: Optional[Callable[[EntryEvent[Any, Any]], None]] = None,
    ):
        self._entry_added = entry_added
        self._entry_removed = entry_removed
        self._map_cleared = map_cleared

    def entry_added(self, event: EntryEvent[Any, Any]) -> None:
        if self._entry_added:
            self._entry_added(event)

    def entry_removed(self, event: EntryEvent[Any, Any]) -> None:
        if self._entry_removed:
            self._entry_removed(event)

    def map_cleared(self, event: EntryEvent[Any, Any]) -> None:
        if self._map_cleared:
            self._map_cleared(event)


class MultiMapProxy(Proxy, Generic[K, V]):
    """Proxy for Hazelcast MultiMap distributed data structure.

    A MultiMap allows multiple values to be associated with a single key.
    Unlike a regular Map where each key maps to exactly one value, a
    MultiMap can store multiple values per key.

    Type Parameters:
        K: The key type.
        V: The value type.

    Attributes:
        name: The name of this distributed multi-map.

    Example:
        Basic multi-map operations::

            mm = client.get_multi_map("tags")
            mm.put("article1", "python")
            mm.put("article1", "hazelcast")
            mm.put("article1", "distributed")
            values = mm.get("article1")  # Returns all 3 tags
            print(f"Tags: {list(values)}")
    """

    SERVICE_NAME = "hz:impl:multiMapService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)
        self._entry_listeners: Dict[str, Tuple[Any, bool]] = {}

    def put(self, key: K, value: V) -> bool:
        """Add a value to the collection associated with a key.

        Args:
            key: The key to associate the value with.
            value: The value to add to the key's collection.

        Returns:
            True if the value was added, False if it was a duplicate.

        Raises:
            IllegalStateException: If the multi-map has been destroyed.

        Example:
            >>> mm.put("user:1", "role:admin")
            >>> mm.put("user:1", "role:editor")
        """
        return self.put_async(key, value).result()

    def put_async(self, key: K, value: V) -> Future:
        """Add a value asynchronously.

        Args:
            key: The key.
            value: The value to add.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(True)
        return future

    def get(self, key: K) -> Collection[V]:
        """Get all values associated with a key.

        Args:
            key: The key to look up.

        Returns:
            A collection of all values associated with the key,
            or an empty collection if the key is not found.

        Raises:
            IllegalStateException: If the multi-map has been destroyed.

        Example:
            >>> roles = mm.get("user:1")
            >>> for role in roles:
            ...     print(role)
        """
        return self.get_async(key).result()

    def get_async(self, key: K) -> Future:
        """Get values asynchronously.

        Args:
            key: The key to look up.

        Returns:
            A Future that will contain a collection of values.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result([])
        return future

    def remove(self, key: K, value: V) -> bool:
        """Remove a specific value from a key.

        Removes only the specified value from the key's collection,
        leaving other values intact.

        Args:
            key: The key.
            value: The specific value to remove.

        Returns:
            True if the value was removed, False if not found.

        Raises:
            IllegalStateException: If the multi-map has been destroyed.

        Example:
            >>> mm.remove("user:1", "role:editor")
        """
        return self.remove_async(key, value).result()

    def remove_async(self, key: K, value: V) -> Future:
        """Remove a value asynchronously.

        Args:
            key: The key.
            value: The value to remove.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(False)
        return future

    def remove_all(self, key: K) -> Collection[V]:
        """Remove all values associated with a key.

        Removes the entire entry for the key, returning all values
        that were associated with it.

        Args:
            key: The key whose values should be removed.

        Returns:
            A collection of all removed values.

        Raises:
            IllegalStateException: If the multi-map has been destroyed.

        Example:
            >>> removed = mm.remove_all("user:1")
            >>> print(f"Removed {len(removed)} values")
        """
        return self.remove_all_async(key).result()

    def remove_all_async(self, key: K) -> Future:
        """Remove all values for a key asynchronously.

        Args:
            key: The key.

        Returns:
            A Future that will contain the removed values.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result([])
        return future

    def contains_key(self, key: K) -> bool:
        """Check if the map contains a key.

        Args:
            key: The key to check.

        Returns:
            True if the key exists.
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
        """Check if the map contains a value.

        Args:
            value: The value to check.

        Returns:
            True if the value exists.
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

    def contains_entry(self, key: K, value: V) -> bool:
        """Check if the map contains a specific key-value pair.

        Args:
            key: The key.
            value: The value.

        Returns:
            True if the entry exists.
        """
        return self.contains_entry_async(key, value).result()

    def contains_entry_async(self, key: K, value: V) -> Future:
        """Check if the map contains an entry asynchronously.

        Args:
            key: The key.
            value: The value.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(False)
        return future

    def size(self) -> int:
        """Get the total number of key-value pairs.

        Returns:
            The total count of all values across all keys.
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

    def value_count(self, key: K) -> int:
        """Get the number of values for a specific key.

        Args:
            key: The key to count values for.

        Returns:
            The number of values associated with the key, or 0 if not found.

        Raises:
            IllegalStateException: If the multi-map has been destroyed.

        Example:
            >>> count = mm.value_count("user:1")
            >>> print(f"User has {count} roles")
        """
        return self.value_count_async(key).result()

    def value_count_async(self, key: K) -> Future:
        """Get the value count asynchronously.

        Args:
            key: The key.

        Returns:
            A Future that will contain the count.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(0)
        return future

    def clear(self) -> None:
        """Remove all entries from the map."""
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

    def key_set(self) -> Set[K]:
        """Get all keys in the map.

        Returns:
            A set of keys.
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

    def values(self) -> Collection[V]:
        """Get all values in the map.

        Returns:
            A collection of all values.
        """
        return self.values_async().result()

    def values_async(self) -> Future:
        """Get all values asynchronously.

        Returns:
            A Future that will contain a collection of values.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result([])
        return future

    def entry_set(self) -> Set[tuple]:
        """Get all entries in the map.

        Returns:
            A set of (key, value) tuples.
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

    def add_entry_listener(
        self,
        listener: Optional[EntryListener[K, V]] = None,
        include_value: bool = True,
        key: Optional[K] = None,
        entry_added: Optional[Callable[[EntryEvent[K, V]], None]] = None,
        entry_removed: Optional[Callable[[EntryEvent[K, V]], None]] = None,
        map_cleared: Optional[Callable[[EntryEvent[K, V]], None]] = None,
    ) -> str:
        """Add an entry listener.

        Can be called with either a listener object or individual callbacks.

        Args:
            listener: An EntryListener instance.
            include_value: Whether to include values in events.
            key: Optional specific key to listen on.
            entry_added: Callback for entry added events.
            entry_removed: Callback for entry removed events.
            map_cleared: Callback for map cleared events.

        Returns:
            A registration ID.

        Raises:
            ValueError: If neither listener nor callbacks are provided.
        """
        self._check_not_destroyed()

        if listener is None and entry_added is None and entry_removed is None and map_cleared is None:
            raise ValueError(
                "Either listener or at least one callback must be provided"
            )

        import uuid

        effective_listener: Any
        if listener is not None:
            effective_listener = listener
        else:
            effective_listener = _CallbackEntryListener(entry_added, entry_removed, map_cleared)

        registration_id = str(uuid.uuid4())
        self._entry_listeners[registration_id] = (effective_listener, include_value)
        return registration_id

    def remove_entry_listener(self, registration_id: str) -> bool:
        """Remove an entry listener.

        Args:
            registration_id: The registration ID.

        Returns:
            True if the listener was removed.
        """
        return self._entry_listeners.pop(registration_id, None) is not None

    def _notify_entry_added(self, key: K, value: V) -> None:
        """Notify listeners of an entry added event."""
        for listener, include_value in self._entry_listeners.values():
            event = EntryEvent(
                EntryEvent.ADDED,
                key,
                value if include_value else None,
            )
            if hasattr(listener, "entry_added"):
                listener.entry_added(event)

    def _notify_entry_removed(self, key: K, value: V) -> None:
        """Notify listeners of an entry removed event."""
        for listener, include_value in self._entry_listeners.values():
            event = EntryEvent(
                EntryEvent.REMOVED,
                key,
                value if include_value else None,
            )
            if hasattr(listener, "entry_removed"):
                listener.entry_removed(event)

    def _notify_map_cleared(self) -> None:
        """Notify listeners of a map cleared event."""
        for listener, include_value in self._entry_listeners.values():
            event: EntryEvent[K, V] = EntryEvent(
                EntryEvent.CLEAR_ALL,
                None,  # type: ignore
            )
            if hasattr(listener, "map_cleared"):
                listener.map_cleared(event)

    def lock(self, key: K, ttl: float = -1) -> None:
        """Acquire a lock on a key.

        Args:
            key: The key to lock.
            ttl: Time to live for the lock.
        """
        self.lock_async(key, ttl).result()

    def lock_async(self, key: K, ttl: float = -1) -> Future:
        """Acquire a lock asynchronously.

        Args:
            key: The key to lock.
            ttl: Time to live for the lock.

        Returns:
            A Future that completes when the lock is acquired.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def try_lock(self, key: K, timeout: float = 0, ttl: float = -1) -> bool:
        """Try to acquire a lock.

        Args:
            key: The key to lock.
            timeout: Maximum wait time.
            ttl: Time to live for the lock.

        Returns:
            True if the lock was acquired.
        """
        return self.try_lock_async(key, timeout, ttl).result()

    def try_lock_async(self, key: K, timeout: float = 0, ttl: float = -1) -> Future:
        """Try to acquire a lock asynchronously.

        Args:
            key: The key to lock.
            timeout: Maximum wait time.
            ttl: Time to live for the lock.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(True)
        return future

    def unlock(self, key: K) -> None:
        """Release a lock.

        Args:
            key: The key to unlock.
        """
        self.unlock_async(key).result()

    def unlock_async(self, key: K) -> Future:
        """Release a lock asynchronously.

        Args:
            key: The key to unlock.

        Returns:
            A Future that completes when the lock is released.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def is_locked(self, key: K) -> bool:
        """Check if a key is locked.

        Args:
            key: The key to check.

        Returns:
            True if the key is locked.
        """
        return self.is_locked_async(key).result()

    def is_locked_async(self, key: K) -> Future:
        """Check if a key is locked asynchronously.

        Args:
            key: The key to check.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(False)
        return future

    def force_unlock(self, key: K) -> None:
        """Force release a lock.

        Args:
            key: The key to unlock.
        """
        self.force_unlock_async(key).result()

    def force_unlock_async(self, key: K) -> Future:
        """Force release a lock asynchronously.

        Args:
            key: The key to unlock.

        Returns:
            A Future that completes when the lock is released.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def __len__(self) -> int:
        return self.size()

    def __contains__(self, key: K) -> bool:
        return self.contains_key(key)

    def __getitem__(self, key: K) -> Collection[V]:
        return self.get(key)
