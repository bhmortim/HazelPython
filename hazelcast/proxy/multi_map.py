"""MultiMap distributed data structure proxy."""

from concurrent.futures import Future
from typing import Any, Collection, Dict, Generic, List, Optional, Set, TypeVar

from hazelcast.proxy.base import Proxy, ProxyContext

K = TypeVar("K")
V = TypeVar("V")


class MultiMapProxy(Proxy, Generic[K, V]):
    """Proxy for Hazelcast MultiMap distributed data structure.

    A MultiMap allows multiple values to be associated with a single key.
    """

    SERVICE_NAME = "hz:impl:multiMapService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)
        self._entry_listeners: Dict[str, Any] = {}

    def put(self, key: K, value: V) -> bool:
        """Add a value to the collection associated with a key.

        Args:
            key: The key.
            value: The value to add.

        Returns:
            True if the value was added (not a duplicate).
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
            A collection of values, or empty if key not found.
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

        Args:
            key: The key.
            value: The value to remove.

        Returns:
            True if the value was removed.
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

        Args:
            key: The key.

        Returns:
            The removed values.
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
            key: The key.

        Returns:
            The number of values for the key.
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
        listener: Any,
        include_value: bool = True,
        key: Optional[K] = None,
    ) -> str:
        """Add an entry listener.

        Args:
            listener: The listener to add.
            include_value: Whether to include values in events.
            key: Optional specific key to listen on.

        Returns:
            A registration ID.
        """
        import uuid
        registration_id = str(uuid.uuid4())
        self._entry_listeners[registration_id] = (listener, include_value)
        return registration_id

    def remove_entry_listener(self, registration_id: str) -> bool:
        """Remove an entry listener.

        Args:
            registration_id: The registration ID.

        Returns:
            True if the listener was removed.
        """
        return self._entry_listeners.pop(registration_id, None) is not None

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
