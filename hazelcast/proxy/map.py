"""Map distributed data structure proxy."""

import threading
import uuid as uuid_module
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
    TYPE_CHECKING,
)

from hazelcast.processor import EntryProcessor
from hazelcast.protocol.codec import MapCodec
from hazelcast.proxy.base import Proxy, ProxyContext
from hazelcast.projection import Projection

if TYPE_CHECKING:
    from hazelcast.near_cache import NearCache
    from hazelcast.config import NearCacheConfig
    from hazelcast.predicate import Predicate
    from hazelcast.aggregator import Aggregator
    from hazelcast.protocol.client_message import ClientMessage

K = TypeVar("K")
V = TypeVar("V")


class EntryEvent(Generic[K, V]):
    """Event fired when a map entry changes."""

    ADDED = 1
    REMOVED = 2
    UPDATED = 4
    EVICTED = 8
    EXPIRED = 16
    EVICT_ALL = 32
    CLEAR_ALL = 64
    MERGED = 128
    INVALIDATION = 256
    LOADED = 512

    def __init__(
        self,
        event_type: int,
        key: K,
        value: Optional[V] = None,
        old_value: Optional[V] = None,
        merging_value: Optional[V] = None,
        member: Any = None,
    ):
        self._event_type = event_type
        self._key = key
        self._value = value
        self._old_value = old_value
        self._merging_value = merging_value
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
    def merging_value(self) -> Optional[V]:
        return self._merging_value

    @property
    def member(self) -> Any:
        return self._member


class EntryListener(Generic[K, V]):
    """Listener for map entry events."""

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

    def entry_expired(self, event: EntryEvent[K, V]) -> None:
        """Called when an entry expires."""
        pass

    def map_evicted(self, event: EntryEvent[K, V]) -> None:
        """Called when the map is evicted."""
        pass

    def map_cleared(self, event: EntryEvent[K, V]) -> None:
        """Called when the map is cleared."""
        pass


class MapProxy(Proxy, Generic[K, V]):
    """Proxy for Hazelcast IMap distributed data structure.

    Provides a distributed, partitioned, and optionally replicated map
    implementation. The map is stored across cluster members and supports
    concurrent access, entry listeners, predicates, aggregations, and
    optional near-cache for reduced latency.

    Type Parameters:
        K: The key type.
        V: The value type.

    Attributes:
        name: The name of this distributed map.
        near_cache: The optional near cache for this map.

    Example:
        Basic operations::

            my_map = client.get_map("users")
            my_map.put("user:1", {"name": "Alice"})
            user = my_map.get("user:1")
            my_map.remove("user:1")

        With entry listener::

            def on_added(event):
                print(f"Added: {event.key}")

            my_map.add_entry_listener(
                listener,
                include_value=True
            )
    """

    SERVICE_NAME = "hz:impl:mapService"

    def __init__(
        self,
        name: str,
        context: Optional[ProxyContext] = None,
        near_cache: Optional["NearCache"] = None,
    ):
        super().__init__(self.SERVICE_NAME, name, context)
        self._entry_listeners: Dict[str, Tuple[EntryListener, bool, Optional[uuid_module.UUID]]] = {}
        self._near_cache: Optional["NearCache"] = near_cache
        self._near_cache_invalidation_listener_id: Optional[str] = None
        self._reference_id_generator = _ReferenceIdGenerator()

    @property
    def near_cache(self) -> Optional["NearCache"]:
        """Get the near cache for this map."""
        return self._near_cache

    def set_near_cache(self, near_cache: "NearCache") -> None:
        """Set the near cache for this map.

        If the near cache config has invalidate_on_change enabled,
        an entry listener will be registered to receive cluster-side
        invalidation events.
        """
        self._near_cache = near_cache
        self._setup_near_cache_invalidation()

    def _setup_near_cache_invalidation(self) -> None:
        """Set up invalidation listener if configured."""
        if self._near_cache is None:
            return

        if not self._near_cache.config.invalidate_on_change:
            return

        if self._near_cache_invalidation_listener_id is not None:
            return

        invalidation_listener = _NearCacheInvalidationListener(self._near_cache)
        self._near_cache_invalidation_listener_id = self.add_entry_listener(
            invalidation_listener,
            include_value=False,
        )

    def _teardown_near_cache_invalidation(self) -> None:
        """Remove invalidation listener if registered."""
        if self._near_cache_invalidation_listener_id is not None:
            self.remove_entry_listener(self._near_cache_invalidation_listener_id)
            self._near_cache_invalidation_listener_id = None

    def _on_destroy(self) -> None:
        """Called when the proxy is destroyed."""
        self._teardown_near_cache_invalidation()
        if self._near_cache is not None:
            self._near_cache.clear()

    def put(self, key: K, value: V, ttl: float = -1) -> Optional[V]:
        """Set a key-value pair in the map.

        Associates the specified value with the specified key. If the map
        previously contained a mapping for the key, the old value is
        replaced and returned.

        Args:
            key: The key to set. Must be serializable.
            value: The value to associate with the key. Must be serializable.
            ttl: Time to live in seconds. The entry will be automatically
                evicted after this duration. Use -1 for infinite (default).

        Returns:
            The previous value associated with the key, or None if there
            was no mapping.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> old_value = my_map.put("key", "new_value")
            >>> print(f"Previous value: {old_value}")
        """
        return self.put_async(key, value, ttl).result()

    def put_async(self, key: K, value: V, ttl: float = -1) -> Future:
        """Set a key-value pair asynchronously.

        Args:
            key: The key to set.
            value: The value to associate with the key.
            ttl: Time to live in seconds. -1 means infinite.

        Returns:
            A Future that will contain the previous value.
        """
        self._check_not_destroyed()

        if self._near_cache is not None:
            self._near_cache.invalidate(key)

        key_data = self._to_data(key)
        value_data = self._to_data(value)
        ttl_millis = int(ttl * 1000) if ttl > 0 else -1

        request = MapCodec.encode_put_request(
            self._name, key_data, value_data, 0, ttl_millis
        )

        def handle_response(response: "ClientMessage") -> Optional[V]:
            data = MapCodec.decode_put_response(response)
            return self._to_object(data) if data else None

        return self._invoke(request, handle_response)

    def get(self, key: K) -> Optional[V]:
        """Get the value associated with a key.

        Returns the value to which the specified key is mapped, or None
        if the map contains no mapping for the key. If near cache is
        enabled, the value may be returned from the local cache.

        Args:
            key: The key whose associated value is to be returned.

        Returns:
            The value associated with the key, or None if not found.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> value = my_map.get("key")
            >>> if value is not None:
            ...     print(f"Found: {value}")
        """
        if self._near_cache is not None:
            cached = self._near_cache.get(key)
            if cached is not None:
                return cached

        value = self.get_async(key).result()

        if self._near_cache is not None and value is not None:
            self._near_cache.put(key, value)

        return value

    def get_async(self, key: K) -> Future:
        """Get a value asynchronously.

        Args:
            key: The key to look up.

        Returns:
            A Future that will contain the value.
        """
        self._check_not_destroyed()

        if self._near_cache is not None:
            cached = self._near_cache.get(key)
            if cached is not None:
                future: Future = Future()
                future.set_result(cached)
                return future

        key_data = self._to_data(key)
        request = MapCodec.encode_get_request(self._name, key_data, 0)

        def handle_response(response: "ClientMessage") -> Optional[V]:
            data = MapCodec.decode_get_response(response)
            value = self._to_object(data) if data else None
            if self._near_cache is not None and value is not None:
                self._near_cache.put(key, value)
            return value

        return self._invoke(request, handle_response)

    def remove(self, key: K) -> Optional[V]:
        """Remove a key-value pair from the map.

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
            >>> removed = my_map.remove("key")
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

        if self._near_cache is not None:
            self._near_cache.invalidate(key)

        key_data = self._to_data(key)
        request = MapCodec.encode_remove_request(self._name, key_data, 0)

        def handle_response(response: "ClientMessage") -> Optional[V]:
            data = MapCodec.decode_remove_response(response)
            return self._to_object(data) if data else None

        return self._invoke(request, handle_response)

    def delete(self, key: K) -> None:
        """Delete a key without returning the old value.

        Args:
            key: The key to delete.
        """
        self.delete_async(key).result()

    def delete_async(self, key: K) -> Future:
        """Delete a key asynchronously.

        Args:
            key: The key to delete.

        Returns:
            A Future that completes when the deletion is done.
        """
        self._check_not_destroyed()

        if self._near_cache is not None:
            self._near_cache.invalidate(key)

        key_data = self._to_data(key)
        request = MapCodec.encode_delete_request(self._name, key_data, 0)

        return self._invoke(request)

    def contains_key(self, key: K) -> bool:
        """Check if the map contains a key.

        Returns True if this map contains a mapping for the specified key.
        This operation does not affect near cache statistics.

        Args:
            key: The key whose presence is to be tested.

        Returns:
            True if the map contains a mapping for the key, False otherwise.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> if my_map.contains_key("user:1"):
            ...     print("User exists")
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

        key_data = self._to_data(key)
        request = MapCodec.encode_contains_key_request(self._name, key_data, 0)

        def handle_response(response: "ClientMessage") -> bool:
            return MapCodec.decode_contains_key_response(response)

        return self._invoke(request, handle_response)

    def contains_value(self, value: V) -> bool:
        """Check if the map contains a value.

        Args:
            value: The value to check.

        Returns:
            True if the value exists, False otherwise.
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

        value_data = self._to_data(value)
        request = MapCodec.encode_contains_value_request(self._name, value_data)

        def handle_response(response: "ClientMessage") -> bool:
            return MapCodec.decode_contains_value_response(response)

        return self._invoke(request, handle_response)

    def put_if_absent(self, key: K, value: V, ttl: float = -1) -> Optional[V]:
        """Put a value only if the key is not already present.

        Args:
            key: The key to set.
            value: The value to associate with the key.
            ttl: Time to live in seconds. -1 means infinite.

        Returns:
            The existing value if present, None if the put succeeded.
        """
        return self.put_if_absent_async(key, value, ttl).result()

    def put_if_absent_async(self, key: K, value: V, ttl: float = -1) -> Future:
        """Put a value only if absent, asynchronously.

        Args:
            key: The key to set.
            value: The value to associate with the key.
            ttl: Time to live in seconds. -1 means infinite.

        Returns:
            A Future that will contain the existing value if present.
        """
        self._check_not_destroyed()

        key_data = self._to_data(key)
        value_data = self._to_data(value)
        ttl_millis = int(ttl * 1000) if ttl > 0 else -1

        request = MapCodec.encode_put_if_absent_request(
            self._name, key_data, value_data, 0, ttl_millis
        )

        def handle_response(response: "ClientMessage") -> Optional[V]:
            data = MapCodec.decode_put_if_absent_response(response)
            return self._to_object(data) if data else None

        return self._invoke(request, handle_response)

    def replace(self, key: K, value: V) -> Optional[V]:
        """Replace the value for a key if it exists.

        Args:
            key: The key to replace.
            value: The new value.

        Returns:
            The old value if the key existed, None otherwise.
        """
        return self.replace_async(key, value).result()

    def replace_async(self, key: K, value: V) -> Future:
        """Replace a value asynchronously.

        Args:
            key: The key to replace.
            value: The new value.

        Returns:
            A Future that will contain the old value.
        """
        self._check_not_destroyed()

        if self._near_cache is not None:
            self._near_cache.invalidate(key)

        key_data = self._to_data(key)
        value_data = self._to_data(value)

        request = MapCodec.encode_replace_request(self._name, key_data, value_data, 0)

        def handle_response(response: "ClientMessage") -> Optional[V]:
            data = MapCodec.decode_replace_response(response)
            return self._to_object(data) if data else None

        return self._invoke(request, handle_response)

    def replace_if_same(self, key: K, old_value: V, new_value: V) -> bool:
        """Replace the value for a key if it equals the expected value.

        Args:
            key: The key to replace.
            old_value: The expected current value.
            new_value: The new value.

        Returns:
            True if the replacement was successful.
        """
        return self.replace_if_same_async(key, old_value, new_value).result()

    def replace_if_same_async(self, key: K, old_value: V, new_value: V) -> Future:
        """Replace a value if same, asynchronously.

        Args:
            key: The key to replace.
            old_value: The expected current value.
            new_value: The new value.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()

        if self._near_cache is not None:
            self._near_cache.invalidate(key)

        key_data = self._to_data(key)
        old_value_data = self._to_data(old_value)
        new_value_data = self._to_data(new_value)

        request = MapCodec.encode_replace_if_same_request(
            self._name, key_data, old_value_data, new_value_data, 0
        )

        def handle_response(response: "ClientMessage") -> bool:
            return MapCodec.decode_replace_if_same_response(response)

        return self._invoke(request, handle_response)

    def set(self, key: K, value: V, ttl: float = -1) -> None:
        """Set a value without returning the old value.

        Args:
            key: The key to set.
            value: The value to associate.
            ttl: Time to live in seconds. -1 means infinite.
        """
        self.set_async(key, value, ttl).result()

    def set_async(self, key: K, value: V, ttl: float = -1) -> Future:
        """Set a value asynchronously without returning the old value.

        Args:
            key: The key to set.
            value: The value to associate.
            ttl: Time to live in seconds. -1 means infinite.

        Returns:
            A Future that completes when the operation is done.
        """
        self._check_not_destroyed()

        if self._near_cache is not None:
            self._near_cache.invalidate(key)

        key_data = self._to_data(key)
        value_data = self._to_data(value)
        ttl_millis = int(ttl * 1000) if ttl > 0 else -1

        request = MapCodec.encode_set_request(
            self._name, key_data, value_data, 0, ttl_millis
        )

        return self._invoke(request)

    def get_all(self, keys: Set[K]) -> Dict[K, V]:
        """Get multiple values at once.

        Args:
            keys: The keys to retrieve.

        Returns:
            A dictionary of key-value pairs for found keys.
        """
        return self.get_all_async(keys).result()

    def get_all_async(self, keys: Set[K]) -> Future:
        """Get multiple values asynchronously.

        Args:
            keys: The keys to retrieve.

        Returns:
            A Future that will contain a dictionary of results.
        """
        self._check_not_destroyed()

        if not keys:
            future: Future = Future()
            future.set_result({})
            return future

        keys_data = [self._to_data(k) for k in keys]
        request = MapCodec.encode_get_all_request(self._name, keys_data)

        def handle_response(response: "ClientMessage") -> Dict[K, V]:
            entries = MapCodec.decode_get_all_response(response)
            result = {}
            for key_data, value_data in entries:
                k = self._to_object(key_data)
                v = self._to_object(value_data)
                result[k] = v
                if self._near_cache is not None:
                    self._near_cache.put(k, v)
            return result

        return self._invoke(request, handle_response)

    def put_all(self, entries: Dict[K, V]) -> None:
        """Put multiple key-value pairs at once.

        Args:
            entries: The key-value pairs to put.
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

        if self._near_cache is not None:
            for key in entries:
                self._near_cache.invalidate(key)

        if not entries:
            future: Future = Future()
            future.set_result(None)
            return future

        entries_data = [(self._to_data(k), self._to_data(v)) for k, v in entries.items()]
        request = MapCodec.encode_put_all_request(self._name, entries_data)

        return self._invoke(request)

    def size(self) -> int:
        """Get the number of entries in the map.

        Returns the number of key-value mappings in this map across
        all cluster members.

        Returns:
            The total number of entries in the distributed map.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> count = my_map.size()
            >>> print(f"Map has {count} entries")
        """
        return self.size_async().result()

    def size_async(self) -> Future:
        """Get the size asynchronously.

        Returns:
            A Future that will contain the size.
        """
        self._check_not_destroyed()

        request = MapCodec.encode_size_request(self._name)

        def handle_response(response: "ClientMessage") -> int:
            return MapCodec.decode_size_response(response)

        return self._invoke(request, handle_response)

    def is_empty(self) -> bool:
        """Check if the map is empty.

        Returns:
            True if the map has no entries.
        """
        return self.is_empty_async().result()

    def is_empty_async(self) -> Future:
        """Check if the map is empty asynchronously.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()

        request = MapCodec.encode_is_empty_request(self._name)

        def handle_response(response: "ClientMessage") -> bool:
            return MapCodec.decode_is_empty_response(response)

        return self._invoke(request, handle_response)

    def clear(self) -> None:
        """Remove all entries from the map.

        Removes all key-value mappings from this map across all cluster
        members. The map will be empty after this call. Also clears
        the near cache if configured.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> my_map.clear()
            >>> assert my_map.size() == 0
        """
        self.clear_async().result()

    def clear_async(self) -> Future:
        """Clear the map asynchronously.

        Returns:
            A Future that completes when the clear is done.
        """
        self._check_not_destroyed()

        if self._near_cache is not None:
            self._near_cache.invalidate_all()

        request = MapCodec.encode_clear_request(self._name)

        return self._invoke(request)

    def key_set(self, predicate: Any = None) -> Set[K]:
        """Get all keys in the map.

        Args:
            predicate: Optional predicate to filter keys.

        Returns:
            A set of keys.
        """
        return self.key_set_async(predicate).result()

    def key_set_async(self, predicate: Any = None) -> Future:
        """Get all keys asynchronously.

        Args:
            predicate: Optional predicate to filter keys.

        Returns:
            A Future that will contain a set of keys.
        """
        self._check_not_destroyed()

        request = MapCodec.encode_key_set_request(self._name)

        def handle_response(response: "ClientMessage") -> Set[K]:
            keys_data = MapCodec.decode_key_set_response(response)
            return {self._to_object(k) for k in keys_data}

        return self._invoke(request, handle_response)

    def values(self, predicate: Any = None) -> List[V]:
        """Get all values in the map.

        Args:
            predicate: Optional predicate to filter values.

        Returns:
            A list of values.
        """
        return self.values_async(predicate).result()

    def values_async(self, predicate: Any = None) -> Future:
        """Get all values asynchronously.

        Args:
            predicate: Optional predicate to filter values.

        Returns:
            A Future that will contain a list of values.
        """
        self._check_not_destroyed()

        request = MapCodec.encode_values_request(self._name)

        def handle_response(response: "ClientMessage") -> List[V]:
            values_data = MapCodec.decode_values_response(response)
            return [self._to_object(v) for v in values_data]

        return self._invoke(request, handle_response)

    def entry_set(self, predicate: Any = None) -> Set[Tuple[K, V]]:
        """Get all entries in the map.

        Args:
            predicate: Optional predicate to filter entries.

        Returns:
            A set of (key, value) tuples.
        """
        return self.entry_set_async(predicate).result()

    def entry_set_async(self, predicate: Any = None) -> Future:
        """Get all entries asynchronously.

        Args:
            predicate: Optional predicate to filter entries.

        Returns:
            A Future that will contain a set of entries.
        """
        self._check_not_destroyed()

        request = MapCodec.encode_entry_set_request(self._name)

        def handle_response(response: "ClientMessage") -> Set[Tuple[K, V]]:
            entries_data = MapCodec.decode_entry_set_response(response)
            return {(self._to_object(k), self._to_object(v)) for k, v in entries_data}

        return self._invoke(request, handle_response)

    def add_entry_listener(
        self,
        listener: EntryListener[K, V],
        include_value: bool = True,
        key: Optional[K] = None,
        predicate: Any = None,
    ) -> str:
        """Add an entry listener to the map.

        Args:
            listener: The listener to add.
            include_value: Whether to include entry values in events.
            key: Optional key to listen on. If None, listens to all keys.
            predicate: Optional predicate to filter events.

        Returns:
            A registration ID for removing the listener.
        """
        self._check_not_destroyed()

        local_id = str(uuid_module.uuid4())

        if key is not None:
            key_data = self._to_data(key)
            request = MapCodec.encode_add_entry_listener_to_key_request(
                self._name, key_data, include_value, False
            )
        else:
            request = MapCodec.encode_add_entry_listener_request(
                self._name, include_value, False
            )

        def handle_response(response: "ClientMessage") -> str:
            server_id = MapCodec.decode_add_entry_listener_response(response)
            self._entry_listeners[local_id] = (listener, include_value, server_id)
            return local_id

        future = self._invoke(request, handle_response)
        return future.result()

    def remove_entry_listener(self, registration_id: str) -> bool:
        """Remove an entry listener.

        Args:
            registration_id: The registration ID from add_entry_listener.

        Returns:
            True if the listener was removed.
        """
        self._check_not_destroyed()

        entry = self._entry_listeners.pop(registration_id, None)
        if entry is None:
            return False

        _, _, server_id = entry
        if server_id is None:
            return True

        request = MapCodec.encode_remove_entry_listener_request(self._name, server_id)

        def handle_response(response: "ClientMessage") -> bool:
            return MapCodec.decode_remove_entry_listener_response(response)

        future = self._invoke(request, handle_response)
        return future.result()

    def aggregate(self, aggregator: Any, predicate: Any = None) -> Any:
        """Aggregate map entries.

        Args:
            aggregator: The aggregator to apply.
            predicate: Optional predicate to filter entries.

        Returns:
            The aggregation result.
        """
        return self.aggregate_async(aggregator, predicate).result()

    def aggregate_async(self, aggregator: Any, predicate: Any = None) -> Future:
        """Aggregate map entries asynchronously.

        Args:
            aggregator: The aggregator to apply.
            predicate: Optional predicate to filter entries.

        Returns:
            A Future that will contain the aggregation result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def project(
        self, projection: Projection, predicate: Optional[Any] = None
    ) -> List[Any]:
        """Project map entries.

        Args:
            projection: The projection to apply.
            predicate: Optional predicate to filter entries.

        Returns:
            A list of projected values.
        """
        return self.project_async(projection, predicate).result()

    def project_async(
        self, projection: Projection, predicate: Optional[Any] = None
    ) -> Future:
        """Project map entries asynchronously.

        Args:
            projection: The projection to apply.
            predicate: Optional predicate to filter entries.

        Returns:
            A Future that will contain a list of projected values.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result([])
        return future

    def execute_on_key(
        self,
        key: K,
        entry_processor: EntryProcessor[K, V],
    ) -> Any:
        """Execute an entry processor on a single key.

        Applies the entry processor to the entry associated with the
        specified key. The processor runs atomically on the partition
        that owns the key.

        Args:
            key: The key whose entry will be processed.
            entry_processor: The entry processor to execute.

        Returns:
            The result returned by the entry processor.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> result = my_map.execute_on_key("counter", IncrementProcessor(5))
        """
        return self.execute_on_key_async(key, entry_processor).result()

    def execute_on_key_async(
        self,
        key: K,
        entry_processor: EntryProcessor[K, V],
    ) -> Future:
        """Execute an entry processor on a single key asynchronously.

        Args:
            key: The key whose entry will be processed.
            entry_processor: The entry processor to execute.

        Returns:
            A Future that will contain the result.
        """
        self._check_not_destroyed()

        if self._near_cache is not None:
            self._near_cache.invalidate(key)

        key_data = self._to_data(key)
        processor_data = self._to_data(entry_processor)

        request = MapCodec.encode_execute_on_key_request(
            self._name, key_data, processor_data, 0
        )

        def handle_response(response: "ClientMessage") -> Any:
            data = MapCodec.decode_execute_on_key_response(response)
            return self._to_object(data) if data else None

        return self._invoke(request, handle_response)

    def execute_on_keys(
        self,
        keys: Set[K],
        entry_processor: EntryProcessor[K, V],
    ) -> Dict[K, Any]:
        """Execute an entry processor on multiple keys.

        Applies the entry processor to entries associated with the
        specified keys. Processing happens in parallel across partitions.

        Args:
            keys: The keys whose entries will be processed.
            entry_processor: The entry processor to execute.

        Returns:
            A dictionary mapping each key to its processor result.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> results = my_map.execute_on_keys(
            ...     {"key1", "key2", "key3"},
            ...     IncrementProcessor(1)
            ... )
        """
        return self.execute_on_keys_async(keys, entry_processor).result()

    def execute_on_keys_async(
        self,
        keys: Set[K],
        entry_processor: EntryProcessor[K, V],
    ) -> Future:
        """Execute an entry processor on multiple keys asynchronously.

        Args:
            keys: The keys whose entries will be processed.
            entry_processor: The entry processor to execute.

        Returns:
            A Future that will contain a dictionary of results.
        """
        self._check_not_destroyed()

        if self._near_cache is not None:
            for key in keys:
                self._near_cache.invalidate(key)

        if not keys:
            future: Future = Future()
            future.set_result({})
            return future

        keys_data = [self._to_data(k) for k in keys]
        processor_data = self._to_data(entry_processor)

        request = MapCodec.encode_execute_on_keys_request(
            self._name, keys_data, processor_data
        )

        def handle_response(response: "ClientMessage") -> Dict[K, Any]:
            entries = MapCodec.decode_execute_on_keys_response(response)
            return {
                self._to_object(k): self._to_object(v)
                for k, v in entries
            }

        return self._invoke(request, handle_response)

    def execute_on_entries(
        self,
        entry_processor: EntryProcessor[K, V],
        predicate: Any = None,
    ) -> Dict[K, Any]:
        """Execute an entry processor on entries matching a predicate.

        Applies the entry processor to all entries that match the
        optional predicate. If no predicate is provided, processes
        all entries in the map.

        Args:
            entry_processor: The entry processor to execute.
            predicate: Optional predicate to filter entries.

        Returns:
            A dictionary mapping each processed key to its result.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> results = my_map.execute_on_entries(
            ...     IncrementProcessor(1),
            ...     predicate=GreaterLessPredicate("value", 10, False, True)
            ... )
        """
        return self.execute_on_entries_async(entry_processor, predicate).result()

    def execute_on_entries_async(
        self,
        entry_processor: EntryProcessor[K, V],
        predicate: Any = None,
    ) -> Future:
        """Execute an entry processor on entries asynchronously.

        Args:
            entry_processor: The entry processor to execute.
            predicate: Optional predicate to filter entries.

        Returns:
            A Future that will contain a dictionary of results.
        """
        self._check_not_destroyed()

        if self._near_cache is not None:
            self._near_cache.invalidate_all()

        processor_data = self._to_data(entry_processor)

        request = MapCodec.encode_execute_on_all_keys_request(self._name, processor_data)

        def handle_response(response: "ClientMessage") -> Dict[K, Any]:
            entries = MapCodec.decode_execute_on_all_keys_response(response)
            return {
                self._to_object(k): self._to_object(v)
                for k, v in entries
            }

        return self._invoke(request, handle_response)

    def execute_on_all_entries(
        self,
        entry_processor: EntryProcessor[K, V],
    ) -> Dict[K, Any]:
        """Execute an entry processor on all entries in the map.

        Applies the entry processor to every entry in the map.
        This is equivalent to calling execute_on_entries without
        a predicate.

        Args:
            entry_processor: The entry processor to execute.

        Returns:
            A dictionary mapping each key to its processor result.

        Raises:
            IllegalStateException: If the map has been destroyed.

        Example:
            >>> results = my_map.execute_on_all_entries(IncrementProcessor(1))
        """
        return self.execute_on_all_entries_async(entry_processor).result()

    def execute_on_all_entries_async(
        self,
        entry_processor: EntryProcessor[K, V],
    ) -> Future:
        """Execute an entry processor on all entries asynchronously.

        Args:
            entry_processor: The entry processor to execute.

        Returns:
            A Future that will contain a dictionary of results.
        """
        return self.execute_on_entries_async(entry_processor, None)

    def lock(self, key: K, ttl: float = -1) -> None:
        """Acquire a lock on a key.

        Args:
            key: The key to lock.
            ttl: Time to live for the lock in seconds.
        """
        self.lock_async(key, ttl).result()

    def lock_async(self, key: K, ttl: float = -1) -> Future:
        """Acquire a lock asynchronously.

        Args:
            key: The key to lock.
            ttl: Time to live for the lock in seconds.

        Returns:
            A Future that completes when the lock is acquired.
        """
        self._check_not_destroyed()

        key_data = self._to_data(key)
        ttl_millis = int(ttl * 1000) if ttl > 0 else -1
        reference_id = self._reference_id_generator.next_id()

        request = MapCodec.encode_lock_request(
            self._name, key_data, 0, ttl_millis, reference_id
        )

        return self._invoke(request)

    def try_lock(self, key: K, timeout: float = 0, ttl: float = -1) -> bool:
        """Try to acquire a lock on a key.

        Args:
            key: The key to lock.
            timeout: Maximum time to wait for the lock in seconds.
            ttl: Time to live for the lock in seconds.

        Returns:
            True if the lock was acquired, False otherwise.
        """
        return self.try_lock_async(key, timeout, ttl).result()

    def try_lock_async(self, key: K, timeout: float = 0, ttl: float = -1) -> Future:
        """Try to acquire a lock asynchronously.

        Args:
            key: The key to lock.
            timeout: Maximum time to wait for the lock in seconds.
            ttl: Time to live for the lock in seconds.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()

        key_data = self._to_data(key)
        ttl_millis = int(ttl * 1000) if ttl > 0 else -1
        timeout_millis = int(timeout * 1000) if timeout > 0 else 0
        reference_id = self._reference_id_generator.next_id()

        request = MapCodec.encode_try_lock_request(
            self._name, key_data, 0, ttl_millis, timeout_millis, reference_id
        )

        def handle_response(response: "ClientMessage") -> bool:
            return MapCodec.decode_try_lock_response(response)

        return self._invoke(request, handle_response)

    def unlock(self, key: K) -> None:
        """Release a lock on a key.

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

        key_data = self._to_data(key)
        reference_id = self._reference_id_generator.next_id()

        request = MapCodec.encode_unlock_request(self._name, key_data, 0, reference_id)

        return self._invoke(request)

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

        key_data = self._to_data(key)

        request = MapCodec.encode_is_locked_request(self._name, key_data)

        def handle_response(response: "ClientMessage") -> bool:
            return MapCodec.decode_is_locked_response(response)

        return self._invoke(request, handle_response)

    def force_unlock(self, key: K) -> None:
        """Force release a lock regardless of owner.

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

        key_data = self._to_data(key)
        reference_id = self._reference_id_generator.next_id()

        request = MapCodec.encode_force_unlock_request(self._name, key_data, reference_id)

        return self._invoke(request)

    def evict(self, key: K) -> bool:
        """Evict a specific key from the map.

        Args:
            key: The key to evict.

        Returns:
            True if the key was evicted.
        """
        return self.evict_async(key).result()

    def evict_async(self, key: K) -> Future:
        """Evict a key asynchronously.

        Args:
            key: The key to evict.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()

        if self._near_cache is not None:
            self._near_cache.invalidate(key)

        key_data = self._to_data(key)

        request = MapCodec.encode_evict_request(self._name, key_data, 0)

        def handle_response(response: "ClientMessage") -> bool:
            return MapCodec.decode_evict_response(response)

        return self._invoke(request, handle_response)

    def evict_all(self) -> None:
        """Evict all entries from the map."""
        self.evict_all_async().result()

    def evict_all_async(self) -> Future:
        """Evict all entries asynchronously.

        Returns:
            A Future that completes when all entries are evicted.
        """
        self._check_not_destroyed()

        if self._near_cache is not None:
            self._near_cache.invalidate_all()

        request = MapCodec.encode_evict_all_request(self._name)

        return self._invoke(request)

    def flush(self) -> None:
        """Flush map store operations."""
        self.flush_async().result()

    def flush_async(self) -> Future:
        """Flush map store operations asynchronously.

        Returns:
            A Future that completes when the flush is done.
        """
        self._check_not_destroyed()

        request = MapCodec.encode_flush_request(self._name)

        return self._invoke(request)

    def load_all(self, keys: Optional[Set[K]] = None, replace_existing: bool = True) -> None:
        """Load entries from the map store.

        Args:
            keys: Optional set of keys to load. If None, loads all.
            replace_existing: Whether to replace existing entries.
        """
        self.load_all_async(keys, replace_existing).result()

    def load_all_async(
        self, keys: Optional[Set[K]] = None, replace_existing: bool = True
    ) -> Future:
        """Load entries from the map store asynchronously.

        Args:
            keys: Optional set of keys to load.
            replace_existing: Whether to replace existing entries.

        Returns:
            A Future that completes when the load is done.
        """
        self._check_not_destroyed()

        keys_data = [self._to_data(k) for k in keys] if keys else None

        request = MapCodec.encode_load_all_request(self._name, keys_data, replace_existing)

        return self._invoke(request)

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


class _ReferenceIdGenerator:
    """Thread-safe reference ID generator for lock operations."""

    def __init__(self) -> None:
        self._counter = 0
        self._lock = threading.Lock()

    def next_id(self) -> int:
        with self._lock:
            self._counter += 1
            return self._counter


class _NearCacheInvalidationListener(EntryListener):
    """Internal listener for near cache invalidation events from cluster."""

    def __init__(self, near_cache: "NearCache"):
        self._near_cache = near_cache

    def entry_added(self, event: EntryEvent) -> None:
        self._near_cache.invalidate(event.key)

    def entry_removed(self, event: EntryEvent) -> None:
        self._near_cache.invalidate(event.key)

    def entry_updated(self, event: EntryEvent) -> None:
        self._near_cache.invalidate(event.key)

    def entry_evicted(self, event: EntryEvent) -> None:
        self._near_cache.invalidate(event.key)

    def entry_expired(self, event: EntryEvent) -> None:
        self._near_cache.invalidate(event.key)

    def map_evicted(self, event: EntryEvent) -> None:
        self._near_cache.invalidate_all()

    def map_cleared(self, event: EntryEvent) -> None:
        self._near_cache.invalidate_all()
