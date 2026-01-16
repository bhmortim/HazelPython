"""Map Loader and Store interfaces for external data source integration."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, Iterable, List, Optional, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class MapLoader(ABC, Generic[K, V]):
    """Interface for loading map entries from an external data source.

    Implement this interface to load data into a Hazelcast map from
    an external data store such as a database, file system, or API.

    The MapLoader is invoked by the cluster when:
    - A key is requested but not present in memory (read-through)
    - `load_all()` is called on the map
    - The map is configured with eager initial loading

    Type Parameters:
        K: The key type.
        V: The value type.

    Example:
        Database loader::

            class DatabaseLoader(MapLoader[str, dict]):
                def __init__(self, connection):
                    self._conn = connection

                def load(self, key):
                    cursor = self._conn.execute(
                        "SELECT data FROM users WHERE id = ?", (key,)
                    )
                    row = cursor.fetchone()
                    return row[0] if row else None

                def load_all(self, keys):
                    result = {}
                    for key in keys:
                        value = self.load(key)
                        if value is not None:
                            result[key] = value
                    return result

                def load_all_keys(self):
                    cursor = self._conn.execute("SELECT id FROM users")
                    return [row[0] for row in cursor.fetchall()]
    """

    @abstractmethod
    def load(self, key: K) -> Optional[V]:
        """Load the value for a single key.

        This method is called when a map entry is requested but not
        present in memory (cache miss with read-through enabled).

        Args:
            key: The key to load the value for.

        Returns:
            The value associated with the key, or None if not found.
        """
        pass

    @abstractmethod
    def load_all(self, keys: Iterable[K]) -> Dict[K, V]:
        """Load values for multiple keys.

        This method is called when bulk loading is requested, either
        explicitly via `MapProxy.load_all()` or during initial loading.

        Args:
            keys: The keys to load values for.

        Returns:
            A dictionary mapping each found key to its value.
            Keys not found in the store should be omitted.
        """
        pass

    @abstractmethod
    def load_all_keys(self) -> Iterable[K]:
        """Load all keys from the external store.

        This method is called during initial loading when no specific
        keys are provided. Return all keys that should be loaded into
        the map.

        Returns:
            An iterable of all keys in the external store.
        """
        pass


class MapStore(MapLoader[K, V], Generic[K, V]):
    """Interface for loading and storing map entries with an external data source.

    Extends MapLoader to add write operations. Implement this interface
    to synchronize a Hazelcast map with an external data store using
    write-through or write-behind persistence.

    Write modes:
    - **Write-through**: Entries are written immediately to the store
      when modified in the map (synchronous).
    - **Write-behind**: Entries are queued and written asynchronously
      after a configured delay, with optional batching and coalescing.

    Type Parameters:
        K: The key type.
        V: The value type.

    Example:
        Database store::

            class DatabaseStore(MapStore[str, dict]):
                def __init__(self, connection):
                    self._conn = connection

                def load(self, key):
                    cursor = self._conn.execute(
                        "SELECT data FROM users WHERE id = ?", (key,)
                    )
                    row = cursor.fetchone()
                    return row[0] if row else None

                def load_all(self, keys):
                    result = {}
                    for key in keys:
                        value = self.load(key)
                        if value is not None:
                            result[key] = value
                    return result

                def load_all_keys(self):
                    cursor = self._conn.execute("SELECT id FROM users")
                    return [row[0] for row in cursor.fetchall()]

                def store(self, key, value):
                    self._conn.execute(
                        "INSERT OR REPLACE INTO users (id, data) VALUES (?, ?)",
                        (key, value)
                    )
                    self._conn.commit()

                def store_all(self, entries):
                    for key, value in entries.items():
                        self.store(key, value)

                def delete(self, key):
                    self._conn.execute("DELETE FROM users WHERE id = ?", (key,))
                    self._conn.commit()

                def delete_all(self, keys):
                    for key in keys:
                        self.delete(key)
    """

    @abstractmethod
    def store(self, key: K, value: V) -> None:
        """Store a single key-value pair.

        This method is called when an entry is added or updated in
        the map. In write-through mode, it is called synchronously.
        In write-behind mode, it may be called asynchronously after
        a delay.

        Args:
            key: The key to store.
            value: The value to store.
        """
        pass

    @abstractmethod
    def store_all(self, entries: Dict[K, V]) -> None:
        """Store multiple key-value pairs.

        This method is called for batch writes in write-behind mode
        when multiple entries are coalesced together.

        Args:
            entries: A dictionary of key-value pairs to store.
        """
        pass

    @abstractmethod
    def delete(self, key: K) -> None:
        """Delete a single key.

        This method is called when an entry is removed from the map.

        Args:
            key: The key to delete.
        """
        pass

    @abstractmethod
    def delete_all(self, keys: Iterable[K]) -> None:
        """Delete multiple keys.

        This method is called for batch deletes in write-behind mode.

        Args:
            keys: The keys to delete.
        """
        pass


class MapLoaderLifecycleSupport(ABC):
    """Optional lifecycle interface for MapLoader/MapStore implementations.

    Implement this interface if your loader/store needs initialization
    or cleanup callbacks.
    """

    @abstractmethod
    def init(self, properties: Dict[str, Any], map_name: str) -> None:
        """Initialize the loader/store.

        Called when the map is created and the loader/store is attached.

        Args:
            properties: Configuration properties from MapStoreConfig.
            map_name: The name of the map this loader is attached to.
        """
        pass

    @abstractmethod
    def destroy(self) -> None:
        """Clean up resources.

        Called when the map is destroyed or the client shuts down.
        """
        pass


class EntryLoader(ABC, Generic[K, V]):
    """Interface for loading map entries with metadata.

    Similar to MapLoader but allows returning entries with additional
    metadata such as expiration time.
    """

    @abstractmethod
    def load(self, key: K) -> Optional["EntryLoaderEntry[K, V]"]:
        """Load an entry with metadata for a single key.

        Args:
            key: The key to load.

        Returns:
            An EntryLoaderEntry with value and metadata, or None if not found.
        """
        pass

    @abstractmethod
    def load_all(self, keys: Iterable[K]) -> Dict[K, "EntryLoaderEntry[K, V]"]:
        """Load entries with metadata for multiple keys.

        Args:
            keys: The keys to load.

        Returns:
            A dictionary mapping keys to EntryLoaderEntry instances.
        """
        pass


class EntryLoaderEntry(Generic[K, V]):
    """An entry loaded by EntryLoader with optional metadata.

    Attributes:
        key: The entry key.
        value: The entry value.
        expiration_time: Optional expiration time in milliseconds since epoch.
    """

    def __init__(
        self,
        key: K,
        value: V,
        expiration_time: Optional[int] = None,
    ):
        self._key = key
        self._value = value
        self._expiration_time = expiration_time

    @property
    def key(self) -> K:
        """Get the entry key."""
        return self._key

    @property
    def value(self) -> V:
        """Get the entry value."""
        return self._value

    @property
    def expiration_time(self) -> Optional[int]:
        """Get the expiration time in milliseconds since epoch."""
        return self._expiration_time

    def __repr__(self) -> str:
        return (
            f"EntryLoaderEntry(key={self._key!r}, value={self._value!r}, "
            f"expiration_time={self._expiration_time})"
        )


class LoadAllKeysCallback(ABC):
    """Callback interface for asynchronous key loading.

    Implement this interface to receive keys asynchronously during
    initial loading operations.
    """

    @abstractmethod
    def on_keys(self, keys: List[K]) -> None:
        """Called with a batch of loaded keys.

        This method may be called multiple times with different batches
        of keys during a single load operation.

        Args:
            keys: A batch of loaded keys.
        """
        pass

    @abstractmethod
    def on_complete(self) -> None:
        """Called when all keys have been loaded.

        This method is called once after all on_keys() calls are complete.
        """
        pass

    @abstractmethod
    def on_error(self, error: Exception) -> None:
        """Called if an error occurs during loading.

        Args:
            error: The exception that occurred.
        """
        pass
