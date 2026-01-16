"""JCache (JSR-107) distributed cache proxy implementation.

This module provides the `Cache` class implementing JCache-compatible
operations for the Hazelcast distributed cache.

Example:
    Basic cache operations::

        cache = client.get_cache("my-cache")
        cache.put("key", "value")
        value = cache.get("key")
        cache.remove("key")

    With expiry policy::

        from hazelcast.proxy.cache import ExpiryPolicy, Duration

        expiry = ExpiryPolicy(
            creation=Duration.minutes(30),
            access=Duration.minutes(10),
        )
        cache.put("key", "value", expiry_policy=expiry)
"""

from concurrent.futures import Future
from enum import IntEnum
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Set,
    TypeVar,
    TYPE_CHECKING,
)

from hazelcast.protocol.codec import CacheCodec
from hazelcast.proxy.base import Proxy, ProxyContext

if TYPE_CHECKING:
    from hazelcast.protocol.client_message import ClientMessage

K = TypeVar("K")
V = TypeVar("V")


class Duration:
    """Represents a time duration for cache expiry.

    Attributes:
        amount: The amount of time units.
        time_unit: The time unit (NANOSECONDS, MICROSECONDS, etc.).

    Example:
        >>> duration = Duration.minutes(30)
        >>> duration = Duration.seconds(60)
        >>> duration = Duration(5, TimeUnit.HOURS)
    """

    class TimeUnit(IntEnum):
        NANOSECONDS = 0
        MICROSECONDS = 1
        MILLISECONDS = 2
        SECONDS = 3
        MINUTES = 4
        HOURS = 5
        DAYS = 6

    ETERNAL = None

    def __init__(self, amount: int, time_unit: "Duration.TimeUnit"):
        self._amount = amount
        self._time_unit = time_unit

    @property
    def amount(self) -> int:
        return self._amount

    @property
    def time_unit(self) -> "Duration.TimeUnit":
        return self._time_unit

    def to_millis(self) -> int:
        """Convert this duration to milliseconds."""
        multipliers = {
            self.TimeUnit.NANOSECONDS: 1e-6,
            self.TimeUnit.MICROSECONDS: 1e-3,
            self.TimeUnit.MILLISECONDS: 1,
            self.TimeUnit.SECONDS: 1000,
            self.TimeUnit.MINUTES: 60000,
            self.TimeUnit.HOURS: 3600000,
            self.TimeUnit.DAYS: 86400000,
        }
        return int(self._amount * multipliers[self._time_unit])

    @classmethod
    def nanoseconds(cls, amount: int) -> "Duration":
        return cls(amount, cls.TimeUnit.NANOSECONDS)

    @classmethod
    def microseconds(cls, amount: int) -> "Duration":
        return cls(amount, cls.TimeUnit.MICROSECONDS)

    @classmethod
    def milliseconds(cls, amount: int) -> "Duration":
        return cls(amount, cls.TimeUnit.MILLISECONDS)

    @classmethod
    def seconds(cls, amount: int) -> "Duration":
        return cls(amount, cls.TimeUnit.SECONDS)

    @classmethod
    def minutes(cls, amount: int) -> "Duration":
        return cls(amount, cls.TimeUnit.MINUTES)

    @classmethod
    def hours(cls, amount: int) -> "Duration":
        return cls(amount, cls.TimeUnit.HOURS)

    @classmethod
    def days(cls, amount: int) -> "Duration":
        return cls(amount, cls.TimeUnit.DAYS)

    @classmethod
    def eternal(cls) -> Optional["Duration"]:
        """Return a sentinel indicating no expiry."""
        return cls.ETERNAL

    def __repr__(self) -> str:
        return f"Duration({self._amount}, {self._time_unit.name})"


class ExpiryPolicy:
    """Defines expiry durations for cache entries.

    JCache-compliant expiry policy that can specify different durations
    for entry creation, access, and update events.

    Attributes:
        creation: Duration after creation before entry expires.
        access: Duration after access before entry expires.
        update: Duration after update before entry expires.

    Example:
        >>> policy = ExpiryPolicy(
        ...     creation=Duration.minutes(30),
        ...     access=Duration.minutes(10),
        ...     update=Duration.minutes(15),
        ... )
        >>> cache.put("key", "value", expiry_policy=policy)
    """

    def __init__(
        self,
        creation: Optional[Duration] = None,
        access: Optional[Duration] = None,
        update: Optional[Duration] = None,
    ):
        self._creation = creation
        self._access = access
        self._update = update

    @property
    def creation(self) -> Optional[Duration]:
        return self._creation

    @property
    def access(self) -> Optional[Duration]:
        return self._access

    @property
    def update(self) -> Optional[Duration]:
        return self._update

    @classmethod
    def created_expiry_policy(cls, duration: Duration) -> "ExpiryPolicy":
        """Create a policy that expires entries after creation."""
        return cls(creation=duration)

    @classmethod
    def accessed_expiry_policy(cls, duration: Duration) -> "ExpiryPolicy":
        """Create a policy that expires entries after last access."""
        return cls(access=duration)

    @classmethod
    def modified_expiry_policy(cls, duration: Duration) -> "ExpiryPolicy":
        """Create a policy that expires entries after last modification."""
        return cls(update=duration)

    @classmethod
    def touched_expiry_policy(cls, duration: Duration) -> "ExpiryPolicy":
        """Create a policy that expires entries after any touch."""
        return cls(creation=duration, access=duration, update=duration)

    @classmethod
    def eternal_expiry_policy(cls) -> "ExpiryPolicy":
        """Create a policy where entries never expire."""
        return cls()

    def __repr__(self) -> str:
        return f"ExpiryPolicy(creation={self._creation}, access={self._access}, update={self._update})"


class CacheEntryProcessor(Generic[K, V]):
    """Base class for cache entry processors.

    Entry processors execute atomically on the cache entry, enabling
    atomic read-modify-write operations without explicit locking.

    Example:
        >>> class IncrementProcessor(CacheEntryProcessor[str, int]):
        ...     def __init__(self, increment: int):
        ...         self.increment = increment
        ...
        ...     def process(self, entry: MutableEntry) -> int:
        ...         old_value = entry.value or 0
        ...         entry.value = old_value + self.increment
        ...         return entry.value
    """

    def process(self, entry: Any, *arguments: Any) -> Any:
        """Process the cache entry.

        Args:
            entry: The mutable cache entry to process.
            *arguments: Additional arguments passed to the processor.

        Returns:
            The result of processing.
        """
        raise NotImplementedError("Subclasses must implement process()")


class MutableEntry(Generic[K, V]):
    """A mutable cache entry for use with entry processors.

    Attributes:
        key: The entry key (read-only).
        value: The entry value (read-write).
        exists: Whether the entry exists in the cache.
    """

    def __init__(self, key: K, value: Optional[V], exists: bool):
        self._key = key
        self._value = value
        self._exists = exists
        self._removed = False
        self._updated = False

    @property
    def key(self) -> K:
        return self._key

    @property
    def value(self) -> Optional[V]:
        return self._value

    @value.setter
    def value(self, new_value: V) -> None:
        self._value = new_value
        self._updated = True
        self._exists = True
        self._removed = False

    @property
    def exists(self) -> bool:
        return self._exists and not self._removed

    def remove(self) -> None:
        """Mark this entry for removal."""
        self._removed = True
        self._exists = False


class CacheConfig:
    """Configuration for a JCache cache.

    Attributes:
        name: The cache name.
        key_type: Optional key type class name.
        value_type: Optional value type class name.
        statistics_enabled: Whether to enable statistics.
        management_enabled: Whether to enable JMX management.
        read_through: Whether to read through to a cache loader.
        write_through: Whether to write through to a cache writer.
        store_by_value: Whether to store by value (copy) or reference.
        expiry_policy: Default expiry policy for the cache.
    """

    def __init__(
        self,
        name: str,
        key_type: Optional[str] = None,
        value_type: Optional[str] = None,
        statistics_enabled: bool = False,
        management_enabled: bool = False,
        read_through: bool = False,
        write_through: bool = False,
        store_by_value: bool = True,
        expiry_policy: Optional[ExpiryPolicy] = None,
    ):
        self._name = name
        self._key_type = key_type
        self._value_type = value_type
        self._statistics_enabled = statistics_enabled
        self._management_enabled = management_enabled
        self._read_through = read_through
        self._write_through = write_through
        self._store_by_value = store_by_value
        self._expiry_policy = expiry_policy

    @property
    def name(self) -> str:
        return self._name

    @property
    def key_type(self) -> Optional[str]:
        return self._key_type

    @property
    def value_type(self) -> Optional[str]:
        return self._value_type

    @property
    def statistics_enabled(self) -> bool:
        return self._statistics_enabled

    @property
    def management_enabled(self) -> bool:
        return self._management_enabled

    @property
    def read_through(self) -> bool:
        return self._read_through

    @property
    def write_through(self) -> bool:
        return self._write_through

    @property
    def store_by_value(self) -> bool:
        return self._store_by_value

    @property
    def expiry_policy(self) -> Optional[ExpiryPolicy]:
        return self._expiry_policy

    def __repr__(self) -> str:
        return f"CacheConfig(name={self._name!r})"


class Cache(Proxy, Generic[K, V]):
    """JCache (JSR-107) compliant distributed cache proxy.

    Provides JCache-compatible operations for the Hazelcast distributed
    cache, including standard CRUD operations, bulk operations, entry
    processors, and expiry policy support.

    Type Parameters:
        K: The key type.
        V: The value type.

    Attributes:
        name: The name of this distributed cache.
        config: The cache configuration.

    Example:
        Basic operations::

            cache = client.get_cache("my-cache")
            cache.put("key", "value")
            value = cache.get("key")

        With expiry::

            expiry = ExpiryPolicy.created_expiry_policy(Duration.minutes(30))
            cache.put("key", "value", expiry_policy=expiry)

        Entry processor::

            result = cache.invoke("key", MyEntryProcessor(), arg1, arg2)
    """

    SERVICE_NAME = "hz:impl:cacheService"

    def __init__(
        self,
        service_name: str,
        name: str,
        context: Optional[ProxyContext] = None,
        config: Optional[CacheConfig] = None,
    ):
        super().__init__(service_name, name, context)
        self._config = config or CacheConfig(name)
        self._default_expiry_policy = self._config.expiry_policy

    @property
    def config(self) -> CacheConfig:
        """Get the cache configuration."""
        return self._config

    def _serialize_expiry_policy(
        self, expiry_policy: Optional[ExpiryPolicy]
    ) -> Optional[bytes]:
        """Serialize an expiry policy for transmission."""
        if expiry_policy is None:
            return None
        return self._to_data(expiry_policy)

    def get(self, key: K) -> Optional[V]:
        """Get the value for a key.

        Returns the value associated with the specified key, or None if
        no mapping exists for the key.

        Args:
            key: The key whose value to retrieve.

        Returns:
            The value associated with the key, or None.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> value = cache.get("user:1")
            >>> if value is not None:
            ...     print(f"Found: {value}")
        """
        return self.get_async(key).result()

    def get_async(self, key: K, expiry_policy: Optional[ExpiryPolicy] = None) -> Future:
        """Get a value asynchronously.

        Args:
            key: The key to look up.
            expiry_policy: Optional custom expiry policy for this operation.

        Returns:
            A Future that will contain the value or None.
        """
        self._check_not_destroyed()

        key_data = self._to_data(key)
        policy = expiry_policy or self._default_expiry_policy
        policy_data = self._serialize_expiry_policy(policy)

        request = CacheCodec.encode_get_request(self._name, key_data, policy_data)

        def handle_response(response: "ClientMessage") -> Optional[V]:
            data = CacheCodec.decode_get_response(response)
            return self._to_object(data) if data else None

        return self._invoke(request, handle_response)

    def put(
        self,
        key: K,
        value: V,
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> None:
        """Put a key-value pair into the cache.

        Associates the specified value with the specified key. If the
        cache previously contained a mapping for the key, the old value
        is replaced.

        Args:
            key: The key to store.
            value: The value to associate with the key.
            expiry_policy: Optional custom expiry policy for this entry.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> cache.put("user:1", {"name": "Alice"})
        """
        self.put_async(key, value, expiry_policy).result()

    def put_async(
        self,
        key: K,
        value: V,
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> Future:
        """Put a key-value pair asynchronously.

        Args:
            key: The key to store.
            value: The value to associate.
            expiry_policy: Optional custom expiry policy.

        Returns:
            A Future that completes when the put is done.
        """
        self._check_not_destroyed()

        key_data = self._to_data(key)
        value_data = self._to_data(value)
        policy = expiry_policy or self._default_expiry_policy
        policy_data = self._serialize_expiry_policy(policy)

        request = CacheCodec.encode_put_request(
            self._name, key_data, value_data, policy_data, get=False
        )

        return self._invoke(request)

    def put_if_absent(
        self,
        key: K,
        value: V,
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> bool:
        """Put a value only if the key is not already in the cache.

        Args:
            key: The key to store.
            value: The value to associate.
            expiry_policy: Optional custom expiry policy.

        Returns:
            True if the value was put, False if the key already exists.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> added = cache.put_if_absent("key", "value")
            >>> if added:
            ...     print("New entry created")
        """
        return self.put_if_absent_async(key, value, expiry_policy).result()

    def put_if_absent_async(
        self,
        key: K,
        value: V,
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> Future:
        """Put if absent asynchronously.

        Args:
            key: The key to store.
            value: The value to associate.
            expiry_policy: Optional custom expiry policy.

        Returns:
            A Future that will contain True if successful.
        """
        self._check_not_destroyed()

        key_data = self._to_data(key)
        value_data = self._to_data(value)
        policy = expiry_policy or self._default_expiry_policy
        policy_data = self._serialize_expiry_policy(policy)

        request = CacheCodec.encode_put_if_absent_request(
            self._name, key_data, value_data, policy_data
        )

        def handle_response(response: "ClientMessage") -> bool:
            return CacheCodec.decode_put_if_absent_response(response)

        return self._invoke(request, handle_response)

    def get_and_put(
        self,
        key: K,
        value: V,
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> Optional[V]:
        """Put a value and return the previous value.

        Associates the specified value with the specified key, returning
        the value previously associated with the key, or None if there
        was no mapping.

        Args:
            key: The key to store.
            value: The value to associate.
            expiry_policy: Optional custom expiry policy.

        Returns:
            The previous value, or None.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> old_value = cache.get_and_put("key", "new_value")
            >>> print(f"Previous: {old_value}")
        """
        return self.get_and_put_async(key, value, expiry_policy).result()

    def get_and_put_async(
        self,
        key: K,
        value: V,
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> Future:
        """Get and put asynchronously.

        Args:
            key: The key to store.
            value: The value to associate.
            expiry_policy: Optional custom expiry policy.

        Returns:
            A Future that will contain the previous value.
        """
        self._check_not_destroyed()

        key_data = self._to_data(key)
        value_data = self._to_data(value)
        policy = expiry_policy or self._default_expiry_policy
        policy_data = self._serialize_expiry_policy(policy)

        request = CacheCodec.encode_get_and_put_request(
            self._name, key_data, value_data, policy_data
        )

        def handle_response(response: "ClientMessage") -> Optional[V]:
            data = CacheCodec.decode_get_and_put_response(response)
            return self._to_object(data) if data else None

        return self._invoke(request, handle_response)

    def remove(self, key: K) -> bool:
        """Remove a key from the cache.

        Removes the mapping for a key from the cache if it is present.

        Args:
            key: The key to remove.

        Returns:
            True if an entry was removed, False if the key was not found.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> removed = cache.remove("key")
        """
        return self.remove_async(key).result()

    def remove_async(self, key: K) -> Future:
        """Remove a key asynchronously.

        Args:
            key: The key to remove.

        Returns:
            A Future that will contain True if removed.
        """
        self._check_not_destroyed()

        key_data = self._to_data(key)
        request = CacheCodec.encode_remove_request(self._name, key_data)

        def handle_response(response: "ClientMessage") -> bool:
            return CacheCodec.decode_remove_response(response)

        return self._invoke(request, handle_response)

    def remove_if_same(self, key: K, old_value: V) -> bool:
        """Remove a key only if mapped to the specified value.

        Args:
            key: The key to remove.
            old_value: The expected current value.

        Returns:
            True if the entry was removed.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> removed = cache.remove_if_same("key", "expected_value")
        """
        return self.remove_if_same_async(key, old_value).result()

    def remove_if_same_async(self, key: K, old_value: V) -> Future:
        """Remove if same asynchronously.

        Args:
            key: The key to remove.
            old_value: The expected current value.

        Returns:
            A Future that will contain True if removed.
        """
        self._check_not_destroyed()

        key_data = self._to_data(key)
        value_data = self._to_data(old_value)
        request = CacheCodec.encode_remove_if_same_request(
            self._name, key_data, value_data
        )

        def handle_response(response: "ClientMessage") -> bool:
            return CacheCodec.decode_remove_if_same_response(response)

        return self._invoke(request, handle_response)

    def get_and_remove(self, key: K) -> Optional[V]:
        """Remove a key and return its value.

        Removes the entry for a key and returns the value that was
        associated with the key.

        Args:
            key: The key to remove.

        Returns:
            The previous value, or None if no mapping existed.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> removed_value = cache.get_and_remove("key")
        """
        return self.get_and_remove_async(key).result()

    def get_and_remove_async(self, key: K) -> Future:
        """Get and remove asynchronously.

        Args:
            key: The key to remove.

        Returns:
            A Future that will contain the removed value.
        """
        self._check_not_destroyed()

        key_data = self._to_data(key)
        request = CacheCodec.encode_get_and_remove_request(self._name, key_data)

        def handle_response(response: "ClientMessage") -> Optional[V]:
            data = CacheCodec.decode_get_and_remove_response(response)
            return self._to_object(data) if data else None

        return self._invoke(request, handle_response)

    def replace(
        self,
        key: K,
        value: V,
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> bool:
        """Replace the value for a key if it exists.

        Replaces the entry for a key only if currently mapped to some value.

        Args:
            key: The key whose value to replace.
            value: The new value.
            expiry_policy: Optional custom expiry policy.

        Returns:
            True if the value was replaced.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> replaced = cache.replace("key", "new_value")
        """
        return self.replace_async(key, value, expiry_policy).result()

    def replace_async(
        self,
        key: K,
        value: V,
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> Future:
        """Replace asynchronously.

        Args:
            key: The key whose value to replace.
            value: The new value.
            expiry_policy: Optional custom expiry policy.

        Returns:
            A Future that will contain True if replaced.
        """
        self._check_not_destroyed()

        key_data = self._to_data(key)
        value_data = self._to_data(value)
        policy = expiry_policy or self._default_expiry_policy
        policy_data = self._serialize_expiry_policy(policy)

        request = CacheCodec.encode_replace_request(
            self._name, key_data, value_data, policy_data
        )

        def handle_response(response: "ClientMessage") -> bool:
            data = CacheCodec.decode_replace_response(response)
            return data is not None

        return self._invoke(request, handle_response)

    def replace_if_same(
        self,
        key: K,
        old_value: V,
        new_value: V,
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> bool:
        """Replace the value only if it matches the expected value.

        Args:
            key: The key whose value to replace.
            old_value: The expected current value.
            new_value: The new value.
            expiry_policy: Optional custom expiry policy.

        Returns:
            True if the value was replaced.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> replaced = cache.replace_if_same("key", "old", "new")
        """
        return self.replace_if_same_async(key, old_value, new_value, expiry_policy).result()

    def replace_if_same_async(
        self,
        key: K,
        old_value: V,
        new_value: V,
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> Future:
        """Replace if same asynchronously.

        Args:
            key: The key whose value to replace.
            old_value: The expected current value.
            new_value: The new value.
            expiry_policy: Optional custom expiry policy.

        Returns:
            A Future that will contain True if replaced.
        """
        self._check_not_destroyed()

        key_data = self._to_data(key)
        old_value_data = self._to_data(old_value)
        new_value_data = self._to_data(new_value)
        policy = expiry_policy or self._default_expiry_policy
        policy_data = self._serialize_expiry_policy(policy)

        request = CacheCodec.encode_replace_if_same_request(
            self._name, key_data, old_value_data, new_value_data, policy_data
        )

        def handle_response(response: "ClientMessage") -> bool:
            return CacheCodec.decode_replace_if_same_response(response)

        return self._invoke(request, handle_response)

    def get_and_replace(
        self,
        key: K,
        value: V,
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> Optional[V]:
        """Replace and return the previous value.

        Replaces the value for a key and returns the old value.

        Args:
            key: The key whose value to replace.
            value: The new value.
            expiry_policy: Optional custom expiry policy.

        Returns:
            The previous value, or None if no mapping existed.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> old = cache.get_and_replace("key", "new_value")
        """
        return self.get_and_replace_async(key, value, expiry_policy).result()

    def get_and_replace_async(
        self,
        key: K,
        value: V,
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> Future:
        """Get and replace asynchronously.

        Args:
            key: The key whose value to replace.
            value: The new value.
            expiry_policy: Optional custom expiry policy.

        Returns:
            A Future that will contain the previous value.
        """
        self._check_not_destroyed()

        key_data = self._to_data(key)
        value_data = self._to_data(value)
        policy = expiry_policy or self._default_expiry_policy
        policy_data = self._serialize_expiry_policy(policy)

        request = CacheCodec.encode_get_and_replace_request(
            self._name, key_data, value_data, policy_data
        )

        def handle_response(response: "ClientMessage") -> Optional[V]:
            data = CacheCodec.decode_get_and_replace_response(response)
            return self._to_object(data) if data else None

        return self._invoke(request, handle_response)

    def contains_key(self, key: K) -> bool:
        """Check if the cache contains a key.

        Args:
            key: The key to check.

        Returns:
            True if the cache contains a mapping for the key.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> if cache.contains_key("user:1"):
            ...     print("User exists in cache")
        """
        return self.contains_key_async(key).result()

    def contains_key_async(self, key: K) -> Future:
        """Check if contains key asynchronously.

        Args:
            key: The key to check.

        Returns:
            A Future that will contain True if the key exists.
        """
        self._check_not_destroyed()

        key_data = self._to_data(key)
        request = CacheCodec.encode_contains_key_request(self._name, key_data)

        def handle_response(response: "ClientMessage") -> bool:
            return CacheCodec.decode_contains_key_response(response)

        return self._invoke(request, handle_response)

    def clear(self) -> None:
        """Remove all entries from the cache.

        Clears the contents of the cache without invoking any
        CacheEntryRemovedListeners or CacheWriters.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> cache.clear()
            >>> assert cache.size() == 0
        """
        self.clear_async().result()

    def clear_async(self) -> Future:
        """Clear the cache asynchronously.

        Returns:
            A Future that completes when the cache is cleared.
        """
        self._check_not_destroyed()

        request = CacheCodec.encode_clear_request(self._name)

        return self._invoke(request)

    def size(self) -> int:
        """Get the number of entries in the cache.

        Returns:
            The number of entries in the cache.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> count = cache.size()
            >>> print(f"Cache has {count} entries")
        """
        return self.size_async().result()

    def size_async(self) -> Future:
        """Get the cache size asynchronously.

        Returns:
            A Future that will contain the size.
        """
        self._check_not_destroyed()

        request = CacheCodec.encode_size_request(self._name)

        def handle_response(response: "ClientMessage") -> int:
            return CacheCodec.decode_size_response(response)

        return self._invoke(request, handle_response)

    def get_all(
        self,
        keys: Set[K],
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> Dict[K, V]:
        """Get multiple values at once.

        Gets a set of entries from the cache, returning them as a
        dictionary of key-value pairs.

        Args:
            keys: The keys to retrieve.
            expiry_policy: Optional custom expiry policy.

        Returns:
            A dictionary of found key-value pairs.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> entries = cache.get_all({"key1", "key2", "key3"})
        """
        return self.get_all_async(keys, expiry_policy).result()

    def get_all_async(
        self,
        keys: Set[K],
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> Future:
        """Get all asynchronously.

        Args:
            keys: The keys to retrieve.
            expiry_policy: Optional custom expiry policy.

        Returns:
            A Future that will contain a dictionary of results.
        """
        self._check_not_destroyed()

        if not keys:
            future: Future = Future()
            future.set_result({})
            return future

        keys_data = [self._to_data(k) for k in keys]
        policy = expiry_policy or self._default_expiry_policy
        policy_data = self._serialize_expiry_policy(policy)

        request = CacheCodec.encode_get_all_request(self._name, keys_data, policy_data)

        def handle_response(response: "ClientMessage") -> Dict[K, V]:
            entries = CacheCodec.decode_get_all_response(response)
            return {
                self._to_object(k): self._to_object(v)
                for k, v in entries
            }

        return self._invoke(request, handle_response)

    def put_all(
        self,
        entries: Dict[K, V],
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> None:
        """Put multiple entries at once.

        Copies all the mappings from the specified dictionary to the cache.

        Args:
            entries: The key-value pairs to put.
            expiry_policy: Optional custom expiry policy.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> cache.put_all({
            ...     "key1": "value1",
            ...     "key2": "value2",
            ... })
        """
        self.put_all_async(entries, expiry_policy).result()

    def put_all_async(
        self,
        entries: Dict[K, V],
        expiry_policy: Optional[ExpiryPolicy] = None,
    ) -> Future:
        """Put all asynchronously.

        Args:
            entries: The key-value pairs to put.
            expiry_policy: Optional custom expiry policy.

        Returns:
            A Future that completes when all entries are put.
        """
        self._check_not_destroyed()

        if not entries:
            future: Future = Future()
            future.set_result(None)
            return future

        entries_data = [
            (self._to_data(k), self._to_data(v))
            for k, v in entries.items()
        ]
        policy = expiry_policy or self._default_expiry_policy
        policy_data = self._serialize_expiry_policy(policy)

        request = CacheCodec.encode_put_all_request(self._name, entries_data, policy_data)

        return self._invoke(request)

    def remove_all(self, keys: Optional[Set[K]] = None) -> None:
        """Remove entries from the cache.

        If keys is specified, removes only those entries. Otherwise,
        removes all entries from the cache.

        Args:
            keys: Optional set of keys to remove. If None, removes all.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> cache.remove_all({"key1", "key2"})  # Remove specific
            >>> cache.remove_all()  # Remove all
        """
        self.remove_all_async(keys).result()

    def remove_all_async(self, keys: Optional[Set[K]] = None) -> Future:
        """Remove all asynchronously.

        Args:
            keys: Optional set of keys to remove.

        Returns:
            A Future that completes when removal is done.
        """
        self._check_not_destroyed()

        if keys is None:
            request = CacheCodec.encode_remove_all_request(self._name)
        else:
            if not keys:
                future: Future = Future()
                future.set_result(None)
                return future
            keys_data = [self._to_data(k) for k in keys]
            request = CacheCodec.encode_remove_all_keys_request(self._name, keys_data)

        return self._invoke(request)

    def invoke(
        self,
        key: K,
        entry_processor: CacheEntryProcessor[K, V],
        *arguments: Any,
    ) -> Any:
        """Execute an entry processor on a single key.

        Invokes an EntryProcessor against the entry specified by the
        provided key. The processor executes atomically.

        Args:
            key: The key to process.
            entry_processor: The entry processor to execute.
            *arguments: Additional arguments passed to the processor.

        Returns:
            The result from the entry processor.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> result = cache.invoke("key", IncrementProcessor(5))
        """
        return self.invoke_async(key, entry_processor, *arguments).result()

    def invoke_async(
        self,
        key: K,
        entry_processor: CacheEntryProcessor[K, V],
        *arguments: Any,
    ) -> Future:
        """Execute an entry processor asynchronously.

        Args:
            key: The key to process.
            entry_processor: The entry processor to execute.
            *arguments: Additional arguments passed to the processor.

        Returns:
            A Future that will contain the result.
        """
        self._check_not_destroyed()

        key_data = self._to_data(key)
        processor_data = self._to_data(entry_processor)
        args_data = [self._to_data(arg) for arg in arguments]

        request = CacheCodec.encode_entry_processor_request(
            self._name, key_data, processor_data, args_data
        )

        def handle_response(response: "ClientMessage") -> Any:
            data = CacheCodec.decode_entry_processor_response(response)
            return self._to_object(data) if data else None

        return self._invoke(request, handle_response)

    def invoke_all(
        self,
        keys: Set[K],
        entry_processor: CacheEntryProcessor[K, V],
        *arguments: Any,
    ) -> Dict[K, Any]:
        """Execute an entry processor on multiple keys.

        Invokes an EntryProcessor against all entries specified by the
        provided keys.

        Args:
            keys: The keys to process.
            entry_processor: The entry processor to execute.
            *arguments: Additional arguments passed to the processor.

        Returns:
            A dictionary mapping each key to its processor result.

        Raises:
            IllegalStateException: If the cache has been destroyed.

        Example:
            >>> results = cache.invoke_all({"k1", "k2"}, IncrementProcessor(1))
        """
        return self.invoke_all_async(keys, entry_processor, *arguments).result()

    def invoke_all_async(
        self,
        keys: Set[K],
        entry_processor: CacheEntryProcessor[K, V],
        *arguments: Any,
    ) -> Future:
        """Execute entry processor on multiple keys asynchronously.

        Args:
            keys: The keys to process.
            entry_processor: The entry processor to execute.
            *arguments: Additional arguments passed to the processor.

        Returns:
            A Future that will contain a dictionary of results.
        """
        self._check_not_destroyed()

        if not keys:
            future: Future = Future()
            future.set_result({})
            return future

        results: Dict[K, Any] = {}
        pending_futures: List[Future] = []

        for key in keys:
            key_future = self.invoke_async(key, entry_processor, *arguments)
            pending_futures.append((key, key_future))

        result_future: Future = Future()

        def collect_results() -> Dict[K, Any]:
            for key, f in pending_futures:
                try:
                    results[key] = f.result()
                except Exception as e:
                    results[key] = e
            return results

        try:
            result_future.set_result(collect_results())
        except Exception as e:
            result_future.set_exception(e)

        return result_future

    def _on_destroy(self) -> None:
        """Called when the cache is destroyed."""
        pass

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
