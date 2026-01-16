"""Near Cache implementation for Hazelcast client.

This module provides a client-side cache that stores frequently accessed
data locally, reducing network round-trips to the cluster. Near caches
are particularly effective for read-heavy workloads with data that changes
infrequently.

Features:
- Multiple eviction policies (LRU, LFU, RANDOM, NONE)
- TTL and max-idle expiration
- Binary or object in-memory format
- Invalidation on cluster-side changes
- Statistics tracking

Example:
    Configure a near cache for a map::

        from hazelcast.config import ClientConfig, NearCacheConfig, EvictionPolicy

        config = ClientConfig()
        near_cache = NearCacheConfig(
            name="my-map",
            max_size=10000,
            time_to_live_seconds=300,
            eviction_policy=EvictionPolicy.LRU
        )
        config.add_near_cache(near_cache)

    Using the NearCacheManager::

        from hazelcast.near_cache import NearCacheManager, NearCacheConfig

        manager = NearCacheManager()
        config = NearCacheConfig(name="users", max_size=1000)
        cache = manager.get_or_create("users", config)

        cache.put("user:1", user_data)
        data = cache.get("user:1")  # Local lookup
"""

import threading
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Generic, Optional, TypeVar

from hazelcast.config import EvictionPolicy, InMemoryFormat, NearCacheConfig


K = TypeVar("K")
V = TypeVar("V")


@dataclass
class NearCacheStats:
    """Statistics for near cache operations.

    Tracks cache effectiveness including hits, misses, and various
    removal types (evictions, expirations, invalidations).

    Attributes:
        hits: Number of successful cache lookups.
        misses: Number of cache lookups that didn't find data.
        evictions: Number of entries removed due to capacity limits.
        expirations: Number of entries removed due to TTL/idle expiration.
        invalidations: Number of entries removed due to cluster updates.
        entries_count: Current number of entries in the cache.
        owned_entry_memory_cost: Estimated memory usage in bytes.
        creation_time: Timestamp when the cache was created.
    """

    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    invalidations: int = 0
    entries_count: int = 0
    owned_entry_memory_cost: int = 0
    creation_time: float = field(default_factory=time.time)

    @property
    def hit_ratio(self) -> float:
        """Calculate the cache hit ratio.

        Returns:
            Ratio of hits to total accesses (0.0 to 1.0).
        """
        total = self.hits + self.misses
        if total == 0:
            return 0.0
        return self.hits / total

    @property
    def miss_ratio(self) -> float:
        """Calculate the cache miss ratio.

        Returns:
            Ratio of misses to total accesses (0.0 to 1.0).
        """
        total = self.hits + self.misses
        if total == 0:
            return 0.0
        return self.misses / total

    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary.

        Returns:
            Dictionary representation of all statistics.
        """
        return {
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "expirations": self.expirations,
            "invalidations": self.invalidations,
            "entries_count": self.entries_count,
            "hit_ratio": self.hit_ratio,
            "miss_ratio": self.miss_ratio,
            "owned_entry_memory_cost": self.owned_entry_memory_cost,
            "creation_time": self.creation_time,
        }


@dataclass
class NearCacheRecord(Generic[V]):
    """A record stored in the near cache.

    Wraps a cached value with metadata for expiration and eviction.

    Attributes:
        value: The cached value.
        creation_time: When the record was created (Unix timestamp).
        last_access_time: When the record was last accessed.
        access_count: Number of times the record was accessed.
        ttl_seconds: Time-to-live in seconds (0 = no TTL).
        max_idle_seconds: Max idle time in seconds (0 = no limit).
    """

    value: V
    creation_time: float = field(default_factory=time.time)
    last_access_time: float = field(default_factory=time.time)
    access_count: int = 0
    ttl_seconds: int = 0
    max_idle_seconds: int = 0

    def is_expired(self) -> bool:
        """Check if this record has expired.

        Returns:
            ``True`` if the record has exceeded TTL or max idle time.
        """
        now = time.time()
        if self.ttl_seconds > 0:
            if now - self.creation_time > self.ttl_seconds:
                return True
        if self.max_idle_seconds > 0:
            if now - self.last_access_time > self.max_idle_seconds:
                return True
        return False

    def record_access(self) -> None:
        """Record an access to this record.

        Updates the last access time and increments the access count.
        """
        self.last_access_time = time.time()
        self.access_count += 1


class EvictionStrategy(ABC):
    """Base class for eviction strategies.

    Eviction strategies determine which entry to remove when the
    cache reaches its capacity limit.
    """

    @abstractmethod
    def select_for_eviction(
        self, records: Dict[Any, NearCacheRecord]
    ) -> Optional[Any]:
        """Select a key for eviction.

        Args:
            records: Dictionary of keys to cache records.

        Returns:
            Key to evict, or ``None`` if no eviction should occur.
        """
        pass


class LRUEvictionStrategy(EvictionStrategy):
    """Least Recently Used eviction strategy.

    Evicts the entry that was accessed longest ago.
    """

    def select_for_eviction(
        self, records: Dict[Any, NearCacheRecord]
    ) -> Optional[Any]:
        if not records:
            return None
        oldest_key = None
        oldest_time = float("inf")
        for key, record in records.items():
            if record.last_access_time < oldest_time:
                oldest_time = record.last_access_time
                oldest_key = key
        return oldest_key


class LFUEvictionStrategy(EvictionStrategy):
    """Least Frequently Used eviction strategy.

    Evicts the entry with the lowest access count.
    """

    def select_for_eviction(
        self, records: Dict[Any, NearCacheRecord]
    ) -> Optional[Any]:
        if not records:
            return None
        least_key = None
        least_count = float("inf")
        for key, record in records.items():
            if record.access_count < least_count:
                least_count = record.access_count
                least_key = key
        return least_key


class RandomEvictionStrategy(EvictionStrategy):
    """Random eviction strategy.

    Evicts a randomly selected entry.
    """

    def select_for_eviction(
        self, records: Dict[Any, NearCacheRecord]
    ) -> Optional[Any]:
        if not records:
            return None
        import random
        return random.choice(list(records.keys()))


class NoneEvictionStrategy(EvictionStrategy):
    """No eviction strategy - cache grows unbounded.

    Warning:
        Using this strategy may lead to memory issues if the cache
        is not bounded by TTL or external invalidation.
    """

    def select_for_eviction(
        self, records: Dict[Any, NearCacheRecord]
    ) -> Optional[Any]:
        return None


def _create_eviction_strategy(policy: EvictionPolicy) -> EvictionStrategy:
    """Create an eviction strategy for the given policy.

    Args:
        policy: The eviction policy enum value.

    Returns:
        Appropriate eviction strategy instance.
    """
    strategies = {
        EvictionPolicy.LRU: LRUEvictionStrategy,
        EvictionPolicy.LFU: LFUEvictionStrategy,
        EvictionPolicy.RANDOM: RandomEvictionStrategy,
        EvictionPolicy.NONE: NoneEvictionStrategy,
    }
    strategy_class = strategies.get(policy, LRUEvictionStrategy)
    return strategy_class()


class NearCache(Generic[K, V]):
    """Local cache for reducing remote calls to the cluster.

    Near cache stores frequently accessed data on the client side,
    reducing network round-trips and improving read performance.
    Supports various eviction policies, TTL/idle expiration, and
    automatic invalidation when data changes on the cluster.

    Type Parameters:
        K: Key type.
        V: Value type.

    Args:
        config: Near cache configuration.
        serialization_service: Optional serialization service for
            binary storage format.

    Attributes:
        name: The near cache name.
        config: The near cache configuration.
        stats: Cache statistics.
        size: Current number of entries.

    Example:
        >>> config = NearCacheConfig(
        ...     name="users",
        ...     max_size=1000,
        ...     time_to_live_seconds=300
        ... )
        >>> cache = NearCache(config)
        >>> cache.put("user:1", user_data)
        >>> data = cache.get("user:1")
    """

    def __init__(
        self,
        config: NearCacheConfig,
        serialization_service: Any = None,
    ):
        self._config = config
        self._serialization_service = serialization_service
        self._records: Dict[K, NearCacheRecord[V]] = {}
        self._stats = NearCacheStats()
        self._lock = threading.RLock()
        self._eviction_strategy = _create_eviction_strategy(config.eviction_policy)
        self._invalidation_listeners: list = []

    @property
    def name(self) -> str:
        """Get the near cache name."""
        return self._config.name

    @property
    def config(self) -> NearCacheConfig:
        """Get the near cache configuration."""
        return self._config

    @property
    def stats(self) -> NearCacheStats:
        """Get near cache statistics."""
        with self._lock:
            self._stats.entries_count = len(self._records)
            return self._stats

    @property
    def size(self) -> int:
        """Get the number of entries in the cache."""
        with self._lock:
            return len(self._records)

    def get(self, key: K) -> Optional[V]:
        """Get a value from the near cache.

        Returns the cached value if present and not expired. Updates
        statistics (hit/miss counts) and access metadata.

        Args:
            key: The key to look up.

        Returns:
            The cached value, or ``None`` if not found or expired.
        """
        with self._lock:
            record = self._records.get(key)
            if record is None:
                self._stats.misses += 1
                return None

            if record.is_expired():
                self._remove_record(key, is_expiration=True)
                self._stats.misses += 1
                return None

            record.record_access()
            self._stats.hits += 1
            return self._deserialize_value(record.value)

    def put(self, key: K, value: V) -> None:
        """Put a value into the near cache.

        Stores the value with the configured TTL and max idle time.
        May trigger eviction if the cache is at capacity.

        Args:
            key: The key to store.
            value: The value to cache.
        """
        with self._lock:
            self._evict_if_needed()
            stored_value = self._serialize_value(value)
            record = NearCacheRecord(
                value=stored_value,
                ttl_seconds=self._config.time_to_live_seconds,
                max_idle_seconds=self._config.max_idle_seconds,
            )
            self._records[key] = record

    def remove(self, key: K) -> Optional[V]:
        """Remove a value from the near cache.

        Args:
            key: The key to remove.

        Returns:
            The removed value, or None if not found.
        """
        with self._lock:
            record = self._records.pop(key, None)
            if record is None:
                return None
            return self._deserialize_value(record.value)

    def invalidate(self, key: K) -> None:
        """Invalidate a key in the cache.

        Removes the entry and notifies invalidation listeners.
        Typically called when the cluster-side data changes.

        Args:
            key: The key to invalidate.
        """
        with self._lock:
            if key in self._records:
                del self._records[key]
                self._stats.invalidations += 1
                self._notify_invalidation(key)

    def _notify_invalidation(self, key: K) -> None:
        """Notify listeners of an invalidation."""
        for _, listener in self._invalidation_listeners:
            try:
                listener(key)
            except Exception:
                pass

    def invalidate_all(self) -> None:
        """Invalidate all entries in the cache.

        Clears all entries and increments the invalidation count.
        """
        with self._lock:
            count = len(self._records)
            self._records.clear()
            self._stats.invalidations += count

    def clear(self) -> None:
        """Clear all entries from the cache."""
        with self._lock:
            self._records.clear()

    def contains(self, key: K) -> bool:
        """Check if a key exists in the cache.

        Args:
            key: The key to check.

        Returns:
            True if the key exists and is not expired.
        """
        with self._lock:
            record = self._records.get(key)
            if record is None:
                return False
            if record.is_expired():
                self._remove_record(key, is_expiration=True)
                return False
            return True

    def _evict_if_needed(self) -> None:
        """Evict entries if the cache is at capacity."""
        while len(self._records) >= self._config.max_size:
            key = self._eviction_strategy.select_for_eviction(self._records)
            if key is None:
                break
            self._remove_record(key, is_eviction=True)

    def _remove_record(
        self,
        key: K,
        is_eviction: bool = False,
        is_expiration: bool = False,
    ) -> None:
        """Remove a record and update statistics."""
        if key in self._records:
            del self._records[key]
            if is_eviction:
                self._stats.evictions += 1
            if is_expiration:
                self._stats.expirations += 1

    def _serialize_value(self, value: V) -> Any:
        """Serialize value based on in-memory format."""
        if self._config.in_memory_format == InMemoryFormat.BINARY:
            if self._serialization_service:
                return self._serialization_service.to_data(value)
            if isinstance(value, bytes):
                return value
            return value
        return value

    def _deserialize_value(self, value: Any) -> V:
        """Deserialize value based on in-memory format."""
        if self._config.in_memory_format == InMemoryFormat.BINARY:
            if self._serialization_service:
                return self._serialization_service.to_object(value)
        return value

    def add_invalidation_listener(
        self, listener: Callable[[K], None]
    ) -> str:
        """Add a listener for invalidation events.

        Args:
            listener: Callback invoked when keys are invalidated.
                Receives the invalidated key as argument.

        Returns:
            Registration ID for removing the listener.
        """
        import uuid
        reg_id = str(uuid.uuid4())
        self._invalidation_listeners.append((reg_id, listener))
        return reg_id

    def remove_invalidation_listener(self, registration_id: str) -> bool:
        """Remove an invalidation listener.

        Args:
            registration_id: The registration ID to remove.

        Returns:
            True if the listener was removed.
        """
        for i, (reg_id, _) in enumerate(self._invalidation_listeners):
            if reg_id == registration_id:
                del self._invalidation_listeners[i]
                return True
        return False

    def do_expiration(self) -> int:
        """Remove expired entries.

        Scans all entries and removes those that have exceeded
        their TTL or max idle time.

        Returns:
            Number of expired entries removed.
        """
        expired_count = 0
        with self._lock:
            expired_keys = [
                k for k, r in self._records.items() if r.is_expired()
            ]
            for key in expired_keys:
                self._remove_record(key, is_expiration=True)
                expired_count += 1
        return expired_count

    def invalidate_batch(self, keys: list) -> int:
        """Invalidate multiple keys at once.

        Args:
            keys: List of keys to invalidate.

        Returns:
            Number of entries invalidated.
        """
        count = 0
        with self._lock:
            for key in keys:
                if key in self._records:
                    del self._records[key]
                    self._stats.invalidations += 1
                    count += 1
        return count

    def get_stats_snapshot(self) -> NearCacheStats:
        """Get a snapshot of the current statistics.

        Returns:
            A copy of the current stats.
        """
        with self._lock:
            return NearCacheStats(
                hits=self._stats.hits,
                misses=self._stats.misses,
                evictions=self._stats.evictions,
                expirations=self._stats.expirations,
                invalidations=self._stats.invalidations,
                entries_count=len(self._records),
                owned_entry_memory_cost=self._stats.owned_entry_memory_cost,
                creation_time=self._stats.creation_time,
            )


class NearCacheManager:
    """Manages near caches for multiple maps.

    Provides centralized management of near cache instances,
    ensuring consistent configuration and lifecycle management.

    Args:
        serialization_service: Optional serialization service
            shared across all managed caches.

    Example:
        >>> manager = NearCacheManager(serialization_service)
        >>> config = NearCacheConfig(name="users", max_size=1000)
        >>> cache = manager.get_or_create("users", config)
    """

    def __init__(self, serialization_service: Any = None):
        self._caches: Dict[str, NearCache] = {}
        self._serialization_service = serialization_service
        self._lock = threading.Lock()

    def get_or_create(
        self, name: str, config: NearCacheConfig
    ) -> NearCache:
        """Get or create a near cache for the given name.

        Creates a new cache if one doesn't exist, otherwise returns
        the existing instance.

        Args:
            name: The map name.
            config: Near cache configuration.

        Returns:
            The near cache instance.
        """
        with self._lock:
            if name not in self._caches:
                self._caches[name] = NearCache(
                    config, self._serialization_service
                )
            return self._caches[name]

    def get(self, name: str) -> Optional[NearCache]:
        """Get a near cache by name.

        Args:
            name: The map name.

        Returns:
            The near cache or ``None`` if not found.
        """
        with self._lock:
            return self._caches.get(name)

    def destroy(self, name: str) -> None:
        """Destroy a near cache.

        Clears and removes the cache from management.

        Args:
            name: The map name.
        """
        with self._lock:
            cache = self._caches.pop(name, None)
            if cache:
                cache.clear()

    def destroy_all(self) -> None:
        """Destroy all managed near caches.

        Clears and removes all caches from management.
        """
        with self._lock:
            for cache in self._caches.values():
                cache.clear()
            self._caches.clear()

    def list_all(self) -> Dict[str, NearCacheStats]:
        """List all near caches with their stats.

        Returns:
            Dictionary mapping cache names to their statistics.
        """
        with self._lock:
            return {name: cache.stats for name, cache in self._caches.items()}
