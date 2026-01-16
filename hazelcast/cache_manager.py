"""JCache CacheManager implementation.

This module provides the `CacheManager` class for managing JCache instances
in a Hazelcast cluster.

Example:
    Basic usage::

        from hazelcast import HazelcastClient
        from hazelcast.cache_manager import CacheManager
        from hazelcast.proxy.cache import CacheConfig

        client = HazelcastClient()
        cache_manager = client.get_cache_manager()

        config = CacheConfig("my-cache")
        cache = cache_manager.create_cache("my-cache", config)
        cache.put("key", "value")

        # Or get existing cache
        cache = cache_manager.get_cache("my-cache")
"""

import threading
from typing import Dict, Optional, TYPE_CHECKING

from hazelcast.proxy.cache import Cache, CacheConfig
from hazelcast.exceptions import IllegalStateException

if TYPE_CHECKING:
    from hazelcast.proxy.base import ProxyContext


class CacheManager:
    """JCache CacheManager for managing cache instances.

    Provides a factory and manager for named Cache instances. Caches
    can be created with custom configurations or retrieved by name.

    This class is typically obtained from a HazelcastClient instance
    via `client.get_cache_manager()`.

    Attributes:
        uri: The URI of this cache manager (usually the cluster name).
        is_closed: Whether this cache manager has been closed.

    Example:
        >>> cache_manager = client.get_cache_manager()
        >>> cache = cache_manager.get_cache("my-cache")
        >>> cache.put("key", "value")
    """

    def __init__(
        self,
        uri: str,
        context: Optional["ProxyContext"] = None,
    ):
        """Initialize the CacheManager.

        Args:
            uri: The URI identifying this cache manager.
            context: The proxy context for creating cache instances.
        """
        self._uri = uri
        self._context = context
        self._caches: Dict[str, Cache] = {}
        self._lock = threading.Lock()
        self._closed = False

    @property
    def uri(self) -> str:
        """Get the URI of this cache manager."""
        return self._uri

    @property
    def is_closed(self) -> bool:
        """Check if this cache manager is closed."""
        return self._closed

    def _check_not_closed(self) -> None:
        """Raise if the cache manager is closed."""
        if self._closed:
            raise IllegalStateException("CacheManager is closed")

    def create_cache(
        self,
        name: str,
        config: Optional[CacheConfig] = None,
    ) -> Cache:
        """Create a new cache with the given name and configuration.

        Creates and returns a new Cache with the specified configuration.
        If a cache with the given name already exists, raises an exception.

        Args:
            name: The name for the cache.
            config: Optional configuration for the cache.

        Returns:
            The newly created Cache instance.

        Raises:
            IllegalStateException: If a cache with this name already exists
                or if the manager is closed.

        Example:
            >>> config = CacheConfig("users", statistics_enabled=True)
            >>> cache = cache_manager.create_cache("users", config)
        """
        self._check_not_closed()

        with self._lock:
            if name in self._caches:
                raise IllegalStateException(f"Cache '{name}' already exists")

            cache_config = config or CacheConfig(name)
            cache = Cache(
                Cache.SERVICE_NAME,
                name,
                self._context,
                cache_config,
            )
            self._caches[name] = cache
            return cache

    def get_cache(self, name: str) -> Optional[Cache]:
        """Get an existing cache by name.

        Returns the Cache with the specified name, or None if no such
        cache exists.

        Args:
            name: The name of the cache to get.

        Returns:
            The Cache instance, or None if not found.

        Raises:
            IllegalStateException: If the manager is closed.

        Example:
            >>> cache = cache_manager.get_cache("users")
            >>> if cache:
            ...     user = cache.get("user:1")
        """
        self._check_not_closed()

        with self._lock:
            return self._caches.get(name)

    def get_or_create_cache(
        self,
        name: str,
        config: Optional[CacheConfig] = None,
    ) -> Cache:
        """Get an existing cache or create a new one.

        Returns the Cache with the specified name if it exists, otherwise
        creates a new cache with the given configuration.

        Args:
            name: The name of the cache.
            config: Optional configuration for creating a new cache.

        Returns:
            The existing or newly created Cache.

        Raises:
            IllegalStateException: If the manager is closed.

        Example:
            >>> cache = cache_manager.get_or_create_cache("users")
        """
        self._check_not_closed()

        with self._lock:
            if name in self._caches:
                cache = self._caches[name]
                if not cache.is_destroyed:
                    return cache

            cache_config = config or CacheConfig(name)
            cache = Cache(
                Cache.SERVICE_NAME,
                name,
                self._context,
                cache_config,
            )
            self._caches[name] = cache
            return cache

    def destroy_cache(self, name: str) -> None:
        """Destroy a cache.

        Destroys and removes the cache with the specified name.
        After destruction, the cache cannot be used.

        Args:
            name: The name of the cache to destroy.

        Raises:
            IllegalStateException: If the manager is closed.

        Example:
            >>> cache_manager.destroy_cache("old-cache")
        """
        self._check_not_closed()

        with self._lock:
            cache = self._caches.pop(name, None)
            if cache is not None and not cache.is_destroyed:
                cache.destroy()

    def get_cache_names(self) -> frozenset:
        """Get the names of all caches managed by this manager.

        Returns:
            A frozenset of cache names.

        Raises:
            IllegalStateException: If the manager is closed.

        Example:
            >>> names = cache_manager.get_cache_names()
            >>> print(f"Caches: {names}")
        """
        self._check_not_closed()

        with self._lock:
            return frozenset(self._caches.keys())

    def close(self) -> None:
        """Close this cache manager.

        Closes the CacheManager and destroys all managed caches.
        After closing, no operations can be performed on this manager.

        Example:
            >>> cache_manager.close()
        """
        if self._closed:
            return

        with self._lock:
            self._closed = True
            for cache in list(self._caches.values()):
                if not cache.is_destroyed:
                    try:
                        cache.destroy()
                    except Exception:
                        pass
            self._caches.clear()

    def __enter__(self) -> "CacheManager":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - closes the manager."""
        self.close()

    def __repr__(self) -> str:
        return f"CacheManager(uri={self._uri!r}, caches={len(self._caches)})"
