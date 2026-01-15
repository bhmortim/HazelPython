"""Near Cache Example.

This example demonstrates how to configure and use Near Cache for
reduced latency on frequently accessed map data.

Near Cache stores a local copy of map entries on the client side,
dramatically reducing network round-trips for read operations.

Prerequisites:
    - A running Hazelcast cluster

Usage:
    python near_cache.py
"""

import time

from hazelcast import HazelcastClient, ClientConfig
from hazelcast.config import NearCacheConfig, EvictionPolicy, InMemoryFormat
from hazelcast.near_cache import NearCache, NearCacheManager


def configure_near_cache_example() -> ClientConfig:
    """Create a client configuration with near cache settings."""
    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    # Configure near cache for "products" map with LRU eviction
    products_cache = NearCacheConfig(
        name="products",
        max_size=1000,
        time_to_live_seconds=300,      # Entries expire after 5 minutes
        max_idle_seconds=60,           # Entries idle for 1 minute are evicted
        eviction_policy=EvictionPolicy.LRU,
        in_memory_format=InMemoryFormat.OBJECT,
        invalidate_on_change=True,     # Invalidate when server-side changes
    )
    config.add_near_cache(products_cache)

    # Configure near cache for "users" map with LFU eviction
    users_cache = NearCacheConfig(
        name="users",
        max_size=500,
        time_to_live_seconds=600,      # 10 minutes TTL
        eviction_policy=EvictionPolicy.LFU,
        in_memory_format=InMemoryFormat.BINARY,
    )
    config.add_near_cache(users_cache)

    print("Near cache configurations:")
    for name, nc_config in config.near_caches.items():
        print(f"  {name}:")
        print(f"    max_size={nc_config.max_size}")
        print(f"    eviction_policy={nc_config.eviction_policy.value}")
        print(f"    ttl={nc_config.time_to_live_seconds}s")
        print(f"    max_idle={nc_config.max_idle_seconds}s")

    return config


def demonstrate_near_cache_operations():
    """Show how near cache affects map operations."""
    print("\n--- Near Cache Operations Demo ---")

    # Create a near cache directly for demonstration
    config = NearCacheConfig(
        name="demo-cache",
        max_size=100,
        time_to_live_seconds=10,
        max_idle_seconds=5,
        eviction_policy=EvictionPolicy.LRU,
    )

    cache: NearCache = NearCache(config)

    # Populate the cache
    print("\nPopulating cache...")
    for i in range(10):
        cache.put(f"key{i}", f"value{i}")

    print(f"Cache size: {cache.size}")

    # Demonstrate cache hits
    print("\n--- Cache Hits ---")
    for i in range(5):
        value = cache.get(f"key{i}")
        print(f"  get('key{i}') = {value}")

    # Check stats
    stats = cache.stats
    print(f"\nCache Statistics:")
    print(f"  Hits: {stats.hits}")
    print(f"  Misses: {stats.misses}")
    print(f"  Hit Ratio: {stats.hit_ratio:.2%}")
    print(f"  Entries: {stats.entries_count}")

    # Demonstrate cache miss
    print("\n--- Cache Miss ---")
    value = cache.get("nonexistent")
    print(f"  get('nonexistent') = {value}")

    stats = cache.stats
    print(f"  Misses after: {stats.misses}")

    # Demonstrate invalidation
    print("\n--- Invalidation ---")
    cache.invalidate("key0")
    print(f"  Invalidated 'key0'")
    print(f"  Invalidations: {cache.stats.invalidations}")

    value = cache.get("key0")
    print(f"  get('key0') after invalidation = {value}")

    return cache


def demonstrate_eviction():
    """Show eviction behavior when cache is full."""
    print("\n--- Eviction Demo ---")

    # Small cache to demonstrate eviction
    config = NearCacheConfig(
        name="small-cache",
        max_size=5,
        eviction_policy=EvictionPolicy.LRU,
    )

    cache: NearCache = NearCache(config)

    # Fill the cache
    print("Filling cache (max_size=5)...")
    for i in range(5):
        cache.put(f"key{i}", f"value{i}")
        print(f"  Added key{i}, size={cache.size}")

    # Access some keys to make them "recently used"
    print("\nAccessing key0 and key1 (making them recently used)...")
    cache.get("key0")
    cache.get("key1")

    # Add more items, triggering eviction of least recently used
    print("\nAdding more items (should trigger LRU eviction)...")
    for i in range(5, 8):
        cache.put(f"key{i}", f"value{i}")
        print(f"  Added key{i}, size={cache.size}, evictions={cache.stats.evictions}")

    # Check which keys remain
    print("\nRemaining keys:")
    for i in range(8):
        value = cache.get(f"key{i}")
        if value is not None:
            print(f"  key{i} = {value}")


def demonstrate_expiration():
    """Show TTL and max-idle expiration."""
    print("\n--- Expiration Demo ---")

    config = NearCacheConfig(
        name="expiring-cache",
        max_size=100,
        time_to_live_seconds=2,  # Short TTL for demo
        max_idle_seconds=1,      # Short idle time for demo
    )

    cache: NearCache = NearCache(config)

    # Add an entry
    cache.put("expiring-key", "expiring-value")
    print(f"Added 'expiring-key', TTL=2s, max_idle=1s")

    # Access immediately
    value = cache.get("expiring-key")
    print(f"Immediate get: {value}")

    # Wait and check (should still be there if accessed within idle time)
    print("Waiting 0.5 seconds...")
    time.sleep(0.5)
    value = cache.get("expiring-key")
    print(f"After 0.5s get: {value}")

    # Wait longer than max_idle
    print("Waiting 1.5 seconds (exceeds max_idle)...")
    time.sleep(1.5)

    # Trigger expiration check
    expired_count = cache.do_expiration()
    print(f"Expired entries removed: {expired_count}")

    value = cache.get("expiring-key")
    print(f"After expiration: {value}")
    print(f"Expirations stat: {cache.stats.expirations}")


def demonstrate_invalidation_listener():
    """Show how to listen for cache invalidations."""
    print("\n--- Invalidation Listener Demo ---")

    config = NearCacheConfig(
        name="listener-cache",
        max_size=100,
    )

    cache: NearCache = NearCache(config)

    # Add invalidation listener
    invalidated_keys = []

    def on_invalidation(key):
        invalidated_keys.append(key)
        print(f"  [INVALIDATION] Key '{key}' was invalidated")

    reg_id = cache.add_invalidation_listener(on_invalidation)
    print(f"Listener registered: {reg_id[:8]}...")

    # Add some entries
    for i in range(3):
        cache.put(f"item{i}", f"data{i}")

    # Invalidate entries
    print("\nInvalidating entries...")
    cache.invalidate("item0")
    cache.invalidate("item1")

    print(f"\nInvalidated keys: {invalidated_keys}")

    # Remove listener
    cache.remove_invalidation_listener(reg_id)
    print("Listener removed")


def demonstrate_near_cache_manager():
    """Show NearCacheManager for managing multiple caches."""
    print("\n--- Near Cache Manager Demo ---")

    manager = NearCacheManager()

    # Create caches for different maps
    maps = ["orders", "customers", "inventory"]
    for map_name in maps:
        config = NearCacheConfig(
            name=map_name,
            max_size=1000,
            eviction_policy=EvictionPolicy.LRU,
        )
        cache = manager.get_or_create(map_name, config)
        # Populate with some data
        for i in range(10):
            cache.put(f"{map_name}:{i}", {"id": i, "map": map_name})

    # List all caches with stats
    print("\nAll managed caches:")
    all_stats = manager.list_all()
    for name, stats in all_stats.items():
        print(f"  {name}:")
        print(f"    entries={stats.entries_count}")
        print(f"    hit_ratio={stats.hit_ratio:.2%}")

    # Access specific cache
    orders_cache = manager.get("orders")
    if orders_cache:
        value = orders_cache.get("orders:5")
        print(f"\nRetrieved from orders cache: {value}")

    # Destroy a specific cache
    print("\nDestroying 'inventory' cache...")
    manager.destroy("inventory")

    print("Remaining caches:", list(manager.list_all().keys()))

    # Destroy all
    manager.destroy_all()
    print("All caches destroyed")


def main():
    # Show configuration
    config = configure_near_cache_example()

    # Demonstrate near cache behavior (without connecting to cluster)
    demonstrate_near_cache_operations()
    demonstrate_eviction()
    demonstrate_expiration()
    demonstrate_invalidation_listener()
    demonstrate_near_cache_manager()

    print("\n--- Integration with HazelcastClient ---")
    print("To use near cache with a real cluster:")
    print("""
    config = ClientConfig()
    config.add_near_cache(NearCacheConfig(
        name="my-map",
        max_size=10000,
        eviction_policy=EvictionPolicy.LRU,
        time_to_live_seconds=300,
    ))

    with HazelcastClient(config) as client:
        my_map = client.get_map("my-map")
        # Near cache is automatically used for this map
        my_map.put("key", "value")
        value = my_map.get("key")  # First call goes to cluster
        value = my_map.get("key")  # Second call hits near cache
    """)


if __name__ == "__main__":
    main()
