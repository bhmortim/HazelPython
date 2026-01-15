"""Basic Map Operations Example.

This example demonstrates fundamental operations with Hazelcast distributed maps,
including CRUD operations, entry listeners, and proper resource management.

Prerequisites:
    - A running Hazelcast cluster (default: localhost:5701)

Usage:
    python basic_map.py
"""

from hazelcast import HazelcastClient, ClientConfig
from hazelcast.proxy.map import EntryEvent, EntryListener


class MyEntryListener(EntryListener):
    """Custom entry listener that logs map events."""

    def entry_added(self, event: EntryEvent) -> None:
        print(f"  [ADDED] key={event.key}, value={event.value}")

    def entry_removed(self, event: EntryEvent) -> None:
        print(f"  [REMOVED] key={event.key}, old_value={event.old_value}")

    def entry_updated(self, event: EntryEvent) -> None:
        print(f"  [UPDATED] key={event.key}, old={event.old_value}, new={event.value}")


def main():
    # Create client configuration
    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    # Use context manager for automatic shutdown
    with HazelcastClient(config) as client:
        print(f"Connected to cluster: {config.cluster_name}")

        # Get a distributed map
        my_map = client.get_map("example-map")
        print(f"\nWorking with map: {my_map.name}")

        # Add an entry listener
        listener = MyEntryListener()
        registration_id = my_map.add_entry_listener(listener, include_value=True)
        print(f"Entry listener registered: {registration_id[:8]}...")

        # Basic CRUD operations
        print("\n--- PUT operations ---")
        my_map.put("user:1", {"name": "Alice", "age": 30})
        my_map.put("user:2", {"name": "Bob", "age": 25})
        my_map.put("user:3", {"name": "Charlie", "age": 35})

        print("\n--- GET operations ---")
        user1 = my_map.get("user:1")
        print(f"Retrieved user:1 = {user1}")

        print("\n--- CONTAINS_KEY operation ---")
        exists = my_map.contains_key("user:2")
        print(f"Key 'user:2' exists: {exists}")

        not_exists = my_map.contains_key("user:999")
        print(f"Key 'user:999' exists: {not_exists}")

        print("\n--- SIZE operation ---")
        size = my_map.size()
        print(f"Map size: {size}")

        print("\n--- PUT_IF_ABSENT operation ---")
        # This should not replace existing entry
        result = my_map.put_if_absent("user:1", {"name": "NewAlice", "age": 99})
        print(f"put_if_absent returned existing value: {result}")

        # This should add new entry
        result = my_map.put_if_absent("user:4", {"name": "Diana", "age": 28})
        print(f"put_if_absent for new key returned: {result}")

        print("\n--- REPLACE operation ---")
        old_value = my_map.replace("user:2", {"name": "Bob Updated", "age": 26})
        print(f"Replaced user:2, old value was: {old_value}")

        print("\n--- REMOVE operation ---")
        removed = my_map.remove("user:3")
        print(f"Removed user:3, value was: {removed}")

        print("\n--- GET_ALL operation ---")
        entries = my_map.get_all({"user:1", "user:2", "user:4"})
        for key, value in entries.items():
            print(f"  {key}: {value}")

        print("\n--- CLEAR operation ---")
        my_map.clear()
        print(f"Map cleared, new size: {my_map.size()}")

        # Remove the listener
        my_map.remove_entry_listener(registration_id)
        print("\nEntry listener removed")

    print("\nClient shutdown complete")


if __name__ == "__main__":
    main()
