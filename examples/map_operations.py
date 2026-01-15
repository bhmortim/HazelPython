"""Map operations example for the Hazelcast Python client.

This example demonstrates advanced Map operations:
- Bulk operations (get_all, put_all)
- Entry listeners
- Conditional operations (put_if_absent, replace)
- Entry processing
"""

from hazelcast import HazelcastClient, ClientConfig
from hazelcast.proxy import MapProxy, EntryEvent, EntryListener


class MyEntryListener(EntryListener):
    """Custom entry listener for map events."""

    def entry_added(self, event: EntryEvent):
        print(f"Entry added: {event.key} -> {event.value}")

    def entry_removed(self, event: EntryEvent):
        print(f"Entry removed: {event.key}")

    def entry_updated(self, event: EntryEvent):
        print(f"Entry updated: {event.key} -> {event.value} (was {event.old_value})")

    def entry_evicted(self, event: EntryEvent):
        print(f"Entry evicted: {event.key}")


def main():
    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    client = HazelcastClient(config)

    try:
        client.start()

        # Get a map
        user_map: MapProxy = MapProxy("users")

        # Add entry listener
        listener = MyEntryListener()
        registration_id = user_map.add_entry_listener(listener, include_value=True)
        print(f"Listener registered: {registration_id}")

        # Bulk put
        print("\n--- Bulk Operations ---")
        users = {
            "user1": {"name": "Alice", "age": 30},
            "user2": {"name": "Bob", "age": 25},
            "user3": {"name": "Charlie", "age": 35},
        }
        user_map.put_all(users)
        print(f"Added {len(users)} users")

        # Bulk get
        keys = {"user1", "user2"}
        result = user_map.get_all(keys)
        print(f"Retrieved users: {result}")

        # Conditional operations
        print("\n--- Conditional Operations ---")

        # put_if_absent - only puts if key doesn't exist
        existing = user_map.put_if_absent("user1", {"name": "Alice2"})
        print(f"put_if_absent('user1'): existing={existing}")

        new_result = user_map.put_if_absent("user4", {"name": "David", "age": 28})
        print(f"put_if_absent('user4'): result={new_result}")

        # replace - only replaces if key exists
        old_value = user_map.replace("user1", {"name": "Alice", "age": 31})
        print(f"replace('user1'): old={old_value}")

        # replace_if_same - atomic compare-and-set
        success = user_map.replace_if_same(
            "user2",
            {"name": "Bob", "age": 25},
            {"name": "Bob", "age": 26}
        )
        print(f"replace_if_same('user2'): success={success}")

        # Locking
        print("\n--- Locking ---")
        user_map.lock("user1")
        print("Locked user1")

        is_locked = user_map.is_locked("user1")
        print(f"user1 is locked: {is_locked}")

        user_map.unlock("user1")
        print("Unlocked user1")

        # Remove listener
        user_map.remove_entry_listener(registration_id)
        print("\nListener removed")

        # Cleanup
        user_map.clear()

    finally:
        client.shutdown()


if __name__ == "__main__":
    main()
