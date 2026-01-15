"""Basic usage example for the Hazelcast Python client.

This example demonstrates how to:
- Create and configure a client
- Connect to a Hazelcast cluster
- Use the Map distributed data structure
- Handle lifecycle events
"""

from hazelcast import HazelcastClient, ClientConfig
from hazelcast.listener import LifecycleEvent


def main():
    # Create configuration
    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    # Create client
    client = HazelcastClient(config)

    # Add a lifecycle listener
    def on_lifecycle_change(event: LifecycleEvent):
        print(f"Lifecycle: {event}")

    client.add_lifecycle_listener(on_state_changed=on_lifecycle_change)

    try:
        # Start the client
        client.start()
        print(f"Connected to cluster: {config.cluster_name}")

        # Get a distributed map
        from hazelcast.proxy import MapProxy
        my_map = MapProxy("my-map")

        # Put some data
        print("\nPutting data into the map...")
        my_map.put("key1", "value1")
        my_map.put("key2", "value2")
        my_map.put("key3", "value3")

        # Get data
        print(f"key1 = {my_map.get('key1')}")
        print(f"key2 = {my_map.get('key2')}")

        # Check if key exists
        print(f"Contains 'key1': {my_map.contains_key('key1')}")
        print(f"Contains 'key4': {my_map.contains_key('key4')}")

        # Get map size
        print(f"Map size: {my_map.size()}")

        # Remove an entry
        removed = my_map.remove("key2")
        print(f"Removed key2: {removed}")

        # Iterate over keys
        print("\nAll keys:")
        for key in my_map.key_set():
            print(f"  {key}")

        # Clear the map
        my_map.clear()
        print(f"\nMap cleared. Size: {my_map.size()}")

    finally:
        # Shutdown the client
        client.shutdown()
        print("\nClient shutdown complete")


if __name__ == "__main__":
    main()
