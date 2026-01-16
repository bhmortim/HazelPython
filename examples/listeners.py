"""Hazelcast Listeners Example.

This example demonstrates the various listener types available in the
Hazelcast Python client for reacting to cluster and data events.

Listener types covered:
- LifecycleListener: Client state changes
- MembershipListener: Cluster membership changes
- DistributedObjectListener: Proxy creation/destruction
- EntryListener: Map entry events
- ItemListener: Collection item events
- MessageListener: Topic message events

Prerequisites:
    - A running Hazelcast cluster

Usage:
    python listeners.py
"""

import time
from typing import List

from hazelcast import HazelcastClient, ClientConfig
from hazelcast.listener import (
    LifecycleState,
    LifecycleEvent,
    LifecycleListener,
    MembershipListener,
    MembershipEvent,
    DistributedObjectListener,
    DistributedObjectEvent,
    DistributedObjectEventType,
)
from hazelcast.proxy.map import EntryEvent, EntryListener
from hazelcast.proxy.queue import ItemEvent, ItemEventType, ItemListener
from hazelcast.proxy.topic import TopicMessage, MessageListener


# =============================================================================
# CLASS-BASED LISTENERS
# =============================================================================

class MyLifecycleListener(LifecycleListener):
    """Custom lifecycle listener that tracks client state changes.
    
    Lifecycle states include:
    - STARTING: Client is initializing
    - CONNECTED: Successfully connected to cluster
    - DISCONNECTED: Lost connection to cluster
    - SHUTTING_DOWN: Client is shutting down
    - SHUTDOWN: Client has completed shutdown
    """

    def __init__(self):
        self.events: List[LifecycleEvent] = []

    def state_changed(self, event: LifecycleEvent) -> None:
        """Called when client lifecycle state changes."""
        self.events.append(event)
        print(f"  [LIFECYCLE] State changed: {event.state.name}")
        if event.previous_state:
            print(f"              Previous: {event.previous_state.name}")


class MyMembershipListener(MembershipListener):
    """Custom membership listener that tracks cluster membership changes.
    
    Notifies when:
    - A new member joins the cluster
    - An existing member leaves the cluster
    """

    def __init__(self):
        self.added_count = 0
        self.removed_count = 0

    def member_added(self, event: MembershipEvent) -> None:
        """Called when a new member joins the cluster."""
        self.added_count += 1
        print(f"  [MEMBERSHIP] Member added: {event.member}")
        print(f"               Cluster size: {len(event.members)}")

    def member_removed(self, event: MembershipEvent) -> None:
        """Called when a member leaves the cluster."""
        self.removed_count += 1
        print(f"  [MEMBERSHIP] Member removed: {event.member}")
        print(f"               Cluster size: {len(event.members)}")


class MyDistributedObjectListener(DistributedObjectListener):
    """Custom listener for distributed object lifecycle events.
    
    Notifies when:
    - A new distributed object (Map, Queue, etc.) is created
    - An existing distributed object is destroyed
    """

    def __init__(self):
        self.created: List[str] = []
        self.destroyed: List[str] = []

    def distributed_object_created(self, event: DistributedObjectEvent) -> None:
        """Called when a distributed object is created."""
        self.created.append(event.name)
        print(f"  [DIST_OBJECT] Created: {event.service_name}/{event.name}")

    def distributed_object_destroyed(self, event: DistributedObjectEvent) -> None:
        """Called when a distributed object is destroyed."""
        self.destroyed.append(event.name)
        print(f"  [DIST_OBJECT] Destroyed: {event.service_name}/{event.name}")


class MyEntryListener(EntryListener):
    """Custom entry listener for Map events.
    
    Tracks all types of entry events:
    - Added: New entry put into map
    - Removed: Entry removed from map
    - Updated: Existing entry value changed
    - Evicted: Entry evicted due to policy
    - Expired: Entry expired due to TTL
    - Merged: Entry merged after split-brain
    - Loaded: Entry loaded from MapStore
    """

    def __init__(self):
        self.events: List[str] = []

    def entry_added(self, event: EntryEvent) -> None:
        """Called when a new entry is added to the map."""
        self.events.append("added")
        print(f"  [ENTRY] Added: key={event.key}, value={event.value}")

    def entry_removed(self, event: EntryEvent) -> None:
        """Called when an entry is removed from the map."""
        self.events.append("removed")
        print(f"  [ENTRY] Removed: key={event.key}, old_value={event.old_value}")

    def entry_updated(self, event: EntryEvent) -> None:
        """Called when an existing entry is updated."""
        self.events.append("updated")
        print(f"  [ENTRY] Updated: key={event.key}")
        print(f"          old_value={event.old_value}, new_value={event.value}")

    def entry_evicted(self, event: EntryEvent) -> None:
        """Called when an entry is evicted from the map."""
        self.events.append("evicted")
        print(f"  [ENTRY] Evicted: key={event.key}")

    def entry_expired(self, event: EntryEvent) -> None:
        """Called when an entry expires due to TTL."""
        self.events.append("expired")
        print(f"  [ENTRY] Expired: key={event.key}")

    def entry_merged(self, event: EntryEvent) -> None:
        """Called when an entry is merged after split-brain recovery."""
        self.events.append("merged")
        print(f"  [ENTRY] Merged: key={event.key}")

    def entry_loaded(self, event: EntryEvent) -> None:
        """Called when an entry is loaded from MapStore."""
        self.events.append("loaded")
        print(f"  [ENTRY] Loaded: key={event.key}")


class MyItemListener(ItemListener):
    """Custom item listener for Queue, Set, and List events.
    
    Tracks:
    - Added: New item added to collection
    - Removed: Item removed from collection
    """

    def __init__(self):
        self.added_count = 0
        self.removed_count = 0

    def item_added(self, event: ItemEvent) -> None:
        """Called when an item is added to the collection."""
        self.added_count += 1
        print(f"  [ITEM] Added: {event.item}")

    def item_removed(self, event: ItemEvent) -> None:
        """Called when an item is removed from the collection."""
        self.removed_count += 1
        print(f"  [ITEM] Removed: {event.item}")


class MyMessageListener(MessageListener):
    """Custom message listener for Topic events.
    
    Receives messages published to a topic.
    """

    def __init__(self):
        self.messages: List[str] = []

    def on_message(self, message: TopicMessage) -> None:
        """Called when a message is published to the topic."""
        self.messages.append(message.message)
        print(f"  [MESSAGE] Received: {message.message}")
        print(f"            Published at: {message.publish_time}")
        if message.publishing_member:
            print(f"            From member: {message.publishing_member}")


# =============================================================================
# EXAMPLE FUNCTIONS
# =============================================================================

def lifecycle_listener_example():
    """Demonstrate LifecycleListener usage."""
    print("\n" + "=" * 50)
    print("Lifecycle Listener Example")
    print("=" * 50)

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    # Class-based listener
    print("\n--- Class-Based Listener ---")
    class_listener = MyLifecycleListener()

    client = HazelcastClient(config)
    reg_id = client.add_lifecycle_listener(listener=class_listener)
    print(f"Listener registered: {reg_id[:8]}...")

    # Do some operations to trigger events
    time.sleep(0.1)

    # Shutdown will trigger SHUTTING_DOWN and SHUTDOWN events
    client.shutdown()

    print(f"\nTotal events captured: {len(class_listener.events)}")

    # Function-based listener
    print("\n--- Function-Based Listener ---")
    events_log = []

    def on_state_changed(event: LifecycleEvent):
        events_log.append(event)
        print(f"  [CALLBACK] State: {event.state.name}")

    client2 = HazelcastClient(config)
    reg_id2 = client2.add_lifecycle_listener(on_state_changed=on_state_changed)
    print(f"Function listener registered: {reg_id2[:8]}...")

    client2.shutdown()
    print(f"Events via callback: {len(events_log)}")


def membership_listener_example():
    """Demonstrate MembershipListener usage."""
    print("\n" + "=" * 50)
    print("Membership Listener Example")
    print("=" * 50)

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    with HazelcastClient(config) as client:
        # Class-based listener
        print("\n--- Class-Based Listener ---")
        class_listener = MyMembershipListener()
        reg_id = client.add_membership_listener(listener=class_listener)
        print(f"Membership listener registered: {reg_id[:8]}...")

        # Function-based listener
        print("\n--- Function-Based Listener ---")

        def on_member_added(event: MembershipEvent):
            print(f"  [CALLBACK] Member joined: {event.member}")

        def on_member_removed(event: MembershipEvent):
            print(f"  [CALLBACK] Member left: {event.member}")

        reg_id2 = client.add_membership_listener(
            on_member_added=on_member_added,
            on_member_removed=on_member_removed,
        )
        print(f"Function listener registered: {reg_id2[:8]}...")

        print("\nNote: Membership events fire when cluster topology changes.")
        print("Start/stop cluster members to see events.")

        # Remove listeners
        client.remove_listener(reg_id)
        client.remove_listener(reg_id2)
        print("\nListeners removed")


def distributed_object_listener_example():
    """Demonstrate DistributedObjectListener usage."""
    print("\n" + "=" * 50)
    print("Distributed Object Listener Example")
    print("=" * 50)

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    with HazelcastClient(config) as client:
        # Add listener
        listener = MyDistributedObjectListener()
        reg_id = client.add_distributed_object_listener(listener)
        print(f"Distributed object listener registered: {reg_id[:8]}...")

        # Create some distributed objects
        print("\nCreating distributed objects...")
        map1 = client.get_map("listener-demo-map")
        queue1 = client.get_queue("listener-demo-queue")
        set1 = client.get_set("listener-demo-set")

        time.sleep(0.1)

        # Destroy objects
        print("\nDestroying distributed objects...")
        map1.destroy()
        queue1.destroy()
        set1.destroy()

        time.sleep(0.1)

        print(f"\nCreated objects: {listener.created}")
        print(f"Destroyed objects: {listener.destroyed}")

        # Remove listener
        client.remove_listener(reg_id)


def entry_listener_example():
    """Demonstrate EntryListener for Maps."""
    print("\n" + "=" * 50)
    print("Entry Listener Example (Map)")
    print("=" * 50)

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    with HazelcastClient(config) as client:
        my_map = client.get_map("entry-listener-demo")

        # Class-based listener
        print("\n--- Class-Based Entry Listener ---")
        class_listener = MyEntryListener()
        reg_id = my_map.add_entry_listener(
            listener=class_listener,
            include_value=True,  # Include values in events
        )
        print(f"Entry listener registered: {reg_id[:8]}...")

        # Trigger events
        print("\nTriggering entry events...")

        # Add entries
        my_map.put("user:1", {"name": "Alice", "role": "admin"})
        my_map.put("user:2", {"name": "Bob", "role": "user"})

        # Update entry
        my_map.put("user:1", {"name": "Alice", "role": "superadmin"})

        # Remove entry
        my_map.remove("user:2")

        time.sleep(0.1)

        print(f"\nEvents captured: {class_listener.events}")

        # Function-based listener
        print("\n--- Function-Based Entry Listener ---")
        added_keys = []
        removed_keys = []

        def on_added(event: EntryEvent):
            added_keys.append(event.key)
            print(f"  [FUNC] Added: {event.key}")

        def on_removed(event: EntryEvent):
            removed_keys.append(event.key)
            print(f"  [FUNC] Removed: {event.key}")

        def on_updated(event: EntryEvent):
            print(f"  [FUNC] Updated: {event.key}")

        reg_id2 = my_map.add_entry_listener(
            on_added=on_added,
            on_removed=on_removed,
            on_updated=on_updated,
            include_value=True,
        )
        print(f"Function listener registered: {reg_id2[:8]}...")

        # More operations
        my_map.put("user:3", {"name": "Charlie"})
        my_map.remove("user:3")

        time.sleep(0.1)

        print(f"\nAdded keys: {added_keys}")
        print(f"Removed keys: {removed_keys}")

        # Cleanup
        my_map.remove_entry_listener(reg_id)
        my_map.remove_entry_listener(reg_id2)
        my_map.clear()


def item_listener_example():
    """Demonstrate ItemListener for Queue, Set, and List."""
    print("\n" + "=" * 50)
    print("Item Listener Example (Queue/Set/List)")
    print("=" * 50)

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    with HazelcastClient(config) as client:
        # Queue example
        print("\n--- Queue Item Listener ---")
        my_queue = client.get_queue("item-listener-queue")

        queue_listener = MyItemListener()
        reg_id = my_queue.add_item_listener(
            listener=queue_listener,
            include_value=True,
        )

        my_queue.offer("task-1")
        my_queue.offer("task-2")
        item = my_queue.poll()

        time.sleep(0.1)
        print(f"Queue - Added: {queue_listener.added_count}, "
              f"Removed: {queue_listener.removed_count}")

        my_queue.remove_item_listener(reg_id)
        my_queue.clear()

        # Set example
        print("\n--- Set Item Listener ---")
        my_set = client.get_set("item-listener-set")

        def on_set_item_added(event: ItemEvent):
            print(f"  [SET] Item added: {event.item}")

        def on_set_item_removed(event: ItemEvent):
            print(f"  [SET] Item removed: {event.item}")

        reg_id2 = my_set.add_item_listener(
            on_added=on_set_item_added,
            on_removed=on_set_item_removed,
            include_value=True,
        )

        my_set.add("unique-1")
        my_set.add("unique-2")
        my_set.remove("unique-1")

        time.sleep(0.1)

        my_set.remove_item_listener(reg_id2)
        my_set.clear()

        # List example
        print("\n--- List Item Listener ---")
        my_list = client.get_list("item-listener-list")

        list_listener = MyItemListener()
        reg_id3 = my_list.add_item_listener(
            listener=list_listener,
            include_value=True,
        )

        my_list.add("item-a")
        my_list.add("item-b")
        my_list.add("item-a")  # Duplicates allowed
        my_list.remove("item-a")

        time.sleep(0.1)
        print(f"List - Added: {list_listener.added_count}, "
              f"Removed: {list_listener.removed_count}")

        my_list.remove_item_listener(reg_id3)
        my_list.clear()


def message_listener_example():
    """Demonstrate MessageListener for Topics."""
    print("\n" + "=" * 50)
    print("Message Listener Example (Topic)")
    print("=" * 50)

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    with HazelcastClient(config) as client:
        # Get a topic
        my_topic = client.get_topic("message-listener-demo")

        # Class-based listener
        print("\n--- Class-Based Message Listener ---")
        class_listener = MyMessageListener()
        reg_id = my_topic.add_message_listener(listener=class_listener)
        print(f"Message listener registered: {reg_id[:8]}...")

        # Publish messages
        print("\nPublishing messages...")
        my_topic.publish("Hello, subscribers!")
        my_topic.publish({"event": "user_login", "user_id": 123})
        my_topic.publish(["batch", "of", "data"])

        time.sleep(0.1)

        print(f"\nMessages received: {len(class_listener.messages)}")

        # Function-based listener
        print("\n--- Function-Based Message Listener ---")
        received = []

        def on_message(message: TopicMessage):
            received.append(message.message)
            print(f"  [CALLBACK] Got message: {message.message}")

        reg_id2 = my_topic.add_message_listener(on_message=on_message)
        print(f"Function listener registered: {reg_id2[:8]}...")

        my_topic.publish("Another message via callback")

        time.sleep(0.1)

        print(f"Received via callback: {received}")

        # Remove listeners
        my_topic.remove_message_listener(reg_id)
        my_topic.remove_message_listener(reg_id2)


def listener_registration_removal_example():
    """Demonstrate proper listener registration and removal patterns."""
    print("\n" + "=" * 50)
    print("Listener Registration and Removal")
    print("=" * 50)

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    with HazelcastClient(config) as client:
        my_map = client.get_map("registration-demo")

        # Track registration IDs
        registration_ids = []

        # Add multiple listeners
        print("\nAdding multiple listeners...")

        def listener_1(event: EntryEvent):
            print(f"  Listener 1: {event.key}")

        def listener_2(event: EntryEvent):
            print(f"  Listener 2: {event.key}")

        def listener_3(event: EntryEvent):
            print(f"  Listener 3: {event.key}")

        reg1 = my_map.add_entry_listener(on_added=listener_1, include_value=True)
        reg2 = my_map.add_entry_listener(on_added=listener_2, include_value=True)
        reg3 = my_map.add_entry_listener(on_added=listener_3, include_value=True)

        registration_ids.extend([reg1, reg2, reg3])
        print(f"Registered {len(registration_ids)} listeners")

        # Trigger events - all listeners fire
        print("\nWith all listeners active:")
        my_map.put("key1", "value1")
        time.sleep(0.1)

        # Remove one listener
        print("\nRemoving listener 2...")
        success = my_map.remove_entry_listener(reg2)
        print(f"Removal successful: {success}")

        # Now only 2 listeners fire
        print("\nWith listener 2 removed:")
        my_map.put("key2", "value2")
        time.sleep(0.1)

        # Remove remaining listeners
        print("\nRemoving remaining listeners...")
        my_map.remove_entry_listener(reg1)
        my_map.remove_entry_listener(reg3)

        # No listeners fire now
        print("\nWith all listeners removed:")
        my_map.put("key3", "value3")
        time.sleep(0.1)
        print("(No output expected)")

        # Cleanup
        my_map.clear()


def key_based_entry_listener_example():
    """Demonstrate entry listener for specific keys."""
    print("\n" + "=" * 50)
    print("Key-Based Entry Listener")
    print("=" * 50)

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    with HazelcastClient(config) as client:
        my_map = client.get_map("key-listener-demo")

        # Listen only to specific key
        target_key = "important-key"

        events_for_key = []

        def on_key_changed(event: EntryEvent):
            events_for_key.append(event)
            print(f"  [KEY LISTENER] Event for '{event.key}': {event.value}")

        # Register listener for specific key
        reg_id = my_map.add_entry_listener(
            on_added=on_key_changed,
            on_updated=on_key_changed,
            on_removed=on_key_changed,
            key=target_key,
            include_value=True,
        )
        print(f"Listening for changes to key: '{target_key}'")

        # Changes to other keys won't trigger listener
        print("\nPutting 'other-key' (should not trigger)...")
        my_map.put("other-key", "other-value")
        time.sleep(0.1)

        # Changes to target key will trigger listener
        print("\nPutting 'important-key' (should trigger)...")
        my_map.put(target_key, "initial-value")
        time.sleep(0.1)

        print("\nUpdating 'important-key' (should trigger)...")
        my_map.put(target_key, "updated-value")
        time.sleep(0.1)

        print(f"\nEvents captured for '{target_key}': {len(events_for_key)}")

        # Cleanup
        my_map.remove_entry_listener(reg_id)
        my_map.clear()


def main():
    print("Hazelcast Listeners Examples")
    print("=" * 50)
    print("""
Hazelcast provides various listeners for reacting to events:

Client-Level Listeners:
- LifecycleListener: Client state changes
- MembershipListener: Cluster topology changes
- DistributedObjectListener: Proxy lifecycle

Data Structure Listeners:
- EntryListener: Map entry changes
- ItemListener: Collection item changes
- MessageListener: Topic messages

Each listener can be implemented as:
- A class implementing the listener interface
- Function callbacks for specific event types
    """)

    # Run examples
    lifecycle_listener_example()
    membership_listener_example()
    distributed_object_listener_example()
    entry_listener_example()
    item_listener_example()
    message_listener_example()
    listener_registration_removal_example()
    key_based_entry_listener_example()

    print("\n" + "=" * 50)
    print("All listener examples completed!")
    print("=" * 50)


if __name__ == "__main__":
    main()
