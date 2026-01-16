Events and Listeners
====================

The Hazelcast Python Client provides various listeners to receive
notifications about cluster and data structure changes.

Lifecycle Listeners
-------------------

Monitor client connection state changes:

.. code-block:: python

    from hazelcast.listener import LifecycleListener, LifecycleEvent, LifecycleState

    class MyLifecycleListener(LifecycleListener):
        def on_state_changed(self, event: LifecycleEvent) -> None:
            print(f"State changed: {event.previous_state} -> {event.state}")

            if event.state == LifecycleState.CONNECTED:
                print("Client connected to cluster")
            elif event.state == LifecycleState.DISCONNECTED:
                print("Client disconnected")
            elif event.state == LifecycleState.SHUTDOWN:
                print("Client shut down")

    # Register listener
    reg_id = client.add_lifecycle_listener(MyLifecycleListener())

    # Or use callback function
    reg_id = client.add_lifecycle_listener(
        on_state_changed=lambda e: print(f"State: {e.state}")
    )

    # Remove listener
    client.remove_listener(reg_id)

Lifecycle States
~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - State
     - Description
   * - ``STARTING``
     - Client is starting up
   * - ``CONNECTED``
     - Client connected to cluster
   * - ``DISCONNECTED``
     - Client disconnected from cluster
   * - ``SHUTTING_DOWN``
     - Client is shutting down
   * - ``SHUTDOWN``
     - Client has shut down

Membership Listeners
--------------------

Monitor cluster membership changes:

.. code-block:: python

    from hazelcast.listener import MembershipListener, MembershipEvent

    class MyMembershipListener(MembershipListener):
        def on_member_added(self, event: MembershipEvent) -> None:
            print(f"Member added: {event.member}")

        def on_member_removed(self, event: MembershipEvent) -> None:
            print(f"Member removed: {event.member}")

    reg_id = client.add_membership_listener(MyMembershipListener())

    # Or use callbacks
    reg_id = client.add_membership_listener(
        on_member_added=lambda e: print(f"Added: {e.member}"),
        on_member_removed=lambda e: print(f"Removed: {e.member}"),
    )

Distributed Object Listeners
----------------------------

Monitor creation and destruction of distributed objects:

.. code-block:: python

    from hazelcast.listener import (
        DistributedObjectListener,
        DistributedObjectEvent,
        DistributedObjectEventType,
    )

    class MyObjectListener(DistributedObjectListener):
        def on_created(self, event: DistributedObjectEvent) -> None:
            print(f"Created: {event.service_name}/{event.name}")

        def on_destroyed(self, event: DistributedObjectEvent) -> None:
            print(f"Destroyed: {event.service_name}/{event.name}")

    reg_id = client.add_distributed_object_listener(MyObjectListener())

Entry Listeners (Map)
---------------------

Monitor map entry changes:

.. code-block:: python

    from hazelcast.proxy.map import EntryListener, EntryEvent

    class MapEntryListener(EntryListener):
        def entry_added(self, event: EntryEvent) -> None:
            print(f"Added: {event.key} = {event.value}")

        def entry_removed(self, event: EntryEvent) -> None:
            print(f"Removed: {event.key}")

        def entry_updated(self, event: EntryEvent) -> None:
            print(f"Updated: {event.key}: {event.old_value} -> {event.value}")

        def entry_evicted(self, event: EntryEvent) -> None:
            print(f"Evicted: {event.key}")

        def entry_expired(self, event: EntryEvent) -> None:
            print(f"Expired: {event.key}")

        def map_evicted(self, event: EntryEvent) -> None:
            print("Map evicted")

        def map_cleared(self, event: EntryEvent) -> None:
            print("Map cleared")

    my_map = client.get_map("my-map")

    # Listen to all keys
    reg_id = my_map.add_entry_listener(
        MapEntryListener(),
        include_value=True,
    )

    # Listen to specific key
    reg_id = my_map.add_entry_listener(
        MapEntryListener(),
        include_value=True,
        key="specific-key",
    )

    # Remove listener
    my_map.remove_entry_listener(reg_id)

Entry Event Types
~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Type
     - Description
   * - ``ADDED``
     - New entry added
   * - ``REMOVED``
     - Entry removed
   * - ``UPDATED``
     - Entry value changed
   * - ``EVICTED``
     - Entry evicted due to policy
   * - ``EXPIRED``
     - Entry expired (TTL)
   * - ``EVICT_ALL``
     - All entries evicted
   * - ``CLEAR_ALL``
     - Map cleared

Item Listeners (Queue, Set, List)
---------------------------------

Monitor collection item changes:

.. code-block:: python

    from hazelcast.proxy.queue import ItemListener, ItemEvent, ItemEventType

    class QueueItemListener(ItemListener):
        def item_added(self, event: ItemEvent) -> None:
            print(f"Item added: {event.item}")

        def item_removed(self, event: ItemEvent) -> None:
            print(f"Item removed: {event.item}")

    queue = client.get_queue("my-queue")

    # Using listener class
    reg_id = queue.add_item_listener(
        QueueItemListener(),
        include_value=True,
    )

    # Using callbacks
    reg_id = queue.add_item_listener(
        item_added=lambda e: print(f"Added: {e.item}"),
        item_removed=lambda e: print(f"Removed: {e.item}"),
        include_value=True,
    )

Message Listeners (Topic)
-------------------------

Receive published messages:

.. code-block:: python

    from hazelcast.proxy.topic import MessageListener, TopicMessage

    class MyMessageListener(MessageListener):
        def on_message(self, message: TopicMessage) -> None:
            print(f"Received: {message.message}")
            print(f"Published at: {message.publish_time}")

    topic = client.get_topic("notifications")

    # Using listener class
    reg_id = topic.add_message_listener(MyMessageListener())

    # Using callback
    reg_id = topic.add_message_listener(
        on_message=lambda m: print(f"Message: {m.message}")
    )

Reliable Message Listeners
--------------------------

For reliable topic with sequence tracking:

.. code-block:: python

    from hazelcast.proxy.reliable_topic import ReliableMessageListener

    class MyReliableListener(ReliableMessageListener):
        def __init__(self):
            self._last_sequence = -1

        def on_message(self, message):
            print(f"Message: {message.message}")

        def store_sequence(self, sequence: int) -> None:
            # Persist for recovery
            self._last_sequence = sequence

        def retrieve_initial_sequence(self) -> int:
            # Return stored sequence or -1
            return self._last_sequence

        def is_loss_tolerant(self) -> bool:
            # True if can handle message loss
            return True

        def is_terminal(self, error: Exception) -> bool:
            # True if error should stop listener
            return False

        def on_stale_sequence(self, head_sequence: int) -> int:
            # Return sequence to resume from
            return head_sequence

    reliable_topic = client.get_reliable_topic("events")
    reg_id = reliable_topic.add_message_listener(MyReliableListener())

Best Practices
--------------

1. **Keep listeners fast**: Listener callbacks block the event
   delivery thread. Offload heavy processing to separate threads.

2. **Handle exceptions**: Wrap listener code in try/except to
   prevent listener removal on errors.

3. **Use include_value wisely**: Set ``include_value=False`` if
   you only need keys, reducing network traffic.

4. **Remove unused listeners**: Always remove listeners when no
   longer needed to prevent memory leaks.

5. **Consider listener scope**: Key-specific listeners receive
   fewer events than global listeners.
