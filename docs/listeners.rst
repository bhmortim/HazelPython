Listeners
=========

Hazelcast provides various listeners to react to cluster events,
data changes, and client lifecycle state changes.

Lifecycle Listeners
-------------------

Monitor client state changes:

.. code-block:: python

   from hazelcast import HazelcastClient, ClientConfig
   from hazelcast.listener import LifecycleState, LifecycleEvent

   def on_state_changed(event: LifecycleEvent):
       print(f"State: {event.state.name}")
       if event.previous_state:
           print(f"Previous: {event.previous_state.name}")

   config = ClientConfig()
   client = HazelcastClient(config)
   
   reg_id = client.add_lifecycle_listener(
       on_state_changed=on_state_changed
   )
   
   client.start()
   # ... use client ...
   client.shutdown()
   
   # Remove listener when done
   client.remove_listener(reg_id)

**Lifecycle States:**

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - State
     - Description
   * - ``STARTING``
     - Client is starting connection process
   * - ``CONNECTED``
     - Client connected to cluster
   * - ``DISCONNECTED``
     - Client disconnected from cluster
   * - ``SHUTTING_DOWN``
     - Client is shutting down
   * - ``SHUTDOWN``
     - Client has shut down

Class-Based Listener
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.listener import LifecycleListener, LifecycleEvent

   class MyLifecycleListener(LifecycleListener):
       def on_state_changed(self, event: LifecycleEvent):
           print(f"Client state: {event.state.name}")

   reg_id = client.add_lifecycle_listener(MyLifecycleListener())

Membership Listeners
--------------------

Monitor cluster membership changes:

.. code-block:: python

   from hazelcast.listener import MembershipEvent

   def on_member_added(event: MembershipEvent):
       print(f"Member added: {event.member}")

   def on_member_removed(event: MembershipEvent):
       print(f"Member removed: {event.member}")

   reg_id = client.add_membership_listener(
       on_member_added=on_member_added,
       on_member_removed=on_member_removed,
   )

Initial Membership Listener
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Receive current members immediately, then track changes:

.. code-block:: python

   from hazelcast.listener import InitialMembershipEvent

   def on_init(event: InitialMembershipEvent):
       print("Current members:")
       for member in event.members:
           print(f"  - {member}")

   def on_added(event):
       print(f"Added: {event.member}")

   def on_removed(event):
       print(f"Removed: {event.member}")

   reg_id = client.add_initial_membership_listener(
       on_init=on_init,
       on_member_added=on_added,
       on_member_removed=on_removed,
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

   class MyDOListener(DistributedObjectListener):
       def on_created(self, event: DistributedObjectEvent):
           print(f"Created: {event.name} ({event.service_name})")
       
       def on_destroyed(self, event: DistributedObjectEvent):
           print(f"Destroyed: {event.name}")

   reg_id = client.add_distributed_object_listener(MyDOListener())

Entry Listeners (Map)
---------------------

React to map entry changes:

.. code-block:: python

   def on_added(event):
       print(f"Added: {event.key} = {event.value}")

   def on_updated(event):
       print(f"Updated: {event.key}: {event.old_value} -> {event.value}")

   def on_removed(event):
       print(f"Removed: {event.key} (was {event.old_value})")

   def on_evicted(event):
       print(f"Evicted: {event.key}")

   def on_expired(event):
       print(f"Expired: {event.key}")

   my_map = client.get_map("my-map")
   
   reg_id = my_map.add_entry_listener(
       on_added=on_added,
       on_updated=on_updated,
       on_removed=on_removed,
       on_evicted=on_evicted,
       on_expired=on_expired,
       include_value=True,  # Include values in events
   )

Filtered Entry Listener
~~~~~~~~~~~~~~~~~~~~~~~

Listen only to specific entries:

.. code-block:: python

   from hazelcast.predicate import attr

   # Listen only to entries where status = "critical"
   reg_id = my_map.add_entry_listener(
       on_added=on_added,
       on_updated=on_updated,
       predicate=attr("status").equal("critical"),
       include_value=True,
   )
   
   # Listen to specific key
   reg_id = my_map.add_entry_listener(
       on_updated=on_updated,
       key="important-key",
       include_value=True,
   )

Item Listeners (Queue/Set/List)
-------------------------------

React to collection changes:

.. code-block:: python

   from hazelcast.listener import ItemEvent, ItemEventType

   def on_item_added(event: ItemEvent):
       print(f"Item added: {event.item}")

   def on_item_removed(event: ItemEvent):
       print(f"Item removed: {event.item}")

   queue = client.get_queue("my-queue")
   
   reg_id = queue.add_item_listener(
       on_added=on_item_added,
       on_removed=on_item_removed,
       include_value=True,
   )

Message Listeners (Topic)
-------------------------

React to topic messages:

.. code-block:: python

   def on_message(message):
       print(f"Received: {message.message}")
       print(f"Published at: {message.publish_time}")
       print(f"Publishing member: {message.publishing_member}")

   topic = client.get_topic("my-topic")
   reg_id = topic.add_message_listener(on_message=on_message)

Partition Lost Listeners
------------------------

Monitor partition loss events:

.. code-block:: python

   from hazelcast.listener import PartitionLostEvent

   def on_partition_lost(event: PartitionLostEvent):
       print(f"Partition {event.partition_id} lost!")
       print(f"Lost backup count: {event.lost_backup_count}")

   reg_id = client.add_partition_lost_listener(
       on_partition_lost=on_partition_lost
   )

Migration Listeners
-------------------

Monitor partition migration:

.. code-block:: python

   from hazelcast.listener import MigrationEvent, MigrationState

   def on_started(event: MigrationEvent):
       print(f"Migration started: partition {event.partition_id}")

   def on_completed(event: MigrationEvent):
       print(f"Migration completed: partition {event.partition_id}")

   def on_failed(event: MigrationEvent):
       print(f"Migration failed: partition {event.partition_id}")

   reg_id = client.add_migration_listener(
       on_migration_started=on_started,
       on_migration_completed=on_completed,
       on_migration_failed=on_failed,
   )

Removing Listeners
------------------

Always remove listeners when no longer needed:

.. code-block:: python

   # Remove by registration ID
   success = client.remove_listener(reg_id)
   
   # For map entry listeners
   success = my_map.remove_entry_listener(reg_id)
   
   # For topic message listeners
   success = topic.remove_message_listener(reg_id)

Best Practices
--------------

1. **Keep Callbacks Fast**
   
   Listener callbacks run on event threads. Keep them fast and non-blocking.

2. **Handle Exceptions**
   
   Wrap callback logic in try-except to prevent listener failures.

3. **Remove When Done**
   
   Always remove listeners when no longer needed to prevent memory leaks.

4. **Use include_value Wisely**
   
   Setting ``include_value=True`` transfers data across the network.
   Disable if you only need keys.

5. **Filter at Source**
   
   Use predicates and keys to filter events at the server side.
