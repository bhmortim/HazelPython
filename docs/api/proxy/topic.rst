hazelcast.proxy.topic
=====================

Distributed Topic (publish-subscribe) data structure proxy.

A ``Topic`` provides a publish-subscribe messaging pattern. Publishers send
messages to a topic, and all subscribers receive those messages. Messages
are delivered in the order they were published but delivery is best-effort
(messages can be lost if a subscriber is slow or disconnected).

For reliable, ordered delivery, see :doc:`reliable_topic`.

Usage Examples
--------------

Basic Publish-Subscribe
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Get or create a topic
   topic = client.get_topic("notifications")
   
   # Subscribe to messages
   def on_message(message):
       print(f"Received: {message.message}")
       print(f"Published at: {message.publish_time}")
       print(f"Publisher: {message.publishing_member}")
   
   reg_id = topic.add_listener(on_message)
   
   # Publish messages
   topic.publish("Hello, World!")
   topic.publish({"event": "user_login", "user_id": 123})
   
   # Later: unsubscribe
   topic.remove_listener(reg_id)
   
   client.shutdown()

Event Broadcasting
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Broadcast events to all cluster nodes
   events_topic = client.get_topic("cluster-events")
   
   # Publisher (e.g., on config change)
   def broadcast_config_change(config):
       events_topic.publish({
           "type": "config_change",
           "config": config,
           "timestamp": time.time()
       })
   
   # Subscriber (on each node)
   def on_event(message):
       event = message.message
       if event["type"] == "config_change":
           reload_config(event["config"])
   
   events_topic.add_listener(on_event)

Notification System
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Real-time notifications
   notifications = client.get_topic("user-notifications")
   
   # Send notification
   notifications.publish({
       "user_id": "user:123",
       "type": "new_message",
       "content": "You have a new message",
       "timestamp": time.time()
   })
   
   # Receive notifications (e.g., in WebSocket handler)
   def notification_handler(message):
       notification = message.message
       send_to_websocket(notification["user_id"], notification)
   
   notifications.add_listener(notification_handler)

Local Statistics
~~~~~~~~~~~~~~~~

.. code-block:: python

   # Get local topic statistics
   stats = topic.get_local_stats()
   
   print(f"Published: {stats.publish_operation_count}")
   print(f"Received: {stats.receive_operation_count}")

Configuration Options
---------------------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Option
     - Default
     - Description
   * - ``global_ordering_enabled``
     - false
     - Enable total ordering across all publishers
   * - ``statistics_enabled``
     - true
     - Enable statistics collection
   * - ``multi_threading_enabled``
     - true
     - Enable parallel listener invocation

Best Practices
--------------

1. **Fire and Forget**: Topic is best-effort delivery. Don't use it
   for critical messages that must not be lost.

2. **Use ReliableTopic for Reliability**: If you need guaranteed
   delivery and ordering, use ``ReliableTopic`` instead.

3. **Keep Messages Small**: Large messages impact all subscribers.
   Consider storing data in a Map and publishing references.

4. **Handle Exceptions**: Exceptions in listeners don't affect
   other listeners but should be logged.

5. **Unsubscribe When Done**: Always remove listeners to prevent
   memory leaks and unnecessary processing.

API Reference
-------------

.. automodule:: hazelcast.proxy.topic
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

TopicProxy
~~~~~~~~~~

.. autoclass:: hazelcast.proxy.topic.TopicProxy
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

TopicMessage
~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.topic.TopicMessage
   :members:
   :undoc-members:
   :show-inheritance:

MessageListener
~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.topic.MessageListener
   :members:
   :undoc-members:
   :show-inheritance:

LocalTopicStats
~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.topic.LocalTopicStats
   :members:
   :undoc-members:
   :show-inheritance:
