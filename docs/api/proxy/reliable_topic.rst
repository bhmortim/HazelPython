hazelcast.proxy.reliable_topic
==============================

Reliable Topic (publish-subscribe with ordering guarantees) data structure proxy.

A ``ReliableTopic`` provides a publish-subscribe pattern with reliability
guarantees backed by a Ringbuffer. Unlike regular Topic, ReliableTopic:

- **Guarantees message ordering** per publisher
- **Supports message replay** for late joiners or reconnecting subscribers
- **Provides backpressure** when the backing Ringbuffer is full
- **Survives client disconnections** (subscribers can resume from where they left off)

Usage Examples
--------------

Basic Reliable Messaging
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient
   from hazelcast.proxy.reliable_topic import ReliableMessageListener

   client = HazelcastClient()
   
   # Get or create a reliable topic
   topic = client.get_reliable_topic("events")
   
   # Create a reliable listener
   class MyListener(ReliableMessageListener):
       def on_message(self, message):
           print(f"Received: {message.message}")
           print(f"Sequence: {message.sequence}")
       
       def retrieve_initial_sequence(self):
           # Start from the oldest available message
           return -1
       
       def store_sequence(self, sequence):
           # Optionally persist sequence for recovery
           pass
       
       def is_loss_tolerant(self):
           # True = skip gaps; False = terminate on gaps
           return False
       
       def is_terminal(self, error):
           # Return True to stop listening on error
           return isinstance(error, SomeUnrecoverableError)
   
   # Subscribe
   reg_id = topic.add_listener(MyListener())
   
   # Publish
   topic.publish("Event 1")
   topic.publish("Event 2")
   
   # Unsubscribe
   topic.remove_listener(reg_id)
   
   client.shutdown()

Resumable Subscription
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import json

   class ResumableListener(ReliableMessageListener):
       def __init__(self, state_file):
           self.state_file = state_file
           self._last_sequence = self._load_sequence()
       
       def _load_sequence(self):
           try:
               with open(self.state_file, "r") as f:
                   return json.load(f).get("sequence", -1)
           except FileNotFoundError:
               return -1
       
       def _save_sequence(self, seq):
           with open(self.state_file, "w") as f:
               json.dump({"sequence": seq}, f)
       
       def retrieve_initial_sequence(self):
           # Resume from last processed message
           return self._last_sequence
       
       def store_sequence(self, sequence):
           self._last_sequence = sequence
           self._save_sequence(sequence)
       
       def on_message(self, message):
           process_message(message.message)
           self.store_sequence(message.sequence)
       
       def is_loss_tolerant(self):
           return False
       
       def is_terminal(self, error):
           return False
   
   listener = ResumableListener("/tmp/topic-state.json")
   topic.add_listener(listener)

Handling Backpressure
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.proxy.reliable_topic import TopicOverloadPolicy

   # Configure overload behavior
   config = ReliableTopicConfig(
       # Block until space available
       overload_policy=TopicOverloadPolicy.BLOCK,
       
       # Or: discard oldest messages
       # overload_policy=TopicOverloadPolicy.DISCARD_OLDEST,
       
       # Or: discard newest (incoming) message
       # overload_policy=TopicOverloadPolicy.DISCARD_NEWEST,
       
       # Or: raise exception
       # overload_policy=TopicOverloadPolicy.ERROR,
   )
   
   topic = client.get_reliable_topic("high-volume", config)
   
   # Publishing will respect the overload policy
   for i in range(10000):
       topic.publish(f"message-{i}")

Event Sourcing Pattern
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Use ReliableTopic for event sourcing
   events_topic = client.get_reliable_topic("order-events")
   
   # Publish domain events
   events_topic.publish({
       "type": "OrderCreated",
       "order_id": "order-123",
       "customer_id": "customer-456",
       "items": [{"sku": "PROD-1", "qty": 2}],
       "timestamp": time.time()
   })
   
   events_topic.publish({
       "type": "OrderShipped",
       "order_id": "order-123",
       "tracking_number": "TRACK-789",
       "timestamp": time.time()
   })
   
   # Event processor rebuilds state from events
   class OrderProjection(ReliableMessageListener):
       def __init__(self):
           self.orders = {}
       
       def retrieve_initial_sequence(self):
           return -1  # Replay all events
       
       def on_message(self, message):
           event = message.message
           order_id = event["order_id"]
           
           if event["type"] == "OrderCreated":
               self.orders[order_id] = {
                   "status": "created",
                   "customer_id": event["customer_id"],
                   "items": event["items"]
               }
           elif event["type"] == "OrderShipped":
               if order_id in self.orders:
                   self.orders[order_id]["status"] = "shipped"
                   self.orders[order_id]["tracking"] = event["tracking_number"]

Configuration Options
---------------------

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Option
     - Default
     - Description
   * - ``read_batch_size``
     - 10
     - Number of messages to read per batch
   * - ``overload_policy``
     - BLOCK
     - Behavior when Ringbuffer is full
   * - ``statistics_enabled``
     - true
     - Enable statistics collection

**Overload Policies:**

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Policy
     - Description
   * - ``BLOCK``
     - Block publish until space is available
   * - ``DISCARD_OLDEST``
     - Overwrite oldest messages in Ringbuffer
   * - ``DISCARD_NEWEST``
     - Discard the incoming message
   * - ``ERROR``
     - Raise ``TopicOverloadException``

Best Practices
--------------

1. **Persist Sequence Numbers**: For critical applications, persist
   the last processed sequence to enable recovery after crashes.

2. **Choose Overload Policy Wisely**: Use ``BLOCK`` for backpressure,
   ``DISCARD_OLDEST`` for latest-wins scenarios.

3. **Size the Ringbuffer Appropriately**: The backing Ringbuffer
   capacity determines how many messages can be replayed.

4. **Handle StaleSequenceException**: When the requested sequence
   is no longer in the Ringbuffer, decide whether to skip or fail.

5. **Use for Event Sourcing**: ReliableTopic is ideal for event
   sourcing patterns where events must be replayable.

API Reference
-------------

.. automodule:: hazelcast.proxy.reliable_topic
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

ReliableTopicProxy
~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.reliable_topic.ReliableTopicProxy
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

ReliableMessageListener
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.reliable_topic.ReliableMessageListener
   :members:
   :undoc-members:
   :show-inheritance:

ReliableTopicConfig
~~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.reliable_topic.ReliableTopicConfig
   :members:
   :undoc-members:
   :show-inheritance:

StaleSequenceException
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.reliable_topic.StaleSequenceException
   :members:
   :undoc-members:
   :show-inheritance:

Enumerations
------------

TopicOverloadPolicy
~~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.reliable_topic.TopicOverloadPolicy
   :members:
   :undoc-members:
