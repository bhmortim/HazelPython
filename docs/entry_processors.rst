Entry Processors
================

Entry processors execute on map entries at the data's location, eliminating
the need to transfer data across the network. This is particularly efficient
for read-modify-write operations.

Built-in Processors
-------------------

IncrementProcessor
~~~~~~~~~~~~~~~~~~

Increment a numeric value:

.. code-block:: python

   from hazelcast.processor import IncrementProcessor

   counters = client.get_map("counters")
   counters.put("page-views", 0)
   
   # Increment by 1
   old_value = counters.execute_on_key("page-views", IncrementProcessor())
   print(f"Old value: {old_value}")  # 0
   
   # Increment by custom amount
   old_value = counters.execute_on_key("page-views", IncrementProcessor(10))

DecrementProcessor
~~~~~~~~~~~~~~~~~~

Decrement a numeric value:

.. code-block:: python

   from hazelcast.processor import DecrementProcessor

   inventory = client.get_map("inventory")
   inventory.put("item:1", 100)
   
   old_value = inventory.execute_on_key("item:1", DecrementProcessor(5))

UpdateEntryProcessor
~~~~~~~~~~~~~~~~~~~~

Update an entry with a new value:

.. code-block:: python

   from hazelcast.processor import UpdateEntryProcessor

   users = client.get_map("users")
   
   # Update and get old value
   old_user = users.execute_on_key(
       "user:1",
       UpdateEntryProcessor({"name": "Alice", "status": "active"})
   )

DeleteEntryProcessor
~~~~~~~~~~~~~~~~~~~~

Delete an entry:

.. code-block:: python

   from hazelcast.processor import DeleteEntryProcessor

   # Returns True if entry existed
   existed = users.execute_on_key("user:1", DeleteEntryProcessor())

CompositeEntryProcessor
~~~~~~~~~~~~~~~~~~~~~~~

Chain multiple processors:

.. code-block:: python

   from hazelcast.processor import (
       CompositeEntryProcessor,
       IncrementProcessor,
   )

   counters = client.get_map("counters")
   counters.put("counter", 0)
   
   # Execute multiple operations atomically
   composite = CompositeEntryProcessor(
       IncrementProcessor(5),
       IncrementProcessor(10),
   )
   
   results = counters.execute_on_key("counter", composite)
   print(results)  # [0, 5] - old values after each operation

Custom Entry Processors
-----------------------

Create custom processors for complex logic:

.. code-block:: python

   from hazelcast.processor import (
       EntryProcessor,
       EntryProcessorEntry,
       AbstractEntryProcessor,
   )

   class ConditionalUpdateProcessor(AbstractEntryProcessor):
       """Update value only if condition is met."""
       
       def __init__(self, field: str, expected: any, new_value: dict):
           super().__init__(apply_on_backup=True)
           self.field = field
           self.expected = expected
           self.new_value = new_value
       
       def process(self, entry: EntryProcessorEntry):
           current = entry.get_value()
           if current and current.get(self.field) == self.expected:
               entry.set_value(self.new_value)
               return True
           return False

   # Usage
   users = client.get_map("users")
   users.put("user:1", {"name": "Alice", "status": "pending"})
   
   updated = users.execute_on_key(
       "user:1",
       ConditionalUpdateProcessor(
           field="status",
           expected="pending",
           new_value={"name": "Alice", "status": "active"}
       )
   )

Executing on Multiple Entries
-----------------------------

Execute on Keys
~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.processor import IncrementProcessor

   counters = client.get_map("counters")
   
   # Execute on specific keys
   results = counters.execute_on_keys(
       ["counter:1", "counter:2", "counter:3"],
       IncrementProcessor()
   )
   
   for key, old_value in results.items():
       print(f"{key}: {old_value}")

Execute on All Entries
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Execute on all entries
   results = counters.execute_on_entries(IncrementProcessor())

Execute with Predicate
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.predicate import attr

   # Execute only on entries matching predicate
   results = counters.execute_on_entries(
       IncrementProcessor(),
       predicate=attr("value").less_than(100)
   )

Backup Processing
-----------------

Control whether processors run on backup replicas:

.. code-block:: python

   class MyProcessor(AbstractEntryProcessor):
       def __init__(self):
           # Set apply_on_backup=False to skip backup processing
           super().__init__(apply_on_backup=True)
       
       def process(self, entry):
           # Main processing logic
           pass
       
       def process_backup(self, entry):
           # Custom backup logic (optional)
           # Default behavior re-applies process()
           pass

Testing Entry Processors
------------------------

Use ``SimpleEntryProcessorEntry`` for testing:

.. code-block:: python

   from hazelcast.processor import (
       SimpleEntryProcessorEntry,
       IncrementProcessor,
   )

   def test_increment_processor():
       entry = SimpleEntryProcessorEntry("counter", 10)
       processor = IncrementProcessor(5)
       
       old_value = processor.process(entry)
       
       assert old_value == 10
       assert entry.get_value() == 15

Best Practices
--------------

1. **Keep Processors Simple**
   
   Entry processors run on partition threads. Keep logic fast and simple.

2. **Avoid External Calls**
   
   Don't make network calls or I/O operations inside processors.

3. **Use for Atomic Operations**
   
   Entry processors are ideal for atomic read-modify-write operations.

4. **Test Thoroughly**
   
   Use ``SimpleEntryProcessorEntry`` for unit testing processors.

5. **Consider Memory**
   
   Avoid creating large objects inside processors as they run frequently.
