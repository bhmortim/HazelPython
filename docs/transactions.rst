Transactions
============

Hazelcast provides ACID transactions across multiple distributed data structures.
Transactions ensure that either all operations succeed or none do.

Basic Usage
-----------

Using context manager (recommended):

.. code-block:: python

   from hazelcast import HazelcastClient

   with HazelcastClient() as client:
       # Create transaction context
       with client.new_transaction_context() as ctx:
           # Get transactional proxies
           accounts = ctx.get_map("accounts")
           audit_log = ctx.get_list("audit-log")
           
           # Perform operations
           balance = accounts.get("account:123")
           accounts.put("account:123", balance - 100)
           accounts.put("account:456", accounts.get("account:456") + 100)
           
           audit_log.add({
               "from": "account:123",
               "to": "account:456",
               "amount": 100
           })
           
           # Auto-commits on successful exit

Manual Transaction Control
--------------------------

For fine-grained control:

.. code-block:: python

   ctx = client.new_transaction_context()
   
   try:
       ctx.begin()
       
       accounts = ctx.get_map("accounts")
       # ... operations ...
       
       ctx.commit()
   except Exception as e:
       ctx.rollback()
       raise

Transaction Options
-------------------

Configure transaction behavior:

.. code-block:: python

   from hazelcast.transaction import TransactionOptions, TransactionType

   options = TransactionOptions(
       timeout=60.0,  # Transaction timeout in seconds
       durability=2,  # Number of backup copies for transaction log
       transaction_type=TransactionType.TWO_PHASE,  # Commit protocol
   )
   
   with client.new_transaction_context(options) as ctx:
       # Transactional operations
       pass

**Transaction Types:**

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Type
     - Description
   * - ``ONE_PHASE``
     - Single-phase commit. Faster but less reliable if coordinator fails.
   * - ``TWO_PHASE``
     - Two-phase commit with prepare and commit phases. Stronger consistency.

Transactional Data Structures
-----------------------------

TransactionalMap
~~~~~~~~~~~~~~~~

.. code-block:: python

   with client.new_transaction_context() as ctx:
       txn_map = ctx.get_map("my-map")
       
       # Read operations
       value = txn_map.get("key")
       exists = txn_map.contains_key("key")
       size = txn_map.size()
       keys = txn_map.key_set()
       values = txn_map.values()
       
       # Write operations
       txn_map.put("key", "value")
       txn_map.put_if_absent("key2", "value2")
       txn_map.set("key3", "value3")
       txn_map.remove("key4")
       txn_map.delete("key5")
       old = txn_map.replace("key", "new-value")

TransactionalSet
~~~~~~~~~~~~~~~~

.. code-block:: python

   with client.new_transaction_context() as ctx:
       txn_set = ctx.get_set("my-set")
       
       txn_set.add("item1")
       txn_set.add("item2")
       exists = txn_set.contains("item1")
       txn_set.remove("item1")
       size = txn_set.size()

TransactionalList
~~~~~~~~~~~~~~~~~

.. code-block:: python

   with client.new_transaction_context() as ctx:
       txn_list = ctx.get_list("my-list")
       
       txn_list.add("item1")
       txn_list.add("item2")
       first = txn_list.get(0)
       txn_list.set(0, "updated")
       txn_list.remove_at(1)
       size = txn_list.size()

TransactionalQueue
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   with client.new_transaction_context() as ctx:
       txn_queue = ctx.get_queue("my-queue")
       
       txn_queue.offer("item1")
       txn_queue.offer("item2", timeout=5.0)
       item = txn_queue.poll()
       item = txn_queue.poll(timeout=5.0)
       peek = txn_queue.peek()
       size = txn_queue.size()

TransactionalMultiMap
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   with client.new_transaction_context() as ctx:
       txn_mm = ctx.get_multi_map("my-multimap")
       
       txn_mm.put("key", "value1")
       txn_mm.put("key", "value2")
       values = txn_mm.get("key")
       count = txn_mm.value_count("key")
       txn_mm.remove("key", "value1")
       txn_mm.remove_all("key")

Transaction States
------------------

Transactions go through the following states:

.. code-block:: text

   NO_TXN → ACTIVE → PREPARING → PREPARED → COMMITTING → COMMITTED
                  ↘                      ↗
                   → ROLLING_BACK → ROLLED_BACK

Check transaction state:

.. code-block:: python

   from hazelcast.transaction import TransactionState

   ctx = client.new_transaction_context()
   print(f"State: {ctx.state}")  # NO_TXN
   
   ctx.begin()
   print(f"State: {ctx.state}")  # ACTIVE
   
   ctx.commit()
   print(f"State: {ctx.state}")  # COMMITTED

Error Handling
--------------

Handle transaction-specific exceptions:

.. code-block:: python

   from hazelcast.transaction import (
       TransactionException,
       TransactionNotActiveException,
       TransactionTimedOutException,
   )

   try:
       with client.new_transaction_context() as ctx:
           accounts = ctx.get_map("accounts")
           # ... operations ...
   except TransactionTimedOutException:
       print("Transaction timed out")
   except TransactionNotActiveException:
       print("Transaction is not active")
   except TransactionException as e:
       print(f"Transaction failed: {e}")

Best Practices
--------------

1. **Keep Transactions Short**
   
   Long-running transactions lock resources and increase contention.

2. **Use Context Manager**
   
   The context manager ensures proper commit/rollback handling.

3. **Set Appropriate Timeout**
   
   Configure timeout based on expected operation duration.

4. **Choose Transaction Type Wisely**
   
   Use ONE_PHASE for better performance when strong consistency isn't critical.
   Use TWO_PHASE when consistency is paramount.

5. **Handle Failures Gracefully**
   
   Always implement proper error handling and consider retry logic.

6. **Avoid Nested Transactions**
   
   Hazelcast doesn't support nested transactions. Design accordingly.
