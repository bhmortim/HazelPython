Transactions
============

Hazelcast supports ACID transactions across multiple distributed data structures.

Basic Usage
-----------

Use the context manager pattern for automatic commit/rollback:

.. code-block:: python

   from hazelcast import HazelcastClient, ClientConfig

   with HazelcastClient(config) as client:
       with client.new_transaction_context() as ctx:
           # Get transactional proxies
           txn_map = ctx.get_map("accounts")
           txn_queue = ctx.get_queue("audit-log")

           # Perform operations
           balance = txn_map.get("account:123") or 0
           txn_map.put("account:123", balance - 100)
           txn_map.put("account:456", (txn_map.get("account:456") or 0) + 100)
           txn_queue.offer("Transfer: 123 -> 456, amount: 100")

           # Transaction auto-commits on successful exit
       # Transaction is committed here

If an exception occurs, the transaction automatically rolls back.


Transaction Options
-------------------

Configure transaction behavior:

.. code-block:: python

   from hazelcast.transaction import TransactionOptions, TransactionType

   options = TransactionOptions(
       timeout=60.0,                              # 60 second timeout
       durability=1,                              # Number of backups
       transaction_type=TransactionType.TWO_PHASE,  # Two-phase commit
   )

   with client.new_transaction_context(options) as ctx:
       # Transaction operations
       pass

Transaction Types
~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Type
     - Description
   * - ``ONE_PHASE``
     - Single-phase commit (default). Faster but less durable.
   * - ``TWO_PHASE``
     - Two-phase commit. More durable but slower.


Manual Commit/Rollback
----------------------

For fine-grained control:

.. code-block:: python

   ctx = client.new_transaction_context()
   ctx.begin_transaction()

   try:
       txn_map = ctx.get_map("data")
       txn_map.put("key", "value")

       # Explicit commit
       ctx.commit_transaction()
   except Exception:
       # Explicit rollback
       ctx.rollback_transaction()
       raise


Transactional Data Structures
-----------------------------

The following data structures support transactions:

TransactionalMap
~~~~~~~~~~~~~~~~

.. code-block:: python

   with client.new_transaction_context() as ctx:
       txn_map = ctx.get_map("my-map")

       # Supported operations
       txn_map.put("key", "value")
       value = txn_map.get("key")
       txn_map.remove("key")
       txn_map.put_if_absent("key", "default")
       old = txn_map.replace("key", "new")
       txn_map.set("key", "value")

       exists = txn_map.contains_key("key")
       keys = txn_map.key_set()
       values = txn_map.values()

       size = txn_map.size()
       is_empty = txn_map.is_empty()

TransactionalSet
~~~~~~~~~~~~~~~~

.. code-block:: python

   with client.new_transaction_context() as ctx:
       txn_set = ctx.get_set("my-set")

       txn_set.add("item")
       txn_set.remove("item")

       size = txn_set.size()

TransactionalList
~~~~~~~~~~~~~~~~~

.. code-block:: python

   with client.new_transaction_context() as ctx:
       txn_list = ctx.get_list("my-list")

       txn_list.add("item")
       txn_list.remove("item")

       size = txn_list.size()

TransactionalQueue
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   with client.new_transaction_context() as ctx:
       txn_queue = ctx.get_queue("my-queue")

       txn_queue.offer("item")
       item = txn_queue.poll()
       item = txn_queue.poll(timeout=5.0)
       item = txn_queue.peek()

       size = txn_queue.size()

TransactionalMultiMap
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   with client.new_transaction_context() as ctx:
       txn_mm = ctx.get_multi_map("my-multimap")

       txn_mm.put("key", "value1")
       txn_mm.put("key", "value2")
       values = txn_mm.get("key")
       txn_mm.remove("key", "value1")
       txn_mm.remove_all("key")

       count = txn_mm.value_count("key")
       size = txn_mm.size()


Transaction States
------------------

A transaction goes through these states:

.. code-block:: text

   NOT_STARTED -> ACTIVE -> PREPARED -> COMMITTED
                    |                       |
                    +--------> ROLLED_BACK <+

You can check the current state:

.. code-block:: python

   from hazelcast.transaction import TransactionState

   ctx = client.new_transaction_context()

   print(ctx.state)  # TransactionState.NOT_STARTED

   ctx.begin_transaction()
   print(ctx.state)  # TransactionState.ACTIVE

   ctx.commit_transaction()
   print(ctx.state)  # TransactionState.COMMITTED


Exception Handling
------------------

Transaction-specific exceptions:

.. code-block:: python

   from hazelcast.transaction import (
       TransactionException,
       TransactionNotActiveException,
       TransactionTimedOutException,
   )

   try:
       with client.new_transaction_context() as ctx:
           txn_map = ctx.get_map("data")
           # Operations...
   except TransactionTimedOutException:
       print("Transaction timed out")
   except TransactionNotActiveException:
       print("Transaction is not active")
   except TransactionException as e:
       print(f"Transaction error: {e}")


Best Practices
--------------

1. **Keep transactions short**: Long-running transactions hold locks and
   can cause contention.

2. **Use context managers**: They ensure proper cleanup on exceptions.

3. **Handle exceptions**: Always be prepared for transaction failures.

4. **Choose the right transaction type**: Use ONE_PHASE for speed,
   TWO_PHASE for durability.

5. **Set appropriate timeouts**: Prevent indefinite blocking.

.. code-block:: python

   # Good: Short, focused transaction
   with client.new_transaction_context() as ctx:
       txn_map = ctx.get_map("accounts")
       balance = txn_map.get(account_id) or 0
       txn_map.put(account_id, balance + amount)

   # Avoid: Long-running transaction
   with client.new_transaction_context() as ctx:
       txn_map = ctx.get_map("data")
       for item in large_dataset:  # Don't do this!
           txn_map.put(item.key, item.value)
