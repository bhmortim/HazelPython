Transactions
============

The Hazelcast Python Client supports ACID transactions across multiple
distributed data structures.

Basic Transaction Usage
-----------------------

Using Context Manager (Recommended)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    with client.new_transaction_context() as ctx:
        txn_map = ctx.get_map("accounts")
        txn_queue = ctx.get_queue("transfers")

        # Read current balance
        balance = txn_map.get("account:123") or 0

        # Update balance
        txn_map.put("account:123", balance + 100)

        # Record transfer
        txn_queue.offer({"account": "123", "amount": 100})

        # Auto-commits on successful exit
        # Auto-rolls back on exception

Manual Transaction Control
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    ctx = client.new_transaction_context()
    try:
        ctx.begin()

        txn_map = ctx.get_map("accounts")
        txn_map.put("key", "value")

        ctx.commit()
    except Exception:
        ctx.rollback()
        raise

Transaction Options
-------------------

Configure transaction behavior:

.. code-block:: python

    from hazelcast.transaction import TransactionOptions, TransactionType

    options = TransactionOptions(
        timeout=60.0,  # Transaction timeout in seconds
        durability=2,  # Number of backup copies
        transaction_type=TransactionType.TWO_PHASE,
    )

    with client.new_transaction_context(options) as ctx:
        # ...

Transaction Types
~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 20 80
   :header-rows: 1

   * - Type
     - Description
   * - ``ONE_PHASE``
     - Single-phase commit. Faster but less reliable.
   * - ``TWO_PHASE``
     - Two-phase commit with prepare and commit phases.
       Provides stronger consistency guarantees.

Transactional Data Structures
-----------------------------

TransactionalMap
~~~~~~~~~~~~~~~~

.. code-block:: python

    with client.new_transaction_context() as ctx:
        txn_map = ctx.get_map("my-map")

        # Supported operations
        txn_map.put("key", "value")
        txn_map.put_if_absent("key2", "value2")
        value = txn_map.get("key")
        txn_map.set("key", "new-value")
        old = txn_map.get_for_update("key")  # Lock for update
        txn_map.remove("key")
        txn_map.delete("key2")

        exists = txn_map.contains_key("key")
        size = txn_map.size()
        is_empty = txn_map.is_empty()

        keys = txn_map.key_set()
        values = txn_map.values()

TransactionalQueue
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    with client.new_transaction_context() as ctx:
        txn_queue = ctx.get_queue("my-queue")

        txn_queue.offer("item")
        item = txn_queue.poll()
        item = txn_queue.poll(timeout=5.0)
        head = txn_queue.peek()

        size = txn_queue.size()

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

TransactionalMultiMap
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    with client.new_transaction_context() as ctx:
        txn_mm = ctx.get_multi_map("my-multimap")

        txn_mm.put("key", "value1")
        txn_mm.put("key", "value2")
        values = txn_mm.get("key")
        txn_mm.remove("key", "value1")
        removed = txn_mm.remove_all("key")

        size = txn_mm.size()
        count = txn_mm.value_count("key")

Example: Bank Transfer
----------------------

.. code-block:: python

    def transfer(from_account: str, to_account: str, amount: float):
        with client.new_transaction_context() as ctx:
            accounts = ctx.get_map("accounts")
            audit_log = ctx.get_list("audit-log")

            # Get current balances
            from_balance = accounts.get(from_account) or 0
            to_balance = accounts.get(to_account) or 0

            # Validate
            if from_balance < amount:
                raise ValueError("Insufficient funds")

            # Update balances
            accounts.put(from_account, from_balance - amount)
            accounts.put(to_account, to_balance + amount)

            # Record audit entry
            audit_log.add({
                "from": from_account,
                "to": to_account,
                "amount": amount,
                "timestamp": datetime.now().isoformat(),
            })

            # Commits automatically on success

Error Handling
--------------

.. code-block:: python

    from hazelcast.transaction import (
        TransactionException,
        TransactionTimedOutException,
    )

    try:
        with client.new_transaction_context() as ctx:
            # ... operations ...
            pass
    except TransactionTimedOutException:
        print("Transaction timed out")
    except TransactionException as e:
        print(f"Transaction failed: {e}")

Best Practices
--------------

1. **Keep transactions short**: Long-running transactions increase
   conflict probability and hold locks longer.

2. **Use timeouts**: Always set appropriate timeouts to avoid
   indefinite blocking.

3. **Handle rollbacks**: Ensure your application handles transaction
   failures gracefully.

4. **Consider isolation**: Transactional operations see a consistent
   snapshot but may conflict with concurrent transactions.

5. **Use TWO_PHASE for critical operations**: When data integrity is
   paramount, use two-phase commit for stronger guarantees.
