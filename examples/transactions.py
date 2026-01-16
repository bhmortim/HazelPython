"""Hazelcast Transactions Example.

This example demonstrates how to use Hazelcast transactions for ACID
operations across multiple distributed data structures.

Transactions ensure that a group of operations either all succeed
(commit) or all fail (rollback), maintaining data consistency.

Prerequisites:
    - A running Hazelcast cluster

Usage:
    python transactions.py
"""

from hazelcast import HazelcastClient, ClientConfig
from hazelcast.transaction import (
    TransactionContext,
    TransactionOptions,
    TransactionType,
    TransactionState,
    TransactionException,
    TransactionNotActiveException,
    TransactionTimedOutException,
)


def basic_transaction_with_context_manager():
    """Demonstrate basic transaction using context manager.
    
    The context manager automatically:
    - Begins the transaction on entry
    - Commits on successful exit
    - Rolls back on exception
    """
    print("\n" + "=" * 50)
    print("Basic Transaction with Context Manager")
    print("=" * 50)

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    with HazelcastClient(config) as client:
        print("Connected to cluster")

        # Using context manager - auto commit/rollback
        with client.new_transaction_context() as ctx:
            print(f"Transaction started: {ctx.txn_id[:8]}...")
            print(f"Transaction state: {ctx.state.name}")

            # Get transactional map
            txn_map = ctx.get_map("account-balances")

            # Perform transactional operations
            txn_map.put("account:1", 1000)
            txn_map.put("account:2", 500)

            print("Operations completed, transaction will auto-commit")
            # Transaction commits automatically on exit

        print("Transaction committed successfully")

        # Verify the data was persisted
        balances = client.get_map("account-balances")
        print(f"Account 1 balance: {balances.get('account:1')}")
        print(f"Account 2 balance: {balances.get('account:2')}")

        # Cleanup
        balances.clear()


def manual_transaction_control():
    """Demonstrate manual transaction control with explicit begin/commit/rollback."""
    print("\n" + "=" * 50)
    print("Manual Transaction Control")
    print("=" * 50)

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    with HazelcastClient(config) as client:
        # Create transaction context (not started yet)
        ctx = client.new_transaction_context()
        print(f"Context created, state: {ctx.state.name}")

        try:
            # Explicitly begin the transaction
            ctx.begin()
            print(f"Transaction begun: {ctx.txn_id[:8]}...")
            print(f"State after begin: {ctx.state.name}")

            # Get transactional proxies
            txn_map = ctx.get_map("inventory")

            # Perform operations
            txn_map.put("item:1", {"name": "Widget", "quantity": 100})
            txn_map.put("item:2", {"name": "Gadget", "quantity": 50})

            # Explicitly commit
            ctx.commit()
            print(f"State after commit: {ctx.state.name}")
            print("Transaction committed manually")

        except Exception as e:
            # Explicitly rollback on error
            print(f"Error occurred: {e}")
            ctx.rollback()
            print(f"State after rollback: {ctx.state.name}")
            raise

        # Verify
        inventory = client.get_map("inventory")
        print(f"Item 1: {inventory.get('item:1')}")
        inventory.clear()


def transaction_options_example():
    """Demonstrate configuring transaction options."""
    print("\n" + "=" * 50)
    print("Transaction Options Configuration")
    print("=" * 50)

    # Default options
    default_opts = TransactionOptions()
    print("\nDefault TransactionOptions:")
    print(f"  Timeout: {default_opts.timeout}s")
    print(f"  Durability: {default_opts.durability}")
    print(f"  Type: {default_opts.transaction_type.name}")

    # Custom options - ONE_PHASE (faster, less reliable)
    one_phase_opts = TransactionOptions(
        timeout=30.0,
        durability=1,
        transaction_type=TransactionType.ONE_PHASE,
    )
    print("\nONE_PHASE TransactionOptions:")
    print(f"  Timeout: {one_phase_opts.timeout}s")
    print(f"  Durability: {one_phase_opts.durability}")
    print(f"  Type: {one_phase_opts.transaction_type.name}")

    # Custom options - TWO_PHASE (stronger consistency)
    two_phase_opts = TransactionOptions(
        timeout=60.0,
        durability=2,
        transaction_type=TransactionType.TWO_PHASE,
    )
    print("\nTWO_PHASE TransactionOptions:")
    print(f"  Timeout: {two_phase_opts.timeout}s")
    print(f"  Durability: {two_phase_opts.durability}")
    print(f"  Type: {two_phase_opts.transaction_type.name}")

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    with HazelcastClient(config) as client:
        # Use TWO_PHASE for critical operations
        print("\nExecuting TWO_PHASE transaction...")
        with client.new_transaction_context(two_phase_opts) as ctx:
            print(f"Transaction type: {ctx.options.transaction_type.name}")
            txn_map = ctx.get_map("critical-data")
            txn_map.put("important-key", "important-value")

        print("TWO_PHASE transaction completed")

        # Cleanup
        client.get_map("critical-data").clear()


def transactional_proxies_example():
    """Demonstrate all available transactional proxies."""
    print("\n" + "=" * 50)
    print("Transactional Proxies")
    print("=" * 50)

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    with HazelcastClient(config) as client:
        with client.new_transaction_context() as ctx:
            # Transactional Map
            print("\n--- Transactional Map ---")
            txn_map = ctx.get_map("txn-demo-map")
            txn_map.put("key1", "value1")
            txn_map.put("key2", "value2")
            value = txn_map.get("key1")
            print(f"  put/get: {value}")
            print(f"  contains_key('key1'): {txn_map.contains_key('key1')}")
            print(f"  size: {txn_map.size()}")

            # Transactional Set
            print("\n--- Transactional Set ---")
            txn_set = ctx.get_set("txn-demo-set")
            txn_set.add("item1")
            txn_set.add("item2")
            txn_set.add("item1")  # Duplicate, won't be added
            print(f"  size after adding 3 (1 duplicate): {txn_set.size()}")
            print(f"  contains('item1'): {txn_set.contains('item1')}")

            # Transactional List
            print("\n--- Transactional List ---")
            txn_list = ctx.get_list("txn-demo-list")
            txn_list.add("first")
            txn_list.add("second")
            txn_list.add("first")  # Duplicates allowed in list
            print(f"  size after adding 3 (with duplicate): {txn_list.size()}")

            # Transactional Queue
            print("\n--- Transactional Queue ---")
            txn_queue = ctx.get_queue("txn-demo-queue")
            txn_queue.offer("task1")
            txn_queue.offer("task2")
            print(f"  size after 2 offers: {txn_queue.size()}")
            polled = txn_queue.poll()
            print(f"  polled item: {polled}")

            # Transactional MultiMap
            print("\n--- Transactional MultiMap ---")
            txn_mm = ctx.get_multi_map("txn-demo-multimap")
            txn_mm.put("user:1", "role:admin")
            txn_mm.put("user:1", "role:editor")
            txn_mm.put("user:2", "role:viewer")
            print(f"  values for 'user:1': {txn_mm.get('user:1')}")
            print(f"  size: {txn_mm.size()}")
            print(f"  value_count('user:1'): {txn_mm.value_count('user:1')}")

        print("\nAll transactional operations committed")

        # Cleanup
        client.get_map("txn-demo-map").clear()
        client.get_set("txn-demo-set").clear()
        client.get_list("txn-demo-list").clear()
        client.get_queue("txn-demo-queue").clear()
        client.get_multi_map("txn-demo-multimap").clear()


def rollback_on_exception_example():
    """Demonstrate automatic rollback when an exception occurs."""
    print("\n" + "=" * 50)
    print("Rollback on Exception")
    print("=" * 50)

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    with HazelcastClient(config) as client:
        # Pre-populate with initial data
        accounts = client.get_map("bank-accounts")
        accounts.put("checking", 1000)
        accounts.put("savings", 5000)
        print(f"Initial checking: {accounts.get('checking')}")
        print(f"Initial savings: {accounts.get('savings')}")

        # Attempt a transfer that will fail
        print("\nAttempting transfer of $2000 from checking to savings...")
        try:
            with client.new_transaction_context() as ctx:
                txn_accounts = ctx.get_map("bank-accounts")

                # Read current balances
                checking = txn_accounts.get("checking")
                savings = txn_accounts.get("savings")

                transfer_amount = 2000

                # Check if sufficient funds
                if checking < transfer_amount:
                    raise ValueError(
                        f"Insufficient funds: {checking} < {transfer_amount}"
                    )

                # Debit checking
                txn_accounts.put("checking", checking - transfer_amount)
                # Credit savings
                txn_accounts.put("savings", savings + transfer_amount)

                # This won't be reached due to the exception above

        except ValueError as e:
            print(f"Transaction rolled back: {e}")

        # Verify balances unchanged
        print(f"\nAfter failed transaction:")
        print(f"  Checking: {accounts.get('checking')}")
        print(f"  Savings: {accounts.get('savings')}")

        # Now do a successful transfer
        print("\nAttempting transfer of $500 from checking to savings...")
        try:
            with client.new_transaction_context() as ctx:
                txn_accounts = ctx.get_map("bank-accounts")

                checking = txn_accounts.get("checking")
                savings = txn_accounts.get("savings")

                transfer_amount = 500

                if checking < transfer_amount:
                    raise ValueError(
                        f"Insufficient funds: {checking} < {transfer_amount}"
                    )

                txn_accounts.put("checking", checking - transfer_amount)
                txn_accounts.put("savings", savings + transfer_amount)

        except ValueError as e:
            print(f"Transaction rolled back: {e}")

        print(f"\nAfter successful transaction:")
        print(f"  Checking: {accounts.get('checking')}")
        print(f"  Savings: {accounts.get('savings')}")

        # Cleanup
        accounts.clear()


def cross_structure_transaction_example():
    """Demonstrate a transaction spanning multiple data structures."""
    print("\n" + "=" * 50)
    print("Cross-Structure Transaction")
    print("=" * 50)

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    with HazelcastClient(config) as client:
        # Scenario: E-commerce order processing
        # - Update inventory (Map)
        # - Add order to order history (List)
        # - Update customer loyalty points (Map)
        # - Add order ID to processing queue (Queue)

        # Initialize data
        inventory = client.get_map("inventory")
        inventory.put("SKU-001", {"name": "Laptop", "stock": 10})
        inventory.put("SKU-002", {"name": "Mouse", "stock": 50})

        customers = client.get_map("customers")
        customers.put("cust-123", {"name": "Alice", "loyalty_points": 100})

        print("Initial state:")
        print(f"  Laptop stock: {inventory.get('SKU-001')['stock']}")
        print(f"  Customer points: {customers.get('cust-123')['loyalty_points']}")

        # Process an order in a transaction
        order_id = "ORD-2024-001"
        print(f"\nProcessing order: {order_id}")

        options = TransactionOptions(
            timeout=30.0,
            transaction_type=TransactionType.TWO_PHASE,
        )

        with client.new_transaction_context(options) as ctx:
            # Get transactional proxies for all structures
            txn_inventory = ctx.get_map("inventory")
            txn_customers = ctx.get_map("customers")
            txn_order_history = ctx.get_list("order-history")
            txn_processing_queue = ctx.get_queue("order-processing")

            # 1. Decrement inventory
            laptop = txn_inventory.get("SKU-001")
            if laptop["stock"] < 1:
                raise ValueError("Out of stock!")
            laptop["stock"] -= 1
            txn_inventory.put("SKU-001", laptop)
            print("  - Decremented laptop inventory")

            # 2. Add order to history
            order_record = {
                "order_id": order_id,
                "customer": "cust-123",
                "items": ["SKU-001"],
                "status": "processing",
            }
            txn_order_history.add(order_record)
            print("  - Added order to history")

            # 3. Award loyalty points
            customer = txn_customers.get("cust-123")
            customer["loyalty_points"] += 50
            txn_customers.put("cust-123", customer)
            print("  - Awarded 50 loyalty points")

            # 4. Queue for processing
            txn_processing_queue.offer(order_id)
            print("  - Queued for processing")

        print("\nTransaction committed - all operations succeeded atomically")

        # Verify all changes
        print("\nFinal state:")
        print(f"  Laptop stock: {inventory.get('SKU-001')['stock']}")
        print(f"  Customer points: {customers.get('cust-123')['loyalty_points']}")
        print(f"  Order history size: {client.get_list('order-history').size()}")
        print(f"  Processing queue size: {client.get_queue('order-processing').size()}")

        # Cleanup
        inventory.clear()
        customers.clear()
        client.get_list("order-history").clear()
        client.get_queue("order-processing").clear()


def transaction_state_machine_example():
    """Demonstrate transaction state transitions."""
    print("\n" + "=" * 50)
    print("Transaction State Machine")
    print("=" * 50)

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    with HazelcastClient(config) as client:
        # Show all possible states
        print("\nTransaction States:")
        for state in TransactionState:
            print(f"  {state.value}: {state.name}")

        # Track states during transaction lifecycle
        print("\nState transitions during normal operation:")
        ctx = client.new_transaction_context()
        print(f"  Created: {ctx.state.name}")

        ctx.begin()
        print(f"  After begin(): {ctx.state.name}")

        txn_map = ctx.get_map("state-demo")
        txn_map.put("key", "value")

        ctx.commit()
        print(f"  After commit(): {ctx.state.name}")

        # State transitions during rollback
        print("\nState transitions during rollback:")
        ctx2 = client.new_transaction_context()
        print(f"  Created: {ctx2.state.name}")

        ctx2.begin()
        print(f"  After begin(): {ctx2.state.name}")

        ctx2.rollback()
        print(f"  After rollback(): {ctx2.state.name}")

        # Cleanup
        client.get_map("state-demo").clear()


def main():
    print("Hazelcast Transactions Examples")
    print("=" * 50)
    print("""
Transactions provide ACID guarantees for operations across
multiple distributed data structures:

- Atomicity: All operations succeed or all fail
- Consistency: Data remains in valid state
- Isolation: Concurrent transactions don't interfere
- Durability: Committed changes persist

Transaction types:
- ONE_PHASE: Faster, single coordinator
- TWO_PHASE: Stronger consistency, prepare + commit phases
    """)

    # Run examples
    basic_transaction_with_context_manager()
    manual_transaction_control()
    transaction_options_example()
    transactional_proxies_example()
    rollback_on_exception_example()
    cross_structure_transaction_example()
    transaction_state_machine_example()

    print("\n" + "=" * 50)
    print("All transaction examples completed!")
    print("=" * 50)


if __name__ == "__main__":
    main()
