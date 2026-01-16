#!/usr/bin/env python3
"""XA Transaction usage patterns example.

Demonstrates XA-compliant distributed transactions in Hazelcast,
including two-phase commit protocol and recovery scenarios.

XA transactions provide:
- Global transaction coordination across multiple resources
- Two-phase commit (2PC) protocol
- Crash recovery support
- Integration with external transaction managers
"""

from hazelcast import HazelcastClient, ClientConfig
from hazelcast.transaction import (
    TransactionContext,
    TransactionOptions,
    TransactionType,
    XATransactionContext,
    Xid,
    XAFlags,
    XAReturnCode,
    XAException,
    XAErrorCode,
    TransactionException,
)


def basic_transaction_example(client: HazelcastClient):
    """Basic transaction with automatic commit/rollback."""
    print("=== Basic Transaction (Context Manager) ===")

    # Using context manager for automatic commit on success, rollback on error
    with client.new_transaction_context() as ctx:
        txn_map = ctx.get_map("accounts")

        # Transfer funds between accounts
        txn_map.put("account-A", 1000)
        txn_map.put("account-B", 500)

        print("  Stored account balances in transaction")
        # Auto-commits when exiting the context manager

    print("  Transaction committed successfully")


def two_phase_commit_example(client: HazelcastClient):
    """Two-phase commit transaction for stronger consistency."""
    print("\n=== Two-Phase Commit Transaction ===")

    options = TransactionOptions(
        timeout=60.0,
        durability=2,  # Number of backup copies
        transaction_type=TransactionType.TWO_PHASE,
    )

    with client.new_transaction_context(options) as ctx:
        txn_map = ctx.get_map("inventory")
        txn_set = ctx.get_set("reserved-items")

        # Atomic inventory update
        txn_map.put("item-123", 50)
        txn_set.add("item-123")

        print("  Two-phase commit transaction completed")


def manual_transaction_control(client: HazelcastClient):
    """Manual transaction control with explicit begin/commit/rollback."""
    print("\n=== Manual Transaction Control ===")

    ctx = client.new_transaction_context()

    try:
        ctx.begin()
        print(f"  Transaction started: {ctx.txn_id}")

        txn_map = ctx.get_map("orders")
        txn_queue = ctx.get_queue("order-queue")

        # Perform operations
        txn_map.put("order-001", {"status": "pending", "amount": 100})
        txn_queue.offer("order-001")

        # Explicit commit
        ctx.commit()
        print("  Transaction committed")

    except TransactionException as e:
        print(f"  Transaction error: {e}")
        ctx.rollback()
        print("  Transaction rolled back")


def transaction_rollback_example(client: HazelcastClient):
    """Demonstrate transaction rollback on error."""
    print("\n=== Transaction Rollback Example ===")

    try:
        with client.new_transaction_context() as ctx:
            txn_map = ctx.get_map("test-rollback")

            txn_map.put("key-1", "value-1")
            txn_map.put("key-2", "value-2")

            # Simulate an error
            raise ValueError("Simulated error - triggering rollback")

    except ValueError as e:
        print(f"  Error caught: {e}")
        print("  Transaction was automatically rolled back")


def xa_transaction_example(client: HazelcastClient):
    """XA transaction with explicit XID management."""
    print("\n=== XA Transaction Example ===")

    # Create XA transaction context
    # Note: In real scenarios, XA context would be obtained from client
    # This demonstrates the XA protocol flow

    # Create a unique XA transaction identifier
    xid = Xid.create(
        global_transaction_id=b"global-txn-12345",
        branch_qualifier=b"hazelcast-branch-001",
        format_id=1,
    )

    print(f"  Created XID: {xid}")

    # Demonstrate XID properties
    print(f"  Format ID: {xid.format_id}")
    print(f"  Global TXN ID: {xid.global_transaction_id.hex()}")
    print(f"  Branch Qualifier: {xid.branch_qualifier.hex()}")


def xa_two_phase_commit_flow():
    """Demonstrate the XA two-phase commit protocol flow."""
    print("\n=== XA Two-Phase Commit Flow ===")

    print("""
    The XA protocol follows this sequence:

    1. start(xid, TMNOFLAGS)    - Begin transaction branch
    2. [perform work]           - Execute transactional operations
    3. end(xid, TMSUCCESS)      - End transaction branch work
    4. prepare(xid)             - Vote on commit (returns XA_OK or XA_RDONLY)
    5a. commit(xid, false)      - Commit if all resources voted OK
    5b. rollback(xid)           - Rollback if any resource voted NO

    For one-phase optimization:
    - commit(xid, true)         - Skip prepare, directly commit

    Recovery operations:
    - recover(TMSTARTRSCAN)     - Get list of prepared XIDs
    - commit/rollback(xid)      - Resolve in-doubt transactions
    - forget(xid)               - Remove heuristically completed XIDs
    """)


def xa_flags_reference():
    """Reference for XA flags and their usage."""
    print("\n=== XA Flags Reference ===")

    flags_info = [
        ("TMNOFLAGS", XAFlags.TMNOFLAGS, "No special options"),
        ("TMJOIN", XAFlags.TMJOIN, "Join existing transaction branch"),
        ("TMRESUME", XAFlags.TMRESUME, "Resume suspended transaction"),
        ("TMSUSPEND", XAFlags.TMSUSPEND, "Suspend transaction (keep association)"),
        ("TMSUCCESS", XAFlags.TMSUCCESS, "End branch successfully"),
        ("TMFAIL", XAFlags.TMFAIL, "End branch with failure"),
        ("TMONEPHASE", XAFlags.TMONEPHASE, "One-phase commit optimization"),
        ("TMSTARTRSCAN", XAFlags.TMSTARTRSCAN, "Start recovery scan"),
        ("TMENDRSCAN", XAFlags.TMENDRSCAN, "End recovery scan"),
    ]

    print(f"  {'Flag':<15} {'Value':<12} Description")
    print("  " + "-" * 60)
    for name, flag, desc in flags_info:
        print(f"  {name:<15} {flag.value:<12} {desc}")


def transaction_options_example():
    """Demonstrate various transaction configuration options."""
    print("\n=== Transaction Options ===")

    # Default options
    default_opts = TransactionOptions()
    print(f"  Default timeout: {default_opts.timeout}s")
    print(f"  Default durability: {default_opts.durability}")
    print(f"  Default type: {default_opts.transaction_type.name}")

    # Custom options for high-durability transactions
    high_durability = TransactionOptions(
        timeout=120.0,
        durability=3,
        transaction_type=TransactionType.TWO_PHASE,
    )
    print(f"\n  High-durability config:")
    print(f"    Timeout: {high_durability.timeout}s")
    print(f"    Durability: {high_durability.durability} backups")
    print(f"    Type: {high_durability.transaction_type.name}")

    # Low-latency options
    low_latency = TransactionOptions(
        timeout=10.0,
        durability=0,
        transaction_type=TransactionType.ONE_PHASE,
    )
    print(f"\n  Low-latency config:")
    print(f"    Timeout: {low_latency.timeout}s")
    print(f"    Durability: {low_latency.durability} (no backups)")
    print(f"    Type: {low_latency.transaction_type.name}")


def multi_map_transaction_example(client: HazelcastClient):
    """Transaction across multiple data structures."""
    print("\n=== Multi-Structure Transaction ===")

    with client.new_transaction_context() as ctx:
        # Get transactional proxies for different structures
        users_map = ctx.get_map("users")
        roles_set = ctx.get_set("admin-users")
        audit_queue = ctx.get_queue("audit-log")
        user_roles = ctx.get_multi_map("user-roles")

        # Atomic user creation with role assignment
        user_id = "user-42"

        users_map.put(user_id, {"name": "Alice", "email": "alice@example.com"})
        roles_set.add(user_id)
        user_roles.put(user_id, "admin")
        user_roles.put(user_id, "editor")
        audit_queue.offer(f"Created user {user_id} with admin role")

        print(f"  Created user {user_id} with roles in single transaction")


def main():
    # Configure client
    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    client = HazelcastClient(config)

    try:
        client.start()

        # Run examples
        basic_transaction_example(client)
        two_phase_commit_example(client)
        manual_transaction_control(client)
        transaction_rollback_example(client)
        xa_transaction_example(client)
        xa_two_phase_commit_flow()
        xa_flags_reference()
        transaction_options_example()
        multi_map_transaction_example(client)

    finally:
        client.shutdown()
        print("\nClient shutdown complete.")


if __name__ == "__main__":
    main()
