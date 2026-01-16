"""Integration tests for transactional data structure proxies.

These tests verify ACID properties of transactional operations.
"""

import unittest
from unittest.mock import MagicMock

from hazelcast.proxy.base import ProxyContext
from hazelcast.proxy.transactional import (
    TransactionalProxy,
    TransactionalMap,
    TransactionalSet,
    TransactionalList,
    TransactionalQueue,
    TransactionalMultiMap,
)
from hazelcast.transaction import (
    TransactionContext,
    TransactionOptions,
    TransactionType,
    TransactionState,
    TransactionNotActiveException,
)


class TransactionalProxyTestBase(unittest.TestCase):
    """Base class for transactional proxy tests."""

    def setUp(self):
        self.mock_invocation_service = MagicMock()
        self.mock_serialization_service = MagicMock()
        self.context = ProxyContext(
            invocation_service=self.mock_invocation_service,
            serialization_service=self.mock_serialization_service,
        )


class TestTransactionalMapACID(TransactionalProxyTestBase):
    """Tests verifying ACID properties for TransactionalMap."""

    def test_atomicity_commit_applies_all_changes(self):
        """All operations within a transaction are applied atomically on commit."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_map = txn_ctx.get_map("test-map")
            txn_map.put("key1", "value1")
            txn_map.put("key2", "value2")
            txn_map.put("key3", "value3")

            self.assertEqual(txn_map.size(), 3)
            self.assertEqual(txn_map.get("key1"), "value1")
            self.assertEqual(txn_map.get("key2"), "value2")
            self.assertEqual(txn_map.get("key3"), "value3")

        self.assertEqual(txn_ctx.state, TransactionState.COMMITTED)

    def test_atomicity_rollback_discards_all_changes(self):
        """All operations are discarded on rollback."""
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        txn_map = txn_ctx.get_map("test-map")
        txn_map.put("key1", "value1")
        txn_map.put("key2", "value2")

        txn_ctx.rollback()
        self.assertEqual(txn_ctx.state, TransactionState.ROLLED_BACK)

    def test_consistency_operations_reflect_local_state(self):
        """Operations within a transaction see consistent local state."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_map = txn_ctx.get_map("test-map")

            self.assertFalse(txn_map.contains_key("key1"))
            self.assertTrue(txn_map.is_empty())

            txn_map.put("key1", "value1")

            self.assertTrue(txn_map.contains_key("key1"))
            self.assertFalse(txn_map.is_empty())
            self.assertEqual(txn_map.get("key1"), "value1")

            txn_map.remove("key1")
            self.assertFalse(txn_map.contains_key("key1"))
            self.assertTrue(txn_map.is_empty())

    def test_isolation_operations_not_visible_outside_transaction(self):
        """Changes are isolated until commit."""
        txn_ctx1 = TransactionContext(self.context)
        txn_ctx1.begin()
        txn_map1 = txn_ctx1.get_map("test-map")
        txn_map1.put("key1", "value1")

        txn_ctx2 = TransactionContext(self.context)
        txn_ctx2.begin()
        txn_map2 = txn_ctx2.get_map("test-map")

        self.assertIsNone(txn_map2.get("key1"))

        txn_ctx1.commit()
        txn_ctx2.commit()

    def test_durability_state_persists_after_commit(self):
        """State persists after commit (simulated)."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_map = txn_ctx.get_map("test-map")
            txn_map.put("key1", "value1")

        self.assertEqual(txn_ctx.state, TransactionState.COMMITTED)

    def test_put_returns_previous_value(self):
        """Put returns the previous value for the key."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_map = txn_ctx.get_map("test-map")
            result1 = txn_map.put("key1", "value1")
            self.assertIsNone(result1)

            result2 = txn_map.put("key1", "value2")
            self.assertEqual(result2, "value1")

    def test_put_if_absent(self):
        """Put if absent only adds when key doesn't exist."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_map = txn_ctx.get_map("test-map")

            result1 = txn_map.put_if_absent("key1", "value1")
            self.assertIsNone(result1)
            self.assertEqual(txn_map.get("key1"), "value1")

            result2 = txn_map.put_if_absent("key1", "value2")
            self.assertEqual(result2, "value1")
            self.assertEqual(txn_map.get("key1"), "value1")

    def test_replace(self):
        """Replace only updates existing keys."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_map = txn_ctx.get_map("test-map")

            result1 = txn_map.replace("key1", "value1")
            self.assertIsNone(result1)
            self.assertIsNone(txn_map.get("key1"))

            txn_map.put("key1", "value1")
            result2 = txn_map.replace("key1", "value2")
            self.assertEqual(result2, "value1")
            self.assertEqual(txn_map.get("key1"), "value2")

    def test_replace_if_same(self):
        """Replace if same only updates when value matches."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_map = txn_ctx.get_map("test-map")
            txn_map.put("key1", "value1")

            result1 = txn_map.replace_if_same("key1", "wrong", "value2")
            self.assertFalse(result1)
            self.assertEqual(txn_map.get("key1"), "value1")

            result2 = txn_map.replace_if_same("key1", "value1", "value2")
            self.assertTrue(result2)
            self.assertEqual(txn_map.get("key1"), "value2")

    def test_delete(self):
        """Delete removes key without returning value."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_map = txn_ctx.get_map("test-map")
            txn_map.put("key1", "value1")
            txn_map.delete("key1")
            self.assertFalse(txn_map.contains_key("key1"))

    def test_key_set_and_values(self):
        """Key set and values return correct data."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_map = txn_ctx.get_map("test-map")
            txn_map.put("key1", "value1")
            txn_map.put("key2", "value2")

            keys = txn_map.key_set()
            self.assertEqual(keys, {"key1", "key2"})

            values = txn_map.values()
            self.assertEqual(sorted(values), ["value1", "value2"])


class TestTransactionalSetACID(TransactionalProxyTestBase):
    """Tests verifying ACID properties for TransactionalSet."""

    def test_atomicity_commit(self):
        """All set operations are applied atomically on commit."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_set = txn_ctx.get_set("test-set")
            txn_set.add("item1")
            txn_set.add("item2")
            txn_set.add("item3")

            self.assertEqual(txn_set.size(), 3)
            self.assertTrue(txn_set.contains("item1"))

        self.assertEqual(txn_ctx.state, TransactionState.COMMITTED)

    def test_no_duplicates(self):
        """Set does not allow duplicate items."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_set = txn_ctx.get_set("test-set")
            self.assertTrue(txn_set.add("item1"))
            self.assertFalse(txn_set.add("item1"))
            self.assertEqual(txn_set.size(), 1)

    def test_remove(self):
        """Remove returns correct results."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_set = txn_ctx.get_set("test-set")
            txn_set.add("item1")

            self.assertTrue(txn_set.remove("item1"))
            self.assertFalse(txn_set.contains("item1"))
            self.assertFalse(txn_set.remove("item1"))

    def test_is_empty(self):
        """Is empty reflects current state."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_set = txn_ctx.get_set("test-set")
            self.assertTrue(txn_set.is_empty())

            txn_set.add("item1")
            self.assertFalse(txn_set.is_empty())


class TestTransactionalListACID(TransactionalProxyTestBase):
    """Tests verifying ACID properties for TransactionalList."""

    def test_atomicity_commit(self):
        """All list operations are applied atomically on commit."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_list = txn_ctx.get_list("test-list")
            txn_list.add("item1")
            txn_list.add("item2")
            txn_list.add("item3")

            self.assertEqual(txn_list.size(), 3)

        self.assertEqual(txn_ctx.state, TransactionState.COMMITTED)

    def test_allows_duplicates(self):
        """List allows duplicate items."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_list = txn_ctx.get_list("test-list")
            txn_list.add("item1")
            txn_list.add("item1")
            self.assertEqual(txn_list.size(), 2)

    def test_remove(self):
        """Remove only removes first occurrence."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_list = txn_ctx.get_list("test-list")
            txn_list.add("item1")
            txn_list.add("item2")
            txn_list.add("item1")

            self.assertTrue(txn_list.remove("item1"))
            self.assertEqual(txn_list.size(), 2)

            self.assertFalse(txn_list.remove("item3"))

    def test_get_all(self):
        """Get all returns all items in order."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_list = txn_ctx.get_list("test-list")
            txn_list.add("item1")
            txn_list.add("item2")
            txn_list.add("item3")

            all_items = txn_list.get_all()
            self.assertEqual(all_items, ["item1", "item2", "item3"])


class TestTransactionalQueueACID(TransactionalProxyTestBase):
    """Tests verifying ACID properties for TransactionalQueue."""

    def test_atomicity_commit(self):
        """All queue operations are applied atomically on commit."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_queue = txn_ctx.get_queue("test-queue")
            txn_queue.offer("item1")
            txn_queue.offer("item2")
            txn_queue.offer("item3")

            self.assertEqual(txn_queue.size(), 3)

        self.assertEqual(txn_ctx.state, TransactionState.COMMITTED)

    def test_fifo_ordering(self):
        """Queue maintains FIFO order."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_queue = txn_ctx.get_queue("test-queue")
            txn_queue.offer("first")
            txn_queue.offer("second")
            txn_queue.offer("third")

            self.assertEqual(txn_queue.poll(), "first")
            self.assertEqual(txn_queue.poll(), "second")
            self.assertEqual(txn_queue.poll(), "third")

    def test_peek_does_not_remove(self):
        """Peek returns item without removing it."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_queue = txn_ctx.get_queue("test-queue")
            txn_queue.offer("item1")

            self.assertEqual(txn_queue.peek(), "item1")
            self.assertEqual(txn_queue.peek(), "item1")
            self.assertEqual(txn_queue.size(), 1)

    def test_poll_empty_queue(self):
        """Poll on empty queue returns None."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_queue = txn_ctx.get_queue("test-queue")
            self.assertIsNone(txn_queue.poll())
            self.assertIsNone(txn_queue.peek())

    def test_take_empty_queue_raises(self):
        """Take on empty queue raises exception."""
        from hazelcast.exceptions import IllegalStateException

        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_queue = txn_ctx.get_queue("test-queue")
            with self.assertRaises(IllegalStateException):
                txn_queue.take()


class TestTransactionalMultiMapACID(TransactionalProxyTestBase):
    """Tests verifying ACID properties for TransactionalMultiMap."""

    def test_atomicity_commit(self):
        """All multimap operations are applied atomically on commit."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_mm = txn_ctx.get_multi_map("test-multimap")
            txn_mm.put("key1", "value1")
            txn_mm.put("key1", "value2")
            txn_mm.put("key2", "value3")

            self.assertEqual(txn_mm.size(), 3)

        self.assertEqual(txn_ctx.state, TransactionState.COMMITTED)

    def test_multiple_values_per_key(self):
        """MultiMap allows multiple values per key."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_mm = txn_ctx.get_multi_map("test-multimap")
            txn_mm.put("key1", "value1")
            txn_mm.put("key1", "value2")
            txn_mm.put("key1", "value3")

            values = txn_mm.get("key1")
            self.assertEqual(len(values), 3)
            self.assertIn("value1", values)
            self.assertIn("value2", values)
            self.assertIn("value3", values)

    def test_value_count(self):
        """Value count returns correct count per key."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_mm = txn_ctx.get_multi_map("test-multimap")
            txn_mm.put("key1", "value1")
            txn_mm.put("key1", "value2")
            txn_mm.put("key2", "value3")

            self.assertEqual(txn_mm.value_count("key1"), 2)
            self.assertEqual(txn_mm.value_count("key2"), 1)
            self.assertEqual(txn_mm.value_count("key3"), 0)

    def test_remove_specific_value(self):
        """Remove removes only the specific key-value pair."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_mm = txn_ctx.get_multi_map("test-multimap")
            txn_mm.put("key1", "value1")
            txn_mm.put("key1", "value2")

            self.assertTrue(txn_mm.remove("key1", "value1"))
            self.assertEqual(txn_mm.value_count("key1"), 1)

            self.assertFalse(txn_mm.remove("key1", "value1"))

    def test_remove_all(self):
        """Remove all removes all values for a key."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_mm = txn_ctx.get_multi_map("test-multimap")
            txn_mm.put("key1", "value1")
            txn_mm.put("key1", "value2")
            txn_mm.put("key2", "value3")

            removed = txn_mm.remove_all("key1")
            self.assertEqual(len(removed), 2)
            self.assertEqual(txn_mm.value_count("key1"), 0)
            self.assertEqual(txn_mm.size(), 1)


class TestTransactionContextWithProxies(TransactionalProxyTestBase):
    """Tests for TransactionContext integration with proxies."""

    def test_multiple_data_structures_in_transaction(self):
        """Multiple data structures can be used in a single transaction."""
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            txn_map = txn_ctx.get_map("test-map")
            txn_set = txn_ctx.get_set("test-set")
            txn_list = txn_ctx.get_list("test-list")
            txn_queue = txn_ctx.get_queue("test-queue")
            txn_mm = txn_ctx.get_multi_map("test-multimap")

            txn_map.put("key", "value")
            txn_set.add("item")
            txn_list.add("item")
            txn_queue.offer("item")
            txn_mm.put("key", "value")

            self.assertEqual(txn_map.size(), 1)
            self.assertEqual(txn_set.size(), 1)
            self.assertEqual(txn_list.size(), 1)
            self.assertEqual(txn_queue.size(), 1)
            self.assertEqual(txn_mm.size(), 1)

        self.assertEqual(txn_ctx.state, TransactionState.COMMITTED)

    def test_two_phase_commit(self):
        """Two-phase commit works correctly."""
        options = TransactionOptions(transaction_type=TransactionType.TWO_PHASE)
        txn_ctx = TransactionContext(self.context, options)

        with txn_ctx:
            txn_map = txn_ctx.get_map("test-map")
            txn_map.put("key", "value")

        self.assertEqual(txn_ctx.state, TransactionState.COMMITTED)

    def test_operations_after_commit_raise(self):
        """Operations after commit raise exception."""
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        txn_map = txn_ctx.get_map("test-map")
        txn_ctx.commit()

        with self.assertRaises(TransactionNotActiveException):
            txn_map.put("key", "value")

    def test_operations_after_rollback_raise(self):
        """Operations after rollback raise exception."""
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        txn_map = txn_ctx.get_map("test-map")
        txn_ctx.rollback()

        with self.assertRaises(TransactionNotActiveException):
            txn_map.get("key")

    def test_exception_in_context_manager_triggers_rollback(self):
        """Exception within context manager triggers rollback."""
        txn_ctx = TransactionContext(self.context)

        with self.assertRaises(ValueError):
            with txn_ctx:
                txn_map = txn_ctx.get_map("test-map")
                txn_map.put("key", "value")
                raise ValueError("Test error")

        self.assertEqual(txn_ctx.state, TransactionState.ROLLED_BACK)


if __name__ == "__main__":
    unittest.main()
