"""Tests for Transaction API."""

import unittest
from unittest.mock import MagicMock, patch
import time

from hazelcast.transaction import (
    TransactionType,
    TransactionState,
    TransactionOptions,
    TransactionContext,
    TransactionException,
    TransactionNotActiveException,
    TransactionTimedOutException,
    TransactionalProxy,
    TransactionalMap,
    TransactionalSet,
    TransactionalList,
    TransactionalQueue,
    TransactionalMultiMap,
)
from hazelcast.proxy.base import ProxyContext
from hazelcast.exceptions import IllegalStateException


class TestTransactionType(unittest.TestCase):
    """Tests for TransactionType enum."""

    def test_one_phase_value(self):
        self.assertEqual(TransactionType.ONE_PHASE.value, 1)

    def test_two_phase_value(self):
        self.assertEqual(TransactionType.TWO_PHASE.value, 2)


class TestTransactionState(unittest.TestCase):
    """Tests for TransactionState enum."""

    def test_all_states_exist(self):
        states = [
            TransactionState.NO_TXN,
            TransactionState.ACTIVE,
            TransactionState.PREPARING,
            TransactionState.PREPARED,
            TransactionState.COMMITTING,
            TransactionState.COMMITTED,
            TransactionState.ROLLING_BACK,
            TransactionState.ROLLED_BACK,
        ]
        self.assertEqual(len(states), 8)


class TestTransactionOptions(unittest.TestCase):
    """Tests for TransactionOptions configuration."""

    def test_default_values(self):
        options = TransactionOptions()
        self.assertEqual(options.timeout, 120.0)
        self.assertEqual(options.durability, 1)
        self.assertEqual(options.transaction_type, TransactionType.ONE_PHASE)

    def test_custom_timeout(self):
        options = TransactionOptions(timeout=60.0)
        self.assertEqual(options.timeout, 60.0)
        self.assertEqual(options.timeout_millis, 60000)

    def test_custom_durability(self):
        options = TransactionOptions(durability=3)
        self.assertEqual(options.durability, 3)

    def test_two_phase_type(self):
        options = TransactionOptions(transaction_type=TransactionType.TWO_PHASE)
        self.assertEqual(options.transaction_type, TransactionType.TWO_PHASE)

    def test_invalid_timeout_raises(self):
        with self.assertRaises(ValueError):
            TransactionOptions(timeout=0)

        with self.assertRaises(ValueError):
            TransactionOptions(timeout=-1)

    def test_invalid_durability_raises(self):
        with self.assertRaises(ValueError):
            TransactionOptions(durability=-1)

    def test_repr(self):
        options = TransactionOptions(
            timeout=30.0,
            durability=2,
            transaction_type=TransactionType.TWO_PHASE,
        )
        repr_str = repr(options)
        self.assertIn("timeout=30.0", repr_str)
        self.assertIn("durability=2", repr_str)
        self.assertIn("TWO_PHASE", repr_str)


class TestTransactionContext(unittest.TestCase):
    """Tests for TransactionContext."""

    def setUp(self):
        self.mock_invocation_service = MagicMock()
        self.mock_serialization_service = MagicMock()
        self.context = ProxyContext(
            invocation_service=self.mock_invocation_service,
            serialization_service=self.mock_serialization_service,
        )

    def test_initial_state(self):
        txn_ctx = TransactionContext(self.context)
        self.assertEqual(txn_ctx.state, TransactionState.NO_TXN)
        self.assertIsNone(txn_ctx.txn_id)

    def test_begin_changes_state_to_active(self):
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        self.assertEqual(txn_ctx.state, TransactionState.ACTIVE)
        self.assertIsNotNone(txn_ctx.txn_id)

    def test_begin_returns_self(self):
        txn_ctx = TransactionContext(self.context)
        result = txn_ctx.begin()
        self.assertIs(result, txn_ctx)

    def test_commit_changes_state_to_committed(self):
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        txn_ctx.commit()
        self.assertEqual(txn_ctx.state, TransactionState.COMMITTED)

    def test_rollback_changes_state_to_rolled_back(self):
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        txn_ctx.rollback()
        self.assertEqual(txn_ctx.state, TransactionState.ROLLED_BACK)

    def test_commit_without_begin_raises(self):
        txn_ctx = TransactionContext(self.context)
        with self.assertRaises(TransactionNotActiveException):
            txn_ctx.commit()

    def test_rollback_without_begin_is_noop(self):
        txn_ctx = TransactionContext(self.context)
        txn_ctx.rollback()
        self.assertEqual(txn_ctx.state, TransactionState.NO_TXN)

    def test_double_begin_raises(self):
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        with self.assertRaises(IllegalStateException):
            txn_ctx.begin()

    def test_double_commit_raises(self):
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        txn_ctx.commit()
        with self.assertRaises(TransactionNotActiveException):
            txn_ctx.commit()

    def test_commit_after_rollback_raises(self):
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        txn_ctx.rollback()
        with self.assertRaises(TransactionNotActiveException):
            txn_ctx.commit()

    def test_two_phase_commit_goes_through_prepare(self):
        options = TransactionOptions(transaction_type=TransactionType.TWO_PHASE)
        txn_ctx = TransactionContext(self.context, options)
        txn_ctx.begin()
        txn_ctx.commit()
        self.assertEqual(txn_ctx.state, TransactionState.COMMITTED)

    def test_context_manager_commits_on_success(self):
        txn_ctx = TransactionContext(self.context)
        with txn_ctx:
            pass
        self.assertEqual(txn_ctx.state, TransactionState.COMMITTED)

    def test_context_manager_rolls_back_on_exception(self):
        txn_ctx = TransactionContext(self.context)
        with self.assertRaises(ValueError):
            with txn_ctx:
                raise ValueError("test error")
        self.assertEqual(txn_ctx.state, TransactionState.ROLLED_BACK)

    def test_get_map_returns_transactional_map(self):
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        txn_map = txn_ctx.get_map("test-map")
        self.assertIsInstance(txn_map, TransactionalMap)
        self.assertEqual(txn_map.name, "test-map")

    def test_get_map_without_begin_raises(self):
        txn_ctx = TransactionContext(self.context)
        with self.assertRaises(TransactionNotActiveException):
            txn_ctx.get_map("test-map")

    def test_get_map_returns_same_instance(self):
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        map1 = txn_ctx.get_map("test-map")
        map2 = txn_ctx.get_map("test-map")
        self.assertIs(map1, map2)

    def test_get_set_returns_transactional_set(self):
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        txn_set = txn_ctx.get_set("test-set")
        self.assertIsInstance(txn_set, TransactionalSet)

    def test_get_list_returns_transactional_list(self):
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        txn_list = txn_ctx.get_list("test-list")
        self.assertIsInstance(txn_list, TransactionalList)

    def test_get_queue_returns_transactional_queue(self):
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        txn_queue = txn_ctx.get_queue("test-queue")
        self.assertIsInstance(txn_queue, TransactionalQueue)

    def test_get_multi_map_returns_transactional_multi_map(self):
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        txn_mm = txn_ctx.get_multi_map("test-multimap")
        self.assertIsInstance(txn_mm, TransactionalMultiMap)

    def test_timeout_raises_exception(self):
        options = TransactionOptions(timeout=0.01)
        txn_ctx = TransactionContext(self.context, options)
        txn_ctx.begin()
        time.sleep(0.02)
        with self.assertRaises(TransactionTimedOutException):
            txn_ctx.commit()

    def test_repr(self):
        txn_ctx = TransactionContext(self.context)
        txn_ctx.begin()
        repr_str = repr(txn_ctx)
        self.assertIn("TransactionContext", repr_str)
        self.assertIn("ACTIVE", repr_str)
        self.assertIn("ONE_PHASE", repr_str)


class TestTransactionalMap(unittest.TestCase):
    """Tests for TransactionalMap proxy."""

    def setUp(self):
        self.mock_context = ProxyContext(
            invocation_service=MagicMock(),
            serialization_service=MagicMock(),
        )
        self.txn_ctx = TransactionContext(self.mock_context)
        self.txn_ctx.begin()
        self.txn_map = self.txn_ctx.get_map("test-map")

    def test_put_requires_active_transaction(self):
        self.txn_ctx.commit()
        with self.assertRaises(TransactionNotActiveException):
            self.txn_map.put("key", "value")

    def test_get_requires_active_transaction(self):
        self.txn_ctx.commit()
        with self.assertRaises(TransactionNotActiveException):
            self.txn_map.get("key")

    def test_remove_requires_active_transaction(self):
        self.txn_ctx.commit()
        with self.assertRaises(TransactionNotActiveException):
            self.txn_map.remove("key")

    def test_contains_key_requires_active_transaction(self):
        self.txn_ctx.commit()
        with self.assertRaises(TransactionNotActiveException):
            self.txn_map.contains_key("key")

    def test_size_requires_active_transaction(self):
        self.txn_ctx.commit()
        with self.assertRaises(TransactionNotActiveException):
            self.txn_map.size()

    def test_is_empty_delegates_to_size(self):
        result = self.txn_map.is_empty()
        self.assertTrue(result)

    def test_repr(self):
        repr_str = repr(self.txn_map)
        self.assertIn("TransactionalMap", repr_str)
        self.assertIn("test-map", repr_str)


class TestTransactionalSet(unittest.TestCase):
    """Tests for TransactionalSet proxy."""

    def setUp(self):
        self.mock_context = ProxyContext(
            invocation_service=MagicMock(),
            serialization_service=MagicMock(),
        )
        self.txn_ctx = TransactionContext(self.mock_context)
        self.txn_ctx.begin()
        self.txn_set = self.txn_ctx.get_set("test-set")

    def test_add_requires_active_transaction(self):
        self.txn_ctx.commit()
        with self.assertRaises(TransactionNotActiveException):
            self.txn_set.add("item")

    def test_remove_requires_active_transaction(self):
        self.txn_ctx.commit()
        with self.assertRaises(TransactionNotActiveException):
            self.txn_set.remove("item")


class TestTransactionalQueue(unittest.TestCase):
    """Tests for TransactionalQueue proxy."""

    def setUp(self):
        self.mock_context = ProxyContext(
            invocation_service=MagicMock(),
            serialization_service=MagicMock(),
        )
        self.txn_ctx = TransactionContext(self.mock_context)
        self.txn_ctx.begin()
        self.txn_queue = self.txn_ctx.get_queue("test-queue")

    def test_offer_requires_active_transaction(self):
        self.txn_ctx.commit()
        with self.assertRaises(TransactionNotActiveException):
            self.txn_queue.offer("item")

    def test_poll_requires_active_transaction(self):
        self.txn_ctx.commit()
        with self.assertRaises(TransactionNotActiveException):
            self.txn_queue.poll()


class TestTransactionExceptions(unittest.TestCase):
    """Tests for transaction exception classes."""

    def test_transaction_exception(self):
        ex = TransactionException("test error")
        self.assertEqual(str(ex), "test error")

    def test_transaction_exception_with_cause(self):
        cause = ValueError("original error")
        ex = TransactionException("wrapped error", cause=cause)
        self.assertIn("wrapped error", str(ex))
        self.assertEqual(ex.cause, cause)

    def test_transaction_not_active_exception(self):
        ex = TransactionNotActiveException()
        self.assertIn("not active", str(ex))

    def test_transaction_timed_out_exception(self):
        ex = TransactionTimedOutException()
        self.assertIn("timed out", str(ex))


if __name__ == "__main__":
    unittest.main()
