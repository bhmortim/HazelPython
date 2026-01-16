"""Unit tests for hazelcast/transaction.py"""

import time
import unittest
from unittest.mock import MagicMock, patch

from hazelcast.transaction import (
    TransactionType,
    TransactionState,
    _VALID_STATE_TRANSITIONS,
    TransactionException,
    TransactionNotActiveException,
    TransactionTimedOutException,
    TransactionOptions,
    TransactionContext,
)
from hazelcast.exceptions import IllegalStateException, HazelcastException


class TestTransactionTypeEnum(unittest.TestCase):
    def test_one_phase_value(self):
        self.assertEqual(TransactionType.ONE_PHASE.value, 1)

    def test_two_phase_value(self):
        self.assertEqual(TransactionType.TWO_PHASE.value, 2)


class TestTransactionStateEnum(unittest.TestCase):
    def test_no_txn(self):
        self.assertEqual(TransactionState.NO_TXN.value, 0)

    def test_active(self):
        self.assertEqual(TransactionState.ACTIVE.value, 1)

    def test_preparing(self):
        self.assertEqual(TransactionState.PREPARING.value, 2)

    def test_prepared(self):
        self.assertEqual(TransactionState.PREPARED.value, 3)

    def test_committing(self):
        self.assertEqual(TransactionState.COMMITTING.value, 4)

    def test_committed(self):
        self.assertEqual(TransactionState.COMMITTED.value, 5)

    def test_rolling_back(self):
        self.assertEqual(TransactionState.ROLLING_BACK.value, 6)

    def test_rolled_back(self):
        self.assertEqual(TransactionState.ROLLED_BACK.value, 7)


class TestValidStateTransitions(unittest.TestCase):
    def test_mapping_exists(self):
        self.assertIsInstance(_VALID_STATE_TRANSITIONS, dict)

    def test_no_txn_transitions(self):
        self.assertEqual(_VALID_STATE_TRANSITIONS[TransactionState.NO_TXN], {TransactionState.ACTIVE})

    def test_active_transitions(self):
        expected = {TransactionState.PREPARING, TransactionState.COMMITTING, TransactionState.ROLLING_BACK}
        self.assertEqual(_VALID_STATE_TRANSITIONS[TransactionState.ACTIVE], expected)

    def test_committed_has_no_transitions(self):
        self.assertEqual(_VALID_STATE_TRANSITIONS[TransactionState.COMMITTED], set())

    def test_rolled_back_has_no_transitions(self):
        self.assertEqual(_VALID_STATE_TRANSITIONS[TransactionState.ROLLED_BACK], set())


class TestTransactionException(unittest.TestCase):
    def test_init_default_message(self):
        exc = TransactionException()
        self.assertIn("Transaction error", str(exc))

    def test_init_with_message(self):
        exc = TransactionException("Custom error")
        self.assertIn("Custom error", str(exc))

    def test_init_with_cause(self):
        cause = ValueError("root cause")
        exc = TransactionException("Error", cause=cause)
        self.assertEqual(exc.__cause__, cause)

    def test_inheritance(self):
        self.assertTrue(issubclass(TransactionException, HazelcastException))


class TestTransactionNotActiveException(unittest.TestCase):
    def test_default_message(self):
        exc = TransactionNotActiveException()
        self.assertIn("not active", str(exc))

    def test_custom_message(self):
        exc = TransactionNotActiveException("Custom message")
        self.assertIn("Custom message", str(exc))

    def test_inheritance(self):
        self.assertTrue(issubclass(TransactionNotActiveException, TransactionException))


class TestTransactionTimedOutException(unittest.TestCase):
    def test_default_message(self):
        exc = TransactionTimedOutException()
        self.assertIn("timed out", str(exc))

    def test_custom_message(self):
        exc = TransactionTimedOutException("Timeout after 60s")
        self.assertIn("Timeout after 60s", str(exc))

    def test_inheritance(self):
        self.assertTrue(issubclass(TransactionTimedOutException, TransactionException))


class TestTransactionOptions(unittest.TestCase):
    def test_default_values(self):
        options = TransactionOptions()
        self.assertEqual(options.timeout, 120.0)
        self.assertEqual(options.durability, 1)
        self.assertEqual(options.transaction_type, TransactionType.ONE_PHASE)

    def test_custom_timeout(self):
        options = TransactionOptions(timeout=60.0)
        self.assertEqual(options.timeout, 60.0)

    def test_custom_durability(self):
        options = TransactionOptions(durability=3)
        self.assertEqual(options.durability, 3)

    def test_custom_transaction_type(self):
        options = TransactionOptions(transaction_type=TransactionType.TWO_PHASE)
        self.assertEqual(options.transaction_type, TransactionType.TWO_PHASE)

    def test_timeout_millis(self):
        options = TransactionOptions(timeout=5.5)
        self.assertEqual(options.timeout_millis, 5500)

    def test_timeout_zero_raises_error(self):
        with self.assertRaises(ValueError):
            TransactionOptions(timeout=0)

    def test_timeout_negative_raises_error(self):
        with self.assertRaises(ValueError):
            TransactionOptions(timeout=-1)

    def test_durability_negative_raises_error(self):
        with self.assertRaises(ValueError):
            TransactionOptions(durability=-1)

    def test_durability_zero_allowed(self):
        options = TransactionOptions(durability=0)
        self.assertEqual(options.durability, 0)

    def test_repr(self):
        options = TransactionOptions(timeout=30.0, durability=2, transaction_type=TransactionType.TWO_PHASE)
        result = repr(options)
        self.assertIn("timeout=30.0", result)
        self.assertIn("durability=2", result)
        self.assertIn("TWO_PHASE", result)


class TestTransactionContext(unittest.TestCase):
    def setUp(self):
        self.mock_context = MagicMock()

    def test_init_with_defaults(self):
        ctx = TransactionContext(self.mock_context)
        self.assertIsNone(ctx.txn_id)
        self.assertEqual(ctx.state, TransactionState.NO_TXN)
        self.assertIsNotNone(ctx.options)

    def test_init_with_options(self):
        options = TransactionOptions(timeout=60.0)
        ctx = TransactionContext(self.mock_context, options)
        self.assertEqual(ctx.options.timeout, 60.0)

    def test_txn_id_none_before_begin(self):
        ctx = TransactionContext(self.mock_context)
        self.assertIsNone(ctx.txn_id)

    def test_begin_transitions_to_active(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        self.assertEqual(ctx.state, TransactionState.ACTIVE)

    def test_begin_generates_txn_id(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        self.assertIsNotNone(ctx.txn_id)
        self.assertIsInstance(ctx.txn_id, str)

    def test_begin_returns_self(self):
        ctx = TransactionContext(self.mock_context)
        result = ctx.begin()
        self.assertIs(result, ctx)

    def test_commit_requires_active_state(self):
        ctx = TransactionContext(self.mock_context)
        with self.assertRaises(TransactionNotActiveException):
            ctx.commit()

    def test_commit_transitions_to_committed(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        ctx.commit()
        self.assertEqual(ctx.state, TransactionState.COMMITTED)

    def test_commit_two_phase_calls_prepare(self):
        options = TransactionOptions(transaction_type=TransactionType.TWO_PHASE)
        ctx = TransactionContext(self.mock_context, options)
        ctx.begin()
        with patch.object(ctx, "_prepare") as mock_prepare:
            with patch.object(ctx, "_do_commit"):
                ctx.commit()
                mock_prepare.assert_called_once()

    def test_rollback_transitions_to_rolled_back(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        ctx.rollback()
        self.assertEqual(ctx.state, TransactionState.ROLLED_BACK)

    def test_rollback_ignores_if_no_txn(self):
        ctx = TransactionContext(self.mock_context)
        ctx.rollback()
        self.assertEqual(ctx.state, TransactionState.NO_TXN)

    def test_rollback_ignores_if_committed(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        ctx.commit()
        ctx.rollback()
        self.assertEqual(ctx.state, TransactionState.COMMITTED)

    def test_rollback_ignores_if_rolled_back(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        ctx.rollback()
        ctx.rollback()
        self.assertEqual(ctx.state, TransactionState.ROLLED_BACK)

    def test_check_timeout_raises_when_exceeded(self):
        options = TransactionOptions(timeout=0.001)
        ctx = TransactionContext(self.mock_context, options)
        ctx.begin()
        time.sleep(0.01)
        with self.assertRaises(TransactionTimedOutException):
            ctx._check_timeout()

    def test_check_timeout_does_not_raise_when_not_exceeded(self):
        options = TransactionOptions(timeout=10.0)
        ctx = TransactionContext(self.mock_context, options)
        ctx.begin()
        ctx._check_timeout()

    def test_transition_state_invalid_raises(self):
        ctx = TransactionContext(self.mock_context)
        with self.assertRaises(IllegalStateException):
            ctx._transition_state(TransactionState.COMMITTED)

    def test_check_active_raises_when_not_active(self):
        ctx = TransactionContext(self.mock_context)
        with self.assertRaises(TransactionNotActiveException):
            ctx._check_active()

    def test_check_active_passes_when_active(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        ctx._check_active()

    def test_get_map_requires_active(self):
        ctx = TransactionContext(self.mock_context)
        with self.assertRaises(TransactionNotActiveException):
            ctx.get_map("test-map")

    def test_get_map_returns_proxy(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        result = ctx.get_map("test-map")
        self.assertIsNotNone(result)

    def test_get_map_caches_proxy(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        proxy1 = ctx.get_map("test-map")
        proxy2 = ctx.get_map("test-map")
        self.assertIs(proxy1, proxy2)

    def test_get_set_requires_active(self):
        ctx = TransactionContext(self.mock_context)
        with self.assertRaises(TransactionNotActiveException):
            ctx.get_set("test-set")

    def test_get_set_returns_proxy(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        result = ctx.get_set("test-set")
        self.assertIsNotNone(result)

    def test_get_list_requires_active(self):
        ctx = TransactionContext(self.mock_context)
        with self.assertRaises(TransactionNotActiveException):
            ctx.get_list("test-list")

    def test_get_list_returns_proxy(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        result = ctx.get_list("test-list")
        self.assertIsNotNone(result)

    def test_get_queue_requires_active(self):
        ctx = TransactionContext(self.mock_context)
        with self.assertRaises(TransactionNotActiveException):
            ctx.get_queue("test-queue")

    def test_get_queue_returns_proxy(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        result = ctx.get_queue("test-queue")
        self.assertIsNotNone(result)

    def test_get_multi_map_requires_active(self):
        ctx = TransactionContext(self.mock_context)
        with self.assertRaises(TransactionNotActiveException):
            ctx.get_multi_map("test-multimap")

    def test_get_multi_map_returns_proxy(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        result = ctx.get_multi_map("test-multimap")
        self.assertIsNotNone(result)

    def test_context_manager_enter_calls_begin(self):
        ctx = TransactionContext(self.mock_context)
        with ctx as entered:
            self.assertIs(entered, ctx)
            self.assertEqual(ctx.state, TransactionState.ACTIVE)

    def test_context_manager_commits_on_success(self):
        ctx = TransactionContext(self.mock_context)
        with ctx:
            pass
        self.assertEqual(ctx.state, TransactionState.COMMITTED)

    def test_context_manager_rollback_on_exception(self):
        ctx = TransactionContext(self.mock_context)
        try:
            with ctx:
                raise ValueError("Test error")
        except ValueError:
            pass
        self.assertEqual(ctx.state, TransactionState.ROLLED_BACK)

    def test_repr(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        result = repr(ctx)
        self.assertIn("TransactionContext", result)
        self.assertIn(ctx.txn_id, result)
        self.assertIn("ACTIVE", result)


class TestTransactionContextEdgeCases(unittest.TestCase):
    def setUp(self):
        self.mock_context = MagicMock()

    def test_commit_with_do_commit_exception(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        with patch.object(ctx, "_do_commit", side_effect=Exception("Commit failed")):
            with self.assertRaises(TransactionException):
                ctx.commit()

    def test_rollback_with_do_rollback_exception(self):
        ctx = TransactionContext(self.mock_context)
        ctx.begin()
        with patch.object(ctx, "_do_rollback", side_effect=Exception("Rollback failed")):
            with self.assertRaises(TransactionException):
                ctx.rollback()
            self.assertEqual(ctx.state, TransactionState.ROLLED_BACK)

    def test_prepare_called_for_two_phase(self):
        options = TransactionOptions(transaction_type=TransactionType.TWO_PHASE)
        ctx = TransactionContext(self.mock_context, options)
        ctx.begin()
        ctx.commit()
        self.assertEqual(ctx.state, TransactionState.COMMITTED)


if __name__ == "__main__":
    unittest.main()
