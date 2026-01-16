"""Unit tests for Hazelcast transaction functionality."""

import unittest
from unittest.mock import MagicMock, patch

from hazelcast.transaction import (
    TransactionContext,
    TransactionOptions,
    TransactionType,
    TransactionState,
    TransactionNotActiveException,
    TransactionTimedOutException,
)
from hazelcast.exceptions import IllegalStateException


class TestTransactionOptions(unittest.TestCase):
    """Tests for TransactionOptions configuration."""

    def test_default_options(self):
        """Test default transaction options."""
        options = TransactionOptions()
        self.assertEqual(120.0, options.timeout)
        self.assertEqual(1, options.durability)
        self.assertEqual(TransactionType.ONE_PHASE, options.transaction_type)

    def test_custom_options(self):
        """Test custom transaction options."""
        options = TransactionOptions(
            timeout=60.0,
            durability=2,
            transaction_type=TransactionType.TWO_PHASE,
        )
        self.assertEqual(60.0, options.timeout)
        self.assertEqual(2, options.durability)
        self.assertEqual(TransactionType.TWO_PHASE, options.transaction_type)

    def test_timeout_millis(self):
        """Test timeout conversion to milliseconds."""
        options = TransactionOptions(timeout=30.5)
        self.assertEqual(30500, options.timeout_millis)

    def test_invalid_timeout_raises(self):
        """Test that non-positive timeout raises ValueError."""
        with self.assertRaises(ValueError):
            TransactionOptions(timeout=0)
        with self.assertRaises(ValueError):
            TransactionOptions(timeout=-1)

    def test_invalid_durability_raises(self):
        """Test that negative durability raises ValueError."""
        with self.assertRaises(ValueError):
            TransactionOptions(durability=-1)


class TestTransactionContext(unittest.TestCase):
    """Tests for TransactionContext lifecycle."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_context = MagicMock()
        self.txn_ctx = TransactionContext(self.mock_context)

    def test_initial_state(self):
        """Test transaction starts in NO_TXN state."""
        self.assertEqual(TransactionState.NO_TXN, self.txn_ctx.state)
        self.assertIsNone(self.txn_ctx.txn_id)

    def test_begin_transitions_to_active(self):
        """Test begin() transitions state to ACTIVE."""
        result = self.txn_ctx.begin()
        self.assertEqual(TransactionState.ACTIVE, self.txn_ctx.state)
        self.assertIsNotNone(self.txn_ctx.txn_id)
        self.assertIs(result, self.txn_ctx)

    def test_begin_returns_self_for_chaining(self):
        """Test begin() returns self for method chaining."""
        result = self.txn_ctx.begin()
        self.assertIs(result, self.txn_ctx)

    def test_commit_after_begin(self):
        """Test commit() succeeds after begin()."""
        self.txn_ctx.begin()
        self.txn_ctx.commit()
        self.assertEqual(TransactionState.COMMITTED, self.txn_ctx.state)

    def test_commit_without_begin_raises(self):
        """Test commit() raises when transaction not active."""
        with self.assertRaises(TransactionNotActiveException):
            self.txn_ctx.commit()

    def test_rollback_after_begin(self):
        """Test rollback() succeeds after begin()."""
        self.txn_ctx.begin()
        self.txn_ctx.rollback()
        self.assertEqual(TransactionState.ROLLED_BACK, self.txn_ctx.state)

    def test_rollback_without_begin_is_noop(self):
        """Test rollback() is no-op when not active."""
        self.txn_ctx.rollback()
        self.assertEqual(TransactionState.NO_TXN, self.txn_ctx.state)

    def test_rollback_after_commit_is_noop(self):
        """Test rollback() is no-op after commit."""
        self.txn_ctx.begin()
        self.txn_ctx.commit()
        self.txn_ctx.rollback()
        self.assertEqual(TransactionState.COMMITTED, self.txn_ctx.state)

    def test_double_begin_raises(self):
        """Test begin() raises when already active."""
        self.txn_ctx.begin()
        with self.assertRaises(IllegalStateException):
            self.txn_ctx.begin()

    def test_double_commit_raises(self):
        """Test commit() raises when already committed."""
        self.txn_ctx.begin()
        self.txn_ctx.commit()
        with self.assertRaises(TransactionNotActiveException):
            self.txn_ctx.commit()


class TestTransactionContextManager(unittest.TestCase):
    """Tests for TransactionContext as context manager."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_context = MagicMock()

    def test_context_manager_commits_on_success(self):
        """Test context manager commits on successful exit."""
        txn_ctx = TransactionContext(self.mock_context)
        with txn_ctx as ctx:
            self.assertEqual(TransactionState.ACTIVE, ctx.state)
            self.assertIsNotNone(ctx.txn_id)
        self.assertEqual(TransactionState.COMMITTED, txn_ctx.state)

    def test_context_manager_rollback_on_exception(self):
        """Test context manager rolls back on exception."""
        txn_ctx = TransactionContext(self.mock_context)
        with self.assertRaises(ValueError):
            with txn_ctx:
                raise ValueError("test error")
        self.assertEqual(TransactionState.ROLLED_BACK, txn_ctx.state)

    def test_context_manager_returns_context(self):
        """Test context manager returns the transaction context."""
        txn_ctx = TransactionContext(self.mock_context)
        with txn_ctx as ctx:
            self.assertIs(ctx, txn_ctx)


class TestTransactionTwoPhase(unittest.TestCase):
    """Tests for two-phase commit transactions."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_context = MagicMock()
        options = TransactionOptions(transaction_type=TransactionType.TWO_PHASE)
        self.txn_ctx = TransactionContext(self.mock_context, options)

    def test_two_phase_commit_goes_through_prepare(self):
        """Test two-phase commit transitions through PREPARED state."""
        self.txn_ctx.begin()
        self.txn_ctx.commit()
        self.assertEqual(TransactionState.COMMITTED, self.txn_ctx.state)

    def test_options_preserved(self):
        """Test that transaction options are preserved."""
        self.assertEqual(
            TransactionType.TWO_PHASE,
            self.txn_ctx.options.transaction_type,
        )


class TestTransactionTimeout(unittest.TestCase):
    """Tests for transaction timeout handling."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_context = MagicMock()

    def test_timeout_raises_on_commit(self):
        """Test that timed out transaction raises on commit."""
        options = TransactionOptions(timeout=0.001)
        txn_ctx = TransactionContext(self.mock_context, options)
        txn_ctx.begin()

        import time
        time.sleep(0.01)

        with self.assertRaises(TransactionTimedOutException):
            txn_ctx.commit()


class TestTransactionProxies(unittest.TestCase):
    """Tests for transactional proxy access."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_context = MagicMock()
        self.txn_ctx = TransactionContext(self.mock_context)

    def test_get_map_requires_active_transaction(self):
        """Test get_map() requires active transaction."""
        with self.assertRaises(TransactionNotActiveException):
            self.txn_ctx.get_map("test-map")

    def test_get_map_returns_transactional_map(self):
        """Test get_map() returns TransactionalMap after begin()."""
        self.txn_ctx.begin()
        txn_map = self.txn_ctx.get_map("test-map")
        self.assertIsNotNone(txn_map)

    def test_get_map_returns_same_instance(self):
        """Test get_map() returns same proxy for same name."""
        self.txn_ctx.begin()
        map1 = self.txn_ctx.get_map("test-map")
        map2 = self.txn_ctx.get_map("test-map")
        self.assertIs(map1, map2)

    def test_get_set_requires_active_transaction(self):
        """Test get_set() requires active transaction."""
        with self.assertRaises(TransactionNotActiveException):
            self.txn_ctx.get_set("test-set")

    def test_get_list_requires_active_transaction(self):
        """Test get_list() requires active transaction."""
        with self.assertRaises(TransactionNotActiveException):
            self.txn_ctx.get_list("test-list")

    def test_get_queue_requires_active_transaction(self):
        """Test get_queue() requires active transaction."""
        with self.assertRaises(TransactionNotActiveException):
            self.txn_ctx.get_queue("test-queue")

    def test_get_multi_map_requires_active_transaction(self):
        """Test get_multi_map() requires active transaction."""
        with self.assertRaises(TransactionNotActiveException):
            self.txn_ctx.get_multi_map("test-multimap")


class TestNewTransactionContextReturnsCorrectType(unittest.TestCase):
    """Tests to verify new_transaction_context returns TransactionContext."""

    @patch("hazelcast.client.SerializationService")
    @patch("hazelcast.client.PartitionService")
    @patch("hazelcast.client.ConnectionManager")
    @patch("hazelcast.client.InvocationService")
    @patch("hazelcast.client.MetricsRegistry")
    def test_new_transaction_context_returns_transaction_context(
        self, mock_metrics, mock_invocation, mock_conn, mock_partition, mock_serial
    ):
        """Test that new_transaction_context returns TransactionContext, not JetService."""
        from hazelcast.client import HazelcastClient
        from hazelcast.config import ClientConfig

        client = HazelcastClient(ClientConfig())
        client.start()

        ctx = client.new_transaction_context()

        self.assertIsInstance(ctx, TransactionContext)
        self.assertEqual(TransactionState.NO_TXN, ctx.state)

        client.shutdown()

    @patch("hazelcast.client.SerializationService")
    @patch("hazelcast.client.PartitionService")
    @patch("hazelcast.client.ConnectionManager")
    @patch("hazelcast.client.InvocationService")
    @patch("hazelcast.client.MetricsRegistry")
    def test_new_transaction_context_with_options(
        self, mock_metrics, mock_invocation, mock_conn, mock_partition, mock_serial
    ):
        """Test new_transaction_context accepts and uses options."""
        from hazelcast.client import HazelcastClient
        from hazelcast.config import ClientConfig

        client = HazelcastClient(ClientConfig())
        client.start()

        options = TransactionOptions(
            timeout=30.0,
            transaction_type=TransactionType.TWO_PHASE,
        )
        ctx = client.new_transaction_context(options)

        self.assertIsInstance(ctx, TransactionContext)
        self.assertEqual(30.0, ctx.options.timeout)
        self.assertEqual(TransactionType.TWO_PHASE, ctx.options.transaction_type)

        client.shutdown()

    @patch("hazelcast.client.SerializationService")
    @patch("hazelcast.client.PartitionService")
    @patch("hazelcast.client.ConnectionManager")
    @patch("hazelcast.client.InvocationService")
    @patch("hazelcast.client.MetricsRegistry")
    def test_new_transaction_context_creates_fresh_instance(
        self, mock_metrics, mock_invocation, mock_conn, mock_partition, mock_serial
    ):
        """Test each call to new_transaction_context creates a new instance."""
        from hazelcast.client import HazelcastClient
        from hazelcast.config import ClientConfig

        client = HazelcastClient(ClientConfig())
        client.start()

        ctx1 = client.new_transaction_context()
        ctx2 = client.new_transaction_context()

        self.assertIsNot(ctx1, ctx2)

        client.shutdown()


if __name__ == "__main__":
    unittest.main()
