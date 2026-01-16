"""Tests for Transaction codecs and service."""

import struct
import uuid
import pytest
import time
from unittest.mock import Mock, MagicMock

from hazelcast.protocol.codec import (
    TransactionCodec,
    TXN_CREATE,
    TXN_COMMIT,
    TXN_ROLLBACK,
    TXN_TYPE_ONE_PHASE,
    TXN_TYPE_TWO_PHASE,
    REQUEST_HEADER_SIZE,
    RESPONSE_HEADER_SIZE,
    LONG_SIZE,
    INT_SIZE,
    UUID_SIZE,
    FixSizedTypesCodec,
)
from hazelcast.transaction import (
    Transaction,
    TransactionService,
    TransactionOptions,
    TransactionContext,
    TransactionType,
    TransactionState,
    TransactionError,
    TransactionNotActiveError,
    TransactionTimedOutError,
)


class TestTransactionCodec:
    """Tests for TransactionCodec encode/decode methods."""

    def test_encode_create_request_message_type(self):
        """Test that create request has correct message type."""
        msg = TransactionCodec.encode_create_request(
            timeout_millis=120000,
            durability=1,
            transaction_type=TXN_TYPE_TWO_PHASE,
            thread_id=12345,
        )

        frame = msg.next_frame()
        assert frame is not None
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        assert message_type == TXN_CREATE

    def test_encode_create_request_parameters(self):
        """Test that create request encodes all parameters correctly."""
        timeout = 60000
        durability = 2
        txn_type = TXN_TYPE_ONE_PHASE
        thread_id = 98765

        msg = TransactionCodec.encode_create_request(
            timeout_millis=timeout,
            durability=durability,
            transaction_type=txn_type,
            thread_id=thread_id,
        )

        frame = msg.next_frame()
        buf = frame.buf
        offset = REQUEST_HEADER_SIZE

        decoded_timeout = struct.unpack_from("<q", buf, offset)[0]
        offset += LONG_SIZE
        decoded_durability = struct.unpack_from("<i", buf, offset)[0]
        offset += INT_SIZE
        decoded_type = struct.unpack_from("<i", buf, offset)[0]
        offset += INT_SIZE
        decoded_thread_id = struct.unpack_from("<q", buf, offset)[0]

        assert decoded_timeout == timeout
        assert decoded_durability == durability
        assert decoded_type == txn_type
        assert decoded_thread_id == thread_id

    def test_decode_create_response(self):
        """Test decoding a create response with transaction ID."""
        expected_uuid = uuid.uuid4()

        response_buf = bytearray(RESPONSE_HEADER_SIZE + UUID_SIZE)
        FixSizedTypesCodec.encode_uuid(response_buf, RESPONSE_HEADER_SIZE, expected_uuid)

        mock_frame = Mock()
        mock_frame.content = bytes(response_buf)

        mock_msg = Mock()
        mock_msg.next_frame.return_value = mock_frame

        result = TransactionCodec.decode_create_response(mock_msg)
        assert result == expected_uuid

    def test_decode_create_response_invalid_frame(self):
        """Test that decode raises error for invalid frame."""
        mock_msg = Mock()
        mock_msg.next_frame.return_value = None

        with pytest.raises(ValueError, match="Invalid transaction create response"):
            TransactionCodec.decode_create_response(mock_msg)

    def test_encode_commit_request_message_type(self):
        """Test that commit request has correct message type."""
        txn_id = uuid.uuid4()

        msg = TransactionCodec.encode_commit_request(
            transaction_id=txn_id,
            thread_id=12345,
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        assert message_type == TXN_COMMIT

    def test_encode_commit_request_parameters(self):
        """Test that commit request encodes parameters correctly."""
        txn_id = uuid.uuid4()
        thread_id = 54321

        msg = TransactionCodec.encode_commit_request(
            transaction_id=txn_id,
            thread_id=thread_id,
        )

        frame = msg.next_frame()
        buf = frame.buf
        offset = REQUEST_HEADER_SIZE

        decoded_uuid, offset = FixSizedTypesCodec.decode_uuid(buf, offset)
        decoded_thread_id = struct.unpack_from("<q", buf, offset)[0]

        assert decoded_uuid == txn_id
        assert decoded_thread_id == thread_id

    def test_encode_rollback_request_message_type(self):
        """Test that rollback request has correct message type."""
        txn_id = uuid.uuid4()

        msg = TransactionCodec.encode_rollback_request(
            transaction_id=txn_id,
            thread_id=12345,
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        assert message_type == TXN_ROLLBACK

    def test_encode_rollback_request_parameters(self):
        """Test that rollback request encodes parameters correctly."""
        txn_id = uuid.uuid4()
        thread_id = 11111

        msg = TransactionCodec.encode_rollback_request(
            transaction_id=txn_id,
            thread_id=thread_id,
        )

        frame = msg.next_frame()
        buf = frame.buf
        offset = REQUEST_HEADER_SIZE

        decoded_uuid, offset = FixSizedTypesCodec.decode_uuid(buf, offset)
        decoded_thread_id = struct.unpack_from("<q", buf, offset)[0]

        assert decoded_uuid == txn_id
        assert decoded_thread_id == thread_id


class TestTransactionOptions:
    """Tests for TransactionOptions configuration."""

    def test_default_options(self):
        """Test default transaction options values."""
        options = TransactionOptions()

        assert options.timeout_millis == TransactionOptions.DEFAULT_TIMEOUT_MILLIS
        assert options.durability == TransactionOptions.DEFAULT_DURABILITY
        assert options.transaction_type == TransactionType.TWO_PHASE

    def test_custom_options(self):
        """Test custom transaction options."""
        options = TransactionOptions(
            timeout_millis=30000,
            durability=3,
            transaction_type=TransactionType.ONE_PHASE,
        )

        assert options.timeout_millis == 30000
        assert options.durability == 3
        assert options.transaction_type == TransactionType.ONE_PHASE

    def test_invalid_timeout_raises_error(self):
        """Test that non-positive timeout raises error."""
        with pytest.raises(ValueError, match="Timeout must be positive"):
            TransactionOptions(timeout_millis=0)

        with pytest.raises(ValueError, match="Timeout must be positive"):
            TransactionOptions(timeout_millis=-1)

    def test_invalid_durability_raises_error(self):
        """Test that negative durability raises error."""
        with pytest.raises(ValueError, match="Durability cannot be negative"):
            TransactionOptions(durability=-1)


class TestTransaction:
    """Tests for Transaction class."""

    def _create_transaction(
        self,
        invocation_service=None,
        timeout_millis=120000,
    ) -> Transaction:
        """Helper to create a transaction for testing."""
        options = TransactionOptions(timeout_millis=timeout_millis)
        return Transaction(
            transaction_id=uuid.uuid4(),
            thread_id=1,
            options=options,
            invocation_service=invocation_service,
        )

    def test_transaction_initial_state(self):
        """Test transaction starts in ACTIVE state."""
        txn = self._create_transaction()
        assert txn.state == TransactionState.ACTIVE
        assert txn.is_active()

    def test_transaction_properties(self):
        """Test transaction property accessors."""
        txn_id = uuid.uuid4()
        thread_id = 42
        options = TransactionOptions(timeout_millis=60000)

        txn = Transaction(
            transaction_id=txn_id,
            thread_id=thread_id,
            options=options,
            invocation_service=None,
        )

        assert txn.transaction_id == txn_id
        assert txn.thread_id == thread_id
        assert txn.timeout_millis == 60000

    def test_commit_changes_state(self):
        """Test that commit changes transaction state."""
        txn = self._create_transaction()
        txn.commit()
        assert txn.state == TransactionState.COMMITTED
        assert not txn.is_active()

    def test_commit_invokes_service(self):
        """Test that commit calls invocation service."""
        mock_service = Mock()
        txn = self._create_transaction(invocation_service=mock_service)

        txn.commit()

        mock_service.invoke.assert_called_once()

    def test_commit_on_committed_raises_error(self):
        """Test that committing already committed transaction raises error."""
        txn = self._create_transaction()
        txn.commit()

        with pytest.raises(TransactionNotActiveError):
            txn.commit()

    def test_rollback_changes_state(self):
        """Test that rollback changes transaction state."""
        txn = self._create_transaction()
        txn.rollback()
        assert txn.state == TransactionState.ROLLED_BACK
        assert not txn.is_active()

    def test_rollback_invokes_service(self):
        """Test that rollback calls invocation service."""
        mock_service = Mock()
        txn = self._create_transaction(invocation_service=mock_service)

        txn.rollback()

        mock_service.invoke.assert_called_once()

    def test_rollback_on_rolled_back_is_idempotent(self):
        """Test that rolling back already rolled back transaction is safe."""
        txn = self._create_transaction()
        txn.rollback()
        txn.rollback()
        assert txn.state == TransactionState.ROLLED_BACK

    def test_rollback_after_commit_is_no_op(self):
        """Test that rollback after commit does nothing."""
        txn = self._create_transaction()
        txn.commit()
        txn.rollback()
        assert txn.state == TransactionState.COMMITTED

    def test_timeout_detection(self):
        """Test that timed out transactions raise error."""
        txn = self._create_transaction(timeout_millis=1)
        time.sleep(0.01)

        with pytest.raises(TransactionTimedOutError):
            txn.commit()


class TestTransactionContext:
    """Tests for TransactionContext (context manager)."""

    def _create_context(self, invocation_service=None) -> TransactionContext:
        """Helper to create a transaction context for testing."""
        options = TransactionOptions()
        txn = Transaction(
            transaction_id=uuid.uuid4(),
            thread_id=1,
            options=options,
            invocation_service=invocation_service,
        )
        return TransactionContext(txn)

    def test_context_properties(self):
        """Test context property accessors."""
        ctx = self._create_context()
        assert ctx.transaction is not None
        assert ctx.transaction_id is not None

    def test_context_auto_commits_on_success(self):
        """Test that context auto-commits when no exception."""
        ctx = self._create_context()

        with ctx:
            pass

        assert ctx.transaction.state == TransactionState.COMMITTED

    def test_context_auto_rollbacks_on_exception(self):
        """Test that context auto-rollbacks on exception."""
        ctx = self._create_context()

        with pytest.raises(RuntimeError):
            with ctx:
                raise RuntimeError("Test error")

        assert ctx.transaction.state == TransactionState.ROLLED_BACK

    def test_context_explicit_commit(self):
        """Test explicit commit within context."""
        ctx = self._create_context()

        with ctx:
            ctx.commit()

        assert ctx.transaction.state == TransactionState.COMMITTED

    def test_context_explicit_rollback(self):
        """Test explicit rollback within context."""
        ctx = self._create_context()

        with ctx:
            ctx.rollback()

        assert ctx.transaction.state == TransactionState.ROLLED_BACK


class TestTransactionService:
    """Tests for TransactionService."""

    def test_begin_creates_transaction(self):
        """Test that begin creates a new transaction."""
        service = TransactionService()
        txn = service.begin()

        assert txn is not None
        assert txn.is_active()
        assert txn.transaction_id is not None

    def test_begin_with_custom_options(self):
        """Test begin with custom options."""
        service = TransactionService()
        options = TransactionOptions(
            timeout_millis=30000,
            durability=2,
            transaction_type=TransactionType.ONE_PHASE,
        )

        txn = service.begin(options)

        assert txn.timeout_millis == 30000

    def test_begin_generates_unique_thread_ids(self):
        """Test that each transaction gets a unique thread ID."""
        service = TransactionService()

        txn1 = service.begin()
        txn2 = service.begin()

        assert txn1.thread_id != txn2.thread_id

    def test_transaction_context_manager(self):
        """Test transaction() returns a context manager."""
        service = TransactionService()

        with service.transaction() as ctx:
            assert ctx.transaction.is_active()

        assert ctx.transaction.state == TransactionState.COMMITTED

    def test_execute_transaction_success(self):
        """Test execute_transaction with successful callback."""
        service = TransactionService()

        def callback(ctx: TransactionContext) -> str:
            return "success"

        result = service.execute_transaction(callback)

        assert result == "success"

    def test_execute_transaction_with_options(self):
        """Test execute_transaction with custom options."""
        service = TransactionService()
        options = TransactionOptions(timeout_millis=30000)

        def callback(ctx: TransactionContext) -> int:
            return ctx.transaction.timeout_millis

        result = service.execute_transaction(callback, options)

        assert result == 30000

    def test_execute_transaction_rollback_on_error(self):
        """Test execute_transaction rollbacks on callback error."""
        service = TransactionService()
        captured_ctx = None

        def callback(ctx: TransactionContext):
            nonlocal captured_ctx
            captured_ctx = ctx
            raise ValueError("Callback error")

        with pytest.raises(ValueError, match="Callback error"):
            service.execute_transaction(callback)

        assert captured_ctx is not None
        assert captured_ctx.transaction.state == TransactionState.ROLLED_BACK


class TestTransactionTypes:
    """Tests for transaction type and state enums."""

    def test_transaction_type_values(self):
        """Test TransactionType enum values match constants."""
        assert TransactionType.ONE_PHASE == TXN_TYPE_ONE_PHASE
        assert TransactionType.TWO_PHASE == TXN_TYPE_TWO_PHASE

    def test_transaction_state_values(self):
        """Test TransactionState enum values."""
        assert TransactionState.ACTIVE == 0
        assert TransactionState.COMMITTED == 2
        assert TransactionState.ROLLED_BACK == 3
        assert TransactionState.NO_TXN == 4


class TestTransactionIntegration:
    """Integration-style tests for transaction workflow."""

    def test_full_transaction_lifecycle_commit(self):
        """Test complete transaction lifecycle with commit."""
        service = TransactionService()

        txn = service.begin()
        assert txn.state == TransactionState.ACTIVE

        txn.commit()
        assert txn.state == TransactionState.COMMITTED

    def test_full_transaction_lifecycle_rollback(self):
        """Test complete transaction lifecycle with rollback."""
        service = TransactionService()

        txn = service.begin()
        assert txn.state == TransactionState.ACTIVE

        txn.rollback()
        assert txn.state == TransactionState.ROLLED_BACK

    def test_context_manager_workflow(self):
        """Test using transaction service with context manager."""
        service = TransactionService()
        operations_performed = []

        with service.transaction() as ctx:
            operations_performed.append(f"txn:{ctx.transaction_id}")

        assert len(operations_performed) == 1
        assert ctx.transaction.state == TransactionState.COMMITTED

    def test_nested_transactions_are_independent(self):
        """Test that multiple transactions are independent."""
        service = TransactionService()

        txn1 = service.begin()
        txn2 = service.begin()

        assert txn1.transaction_id != txn2.transaction_id

        txn1.commit()
        assert txn1.state == TransactionState.COMMITTED
        assert txn2.state == TransactionState.ACTIVE

        txn2.rollback()
        assert txn2.state == TransactionState.ROLLED_BACK
