"""Tests for Hazelcast transaction API."""

import pytest
import threading
import time
from unittest.mock import MagicMock, patch

from hazelcast.transaction import (
    TransactionType,
    TransactionState,
    TransactionOptions,
    TransactionContext,
    TransactionException,
    TransactionNotActiveException,
    TransactionTimedOutException,
    XAFlags,
    XAReturnCode,
    XAErrorCode,
    Xid,
    XAException,
    XAResource,
    XATransactionContext,
)
from hazelcast.exceptions import IllegalStateException


class TestTransactionOptions:
    """Tests for TransactionOptions configuration."""

    def test_default_options(self):
        options = TransactionOptions()
        assert options.timeout == 120.0
        assert options.timeout_millis == 120000
        assert options.durability == 1
        assert options.transaction_type == TransactionType.ONE_PHASE

    def test_custom_options(self):
        options = TransactionOptions(
            timeout=60.0,
            durability=2,
            transaction_type=TransactionType.TWO_PHASE,
        )
        assert options.timeout == 60.0
        assert options.timeout_millis == 60000
        assert options.durability == 2
        assert options.transaction_type == TransactionType.TWO_PHASE

    def test_invalid_timeout(self):
        with pytest.raises(ValueError, match="Timeout must be positive"):
            TransactionOptions(timeout=0)
        with pytest.raises(ValueError, match="Timeout must be positive"):
            TransactionOptions(timeout=-1)

    def test_invalid_durability(self):
        with pytest.raises(ValueError, match="Durability must be non-negative"):
            TransactionOptions(durability=-1)

    def test_repr(self):
        options = TransactionOptions()
        repr_str = repr(options)
        assert "timeout=120.0" in repr_str
        assert "durability=1" in repr_str
        assert "ONE_PHASE" in repr_str


class TestTransactionContext:
    """Tests for TransactionContext lifecycle."""

    @pytest.fixture
    def mock_context(self):
        return MagicMock()

    @pytest.fixture
    def txn_context(self, mock_context):
        return TransactionContext(mock_context)

    def test_initial_state(self, txn_context):
        assert txn_context.state == TransactionState.NO_TXN
        assert txn_context.txn_id is None

    def test_begin_transaction(self, txn_context):
        result = txn_context.begin()
        assert result is txn_context
        assert txn_context.state == TransactionState.ACTIVE
        assert txn_context.txn_id is not None

    def test_commit_one_phase(self, txn_context):
        txn_context.begin()
        txn_context.commit()
        assert txn_context.state == TransactionState.COMMITTED

    def test_commit_two_phase(self, mock_context):
        options = TransactionOptions(transaction_type=TransactionType.TWO_PHASE)
        txn_context = TransactionContext(mock_context, options)
        txn_context.begin()
        txn_context.commit()
        assert txn_context.state == TransactionState.COMMITTED

    def test_rollback(self, txn_context):
        txn_context.begin()
        txn_context.rollback()
        assert txn_context.state == TransactionState.ROLLED_BACK

    def test_rollback_not_started(self, txn_context):
        txn_context.rollback()
        assert txn_context.state == TransactionState.NO_TXN

    def test_commit_without_begin(self, txn_context):
        with pytest.raises(TransactionNotActiveException):
            txn_context.commit()

    def test_invalid_state_transition(self, txn_context):
        txn_context.begin()
        txn_context.commit()
        with pytest.raises(IllegalStateException):
            txn_context.begin()

    def test_context_manager_commit(self, mock_context):
        with TransactionContext(mock_context) as ctx:
            assert ctx.state == TransactionState.ACTIVE
        assert ctx.state == TransactionState.COMMITTED

    def test_context_manager_rollback_on_exception(self, mock_context):
        with pytest.raises(ValueError):
            with TransactionContext(mock_context) as ctx:
                raise ValueError("test error")
        assert ctx.state == TransactionState.ROLLED_BACK

    def test_timeout_check(self, mock_context):
        options = TransactionOptions(timeout=0.01)
        txn_context = TransactionContext(mock_context, options)
        txn_context.begin()
        time.sleep(0.02)
        with pytest.raises(TransactionTimedOutException):
            txn_context.commit()

    def test_get_map_requires_active(self, txn_context):
        with pytest.raises(TransactionNotActiveException):
            txn_context.get_map("test")

    def test_get_proxies(self, txn_context):
        txn_context.begin()
        map_proxy = txn_context.get_map("test-map")
        set_proxy = txn_context.get_set("test-set")
        list_proxy = txn_context.get_list("test-list")
        queue_proxy = txn_context.get_queue("test-queue")
        multimap_proxy = txn_context.get_multi_map("test-multimap")

        assert map_proxy is txn_context.get_map("test-map")
        assert set_proxy is txn_context.get_set("test-set")
        assert list_proxy is txn_context.get_list("test-list")
        assert queue_proxy is txn_context.get_queue("test-queue")
        assert multimap_proxy is txn_context.get_multi_map("test-multimap")


class TestXid:
    """Tests for XA Transaction Identifier."""

    def test_create_xid(self):
        xid = Xid(
            format_id=1,
            global_transaction_id=b"global-123",
            branch_qualifier=b"branch-001",
        )
        assert xid.format_id == 1
        assert xid.global_transaction_id == b"global-123"
        assert xid.branch_qualifier == b"branch-001"

    def test_create_factory(self):
        xid = Xid.create(b"global-123", b"branch-001")
        assert xid.format_id == 1
        assert xid.global_transaction_id == b"global-123"

    def test_xid_immutable(self):
        xid = Xid(1, b"global", b"branch")
        with pytest.raises(AttributeError):
            xid.format_id = 2

    def test_xid_equality(self):
        xid1 = Xid(1, b"global", b"branch")
        xid2 = Xid(1, b"global", b"branch")
        xid3 = Xid(1, b"global", b"other")
        assert xid1 == xid2
        assert xid1 != xid3

    def test_xid_hash(self):
        xid1 = Xid(1, b"global", b"branch")
        xid2 = Xid(1, b"global", b"branch")
        assert hash(xid1) == hash(xid2)
        xid_set = {xid1, xid2}
        assert len(xid_set) == 1

    def test_global_transaction_id_too_long(self):
        with pytest.raises(ValueError, match="global_transaction_id must be at most 64 bytes"):
            Xid(1, b"x" * 65, b"branch")

    def test_branch_qualifier_too_long(self):
        with pytest.raises(ValueError, match="branch_qualifier must be at most 64 bytes"):
            Xid(1, b"global", b"x" * 65)

    def test_str_representation(self):
        xid = Xid(1, b"\x01\x02", b"\x03\x04")
        str_repr = str(xid)
        assert "format_id=1" in str_repr
        assert "gtrid=0102" in str_repr
        assert "bqual=0304" in str_repr


class TestXAException:
    """Tests for XA Exception handling."""

    def test_xa_exception_with_code(self):
        exc = XAException(XAErrorCode.XAER_NOTA, "Unknown XID")
        assert exc.error_code == XAErrorCode.XAER_NOTA
        assert "Unknown XID" in str(exc)

    def test_xa_exception_default_message(self):
        exc = XAException(XAErrorCode.XAER_RMERR)
        assert exc.error_code == XAErrorCode.XAER_RMERR
        assert "XAER_RMERR" in str(exc)


class TestXATransactionContext:
    """Tests for XA Transaction Context."""

    @pytest.fixture
    def mock_context(self):
        return MagicMock()

    @pytest.fixture
    def xa_context(self, mock_context):
        return XATransactionContext(mock_context)

    @pytest.fixture
    def xid(self):
        return Xid.create(b"global-txn-123", b"branch-001")

    def test_initial_state(self, xa_context):
        assert xa_context.xid is None
        assert xa_context.xa_state == XATransactionContext.XAState.INITIAL

    def test_start(self, xa_context, xid):
        xa_context.start(xid)
        assert xa_context.xid == xid
        assert xa_context.xa_state == XATransactionContext.XAState.STARTED
        assert xa_context.state == TransactionState.ACTIVE

    def test_start_with_join(self, xa_context, xid):
        xa_context.start(xid, XAFlags.TMJOIN)
        assert xa_context.xa_state == XATransactionContext.XAState.STARTED

    def test_start_already_started(self, xa_context, xid):
        xa_context.start(xid)
        with pytest.raises(XAException) as exc_info:
            xa_context.start(xid)
        assert exc_info.value.error_code == XAErrorCode.XAER_PROTO

    def test_end(self, xa_context, xid):
        xa_context.start(xid)
        xa_context.end(xid)
        assert xa_context.xa_state == XATransactionContext.XAState.ENDED

    def test_end_with_fail(self, xa_context, xid):
        xa_context.start(xid)
        xa_context.end(xid, XAFlags.TMFAIL)
        assert xa_context.xa_state == XATransactionContext.XAState.ENDED

    def test_end_not_started(self, xa_context, xid):
        with pytest.raises(XAException) as exc_info:
            xa_context.end(xid)
        assert exc_info.value.error_code == XAErrorCode.XAER_NOTA

    def test_end_wrong_xid(self, xa_context, xid):
        xa_context.start(xid)
        other_xid = Xid.create(b"other-global", b"branch")
        with pytest.raises(XAException) as exc_info:
            xa_context.end(other_xid)
        assert exc_info.value.error_code == XAErrorCode.XAER_NOTA

    def test_prepare(self, xa_context, xid):
        xa_context.start(xid)
        xa_context.end(xid)
        result = xa_context.prepare(xid)
        assert result == XAReturnCode.XA_OK
        assert xa_context.xa_state == XATransactionContext.XAState.PREPARED

    def test_prepare_not_ended(self, xa_context, xid):
        xa_context.start(xid)
        with pytest.raises(XAException) as exc_info:
            xa_context.prepare(xid)
        assert exc_info.value.error_code == XAErrorCode.XAER_PROTO

    def test_commit_two_phase(self, xa_context, xid):
        xa_context.start(xid)
        xa_context.end(xid)
        xa_context.prepare(xid)
        xa_context.commit(xid)
        assert xa_context.xa_state == XATransactionContext.XAState.COMMITTED
        assert xa_context.state == TransactionState.COMMITTED

    def test_commit_one_phase(self, xa_context, xid):
        xa_context.start(xid)
        xa_context.end(xid)
        xa_context.commit(xid, one_phase=True)
        assert xa_context.xa_state == XATransactionContext.XAState.COMMITTED

    def test_commit_one_phase_wrong_state(self, xa_context, xid):
        xa_context.start(xid)
        with pytest.raises(XAException) as exc_info:
            xa_context.commit(xid, one_phase=True)
        assert exc_info.value.error_code == XAErrorCode.XAER_PROTO

    def test_commit_two_phase_not_prepared(self, xa_context, xid):
        xa_context.start(xid)
        xa_context.end(xid)
        with pytest.raises(XAException) as exc_info:
            xa_context.commit(xid)
        assert exc_info.value.error_code == XAErrorCode.XAER_PROTO

    def test_rollback(self, xa_context, xid):
        xa_context.start(xid)
        xa_context.rollback(xid)
        assert xa_context.xa_state == XATransactionContext.XAState.ROLLED_BACK

    def test_rollback_after_prepare(self, xa_context, xid):
        xa_context.start(xid)
        xa_context.end(xid)
        xa_context.prepare(xid)
        xa_context.rollback(xid)
        assert xa_context.xa_state == XATransactionContext.XAState.ROLLED_BACK

    def test_rollback_not_started(self, xa_context, xid):
        xa_context.rollback(xid)
        assert xa_context.xa_state == XATransactionContext.XAState.INITIAL

    def test_recover(self, xa_context, xid):
        xa_context.start(xid)
        xa_context.end(xid)
        xa_context.prepare(xid)

        recovered = xa_context.recover()
        assert len(recovered) == 1
        assert recovered[0] == xid

    def test_recover_empty(self, xa_context):
        recovered = xa_context.recover()
        assert len(recovered) == 0

    def test_forget(self, xa_context, xid):
        xa_context.start(xid)
        xa_context.end(xid)
        xa_context.prepare(xid)
        xa_context.forget(xid)
        assert xid not in xa_context.recover()

    def test_forget_unknown_xid(self, xa_context, xid):
        with pytest.raises(XAException) as exc_info:
            xa_context.forget(xid)
        assert exc_info.value.error_code == XAErrorCode.XAER_NOTA

    def test_is_same_rm(self, mock_context):
        ctx1 = XATransactionContext(mock_context)
        ctx2 = XATransactionContext(mock_context)
        other_mock = MagicMock()
        ctx3 = XATransactionContext(other_mock)

        assert ctx1.is_same_rm(ctx2) is True
        assert ctx1.is_same_rm(ctx3) is False
        assert ctx1.is_same_rm(MagicMock()) is False

    def test_transaction_timeout(self, xa_context):
        assert xa_context.get_transaction_timeout() == 120
        assert xa_context.set_transaction_timeout(60) is True
        assert xa_context.get_transaction_timeout() == 60
        assert xa_context.set_transaction_timeout(-1) is False

    def test_resume(self, xa_context, xid):
        xa_context.start(xid)
        xa_context.end(xid, XAFlags.TMSUSPEND)
        xa_context.start(xid, XAFlags.TMRESUME)
        assert xa_context.xa_state == XATransactionContext.XAState.STARTED

    def test_resume_wrong_state(self, xa_context, xid):
        with pytest.raises(XAException) as exc_info:
            xa_context.start(xid, XAFlags.TMRESUME)
        assert exc_info.value.error_code == XAErrorCode.XAER_PROTO

    def test_context_manager(self, mock_context, xid):
        xa_ctx = XATransactionContext(mock_context)
        with xa_ctx:
            xa_ctx.start(xid)
        assert xa_ctx.xa_state == XATransactionContext.XAState.ROLLED_BACK

    def test_context_manager_with_commit(self, mock_context, xid):
        xa_ctx = XATransactionContext(mock_context)
        with xa_ctx:
            xa_ctx.start(xid)
            xa_ctx.end(xid)
            xa_ctx.prepare(xid)
            xa_ctx.commit(xid)
        assert xa_ctx.xa_state == XATransactionContext.XAState.COMMITTED

    def test_repr(self, xa_context, xid):
        repr_str = repr(xa_context)
        assert "XATransactionContext" in repr_str
        assert "INITIAL" in repr_str


class TestXAFlags:
    """Tests for XA Flags enumeration."""

    def test_flag_values(self):
        assert XAFlags.TMNOFLAGS.value == 0
        assert XAFlags.TMSUCCESS.value == 0x04000000
        assert XAFlags.TMFAIL.value == 0x20000000
        assert XAFlags.TMONEPHASE.value == 0x40000000


class TestXAReturnCode:
    """Tests for XA Return Codes."""

    def test_return_codes(self):
        assert XAReturnCode.XA_OK.value == 0
        assert XAReturnCode.XA_RDONLY.value == 3


class TestXAErrorCode:
    """Tests for XA Error Codes."""

    def test_error_codes(self):
        assert XAErrorCode.XAER_NOTA.value == -4
        assert XAErrorCode.XAER_PROTO.value == -6
        assert XAErrorCode.XAER_RMERR.value == -3
        assert XAErrorCode.XA_RBROLLBACK.value == 100
