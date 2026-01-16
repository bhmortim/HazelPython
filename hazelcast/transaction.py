"""Hazelcast transaction API for ACID operations across distributed data structures."""

import threading
import uuid
from abc import ABC, abstractmethod
from concurrent.futures import Future
from enum import Enum
from typing import Any, Dict, Optional, TYPE_CHECKING

from hazelcast.exceptions import (
    IllegalStateException,
    HazelcastException,
)
from dataclasses import dataclass
from hazelcast.logging import get_logger
from hazelcast.proxy.transactional import (
    TransactionalProxy,
    TransactionalMap,
    TransactionalSet,
    TransactionalList,
    TransactionalQueue,
    TransactionalMultiMap,
)

if TYPE_CHECKING:
    from hazelcast.proxy.base import ProxyContext
    from hazelcast.protocol.client_message import ClientMessage

_logger = get_logger("transaction")


class XAFlags(Enum):
    """XA transaction flags for start, end, and recover operations.

    These flags control the behavior of XA operations and follow the
    X/Open XA specification.
    """

    TMNOFLAGS = 0x00000000
    TMJOIN = 0x00200000
    TMENDRSCAN = 0x00800000
    TMSTARTRSCAN = 0x01000000
    TMSUSPEND = 0x02000000
    TMSUCCESS = 0x04000000
    TMRESUME = 0x08000000
    TMFAIL = 0x20000000
    TMONEPHASE = 0x40000000


class XAReturnCode(Enum):
    """XA return codes from prepare operations."""

    XA_RDONLY = 3
    XA_OK = 0


class XAErrorCode(Enum):
    """XA error codes following X/Open XA specification."""

    XA_RBBASE = 100
    XA_RBROLLBACK = 100
    XA_RBCOMMFAIL = 101
    XA_RBDEADLOCK = 102
    XA_RBINTEGRITY = 103
    XA_RBOTHER = 104
    XA_RBPROTO = 105
    XA_RBTIMEOUT = 106
    XA_RBTRANSIENT = 107
    XA_RBEND = 107
    XAER_ASYNC = -2
    XAER_RMERR = -3
    XAER_NOTA = -4
    XAER_INVAL = -5
    XAER_PROTO = -6
    XAER_RMFAIL = -7
    XAER_DUPID = -8
    XAER_OUTSIDE = -9


@dataclass(frozen=True)
class Xid:
    """XA Transaction Identifier.

    An Xid uniquely identifies a global transaction branch in the XA
    protocol. It consists of three components that together provide
    global uniqueness across distributed systems.

    Attributes:
        format_id: Format identifier. Typically 1 for standard XA.
            A value of -1 indicates a null Xid.
        global_transaction_id: Global transaction identifier, unique
            across all resource managers. Maximum 64 bytes.
        branch_qualifier: Branch qualifier, unique within a global
            transaction. Maximum 64 bytes.

    Note:
        This implementation follows the X/Open XA specification.
        In Python, the global_transaction_id and branch_qualifier
        are represented as bytes objects.

    Example:
        >>> xid = Xid(
        ...     format_id=1,
        ...     global_transaction_id=b"global-txn-123",
        ...     branch_qualifier=b"branch-001",
        ... )
    """

    format_id: int
    global_transaction_id: bytes
    branch_qualifier: bytes

    def __post_init__(self):
        if len(self.global_transaction_id) > 64:
            raise ValueError("global_transaction_id must be at most 64 bytes")
        if len(self.branch_qualifier) > 64:
            raise ValueError("branch_qualifier must be at most 64 bytes")

    @classmethod
    def create(
        cls,
        global_transaction_id: bytes,
        branch_qualifier: bytes,
        format_id: int = 1,
    ) -> "Xid":
        """Factory method to create an Xid.

        Args:
            global_transaction_id: Global transaction identifier.
            branch_qualifier: Branch qualifier.
            format_id: Format identifier. Defaults to 1.

        Returns:
            A new Xid instance.
        """
        return cls(
            format_id=format_id,
            global_transaction_id=global_transaction_id,
            branch_qualifier=branch_qualifier,
        )

    def __str__(self) -> str:
        return (
            f"Xid(format_id={self.format_id}, "
            f"gtrid={self.global_transaction_id.hex()}, "
            f"bqual={self.branch_qualifier.hex()})"
        )


class TransactionType(Enum):
    """Transaction commit type.

    ONE_PHASE: Single-phase commit. Faster but less reliable.
    TWO_PHASE: Two-phase commit with prepare and commit phases.
        Provides stronger consistency guarantees.
    """

    ONE_PHASE = 1
    TWO_PHASE = 2


class TransactionState(Enum):
    """Internal transaction state machine states."""

    NO_TXN = 0
    ACTIVE = 1
    PREPARING = 2
    PREPARED = 3
    COMMITTING = 4
    COMMITTED = 5
    ROLLING_BACK = 6
    ROLLED_BACK = 7


_VALID_STATE_TRANSITIONS = {
    TransactionState.NO_TXN: {TransactionState.ACTIVE},
    TransactionState.ACTIVE: {
        TransactionState.PREPARING,
        TransactionState.COMMITTING,
        TransactionState.ROLLING_BACK,
    },
    TransactionState.PREPARING: {TransactionState.PREPARED, TransactionState.ROLLING_BACK},
    TransactionState.PREPARED: {TransactionState.COMMITTING, TransactionState.ROLLING_BACK},
    TransactionState.COMMITTING: {TransactionState.COMMITTED, TransactionState.ROLLING_BACK},
    TransactionState.COMMITTED: set(),
    TransactionState.ROLLING_BACK: {TransactionState.ROLLED_BACK},
    TransactionState.ROLLED_BACK: set(),
}


class TransactionException(HazelcastException):
    """Raised when a transaction operation fails."""

    def __init__(self, message: str = "Transaction error", cause: Exception = None):
        super().__init__(message, cause)


class TransactionNotActiveException(TransactionException):
    """Raised when an operation requires an active transaction but none exists."""

    def __init__(self, message: str = "Transaction is not active"):
        super().__init__(message)


class TransactionTimedOutException(TransactionException):
    """Raised when a transaction times out."""

    def __init__(self, message: str = "Transaction timed out"):
        super().__init__(message)


class XAException(TransactionException):
    """Exception raised for XA transaction errors.

    XA exceptions include an error code that indicates the specific
    type of failure according to the X/Open XA specification.

    Attributes:
        error_code: The XA error code indicating the failure type.

    Example:
        >>> raise XAException(XAErrorCode.XAER_NOTA, "Unknown XID")
    """

    def __init__(
        self,
        error_code: XAErrorCode,
        message: str = None,
        cause: Exception = None,
    ):
        self._error_code = error_code
        msg = message or f"XA error: {error_code.name}"
        super().__init__(msg, cause)

    @property
    def error_code(self) -> XAErrorCode:
        """The XA error code."""
        return self._error_code


class TransactionOptions:
    """Configuration options for a transaction.

    Attributes:
        timeout: Transaction timeout in seconds. Default is 120 seconds.
        durability: Number of backups for transaction log. Default is 1.
        transaction_type: ONE_PHASE or TWO_PHASE commit. Default is ONE_PHASE.

    Example:
        >>> options = TransactionOptions(
        ...     timeout=60.0,
        ...     durability=2,
        ...     transaction_type=TransactionType.TWO_PHASE,
        ... )
        >>> with client.new_transaction_context(options) as ctx:
        ...     # transactional operations
    """

    DEFAULT_TIMEOUT = 120.0
    DEFAULT_DURABILITY = 1

    def __init__(
        self,
        timeout: float = DEFAULT_TIMEOUT,
        durability: int = DEFAULT_DURABILITY,
        transaction_type: TransactionType = TransactionType.ONE_PHASE,
    ):
        if timeout <= 0:
            raise ValueError("Timeout must be positive")
        if durability < 0:
            raise ValueError("Durability must be non-negative")

        self._timeout = timeout
        self._durability = durability
        self._transaction_type = transaction_type

    @property
    def timeout(self) -> float:
        """Transaction timeout in seconds."""
        return self._timeout

    @property
    def timeout_millis(self) -> int:
        """Transaction timeout in milliseconds."""
        return int(self._timeout * 1000)

    @property
    def durability(self) -> int:
        """Number of backup copies for transaction log."""
        return self._durability

    @property
    def transaction_type(self) -> TransactionType:
        """The transaction commit type."""
        return self._transaction_type

    def __repr__(self) -> str:
        return (
            f"TransactionOptions(timeout={self._timeout}, "
            f"durability={self._durability}, "
            f"transaction_type={self._transaction_type.name})"
        )


class TransactionContext:
    """Context for executing transactional operations.

    A TransactionContext provides methods to begin, commit, and rollback
    transactions, as well as factory methods to obtain transactional
    versions of distributed data structures.

    Transactions provide ACID guarantees across multiple operations on
    distributed data structures within the Hazelcast cluster.

    The context can be used as a context manager for automatic commit
    on success or rollback on exception.

    Attributes:
        txn_id: The unique transaction identifier (available after begin).
        state: The current transaction state.
        options: The transaction configuration options.

    Example:
        Using as context manager (recommended)::

            with client.new_transaction_context() as ctx:
                txn_map = ctx.get_map("my-map")
                txn_map.put("key", "value")
                # Auto-commits on successful exit

        Manual transaction control::

            ctx = client.new_transaction_context()
            try:
                ctx.begin()
                txn_map = ctx.get_map("my-map")
                txn_map.put("key", "value")
                ctx.commit()
            except Exception:
                ctx.rollback()
                raise

        Two-phase commit::

            options = TransactionOptions(
                transaction_type=TransactionType.TWO_PHASE
            )
            with client.new_transaction_context(options) as ctx:
                # Operations use two-phase commit protocol
                pass
    """

    def __init__(
        self,
        context: "ProxyContext",
        options: TransactionOptions = None,
    ):
        self._context = context
        self._options = options or TransactionOptions()
        self._state = TransactionState.NO_TXN
        self._state_lock = threading.Lock()
        self._txn_id: Optional[str] = None
        self._start_time: Optional[float] = None
        self._transactional_proxies: Dict[str, "TransactionalProxy"] = {}

    @property
    def txn_id(self) -> Optional[str]:
        """Get the transaction ID. Available after begin() is called."""
        return self._txn_id

    @property
    def state(self) -> TransactionState:
        """Get the current transaction state."""
        with self._state_lock:
            return self._state

    @property
    def options(self) -> TransactionOptions:
        """Get the transaction options."""
        return self._options

    def _transition_state(self, new_state: TransactionState) -> None:
        """Transition to a new state if valid."""
        with self._state_lock:
            current = self._state
            valid_targets = _VALID_STATE_TRANSITIONS.get(current, set())

            if new_state not in valid_targets:
                raise IllegalStateException(
                    f"Invalid transaction state transition: {current.name} -> {new_state.name}"
                )

            self._state = new_state
            _logger.debug(
                "Transaction %s state: %s -> %s",
                self._txn_id,
                current.name,
                new_state.name,
            )

    def _check_active(self) -> None:
        """Verify the transaction is in ACTIVE state."""
        if self._state != TransactionState.ACTIVE:
            raise TransactionNotActiveException(
                f"Transaction is not active (state={self._state.name})"
            )

    def begin(self) -> "TransactionContext":
        """Begin the transaction.

        Starts a new transaction. After calling begin(), you can perform
        transactional operations using the proxies obtained from this context.

        Returns:
            This TransactionContext for method chaining.

        Raises:
            IllegalStateException: If transaction is not in NO_TXN state.
            TransactionException: If beginning the transaction fails.

        Example:
            >>> ctx = client.new_transaction_context()
            >>> ctx.begin()
            >>> # ... perform transactional operations ...
            >>> ctx.commit()
        """
        _logger.debug("Beginning transaction with options: %s", self._options)

        self._transition_state(TransactionState.ACTIVE)
        self._txn_id = str(uuid.uuid4())

        import time
        self._start_time = time.time()

        _logger.info("Transaction %s started", self._txn_id)
        return self

    def commit(self) -> None:
        """Commit the transaction.

        Commits all operations performed within this transaction. For
        TWO_PHASE transactions, this triggers the prepare phase followed
        by the commit phase.

        Raises:
            TransactionNotActiveException: If no transaction is active.
            TransactionException: If commit fails.
            TransactionTimedOutException: If the transaction has timed out.

        Note:
            After commit, this TransactionContext cannot be reused.

        Example:
            >>> ctx.begin()
            >>> txn_map = ctx.get_map("my-map")
            >>> txn_map.put("key", "value")
            >>> ctx.commit()  # Changes become visible
        """
        self._check_active()
        self._check_timeout()

        _logger.debug("Committing transaction %s", self._txn_id)

        if self._options.transaction_type == TransactionType.TWO_PHASE:
            self._prepare()

        self._transition_state(TransactionState.COMMITTING)

        try:
            self._do_commit()
            self._transition_state(TransactionState.COMMITTED)
            _logger.info("Transaction %s committed", self._txn_id)
        except Exception as e:
            _logger.error("Transaction %s commit failed: %s", self._txn_id, e)
            raise TransactionException(f"Commit failed: {e}", cause=e)

    def _prepare(self) -> None:
        """Prepare phase for two-phase commit."""
        self._transition_state(TransactionState.PREPARING)

        try:
            self._do_prepare()
            self._transition_state(TransactionState.PREPARED)
            _logger.debug("Transaction %s prepared", self._txn_id)
        except Exception as e:
            _logger.error("Transaction %s prepare failed: %s", self._txn_id, e)
            raise TransactionException(f"Prepare failed: {e}", cause=e)

    def _do_prepare(self) -> None:
        """Execute the prepare phase. Override for actual implementation."""
        pass

    def _do_commit(self) -> None:
        """Execute the commit. Override for actual implementation."""
        pass

    def rollback(self) -> None:
        """Rollback the transaction.

        Aborts all operations performed within this transaction. All
        changes made during the transaction are discarded.

        Raises:
            TransactionNotActiveException: If no transaction is active.
            TransactionException: If rollback fails.

        Note:
            After rollback, this TransactionContext cannot be reused.

        Example:
            >>> ctx.begin()
            >>> try:
            ...     txn_map = ctx.get_map("my-map")
            ...     txn_map.put("key", "value")
            ...     raise Exception("Something went wrong")
            ... except Exception:
            ...     ctx.rollback()  # Discard changes
        """
        current_state = self.state
        if current_state in (
            TransactionState.NO_TXN,
            TransactionState.COMMITTED,
            TransactionState.ROLLED_BACK,
        ):
            _logger.debug(
                "Ignoring rollback for transaction %s in state %s",
                self._txn_id,
                current_state.name,
            )
            return

        _logger.debug("Rolling back transaction %s", self._txn_id)
        self._transition_state(TransactionState.ROLLING_BACK)

        try:
            self._do_rollback()
            self._transition_state(TransactionState.ROLLED_BACK)
            _logger.info("Transaction %s rolled back", self._txn_id)
        except Exception as e:
            _logger.error("Transaction %s rollback failed: %s", self._txn_id, e)
            self._transition_state(TransactionState.ROLLED_BACK)
            raise TransactionException(f"Rollback failed: {e}", cause=e)

    def _do_rollback(self) -> None:
        """Execute the rollback. Override for actual implementation."""
        pass

    def _check_timeout(self) -> None:
        """Check if the transaction has timed out."""
        if self._start_time is None:
            return

        import time
        elapsed = time.time() - self._start_time
        if elapsed > self._options.timeout:
            raise TransactionTimedOutException(
                f"Transaction timed out after {elapsed:.2f}s "
                f"(timeout={self._options.timeout}s)"
            )

    def get_map(self, name: str) -> "TransactionalMap":
        """Get a transactional map proxy.

        Args:
            name: Name of the distributed map.

        Returns:
            A TransactionalMap proxy for transactional map operations.

        Raises:
            TransactionNotActiveException: If transaction is not active.

        Example:
            >>> with client.new_transaction_context() as ctx:
            ...     txn_map = ctx.get_map("my-map")
            ...     txn_map.put("key", "value")
        """
        self._check_active()
        key = f"map:{name}"
        if key not in self._transactional_proxies:
            self._transactional_proxies[key] = TransactionalMap(name, self)
        return self._transactional_proxies[key]

    def get_set(self, name: str) -> "TransactionalSet":
        """Get a transactional set proxy.

        Args:
            name: Name of the distributed set.

        Returns:
            A TransactionalSet proxy for transactional set operations.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_active()
        key = f"set:{name}"
        if key not in self._transactional_proxies:
            self._transactional_proxies[key] = TransactionalSet(name, self)
        return self._transactional_proxies[key]

    def get_list(self, name: str) -> "TransactionalList":
        """Get a transactional list proxy.

        Args:
            name: Name of the distributed list.

        Returns:
            A TransactionalList proxy for transactional list operations.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_active()
        key = f"list:{name}"
        if key not in self._transactional_proxies:
            self._transactional_proxies[key] = TransactionalList(name, self)
        return self._transactional_proxies[key]

    def get_queue(self, name: str) -> "TransactionalQueue":
        """Get a transactional queue proxy.

        Args:
            name: Name of the distributed queue.

        Returns:
            A TransactionalQueue proxy for transactional queue operations.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_active()
        key = f"queue:{name}"
        if key not in self._transactional_proxies:
            self._transactional_proxies[key] = TransactionalQueue(name, self)
        return self._transactional_proxies[key]

    def get_multi_map(self, name: str) -> "TransactionalMultiMap":
        """Get a transactional multi-map proxy.

        Args:
            name: Name of the distributed multi-map.

        Returns:
            A TransactionalMultiMap proxy for transactional multi-map operations.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_active()
        key = f"multimap:{name}"
        if key not in self._transactional_proxies:
            self._transactional_proxies[key] = TransactionalMultiMap(name, self)
        return self._transactional_proxies[key]

    def __enter__(self) -> "TransactionContext":
        """Enter context manager - begins the transaction."""
        return self.begin()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager - commits or rolls back the transaction."""
        if exc_type is None:
            try:
                self.commit()
            except Exception:
                self.rollback()
                raise
        else:
            self.rollback()

    def __repr__(self) -> str:
        return (
            f"TransactionContext(txn_id={self._txn_id!r}, "
            f"state={self._state.name}, "
            f"type={self._options.transaction_type.name})"
        )


class XAResource(ABC):
    """Abstract base class for XA-compliant transaction resources.

    XAResource defines the contract between a Resource Manager and a
    Transaction Manager in a distributed transaction processing (DTP)
    environment. This interface follows the X/Open XA specification.

    Limitations:
        This is a compatibility layer for Python. Full XA coordination
        requires an external transaction manager. Python does not have
        native JTA/XA support like Java, so some advanced features
        (such as automatic enlistment with application servers) are
        not available.

    The typical XA transaction lifecycle is:
        1. start() - Associate the resource with a transaction branch
        2. [perform work]
        3. end() - Disassociate the resource from the transaction
        4. prepare() - Vote on whether to commit (two-phase commit)
        5. commit() or rollback() - Complete the transaction

    Example:
        >>> class MyXAResource(XAResource):
        ...     def start(self, xid, flags):
        ...         # Begin transaction branch work
        ...         pass
        ...     # ... implement other methods
    """

    @abstractmethod
    def start(self, xid: Xid, flags: XAFlags = XAFlags.TMNOFLAGS) -> None:
        """Start work on behalf of a transaction branch.

        Args:
            xid: The XA transaction identifier.
            flags: XA flags (TMNOFLAGS, TMJOIN, or TMRESUME).

        Raises:
            XAException: If an error occurs starting the transaction.
        """
        pass

    @abstractmethod
    def end(self, xid: Xid, flags: XAFlags = XAFlags.TMSUCCESS) -> None:
        """End work on behalf of a transaction branch.

        Args:
            xid: The XA transaction identifier.
            flags: XA flags (TMSUCCESS, TMFAIL, or TMSUSPEND).

        Raises:
            XAException: If an error occurs ending the transaction.
        """
        pass

    @abstractmethod
    def prepare(self, xid: Xid) -> XAReturnCode:
        """Prepare to commit the transaction branch.

        This is the first phase of two-phase commit. The resource
        manager votes on whether the transaction can be committed.

        Args:
            xid: The XA transaction identifier.

        Returns:
            XA_OK if the transaction can be committed, or
            XA_RDONLY if the branch was read-only.

        Raises:
            XAException: If the transaction should be rolled back.
        """
        pass

    @abstractmethod
    def commit(self, xid: Xid, one_phase: bool = False) -> None:
        """Commit the transaction branch.

        Args:
            xid: The XA transaction identifier.
            one_phase: If True, use one-phase commit optimization.

        Raises:
            XAException: If commit fails.
        """
        pass

    @abstractmethod
    def rollback(self, xid: Xid) -> None:
        """Rollback the transaction branch.

        Args:
            xid: The XA transaction identifier.

        Raises:
            XAException: If rollback fails.
        """
        pass

    @abstractmethod
    def recover(self, flags: XAFlags = XAFlags.TMSTARTRSCAN) -> list:
        """Obtain a list of prepared transaction branches.

        Used for crash recovery to identify in-doubt transactions.

        Args:
            flags: Recovery scan flags (TMSTARTRSCAN, TMENDRSCAN,
                or TMNOFLAGS for continuation).

        Returns:
            List of Xid objects for prepared transactions.

        Raises:
            XAException: If recovery fails.
        """
        pass

    @abstractmethod
    def forget(self, xid: Xid) -> None:
        """Forget about a heuristically completed transaction branch.

        Called to inform the resource manager that it can discard
        knowledge about a heuristically completed branch.

        Args:
            xid: The XA transaction identifier.

        Raises:
            XAException: If the operation fails.
        """
        pass

    @abstractmethod
    def is_same_rm(self, xa_resource: "XAResource") -> bool:
        """Determine if two XAResource instances represent the same RM.

        Args:
            xa_resource: Another XAResource to compare.

        Returns:
            True if both resources represent the same resource manager.
        """
        pass

    @abstractmethod
    def get_transaction_timeout(self) -> int:
        """Get the current transaction timeout in seconds.

        Returns:
            The transaction timeout in seconds.
        """
        pass

    @abstractmethod
    def set_transaction_timeout(self, seconds: int) -> bool:
        """Set the transaction timeout.

        Args:
            seconds: The timeout in seconds.

        Returns:
            True if the timeout was set successfully.
        """
        pass


class XATransactionContext(TransactionContext):
    """XA-compliant transaction context for distributed transactions.

    XATransactionContext extends TransactionContext to support the XA
    protocol for distributed transaction processing. It implements the
    XAResource interface and can participate in global transactions
    coordinated by an external transaction manager.

    Limitations:
        - Python lacks native JTA/XA infrastructure found in Java EE.
        - Full XA coordination requires an external transaction manager.
        - Automatic resource enlistment is not supported.
        - Recovery coordination must be handled externally.
        - This implementation provides the API surface for compatibility
          with transaction managers that support Python bindings.

    The XA protocol provides stronger consistency guarantees than local
    transactions by using two-phase commit across multiple resource
    managers.

    Attributes:
        xid: The XA transaction identifier (set via start()).
        xa_state: Current XA state (started, ended, prepared, etc.).

    Example:
        >>> xa_ctx = client.new_xa_transaction_context()
        >>> xid = Xid.create(b"global-123", b"branch-1")
        >>> xa_ctx.start(xid)
        >>> try:
        ...     txn_map = xa_ctx.get_map("my-map")
        ...     txn_map.put("key", "value")
        ...     xa_ctx.end(xid)
        ...     xa_ctx.prepare(xid)
        ...     xa_ctx.commit(xid)
        ... except Exception:
        ...     xa_ctx.rollback(xid)
    """

    class XAState(Enum):
        """Internal XA state for resource lifecycle."""

        INITIAL = 0
        STARTED = 1
        ENDED = 2
        PREPARED = 3
        COMMITTED = 4
        ROLLED_BACK = 5

    def __init__(
        self,
        context: "ProxyContext",
        options: TransactionOptions = None,
    ):
        super().__init__(context, options)
        self._xid: Optional[Xid] = None
        self._xa_state = self.XAState.INITIAL
        self._xa_timeout = int(self._options.timeout)
        self._prepared_xids: Dict[bytes, Xid] = {}

    @property
    def xid(self) -> Optional[Xid]:
        """Get the current XA transaction identifier."""
        return self._xid

    @property
    def xa_state(self) -> "XATransactionContext.XAState":
        """Get the current XA state."""
        return self._xa_state

    def start(self, xid: Xid, flags: XAFlags = XAFlags.TMNOFLAGS) -> None:
        """Start work on behalf of a transaction branch.

        Associates this context with the specified XA transaction.
        Must be called before performing any transactional operations.

        Args:
            xid: The XA transaction identifier.
            flags: Start flags - TMNOFLAGS for new branch, TMJOIN to
                join existing branch, TMRESUME to resume suspended.

        Raises:
            XAException: If the transaction cannot be started.
        """
        _logger.debug("XA start: xid=%s, flags=%s", xid, flags.name)

        if flags == XAFlags.TMRESUME:
            if self._xa_state != self.XAState.ENDED:
                raise XAException(
                    XAErrorCode.XAER_PROTO,
                    "Cannot resume: transaction not suspended",
                )
        elif flags == XAFlags.TMJOIN:
            if self._xa_state != self.XAState.INITIAL:
                raise XAException(
                    XAErrorCode.XAER_PROTO,
                    "Cannot join: invalid state",
                )
        else:
            if self._xa_state != self.XAState.INITIAL:
                raise XAException(
                    XAErrorCode.XAER_PROTO,
                    f"Cannot start: invalid state {self._xa_state.name}",
                )

        self._xid = xid
        self._xa_state = self.XAState.STARTED

        self._transition_state(TransactionState.ACTIVE)
        self._txn_id = xid.global_transaction_id.hex()

        import time
        self._start_time = time.time()

        _logger.info("XA transaction started: %s", xid)

    def end(self, xid: Xid, flags: XAFlags = XAFlags.TMSUCCESS) -> None:
        """End work on behalf of a transaction branch.

        Disassociates this context from the XA transaction. After end(),
        no more transactional operations can be performed until start()
        is called again (with TMRESUME).

        Args:
            xid: The XA transaction identifier.
            flags: End flags - TMSUCCESS for normal end, TMFAIL to
                mark for rollback, TMSUSPEND to suspend.

        Raises:
            XAException: If the transaction cannot be ended.
        """
        _logger.debug("XA end: xid=%s, flags=%s", xid, flags.name)

        self._verify_xid(xid)

        if self._xa_state != self.XAState.STARTED:
            raise XAException(
                XAErrorCode.XAER_PROTO,
                f"Cannot end: invalid state {self._xa_state.name}",
            )

        if flags == XAFlags.TMFAIL:
            _logger.debug("XA end with TMFAIL - transaction marked for rollback")

        self._xa_state = self.XAState.ENDED
        _logger.info("XA transaction ended: %s", xid)

    def prepare(self, xid: Xid) -> XAReturnCode:
        """Prepare to commit the transaction branch.

        This is the first phase of two-phase commit. The resource
        manager prepares all changes and votes on whether the
        transaction can be committed.

        Args:
            xid: The XA transaction identifier.

        Returns:
            XA_OK if ready to commit, XA_RDONLY if read-only.

        Raises:
            XAException: If prepare fails (triggers rollback).
        """
        _logger.debug("XA prepare: xid=%s", xid)

        self._verify_xid(xid)
        self._check_timeout()

        if self._xa_state != self.XAState.ENDED:
            raise XAException(
                XAErrorCode.XAER_PROTO,
                f"Cannot prepare: invalid state {self._xa_state.name}",
            )

        self._transition_state(TransactionState.PREPARING)

        try:
            self._do_prepare()
            self._transition_state(TransactionState.PREPARED)
            self._xa_state = self.XAState.PREPARED
            self._prepared_xids[xid.global_transaction_id] = xid
            _logger.info("XA transaction prepared: %s", xid)
            return XAReturnCode.XA_OK
        except Exception as e:
            _logger.error("XA prepare failed: %s", e)
            raise XAException(XAErrorCode.XAER_RMERR, str(e), cause=e)

    def commit(self, xid: Xid, one_phase: bool = False) -> None:
        """Commit the transaction branch.

        Commits all changes made during the transaction. For two-phase
        commit, prepare() must be called first unless one_phase is True.

        Args:
            xid: The XA transaction identifier.
            one_phase: If True, use one-phase commit optimization
                (skips prepare phase).

        Raises:
            XAException: If commit fails.
        """
        _logger.debug("XA commit: xid=%s, one_phase=%s", xid, one_phase)

        self._verify_xid(xid)

        if one_phase:
            if self._xa_state != self.XAState.ENDED:
                raise XAException(
                    XAErrorCode.XAER_PROTO,
                    "One-phase commit requires ENDED state",
                )
        else:
            if self._xa_state != self.XAState.PREPARED:
                raise XAException(
                    XAErrorCode.XAER_PROTO,
                    "Two-phase commit requires PREPARED state",
                )

        self._transition_state(TransactionState.COMMITTING)

        try:
            self._do_commit()
            self._transition_state(TransactionState.COMMITTED)
            self._xa_state = self.XAState.COMMITTED
            self._prepared_xids.pop(xid.global_transaction_id, None)
            _logger.info("XA transaction committed: %s", xid)
        except Exception as e:
            _logger.error("XA commit failed: %s", e)
            raise XAException(XAErrorCode.XAER_RMERR, str(e), cause=e)

    def rollback(self, xid: Xid = None) -> None:
        """Rollback the transaction branch.

        Discards all changes made during the transaction.

        Args:
            xid: The XA transaction identifier. If None, uses the
                current transaction's xid.

        Raises:
            XAException: If rollback fails.
        """
        if xid is None:
            xid = self._xid

        if xid is not None:
            _logger.debug("XA rollback: xid=%s", xid)
            self._verify_xid(xid)

        if self._xa_state in (
            self.XAState.INITIAL,
            self.XAState.COMMITTED,
            self.XAState.ROLLED_BACK,
        ):
            _logger.debug("Ignoring XA rollback in state %s", self._xa_state.name)
            return

        self._transition_state(TransactionState.ROLLING_BACK)

        try:
            self._do_rollback()
            self._transition_state(TransactionState.ROLLED_BACK)
            self._xa_state = self.XAState.ROLLED_BACK
            if xid:
                self._prepared_xids.pop(xid.global_transaction_id, None)
            _logger.info("XA transaction rolled back: %s", xid)
        except Exception as e:
            self._xa_state = self.XAState.ROLLED_BACK
            _logger.error("XA rollback failed: %s", e)
            raise XAException(XAErrorCode.XAER_RMERR, str(e), cause=e)

    def recover(self, flags: XAFlags = XAFlags.TMSTARTRSCAN) -> list:
        """Obtain a list of prepared transaction branches.

        Used for crash recovery to identify in-doubt transactions
        that need to be resolved.

        Args:
            flags: Recovery flags (TMSTARTRSCAN, TMENDRSCAN, TMNOFLAGS).

        Returns:
            List of Xid objects for prepared transactions.

        Note:
            This implementation returns locally tracked prepared XIDs.
            Full recovery requires coordination with the Hazelcast cluster.
        """
        _logger.debug("XA recover: flags=%s", flags.name)
        return list(self._prepared_xids.values())

    def forget(self, xid: Xid) -> None:
        """Forget about a heuristically completed transaction branch.

        Called when a transaction was completed heuristically (outside
        normal two-phase commit) and the resource manager can discard
        its knowledge of the transaction.

        Args:
            xid: The XA transaction identifier.

        Raises:
            XAException: If the XID is unknown.
        """
        _logger.debug("XA forget: xid=%s", xid)

        if xid.global_transaction_id in self._prepared_xids:
            del self._prepared_xids[xid.global_transaction_id]
        else:
            raise XAException(XAErrorCode.XAER_NOTA, f"Unknown XID: {xid}")

    def is_same_rm(self, xa_resource: "XAResource") -> bool:
        """Determine if another XAResource represents the same RM.

        Args:
            xa_resource: Another XAResource to compare.

        Returns:
            True if both resources represent the same Hazelcast cluster.
        """
        if not isinstance(xa_resource, XATransactionContext):
            return False
        return self._context is xa_resource._context

    def get_transaction_timeout(self) -> int:
        """Get the current transaction timeout in seconds.

        Returns:
            The transaction timeout in seconds.
        """
        return self._xa_timeout

    def set_transaction_timeout(self, seconds: int) -> bool:
        """Set the transaction timeout.

        Args:
            seconds: The timeout in seconds.

        Returns:
            True if the timeout was set successfully.
        """
        if seconds < 0:
            return False
        self._xa_timeout = seconds
        return True

    def _verify_xid(self, xid: Xid) -> None:
        """Verify the provided XID matches the current transaction."""
        if self._xid is None:
            raise XAException(XAErrorCode.XAER_NOTA, "No transaction started")
        if xid != self._xid:
            raise XAException(
                XAErrorCode.XAER_NOTA,
                f"XID mismatch: expected {self._xid}, got {xid}",
            )

    def __enter__(self) -> "XATransactionContext":
        """Context manager entry - does not auto-start for XA."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - rolls back if not committed."""
        if self._xa_state in (self.XAState.STARTED, self.XAState.ENDED):
            self.rollback()

    def __repr__(self) -> str:
        return (
            f"XATransactionContext(xid={self._xid}, "
            f"xa_state={self._xa_state.name}, "
            f"state={self._state.name})"
        )
