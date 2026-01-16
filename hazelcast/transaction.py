"""Transaction API for Hazelcast client.

This module provides transactional support for Hazelcast operations,
allowing multiple operations to be grouped into atomic units.
"""

import threading
import time
import uuid as uuid_module
from enum import IntEnum
from typing import Optional, Callable, TypeVar, Any, TYPE_CHECKING

from hazelcast.protocol.codec import (
    TransactionCodec,
    TXN_TYPE_ONE_PHASE,
    TXN_TYPE_TWO_PHASE,
    TXN_STATE_ACTIVE,
    TXN_STATE_COMMITTED,
    TXN_STATE_ROLLED_BACK,
    TXN_STATE_NO_TXN,
)

if TYPE_CHECKING:
    from hazelcast.proxy.transactional import (
        TransactionalMap,
        TransactionalSet,
        TransactionalList,
        TransactionalQueue,
        TransactionalMultiMap,
    )


T = TypeVar("T")


class TransactionType(IntEnum):
    """Transaction type enumeration."""

    ONE_PHASE = TXN_TYPE_ONE_PHASE
    TWO_PHASE = TXN_TYPE_TWO_PHASE


class TransactionState(IntEnum):
    """Transaction state enumeration."""

    ACTIVE = TXN_STATE_ACTIVE
    COMMITTED = TXN_STATE_COMMITTED
    ROLLED_BACK = TXN_STATE_ROLLED_BACK
    NO_TXN = TXN_STATE_NO_TXN


class TransactionOptions:
    """Options for configuring a transaction."""

    DEFAULT_TIMEOUT_MILLIS = 120000
    DEFAULT_DURABILITY = 1

    def __init__(
        self,
        timeout_millis: int = DEFAULT_TIMEOUT_MILLIS,
        durability: int = DEFAULT_DURABILITY,
        transaction_type: TransactionType = TransactionType.TWO_PHASE,
    ):
        """Initialize transaction options.

        Args:
            timeout_millis: Transaction timeout in milliseconds.
            durability: Number of backups for transaction log entries.
            transaction_type: The transaction type (ONE_PHASE or TWO_PHASE).
        """
        if timeout_millis <= 0:
            raise ValueError("Timeout must be positive")
        if durability < 0:
            raise ValueError("Durability cannot be negative")

        self._timeout_millis = timeout_millis
        self._durability = durability
        self._transaction_type = transaction_type

    @property
    def timeout_millis(self) -> int:
        """Return the transaction timeout in milliseconds."""
        return self._timeout_millis

    @property
    def durability(self) -> int:
        """Return the durability (number of backups)."""
        return self._durability

    @property
    def transaction_type(self) -> TransactionType:
        """Return the transaction type."""
        return self._transaction_type


class TransactionError(Exception):
    """Base exception for transaction errors."""

    pass


class TransactionNotActiveError(TransactionError):
    """Raised when an operation is attempted on a non-active transaction."""

    pass


TransactionNotActiveException = TransactionNotActiveError


class TransactionTimedOutError(TransactionError):
    """Raised when a transaction times out."""

    pass


class Transaction:
    """Represents a Hazelcast transaction.

    A transaction provides atomic operations across multiple
    data structures. Use `TransactionService.begin()` to create
    a new transaction.
    """

    def __init__(
        self,
        transaction_id: uuid_module.UUID,
        thread_id: int,
        options: TransactionOptions,
        invocation_service: Any,
    ):
        """Initialize a transaction.

        Args:
            transaction_id: The server-assigned transaction ID.
            thread_id: The thread ID associated with this transaction.
            options: The transaction options.
            invocation_service: The invocation service for sending requests.
        """
        self._transaction_id = transaction_id
        self._thread_id = thread_id
        self._options = options
        self._invocation_service = invocation_service
        self._state = TransactionState.ACTIVE
        self._start_time = time.time()
        self._lock = threading.Lock()

    @property
    def transaction_id(self) -> uuid_module.UUID:
        """Return the transaction ID."""
        return self._transaction_id

    @property
    def thread_id(self) -> int:
        """Return the thread ID."""
        return self._thread_id

    @property
    def state(self) -> TransactionState:
        """Return the current transaction state."""
        return self._state

    @property
    def timeout_millis(self) -> int:
        """Return the transaction timeout in milliseconds."""
        return self._options.timeout_millis

    def is_active(self) -> bool:
        """Check if the transaction is currently active."""
        return self._state == TransactionState.ACTIVE

    def _check_active(self) -> None:
        """Verify the transaction is active.

        Raises:
            TransactionNotActiveError: If transaction is not active.
            TransactionTimedOutError: If transaction has timed out.
        """
        if self._state != TransactionState.ACTIVE:
            raise TransactionNotActiveError(
                f"Transaction is not active, current state: {self._state.name}"
            )

        elapsed_ms = (time.time() - self._start_time) * 1000
        if elapsed_ms > self._options.timeout_millis:
            self._state = TransactionState.ROLLED_BACK
            raise TransactionTimedOutError("Transaction has timed out")

    def commit(self) -> None:
        """Commit this transaction.

        All operations performed within this transaction will be
        made permanent.

        Raises:
            TransactionNotActiveError: If transaction is not active.
            TransactionTimedOutError: If transaction has timed out.
        """
        with self._lock:
            self._check_active()

            request = TransactionCodec.encode_commit_request(
                self._transaction_id,
                self._thread_id,
            )

            if self._invocation_service is not None:
                self._invocation_service.invoke(request)

            self._state = TransactionState.COMMITTED

    def rollback(self) -> None:
        """Rollback this transaction.

        All operations performed within this transaction will be
        discarded.

        Raises:
            TransactionNotActiveError: If transaction is not active.
        """
        with self._lock:
            if self._state != TransactionState.ACTIVE:
                return

            request = TransactionCodec.encode_rollback_request(
                self._transaction_id,
                self._thread_id,
            )

            if self._invocation_service is not None:
                self._invocation_service.invoke(request)

            self._state = TransactionState.ROLLED_BACK


class TransactionContext:
    """Context for managing transactions and transactional proxies.

    Provides automatic commit on success and rollback on failure when
    used as a context manager. Also provides methods to obtain
    transactional proxies for distributed data structures.
    """

    def __init__(self, context: Any = None, transaction: Optional[Transaction] = None):
        """Initialize the transaction context.

        Args:
            context: A ProxyContext with invocation_service and serialization_service.
            transaction: An existing Transaction (for backward compatibility).
        """
        if transaction is not None:
            self._transaction = transaction
            self._context = None
            self._is_active = transaction.is_active()
        else:
            self._context = context
            self._transaction = None
            self._is_active = False
        self._thread_id_counter = 0
        self._thread_id = 0
        self._txn_id: Optional[uuid_module.UUID] = None
        self._lock = threading.Lock()

    @property
    def transaction(self) -> Optional[Transaction]:
        """Return the managed transaction."""
        return self._transaction

    @property
    def transaction_id(self) -> Optional[uuid_module.UUID]:
        """Return the transaction ID."""
        if self._transaction:
            return self._transaction.transaction_id
        return self._txn_id

    def begin(self) -> None:
        """Begin a new transaction.

        Raises:
            TransactionError: If a transaction is already active.
        """
        with self._lock:
            if self._is_active:
                raise TransactionError("Transaction is already active")

            self._thread_id_counter += 1
            self._thread_id = self._thread_id_counter
            self._txn_id = uuid_module.uuid4()
            self._is_active = True

            if self._context and self._context.invocation_service:
                options = TransactionOptions()
                request = TransactionCodec.encode_create_request(
                    options.timeout_millis,
                    options.durability,
                    options.transaction_type,
                    self._thread_id,
                )
                try:
                    response = self._context.invocation_service.invoke(request)
                    self._txn_id = TransactionCodec.decode_create_response(response)
                except Exception:
                    pass

    def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            TransactionNotActiveException: If no transaction is active.
        """
        with self._lock:
            if self._transaction:
                self._transaction.commit()
                self._is_active = False
                return

            if not self._is_active:
                raise TransactionNotActiveException("Transaction is not active")

            if self._context and self._context.invocation_service and self._txn_id:
                request = TransactionCodec.encode_commit_request(
                    self._txn_id,
                    self._thread_id,
                )
                try:
                    self._context.invocation_service.invoke(request)
                except Exception:
                    pass

            self._is_active = False

    def rollback(self) -> None:
        """Rollback the current transaction."""
        with self._lock:
            if self._transaction:
                self._transaction.rollback()
                self._is_active = False
                return

            if not self._is_active:
                return

            if self._context and self._context.invocation_service and self._txn_id:
                request = TransactionCodec.encode_rollback_request(
                    self._txn_id,
                    self._thread_id,
                )
                try:
                    self._context.invocation_service.invoke(request)
                except Exception:
                    pass

            self._is_active = False

    def _check_active(self) -> None:
        """Verify the transaction is active.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        if self._transaction:
            self._transaction._check_active()
            return

        if not self._is_active:
            raise TransactionNotActiveException("Transaction is not active")

    def _get_txn_id(self) -> Optional[uuid_module.UUID]:
        """Get the current transaction ID."""
        if self._transaction:
            return self._transaction.transaction_id
        return self._txn_id

    def _get_thread_id(self) -> int:
        """Get the current thread ID."""
        if self._transaction:
            return self._transaction.thread_id
        return self._thread_id

    def get_map(self, name: str) -> "TransactionalMap":
        """Get a transactional map proxy.

        Args:
            name: The name of the map.

        Returns:
            A TransactionalMap instance.
        """
        from hazelcast.proxy.transactional import TransactionalMap
        return TransactionalMap(name, self)

    def get_set(self, name: str) -> "TransactionalSet":
        """Get a transactional set proxy.

        Args:
            name: The name of the set.

        Returns:
            A TransactionalSet instance.
        """
        from hazelcast.proxy.transactional import TransactionalSet
        return TransactionalSet(name, self)

    def get_list(self, name: str) -> "TransactionalList":
        """Get a transactional list proxy.

        Args:
            name: The name of the list.

        Returns:
            A TransactionalList instance.
        """
        from hazelcast.proxy.transactional import TransactionalList
        return TransactionalList(name, self)

    def get_queue(self, name: str) -> "TransactionalQueue":
        """Get a transactional queue proxy.

        Args:
            name: The name of the queue.

        Returns:
            A TransactionalQueue instance.
        """
        from hazelcast.proxy.transactional import TransactionalQueue
        return TransactionalQueue(name, self)

    def get_multi_map(self, name: str) -> "TransactionalMultiMap":
        """Get a transactional multi-map proxy.

        Args:
            name: The name of the multi-map.

        Returns:
            A TransactionalMultiMap instance.
        """
        from hazelcast.proxy.transactional import TransactionalMultiMap
        return TransactionalMultiMap(name, self)

    def __enter__(self) -> "TransactionContext":
        """Enter the transaction context."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Exit the transaction context.

        Commits on success, rolls back on exception.

        Returns:
            False to propagate any exceptions.
        """
        if exc_type is None:
            try:
                self.commit()
            except Exception:
                self.rollback()
                raise
        else:
            try:
                self.rollback()
            except Exception:
                pass
        return False


class TransactionService:
    """Service for managing Hazelcast transactions.

    This service provides methods to begin, commit, and rollback
    transactions. It also supports transactional operations using
    context managers.
    """

    def __init__(self, invocation_service: Any = None):
        """Initialize the transaction service.

        Args:
            invocation_service: The invocation service for sending requests.
        """
        self._invocation_service = invocation_service
        self._thread_id_counter = 0
        self._lock = threading.Lock()

    def _next_thread_id(self) -> int:
        """Generate the next thread ID."""
        with self._lock:
            self._thread_id_counter += 1
            return self._thread_id_counter

    def begin(
        self,
        options: Optional[TransactionOptions] = None,
    ) -> Transaction:
        """Begin a new transaction.

        Args:
            options: Optional transaction configuration options.
                    Uses defaults if not provided.

        Returns:
            A new Transaction instance.
        """
        if options is None:
            options = TransactionOptions()

        thread_id = self._next_thread_id()

        request = TransactionCodec.encode_create_request(
            options.timeout_millis,
            options.durability,
            options.transaction_type,
            thread_id,
        )

        if self._invocation_service is not None:
            response = self._invocation_service.invoke(request)
            transaction_id = TransactionCodec.decode_create_response(response)
        else:
            transaction_id = uuid_module.uuid4()

        return Transaction(
            transaction_id=transaction_id,
            thread_id=thread_id,
            options=options,
            invocation_service=self._invocation_service,
        )

    def transaction(
        self,
        options: Optional[TransactionOptions] = None,
    ) -> TransactionContext:
        """Create a transaction context for use with 'with' statement.

        Args:
            options: Optional transaction configuration options.

        Returns:
            A TransactionContext that auto-commits or rolls back.

        Example:
            with transaction_service.transaction() as ctx:
                # perform operations
                # auto-commits on success, auto-rollbacks on exception
        """
        txn = self.begin(options)
        return TransactionContext(transaction=txn)

    def execute_transaction(
        self,
        callback: Callable[[TransactionContext], T],
        options: Optional[TransactionOptions] = None,
    ) -> T:
        """Execute a callback within a transaction.

        The transaction is automatically committed if the callback
        completes successfully, or rolled back if an exception is raised.

        Args:
            callback: A callable that receives the TransactionContext
                     and returns a result.
            options: Optional transaction configuration options.

        Returns:
            The result of the callback.

        Raises:
            Any exception raised by the callback (after rollback).
        """
        with self.transaction(options) as ctx:
            return callback(ctx)
