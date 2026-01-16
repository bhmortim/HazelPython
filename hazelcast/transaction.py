"""Transaction API for Hazelcast client.

This module provides transactional support for Hazelcast operations,
allowing multiple operations to be grouped into atomic units.
"""

import threading
import time
import uuid as uuid_module
from enum import IntEnum
from typing import Optional, Callable, TypeVar, Any

from hazelcast.protocol.codec import (
    TransactionCodec,
    TXN_TYPE_ONE_PHASE,
    TXN_TYPE_TWO_PHASE,
    TXN_STATE_ACTIVE,
    TXN_STATE_COMMITTED,
    TXN_STATE_ROLLED_BACK,
    TXN_STATE_NO_TXN,
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
    """Context manager for transactions.

    Provides automatic commit on success and rollback on failure.
    """

    def __init__(self, transaction: Transaction):
        """Initialize the context with a transaction.

        Args:
            transaction: The transaction to manage.
        """
        self._transaction = transaction

    @property
    def transaction(self) -> Transaction:
        """Return the managed transaction."""
        return self._transaction

    @property
    def transaction_id(self) -> uuid_module.UUID:
        """Return the transaction ID."""
        return self._transaction.transaction_id

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
                self._transaction.commit()
            except Exception:
                self._transaction.rollback()
                raise
        else:
            try:
                self._transaction.rollback()
            except Exception:
                pass
        return False

    def commit(self) -> None:
        """Explicitly commit the transaction."""
        self._transaction.commit()

    def rollback(self) -> None:
        """Explicitly rollback the transaction."""
        self._transaction.rollback()


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
        return TransactionContext(txn)

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
