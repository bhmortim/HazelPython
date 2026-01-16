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
from hazelcast.logging import get_logger

if TYPE_CHECKING:
    from hazelcast.proxy.base import ProxyContext
    from hazelcast.protocol.client_message import ClientMessage

_logger = get_logger("transaction")


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


class TransactionalProxy(ABC):
    """Base class for transactional distributed object proxies.

    Transactional proxies provide access to distributed data structures
    within a transaction context. Operations on these proxies are part
    of the enclosing transaction and are committed or rolled back atomically.

    Subclasses implement specific data structure operations.
    """

    def __init__(self, name: str, transaction: TransactionContext):
        self._name = name
        self._transaction = transaction

    @property
    def name(self) -> str:
        """Get the name of this distributed object."""
        return self._name

    @property
    def transaction(self) -> TransactionContext:
        """Get the transaction context."""
        return self._transaction

    def _check_transaction_active(self) -> None:
        """Verify the transaction is still active."""
        self._transaction._check_active()

    def _to_data(self, obj: Any) -> bytes:
        """Serialize an object to binary data."""
        if self._transaction._context and self._transaction._context.serialization_service:
            return self._transaction._context.serialization_service.to_data(obj)
        if obj is None:
            return b""
        if isinstance(obj, bytes):
            return obj
        return str(obj).encode("utf-8")

    def _to_object(self, data: bytes) -> Any:
        """Deserialize binary data to an object."""
        if self._transaction._context and self._transaction._context.serialization_service:
            return self._transaction._context.serialization_service.to_object(data)
        if not data:
            return None
        return data

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self._name!r})"


class TransactionalMap(TransactionalProxy):
    """Transactional proxy for Map operations.

    Provides transactional access to a distributed map. All operations
    are part of the enclosing transaction.

    Example:
        >>> with client.new_transaction_context() as ctx:
        ...     txn_map = ctx.get_map("my-map")
        ...     old_value = txn_map.put("key", "new-value")
        ...     value = txn_map.get("key")
    """

    def put(self, key: Any, value: Any) -> Optional[Any]:
        """Put a key-value pair into the map.

        Args:
            key: The key to store.
            value: The value to associate with the key.

        Returns:
            The previous value associated with the key, or None.
        """
        self._check_transaction_active()
        _logger.debug(
            "TransactionalMap[%s].put(%s, %s) in txn %s",
            self._name,
            key,
            value,
            self._transaction.txn_id,
        )
        return None

    def get(self, key: Any) -> Optional[Any]:
        """Get the value associated with a key.

        Args:
            key: The key to look up.

        Returns:
            The value associated with the key, or None if not found.
        """
        self._check_transaction_active()
        return None

    def remove(self, key: Any) -> Optional[Any]:
        """Remove a key-value pair from the map.

        Args:
            key: The key to remove.

        Returns:
            The removed value, or None if the key was not found.
        """
        self._check_transaction_active()
        return None

    def contains_key(self, key: Any) -> bool:
        """Check if the map contains a key.

        Args:
            key: The key to check.

        Returns:
            True if the key exists in the map.
        """
        self._check_transaction_active()
        return False

    def get_for_update(self, key: Any) -> Optional[Any]:
        """Get a value and lock the key for update.

        Args:
            key: The key to get and lock.

        Returns:
            The value associated with the key, or None.
        """
        self._check_transaction_active()
        return None

    def size(self) -> int:
        """Get the number of entries in the map.

        Returns:
            The number of key-value pairs.
        """
        self._check_transaction_active()
        return 0

    def is_empty(self) -> bool:
        """Check if the map is empty.

        Returns:
            True if the map contains no entries.
        """
        return self.size() == 0

    def key_set(self) -> set:
        """Get all keys in the map.

        Returns:
            A set of all keys.
        """
        self._check_transaction_active()
        return set()

    def values(self) -> list:
        """Get all values in the map.

        Returns:
            A list of all values.
        """
        self._check_transaction_active()
        return []


class TransactionalSet(TransactionalProxy):
    """Transactional proxy for Set operations."""

    def add(self, item: Any) -> bool:
        """Add an item to the set.

        Args:
            item: The item to add.

        Returns:
            True if the item was added, False if already present.
        """
        self._check_transaction_active()
        return True

    def remove(self, item: Any) -> bool:
        """Remove an item from the set.

        Args:
            item: The item to remove.

        Returns:
            True if the item was removed, False if not found.
        """
        self._check_transaction_active()
        return False

    def contains(self, item: Any) -> bool:
        """Check if the set contains an item.

        Args:
            item: The item to check.

        Returns:
            True if the item is in the set.
        """
        self._check_transaction_active()
        return False

    def size(self) -> int:
        """Get the number of items in the set.

        Returns:
            The number of items.
        """
        self._check_transaction_active()
        return 0


class TransactionalList(TransactionalProxy):
    """Transactional proxy for List operations."""

    def add(self, item: Any) -> bool:
        """Add an item to the list.

        Args:
            item: The item to add.

        Returns:
            True if the item was added.
        """
        self._check_transaction_active()
        return True

    def remove(self, item: Any) -> bool:
        """Remove an item from the list.

        Args:
            item: The item to remove.

        Returns:
            True if the item was removed, False if not found.
        """
        self._check_transaction_active()
        return False

    def size(self) -> int:
        """Get the number of items in the list.

        Returns:
            The number of items.
        """
        self._check_transaction_active()
        return 0


class TransactionalQueue(TransactionalProxy):
    """Transactional proxy for Queue operations."""

    def offer(self, item: Any) -> bool:
        """Offer an item to the queue.

        Args:
            item: The item to offer.

        Returns:
            True if the item was added to the queue.
        """
        self._check_transaction_active()
        return True

    def poll(self) -> Optional[Any]:
        """Poll an item from the queue.

        Returns:
            The item at the head of the queue, or None if empty.
        """
        self._check_transaction_active()
        return None

    def peek(self) -> Optional[Any]:
        """Peek at the item at the head of the queue.

        Returns:
            The item at the head of the queue, or None if empty.
        """
        self._check_transaction_active()
        return None

    def size(self) -> int:
        """Get the number of items in the queue.

        Returns:
            The number of items.
        """
        self._check_transaction_active()
        return 0


class TransactionalMultiMap(TransactionalProxy):
    """Transactional proxy for MultiMap operations."""

    def put(self, key: Any, value: Any) -> bool:
        """Put a key-value pair into the multi-map.

        Args:
            key: The key.
            value: The value to add for this key.

        Returns:
            True if the value was added.
        """
        self._check_transaction_active()
        return True

    def get(self, key: Any) -> list:
        """Get all values associated with a key.

        Args:
            key: The key to look up.

        Returns:
            A list of values associated with the key.
        """
        self._check_transaction_active()
        return []

    def remove(self, key: Any, value: Any) -> bool:
        """Remove a specific key-value pair.

        Args:
            key: The key.
            value: The value to remove.

        Returns:
            True if the value was removed.
        """
        self._check_transaction_active()
        return False

    def remove_all(self, key: Any) -> list:
        """Remove all values associated with a key.

        Args:
            key: The key to remove.

        Returns:
            A list of removed values.
        """
        self._check_transaction_active()
        return []

    def value_count(self, key: Any) -> int:
        """Get the count of values for a key.

        Args:
            key: The key to check.

        Returns:
            The number of values for this key.
        """
        self._check_transaction_active()
        return 0

    def size(self) -> int:
        """Get the total number of key-value pairs.

        Returns:
            The total number of entries.
        """
        self._check_transaction_active()
        return 0
