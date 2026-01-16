"""Transactional Queue proxy implementation."""

from collections import deque
from typing import Any, Optional, TYPE_CHECKING

from hazelcast.proxy.transactional.base import TransactionalProxy

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext


class TransactionalQueue(TransactionalProxy):
    """Transactional proxy for IQueue operations.

    Provides queue operations that participate in a transaction. Changes
    made through this proxy are isolated until the transaction commits.

    Example:
        >>> with client.new_transaction_context() as ctx:
        ...     txn_queue = ctx.get_queue("my-queue")
        ...     txn_queue.offer("item")
        ...     item = txn_queue.poll()
    """

    def __init__(self, name: str, transaction_context: "TransactionContext"):
        super().__init__(name, transaction_context)
        self._items: deque = deque()

    def offer(self, item: Any, timeout: float = 0) -> bool:
        """Add an item to the queue within the transaction.

        Args:
            item: The item to add.
            timeout: Timeout in seconds (ignored in transactional context).

        Returns:
            True if the item was added successfully.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        self._items.append(item)
        return True

    def poll(self, timeout: float = 0) -> Optional[Any]:
        """Remove and return the head of the queue.

        Args:
            timeout: Timeout in seconds (ignored in transactional context).

        Returns:
            The head item, or None if the queue is empty.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        if not self._items:
            return None
        return self._items.popleft()

    def peek(self, timeout: float = 0) -> Optional[Any]:
        """Return the head of the queue without removing it.

        Args:
            timeout: Timeout in seconds (ignored in transactional context).

        Returns:
            The head item, or None if the queue is empty.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        if not self._items:
            return None
        return self._items[0]

    def take(self) -> Optional[Any]:
        """Remove and return the head of the queue, blocking if necessary.

        In transactional context, this behaves like poll().

        Returns:
            The head item, or None if the queue is empty.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        return self.poll()

    def size(self) -> int:
        """Get the number of items in the queue within the transaction.

        Returns:
            The number of items.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return len(self._items)

    def is_empty(self) -> bool:
        """Check if the queue is empty within the transaction.

        Returns:
            True if the queue has no items, False otherwise.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        return self.size() == 0
