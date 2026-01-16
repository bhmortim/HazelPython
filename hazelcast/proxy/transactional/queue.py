"""Transactional Queue proxy implementation."""

from collections import deque
from typing import Any, Optional, TYPE_CHECKING

from hazelcast.proxy.transactional.base import TransactionalProxy
from hazelcast.logging import get_logger

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext

_logger = get_logger("transactional.queue")


class TransactionalQueue(TransactionalProxy):
    """Transactional proxy for Queue operations.

    Provides transactional access to a distributed queue. All operations
    are part of the enclosing transaction.

    Example:
        >>> with client.new_transaction_context() as ctx:
        ...     txn_queue = ctx.get_queue("my-queue")
        ...     txn_queue.offer("item1")
        ...     item = txn_queue.poll()
    """

    def __init__(self, name: str, transaction: "TransactionContext"):
        super().__init__(name, transaction)
        self._queue: deque = deque()

    def offer(self, item: Any, timeout: float = 0) -> bool:
        """Offer an item to the queue.

        Args:
            item: The item to offer.
            timeout: Optional timeout in seconds (ignored in transactional context).

        Returns:
            True if the item was added to the queue.
        """
        self._check_transaction_active()
        _logger.debug(
            "TransactionalQueue[%s].offer(%s) in txn %s",
            self._name,
            item,
            self._transaction.txn_id,
        )
        self._queue.append(item)
        return True

    def poll(self, timeout: float = 0) -> Optional[Any]:
        """Poll an item from the queue.

        Args:
            timeout: Optional timeout in seconds (ignored in transactional context).

        Returns:
            The item at the head of the queue, or None if empty.
        """
        self._check_transaction_active()
        if not self._queue:
            return None
        return self._queue.popleft()

    def take(self) -> Any:
        """Take an item from the queue, waiting if necessary.

        In transactional context, this behaves like poll() since
        blocking is not supported within transactions.

        Returns:
            The item at the head of the queue.

        Raises:
            IllegalStateException: If the queue is empty.
        """
        self._check_transaction_active()
        if not self._queue:
            from hazelcast.exceptions import IllegalStateException
            raise IllegalStateException("Queue is empty")
        return self._queue.popleft()

    def peek(self, timeout: float = 0) -> Optional[Any]:
        """Peek at the item at the head of the queue.

        Args:
            timeout: Optional timeout in seconds (ignored in transactional context).

        Returns:
            The item at the head of the queue, or None if empty.
        """
        self._check_transaction_active()
        if not self._queue:
            return None
        return self._queue[0]

    def size(self) -> int:
        """Get the number of items in the queue.

        Returns:
            The number of items.
        """
        self._check_transaction_active()
        return len(self._queue)

    def is_empty(self) -> bool:
        """Check if the queue is empty.

        Returns:
            True if the queue contains no items.
        """
        return self.size() == 0
