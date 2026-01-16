"""Transactional Queue proxy implementation."""

from collections import deque
from typing import Any, Optional, TYPE_CHECKING

from hazelcast.proxy.transactional.base import TransactionalProxy

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext


class TransactionalQueue(TransactionalProxy):
    """Transactional proxy for IQueue operations."""

    def __init__(self, name: str, transaction_context: "TransactionContext"):
        super().__init__(name, transaction_context)
        self._items: deque = deque()

    def offer(self, item: Any) -> bool:
        """Add an item to the queue."""
        self._check_transaction_active()
        self._items.append(item)
        return True

    def poll(self) -> Optional[Any]:
        """Remove and return the head of the queue."""
        self._check_transaction_active()
        if not self._items:
            return None
        return self._items.popleft()

    def peek(self) -> Optional[Any]:
        """Return the head of the queue without removing it."""
        self._check_transaction_active()
        if not self._items:
            return None
        return self._items[0]

    def take(self) -> Optional[Any]:
        """Remove and return the head of the queue, blocking if necessary."""
        self._check_transaction_active()
        if not self._items:
            return None
        return self._items.popleft()

    def size(self) -> int:
        """Get the number of items in the queue."""
        self._check_transaction_active()
        return len(self._items)

    def is_empty(self) -> bool:
        """Check if the queue is empty."""
        return self.size() == 0
