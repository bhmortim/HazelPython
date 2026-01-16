"""Transactional Set proxy implementation."""

from typing import Any, TYPE_CHECKING

from hazelcast.proxy.transactional.base import TransactionalProxy

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext


class TransactionalSet(TransactionalProxy):
    """Transactional proxy for ISet operations."""

    def __init__(self, name: str, transaction_context: "TransactionContext"):
        super().__init__(name, transaction_context)
        self._items: set = set()

    def add(self, item: Any) -> bool:
        """Add an item to the set."""
        self._check_transaction_active()
        if item in self._items:
            return False
        self._items.add(item)
        return True

    def remove(self, item: Any) -> bool:
        """Remove an item from the set."""
        self._check_transaction_active()
        if item not in self._items:
            return False
        self._items.discard(item)
        return True

    def contains(self, item: Any) -> bool:
        """Check if the set contains an item."""
        self._check_transaction_active()
        return item in self._items

    def size(self) -> int:
        """Get the number of items in the set."""
        self._check_transaction_active()
        return len(self._items)

    def is_empty(self) -> bool:
        """Check if the set is empty."""
        return self.size() == 0
