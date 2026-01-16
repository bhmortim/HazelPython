"""Transactional List proxy implementation."""

from typing import Any, List as ListType, TYPE_CHECKING

from hazelcast.proxy.transactional.base import TransactionalProxy

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext


class TransactionalList(TransactionalProxy):
    """Transactional proxy for IList operations."""

    def __init__(self, name: str, transaction_context: "TransactionContext"):
        super().__init__(name, transaction_context)
        self._items: ListType[Any] = []

    def add(self, item: Any) -> bool:
        """Add an item to the list."""
        self._check_transaction_active()
        self._items.append(item)
        return True

    def remove(self, item: Any) -> bool:
        """Remove the first occurrence of an item from the list."""
        self._check_transaction_active()
        if item not in self._items:
            return False
        self._items.remove(item)
        return True

    def contains(self, item: Any) -> bool:
        """Check if the list contains an item."""
        self._check_transaction_active()
        return item in self._items

    def size(self) -> int:
        """Get the number of items in the list."""
        self._check_transaction_active()
        return len(self._items)

    def is_empty(self) -> bool:
        """Check if the list is empty."""
        return self.size() == 0
