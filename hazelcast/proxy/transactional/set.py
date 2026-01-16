"""Transactional Set proxy implementation."""

from typing import Any, Set as PySet, TYPE_CHECKING

from hazelcast.proxy.transactional.base import TransactionalProxy

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext


class TransactionalSet(TransactionalProxy):
    """Transactional proxy for ISet operations.

    Provides set operations that participate in a transaction. Changes
    made through this proxy are isolated until the transaction commits.

    Example:
        >>> with client.new_transaction_context() as ctx:
        ...     txn_set = ctx.get_set("my-set")
        ...     txn_set.add("item")
        ...     exists = txn_set.contains("item")
    """

    def __init__(self, name: str, transaction_context: "TransactionContext"):
        super().__init__(name, transaction_context)
        self._items: PySet[Any] = set()

    def add(self, item: Any) -> bool:
        """Add an item to the set within the transaction.

        Args:
            item: The item to add.

        Returns:
            True if the item was added, False if it already existed.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        if item in self._items:
            return False
        self._items.add(item)
        return True

    def remove(self, item: Any) -> bool:
        """Remove an item from the set within the transaction.

        Args:
            item: The item to remove.

        Returns:
            True if the item was removed, False if it didn't exist.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        if item not in self._items:
            return False
        self._items.discard(item)
        return True

    def contains(self, item: Any) -> bool:
        """Check if an item exists in the set within the transaction.

        Args:
            item: The item to check.

        Returns:
            True if the item exists, False otherwise.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return item in self._items

    def size(self) -> int:
        """Get the number of items in the set within the transaction.

        Returns:
            The number of items.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return len(self._items)

    def is_empty(self) -> bool:
        """Check if the set is empty within the transaction.

        Returns:
            True if the set has no items, False otherwise.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        return self.size() == 0
