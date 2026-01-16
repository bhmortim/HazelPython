"""Transactional List proxy implementation."""

from typing import Any, List as PyList, TYPE_CHECKING

from hazelcast.proxy.transactional.base import TransactionalProxy

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext


class TransactionalList(TransactionalProxy):
    """Transactional proxy for IList operations.

    Provides list operations that participate in a transaction. Changes
    made through this proxy are isolated until the transaction commits.

    Example:
        >>> with client.new_transaction_context() as ctx:
        ...     txn_list = ctx.get_list("my-list")
        ...     txn_list.add("item")
        ...     size = txn_list.size()
    """

    def __init__(self, name: str, transaction_context: "TransactionContext"):
        super().__init__(name, transaction_context)
        self._items: PyList[Any] = []

    def add(self, item: Any) -> bool:
        """Add an item to the list within the transaction.

        Args:
            item: The item to add.

        Returns:
            True (list always accepts items).

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        self._items.append(item)
        return True

    def remove(self, item: Any) -> bool:
        """Remove the first occurrence of an item from the list.

        Args:
            item: The item to remove.

        Returns:
            True if the item was removed, False if not found.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        try:
            self._items.remove(item)
            return True
        except ValueError:
            return False

    def contains(self, item: Any) -> bool:
        """Check if an item exists in the list within the transaction.

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
        """Get the number of items in the list within the transaction.

        Returns:
            The number of items.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return len(self._items)

    def is_empty(self) -> bool:
        """Check if the list is empty within the transaction.

        Returns:
            True if the list has no items, False otherwise.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        return self.size() == 0
