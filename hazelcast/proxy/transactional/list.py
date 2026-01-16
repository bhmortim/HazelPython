"""Transactional List proxy implementation."""

from typing import Any, List as ListType, TYPE_CHECKING

from hazelcast.proxy.transactional.base import TransactionalProxy
from hazelcast.logging import get_logger

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext

_logger = get_logger("transactional.list")


class TransactionalList(TransactionalProxy):
    """Transactional proxy for List operations.

    Provides transactional access to a distributed list. All operations
    are part of the enclosing transaction.

    Example:
        >>> with client.new_transaction_context() as ctx:
        ...     txn_list = ctx.get_list("my-list")
        ...     txn_list.add("item1")
        ...     size = txn_list.size()
    """

    def __init__(self, name: str, transaction: "TransactionContext"):
        super().__init__(name, transaction)
        self._items: ListType[Any] = []

    def add(self, item: Any) -> bool:
        """Add an item to the list.

        Args:
            item: The item to add.

        Returns:
            True if the item was added.
        """
        self._check_transaction_active()
        _logger.debug(
            "TransactionalList[%s].add(%s) in txn %s",
            self._name,
            item,
            self._transaction.txn_id,
        )
        self._items.append(item)
        return True

    def remove(self, item: Any) -> bool:
        """Remove an item from the list.

        Args:
            item: The item to remove.

        Returns:
            True if the item was removed, False if not found.
        """
        self._check_transaction_active()
        try:
            self._items.remove(item)
            return True
        except ValueError:
            return False

    def size(self) -> int:
        """Get the number of items in the list.

        Returns:
            The number of items.
        """
        self._check_transaction_active()
        return len(self._items)

    def is_empty(self) -> bool:
        """Check if the list is empty.

        Returns:
            True if the list contains no items.
        """
        return self.size() == 0

    def get_all(self) -> ListType[Any]:
        """Get all items in the list.

        Returns:
            A list of all items.
        """
        self._check_transaction_active()
        return list(self._items)
