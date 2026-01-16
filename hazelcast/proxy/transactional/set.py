"""Transactional Set proxy implementation."""

from typing import Any, TYPE_CHECKING

from hazelcast.proxy.transactional.base import TransactionalProxy
from hazelcast.logging import get_logger

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext

_logger = get_logger("transactional.set")


class TransactionalSet(TransactionalProxy):
    """Transactional proxy for Set operations.

    Provides transactional access to a distributed set. All operations
    are part of the enclosing transaction.

    Example:
        >>> with client.new_transaction_context() as ctx:
        ...     txn_set = ctx.get_set("my-set")
        ...     txn_set.add("item1")
        ...     exists = txn_set.contains("item1")
    """

    def __init__(self, name: str, transaction: "TransactionContext"):
        super().__init__(name, transaction)
        self._added_items: set = set()
        self._removed_items: set = set()

    def add(self, item: Any) -> bool:
        """Add an item to the set.

        Args:
            item: The item to add.

        Returns:
            True if the item was added, False if already present.
        """
        self._check_transaction_active()
        _logger.debug(
            "TransactionalSet[%s].add(%s) in txn %s",
            self._name,
            item,
            self._transaction.txn_id,
        )
        if item in self._added_items:
            return False
        self._added_items.add(item)
        self._removed_items.discard(item)
        return True

    def remove(self, item: Any) -> bool:
        """Remove an item from the set.

        Args:
            item: The item to remove.

        Returns:
            True if the item was removed, False if not found.
        """
        self._check_transaction_active()
        if item not in self._added_items:
            return False
        self._added_items.discard(item)
        self._removed_items.add(item)
        return True

    def contains(self, item: Any) -> bool:
        """Check if the set contains an item.

        Args:
            item: The item to check.

        Returns:
            True if the item is in the set.
        """
        self._check_transaction_active()
        if item in self._removed_items:
            return False
        return item in self._added_items

    def size(self) -> int:
        """Get the number of items in the set.

        Returns:
            The number of items.
        """
        self._check_transaction_active()
        return len(self._added_items - self._removed_items)

    def is_empty(self) -> bool:
        """Check if the set is empty.

        Returns:
            True if the set contains no items.
        """
        return self.size() == 0
