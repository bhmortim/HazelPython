"""Transactional MultiMap proxy implementation."""

from collections import defaultdict
from typing import Any, List, TYPE_CHECKING

from hazelcast.proxy.transactional.base import TransactionalProxy
from hazelcast.logging import get_logger

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext

_logger = get_logger("transactional.multi_map")


class TransactionalMultiMap(TransactionalProxy):
    """Transactional proxy for MultiMap operations.

    Provides transactional access to a distributed multi-map. All operations
    are part of the enclosing transaction.

    Example:
        >>> with client.new_transaction_context() as ctx:
        ...     txn_mm = ctx.get_multi_map("my-multimap")
        ...     txn_mm.put("key", "value1")
        ...     txn_mm.put("key", "value2")
        ...     values = txn_mm.get("key")
    """

    def __init__(self, name: str, transaction: "TransactionContext"):
        super().__init__(name, transaction)
        self._entries: dict = defaultdict(list)

    def put(self, key: Any, value: Any) -> bool:
        """Put a key-value pair into the multi-map.

        Args:
            key: The key.
            value: The value to add for this key.

        Returns:
            True if the value was added.
        """
        self._check_transaction_active()
        _logger.debug(
            "TransactionalMultiMap[%s].put(%s, %s) in txn %s",
            self._name,
            key,
            value,
            self._transaction.txn_id,
        )
        self._entries[key].append(value)
        return True

    def get(self, key: Any) -> List[Any]:
        """Get all values associated with a key.

        Args:
            key: The key to look up.

        Returns:
            A list of values associated with the key.
        """
        self._check_transaction_active()
        return list(self._entries.get(key, []))

    def remove(self, key: Any, value: Any) -> bool:
        """Remove a specific key-value pair.

        Args:
            key: The key.
            value: The value to remove.

        Returns:
            True if the value was removed.
        """
        self._check_transaction_active()
        if key not in self._entries:
            return False
        values = self._entries[key]
        if value not in values:
            return False
        values.remove(value)
        if not values:
            del self._entries[key]
        return True

    def remove_all(self, key: Any) -> List[Any]:
        """Remove all values associated with a key.

        Args:
            key: The key to remove.

        Returns:
            A list of removed values.
        """
        self._check_transaction_active()
        if key not in self._entries:
            return []
        values = self._entries.pop(key)
        return list(values)

    def value_count(self, key: Any) -> int:
        """Get the count of values for a key.

        Args:
            key: The key to check.

        Returns:
            The number of values for this key.
        """
        self._check_transaction_active()
        return len(self._entries.get(key, []))

    def size(self) -> int:
        """Get the total number of key-value pairs.

        Returns:
            The total number of entries.
        """
        self._check_transaction_active()
        return sum(len(values) for values in self._entries.values())

    def is_empty(self) -> bool:
        """Check if the multi-map is empty.

        Returns:
            True if the multi-map contains no entries.
        """
        return self.size() == 0
