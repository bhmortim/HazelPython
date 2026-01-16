"""Transactional MultiMap proxy implementation."""

from collections import defaultdict
from typing import Any, Collection, List, Set, TYPE_CHECKING

from hazelcast.proxy.transactional.base import TransactionalProxy

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext


class TransactionalMultiMap(TransactionalProxy):
    """Transactional proxy for MultiMap operations.

    Provides multi-map operations that participate in a transaction.
    A multi-map allows multiple values per key. Changes made through
    this proxy are isolated until the transaction commits.

    Example:
        >>> with client.new_transaction_context() as ctx:
        ...     txn_mm = ctx.get_multi_map("my-multimap")
        ...     txn_mm.put("key", "value1")
        ...     txn_mm.put("key", "value2")
        ...     values = txn_mm.get("key")  # ["value1", "value2"]
    """

    def __init__(self, name: str, transaction_context: "TransactionContext"):
        super().__init__(name, transaction_context)
        self._entries: defaultdict = defaultdict(list)

    def put(self, key: Any, value: Any) -> bool:
        """Add a key-value pair to the multi-map.

        Multiple values can be associated with the same key.

        Args:
            key: The key to store.
            value: The value to add for this key.

        Returns:
            True if the entry was added.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        self._entries[key].append(value)
        return True

    def get(self, key: Any) -> List[Any]:
        """Get all values associated with a key.

        Args:
            key: The key to look up.

        Returns:
            A list of values for the key, or empty list if not found.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return list(self._entries.get(key, []))

    def remove(self, key: Any, value: Any) -> bool:
        """Remove a specific key-value pair from the multi-map.

        Args:
            key: The key.
            value: The value to remove.

        Returns:
            True if the entry was removed, False if not found.

        Raises:
            TransactionNotActiveException: If transaction is not active.
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
            The list of removed values.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return self._entries.pop(key, [])

    def contains_key(self, key: Any) -> bool:
        """Check if a key exists in the multi-map.

        Args:
            key: The key to check.

        Returns:
            True if the key exists, False otherwise.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return key in self._entries and len(self._entries[key]) > 0

    def contains_value(self, value: Any) -> bool:
        """Check if a value exists in the multi-map.

        Args:
            value: The value to check.

        Returns:
            True if the value exists, False otherwise.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        for values in self._entries.values():
            if value in values:
                return True
        return False

    def contains_entry(self, key: Any, value: Any) -> bool:
        """Check if a specific key-value pair exists.

        Args:
            key: The key to check.
            value: The value to check.

        Returns:
            True if the entry exists, False otherwise.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return key in self._entries and value in self._entries[key]

    def value_count(self, key: Any) -> int:
        """Get the number of values associated with a key.

        Args:
            key: The key to check.

        Returns:
            The number of values for this key.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return len(self._entries.get(key, []))

    def size(self) -> int:
        """Get the total number of key-value pairs.

        Returns:
            The total number of entries.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return sum(len(values) for values in self._entries.values())

    def is_empty(self) -> bool:
        """Check if the multi-map is empty.

        Returns:
            True if the multi-map has no entries.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        return self.size() == 0

    def key_set(self) -> Set[Any]:
        """Get all keys in the multi-map.

        Returns:
            A set of all keys.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return set(k for k, v in self._entries.items() if v)

    def values(self) -> List[Any]:
        """Get all values in the multi-map.

        Returns:
            A list of all values.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        result = []
        for values in self._entries.values():
            result.extend(values)
        return result
