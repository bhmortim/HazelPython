"""Transactional Map proxy implementation."""

from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING

from hazelcast.proxy.transactional.base import TransactionalProxy

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext


class TransactionalMap(TransactionalProxy):
    """Transactional proxy for IMap operations.

    Provides map operations that participate in a transaction. Changes
    made through this proxy are isolated until the transaction commits.

    All operations require an active transaction.

    Example:
        >>> with client.new_transaction_context() as ctx:
        ...     txn_map = ctx.get_map("my-map")
        ...     txn_map.put("key", "value")
        ...     value = txn_map.get("key")
    """

    def __init__(self, name: str, transaction_context: "TransactionContext"):
        super().__init__(name, transaction_context)
        self._local_changes: Dict[Any, Any] = {}
        self._deleted_keys: Set[Any] = set()

    def put(self, key: Any, value: Any) -> Optional[Any]:
        """Put a key-value pair into the map within the transaction.

        Args:
            key: The key to store.
            value: The value to associate with the key.

        Returns:
            The previous value associated with the key, or None.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        old_value = self._local_changes.get(key)
        self._local_changes[key] = value
        self._deleted_keys.discard(key)
        return old_value

    def get(self, key: Any) -> Optional[Any]:
        """Get the value associated with a key within the transaction.

        Args:
            key: The key to look up.

        Returns:
            The value associated with the key, or None if not found.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        if key in self._deleted_keys:
            return None
        return self._local_changes.get(key)

    def remove(self, key: Any) -> Optional[Any]:
        """Remove a key from the map within the transaction.

        Args:
            key: The key to remove.

        Returns:
            The previous value associated with the key, or None.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        old_value = self._local_changes.pop(key, None)
        self._deleted_keys.add(key)
        return old_value

    def contains_key(self, key: Any) -> bool:
        """Check if a key exists in the map within the transaction.

        Args:
            key: The key to check.

        Returns:
            True if the key exists, False otherwise.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        if key in self._deleted_keys:
            return False
        return key in self._local_changes

    def size(self) -> int:
        """Get the number of entries in the map within the transaction.

        Returns:
            The number of entries.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return len(self._local_changes)

    def is_empty(self) -> bool:
        """Check if the map is empty within the transaction.

        Returns:
            True if the map has no entries, False otherwise.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        return self.size() == 0

    def key_set(self) -> Set[Any]:
        """Get all keys in the map within the transaction.

        Returns:
            A set of all keys.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return set(self._local_changes.keys())

    def values(self) -> List[Any]:
        """Get all values in the map within the transaction.

        Returns:
            A list of all values.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return list(self._local_changes.values())

    def get_for_update(self, key: Any) -> Optional[Any]:
        """Get the value for a key and lock it for update.

        This method acquires a lock on the key for the duration
        of the transaction.

        Args:
            key: The key to lock and retrieve.

        Returns:
            The value associated with the key, or None.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        if key in self._deleted_keys:
            return None
        return self._local_changes.get(key)

    def put_if_absent(self, key: Any, value: Any) -> Optional[Any]:
        """Put a value if the key is not already present.

        Args:
            key: The key to store.
            value: The value to associate with the key.

        Returns:
            The existing value if present, None if the put succeeded.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        if key in self._local_changes and key not in self._deleted_keys:
            return self._local_changes[key]
        self._local_changes[key] = value
        self._deleted_keys.discard(key)
        return None

    def replace(self, key: Any, value: Any) -> Optional[Any]:
        """Replace the value for a key if it exists.

        Args:
            key: The key to replace.
            value: The new value.

        Returns:
            The previous value, or None if the key didn't exist.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        if key not in self._local_changes or key in self._deleted_keys:
            return None
        old_value = self._local_changes[key]
        self._local_changes[key] = value
        return old_value

    def replace_if_same(self, key: Any, old_value: Any, new_value: Any) -> bool:
        """Replace the value only if it matches the expected value.

        Args:
            key: The key to replace.
            old_value: The expected current value.
            new_value: The new value.

        Returns:
            True if the replacement was successful.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        if key in self._deleted_keys:
            return False
        if key not in self._local_changes:
            return False
        if self._local_changes[key] != old_value:
            return False
        self._local_changes[key] = new_value
        return True

    def delete(self, key: Any) -> None:
        """Delete a key from the map within the transaction.

        Args:
            key: The key to delete.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        self._local_changes.pop(key, None)
        self._deleted_keys.add(key)

    def set(self, key: Any, value: Any) -> None:
        """Set a value for a key without returning the old value.

        Args:
            key: The key to set.
            value: The value to set.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        self._local_changes[key] = value
        self._deleted_keys.discard(key)
