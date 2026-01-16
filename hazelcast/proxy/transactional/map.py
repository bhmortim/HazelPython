"""Transactional Map proxy implementation."""

from typing import Any, Optional, Set, List, TYPE_CHECKING

from hazelcast.proxy.transactional.base import TransactionalProxy
from hazelcast.logging import get_logger

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext

_logger = get_logger("transactional.map")


class TransactionalMap(TransactionalProxy):
    """Transactional proxy for Map operations.

    Provides transactional access to a distributed map. All operations
    are part of the enclosing transaction.

    Example:
        >>> with client.new_transaction_context() as ctx:
        ...     txn_map = ctx.get_map("my-map")
        ...     old_value = txn_map.put("key", "new-value")
        ...     value = txn_map.get("key")
    """

    def __init__(self, name: str, transaction: "TransactionContext"):
        super().__init__(name, transaction)
        self._local_changes: dict = {}
        self._deleted_keys: set = set()

    def put(self, key: Any, value: Any) -> Optional[Any]:
        """Put a key-value pair into the map.

        Args:
            key: The key to store.
            value: The value to associate with the key.

        Returns:
            The previous value associated with the key, or None.
        """
        self._check_transaction_active()
        _logger.debug(
            "TransactionalMap[%s].put(%s, %s) in txn %s",
            self._name,
            key,
            value,
            self._transaction.txn_id,
        )
        old_value = self._local_changes.get(key)
        self._local_changes[key] = value
        self._deleted_keys.discard(key)
        return old_value

    def put_if_absent(self, key: Any, value: Any) -> Optional[Any]:
        """Put a key-value pair if the key is not already present.

        Args:
            key: The key to store.
            value: The value to associate with the key.

        Returns:
            The existing value if present, or None if the put succeeded.
        """
        self._check_transaction_active()
        if key in self._local_changes and key not in self._deleted_keys:
            return self._local_changes[key]
        self._local_changes[key] = value
        self._deleted_keys.discard(key)
        return None

    def get(self, key: Any) -> Optional[Any]:
        """Get the value associated with a key.

        Args:
            key: The key to look up.

        Returns:
            The value associated with the key, or None if not found.
        """
        self._check_transaction_active()
        if key in self._deleted_keys:
            return None
        return self._local_changes.get(key)

    def remove(self, key: Any) -> Optional[Any]:
        """Remove a key-value pair from the map.

        Args:
            key: The key to remove.

        Returns:
            The removed value, or None if the key was not found.
        """
        self._check_transaction_active()
        old_value = self._local_changes.pop(key, None)
        self._deleted_keys.add(key)
        return old_value

    def delete(self, key: Any) -> None:
        """Delete a key from the map without returning the old value.

        Args:
            key: The key to delete.
        """
        self._check_transaction_active()
        self._local_changes.pop(key, None)
        self._deleted_keys.add(key)

    def contains_key(self, key: Any) -> bool:
        """Check if the map contains a key.

        Args:
            key: The key to check.

        Returns:
            True if the key exists in the map.
        """
        self._check_transaction_active()
        if key in self._deleted_keys:
            return False
        return key in self._local_changes

    def get_for_update(self, key: Any) -> Optional[Any]:
        """Get a value and lock the key for update.

        Args:
            key: The key to get and lock.

        Returns:
            The value associated with the key, or None.
        """
        self._check_transaction_active()
        if key in self._deleted_keys:
            return None
        return self._local_changes.get(key)

    def replace(self, key: Any, value: Any) -> Optional[Any]:
        """Replace the value for a key if it exists.

        Args:
            key: The key to replace.
            value: The new value.

        Returns:
            The previous value, or None if the key was not present.
        """
        self._check_transaction_active()
        if key not in self._local_changes or key in self._deleted_keys:
            return None
        old_value = self._local_changes[key]
        self._local_changes[key] = value
        return old_value

    def replace_if_same(self, key: Any, old_value: Any, new_value: Any) -> bool:
        """Replace the value for a key only if it matches the expected value.

        Args:
            key: The key to replace.
            old_value: The expected current value.
            new_value: The new value.

        Returns:
            True if the replacement was successful.
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

    def set(self, key: Any, value: Any) -> None:
        """Set a value without returning the old value.

        Args:
            key: The key to set.
            value: The value to associate with the key.
        """
        self._check_transaction_active()
        self._local_changes[key] = value
        self._deleted_keys.discard(key)

    def size(self) -> int:
        """Get the number of entries in the map.

        Returns:
            The number of key-value pairs.
        """
        self._check_transaction_active()
        return len(self._local_changes) - len(
            self._deleted_keys & set(self._local_changes.keys())
        )

    def is_empty(self) -> bool:
        """Check if the map is empty.

        Returns:
            True if the map contains no entries.
        """
        return self.size() == 0

    def key_set(self) -> Set[Any]:
        """Get all keys in the map.

        Returns:
            A set of all keys.
        """
        self._check_transaction_active()
        return set(self._local_changes.keys()) - self._deleted_keys

    def values(self) -> List[Any]:
        """Get all values in the map.

        Returns:
            A list of all values.
        """
        self._check_transaction_active()
        return [
            v for k, v in self._local_changes.items()
            if k not in self._deleted_keys
        ]
