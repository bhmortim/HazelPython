"""Transactional MultiMap proxy implementation."""

from collections import defaultdict
from typing import Any, Dict, List, Set, TYPE_CHECKING

from hazelcast.proxy.transactional.base import TransactionalProxy

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext


class TransactionalMultiMap(TransactionalProxy):
    """Transactional proxy for MultiMap operations."""

    def __init__(self, name: str, transaction_context: "TransactionContext"):
        super().__init__(name, transaction_context)
        self._data: Dict[Any, List[Any]] = defaultdict(list)

    def put(self, key: Any, value: Any) -> bool:
        """Put a key-value pair into the multimap."""
        self._check_transaction_active()
        self._data[key].append(value)
        return True

    def get(self, key: Any) -> List[Any]:
        """Get all values associated with a key."""
        self._check_transaction_active()
        return list(self._data.get(key, []))

    def remove(self, key: Any, value: Any) -> bool:
        """Remove a specific key-value pair."""
        self._check_transaction_active()
        if key not in self._data or value not in self._data[key]:
            return False
        self._data[key].remove(value)
        if not self._data[key]:
            del self._data[key]
        return True

    def remove_all(self, key: Any) -> List[Any]:
        """Remove all values associated with a key."""
        self._check_transaction_active()
        values = list(self._data.get(key, []))
        if key in self._data:
            del self._data[key]
        return values

    def contains_key(self, key: Any) -> bool:
        """Check if the multimap contains a key."""
        self._check_transaction_active()
        return key in self._data and len(self._data[key]) > 0

    def contains_value(self, value: Any) -> bool:
        """Check if the multimap contains a value."""
        self._check_transaction_active()
        for values in self._data.values():
            if value in values:
                return True
        return False

    def contains_entry(self, key: Any, value: Any) -> bool:
        """Check if the multimap contains a specific entry."""
        self._check_transaction_active()
        return key in self._data and value in self._data[key]

    def value_count(self, key: Any) -> int:
        """Get the number of values for a key."""
        self._check_transaction_active()
        return len(self._data.get(key, []))

    def size(self) -> int:
        """Get the total number of entries."""
        self._check_transaction_active()
        return sum(len(v) for v in self._data.values())

    def is_empty(self) -> bool:
        """Check if the multimap is empty."""
        return self.size() == 0

    def key_set(self) -> Set[Any]:
        """Get all keys."""
        self._check_transaction_active()
        return set(self._data.keys())

    def values(self) -> List[Any]:
        """Get all values."""
        self._check_transaction_active()
        result = []
        for values in self._data.values():
            result.extend(values)
        return result
