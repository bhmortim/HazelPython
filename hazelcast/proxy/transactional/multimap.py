"""Transactional MultiMap proxy implementation."""

from collections import defaultdict
from typing import Any, Dict, List, Set, TYPE_CHECKING

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
        self._data: Dict[Any, List[Any]] = defaultdict(list)

    def put(self, key: Any, value: Any) -> bool:
        """Put a key-value pair into the multimap within the transaction.

        Args:
            key: The key to store.
            value: The value to add for this key.

        Returns:
            True if the entry was added.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()

        if self._has_server_connection():
            from hazelcast.protocol.codec import TransactionalMultiMapCodec
            key_data = self._to_data(key)
            value_data = self._to_data(value)
            request = TransactionalMultiMapCodec.encode_put_request(
                self._name,
                self._transaction_context._get_txn_id(),
                self._transaction_context._get_thread_id(),
                key_data,
                value_data,
            )
            response = self._invoke(request)
            if response:
                return TransactionalMultiMapCodec.decode_put_response(response)

        self._data[key].append(value)
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

        if self._has_server_connection():
            from hazelcast.protocol.codec import TransactionalMultiMapCodec
            key_data = self._to_data(key)
            request = TransactionalMultiMapCodec.encode_get_request(
                self._name,
                self._transaction_context._get_txn_id(),
                self._transaction_context._get_thread_id(),
                key_data,
            )
            response = self._invoke(request)
            if response:
                values_data = TransactionalMultiMapCodec.decode_get_response(response)
                return [self._to_object(v) for v in values_data]

        return list(self._data.get(key, []))

    def remove(self, key: Any, value: Any) -> bool:
        """Remove a specific key-value pair from the multimap.

        Args:
            key: The key.
            value: The value to remove.

        Returns:
            True if the entry was removed, False if not found.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()

        if self._has_server_connection():
            from hazelcast.protocol.codec import TransactionalMultiMapCodec
            key_data = self._to_data(key)
            value_data = self._to_data(value)
            request = TransactionalMultiMapCodec.encode_remove_request(
                self._name,
                self._transaction_context._get_txn_id(),
                self._transaction_context._get_thread_id(),
                key_data,
                value_data,
            )
            response = self._invoke(request)
            if response:
                return TransactionalMultiMapCodec.decode_remove_response(response)

        if key not in self._data or value not in self._data[key]:
            return False
        self._data[key].remove(value)
        if not self._data[key]:
            del self._data[key]
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

        if self._has_server_connection():
            from hazelcast.protocol.codec import TransactionalMultiMapCodec
            key_data = self._to_data(key)
            request = TransactionalMultiMapCodec.encode_remove_all_request(
                self._name,
                self._transaction_context._get_txn_id(),
                self._transaction_context._get_thread_id(),
                key_data,
            )
            response = self._invoke(request)
            if response:
                values_data = TransactionalMultiMapCodec.decode_remove_all_response(response)
                return [self._to_object(v) for v in values_data]

        values = list(self._data.get(key, []))
        if key in self._data:
            del self._data[key]
        return values

    def contains_key(self, key: Any) -> bool:
        """Check if the multimap contains a key.

        Args:
            key: The key to check.

        Returns:
            True if the key exists, False otherwise.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return key in self._data and len(self._data[key]) > 0

    def contains_value(self, value: Any) -> bool:
        """Check if the multimap contains a value.

        Args:
            value: The value to check.

        Returns:
            True if the value exists, False otherwise.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        for values in self._data.values():
            if value in values:
                return True
        return False

    def contains_entry(self, key: Any, value: Any) -> bool:
        """Check if the multimap contains a specific entry.

        Args:
            key: The key to check.
            value: The value to check.

        Returns:
            True if the entry exists, False otherwise.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return key in self._data and value in self._data[key]

    def value_count(self, key: Any) -> int:
        """Get the number of values for a key.

        Args:
            key: The key to check.

        Returns:
            The number of values for this key.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()

        if self._has_server_connection():
            from hazelcast.protocol.codec import TransactionalMultiMapCodec
            key_data = self._to_data(key)
            request = TransactionalMultiMapCodec.encode_value_count_request(
                self._name,
                self._transaction_context._get_txn_id(),
                self._transaction_context._get_thread_id(),
                key_data,
            )
            response = self._invoke(request)
            if response:
                return TransactionalMultiMapCodec.decode_value_count_response(response)

        return len(self._data.get(key, []))

    def size(self) -> int:
        """Get the total number of entries.

        Returns:
            The total number of key-value pairs.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()

        if self._has_server_connection():
            from hazelcast.protocol.codec import TransactionalMultiMapCodec
            request = TransactionalMultiMapCodec.encode_size_request(
                self._name,
                self._transaction_context._get_txn_id(),
                self._transaction_context._get_thread_id(),
            )
            response = self._invoke(request)
            if response:
                return TransactionalMultiMapCodec.decode_size_response(response)

        return sum(len(v) for v in self._data.values())

    def is_empty(self) -> bool:
        """Check if the multimap is empty.

        Returns:
            True if the multimap has no entries.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        return self.size() == 0

    def key_set(self) -> Set[Any]:
        """Get all keys in the multimap.

        Returns:
            A set of all keys.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        return set(k for k, v in self._data.items() if v)

    def values(self) -> List[Any]:
        """Get all values in the multimap.

        Returns:
            A list of all values.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()
        result = []
        for values in self._data.values():
            result.extend(values)
        return result
