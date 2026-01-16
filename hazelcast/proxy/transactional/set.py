"""Transactional Set proxy implementation."""

from typing import Any, TYPE_CHECKING

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
        ...     txn_set.add("item1")
        ...     txn_set.add("item2")
    """

    def __init__(self, name: str, transaction_context: "TransactionContext"):
        super().__init__(name, transaction_context)
        self._items: set = set()

    def add(self, item: Any) -> bool:
        """Add an item to the set within the transaction.

        Args:
            item: The item to add.

        Returns:
            True if the item was added, False if already present.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()

        if self._has_server_connection():
            from hazelcast.protocol.codec import TransactionalSetCodec
            item_data = self._to_data(item)
            request = TransactionalSetCodec.encode_add_request(
                self._name,
                self._transaction_context._get_txn_id(),
                self._transaction_context._get_thread_id(),
                item_data,
            )
            response = self._invoke(request)
            if response:
                return TransactionalSetCodec.decode_add_response(response)

        if item in self._items:
            return False
        self._items.add(item)
        return True

    def remove(self, item: Any) -> bool:
        """Remove an item from the set within the transaction.

        Args:
            item: The item to remove.

        Returns:
            True if the item was removed, False if not found.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()

        if self._has_server_connection():
            from hazelcast.protocol.codec import TransactionalSetCodec
            item_data = self._to_data(item)
            request = TransactionalSetCodec.encode_remove_request(
                self._name,
                self._transaction_context._get_txn_id(),
                self._transaction_context._get_thread_id(),
                item_data,
            )
            response = self._invoke(request)
            if response:
                return TransactionalSetCodec.decode_remove_response(response)

        if item not in self._items:
            return False
        self._items.discard(item)
        return True

    def contains(self, item: Any) -> bool:
        """Check if the set contains an item.

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
        """Get the number of items in the set.

        Returns:
            The number of items.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()

        if self._has_server_connection():
            from hazelcast.protocol.codec import TransactionalSetCodec
            request = TransactionalSetCodec.encode_size_request(
                self._name,
                self._transaction_context._get_txn_id(),
                self._transaction_context._get_thread_id(),
            )
            response = self._invoke(request)
            if response:
                return TransactionalSetCodec.decode_size_response(response)

        return len(self._items)

    def is_empty(self) -> bool:
        """Check if the set is empty.

        Returns:
            True if the set has no items.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        return self.size() == 0
