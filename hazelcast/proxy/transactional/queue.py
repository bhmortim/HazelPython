"""Transactional Queue proxy implementation."""

from collections import deque
from typing import Any, Optional, TYPE_CHECKING

from hazelcast.proxy.transactional.base import TransactionalProxy

if TYPE_CHECKING:
    from hazelcast.transaction import TransactionContext


class TransactionalQueue(TransactionalProxy):
    """Transactional proxy for IQueue operations.

    Provides queue operations that participate in a transaction. Changes
    made through this proxy are isolated until the transaction commits.

    Example:
        >>> with client.new_transaction_context() as ctx:
        ...     txn_queue = ctx.get_queue("my-queue")
        ...     txn_queue.offer("item1")
        ...     item = txn_queue.poll()
    """

    def __init__(self, name: str, transaction_context: "TransactionContext"):
        super().__init__(name, transaction_context)
        self._items: deque = deque()

    def offer(self, item: Any, timeout_millis: int = 0) -> bool:
        """Add an item to the queue within the transaction.

        Args:
            item: The item to add.
            timeout_millis: Timeout in milliseconds (0 for no wait).

        Returns:
            True if the item was added.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()

        if self._has_server_connection():
            from hazelcast.protocol.codec import TransactionalQueueCodec
            item_data = self._to_data(item)
            request = TransactionalQueueCodec.encode_offer_request(
                self._name,
                self._transaction_context._get_txn_id(),
                self._transaction_context._get_thread_id(),
                item_data,
                timeout_millis,
            )
            response = self._invoke(request)
            if response:
                return TransactionalQueueCodec.decode_offer_response(response)

        self._items.append(item)
        return True

    def poll(self, timeout_millis: int = 0) -> Optional[Any]:
        """Remove and return the head of the queue.

        Args:
            timeout_millis: Timeout in milliseconds (0 for no wait).

        Returns:
            The head item, or None if empty.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()

        if self._has_server_connection():
            from hazelcast.protocol.codec import TransactionalQueueCodec
            request = TransactionalQueueCodec.encode_poll_request(
                self._name,
                self._transaction_context._get_txn_id(),
                self._transaction_context._get_thread_id(),
                timeout_millis,
            )
            response = self._invoke(request)
            if response:
                result_data = TransactionalQueueCodec.decode_poll_response(response)
                return self._to_object(result_data)

        if not self._items:
            return None
        return self._items.popleft()

    def peek(self, timeout_millis: int = 0) -> Optional[Any]:
        """Return the head of the queue without removing it.

        Args:
            timeout_millis: Timeout in milliseconds (0 for no wait).

        Returns:
            The head item, or None if empty.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()

        if self._has_server_connection():
            from hazelcast.protocol.codec import TransactionalQueueCodec
            request = TransactionalQueueCodec.encode_peek_request(
                self._name,
                self._transaction_context._get_txn_id(),
                self._transaction_context._get_thread_id(),
                timeout_millis,
            )
            response = self._invoke(request)
            if response:
                result_data = TransactionalQueueCodec.decode_peek_response(response)
                return self._to_object(result_data)

        if not self._items:
            return None
        return self._items[0]

    def take(self) -> Optional[Any]:
        """Remove and return the head of the queue, blocking if necessary.

        Returns:
            The head item, or None if empty.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()

        if self._has_server_connection():
            from hazelcast.protocol.codec import TransactionalQueueCodec
            request = TransactionalQueueCodec.encode_take_request(
                self._name,
                self._transaction_context._get_txn_id(),
                self._transaction_context._get_thread_id(),
            )
            response = self._invoke(request)
            if response:
                result_data = TransactionalQueueCodec.decode_take_response(response)
                return self._to_object(result_data)

        if not self._items:
            return None
        return self._items.popleft()

    def size(self) -> int:
        """Get the number of items in the queue.

        Returns:
            The number of items.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        self._check_transaction_active()

        if self._has_server_connection():
            from hazelcast.protocol.codec import TransactionalQueueCodec
            request = TransactionalQueueCodec.encode_size_request(
                self._name,
                self._transaction_context._get_txn_id(),
                self._transaction_context._get_thread_id(),
            )
            response = self._invoke(request)
            if response:
                return TransactionalQueueCodec.decode_size_response(response)

        return len(self._items)

    def is_empty(self) -> bool:
        """Check if the queue is empty.

        Returns:
            True if the queue has no items.

        Raises:
            TransactionNotActiveException: If transaction is not active.
        """
        return self.size() == 0
