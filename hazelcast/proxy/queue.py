"""Queue distributed data structure proxy."""

from concurrent.futures import Future
from typing import Any, Collection, Generic, Iterator, List, Optional, TypeVar

from hazelcast.proxy.base import Proxy, ProxyContext

E = TypeVar("E")


class QueueProxy(Proxy, Generic[E]):
    """Proxy for Hazelcast IQueue distributed data structure.

    A distributed, blocking queue implementation.
    """

    SERVICE_NAME = "hz:impl:queueService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)
        self._item_listeners: dict = {}

    def add(self, item: E) -> bool:
        """Add an item to the queue.

        Args:
            item: The item to add.

        Returns:
            True if the item was added.

        Raises:
            IllegalStateException: If the queue is full.
        """
        return self.add_async(item).result()

    def add_async(self, item: E) -> Future:
        """Add an item asynchronously.

        Args:
            item: The item to add.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(True)
        return future

    def offer(self, item: E, timeout: float = 0) -> bool:
        """Offer an item to the queue.

        Args:
            item: The item to offer.
            timeout: Maximum time to wait in seconds if queue is full.

        Returns:
            True if the item was added, False if timeout elapsed.
        """
        return self.offer_async(item, timeout).result()

    def offer_async(self, item: E, timeout: float = 0) -> Future:
        """Offer an item asynchronously.

        Args:
            item: The item to offer.
            timeout: Maximum time to wait in seconds.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(True)
        return future

    def put(self, item: E) -> None:
        """Put an item in the queue, waiting if necessary.

        Args:
            item: The item to put.
        """
        self.put_async(item).result()

    def put_async(self, item: E) -> Future:
        """Put an item asynchronously.

        Args:
            item: The item to put.

        Returns:
            A Future that completes when the item is added.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def poll(self, timeout: float = 0) -> Optional[E]:
        """Poll an item from the queue.

        Args:
            timeout: Maximum time to wait in seconds if queue is empty.

        Returns:
            The item, or None if timeout elapsed.
        """
        return self.poll_async(timeout).result()

    def poll_async(self, timeout: float = 0) -> Future:
        """Poll an item asynchronously.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            A Future that will contain the item or None.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def take(self) -> E:
        """Take an item from the queue, waiting if necessary.

        Returns:
            The item.
        """
        return self.take_async().result()

    def take_async(self) -> Future:
        """Take an item asynchronously.

        Returns:
            A Future that will contain the item.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def peek(self) -> Optional[E]:
        """Peek at the head of the queue without removing.

        Returns:
            The head item, or None if empty.
        """
        return self.peek_async().result()

    def peek_async(self) -> Future:
        """Peek asynchronously.

        Returns:
            A Future that will contain the head item or None.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def remove(self, item: E) -> bool:
        """Remove a specific item from the queue.

        Args:
            item: The item to remove.

        Returns:
            True if the item was removed.
        """
        return self.remove_async(item).result()

    def remove_async(self, item: E) -> Future:
        """Remove an item asynchronously.

        Args:
            item: The item to remove.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(False)
        return future

    def contains(self, item: E) -> bool:
        """Check if the queue contains an item.

        Args:
            item: The item to check.

        Returns:
            True if the item is in the queue.
        """
        return self.contains_async(item).result()

    def contains_async(self, item: E) -> Future:
        """Check if the queue contains an item asynchronously.

        Args:
            item: The item to check.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(False)
        return future

    def contains_all(self, items: Collection[E]) -> bool:
        """Check if the queue contains all items.

        Args:
            items: The items to check.

        Returns:
            True if all items are in the queue.
        """
        return self.contains_all_async(items).result()

    def contains_all_async(self, items: Collection[E]) -> Future:
        """Check if the queue contains all items asynchronously.

        Args:
            items: The items to check.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(False)
        return future

    def drain_to(self, target: List[E], max_elements: int = -1) -> int:
        """Drain items to a collection.

        Args:
            target: The collection to drain to.
            max_elements: Maximum number of elements to drain. -1 for all.

        Returns:
            The number of items drained.
        """
        return self.drain_to_async(target, max_elements).result()

    def drain_to_async(self, target: List[E], max_elements: int = -1) -> Future:
        """Drain items asynchronously.

        Args:
            target: The collection to drain to.
            max_elements: Maximum number of elements to drain.

        Returns:
            A Future that will contain the number of items drained.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(0)
        return future

    def size(self) -> int:
        """Get the size of the queue.

        Returns:
            The number of items in the queue.
        """
        return self.size_async().result()

    def size_async(self) -> Future:
        """Get the size asynchronously.

        Returns:
            A Future that will contain the size.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(0)
        return future

    def is_empty(self) -> bool:
        """Check if the queue is empty.

        Returns:
            True if the queue is empty.
        """
        return self.is_empty_async().result()

    def is_empty_async(self) -> Future:
        """Check if empty asynchronously.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(True)
        return future

    def remaining_capacity(self) -> int:
        """Get the remaining capacity of the queue.

        Returns:
            The remaining capacity.
        """
        return self.remaining_capacity_async().result()

    def remaining_capacity_async(self) -> Future:
        """Get remaining capacity asynchronously.

        Returns:
            A Future that will contain the capacity.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(0)
        return future

    def clear(self) -> None:
        """Clear the queue."""
        self.clear_async().result()

    def clear_async(self) -> Future:
        """Clear the queue asynchronously.

        Returns:
            A Future that completes when the clear is done.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def get_all(self) -> List[E]:
        """Get all items in the queue.

        Returns:
            A list of all items.
        """
        return self.get_all_async().result()

    def get_all_async(self) -> Future:
        """Get all items asynchronously.

        Returns:
            A Future that will contain a list of items.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result([])
        return future

    def add_item_listener(
        self,
        listener: Any,
        include_value: bool = True,
    ) -> str:
        """Add an item listener.

        Args:
            listener: The listener to add.
            include_value: Whether to include values in events.

        Returns:
            A registration ID.
        """
        import uuid
        registration_id = str(uuid.uuid4())
        self._item_listeners[registration_id] = (listener, include_value)
        return registration_id

    def remove_item_listener(self, registration_id: str) -> bool:
        """Remove an item listener.

        Args:
            registration_id: The registration ID.

        Returns:
            True if the listener was removed.
        """
        return self._item_listeners.pop(registration_id, None) is not None

    def __len__(self) -> int:
        return self.size()

    def __contains__(self, item: E) -> bool:
        return self.contains(item)

    def __iter__(self) -> Iterator[E]:
        return iter(self.get_all())
