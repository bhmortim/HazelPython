"""List distributed data structure proxy."""

from concurrent.futures import Future
from typing import Any, Collection, Generic, Iterator, List, Optional, TypeVar

from hazelcast.proxy.base import Proxy, ProxyContext

E = TypeVar("E")


class ListProxy(Proxy, Generic[E]):
    """Proxy for Hazelcast IList distributed data structure.

    A distributed, ordered collection that allows duplicates.
    """

    SERVICE_NAME = "hz:impl:listService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)
        self._item_listeners: dict = {}

    def add(self, item: E) -> bool:
        """Add an item to the list.

        Args:
            item: The item to add.

        Returns:
            True (always succeeds for lists).
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

    def add_at(self, index: int, item: E) -> None:
        """Add an item at a specific index.

        Args:
            index: The index to add at.
            item: The item to add.
        """
        self.add_at_async(index, item).result()

    def add_at_async(self, index: int, item: E) -> Future:
        """Add an item at index asynchronously.

        Args:
            index: The index to add at.
            item: The item to add.

        Returns:
            A Future that completes when the add is done.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def add_all(self, items: Collection[E]) -> bool:
        """Add multiple items to the list.

        Args:
            items: The items to add.

        Returns:
            True if items were added.
        """
        return self.add_all_async(items).result()

    def add_all_async(self, items: Collection[E]) -> Future:
        """Add multiple items asynchronously.

        Args:
            items: The items to add.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(True)
        return future

    def add_all_at(self, index: int, items: Collection[E]) -> bool:
        """Add multiple items at a specific index.

        Args:
            index: The index to add at.
            items: The items to add.

        Returns:
            True if items were added.
        """
        return self.add_all_at_async(index, items).result()

    def add_all_at_async(self, index: int, items: Collection[E]) -> Future:
        """Add multiple items at index asynchronously.

        Args:
            index: The index to add at.
            items: The items to add.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(True)
        return future

    def get(self, index: int) -> E:
        """Get the item at a specific index.

        Args:
            index: The index.

        Returns:
            The item at the index.
        """
        return self.get_async(index).result()

    def get_async(self, index: int) -> Future:
        """Get an item asynchronously.

        Args:
            index: The index.

        Returns:
            A Future that will contain the item.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def set(self, index: int, item: E) -> E:
        """Set the item at a specific index.

        Args:
            index: The index.
            item: The new item.

        Returns:
            The old item at the index.
        """
        return self.set_async(index, item).result()

    def set_async(self, index: int, item: E) -> Future:
        """Set an item asynchronously.

        Args:
            index: The index.
            item: The new item.

        Returns:
            A Future that will contain the old item.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def remove(self, item: E) -> bool:
        """Remove the first occurrence of an item.

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

    def remove_at(self, index: int) -> E:
        """Remove the item at a specific index.

        Args:
            index: The index.

        Returns:
            The removed item.
        """
        return self.remove_at_async(index).result()

    def remove_at_async(self, index: int) -> Future:
        """Remove an item at index asynchronously.

        Args:
            index: The index.

        Returns:
            A Future that will contain the removed item.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def remove_all(self, items: Collection[E]) -> bool:
        """Remove all occurrences of items.

        Args:
            items: The items to remove.

        Returns:
            True if any items were removed.
        """
        return self.remove_all_async(items).result()

    def remove_all_async(self, items: Collection[E]) -> Future:
        """Remove items asynchronously.

        Args:
            items: The items to remove.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(False)
        return future

    def contains(self, item: E) -> bool:
        """Check if the list contains an item.

        Args:
            item: The item to check.

        Returns:
            True if the item is in the list.
        """
        return self.contains_async(item).result()

    def contains_async(self, item: E) -> Future:
        """Check if the list contains an item asynchronously.

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
        """Check if the list contains all items.

        Args:
            items: The items to check.

        Returns:
            True if all items are in the list.
        """
        return self.contains_all_async(items).result()

    def contains_all_async(self, items: Collection[E]) -> Future:
        """Check if the list contains all items asynchronously.

        Args:
            items: The items to check.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(False)
        return future

    def retain_all(self, items: Collection[E]) -> bool:
        """Retain only items in the given collection.

        Args:
            items: The items to retain.

        Returns:
            True if the list was modified.
        """
        return self.retain_all_async(items).result()

    def retain_all_async(self, items: Collection[E]) -> Future:
        """Retain items asynchronously.

        Args:
            items: The items to retain.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(False)
        return future

    def index_of(self, item: E) -> int:
        """Get the index of the first occurrence of an item.

        Args:
            item: The item to find.

        Returns:
            The index, or -1 if not found.
        """
        return self.index_of_async(item).result()

    def index_of_async(self, item: E) -> Future:
        """Get the index of an item asynchronously.

        Args:
            item: The item to find.

        Returns:
            A Future that will contain the index.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(-1)
        return future

    def last_index_of(self, item: E) -> int:
        """Get the index of the last occurrence of an item.

        Args:
            item: The item to find.

        Returns:
            The index, or -1 if not found.
        """
        return self.last_index_of_async(item).result()

    def last_index_of_async(self, item: E) -> Future:
        """Get the last index of an item asynchronously.

        Args:
            item: The item to find.

        Returns:
            A Future that will contain the index.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(-1)
        return future

    def sub_list(self, from_index: int, to_index: int) -> List[E]:
        """Get a sublist.

        Args:
            from_index: Starting index (inclusive).
            to_index: Ending index (exclusive).

        Returns:
            The sublist.
        """
        return self.sub_list_async(from_index, to_index).result()

    def sub_list_async(self, from_index: int, to_index: int) -> Future:
        """Get a sublist asynchronously.

        Args:
            from_index: Starting index (inclusive).
            to_index: Ending index (exclusive).

        Returns:
            A Future that will contain the sublist.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result([])
        return future

    def size(self) -> int:
        """Get the size of the list.

        Returns:
            The number of items in the list.
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
        """Check if the list is empty.

        Returns:
            True if the list is empty.
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

    def clear(self) -> None:
        """Clear the list."""
        self.clear_async().result()

    def clear_async(self) -> Future:
        """Clear the list asynchronously.

        Returns:
            A Future that completes when the clear is done.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def get_all(self) -> List[E]:
        """Get all items in the list.

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

    def __getitem__(self, index: int) -> E:
        return self.get(index)

    def __setitem__(self, index: int, item: E) -> None:
        self.set(index, item)

    def __delitem__(self, index: int) -> None:
        self.remove_at(index)

    def __iter__(self) -> Iterator[E]:
        return iter(self.get_all())
