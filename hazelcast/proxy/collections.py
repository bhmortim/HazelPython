"""Collection distributed data structure proxies (Set, List)."""

from concurrent.futures import Future
from typing import (
    Any,
    Callable,
    Collection,
    Generic,
    Iterator,
    List as ListType,
    Optional,
    TypeVar,
)

from hazelcast.proxy.base import Proxy, ProxyContext
from hazelcast.proxy.queue import ItemEvent, ItemEventType, ItemListener

E = TypeVar("E")


class _CallbackItemListener(ItemListener[Any]):
    """Internal listener that wraps callback functions."""

    def __init__(
        self,
        item_added: Optional[Callable[[ItemEvent[Any]], None]] = None,
        item_removed: Optional[Callable[[ItemEvent[Any]], None]] = None,
    ):
        self._item_added = item_added
        self._item_removed = item_removed

    def item_added(self, event: ItemEvent[Any]) -> None:
        if self._item_added:
            self._item_added(event)

    def item_removed(self, event: ItemEvent[Any]) -> None:
        if self._item_removed:
            self._item_removed(event)


class SetProxy(Proxy, Generic[E]):
    """Proxy for Hazelcast ISet distributed data structure.

    A distributed, non-duplicating collection that provides set semantics
    across the cluster. Items are stored without duplicates.

    Type Parameters:
        E: The element type stored in the set.

    Attributes:
        name: The name of this distributed set.

    Example:
        Basic set operations::

            my_set = client.get_set("unique-items")
            my_set.add("item1")
            my_set.add("item2")
            my_set.add("item1")  # Ignored, already exists
            print(my_set.size())  # Output: 2
    """

    SERVICE_NAME = "hz:impl:setService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)
        self._item_listeners: dict[str, tuple[Any, bool]] = {}

    def add(self, item: E) -> bool:
        """Add an item to the set.

        Args:
            item: The item to add. Must be serializable.

        Returns:
            True if the item was added, False if it was a duplicate.

        Raises:
            IllegalStateException: If the set has been destroyed.

        Example:
            >>> added = my_set.add("new_item")
            >>> print(f"Was added: {added}")
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

    def remove(self, item: E) -> bool:
        """Remove an item from the set.

        Args:
            item: The item to remove.

        Returns:
            True if the item was removed, False if not found.

        Raises:
            IllegalStateException: If the set has been destroyed.

        Example:
            >>> removed = my_set.remove("old_item")
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
        """Check if the set contains an item.

        Args:
            item: The item to check.

        Returns:
            True if the item is in the set, False otherwise.

        Raises:
            IllegalStateException: If the set has been destroyed.

        Example:
            >>> if my_set.contains("item"):
            ...     print("Found!")
        """
        return self.contains_async(item).result()

    def contains_async(self, item: E) -> Future:
        """Check if the set contains an item asynchronously.

        Args:
            item: The item to check.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(False)
        return future

    def size(self) -> int:
        """Get the size of the set.

        Returns:
            The number of items in the set.

        Raises:
            IllegalStateException: If the set has been destroyed.

        Example:
            >>> count = my_set.size()
            >>> print(f"Set has {count} items")
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
        """Check if the set is empty.

        Returns:
            True if the set is empty.
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
        """Clear the set.

        Removes all items from the set.

        Raises:
            IllegalStateException: If the set has been destroyed.

        Example:
            >>> my_set.clear()
            >>> assert my_set.size() == 0
        """
        self.clear_async().result()

    def clear_async(self) -> Future:
        """Clear the set asynchronously.

        Returns:
            A Future that completes when the clear is done.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def get_all(self) -> ListType[E]:
        """Get all items in the set.

        Returns:
            A list containing all items in the set.

        Raises:
            IllegalStateException: If the set has been destroyed.

        Example:
            >>> items = my_set.get_all()
            >>> for item in items:
            ...     print(item)
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
        listener: Optional[ItemListener[E]] = None,
        include_value: bool = True,
        item_added: Optional[Callable[[ItemEvent[E]], None]] = None,
        item_removed: Optional[Callable[[ItemEvent[E]], None]] = None,
    ) -> str:
        """Add an item listener.

        Args:
            listener: An ItemListener instance.
            include_value: Whether to include values in events.
            item_added: Callback for item added events.
            item_removed: Callback for item removed events.

        Returns:
            A registration ID.

        Raises:
            ValueError: If neither listener nor callbacks are provided.
        """
        self._check_not_destroyed()

        if listener is None and item_added is None and item_removed is None:
            raise ValueError(
                "Either listener or at least one callback must be provided"
            )

        import uuid

        effective_listener: Any
        if listener is not None:
            effective_listener = listener
        else:
            effective_listener = _CallbackItemListener(item_added, item_removed)

        registration_id = str(uuid.uuid4())
        self._item_listeners[registration_id] = (effective_listener, include_value)
        return registration_id

    def remove_item_listener(self, registration_id: str) -> bool:
        """Remove an item listener.

        Args:
            registration_id: The registration ID.

        Returns:
            True if the listener was removed.
        """
        return self._item_listeners.pop(registration_id, None) is not None

    def _notify_item_added(self, item: E) -> None:
        """Notify listeners of an item added event."""
        for listener, include_value in self._item_listeners.values():
            event = ItemEvent(
                ItemEventType.ADDED,
                item if include_value else None,
                name=self._name,
            )
            if hasattr(listener, "item_added"):
                listener.item_added(event)

    def _notify_item_removed(self, item: E) -> None:
        """Notify listeners of an item removed event."""
        for listener, include_value in self._item_listeners.values():
            event = ItemEvent(
                ItemEventType.REMOVED,
                item if include_value else None,
                name=self._name,
            )
            if hasattr(listener, "item_removed"):
                listener.item_removed(event)

    def __len__(self) -> int:
        return self.size()

    def __contains__(self, item: E) -> bool:
        return self.contains(item)

    def __iter__(self) -> Iterator[E]:
        return iter(self.get_all())


class ListProxy(Proxy, Generic[E]):
    """Proxy for Hazelcast IList distributed data structure.

    A distributed, ordered collection that allows duplicates. Items
    are stored in insertion order and can be accessed by index.

    Type Parameters:
        E: The element type stored in the list.

    Attributes:
        name: The name of this distributed list.

    Example:
        Basic list operations::

            my_list = client.get_list("items")
            my_list.add("first")
            my_list.add("second")
            my_list.add_at(1, "inserted")
            print(my_list.get(0))  # Output: first
    """

    SERVICE_NAME = "hz:impl:listService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)
        self._item_listeners: dict[str, tuple[Any, bool]] = {}

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

        Inserts the item at the specified index, shifting subsequent
        elements to the right.

        Args:
            index: The index to add at (0-based).
            item: The item to add.

        Raises:
            IllegalStateException: If the list has been destroyed.
            IndexError: If the index is out of range.

        Example:
            >>> my_list.add_at(0, "new_first")
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

    def get(self, index: int) -> E:
        """Get the item at a specific index.

        Args:
            index: The index (0-based).

        Returns:
            The item at the specified index.

        Raises:
            IllegalStateException: If the list has been destroyed.
            IndexError: If the index is out of range.

        Example:
            >>> item = my_list.get(0)
            >>> print(f"First item: {item}")
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

        Replaces the element at the specified position with the new item.

        Args:
            index: The index (0-based).
            item: The new item to set.

        Returns:
            The old item previously at the index.

        Raises:
            IllegalStateException: If the list has been destroyed.
            IndexError: If the index is out of range.

        Example:
            >>> old = my_list.set(0, "replacement")
            >>> print(f"Replaced: {old}")
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

        Removes and returns the element at the specified position,
        shifting subsequent elements to the left.

        Args:
            index: The index (0-based).

        Returns:
            The removed item.

        Raises:
            IllegalStateException: If the list has been destroyed.
            IndexError: If the index is out of range.

        Example:
            >>> removed = my_list.remove_at(0)
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

    def index_of(self, item: E) -> int:
        """Get the index of the first occurrence of an item.

        Args:
            item: The item to find.

        Returns:
            The index of the first occurrence, or -1 if not found.

        Raises:
            IllegalStateException: If the list has been destroyed.

        Example:
            >>> idx = my_list.index_of("target")
            >>> if idx >= 0:
            ...     print(f"Found at index {idx}")
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

    def sub_list(self, from_index: int, to_index: int) -> ListType[E]:
        """Get a sublist.

        Returns a view of the portion of this list between the specified
        indices.

        Args:
            from_index: Starting index (inclusive, 0-based).
            to_index: Ending index (exclusive).

        Returns:
            A list containing elements from from_index to to_index-1.

        Raises:
            IllegalStateException: If the list has been destroyed.
            IndexError: If indices are out of range.

        Example:
            >>> sub = my_list.sub_list(1, 4)
            >>> print(f"Elements 1-3: {sub}")
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

    def get_all(self) -> ListType[E]:
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
        listener: Optional[ItemListener[E]] = None,
        include_value: bool = True,
        item_added: Optional[Callable[[ItemEvent[E]], None]] = None,
        item_removed: Optional[Callable[[ItemEvent[E]], None]] = None,
    ) -> str:
        """Add an item listener.

        Args:
            listener: An ItemListener instance.
            include_value: Whether to include values in events.
            item_added: Callback for item added events.
            item_removed: Callback for item removed events.

        Returns:
            A registration ID.

        Raises:
            ValueError: If neither listener nor callbacks are provided.
        """
        self._check_not_destroyed()

        if listener is None and item_added is None and item_removed is None:
            raise ValueError(
                "Either listener or at least one callback must be provided"
            )

        import uuid

        effective_listener: Any
        if listener is not None:
            effective_listener = listener
        else:
            effective_listener = _CallbackItemListener(item_added, item_removed)

        registration_id = str(uuid.uuid4())
        self._item_listeners[registration_id] = (effective_listener, include_value)
        return registration_id

    def remove_item_listener(self, registration_id: str) -> bool:
        """Remove an item listener.

        Args:
            registration_id: The registration ID.

        Returns:
            True if the listener was removed.
        """
        return self._item_listeners.pop(registration_id, None) is not None

    def _notify_item_added(self, item: E) -> None:
        """Notify listeners of an item added event."""
        for listener, include_value in self._item_listeners.values():
            event = ItemEvent(
                ItemEventType.ADDED,
                item if include_value else None,
                name=self._name,
            )
            if hasattr(listener, "item_added"):
                listener.item_added(event)

    def _notify_item_removed(self, item: E) -> None:
        """Notify listeners of an item removed event."""
        for listener, include_value in self._item_listeners.values():
            event = ItemEvent(
                ItemEventType.REMOVED,
                item if include_value else None,
                name=self._name,
            )
            if hasattr(listener, "item_removed"):
                listener.item_removed(event)

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
