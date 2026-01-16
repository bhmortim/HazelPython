"""List distributed data structure proxy."""

import uuid as uuid_module
from concurrent.futures import Future
from typing import Any, Collection, Generic, Iterator, List, Optional, TypeVar, TYPE_CHECKING

from hazelcast.protocol.codec import ListCodec
from hazelcast.proxy.base import Proxy, ProxyContext

if TYPE_CHECKING:
    from hazelcast.protocol.client_message import ClientMessage

E = TypeVar("E")


class ListProxy(Proxy, Generic[E]):
    """Proxy for Hazelcast IList distributed data structure.

    A distributed, ordered collection that allows duplicates.
    """

    SERVICE_NAME = "hz:impl:listService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)
        self._item_listeners: dict[str, tuple[Any, bool, Optional[uuid_module.UUID]]] = {}

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
        item_data = self._to_data(item)
        request = ListCodec.encode_add_request(self._name, item_data)

        def handle_response(response: "ClientMessage") -> bool:
            return ListCodec.decode_add_response(response)

        return self._invoke(request, handle_response)

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
        item_data = self._to_data(item)
        request = ListCodec.encode_add_at_request(self._name, index, item_data)
        return self._invoke(request)

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
        items_data = [self._to_data(item) for item in items]
        request = ListCodec.encode_add_all_request(self._name, items_data)

        def handle_response(response: "ClientMessage") -> bool:
            return ListCodec.decode_add_all_response(response)

        return self._invoke(request, handle_response)

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
        items_data = [self._to_data(item) for item in items]
        request = ListCodec.encode_add_all_at_request(self._name, index, items_data)

        def handle_response(response: "ClientMessage") -> bool:
            return ListCodec.decode_add_all_at_response(response)

        return self._invoke(request, handle_response)

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
        request = ListCodec.encode_get_request(self._name, index)

        def handle_response(response: "ClientMessage") -> Optional[E]:
            data = ListCodec.decode_get_response(response)
            return self._to_object(data) if data else None

        return self._invoke(request, handle_response)

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
        item_data = self._to_data(item)
        request = ListCodec.encode_set_request(self._name, index, item_data)

        def handle_response(response: "ClientMessage") -> Optional[E]:
            data = ListCodec.decode_set_response(response)
            return self._to_object(data) if data else None

        return self._invoke(request, handle_response)

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
        item_data = self._to_data(item)
        request = ListCodec.encode_remove_item_request(self._name, item_data)

        def handle_response(response: "ClientMessage") -> bool:
            return ListCodec.decode_remove_item_response(response)

        return self._invoke(request, handle_response)

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
        request = ListCodec.encode_remove_at_request(self._name, index)

        def handle_response(response: "ClientMessage") -> Optional[E]:
            data = ListCodec.decode_remove_at_response(response)
            return self._to_object(data) if data else None

        return self._invoke(request, handle_response)

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
        items_data = [self._to_data(item) for item in items]
        request = ListCodec.encode_remove_all_request(self._name, items_data)

        def handle_response(response: "ClientMessage") -> bool:
            return ListCodec.decode_remove_all_response(response)

        return self._invoke(request, handle_response)

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
        item_data = self._to_data(item)
        request = ListCodec.encode_contains_request(self._name, item_data)

        def handle_response(response: "ClientMessage") -> bool:
            return ListCodec.decode_contains_response(response)

        return self._invoke(request, handle_response)

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
        items_data = [self._to_data(item) for item in items]
        request = ListCodec.encode_contains_all_request(self._name, items_data)

        def handle_response(response: "ClientMessage") -> bool:
            return ListCodec.decode_contains_all_response(response)

        return self._invoke(request, handle_response)

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
        items_data = [self._to_data(item) for item in items]
        request = ListCodec.encode_retain_all_request(self._name, items_data)

        def handle_response(response: "ClientMessage") -> bool:
            return ListCodec.decode_retain_all_response(response)

        return self._invoke(request, handle_response)

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
        item_data = self._to_data(item)
        request = ListCodec.encode_index_of_request(self._name, item_data)

        def handle_response(response: "ClientMessage") -> int:
            return ListCodec.decode_index_of_response(response)

        return self._invoke(request, handle_response)

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
        item_data = self._to_data(item)
        request = ListCodec.encode_last_index_of_request(self._name, item_data)

        def handle_response(response: "ClientMessage") -> int:
            return ListCodec.decode_last_index_of_response(response)

        return self._invoke(request, handle_response)

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
        request = ListCodec.encode_sub_list_request(self._name, from_index, to_index)

        def handle_response(response: "ClientMessage") -> List[E]:
            items_data = ListCodec.decode_sub_list_response(response)
            return [self._to_object(data) for data in items_data]

        return self._invoke(request, handle_response)

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
        request = ListCodec.encode_size_request(self._name)

        def handle_response(response: "ClientMessage") -> int:
            return ListCodec.decode_size_response(response)

        return self._invoke(request, handle_response)

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
        request = ListCodec.encode_is_empty_request(self._name)

        def handle_response(response: "ClientMessage") -> bool:
            return ListCodec.decode_is_empty_response(response)

        return self._invoke(request, handle_response)

    def clear(self) -> None:
        """Clear the list."""
        self.clear_async().result()

    def clear_async(self) -> Future:
        """Clear the list asynchronously.

        Returns:
            A Future that completes when the clear is done.
        """
        self._check_not_destroyed()
        request = ListCodec.encode_clear_request(self._name)
        return self._invoke(request)

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
        request = ListCodec.encode_get_all_request(self._name)

        def handle_response(response: "ClientMessage") -> List[E]:
            items_data = ListCodec.decode_get_all_response(response)
            return [self._to_object(data) for data in items_data]

        return self._invoke(request, handle_response)

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
        self._check_not_destroyed()
        local_id = str(uuid_module.uuid4())
        request = ListCodec.encode_add_item_listener_request(
            self._name, include_value, False
        )

        def handle_response(response: "ClientMessage") -> str:
            server_id = ListCodec.decode_add_item_listener_response(response)
            self._item_listeners[local_id] = (listener, include_value, server_id)
            return local_id

        future = self._invoke(request, handle_response)
        return future.result()

    def remove_item_listener(self, registration_id: str) -> bool:
        """Remove an item listener.

        Args:
            registration_id: The registration ID.

        Returns:
            True if the listener was removed.
        """
        self._check_not_destroyed()

        entry = self._item_listeners.pop(registration_id, None)
        if entry is None:
            return False

        _, _, server_id = entry
        if server_id is None:
            return True

        request = ListCodec.encode_remove_item_listener_request(self._name, server_id)

        def handle_response(response: "ClientMessage") -> bool:
            return ListCodec.decode_remove_item_listener_response(response)

        future = self._invoke(request, handle_response)
        return future.result()

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
