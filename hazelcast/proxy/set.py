"""Set distributed data structure proxy."""

import uuid as uuid_module
from concurrent.futures import Future
from typing import Any, Collection, Generic, Iterator, List, Optional, TypeVar, TYPE_CHECKING

from hazelcast.protocol.codec import SetCodec
from hazelcast.proxy.base import Proxy, ProxyContext

if TYPE_CHECKING:
    from hazelcast.protocol.client_message import ClientMessage

E = TypeVar("E")


class SetProxy(Proxy, Generic[E]):
    """Proxy for Hazelcast ISet distributed data structure.

    A distributed, non-duplicating collection.
    """

    SERVICE_NAME = "hz:impl:setService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)
        self._item_listeners: dict[str, tuple[Any, bool, Optional[uuid_module.UUID]]] = {}

    def add(self, item: E) -> bool:
        """Add an item to the set.

        Args:
            item: The item to add.

        Returns:
            True if the item was added (not a duplicate).
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
        request = SetCodec.encode_add_request(self._name, item_data)

        def handle_response(response: "ClientMessage") -> bool:
            return SetCodec.decode_add_response(response)

        return self._invoke(request, handle_response)

    def add_all(self, items: Collection[E]) -> bool:
        """Add multiple items to the set.

        Args:
            items: The items to add.

        Returns:
            True if any items were added.
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
        request = SetCodec.encode_add_all_request(self._name, items_data)

        def handle_response(response: "ClientMessage") -> bool:
            return SetCodec.decode_add_all_response(response)

        return self._invoke(request, handle_response)

    def remove(self, item: E) -> bool:
        """Remove an item from the set.

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
        request = SetCodec.encode_remove_request(self._name, item_data)

        def handle_response(response: "ClientMessage") -> bool:
            return SetCodec.decode_remove_response(response)

        return self._invoke(request, handle_response)

    def remove_all(self, items: Collection[E]) -> bool:
        """Remove multiple items from the set.

        Args:
            items: The items to remove.

        Returns:
            True if any items were removed.
        """
        return self.remove_all_async(items).result()

    def remove_all_async(self, items: Collection[E]) -> Future:
        """Remove multiple items asynchronously.

        Args:
            items: The items to remove.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        items_data = [self._to_data(item) for item in items]
        request = SetCodec.encode_remove_all_request(self._name, items_data)

        def handle_response(response: "ClientMessage") -> bool:
            return SetCodec.decode_remove_all_response(response)

        return self._invoke(request, handle_response)

    def contains(self, item: E) -> bool:
        """Check if the set contains an item.

        Args:
            item: The item to check.

        Returns:
            True if the item is in the set.
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
        item_data = self._to_data(item)
        request = SetCodec.encode_contains_request(self._name, item_data)

        def handle_response(response: "ClientMessage") -> bool:
            return SetCodec.decode_contains_response(response)

        return self._invoke(request, handle_response)

    def contains_all(self, items: Collection[E]) -> bool:
        """Check if the set contains all items.

        Args:
            items: The items to check.

        Returns:
            True if all items are in the set.
        """
        return self.contains_all_async(items).result()

    def contains_all_async(self, items: Collection[E]) -> Future:
        """Check if the set contains all items asynchronously.

        Args:
            items: The items to check.

        Returns:
            A Future that will contain a boolean result.
        """
        self._check_not_destroyed()
        items_data = [self._to_data(item) for item in items]
        request = SetCodec.encode_contains_all_request(self._name, items_data)

        def handle_response(response: "ClientMessage") -> bool:
            return SetCodec.decode_contains_all_response(response)

        return self._invoke(request, handle_response)

    def retain_all(self, items: Collection[E]) -> bool:
        """Retain only items that are in the given collection.

        Args:
            items: The items to retain.

        Returns:
            True if the set was modified.
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
        request = SetCodec.encode_retain_all_request(self._name, items_data)

        def handle_response(response: "ClientMessage") -> bool:
            return SetCodec.decode_retain_all_response(response)

        return self._invoke(request, handle_response)

    def size(self) -> int:
        """Get the size of the set.

        Returns:
            The number of items in the set.
        """
        return self.size_async().result()

    def size_async(self) -> Future:
        """Get the size asynchronously.

        Returns:
            A Future that will contain the size.
        """
        self._check_not_destroyed()
        request = SetCodec.encode_size_request(self._name)

        def handle_response(response: "ClientMessage") -> int:
            return SetCodec.decode_size_response(response)

        return self._invoke(request, handle_response)

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
        request = SetCodec.encode_is_empty_request(self._name)

        def handle_response(response: "ClientMessage") -> bool:
            return SetCodec.decode_is_empty_response(response)

        return self._invoke(request, handle_response)

    def clear(self) -> None:
        """Clear the set."""
        self.clear_async().result()

    def clear_async(self) -> Future:
        """Clear the set asynchronously.

        Returns:
            A Future that completes when the clear is done.
        """
        self._check_not_destroyed()
        request = SetCodec.encode_clear_request(self._name)
        return self._invoke(request)

    def get_all(self) -> List[E]:
        """Get all items in the set.

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
        request = SetCodec.encode_get_all_request(self._name)

        def handle_response(response: "ClientMessage") -> List[E]:
            items_data = SetCodec.decode_get_all_response(response)
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
        request = SetCodec.encode_add_item_listener_request(
            self._name, include_value, False
        )

        def handle_response(response: "ClientMessage") -> str:
            server_id = SetCodec.decode_add_item_listener_response(response)
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

        request = SetCodec.encode_remove_item_listener_request(self._name, server_id)

        def handle_response(response: "ClientMessage") -> bool:
            return SetCodec.decode_remove_item_listener_response(response)

        future = self._invoke(request, handle_response)
        return future.result()

    def __len__(self) -> int:
        return self.size()

    def __contains__(self, item: E) -> bool:
        return self.contains(item)

    def __iter__(self) -> Iterator[E]:
        return iter(self.get_all())
