"""CP Subsystem distributed Map."""

from concurrent.futures import Future
from typing import Any, Optional, TYPE_CHECKING

from hazelcast.proxy.base import Proxy, ProxyContext
from hazelcast.protocol.codec import CPMapCodec

if TYPE_CHECKING:
    from hazelcast.protocol.client_message import ClientMessage


class CPMap(Proxy):
    """A distributed Map with strong consistency using CP Subsystem.

    CPMap provides linearizable operations on key-value pairs using the
    CP Subsystem's Raft consensus algorithm. Unlike the standard IMap,
    CPMap guarantees strong consistency at the cost of reduced throughput.

    All operations are atomic and thread-safe. The map supports
    compare-and-set operations for lock-free algorithms.

    Example:
        >>> cp_map = client.get_cp_map("my-cp-map")
        >>> cp_map.put("key", "value")
        >>> value = cp_map.get("key")
        >>> cp_map.compare_and_set("key", "value", "new_value")
    """

    def __init__(
        self,
        service_name: str,
        name: str,
        context: Optional[ProxyContext] = None,
    ):
        super().__init__(service_name, name, context)
        self._group_id = self._parse_group_id(name)

    def _parse_group_id(self, name: str) -> str:
        """Parse CP group ID from name."""
        if "@" in name:
            return name.split("@")[1]
        return "default"

    def _get_object_name(self) -> str:
        """Get the object name without group suffix."""
        if "@" in self._name:
            return self._name.split("@")[0]
        return self._name

    def get(self, key: Any) -> Any:
        """Get the value for a key.

        Args:
            key: The key to look up.

        Returns:
            The value associated with the key, or None if not found.
        """
        return self.get_async(key).result()

    def get_async(self, key: Any) -> Future:
        """Get the value for a key asynchronously.

        Args:
            key: The key to look up.

        Returns:
            A Future that will contain the value or None.
        """
        key_data = self._to_data(key)
        request = CPMapCodec.encode_get_request(
            self._group_id, self._get_object_name(), key_data
        )

        def decode_response(msg: "ClientMessage") -> Any:
            data = CPMapCodec.decode_get_response(msg)
            return self._to_object(data) if data else None

        return self._invoke(request, decode_response)

    def put(self, key: Any, value: Any) -> Any:
        """Put a key-value pair and return the old value.

        Args:
            key: The key to store.
            value: The value to associate with the key.

        Returns:
            The previous value associated with the key, or None.
        """
        return self.put_async(key, value).result()

    def put_async(self, key: Any, value: Any) -> Future:
        """Put a key-value pair asynchronously.

        Args:
            key: The key to store.
            value: The value to associate with the key.

        Returns:
            A Future that will contain the previous value or None.
        """
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        request = CPMapCodec.encode_put_request(
            self._group_id, self._get_object_name(), key_data, value_data
        )

        def decode_response(msg: "ClientMessage") -> Any:
            data = CPMapCodec.decode_put_response(msg)
            return self._to_object(data) if data else None

        return self._invoke(request, decode_response)

    def set(self, key: Any, value: Any) -> None:
        """Set a key-value pair without returning the old value.

        Args:
            key: The key to store.
            value: The value to associate with the key.
        """
        self.set_async(key, value).result()

    def set_async(self, key: Any, value: Any) -> Future:
        """Set a key-value pair asynchronously.

        Args:
            key: The key to store.
            value: The value to associate with the key.

        Returns:
            A Future that completes when the operation is done.
        """
        key_data = self._to_data(key)
        value_data = self._to_data(value)
        request = CPMapCodec.encode_set_request(
            self._group_id, self._get_object_name(), key_data, value_data
        )
        return self._invoke(request)

    def remove(self, key: Any) -> Any:
        """Remove a key and return its value.

        Args:
            key: The key to remove.

        Returns:
            The value that was associated with the key, or None.
        """
        return self.remove_async(key).result()

    def remove_async(self, key: Any) -> Future:
        """Remove a key asynchronously.

        Args:
            key: The key to remove.

        Returns:
            A Future that will contain the removed value or None.
        """
        key_data = self._to_data(key)
        request = CPMapCodec.encode_remove_request(
            self._group_id, self._get_object_name(), key_data
        )

        def decode_response(msg: "ClientMessage") -> Any:
            data = CPMapCodec.decode_remove_response(msg)
            return self._to_object(data) if data else None

        return self._invoke(request, decode_response)

    def delete(self, key: Any) -> None:
        """Delete a key without returning its value.

        Args:
            key: The key to delete.
        """
        self.delete_async(key).result()

    def delete_async(self, key: Any) -> Future:
        """Delete a key asynchronously.

        Args:
            key: The key to delete.

        Returns:
            A Future that completes when the operation is done.
        """
        key_data = self._to_data(key)
        request = CPMapCodec.encode_delete_request(
            self._group_id, self._get_object_name(), key_data
        )
        return self._invoke(request)

    def compare_and_set(self, key: Any, expected: Any, update: Any) -> bool:
        """Atomically set the value if current value equals expected.

        Args:
            key: The key to update.
            expected: The expected current value (can be None).
            update: The new value to set if expectation is met (can be None).

        Returns:
            True if the update was successful, False otherwise.
        """
        return self.compare_and_set_async(key, expected, update).result()

    def compare_and_set_async(self, key: Any, expected: Any, update: Any) -> Future:
        """Atomically set the value if current value equals expected.

        Args:
            key: The key to update.
            expected: The expected current value (can be None).
            update: The new value to set if expectation is met (can be None).

        Returns:
            A Future that will contain True if successful.
        """
        key_data = self._to_data(key)
        expected_data = self._to_data(expected) if expected is not None else None
        update_data = self._to_data(update) if update is not None else None
        request = CPMapCodec.encode_compare_and_set_request(
            self._group_id,
            self._get_object_name(),
            key_data,
            expected_data,
            update_data,
        )
        return self._invoke(request, CPMapCodec.decode_compare_and_set_response)
