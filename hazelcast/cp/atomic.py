"""CP Subsystem atomic data structures."""

import threading
from concurrent.futures import Future
from typing import Any, Optional, TYPE_CHECKING

from hazelcast.proxy.base import Proxy, ProxyContext
from hazelcast.protocol.codec import (
    AtomicLongCodec,
    AtomicReferenceCodec,
)

if TYPE_CHECKING:
    from hazelcast.protocol.client_message import ClientMessage


class AtomicLong(Proxy):
    """A distributed atomic long counter with strong consistency.

    AtomicLong provides linearizable operations on a 64-bit integer
    using the CP Subsystem's Raft consensus algorithm.

    All operations are atomic and thread-safe. The counter supports
    compare-and-set operations for lock-free algorithms.

    Example:
        >>> counter = client.get_atomic_long("my-counter")
        >>> counter.set(0)
        >>> counter.increment_and_get()  # Returns 1
        >>> counter.compare_and_set(1, 10)  # Returns True
        >>> counter.get()  # Returns 10
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

    def get(self) -> int:
        """Get the current value.

        Returns:
            The current value of the counter.
        """
        return self.get_async().result()

    def get_async(self) -> Future:
        """Get the current value asynchronously.

        Returns:
            A Future that will contain the current value.
        """
        request = AtomicLongCodec.encode_get_request(
            self._group_id, self._get_object_name()
        )
        return self._invoke(request, AtomicLongCodec.decode_get_response)

    def set(self, value: int) -> None:
        """Set the value.

        Args:
            value: The new value to set.
        """
        self.set_async(value).result()

    def set_async(self, value: int) -> Future:
        """Set the value asynchronously.

        Args:
            value: The new value to set.

        Returns:
            A Future that completes when the operation is done.
        """
        request = AtomicLongCodec.encode_set_request(
            self._group_id, self._get_object_name(), value
        )
        return self._invoke(request)

    def get_and_set(self, value: int) -> int:
        """Atomically set the value and return the old value.

        Args:
            value: The new value to set.

        Returns:
            The previous value.
        """
        return self.get_and_set_async(value).result()

    def get_and_set_async(self, value: int) -> Future:
        """Atomically set the value and return the old value asynchronously.

        Args:
            value: The new value to set.

        Returns:
            A Future that will contain the previous value.
        """
        request = AtomicLongCodec.encode_get_and_set_request(
            self._group_id, self._get_object_name(), value
        )
        return self._invoke(request, AtomicLongCodec.decode_get_and_set_response)

    def compare_and_set(self, expected: int, update: int) -> bool:
        """Atomically set the value if current value equals expected.

        Args:
            expected: The expected current value.
            update: The new value to set if expectation is met.

        Returns:
            True if the update was successful, False otherwise.
        """
        return self.compare_and_set_async(expected, update).result()

    def compare_and_set_async(self, expected: int, update: int) -> Future:
        """Atomically set the value if current value equals expected.

        Args:
            expected: The expected current value.
            update: The new value to set if expectation is met.

        Returns:
            A Future that will contain True if successful.
        """
        request = AtomicLongCodec.encode_compare_and_set_request(
            self._group_id, self._get_object_name(), expected, update
        )
        return self._invoke(request, AtomicLongCodec.decode_compare_and_set_response)

    def increment_and_get(self) -> int:
        """Atomically increment and return the new value.

        Returns:
            The value after incrementing.
        """
        return self.add_and_get(1)

    def increment_and_get_async(self) -> Future:
        """Atomically increment and return the new value asynchronously.

        Returns:
            A Future that will contain the new value.
        """
        return self.add_and_get_async(1)

    def decrement_and_get(self) -> int:
        """Atomically decrement and return the new value.

        Returns:
            The value after decrementing.
        """
        return self.add_and_get(-1)

    def decrement_and_get_async(self) -> Future:
        """Atomically decrement and return the new value asynchronously.

        Returns:
            A Future that will contain the new value.
        """
        return self.add_and_get_async(-1)

    def get_and_increment(self) -> int:
        """Atomically get the current value and increment.

        Returns:
            The value before incrementing.
        """
        return self.get_and_add(1)

    def get_and_increment_async(self) -> Future:
        """Atomically get the current value and increment asynchronously.

        Returns:
            A Future that will contain the previous value.
        """
        return self.get_and_add_async(1)

    def get_and_decrement(self) -> int:
        """Atomically get the current value and decrement.

        Returns:
            The value before decrementing.
        """
        return self.get_and_add(-1)

    def get_and_decrement_async(self) -> Future:
        """Atomically get the current value and decrement asynchronously.

        Returns:
            A Future that will contain the previous value.
        """
        return self.get_and_add_async(-1)

    def add_and_get(self, delta: int) -> int:
        """Atomically add a delta and return the new value.

        Args:
            delta: The value to add (can be negative).

        Returns:
            The value after adding.
        """
        return self.add_and_get_async(delta).result()

    def add_and_get_async(self, delta: int) -> Future:
        """Atomically add a delta and return the new value asynchronously.

        Args:
            delta: The value to add (can be negative).

        Returns:
            A Future that will contain the new value.
        """
        request = AtomicLongCodec.encode_add_and_get_request(
            self._group_id, self._get_object_name(), delta
        )
        return self._invoke(request, AtomicLongCodec.decode_add_and_get_response)

    def get_and_add(self, delta: int) -> int:
        """Atomically get the current value and add a delta.

        Args:
            delta: The value to add (can be negative).

        Returns:
            The value before adding.
        """
        return self.get_and_add_async(delta).result()

    def get_and_add_async(self, delta: int) -> Future:
        """Atomically get the current value and add a delta asynchronously.

        Args:
            delta: The value to add (can be negative).

        Returns:
            A Future that will contain the previous value.
        """
        request = AtomicLongCodec.encode_get_and_add_request(
            self._group_id, self._get_object_name(), delta
        )
        return self._invoke(request, AtomicLongCodec.decode_get_and_add_response)

    def alter(self, function: Any) -> None:
        """Apply a function to the current value.

        Args:
            function: A serializable function to apply.
        """
        self.alter_async(function).result()

    def alter_async(self, function: Any) -> Future:
        """Apply a function to the current value asynchronously.

        Args:
            function: A serializable function to apply.

        Returns:
            A Future that completes when the operation is done.
        """
        function_data = self._to_data(function)
        request = AtomicLongCodec.encode_alter_request(
            self._group_id, self._get_object_name(), function_data
        )
        return self._invoke(request)

    def alter_and_get(self, function: Any) -> int:
        """Apply a function and return the new value.

        Args:
            function: A serializable function to apply.

        Returns:
            The value after applying the function.
        """
        return self.alter_and_get_async(function).result()

    def alter_and_get_async(self, function: Any) -> Future:
        """Apply a function and return the new value asynchronously.

        Args:
            function: A serializable function to apply.

        Returns:
            A Future that will contain the new value.
        """
        function_data = self._to_data(function)
        request = AtomicLongCodec.encode_alter_and_get_request(
            self._group_id, self._get_object_name(), function_data
        )
        return self._invoke(request, AtomicLongCodec.decode_alter_and_get_response)

    def get_and_alter(self, function: Any) -> int:
        """Get the current value and apply a function.

        Args:
            function: A serializable function to apply.

        Returns:
            The value before applying the function.
        """
        return self.get_and_alter_async(function).result()

    def get_and_alter_async(self, function: Any) -> Future:
        """Get the current value and apply a function asynchronously.

        Args:
            function: A serializable function to apply.

        Returns:
            A Future that will contain the previous value.
        """
        function_data = self._to_data(function)
        request = AtomicLongCodec.encode_get_and_alter_request(
            self._group_id, self._get_object_name(), function_data
        )
        return self._invoke(request, AtomicLongCodec.decode_get_and_alter_response)

    def apply(self, function: Any) -> Any:
        """Apply a function and return its result.

        Args:
            function: A serializable function to apply.

        Returns:
            The result of applying the function.
        """
        return self.apply_async(function).result()

    def apply_async(self, function: Any) -> Future:
        """Apply a function and return its result asynchronously.

        Args:
            function: A serializable function to apply.

        Returns:
            A Future that will contain the function result.
        """
        function_data = self._to_data(function)
        request = AtomicLongCodec.encode_apply_request(
            self._group_id, self._get_object_name(), function_data
        )

        def decode_response(msg: "ClientMessage") -> Any:
            data = AtomicLongCodec.decode_apply_response(msg)
            return self._to_object(data) if data else None

        return self._invoke(request, decode_response)


class AtomicReference(Proxy):
    """A distributed atomic reference with strong consistency.

    AtomicReference provides linearizable operations on an object reference
    using the CP Subsystem's Raft consensus algorithm.

    The stored object must be serializable. All operations are atomic
    and thread-safe.

    Example:
        >>> ref = client.get_atomic_reference("my-ref")
        >>> ref.set({"key": "value"})
        >>> ref.compare_and_set({"key": "value"}, {"key": "new_value"})
        >>> ref.get()  # Returns {"key": "new_value"}
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

    def get(self) -> Any:
        """Get the current value.

        Returns:
            The current value, or None if not set.
        """
        return self.get_async().result()

    def get_async(self) -> Future:
        """Get the current value asynchronously.

        Returns:
            A Future that will contain the current value.
        """
        request = AtomicReferenceCodec.encode_get_request(
            self._group_id, self._get_object_name()
        )

        def decode_response(msg: "ClientMessage") -> Any:
            data = AtomicReferenceCodec.decode_get_response(msg)
            return self._to_object(data) if data else None

        return self._invoke(request, decode_response)

    def set(self, value: Any) -> None:
        """Set the value.

        Args:
            value: The new value to set. Can be None.
        """
        self.set_async(value).result()

    def set_async(self, value: Any) -> Future:
        """Set the value asynchronously.

        Args:
            value: The new value to set. Can be None.

        Returns:
            A Future that completes when the operation is done.
        """
        value_data = self._to_data(value) if value is not None else None
        request = AtomicReferenceCodec.encode_set_request(
            self._group_id, self._get_object_name(), value_data
        )
        return self._invoke(request)

    def get_and_set(self, value: Any) -> Any:
        """Atomically set the value and return the old value.

        Args:
            value: The new value to set.

        Returns:
            The previous value.
        """
        return self.get_and_set_async(value).result()

    def get_and_set_async(self, value: Any) -> Future:
        """Atomically set the value and return the old value asynchronously.

        Args:
            value: The new value to set.

        Returns:
            A Future that will contain the previous value.
        """
        value_data = self._to_data(value) if value is not None else None
        request = AtomicReferenceCodec.encode_get_and_set_request(
            self._group_id, self._get_object_name(), value_data
        )

        def decode_response(msg: "ClientMessage") -> Any:
            data = AtomicReferenceCodec.decode_get_and_set_response(msg)
            return self._to_object(data) if data else None

        return self._invoke(request, decode_response)

    def compare_and_set(self, expected: Any, update: Any) -> bool:
        """Atomically set the value if current value equals expected.

        Comparison is done using serialized form equality.

        Args:
            expected: The expected current value.
            update: The new value to set if expectation is met.

        Returns:
            True if the update was successful, False otherwise.
        """
        return self.compare_and_set_async(expected, update).result()

    def compare_and_set_async(self, expected: Any, update: Any) -> Future:
        """Atomically set the value if current value equals expected.

        Args:
            expected: The expected current value.
            update: The new value to set if expectation is met.

        Returns:
            A Future that will contain True if successful.
        """
        expected_data = self._to_data(expected) if expected is not None else None
        update_data = self._to_data(update) if update is not None else None
        request = AtomicReferenceCodec.encode_compare_and_set_request(
            self._group_id, self._get_object_name(), expected_data, update_data
        )
        return self._invoke(request, AtomicReferenceCodec.decode_compare_and_set_response)

    def is_null(self) -> bool:
        """Check if the current value is None.

        Returns:
            True if the value is None, False otherwise.
        """
        return self.is_null_async().result()

    def is_null_async(self) -> Future:
        """Check if the current value is None asynchronously.

        Returns:
            A Future that will contain True if value is None.
        """
        request = AtomicReferenceCodec.encode_is_null_request(
            self._group_id, self._get_object_name()
        )
        return self._invoke(request, AtomicReferenceCodec.decode_is_null_response)

    def clear(self) -> None:
        """Set the value to None."""
        self.set(None)

    def clear_async(self) -> Future:
        """Set the value to None asynchronously.

        Returns:
            A Future that completes when the operation is done.
        """
        return self.set_async(None)

    def contains(self, value: Any) -> bool:
        """Check if the current value equals the given value.

        Comparison is done using serialized form equality.

        Args:
            value: The value to compare with.

        Returns:
            True if values are equal, False otherwise.
        """
        return self.contains_async(value).result()

    def contains_async(self, value: Any) -> Future:
        """Check if the current value equals the given value asynchronously.

        Args:
            value: The value to compare with.

        Returns:
            A Future that will contain True if values are equal.
        """
        value_data = self._to_data(value) if value is not None else None
        request = AtomicReferenceCodec.encode_contains_request(
            self._group_id, self._get_object_name(), value_data
        )
        return self._invoke(request, AtomicReferenceCodec.decode_contains_response)

    def alter(self, function: Any) -> None:
        """Apply a function to the current value.

        Args:
            function: A serializable function to apply.
        """
        self.alter_async(function).result()

    def alter_async(self, function: Any) -> Future:
        """Apply a function to the current value asynchronously.

        Args:
            function: A serializable function to apply.

        Returns:
            A Future that completes when the operation is done.
        """
        function_data = self._to_data(function)
        request = AtomicReferenceCodec.encode_alter_request(
            self._group_id, self._get_object_name(), function_data
        )
        return self._invoke(request)

    def alter_and_get(self, function: Any) -> Any:
        """Apply a function and return the new value.

        Args:
            function: A serializable function to apply.

        Returns:
            The value after applying the function.
        """
        return self.alter_and_get_async(function).result()

    def alter_and_get_async(self, function: Any) -> Future:
        """Apply a function and return the new value asynchronously.

        Args:
            function: A serializable function to apply.

        Returns:
            A Future that will contain the new value.
        """
        function_data = self._to_data(function)
        request = AtomicReferenceCodec.encode_alter_and_get_request(
            self._group_id, self._get_object_name(), function_data
        )

        def decode_response(msg: "ClientMessage") -> Any:
            data = AtomicReferenceCodec.decode_alter_and_get_response(msg)
            return self._to_object(data) if data else None

        return self._invoke(request, decode_response)

    def get_and_alter(self, function: Any) -> Any:
        """Get the current value and apply a function.

        Args:
            function: A serializable function to apply.

        Returns:
            The value before applying the function.
        """
        return self.get_and_alter_async(function).result()

    def get_and_alter_async(self, function: Any) -> Future:
        """Get the current value and apply a function asynchronously.

        Args:
            function: A serializable function to apply.

        Returns:
            A Future that will contain the previous value.
        """
        function_data = self._to_data(function)
        request = AtomicReferenceCodec.encode_get_and_alter_request(
            self._group_id, self._get_object_name(), function_data
        )

        def decode_response(msg: "ClientMessage") -> Any:
            data = AtomicReferenceCodec.decode_get_and_alter_response(msg)
            return self._to_object(data) if data else None

        return self._invoke(request, decode_response)

    def apply(self, function: Any) -> Any:
        """Apply a function and return its result.

        Args:
            function: A serializable function to apply.

        Returns:
            The result of applying the function.
        """
        return self.apply_async(function).result()

    def apply_async(self, function: Any) -> Future:
        """Apply a function and return its result asynchronously.

        Args:
            function: A serializable function to apply.

        Returns:
            A Future that will contain the function result.
        """
        function_data = self._to_data(function)
        request = AtomicReferenceCodec.encode_apply_request(
            self._group_id, self._get_object_name(), function_data
        )

        def decode_response(msg: "ClientMessage") -> Any:
            data = AtomicReferenceCodec.decode_apply_response(msg)
            return self._to_object(data) if data else None

        return self._invoke(request, decode_response)
