"""JSON serialization support for Hazelcast.

This module provides support for storing and querying JSON documents
in Hazelcast data structures.

Example:
    Basic usage::

        from hazelcast.serialization.json import HazelcastJsonValue, to_json, from_json

        # Store JSON in a map
        json_value = HazelcastJsonValue('{"name": "John", "age": 30}')
        my_map.put("person", json_value)

        # Or use helper functions
        json_value = to_json({"name": "John", "age": 30})
        my_map.put("person", json_value)

        # Retrieve and parse
        result = my_map.get("person")
        data = from_json(result)
"""

import json as json_module
from typing import Any, Optional

from hazelcast.serialization.api import ObjectDataInput, ObjectDataOutput, Serializer
from hazelcast.exceptions import HazelcastSerializationException


JSON_TYPE_ID = -130


class HazelcastJsonValue:
    """Wrapper class for JSON values stored in Hazelcast.

    This class is used to store and retrieve JSON documents in Hazelcast
    data structures. The JSON is stored as a string and can be queried
    using JSON predicates or SQL.

    Attributes:
        value: The raw JSON string.

    Example:
        From a JSON string::

            value = HazelcastJsonValue('{"name": "John", "age": 30}')
            my_map.put("person", value)

        From a Python object::

            value = HazelcastJsonValue.from_object({"name": "John", "age": 30})
            my_map.put("person", value)

        Converting back to Python::

            json_value = my_map.get("person")
            data = json_value.to_object()
            print(data["name"])  # "John"
    """

    __slots__ = ("_value",)

    def __init__(self, value: str):
        """Initialize with a JSON string.

        Args:
            value: A valid JSON string.

        Raises:
            HazelcastSerializationException: If the value is not a string.
        """
        if not isinstance(value, str):
            raise HazelcastSerializationException(
                f"HazelcastJsonValue requires a string, got {type(value).__name__}"
            )
        self._value = value

    @property
    def value(self) -> str:
        """Get the JSON string value."""
        return self._value

    def to_object(self) -> Any:
        """Parse the JSON string to a Python object.

        Returns:
            The parsed Python object (dict, list, str, int, float, bool, or None).

        Raises:
            HazelcastSerializationException: If parsing fails.
        """
        try:
            return json_module.loads(self._value)
        except json_module.JSONDecodeError as e:
            raise HazelcastSerializationException(f"Failed to parse JSON: {e}")

    @classmethod
    def from_object(cls, obj: Any) -> "HazelcastJsonValue":
        """Create a HazelcastJsonValue from a Python object.

        Args:
            obj: A JSON-serializable Python object.

        Returns:
            A new HazelcastJsonValue instance.

        Raises:
            HazelcastSerializationException: If serialization fails.
        """
        try:
            json_str = json_module.dumps(obj, separators=(",", ":"))
            return cls(json_str)
        except (TypeError, ValueError) as e:
            raise HazelcastSerializationException(f"Failed to serialize to JSON: {e}")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, HazelcastJsonValue):
            return False
        return self._value == other._value

    def __hash__(self) -> int:
        return hash(self._value)

    def __str__(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return f"HazelcastJsonValue({self._value!r})"


class HazelcastJsonValueSerializer(Serializer[HazelcastJsonValue]):
    """Serializer for HazelcastJsonValue objects."""

    @property
    def type_id(self) -> int:
        return JSON_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: HazelcastJsonValue) -> None:
        output.write_string(obj.value)

    def read(self, input: ObjectDataInput) -> HazelcastJsonValue:
        json_str = input.read_string()
        return HazelcastJsonValue(json_str)


def to_json(obj: Any) -> HazelcastJsonValue:
    """Convert a Python object to HazelcastJsonValue.

    This is a convenience function for creating HazelcastJsonValue instances
    from Python objects.

    Args:
        obj: A JSON-serializable Python object (dict, list, str, int, float, bool, None).

    Returns:
        A HazelcastJsonValue containing the JSON representation.

    Raises:
        HazelcastSerializationException: If serialization fails.

    Example:
        >>> value = to_json({"name": "Alice", "scores": [95, 87, 92]})
        >>> my_map.put("student", value)
    """
    return HazelcastJsonValue.from_object(obj)


def from_json(value: Optional[HazelcastJsonValue]) -> Any:
    """Convert a HazelcastJsonValue to a Python object.

    This is a convenience function for parsing HazelcastJsonValue instances
    back to Python objects.

    Args:
        value: A HazelcastJsonValue instance, or None.

    Returns:
        The parsed Python object, or None if input is None.

    Raises:
        HazelcastSerializationException: If parsing fails.

    Example:
        >>> json_value = my_map.get("student")
        >>> data = from_json(json_value)
        >>> print(data["name"])
        Alice
    """
    if value is None:
        return None
    return value.to_object()


class JsonPathPredicate:
    """Predicate for querying JSON fields using path expressions.

    This predicate allows filtering entries based on JSON field values
    using a path expression (similar to JSONPath syntax).

    The path uses dot notation to navigate nested objects:
    - ``$.name`` - top-level field "name"
    - ``$.address.city`` - nested field "city" inside "address"
    - ``$.items[0]`` - first element of array "items"

    Example:
        >>> predicate = JsonPathPredicate("$.address.city", "New York")
        >>> results = my_map.values(predicate)
    """

    __slots__ = ("_path", "_value")

    def __init__(self, path: str, value: Any):
        """Initialize a JSON path predicate.

        Args:
            path: The JSON path expression (e.g., "$.name" or "$.address.city").
            value: The value to match.
        """
        self._path = path
        self._value = value

    @property
    def path(self) -> str:
        """Get the JSON path expression."""
        return self._path

    @property
    def value(self) -> Any:
        """Get the value to match."""
        return self._value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JsonPathPredicate):
            return False
        return self._path == other._path and self._value == other._value

    def __hash__(self) -> int:
        return hash((self._path, self._value))

    def __repr__(self) -> str:
        return f"JsonPathPredicate(path={self._path!r}, value={self._value!r})"


class JsonContainsPredicate:
    """Predicate for checking if JSON contains a specific key.

    This predicate matches entries where the JSON document contains
    the specified key at the top level.

    Example:
        >>> predicate = JsonContainsPredicate("email")
        >>> results = my_map.values(predicate)
    """

    __slots__ = ("_key",)

    def __init__(self, key: str):
        """Initialize a JSON contains predicate.

        Args:
            key: The key to check for existence.
        """
        self._key = key

    @property
    def key(self) -> str:
        """Get the key to check."""
        return self._key

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JsonContainsPredicate):
            return False
        return self._key == other._key

    def __hash__(self) -> int:
        return hash(self._key)

    def __repr__(self) -> str:
        return f"JsonContainsPredicate(key={self._key!r})"


class JsonValuePredicate:
    """Predicate for matching JSON values with comparison operators.

    Supports comparison operations on JSON field values using
    standard SQL-like operators.

    Supported operators:
        - ``=`` - Equal
        - ``!=`` - Not equal
        - ``<`` - Less than
        - ``<=`` - Less than or equal
        - ``>`` - Greater than
        - ``>=`` - Greater than or equal
        - ``LIKE`` - Pattern matching
        - ``IN`` - Value in list

    Example:
        >>> predicate = JsonValuePredicate("$.age", ">", 25)
        >>> results = my_map.values(predicate)

        >>> predicate = JsonValuePredicate("$.status", "IN", ["active", "pending"])
        >>> results = my_map.values(predicate)
    """

    __slots__ = ("_path", "_operator", "_value")

    VALID_OPERATORS = ("=", "!=", "<", "<=", ">", ">=", "LIKE", "IN")

    def __init__(self, path: str, operator: str, value: Any):
        """Initialize a JSON value predicate.

        Args:
            path: The JSON path expression.
            operator: The comparison operator (=, !=, <, <=, >, >=, LIKE, IN).
            value: The value to compare against.

        Raises:
            ValueError: If the operator is not valid.
        """
        operator_upper = operator.upper() if isinstance(operator, str) else operator
        if operator_upper not in self.VALID_OPERATORS:
            raise ValueError(
                f"Invalid operator: {operator}. Must be one of {self.VALID_OPERATORS}"
            )
        self._path = path
        self._operator = operator_upper
        self._value = value

    @property
    def path(self) -> str:
        """Get the JSON path expression."""
        return self._path

    @property
    def operator(self) -> str:
        """Get the comparison operator."""
        return self._operator

    @property
    def value(self) -> Any:
        """Get the value to compare against."""
        return self._value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, JsonValuePredicate):
            return False
        return (
            self._path == other._path
            and self._operator == other._operator
            and self._value == other._value
        )

    def __hash__(self) -> int:
        value_hash = hash(tuple(self._value)) if isinstance(self._value, list) else hash(self._value)
        return hash((self._path, self._operator, value_hash))

    def __repr__(self) -> str:
        return f"JsonValuePredicate(path={self._path!r}, operator={self._operator!r}, value={self._value!r})"
