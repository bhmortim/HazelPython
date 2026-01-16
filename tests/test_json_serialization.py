"""Unit tests for hazelcast.serialization.json module."""

import pytest
from unittest.mock import MagicMock

from hazelcast.serialization.json import (
    HazelcastJsonValue,
    HazelcastJsonValueSerializer,
    to_json,
    from_json,
    JsonPathPredicate,
    JsonContainsPredicate,
    JsonValuePredicate,
    JSON_TYPE_ID,
)
from hazelcast.exceptions import HazelcastSerializationException


class TestHazelcastJsonValue:
    """Tests for HazelcastJsonValue class."""

    def test_create_with_string(self):
        value = HazelcastJsonValue('{"name": "test"}')
        assert value.value == '{"name": "test"}'

    def test_create_with_empty_object(self):
        value = HazelcastJsonValue('{}')
        assert value.value == '{}'

    def test_create_with_array(self):
        value = HazelcastJsonValue('[1, 2, 3]')
        assert value.value == '[1, 2, 3]'

    def test_create_with_non_string_raises(self):
        with pytest.raises(HazelcastSerializationException) as exc_info:
            HazelcastJsonValue(123)
        assert "requires a string" in str(exc_info.value)

    def test_create_with_dict_raises(self):
        with pytest.raises(HazelcastSerializationException):
            HazelcastJsonValue({"name": "test"})

    def test_to_object_simple(self):
        value = HazelcastJsonValue('{"name": "Alice", "age": 30}')
        obj = value.to_object()
        assert obj == {"name": "Alice", "age": 30}

    def test_to_object_array(self):
        value = HazelcastJsonValue('[1, 2, 3]')
        obj = value.to_object()
        assert obj == [1, 2, 3]

    def test_to_object_nested(self):
        value = HazelcastJsonValue('{"user": {"name": "Bob"}, "scores": [1, 2]}')
        obj = value.to_object()
        assert obj == {"user": {"name": "Bob"}, "scores": [1, 2]}

    def test_to_object_invalid_json(self):
        value = HazelcastJsonValue('not valid json')
        with pytest.raises(HazelcastSerializationException) as exc_info:
            value.to_object()
        assert "Failed to parse JSON" in str(exc_info.value)

    def test_from_object_dict(self):
        value = HazelcastJsonValue.from_object({"name": "test"})
        assert isinstance(value, HazelcastJsonValue)
        assert value.to_object() == {"name": "test"}

    def test_from_object_list(self):
        value = HazelcastJsonValue.from_object([1, 2, 3])
        assert value.to_object() == [1, 2, 3]

    def test_from_object_string(self):
        value = HazelcastJsonValue.from_object("hello")
        assert value.to_object() == "hello"

    def test_from_object_number(self):
        value = HazelcastJsonValue.from_object(42)
        assert value.to_object() == 42

    def test_from_object_boolean(self):
        value = HazelcastJsonValue.from_object(True)
        assert value.to_object() is True

    def test_from_object_null(self):
        value = HazelcastJsonValue.from_object(None)
        assert value.to_object() is None

    def test_from_object_non_serializable(self):
        with pytest.raises(HazelcastSerializationException) as exc_info:
            HazelcastJsonValue.from_object(object())
        assert "Failed to serialize" in str(exc_info.value)

    def test_equality(self):
        v1 = HazelcastJsonValue('{"a": 1}')
        v2 = HazelcastJsonValue('{"a": 1}')
        v3 = HazelcastJsonValue('{"a": 2}')
        assert v1 == v2
        assert v1 != v3

    def test_equality_with_non_json_value(self):
        v1 = HazelcastJsonValue('{"a": 1}')
        assert v1 != '{"a": 1}'
        assert v1 != {"a": 1}

    def test_hash(self):
        v1 = HazelcastJsonValue('{"a": 1}')
        v2 = HazelcastJsonValue('{"a": 1}')
        assert hash(v1) == hash(v2)
        
        values = {v1, v2}
        assert len(values) == 1

    def test_str(self):
        value = HazelcastJsonValue('{"test": true}')
        assert str(value) == '{"test": true}'

    def test_repr(self):
        value = HazelcastJsonValue('{"x": 1}')
        assert "HazelcastJsonValue" in repr(value)
        assert '{"x": 1}' in repr(value)

    @pytest.mark.parametrize("json_str,expected", [
        ('null', None),
        ('true', True),
        ('false', False),
        ('123', 123),
        ('3.14', 3.14),
        ('"hello"', "hello"),
        ('[]', []),
        ('{}', {}),
    ])
    def test_various_json_types(self, json_str, expected):
        value = HazelcastJsonValue(json_str)
        assert value.to_object() == expected


class TestHazelcastJsonValueSerializer:
    """Tests for HazelcastJsonValueSerializer."""

    @pytest.fixture
    def serializer(self):
        return HazelcastJsonValueSerializer()

    def test_type_id(self, serializer):
        assert serializer.type_id == JSON_TYPE_ID

    def test_write(self, serializer):
        output = MagicMock()
        value = HazelcastJsonValue('{"test": 1}')
        serializer.write(output, value)
        output.write_string.assert_called_once_with('{"test": 1}')

    def test_read(self, serializer):
        input_mock = MagicMock()
        input_mock.read_string.return_value = '{"name": "test"}'
        
        result = serializer.read(input_mock)
        
        assert isinstance(result, HazelcastJsonValue)
        assert result.value == '{"name": "test"}'


class TestToJsonFunction:
    """Tests for to_json convenience function."""

    def test_to_json_dict(self):
        result = to_json({"name": "Alice"})
        assert isinstance(result, HazelcastJsonValue)
        assert result.to_object() == {"name": "Alice"}

    def test_to_json_list(self):
        result = to_json([1, 2, 3])
        assert result.to_object() == [1, 2, 3]

    def test_to_json_nested(self):
        data = {"users": [{"id": 1}, {"id": 2}]}
        result = to_json(data)
        assert result.to_object() == data


class TestFromJsonFunction:
    """Tests for from_json convenience function."""

    def test_from_json_value(self):
        value = HazelcastJsonValue('{"name": "Bob"}')
        result = from_json(value)
        assert result == {"name": "Bob"}

    def test_from_json_none(self):
        result = from_json(None)
        assert result is None

    def test_from_json_roundtrip(self):
        original = {"users": [{"id": 1, "name": "Alice"}]}
        json_value = to_json(original)
        result = from_json(json_value)
        assert result == original


class TestJsonPathPredicate:
    """Tests for JsonPathPredicate."""

    def test_create(self):
        pred = JsonPathPredicate("$.name", "Alice")
        assert pred.path == "$.name"
        assert pred.value == "Alice"

    def test_nested_path(self):
        pred = JsonPathPredicate("$.address.city", "NYC")
        assert pred.path == "$.address.city"
        assert pred.value == "NYC"

    def test_equality(self):
        p1 = JsonPathPredicate("$.name", "test")
        p2 = JsonPathPredicate("$.name", "test")
        p3 = JsonPathPredicate("$.name", "other")
        p4 = JsonPathPredicate("$.other", "test")
        
        assert p1 == p2
        assert p1 != p3
        assert p1 != p4

    def test_equality_with_non_predicate(self):
        p1 = JsonPathPredicate("$.name", "test")
        assert p1 != "$.name"
        assert p1 != {"path": "$.name"}

    def test_hash(self):
        p1 = JsonPathPredicate("$.name", "test")
        p2 = JsonPathPredicate("$.name", "test")
        assert hash(p1) == hash(p2)
        
        predicates = {p1, p2}
        assert len(predicates) == 1

    def test_repr(self):
        pred = JsonPathPredicate("$.age", 30)
        repr_str = repr(pred)
        assert "JsonPathPredicate" in repr_str
        assert "$.age" in repr_str
        assert "30" in repr_str


class TestJsonContainsPredicate:
    """Tests for JsonContainsPredicate."""

    def test_create(self):
        pred = JsonContainsPredicate("email")
        assert pred.key == "email"

    def test_equality(self):
        p1 = JsonContainsPredicate("name")
        p2 = JsonContainsPredicate("name")
        p3 = JsonContainsPredicate("other")
        
        assert p1 == p2
        assert p1 != p3

    def test_equality_with_non_predicate(self):
        p1 = JsonContainsPredicate("name")
        assert p1 != "name"

    def test_hash(self):
        p1 = JsonContainsPredicate("key")
        p2 = JsonContainsPredicate("key")
        assert hash(p1) == hash(p2)

    def test_repr(self):
        pred = JsonContainsPredicate("email")
        assert "JsonContainsPredicate" in repr(pred)
        assert "email" in repr(pred)


class TestJsonValuePredicate:
    """Tests for JsonValuePredicate."""

    @pytest.mark.parametrize("operator", ["=", "!=", "<", "<=", ">", ">=", "LIKE", "IN"])
    def test_valid_operators(self, operator):
        pred = JsonValuePredicate("$.age", operator, 25)
        assert pred.operator == operator.upper()

    def test_invalid_operator(self):
        with pytest.raises(ValueError) as exc_info:
            JsonValuePredicate("$.age", "INVALID", 25)
        assert "Invalid operator" in str(exc_info.value)

    def test_case_insensitive_operator(self):
        pred = JsonValuePredicate("$.name", "like", "%test%")
        assert pred.operator == "LIKE"

    def test_properties(self):
        pred = JsonValuePredicate("$.score", ">=", 90)
        assert pred.path == "$.score"
        assert pred.operator == ">="
        assert pred.value == 90

    def test_in_operator_with_list(self):
        pred = JsonValuePredicate("$.status", "IN", ["active", "pending"])
        assert pred.value == ["active", "pending"]

    def test_equality(self):
        p1 = JsonValuePredicate("$.age", ">", 25)
        p2 = JsonValuePredicate("$.age", ">", 25)
        p3 = JsonValuePredicate("$.age", ">=", 25)
        
        assert p1 == p2
        assert p1 != p3

    def test_equality_with_non_predicate(self):
        p1 = JsonValuePredicate("$.age", ">", 25)
        assert p1 != ("$.age", ">", 25)

    def test_hash(self):
        p1 = JsonValuePredicate("$.x", "=", 1)
        p2 = JsonValuePredicate("$.x", "=", 1)
        assert hash(p1) == hash(p2)

    def test_hash_with_list_value(self):
        p1 = JsonValuePredicate("$.x", "IN", [1, 2])
        p2 = JsonValuePredicate("$.x", "IN", [1, 2])
        assert hash(p1) == hash(p2)

    def test_repr(self):
        pred = JsonValuePredicate("$.price", "<", 100)
        repr_str = repr(pred)
        assert "JsonValuePredicate" in repr_str
        assert "$.price" in repr_str
        assert "<" in repr_str
        assert "100" in repr_str
