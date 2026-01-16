"""Tests for JSON serialization support."""

import pytest

from hazelcast.serialization.json import (
    HazelcastJsonValue,
    HazelcastJsonValueSerializer,
    JsonPathPredicate,
    JsonContainsPredicate,
    JsonValuePredicate,
    to_json,
    from_json,
    JSON_TYPE_ID,
)
from hazelcast.serialization.service import (
    SerializationService,
    ObjectDataInputImpl,
    ObjectDataOutputImpl,
)
from hazelcast.exceptions import HazelcastSerializationException


class TestHazelcastJsonValue:
    """Tests for HazelcastJsonValue class."""

    def test_init_with_valid_json_string(self):
        value = HazelcastJsonValue('{"name": "test"}')
        assert value.value == '{"name": "test"}'

    def test_init_with_empty_object(self):
        value = HazelcastJsonValue("{}")
        assert value.value == "{}"

    def test_init_with_array(self):
        value = HazelcastJsonValue("[1, 2, 3]")
        assert value.value == "[1, 2, 3]"

    def test_init_with_primitive_string(self):
        value = HazelcastJsonValue('"hello"')
        assert value.value == '"hello"'

    def test_init_with_number(self):
        value = HazelcastJsonValue("42")
        assert value.value == "42"

    def test_init_with_null(self):
        value = HazelcastJsonValue("null")
        assert value.value == "null"

    def test_init_rejects_non_string(self):
        with pytest.raises(HazelcastSerializationException) as exc_info:
            HazelcastJsonValue({"name": "test"})
        assert "requires a string" in str(exc_info.value)

    def test_init_rejects_none(self):
        with pytest.raises(HazelcastSerializationException):
            HazelcastJsonValue(None)

    def test_init_rejects_int(self):
        with pytest.raises(HazelcastSerializationException):
            HazelcastJsonValue(42)

    def test_to_object_dict(self):
        value = HazelcastJsonValue('{"name": "John", "age": 30}')
        obj = value.to_object()
        assert obj == {"name": "John", "age": 30}

    def test_to_object_list(self):
        value = HazelcastJsonValue("[1, 2, 3]")
        obj = value.to_object()
        assert obj == [1, 2, 3]

    def test_to_object_nested(self):
        value = HazelcastJsonValue('{"person": {"name": "Alice", "scores": [95, 87]}}')
        obj = value.to_object()
        assert obj == {"person": {"name": "Alice", "scores": [95, 87]}}

    def test_to_object_primitive_string(self):
        value = HazelcastJsonValue('"hello"')
        assert value.to_object() == "hello"

    def test_to_object_number(self):
        value = HazelcastJsonValue("42.5")
        assert value.to_object() == 42.5

    def test_to_object_boolean(self):
        value = HazelcastJsonValue("true")
        assert value.to_object() is True

    def test_to_object_null(self):
        value = HazelcastJsonValue("null")
        assert value.to_object() is None

    def test_to_object_invalid_json(self):
        value = HazelcastJsonValue("not valid json")
        with pytest.raises(HazelcastSerializationException) as exc_info:
            value.to_object()
        assert "Failed to parse JSON" in str(exc_info.value)

    def test_from_object_dict(self):
        obj = {"name": "test", "value": 123}
        value = HazelcastJsonValue.from_object(obj)
        assert value.to_object() == obj

    def test_from_object_list(self):
        obj = [1, "two", 3.0, None, True]
        value = HazelcastJsonValue.from_object(obj)
        assert value.to_object() == obj

    def test_from_object_nested(self):
        obj = {"users": [{"name": "Alice"}, {"name": "Bob"}]}
        value = HazelcastJsonValue.from_object(obj)
        assert value.to_object() == obj

    def test_from_object_primitive(self):
        assert HazelcastJsonValue.from_object("hello").to_object() == "hello"
        assert HazelcastJsonValue.from_object(42).to_object() == 42
        assert HazelcastJsonValue.from_object(3.14).to_object() == 3.14
        assert HazelcastJsonValue.from_object(True).to_object() is True
        assert HazelcastJsonValue.from_object(None).to_object() is None

    def test_from_object_non_serializable(self):
        with pytest.raises(HazelcastSerializationException) as exc_info:
            HazelcastJsonValue.from_object(lambda x: x)
        assert "Failed to serialize to JSON" in str(exc_info.value)

    def test_equality_same_value(self):
        v1 = HazelcastJsonValue('{"a": 1}')
        v2 = HazelcastJsonValue('{"a": 1}')
        assert v1 == v2

    def test_equality_different_value(self):
        v1 = HazelcastJsonValue('{"a": 1}')
        v2 = HazelcastJsonValue('{"a": 2}')
        assert v1 != v2

    def test_equality_with_non_json_value(self):
        v1 = HazelcastJsonValue('{"a": 1}')
        assert v1 != '{"a": 1}'
        assert v1 != {"a": 1}
        assert v1 != None

    def test_hash_same_value(self):
        v1 = HazelcastJsonValue('{"a": 1}')
        v2 = HazelcastJsonValue('{"a": 1}')
        assert hash(v1) == hash(v2)

    def test_hash_usable_in_set(self):
        v1 = HazelcastJsonValue('{"a": 1}')
        v2 = HazelcastJsonValue('{"a": 1}')
        v3 = HazelcastJsonValue('{"a": 2}')
        s = {v1, v2, v3}
        assert len(s) == 2

    def test_str(self):
        value = HazelcastJsonValue('{"name": "test"}')
        assert str(value) == '{"name": "test"}'

    def test_repr(self):
        value = HazelcastJsonValue('{"name": "test"}')
        assert repr(value) == "HazelcastJsonValue('{\"name\": \"test\"}')"


class TestHelperFunctions:
    """Tests for to_json and from_json helper functions."""

    def test_to_json_dict(self):
        result = to_json({"key": "value"})
        assert isinstance(result, HazelcastJsonValue)
        assert result.to_object() == {"key": "value"}

    def test_to_json_list(self):
        result = to_json([1, 2, 3])
        assert isinstance(result, HazelcastJsonValue)
        assert result.to_object() == [1, 2, 3]

    def test_to_json_primitive(self):
        assert to_json("hello").to_object() == "hello"
        assert to_json(42).to_object() == 42

    def test_from_json_valid(self):
        value = HazelcastJsonValue('{"a": 1}')
        result = from_json(value)
        assert result == {"a": 1}

    def test_from_json_none(self):
        result = from_json(None)
        assert result is None

    def test_round_trip(self):
        original = {"users": [{"name": "Alice", "age": 30}], "count": 1}
        json_value = to_json(original)
        result = from_json(json_value)
        assert result == original


class TestHazelcastJsonValueSerializer:
    """Tests for HazelcastJsonValueSerializer."""

    def test_type_id(self):
        serializer = HazelcastJsonValueSerializer()
        assert serializer.type_id == JSON_TYPE_ID
        assert serializer.type_id == -130

    def test_write_and_read(self):
        service = SerializationService()
        serializer = HazelcastJsonValueSerializer()

        original = HazelcastJsonValue('{"name": "test", "value": 42}')

        output = ObjectDataOutputImpl(service)
        serializer.write(output, original)
        data = output.to_byte_array()

        input_stream = ObjectDataInputImpl(data, service)
        result = serializer.read(input_stream)

        assert result == original
        assert result.to_object() == {"name": "test", "value": 42}

    def test_serialize_empty_object(self):
        service = SerializationService()
        serializer = HazelcastJsonValueSerializer()

        original = HazelcastJsonValue("{}")

        output = ObjectDataOutputImpl(service)
        serializer.write(output, original)
        data = output.to_byte_array()

        input_stream = ObjectDataInputImpl(data, service)
        result = serializer.read(input_stream)

        assert result.value == "{}"

    def test_serialize_complex_json(self):
        service = SerializationService()
        serializer = HazelcastJsonValueSerializer()

        complex_data = {
            "string": "value",
            "number": 42,
            "float": 3.14,
            "boolean": True,
            "null": None,
            "array": [1, 2, 3],
            "nested": {"a": {"b": {"c": "deep"}}}
        }
        original = HazelcastJsonValue.from_object(complex_data)

        output = ObjectDataOutputImpl(service)
        serializer.write(output, original)
        data = output.to_byte_array()

        input_stream = ObjectDataInputImpl(data, service)
        result = serializer.read(input_stream)

        assert result.to_object() == complex_data


class TestSerializationServiceIntegration:
    """Tests for JSON serialization integration with SerializationService."""

    def test_to_data_json_value(self):
        service = SerializationService()
        json_value = HazelcastJsonValue('{"key": "value"}')

        data = service.to_data(json_value)
        assert data is not None
        assert data.get_type_id() == JSON_TYPE_ID

    def test_to_object_json_value(self):
        service = SerializationService()
        json_value = HazelcastJsonValue('{"key": "value"}')

        data = service.to_data(json_value)
        result = service.to_object(data)

        assert isinstance(result, HazelcastJsonValue)
        assert result == json_value

    def test_round_trip_through_service(self):
        service = SerializationService()
        original = HazelcastJsonValue('{"users": [{"name": "Alice"}], "count": 1}')

        data = service.to_data(original)
        result = service.to_object(data)

        assert result.to_object() == original.to_object()


class TestJsonPathPredicate:
    """Tests for JsonPathPredicate."""

    def test_init(self):
        pred = JsonPathPredicate("$.name", "John")
        assert pred.path == "$.name"
        assert pred.value == "John"

    def test_nested_path(self):
        pred = JsonPathPredicate("$.address.city", "NYC")
        assert pred.path == "$.address.city"
        assert pred.value == "NYC"

    def test_array_path(self):
        pred = JsonPathPredicate("$.items[0]", "first")
        assert pred.path == "$.items[0]"
        assert pred.value == "first"

    def test_value_types(self):
        assert JsonPathPredicate("$.count", 42).value == 42
        assert JsonPathPredicate("$.active", True).value is True
        assert JsonPathPredicate("$.data", None).value is None
        assert JsonPathPredicate("$.tags", ["a", "b"]).value == ["a", "b"]

    def test_equality(self):
        p1 = JsonPathPredicate("$.name", "test")
        p2 = JsonPathPredicate("$.name", "test")
        p3 = JsonPathPredicate("$.name", "other")
        p4 = JsonPathPredicate("$.other", "test")

        assert p1 == p2
        assert p1 != p3
        assert p1 != p4
        assert p1 != "not a predicate"

    def test_hash(self):
        p1 = JsonPathPredicate("$.name", "test")
        p2 = JsonPathPredicate("$.name", "test")
        assert hash(p1) == hash(p2)
        assert len({p1, p2}) == 1

    def test_repr(self):
        pred = JsonPathPredicate("$.name", "John")
        assert "JsonPathPredicate" in repr(pred)
        assert "$.name" in repr(pred)
        assert "John" in repr(pred)


class TestJsonContainsPredicate:
    """Tests for JsonContainsPredicate."""

    def test_init(self):
        pred = JsonContainsPredicate("email")
        assert pred.key == "email"

    def test_equality(self):
        p1 = JsonContainsPredicate("key")
        p2 = JsonContainsPredicate("key")
        p3 = JsonContainsPredicate("other")

        assert p1 == p2
        assert p1 != p3
        assert p1 != "key"

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

    def test_init_equal(self):
        pred = JsonValuePredicate("$.age", "=", 30)
        assert pred.path == "$.age"
        assert pred.operator == "="
        assert pred.value == 30

    def test_operators(self):
        assert JsonValuePredicate("$.x", "=", 1).operator == "="
        assert JsonValuePredicate("$.x", "!=", 1).operator == "!="
        assert JsonValuePredicate("$.x", "<", 1).operator == "<"
        assert JsonValuePredicate("$.x", "<=", 1).operator == "<="
        assert JsonValuePredicate("$.x", ">", 1).operator == ">"
        assert JsonValuePredicate("$.x", ">=", 1).operator == ">="
        assert JsonValuePredicate("$.x", "LIKE", "%test%").operator == "LIKE"
        assert JsonValuePredicate("$.x", "IN", [1, 2]).operator == "IN"

    def test_case_insensitive_operator(self):
        assert JsonValuePredicate("$.x", "like", "test").operator == "LIKE"
        assert JsonValuePredicate("$.x", "Like", "test").operator == "LIKE"
        assert JsonValuePredicate("$.x", "in", [1]).operator == "IN"

    def test_invalid_operator(self):
        with pytest.raises(ValueError) as exc_info:
            JsonValuePredicate("$.x", "INVALID", 1)
        assert "Invalid operator" in str(exc_info.value)

    def test_equality(self):
        p1 = JsonValuePredicate("$.age", ">", 25)
        p2 = JsonValuePredicate("$.age", ">", 25)
        p3 = JsonValuePredicate("$.age", ">", 30)
        p4 = JsonValuePredicate("$.age", "<", 25)

        assert p1 == p2
        assert p1 != p3
        assert p1 != p4

    def test_hash(self):
        p1 = JsonValuePredicate("$.age", ">", 25)
        p2 = JsonValuePredicate("$.age", ">", 25)
        assert hash(p1) == hash(p2)

    def test_hash_with_list_value(self):
        p1 = JsonValuePredicate("$.status", "IN", ["a", "b"])
        p2 = JsonValuePredicate("$.status", "IN", ["a", "b"])
        assert hash(p1) == hash(p2)

    def test_repr(self):
        pred = JsonValuePredicate("$.age", ">", 25)
        assert "JsonValuePredicate" in repr(pred)
        assert "$.age" in repr(pred)
        assert ">" in repr(pred)
        assert "25" in repr(pred)
