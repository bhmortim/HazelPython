"""Unit tests for serialization module."""

import struct
import uuid
import pytest

from hazelcast.serialization.api import (
    ObjectDataInput,
    ObjectDataOutput,
    Serializer,
    IdentifiedDataSerializable,
)
from hazelcast.serialization.builtin import (
    NONE_TYPE_ID,
    BOOLEAN_TYPE_ID,
    INT_TYPE_ID,
    FLOAT_TYPE_ID,
    DOUBLE_TYPE_ID,
    STRING_TYPE_ID,
    BYTE_ARRAY_TYPE_ID,
    LIST_TYPE_ID,
    DICT_TYPE_ID,
    UUID_TYPE_ID,
    NoneSerializer,
    BoolSerializer,
    IntSerializer,
    FloatSerializer,
    DoubleSerializer,
    StringSerializer,
    ByteArraySerializer,
    ListSerializer,
    DictSerializer,
    UUIDSerializer,
    get_builtin_serializers,
    get_type_serializer_mapping,
)
from hazelcast.serialization.service import (
    Data,
    ObjectDataInputImpl,
    ObjectDataOutputImpl,
    SerializationService,
)
from hazelcast.exceptions import SerializationException


class TestTypeIdConstants:
    """Tests for type ID constants."""

    def test_none_type_id(self):
        assert NONE_TYPE_ID == 0

    def test_boolean_type_id(self):
        assert BOOLEAN_TYPE_ID == -1

    def test_int_type_id(self):
        assert INT_TYPE_ID == -4

    def test_float_type_id(self):
        assert FLOAT_TYPE_ID == -6

    def test_double_type_id(self):
        assert DOUBLE_TYPE_ID == -7

    def test_string_type_id(self):
        assert STRING_TYPE_ID == -8

    def test_byte_array_type_id(self):
        assert BYTE_ARRAY_TYPE_ID == -9

    def test_list_type_id(self):
        assert LIST_TYPE_ID == -17

    def test_dict_type_id(self):
        assert DICT_TYPE_ID == -18

    def test_uuid_type_id(self):
        assert UUID_TYPE_ID == -19


class TestData:
    """Tests for Data class."""

    def test_empty_data(self):
        data = Data(b"")
        assert data.total_size() == 0
        assert len(data) == 0
        assert data.get_type_id() == NONE_TYPE_ID

    def test_data_with_type_id(self):
        buffer = struct.pack("<i", INT_TYPE_ID) + b"\x00\x00\x00\x01"
        data = Data(buffer)
        assert data.get_type_id() == INT_TYPE_ID
        assert data.get_payload() == b"\x00\x00\x00\x01"

    def test_data_equality(self):
        buffer = b"\x01\x02\x03\x04"
        data1 = Data(buffer)
        data2 = Data(buffer)
        assert data1 == data2

    def test_data_inequality(self):
        data1 = Data(b"\x01\x02\x03")
        data2 = Data(b"\x01\x02\x04")
        assert data1 != data2

    def test_data_hash(self):
        buffer = b"\x01\x02\x03\x04"
        data1 = Data(buffer)
        data2 = Data(buffer)
        assert hash(data1) == hash(data2)

    def test_data_bytes(self):
        buffer = b"\x01\x02\x03\x04"
        data = Data(buffer)
        assert bytes(data) == buffer


class TestObjectDataOutput:
    """Tests for ObjectDataOutputImpl."""

    def setup_method(self):
        self.service = SerializationService()
        self.output = ObjectDataOutputImpl(self.service)

    def test_write_boolean_true(self):
        self.output.write_boolean(True)
        assert self.output.to_byte_array() == b"\x01"

    def test_write_boolean_false(self):
        self.output.write_boolean(False)
        assert self.output.to_byte_array() == b"\x00"

    def test_write_byte(self):
        self.output.write_byte(42)
        assert self.output.to_byte_array() == struct.pack("<b", 42)

    def test_write_short(self):
        self.output.write_short(1000)
        assert self.output.to_byte_array() == struct.pack("<h", 1000)

    def test_write_int(self):
        self.output.write_int(100000)
        assert self.output.to_byte_array() == struct.pack("<i", 100000)

    def test_write_long(self):
        self.output.write_long(10000000000)
        assert self.output.to_byte_array() == struct.pack("<q", 10000000000)

    def test_write_float(self):
        self.output.write_float(3.14)
        result = self.output.to_byte_array()
        assert len(result) == 4

    def test_write_double(self):
        self.output.write_double(3.14159265359)
        result = self.output.to_byte_array()
        assert len(result) == 8

    def test_write_string(self):
        self.output.write_string("hello")
        result = self.output.to_byte_array()
        assert result[:4] == struct.pack("<i", 5)
        assert result[4:] == b"hello"

    def test_write_string_none(self):
        self.output.write_string(None)
        result = self.output.to_byte_array()
        assert result == struct.pack("<i", -1)

    def test_write_string_unicode(self):
        self.output.write_string("héllo wörld 日本語")
        result = self.output.to_byte_array()
        expected = "héllo wörld 日本語".encode("utf-8")
        assert result[:4] == struct.pack("<i", len(expected))
        assert result[4:] == expected

    def test_write_byte_array(self):
        self.output.write_byte_array(b"\x01\x02\x03")
        result = self.output.to_byte_array()
        assert result[:4] == struct.pack("<i", 3)
        assert result[4:] == b"\x01\x02\x03"

    def test_write_byte_array_none(self):
        self.output.write_byte_array(None)
        result = self.output.to_byte_array()
        assert result == struct.pack("<i", -1)


class TestObjectDataInput:
    """Tests for ObjectDataInputImpl."""

    def setup_method(self):
        self.service = SerializationService()

    def _create_input(self, buffer: bytes) -> ObjectDataInputImpl:
        return ObjectDataInputImpl(buffer, self.service)

    def test_read_boolean_true(self):
        input_stream = self._create_input(b"\x01")
        assert input_stream.read_boolean() is True

    def test_read_boolean_false(self):
        input_stream = self._create_input(b"\x00")
        assert input_stream.read_boolean() is False

    def test_read_byte(self):
        input_stream = self._create_input(struct.pack("<b", -42))
        assert input_stream.read_byte() == -42

    def test_read_short(self):
        input_stream = self._create_input(struct.pack("<h", -1000))
        assert input_stream.read_short() == -1000

    def test_read_int(self):
        input_stream = self._create_input(struct.pack("<i", -100000))
        assert input_stream.read_int() == -100000

    def test_read_long(self):
        input_stream = self._create_input(struct.pack("<q", -10000000000))
        assert input_stream.read_long() == -10000000000

    def test_read_float(self):
        input_stream = self._create_input(struct.pack("<f", 3.14))
        result = input_stream.read_float()
        assert abs(result - 3.14) < 0.001

    def test_read_double(self):
        input_stream = self._create_input(struct.pack("<d", 3.14159265359))
        result = input_stream.read_double()
        assert abs(result - 3.14159265359) < 0.0000001

    def test_read_string(self):
        buffer = struct.pack("<i", 5) + b"hello"
        input_stream = self._create_input(buffer)
        assert input_stream.read_string() == "hello"

    def test_read_string_empty(self):
        buffer = struct.pack("<i", -1)
        input_stream = self._create_input(buffer)
        assert input_stream.read_string() == ""

    def test_read_byte_array(self):
        buffer = struct.pack("<i", 3) + b"\x01\x02\x03"
        input_stream = self._create_input(buffer)
        assert input_stream.read_byte_array() == b"\x01\x02\x03"

    def test_read_byte_array_empty(self):
        buffer = struct.pack("<i", -1)
        input_stream = self._create_input(buffer)
        assert input_stream.read_byte_array() == b""

    def test_position(self):
        input_stream = self._create_input(b"\x00\x01\x02\x03")
        assert input_stream.position() == 0
        input_stream.read_byte()
        assert input_stream.position() == 1

    def test_set_position(self):
        input_stream = self._create_input(b"\x00\x01\x02\x03")
        input_stream.set_position(2)
        assert input_stream.position() == 2


class TestBuiltinSerializers:
    """Tests for built-in serializers."""

    def setup_method(self):
        self.service = SerializationService()

    def test_none_serializer_type_id(self):
        serializer = NoneSerializer()
        assert serializer.type_id == NONE_TYPE_ID

    def test_bool_serializer_type_id(self):
        serializer = BoolSerializer()
        assert serializer.type_id == BOOLEAN_TYPE_ID

    def test_int_serializer_type_id(self):
        serializer = IntSerializer()
        assert serializer.type_id == INT_TYPE_ID

    def test_float_serializer_type_id(self):
        serializer = FloatSerializer()
        assert serializer.type_id == FLOAT_TYPE_ID

    def test_double_serializer_type_id(self):
        serializer = DoubleSerializer()
        assert serializer.type_id == DOUBLE_TYPE_ID

    def test_string_serializer_type_id(self):
        serializer = StringSerializer()
        assert serializer.type_id == STRING_TYPE_ID

    def test_byte_array_serializer_type_id(self):
        serializer = ByteArraySerializer()
        assert serializer.type_id == BYTE_ARRAY_TYPE_ID

    def test_list_serializer_type_id(self):
        serializer = ListSerializer()
        assert serializer.type_id == LIST_TYPE_ID

    def test_dict_serializer_type_id(self):
        serializer = DictSerializer()
        assert serializer.type_id == DICT_TYPE_ID

    def test_uuid_serializer_type_id(self):
        serializer = UUIDSerializer()
        assert serializer.type_id == UUID_TYPE_ID


class TestGetBuiltinSerializers:
    """Tests for get_builtin_serializers function."""

    def test_returns_dict(self):
        serializers = get_builtin_serializers()
        assert isinstance(serializers, dict)

    def test_contains_none_serializer(self):
        serializers = get_builtin_serializers()
        assert NONE_TYPE_ID in serializers

    def test_contains_bool_serializer(self):
        serializers = get_builtin_serializers()
        assert BOOLEAN_TYPE_ID in serializers

    def test_contains_int_serializer(self):
        serializers = get_builtin_serializers()
        assert INT_TYPE_ID in serializers

    def test_contains_string_serializer(self):
        serializers = get_builtin_serializers()
        assert STRING_TYPE_ID in serializers

    def test_contains_list_serializer(self):
        serializers = get_builtin_serializers()
        assert LIST_TYPE_ID in serializers

    def test_contains_dict_serializer(self):
        serializers = get_builtin_serializers()
        assert DICT_TYPE_ID in serializers


class TestGetTypeSerializerMapping:
    """Tests for get_type_serializer_mapping function."""

    def test_returns_dict(self):
        mapping = get_type_serializer_mapping()
        assert isinstance(mapping, dict)

    def test_contains_none_type(self):
        mapping = get_type_serializer_mapping()
        assert type(None) in mapping

    def test_contains_bool_type(self):
        mapping = get_type_serializer_mapping()
        assert bool in mapping

    def test_contains_int_type(self):
        mapping = get_type_serializer_mapping()
        assert int in mapping

    def test_contains_float_type(self):
        mapping = get_type_serializer_mapping()
        assert float in mapping

    def test_contains_str_type(self):
        mapping = get_type_serializer_mapping()
        assert str in mapping

    def test_contains_bytes_type(self):
        mapping = get_type_serializer_mapping()
        assert bytes in mapping

    def test_contains_list_type(self):
        mapping = get_type_serializer_mapping()
        assert list in mapping

    def test_contains_dict_type(self):
        mapping = get_type_serializer_mapping()
        assert dict in mapping


class TestSerializationService:
    """Tests for SerializationService."""

    def setup_method(self):
        self.service = SerializationService()

    def test_serialize_none(self):
        data = self.service.to_data(None)
        assert data.get_type_id() == NONE_TYPE_ID

    def test_deserialize_none(self):
        data = self.service.to_data(None)
        result = self.service.to_object(data)
        assert result is None

    def test_serialize_bool_true(self):
        data = self.service.to_data(True)
        assert data.get_type_id() == BOOLEAN_TYPE_ID

    def test_roundtrip_bool_true(self):
        data = self.service.to_data(True)
        result = self.service.to_object(data)
        assert result is True

    def test_roundtrip_bool_false(self):
        data = self.service.to_data(False)
        result = self.service.to_object(data)
        assert result is False

    def test_serialize_int(self):
        data = self.service.to_data(42)
        assert data.get_type_id() == INT_TYPE_ID

    def test_roundtrip_int_positive(self):
        data = self.service.to_data(12345)
        result = self.service.to_object(data)
        assert result == 12345

    def test_roundtrip_int_negative(self):
        data = self.service.to_data(-12345)
        result = self.service.to_object(data)
        assert result == -12345

    def test_roundtrip_int_zero(self):
        data = self.service.to_data(0)
        result = self.service.to_object(data)
        assert result == 0

    def test_serialize_float(self):
        data = self.service.to_data(3.14)
        assert data.get_type_id() == FLOAT_TYPE_ID

    def test_roundtrip_float(self):
        data = self.service.to_data(3.14)
        result = self.service.to_object(data)
        assert abs(result - 3.14) < 0.001

    def test_serialize_string(self):
        data = self.service.to_data("hello")
        assert data.get_type_id() == STRING_TYPE_ID

    def test_roundtrip_string(self):
        data = self.service.to_data("hello world")
        result = self.service.to_object(data)
        assert result == "hello world"

    def test_roundtrip_string_empty(self):
        data = self.service.to_data("")
        result = self.service.to_object(data)
        assert result == ""

    def test_roundtrip_string_unicode(self):
        original = "héllo wörld 日本語 🎉"
        data = self.service.to_data(original)
        result = self.service.to_object(data)
        assert result == original

    def test_serialize_bytes(self):
        data = self.service.to_data(b"\x01\x02\x03")
        assert data.get_type_id() == BYTE_ARRAY_TYPE_ID

    def test_roundtrip_bytes(self):
        original = b"\x00\x01\x02\xff"
        data = self.service.to_data(original)
        result = self.service.to_object(data)
        assert result == original

    def test_roundtrip_bytes_empty(self):
        data = self.service.to_data(b"")
        result = self.service.to_object(data)
        assert result == b""

    def test_serialize_list(self):
        data = self.service.to_data([1, 2, 3])
        assert data.get_type_id() == LIST_TYPE_ID

    def test_roundtrip_list(self):
        original = [1, 2, 3, 4, 5]
        data = self.service.to_data(original)
        result = self.service.to_object(data)
        assert result == original

    def test_roundtrip_list_empty(self):
        data = self.service.to_data([])
        result = self.service.to_object(data)
        assert result == []

    def test_roundtrip_list_mixed_types(self):
        original = [1, "hello", 3.14, True, None]
        data = self.service.to_data(original)
        result = self.service.to_object(data)
        assert len(result) == len(original)
        assert result[0] == 1
        assert result[1] == "hello"
        assert result[3] is True
        assert result[4] is None

    def test_roundtrip_nested_list(self):
        original = [[1, 2], [3, 4], [5, 6]]
        data = self.service.to_data(original)
        result = self.service.to_object(data)
        assert result == original

    def test_serialize_dict(self):
        data = self.service.to_data({"key": "value"})
        assert data.get_type_id() == DICT_TYPE_ID

    def test_roundtrip_dict(self):
        original = {"a": 1, "b": 2, "c": 3}
        data = self.service.to_data(original)
        result = self.service.to_object(data)
        assert result == original

    def test_roundtrip_dict_empty(self):
        data = self.service.to_data({})
        result = self.service.to_object(data)
        assert result == {}

    def test_roundtrip_dict_mixed_values(self):
        original = {"int": 42, "str": "hello", "bool": True, "none": None}
        data = self.service.to_data(original)
        result = self.service.to_object(data)
        assert result == original

    def test_roundtrip_nested_dict(self):
        original = {"outer": {"inner": {"deep": 42}}}
        data = self.service.to_data(original)
        result = self.service.to_object(data)
        assert result == original

    def test_serialize_tuple_as_list(self):
        data = self.service.to_data((1, 2, 3))
        assert data.get_type_id() == LIST_TYPE_ID

    def test_roundtrip_tuple(self):
        original = (1, 2, 3)
        data = self.service.to_data(original)
        result = self.service.to_object(data)
        assert result == [1, 2, 3]

    def test_to_data_returns_data_unchanged(self):
        original_data = self.service.to_data("test")
        result = self.service.to_data(original_data)
        assert result is original_data

    def test_to_object_with_none(self):
        result = self.service.to_object(None)
        assert result is None

    def test_to_object_with_bytes(self):
        data = self.service.to_data(42)
        result = self.service.to_object(bytes(data))
        assert result == 42

    def test_to_object_non_data_passthrough(self):
        result = self.service.to_object("not a data object")
        assert result == "not a data object"

    def test_to_object_empty_data(self):
        data = Data(b"")
        result = self.service.to_object(data)
        assert result is None


class TestCustomSerializer:
    """Tests for custom serializer registration."""

    def test_register_custom_serializer(self):
        class Point:
            def __init__(self, x: int = 0, y: int = 0):
                self.x = x
                self.y = y

        class PointSerializer(Serializer):
            @property
            def type_id(self) -> int:
                return 100

            def write(self, output: ObjectDataOutput, obj: Point) -> None:
                output.write_int(obj.x)
                output.write_int(obj.y)

            def read(self, input: ObjectDataInput) -> Point:
                x = input.read_int()
                y = input.read_int()
                return Point(x, y)

        service = SerializationService(
            custom_serializers={Point: PointSerializer()}
        )

        original = Point(10, 20)
        data = service.to_data(original)
        result = service.to_object(data)

        assert isinstance(result, Point)
        assert result.x == 10
        assert result.y == 20


class TestIdentifiedDataSerializableInterface:
    """Tests for IdentifiedDataSerializable interface."""

    def test_identified_data_serializable_roundtrip(self):
        class TestFactory:
            def create(self, class_id: int):
                if class_id == 1:
                    return TestObject()
                return None

        class TestObject(IdentifiedDataSerializable):
            def __init__(self):
                self.value = 0

            @property
            def factory_id(self) -> int:
                return 1

            @property
            def class_id(self) -> int:
                return 1

            def write_data(self, output: ObjectDataOutput) -> None:
                output.write_int(self.value)

            def read_data(self, input: ObjectDataInput) -> None:
                self.value = input.read_int()

        service = SerializationService(
            data_serializable_factories={1: TestFactory()}
        )

        original = TestObject()
        original.value = 42

        data = service.to_data(original)
        result = service.to_object(data)

        assert isinstance(result, TestObject)
        assert result.value == 42


class TestSerializationExceptions:
    """Tests for serialization exceptions."""

    def test_no_serializer_for_unknown_type(self):
        class UnknownType:
            pass

        service = SerializationService()

        with pytest.raises(SerializationException) as exc_info:
            service.to_data(UnknownType())

        assert "No serializer found" in str(exc_info.value)

    def test_unknown_type_id_deserialization(self):
        buffer = struct.pack("<i", 9999)
        data = Data(buffer)
        service = SerializationService()

        with pytest.raises(SerializationException) as exc_info:
            service.to_object(data)

        assert "No serializer found for type ID" in str(exc_info.value)


class TestUUIDSerialization:
    """Tests for UUID serialization."""

    def setup_method(self):
        self.service = SerializationService()
        self.service.register_serializer(uuid.UUID, UUIDSerializer())

    def test_uuid_roundtrip(self):
        original = uuid.uuid4()
        data = self.service.to_data(original)
        result = self.service.to_object(data)
        assert result == original

    def test_uuid_specific_value(self):
        original = uuid.UUID("12345678-1234-5678-1234-567812345678")
        data = self.service.to_data(original)
        result = self.service.to_object(data)
        assert result == original


class TestCompactSchema:
    """Tests for compact serialization Schema."""

    def test_schema_creation(self):
        from hazelcast.serialization.compact import Schema
        schema = Schema(type_name="Person")
        assert schema.type_name == "Person"
        assert schema.schema_id != 0

    def test_schema_with_fields(self):
        from hazelcast.serialization.compact import Schema, FieldDescriptor
        fields = [
            FieldDescriptor(name="name", field_type="string", index=0),
            FieldDescriptor(name="age", field_type="int32", index=1),
        ]
        schema = Schema(type_name="Person", fields=fields)
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "name"

    def test_schema_id_computation(self):
        from hazelcast.serialization.compact import Schema
        schema1 = Schema(type_name="Person")
        schema2 = Schema(type_name="Person")
        assert schema1.schema_id == schema2.schema_id

    def test_different_type_names_different_ids(self):
        from hazelcast.serialization.compact import Schema
        schema1 = Schema(type_name="Person")
        schema2 = Schema(type_name="Address")
        assert schema1.schema_id != schema2.schema_id

    def test_get_field(self):
        from hazelcast.serialization.compact import Schema, FieldDescriptor
        fields = [FieldDescriptor(name="name", field_type="string", index=0)]
        schema = Schema(type_name="Person", fields=fields)
        field = schema.get_field("name")
        assert field is not None
        assert field.name == "name"

    def test_get_field_not_found(self):
        from hazelcast.serialization.compact import Schema
        schema = Schema(type_name="Person")
        field = schema.get_field("nonexistent")
        assert field is None

    def test_add_field(self):
        from hazelcast.serialization.compact import Schema
        schema = Schema(type_name="Person")
        field = schema.add_field("name", "string")
        assert field.name == "name"
        assert field.field_type == "string"
        assert field.index == 0


class TestFieldDescriptor:
    """Tests for FieldDescriptor."""

    def test_field_descriptor_creation(self):
        from hazelcast.serialization.compact import FieldDescriptor
        field = FieldDescriptor(name="age", field_type="int32", index=0)
        assert field.name == "age"
        assert field.field_type == "int32"
        assert field.index == 0

    def test_field_kind_property(self):
        from hazelcast.serialization.compact import FieldDescriptor, FieldKind
        field = FieldDescriptor(name="age", field_type="int32", index=0)
        assert field.kind == FieldKind.INT32

    def test_field_kind_string(self):
        from hazelcast.serialization.compact import FieldDescriptor, FieldKind
        field = FieldDescriptor(name="name", field_type="string", index=0)
        assert field.kind == FieldKind.STRING


class TestSchemaService:
    """Tests for SchemaService."""

    def test_register_and_get_by_id(self):
        from hazelcast.serialization.compact import SchemaService, Schema
        service = SchemaService()
        schema = Schema(type_name="Person")
        service.register(schema)
        result = service.get_by_id(schema.schema_id)
        assert result is schema

    def test_get_by_type_name(self):
        from hazelcast.serialization.compact import SchemaService, Schema
        service = SchemaService()
        schema = Schema(type_name="Person")
        service.register(schema)
        result = service.get_by_type_name("Person")
        assert result is schema

    def test_has_schema(self):
        from hazelcast.serialization.compact import SchemaService, Schema
        service = SchemaService()
        schema = Schema(type_name="Person")
        assert not service.has_schema(schema.schema_id)
        service.register(schema)
        assert service.has_schema(schema.schema_id)

    def test_all_schemas(self):
        from hazelcast.serialization.compact import SchemaService, Schema
        service = SchemaService()
        schema1 = Schema(type_name="Person")
        schema2 = Schema(type_name="Address")
        service.register(schema1)
        service.register(schema2)
        schemas = service.all_schemas()
        assert len(schemas) == 2


class TestCompactWriter:
    """Tests for DefaultCompactWriter."""

    def test_write_boolean(self):
        from hazelcast.serialization.compact import DefaultCompactWriter, Schema
        schema = Schema(type_name="Test")
        writer = DefaultCompactWriter(schema)
        writer.write_boolean("flag", True)
        assert writer.fields["flag"] is True
        assert writer.field_types["flag"] == "boolean"

    def test_write_int32(self):
        from hazelcast.serialization.compact import DefaultCompactWriter, Schema
        schema = Schema(type_name="Test")
        writer = DefaultCompactWriter(schema)
        writer.write_int32("count", 42)
        assert writer.fields["count"] == 42
        assert writer.field_types["count"] == "int32"

    def test_write_string(self):
        from hazelcast.serialization.compact import DefaultCompactWriter, Schema
        schema = Schema(type_name="Test")
        writer = DefaultCompactWriter(schema)
        writer.write_string("name", "hello")
        assert writer.fields["name"] == "hello"
        assert writer.field_types["name"] == "string"

    def test_write_float64(self):
        from hazelcast.serialization.compact import DefaultCompactWriter, Schema
        schema = Schema(type_name="Test")
        writer = DefaultCompactWriter(schema)
        writer.write_float64("value", 3.14)
        assert writer.fields["value"] == 3.14
        assert writer.field_types["value"] == "float64"

    def test_write_array_of_int32(self):
        from hazelcast.serialization.compact import DefaultCompactWriter, Schema
        schema = Schema(type_name="Test")
        writer = DefaultCompactWriter(schema)
        writer.write_array_of_int32("numbers", [1, 2, 3])
        assert writer.fields["numbers"] == [1, 2, 3]
        assert writer.field_types["numbers"] == "array_int32"

    def test_write_nullable_int32(self):
        from hazelcast.serialization.compact import DefaultCompactWriter, Schema
        schema = Schema(type_name="Test")
        writer = DefaultCompactWriter(schema)
        writer.write_nullable_int32("value", None)
        assert writer.fields["value"] is None
        assert writer.field_types["value"] == "nullable_int32"


class TestCompactReader:
    """Tests for DefaultCompactReader."""

    def test_read_boolean(self):
        from hazelcast.serialization.compact import DefaultCompactReader, Schema
        schema = Schema(type_name="Test")
        reader = DefaultCompactReader(schema, {"flag": True})
        assert reader.read_boolean("flag") is True

    def test_read_boolean_default(self):
        from hazelcast.serialization.compact import DefaultCompactReader, Schema
        schema = Schema(type_name="Test")
        reader = DefaultCompactReader(schema, {})
        assert reader.read_boolean("flag") is False

    def test_read_int32(self):
        from hazelcast.serialization.compact import DefaultCompactReader, Schema
        schema = Schema(type_name="Test")
        reader = DefaultCompactReader(schema, {"count": 42})
        assert reader.read_int32("count") == 42

    def test_read_string(self):
        from hazelcast.serialization.compact import DefaultCompactReader, Schema
        schema = Schema(type_name="Test")
        reader = DefaultCompactReader(schema, {"name": "hello"})
        assert reader.read_string("name") == "hello"

    def test_read_float64(self):
        from hazelcast.serialization.compact import DefaultCompactReader, Schema
        schema = Schema(type_name="Test")
        reader = DefaultCompactReader(schema, {"value": 3.14})
        assert reader.read_float64("value") == 3.14

    def test_read_array_of_int32(self):
        from hazelcast.serialization.compact import DefaultCompactReader, Schema
        schema = Schema(type_name="Test")
        reader = DefaultCompactReader(schema, {"numbers": [1, 2, 3]})
        assert reader.read_array_of_int32("numbers") == [1, 2, 3]

    def test_read_nullable_int32_present(self):
        from hazelcast.serialization.compact import DefaultCompactReader, Schema
        schema = Schema(type_name="Test")
        reader = DefaultCompactReader(schema, {"value": 42})
        assert reader.read_nullable_int32("value") == 42

    def test_read_nullable_int32_absent(self):
        from hazelcast.serialization.compact import DefaultCompactReader, Schema
        schema = Schema(type_name="Test")
        reader = DefaultCompactReader(schema, {"value": None})
        assert reader.read_nullable_int32("value") is None


class TestCompactSerializationService:
    """Tests for CompactSerializationService."""

    def test_register_serializer(self):
        from dataclasses import dataclass
        from hazelcast.serialization.compact import (
            CompactSerializationService,
            ReflectiveCompactSerializer,
        )

        @dataclass
        class Person:
            name: str
            age: int

        service = CompactSerializationService()
        serializer = ReflectiveCompactSerializer(Person, "Person")
        service.register_serializer(serializer)

        assert service.get_serializer("Person") is serializer
        assert service.get_serializer_for_class(Person) is serializer

    def test_serialize_deserialize_dataclass(self):
        from dataclasses import dataclass
        from hazelcast.serialization.compact import (
            CompactSerializationService,
            ReflectiveCompactSerializer,
        )

        @dataclass
        class Person:
            name: str
            age: int

        service = CompactSerializationService()
        serializer = ReflectiveCompactSerializer(Person, "Person")
        service.register_serializer(serializer)

        original = Person(name="Alice", age=30)
        data = service.serialize(original)
        result = service.deserialize(data, "Person")

        assert result.name == "Alice"
        assert result.age == 30

    def test_serialize_no_serializer_error(self):
        from hazelcast.serialization.compact import CompactSerializationService

        service = CompactSerializationService()

        with pytest.raises(ValueError) as exc_info:
            service.serialize(object())

        assert "No compact serializer registered" in str(exc_info.value)

    def test_deserialize_no_serializer_error(self):
        from hazelcast.serialization.compact import CompactSerializationService

        service = CompactSerializationService()

        with pytest.raises(ValueError) as exc_info:
            service.deserialize(b"", "Unknown")

        assert "No compact serializer registered" in str(exc_info.value)

    def test_schema_service_property(self):
        from hazelcast.serialization.compact import CompactSerializationService, SchemaService

        service = CompactSerializationService()
        assert isinstance(service.schema_service, SchemaService)

    def test_serialize_registers_schema(self):
        from dataclasses import dataclass
        from hazelcast.serialization.compact import (
            CompactSerializationService,
            ReflectiveCompactSerializer,
        )

        @dataclass
        class Person:
            name: str
            age: int

        service = CompactSerializationService()
        serializer = ReflectiveCompactSerializer(Person, "Person")
        service.register_serializer(serializer)

        original = Person(name="Bob", age=25)
        service.serialize(original)

        assert service.schema_service.get_by_type_name("Person") is not None


class TestReflectiveCompactSerializer:
    """Tests for ReflectiveCompactSerializer."""

    def test_type_name(self):
        from dataclasses import dataclass
        from hazelcast.serialization.compact import ReflectiveCompactSerializer

        @dataclass
        class Person:
            name: str

        serializer = ReflectiveCompactSerializer(Person, "CustomPerson")
        assert serializer.type_name == "CustomPerson"

    def test_type_name_default(self):
        from dataclasses import dataclass
        from hazelcast.serialization.compact import ReflectiveCompactSerializer

        @dataclass
        class Person:
            name: str

        serializer = ReflectiveCompactSerializer(Person)
        assert serializer.type_name == "Person"

    def test_clazz_property(self):
        from dataclasses import dataclass
        from hazelcast.serialization.compact import ReflectiveCompactSerializer

        @dataclass
        class Person:
            name: str

        serializer = ReflectiveCompactSerializer(Person)
        assert serializer.clazz is Person

    def test_write_bool_field(self):
        from dataclasses import dataclass
        from hazelcast.serialization.compact import (
            ReflectiveCompactSerializer,
            DefaultCompactWriter,
            Schema,
        )

        @dataclass
        class Flags:
            active: bool

        serializer = ReflectiveCompactSerializer(Flags)
        writer = DefaultCompactWriter(Schema(type_name="Flags"))

        serializer.write(writer, Flags(active=True))
        assert writer.fields["active"] is True

    def test_write_int_field(self):
        from dataclasses import dataclass
        from hazelcast.serialization.compact import (
            ReflectiveCompactSerializer,
            DefaultCompactWriter,
            Schema,
        )

        @dataclass
        class Counter:
            count: int

        serializer = ReflectiveCompactSerializer(Counter)
        writer = DefaultCompactWriter(Schema(type_name="Counter"))

        serializer.write(writer, Counter(count=42))
        assert writer.fields["count"] == 42

    def test_read_dataclass(self):
        from dataclasses import dataclass
        from hazelcast.serialization.compact import (
            ReflectiveCompactSerializer,
            DefaultCompactReader,
            Schema,
        )

        @dataclass
        class Person:
            name: str
            age: int

        serializer = ReflectiveCompactSerializer(Person)
        reader = DefaultCompactReader(
            Schema(type_name="Person"),
            {"name": "Charlie", "age": 35}
        )

        result = serializer.read(reader)
        assert result.name == "Charlie"
        assert result.age == 35


class TestCompactStreamWriter:
    """Tests for CompactStreamWriter."""

    def test_write_boolean_true(self):
        from hazelcast.serialization.compact import CompactStreamWriter
        writer = CompactStreamWriter()
        writer.write_boolean(True)
        assert writer.to_bytes() == b"\x01"

    def test_write_boolean_false(self):
        from hazelcast.serialization.compact import CompactStreamWriter
        writer = CompactStreamWriter()
        writer.write_boolean(False)
        assert writer.to_bytes() == b"\x00"

    def test_write_int32(self):
        from hazelcast.serialization.compact import CompactStreamWriter
        writer = CompactStreamWriter()
        writer.write_int32(42)
        assert writer.to_bytes() == struct.pack("<i", 42)

    def test_write_int64(self):
        from hazelcast.serialization.compact import CompactStreamWriter
        writer = CompactStreamWriter()
        writer.write_int64(10000000000)
        assert writer.to_bytes() == struct.pack("<q", 10000000000)

    def test_write_float32(self):
        from hazelcast.serialization.compact import CompactStreamWriter
        writer = CompactStreamWriter()
        writer.write_float32(3.14)
        result = writer.to_bytes()
        assert len(result) == 4

    def test_write_string(self):
        from hazelcast.serialization.compact import CompactStreamWriter
        writer = CompactStreamWriter()
        writer.write_string("hello")
        result = writer.to_bytes()
        assert result[:4] == struct.pack("<i", 5)
        assert result[4:] == b"hello"

    def test_write_string_none(self):
        from hazelcast.serialization.compact import CompactStreamWriter
        writer = CompactStreamWriter()
        writer.write_string(None)
        assert writer.to_bytes() == struct.pack("<i", -1)


class TestCompactStreamReader:
    """Tests for CompactStreamReader."""

    def test_read_boolean_true(self):
        from hazelcast.serialization.compact import CompactStreamReader
        reader = CompactStreamReader(b"\x01")
        assert reader.read_boolean() is True

    def test_read_boolean_false(self):
        from hazelcast.serialization.compact import CompactStreamReader
        reader = CompactStreamReader(b"\x00")
        assert reader.read_boolean() is False

    def test_read_int32(self):
        from hazelcast.serialization.compact import CompactStreamReader
        data = struct.pack("<i", -12345)
        reader = CompactStreamReader(data)
        assert reader.read_int32() == -12345

    def test_read_int64(self):
        from hazelcast.serialization.compact import CompactStreamReader
        data = struct.pack("<q", 10000000000)
        reader = CompactStreamReader(data)
        assert reader.read_int64() == 10000000000

    def test_read_float64(self):
        from hazelcast.serialization.compact import CompactStreamReader
        data = struct.pack("<d", 3.14159)
        reader = CompactStreamReader(data)
        result = reader.read_float64()
        assert abs(result - 3.14159) < 0.00001

    def test_read_string(self):
        from hazelcast.serialization.compact import CompactStreamReader
        data = struct.pack("<i", 5) + b"hello"
        reader = CompactStreamReader(data)
        assert reader.read_string() == "hello"

    def test_read_string_null(self):
        from hazelcast.serialization.compact import CompactStreamReader
        data = struct.pack("<i", -1)
        reader = CompactStreamReader(data)
        assert reader.read_string() is None

    def test_position_tracking(self):
        from hazelcast.serialization.compact import CompactStreamReader
        data = struct.pack("<ii", 1, 2)
        reader = CompactStreamReader(data)
        assert reader.position == 0
        reader.read_int32()
        assert reader.position == 4
        reader.read_int32()
        assert reader.position == 8
