"""Unit tests for hazelcast.serialization.compact module."""

import pytest
from dataclasses import dataclass
from unittest.mock import Mock

from hazelcast.serialization.compact import (
    FieldKind,
    FieldDescriptor,
    Schema,
    SchemaService,
    DefaultCompactWriter,
    DefaultCompactReader,
    CompactStreamWriter,
    CompactStreamReader,
    CompactSerializationService,
    ReflectiveCompactSerializer,
    FIELD_KIND_MAP,
)
from hazelcast.serialization.api import CompactSerializer


class TestFieldKind:
    """Tests for FieldKind enum."""

    def test_all_field_kinds_mapped(self):
        expected_kinds = [
            "boolean", "int8", "int16", "int32", "int64",
            "float32", "float64", "string", "compact",
            "array_boolean", "array_int32", "array_string",
            "nullable_boolean", "nullable_int32"
        ]
        for kind in expected_kinds:
            assert kind in FIELD_KIND_MAP


class TestFieldDescriptor:
    """Tests for FieldDescriptor."""

    def test_basic_creation(self):
        fd = FieldDescriptor(name="test", field_type="int32", index=0)
        assert fd.name == "test"
        assert fd.field_type == "int32"
        assert fd.index == 0

    def test_kind_property(self):
        fd = FieldDescriptor(name="test", field_type="int32", index=0)
        assert fd.kind == FieldKind.INT32

    def test_kind_unknown_type(self):
        fd = FieldDescriptor(name="test", field_type="unknown_type", index=0)
        assert fd.kind == FieldKind.COMPACT


class TestSchema:
    """Tests for Schema class."""

    def test_empty_schema(self):
        schema = Schema(type_name="TestType")
        assert schema.type_name == "TestType"
        assert schema.fields == []
        assert schema.schema_id != 0

    def test_schema_with_fields(self):
        fields = [
            FieldDescriptor(name="f1", field_type="int32", index=0),
            FieldDescriptor(name="f2", field_type="string", index=1),
        ]
        schema = Schema(type_name="TestType", fields=fields)
        assert len(schema.fields) == 2

    def test_get_field_existing(self):
        fields = [FieldDescriptor(name="test_field", field_type="int32", index=0)]
        schema = Schema(type_name="TestType", fields=fields)
        
        field = schema.get_field("test_field")
        assert field is not None
        assert field.name == "test_field"

    def test_get_field_missing(self):
        schema = Schema(type_name="TestType")
        assert schema.get_field("nonexistent") is None

    def test_add_field(self):
        schema = Schema(type_name="TestType")
        fd = schema.add_field("new_field", "boolean")
        
        assert fd.name == "new_field"
        assert fd.field_type == "boolean"
        assert fd.index == 0
        assert len(schema.fields) == 1

    def test_schema_id_consistency(self):
        schema1 = Schema(type_name="TestType")
        schema1.add_field("f1", "int32")
        
        schema2 = Schema(type_name="TestType")
        schema2.add_field("f1", "int32")
        
        assert schema1._compute_schema_id() == schema2._compute_schema_id()

    def test_schema_id_differs_with_different_fields(self):
        schema1 = Schema(type_name="TestType")
        schema1.add_field("f1", "int32")
        
        schema2 = Schema(type_name="TestType")
        schema2.add_field("f2", "string")
        
        assert schema1._compute_schema_id() != schema2._compute_schema_id()

    def test_explicit_schema_id(self):
        schema = Schema(type_name="TestType", schema_id=12345)
        assert schema.schema_id == 12345


class TestSchemaService:
    """Tests for SchemaService."""

    def test_register_and_get_by_id(self):
        service = SchemaService()
        schema = Schema(type_name="TestType", schema_id=100)
        
        service.register(schema)
        
        result = service.get_by_id(100)
        assert result is schema

    def test_register_and_get_by_type_name(self):
        service = SchemaService()
        schema = Schema(type_name="TestType", schema_id=100)
        
        service.register(schema)
        
        result = service.get_by_type_name("TestType")
        assert result is schema

    def test_get_by_id_missing(self):
        service = SchemaService()
        assert service.get_by_id(999) is None

    def test_get_by_type_name_missing(self):
        service = SchemaService()
        assert service.get_by_type_name("NonExistent") is None

    def test_has_schema(self):
        service = SchemaService()
        schema = Schema(type_name="TestType", schema_id=100)
        
        assert not service.has_schema(100)
        service.register(schema)
        assert service.has_schema(100)

    def test_all_schemas(self):
        service = SchemaService()
        schema1 = Schema(type_name="Type1", schema_id=100)
        schema2 = Schema(type_name="Type2", schema_id=200)
        
        service.register(schema1)
        service.register(schema2)
        
        all_schemas = service.all_schemas()
        assert len(all_schemas) == 2
        assert schema1 in all_schemas
        assert schema2 in all_schemas


class TestDefaultCompactWriter:
    """Tests for DefaultCompactWriter."""

    def setup_method(self):
        self.schema = Schema(type_name="TestType")
        self.writer = DefaultCompactWriter(self.schema)

    def test_write_boolean(self):
        self.writer.write_boolean("flag", True)
        assert self.writer.fields["flag"] is True
        assert self.writer.field_types["flag"] == "boolean"

    def test_write_int8(self):
        self.writer.write_int8("byte_val", 127)
        assert self.writer.fields["byte_val"] == 127
        assert self.writer.field_types["byte_val"] == "int8"

    def test_write_int16(self):
        self.writer.write_int16("short_val", 32000)
        assert self.writer.fields["short_val"] == 32000
        assert self.writer.field_types["short_val"] == "int16"

    def test_write_int32(self):
        self.writer.write_int32("int_val", 123456)
        assert self.writer.fields["int_val"] == 123456
        assert self.writer.field_types["int_val"] == "int32"

    def test_write_int64(self):
        self.writer.write_int64("long_val", 9876543210)
        assert self.writer.fields["long_val"] == 9876543210
        assert self.writer.field_types["long_val"] == "int64"

    def test_write_float32(self):
        self.writer.write_float32("float_val", 3.14)
        assert abs(self.writer.fields["float_val"] - 3.14) < 0.001
        assert self.writer.field_types["float_val"] == "float32"

    def test_write_float64(self):
        self.writer.write_float64("double_val", 3.14159265359)
        assert abs(self.writer.fields["double_val"] - 3.14159265359) < 0.0000001
        assert self.writer.field_types["double_val"] == "float64"

    def test_write_string(self):
        self.writer.write_string("str_val", "hello")
        assert self.writer.fields["str_val"] == "hello"
        assert self.writer.field_types["str_val"] == "string"

    def test_write_compact(self):
        nested = {"nested": True}
        self.writer.write_compact("nested_val", nested)
        assert self.writer.fields["nested_val"] == nested
        assert self.writer.field_types["nested_val"] == "compact"

    def test_write_array_of_boolean(self):
        arr = [True, False, True]
        self.writer.write_array_of_boolean("bool_arr", arr)
        assert self.writer.fields["bool_arr"] == arr
        assert self.writer.field_types["bool_arr"] == "array_boolean"

    def test_write_array_of_int32(self):
        arr = [1, 2, 3, 4, 5]
        self.writer.write_array_of_int32("int_arr", arr)
        assert self.writer.fields["int_arr"] == arr
        assert self.writer.field_types["int_arr"] == "array_int32"

    def test_write_array_of_string(self):
        arr = ["a", "b", "c"]
        self.writer.write_array_of_string("str_arr", arr)
        assert self.writer.fields["str_arr"] == arr
        assert self.writer.field_types["str_arr"] == "array_string"

    def test_write_nullable_boolean_with_value(self):
        self.writer.write_nullable_boolean("nullable_bool", True)
        assert self.writer.fields["nullable_bool"] is True
        assert self.writer.field_types["nullable_bool"] == "nullable_boolean"

    def test_write_nullable_boolean_none(self):
        self.writer.write_nullable_boolean("nullable_bool", None)
        assert self.writer.fields["nullable_bool"] is None

    def test_write_nullable_int32_with_value(self):
        self.writer.write_nullable_int32("nullable_int", 42)
        assert self.writer.fields["nullable_int"] == 42
        assert self.writer.field_types["nullable_int"] == "nullable_int32"

    def test_write_nullable_int32_none(self):
        self.writer.write_nullable_int32("nullable_int", None)
        assert self.writer.fields["nullable_int"] is None


class TestDefaultCompactReader:
    """Tests for DefaultCompactReader."""

    def setup_method(self):
        self.schema = Schema(type_name="TestType")
        self.fields = {
            "bool_val": True,
            "int8_val": 42,
            "int16_val": 1000,
            "int32_val": 100000,
            "int64_val": 9876543210,
            "float32_val": 3.14,
            "float64_val": 3.14159265359,
            "str_val": "hello",
            "compact_val": {"nested": True},
            "bool_arr": [True, False],
            "int_arr": [1, 2, 3],
            "str_arr": ["a", "b"],
            "nullable_bool": True,
            "nullable_int": 42,
        }
        self.reader = DefaultCompactReader(self.schema, self.fields)

    def test_read_boolean(self):
        assert self.reader.read_boolean("bool_val") is True

    def test_read_boolean_default(self):
        assert self.reader.read_boolean("missing") is False

    def test_read_int8(self):
        assert self.reader.read_int8("int8_val") == 42

    def test_read_int8_default(self):
        assert self.reader.read_int8("missing") == 0

    def test_read_int16(self):
        assert self.reader.read_int16("int16_val") == 1000

    def test_read_int32(self):
        assert self.reader.read_int32("int32_val") == 100000

    def test_read_int64(self):
        assert self.reader.read_int64("int64_val") == 9876543210

    def test_read_float32(self):
        assert abs(self.reader.read_float32("float32_val") - 3.14) < 0.001

    def test_read_float64(self):
        assert abs(self.reader.read_float64("float64_val") - 3.14159265359) < 0.0000001

    def test_read_string(self):
        assert self.reader.read_string("str_val") == "hello"

    def test_read_string_default(self):
        assert self.reader.read_string("missing") == ""

    def test_read_compact(self):
        assert self.reader.read_compact("compact_val") == {"nested": True}

    def test_read_array_of_boolean(self):
        assert self.reader.read_array_of_boolean("bool_arr") == [True, False]

    def test_read_array_of_boolean_default(self):
        assert self.reader.read_array_of_boolean("missing") == []

    def test_read_array_of_int32(self):
        assert self.reader.read_array_of_int32("int_arr") == [1, 2, 3]

    def test_read_array_of_string(self):
        assert self.reader.read_array_of_string("str_arr") == ["a", "b"]

    def test_read_nullable_boolean(self):
        assert self.reader.read_nullable_boolean("nullable_bool") is True

    def test_read_nullable_boolean_missing(self):
        assert self.reader.read_nullable_boolean("missing") is None

    def test_read_nullable_int32(self):
        assert self.reader.read_nullable_int32("nullable_int") == 42


class TestCompactStreamWriter:
    """Tests for CompactStreamWriter."""

    def test_write_boolean(self):
        writer = CompactStreamWriter()
        writer.write_boolean(True)
        writer.write_boolean(False)
        data = writer.to_bytes()
        assert data == bytes([1, 0])

    def test_write_int8(self):
        writer = CompactStreamWriter()
        writer.write_int8(-42)
        data = writer.to_bytes()
        assert len(data) == 1

    def test_write_int16(self):
        writer = CompactStreamWriter()
        writer.write_int16(1000)
        data = writer.to_bytes()
        assert len(data) == 2

    def test_write_int32(self):
        writer = CompactStreamWriter()
        writer.write_int32(123456)
        data = writer.to_bytes()
        assert len(data) == 4

    def test_write_int64(self):
        writer = CompactStreamWriter()
        writer.write_int64(9876543210)
        data = writer.to_bytes()
        assert len(data) == 8

    def test_write_float32(self):
        writer = CompactStreamWriter()
        writer.write_float32(3.14)
        data = writer.to_bytes()
        assert len(data) == 4

    def test_write_float64(self):
        writer = CompactStreamWriter()
        writer.write_float64(3.14159265359)
        data = writer.to_bytes()
        assert len(data) == 8

    def test_write_string(self):
        writer = CompactStreamWriter()
        writer.write_string("hello")
        data = writer.to_bytes()
        assert len(data) == 4 + 5

    def test_write_string_none(self):
        writer = CompactStreamWriter()
        writer.write_string(None)
        data = writer.to_bytes()
        assert len(data) == 4

    def test_write_bytes(self):
        writer = CompactStreamWriter()
        writer.write_bytes(b"\x01\x02\x03")
        data = writer.to_bytes()
        assert data == b"\x01\x02\x03"


class TestCompactStreamReader:
    """Tests for CompactStreamReader."""

    def test_read_boolean(self):
        reader = CompactStreamReader(bytes([1, 0]))
        assert reader.read_boolean() is True
        assert reader.read_boolean() is False

    def test_read_int8(self):
        writer = CompactStreamWriter()
        writer.write_int8(-42)
        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_int8() == -42

    def test_read_int16(self):
        writer = CompactStreamWriter()
        writer.write_int16(1000)
        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_int16() == 1000

    def test_read_int32(self):
        writer = CompactStreamWriter()
        writer.write_int32(123456)
        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_int32() == 123456

    def test_read_int64(self):
        writer = CompactStreamWriter()
        writer.write_int64(9876543210)
        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_int64() == 9876543210

    def test_read_float32(self):
        writer = CompactStreamWriter()
        writer.write_float32(3.14)
        reader = CompactStreamReader(writer.to_bytes())
        assert abs(reader.read_float32() - 3.14) < 0.001

    def test_read_float64(self):
        writer = CompactStreamWriter()
        writer.write_float64(3.14159265359)
        reader = CompactStreamReader(writer.to_bytes())
        assert abs(reader.read_float64() - 3.14159265359) < 0.0000001

    def test_read_string(self):
        writer = CompactStreamWriter()
        writer.write_string("hello")
        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_string() == "hello"

    def test_read_string_none(self):
        writer = CompactStreamWriter()
        writer.write_string(None)
        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_string() is None

    def test_read_bytes(self):
        data = b"\x01\x02\x03\x04\x05"
        reader = CompactStreamReader(data)
        assert reader.read_bytes(3) == b"\x01\x02\x03"
        assert reader.read_bytes(2) == b"\x04\x05"

    def test_position(self):
        reader = CompactStreamReader(b"\x01\x02\x03\x04")
        assert reader.position == 0
        reader.read_int8()
        assert reader.position == 1
        reader.read_int16()
        assert reader.position == 3


class TestCompactSerializationService:
    """Tests for CompactSerializationService."""

    def test_register_and_get_serializer(self):
        service = CompactSerializationService()
        
        serializer = Mock(spec=CompactSerializer)
        serializer.type_name = "TestType"
        serializer.clazz = dict
        
        service.register_serializer(serializer)
        
        assert service.get_serializer("TestType") is serializer
        assert service.get_serializer_for_class(dict) is serializer

    def test_get_missing_serializer(self):
        service = CompactSerializationService()
        assert service.get_serializer("NonExistent") is None

    def test_get_missing_serializer_for_class(self):
        service = CompactSerializationService()
        assert service.get_serializer_for_class(dict) is None

    def test_register_and_get_schema(self):
        service = CompactSerializationService()
        schema = Schema(type_name="TestType", schema_id=100)
        
        service.register_schema(schema)
        
        assert service.get_schema(100) is schema

    def test_schema_service_property(self):
        service = CompactSerializationService()
        assert service.schema_service is not None

    def test_serialize_no_serializer(self):
        service = CompactSerializationService()
        
        with pytest.raises(ValueError) as exc_info:
            service.serialize({"key": "value"})
        assert "No compact serializer" in str(exc_info.value)

    def test_deserialize_no_serializer(self):
        service = CompactSerializationService()
        
        with pytest.raises(ValueError) as exc_info:
            service.deserialize(b"\x00\x00\x00\x00", "NonExistent")
        assert "No compact serializer" in str(exc_info.value)

    def test_round_trip_serialization(self):
        @dataclass
        class Person:
            name: str
            age: int

        class PersonSerializer(CompactSerializer):
            @property
            def type_name(self):
                return "Person"

            @property
            def clazz(self):
                return Person

            def write(self, writer, obj):
                writer.write_string("name", obj.name)
                writer.write_int32("age", obj.age)

            def read(self, reader):
                name = reader.read_string("name")
                age = reader.read_int32("age")
                return Person(name=name, age=age)

        service = CompactSerializationService()
        service.register_serializer(PersonSerializer())
        
        original = Person(name="Alice", age=30)
        data = service.serialize(original)
        result = service.deserialize(data, "Person")
        
        assert result.name == "Alice"
        assert result.age == 30


class TestReflectiveCompactSerializer:
    """Tests for ReflectiveCompactSerializer."""

    def test_with_dataclass(self):
        @dataclass
        class TestClass:
            name: str
            age: int
            active: bool

        serializer = ReflectiveCompactSerializer(TestClass)
        
        assert serializer.type_name == "TestClass"
        assert serializer.clazz is TestClass

    def test_custom_type_name(self):
        @dataclass
        class TestClass:
            value: int

        serializer = ReflectiveCompactSerializer(TestClass, "CustomTypeName")
        assert serializer.type_name == "CustomTypeName"

    def test_write_dataclass(self):
        @dataclass
        class TestClass:
            name: str
            age: int
            score: float
            active: bool

        serializer = ReflectiveCompactSerializer(TestClass)
        writer = Mock()
        
        obj = TestClass(name="Test", age=25, score=95.5, active=True)
        serializer.write(writer, obj)
        
        writer.write_string.assert_called_with("name", "Test")
        writer.write_int32.assert_called_with("age", 25)
        writer.write_float64.assert_called_with("score", 95.5)
        writer.write_boolean.assert_called_with("active", True)

    def test_read_dataclass(self):
        @dataclass
        class TestClass:
            name: str
            age: int

        serializer = ReflectiveCompactSerializer(TestClass)
        reader = Mock()
        reader.read_string.return_value = "Alice"
        reader.read_int32.return_value = 30
        
        result = serializer.read(reader)
        
        assert result.name == "Alice"
        assert result.age == 30

    def test_write_regular_object(self):
        class RegularClass:
            def __init__(self, value):
                self.value = value

        serializer = ReflectiveCompactSerializer(RegularClass)
        writer = Mock()
        
        obj = RegularClass(42)
        serializer.write(writer, obj)
        
        writer.write_int32.assert_called_with("value", 42)

    def test_write_skips_private_fields(self):
        class RegularClass:
            def __init__(self):
                self.public = 1
                self._private = 2
                self.__very_private = 3

        serializer = ReflectiveCompactSerializer(RegularClass)
        writer = Mock()
        
        obj = RegularClass()
        serializer.write(writer, obj)
        
        writer.write_int32.assert_called_once_with("public", 1)


class TestCompactFieldValueWriteRead:
    """Tests for field value writing and reading edge cases."""

    def test_nullable_boolean_roundtrip_with_value(self):
        service = CompactSerializationService()
        writer = CompactStreamWriter()
        
        service._write_field_value(writer, True, "nullable_boolean")
        
        reader = CompactStreamReader(writer.to_bytes())
        result = service._read_field_value(reader, FieldKind.NULLABLE_BOOLEAN)
        
        assert result is True

    def test_nullable_boolean_roundtrip_none(self):
        service = CompactSerializationService()
        writer = CompactStreamWriter()
        
        service._write_field_value(writer, None, "nullable_boolean")
        
        reader = CompactStreamReader(writer.to_bytes())
        result = service._read_field_value(reader, FieldKind.NULLABLE_BOOLEAN)
        
        assert result is None

    def test_nullable_int32_roundtrip_with_value(self):
        service = CompactSerializationService()
        writer = CompactStreamWriter()
        
        service._write_field_value(writer, 42, "nullable_int32")
        
        reader = CompactStreamReader(writer.to_bytes())
        result = service._read_field_value(reader, FieldKind.NULLABLE_INT32)
        
        assert result == 42

    def test_nullable_int32_roundtrip_none(self):
        service = CompactSerializationService()
        writer = CompactStreamWriter()
        
        service._write_field_value(writer, None, "nullable_int32")
        
        reader = CompactStreamReader(writer.to_bytes())
        result = service._read_field_value(reader, FieldKind.NULLABLE_INT32)
        
        assert result is None

    def test_array_null_roundtrip(self):
        service = CompactSerializationService()
        writer = CompactStreamWriter()
        
        service._write_field_value(writer, None, "array_int32")
        
        reader = CompactStreamReader(writer.to_bytes())
        result = service._read_field_value(reader, FieldKind.ARRAY_INT32)
        
        assert result is None

    def test_unknown_field_kind(self):
        service = CompactSerializationService()
        writer = CompactStreamWriter()
        
        service._write_field_value(writer, "something", "unknown_type")
        
        reader = CompactStreamReader(writer.to_bytes())
        result = service._read_field_value(reader, 999)
        
        assert result is None
