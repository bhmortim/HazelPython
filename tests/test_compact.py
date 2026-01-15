"""Unit tests for hazelcast.serialization.compact module."""

import pytest
from dataclasses import dataclass

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
)
from hazelcast.serialization.api import CompactSerializer, CompactWriter, CompactReader


class TestFieldKind:
    """Tests for FieldKind enum."""

    def test_values(self):
        assert FieldKind.BOOLEAN == 0
        assert FieldKind.INT32 == 3
        assert FieldKind.STRING == 7


class TestFieldDescriptor:
    """Tests for FieldDescriptor class."""

    def test_init(self):
        fd = FieldDescriptor(name="age", field_type="int32", index=0)
        assert fd.name == "age"
        assert fd.field_type == "int32"
        assert fd.index == 0

    def test_kind(self):
        fd = FieldDescriptor(name="active", field_type="boolean", index=0)
        assert fd.kind == FieldKind.BOOLEAN


class TestSchema:
    """Tests for Schema class."""

    def test_init(self):
        schema = Schema(type_name="Person")
        assert schema.type_name == "Person"
        assert schema.fields == []

    def test_add_field(self):
        schema = Schema(type_name="Person")
        fd = schema.add_field("name", "string")
        assert len(schema.fields) == 1
        assert fd.name == "name"
        assert fd.index == 0

    def test_get_field(self):
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")
        schema.add_field("age", "int32")
        fd = schema.get_field("age")
        assert fd is not None
        assert fd.name == "age"

    def test_get_field_not_found(self):
        schema = Schema(type_name="Person")
        assert schema.get_field("nonexistent") is None

    def test_schema_id_computed(self):
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")
        assert schema.schema_id > 0


class TestSchemaService:
    """Tests for SchemaService class."""

    def test_register(self):
        service = SchemaService()
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")
        service.register(schema)
        assert service.has_schema(schema.schema_id)

    def test_get_by_id(self):
        service = SchemaService()
        schema = Schema(type_name="Person")
        service.register(schema)
        found = service.get_by_id(schema.schema_id)
        assert found is schema

    def test_get_by_type_name(self):
        service = SchemaService()
        schema = Schema(type_name="Person")
        service.register(schema)
        found = service.get_by_type_name("Person")
        assert found is schema

    def test_all_schemas(self):
        service = SchemaService()
        s1 = Schema(type_name="Type1")
        s2 = Schema(type_name="Type2")
        service.register(s1)
        service.register(s2)
        all_schemas = service.all_schemas()
        assert len(all_schemas) == 2


class TestDefaultCompactWriter:
    """Tests for DefaultCompactWriter class."""

    def test_write_boolean(self):
        schema = Schema(type_name="Test")
        writer = DefaultCompactWriter(schema)
        writer.write_boolean("active", True)
        assert writer.fields["active"] is True
        assert writer.field_types["active"] == "boolean"

    def test_write_int32(self):
        schema = Schema(type_name="Test")
        writer = DefaultCompactWriter(schema)
        writer.write_int32("count", 42)
        assert writer.fields["count"] == 42

    def test_write_string(self):
        schema = Schema(type_name="Test")
        writer = DefaultCompactWriter(schema)
        writer.write_string("name", "test")
        assert writer.fields["name"] == "test"

    def test_write_multiple_fields(self):
        schema = Schema(type_name="Test")
        writer = DefaultCompactWriter(schema)
        writer.write_string("name", "test")
        writer.write_int32("age", 25)
        writer.write_boolean("active", True)
        assert len(writer.fields) == 3


class TestDefaultCompactReader:
    """Tests for DefaultCompactReader class."""

    def test_read_boolean(self):
        schema = Schema(type_name="Test")
        fields = {"active": True}
        reader = DefaultCompactReader(schema, fields)
        assert reader.read_boolean("active") is True

    def test_read_int32(self):
        schema = Schema(type_name="Test")
        fields = {"count": 42}
        reader = DefaultCompactReader(schema, fields)
        assert reader.read_int32("count") == 42

    def test_read_string(self):
        schema = Schema(type_name="Test")
        fields = {"name": "test"}
        reader = DefaultCompactReader(schema, fields)
        assert reader.read_string("name") == "test"

    def test_read_missing_field_default(self):
        schema = Schema(type_name="Test")
        reader = DefaultCompactReader(schema, {})
        assert reader.read_boolean("missing") is False
        assert reader.read_int32("missing") == 0
        assert reader.read_string("missing") == ""


class TestCompactStreamWriter:
    """Tests for CompactStreamWriter class."""

    def test_write_boolean(self):
        writer = CompactStreamWriter()
        writer.write_boolean(True)
        writer.write_boolean(False)
        data = writer.to_bytes()
        assert data == bytes([1, 0])

    def test_write_int32(self):
        writer = CompactStreamWriter()
        writer.write_int32(42)
        data = writer.to_bytes()
        assert len(data) == 4

    def test_write_string(self):
        writer = CompactStreamWriter()
        writer.write_string("hello")
        data = writer.to_bytes()
        assert len(data) > 4

    def test_write_string_none(self):
        writer = CompactStreamWriter()
        writer.write_string(None)
        data = writer.to_bytes()
        assert len(data) == 4


class TestCompactStreamReader:
    """Tests for CompactStreamReader class."""

    def test_read_boolean(self):
        reader = CompactStreamReader(bytes([1, 0]))
        assert reader.read_boolean() is True
        assert reader.read_boolean() is False

    def test_read_int32(self):
        import struct
        data = struct.pack("<i", 42)
        reader = CompactStreamReader(data)
        assert reader.read_int32() == 42

    def test_position(self):
        reader = CompactStreamReader(bytes([1, 2, 3, 4]))
        assert reader.position == 0
        reader.read_boolean()
        assert reader.position == 1


class TestCompactSerializationService:
    """Tests for CompactSerializationService class."""

    def test_register_serializer(self):
        service = CompactSerializationService()

        class PersonSerializer(CompactSerializer):
            @property
            def type_name(self):
                return "Person"

            @property
            def clazz(self):
                return dict

            def write(self, writer, obj):
                writer.write_string("name", obj.get("name", ""))

            def read(self, reader):
                return {"name": reader.read_string("name")}

        service.register_serializer(PersonSerializer())
        assert service.get_serializer("Person") is not None

    def test_get_serializer_for_class(self):
        service = CompactSerializationService()

        class TestSerializer(CompactSerializer):
            @property
            def type_name(self):
                return "TestType"

            @property
            def clazz(self):
                return str

            def write(self, writer, obj):
                pass

            def read(self, reader):
                return ""

        service.register_serializer(TestSerializer())
        serializer = service.get_serializer_for_class(str)
        assert serializer is not None


class TestReflectiveCompactSerializer:
    """Tests for ReflectiveCompactSerializer class."""

    def test_type_name(self):
        @dataclass
        class Person:
            name: str
            age: int

        serializer = ReflectiveCompactSerializer(Person)
        assert serializer.type_name == "Person"

    def test_clazz(self):
        @dataclass
        class Person:
            name: str

        serializer = ReflectiveCompactSerializer(Person)
        assert serializer.clazz is Person

    def test_custom_type_name(self):
        serializer = ReflectiveCompactSerializer(dict, "CustomType")
        assert serializer.type_name == "CustomType"
