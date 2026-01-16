"""Tests for compact serialization."""

import pytest
from dataclasses import dataclass
from typing import List, Optional

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
    GenericRecord,
    GenericRecordBuilder,
    GenericRecordSerializer,
    ReflectiveCompactSerializer,
)
from hazelcast.serialization.api import CompactSerializer, CompactReader, CompactWriter


class TestFieldKind:
    """Tests for FieldKind enum."""

    def test_basic_field_kinds_exist(self):
        assert FieldKind.BOOLEAN == 0
        assert FieldKind.INT8 == 1
        assert FieldKind.INT32 == 3
        assert FieldKind.STRING == 7

    def test_array_field_kinds_exist(self):
        assert FieldKind.ARRAY_BOOLEAN == 9
        assert FieldKind.ARRAY_INT32 == 12
        assert FieldKind.ARRAY_STRING == 16

    def test_nullable_field_kinds_exist(self):
        assert FieldKind.NULLABLE_BOOLEAN == 18
        assert FieldKind.NULLABLE_INT32 == 21


class TestFieldDescriptor:
    """Tests for FieldDescriptor."""

    def test_create_field_descriptor(self):
        field = FieldDescriptor(name="age", field_type="int32", index=0)
        assert field.name == "age"
        assert field.field_type == "int32"
        assert field.index == 0
        assert field.kind == FieldKind.INT32

    def test_is_variable_size(self):
        string_field = FieldDescriptor(name="name", field_type="string", index=0)
        int_field = FieldDescriptor(name="age", field_type="int32", index=1)

        assert string_field.is_variable_size() is True
        assert int_field.is_variable_size() is False


class TestSchema:
    """Tests for Schema."""

    def test_create_schema(self):
        schema = Schema(type_name="Person")
        assert schema.type_name == "Person"
        assert len(schema.fields) == 0

    def test_add_field(self):
        schema = Schema(type_name="Person")
        field = schema.add_field("name", "string")

        assert field.name == "name"
        assert field.field_type == "string"
        assert len(schema.fields) == 1

    def test_get_field(self):
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")
        schema.add_field("age", "int32")

        name_field = schema.get_field("name")
        assert name_field is not None
        assert name_field.name == "name"

        missing = schema.get_field("missing")
        assert missing is None

    def test_schema_id_computed(self):
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")

        assert schema.schema_id != 0


class TestSchemaService:
    """Tests for SchemaService."""

    def test_register_and_get_schema(self):
        service = SchemaService()
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")

        service.register(schema)

        retrieved = service.get_by_id(schema.schema_id)
        assert retrieved is not None
        assert retrieved.type_name == "Person"

    def test_get_by_type_name(self):
        service = SchemaService()
        schema = Schema(type_name="Person")
        service.register(schema)

        retrieved = service.get_by_type_name("Person")
        assert retrieved is not None

    def test_schema_compatibility(self):
        service = SchemaService()

        old_schema = Schema(type_name="Person")
        old_schema.add_field("name", "string")

        new_schema = Schema(type_name="Person")
        new_schema.add_field("name", "string")
        new_schema.add_field("age", "int32")

        assert service.is_compatible(old_schema, new_schema) is True

    def test_schema_incompatibility_type_change(self):
        service = SchemaService()

        old_schema = Schema(type_name="Person")
        old_schema.add_field("age", "int32")

        new_schema = Schema(type_name="Person")
        new_schema.add_field("age", "string")

        assert service.is_compatible(old_schema, new_schema) is False


class TestCompactStreamWriter:
    """Tests for CompactStreamWriter."""

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
        assert len(data) == 4 + 5

    def test_write_null_string(self):
        writer = CompactStreamWriter()
        writer.write_string(None)

        data = writer.to_bytes()
        reader = CompactStreamReader(data)
        length = reader.read_int32()
        assert length == -1


class TestCompactStreamReader:
    """Tests for CompactStreamReader."""

    def test_read_boolean(self):
        reader = CompactStreamReader(bytes([1, 0]))
        assert reader.read_boolean() is True
        assert reader.read_boolean() is False

    def test_read_int32(self):
        writer = CompactStreamWriter()
        writer.write_int32(12345)
        data = writer.to_bytes()

        reader = CompactStreamReader(data)
        assert reader.read_int32() == 12345

    def test_read_string(self):
        writer = CompactStreamWriter()
        writer.write_string("test")
        data = writer.to_bytes()

        reader = CompactStreamReader(data)
        assert reader.read_string() == "test"


class TestDefaultCompactWriter:
    """Tests for DefaultCompactWriter."""

    def test_write_fields(self):
        schema = Schema(type_name="Person")
        writer = DefaultCompactWriter(schema)

        writer.write_string("name", "Alice")
        writer.write_int32("age", 30)
        writer.write_boolean("active", True)

        assert writer.fields["name"] == "Alice"
        assert writer.fields["age"] == 30
        assert writer.fields["active"] is True

    def test_write_arrays(self):
        schema = Schema(type_name="Data")
        writer = DefaultCompactWriter(schema)

        writer.write_array_of_int32("numbers", [1, 2, 3])
        writer.write_array_of_string("names", ["a", "b"])

        assert writer.fields["numbers"] == [1, 2, 3]
        assert writer.fields["names"] == ["a", "b"]

    def test_write_nullable(self):
        schema = Schema(type_name="Data")
        writer = DefaultCompactWriter(schema)

        writer.write_nullable_int32("value", None)
        writer.write_nullable_boolean("flag", True)

        assert writer.fields["value"] is None
        assert writer.fields["flag"] is True


class TestDefaultCompactReader:
    """Tests for DefaultCompactReader."""

    def test_read_fields(self):
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")
        schema.add_field("age", "int32")

        fields = {"name": "Bob", "age": 25}
        reader = DefaultCompactReader(schema, fields)

        assert reader.read_string("name") == "Bob"
        assert reader.read_int32("age") == 25

    def test_read_missing_field_returns_default(self):
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")

        fields = {}
        reader = DefaultCompactReader(schema, fields)

        assert reader.read_string("name") is None
        assert reader.read_int32("age") == 0

    def test_has_field(self):
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")

        fields = {"name": "Alice"}
        reader = DefaultCompactReader(schema, fields)

        assert reader.has_field("name") is True
        assert reader.has_field("missing") is False

    def test_get_field_kind(self):
        schema = Schema(type_name="Person")
        schema.add_field("age", "int32")

        fields = {"age": 30}
        reader = DefaultCompactReader(schema, fields)

        assert reader.get_field_kind("age") == FieldKind.INT32
        assert reader.get_field_kind("missing") is None


class TestGenericRecord:
    """Tests for GenericRecord."""

    def test_create_generic_record(self):
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")
        schema.add_field("age", "int32")

        fields = {"name": "Alice", "age": 30}
        record = GenericRecord(schema, fields)

        assert record.type_name == "Person"
        assert record.get_string("name") == "Alice"
        assert record.get_int32("age") == 30

    def test_has_field(self):
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")

        record = GenericRecord(schema, {"name": "Bob"})

        assert record.has_field("name") is True
        assert record.has_field("missing") is False

    def test_get_field_kind(self):
        schema = Schema(type_name="Person")
        schema.add_field("age", "int32")

        record = GenericRecord(schema, {"age": 25})

        assert record.get_field_kind("age") == FieldKind.INT32

    def test_to_dict(self):
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")
        schema.add_field("age", "int32")

        record = GenericRecord(schema, {"name": "Charlie", "age": 35})

        result = record.to_dict()
        assert result == {"name": "Charlie", "age": 35}

    def test_get_wrong_type_raises_error(self):
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")

        record = GenericRecord(schema, {"name": "Alice"})

        with pytest.raises(TypeError):
            record.get_int32("name")

    def test_get_missing_field_raises_error(self):
        schema = Schema(type_name="Person")

        record = GenericRecord(schema, {})

        with pytest.raises(ValueError):
            record.get_string("missing")


class TestGenericRecordBuilder:
    """Tests for GenericRecordBuilder."""

    def test_build_simple_record(self):
        record = (
            GenericRecordBuilder("Person")
            .set_string("name", "Alice")
            .set_int32("age", 30)
            .build()
        )

        assert record.type_name == "Person"
        assert record.get_string("name") == "Alice"
        assert record.get_int32("age") == 30

    def test_build_with_arrays(self):
        record = (
            GenericRecordBuilder("Data")
            .set_array_of_int32("numbers", [1, 2, 3])
            .set_array_of_string("names", ["a", "b"])
            .build()
        )

        assert record.get_array_of_int32("numbers") == [1, 2, 3]
        assert record.get_array_of_string("names") == ["a", "b"]

    def test_build_with_nullable(self):
        record = (
            GenericRecordBuilder("Data")
            .set_nullable_int32("value", None)
            .set_nullable_boolean("flag", True)
            .build()
        )

        assert record.get_nullable_int32("value") is None
        assert record.get_nullable_boolean("flag") is True

    def test_from_existing_record(self):
        original = (
            GenericRecordBuilder("Person")
            .set_string("name", "Bob")
            .set_int32("age", 25)
            .build()
        )

        modified = (
            original.new_builder()
            .set_int32("age", 26)
            .build()
        )

        assert modified.get_string("name") == "Bob"
        assert modified.get_int32("age") == 26


class TestCompactSerializationService:
    """Tests for CompactSerializationService."""

    def test_serialize_and_deserialize(self):
        @dataclass
        class Person:
            name: str
            age: int

        class PersonSerializer(CompactSerializer[Person]):
            @property
            def type_name(self) -> str:
                return "Person"

            @property
            def clazz(self) -> type:
                return Person

            def write(self, writer: CompactWriter, obj: Person) -> None:
                writer.write_string("name", obj.name)
                writer.write_int32("age", obj.age)

            def read(self, reader: CompactReader) -> Person:
                return Person(
                    name=reader.read_string("name") or "",
                    age=reader.read_int32("age"),
                )

        service = CompactSerializationService()
        service.register_serializer(PersonSerializer())

        person = Person(name="Alice", age=30)
        data = service.serialize(person)

        result = service.deserialize(data, "Person")
        assert result.name == "Alice"
        assert result.age == 30

    def test_to_generic_record(self):
        service = CompactSerializationService()

        record = (
            GenericRecordBuilder("Test")
            .set_string("message", "hello")
            .set_int32("count", 42)
            .build()
        )

        service.register_serializer(GenericRecordSerializer("Test"))
        data = service.serialize(record)

        result = service.to_generic_record(data, "Test")
        assert result.get_string("message") == "hello"
        assert result.get_int32("count") == 42

    def test_schema_registration(self):
        service = CompactSerializationService()

        schema = Schema(type_name="Person")
        schema.add_field("name", "string")
        service.register_schema(schema)

        retrieved = service.get_schema(schema.schema_id)
        assert retrieved is not None
        assert retrieved.type_name == "Person"


class TestReflectiveCompactSerializer:
    """Tests for ReflectiveCompactSerializer."""

    def test_serialize_dataclass(self):
        @dataclass
        class Point:
            x: int
            y: int

        serializer = ReflectiveCompactSerializer(Point)
        service = CompactSerializationService()
        service.register_serializer(serializer)

        point = Point(x=10, y=20)
        data = service.serialize(point)
        result = service.deserialize(data, "Point")

        assert result.x == 10
        assert result.y == 20

    def test_serialize_dataclass_with_string(self):
        @dataclass
        class Named:
            name: str
            value: int

        serializer = ReflectiveCompactSerializer(Named)
        service = CompactSerializationService()
        service.register_serializer(serializer)

        obj = Named(name="test", value=42)
        data = service.serialize(obj)
        result = service.deserialize(data, "Named")

        assert result.name == "test"
        assert result.value == 42


class TestSchemaEvolution:
    """Tests for schema evolution support."""

    def test_add_field_evolution(self):
        v1_schema = Schema(type_name="Person")
        v1_schema.add_field("name", "string")

        v2_schema = Schema(type_name="Person")
        v2_schema.add_field("name", "string")
        v2_schema.add_field("age", "int32")

        fields = {"name": "Alice"}
        reader = DefaultCompactReader(v1_schema, fields, v2_schema)

        assert reader.read_string("name") == "Alice"
        assert reader.read_int32("age") == 0

    def test_service_schema_compatibility_check(self):
        service = CompactSerializationService()

        v1_schema = Schema(type_name="Person")
        v1_schema.add_field("name", "string")
        service.register_schema(v1_schema)

        v2_schema = Schema(type_name="Person")
        v2_schema.add_field("name", "string")
        v2_schema.add_field("age", "int32")
        service.register_schema(v2_schema)

        assert service.check_schema_compatibility(v1_schema.schema_id, v2_schema.schema_id)


class TestCompactReaderFieldTypeChecking:
    """Tests for field type checking in CompactReader."""

    def test_read_wrong_type_raises_error(self):
        schema = Schema(type_name="Data")
        schema.add_field("value", "string")

        fields = {"value": "test"}
        reader = DefaultCompactReader(schema, fields)

        with pytest.raises(TypeError):
            reader.read_int32("value")

    def test_read_correct_type_succeeds(self):
        schema = Schema(type_name="Data")
        schema.add_field("value", "int32")

        fields = {"value": 42}
        reader = DefaultCompactReader(schema, fields)

        assert reader.read_int32("value") == 42
