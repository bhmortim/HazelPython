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
    GenericRecord,
    GenericRecordBuilder,
    GenericRecordSerializer,
    ReflectiveCompactSerializer,
    FIELD_KIND_MAP,
    FIELD_KIND_DEFAULTS,
)
from hazelcast.serialization.api import CompactSerializer, CompactWriter, CompactReader


class TestFieldKind:
    """Tests for FieldKind enum."""

    def test_field_kinds_exist(self):
        assert FieldKind.BOOLEAN == 0
        assert FieldKind.INT32 == 3
        assert FieldKind.STRING == 7

    def test_field_kind_map(self):
        assert FIELD_KIND_MAP["boolean"] == FieldKind.BOOLEAN
        assert FIELD_KIND_MAP["int32"] == FieldKind.INT32
        assert FIELD_KIND_MAP["string"] == FieldKind.STRING

    def test_field_kind_defaults(self):
        assert FIELD_KIND_DEFAULTS[FieldKind.BOOLEAN] is False
        assert FIELD_KIND_DEFAULTS[FieldKind.INT32] == 0
        assert FIELD_KIND_DEFAULTS[FieldKind.STRING] is None


class TestFieldDescriptor:
    """Tests for FieldDescriptor."""

    def test_create(self):
        fd = FieldDescriptor(name="age", field_type="int32", index=0)
        assert fd.name == "age"
        assert fd.field_type == "int32"
        assert fd.index == 0

    def test_kind_property(self):
        fd = FieldDescriptor(name="name", field_type="string", index=0)
        assert fd.kind == FieldKind.STRING

    def test_is_variable_size(self):
        string_fd = FieldDescriptor(name="s", field_type="string", index=0)
        int_fd = FieldDescriptor(name="i", field_type="int32", index=0)
        
        assert string_fd.is_variable_size() is True
        assert int_fd.is_variable_size() is False


class TestSchema:
    """Tests for Schema."""

    def test_create_empty(self):
        schema = Schema(type_name="Person")
        assert schema.type_name == "Person"
        assert len(schema.fields) == 0

    def test_add_field(self):
        schema = Schema(type_name="Person")
        fd = schema.add_field("name", "string")
        
        assert fd.name == "name"
        assert len(schema.fields) == 1

    def test_get_field(self):
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")
        schema.add_field("age", "int32")
        
        assert schema.get_field("name") is not None
        assert schema.get_field("age").field_type == "int32"
        assert schema.get_field("missing") is None

    def test_schema_id_computed(self):
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")
        
        assert schema.schema_id != 0

    def test_different_schemas_different_ids(self):
        s1 = Schema(type_name="Person")
        s1.add_field("name", "string")
        
        s2 = Schema(type_name="Person")
        s2.add_field("age", "int32")
        
        assert s1._compute_schema_id() != s2._compute_schema_id()


class TestSchemaService:
    """Tests for SchemaService."""

    @pytest.fixture
    def service(self):
        return SchemaService()

    def test_register_and_get_by_id(self, service):
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")
        
        service.register(schema)
        
        found = service.get_by_id(schema.schema_id)
        assert found is schema

    def test_get_by_type_name(self, service):
        schema = Schema(type_name="Person")
        service.register(schema)
        
        found = service.get_by_type_name("Person")
        assert found is schema

    def test_get_nonexistent(self, service):
        assert service.get_by_id(12345) is None
        assert service.get_by_type_name("Unknown") is None

    def test_has_schema(self, service):
        schema = Schema(type_name="Person")
        service.register(schema)
        
        assert service.has_schema(schema.schema_id) is True
        assert service.has_schema(99999) is False

    def test_all_schemas(self, service):
        s1 = Schema(type_name="A")
        s2 = Schema(type_name="B")
        service.register(s1)
        service.register(s2)
        
        all_schemas = service.all_schemas()
        assert len(all_schemas) == 2

    def test_is_compatible_same_fields(self, service):
        s1 = Schema(type_name="Person")
        s1.add_field("name", "string")
        
        s2 = Schema(type_name="Person")
        s2.add_field("name", "string")
        
        assert service.is_compatible(s1, s2) is True

    def test_is_compatible_different_types(self, service):
        s1 = Schema(type_name="Person")
        s1.add_field("age", "int32")
        
        s2 = Schema(type_name="Person")
        s2.add_field("age", "string")
        
        assert service.is_compatible(s1, s2) is False

    def test_is_compatible_different_type_names(self, service):
        s1 = Schema(type_name="Person")
        s2 = Schema(type_name="Employee")
        
        assert service.is_compatible(s1, s2) is False


class TestDefaultCompactWriter:
    """Tests for DefaultCompactWriter."""

    @pytest.fixture
    def writer(self):
        schema = Schema(type_name="Test")
        return DefaultCompactWriter(schema)

    def test_write_boolean(self, writer):
        writer.write_boolean("active", True)
        assert writer.fields["active"] is True
        assert writer.field_types["active"] == "boolean"

    def test_write_int32(self, writer):
        writer.write_int32("count", 42)
        assert writer.fields["count"] == 42
        assert writer.field_types["count"] == "int32"

    def test_write_string(self, writer):
        writer.write_string("name", "Alice")
        assert writer.fields["name"] == "Alice"
        assert writer.field_types["name"] == "string"

    def test_write_multiple(self, writer):
        writer.write_boolean("active", True)
        writer.write_int32("count", 5)
        writer.write_string("name", "Test")
        
        assert len(writer.fields) == 3


class TestDefaultCompactReader:
    """Tests for DefaultCompactReader."""

    def test_read_existing_field(self):
        schema = Schema(type_name="Test")
        schema.add_field("name", "string")
        
        reader = DefaultCompactReader(schema, {"name": "Alice"})
        
        assert reader.read_string("name") == "Alice"

    def test_read_missing_field_returns_default(self):
        schema = Schema(type_name="Test")
        schema.add_field("count", "int32")
        
        reader = DefaultCompactReader(schema, {})
        
        assert reader.read_int32("count") == 0

    def test_read_wrong_type_raises(self):
        schema = Schema(type_name="Test")
        schema.add_field("name", "string")
        
        reader = DefaultCompactReader(schema, {"name": "Alice"})
        
        with pytest.raises(TypeError):
            reader.read_int32("name")

    def test_has_field(self):
        schema = Schema(type_name="Test")
        schema.add_field("name", "string")
        
        reader = DefaultCompactReader(schema, {"name": "Alice"})
        
        assert reader.has_field("name") is True
        assert reader.has_field("missing") is False

    def test_get_field_names(self):
        schema = Schema(type_name="Test")
        schema.add_field("a", "int32")
        schema.add_field("b", "string")
        
        reader = DefaultCompactReader(schema, {})
        
        names = reader.get_field_names()
        assert "a" in names
        assert "b" in names


class TestCompactStreamIO:
    """Tests for CompactStreamWriter and CompactStreamReader."""

    def test_write_and_read_boolean(self):
        writer = CompactStreamWriter()
        writer.write_boolean(True)
        writer.write_boolean(False)
        
        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_boolean() is True
        assert reader.read_boolean() is False

    def test_write_and_read_integers(self):
        writer = CompactStreamWriter()
        writer.write_int8(42)
        writer.write_int16(1000)
        writer.write_int32(100000)
        writer.write_int64(10000000000)
        
        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_int8() == 42
        assert reader.read_int16() == 1000
        assert reader.read_int32() == 100000
        assert reader.read_int64() == 10000000000

    def test_write_and_read_floats(self):
        writer = CompactStreamWriter()
        writer.write_float32(3.14)
        writer.write_float64(2.718281828)
        
        reader = CompactStreamReader(writer.to_bytes())
        assert abs(reader.read_float32() - 3.14) < 0.001
        assert abs(reader.read_float64() - 2.718281828) < 0.0001

    def test_write_and_read_string(self):
        writer = CompactStreamWriter()
        writer.write_string("hello")
        writer.write_string(None)
        
        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_string() == "hello"
        assert reader.read_string() is None

    def test_position(self):
        writer = CompactStreamWriter()
        writer.write_int32(1)
        writer.write_int32(2)
        
        reader = CompactStreamReader(writer.to_bytes())
        assert reader.position == 0
        reader.read_int32()
        assert reader.position == 4


class TestGenericRecord:
    """Tests for GenericRecord."""

    @pytest.fixture
    def person_schema(self):
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")
        schema.add_field("age", "int32")
        return schema

    def test_create(self, person_schema):
        record = GenericRecord(person_schema, {"name": "Alice", "age": 30})
        assert record.type_name == "Person"

    def test_has_field(self, person_schema):
        record = GenericRecord(person_schema, {})
        assert record.has_field("name") is True
        assert record.has_field("missing") is False

    def test_get_field_kind(self, person_schema):
        record = GenericRecord(person_schema, {})
        assert record.get_field_kind("name") == FieldKind.STRING
        assert record.get_field_kind("age") == FieldKind.INT32
        assert record.get_field_kind("missing") is None

    def test_get_string(self, person_schema):
        record = GenericRecord(person_schema, {"name": "Bob"})
        assert record.get_string("name") == "Bob"

    def test_get_int32(self, person_schema):
        record = GenericRecord(person_schema, {"age": 25})
        assert record.get_int32("age") == 25

    def test_get_field_wrong_type(self, person_schema):
        record = GenericRecord(person_schema, {"name": "Alice"})
        with pytest.raises(TypeError):
            record.get_int32("name")

    def test_get_nonexistent_field(self, person_schema):
        record = GenericRecord(person_schema, {})
        with pytest.raises(ValueError):
            record.get_string("missing")

    def test_to_dict(self, person_schema):
        record = GenericRecord(person_schema, {"name": "Alice", "age": 30})
        d = record.to_dict()
        assert d == {"name": "Alice", "age": 30}

    def test_equality(self, person_schema):
        r1 = GenericRecord(person_schema, {"name": "Alice"})
        r2 = GenericRecord(person_schema, {"name": "Alice"})
        r3 = GenericRecord(person_schema, {"name": "Bob"})
        
        assert r1 == r2
        assert r1 != r3

    def test_hash(self, person_schema):
        r1 = GenericRecord(person_schema, {"name": "Alice"})
        r2 = GenericRecord(person_schema, {"name": "Alice"})
        assert hash(r1) == hash(r2)

    def test_repr(self, person_schema):
        record = GenericRecord(person_schema, {"name": "Alice"})
        repr_str = repr(record)
        assert "GenericRecord" in repr_str
        assert "Person" in repr_str


class TestGenericRecordBuilder:
    """Tests for GenericRecordBuilder."""

    def test_build_simple(self):
        record = (GenericRecordBuilder("Person")
                  .set_string("name", "Alice")
                  .set_int32("age", 30)
                  .build())
        
        assert record.type_name == "Person"
        assert record.get_string("name") == "Alice"
        assert record.get_int32("age") == 30

    def test_build_with_arrays(self):
        record = (GenericRecordBuilder("Data")
                  .set_array_of_int32("values", [1, 2, 3])
                  .set_array_of_string("names", ["a", "b"])
                  .build())
        
        assert record.get_array_of_int32("values") == [1, 2, 3]
        assert record.get_array_of_string("names") == ["a", "b"]

    def test_build_with_nullable(self):
        record = (GenericRecordBuilder("Data")
                  .set_nullable_int32("count", None)
                  .set_nullable_boolean("flag", True)
                  .build())
        
        assert record.get_nullable_int32("count") is None
        assert record.get_nullable_boolean("flag") is True

    def test_from_record(self):
        original_schema = Schema(type_name="Person")
        original_schema.add_field("name", "string")
        original = GenericRecord(original_schema, {"name": "Alice"})
        
        builder = GenericRecordBuilder("Person").from_record(original)
        builder.set_int32("age", 30)
        record = builder.build()
        
        assert record.get_string("name") == "Alice"


class TestCompactSerializationService:
    """Tests for CompactSerializationService."""

    @pytest.fixture
    def service(self):
        return CompactSerializationService()

    def test_serialize_and_deserialize(self, service):
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
                    name=reader.read_string("name"),
                    age=reader.read_int32("age"),
                )
        
        service.register_serializer(PersonSerializer())
        
        person = Person(name="Alice", age=30)
        data = service.serialize(person)
        result = service.deserialize(data, "Person")
        
        assert result.name == "Alice"
        assert result.age == 30

    def test_to_generic_record(self, service):
        @dataclass
        class Simple:
            value: int
        
        class SimpleSerializer(CompactSerializer[Simple]):
            @property
            def type_name(self) -> str:
                return "Simple"
            
            @property
            def clazz(self) -> type:
                return Simple
            
            def write(self, writer: CompactWriter, obj: Simple) -> None:
                writer.write_int32("value", obj.value)
            
            def read(self, reader: CompactReader) -> Simple:
                return Simple(value=reader.read_int32("value"))
        
        service.register_serializer(SimpleSerializer())
        
        data = service.serialize(Simple(value=42))
        record = service.to_generic_record(data, "Simple")
        
        assert isinstance(record, GenericRecord)

    def test_get_serializer(self, service):
        class DummySerializer(CompactSerializer):
            @property
            def type_name(self) -> str:
                return "Dummy"
            
            @property
            def clazz(self) -> type:
                return dict
            
            def write(self, writer, obj): pass
            def read(self, reader): return {}
        
        serializer = DummySerializer()
        service.register_serializer(serializer)
        
        assert service.get_serializer("Dummy") is serializer
        assert service.get_serializer("Unknown") is None


class TestReflectiveCompactSerializer:
    """Tests for ReflectiveCompactSerializer."""

    def test_with_dataclass(self):
        @dataclass
        class Point:
            x: int
            y: int
        
        serializer = ReflectiveCompactSerializer(Point)
        
        assert serializer.type_name == "Point"
        assert serializer.clazz is Point
