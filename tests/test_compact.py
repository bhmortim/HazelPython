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

    def test_all_field_kinds_have_defaults(self):
        for kind in FieldKind:
            assert kind in FIELD_KIND_DEFAULTS

    def test_nullable_field_kinds(self):
        assert FieldKind.NULLABLE_BOOLEAN == 18
        assert FieldKind.NULLABLE_INT8 == 19
        assert FieldKind.NULLABLE_INT32 == 21
        assert FieldKind.NULLABLE_FLOAT64 == 24

    def test_array_field_kinds(self):
        assert FieldKind.ARRAY_BOOLEAN == 9
        assert FieldKind.ARRAY_INT8 == 10
        assert FieldKind.ARRAY_STRING == 16
        assert FieldKind.ARRAY_COMPACT == 17


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

    def test_write_int8(self, writer):
        writer.write_int8("byte_val", 127)
        assert writer.fields["byte_val"] == 127
        assert writer.field_types["byte_val"] == "int8"

    def test_write_int16(self, writer):
        writer.write_int16("short_val", 32000)
        assert writer.fields["short_val"] == 32000
        assert writer.field_types["short_val"] == "int16"

    def test_write_int32(self, writer):
        writer.write_int32("count", 42)
        assert writer.fields["count"] == 42
        assert writer.field_types["count"] == "int32"

    def test_write_int64(self, writer):
        writer.write_int64("big_num", 9999999999)
        assert writer.fields["big_num"] == 9999999999
        assert writer.field_types["big_num"] == "int64"

    def test_write_float32(self, writer):
        writer.write_float32("ratio", 3.14)
        assert abs(writer.fields["ratio"] - 3.14) < 0.001
        assert writer.field_types["ratio"] == "float32"

    def test_write_float64(self, writer):
        writer.write_float64("precise", 3.141592653589793)
        assert writer.fields["precise"] == 3.141592653589793
        assert writer.field_types["precise"] == "float64"

    def test_write_string(self, writer):
        writer.write_string("name", "Alice")
        assert writer.fields["name"] == "Alice"
        assert writer.field_types["name"] == "string"

    def test_write_compact(self, writer):
        nested = {"inner": "value"}
        writer.write_compact("nested", nested)
        assert writer.fields["nested"] == nested
        assert writer.field_types["nested"] == "compact"

    def test_write_array_of_boolean(self, writer):
        writer.write_array_of_boolean("flags", [True, False, True])
        assert writer.fields["flags"] == [True, False, True]
        assert writer.field_types["flags"] == "array_boolean"

    def test_write_array_of_int32(self, writer):
        writer.write_array_of_int32("numbers", [1, 2, 3])
        assert writer.fields["numbers"] == [1, 2, 3]
        assert writer.field_types["numbers"] == "array_int32"

    def test_write_array_of_string(self, writer):
        writer.write_array_of_string("names", ["a", "b", "c"])
        assert writer.fields["names"] == ["a", "b", "c"]
        assert writer.field_types["names"] == "array_string"

    def test_write_nullable_boolean(self, writer):
        writer.write_nullable_boolean("flag", True)
        assert writer.fields["flag"] is True
        assert writer.field_types["flag"] == "nullable_boolean"

    def test_write_nullable_boolean_none(self, writer):
        writer.write_nullable_boolean("flag", None)
        assert writer.fields["flag"] is None
        assert writer.field_types["flag"] == "nullable_boolean"

    def test_write_nullable_int32(self, writer):
        writer.write_nullable_int32("count", 42)
        assert writer.fields["count"] == 42
        assert writer.field_types["count"] == "nullable_int32"

    def test_write_nullable_int32_none(self, writer):
        writer.write_nullable_int32("count", None)
        assert writer.fields["count"] is None
        assert writer.field_types["count"] == "nullable_int32"

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

    def test_read_boolean(self):
        schema = Schema(type_name="Test")
        schema.add_field("flag", "boolean")
        reader = DefaultCompactReader(schema, {"flag": True})
        assert reader.read_boolean("flag") is True

    def test_read_boolean_default(self):
        schema = Schema(type_name="Test")
        schema.add_field("flag", "boolean")
        reader = DefaultCompactReader(schema, {})
        assert reader.read_boolean("flag") is False

    def test_read_int8(self):
        schema = Schema(type_name="Test")
        schema.add_field("val", "int8")
        reader = DefaultCompactReader(schema, {"val": 42})
        assert reader.read_int8("val") == 42

    def test_read_int16(self):
        schema = Schema(type_name="Test")
        schema.add_field("val", "int16")
        reader = DefaultCompactReader(schema, {"val": 1000})
        assert reader.read_int16("val") == 1000

    def test_read_int64(self):
        schema = Schema(type_name="Test")
        schema.add_field("val", "int64")
        reader = DefaultCompactReader(schema, {"val": 9999999999})
        assert reader.read_int64("val") == 9999999999

    def test_read_float32(self):
        schema = Schema(type_name="Test")
        schema.add_field("val", "float32")
        reader = DefaultCompactReader(schema, {"val": 3.14})
        assert abs(reader.read_float32("val") - 3.14) < 0.001

    def test_read_float64(self):
        schema = Schema(type_name="Test")
        schema.add_field("val", "float64")
        reader = DefaultCompactReader(schema, {"val": 3.14159265359})
        assert reader.read_float64("val") == 3.14159265359

    def test_read_compact(self):
        schema = Schema(type_name="Test")
        schema.add_field("nested", "compact")
        nested = {"inner": "value"}
        reader = DefaultCompactReader(schema, {"nested": nested})
        assert reader.read_compact("nested") == nested

    def test_read_array_of_int8(self):
        schema = Schema(type_name="Test")
        schema.add_field("vals", "array_int8")
        reader = DefaultCompactReader(schema, {"vals": [1, 2, 3]})
        assert reader.read_array_of_int8("vals") == [1, 2, 3]

    def test_read_array_of_int16(self):
        schema = Schema(type_name="Test")
        schema.add_field("vals", "array_int16")
        reader = DefaultCompactReader(schema, {"vals": [100, 200]})
        assert reader.read_array_of_int16("vals") == [100, 200]

    def test_read_array_of_int64(self):
        schema = Schema(type_name="Test")
        schema.add_field("vals", "array_int64")
        reader = DefaultCompactReader(schema, {"vals": [9999999999]})
        assert reader.read_array_of_int64("vals") == [9999999999]

    def test_read_array_of_float32(self):
        schema = Schema(type_name="Test")
        schema.add_field("vals", "array_float32")
        reader = DefaultCompactReader(schema, {"vals": [1.1, 2.2]})
        assert reader.read_array_of_float32("vals") == [1.1, 2.2]

    def test_read_array_of_float64(self):
        schema = Schema(type_name="Test")
        schema.add_field("vals", "array_float64")
        reader = DefaultCompactReader(schema, {"vals": [1.111, 2.222]})
        assert reader.read_array_of_float64("vals") == [1.111, 2.222]

    def test_read_array_of_compact(self):
        schema = Schema(type_name="Test")
        schema.add_field("items", "array_compact")
        items = [{"a": 1}, {"b": 2}]
        reader = DefaultCompactReader(schema, {"items": items})
        assert reader.read_array_of_compact("items") == items

    def test_read_nullable_int8(self):
        schema = Schema(type_name="Test")
        schema.add_field("val", "nullable_int8")
        reader = DefaultCompactReader(schema, {"val": 42})
        assert reader.read_nullable_int8("val") == 42

    def test_read_nullable_int8_none(self):
        schema = Schema(type_name="Test")
        schema.add_field("val", "nullable_int8")
        reader = DefaultCompactReader(schema, {})
        assert reader.read_nullable_int8("val") is None

    def test_read_nullable_int16(self):
        schema = Schema(type_name="Test")
        schema.add_field("val", "nullable_int16")
        reader = DefaultCompactReader(schema, {"val": 1000})
        assert reader.read_nullable_int16("val") == 1000

    def test_read_nullable_int64(self):
        schema = Schema(type_name="Test")
        schema.add_field("val", "nullable_int64")
        reader = DefaultCompactReader(schema, {"val": 9999999999})
        assert reader.read_nullable_int64("val") == 9999999999

    def test_read_nullable_float32(self):
        schema = Schema(type_name="Test")
        schema.add_field("val", "nullable_float32")
        reader = DefaultCompactReader(schema, {"val": 3.14})
        assert abs(reader.read_nullable_float32("val") - 3.14) < 0.001

    def test_read_nullable_float64(self):
        schema = Schema(type_name="Test")
        schema.add_field("val", "nullable_float64")
        reader = DefaultCompactReader(schema, {"val": 3.14159})
        assert reader.read_nullable_float64("val") == 3.14159

    def test_get_field_kind(self):
        schema = Schema(type_name="Test")
        schema.add_field("name", "string")
        schema.add_field("count", "int32")
        reader = DefaultCompactReader(schema, {})
        
        assert reader.get_field_kind("name") == FieldKind.STRING
        assert reader.get_field_kind("count") == FieldKind.INT32
        assert reader.get_field_kind("missing") is None


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

    @pytest.fixture
    def extended_schema(self):
        schema = Schema(type_name="Extended")
        schema.add_field("int8_val", "int8")
        schema.add_field("int16_val", "int16")
        schema.add_field("int64_val", "int64")
        schema.add_field("float32_val", "float32")
        schema.add_field("float64_val", "float64")
        schema.add_field("bool_val", "boolean")
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

    def test_get_int8(self, extended_schema):
        record = GenericRecord(extended_schema, {"int8_val": 42})
        assert record.get_int8("int8_val") == 42

    def test_get_int16(self, extended_schema):
        record = GenericRecord(extended_schema, {"int16_val": 1000})
        assert record.get_int16("int16_val") == 1000

    def test_get_int64(self, extended_schema):
        record = GenericRecord(extended_schema, {"int64_val": 9999999999})
        assert record.get_int64("int64_val") == 9999999999

    def test_get_float32(self, extended_schema):
        record = GenericRecord(extended_schema, {"float32_val": 3.14})
        assert abs(record.get_float32("float32_val") - 3.14) < 0.001

    def test_get_float64(self, extended_schema):
        record = GenericRecord(extended_schema, {"float64_val": 3.14159265359})
        assert record.get_float64("float64_val") == 3.14159265359

    def test_get_boolean(self, extended_schema):
        record = GenericRecord(extended_schema, {"bool_val": True})
        assert record.get_boolean("bool_val") is True

    def test_get_field_wrong_type(self, person_schema):
        record = GenericRecord(person_schema, {"name": "Alice"})
        with pytest.raises(TypeError):
            record.get_int32("name")

    def test_get_nonexistent_field(self, person_schema):
        record = GenericRecord(person_schema, {})
        with pytest.raises(ValueError):
            record.get_string("missing")

    def test_get_array_of_int32(self):
        schema = Schema(type_name="Arrays")
        schema.add_field("ints", "array_int32")
        record = GenericRecord(schema, {"ints": [1, 2, 3]})
        assert record.get_array_of_int32("ints") == [1, 2, 3]

    def test_get_array_of_int64(self):
        schema = Schema(type_name="Arrays")
        schema.add_field("longs", "array_int64")
        record = GenericRecord(schema, {"longs": [999999999, 888888888]})
        assert record.get_array_of_int64("longs") == [999999999, 888888888]

    def test_get_array_of_float32(self):
        schema = Schema(type_name="Arrays")
        schema.add_field("floats", "array_float32")
        record = GenericRecord(schema, {"floats": [1.1, 2.2]})
        assert record.get_array_of_float32("floats") == [1.1, 2.2]

    def test_get_array_of_float64(self):
        schema = Schema(type_name="Arrays")
        schema.add_field("doubles", "array_float64")
        record = GenericRecord(schema, {"doubles": [1.111, 2.222]})
        assert record.get_array_of_float64("doubles") == [1.111, 2.222]

    def test_get_array_of_boolean(self):
        schema = Schema(type_name="Arrays")
        schema.add_field("flags", "array_boolean")
        record = GenericRecord(schema, {"flags": [True, False, True]})
        assert record.get_array_of_boolean("flags") == [True, False, True]

    def test_get_array_of_string(self):
        schema = Schema(type_name="Arrays")
        schema.add_field("names", "array_string")
        record = GenericRecord(schema, {"names": ["a", "b"]})
        assert record.get_array_of_string("names") == ["a", "b"]

    def test_get_nullable_boolean(self):
        schema = Schema(type_name="Nullable")
        schema.add_field("flag", "nullable_boolean")
        record = GenericRecord(schema, {"flag": True})
        assert record.get_nullable_boolean("flag") is True

    def test_get_nullable_boolean_none(self):
        schema = Schema(type_name="Nullable")
        schema.add_field("flag", "nullable_boolean")
        record = GenericRecord(schema, {"flag": None})
        assert record.get_nullable_boolean("flag") is None

    def test_get_nullable_int32(self):
        schema = Schema(type_name="Nullable")
        schema.add_field("val", "nullable_int32")
        record = GenericRecord(schema, {"val": 42})
        assert record.get_nullable_int32("val") == 42

    def test_get_nullable_int64(self):
        schema = Schema(type_name="Nullable")
        schema.add_field("val", "nullable_int64")
        record = GenericRecord(schema, {"val": 9999999999})
        assert record.get_nullable_int64("val") == 9999999999

    def test_get_compact(self):
        outer_schema = Schema(type_name="Outer")
        outer_schema.add_field("inner", "compact")
        inner_schema = Schema(type_name="Inner")
        inner_schema.add_field("value", "int32")
        inner = GenericRecord(inner_schema, {"value": 42})
        record = GenericRecord(outer_schema, {"inner": inner})
        assert record.get_compact("inner") == inner

    def test_get_array_of_compact(self):
        schema = Schema(type_name="Container")
        schema.add_field("items", "array_compact")
        inner_schema = Schema(type_name="Item")
        inner_schema.add_field("id", "int32")
        items = [
            GenericRecord(inner_schema, {"id": 1}),
            GenericRecord(inner_schema, {"id": 2}),
        ]
        record = GenericRecord(schema, {"items": items})
        assert record.get_array_of_compact("items") == items

    def test_to_dict(self, person_schema):
        record = GenericRecord(person_schema, {"name": "Alice", "age": 30})
        d = record.to_dict()
        assert d == {"name": "Alice", "age": 30}

    def test_to_dict_with_nested(self):
        outer_schema = Schema(type_name="Outer")
        outer_schema.add_field("inner", "compact")
        inner_schema = Schema(type_name="Inner")
        inner_schema.add_field("value", "int32")
        inner = GenericRecord(inner_schema, {"value": 42})
        record = GenericRecord(outer_schema, {"inner": inner})
        d = record.to_dict()
        assert d == {"inner": {"value": 42}}

    def test_to_dict_with_nested_array(self):
        schema = Schema(type_name="Container")
        schema.add_field("items", "array_compact")
        inner_schema = Schema(type_name="Item")
        inner_schema.add_field("id", "int32")
        items = [
            GenericRecord(inner_schema, {"id": 1}),
            GenericRecord(inner_schema, {"id": 2}),
        ]
        record = GenericRecord(schema, {"items": items})
        d = record.to_dict()
        assert d == {"items": [{"id": 1}, {"id": 2}]}

    def test_equality(self, person_schema):
        r1 = GenericRecord(person_schema, {"name": "Alice"})
        r2 = GenericRecord(person_schema, {"name": "Alice"})
        r3 = GenericRecord(person_schema, {"name": "Bob"})
        
        assert r1 == r2
        assert r1 != r3

    def test_equality_different_type(self, person_schema):
        record = GenericRecord(person_schema, {"name": "Alice"})
        assert record != "not a record"

    def test_hash(self, person_schema):
        r1 = GenericRecord(person_schema, {"name": "Alice"})
        r2 = GenericRecord(person_schema, {"name": "Alice"})
        assert hash(r1) == hash(r2)

    def test_repr(self, person_schema):
        record = GenericRecord(person_schema, {"name": "Alice"})
        repr_str = repr(record)
        assert "GenericRecord" in repr_str
        assert "Person" in repr_str

    def test_new_builder(self, person_schema):
        record = GenericRecord(person_schema, {"name": "Alice", "age": 30})
        builder = record.new_builder()
        builder.set_string("name", "Bob")
        new_record = builder.build()
        assert new_record.get_string("name") == "Bob"

    def test_schema_property(self, person_schema):
        record = GenericRecord(person_schema, {"name": "Alice"})
        assert record.schema is person_schema

    def test_get_field_names(self, person_schema):
        record = GenericRecord(person_schema, {})
        names = record.get_field_names()
        assert "name" in names
        assert "age" in names


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

    def test_build_with_int8(self):
        record = GenericRecordBuilder("Data").set_int8("val", 127).build()
        assert record.get_int8("val") == 127

    def test_build_with_int16(self):
        record = GenericRecordBuilder("Data").set_int16("val", 32000).build()
        assert record.get_int16("val") == 32000

    def test_build_with_int64(self):
        record = GenericRecordBuilder("Data").set_int64("val", 9999999999).build()
        assert record.get_int64("val") == 9999999999

    def test_build_with_float32(self):
        record = GenericRecordBuilder("Data").set_float32("val", 3.14).build()
        assert abs(record.get_float32("val") - 3.14) < 0.001

    def test_build_with_float64(self):
        record = GenericRecordBuilder("Data").set_float64("val", 3.14159265359).build()
        assert record.get_float64("val") == 3.14159265359

    def test_build_with_boolean(self):
        record = GenericRecordBuilder("Data").set_boolean("flag", True).build()
        assert record.get_boolean("flag") is True

    def test_build_with_arrays(self):
        record = (GenericRecordBuilder("Data")
                  .set_array_of_int32("values", [1, 2, 3])
                  .set_array_of_string("names", ["a", "b"])
                  .build())
        
        assert record.get_array_of_int32("values") == [1, 2, 3]
        assert record.get_array_of_string("names") == ["a", "b"]

    def test_build_with_array_of_int8(self):
        record = GenericRecordBuilder("Data").set_array_of_int8("vals", [1, 2, 3]).build()
        schema = record.schema
        assert schema.get_field("vals").field_type == "array_int8"

    def test_build_with_array_of_int16(self):
        record = GenericRecordBuilder("Data").set_array_of_int16("vals", [100, 200]).build()
        schema = record.schema
        assert schema.get_field("vals").field_type == "array_int16"

    def test_build_with_array_of_int64(self):
        record = GenericRecordBuilder("Data").set_array_of_int64("vals", [9999999999]).build()
        schema = record.schema
        assert schema.get_field("vals").field_type == "array_int64"

    def test_build_with_array_of_float32(self):
        record = GenericRecordBuilder("Data").set_array_of_float32("vals", [1.1, 2.2]).build()
        schema = record.schema
        assert schema.get_field("vals").field_type == "array_float32"

    def test_build_with_array_of_float64(self):
        record = GenericRecordBuilder("Data").set_array_of_float64("vals", [1.111, 2.222]).build()
        schema = record.schema
        assert schema.get_field("vals").field_type == "array_float64"

    def test_build_with_array_of_boolean(self):
        record = GenericRecordBuilder("Data").set_array_of_boolean("flags", [True, False]).build()
        assert record.get_array_of_boolean("flags") == [True, False]

    def test_build_with_nullable(self):
        record = (GenericRecordBuilder("Data")
                  .set_nullable_int32("count", None)
                  .set_nullable_boolean("flag", True)
                  .build())
        
        assert record.get_nullable_int32("count") is None
        assert record.get_nullable_boolean("flag") is True

    def test_build_with_nullable_int8(self):
        record = GenericRecordBuilder("Data").set_nullable_int8("val", 42).build()
        schema = record.schema
        assert schema.get_field("val").field_type == "nullable_int8"

    def test_build_with_nullable_int16(self):
        record = GenericRecordBuilder("Data").set_nullable_int16("val", 1000).build()
        schema = record.schema
        assert schema.get_field("val").field_type == "nullable_int16"

    def test_build_with_nullable_int64(self):
        record = GenericRecordBuilder("Data").set_nullable_int64("val", 9999999999).build()
        schema = record.schema
        assert schema.get_field("val").field_type == "nullable_int64"

    def test_build_with_nullable_float32(self):
        record = GenericRecordBuilder("Data").set_nullable_float32("val", 3.14).build()
        schema = record.schema
        assert schema.get_field("val").field_type == "nullable_float32"

    def test_build_with_nullable_float64(self):
        record = GenericRecordBuilder("Data").set_nullable_float64("val", 3.14159).build()
        schema = record.schema
        assert schema.get_field("val").field_type == "nullable_float64"

    def test_build_with_compact(self):
        inner_schema = Schema(type_name="Inner")
        inner_schema.add_field("value", "int32")
        inner = GenericRecord(inner_schema, {"value": 42})
        
        record = GenericRecordBuilder("Outer").set_compact("inner", inner).build()
        assert record.get_compact("inner") == inner

    def test_build_with_array_of_compact(self):
        inner_schema = Schema(type_name="Item")
        inner_schema.add_field("id", "int32")
        items = [
            GenericRecord(inner_schema, {"id": 1}),
            GenericRecord(inner_schema, {"id": 2}),
        ]
        
        record = GenericRecordBuilder("Container").set_array_of_compact("items", items).build()
        assert record.get_array_of_compact("items") == items

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

    def test_get_serializer_for_class(self, service):
        @dataclass
        class MyClass:
            value: int
        
        class MySerializer(CompactSerializer[MyClass]):
            @property
            def type_name(self) -> str:
                return "MyClass"
            
            @property
            def clazz(self) -> type:
                return MyClass
            
            def write(self, writer, obj): pass
            def read(self, reader): return MyClass(value=0)
        
        serializer = MySerializer()
        service.register_serializer(serializer)
        
        assert service.get_serializer_for_class(MyClass) is serializer
        assert service.get_serializer_for_class(str) is None

    def test_schema_service_property(self, service):
        assert service.schema_service is not None
        assert isinstance(service.schema_service, SchemaService)

    def test_register_and_get_schema(self, service):
        schema = Schema(type_name="Test")
        schema.add_field("value", "int32")
        
        service.register_schema(schema)
        
        found = service.get_schema(schema.schema_id)
        assert found is schema

    def test_serialize_no_serializer(self, service):
        class UnknownClass:
            pass
        
        with pytest.raises(ValueError, match="No compact serializer"):
            service.serialize(UnknownClass())

    def test_deserialize_no_serializer(self, service):
        with pytest.raises(ValueError, match="No compact serializer"):
            service.deserialize(b"\x00\x00\x00\x00", "Unknown")

    def test_infer_field_type_none(self, service):
        assert service._infer_field_type(None) == "compact"

    def test_infer_field_type_bool(self, service):
        assert service._infer_field_type(True) == "boolean"
        assert service._infer_field_type(False) == "boolean"

    def test_infer_field_type_int(self, service):
        assert service._infer_field_type(42) == "int64"

    def test_infer_field_type_float(self, service):
        assert service._infer_field_type(3.14) == "float64"

    def test_infer_field_type_string(self, service):
        assert service._infer_field_type("hello") == "string"

    def test_infer_field_type_empty_list(self, service):
        assert service._infer_field_type([]) == "array_int32"

    def test_infer_field_type_bool_list(self, service):
        assert service._infer_field_type([True, False]) == "array_boolean"

    def test_infer_field_type_int_list(self, service):
        assert service._infer_field_type([1, 2, 3]) == "array_int32"

    def test_infer_field_type_string_list(self, service):
        assert service._infer_field_type(["a", "b"]) == "array_string"

    def test_serialize_with_nullable_fields(self, service):
        @dataclass
        class WithNullable:
            value: int
            optional: int
        
        class WithNullableSerializer(CompactSerializer[WithNullable]):
            @property
            def type_name(self) -> str:
                return "WithNullable"
            
            @property
            def clazz(self) -> type:
                return WithNullable
            
            def write(self, writer: CompactWriter, obj: WithNullable) -> None:
                writer.write_int32("value", obj.value)
                writer.write_nullable_int32("optional", obj.optional)
            
            def read(self, reader: CompactReader) -> WithNullable:
                return WithNullable(
                    value=reader.read_int32("value"),
                    optional=reader.read_nullable_int32("optional"),
                )
        
        service.register_serializer(WithNullableSerializer())
        
        obj = WithNullable(value=42, optional=None)
        data = service.serialize(obj)
        result = service.deserialize(data, "WithNullable")
        
        assert result.value == 42
        assert result.optional is None

    def test_serialize_with_arrays(self, service):
        @dataclass
        class WithArrays:
            ints: list
            strings: list
        
        class WithArraysSerializer(CompactSerializer[WithArrays]):
            @property
            def type_name(self) -> str:
                return "WithArrays"
            
            @property
            def clazz(self) -> type:
                return WithArrays
            
            def write(self, writer: CompactWriter, obj: WithArrays) -> None:
                writer.write_array_of_int32("ints", obj.ints)
                writer.write_array_of_string("strings", obj.strings)
            
            def read(self, reader: CompactReader) -> WithArrays:
                return WithArrays(
                    ints=reader.read_array_of_int32("ints"),
                    strings=reader.read_array_of_string("strings"),
                )
        
        service.register_serializer(WithArraysSerializer())
        
        obj = WithArrays(ints=[1, 2, 3], strings=["a", "b"])
        data = service.serialize(obj)
        result = service.deserialize(data, "WithArrays")
        
        assert result.ints == [1, 2, 3]
        assert result.strings == ["a", "b"]

    def test_compact_type_id(self, service):
        assert CompactSerializationService.COMPACT_TYPE_ID == -55


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

    def test_with_custom_type_name(self):
        @dataclass
        class Point:
            x: int
            y: int
        
        serializer = ReflectiveCompactSerializer(Point, "CustomPoint")
        assert serializer.type_name == "CustomPoint"

    def test_write_dataclass(self):
        @dataclass
        class Person:
            name: str
            age: int
            active: bool
            score: float
        
        serializer = ReflectiveCompactSerializer(Person)
        schema = Schema(type_name="Person")
        writer = DefaultCompactWriter(schema)
        
        person = Person(name="Alice", age=30, active=True, score=95.5)
        serializer.write(writer, person)
        
        assert writer.fields["name"] == "Alice"
        assert writer.fields["age"] == 30
        assert writer.fields["active"] is True

    def test_read_dataclass(self):
        @dataclass
        class Person:
            name: str
            age: int
        
        serializer = ReflectiveCompactSerializer(Person)
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")
        schema.add_field("age", "int32")
        
        reader = DefaultCompactReader(schema, {"name": "Alice", "age": 30})
        result = serializer.read(reader)
        
        assert result.name == "Alice"
        assert result.age == 30

    def test_with_non_dataclass(self):
        class RegularClass:
            def __init__(self):
                self.name = "test"
                self.value = 42
        
        serializer = ReflectiveCompactSerializer(RegularClass)
        schema = Schema(type_name="RegularClass")
        writer = DefaultCompactWriter(schema)
        
        obj = RegularClass()
        serializer.write(writer, obj)
        
        assert writer.fields["name"] == "test"
        assert writer.fields["value"] == 42

    def test_write_with_compact_field(self):
        @dataclass
        class Outer:
            inner: object
        
        serializer = ReflectiveCompactSerializer(Outer)
        schema = Schema(type_name="Outer")
        writer = DefaultCompactWriter(schema)
        
        outer = Outer(inner={"nested": "value"})
        serializer.write(writer, outer)
        
        assert writer.fields["inner"] == {"nested": "value"}


class TestGenericRecordSerializer:
    """Tests for GenericRecordSerializer."""

    def test_init(self):
        serializer = GenericRecordSerializer("TestType")
        assert serializer.type_name == "TestType"
        assert serializer.clazz is GenericRecord

    def test_write_boolean(self):
        serializer = GenericRecordSerializer("Test")
        schema = Schema(type_name="Test")
        schema.add_field("flag", "boolean")
        record = GenericRecord(schema, {"flag": True})
        
        writer = DefaultCompactWriter(Schema(type_name="Test"))
        serializer.write(writer, record)
        
        assert writer.fields["flag"] is True

    def test_write_integers(self):
        serializer = GenericRecordSerializer("Test")
        schema = Schema(type_name="Test")
        schema.add_field("int8_val", "int8")
        schema.add_field("int16_val", "int16")
        schema.add_field("int32_val", "int32")
        schema.add_field("int64_val", "int64")
        record = GenericRecord(schema, {
            "int8_val": 1,
            "int16_val": 100,
            "int32_val": 10000,
            "int64_val": 9999999999,
        })
        
        writer = DefaultCompactWriter(Schema(type_name="Test"))
        serializer.write(writer, record)
        
        assert writer.fields["int8_val"] == 1
        assert writer.fields["int32_val"] == 10000

    def test_write_floats(self):
        serializer = GenericRecordSerializer("Test")
        schema = Schema(type_name="Test")
        schema.add_field("float32_val", "float32")
        schema.add_field("float64_val", "float64")
        record = GenericRecord(schema, {
            "float32_val": 3.14,
            "float64_val": 3.14159265359,
        })
        
        writer = DefaultCompactWriter(Schema(type_name="Test"))
        serializer.write(writer, record)
        
        assert abs(writer.fields["float32_val"] - 3.14) < 0.001

    def test_write_string(self):
        serializer = GenericRecordSerializer("Test")
        schema = Schema(type_name="Test")
        schema.add_field("name", "string")
        record = GenericRecord(schema, {"name": "Alice"})
        
        writer = DefaultCompactWriter(Schema(type_name="Test"))
        serializer.write(writer, record)
        
        assert writer.fields["name"] == "Alice"

    def test_write_arrays(self):
        serializer = GenericRecordSerializer("Test")
        schema = Schema(type_name="Test")
        schema.add_field("bools", "array_boolean")
        schema.add_field("ints", "array_int32")
        schema.add_field("strings", "array_string")
        record = GenericRecord(schema, {
            "bools": [True, False],
            "ints": [1, 2, 3],
            "strings": ["a", "b"],
        })
        
        writer = DefaultCompactWriter(Schema(type_name="Test"))
        serializer.write(writer, record)
        
        assert writer.fields["bools"] == [True, False]
        assert writer.fields["ints"] == [1, 2, 3]
        assert writer.fields["strings"] == ["a", "b"]

    def test_read(self):
        serializer = GenericRecordSerializer("Test")
        schema = Schema(type_name="Test")
        schema.add_field("name", "string")
        schema.add_field("age", "int32")
        
        reader = DefaultCompactReader(schema, {"name": "Alice", "age": 30})
        result = serializer.read(reader)
        
        assert isinstance(result, GenericRecord)
        assert result.get_string("name") == "Alice"
        assert result.get_int32("age") == 30

    def test_infer_field_type_bool(self):
        serializer = GenericRecordSerializer("Test")
        assert serializer._infer_field_type(True) == "boolean"
        assert serializer._infer_field_type(False) == "boolean"

    def test_infer_field_type_int(self):
        serializer = GenericRecordSerializer("Test")
        assert serializer._infer_field_type(42) == "int32"

    def test_infer_field_type_float(self):
        serializer = GenericRecordSerializer("Test")
        assert serializer._infer_field_type(3.14) == "float64"

    def test_infer_field_type_string(self):
        serializer = GenericRecordSerializer("Test")
        assert serializer._infer_field_type("hello") == "string"

    def test_infer_field_type_generic_record(self):
        serializer = GenericRecordSerializer("Test")
        schema = Schema(type_name="Inner")
        record = GenericRecord(schema, {})
        assert serializer._infer_field_type(record) == "compact"

    def test_infer_field_type_list(self):
        serializer = GenericRecordSerializer("Test")
        assert serializer._infer_field_type([]) == "array_int32"
        assert serializer._infer_field_type([True, False]) == "array_boolean"
        assert serializer._infer_field_type([1, 2, 3]) == "array_int32"
        assert serializer._infer_field_type(["a", "b"]) == "array_string"

    def test_infer_field_type_list_of_records(self):
        serializer = GenericRecordSerializer("Test")
        schema = Schema(type_name="Inner")
        records = [GenericRecord(schema, {})]
        assert serializer._infer_field_type(records) == "array_compact"


class TestSchemaEvolution:
    """Tests for schema evolution scenarios."""

    @pytest.fixture
    def service(self):
        return CompactSerializationService()

    def test_read_with_newer_schema_additional_fields(self):
        old_schema = Schema(type_name="Person")
        old_schema.add_field("name", "string")
        
        new_schema = Schema(type_name="Person")
        new_schema.add_field("name", "string")
        new_schema.add_field("age", "int32")
        new_schema.add_field("email", "string")
        
        reader = DefaultCompactReader(old_schema, {"name": "Alice"}, new_schema)
        
        assert reader.read_string("name") == "Alice"

    def test_read_with_older_schema_missing_fields(self):
        new_schema = Schema(type_name="Person")
        new_schema.add_field("name", "string")
        new_schema.add_field("age", "int32")
        
        reader = DefaultCompactReader(new_schema, {"name": "Alice"})
        
        assert reader.read_string("name") == "Alice"
        assert reader.read_int32("age") == 0

    def test_schema_compatibility_same_fields(self, service):
        s1 = Schema(type_name="Person")
        s1.add_field("name", "string")
        s1.add_field("age", "int32")
        
        s2 = Schema(type_name="Person")
        s2.add_field("name", "string")
        s2.add_field("age", "int32")
        
        service.register_schema(s1)
        service.register_schema(s2)
        
        assert service.check_schema_compatibility(s1.schema_id, s2.schema_id) is True

    def test_schema_compatibility_added_field(self, service):
        s1 = Schema(type_name="Person")
        s1.add_field("name", "string")
        
        s2 = Schema(type_name="Person")
        s2.add_field("name", "string")
        s2.add_field("age", "int32")
        
        service.register_schema(s1)
        service.register_schema(s2)
        
        assert service.check_schema_compatibility(s1.schema_id, s2.schema_id) is True

    def test_schema_compatibility_type_change(self, service):
        s1 = Schema(type_name="Person")
        s1.add_field("age", "int32")
        
        s2 = Schema(type_name="Person")
        s2.add_field("age", "string")
        
        service.register_schema(s1)
        service.register_schema(s2)
        
        assert service.check_schema_compatibility(s1.schema_id, s2.schema_id) is False

    def test_schema_compatibility_unknown_schema(self, service):
        s1 = Schema(type_name="Person")
        service.register_schema(s1)
        
        assert service.check_schema_compatibility(s1.schema_id, 99999) is False
        assert service.check_schema_compatibility(99999, s1.schema_id) is False

    def test_deserialize_with_schema(self, service):
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
        
        reader_schema = Schema(type_name="Person")
        reader_schema.add_field("name", "string")
        reader_schema.add_field("age", "int32")
        
        result = service.deserialize_with_schema(data, "Person", reader_schema)
        
        assert result.name == "Alice"
        assert result.age == 30


class TestNestedCompactTypes:
    """Tests for nested compact types."""

    @pytest.fixture
    def service(self):
        return CompactSerializationService()

    def test_nested_generic_record(self):
        inner_schema = Schema(type_name="Address")
        inner_schema.add_field("city", "string")
        inner_schema.add_field("zip", "int32")
        
        outer_schema = Schema(type_name="Person")
        outer_schema.add_field("name", "string")
        outer_schema.add_field("address", "compact")
        
        inner = GenericRecord(inner_schema, {"city": "NYC", "zip": 10001})
        outer = GenericRecord(outer_schema, {"name": "Alice", "address": inner})
        
        assert outer.get_string("name") == "Alice"
        assert outer.get_compact("address").get_string("city") == "NYC"

    def test_array_of_generic_records(self):
        item_schema = Schema(type_name="Item")
        item_schema.add_field("id", "int32")
        item_schema.add_field("name", "string")
        
        container_schema = Schema(type_name="Container")
        container_schema.add_field("items", "array_compact")
        
        items = [
            GenericRecord(item_schema, {"id": 1, "name": "Item1"}),
            GenericRecord(item_schema, {"id": 2, "name": "Item2"}),
        ]
        container = GenericRecord(container_schema, {"items": items})
        
        result_items = container.get_array_of_compact("items")
        assert len(result_items) == 2
        assert result_items[0].get_int32("id") == 1
        assert result_items[1].get_string("name") == "Item2"

    def test_builder_with_nested(self):
        inner = (GenericRecordBuilder("Address")
                 .set_string("city", "Boston")
                 .set_int32("zip", 02101)
                 .build())
        
        outer = (GenericRecordBuilder("Person")
                 .set_string("name", "Bob")
                 .set_compact("address", inner)
                 .build())
        
        assert outer.get_compact("address").get_string("city") == "Boston"

    def test_to_dict_nested(self):
        inner = (GenericRecordBuilder("Address")
                 .set_string("city", "LA")
                 .build())
        
        outer = (GenericRecordBuilder("Person")
                 .set_string("name", "Carol")
                 .set_compact("home", inner)
                 .build())
        
        d = outer.to_dict()
        assert d["name"] == "Carol"
        assert d["home"]["city"] == "LA"
