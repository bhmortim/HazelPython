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

    def test_create_with_offset(self):
        fd = FieldDescriptor(name="age", field_type="int32", index=0, offset=16)
        assert fd.offset == 16

    def test_kind_property(self):
        fd = FieldDescriptor(name="name", field_type="string", index=0)
        assert fd.kind == FieldKind.STRING

    def test_kind_property_unknown_type(self):
        fd = FieldDescriptor(name="unknown", field_type="unknown_type", index=0)
        assert fd.kind == FieldKind.COMPACT

    def test_is_variable_size(self):
        string_fd = FieldDescriptor(name="s", field_type="string", index=0)
        int_fd = FieldDescriptor(name="i", field_type="int32", index=0)
        
        assert string_fd.is_variable_size() is True
        assert int_fd.is_variable_size() is False

    def test_is_variable_size_compact(self):
        fd = FieldDescriptor(name="c", field_type="compact", index=0)
        assert fd.is_variable_size() is True

    def test_is_variable_size_arrays(self):
        for array_type in ["array_boolean", "array_int8", "array_int16", "array_int32",
                           "array_int64", "array_float32", "array_float64", "array_string",
                           "array_compact"]:
            fd = FieldDescriptor(name="arr", field_type=array_type, index=0)
            assert fd.is_variable_size() is True, f"{array_type} should be variable size"

    def test_is_variable_size_decimal(self):
        fd = FieldDescriptor(name="d", field_type="decimal", index=0)
        assert fd.is_variable_size() is True

    def test_is_variable_size_date_arrays(self):
        for date_type in ["array_date", "array_time", "array_timestamp", "array_decimal"]:
            fd = FieldDescriptor(name="dt", field_type=date_type, index=0)
            assert fd.is_variable_size() is True, f"{date_type} should be variable size"

    def test_is_variable_size_fixed_types(self):
        for fixed_type in ["boolean", "int8", "int16", "int32", "int64", "float32", "float64"]:
            fd = FieldDescriptor(name="f", field_type=fixed_type, index=0)
            assert fd.is_variable_size() is False, f"{fixed_type} should be fixed size"

    def test_is_variable_size_nullable_fixed_types(self):
        for nullable_type in ["nullable_boolean", "nullable_int8", "nullable_int16",
                              "nullable_int32", "nullable_int64", "nullable_float32",
                              "nullable_float64"]:
            fd = FieldDescriptor(name="n", field_type=nullable_type, index=0)
            assert fd.is_variable_size() is False, f"{nullable_type} should be fixed size"


class TestSchema:
    """Tests for Schema."""

    def test_create_empty(self):
        schema = Schema(type_name="Person")
        assert schema.type_name == "Person"
        assert len(schema.fields) == 0

    def test_create_with_preset_schema_id(self):
        schema = Schema(type_name="Person", schema_id=12345)
        assert schema.schema_id == 12345

    def test_create_with_fields(self):
        fields = [
            FieldDescriptor(name="name", field_type="string", index=0),
            FieldDescriptor(name="age", field_type="int32", index=1),
        ]
        schema = Schema(type_name="Person", fields=fields)
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "name"

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

    def test_get_all_versions(self, service):
        s1 = Schema(type_name="Person")
        s1.add_field("name", "string")
        
        s2 = Schema(type_name="Person")
        s2.add_field("name", "string")
        s2.add_field("age", "int32")
        
        service.register(s1)
        service.register(s2)
        
        versions = service.get_all_versions("Person")
        assert len(versions) == 2
        assert s1 in versions
        assert s2 in versions

    def test_get_all_versions_empty(self, service):
        versions = service.get_all_versions("NonExistent")
        assert versions == []

    def test_register_same_schema_twice(self, service):
        schema = Schema(type_name="Person")
        service.register(schema)
        service.register(schema)
        
        versions = service.get_all_versions("Person")
        assert len(versions) == 1


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

    def test_write_array_of_int8(self, writer):
        writer.write_array_of_int8("bytes", [1, 2, 3])
        assert writer.fields["bytes"] == [1, 2, 3]
        assert writer.field_types["bytes"] == "array_int8"

    def test_write_array_of_int16(self, writer):
        writer.write_array_of_int16("shorts", [100, 200])
        assert writer.fields["shorts"] == [100, 200]
        assert writer.field_types["shorts"] == "array_int16"

    def test_write_array_of_int64(self, writer):
        writer.write_array_of_int64("longs", [9999999999])
        assert writer.fields["longs"] == [9999999999]
        assert writer.field_types["longs"] == "array_int64"

    def test_write_array_of_float32(self, writer):
        writer.write_array_of_float32("floats", [1.1, 2.2])
        assert writer.fields["floats"] == [1.1, 2.2]
        assert writer.field_types["floats"] == "array_float32"

    def test_write_array_of_float64(self, writer):
        writer.write_array_of_float64("doubles", [1.111, 2.222])
        assert writer.fields["doubles"] == [1.111, 2.222]
        assert writer.field_types["doubles"] == "array_float64"

    def test_write_array_of_compact(self, writer):
        items = [{"a": 1}, {"b": 2}]
        writer.write_array_of_compact("items", items)
        assert writer.fields["items"] == items
        assert writer.field_types["items"] == "array_compact"

    def test_write_nullable_int8(self, writer):
        writer.write_nullable_int8("val", 42)
        assert writer.fields["val"] == 42
        assert writer.field_types["val"] == "nullable_int8"

    def test_write_nullable_int16(self, writer):
        writer.write_nullable_int16("val", 1000)
        assert writer.fields["val"] == 1000
        assert writer.field_types["val"] == "nullable_int16"

    def test_write_nullable_int64(self, writer):
        writer.write_nullable_int64("val", 9999999999)
        assert writer.fields["val"] == 9999999999
        assert writer.field_types["val"] == "nullable_int64"

    def test_write_nullable_float32(self, writer):
        writer.write_nullable_float32("val", 3.14)
        assert abs(writer.fields["val"] - 3.14) < 0.001
        assert writer.field_types["val"] == "nullable_float32"

    def test_write_nullable_float64(self, writer):
        writer.write_nullable_float64("val", 3.14159)
        assert writer.fields["val"] == 3.14159
        assert writer.field_types["val"] == "nullable_float64"


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

    def test_write_and_read_bytes(self):
        writer = CompactStreamWriter()
        writer.write_bytes(b"\x01\x02\x03\x04")
        
        reader = CompactStreamReader(writer.to_bytes())
        data = reader.read_bytes(4)
        assert data == b"\x01\x02\x03\x04"

    def test_read_bytes_partial(self):
        writer = CompactStreamWriter()
        writer.write_bytes(b"\x01\x02\x03\x04\x05\x06")
        
        reader = CompactStreamReader(writer.to_bytes())
        first = reader.read_bytes(3)
        second = reader.read_bytes(3)
        assert first == b"\x01\x02\x03"
        assert second == b"\x04\x05\x06"

    def test_write_and_read_negative_integers(self):
        writer = CompactStreamWriter()
        writer.write_int8(-1)
        writer.write_int16(-100)
        writer.write_int32(-10000)
        writer.write_int64(-999999999)
        
        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_int8() == -1
        assert reader.read_int16() == -100
        assert reader.read_int32() == -10000
        assert reader.read_int64() == -999999999

    def test_write_and_read_unicode_string(self):
        writer = CompactStreamWriter()
        writer.write_string("こんにちは")
        writer.write_string("émoji: 🎉")
        
        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_string() == "こんにちは"
        assert reader.read_string() == "émoji: 🎉"

    def test_write_and_read_empty_string(self):
        writer = CompactStreamWriter()
        writer.write_string("")
        
        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_string() == ""


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

    def test_serialize_all_primitive_types(self, service):
        @dataclass
        class AllPrimitives:
            bool_val: bool
            int8_val: int
            int16_val: int
            int32_val: int
            int64_val: int
            float32_val: float
            float64_val: float
            string_val: str
        
        class AllPrimitivesSerializer(CompactSerializer[AllPrimitives]):
            @property
            def type_name(self) -> str:
                return "AllPrimitives"
            
            @property
            def clazz(self) -> type:
                return AllPrimitives
            
            def write(self, writer: CompactWriter, obj: AllPrimitives) -> None:
                writer.write_boolean("bool_val", obj.bool_val)
                writer.write_int8("int8_val", obj.int8_val)
                writer.write_int16("int16_val", obj.int16_val)
                writer.write_int32("int32_val", obj.int32_val)
                writer.write_int64("int64_val", obj.int64_val)
                writer.write_float32("float32_val", obj.float32_val)
                writer.write_float64("float64_val", obj.float64_val)
                writer.write_string("string_val", obj.string_val)
            
            def read(self, reader: CompactReader) -> AllPrimitives:
                return AllPrimitives(
                    bool_val=reader.read_boolean("bool_val"),
                    int8_val=reader.read_int8("int8_val"),
                    int16_val=reader.read_int16("int16_val"),
                    int32_val=reader.read_int32("int32_val"),
                    int64_val=reader.read_int64("int64_val"),
                    float32_val=reader.read_float32("float32_val"),
                    float64_val=reader.read_float64("float64_val"),
                    string_val=reader.read_string("string_val"),
                )
        
        service.register_serializer(AllPrimitivesSerializer())
        
        obj = AllPrimitives(
            bool_val=True,
            int8_val=127,
            int16_val=32000,
            int32_val=100000,
            int64_val=9999999999,
            float32_val=3.14,
            float64_val=2.718281828,
            string_val="test",
        )
        data = service.serialize(obj)
        result = service.deserialize(data, "AllPrimitives")
        
        assert result.bool_val is True
        assert result.int8_val == 127
        assert result.int16_val == 32000
        assert result.int32_val == 100000
        assert result.int64_val == 9999999999
        assert abs(result.float32_val - 3.14) < 0.01
        assert abs(result.float64_val - 2.718281828) < 0.0001
        assert result.string_val == "test"

    def test_serialize_all_nullable_types(self, service):
        @dataclass
        class AllNullables:
            nb: bool
            ni8: int
            ni16: int
            ni32: int
            ni64: int
            nf32: float
            nf64: float
        
        class AllNullablesSerializer(CompactSerializer[AllNullables]):
            @property
            def type_name(self) -> str:
                return "AllNullables"
            
            @property
            def clazz(self) -> type:
                return AllNullables
            
            def write(self, writer: CompactWriter, obj: AllNullables) -> None:
                writer.write_nullable_boolean("nb", obj.nb)
                writer.write_nullable_int8("ni8", obj.ni8)
                writer.write_nullable_int16("ni16", obj.ni16)
                writer.write_nullable_int32("ni32", obj.ni32)
                writer.write_nullable_int64("ni64", obj.ni64)
                writer.write_nullable_float32("nf32", obj.nf32)
                writer.write_nullable_float64("nf64", obj.nf64)
            
            def read(self, reader: CompactReader) -> AllNullables:
                return AllNullables(
                    nb=reader.read_nullable_boolean("nb"),
                    ni8=reader.read_nullable_int8("ni8"),
                    ni16=reader.read_nullable_int16("ni16"),
                    ni32=reader.read_nullable_int32("ni32"),
                    ni64=reader.read_nullable_int64("ni64"),
                    nf32=reader.read_nullable_float32("nf32"),
                    nf64=reader.read_nullable_float64("nf64"),
                )
        
        service.register_serializer(AllNullablesSerializer())
        
        obj = AllNullables(nb=None, ni8=None, ni16=100, ni32=None, ni64=999, nf32=None, nf64=1.5)
        data = service.serialize(obj)
        result = service.deserialize(data, "AllNullables")
        
        assert result.nb is None
        assert result.ni8 is None
        assert result.ni16 == 100
        assert result.ni32 is None
        assert result.ni64 == 999
        assert result.nf32 is None
        assert result.nf64 == 1.5

    def test_serialize_all_array_types(self, service):
        @dataclass
        class AllArrays:
            ab: list
            ai8: list
            ai16: list
            ai32: list
            ai64: list
            af32: list
            af64: list
            astr: list
        
        class AllArraysSerializer(CompactSerializer[AllArrays]):
            @property
            def type_name(self) -> str:
                return "AllArrays"
            
            @property
            def clazz(self) -> type:
                return AllArrays
            
            def write(self, writer: CompactWriter, obj: AllArrays) -> None:
                writer.write_array_of_boolean("ab", obj.ab)
                writer.write_array_of_int8("ai8", obj.ai8)
                writer.write_array_of_int16("ai16", obj.ai16)
                writer.write_array_of_int32("ai32", obj.ai32)
                writer.write_array_of_int64("ai64", obj.ai64)
                writer.write_array_of_float32("af32", obj.af32)
                writer.write_array_of_float64("af64", obj.af64)
                writer.write_array_of_string("astr", obj.astr)
            
            def read(self, reader: CompactReader) -> AllArrays:
                return AllArrays(
                    ab=reader.read_array_of_boolean("ab"),
                    ai8=reader.read_array_of_int8("ai8"),
                    ai16=reader.read_array_of_int16("ai16"),
                    ai32=reader.read_array_of_int32("ai32"),
                    ai64=reader.read_array_of_int64("ai64"),
                    af32=reader.read_array_of_float32("af32"),
                    af64=reader.read_array_of_float64("af64"),
                    astr=reader.read_array_of_string("astr"),
                )
        
        service.register_serializer(AllArraysSerializer())
        
        obj = AllArrays(
            ab=[True, False],
            ai8=[1, 2, 3],
            ai16=[100, 200],
            ai32=[1000, 2000],
            ai64=[9999999999],
            af32=[1.1, 2.2],
            af64=[1.111, 2.222],
            astr=["a", "b"],
        )
        data = service.serialize(obj)
        result = service.deserialize(data, "AllArrays")
        
        assert result.ab == [True, False]
        assert result.ai8 == [1, 2, 3]
        assert result.ai16 == [100, 200]
        assert result.ai32 == [1000, 2000]
        assert result.ai64 == [9999999999]
        assert result.astr == ["a", "b"]

    def test_serialize_null_arrays(self, service):
        @dataclass
        class NullArrays:
            ints: list
            strings: list
        
        class NullArraysSerializer(CompactSerializer[NullArrays]):
            @property
            def type_name(self) -> str:
                return "NullArrays"
            
            @property
            def clazz(self) -> type:
                return NullArrays
            
            def write(self, writer: CompactWriter, obj: NullArrays) -> None:
                writer.write_array_of_int32("ints", obj.ints)
                writer.write_array_of_string("strings", obj.strings)
            
            def read(self, reader: CompactReader) -> NullArrays:
                return NullArrays(
                    ints=reader.read_array_of_int32("ints"),
                    strings=reader.read_array_of_string("strings"),
                )
        
        service.register_serializer(NullArraysSerializer())
        
        obj = NullArrays(ints=None, strings=None)
        data = service.serialize(obj)
        result = service.deserialize(data, "NullArrays")
        
        assert result.ints is None
        assert result.strings is None

    def test_serialize_compact_field(self, service):
        @dataclass
        class Inner:
            value: int
        
        @dataclass
        class Outer:
            inner: object
        
        class InnerSerializer(CompactSerializer[Inner]):
            @property
            def type_name(self) -> str:
                return "Inner"
            
            @property
            def clazz(self) -> type:
                return Inner
            
            def write(self, writer: CompactWriter, obj: Inner) -> None:
                writer.write_int32("value", obj.value)
            
            def read(self, reader: CompactReader) -> Inner:
                return Inner(value=reader.read_int32("value"))
        
        class OuterSerializer(CompactSerializer[Outer]):
            @property
            def type_name(self) -> str:
                return "Outer"
            
            @property
            def clazz(self) -> type:
                return Outer
            
            def write(self, writer: CompactWriter, obj: Outer) -> None:
                writer.write_compact("inner", obj.inner)
            
            def read(self, reader: CompactReader) -> Outer:
                return Outer(inner=reader.read_compact("inner"))
        
        service.register_serializer(OuterSerializer())
        
        obj = Outer(inner=Inner(value=42))
        data = service.serialize(obj)
        result = service.deserialize(data, "Outer")
        
        assert result.inner is None

    def test_serialize_array_of_compact(self, service):
        @dataclass
        class Container:
            items: list
        
        class ContainerSerializer(CompactSerializer[Container]):
            @property
            def type_name(self) -> str:
                return "Container"
            
            @property
            def clazz(self) -> type:
                return Container
            
            def write(self, writer: CompactWriter, obj: Container) -> None:
                writer.write_array_of_compact("items", obj.items)
            
            def read(self, reader: CompactReader) -> Container:
                return Container(items=reader.read_array_of_compact("items"))
        
        service.register_serializer(ContainerSerializer())
        
        obj = Container(items=[{"a": 1}, {"b": 2}])
        data = service.serialize(obj)
        result = service.deserialize(data, "Container")
        
        assert result.items == [None, None]

    def test_serialize_with_unknown_field_type(self, service):
        @dataclass
        class Unknown:
            data: object
        
        class UnknownSerializer(CompactSerializer[Unknown]):
            @property
            def type_name(self) -> str:
                return "Unknown"
            
            @property
            def clazz(self) -> type:
                return Unknown
            
            def write(self, writer: CompactWriter, obj: Unknown) -> None:
                writer.write_compact("data", obj.data)
            
            def read(self, reader: CompactReader) -> Unknown:
                return Unknown(data=reader.read_compact("data"))
        
        service.register_serializer(UnknownSerializer())
        
        obj = Unknown(data=None)
        data = service.serialize(obj)
        result = service.deserialize(data, "Unknown")
        
        assert result.data is None


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

    def test_read_non_dataclass(self):
        class SimpleClass:
            pass
        
        serializer = ReflectiveCompactSerializer(SimpleClass)
        schema = Schema(type_name="SimpleClass")
        reader = DefaultCompactReader(schema, {})
        
        result = serializer.read(reader)
        assert isinstance(result, SimpleClass)

    def test_write_private_fields_ignored(self):
        class WithPrivate:
            def __init__(self):
                self.public = "visible"
                self._private = "hidden"
        
        serializer = ReflectiveCompactSerializer(WithPrivate)
        schema = Schema(type_name="WithPrivate")
        writer = DefaultCompactWriter(schema)
        
        obj = WithPrivate()
        serializer.write(writer, obj)
        
        assert "public" in writer.fields
        assert "_private" not in writer.fields

    def test_write_all_type_annotations(self):
        @dataclass
        class AllTypes:
            b: bool
            i: int
            f: float
            s: str
            o: object
        
        serializer = ReflectiveCompactSerializer(AllTypes)
        schema = Schema(type_name="AllTypes")
        writer = DefaultCompactWriter(schema)
        
        obj = AllTypes(b=True, i=42, f=3.14, s="test", o={"nested": True})
        serializer.write(writer, obj)
        
        assert writer.fields["b"] is True
        assert writer.fields["i"] == 42
        assert writer.fields["f"] == 3.14
        assert writer.fields["s"] == "test"
        assert writer.fields["o"] == {"nested": True}

    def test_read_all_type_annotations(self):
        @dataclass
        class AllTypes:
            b: bool
            i: int
            f: float
            s: str
            o: object
        
        serializer = ReflectiveCompactSerializer(AllTypes)
        schema = Schema(type_name="AllTypes")
        schema.add_field("b", "boolean")
        schema.add_field("i", "int32")
        schema.add_field("f", "float64")
        schema.add_field("s", "string")
        schema.add_field("o", "compact")
        
        reader = DefaultCompactReader(schema, {
            "b": True, "i": 42, "f": 3.14, "s": "test", "o": None
        })
        result = serializer.read(reader)
        
        assert result.b is True
        assert result.i == 42
        assert result.f == 3.14
        assert result.s == "test"
        assert result.o is None


class TestGenericRecordSerializer:
    """Tests for GenericRecordSerializer."""

    def test_init(self):
        serializer = GenericRecordSerializer("TestType")
        assert serializer.type_name == "TestType"
        assert serializer.clazz is GenericRecord

    def test_write_compact_field(self):
        serializer = GenericRecordSerializer("Test")
        inner_schema = Schema(type_name="Inner")
        inner_schema.add_field("value", "int32")
        inner = GenericRecord(inner_schema, {"value": 42})
        
        outer_schema = Schema(type_name="Test")
        outer_schema.add_field("nested", "compact")
        record = GenericRecord(outer_schema, {"nested": inner})
        
        writer = DefaultCompactWriter(Schema(type_name="Test"))
        serializer.write(writer, record)
        
        assert writer.fields["nested"] == inner

    def test_write_nullable_fields(self):
        serializer = GenericRecordSerializer("Test")
        schema = Schema(type_name="Test")
        schema.add_field("nb", "nullable_boolean")
        schema.add_field("ni", "nullable_int32")
        record = GenericRecord(schema, {"nb": True, "ni": 42})
        
        writer = DefaultCompactWriter(Schema(type_name="Test"))
        serializer.write(writer, record)
        
        assert writer.fields["nb"] is True
        assert writer.fields["ni"] == 42

    def test_read_compact_field(self):
        serializer = GenericRecordSerializer("Test")
        schema = Schema(type_name="Test")
        schema.add_field("nested", "compact")
        
        reader = DefaultCompactReader(schema, {"nested": None})
        result = serializer.read(reader)
        
        assert result.get_compact("nested") is None

    def test_read_nullable_fields(self):
        serializer = GenericRecordSerializer("Test")
        schema = Schema(type_name="Test")
        schema.add_field("nb", "nullable_boolean")
        schema.add_field("ni", "nullable_int32")
        
        reader = DefaultCompactReader(schema, {"nb": None, "ni": 100})
        result = serializer.read(reader)
        
        assert result.get_nullable_boolean("nb") is None
        assert result.get_nullable_int32("ni") == 100

    def test_read_returns_none_for_unknown_kind(self):
        serializer = GenericRecordSerializer("Test")
        schema = Schema(type_name="Test")
        schema.add_field("unknown", "date")
        
        reader = DefaultCompactReader(schema, {"unknown": None})
        result = serializer.read(reader)
        
        assert isinstance(result, GenericRecord)

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


class TestFieldKindCompleteness:
    """Tests for complete FieldKind coverage."""

    def test_all_field_kinds_in_map(self):
        expected_keys = [
            "boolean", "int8", "int16", "int32", "int64",
            "float32", "float64", "string", "compact",
            "array_boolean", "array_int8", "array_int16", "array_int32",
            "array_int64", "array_float32", "array_float64", "array_string",
            "array_compact", "nullable_boolean", "nullable_int8", "nullable_int16",
            "nullable_int32", "nullable_int64", "nullable_float32", "nullable_float64",
            "date", "time", "timestamp", "timestamp_with_timezone", "decimal",
            "array_date", "array_time", "array_timestamp", "array_decimal",
        ]
        for key in expected_keys:
            assert key in FIELD_KIND_MAP, f"Missing key: {key}"

    def test_date_time_field_kinds(self):
        assert FieldKind.DATE == 25
        assert FieldKind.TIME == 26
        assert FieldKind.TIMESTAMP == 27
        assert FieldKind.TIMESTAMP_WITH_TIMEZONE == 28
        assert FieldKind.DECIMAL == 29

    def test_date_time_array_field_kinds(self):
        assert FieldKind.ARRAY_DATE == 30
        assert FieldKind.ARRAY_TIME == 31
        assert FieldKind.ARRAY_TIMESTAMP == 32
        assert FieldKind.ARRAY_DECIMAL == 33

    def test_field_kind_defaults_for_date_types(self):
        assert FIELD_KIND_DEFAULTS[FieldKind.DATE] is None
        assert FIELD_KIND_DEFAULTS[FieldKind.TIME] is None
        assert FIELD_KIND_DEFAULTS[FieldKind.TIMESTAMP] is None
        assert FIELD_KIND_DEFAULTS[FieldKind.TIMESTAMP_WITH_TIMEZONE] is None
        assert FIELD_KIND_DEFAULTS[FieldKind.DECIMAL] is None
        assert FIELD_KIND_DEFAULTS[FieldKind.ARRAY_DATE] is None
        assert FIELD_KIND_DEFAULTS[FieldKind.ARRAY_TIME] is None
        assert FIELD_KIND_DEFAULTS[FieldKind.ARRAY_TIMESTAMP] is None
        assert FIELD_KIND_DEFAULTS[FieldKind.ARRAY_DECIMAL] is None


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


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_schema_compute_id_empty_fields(self):
        schema = Schema(type_name="Empty")
        assert schema.schema_id != 0

    def test_schema_add_multiple_fields_updates_indices(self):
        schema = Schema(type_name="Test")
        f1 = schema.add_field("a", "int32")
        f2 = schema.add_field("b", "int32")
        f3 = schema.add_field("c", "int32")
        
        assert f1.index == 0
        assert f2.index == 1
        assert f3.index == 2

    def test_generic_record_default_fields(self):
        schema = Schema(type_name="Test")
        schema.add_field("val", "int32")
        record = GenericRecord(schema)
        
        assert record.get_int32("val") == 0

    def test_generic_record_equality_with_different_schemas(self):
        s1 = Schema(type_name="Type1")
        s1.add_field("x", "int32")
        s2 = Schema(type_name="Type2")
        s2.add_field("x", "int32")
        
        r1 = GenericRecord(s1, {"x": 1})
        r2 = GenericRecord(s2, {"x": 1})
        
        assert r1 != r2

    def test_reader_check_field_kind_for_missing_field(self):
        schema = Schema(type_name="Test")
        reader = DefaultCompactReader(schema, {})
        
        reader._check_field_kind("nonexistent", FieldKind.INT32)

    def test_serialization_service_write_array_null(self):
        service = CompactSerializationService()
        
        @dataclass
        class WithNullArray:
            items: list
        
        class WithNullArraySerializer(CompactSerializer[WithNullArray]):
            @property
            def type_name(self) -> str:
                return "WithNullArray"
            
            @property
            def clazz(self) -> type:
                return WithNullArray
            
            def write(self, writer: CompactWriter, obj: WithNullArray) -> None:
                writer.write_array_of_int8("items", obj.items)
            
            def read(self, reader: CompactReader) -> WithNullArray:
                return WithNullArray(items=reader.read_array_of_int8("items"))
        
        service.register_serializer(WithNullArraySerializer())
        
        obj = WithNullArray(items=None)
        data = service.serialize(obj)
        result = service.deserialize(data, "WithNullArray")
        
        assert result.items is None

    def test_compact_stream_boundary_values(self):
        writer = CompactStreamWriter()
        writer.write_int8(-128)
        writer.write_int8(127)
        writer.write_int16(-32768)
        writer.write_int16(32767)
        
        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_int8() == -128
        assert reader.read_int8() == 127
        assert reader.read_int16() == -32768
        assert reader.read_int16() == 32767

    def test_generic_record_builder_chain(self):
        record = (GenericRecordBuilder("Chain")
                  .set_boolean("b", True)
                  .set_int8("i8", 1)
                  .set_int16("i16", 2)
                  .set_int32("i32", 3)
                  .set_int64("i64", 4)
                  .set_float32("f32", 1.0)
                  .set_float64("f64", 2.0)
                  .set_string("s", "test")
                  .build())
        
        assert record.get_boolean("b") is True
        assert record.get_int8("i8") == 1
        assert record.get_int16("i16") == 2
        assert record.get_int32("i32") == 3
        assert record.get_int64("i64") == 4

    def test_to_generic_record_infers_schema(self):
        service = CompactSerializationService()
        
        @dataclass
        class Simple:
            name: str
            count: int
        
        class SimpleSerializer(CompactSerializer[Simple]):
            @property
            def type_name(self) -> str:
                return "Simple"
            
            @property
            def clazz(self) -> type:
                return Simple
            
            def write(self, writer: CompactWriter, obj: Simple) -> None:
                writer.write_string("name", obj.name)
                writer.write_int32("count", obj.count)
            
            def read(self, reader: CompactReader) -> Simple:
                return Simple(
                    name=reader.read_string("name"),
                    count=reader.read_int32("count"),
                )
        
        service.register_serializer(SimpleSerializer())
        data = service.serialize(Simple(name="test", count=5))
        
        record = service.to_generic_record(data, "Simple")
        assert record.type_name == "Simple"

    def test_read_array_of_boolean(self):
        schema = Schema(type_name="Test")
        schema.add_field("flags", "array_boolean")
        reader = DefaultCompactReader(schema, {"flags": [True, False, True]})
        assert reader.read_array_of_boolean("flags") == [True, False, True]

    def test_read_array_of_int32(self):
        schema = Schema(type_name="Test")
        schema.add_field("nums", "array_int32")
        reader = DefaultCompactReader(schema, {"nums": [1, 2, 3]})
        assert reader.read_array_of_int32("nums") == [1, 2, 3]

    def test_read_array_of_string(self):
        schema = Schema(type_name="Test")
        schema.add_field("strs", "array_string")
        reader = DefaultCompactReader(schema, {"strs": ["a", "b"]})
        assert reader.read_array_of_string("strs") == ["a", "b"]
