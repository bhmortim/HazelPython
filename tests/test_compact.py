"""Comprehensive tests for compact serialization."""

from dataclasses import dataclass
from typing import List, Optional

import pytest

from hazelcast.serialization.api import CompactReader, CompactWriter, CompactSerializer
from hazelcast.serialization.compact import (
    CompactSerializationService,
    Schema,
    SchemaService,
    FieldDescriptor,
    FieldKind,
    DefaultCompactReader,
    DefaultCompactWriter,
    CompactStreamReader,
    CompactStreamWriter,
    ReflectiveCompactSerializer,
)


@dataclass
class Person:
    """Sample dataclass for testing."""

    name: str
    age: int
    active: bool


@dataclass
class Address:
    """Sample address dataclass."""

    street: str
    city: str
    zip_code: int


@dataclass
class Employee:
    """Employee with nested address."""

    name: str
    employee_id: int
    salary: float


class PersonSerializer(CompactSerializer[Person]):
    """Serializer for Person."""

    @property
    def type_name(self) -> str:
        return "Person"

    @property
    def clazz(self) -> type:
        return Person

    def write(self, writer: CompactWriter, obj: Person) -> None:
        writer.write_string("name", obj.name)
        writer.write_int32("age", obj.age)
        writer.write_boolean("active", obj.active)

    def read(self, reader: CompactReader) -> Person:
        return Person(
            name=reader.read_string("name"),
            age=reader.read_int32("age"),
            active=reader.read_boolean("active"),
        )


class EmployeeSerializer(CompactSerializer[Employee]):
    """Serializer for Employee."""

    @property
    def type_name(self) -> str:
        return "Employee"

    @property
    def clazz(self) -> type:
        return Employee

    def write(self, writer: CompactWriter, obj: Employee) -> None:
        writer.write_string("name", obj.name)
        writer.write_int32("employee_id", obj.employee_id)
        writer.write_float64("salary", obj.salary)

    def read(self, reader: CompactReader) -> Employee:
        return Employee(
            name=reader.read_string("name"),
            employee_id=reader.read_int32("employee_id"),
            salary=reader.read_float64("salary"),
        )


class TestSchema:
    """Tests for Schema class."""

    def test_schema_creation(self):
        schema = Schema(type_name="TestType")
        assert schema.type_name == "TestType"
        assert schema.schema_id != 0

    def test_add_field(self):
        schema = Schema(type_name="TestType")
        field = schema.add_field("name", "string")
        assert field.name == "name"
        assert field.field_type == "string"
        assert field.index == 0

    def test_get_field(self):
        schema = Schema(type_name="TestType")
        schema.add_field("name", "string")
        schema.add_field("age", "int32")

        field = schema.get_field("name")
        assert field is not None
        assert field.name == "name"

        field = schema.get_field("nonexistent")
        assert field is None

    def test_schema_id_computation(self):
        schema1 = Schema(type_name="Type1")
        schema1.add_field("field1", "string")

        schema2 = Schema(type_name="Type1")
        schema2.add_field("field1", "string")

        schema3 = Schema(type_name="Type1")
        schema3.add_field("field2", "string")

        assert schema1.schema_id != schema3.schema_id


class TestFieldDescriptor:
    """Tests for FieldDescriptor class."""

    def test_field_kind(self):
        field = FieldDescriptor(name="test", field_type="boolean", index=0)
        assert field.kind == FieldKind.BOOLEAN

        field = FieldDescriptor(name="test", field_type="int32", index=0)
        assert field.kind == FieldKind.INT32

        field = FieldDescriptor(name="test", field_type="string", index=0)
        assert field.kind == FieldKind.STRING


class TestSchemaService:
    """Tests for SchemaService class."""

    def test_register_and_get_by_id(self):
        service = SchemaService()
        schema = Schema(type_name="TestType")
        service.register(schema)

        retrieved = service.get_by_id(schema.schema_id)
        assert retrieved == schema

    def test_get_by_type_name(self):
        service = SchemaService()
        schema = Schema(type_name="TestType")
        service.register(schema)

        retrieved = service.get_by_type_name("TestType")
        assert retrieved == schema

    def test_has_schema(self):
        service = SchemaService()
        schema = Schema(type_name="TestType")

        assert not service.has_schema(schema.schema_id)
        service.register(schema)
        assert service.has_schema(schema.schema_id)

    def test_all_schemas(self):
        service = SchemaService()
        schema1 = Schema(type_name="Type1")
        schema2 = Schema(type_name="Type2")

        service.register(schema1)
        service.register(schema2)

        all_schemas = service.all_schemas()
        assert len(all_schemas) == 2
        assert schema1 in all_schemas
        assert schema2 in all_schemas

    def test_schema_versioning(self):
        service = SchemaService()

        v1 = Schema(type_name="Person")
        v1.add_field("name", "string")
        service.register(v1)

        v2 = Schema(type_name="Person")
        v2.add_field("name", "string")
        v2.add_field("age", "int32")
        service.register(v2)

        versions = service.get_all_versions("Person")
        assert len(versions) == 2

    def test_schema_compatibility(self):
        service = SchemaService()

        old_schema = Schema(type_name="Person")
        old_schema.add_field("name", "string")

        new_schema = Schema(type_name="Person")
        new_schema.add_field("name", "string")
        new_schema.add_field("age", "int32")

        assert service.is_compatible(old_schema, new_schema)

        incompatible_schema = Schema(type_name="Person")
        incompatible_schema.add_field("name", "int32")

        assert not service.is_compatible(old_schema, incompatible_schema)


class TestCompactStreamIO:
    """Tests for CompactStreamReader and CompactStreamWriter."""

    def test_boolean_roundtrip(self):
        writer = CompactStreamWriter()
        writer.write_boolean(True)
        writer.write_boolean(False)

        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_boolean() is True
        assert reader.read_boolean() is False

    def test_int8_roundtrip(self):
        writer = CompactStreamWriter()
        writer.write_int8(127)
        writer.write_int8(-128)

        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_int8() == 127
        assert reader.read_int8() == -128

    def test_int16_roundtrip(self):
        writer = CompactStreamWriter()
        writer.write_int16(32767)
        writer.write_int16(-32768)

        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_int16() == 32767
        assert reader.read_int16() == -32768

    def test_int32_roundtrip(self):
        writer = CompactStreamWriter()
        writer.write_int32(2147483647)
        writer.write_int32(-2147483648)

        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_int32() == 2147483647
        assert reader.read_int32() == -2147483648

    def test_int64_roundtrip(self):
        writer = CompactStreamWriter()
        writer.write_int64(9223372036854775807)
        writer.write_int64(-9223372036854775808)

        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_int64() == 9223372036854775807
        assert reader.read_int64() == -9223372036854775808

    def test_float32_roundtrip(self):
        writer = CompactStreamWriter()
        writer.write_float32(3.14)

        reader = CompactStreamReader(writer.to_bytes())
        assert abs(reader.read_float32() - 3.14) < 0.001

    def test_float64_roundtrip(self):
        writer = CompactStreamWriter()
        writer.write_float64(3.141592653589793)

        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_float64() == 3.141592653589793

    def test_string_roundtrip(self):
        writer = CompactStreamWriter()
        writer.write_string("Hello, World!")
        writer.write_string("")
        writer.write_string("日本語")

        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_string() == "Hello, World!"
        assert reader.read_string() == ""
        assert reader.read_string() == "日本語"

    def test_null_string(self):
        writer = CompactStreamWriter()
        writer.write_string(None)

        reader = CompactStreamReader(writer.to_bytes())
        assert reader.read_string() is None


class TestDefaultCompactWriter:
    """Tests for DefaultCompactWriter."""

    def test_write_primitives(self):
        schema = Schema(type_name="Test")
        writer = DefaultCompactWriter(schema)

        writer.write_boolean("bool_field", True)
        writer.write_int8("int8_field", 42)
        writer.write_int16("int16_field", 1000)
        writer.write_int32("int32_field", 100000)
        writer.write_int64("int64_field", 10000000000)
        writer.write_float32("float32_field", 3.14)
        writer.write_float64("float64_field", 3.141592653589793)
        writer.write_string("string_field", "test")

        assert writer.fields["bool_field"] is True
        assert writer.fields["int8_field"] == 42
        assert writer.fields["int16_field"] == 1000
        assert writer.fields["int32_field"] == 100000
        assert writer.fields["int64_field"] == 10000000000
        assert abs(writer.fields["float32_field"] - 3.14) < 0.001
        assert writer.fields["float64_field"] == 3.141592653589793
        assert writer.fields["string_field"] == "test"

    def test_write_arrays(self):
        schema = Schema(type_name="Test")
        writer = DefaultCompactWriter(schema)

        writer.write_array_of_boolean("bool_array", [True, False, True])
        writer.write_array_of_int32("int_array", [1, 2, 3])
        writer.write_array_of_string("string_array", ["a", "b", "c"])

        assert writer.fields["bool_array"] == [True, False, True]
        assert writer.fields["int_array"] == [1, 2, 3]
        assert writer.fields["string_array"] == ["a", "b", "c"]

    def test_write_nullable(self):
        schema = Schema(type_name="Test")
        writer = DefaultCompactWriter(schema)

        writer.write_nullable_boolean("nullable_bool", True)
        writer.write_nullable_boolean("null_bool", None)
        writer.write_nullable_int32("nullable_int", 42)
        writer.write_nullable_int32("null_int", None)

        assert writer.fields["nullable_bool"] is True
        assert writer.fields["null_bool"] is None
        assert writer.fields["nullable_int"] == 42
        assert writer.fields["null_int"] is None


class TestDefaultCompactReader:
    """Tests for DefaultCompactReader."""

    def test_read_primitives(self):
        schema = Schema(type_name="Test")
        fields = {
            "bool_field": True,
            "int8_field": 42,
            "int16_field": 1000,
            "int32_field": 100000,
            "int64_field": 10000000000,
            "float32_field": 3.14,
            "float64_field": 3.141592653589793,
            "string_field": "test",
        }
        reader = DefaultCompactReader(schema, fields)

        assert reader.read_boolean("bool_field") is True
        assert reader.read_int8("int8_field") == 42
        assert reader.read_int16("int16_field") == 1000
        assert reader.read_int32("int32_field") == 100000
        assert reader.read_int64("int64_field") == 10000000000
        assert abs(reader.read_float32("float32_field") - 3.14) < 0.001
        assert reader.read_float64("float64_field") == 3.141592653589793
        assert reader.read_string("string_field") == "test"

    def test_read_missing_fields_returns_defaults(self):
        schema = Schema(type_name="Test")
        reader = DefaultCompactReader(schema, {})

        assert reader.read_boolean("missing") is False
        assert reader.read_int8("missing") == 0
        assert reader.read_int16("missing") == 0
        assert reader.read_int32("missing") == 0
        assert reader.read_int64("missing") == 0
        assert reader.read_float32("missing") == 0.0
        assert reader.read_float64("missing") == 0.0
        assert reader.read_string("missing") == ""

    def test_read_arrays(self):
        schema = Schema(type_name="Test")
        fields = {
            "bool_array": [True, False],
            "int_array": [1, 2, 3],
            "string_array": ["a", "b"],
        }
        reader = DefaultCompactReader(schema, fields)

        assert reader.read_array_of_boolean("bool_array") == [True, False]
        assert reader.read_array_of_int32("int_array") == [1, 2, 3]
        assert reader.read_array_of_string("string_array") == ["a", "b"]

    def test_read_nullable(self):
        schema = Schema(type_name="Test")
        fields = {
            "nullable_bool": True,
            "null_bool": None,
            "nullable_int": 42,
            "null_int": None,
        }
        reader = DefaultCompactReader(schema, fields)

        assert reader.read_nullable_boolean("nullable_bool") is True
        assert reader.read_nullable_boolean("null_bool") is None
        assert reader.read_nullable_int32("nullable_int") == 42
        assert reader.read_nullable_int32("null_int") is None

    def test_has_field(self):
        schema = Schema(type_name="Test")
        fields = {"existing": 42}
        reader = DefaultCompactReader(schema, fields)

        assert reader.has_field("existing")
        assert not reader.has_field("missing")


class TestCompactSerializationService:
    """Tests for CompactSerializationService."""

    def test_register_serializer(self):
        service = CompactSerializationService()
        serializer = PersonSerializer()
        service.register_serializer(serializer)

        retrieved = service.get_serializer("Person")
        assert retrieved == serializer

        retrieved_by_class = service.get_serializer_for_class(Person)
        assert retrieved_by_class == serializer

    def test_simple_roundtrip(self):
        service = CompactSerializationService()
        service.register_serializer(PersonSerializer())

        original = Person(name="Alice", age=30, active=True)
        data = service.serialize(original)
        result = service.deserialize(data, "Person")

        assert result.name == original.name
        assert result.age == original.age
        assert result.active == original.active

    def test_multiple_types(self):
        service = CompactSerializationService()
        service.register_serializer(PersonSerializer())
        service.register_serializer(EmployeeSerializer())

        person = Person(name="Bob", age=25, active=False)
        employee = Employee(name="Charlie", employee_id=12345, salary=75000.50)

        person_data = service.serialize(person)
        employee_data = service.serialize(employee)

        person_result = service.deserialize(person_data, "Person")
        employee_result = service.deserialize(employee_data, "Employee")

        assert person_result.name == person.name
        assert employee_result.employee_id == employee.employee_id

    def test_schema_registration(self):
        service = CompactSerializationService()
        service.register_serializer(PersonSerializer())

        original = Person(name="Test", age=20, active=True)
        service.serialize(original)

        assert service.schema_service.get_by_type_name("Person") is not None

    def test_missing_serializer_raises(self):
        service = CompactSerializationService()

        with pytest.raises(ValueError):
            service.serialize(Person(name="Test", age=20, active=True))

    def test_schema_evolution_field_added(self):
        service = CompactSerializationService()
        service.register_serializer(PersonSerializer())

        old_schema = Schema(type_name="Person")
        old_schema.add_field("name", "string")
        old_schema.add_field("age", "int32")

        new_schema = Schema(type_name="Person")
        new_schema.add_field("name", "string")
        new_schema.add_field("age", "int32")
        new_schema.add_field("active", "boolean")

        service.register_schema(old_schema)
        service.register_schema(new_schema)

        assert service.check_schema_compatibility(old_schema.schema_id, new_schema.schema_id)

    def test_deserialize_with_missing_new_field(self):
        service = CompactSerializationService()
        service.register_serializer(PersonSerializer())

        fields = {"name": "Test", "age": 25}
        schema = Schema(type_name="Person")
        schema.add_field("name", "string")
        schema.add_field("age", "int32")
        service.register_schema(schema)

        reader = DefaultCompactReader(schema, fields)
        result = PersonSerializer().read(reader)

        assert result.name == "Test"
        assert result.age == 25
        assert result.active is False


class TestReflectiveCompactSerializer:
    """Tests for ReflectiveCompactSerializer."""

    def test_dataclass_serialization(self):
        service = CompactSerializationService()
        serializer = ReflectiveCompactSerializer(Person)
        service.register_serializer(serializer)

        original = Person(name="Reflective", age=40, active=True)
        data = service.serialize(original)
        result = service.deserialize(data, "Person")

        assert result.name == original.name
        assert result.age == original.age
        assert result.active == original.active

    def test_custom_type_name(self):
        serializer = ReflectiveCompactSerializer(Person, "CustomPerson")
        assert serializer.type_name == "CustomPerson"

    def test_address_dataclass(self):
        service = CompactSerializationService()
        serializer = ReflectiveCompactSerializer(Address)
        service.register_serializer(serializer)

        original = Address(street="123 Main St", city="Springfield", zip_code=12345)
        data = service.serialize(original)
        result = service.deserialize(data, "Address")

        assert result.street == original.street
        assert result.city == original.city
        assert result.zip_code == original.zip_code


class TestFieldKind:
    """Tests for FieldKind enum."""

    def test_field_kind_values(self):
        assert FieldKind.BOOLEAN == 0
        assert FieldKind.INT8 == 1
        assert FieldKind.INT16 == 2
        assert FieldKind.INT32 == 3
        assert FieldKind.INT64 == 4
        assert FieldKind.FLOAT32 == 5
        assert FieldKind.FLOAT64 == 6
        assert FieldKind.STRING == 7
        assert FieldKind.COMPACT == 8


class TestCompactArraySerialization:
    """Tests for array field serialization in compact format."""

    def test_boolean_array(self):
        service = CompactSerializationService()

        @dataclass
        class BoolArrayContainer:
            values: list

        class BoolArraySerializer(CompactSerializer):
            @property
            def type_name(self) -> str:
                return "BoolArrayContainer"

            @property
            def clazz(self) -> type:
                return BoolArrayContainer

            def write(self, writer: CompactWriter, obj) -> None:
                writer.write_array_of_boolean("values", obj.values)

            def read(self, reader: CompactReader):
                return BoolArrayContainer(values=reader.read_array_of_boolean("values"))

        service.register_serializer(BoolArraySerializer())

        original = BoolArrayContainer(values=[True, False, True, False])
        data = service.serialize(original)
        result = service.deserialize(data, "BoolArrayContainer")

        assert result.values == original.values

    def test_int_array(self):
        service = CompactSerializationService()

        @dataclass
        class IntArrayContainer:
            values: list

        class IntArraySerializer(CompactSerializer):
            @property
            def type_name(self) -> str:
                return "IntArrayContainer"

            @property
            def clazz(self) -> type:
                return IntArrayContainer

            def write(self, writer: CompactWriter, obj) -> None:
                writer.write_array_of_int32("values", obj.values)

            def read(self, reader: CompactReader):
                return IntArrayContainer(values=reader.read_array_of_int32("values"))

        service.register_serializer(IntArraySerializer())

        original = IntArrayContainer(values=[1, 2, 3, 4, 5])
        data = service.serialize(original)
        result = service.deserialize(data, "IntArrayContainer")

        assert result.values == original.values

    def test_string_array(self):
        service = CompactSerializationService()

        @dataclass
        class StringArrayContainer:
            values: list

        class StringArraySerializer(CompactSerializer):
            @property
            def type_name(self) -> str:
                return "StringArrayContainer"

            @property
            def clazz(self) -> type:
                return StringArrayContainer

            def write(self, writer: CompactWriter, obj) -> None:
                writer.write_array_of_string("values", obj.values)

            def read(self, reader: CompactReader):
                return StringArrayContainer(values=reader.read_array_of_string("values"))

        service.register_serializer(StringArraySerializer())

        original = StringArrayContainer(values=["hello", "world", "test"])
        data = service.serialize(original)
        result = service.deserialize(data, "StringArrayContainer")

        assert result.values == original.values


class TestNullableFieldSerialization:
    """Tests for nullable field serialization."""

    def test_nullable_boolean(self):
        service = CompactSerializationService()

        @dataclass
        class NullableBoolContainer:
            value: Optional[bool]

        class NullableBoolSerializer(CompactSerializer):
            @property
            def type_name(self) -> str:
                return "NullableBoolContainer"

            @property
            def clazz(self) -> type:
                return NullableBoolContainer

            def write(self, writer: CompactWriter, obj) -> None:
                writer.write_nullable_boolean("value", obj.value)

            def read(self, reader: CompactReader):
                return NullableBoolContainer(value=reader.read_nullable_boolean("value"))

        service.register_serializer(NullableBoolSerializer())

        for value in [True, False, None]:
            original = NullableBoolContainer(value=value)
            data = service.serialize(original)
            result = service.deserialize(data, "NullableBoolContainer")
            assert result.value == original.value

    def test_nullable_int(self):
        service = CompactSerializationService()

        @dataclass
        class NullableIntContainer:
            value: Optional[int]

        class NullableIntSerializer(CompactSerializer):
            @property
            def type_name(self) -> str:
                return "NullableIntContainer"

            @property
            def clazz(self) -> type:
                return NullableIntContainer

            def write(self, writer: CompactWriter, obj) -> None:
                writer.write_nullable_int32("value", obj.value)

            def read(self, reader: CompactReader):
                return NullableIntContainer(value=reader.read_nullable_int32("value"))

        service.register_serializer(NullableIntSerializer())

        for value in [42, 0, -100, None]:
            original = NullableIntContainer(value=value)
            data = service.serialize(original)
            result = service.deserialize(data, "NullableIntContainer")
            assert result.value == original.value
