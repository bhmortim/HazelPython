#!/usr/bin/env python3
"""Compact Serialization with Schema Evolution example.

Demonstrates Hazelcast's Compact serialization format, which provides:
- Efficient binary serialization without class availability
- Schema evolution (add/remove fields without breaking compatibility)
- Type-safe field access
- Support for nullable types and arrays

Topics covered:
- Custom CompactSerializer implementation
- Schema registration and evolution
- GenericRecord for schema-less access
- Reflective serialization for dataclasses
"""

from dataclasses import dataclass
from typing import List, Optional

from hazelcast.serialization.compact import (
    CompactSerializationService,
    Schema,
    FieldDescriptor,
    FieldKind,
    GenericRecord,
    GenericRecordBuilder,
    DefaultCompactReader,
    DefaultCompactWriter,
    ReflectiveCompactSerializer,
    SchemaService,
)
from hazelcast.serialization.api import CompactSerializer, CompactReader, CompactWriter


# -----------------------------------------------------------------------------
# Domain Classes
# -----------------------------------------------------------------------------


@dataclass
class PersonV1:
    """Version 1 of Person class."""

    name: str
    age: int


@dataclass
class PersonV2:
    """Version 2 of Person class with additional fields."""

    name: str
    age: int
    email: str  # New field in V2
    active: bool  # New field in V2


@dataclass
class Address:
    """Nested object for demonstrating compact serialization."""

    street: str
    city: str
    zip_code: str


@dataclass
class Employee:
    """Complex object with nested types and arrays."""

    id: int
    name: str
    department: str
    skills: List[str]
    salary: float
    manager_id: Optional[int]


# -----------------------------------------------------------------------------
# Custom Serializers
# -----------------------------------------------------------------------------


class PersonV1Serializer(CompactSerializer[PersonV1]):
    """Serializer for PersonV1."""

    @property
    def type_name(self) -> str:
        return "Person"

    @property
    def clazz(self) -> type:
        return PersonV1

    def write(self, writer: CompactWriter, obj: PersonV1) -> None:
        writer.write_string("name", obj.name)
        writer.write_int32("age", obj.age)

    def read(self, reader: CompactReader) -> PersonV1:
        name = reader.read_string("name")
        age = reader.read_int32("age")
        return PersonV1(name=name, age=age)


class PersonV2Serializer(CompactSerializer[PersonV2]):
    """Serializer for PersonV2 with backward compatibility."""

    @property
    def type_name(self) -> str:
        return "Person"  # Same type name for evolution

    @property
    def clazz(self) -> type:
        return PersonV2

    def write(self, writer: CompactWriter, obj: PersonV2) -> None:
        writer.write_string("name", obj.name)
        writer.write_int32("age", obj.age)
        writer.write_string("email", obj.email)
        writer.write_boolean("active", obj.active)

    def read(self, reader: CompactReader) -> PersonV2:
        name = reader.read_string("name")
        age = reader.read_int32("age")
        # Handle missing fields from older schema
        email = reader.read_string("email") or ""
        active = reader.read_boolean("active") if reader.has_field("active") else True
        return PersonV2(name=name, age=age, email=email, active=active)


class EmployeeSerializer(CompactSerializer[Employee]):
    """Serializer demonstrating various field types."""

    @property
    def type_name(self) -> str:
        return "Employee"

    @property
    def clazz(self) -> type:
        return Employee

    def write(self, writer: CompactWriter, obj: Employee) -> None:
        writer.write_int32("id", obj.id)
        writer.write_string("name", obj.name)
        writer.write_string("department", obj.department)
        writer.write_array_of_string("skills", obj.skills)
        writer.write_float64("salary", obj.salary)
        writer.write_nullable_int32("manager_id", obj.manager_id)

    def read(self, reader: CompactReader) -> Employee:
        return Employee(
            id=reader.read_int32("id"),
            name=reader.read_string("name"),
            department=reader.read_string("department"),
            skills=reader.read_array_of_string("skills") or [],
            salary=reader.read_float64("salary"),
            manager_id=reader.read_nullable_int32("manager_id"),
        )


# -----------------------------------------------------------------------------
# Examples
# -----------------------------------------------------------------------------


def basic_serialization_example():
    """Basic compact serialization and deserialization."""
    print("=== Basic Compact Serialization ===")

    service = CompactSerializationService()
    service.register_serializer(PersonV1Serializer())

    # Serialize a Person
    person = PersonV1(name="Alice", age=30)
    data = service.serialize(person)
    print(f"  Original: {person}")
    print(f"  Serialized size: {len(data)} bytes")

    # Deserialize back
    restored = service.deserialize(data, "Person")
    print(f"  Restored: {restored}")


def schema_evolution_example():
    """Demonstrate schema evolution - reading V1 data with V2 schema."""
    print("\n=== Schema Evolution Example ===")

    # Service with V1 serializer
    service_v1 = CompactSerializationService()
    service_v1.register_serializer(PersonV1Serializer())

    # Serialize with V1 schema
    person_v1 = PersonV1(name="Bob", age=25)
    data_v1 = service_v1.serialize(person_v1)
    print(f"  Serialized V1: {person_v1}")

    # Service with V2 serializer (can read V1 data)
    service_v2 = CompactSerializationService()
    service_v2.register_serializer(PersonV2Serializer())

    # Deserialize V1 data using V2 reader
    # Missing fields get default values
    person_v2 = service_v2.deserialize(data_v1, "Person")
    print(f"  Deserialized as V2: {person_v2}")
    print("  Note: email and active have default values")


def generic_record_example():
    """Using GenericRecord for schema-less access."""
    print("\n=== GenericRecord Example ===")

    # Build a GenericRecord without a class definition
    record = (
        GenericRecordBuilder("Product")
        .set_string("name", "Laptop")
        .set_int32("price", 999)
        .set_boolean("in_stock", True)
        .set_array_of_string("tags", ["electronics", "computer", "portable"])
        .build()
    )

    print(f"  Type: {record.type_name}")
    print(f"  Fields: {record.get_field_names()}")
    print(f"  Name: {record.get_string('name')}")
    print(f"  Price: {record.get_int32('price')}")
    print(f"  In Stock: {record.get_boolean('in_stock')}")
    print(f"  Tags: {record.get_array_of_string('tags')}")

    # Convert to dictionary
    print(f"  As dict: {record.to_dict()}")


def nullable_fields_example():
    """Demonstrate nullable field handling."""
    print("\n=== Nullable Fields Example ===")

    # Build record with nullable fields
    employee_with_manager = (
        GenericRecordBuilder("Employee")
        .set_int32("id", 1)
        .set_string("name", "Alice")
        .set_nullable_int32("manager_id", 100)
        .build()
    )

    employee_without_manager = (
        GenericRecordBuilder("Employee")
        .set_int32("id", 2)
        .set_string("name", "Bob")
        .set_nullable_int32("manager_id", None)
        .build()
    )

    print(f"  Employee 1 manager: {employee_with_manager.get_nullable_int32('manager_id')}")
    print(f"  Employee 2 manager: {employee_without_manager.get_nullable_int32('manager_id')}")


def complex_serialization_example():
    """Serialize complex objects with arrays and nested types."""
    print("\n=== Complex Serialization Example ===")

    service = CompactSerializationService()
    service.register_serializer(EmployeeSerializer())

    # Create employee with various field types
    employee = Employee(
        id=12345,
        name="Charlie",
        department="Engineering",
        skills=["Python", "Java", "Hazelcast"],
        salary=95000.50,
        manager_id=999,
    )

    # Serialize and deserialize
    data = service.serialize(employee)
    restored = service.deserialize(data, "Employee")

    print(f"  Original: {employee}")
    print(f"  Serialized size: {len(data)} bytes")
    print(f"  Restored: {restored}")
    print(f"  Skills match: {employee.skills == restored.skills}")


def schema_service_example():
    """Demonstrate schema management and compatibility checking."""
    print("\n=== Schema Service Example ===")

    schema_service = SchemaService()

    # Register V1 schema
    schema_v1 = Schema(type_name="Person")
    schema_v1.add_field("name", "string")
    schema_v1.add_field("age", "int32")
    schema_service.register(schema_v1)

    print(f"  Registered V1 schema ID: {schema_v1.schema_id}")

    # Register V2 schema (compatible - adds fields)
    schema_v2 = Schema(type_name="Person")
    schema_v2.add_field("name", "string")
    schema_v2.add_field("age", "int32")
    schema_v2.add_field("email", "string")
    schema_v2.add_field("active", "boolean")
    schema_service.register(schema_v2)

    print(f"  Registered V2 schema ID: {schema_v2.schema_id}")

    # Check compatibility
    is_compatible = schema_service.is_compatible(schema_v1, schema_v2)
    print(f"  V1 -> V2 compatible: {is_compatible}")

    # List all versions
    all_versions = schema_service.get_all_versions("Person")
    print(f"  Total schema versions: {len(all_versions)}")


def reflective_serializer_example():
    """Use reflective serializer for dataclasses."""
    print("\n=== Reflective Serializer Example ===")

    @dataclass
    class SimpleProduct:
        name: str
        price: int
        available: bool

    service = CompactSerializationService()

    # Use reflective serializer for automatic field mapping
    serializer = ReflectiveCompactSerializer(SimpleProduct, "SimpleProduct")
    service.register_serializer(serializer)

    product = SimpleProduct(name="Widget", price=25, available=True)
    data = service.serialize(product)
    restored = service.deserialize(data, "SimpleProduct")

    print(f"  Original: {product}")
    print(f"  Restored: {restored}")
    print(f"  Match: {product == restored}")


def field_kinds_reference():
    """Reference for available field kinds."""
    print("\n=== Field Kinds Reference ===")

    # Primitive types
    print("\n  Primitive Types:")
    primitives = [
        ("BOOLEAN", "bool", "True/False"),
        ("INT8", "int", "-128 to 127"),
        ("INT16", "int", "-32768 to 32767"),
        ("INT32", "int", "-2^31 to 2^31-1"),
        ("INT64", "int", "-2^63 to 2^63-1"),
        ("FLOAT32", "float", "32-bit IEEE 754"),
        ("FLOAT64", "float", "64-bit IEEE 754"),
        ("STRING", "str", "UTF-8 encoded text"),
    ]
    print(f"    {'Kind':<12} {'Python':<8} Range/Notes")
    for kind, py_type, notes in primitives:
        print(f"    {kind:<12} {py_type:<8} {notes}")

    # Nullable types
    print("\n  Nullable Types (can be None):")
    print("    NULLABLE_BOOLEAN, NULLABLE_INT8, NULLABLE_INT16,")
    print("    NULLABLE_INT32, NULLABLE_INT64, NULLABLE_FLOAT32, NULLABLE_FLOAT64")

    # Array types
    print("\n  Array Types:")
    print("    ARRAY_BOOLEAN, ARRAY_INT8, ARRAY_INT16, ARRAY_INT32,")
    print("    ARRAY_INT64, ARRAY_FLOAT32, ARRAY_FLOAT64, ARRAY_STRING")

    # Complex types
    print("\n  Complex Types:")
    print("    COMPACT      - Nested compact objects")
    print("    ARRAY_COMPACT - Array of nested compact objects")

    # Date/Time types
    print("\n  Date/Time Types:")
    print("    DATE, TIME, TIMESTAMP, TIMESTAMP_WITH_TIMEZONE")
    print("    ARRAY_DATE, ARRAY_TIME, ARRAY_TIMESTAMP")

    # Special types
    print("\n  Special Types:")
    print("    DECIMAL      - Arbitrary precision decimal")
    print("    ARRAY_DECIMAL - Array of decimals")


def schema_evolution_best_practices():
    """Best practices for schema evolution."""
    print("\n=== Schema Evolution Best Practices ===")

    print("""
  SAFE Changes (backward compatible):
    - Adding new optional (nullable) fields
    - Adding new fields with default values
    - Removing unused fields (readers ignore unknown fields)

  UNSAFE Changes (break compatibility):
    - Changing a field's type (e.g., int32 to string)
    - Renaming a field
    - Changing a non-nullable field to nullable without defaults
    - Removing required fields without default handling

  Recommendations:
    1. Use nullable types for fields that may be added later
    2. Provide default values in your reader for missing fields
    3. Use has_field() to check for optional fields
    4. Keep old serializers around for reading legacy data
    5. Test schema migrations with real data before deployment
    """)


def main():
    basic_serialization_example()
    schema_evolution_example()
    generic_record_example()
    nullable_fields_example()
    complex_serialization_example()
    schema_service_example()
    reflective_serializer_example()
    field_kinds_reference()
    schema_evolution_best_practices()

    print("\n=== Examples Complete ===")


if __name__ == "__main__":
    main()
