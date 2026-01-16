"""Compact serialization implementation."""

import struct
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Callable, Dict, List, Optional, Type

from hazelcast.serialization.api import (
    CompactReader,
    CompactWriter,
    CompactSerializer,
)


class FieldKind(IntEnum):
    """Field type identifiers for compact serialization."""

    BOOLEAN = 0
    INT8 = 1
    INT16 = 2
    INT32 = 3
    INT64 = 4
    FLOAT32 = 5
    FLOAT64 = 6
    STRING = 7
    COMPACT = 8
    ARRAY_BOOLEAN = 9
    ARRAY_INT8 = 10
    ARRAY_INT16 = 11
    ARRAY_INT32 = 12
    ARRAY_INT64 = 13
    ARRAY_FLOAT32 = 14
    ARRAY_FLOAT64 = 15
    ARRAY_STRING = 16
    ARRAY_COMPACT = 17
    NULLABLE_BOOLEAN = 18
    NULLABLE_INT8 = 19
    NULLABLE_INT16 = 20
    NULLABLE_INT32 = 21
    NULLABLE_INT64 = 22
    NULLABLE_FLOAT32 = 23
    NULLABLE_FLOAT64 = 24
    DATE = 25
    TIME = 26
    TIMESTAMP = 27
    TIMESTAMP_WITH_TIMEZONE = 28
    DECIMAL = 29
    ARRAY_DATE = 30
    ARRAY_TIME = 31
    ARRAY_TIMESTAMP = 32
    ARRAY_DECIMAL = 33


FIELD_KIND_MAP = {
    "boolean": FieldKind.BOOLEAN,
    "int8": FieldKind.INT8,
    "int16": FieldKind.INT16,
    "int32": FieldKind.INT32,
    "int64": FieldKind.INT64,
    "float32": FieldKind.FLOAT32,
    "float64": FieldKind.FLOAT64,
    "string": FieldKind.STRING,
    "compact": FieldKind.COMPACT,
    "array_boolean": FieldKind.ARRAY_BOOLEAN,
    "array_int8": FieldKind.ARRAY_INT8,
    "array_int16": FieldKind.ARRAY_INT16,
    "array_int32": FieldKind.ARRAY_INT32,
    "array_int64": FieldKind.ARRAY_INT64,
    "array_float32": FieldKind.ARRAY_FLOAT32,
    "array_float64": FieldKind.ARRAY_FLOAT64,
    "array_string": FieldKind.ARRAY_STRING,
    "array_compact": FieldKind.ARRAY_COMPACT,
    "nullable_boolean": FieldKind.NULLABLE_BOOLEAN,
    "nullable_int8": FieldKind.NULLABLE_INT8,
    "nullable_int16": FieldKind.NULLABLE_INT16,
    "nullable_int32": FieldKind.NULLABLE_INT32,
    "nullable_int64": FieldKind.NULLABLE_INT64,
    "nullable_float32": FieldKind.NULLABLE_FLOAT32,
    "nullable_float64": FieldKind.NULLABLE_FLOAT64,
    "date": FieldKind.DATE,
    "time": FieldKind.TIME,
    "timestamp": FieldKind.TIMESTAMP,
    "timestamp_with_timezone": FieldKind.TIMESTAMP_WITH_TIMEZONE,
    "decimal": FieldKind.DECIMAL,
    "array_date": FieldKind.ARRAY_DATE,
    "array_time": FieldKind.ARRAY_TIME,
    "array_timestamp": FieldKind.ARRAY_TIMESTAMP,
    "array_decimal": FieldKind.ARRAY_DECIMAL,
}


FIELD_KIND_DEFAULTS: Dict[FieldKind, Any] = {
    FieldKind.BOOLEAN: False,
    FieldKind.INT8: 0,
    FieldKind.INT16: 0,
    FieldKind.INT32: 0,
    FieldKind.INT64: 0,
    FieldKind.FLOAT32: 0.0,
    FieldKind.FLOAT64: 0.0,
    FieldKind.STRING: None,
    FieldKind.COMPACT: None,
    FieldKind.ARRAY_BOOLEAN: None,
    FieldKind.ARRAY_INT8: None,
    FieldKind.ARRAY_INT16: None,
    FieldKind.ARRAY_INT32: None,
    FieldKind.ARRAY_INT64: None,
    FieldKind.ARRAY_FLOAT32: None,
    FieldKind.ARRAY_FLOAT64: None,
    FieldKind.ARRAY_STRING: None,
    FieldKind.ARRAY_COMPACT: None,
    FieldKind.NULLABLE_BOOLEAN: None,
    FieldKind.NULLABLE_INT8: None,
    FieldKind.NULLABLE_INT16: None,
    FieldKind.NULLABLE_INT32: None,
    FieldKind.NULLABLE_INT64: None,
    FieldKind.NULLABLE_FLOAT32: None,
    FieldKind.NULLABLE_FLOAT64: None,
    FieldKind.DATE: None,
    FieldKind.TIME: None,
    FieldKind.TIMESTAMP: None,
    FieldKind.TIMESTAMP_WITH_TIMEZONE: None,
    FieldKind.DECIMAL: None,
    FieldKind.ARRAY_DATE: None,
    FieldKind.ARRAY_TIME: None,
    FieldKind.ARRAY_TIMESTAMP: None,
    FieldKind.ARRAY_DECIMAL: None,
}


@dataclass
class FieldDescriptor:
    """Describes a field in a compact schema."""

    name: str
    field_type: str
    index: int
    offset: int = -1

    @property
    def kind(self) -> FieldKind:
        """Get the field kind enum value."""
        return FIELD_KIND_MAP.get(self.field_type, FieldKind.COMPACT)

    def is_variable_size(self) -> bool:
        """Check if this field has variable size."""
        return self.kind in (
            FieldKind.STRING,
            FieldKind.COMPACT,
            FieldKind.ARRAY_BOOLEAN,
            FieldKind.ARRAY_INT8,
            FieldKind.ARRAY_INT16,
            FieldKind.ARRAY_INT32,
            FieldKind.ARRAY_INT64,
            FieldKind.ARRAY_FLOAT32,
            FieldKind.ARRAY_FLOAT64,
            FieldKind.ARRAY_STRING,
            FieldKind.ARRAY_COMPACT,
            FieldKind.DECIMAL,
            FieldKind.ARRAY_DATE,
            FieldKind.ARRAY_TIME,
            FieldKind.ARRAY_TIMESTAMP,
            FieldKind.ARRAY_DECIMAL,
        )


@dataclass
class Schema:
    """Schema for compact serialization."""

    type_name: str
    fields: List[FieldDescriptor] = field(default_factory=list)
    schema_id: int = 0

    def __post_init__(self):
        if self.schema_id == 0 and self.type_name:
            self.schema_id = self._compute_schema_id()

    def _compute_schema_id(self) -> int:
        """Compute a schema ID based on type name and fields."""
        h = hash(self.type_name)
        for f in self.fields:
            h = h * 31 + hash(f.name) + hash(f.field_type)
        return h & 0x7FFFFFFF

    def get_field(self, name: str) -> Optional[FieldDescriptor]:
        """Get a field descriptor by name."""
        for f in self.fields:
            if f.name == name:
                return f
        return None

    def add_field(self, name: str, field_type: str) -> FieldDescriptor:
        """Add a field to the schema."""
        descriptor = FieldDescriptor(name=name, field_type=field_type, index=len(self.fields))
        self.fields.append(descriptor)
        return descriptor


class SchemaService:
    """Service for managing compact serialization schemas."""

    def __init__(self):
        self._schemas_by_id: Dict[int, Schema] = {}
        self._schemas_by_type: Dict[str, List[Schema]] = {}

    def register(self, schema: Schema) -> None:
        """Register a schema."""
        self._schemas_by_id[schema.schema_id] = schema
        if schema.type_name not in self._schemas_by_type:
            self._schemas_by_type[schema.type_name] = []
        existing_versions = self._schemas_by_type[schema.type_name]
        if schema not in existing_versions:
            existing_versions.append(schema)

    def get_by_id(self, schema_id: int) -> Optional[Schema]:
        """Get a schema by its ID."""
        return self._schemas_by_id.get(schema_id)

    def get_by_type_name(self, type_name: str) -> Optional[Schema]:
        """Get the latest schema by type name."""
        versions = self._schemas_by_type.get(type_name)
        if versions:
            return versions[-1]
        return None

    def get_all_versions(self, type_name: str) -> List[Schema]:
        """Get all schema versions for a type."""
        return self._schemas_by_type.get(type_name, [])

    def has_schema(self, schema_id: int) -> bool:
        """Check if a schema is registered."""
        return schema_id in self._schemas_by_id

    def all_schemas(self) -> List[Schema]:
        """Get all registered schemas."""
        return list(self._schemas_by_id.values())

    def is_compatible(self, old_schema: Schema, new_schema: Schema) -> bool:
        """Check if two schemas are compatible (new can read old)."""
        if old_schema.type_name != new_schema.type_name:
            return False
        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}
        for name, old_field in old_fields.items():
            if name in new_fields:
                new_field = new_fields[name]
                if old_field.field_type != new_field.field_type:
                    return False
        return True


class DefaultCompactWriter(CompactWriter):
    """Default implementation of CompactWriter."""

    def __init__(self, schema: Schema):
        self._schema = schema
        self._fields: Dict[str, Any] = {}
        self._field_types: Dict[str, str] = {}

    @property
    def fields(self) -> Dict[str, Any]:
        return self._fields

    @property
    def field_types(self) -> Dict[str, str]:
        return self._field_types

    def write_boolean(self, field_name: str, value: bool) -> None:
        self._fields[field_name] = value
        self._field_types[field_name] = "boolean"

    def write_int8(self, field_name: str, value: int) -> None:
        self._fields[field_name] = value
        self._field_types[field_name] = "int8"

    def write_int16(self, field_name: str, value: int) -> None:
        self._fields[field_name] = value
        self._field_types[field_name] = "int16"

    def write_int32(self, field_name: str, value: int) -> None:
        self._fields[field_name] = value
        self._field_types[field_name] = "int32"

    def write_int64(self, field_name: str, value: int) -> None:
        self._fields[field_name] = value
        self._field_types[field_name] = "int64"

    def write_float32(self, field_name: str, value: float) -> None:
        self._fields[field_name] = value
        self._field_types[field_name] = "float32"

    def write_float64(self, field_name: str, value: float) -> None:
        self._fields[field_name] = value
        self._field_types[field_name] = "float64"

    def write_string(self, field_name: str, value: str) -> None:
        self._fields[field_name] = value
        self._field_types[field_name] = "string"

    def write_compact(self, field_name: str, value: Any) -> None:
        self._fields[field_name] = value
        self._field_types[field_name] = "compact"

    def write_array_of_boolean(self, field_name: str, value: list) -> None:
        self._fields[field_name] = value
        self._field_types[field_name] = "array_boolean"

    def write_array_of_int32(self, field_name: str, value: list) -> None:
        self._fields[field_name] = value
        self._field_types[field_name] = "array_int32"

    def write_array_of_string(self, field_name: str, value: list) -> None:
        self._fields[field_name] = value
        self._field_types[field_name] = "array_string"

    def write_nullable_boolean(self, field_name: str, value: bool) -> None:
        self._fields[field_name] = value
        self._field_types[field_name] = "nullable_boolean"

    def write_nullable_int32(self, field_name: str, value: int) -> None:
        self._fields[field_name] = value
        self._field_types[field_name] = "nullable_int32"


class DefaultCompactReader(CompactReader):
    """Default implementation of CompactReader with schema evolution support."""

    def __init__(
        self,
        schema: Schema,
        fields: Dict[str, Any],
        reader_schema: Schema = None,
    ):
        self._schema = schema
        self._fields = fields
        self._reader_schema = reader_schema or schema

    def _get_field_value(self, field_name: str, field_kind: FieldKind) -> Any:
        """Get field value with schema evolution support."""
        if field_name in self._fields:
            return self._fields[field_name]
        return FIELD_KIND_DEFAULTS.get(field_kind)

    def _check_field_kind(self, field_name: str, expected: FieldKind) -> None:
        """Check that the field has the expected kind."""
        field = self._schema.get_field(field_name)
        if field is not None and field.kind != expected:
            raise TypeError(
                f"Field '{field_name}' is of type {field.kind.name}, not {expected.name}"
            )

    def read_boolean(self, field_name: str) -> bool:
        self._check_field_kind(field_name, FieldKind.BOOLEAN)
        value = self._get_field_value(field_name, FieldKind.BOOLEAN)
        return value if value is not None else False

    def read_int8(self, field_name: str) -> int:
        self._check_field_kind(field_name, FieldKind.INT8)
        value = self._get_field_value(field_name, FieldKind.INT8)
        return value if value is not None else 0

    def read_int16(self, field_name: str) -> int:
        self._check_field_kind(field_name, FieldKind.INT16)
        value = self._get_field_value(field_name, FieldKind.INT16)
        return value if value is not None else 0

    def read_int32(self, field_name: str) -> int:
        self._check_field_kind(field_name, FieldKind.INT32)
        value = self._get_field_value(field_name, FieldKind.INT32)
        return value if value is not None else 0

    def read_int64(self, field_name: str) -> int:
        self._check_field_kind(field_name, FieldKind.INT64)
        value = self._get_field_value(field_name, FieldKind.INT64)
        return value if value is not None else 0

    def read_float32(self, field_name: str) -> float:
        self._check_field_kind(field_name, FieldKind.FLOAT32)
        value = self._get_field_value(field_name, FieldKind.FLOAT32)
        return value if value is not None else 0.0

    def read_float64(self, field_name: str) -> float:
        self._check_field_kind(field_name, FieldKind.FLOAT64)
        value = self._get_field_value(field_name, FieldKind.FLOAT64)
        return value if value is not None else 0.0

    def read_string(self, field_name: str) -> Optional[str]:
        self._check_field_kind(field_name, FieldKind.STRING)
        return self._get_field_value(field_name, FieldKind.STRING)

    def read_compact(self, field_name: str) -> Any:
        self._check_field_kind(field_name, FieldKind.COMPACT)
        return self._get_field_value(field_name, FieldKind.COMPACT)

    def read_array_of_boolean(self, field_name: str) -> Optional[List[bool]]:
        self._check_field_kind(field_name, FieldKind.ARRAY_BOOLEAN)
        return self._get_field_value(field_name, FieldKind.ARRAY_BOOLEAN)

    def read_array_of_int8(self, field_name: str) -> Optional[List[int]]:
        self._check_field_kind(field_name, FieldKind.ARRAY_INT8)
        return self._get_field_value(field_name, FieldKind.ARRAY_INT8)

    def read_array_of_int16(self, field_name: str) -> Optional[List[int]]:
        self._check_field_kind(field_name, FieldKind.ARRAY_INT16)
        return self._get_field_value(field_name, FieldKind.ARRAY_INT16)

    def read_array_of_int32(self, field_name: str) -> Optional[List[int]]:
        self._check_field_kind(field_name, FieldKind.ARRAY_INT32)
        return self._get_field_value(field_name, FieldKind.ARRAY_INT32)

    def read_array_of_int64(self, field_name: str) -> Optional[List[int]]:
        self._check_field_kind(field_name, FieldKind.ARRAY_INT64)
        return self._get_field_value(field_name, FieldKind.ARRAY_INT64)

    def read_array_of_float32(self, field_name: str) -> Optional[List[float]]:
        self._check_field_kind(field_name, FieldKind.ARRAY_FLOAT32)
        return self._get_field_value(field_name, FieldKind.ARRAY_FLOAT32)

    def read_array_of_float64(self, field_name: str) -> Optional[List[float]]:
        self._check_field_kind(field_name, FieldKind.ARRAY_FLOAT64)
        return self._get_field_value(field_name, FieldKind.ARRAY_FLOAT64)

    def read_array_of_string(self, field_name: str) -> Optional[List[str]]:
        self._check_field_kind(field_name, FieldKind.ARRAY_STRING)
        return self._get_field_value(field_name, FieldKind.ARRAY_STRING)

    def read_array_of_compact(self, field_name: str) -> Optional[List[Any]]:
        self._check_field_kind(field_name, FieldKind.ARRAY_COMPACT)
        return self._get_field_value(field_name, FieldKind.ARRAY_COMPACT)

    def read_nullable_boolean(self, field_name: str) -> Optional[bool]:
        self._check_field_kind(field_name, FieldKind.NULLABLE_BOOLEAN)
        return self._get_field_value(field_name, FieldKind.NULLABLE_BOOLEAN)

    def read_nullable_int8(self, field_name: str) -> Optional[int]:
        self._check_field_kind(field_name, FieldKind.NULLABLE_INT8)
        return self._get_field_value(field_name, FieldKind.NULLABLE_INT8)

    def read_nullable_int16(self, field_name: str) -> Optional[int]:
        self._check_field_kind(field_name, FieldKind.NULLABLE_INT16)
        return self._get_field_value(field_name, FieldKind.NULLABLE_INT16)

    def read_nullable_int32(self, field_name: str) -> Optional[int]:
        self._check_field_kind(field_name, FieldKind.NULLABLE_INT32)
        return self._get_field_value(field_name, FieldKind.NULLABLE_INT32)

    def read_nullable_int64(self, field_name: str) -> Optional[int]:
        self._check_field_kind(field_name, FieldKind.NULLABLE_INT64)
        return self._get_field_value(field_name, FieldKind.NULLABLE_INT64)

    def read_nullable_float32(self, field_name: str) -> Optional[float]:
        self._check_field_kind(field_name, FieldKind.NULLABLE_FLOAT32)
        return self._get_field_value(field_name, FieldKind.NULLABLE_FLOAT32)

    def read_nullable_float64(self, field_name: str) -> Optional[float]:
        self._check_field_kind(field_name, FieldKind.NULLABLE_FLOAT64)
        return self._get_field_value(field_name, FieldKind.NULLABLE_FLOAT64)

    def get_field_kind(self, field_name: str) -> Optional[FieldKind]:
        """Get the kind of a field, or None if not present."""
        field = self._schema.get_field(field_name)
        return field.kind if field else None

    def has_field(self, field_name: str) -> bool:
        """Check if a field exists in the schema."""
        return self._schema.get_field(field_name) is not None

    def get_field_names(self) -> List[str]:
        """Get all field names in the schema."""
        return [f.name for f in self._schema.fields]


class CompactStreamWriter:
    """Binary stream writer for compact serialization."""

    def __init__(self):
        self._buffer = bytearray()

    def write_boolean(self, value: bool) -> None:
        self._buffer.append(1 if value else 0)

    def write_int8(self, value: int) -> None:
        self._buffer.extend(struct.pack("<b", value))

    def write_int16(self, value: int) -> None:
        self._buffer.extend(struct.pack("<h", value))

    def write_int32(self, value: int) -> None:
        self._buffer.extend(struct.pack("<i", value))

    def write_int64(self, value: int) -> None:
        self._buffer.extend(struct.pack("<q", value))

    def write_float32(self, value: float) -> None:
        self._buffer.extend(struct.pack("<f", value))

    def write_float64(self, value: float) -> None:
        self._buffer.extend(struct.pack("<d", value))

    def write_string(self, value: str) -> None:
        if value is None:
            self.write_int32(-1)
        else:
            encoded = value.encode("utf-8")
            self.write_int32(len(encoded))
            self._buffer.extend(encoded)

    def write_bytes(self, value: bytes) -> None:
        self._buffer.extend(value)

    def to_bytes(self) -> bytes:
        return bytes(self._buffer)


class CompactStreamReader:
    """Binary stream reader for compact serialization."""

    def __init__(self, data: bytes):
        self._data = data
        self._pos = 0

    def read_boolean(self) -> bool:
        value = self._data[self._pos]
        self._pos += 1
        return value != 0

    def read_int8(self) -> int:
        value = struct.unpack_from("<b", self._data, self._pos)[0]
        self._pos += 1
        return value

    def read_int16(self) -> int:
        value = struct.unpack_from("<h", self._data, self._pos)[0]
        self._pos += 2
        return value

    def read_int32(self) -> int:
        value = struct.unpack_from("<i", self._data, self._pos)[0]
        self._pos += 4
        return value

    def read_int64(self) -> int:
        value = struct.unpack_from("<q", self._data, self._pos)[0]
        self._pos += 8
        return value

    def read_float32(self) -> float:
        value = struct.unpack_from("<f", self._data, self._pos)[0]
        self._pos += 4
        return value

    def read_float64(self) -> float:
        value = struct.unpack_from("<d", self._data, self._pos)[0]
        self._pos += 8
        return value

    def read_string(self) -> Optional[str]:
        length = self.read_int32()
        if length < 0:
            return None
        data = self._data[self._pos:self._pos + length]
        self._pos += length
        return data.decode("utf-8")

    def read_bytes(self, length: int) -> bytes:
        data = self._data[self._pos:self._pos + length]
        self._pos += length
        return data

    @property
    def position(self) -> int:
        return self._pos


class CompactSerializationService:
    """Service for compact serialization."""

    COMPACT_TYPE_ID = -55

    def __init__(self):
        self._serializers: Dict[str, CompactSerializer] = {}
        self._class_to_type_name: Dict[type, str] = {}
        self._schema_service = SchemaService()

    @property
    def schema_service(self) -> SchemaService:
        """Get the schema service."""
        return self._schema_service

    def register_serializer(self, serializer: CompactSerializer) -> None:
        """Register a compact serializer."""
        self._serializers[serializer.type_name] = serializer
        self._class_to_type_name[serializer.clazz] = serializer.type_name

    def get_serializer(self, type_name: str) -> Optional[CompactSerializer]:
        """Get a serializer by type name."""
        return self._serializers.get(type_name)

    def get_serializer_for_class(self, clazz: type) -> Optional[CompactSerializer]:
        """Get a serializer for a class."""
        type_name = self._class_to_type_name.get(clazz)
        if type_name:
            return self._serializers.get(type_name)
        return None

    def register_schema(self, schema: Schema) -> None:
        """Register a schema."""
        self._schema_service.register(schema)

    def get_schema(self, schema_id: int) -> Optional[Schema]:
        """Get a schema by ID."""
        return self._schema_service.get_by_id(schema_id)

    def serialize(self, obj: Any) -> bytes:
        """Serialize an object using compact format."""
        clazz = type(obj)
        serializer = self.get_serializer_for_class(clazz)
        if serializer is None:
            raise ValueError(f"No compact serializer registered for {clazz}")

        schema = Schema(type_name=serializer.type_name)
        writer = DefaultCompactWriter(schema)
        serializer.write(writer, obj)

        fields_with_types = []
        for name, value in writer.fields.items():
            field_type = writer.field_types.get(name, "compact")
            fields_with_types.append((name, field_type, value))

        schema.fields = [
            FieldDescriptor(name=name, field_type=ft, index=i)
            for i, (name, ft, _) in enumerate(fields_with_types)
        ]
        schema.schema_id = schema._compute_schema_id()
        self.register_schema(schema)

        return self._encode(schema, writer.fields, writer.field_types)

    def deserialize(self, data: bytes, type_name: str) -> Any:
        """Deserialize bytes to an object with schema evolution support."""
        serializer = self.get_serializer(type_name)
        if serializer is None:
            raise ValueError(f"No compact serializer registered for {type_name}")

        schema_id, fields = self._decode(data)
        writer_schema = self.get_schema(schema_id)
        if writer_schema is None:
            writer_schema = Schema(type_name=type_name, schema_id=schema_id)

        reader_schema = self._schema_service.get_by_type_name(type_name)
        if reader_schema is None:
            reader_schema = writer_schema

        reader = DefaultCompactReader(writer_schema, fields, reader_schema)
        return serializer.read(reader)

    def deserialize_with_schema(
        self, data: bytes, type_name: str, reader_schema: Schema
    ) -> Any:
        """Deserialize with explicit reader schema for evolution."""
        serializer = self.get_serializer(type_name)
        if serializer is None:
            raise ValueError(f"No compact serializer registered for {type_name}")

        schema_id, fields = self._decode(data)
        writer_schema = self.get_schema(schema_id)
        if writer_schema is None:
            writer_schema = Schema(type_name=type_name, schema_id=schema_id)

        reader = DefaultCompactReader(writer_schema, fields, reader_schema)
        return serializer.read(reader)

    def check_schema_compatibility(
        self, old_schema_id: int, new_schema_id: int
    ) -> bool:
        """Check if schemas are compatible for evolution."""
        old_schema = self.get_schema(old_schema_id)
        new_schema = self.get_schema(new_schema_id)
        if old_schema is None or new_schema is None:
            return False
        return self._schema_service.is_compatible(old_schema, new_schema)

    def _encode(self, schema: Schema, fields: Dict[str, Any], field_types: Dict[str, str]) -> bytes:
        """Encode schema and fields to bytes."""
        writer = CompactStreamWriter()
        writer.write_int32(schema.schema_id)
        writer.write_int32(len(fields))

        for name, value in fields.items():
            field_type = field_types.get(name, "compact")
            writer.write_string(name)
            writer.write_int8(FIELD_KIND_MAP.get(field_type, FieldKind.COMPACT))
            self._write_field_value(writer, value, field_type)

        return writer.to_bytes()

    def _decode(self, data: bytes) -> tuple:
        """Decode bytes to schema ID and fields."""
        reader = CompactStreamReader(data)
        schema_id = reader.read_int32()
        field_count = reader.read_int32()

        fields = {}
        for _ in range(field_count):
            name = reader.read_string()
            field_kind = reader.read_int8()
            value = self._read_field_value(reader, field_kind)
            fields[name] = value

        return schema_id, fields

    def _write_field_value(self, writer: CompactStreamWriter, value: Any, field_type: str) -> None:
        """Write a field value based on its type."""
        if field_type == "boolean":
            writer.write_boolean(value)
        elif field_type == "int8":
            writer.write_int8(value)
        elif field_type == "int16":
            writer.write_int16(value)
        elif field_type == "int32":
            writer.write_int32(value)
        elif field_type == "int64":
            writer.write_int64(value)
        elif field_type == "float32":
            writer.write_float32(value)
        elif field_type == "float64":
            writer.write_float64(value)
        elif field_type == "string":
            writer.write_string(value)
        elif field_type == "nullable_boolean":
            writer.write_boolean(value is not None)
            if value is not None:
                writer.write_boolean(value)
        elif field_type == "nullable_int8":
            writer.write_boolean(value is not None)
            if value is not None:
                writer.write_int8(value)
        elif field_type == "nullable_int16":
            writer.write_boolean(value is not None)
            if value is not None:
                writer.write_int16(value)
        elif field_type == "nullable_int32":
            writer.write_boolean(value is not None)
            if value is not None:
                writer.write_int32(value)
        elif field_type == "nullable_int64":
            writer.write_boolean(value is not None)
            if value is not None:
                writer.write_int64(value)
        elif field_type == "nullable_float32":
            writer.write_boolean(value is not None)
            if value is not None:
                writer.write_float32(value)
        elif field_type == "nullable_float64":
            writer.write_boolean(value is not None)
            if value is not None:
                writer.write_float64(value)
        elif field_type == "array_boolean":
            self._write_array(writer, value, writer.write_boolean)
        elif field_type == "array_int8":
            self._write_array(writer, value, writer.write_int8)
        elif field_type == "array_int16":
            self._write_array(writer, value, writer.write_int16)
        elif field_type == "array_int32":
            self._write_array(writer, value, writer.write_int32)
        elif field_type == "array_int64":
            self._write_array(writer, value, writer.write_int64)
        elif field_type == "array_float32":
            self._write_array(writer, value, writer.write_float32)
        elif field_type == "array_float64":
            self._write_array(writer, value, writer.write_float64)
        elif field_type == "array_string":
            self._write_array(writer, value, writer.write_string)
        elif field_type == "compact":
            writer.write_boolean(value is not None)
        elif field_type == "array_compact":
            if value is None:
                writer.write_int32(-1)
            else:
                writer.write_int32(len(value))
        else:
            writer.write_boolean(value is not None)

    def _read_field_value(self, reader: CompactStreamReader, field_kind: int) -> Any:
        """Read a field value based on its kind."""
        if field_kind == FieldKind.BOOLEAN:
            return reader.read_boolean()
        elif field_kind == FieldKind.INT8:
            return reader.read_int8()
        elif field_kind == FieldKind.INT16:
            return reader.read_int16()
        elif field_kind == FieldKind.INT32:
            return reader.read_int32()
        elif field_kind == FieldKind.INT64:
            return reader.read_int64()
        elif field_kind == FieldKind.FLOAT32:
            return reader.read_float32()
        elif field_kind == FieldKind.FLOAT64:
            return reader.read_float64()
        elif field_kind == FieldKind.STRING:
            return reader.read_string()
        elif field_kind == FieldKind.NULLABLE_BOOLEAN:
            if reader.read_boolean():
                return reader.read_boolean()
            return None
        elif field_kind == FieldKind.NULLABLE_INT8:
            if reader.read_boolean():
                return reader.read_int8()
            return None
        elif field_kind == FieldKind.NULLABLE_INT16:
            if reader.read_boolean():
                return reader.read_int16()
            return None
        elif field_kind == FieldKind.NULLABLE_INT32:
            if reader.read_boolean():
                return reader.read_int32()
            return None
        elif field_kind == FieldKind.NULLABLE_INT64:
            if reader.read_boolean():
                return reader.read_int64()
            return None
        elif field_kind == FieldKind.NULLABLE_FLOAT32:
            if reader.read_boolean():
                return reader.read_float32()
            return None
        elif field_kind == FieldKind.NULLABLE_FLOAT64:
            if reader.read_boolean():
                return reader.read_float64()
            return None
        elif field_kind == FieldKind.ARRAY_BOOLEAN:
            return self._read_array(reader, reader.read_boolean)
        elif field_kind == FieldKind.ARRAY_INT8:
            return self._read_array(reader, reader.read_int8)
        elif field_kind == FieldKind.ARRAY_INT16:
            return self._read_array(reader, reader.read_int16)
        elif field_kind == FieldKind.ARRAY_INT32:
            return self._read_array(reader, reader.read_int32)
        elif field_kind == FieldKind.ARRAY_INT64:
            return self._read_array(reader, reader.read_int64)
        elif field_kind == FieldKind.ARRAY_FLOAT32:
            return self._read_array(reader, reader.read_float32)
        elif field_kind == FieldKind.ARRAY_FLOAT64:
            return self._read_array(reader, reader.read_float64)
        elif field_kind == FieldKind.ARRAY_STRING:
            return self._read_array(reader, reader.read_string)
        elif field_kind == FieldKind.COMPACT:
            has_value = reader.read_boolean()
            return None
        elif field_kind == FieldKind.ARRAY_COMPACT:
            return self._read_array(reader, lambda: None)
        else:
            has_value = reader.read_boolean()
            return None

    def to_generic_record(self, data: bytes, type_name: str) -> GenericRecord:
        """Deserialize bytes to a GenericRecord."""
        schema_id, fields = self._decode(data)
        schema = self.get_schema(schema_id)
        if schema is None:
            schema = Schema(type_name=type_name, schema_id=schema_id)
            for i, (name, value) in enumerate(fields.items()):
                field_type = self._infer_field_type(value)
                schema.fields.append(FieldDescriptor(name=name, field_type=field_type, index=i))
        return GenericRecord(schema, fields)

    def _infer_field_type(self, value: Any) -> str:
        """Infer field type from runtime value."""
        if value is None:
            return "compact"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "int64"
        if isinstance(value, float):
            return "float64"
        if isinstance(value, str):
            return "string"
        if isinstance(value, list):
            if not value:
                return "array_int32"
            first = value[0]
            if isinstance(first, bool):
                return "array_boolean"
            if isinstance(first, int):
                return "array_int32"
            if isinstance(first, str):
                return "array_string"
        return "compact"

    def _write_array(self, writer: CompactStreamWriter, items: list, write_fn: Callable) -> None:
        """Write an array of items."""
        if items is None:
            writer.write_int32(-1)
        else:
            writer.write_int32(len(items))
            for item in items:
                write_fn(item)

    def _read_array(self, reader: CompactStreamReader, read_fn: Callable) -> Optional[list]:
        """Read an array of items."""
        length = reader.read_int32()
        if length < 0:
            return None
        return [read_fn() for _ in range(length)]


class GenericRecord:
    """A generic, schema-aware record for compact serialization.

    GenericRecord provides schema-less access to compact serialized data
    without requiring a specific class. It can be used for reading data
    when the original class is not available, or for dynamic data access.
    """

    def __init__(self, schema: Schema, fields: Dict[str, Any] = None):
        self._schema = schema
        self._fields = fields or {}

    @property
    def schema(self) -> Schema:
        """Get the schema of this record."""
        return self._schema

    @property
    def type_name(self) -> str:
        """Get the type name of this record."""
        return self._schema.type_name

    def has_field(self, field_name: str) -> bool:
        """Check if this record has a field with the given name."""
        return self._schema.get_field(field_name) is not None

    def get_field_kind(self, field_name: str) -> Optional[FieldKind]:
        """Get the kind of a field."""
        field = self._schema.get_field(field_name)
        return field.kind if field else None

    def get_field_names(self) -> List[str]:
        """Get all field names."""
        return [f.name for f in self._schema.fields]

    def get_boolean(self, field_name: str) -> bool:
        """Get a boolean field value."""
        self._check_field(field_name, FieldKind.BOOLEAN)
        return self._fields.get(field_name, False)

    def get_int8(self, field_name: str) -> int:
        """Get an int8 field value."""
        self._check_field(field_name, FieldKind.INT8)
        return self._fields.get(field_name, 0)

    def get_int16(self, field_name: str) -> int:
        """Get an int16 field value."""
        self._check_field(field_name, FieldKind.INT16)
        return self._fields.get(field_name, 0)

    def get_int32(self, field_name: str) -> int:
        """Get an int32 field value."""
        self._check_field(field_name, FieldKind.INT32)
        return self._fields.get(field_name, 0)

    def get_int64(self, field_name: str) -> int:
        """Get an int64 field value."""
        self._check_field(field_name, FieldKind.INT64)
        return self._fields.get(field_name, 0)

    def get_float32(self, field_name: str) -> float:
        """Get a float32 field value."""
        self._check_field(field_name, FieldKind.FLOAT32)
        return self._fields.get(field_name, 0.0)

    def get_float64(self, field_name: str) -> float:
        """Get a float64 field value."""
        self._check_field(field_name, FieldKind.FLOAT64)
        return self._fields.get(field_name, 0.0)

    def get_string(self, field_name: str) -> Optional[str]:
        """Get a string field value."""
        self._check_field(field_name, FieldKind.STRING)
        return self._fields.get(field_name)

    def get_compact(self, field_name: str) -> Optional["GenericRecord"]:
        """Get a nested compact record."""
        self._check_field(field_name, FieldKind.COMPACT)
        return self._fields.get(field_name)

    def get_array_of_boolean(self, field_name: str) -> Optional[List[bool]]:
        """Get a boolean array field value."""
        self._check_field(field_name, FieldKind.ARRAY_BOOLEAN)
        return self._fields.get(field_name)

    def get_array_of_int32(self, field_name: str) -> Optional[List[int]]:
        """Get an int32 array field value."""
        self._check_field(field_name, FieldKind.ARRAY_INT32)
        return self._fields.get(field_name)

    def get_array_of_int64(self, field_name: str) -> Optional[List[int]]:
        """Get an int64 array field value."""
        self._check_field(field_name, FieldKind.ARRAY_INT64)
        return self._fields.get(field_name)

    def get_array_of_float32(self, field_name: str) -> Optional[List[float]]:
        """Get a float32 array field value."""
        self._check_field(field_name, FieldKind.ARRAY_FLOAT32)
        return self._fields.get(field_name)

    def get_array_of_float64(self, field_name: str) -> Optional[List[float]]:
        """Get a float64 array field value."""
        self._check_field(field_name, FieldKind.ARRAY_FLOAT64)
        return self._fields.get(field_name)

    def get_array_of_string(self, field_name: str) -> Optional[List[str]]:
        """Get a string array field value."""
        self._check_field(field_name, FieldKind.ARRAY_STRING)
        return self._fields.get(field_name)

    def get_array_of_compact(self, field_name: str) -> Optional[List["GenericRecord"]]:
        """Get an array of nested compact records."""
        self._check_field(field_name, FieldKind.ARRAY_COMPACT)
        return self._fields.get(field_name)

    def get_nullable_boolean(self, field_name: str) -> Optional[bool]:
        """Get a nullable boolean field value."""
        self._check_field(field_name, FieldKind.NULLABLE_BOOLEAN)
        return self._fields.get(field_name)

    def get_nullable_int32(self, field_name: str) -> Optional[int]:
        """Get a nullable int32 field value."""
        self._check_field(field_name, FieldKind.NULLABLE_INT32)
        return self._fields.get(field_name)

    def get_nullable_int64(self, field_name: str) -> Optional[int]:
        """Get a nullable int64 field value."""
        self._check_field(field_name, FieldKind.NULLABLE_INT64)
        return self._fields.get(field_name)

    def _check_field(self, field_name: str, expected_kind: FieldKind) -> None:
        """Check that a field exists and has the expected kind."""
        field = self._schema.get_field(field_name)
        if field is None:
            raise ValueError(f"Field '{field_name}' does not exist in schema")
        if field.kind != expected_kind:
            raise TypeError(
                f"Field '{field_name}' is {field.kind.name}, not {expected_kind.name}"
            )

    def to_dict(self) -> Dict[str, Any]:
        """Convert this record to a dictionary."""
        result = {}
        for field_name, value in self._fields.items():
            if isinstance(value, GenericRecord):
                result[field_name] = value.to_dict()
            elif isinstance(value, list) and value and isinstance(value[0], GenericRecord):
                result[field_name] = [v.to_dict() for v in value]
            else:
                result[field_name] = value
        return result

    def new_builder(self) -> "GenericRecordBuilder":
        """Create a new builder from this record's schema."""
        return GenericRecordBuilder(self._schema.type_name).from_record(self)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GenericRecord):
            return False
        return self._schema.type_name == other._schema.type_name and self._fields == other._fields

    def __hash__(self) -> int:
        return hash((self._schema.type_name, tuple(sorted(self._fields.items()))))

    def __repr__(self) -> str:
        return f"GenericRecord(type_name={self._schema.type_name!r}, fields={self._fields!r})"


class GenericRecordBuilder:
    """Builder for creating GenericRecord instances."""

    def __init__(self, type_name: str):
        self._type_name = type_name
        self._fields: Dict[str, Any] = {}
        self._field_types: Dict[str, str] = {}

    def from_record(self, record: GenericRecord) -> "GenericRecordBuilder":
        """Initialize builder from an existing record."""
        self._fields = dict(record._fields)
        for field in record._schema.fields:
            self._field_types[field.name] = field.field_type
        return self

    def set_boolean(self, field_name: str, value: bool) -> "GenericRecordBuilder":
        """Set a boolean field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "boolean"
        return self

    def set_int8(self, field_name: str, value: int) -> "GenericRecordBuilder":
        """Set an int8 field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "int8"
        return self

    def set_int16(self, field_name: str, value: int) -> "GenericRecordBuilder":
        """Set an int16 field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "int16"
        return self

    def set_int32(self, field_name: str, value: int) -> "GenericRecordBuilder":
        """Set an int32 field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "int32"
        return self

    def set_int64(self, field_name: str, value: int) -> "GenericRecordBuilder":
        """Set an int64 field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "int64"
        return self

    def set_float32(self, field_name: str, value: float) -> "GenericRecordBuilder":
        """Set a float32 field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "float32"
        return self

    def set_float64(self, field_name: str, value: float) -> "GenericRecordBuilder":
        """Set a float64 field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "float64"
        return self

    def set_string(self, field_name: str, value: Optional[str]) -> "GenericRecordBuilder":
        """Set a string field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "string"
        return self

    def set_compact(self, field_name: str, value: Optional[GenericRecord]) -> "GenericRecordBuilder":
        """Set a nested compact record field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "compact"
        return self

    def set_array_of_boolean(self, field_name: str, value: Optional[List[bool]]) -> "GenericRecordBuilder":
        """Set a boolean array field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "array_boolean"
        return self

    def set_array_of_int8(self, field_name: str, value: Optional[List[int]]) -> "GenericRecordBuilder":
        """Set an int8 array field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "array_int8"
        return self

    def set_array_of_int16(self, field_name: str, value: Optional[List[int]]) -> "GenericRecordBuilder":
        """Set an int16 array field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "array_int16"
        return self

    def set_array_of_int32(self, field_name: str, value: Optional[List[int]]) -> "GenericRecordBuilder":
        """Set an int32 array field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "array_int32"
        return self

    def set_array_of_int64(self, field_name: str, value: Optional[List[int]]) -> "GenericRecordBuilder":
        """Set an int64 array field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "array_int64"
        return self

    def set_array_of_float32(self, field_name: str, value: Optional[List[float]]) -> "GenericRecordBuilder":
        """Set a float32 array field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "array_float32"
        return self

    def set_array_of_float64(self, field_name: str, value: Optional[List[float]]) -> "GenericRecordBuilder":
        """Set a float64 array field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "array_float64"
        return self

    def set_array_of_string(self, field_name: str, value: Optional[List[str]]) -> "GenericRecordBuilder":
        """Set a string array field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "array_string"
        return self

    def set_array_of_compact(
        self, field_name: str, value: Optional[List[GenericRecord]]
    ) -> "GenericRecordBuilder":
        """Set an array of nested compact records."""
        self._fields[field_name] = value
        self._field_types[field_name] = "array_compact"
        return self

    def set_nullable_boolean(self, field_name: str, value: Optional[bool]) -> "GenericRecordBuilder":
        """Set a nullable boolean field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "nullable_boolean"
        return self

    def set_nullable_int8(self, field_name: str, value: Optional[int]) -> "GenericRecordBuilder":
        """Set a nullable int8 field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "nullable_int8"
        return self

    def set_nullable_int16(self, field_name: str, value: Optional[int]) -> "GenericRecordBuilder":
        """Set a nullable int16 field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "nullable_int16"
        return self

    def set_nullable_int32(self, field_name: str, value: Optional[int]) -> "GenericRecordBuilder":
        """Set a nullable int32 field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "nullable_int32"
        return self

    def set_nullable_int64(self, field_name: str, value: Optional[int]) -> "GenericRecordBuilder":
        """Set a nullable int64 field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "nullable_int64"
        return self

    def set_nullable_float32(self, field_name: str, value: Optional[float]) -> "GenericRecordBuilder":
        """Set a nullable float32 field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "nullable_float32"
        return self

    def set_nullable_float64(self, field_name: str, value: Optional[float]) -> "GenericRecordBuilder":
        """Set a nullable float64 field."""
        self._fields[field_name] = value
        self._field_types[field_name] = "nullable_float64"
        return self

    def build(self) -> GenericRecord:
        """Build the GenericRecord."""
        schema = Schema(type_name=self._type_name)
        for i, (name, field_type) in enumerate(self._field_types.items()):
            schema.fields.append(FieldDescriptor(name=name, field_type=field_type, index=i))
        schema.schema_id = schema._compute_schema_id()
        return GenericRecord(schema, dict(self._fields))


class GenericRecordSerializer(CompactSerializer[GenericRecord]):
    """Serializer for GenericRecord objects."""

    def __init__(self, type_name: str):
        self._type_name = type_name

    @property
    def type_name(self) -> str:
        return self._type_name

    @property
    def clazz(self) -> type:
        return GenericRecord

    def write(self, writer: CompactWriter, obj: GenericRecord) -> None:
        """Write a GenericRecord using compact format."""
        for field in obj._schema.fields:
            value = obj._fields.get(field.name)
            self._write_field(writer, field.name, value, field.kind)

    def read(self, reader: CompactReader) -> GenericRecord:
        """Read a GenericRecord using compact format."""
        fields = {}
        for field_name in reader.get_field_names():
            kind = reader.get_field_kind(field_name)
            if kind is not None:
                fields[field_name] = self._read_field(reader, field_name, kind)

        schema = Schema(type_name=self._type_name)
        for i, (name, value) in enumerate(fields.items()):
            field_type = self._infer_field_type(value)
            schema.fields.append(FieldDescriptor(name=name, field_type=field_type, index=i))
        schema.schema_id = schema._compute_schema_id()

        return GenericRecord(schema, fields)

    def _write_field(self, writer: CompactWriter, name: str, value: Any, kind: FieldKind) -> None:
        """Write a field based on its kind."""
        if kind == FieldKind.BOOLEAN:
            writer.write_boolean(name, value)
        elif kind == FieldKind.INT8:
            writer.write_int8(name, value)
        elif kind == FieldKind.INT16:
            writer.write_int16(name, value)
        elif kind == FieldKind.INT32:
            writer.write_int32(name, value)
        elif kind == FieldKind.INT64:
            writer.write_int64(name, value)
        elif kind == FieldKind.FLOAT32:
            writer.write_float32(name, value)
        elif kind == FieldKind.FLOAT64:
            writer.write_float64(name, value)
        elif kind == FieldKind.STRING:
            writer.write_string(name, value)
        elif kind == FieldKind.COMPACT:
            writer.write_compact(name, value)
        elif kind == FieldKind.ARRAY_BOOLEAN:
            writer.write_array_of_boolean(name, value)
        elif kind == FieldKind.ARRAY_INT32:
            writer.write_array_of_int32(name, value)
        elif kind == FieldKind.ARRAY_STRING:
            writer.write_array_of_string(name, value)
        elif kind == FieldKind.NULLABLE_BOOLEAN:
            writer.write_nullable_boolean(name, value)
        elif kind == FieldKind.NULLABLE_INT32:
            writer.write_nullable_int32(name, value)

    def _read_field(self, reader: CompactReader, name: str, kind: FieldKind) -> Any:
        """Read a field based on its kind."""
        if kind == FieldKind.BOOLEAN:
            return reader.read_boolean(name)
        elif kind == FieldKind.INT8:
            return reader.read_int8(name)
        elif kind == FieldKind.INT16:
            return reader.read_int16(name)
        elif kind == FieldKind.INT32:
            return reader.read_int32(name)
        elif kind == FieldKind.INT64:
            return reader.read_int64(name)
        elif kind == FieldKind.FLOAT32:
            return reader.read_float32(name)
        elif kind == FieldKind.FLOAT64:
            return reader.read_float64(name)
        elif kind == FieldKind.STRING:
            return reader.read_string(name)
        elif kind == FieldKind.COMPACT:
            return reader.read_compact(name)
        elif kind == FieldKind.ARRAY_BOOLEAN:
            return reader.read_array_of_boolean(name)
        elif kind == FieldKind.ARRAY_INT32:
            return reader.read_array_of_int32(name)
        elif kind == FieldKind.ARRAY_STRING:
            return reader.read_array_of_string(name)
        elif kind == FieldKind.NULLABLE_BOOLEAN:
            return reader.read_nullable_boolean(name)
        elif kind == FieldKind.NULLABLE_INT32:
            return reader.read_nullable_int32(name)
        return None

    def _infer_field_type(self, value: Any) -> str:
        """Infer field type from value."""
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "int32"
        elif isinstance(value, float):
            return "float64"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, GenericRecord):
            return "compact"
        elif isinstance(value, list):
            if not value:
                return "array_int32"
            first = value[0]
            if isinstance(first, bool):
                return "array_boolean"
            elif isinstance(first, int):
                return "array_int32"
            elif isinstance(first, str):
                return "array_string"
            elif isinstance(first, GenericRecord):
                return "array_compact"
        return "compact"


class ReflectiveCompactSerializer(CompactSerializer):
    """Compact serializer that uses reflection on dataclasses."""

    def __init__(self, clazz: type, type_name: str = None):
        self._clazz = clazz
        self._type_name = type_name or clazz.__name__

    @property
    def type_name(self) -> str:
        return self._type_name

    @property
    def clazz(self) -> type:
        return self._clazz

    def write(self, writer: CompactWriter, obj: Any) -> None:
        """Write object fields to the writer."""
        if hasattr(obj, "__dataclass_fields__"):
            for field_name, field_info in obj.__dataclass_fields__.items():
                value = getattr(obj, field_name)
                self._write_field(writer, field_name, value, field_info.type)
        elif hasattr(obj, "__dict__"):
            for field_name, value in obj.__dict__.items():
                if not field_name.startswith("_"):
                    self._write_field_auto(writer, field_name, value)

    def read(self, reader: CompactReader) -> Any:
        """Read object fields from the reader."""
        if hasattr(self._clazz, "__dataclass_fields__"):
            kwargs = {}
            for field_name, field_info in self._clazz.__dataclass_fields__.items():
                kwargs[field_name] = self._read_field(reader, field_name, field_info.type)
            return self._clazz(**kwargs)
        else:
            obj = object.__new__(self._clazz)
            return obj

    def _write_field(self, writer: CompactWriter, name: str, value: Any, field_type) -> None:
        """Write a field based on its type annotation."""
        if field_type == bool:
            writer.write_boolean(name, value)
        elif field_type == int:
            writer.write_int32(name, value)
        elif field_type == float:
            writer.write_float64(name, value)
        elif field_type == str:
            writer.write_string(name, value)
        else:
            writer.write_compact(name, value)

    def _write_field_auto(self, writer: CompactWriter, name: str, value: Any) -> None:
        """Write a field based on its runtime type."""
        if isinstance(value, bool):
            writer.write_boolean(name, value)
        elif isinstance(value, int):
            writer.write_int32(name, value)
        elif isinstance(value, float):
            writer.write_float64(name, value)
        elif isinstance(value, str):
            writer.write_string(name, value)
        else:
            writer.write_compact(name, value)

    def _read_field(self, reader: CompactReader, name: str, field_type) -> Any:
        """Read a field based on its type annotation."""
        if field_type == bool:
            return reader.read_boolean(name)
        elif field_type == int:
            return reader.read_int32(name)
        elif field_type == float:
            return reader.read_float64(name)
        elif field_type == str:
            return reader.read_string(name)
        else:
            return reader.read_compact(name)
