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
    ARRAY_INT32 = 10
    ARRAY_STRING = 11
    NULLABLE_BOOLEAN = 12
    NULLABLE_INT32 = 13


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
    "array_int32": FieldKind.ARRAY_INT32,
    "array_string": FieldKind.ARRAY_STRING,
    "nullable_boolean": FieldKind.NULLABLE_BOOLEAN,
    "nullable_int32": FieldKind.NULLABLE_INT32,
}


@dataclass
class FieldDescriptor:
    """Describes a field in a compact schema."""

    name: str
    field_type: str
    index: int

    @property
    def kind(self) -> FieldKind:
        """Get the field kind enum value."""
        return FIELD_KIND_MAP.get(self.field_type, FieldKind.COMPACT)


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
        self._schemas_by_type: Dict[str, Schema] = {}

    def register(self, schema: Schema) -> None:
        """Register a schema."""
        self._schemas_by_id[schema.schema_id] = schema
        self._schemas_by_type[schema.type_name] = schema

    def get_by_id(self, schema_id: int) -> Optional[Schema]:
        """Get a schema by its ID."""
        return self._schemas_by_id.get(schema_id)

    def get_by_type_name(self, type_name: str) -> Optional[Schema]:
        """Get a schema by type name."""
        return self._schemas_by_type.get(type_name)

    def has_schema(self, schema_id: int) -> bool:
        """Check if a schema is registered."""
        return schema_id in self._schemas_by_id

    def all_schemas(self) -> List[Schema]:
        """Get all registered schemas."""
        return list(self._schemas_by_id.values())


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
    """Default implementation of CompactReader."""

    def __init__(self, schema: Schema, fields: Dict[str, Any]):
        self._schema = schema
        self._fields = fields

    def read_boolean(self, field_name: str) -> bool:
        return self._fields.get(field_name, False)

    def read_int8(self, field_name: str) -> int:
        return self._fields.get(field_name, 0)

    def read_int16(self, field_name: str) -> int:
        return self._fields.get(field_name, 0)

    def read_int32(self, field_name: str) -> int:
        return self._fields.get(field_name, 0)

    def read_int64(self, field_name: str) -> int:
        return self._fields.get(field_name, 0)

    def read_float32(self, field_name: str) -> float:
        return self._fields.get(field_name, 0.0)

    def read_float64(self, field_name: str) -> float:
        return self._fields.get(field_name, 0.0)

    def read_string(self, field_name: str) -> str:
        return self._fields.get(field_name, "")

    def read_compact(self, field_name: str) -> Any:
        return self._fields.get(field_name)

    def read_array_of_boolean(self, field_name: str) -> list:
        return self._fields.get(field_name, [])

    def read_array_of_int32(self, field_name: str) -> list:
        return self._fields.get(field_name, [])

    def read_array_of_string(self, field_name: str) -> list:
        return self._fields.get(field_name, [])

    def read_nullable_boolean(self, field_name: str) -> bool:
        return self._fields.get(field_name)

    def read_nullable_int32(self, field_name: str) -> int:
        return self._fields.get(field_name)


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
        """Deserialize bytes to an object."""
        serializer = self.get_serializer(type_name)
        if serializer is None:
            raise ValueError(f"No compact serializer registered for {type_name}")

        schema_id, fields = self._decode(data)
        schema = self.get_schema(schema_id)
        if schema is None:
            schema = Schema(type_name=type_name, schema_id=schema_id)

        reader = DefaultCompactReader(schema, fields)
        return serializer.read(reader)

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
        elif field_type == "nullable_int32":
            writer.write_boolean(value is not None)
            if value is not None:
                writer.write_int32(value)
        elif field_type == "array_boolean":
            self._write_array(writer, value, writer.write_boolean)
        elif field_type == "array_int32":
            self._write_array(writer, value, writer.write_int32)
        elif field_type == "array_string":
            self._write_array(writer, value, writer.write_string)
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
        elif field_kind == FieldKind.NULLABLE_INT32:
            if reader.read_boolean():
                return reader.read_int32()
            return None
        elif field_kind == FieldKind.ARRAY_BOOLEAN:
            return self._read_array(reader, reader.read_boolean)
        elif field_kind == FieldKind.ARRAY_INT32:
            return self._read_array(reader, reader.read_int32)
        elif field_kind == FieldKind.ARRAY_STRING:
            return self._read_array(reader, reader.read_string)
        else:
            has_value = reader.read_boolean()
            return None

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
