"""Compact serialization implementation."""

import struct
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Type

from hazelcast.serialization.api import (
    CompactReader,
    CompactWriter,
    CompactSerializer,
)


@dataclass
class FieldDescriptor:
    """Describes a field in a compact schema."""

    name: str
    field_type: str
    index: int


@dataclass
class Schema:
    """Schema for compact serialization."""

    type_name: str
    fields: List[FieldDescriptor] = field(default_factory=list)
    schema_id: int = 0

    def __post_init__(self):
        if self.schema_id == 0:
            self.schema_id = self._compute_schema_id()

    def _compute_schema_id(self) -> int:
        """Compute a schema ID based on type name and fields."""
        h = hash(self.type_name)
        for f in self.fields:
            h = h * 31 + hash(f.name) + hash(f.field_type)
        return h & 0x7FFFFFFF


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


class CompactSerializationService:
    """Service for compact serialization."""

    COMPACT_TYPE_ID = -55

    def __init__(self):
        self._serializers: Dict[str, CompactSerializer] = {}
        self._class_to_type_name: Dict[type, str] = {}
        self._schemas: Dict[int, Schema] = {}

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
        self._schemas[schema.schema_id] = schema

    def get_schema(self, schema_id: int) -> Optional[Schema]:
        """Get a schema by ID."""
        return self._schemas.get(schema_id)

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
            field_type = writer.field_types.get(name, "object")
            fields_with_types.append((name, field_type, value))

        schema.fields = [
            FieldDescriptor(name=name, field_type=ft, index=i)
            for i, (name, ft, _) in enumerate(fields_with_types)
        ]
        self.register_schema(schema)

        return self._encode(schema, writer.fields)

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

    def _encode(self, schema: Schema, fields: Dict[str, Any]) -> bytes:
        """Encode schema and fields to bytes."""
        import json
        payload = {
            "schema_id": schema.schema_id,
            "type_name": schema.type_name,
            "fields": fields,
        }
        return json.dumps(payload).encode("utf-8")

    def _decode(self, data: bytes) -> tuple:
        """Decode bytes to schema ID and fields."""
        import json
        payload = json.loads(data.decode("utf-8"))
        return payload.get("schema_id", 0), payload.get("fields", {})


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
