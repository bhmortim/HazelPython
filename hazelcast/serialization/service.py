"""Serialization service implementation."""

import struct
import threading
from enum import IntEnum
from typing import Any, Callable, Dict, List, Optional, Set, Type

from hazelcast.serialization.api import (
    ObjectDataInput,
    ObjectDataOutput,
    Serializer,
    IdentifiedDataSerializable,
    Portable,
    PortableReader,
    PortableWriter,
)
from hazelcast.serialization.builtin import (
    get_builtin_serializers,
    get_type_serializer_mapping,
    NONE_TYPE_ID,
)
from hazelcast.serialization.compact import (
    CompactSerializationService,
    GenericRecord,
)
from hazelcast.exceptions import SerializationException


TYPE_ID_SIZE = 4
DATA_OFFSET = TYPE_ID_SIZE


class FieldType(IntEnum):
    """Field types for Portable serialization."""

    BOOLEAN = 0
    BYTE = 1
    SHORT = 2
    INT = 3
    LONG = 4
    FLOAT = 5
    DOUBLE = 6
    STRING = 7
    PORTABLE = 8
    BYTE_ARRAY = 9
    BOOLEAN_ARRAY = 10
    SHORT_ARRAY = 11
    INT_ARRAY = 12
    LONG_ARRAY = 13
    FLOAT_ARRAY = 14
    DOUBLE_ARRAY = 15
    STRING_ARRAY = 16
    PORTABLE_ARRAY = 17


class FieldDefinition:
    """Definition of a field in a Portable class."""

    def __init__(
        self,
        index: int,
        name: str,
        field_type: FieldType,
        factory_id: int = 0,
        class_id: int = 0,
        version: int = 0,
    ):
        self.index = index
        self.name = name
        self.field_type = field_type
        self.factory_id = factory_id
        self.class_id = class_id
        self.version = version

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FieldDefinition):
            return False
        return (
            self.name == other.name
            and self.field_type == other.field_type
            and self.factory_id == other.factory_id
            and self.class_id == other.class_id
        )

    def __hash__(self) -> int:
        return hash((self.name, self.field_type, self.factory_id, self.class_id))


class ClassDefinition:
    """Definition of a Portable class structure."""

    def __init__(self, factory_id: int, class_id: int, version: int):
        self.factory_id = factory_id
        self.class_id = class_id
        self.version = version
        self._fields: Dict[str, FieldDefinition] = {}
        self._field_list: List[FieldDefinition] = []

    def add_field(self, field_def: FieldDefinition) -> "ClassDefinition":
        """Add a field definition."""
        self._fields[field_def.name] = field_def
        self._field_list.append(field_def)
        return self

    def get_field(self, name: str) -> Optional[FieldDefinition]:
        """Get a field by name."""
        return self._fields.get(name)

    def get_field_by_index(self, index: int) -> Optional[FieldDefinition]:
        """Get a field by index."""
        if 0 <= index < len(self._field_list):
            return self._field_list[index]
        return None

    def has_field(self, name: str) -> bool:
        """Check if field exists."""
        return name in self._fields

    def get_field_count(self) -> int:
        """Get the number of fields."""
        return len(self._fields)

    def get_field_names(self) -> List[str]:
        """Get all field names."""
        return list(self._fields.keys())

    def get_field_type(self, name: str) -> Optional[FieldType]:
        """Get the type of a field."""
        field = self._fields.get(name)
        return field.field_type if field else None

    @property
    def fields(self) -> List[FieldDefinition]:
        """Get all field definitions."""
        return self._field_list

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ClassDefinition):
            return False
        return (
            self.factory_id == other.factory_id
            and self.class_id == other.class_id
            and self.version == other.version
            and self._fields == other._fields
        )

    def __hash__(self) -> int:
        return hash((self.factory_id, self.class_id, self.version))


class ClassDefinitionBuilder:
    """Builder for creating ClassDefinitions."""

    def __init__(self, factory_id: int, class_id: int, version: int = 0):
        self._factory_id = factory_id
        self._class_id = class_id
        self._version = version
        self._fields: List[FieldDefinition] = []
        self._index = 0

    def add_boolean_field(self, name: str) -> "ClassDefinitionBuilder":
        self._add_field(name, FieldType.BOOLEAN)
        return self

    def add_byte_field(self, name: str) -> "ClassDefinitionBuilder":
        self._add_field(name, FieldType.BYTE)
        return self

    def add_short_field(self, name: str) -> "ClassDefinitionBuilder":
        self._add_field(name, FieldType.SHORT)
        return self

    def add_int_field(self, name: str) -> "ClassDefinitionBuilder":
        self._add_field(name, FieldType.INT)
        return self

    def add_long_field(self, name: str) -> "ClassDefinitionBuilder":
        self._add_field(name, FieldType.LONG)
        return self

    def add_float_field(self, name: str) -> "ClassDefinitionBuilder":
        self._add_field(name, FieldType.FLOAT)
        return self

    def add_double_field(self, name: str) -> "ClassDefinitionBuilder":
        self._add_field(name, FieldType.DOUBLE)
        return self

    def add_string_field(self, name: str) -> "ClassDefinitionBuilder":
        self._add_field(name, FieldType.STRING)
        return self

    def add_byte_array_field(self, name: str) -> "ClassDefinitionBuilder":
        self._add_field(name, FieldType.BYTE_ARRAY)
        return self

    def add_boolean_array_field(self, name: str) -> "ClassDefinitionBuilder":
        self._add_field(name, FieldType.BOOLEAN_ARRAY)
        return self

    def add_int_array_field(self, name: str) -> "ClassDefinitionBuilder":
        self._add_field(name, FieldType.INT_ARRAY)
        return self

    def add_long_array_field(self, name: str) -> "ClassDefinitionBuilder":
        self._add_field(name, FieldType.LONG_ARRAY)
        return self

    def add_string_array_field(self, name: str) -> "ClassDefinitionBuilder":
        self._add_field(name, FieldType.STRING_ARRAY)
        return self

    def add_portable_field(
        self, name: str, class_def: ClassDefinition
    ) -> "ClassDefinitionBuilder":
        field = FieldDefinition(
            self._index,
            name,
            FieldType.PORTABLE,
            class_def.factory_id,
            class_def.class_id,
            class_def.version,
        )
        self._fields.append(field)
        self._index += 1
        return self

    def add_portable_array_field(
        self, name: str, class_def: ClassDefinition
    ) -> "ClassDefinitionBuilder":
        field = FieldDefinition(
            self._index,
            name,
            FieldType.PORTABLE_ARRAY,
            class_def.factory_id,
            class_def.class_id,
            class_def.version,
        )
        self._fields.append(field)
        self._index += 1
        return self

    def _add_field(self, name: str, field_type: FieldType) -> None:
        field = FieldDefinition(self._index, name, field_type)
        self._fields.append(field)
        self._index += 1

    def build(self) -> ClassDefinition:
        """Build the class definition."""
        class_def = ClassDefinition(self._factory_id, self._class_id, self._version)
        for field in self._fields:
            class_def.add_field(field)
        return class_def


class ClassDefinitionContext:
    """Context for managing class definitions."""

    def __init__(self, portable_version: int):
        self._portable_version = portable_version
        self._definitions: Dict[int, Dict[int, ClassDefinition]] = {}

    def register(self, class_def: ClassDefinition) -> ClassDefinition:
        """Register a class definition."""
        factory_defs = self._definitions.setdefault(class_def.factory_id, {})
        key = self._make_key(class_def.class_id, class_def.version)
        existing = factory_defs.get(key)
        if existing:
            return existing
        factory_defs[key] = class_def
        return class_def

    def lookup(
        self, factory_id: int, class_id: int, version: int
    ) -> Optional[ClassDefinition]:
        """Look up a class definition."""
        factory_defs = self._definitions.get(factory_id)
        if factory_defs:
            key = self._make_key(class_id, version)
            return factory_defs.get(key)
        return None

    @staticmethod
    def _make_key(class_id: int, version: int) -> int:
        return (class_id << 16) | (version & 0xFFFF)


class Data:
    """Represents serialized data."""

    def __init__(self, buffer: bytes):
        self._buffer = buffer

    @property
    def buffer(self) -> bytes:
        return self._buffer

    def get_type_id(self) -> int:
        """Get the type ID from the data."""
        if len(self._buffer) < TYPE_ID_SIZE:
            return NONE_TYPE_ID
        return struct.unpack_from("<i", self._buffer, 0)[0]

    def get_payload(self) -> bytes:
        """Get the payload (data after type ID)."""
        if len(self._buffer) <= DATA_OFFSET:
            return b""
        return self._buffer[DATA_OFFSET:]

    def total_size(self) -> int:
        """Get total size of the data."""
        return len(self._buffer)

    def __len__(self) -> int:
        return len(self._buffer)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Data):
            return False
        return self._buffer == other._buffer

    def __hash__(self) -> int:
        return hash(self._buffer)

    def __bytes__(self) -> bytes:
        return self._buffer


class ObjectDataInputImpl(ObjectDataInput):
    """Implementation of ObjectDataInput."""

    def __init__(self, buffer: bytes, service: "SerializationService"):
        self._buffer = buffer
        self._service = service
        self._pos = 0

    def read_boolean(self) -> bool:
        value = struct.unpack_from("<B", self._buffer, self._pos)[0]
        self._pos += 1
        return value != 0

    def read_byte(self) -> int:
        value = struct.unpack_from("<b", self._buffer, self._pos)[0]
        self._pos += 1
        return value

    def read_short(self) -> int:
        value = struct.unpack_from("<h", self._buffer, self._pos)[0]
        self._pos += 2
        return value

    def read_int(self) -> int:
        value = struct.unpack_from("<i", self._buffer, self._pos)[0]
        self._pos += 4
        return value

    def read_long(self) -> int:
        value = struct.unpack_from("<q", self._buffer, self._pos)[0]
        self._pos += 8
        return value

    def read_float(self) -> float:
        value = struct.unpack_from("<f", self._buffer, self._pos)[0]
        self._pos += 4
        return value

    def read_double(self) -> float:
        value = struct.unpack_from("<d", self._buffer, self._pos)[0]
        self._pos += 8
        return value

    def read_string(self) -> str:
        length = self.read_int()
        if length < 0:
            return ""
        data = self._buffer[self._pos:self._pos + length]
        self._pos += length
        return data.decode("utf-8")

    def read_byte_array(self) -> bytes:
        length = self.read_int()
        if length < 0:
            return b""
        data = self._buffer[self._pos:self._pos + length]
        self._pos += length
        return data

    def read_object(self) -> Any:
        return self._service._read_object(self)

    def position(self) -> int:
        return self._pos

    def set_position(self, pos: int) -> None:
        self._pos = pos


class ObjectDataOutputImpl(ObjectDataOutput):
    """Implementation of ObjectDataOutput."""

    def __init__(self, service: "SerializationService"):
        self._buffer = bytearray()
        self._service = service

    def write_boolean(self, value: bool) -> None:
        self._buffer.extend(struct.pack("<B", 1 if value else 0))

    def write_byte(self, value: int) -> None:
        self._buffer.extend(struct.pack("<b", value))

    def write_short(self, value: int) -> None:
        self._buffer.extend(struct.pack("<h", value))

    def write_int(self, value: int) -> None:
        self._buffer.extend(struct.pack("<i", value))

    def write_long(self, value: int) -> None:
        self._buffer.extend(struct.pack("<q", value))

    def write_float(self, value: float) -> None:
        self._buffer.extend(struct.pack("<f", value))

    def write_double(self, value: float) -> None:
        self._buffer.extend(struct.pack("<d", value))

    def write_string(self, value: str) -> None:
        if value is None:
            self.write_int(-1)
            return
        encoded = value.encode("utf-8")
        self.write_int(len(encoded))
        self._buffer.extend(encoded)

    def write_byte_array(self, value: bytes) -> None:
        if value is None:
            self.write_int(-1)
            return
        self.write_int(len(value))
        self._buffer.extend(value)

    def write_object(self, value: Any) -> None:
        self._service._write_object(self, value)

    def to_byte_array(self) -> bytes:
        return bytes(self._buffer)


class SerializationService:
    """Main serialization service for the Hazelcast client."""

    IDENTIFIED_DATA_SERIALIZABLE_ID = -2
    PORTABLE_ID = -1

    COMPACT_TYPE_ID = -55

    def __init__(
        self,
        portable_version: int = 0,
        portable_factories: Dict[int, Any] = None,
        data_serializable_factories: Dict[int, Any] = None,
        custom_serializers: Dict[type, Serializer] = None,
        compact_serializers: List[Any] = None,
    ):
        self._portable_version = portable_version
        self._portable_factories = portable_factories or {}
        self._data_serializable_factories = data_serializable_factories or {}

        self._type_id_to_serializer: Dict[int, Serializer] = get_builtin_serializers()
        self._type_to_serializer: Dict[type, Serializer] = get_type_serializer_mapping()

        if custom_serializers:
            for clazz, serializer in custom_serializers.items():
                self.register_serializer(clazz, serializer)

        self._compact_service = CompactSerializationService()
        self._compact_classes: set = set()

        if compact_serializers:
            for serializer in compact_serializers:
                self._compact_service.register_serializer(serializer)
                self._compact_classes.add(serializer.clazz)

        self._class_definition_context = ClassDefinitionContext(portable_version)
        self._lock = threading.Lock()

    @property
    def class_definition_context(self) -> ClassDefinitionContext:
        """Get the class definition context."""
        return self._class_definition_context

    @property
    def compact_service(self) -> CompactSerializationService:
        """Get the compact serialization service."""
        return self._compact_service

    def register_serializer(self, clazz: type, serializer: Serializer) -> None:
        """Register a custom serializer for a type.

        Args:
            clazz: The class to serialize.
            serializer: The serializer to use.
        """
        with self._lock:
            self._type_to_serializer[clazz] = serializer
            self._type_id_to_serializer[serializer.type_id] = serializer

    def to_data(self, obj: Any) -> Data:
        """Serialize an object to Data.

        Args:
            obj: The object to serialize.

        Returns:
            The serialized Data.
        """
        if obj is None:
            return Data(struct.pack("<i", NONE_TYPE_ID))

        if isinstance(obj, Data):
            return obj

        if self._is_compact_object(obj):
            return self._serialize_compact(obj)

        serializer = self._find_serializer_for_object(obj)
        if serializer is None:
            raise SerializationException(
                f"No serializer found for type: {type(obj)}"
            )

        output = ObjectDataOutputImpl(self)
        output.write_int(serializer.type_id)
        serializer.write(output, obj)

        return Data(output.to_byte_array())

    def _serialize_compact(self, obj: Any) -> Data:
        """Serialize an object using compact serialization."""
        compact_bytes = self._compact_service.serialize(obj)
        output = ObjectDataOutputImpl(self)
        output.write_int(self.COMPACT_TYPE_ID)
        output.write_byte_array(compact_bytes)
        return Data(output.to_byte_array())

    def to_object(self, data: Any) -> Any:
        """Deserialize Data to an object.

        Args:
            data: The data to deserialize (Data or bytes).

        Returns:
            The deserialized object.
        """
        if data is None:
            return None

        if isinstance(data, bytes):
            data = Data(data)

        if not isinstance(data, Data):
            return data

        if data.total_size() == 0:
            return None

        type_id = data.get_type_id()

        if type_id == NONE_TYPE_ID:
            return None

        if type_id == self.COMPACT_TYPE_ID:
            return self._deserialize_compact(data)

        serializer = self._type_id_to_serializer.get(type_id)
        if serializer is None:
            raise SerializationException(
                f"No serializer found for type ID: {type_id}"
            )

        payload = data.get_payload()
        input_stream = ObjectDataInputImpl(payload, self)

        return serializer.read(input_stream)

    def _deserialize_compact(self, data: Data) -> Any:
        """Deserialize compact data."""
        payload = data.get_payload()
        input_stream = ObjectDataInputImpl(payload, self)
        compact_bytes = input_stream.read_byte_array()
        return self._compact_service.to_generic_record(compact_bytes, "unknown")

    def deserialize_compact(self, data: Data, type_name: str) -> Any:
        """Deserialize compact data to a specific type."""
        if data is None:
            return None

        if isinstance(data, bytes):
            data = Data(data)

        type_id = data.get_type_id()
        if type_id != self.COMPACT_TYPE_ID:
            raise SerializationException(
                f"Expected compact type ID {self.COMPACT_TYPE_ID}, got {type_id}"
            )

        payload = data.get_payload()
        input_stream = ObjectDataInputImpl(payload, self)
        compact_bytes = input_stream.read_byte_array()
        return self._compact_service.deserialize(compact_bytes, type_name)

    def register_compact_serializer(self, serializer: Any) -> None:
        """Register a compact serializer.

        Args:
            serializer: The compact serializer to register.
        """
        self._compact_service.register_serializer(serializer)
        self._compact_classes.add(serializer.clazz)

    def _find_serializer_for_object(self, obj: Any) -> Optional[Serializer]:
        """Find a serializer for the given object."""
        obj_type = type(obj)

        serializer = self._type_to_serializer.get(obj_type)
        if serializer:
            return serializer

        if isinstance(obj, IdentifiedDataSerializable):
            return self._get_data_serializable_serializer()

        if isinstance(obj, Portable):
            return self._get_portable_serializer()

        if isinstance(obj, GenericRecord) or obj_type in self._compact_classes:
            return None

        for base_type, ser in self._type_to_serializer.items():
            if isinstance(obj, base_type):
                return ser

        return None

    def _is_compact_object(self, obj: Any) -> bool:
        """Check if an object should use compact serialization."""
        if isinstance(obj, GenericRecord):
            return True
        return type(obj) in self._compact_classes

    def _get_data_serializable_serializer(self) -> Serializer:
        """Get or create serializer for IdentifiedDataSerializable."""
        if self.IDENTIFIED_DATA_SERIALIZABLE_ID not in self._type_id_to_serializer:
            serializer = IdentifiedDataSerializableSerializer(
                self._data_serializable_factories
            )
            self._type_id_to_serializer[self.IDENTIFIED_DATA_SERIALIZABLE_ID] = serializer
        return self._type_id_to_serializer[self.IDENTIFIED_DATA_SERIALIZABLE_ID]

    def _get_portable_serializer(self) -> PortableSerializer:
        """Get or create serializer for Portable."""
        if self.PORTABLE_ID not in self._type_id_to_serializer:
            serializer = PortableSerializer(
                self._portable_version,
                self._portable_factories,
                self._class_definition_context,
            )
            self._type_id_to_serializer[self.PORTABLE_ID] = serializer
        return self._type_id_to_serializer[self.PORTABLE_ID]

    def register_class_definition(self, class_def: ClassDefinition) -> None:
        """Register a class definition for portable serialization."""
        self._class_definition_context.register(class_def)
        serializer = self._get_portable_serializer()
        serializer.register_class_definition(class_def)

    def _write_object(self, output: ObjectDataOutputImpl, obj: Any) -> None:
        """Write an object to output (for nested objects)."""
        if obj is None:
            output.write_int(NONE_TYPE_ID)
            return

        serializer = self._find_serializer_for_object(obj)
        if serializer is None:
            raise SerializationException(f"No serializer for type: {type(obj)}")

        output.write_int(serializer.type_id)
        serializer.write(output, obj)

    def _read_object(self, input_stream: ObjectDataInputImpl) -> Any:
        """Read an object from input (for nested objects)."""
        type_id = input_stream.read_int()

        if type_id == NONE_TYPE_ID:
            return None

        serializer = self._type_id_to_serializer.get(type_id)
        if serializer is None:
            raise SerializationException(f"No serializer for type ID: {type_id}")

        return serializer.read(input_stream)


class IdentifiedDataSerializableSerializer(Serializer[IdentifiedDataSerializable]):
    """Serializer for IdentifiedDataSerializable objects."""

    def __init__(self, factories: Dict[int, Any]):
        self._factories = factories

    @property
    def type_id(self) -> int:
        return SerializationService.IDENTIFIED_DATA_SERIALIZABLE_ID

    def write(self, output: ObjectDataOutput, obj: IdentifiedDataSerializable) -> None:
        output.write_int(obj.factory_id)
        output.write_int(obj.class_id)
        obj.write_data(output)

    def read(self, input: ObjectDataInput) -> IdentifiedDataSerializable:
        factory_id = input.read_int()
        class_id = input.read_int()

        factory = self._factories.get(factory_id)
        if factory is None:
            raise SerializationException(
                f"No factory registered for factory ID: {factory_id}"
            )

        obj = factory.create(class_id)
        if obj is None:
            raise SerializationException(
                f"Factory {factory_id} returned None for class ID: {class_id}"
            )

        obj.read_data(input)
        return obj


class DefaultPortableWriter(PortableWriter):
    """Default implementation of PortableWriter."""

    def __init__(
        self,
        serializer: "PortableSerializer",
        output: ObjectDataOutput,
        class_def: ClassDefinition,
    ):
        self._serializer = serializer
        self._output = output
        self._class_def = class_def
        self._written_fields: set = set()

    def write_int(self, field_name: str, value: int) -> None:
        self._set_position(field_name, FieldType.INT)
        self._output.write_int(value)

    def write_long(self, field_name: str, value: int) -> None:
        self._set_position(field_name, FieldType.LONG)
        self._output.write_long(value)

    def write_string(self, field_name: str, value: str) -> None:
        self._set_position(field_name, FieldType.STRING)
        self._output.write_string(value)

    def write_boolean(self, field_name: str, value: bool) -> None:
        self._set_position(field_name, FieldType.BOOLEAN)
        self._output.write_boolean(value)

    def write_float(self, field_name: str, value: float) -> None:
        self._set_position(field_name, FieldType.FLOAT)
        self._output.write_float(value)

    def write_double(self, field_name: str, value: float) -> None:
        self._set_position(field_name, FieldType.DOUBLE)
        self._output.write_double(value)

    def write_byte_array(self, field_name: str, value: bytes) -> None:
        self._set_position(field_name, FieldType.BYTE_ARRAY)
        self._output.write_byte_array(value)

    def write_portable(self, field_name: str, value: "Portable") -> None:
        self._set_position(field_name, FieldType.PORTABLE)
        is_null = value is None
        self._output.write_boolean(is_null)
        if not is_null:
            self._serializer.write_internal(self._output, value)

    def write_portable_array(self, field_name: str, value: list) -> None:
        self._set_position(field_name, FieldType.PORTABLE_ARRAY)
        if value is None:
            self._output.write_int(-1)
        else:
            self._output.write_int(len(value))
            for item in value:
                self._serializer.write_internal(self._output, item)

    def _set_position(self, field_name: str, field_type: FieldType) -> None:
        if field_name in self._written_fields:
            raise SerializationException(f"Field '{field_name}' already written")
        field_def = self._class_def.get_field(field_name)
        if field_def and field_def.field_type != field_type:
            raise SerializationException(
                f"Field '{field_name}' type mismatch: expected {field_def.field_type}, got {field_type}"
            )
        self._written_fields.add(field_name)


class DefaultPortableReader(PortableReader):
    """Default implementation of PortableReader."""

    def __init__(
        self,
        serializer: "PortableSerializer",
        input: ObjectDataInput,
        class_def: ClassDefinition,
    ):
        self._serializer = serializer
        self._input = input
        self._class_def = class_def

    def read_int(self, field_name: str) -> int:
        self._check_field(field_name, FieldType.INT)
        return self._input.read_int()

    def read_long(self, field_name: str) -> int:
        self._check_field(field_name, FieldType.LONG)
        return self._input.read_long()

    def read_string(self, field_name: str) -> str:
        self._check_field(field_name, FieldType.STRING)
        return self._input.read_string()

    def read_boolean(self, field_name: str) -> bool:
        self._check_field(field_name, FieldType.BOOLEAN)
        return self._input.read_boolean()

    def read_float(self, field_name: str) -> float:
        self._check_field(field_name, FieldType.FLOAT)
        return self._input.read_float()

    def read_double(self, field_name: str) -> float:
        self._check_field(field_name, FieldType.DOUBLE)
        return self._input.read_double()

    def read_byte_array(self, field_name: str) -> bytes:
        self._check_field(field_name, FieldType.BYTE_ARRAY)
        return self._input.read_byte_array()

    def read_portable(self, field_name: str) -> "Portable":
        self._check_field(field_name, FieldType.PORTABLE)
        is_null = self._input.read_boolean()
        if is_null:
            return None
        field_def = self._class_def.get_field(field_name)
        return self._serializer.read_internal(
            self._input, field_def.factory_id, field_def.class_id, field_def.version
        )

    def read_portable_array(self, field_name: str) -> list:
        self._check_field(field_name, FieldType.PORTABLE_ARRAY)
        length = self._input.read_int()
        if length < 0:
            return []
        field_def = self._class_def.get_field(field_name)
        result = []
        for _ in range(length):
            item = self._serializer.read_internal(
                self._input, field_def.factory_id, field_def.class_id, field_def.version
            )
            result.append(item)
        return result

    def _check_field(self, field_name: str, expected_type: FieldType) -> None:
        field_def = self._class_def.get_field(field_name)
        if field_def and field_def.field_type != expected_type:
            raise SerializationException(
                f"Field '{field_name}' type mismatch: expected {expected_type}, got {field_def.field_type}"
            )


class PortableSerializer(Serializer[Portable]):
    """Serializer for Portable objects."""

    def __init__(
        self,
        version: int,
        factories: Dict[int, Any],
        context: ClassDefinitionContext = None,
    ):
        self._version = version
        self._factories = factories
        self._context = context or ClassDefinitionContext(version)
        self._class_definitions: Dict[tuple, ClassDefinition] = {}

    @property
    def type_id(self) -> int:
        return SerializationService.PORTABLE_ID

    @property
    def context(self) -> ClassDefinitionContext:
        return self._context

    def register_class_definition(self, class_def: ClassDefinition) -> None:
        """Register a class definition."""
        key = (class_def.factory_id, class_def.class_id, class_def.version)
        self._class_definitions[key] = class_def
        self._context.register(class_def)

    def get_or_create_class_definition(self, portable: Portable) -> ClassDefinition:
        """Get or create a class definition for a portable object."""
        key = (portable.factory_id, portable.class_id, self._version)
        if key in self._class_definitions:
            return self._class_definitions[key]

        class_def = ClassDefinition(
            portable.factory_id, portable.class_id, self._version
        )
        self._class_definitions[key] = class_def
        self._context.register(class_def)
        return class_def

    def write(self, output: ObjectDataOutput, obj: Portable) -> None:
        class_def = self.get_or_create_class_definition(obj)
        output.write_int(obj.factory_id)
        output.write_int(obj.class_id)
        output.write_int(self._version)
        self._write_class_definition(output, class_def)
        self.write_internal(output, obj)

    def write_internal(self, output: ObjectDataOutput, obj: Portable) -> None:
        """Write portable object without header."""
        class_def = self.get_or_create_class_definition(obj)
        writer = DefaultPortableWriter(self, output, class_def)
        obj.write_portable(writer)

    def _write_class_definition(
        self, output: ObjectDataOutput, class_def: ClassDefinition
    ) -> None:
        """Write class definition to output."""
        output.write_int(class_def.get_field_count())
        for field in class_def.fields:
            output.write_string(field.name)
            output.write_int(field.field_type)
            output.write_int(field.factory_id)
            output.write_int(field.class_id)
            output.write_int(field.version)

    def read(self, input: ObjectDataInput) -> Portable:
        factory_id = input.read_int()
        class_id = input.read_int()
        version = input.read_int()

        class_def = self._read_class_definition(input, factory_id, class_id, version)
        return self.read_internal(input, factory_id, class_id, version, class_def)

    def read_internal(
        self,
        input: ObjectDataInput,
        factory_id: int,
        class_id: int,
        version: int,
        class_def: ClassDefinition = None,
    ) -> Portable:
        """Read portable object from input."""
        factory = self._factories.get(factory_id)
        if factory is None:
            raise SerializationException(
                f"No factory registered for factory ID: {factory_id}"
            )

        obj = factory.create(class_id)
        if obj is None:
            raise SerializationException(
                f"Factory {factory_id} returned None for class ID: {class_id}"
            )

        if class_def is None:
            class_def = self._context.lookup(factory_id, class_id, version)
            if class_def is None:
                class_def = ClassDefinition(factory_id, class_id, version)

        reader = DefaultPortableReader(self, input, class_def)
        obj.read_portable(reader)
        return obj

    def _read_class_definition(
        self, input: ObjectDataInput, factory_id: int, class_id: int, version: int
    ) -> ClassDefinition:
        """Read class definition from input."""
        field_count = input.read_int()
        builder = ClassDefinitionBuilder(factory_id, class_id, version)

        for _ in range(field_count):
            name = input.read_string()
            field_type = FieldType(input.read_int())
            nested_factory_id = input.read_int()
            nested_class_id = input.read_int()
            nested_version = input.read_int()

            if field_type == FieldType.PORTABLE:
                nested_def = ClassDefinition(
                    nested_factory_id, nested_class_id, nested_version
                )
                builder.add_portable_field(name, nested_def)
            elif field_type == FieldType.PORTABLE_ARRAY:
                nested_def = ClassDefinition(
                    nested_factory_id, nested_class_id, nested_version
                )
                builder.add_portable_array_field(name, nested_def)
            else:
                builder._add_field(name, field_type)

        class_def = builder.build()
        self._context.register(class_def)
        return class_def
