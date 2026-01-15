"""Serialization service implementation."""

import struct
import threading
from typing import Any, Dict, List, Optional, Type

from hazelcast.serialization.api import (
    ObjectDataInput,
    ObjectDataOutput,
    Serializer,
    IdentifiedDataSerializable,
    Portable,
)
from hazelcast.serialization.builtin import (
    get_builtin_serializers,
    get_type_serializer_mapping,
    NONE_TYPE_ID,
)
from hazelcast.serialization.compact import CompactSerializationService
from hazelcast.exceptions import SerializationException


TYPE_ID_SIZE = 4
DATA_OFFSET = TYPE_ID_SIZE


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

    def __init__(
        self,
        portable_version: int = 0,
        portable_factories: Dict[int, Any] = None,
        data_serializable_factories: Dict[int, Any] = None,
        custom_serializers: Dict[type, Serializer] = None,
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
        self._lock = threading.Lock()

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

        serializer = self._find_serializer_for_object(obj)
        if serializer is None:
            raise SerializationException(
                f"No serializer found for type: {type(obj)}"
            )

        output = ObjectDataOutputImpl(self)
        output.write_int(serializer.type_id)
        serializer.write(output, obj)

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

        serializer = self._type_id_to_serializer.get(type_id)
        if serializer is None:
            raise SerializationException(
                f"No serializer found for type ID: {type_id}"
            )

        payload = data.get_payload()
        input_stream = ObjectDataInputImpl(payload, self)

        return serializer.read(input_stream)

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

        for base_type, ser in self._type_to_serializer.items():
            if isinstance(obj, base_type):
                return ser

        return None

    def _get_data_serializable_serializer(self) -> Serializer:
        """Get or create serializer for IdentifiedDataSerializable."""
        if self.IDENTIFIED_DATA_SERIALIZABLE_ID not in self._type_id_to_serializer:
            serializer = IdentifiedDataSerializableSerializer(
                self._data_serializable_factories
            )
            self._type_id_to_serializer[self.IDENTIFIED_DATA_SERIALIZABLE_ID] = serializer
        return self._type_id_to_serializer[self.IDENTIFIED_DATA_SERIALIZABLE_ID]

    def _get_portable_serializer(self) -> Serializer:
        """Get or create serializer for Portable."""
        if self.PORTABLE_ID not in self._type_id_to_serializer:
            serializer = PortableSerializer(
                self._portable_version, self._portable_factories
            )
            self._type_id_to_serializer[self.PORTABLE_ID] = serializer
        return self._type_id_to_serializer[self.PORTABLE_ID]

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


class PortableSerializer(Serializer[Portable]):
    """Serializer for Portable objects."""

    def __init__(self, version: int, factories: Dict[int, Any]):
        self._version = version
        self._factories = factories

    @property
    def type_id(self) -> int:
        return SerializationService.PORTABLE_ID

    def write(self, output: ObjectDataOutput, obj: Portable) -> None:
        output.write_int(obj.factory_id)
        output.write_int(obj.class_id)
        output.write_int(self._version)

    def read(self, input: ObjectDataInput) -> Portable:
        factory_id = input.read_int()
        class_id = input.read_int()
        version = input.read_int()

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

        return obj
