"""Serialization API interfaces."""

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class ObjectDataInput(ABC):
    """Interface for reading serialized data."""

    @abstractmethod
    def read_boolean(self) -> bool:
        """Read a boolean value."""
        pass

    @abstractmethod
    def read_byte(self) -> int:
        """Read a byte value."""
        pass

    @abstractmethod
    def read_short(self) -> int:
        """Read a short value."""
        pass

    @abstractmethod
    def read_int(self) -> int:
        """Read an integer value."""
        pass

    @abstractmethod
    def read_long(self) -> int:
        """Read a long value."""
        pass

    @abstractmethod
    def read_float(self) -> float:
        """Read a float value."""
        pass

    @abstractmethod
    def read_double(self) -> float:
        """Read a double value."""
        pass

    @abstractmethod
    def read_string(self) -> str:
        """Read a string value."""
        pass

    @abstractmethod
    def read_byte_array(self) -> bytes:
        """Read a byte array."""
        pass

    @abstractmethod
    def read_object(self) -> Any:
        """Read an object."""
        pass

    @abstractmethod
    def position(self) -> int:
        """Get current position."""
        pass

    @abstractmethod
    def set_position(self, pos: int) -> None:
        """Set current position."""
        pass


class ObjectDataOutput(ABC):
    """Interface for writing serialized data."""

    @abstractmethod
    def write_boolean(self, value: bool) -> None:
        """Write a boolean value."""
        pass

    @abstractmethod
    def write_byte(self, value: int) -> None:
        """Write a byte value."""
        pass

    @abstractmethod
    def write_short(self, value: int) -> None:
        """Write a short value."""
        pass

    @abstractmethod
    def write_int(self, value: int) -> None:
        """Write an integer value."""
        pass

    @abstractmethod
    def write_long(self, value: int) -> None:
        """Write a long value."""
        pass

    @abstractmethod
    def write_float(self, value: float) -> None:
        """Write a float value."""
        pass

    @abstractmethod
    def write_double(self, value: float) -> None:
        """Write a double value."""
        pass

    @abstractmethod
    def write_string(self, value: str) -> None:
        """Write a string value."""
        pass

    @abstractmethod
    def write_byte_array(self, value: bytes) -> None:
        """Write a byte array."""
        pass

    @abstractmethod
    def write_object(self, value: Any) -> None:
        """Write an object."""
        pass

    @abstractmethod
    def to_byte_array(self) -> bytes:
        """Get the output as bytes."""
        pass


class Serializer(ABC, Generic[T]):
    """Base serializer interface."""

    @property
    @abstractmethod
    def type_id(self) -> int:
        """Get the type ID for this serializer."""
        pass

    @abstractmethod
    def write(self, output: ObjectDataOutput, obj: T) -> None:
        """Write an object to the output."""
        pass

    @abstractmethod
    def read(self, input: ObjectDataInput) -> T:
        """Read an object from the input."""
        pass


class StreamSerializer(Serializer[T], Generic[T]):
    """Serializer that works with object data streams."""

    pass


class IdentifiedDataSerializable(ABC):
    """Interface for objects that can serialize themselves."""

    @property
    @abstractmethod
    def factory_id(self) -> int:
        """Get the factory ID."""
        pass

    @property
    @abstractmethod
    def class_id(self) -> int:
        """Get the class ID."""
        pass

    @abstractmethod
    def write_data(self, output: ObjectDataOutput) -> None:
        """Write this object to the output."""
        pass

    @abstractmethod
    def read_data(self, input: ObjectDataInput) -> None:
        """Read this object from the input."""
        pass


class PortableReader(ABC):
    """Interface for reading portable objects."""

    @abstractmethod
    def read_int(self, field_name: str) -> int:
        """Read an integer field."""
        pass

    @abstractmethod
    def read_long(self, field_name: str) -> int:
        """Read a long field."""
        pass

    @abstractmethod
    def read_string(self, field_name: str) -> str:
        """Read a string field."""
        pass

    @abstractmethod
    def read_boolean(self, field_name: str) -> bool:
        """Read a boolean field."""
        pass

    @abstractmethod
    def read_float(self, field_name: str) -> float:
        """Read a float field."""
        pass

    @abstractmethod
    def read_double(self, field_name: str) -> float:
        """Read a double field."""
        pass

    @abstractmethod
    def read_byte_array(self, field_name: str) -> bytes:
        """Read a byte array field."""
        pass

    @abstractmethod
    def read_portable(self, field_name: str) -> "Portable":
        """Read a nested portable field."""
        pass

    @abstractmethod
    def read_portable_array(self, field_name: str) -> list:
        """Read a portable array field."""
        pass


class PortableWriter(ABC):
    """Interface for writing portable objects."""

    @abstractmethod
    def write_int(self, field_name: str, value: int) -> None:
        """Write an integer field."""
        pass

    @abstractmethod
    def write_long(self, field_name: str, value: int) -> None:
        """Write a long field."""
        pass

    @abstractmethod
    def write_string(self, field_name: str, value: str) -> None:
        """Write a string field."""
        pass

    @abstractmethod
    def write_boolean(self, field_name: str, value: bool) -> None:
        """Write a boolean field."""
        pass

    @abstractmethod
    def write_float(self, field_name: str, value: float) -> None:
        """Write a float field."""
        pass

    @abstractmethod
    def write_double(self, field_name: str, value: float) -> None:
        """Write a double field."""
        pass

    @abstractmethod
    def write_byte_array(self, field_name: str, value: bytes) -> None:
        """Write a byte array field."""
        pass

    @abstractmethod
    def write_portable(self, field_name: str, value: "Portable") -> None:
        """Write a nested portable field."""
        pass

    @abstractmethod
    def write_portable_array(self, field_name: str, value: list) -> None:
        """Write a portable array field."""
        pass


class Portable(ABC):
    """Interface for portable serializable objects."""

    @property
    @abstractmethod
    def factory_id(self) -> int:
        """Get the factory ID."""
        pass

    @property
    @abstractmethod
    def class_id(self) -> int:
        """Get the class ID."""
        pass

    @abstractmethod
    def write_portable(self, writer: PortableWriter) -> None:
        """Write this object using the writer."""
        pass

    @abstractmethod
    def read_portable(self, reader: PortableReader) -> None:
        """Read this object using the reader."""
        pass


class CompactReader(ABC):
    """Interface for reading compact serialized objects."""

    @abstractmethod
    def read_boolean(self, field_name: str) -> bool:
        """Read a boolean field."""
        pass

    @abstractmethod
    def read_int8(self, field_name: str) -> int:
        """Read an int8 field."""
        pass

    @abstractmethod
    def read_int16(self, field_name: str) -> int:
        """Read an int16 field."""
        pass

    @abstractmethod
    def read_int32(self, field_name: str) -> int:
        """Read an int32 field."""
        pass

    @abstractmethod
    def read_int64(self, field_name: str) -> int:
        """Read an int64 field."""
        pass

    @abstractmethod
    def read_float32(self, field_name: str) -> float:
        """Read a float32 field."""
        pass

    @abstractmethod
    def read_float64(self, field_name: str) -> float:
        """Read a float64 field."""
        pass

    @abstractmethod
    def read_string(self, field_name: str) -> str:
        """Read a string field."""
        pass

    @abstractmethod
    def read_compact(self, field_name: str) -> Any:
        """Read a nested compact object."""
        pass

    @abstractmethod
    def read_array_of_boolean(self, field_name: str) -> list:
        """Read a boolean array field."""
        pass

    @abstractmethod
    def read_array_of_int32(self, field_name: str) -> list:
        """Read an int32 array field."""
        pass

    @abstractmethod
    def read_array_of_string(self, field_name: str) -> list:
        """Read a string array field."""
        pass

    @abstractmethod
    def read_nullable_boolean(self, field_name: str) -> bool:
        """Read a nullable boolean field."""
        pass

    @abstractmethod
    def read_nullable_int32(self, field_name: str) -> int:
        """Read a nullable int32 field."""
        pass


class CompactWriter(ABC):
    """Interface for writing compact serialized objects."""

    @abstractmethod
    def write_boolean(self, field_name: str, value: bool) -> None:
        """Write a boolean field."""
        pass

    @abstractmethod
    def write_int8(self, field_name: str, value: int) -> None:
        """Write an int8 field."""
        pass

    @abstractmethod
    def write_int16(self, field_name: str, value: int) -> None:
        """Write an int16 field."""
        pass

    @abstractmethod
    def write_int32(self, field_name: str, value: int) -> None:
        """Write an int32 field."""
        pass

    @abstractmethod
    def write_int64(self, field_name: str, value: int) -> None:
        """Write an int64 field."""
        pass

    @abstractmethod
    def write_float32(self, field_name: str, value: float) -> None:
        """Write a float32 field."""
        pass

    @abstractmethod
    def write_float64(self, field_name: str, value: float) -> None:
        """Write a float64 field."""
        pass

    @abstractmethod
    def write_string(self, field_name: str, value: str) -> None:
        """Write a string field."""
        pass

    @abstractmethod
    def write_compact(self, field_name: str, value: Any) -> None:
        """Write a nested compact object."""
        pass

    @abstractmethod
    def write_array_of_boolean(self, field_name: str, value: list) -> None:
        """Write a boolean array field."""
        pass

    @abstractmethod
    def write_array_of_int32(self, field_name: str, value: list) -> None:
        """Write an int32 array field."""
        pass

    @abstractmethod
    def write_array_of_string(self, field_name: str, value: list) -> None:
        """Write a string array field."""
        pass

    @abstractmethod
    def write_nullable_boolean(self, field_name: str, value: bool) -> None:
        """Write a nullable boolean field."""
        pass

    @abstractmethod
    def write_nullable_int32(self, field_name: str, value: int) -> None:
        """Write a nullable int32 field."""
        pass


class CompactSerializer(ABC, Generic[T]):
    """Serializer for compact serialization format."""

    @property
    @abstractmethod
    def type_name(self) -> str:
        """Get the type name for this serializer."""
        pass

    @property
    @abstractmethod
    def clazz(self) -> type:
        """Get the class this serializer handles."""
        pass

    @abstractmethod
    def write(self, writer: CompactWriter, obj: T) -> None:
        """Write an object using compact format."""
        pass

    @abstractmethod
    def read(self, reader: CompactReader) -> T:
        """Read an object using compact format."""
        pass
