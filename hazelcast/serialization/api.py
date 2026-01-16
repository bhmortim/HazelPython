"""Serialization API interfaces.

This module defines the core interfaces for Hazelcast serialization.
It provides abstract base classes for reading and writing serialized data,
as well as interfaces for implementing custom serializers.

The serialization system supports multiple formats:
- Built-in serializers for Python primitives
- IdentifiedDataSerializable for efficient versioned serialization
- Portable for cross-language schema-based serialization
- Compact for efficient schema-less serialization

Example:
    Implementing a custom serializer::

        from hazelcast.serialization.api import Serializer, ObjectDataInput, ObjectDataOutput

        class PersonSerializer(Serializer[Person]):
            @property
            def type_id(self) -> int:
                return 1000

            def write(self, output: ObjectDataOutput, person: Person) -> None:
                output.write_string(person.name)
                output.write_int(person.age)

            def read(self, input: ObjectDataInput) -> Person:
                name = input.read_string()
                age = input.read_int()
                return Person(name, age)
"""

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class ObjectDataInput(ABC):
    """Interface for reading serialized data.

    Provides methods for reading primitive types and objects from
    a binary stream. Used by serializers during deserialization.
    """

    @abstractmethod
    def read_boolean(self) -> bool:
        """Read a boolean value.

        Returns:
            The boolean value read from the stream.
        """
        pass

    @abstractmethod
    def read_byte(self) -> int:
        """Read a byte value (-128 to 127).

        Returns:
            The byte value as an integer.
        """
        pass

    @abstractmethod
    def read_short(self) -> int:
        """Read a 16-bit short value.

        Returns:
            The short value as an integer.
        """
        pass

    @abstractmethod
    def read_int(self) -> int:
        """Read a 32-bit integer value.

        Returns:
            The integer value.
        """
        pass

    @abstractmethod
    def read_long(self) -> int:
        """Read a 64-bit long value.

        Returns:
            The long value as an integer.
        """
        pass

    @abstractmethod
    def read_float(self) -> float:
        """Read a 32-bit float value.

        Returns:
            The float value.
        """
        pass

    @abstractmethod
    def read_double(self) -> float:
        """Read a 64-bit double value.

        Returns:
            The double value.
        """
        pass

    @abstractmethod
    def read_string(self) -> str:
        """Read a UTF-8 encoded string.

        Returns:
            The string value.
        """
        pass

    @abstractmethod
    def read_byte_array(self) -> bytes:
        """Read a byte array.

        Returns:
            The byte array.
        """
        pass

    @abstractmethod
    def read_object(self) -> Any:
        """Read a nested object using the serialization service.

        Returns:
            The deserialized object.
        """
        pass

    @abstractmethod
    def position(self) -> int:
        """Get current read position in the buffer.

        Returns:
            The current byte position.
        """
        pass

    @abstractmethod
    def set_position(self, pos: int) -> None:
        """Set current read position in the buffer.

        Args:
            pos: The byte position to set.
        """
        pass


class ObjectDataOutput(ABC):
    """Interface for writing serialized data.

    Provides methods for writing primitive types and objects to
    a binary stream. Used by serializers during serialization.
    """

    @abstractmethod
    def write_boolean(self, value: bool) -> None:
        """Write a boolean value.

        Args:
            value: The boolean value to write.
        """
        pass

    @abstractmethod
    def write_byte(self, value: int) -> None:
        """Write a byte value (-128 to 127).

        Args:
            value: The byte value as an integer.
        """
        pass

    @abstractmethod
    def write_short(self, value: int) -> None:
        """Write a 16-bit short value.

        Args:
            value: The short value.
        """
        pass

    @abstractmethod
    def write_int(self, value: int) -> None:
        """Write a 32-bit integer value.

        Args:
            value: The integer value.
        """
        pass

    @abstractmethod
    def write_long(self, value: int) -> None:
        """Write a 64-bit long value.

        Args:
            value: The long value.
        """
        pass

    @abstractmethod
    def write_float(self, value: float) -> None:
        """Write a 32-bit float value.

        Args:
            value: The float value.
        """
        pass

    @abstractmethod
    def write_double(self, value: float) -> None:
        """Write a 64-bit double value.

        Args:
            value: The double value.
        """
        pass

    @abstractmethod
    def write_string(self, value: str) -> None:
        """Write a UTF-8 encoded string.

        Args:
            value: The string to write.
        """
        pass

    @abstractmethod
    def write_byte_array(self, value: bytes) -> None:
        """Write a byte array.

        Args:
            value: The byte array to write.
        """
        pass

    @abstractmethod
    def write_object(self, value: Any) -> None:
        """Write a nested object using the serialization service.

        Args:
            value: The object to serialize and write.
        """
        pass

    @abstractmethod
    def to_byte_array(self) -> bytes:
        """Get the serialized output as bytes.

        Returns:
            The complete serialized byte array.
        """
        pass


class Serializer(ABC, Generic[T]):
    """Base serializer interface for custom serialization.

    Implement this interface to create custom serializers for your
    domain objects. Each serializer must have a unique type ID.

    Type Parameters:
        T: The type of object this serializer handles.

    Example:
        >>> class PersonSerializer(Serializer[Person]):
        ...     @property
        ...     def type_id(self) -> int:
        ...         return 1000
        ...
        ...     def write(self, output: ObjectDataOutput, obj: Person) -> None:
        ...         output.write_string(obj.name)
        ...         output.write_int(obj.age)
        ...
        ...     def read(self, input: ObjectDataInput) -> Person:
        ...         return Person(input.read_string(), input.read_int())
    """

    @property
    @abstractmethod
    def type_id(self) -> int:
        """Get the unique type ID for this serializer.

        Returns:
            A unique integer identifying the serialized type.
            Positive IDs are reserved for user types.
        """
        pass

    @abstractmethod
    def write(self, output: ObjectDataOutput, obj: T) -> None:
        """Write an object to the output stream.

        Args:
            output: The output stream to write to.
            obj: The object to serialize.
        """
        pass

    @abstractmethod
    def read(self, input: ObjectDataInput) -> T:
        """Read an object from the input stream.

        Args:
            input: The input stream to read from.

        Returns:
            The deserialized object.
        """
        pass


class StreamSerializer(Serializer[T], Generic[T]):
    """Serializer that works with object data streams.

    A marker interface for serializers that use ObjectDataInput
    and ObjectDataOutput for serialization.
    """

    pass


class IdentifiedDataSerializable(ABC):
    """Interface for objects that can serialize themselves.

    IdentifiedDataSerializable provides efficient serialization with
    versioning support. Objects implement their own serialization logic
    and are identified by factory and class IDs.

    This is the recommended serialization method for Java/Python
    interoperability when Portable is not required.

    Example:
        >>> class Employee(IdentifiedDataSerializable):
        ...     FACTORY_ID = 1
        ...     CLASS_ID = 1
        ...
        ...     def __init__(self, id: int = 0, name: str = ""):
        ...         self.id = id
        ...         self.name = name
        ...
        ...     @property
        ...     def factory_id(self) -> int:
        ...         return self.FACTORY_ID
        ...
        ...     @property
        ...     def class_id(self) -> int:
        ...         return self.CLASS_ID
        ...
        ...     def write_data(self, output: ObjectDataOutput) -> None:
        ...         output.write_int(self.id)
        ...         output.write_string(self.name)
        ...
        ...     def read_data(self, input: ObjectDataInput) -> None:
        ...         self.id = input.read_int()
        ...         self.name = input.read_string()
    """

    @property
    @abstractmethod
    def factory_id(self) -> int:
        """Get the factory ID.

        Returns:
            Unique identifier for the factory that creates this type.
        """
        pass

    @property
    @abstractmethod
    def class_id(self) -> int:
        """Get the class ID.

        Returns:
            Unique identifier for this class within its factory.
        """
        pass

    @abstractmethod
    def write_data(self, output: ObjectDataOutput) -> None:
        """Write this object's data to the output.

        Args:
            output: The output stream to write to.
        """
        pass

    @abstractmethod
    def read_data(self, input: ObjectDataInput) -> None:
        """Read this object's data from the input.

        Args:
            input: The input stream to read from.
        """
        pass


class PortableReader(ABC):
    """Interface for reading portable objects.

    Provides field-based access to portable serialized data.
    Fields are accessed by name, enabling schema evolution.
    """

    @abstractmethod
    def read_int(self, field_name: str) -> int:
        """Read an integer field.

        Args:
            field_name: Name of the field to read.

        Returns:
            The integer value.
        """
        pass

    @abstractmethod
    def read_long(self, field_name: str) -> int:
        """Read a long field.

        Args:
            field_name: Name of the field to read.

        Returns:
            The long value.
        """
        pass

    @abstractmethod
    def read_string(self, field_name: str) -> str:
        """Read a string field.

        Args:
            field_name: Name of the field to read.

        Returns:
            The string value.
        """
        pass

    @abstractmethod
    def read_boolean(self, field_name: str) -> bool:
        """Read a boolean field.

        Args:
            field_name: Name of the field to read.

        Returns:
            The boolean value.
        """
        pass

    @abstractmethod
    def read_float(self, field_name: str) -> float:
        """Read a float field.

        Args:
            field_name: Name of the field to read.

        Returns:
            The float value.
        """
        pass

    @abstractmethod
    def read_double(self, field_name: str) -> float:
        """Read a double field.

        Args:
            field_name: Name of the field to read.

        Returns:
            The double value.
        """
        pass

    @abstractmethod
    def read_byte_array(self, field_name: str) -> bytes:
        """Read a byte array field.

        Args:
            field_name: Name of the field to read.

        Returns:
            The byte array.
        """
        pass

    @abstractmethod
    def read_portable(self, field_name: str) -> "Portable":
        """Read a nested portable field.

        Args:
            field_name: Name of the field to read.

        Returns:
            The nested Portable object.
        """
        pass

    @abstractmethod
    def read_portable_array(self, field_name: str) -> list:
        """Read a portable array field.

        Args:
            field_name: Name of the field to read.

        Returns:
            List of Portable objects.
        """
        pass


class PortableWriter(ABC):
    """Interface for writing portable objects.

    Provides field-based writing of portable serialized data.
    Fields are written by name, enabling schema evolution.
    """

    @abstractmethod
    def write_int(self, field_name: str, value: int) -> None:
        """Write an integer field.

        Args:
            field_name: Name of the field.
            value: The integer value to write.
        """
        pass

    @abstractmethod
    def write_long(self, field_name: str, value: int) -> None:
        """Write a long field.

        Args:
            field_name: Name of the field.
            value: The long value to write.
        """
        pass

    @abstractmethod
    def write_string(self, field_name: str, value: str) -> None:
        """Write a string field.

        Args:
            field_name: Name of the field.
            value: The string value to write.
        """
        pass

    @abstractmethod
    def write_boolean(self, field_name: str, value: bool) -> None:
        """Write a boolean field.

        Args:
            field_name: Name of the field.
            value: The boolean value to write.
        """
        pass

    @abstractmethod
    def write_float(self, field_name: str, value: float) -> None:
        """Write a float field.

        Args:
            field_name: Name of the field.
            value: The float value to write.
        """
        pass

    @abstractmethod
    def write_double(self, field_name: str, value: float) -> None:
        """Write a double field.

        Args:
            field_name: Name of the field.
            value: The double value to write.
        """
        pass

    @abstractmethod
    def write_byte_array(self, field_name: str, value: bytes) -> None:
        """Write a byte array field.

        Args:
            field_name: Name of the field.
            value: The byte array to write.
        """
        pass

    @abstractmethod
    def write_portable(self, field_name: str, value: "Portable") -> None:
        """Write a nested portable field.

        Args:
            field_name: Name of the field.
            value: The Portable object to write.
        """
        pass

    @abstractmethod
    def write_portable_array(self, field_name: str, value: list) -> None:
        """Write a portable array field.

        Args:
            field_name: Name of the field.
            value: List of Portable objects to write.
        """
        pass


class Portable(ABC):
    """Interface for portable serializable objects.

    Portable serialization provides cross-language compatibility and
    schema evolution support. Fields are written and read by name,
    allowing newer versions to read older data and vice versa.

    Example:
        >>> class Customer(Portable):
        ...     FACTORY_ID = 1
        ...     CLASS_ID = 1
        ...
        ...     def __init__(self, id: int = 0, name: str = ""):
        ...         self.id = id
        ...         self.name = name
        ...
        ...     @property
        ...     def factory_id(self) -> int:
        ...         return self.FACTORY_ID
        ...
        ...     @property
        ...     def class_id(self) -> int:
        ...         return self.CLASS_ID
        ...
        ...     def write_portable(self, writer: PortableWriter) -> None:
        ...         writer.write_int("id", self.id)
        ...         writer.write_string("name", self.name)
        ...
        ...     def read_portable(self, reader: PortableReader) -> None:
        ...         self.id = reader.read_int("id")
        ...         self.name = reader.read_string("name")
    """

    @property
    @abstractmethod
    def factory_id(self) -> int:
        """Get the factory ID.

        Returns:
            Unique identifier for the factory that creates this type.
        """
        pass

    @property
    @abstractmethod
    def class_id(self) -> int:
        """Get the class ID.

        Returns:
            Unique identifier for this class within its factory.
        """
        pass

    @abstractmethod
    def write_portable(self, writer: PortableWriter) -> None:
        """Write this object's fields using the writer.

        Args:
            writer: The writer to use for field output.
        """
        pass

    @abstractmethod
    def read_portable(self, reader: PortableReader) -> None:
        """Read this object's fields using the reader.

        Args:
            reader: The reader to use for field input.
        """
        pass


class CompactReader(ABC):
    """Interface for reading compact serialized objects.

    Compact serialization is the recommended format for new applications.
    It provides efficient, schema-based serialization with support for
    schema evolution.
    """

    @abstractmethod
    def read_boolean(self, field_name: str) -> bool:
        """Read a boolean field.

        Args:
            field_name: Name of the field.

        Returns:
            The boolean value.
        """
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
    """Interface for writing compact serialized objects.

    Compact serialization is the recommended format for new applications.
    It provides efficient, schema-based serialization with support for
    schema evolution.
    """

    @abstractmethod
    def write_boolean(self, field_name: str, value: bool) -> None:
        """Write a boolean field.

        Args:
            field_name: Name of the field.
            value: The boolean value to write.
        """
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
    """Serializer for compact serialization format.

    Compact serialization is the recommended format for new applications.
    It provides efficient, schema-based serialization with support for
    schema evolution without configuration.

    Example:
        >>> class PersonCompactSerializer(CompactSerializer[Person]):
        ...     @property
        ...     def type_name(self) -> str:
        ...         return "Person"
        ...
        ...     @property
        ...     def clazz(self) -> type:
        ...         return Person
        ...
        ...     def write(self, writer: CompactWriter, obj: Person) -> None:
        ...         writer.write_string("name", obj.name)
        ...         writer.write_int32("age", obj.age)
        ...
        ...     def read(self, reader: CompactReader) -> Person:
        ...         name = reader.read_string("name")
        ...         age = reader.read_int32("age")
        ...         return Person(name, age)
    """

    @property
    @abstractmethod
    def type_name(self) -> str:
        """Get the unique type name for this serializer.

        Returns:
            A unique string identifying the serialized type.
        """
        pass

    @property
    @abstractmethod
    def clazz(self) -> type:
        """Get the class this serializer handles.

        Returns:
            The Python class that this serializer serializes.
        """
        pass

    @abstractmethod
    def write(self, writer: CompactWriter, obj: T) -> None:
        """Write an object using compact format.

        Args:
            writer: The compact writer to use.
            obj: The object to serialize.
        """
        pass

    @abstractmethod
    def read(self, reader: CompactReader) -> T:
        """Read an object using compact format.

        Args:
            reader: The compact reader to use.

        Returns:
            The deserialized object.
        """
        pass
