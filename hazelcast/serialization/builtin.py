"""Built-in serializers for Python primitive types."""

import struct
from typing import Any, Dict, List, Optional

from hazelcast.serialization.api import ObjectDataInput, ObjectDataOutput, Serializer


NONE_TYPE_ID = 0
BOOLEAN_TYPE_ID = -1
BYTE_TYPE_ID = -2
SHORT_TYPE_ID = -3
INT_TYPE_ID = -4
LONG_TYPE_ID = -5
FLOAT_TYPE_ID = -6
DOUBLE_TYPE_ID = -7
STRING_TYPE_ID = -8
BYTE_ARRAY_TYPE_ID = -9
BOOLEAN_ARRAY_TYPE_ID = -10
SHORT_ARRAY_TYPE_ID = -11
INT_ARRAY_TYPE_ID = -12
LONG_ARRAY_TYPE_ID = -13
FLOAT_ARRAY_TYPE_ID = -14
DOUBLE_ARRAY_TYPE_ID = -15
STRING_ARRAY_TYPE_ID = -16
LIST_TYPE_ID = -17
DICT_TYPE_ID = -18
UUID_TYPE_ID = -19


class NoneSerializer(Serializer[None]):
    """Serializer for None values."""

    @property
    def type_id(self) -> int:
        return NONE_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: None) -> None:
        pass

    def read(self, input: ObjectDataInput) -> None:
        return None


class BoolSerializer(Serializer[bool]):
    """Serializer for boolean values."""

    @property
    def type_id(self) -> int:
        return BOOLEAN_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: bool) -> None:
        output.write_boolean(obj)

    def read(self, input: ObjectDataInput) -> bool:
        return input.read_boolean()


class ByteSerializer(Serializer[int]):
    """Serializer for byte values."""

    @property
    def type_id(self) -> int:
        return BYTE_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: int) -> None:
        output.write_byte(obj)

    def read(self, input: ObjectDataInput) -> int:
        return input.read_byte()


class ShortSerializer(Serializer[int]):
    """Serializer for short values."""

    @property
    def type_id(self) -> int:
        return SHORT_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: int) -> None:
        output.write_short(obj)

    def read(self, input: ObjectDataInput) -> int:
        return input.read_short()


class IntSerializer(Serializer[int]):
    """Serializer for integer values."""

    @property
    def type_id(self) -> int:
        return INT_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: int) -> None:
        output.write_int(obj)

    def read(self, input: ObjectDataInput) -> int:
        return input.read_int()


class LongSerializer(Serializer[int]):
    """Serializer for long values."""

    @property
    def type_id(self) -> int:
        return LONG_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: int) -> None:
        output.write_long(obj)

    def read(self, input: ObjectDataInput) -> int:
        return input.read_long()


class FloatSerializer(Serializer[float]):
    """Serializer for float values."""

    @property
    def type_id(self) -> int:
        return FLOAT_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: float) -> None:
        output.write_float(obj)

    def read(self, input: ObjectDataInput) -> float:
        return input.read_float()


class DoubleSerializer(Serializer[float]):
    """Serializer for double precision float values."""

    @property
    def type_id(self) -> int:
        return DOUBLE_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: float) -> None:
        output.write_double(obj)

    def read(self, input: ObjectDataInput) -> float:
        return input.read_double()


class StringSerializer(Serializer[str]):
    """Serializer for string values."""

    @property
    def type_id(self) -> int:
        return STRING_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: str) -> None:
        output.write_string(obj)

    def read(self, input: ObjectDataInput) -> str:
        return input.read_string()


class ByteArraySerializer(Serializer[bytes]):
    """Serializer for byte array values."""

    @property
    def type_id(self) -> int:
        return BYTE_ARRAY_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: bytes) -> None:
        output.write_byte_array(obj)

    def read(self, input: ObjectDataInput) -> bytes:
        return input.read_byte_array()


class ListSerializer(Serializer[list]):
    """Serializer for list values."""

    @property
    def type_id(self) -> int:
        return LIST_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: list) -> None:
        output.write_int(len(obj))
        for item in obj:
            output.write_object(item)

    def read(self, input: ObjectDataInput) -> list:
        size = input.read_int()
        result = []
        for _ in range(size):
            result.append(input.read_object())
        return result


class DictSerializer(Serializer[dict]):
    """Serializer for dictionary values."""

    @property
    def type_id(self) -> int:
        return DICT_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: dict) -> None:
        output.write_int(len(obj))
        for key, value in obj.items():
            output.write_object(key)
            output.write_object(value)

    def read(self, input: ObjectDataInput) -> dict:
        size = input.read_int()
        result = {}
        for _ in range(size):
            key = input.read_object()
            value = input.read_object()
            result[key] = value
        return result


class UUIDSerializer(Serializer):
    """Serializer for UUID values."""

    @property
    def type_id(self) -> int:
        return UUID_TYPE_ID

    def write(self, output: ObjectDataOutput, obj) -> None:
        output.write_long(obj.int >> 64)
        output.write_long(obj.int & ((1 << 64) - 1))

    def read(self, input: ObjectDataInput):
        import uuid
        msb = input.read_long()
        lsb = input.read_long()
        int_val = (msb << 64) | (lsb & ((1 << 64) - 1))
        return uuid.UUID(int=int_val)


def get_builtin_serializers() -> Dict[int, Serializer]:
    """Get all built-in serializers indexed by type ID."""
    serializers = [
        NoneSerializer(),
        BoolSerializer(),
        ByteSerializer(),
        ShortSerializer(),
        IntSerializer(),
        LongSerializer(),
        FloatSerializer(),
        DoubleSerializer(),
        StringSerializer(),
        ByteArraySerializer(),
        ListSerializer(),
        DictSerializer(),
        UUIDSerializer(),
    ]
    return {s.type_id: s for s in serializers}


def get_type_serializer_mapping() -> Dict[type, Serializer]:
    """Get mapping from Python types to serializers."""
    return {
        type(None): NoneSerializer(),
        bool: BoolSerializer(),
        int: IntSerializer(),
        float: FloatSerializer(),
        str: StringSerializer(),
        bytes: ByteArraySerializer(),
        bytearray: ByteArraySerializer(),
        list: ListSerializer(),
        tuple: ListSerializer(),
        dict: DictSerializer(),
    }
