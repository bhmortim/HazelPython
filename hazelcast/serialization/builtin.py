"""Built-in serializers for Python primitive types.

This module provides serializers for Python's built-in types including
primitives (bool, int, float, str), collections (list, dict), and
standard library types (datetime, Decimal, UUID).

These serializers are automatically registered with the SerializationService
and handle the most common data types without requiring custom serializers.

Supported Types:
    - Primitives: None, bool, int, float, str, bytes
    - Collections: list, tuple, dict
    - Date/Time: datetime, date, time
    - Numbers: Decimal (BigDecimal)
    - Other: UUID, HazelcastJsonValue

Type IDs:
    Built-in serializers use negative type IDs to distinguish them from
    user-defined serializers (which should use positive IDs).
"""

import struct
from datetime import datetime, date, time, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from hazelcast.serialization.api import ObjectDataInput, ObjectDataOutput, Serializer

if TYPE_CHECKING:
    from hazelcast.serialization.json import HazelcastJsonValue


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
DATETIME_TYPE_ID = -20
DATE_TYPE_ID = -21
TIME_TYPE_ID = -22
BIG_DECIMAL_TYPE_ID = -23
JSON_TYPE_ID = -130


class NoneSerializer(Serializer[None]):
    """Serializer for None values.

    Handles Python's None (null) type. No data is written or read
    since the type ID alone indicates a None value.
    """

    @property
    def type_id(self) -> int:
        return NONE_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: None) -> None:
        pass

    def read(self, input: ObjectDataInput) -> None:
        return None


class BoolSerializer(Serializer[bool]):
    """Serializer for boolean values.

    Serializes Python bool as a single byte (0 or 1).
    """

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
    """Serializer for integer values.

    Serializes Python int as a 32-bit signed integer.

    Note:
        Python integers have arbitrary precision, but this serializer
        only handles values in the 32-bit signed range (-2^31 to 2^31-1).
        Use LongSerializer for larger values.
    """

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
    """Serializer for string values.

    Serializes Python str as UTF-8 encoded bytes with a length prefix.
    """

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
    """Serializer for list values.

    Serializes Python list (and tuple) as a length-prefixed sequence
    of serialized objects. Each element is serialized recursively.
    """

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
    """Serializer for dictionary values.

    Serializes Python dict as a length-prefixed sequence of
    key-value pairs. Both keys and values are serialized recursively.
    """

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


class BooleanArraySerializer(Serializer[List[bool]]):
    """Serializer for boolean array values."""

    @property
    def type_id(self) -> int:
        return BOOLEAN_ARRAY_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: List[bool]) -> None:
        if obj is None:
            output.write_int(-1)
            return
        output.write_int(len(obj))
        for item in obj:
            output.write_boolean(item)

    def read(self, input: ObjectDataInput) -> List[bool]:
        length = input.read_int()
        if length < 0:
            return []
        return [input.read_boolean() for _ in range(length)]


class ShortArraySerializer(Serializer[List[int]]):
    """Serializer for short array values."""

    @property
    def type_id(self) -> int:
        return SHORT_ARRAY_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: List[int]) -> None:
        if obj is None:
            output.write_int(-1)
            return
        output.write_int(len(obj))
        for item in obj:
            output.write_short(item)

    def read(self, input: ObjectDataInput) -> List[int]:
        length = input.read_int()
        if length < 0:
            return []
        return [input.read_short() for _ in range(length)]


class IntArraySerializer(Serializer[List[int]]):
    """Serializer for integer array values."""

    @property
    def type_id(self) -> int:
        return INT_ARRAY_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: List[int]) -> None:
        if obj is None:
            output.write_int(-1)
            return
        output.write_int(len(obj))
        for item in obj:
            output.write_int(item)

    def read(self, input: ObjectDataInput) -> List[int]:
        length = input.read_int()
        if length < 0:
            return []
        return [input.read_int() for _ in range(length)]


class LongArraySerializer(Serializer[List[int]]):
    """Serializer for long array values."""

    @property
    def type_id(self) -> int:
        return LONG_ARRAY_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: List[int]) -> None:
        if obj is None:
            output.write_int(-1)
            return
        output.write_int(len(obj))
        for item in obj:
            output.write_long(item)

    def read(self, input: ObjectDataInput) -> List[int]:
        length = input.read_int()
        if length < 0:
            return []
        return [input.read_long() for _ in range(length)]


class FloatArraySerializer(Serializer[List[float]]):
    """Serializer for float array values."""

    @property
    def type_id(self) -> int:
        return FLOAT_ARRAY_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: List[float]) -> None:
        if obj is None:
            output.write_int(-1)
            return
        output.write_int(len(obj))
        for item in obj:
            output.write_float(item)

    def read(self, input: ObjectDataInput) -> List[float]:
        length = input.read_int()
        if length < 0:
            return []
        return [input.read_float() for _ in range(length)]


class DoubleArraySerializer(Serializer[List[float]]):
    """Serializer for double array values."""

    @property
    def type_id(self) -> int:
        return DOUBLE_ARRAY_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: List[float]) -> None:
        if obj is None:
            output.write_int(-1)
            return
        output.write_int(len(obj))
        for item in obj:
            output.write_double(item)

    def read(self, input: ObjectDataInput) -> List[float]:
        length = input.read_int()
        if length < 0:
            return []
        return [input.read_double() for _ in range(length)]


class StringArraySerializer(Serializer[List[str]]):
    """Serializer for string array values."""

    @property
    def type_id(self) -> int:
        return STRING_ARRAY_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: List[str]) -> None:
        if obj is None:
            output.write_int(-1)
            return
        output.write_int(len(obj))
        for item in obj:
            output.write_string(item)

    def read(self, input: ObjectDataInput) -> List[str]:
        length = input.read_int()
        if length < 0:
            return []
        return [input.read_string() for _ in range(length)]


class DateTimeSerializer(Serializer[datetime]):
    """Serializer for datetime values.

    Serializes Python datetime with year, month, day, hour, minute,
    second, and microsecond components.
    """

    @property
    def type_id(self) -> int:
        return DATETIME_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: datetime) -> None:
        output.write_int(obj.year)
        output.write_byte(obj.month)
        output.write_byte(obj.day)
        output.write_byte(obj.hour)
        output.write_byte(obj.minute)
        output.write_byte(obj.second)
        output.write_int(obj.microsecond)

    def read(self, input: ObjectDataInput) -> datetime:
        year = input.read_int()
        month = input.read_byte()
        day = input.read_byte()
        hour = input.read_byte()
        minute = input.read_byte()
        second = input.read_byte()
        microsecond = input.read_int()
        return datetime(year, month, day, hour, minute, second, microsecond)


class DateSerializer(Serializer[date]):
    """Serializer for date values."""

    @property
    def type_id(self) -> int:
        return DATE_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: date) -> None:
        output.write_int(obj.year)
        output.write_byte(obj.month)
        output.write_byte(obj.day)

    def read(self, input: ObjectDataInput) -> date:
        year = input.read_int()
        month = input.read_byte()
        day = input.read_byte()
        return date(year, month, day)


class TimeSerializer(Serializer[time]):
    """Serializer for time values."""

    @property
    def type_id(self) -> int:
        return TIME_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: time) -> None:
        output.write_byte(obj.hour)
        output.write_byte(obj.minute)
        output.write_byte(obj.second)
        output.write_int(obj.microsecond)

    def read(self, input: ObjectDataInput) -> time:
        hour = input.read_byte()
        minute = input.read_byte()
        second = input.read_byte()
        microsecond = input.read_int()
        return time(hour, minute, second, microsecond)


class BigDecimalSerializer(Serializer[Decimal]):
    """Serializer for Decimal (BigDecimal) values.

    Serializes Python Decimal as unscaled integer bytes plus scale,
    compatible with Java's BigDecimal representation.
    """

    @property
    def type_id(self) -> int:
        return BIG_DECIMAL_TYPE_ID

    def write(self, output: ObjectDataOutput, obj: Decimal) -> None:
        sign, digits, exponent = obj.as_tuple()
        unscaled = int("".join(map(str, digits))) if digits else 0
        if sign:
            unscaled = -unscaled
        scale = -exponent if isinstance(exponent, int) else 0

        unscaled_bytes = self._int_to_bytes(unscaled)
        output.write_int(len(unscaled_bytes))
        output.write_byte_array(unscaled_bytes)
        output.write_int(scale)

    def read(self, input: ObjectDataInput) -> Decimal:
        length = input.read_int()
        unscaled_bytes = input.read_byte_array()
        scale = input.read_int()

        unscaled = self._bytes_to_int(unscaled_bytes)
        if scale >= 0:
            return Decimal(unscaled) / (Decimal(10) ** scale)
        else:
            return Decimal(unscaled) * (Decimal(10) ** (-scale))

    @staticmethod
    def _int_to_bytes(value: int) -> bytes:
        if value == 0:
            return b"\x00"
        negative = value < 0
        if negative:
            value = -value
        byte_length = (value.bit_length() + 8) // 8
        result = value.to_bytes(byte_length, byteorder="big", signed=True if negative else False)
        if negative:
            value = -value - 1
            byte_length = (value.bit_length() + 8) // 8
            result = (value ^ ((1 << (byte_length * 8)) - 1)).to_bytes(byte_length, "big")
            result = bytes([(~b) & 0xFF for b in result])
        return result

    @staticmethod
    def _bytes_to_int(data: bytes) -> int:
        if not data:
            return 0
        return int.from_bytes(data, byteorder="big", signed=True)


def get_builtin_serializers() -> Dict[int, Serializer]:
    """Get all built-in serializers indexed by type ID.

    Returns:
        Dictionary mapping type IDs to serializer instances.
    """
    from hazelcast.serialization.json import HazelcastJsonValueSerializer

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
        BooleanArraySerializer(),
        ShortArraySerializer(),
        IntArraySerializer(),
        LongArraySerializer(),
        FloatArraySerializer(),
        DoubleArraySerializer(),
        StringArraySerializer(),
        ListSerializer(),
        DictSerializer(),
        UUIDSerializer(),
        DateTimeSerializer(),
        DateSerializer(),
        TimeSerializer(),
        BigDecimalSerializer(),
        HazelcastJsonValueSerializer(),
    ]
    return {s.type_id: s for s in serializers}


def get_type_serializer_mapping() -> Dict[type, Serializer]:
    """Get mapping from Python types to serializers.

    Returns:
        Dictionary mapping Python types to their default serializers.
    """
    from hazelcast.serialization.json import HazelcastJsonValue, HazelcastJsonValueSerializer

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
        datetime: DateTimeSerializer(),
        date: DateSerializer(),
        time: TimeSerializer(),
        Decimal: BigDecimalSerializer(),
        HazelcastJsonValue: HazelcastJsonValueSerializer(),
    }
