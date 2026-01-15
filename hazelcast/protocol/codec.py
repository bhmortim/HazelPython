"""Base codec functionality for Hazelcast protocol serialization."""

import struct
from typing import Tuple

BYTE_SIZE = 1
BOOLEAN_SIZE = 1
SHORT_SIZE = 2
INT_SIZE = 4
LONG_SIZE = 8
FLOAT_SIZE = 4
DOUBLE_SIZE = 8
UUID_SIZE = 17


class FixSizedTypesCodec:
    """Codec for fixed-size primitive types."""

    @staticmethod
    def encode_byte(buffer: bytearray, offset: int, value: int) -> int:
        struct.pack_into("<b", buffer, offset, value)
        return offset + BYTE_SIZE

    @staticmethod
    def decode_byte(buffer: bytes, offset: int) -> Tuple[int, int]:
        value = struct.unpack_from("<b", buffer, offset)[0]
        return value, offset + BYTE_SIZE

    @staticmethod
    def encode_boolean(buffer: bytearray, offset: int, value: bool) -> int:
        struct.pack_into("<B", buffer, offset, 1 if value else 0)
        return offset + BOOLEAN_SIZE

    @staticmethod
    def decode_boolean(buffer: bytes, offset: int) -> Tuple[bool, int]:
        value = struct.unpack_from("<B", buffer, offset)[0]
        return value != 0, offset + BOOLEAN_SIZE

    @staticmethod
    def encode_short(buffer: bytearray, offset: int, value: int) -> int:
        struct.pack_into("<h", buffer, offset, value)
        return offset + SHORT_SIZE

    @staticmethod
    def decode_short(buffer: bytes, offset: int) -> Tuple[int, int]:
        value = struct.unpack_from("<h", buffer, offset)[0]
        return value, offset + SHORT_SIZE

    @staticmethod
    def encode_int(buffer: bytearray, offset: int, value: int) -> int:
        struct.pack_into("<i", buffer, offset, value)
        return offset + INT_SIZE

    @staticmethod
    def decode_int(buffer: bytes, offset: int) -> Tuple[int, int]:
        value = struct.unpack_from("<i", buffer, offset)[0]
        return value, offset + INT_SIZE

    @staticmethod
    def encode_long(buffer: bytearray, offset: int, value: int) -> int:
        struct.pack_into("<q", buffer, offset, value)
        return offset + LONG_SIZE

    @staticmethod
    def decode_long(buffer: bytes, offset: int) -> Tuple[int, int]:
        value = struct.unpack_from("<q", buffer, offset)[0]
        return value, offset + LONG_SIZE

    @staticmethod
    def encode_float(buffer: bytearray, offset: int, value: float) -> int:
        struct.pack_into("<f", buffer, offset, value)
        return offset + FLOAT_SIZE

    @staticmethod
    def decode_float(buffer: bytes, offset: int) -> Tuple[float, int]:
        value = struct.unpack_from("<f", buffer, offset)[0]
        return value, offset + FLOAT_SIZE

    @staticmethod
    def encode_double(buffer: bytearray, offset: int, value: float) -> int:
        struct.pack_into("<d", buffer, offset, value)
        return offset + DOUBLE_SIZE

    @staticmethod
    def decode_double(buffer: bytes, offset: int) -> Tuple[float, int]:
        value = struct.unpack_from("<d", buffer, offset)[0]
        return value, offset + DOUBLE_SIZE

    @staticmethod
    def encode_uuid(buffer: bytearray, offset: int, value) -> int:
        if value is None:
            struct.pack_into("<B", buffer, offset, 1)
            return offset + 1

        struct.pack_into("<B", buffer, offset, 0)
        offset += 1
        struct.pack_into("<q", buffer, offset, value.int >> 64)
        offset += LONG_SIZE
        struct.pack_into("<q", buffer, offset, value.int & ((1 << 64) - 1))
        return offset + LONG_SIZE

    @staticmethod
    def decode_uuid(buffer: bytes, offset: int) -> Tuple:
        import uuid

        is_null = struct.unpack_from("<B", buffer, offset)[0]
        offset += 1

        if is_null:
            return None, offset

        msb = struct.unpack_from("<q", buffer, offset)[0]
        offset += LONG_SIZE
        lsb = struct.unpack_from("<q", buffer, offset)[0]
        offset += LONG_SIZE

        int_val = (msb << 64) | (lsb & ((1 << 64) - 1))
        return uuid.UUID(int=int_val), offset


class LeBytes:
    """Little-endian byte encoding utilities."""

    @staticmethod
    def int_to_bytes(value: int) -> bytes:
        return struct.pack("<i", value)

    @staticmethod
    def bytes_to_int(data: bytes, offset: int = 0) -> int:
        return struct.unpack_from("<i", data, offset)[0]

    @staticmethod
    def long_to_bytes(value: int) -> bytes:
        return struct.pack("<q", value)

    @staticmethod
    def bytes_to_long(data: bytes, offset: int = 0) -> int:
        return struct.unpack_from("<q", data, offset)[0]
