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


# FlakeIdGenerator protocol constants
FLAKE_ID_GENERATOR_NEW_ID_BATCH = 0x1C0100

# Response frame offsets
FLAKE_ID_RESPONSE_HEADER_SIZE = 22  # response header size


class IdBatch:
    """Represents a batch of IDs from FlakeIdGenerator."""

    def __init__(self, base: int, increment: int, batch_size: int):
        self._base = base
        self._increment = increment
        self._batch_size = batch_size
        self._index = 0

    @property
    def base(self) -> int:
        return self._base

    @property
    def increment(self) -> int:
        return self._increment

    @property
    def batch_size(self) -> int:
        return self._batch_size

    def next_id(self) -> int:
        """Get the next ID from this batch.

        Returns:
            The next available ID.

        Raises:
            StopIteration: If the batch is exhausted.
        """
        if self._index >= self._batch_size:
            raise StopIteration("Batch exhausted")
        id_value = self._base + self._index * self._increment
        self._index += 1
        return id_value

    def remaining(self) -> int:
        """Return the number of IDs remaining in this batch."""
        return self._batch_size - self._index

    def is_exhausted(self) -> bool:
        """Check if the batch has been fully consumed."""
        return self._index >= self._batch_size


class FlakeIdGeneratorCodec:
    """Codec for FlakeIdGenerator protocol messages."""

    REQUEST_INITIAL_FRAME_SIZE = 22  # header(22)
    REQUEST_BATCH_SIZE_OFFSET = 22

    @staticmethod
    def encode_new_id_batch_request(
        name: str, batch_size: int
    ) -> "ClientMessage":
        """Encode a NewIdBatch request message.

        Args:
            name: The name of the FlakeIdGenerator.
            batch_size: The number of IDs to request.

        Returns:
            The encoded ClientMessage.
        """
        from hazelcast.protocol.client_message import ClientMessage, Frame

        initial_frame = Frame(
            bytearray(FlakeIdGeneratorCodec.REQUEST_INITIAL_FRAME_SIZE + INT_SIZE)
        )
        struct.pack_into(
            "<i",
            initial_frame.buf,
            FlakeIdGeneratorCodec.REQUEST_BATCH_SIZE_OFFSET,
            batch_size,
        )

        request = ClientMessage.create_for_encode()
        request.add_frame(initial_frame)
        request.set_message_type(FLAKE_ID_GENERATOR_NEW_ID_BATCH)
        request.set_partition_id(-1)

        StringCodec.encode(request, name)
        return request

    @staticmethod
    def decode_new_id_batch_response(msg: "ClientMessage") -> IdBatch:
        """Decode a NewIdBatch response message.

        Args:
            msg: The response ClientMessage.

        Returns:
            An IdBatch containing the allocated IDs.
        """
        frame = msg.next_frame()
        buf = frame.buf

        base = struct.unpack_from("<q", buf, FLAKE_ID_RESPONSE_HEADER_SIZE)[0]
        increment = struct.unpack_from(
            "<q", buf, FLAKE_ID_RESPONSE_HEADER_SIZE + LONG_SIZE
        )[0]
        batch_size = struct.unpack_from(
            "<i", buf, FLAKE_ID_RESPONSE_HEADER_SIZE + 2 * LONG_SIZE
        )[0]

        return IdBatch(base, increment, batch_size)


class StringCodec:
    """Codec for variable-length strings."""

    @staticmethod
    def encode(msg: "ClientMessage", value: str) -> None:
        """Encode a string into the message."""
        from hazelcast.protocol.client_message import Frame

        encoded = value.encode("utf-8")
        frame = Frame(bytearray(encoded))
        msg.add_frame(frame)

    @staticmethod
    def decode(msg: "ClientMessage") -> str:
        """Decode a string from the message."""
        frame = msg.next_frame()
        return frame.buf.decode("utf-8")


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
