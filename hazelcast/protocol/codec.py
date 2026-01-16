"""Base codec functionality for Hazelcast protocol serialization."""

import struct
import uuid as uuid_module
from typing import List, Optional, Tuple

BYTE_SIZE = 1
BOOLEAN_SIZE = 1
SHORT_SIZE = 2
INT_SIZE = 4
LONG_SIZE = 8
FLOAT_SIZE = 4
DOUBLE_SIZE = 8
UUID_SIZE = 17
UUID_SIZE_IN_BYTES = 16

REQUEST_HEADER_SIZE = 22
RESPONSE_HEADER_SIZE = 22


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
    def encode_uuid(buffer: bytearray, offset: int, value: Optional[uuid_module.UUID]) -> int:
        if value is None:
            struct.pack_into("<B", buffer, offset, 1)
            return offset + UUID_SIZE

        struct.pack_into("<B", buffer, offset, 0)
        offset += 1
        msb = (value.int >> 64) & 0xFFFFFFFFFFFFFFFF
        lsb = value.int & 0xFFFFFFFFFFFFFFFF
        struct.pack_into("<q", buffer, offset, msb if msb < (1 << 63) else msb - (1 << 64))
        offset += LONG_SIZE
        struct.pack_into("<q", buffer, offset, lsb if lsb < (1 << 63) else lsb - (1 << 64))
        return offset + LONG_SIZE

    @staticmethod
    def decode_uuid(buffer: bytes, offset: int) -> Tuple[Optional[uuid_module.UUID], int]:
        is_null = struct.unpack_from("<B", buffer, offset)[0]
        offset += 1

        if is_null:
            return None, offset + UUID_SIZE_IN_BYTES

        msb = struct.unpack_from("<q", buffer, offset)[0]
        offset += LONG_SIZE
        lsb = struct.unpack_from("<q", buffer, offset)[0]
        offset += LONG_SIZE

        msb_unsigned = msb if msb >= 0 else msb + (1 << 64)
        lsb_unsigned = lsb if lsb >= 0 else lsb + (1 << 64)
        int_val = (msb_unsigned << 64) | lsb_unsigned
        return uuid_module.UUID(int=int_val), offset


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


# Protocol message type constants
MAP_PUT = 0x010100
MAP_GET = 0x010200
MAP_REMOVE = 0x010300
MAP_REPLACE = 0x010400
MAP_CONTAINS_KEY = 0x010500
MAP_CONTAINS_VALUE = 0x010600
MAP_SIZE = 0x010900
MAP_IS_EMPTY = 0x010A00
MAP_CLEAR = 0x010E00
MAP_PUT_ALL = 0x011400
MAP_KEY_SET = 0x011700
MAP_VALUES = 0x011800
MAP_ENTRY_SET = 0x011900
MAP_DELETE = 0x011B00

QUEUE_OFFER = 0x030100
QUEUE_PUT = 0x030200
QUEUE_POLL = 0x030300
QUEUE_SIZE = 0x030400
QUEUE_PEEK = 0x030500
QUEUE_TAKE = 0x030600
QUEUE_CLEAR = 0x030A00
QUEUE_CONTAINS = 0x030B00
QUEUE_IS_EMPTY = 0x030E00

LIST_ADD = 0x050100
LIST_GET = 0x050200
LIST_REMOVE = 0x050300
LIST_SET = 0x050400
LIST_SIZE = 0x050500
LIST_CONTAINS = 0x050600
LIST_INDEX_OF = 0x050700
LIST_CLEAR = 0x050800
LIST_ADD_ALL = 0x050900
LIST_IS_EMPTY = 0x050D00
LIST_SUB = 0x050E00

SET_ADD = 0x060100
SET_REMOVE = 0x060200
SET_ADD_ALL = 0x060300
SET_SIZE = 0x060400
SET_CONTAINS = 0x060500
SET_CONTAINS_ALL = 0x060600
SET_CLEAR = 0x060700
SET_IS_EMPTY = 0x060800
SET_GET_ALL = 0x060900

TOPIC_PUBLISH = 0x040100
TOPIC_ADD_LISTENER = 0x040200
TOPIC_REMOVE_LISTENER = 0x040300
TOPIC_PUBLISH_ALL = 0x040400


def _create_initial_buffer(message_type: int, partition_id: int = -1) -> bytearray:
    """Create initial frame buffer with header."""
    buffer = bytearray(REQUEST_HEADER_SIZE)
    struct.pack_into("<I", buffer, 0, message_type)
    struct.pack_into("<i", buffer, 12, partition_id)
    return buffer


def _encode_request(
    message_type: int,
    partition_id: int = -1,
    extra_size: int = 0
) -> Tuple["ClientMessage", bytearray]:
    """Create a request message with initial frame."""
    from hazelcast.protocol.client_message import ClientMessage, Frame

    buffer = bytearray(REQUEST_HEADER_SIZE + extra_size)
    struct.pack_into("<I", buffer, 0, message_type)
    struct.pack_into("<i", buffer, 12, partition_id)

    msg = ClientMessage.create_for_encode()
    return msg, buffer


class MapCodec:
    """Codec for Map protocol messages."""

    @staticmethod
    def encode_put_request(
        name: str,
        key: bytes,
        value: bytes,
        thread_id: int,
        ttl: int
    ) -> "ClientMessage":
        """Encode a Map.put request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE + LONG_SIZE + LONG_SIZE)
        struct.pack_into("<I", buffer, 0, MAP_PUT)
        struct.pack_into("<i", buffer, 12, -1)
        struct.pack_into("<q", buffer, REQUEST_HEADER_SIZE, thread_id)
        struct.pack_into("<q", buffer, REQUEST_HEADER_SIZE + LONG_SIZE, ttl)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        msg.add_frame(Frame(key))
        msg.add_frame(Frame(value))
        return msg

    @staticmethod
    def decode_put_response(msg: "ClientMessage") -> Optional[bytes]:
        """Decode a Map.put response."""
        msg.next_frame()
        frame = msg.next_frame()
        if frame is None or frame.is_null_frame:
            return None
        return frame.content

    @staticmethod
    def encode_get_request(name: str, key: bytes, thread_id: int) -> "ClientMessage":
        """Encode a Map.get request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE + LONG_SIZE)
        struct.pack_into("<I", buffer, 0, MAP_GET)
        struct.pack_into("<i", buffer, 12, -1)
        struct.pack_into("<q", buffer, REQUEST_HEADER_SIZE, thread_id)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        msg.add_frame(Frame(key))
        return msg

    @staticmethod
    def decode_get_response(msg: "ClientMessage") -> Optional[bytes]:
        """Decode a Map.get response."""
        msg.next_frame()
        frame = msg.next_frame()
        if frame is None or frame.is_null_frame:
            return None
        return frame.content

    @staticmethod
    def encode_remove_request(name: str, key: bytes, thread_id: int) -> "ClientMessage":
        """Encode a Map.remove request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE + LONG_SIZE)
        struct.pack_into("<I", buffer, 0, MAP_REMOVE)
        struct.pack_into("<i", buffer, 12, -1)
        struct.pack_into("<q", buffer, REQUEST_HEADER_SIZE, thread_id)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        msg.add_frame(Frame(key))
        return msg

    @staticmethod
    def decode_remove_response(msg: "ClientMessage") -> Optional[bytes]:
        """Decode a Map.remove response."""
        msg.next_frame()
        frame = msg.next_frame()
        if frame is None or frame.is_null_frame:
            return None
        return frame.content

    @staticmethod
    def encode_contains_key_request(name: str, key: bytes, thread_id: int) -> "ClientMessage":
        """Encode a Map.containsKey request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE + LONG_SIZE)
        struct.pack_into("<I", buffer, 0, MAP_CONTAINS_KEY)
        struct.pack_into("<i", buffer, 12, -1)
        struct.pack_into("<q", buffer, REQUEST_HEADER_SIZE, thread_id)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        msg.add_frame(Frame(key))
        return msg

    @staticmethod
    def decode_contains_key_response(msg: "ClientMessage") -> bool:
        """Decode a Map.containsKey response."""
        frame = msg.next_frame()
        if frame is None or len(frame.content) < RESPONSE_HEADER_SIZE + BOOLEAN_SIZE:
            return False
        return struct.unpack_from("<B", frame.content, RESPONSE_HEADER_SIZE)[0] != 0

    @staticmethod
    def encode_size_request(name: str) -> "ClientMessage":
        """Encode a Map.size request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, MAP_SIZE)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def decode_size_response(msg: "ClientMessage") -> int:
        """Decode a Map.size response."""
        frame = msg.next_frame()
        if frame is None or len(frame.content) < RESPONSE_HEADER_SIZE + INT_SIZE:
            return 0
        return struct.unpack_from("<i", frame.content, RESPONSE_HEADER_SIZE)[0]

    @staticmethod
    def encode_clear_request(name: str) -> "ClientMessage":
        """Encode a Map.clear request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, MAP_CLEAR)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def encode_key_set_request(name: str) -> "ClientMessage":
        """Encode a Map.keySet request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, MAP_KEY_SET)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def decode_key_set_response(msg: "ClientMessage") -> List[bytes]:
        """Decode a Map.keySet response."""
        msg.next_frame()
        return _decode_data_list(msg)

    @staticmethod
    def encode_values_request(name: str) -> "ClientMessage":
        """Encode a Map.values request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, MAP_VALUES)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def decode_values_response(msg: "ClientMessage") -> List[bytes]:
        """Decode a Map.values response."""
        msg.next_frame()
        return _decode_data_list(msg)

    @staticmethod
    def encode_entry_set_request(name: str) -> "ClientMessage":
        """Encode a Map.entrySet request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, MAP_ENTRY_SET)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def decode_entry_set_response(msg: "ClientMessage") -> List[Tuple[bytes, bytes]]:
        """Decode a Map.entrySet response."""
        msg.next_frame()
        return _decode_entry_list(msg)


class QueueCodec:
    """Codec for Queue protocol messages."""

    @staticmethod
    def encode_offer_request(name: str, value: bytes, timeout_millis: int) -> "ClientMessage":
        """Encode a Queue.offer request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE + LONG_SIZE)
        struct.pack_into("<I", buffer, 0, QUEUE_OFFER)
        struct.pack_into("<i", buffer, 12, -1)
        struct.pack_into("<q", buffer, REQUEST_HEADER_SIZE, timeout_millis)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        msg.add_frame(Frame(value))
        return msg

    @staticmethod
    def decode_offer_response(msg: "ClientMessage") -> bool:
        """Decode a Queue.offer response."""
        frame = msg.next_frame()
        if frame is None or len(frame.content) < RESPONSE_HEADER_SIZE + BOOLEAN_SIZE:
            return False
        return struct.unpack_from("<B", frame.content, RESPONSE_HEADER_SIZE)[0] != 0

    @staticmethod
    def encode_poll_request(name: str, timeout_millis: int) -> "ClientMessage":
        """Encode a Queue.poll request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE + LONG_SIZE)
        struct.pack_into("<I", buffer, 0, QUEUE_POLL)
        struct.pack_into("<i", buffer, 12, -1)
        struct.pack_into("<q", buffer, REQUEST_HEADER_SIZE, timeout_millis)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def decode_poll_response(msg: "ClientMessage") -> Optional[bytes]:
        """Decode a Queue.poll response."""
        msg.next_frame()
        frame = msg.next_frame()
        if frame is None or frame.is_null_frame:
            return None
        return frame.content

    @staticmethod
    def encode_size_request(name: str) -> "ClientMessage":
        """Encode a Queue.size request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, QUEUE_SIZE)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def decode_size_response(msg: "ClientMessage") -> int:
        """Decode a Queue.size response."""
        frame = msg.next_frame()
        if frame is None or len(frame.content) < RESPONSE_HEADER_SIZE + INT_SIZE:
            return 0
        return struct.unpack_from("<i", frame.content, RESPONSE_HEADER_SIZE)[0]

    @staticmethod
    def encode_peek_request(name: str) -> "ClientMessage":
        """Encode a Queue.peek request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, QUEUE_PEEK)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def decode_peek_response(msg: "ClientMessage") -> Optional[bytes]:
        """Decode a Queue.peek response."""
        msg.next_frame()
        frame = msg.next_frame()
        if frame is None or frame.is_null_frame:
            return None
        return frame.content

    @staticmethod
    def encode_clear_request(name: str) -> "ClientMessage":
        """Encode a Queue.clear request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, QUEUE_CLEAR)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg


class ListCodec:
    """Codec for List protocol messages."""

    @staticmethod
    def encode_add_request(name: str, value: bytes) -> "ClientMessage":
        """Encode a List.add request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, LIST_ADD)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        msg.add_frame(Frame(value))
        return msg

    @staticmethod
    def decode_add_response(msg: "ClientMessage") -> bool:
        """Decode a List.add response."""
        frame = msg.next_frame()
        if frame is None or len(frame.content) < RESPONSE_HEADER_SIZE + BOOLEAN_SIZE:
            return False
        return struct.unpack_from("<B", frame.content, RESPONSE_HEADER_SIZE)[0] != 0

    @staticmethod
    def encode_get_request(name: str, index: int) -> "ClientMessage":
        """Encode a List.get request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE + INT_SIZE)
        struct.pack_into("<I", buffer, 0, LIST_GET)
        struct.pack_into("<i", buffer, 12, -1)
        struct.pack_into("<i", buffer, REQUEST_HEADER_SIZE, index)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def decode_get_response(msg: "ClientMessage") -> Optional[bytes]:
        """Decode a List.get response."""
        msg.next_frame()
        frame = msg.next_frame()
        if frame is None or frame.is_null_frame:
            return None
        return frame.content

    @staticmethod
    def encode_remove_request(name: str, index: int) -> "ClientMessage":
        """Encode a List.remove request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE + INT_SIZE)
        struct.pack_into("<I", buffer, 0, LIST_REMOVE)
        struct.pack_into("<i", buffer, 12, -1)
        struct.pack_into("<i", buffer, REQUEST_HEADER_SIZE, index)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def decode_remove_response(msg: "ClientMessage") -> Optional[bytes]:
        """Decode a List.remove response."""
        msg.next_frame()
        frame = msg.next_frame()
        if frame is None or frame.is_null_frame:
            return None
        return frame.content

    @staticmethod
    def encode_size_request(name: str) -> "ClientMessage":
        """Encode a List.size request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, LIST_SIZE)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def decode_size_response(msg: "ClientMessage") -> int:
        """Decode a List.size response."""
        frame = msg.next_frame()
        if frame is None or len(frame.content) < RESPONSE_HEADER_SIZE + INT_SIZE:
            return 0
        return struct.unpack_from("<i", frame.content, RESPONSE_HEADER_SIZE)[0]

    @staticmethod
    def encode_clear_request(name: str) -> "ClientMessage":
        """Encode a List.clear request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, LIST_CLEAR)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def encode_contains_request(name: str, value: bytes) -> "ClientMessage":
        """Encode a List.contains request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, LIST_CONTAINS)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        msg.add_frame(Frame(value))
        return msg

    @staticmethod
    def decode_contains_response(msg: "ClientMessage") -> bool:
        """Decode a List.contains response."""
        frame = msg.next_frame()
        if frame is None or len(frame.content) < RESPONSE_HEADER_SIZE + BOOLEAN_SIZE:
            return False
        return struct.unpack_from("<B", frame.content, RESPONSE_HEADER_SIZE)[0] != 0


class SetCodec:
    """Codec for Set protocol messages."""

    @staticmethod
    def encode_add_request(name: str, value: bytes) -> "ClientMessage":
        """Encode a Set.add request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, SET_ADD)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        msg.add_frame(Frame(value))
        return msg

    @staticmethod
    def decode_add_response(msg: "ClientMessage") -> bool:
        """Decode a Set.add response."""
        frame = msg.next_frame()
        if frame is None or len(frame.content) < RESPONSE_HEADER_SIZE + BOOLEAN_SIZE:
            return False
        return struct.unpack_from("<B", frame.content, RESPONSE_HEADER_SIZE)[0] != 0

    @staticmethod
    def encode_remove_request(name: str, value: bytes) -> "ClientMessage":
        """Encode a Set.remove request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, SET_REMOVE)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        msg.add_frame(Frame(value))
        return msg

    @staticmethod
    def decode_remove_response(msg: "ClientMessage") -> bool:
        """Decode a Set.remove response."""
        frame = msg.next_frame()
        if frame is None or len(frame.content) < RESPONSE_HEADER_SIZE + BOOLEAN_SIZE:
            return False
        return struct.unpack_from("<B", frame.content, RESPONSE_HEADER_SIZE)[0] != 0

    @staticmethod
    def encode_size_request(name: str) -> "ClientMessage":
        """Encode a Set.size request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, SET_SIZE)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def decode_size_response(msg: "ClientMessage") -> int:
        """Decode a Set.size response."""
        frame = msg.next_frame()
        if frame is None or len(frame.content) < RESPONSE_HEADER_SIZE + INT_SIZE:
            return 0
        return struct.unpack_from("<i", frame.content, RESPONSE_HEADER_SIZE)[0]

    @staticmethod
    def encode_contains_request(name: str, value: bytes) -> "ClientMessage":
        """Encode a Set.contains request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, SET_CONTAINS)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        msg.add_frame(Frame(value))
        return msg

    @staticmethod
    def decode_contains_response(msg: "ClientMessage") -> bool:
        """Decode a Set.contains response."""
        frame = msg.next_frame()
        if frame is None or len(frame.content) < RESPONSE_HEADER_SIZE + BOOLEAN_SIZE:
            return False
        return struct.unpack_from("<B", frame.content, RESPONSE_HEADER_SIZE)[0] != 0

    @staticmethod
    def encode_clear_request(name: str) -> "ClientMessage":
        """Encode a Set.clear request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, SET_CLEAR)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def encode_get_all_request(name: str) -> "ClientMessage":
        """Encode a Set.getAll request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, SET_GET_ALL)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def decode_get_all_response(msg: "ClientMessage") -> List[bytes]:
        """Decode a Set.getAll response."""
        msg.next_frame()
        return _decode_data_list(msg)


class TopicCodec:
    """Codec for Topic protocol messages."""

    @staticmethod
    def encode_publish_request(name: str, message: bytes) -> "ClientMessage":
        """Encode a Topic.publish request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE)
        struct.pack_into("<I", buffer, 0, TOPIC_PUBLISH)
        struct.pack_into("<i", buffer, 12, -1)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        msg.add_frame(Frame(message))
        return msg

    @staticmethod
    def encode_add_listener_request(name: str, local_only: bool) -> "ClientMessage":
        """Encode a Topic.addMessageListener request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE + BOOLEAN_SIZE)
        struct.pack_into("<I", buffer, 0, TOPIC_ADD_LISTENER)
        struct.pack_into("<i", buffer, 12, -1)
        struct.pack_into("<B", buffer, REQUEST_HEADER_SIZE, 1 if local_only else 0)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def decode_add_listener_response(msg: "ClientMessage") -> Optional[uuid_module.UUID]:
        """Decode a Topic.addMessageListener response."""
        frame = msg.next_frame()
        if frame is None or len(frame.content) < RESPONSE_HEADER_SIZE + UUID_SIZE:
            return None
        uuid_val, _ = FixSizedTypesCodec.decode_uuid(frame.content, RESPONSE_HEADER_SIZE)
        return uuid_val

    @staticmethod
    def encode_remove_listener_request(name: str, registration_id: uuid_module.UUID) -> "ClientMessage":
        """Encode a Topic.removeMessageListener request."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(REQUEST_HEADER_SIZE + UUID_SIZE)
        struct.pack_into("<I", buffer, 0, TOPIC_REMOVE_LISTENER)
        struct.pack_into("<i", buffer, 12, -1)
        FixSizedTypesCodec.encode_uuid(buffer, REQUEST_HEADER_SIZE, registration_id)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer)))
        StringCodec.encode(msg, name)
        return msg

    @staticmethod
    def decode_remove_listener_response(msg: "ClientMessage") -> bool:
        """Decode a Topic.removeMessageListener response."""
        frame = msg.next_frame()
        if frame is None or len(frame.content) < RESPONSE_HEADER_SIZE + BOOLEAN_SIZE:
            return False
        return struct.unpack_from("<B", frame.content, RESPONSE_HEADER_SIZE)[0] != 0


def _decode_data_list(msg: "ClientMessage") -> List[bytes]:
    """Decode a list of data frames from the message."""
    from hazelcast.protocol.client_message import BEGIN_DATA_STRUCTURE_FLAG, END_DATA_STRUCTURE_FLAG

    result = []
    frame = msg.next_frame()
    if frame is None:
        return result

    while msg.has_next_frame():
        frame = msg.peek_next_frame()
        if frame is None or frame.is_end_data_structure_frame:
            msg.skip_frame()
            break
        frame = msg.next_frame()
        if frame is not None and not frame.is_null_frame:
            result.append(frame.content)

    return result


def _decode_entry_list(msg: "ClientMessage") -> List[Tuple[bytes, bytes]]:
    """Decode a list of entry (key-value) pairs from the message."""
    result = []
    frame = msg.next_frame()
    if frame is None:
        return result

    while msg.has_next_frame():
        frame = msg.peek_next_frame()
        if frame is None or frame.is_end_data_structure_frame:
            msg.skip_frame()
            break

        key_frame = msg.next_frame()
        value_frame = msg.next_frame()

        if key_frame is not None and value_frame is not None:
            key = key_frame.content if not key_frame.is_null_frame else b""
            value = value_frame.content if not value_frame.is_null_frame else b""
            result.append((key, value))

    return result
