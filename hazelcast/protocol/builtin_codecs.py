"""Builtin codecs for Hazelcast protocol types."""

from typing import Callable, List, Optional, TypeVar

from hazelcast.protocol.client_message import (
    ClientMessage,
    Frame,
    NULL_FRAME,
    BEGIN_FRAME,
    END_FRAME,
)
from hazelcast.protocol.codec import (
    FixSizedTypesCodec,
    INT_SIZE,
    LONG_SIZE,
)

T = TypeVar("T")


class CodecUtil:
    """Utility functions for codec operations."""

    @staticmethod
    def fast_forward_to_end_frame(message: ClientMessage) -> None:
        depth = 0
        while message.has_next_frame():
            frame = message.next_frame()
            if frame.is_begin_data_structure_frame:
                depth += 1
            elif frame.is_end_data_structure_frame:
                if depth == 0:
                    return
                depth -= 1

    @staticmethod
    def encode_nullable(
        message: ClientMessage,
        value: Optional[T],
        encoder: Callable[[ClientMessage, T], None],
    ) -> None:
        if value is None:
            message.add_frame(NULL_FRAME)
        else:
            encoder(message, value)

    @staticmethod
    def decode_nullable(
        message: ClientMessage, decoder: Callable[[ClientMessage], T]
    ) -> Optional[T]:
        frame = message.peek_next_frame()
        if frame is None or frame.is_null_frame:
            message.skip_frame()
            return None
        return decoder(message)


class StringCodec:
    """Codec for string values."""

    @staticmethod
    def encode(message: ClientMessage, value: str) -> None:
        encoded = value.encode("utf-8")
        message.add_frame(Frame(encoded))

    @staticmethod
    def decode(message: ClientMessage) -> str:
        frame = message.next_frame()
        if frame is None:
            return ""
        return frame.content.decode("utf-8")


class ByteArrayCodec:
    """Codec for byte array values."""

    @staticmethod
    def encode(message: ClientMessage, value: bytes) -> None:
        message.add_frame(Frame(value))

    @staticmethod
    def decode(message: ClientMessage) -> bytes:
        frame = message.next_frame()
        if frame is None:
            return b""
        return frame.content


class DataCodec:
    """Codec for serialized Data objects."""

    @staticmethod
    def encode(message: ClientMessage, value: bytes) -> None:
        message.add_frame(Frame(value))

    @staticmethod
    def decode(message: ClientMessage) -> bytes:
        frame = message.next_frame()
        if frame is None:
            return b""
        return frame.content

    @staticmethod
    def encode_nullable(message: ClientMessage, value: Optional[bytes]) -> None:
        CodecUtil.encode_nullable(message, value, DataCodec.encode)

    @staticmethod
    def decode_nullable(message: ClientMessage) -> Optional[bytes]:
        return CodecUtil.decode_nullable(message, DataCodec.decode)


class LongCodec:
    """Codec for long values in a frame."""

    @staticmethod
    def encode(message: ClientMessage, value: int) -> None:
        buffer = bytearray(LONG_SIZE)
        FixSizedTypesCodec.encode_long(buffer, 0, value)
        message.add_frame(Frame(bytes(buffer)))

    @staticmethod
    def decode(message: ClientMessage) -> int:
        frame = message.next_frame()
        if frame is None or len(frame.content) < LONG_SIZE:
            return 0
        return FixSizedTypesCodec.decode_long(frame.content, 0)[0]


class IntegerCodec:
    """Codec for integer values in a frame."""

    @staticmethod
    def encode(message: ClientMessage, value: int) -> None:
        buffer = bytearray(INT_SIZE)
        FixSizedTypesCodec.encode_int(buffer, 0, value)
        message.add_frame(Frame(bytes(buffer)))

    @staticmethod
    def decode(message: ClientMessage) -> int:
        frame = message.next_frame()
        if frame is None or len(frame.content) < INT_SIZE:
            return 0
        return FixSizedTypesCodec.decode_int(frame.content, 0)[0]


class ListMultiFrameCodec:
    """Codec for list values spread across multiple frames."""

    @staticmethod
    def encode(
        message: ClientMessage,
        items: List[T],
        encoder: Callable[[ClientMessage, T], None],
    ) -> None:
        message.add_frame(BEGIN_FRAME)
        for item in items:
            encoder(message, item)
        message.add_frame(END_FRAME)

    @staticmethod
    def decode(
        message: ClientMessage, decoder: Callable[[ClientMessage], T]
    ) -> List[T]:
        result = []
        message.next_frame()

        while message.has_next_frame():
            frame = message.peek_next_frame()
            if frame is None or frame.is_end_data_structure_frame:
                message.next_frame()
                break
            result.append(decoder(message))

        return result

    @staticmethod
    def encode_nullable(
        message: ClientMessage,
        items: Optional[List[T]],
        encoder: Callable[[ClientMessage, T], None],
    ) -> None:
        if items is None:
            message.add_frame(NULL_FRAME)
        else:
            ListMultiFrameCodec.encode(message, items, encoder)

    @staticmethod
    def decode_nullable(
        message: ClientMessage, decoder: Callable[[ClientMessage], T]
    ) -> Optional[List[T]]:
        frame = message.peek_next_frame()
        if frame is None or frame.is_null_frame:
            message.skip_frame()
            return None
        return ListMultiFrameCodec.decode(message, decoder)


class ListIntegerCodec:
    """Codec for list of integers packed in a single frame."""

    @staticmethod
    def encode(message: ClientMessage, items: List[int]) -> None:
        buffer = bytearray(len(items) * INT_SIZE)
        offset = 0
        for item in items:
            offset = FixSizedTypesCodec.encode_int(buffer, offset, item)
        message.add_frame(Frame(bytes(buffer)))

    @staticmethod
    def decode(message: ClientMessage) -> List[int]:
        frame = message.next_frame()
        if frame is None:
            return []

        result = []
        offset = 0
        while offset < len(frame.content):
            value, offset = FixSizedTypesCodec.decode_int(frame.content, offset)
            result.append(value)
        return result


class ListLongCodec:
    """Codec for list of longs packed in a single frame."""

    @staticmethod
    def encode(message: ClientMessage, items: List[int]) -> None:
        buffer = bytearray(len(items) * LONG_SIZE)
        offset = 0
        for item in items:
            offset = FixSizedTypesCodec.encode_long(buffer, offset, item)
        message.add_frame(Frame(bytes(buffer)))

    @staticmethod
    def decode(message: ClientMessage) -> List[int]:
        frame = message.next_frame()
        if frame is None:
            return []

        result = []
        offset = 0
        while offset < len(frame.content):
            value, offset = FixSizedTypesCodec.decode_long(frame.content, offset)
            result.append(value)
        return result


class EntryListCodec:
    """Codec for entry lists (key-value pairs)."""

    @staticmethod
    def encode(
        message: ClientMessage,
        entries: List[tuple],
        key_encoder: Callable[[ClientMessage, any], None],
        value_encoder: Callable[[ClientMessage, any], None],
    ) -> None:
        message.add_frame(BEGIN_FRAME)
        for key, value in entries:
            key_encoder(message, key)
            value_encoder(message, value)
        message.add_frame(END_FRAME)

    @staticmethod
    def decode(
        message: ClientMessage,
        key_decoder: Callable[[ClientMessage], any],
        value_decoder: Callable[[ClientMessage], any],
    ) -> List[tuple]:
        result = []
        message.next_frame()

        while message.has_next_frame():
            frame = message.peek_next_frame()
            if frame is None or frame.is_end_data_structure_frame:
                message.next_frame()
                break
            key = key_decoder(message)
            value = value_decoder(message)
            result.append((key, value))

        return result
