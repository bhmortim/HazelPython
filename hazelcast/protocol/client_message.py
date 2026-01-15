"""Hazelcast protocol client message implementation."""

import struct
from typing import List, Optional

BEGIN_FLAG = 1 << 15
END_FLAG = 1 << 14
UNFRAGMENTED_FLAG = BEGIN_FLAG | END_FLAG
IS_FINAL_FLAG = 1 << 13
BEGIN_DATA_STRUCTURE_FLAG = 1 << 12
END_DATA_STRUCTURE_FLAG = 1 << 11
IS_NULL_FLAG = 1 << 10
IS_EVENT_FLAG = 1 << 9
BACKUP_AWARE_FLAG = 1 << 8
BACKUP_EVENT_FLAG = 1 << 7

SIZE_OF_FRAME_LENGTH_AND_FLAGS = 6
INITIAL_FRAME_SIZE = 22

TYPE_OFFSET = 0
CORRELATION_ID_OFFSET = 4
PARTITION_ID_OFFSET = 12
INITIAL_FRAME_HEADER_SIZE = 16


class Frame:
    """A single frame in a Hazelcast protocol message."""

    def __init__(self, content: bytes = b"", flags: int = 0):
        self.content = content
        self.flags = flags

    @property
    def is_begin_frame(self) -> bool:
        return (self.flags & BEGIN_FLAG) != 0

    @property
    def is_end_frame(self) -> bool:
        return (self.flags & END_FLAG) != 0

    @property
    def is_final_frame(self) -> bool:
        return (self.flags & IS_FINAL_FLAG) != 0

    @property
    def is_null_frame(self) -> bool:
        return (self.flags & IS_NULL_FLAG) != 0

    @property
    def is_begin_data_structure_frame(self) -> bool:
        return (self.flags & BEGIN_DATA_STRUCTURE_FLAG) != 0

    @property
    def is_end_data_structure_frame(self) -> bool:
        return (self.flags & END_DATA_STRUCTURE_FLAG) != 0

    def copy_with_new_flags(self, flags: int) -> "Frame":
        return Frame(self.content, flags)

    def __len__(self) -> int:
        return len(self.content)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Frame):
            return False
        return self.content == other.content and self.flags == other.flags


NULL_FRAME = Frame(b"", IS_NULL_FLAG)
BEGIN_FRAME = Frame(b"", BEGIN_DATA_STRUCTURE_FLAG)
END_FRAME = Frame(b"", END_DATA_STRUCTURE_FLAG)


class ClientMessage:
    """Hazelcast protocol client message.

    A message consists of one or more frames. The first frame contains
    the message header with type, correlation ID, and partition ID.
    """

    def __init__(self, frames: List[Frame] = None):
        self._frames: List[Frame] = frames or []
        self._read_index = 0

    @classmethod
    def create_for_encode(cls) -> "ClientMessage":
        return cls()

    @classmethod
    def create_for_decode(cls, frames: List[Frame]) -> "ClientMessage":
        msg = cls(frames)
        msg._read_index = 0
        return msg

    def add_frame(self, frame: Frame) -> None:
        self._frames.append(frame)

    @property
    def frames(self) -> List[Frame]:
        return self._frames

    @property
    def start_frame(self) -> Optional[Frame]:
        return self._frames[0] if self._frames else None

    def next_frame(self) -> Optional[Frame]:
        if self._read_index >= len(self._frames):
            return None
        frame = self._frames[self._read_index]
        self._read_index += 1
        return frame

    def peek_next_frame(self) -> Optional[Frame]:
        if self._read_index >= len(self._frames):
            return None
        return self._frames[self._read_index]

    def has_next_frame(self) -> bool:
        return self._read_index < len(self._frames)

    def reset_read_index(self) -> None:
        self._read_index = 0

    def skip_frame(self) -> None:
        if self._read_index < len(self._frames):
            self._read_index += 1

    def get_message_type(self) -> int:
        if not self._frames:
            return 0
        content = self._frames[0].content
        if len(content) < 4:
            return 0
        return struct.unpack_from("<I", content, TYPE_OFFSET)[0]

    def set_message_type(self, message_type: int) -> None:
        if self._frames and len(self._frames[0].content) >= 4:
            content = bytearray(self._frames[0].content)
            struct.pack_into("<I", content, TYPE_OFFSET, message_type)
            self._frames[0] = Frame(bytes(content), self._frames[0].flags)

    def get_correlation_id(self) -> int:
        if not self._frames:
            return 0
        content = self._frames[0].content
        if len(content) < 12:
            return 0
        return struct.unpack_from("<q", content, CORRELATION_ID_OFFSET)[0]

    def set_correlation_id(self, correlation_id: int) -> None:
        if self._frames and len(self._frames[0].content) >= 12:
            content = bytearray(self._frames[0].content)
            struct.pack_into("<q", content, CORRELATION_ID_OFFSET, correlation_id)
            self._frames[0] = Frame(bytes(content), self._frames[0].flags)

    def get_partition_id(self) -> int:
        if not self._frames:
            return -1
        content = self._frames[0].content
        if len(content) < 16:
            return -1
        return struct.unpack_from("<i", content, PARTITION_ID_OFFSET)[0]

    def set_partition_id(self, partition_id: int) -> None:
        if self._frames and len(self._frames[0].content) >= 16:
            content = bytearray(self._frames[0].content)
            struct.pack_into("<i", content, PARTITION_ID_OFFSET, partition_id)
            self._frames[0] = Frame(bytes(content), self._frames[0].flags)

    def get_total_length(self) -> int:
        total = 0
        for frame in self._frames:
            total += SIZE_OF_FRAME_LENGTH_AND_FLAGS + len(frame.content)
        return total

    def to_bytes(self) -> bytes:
        result = bytearray()
        for i, frame in enumerate(self._frames):
            flags = frame.flags
            if i == 0:
                flags |= BEGIN_FLAG
            if i == len(self._frames) - 1:
                flags |= END_FLAG

            frame_length = SIZE_OF_FRAME_LENGTH_AND_FLAGS + len(frame.content)
            result.extend(struct.pack("<I", frame_length))
            result.extend(struct.pack("<H", flags))
            result.extend(frame.content)

        return bytes(result)

    @classmethod
    def from_bytes(cls, data: bytes) -> "ClientMessage":
        frames = []
        offset = 0

        while offset < len(data):
            if offset + SIZE_OF_FRAME_LENGTH_AND_FLAGS > len(data):
                break

            frame_length = struct.unpack_from("<I", data, offset)[0]
            flags = struct.unpack_from("<H", data, offset + 4)[0]

            content_start = offset + SIZE_OF_FRAME_LENGTH_AND_FLAGS
            content_end = offset + frame_length

            if content_end > len(data):
                break

            content = data[content_start:content_end]
            frames.append(Frame(content, flags))
            offset = content_end

        return cls.create_for_decode(frames)

    @staticmethod
    def create_initial_frame(
        payload_size: int, flags: int = UNFRAGMENTED_FLAG
    ) -> Frame:
        total_size = INITIAL_FRAME_HEADER_SIZE + payload_size
        content = bytearray(total_size)
        return Frame(bytes(content), flags)

    def __len__(self) -> int:
        return len(self._frames)
