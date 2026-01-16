"""Unit tests for Hazelcast protocol codecs and message handling."""

import struct
import uuid
import pytest

from hazelcast.protocol.client_message import (
    Frame,
    ClientMessage,
    MessageAccumulator,
    InboundMessage,
    BEGIN_FLAG,
    END_FLAG,
    UNFRAGMENTED_FLAG,
    IS_FINAL_FLAG,
    BEGIN_DATA_STRUCTURE_FLAG,
    END_DATA_STRUCTURE_FLAG,
    IS_NULL_FLAG,
    IS_EVENT_FLAG,
    BACKUP_AWARE_FLAG,
    BACKUP_EVENT_FLAG,
    SIZE_OF_FRAME_LENGTH_AND_FLAGS,
    NULL_FRAME,
    BEGIN_FRAME,
    END_FRAME,
)
from hazelcast.protocol.codec import (
    FixSizedTypesCodec,
    LeBytes,
    MapCodec,
    QueueCodec,
    ListCodec,
    SetCodec,
    TopicCodec,
    FlakeIdGeneratorCodec,
    IdBatch,
    StringCodec,
    ClientCodec,
    CLIENT_AUTHENTICATION,
    CLIENT_AUTHENTICATION_CUSTOM,
    CLIENT_ADD_CLUSTER_VIEW_LISTENER,
    CLIENT_CREATE_PROXY,
    CLIENT_DESTROY_PROXY,
    CLIENT_GET_DISTRIBUTED_OBJECTS,
    CLIENT_PING,
    CLIENT_GET_PARTITIONS,
    CLIENT_SEND_SCHEMA,
    CLIENT_FETCH_SCHEMA,
    AUTH_STATUS_AUTHENTICATED,
    AUTH_STATUS_CREDENTIALS_FAILED,
    BYTE_SIZE,
    BOOLEAN_SIZE,
    SHORT_SIZE,
    INT_SIZE,
    LONG_SIZE,
    FLOAT_SIZE,
    DOUBLE_SIZE,
    UUID_SIZE,
    REQUEST_HEADER_SIZE,
    RESPONSE_HEADER_SIZE,
    MAP_PUT,
    MAP_GET,
    MAP_REMOVE,
    MAP_CONTAINS_KEY,
    MAP_SIZE,
    MAP_CLEAR,
    MAP_KEY_SET,
    MAP_VALUES,
    MAP_ENTRY_SET,
    QUEUE_OFFER,
    QUEUE_POLL,
    QUEUE_SIZE,
    QUEUE_PEEK,
    QUEUE_CLEAR,
    LIST_ADD,
    LIST_GET,
    LIST_REMOVE,
    LIST_SIZE,
    LIST_CLEAR,
    LIST_CONTAINS,
    SET_ADD,
    SET_REMOVE,
    SET_SIZE,
    SET_CONTAINS,
    SET_CLEAR,
    SET_GET_ALL,
    TOPIC_PUBLISH,
    TOPIC_ADD_LISTENER,
    TOPIC_REMOVE_LISTENER,
)
from hazelcast.protocol.builtin_codecs import (
    CodecUtil,
    StringCodec as BuiltinStringCodec,
    ByteArrayCodec,
    DataCodec,
    LongCodec,
    IntegerCodec,
    BooleanCodec,
    UUIDCodec,
    ListMultiFrameCodec,
    ListIntegerCodec,
    ListLongCodec,
    EntryListCodec,
    MapCodec as BuiltinMapCodec,
    CardinalityEstimatorCodec,
)


class TestFrame:
    """Tests for Frame class."""

    def test_frame_creation(self):
        content = b"test content"
        flags = UNFRAGMENTED_FLAG
        frame = Frame(content, flags)

        assert frame.content == content
        assert frame.flags == flags
        assert len(frame) == len(content)

    def test_frame_default_values(self):
        frame = Frame()
        assert frame.content == b""
        assert frame.flags == 0

    def test_is_begin_frame(self):
        frame = Frame(b"", BEGIN_FLAG)
        assert frame.is_begin_frame
        assert not Frame(b"", 0).is_begin_frame

    def test_is_end_frame(self):
        frame = Frame(b"", END_FLAG)
        assert frame.is_end_frame
        assert not Frame(b"", 0).is_end_frame

    def test_is_final_frame(self):
        frame = Frame(b"", IS_FINAL_FLAG)
        assert frame.is_final_frame
        assert not Frame(b"", 0).is_final_frame

    def test_is_null_frame(self):
        frame = Frame(b"", IS_NULL_FLAG)
        assert frame.is_null_frame
        assert not Frame(b"", 0).is_null_frame

    def test_is_begin_data_structure_frame(self):
        frame = Frame(b"", BEGIN_DATA_STRUCTURE_FLAG)
        assert frame.is_begin_data_structure_frame
        assert not Frame(b"", 0).is_begin_data_structure_frame

    def test_is_end_data_structure_frame(self):
        frame = Frame(b"", END_DATA_STRUCTURE_FLAG)
        assert frame.is_end_data_structure_frame
        assert not Frame(b"", 0).is_end_data_structure_frame

    def test_copy_with_new_flags(self):
        frame = Frame(b"content", BEGIN_FLAG)
        new_frame = frame.copy_with_new_flags(END_FLAG)

        assert new_frame.content == frame.content
        assert new_frame.flags == END_FLAG
        assert frame.flags == BEGIN_FLAG

    def test_frame_equality(self):
        frame1 = Frame(b"content", BEGIN_FLAG)
        frame2 = Frame(b"content", BEGIN_FLAG)
        frame3 = Frame(b"different", BEGIN_FLAG)
        frame4 = Frame(b"content", END_FLAG)

        assert frame1 == frame2
        assert frame1 != frame3
        assert frame1 != frame4
        assert frame1 != "not a frame"

    def test_frame_size(self):
        frame = Frame(b"12345", 0)
        assert frame.size() == 5 + SIZE_OF_FRAME_LENGTH_AND_FLAGS

    def test_frame_repr(self):
        frame = Frame(b"test", BEGIN_FLAG)
        repr_str = repr(frame)
        assert "content_len=4" in repr_str
        assert "flags=" in repr_str


class TestClientMessage:
    """Tests for ClientMessage class."""

    def test_create_for_encode(self):
        msg = ClientMessage.create_for_encode()
        assert len(msg) == 0
        assert msg.frames == []

    def test_create_for_decode(self):
        frames = [Frame(b"test", 0)]
        msg = ClientMessage.create_for_decode(frames)
        assert len(msg) == 1
        assert msg.frames == frames

    def test_add_frame(self):
        msg = ClientMessage.create_for_encode()
        frame = Frame(b"test", 0)
        msg.add_frame(frame)
        assert len(msg) == 1
        assert msg.frames[0] == frame

    def test_start_frame(self):
        msg = ClientMessage.create_for_encode()
        assert msg.start_frame is None

        frame = Frame(b"first", 0)
        msg.add_frame(frame)
        assert msg.start_frame == frame

    def test_next_frame(self):
        frames = [Frame(b"1", 0), Frame(b"2", 0), Frame(b"3", 0)]
        msg = ClientMessage.create_for_decode(frames)

        assert msg.next_frame() == frames[0]
        assert msg.next_frame() == frames[1]
        assert msg.next_frame() == frames[2]
        assert msg.next_frame() is None

    def test_peek_next_frame(self):
        frames = [Frame(b"1", 0)]
        msg = ClientMessage.create_for_decode(frames)

        assert msg.peek_next_frame() == frames[0]
        assert msg.peek_next_frame() == frames[0]
        msg.next_frame()
        assert msg.peek_next_frame() is None

    def test_has_next_frame(self):
        msg = ClientMessage.create_for_decode([Frame(b"1", 0)])

        assert msg.has_next_frame()
        msg.next_frame()
        assert not msg.has_next_frame()

    def test_reset_read_index(self):
        frames = [Frame(b"1", 0), Frame(b"2", 0)]
        msg = ClientMessage.create_for_decode(frames)

        msg.next_frame()
        msg.next_frame()
        assert not msg.has_next_frame()

        msg.reset_read_index()
        assert msg.has_next_frame()
        assert msg.next_frame() == frames[0]

    def test_skip_frame(self):
        frames = [Frame(b"1", 0), Frame(b"2", 0)]
        msg = ClientMessage.create_for_decode(frames)

        msg.skip_frame()
        assert msg.next_frame() == frames[1]

    def test_message_type(self):
        content = bytearray(22)
        struct.pack_into("<I", content, 0, 0x010100)
        msg = ClientMessage.create_for_decode([Frame(bytes(content), 0)])

        assert msg.get_message_type() == 0x010100

    def test_set_message_type(self):
        content = bytearray(22)
        msg = ClientMessage.create_for_decode([Frame(bytes(content), 0)])
        msg.set_message_type(0x010200)

        assert msg.get_message_type() == 0x010200

    def test_correlation_id(self):
        content = bytearray(22)
        struct.pack_into("<q", content, 4, 12345)
        msg = ClientMessage.create_for_decode([Frame(bytes(content), 0)])

        assert msg.get_correlation_id() == 12345

    def test_set_correlation_id(self):
        content = bytearray(22)
        msg = ClientMessage.create_for_decode([Frame(bytes(content), 0)])
        msg.set_correlation_id(67890)

        assert msg.get_correlation_id() == 67890

    def test_partition_id(self):
        content = bytearray(22)
        struct.pack_into("<i", content, 12, 42)
        msg = ClientMessage.create_for_decode([Frame(bytes(content), 0)])

        assert msg.get_partition_id() == 42

    def test_set_partition_id(self):
        content = bytearray(22)
        msg = ClientMessage.create_for_decode([Frame(bytes(content), 0)])
        msg.set_partition_id(99)

        assert msg.get_partition_id() == 99

    def test_partition_id_default(self):
        msg = ClientMessage.create_for_encode()
        assert msg.get_partition_id() == -1

    def test_get_total_length(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(b"12345", 0))
        msg.add_frame(Frame(b"67890", 0))

        expected = 2 * SIZE_OF_FRAME_LENGTH_AND_FLAGS + 10
        assert msg.get_total_length() == expected

    def test_to_bytes_and_from_bytes(self):
        msg = ClientMessage.create_for_encode()
        content1 = bytearray(22)
        struct.pack_into("<I", content1, 0, 0x010100)
        struct.pack_into("<q", content1, 4, 12345)
        struct.pack_into("<i", content1, 12, 42)

        msg.add_frame(Frame(bytes(content1), 0))
        msg.add_frame(Frame(b"data", 0))

        serialized = msg.to_bytes()
        restored = ClientMessage.from_bytes(serialized)

        assert len(restored) == 2
        assert restored.get_message_type() == 0x010100
        assert restored.get_correlation_id() == 12345
        assert restored.get_partition_id() == 42

    def test_to_bytes_sets_begin_end_flags(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(b"1", 0))
        msg.add_frame(Frame(b"2", 0))

        serialized = msg.to_bytes()
        restored = ClientMessage.from_bytes(serialized)

        assert restored.frames[0].is_begin_frame
        assert restored.frames[1].is_end_frame

    def test_create_initial_frame(self):
        frame = ClientMessage.create_initial_frame(10, UNFRAGMENTED_FLAG)
        assert len(frame) == 16 + 10
        assert frame.flags == UNFRAGMENTED_FLAG

    def test_is_event(self):
        content = bytearray(22)
        msg = ClientMessage.create_for_decode([Frame(bytes(content), IS_EVENT_FLAG)])
        assert msg.is_event()

        msg2 = ClientMessage.create_for_decode([Frame(bytes(content), 0)])
        assert not msg2.is_event()

    def test_is_backup_event(self):
        content = bytearray(22)
        msg = ClientMessage.create_for_decode([Frame(bytes(content), BACKUP_EVENT_FLAG)])
        assert msg.is_backup_event()

    def test_is_retryable(self):
        content = bytearray(22)
        msg = ClientMessage.create_for_decode([Frame(bytes(content), BACKUP_AWARE_FLAG)])
        assert msg.is_retryable()

    def test_copy(self):
        frames = [Frame(b"1", BEGIN_FLAG), Frame(b"2", END_FLAG)]
        msg = ClientMessage.create_for_decode(frames)
        copied = msg.copy()

        assert len(copied) == 2
        assert copied.frames[0].content == b"1"
        assert copied.frames[1].content == b"2"

    def test_repr(self):
        content = bytearray(22)
        struct.pack_into("<I", content, 0, 0x010100)
        msg = ClientMessage.create_for_decode([Frame(bytes(content), 0)])

        repr_str = repr(msg)
        assert "0x010100" in repr_str
        assert "frames=1" in repr_str

    def test_empty_message_defaults(self):
        msg = ClientMessage.create_for_encode()
        assert msg.get_message_type() == 0
        assert msg.get_correlation_id() == 0
        assert msg.get_partition_id() == -1

    def test_incomplete_frame_content(self):
        msg = ClientMessage.create_for_decode([Frame(b"ab", 0)])
        assert msg.get_message_type() == 0
        assert msg.get_correlation_id() == 0
        assert msg.get_partition_id() == -1


class TestMessageAccumulator:
    """Tests for MessageAccumulator class."""

    def test_unfragmented_message(self):
        accumulator = MessageAccumulator()
        content = bytearray(22)
        msg = ClientMessage.create_for_decode([Frame(bytes(content), UNFRAGMENTED_FLAG)])

        result = accumulator.add(msg)
        assert result is msg
        assert accumulator.pending_count == 0

    def test_fragmented_message_assembly(self):
        accumulator = MessageAccumulator()

        frag_id_bytes = struct.pack("<q", 12345)
        content1 = bytearray(22)
        content1[:8] = frag_id_bytes
        begin_msg = ClientMessage.create_for_decode([
            Frame(bytes(content1), BEGIN_FLAG)
        ])

        result1 = accumulator.add(begin_msg)
        assert result1 is None
        assert accumulator.pending_count == 1

        content2 = bytearray(22)
        content2[:8] = frag_id_bytes
        middle_msg = ClientMessage.create_for_decode([
            Frame(bytes(content2), 0),
            Frame(b"middle_data", 0)
        ])

        result2 = accumulator.add(middle_msg)
        assert result2 is None

        content3 = bytearray(22)
        content3[:8] = frag_id_bytes
        end_msg = ClientMessage.create_for_decode([
            Frame(bytes(content3), END_FLAG),
            Frame(b"end_data", 0)
        ])

        result3 = accumulator.add(end_msg)
        assert result3 is not None
        assert accumulator.pending_count == 0

    def test_missing_begin_fragment(self):
        accumulator = MessageAccumulator()
        content = bytearray(22)
        struct.pack_into("<q", content, 0, 99999)

        middle_msg = ClientMessage.create_for_decode([Frame(bytes(content), 0)])
        result = accumulator.add(middle_msg)
        assert result is None

    def test_clear(self):
        accumulator = MessageAccumulator()
        content = bytearray(22)
        struct.pack_into("<q", content, 0, 12345)

        begin_msg = ClientMessage.create_for_decode([Frame(bytes(content), BEGIN_FLAG)])
        accumulator.add(begin_msg)
        assert accumulator.pending_count == 1

        accumulator.clear()
        assert accumulator.pending_count == 0

    def test_empty_message(self):
        accumulator = MessageAccumulator()
        msg = ClientMessage.create_for_encode()
        result = accumulator.add(msg)
        assert result is None


class TestInboundMessage:
    """Tests for InboundMessage class."""

    def test_read_initial_frame(self):
        frame = Frame(b"initial", 0)
        msg = ClientMessage.create_for_decode([frame, Frame(b"data", 0)])
        inbound = InboundMessage(msg)

        initial = inbound.read_initial_frame()
        assert initial == frame

    def test_read_initial_frame_empty(self):
        msg = ClientMessage.create_for_encode()
        inbound = InboundMessage(msg)

        with pytest.raises(ValueError):
            inbound.read_initial_frame()

    def test_skip_initial_frame(self):
        frames = [Frame(b"initial", 0), Frame(b"data", 0)]
        msg = ClientMessage.create_for_decode(frames)
        inbound = InboundMessage(msg)

        inbound.skip_initial_frame()
        assert inbound.next_frame() == frames[1]

    def test_has_next_frame(self):
        msg = ClientMessage.create_for_decode([Frame(b"1", 0)])
        inbound = InboundMessage(msg)

        assert inbound.has_next_frame()
        inbound.next_frame()
        assert not inbound.has_next_frame()

    def test_peek_next_frame(self):
        frame = Frame(b"data", 0)
        msg = ClientMessage.create_for_decode([frame])
        inbound = InboundMessage(msg)

        assert inbound.peek_next_frame() == frame
        assert inbound.peek_next_frame() == frame

    def test_skip_frame(self):
        frames = [Frame(b"1", 0), Frame(b"2", 0)]
        msg = ClientMessage.create_for_decode(frames)
        inbound = InboundMessage(msg)

        inbound.skip_frame()
        assert inbound.next_frame() == frames[1]

    def test_message_property(self):
        msg = ClientMessage.create_for_decode([Frame(b"1", 0)])
        inbound = InboundMessage(msg)
        assert inbound.message is msg


class TestFixSizedTypesCodec:
    """Tests for FixSizedTypesCodec class."""

    def test_encode_decode_byte(self):
        buffer = bytearray(10)
        offset = FixSizedTypesCodec.encode_byte(buffer, 2, -100)
        assert offset == 3

        value, new_offset = FixSizedTypesCodec.decode_byte(buffer, 2)
        assert value == -100
        assert new_offset == 3

    def test_encode_decode_boolean(self):
        buffer = bytearray(10)
        offset = FixSizedTypesCodec.encode_boolean(buffer, 0, True)
        assert offset == 1

        value, _ = FixSizedTypesCodec.decode_boolean(buffer, 0)
        assert value is True

        FixSizedTypesCodec.encode_boolean(buffer, 1, False)
        value, _ = FixSizedTypesCodec.decode_boolean(buffer, 1)
        assert value is False

    def test_encode_decode_short(self):
        buffer = bytearray(10)
        offset = FixSizedTypesCodec.encode_short(buffer, 0, -32000)
        assert offset == 2

        value, new_offset = FixSizedTypesCodec.decode_short(buffer, 0)
        assert value == -32000
        assert new_offset == 2

    def test_encode_decode_int(self):
        buffer = bytearray(10)
        offset = FixSizedTypesCodec.encode_int(buffer, 0, -2147483648)
        assert offset == 4

        value, new_offset = FixSizedTypesCodec.decode_int(buffer, 0)
        assert value == -2147483648
        assert new_offset == 4

    def test_encode_decode_long(self):
        buffer = bytearray(16)
        offset = FixSizedTypesCodec.encode_long(buffer, 0, -9223372036854775808)
        assert offset == 8

        value, new_offset = FixSizedTypesCodec.decode_long(buffer, 0)
        assert value == -9223372036854775808
        assert new_offset == 8

    def test_encode_decode_float(self):
        buffer = bytearray(10)
        offset = FixSizedTypesCodec.encode_float(buffer, 0, 3.14)
        assert offset == 4

        value, new_offset = FixSizedTypesCodec.decode_float(buffer, 0)
        assert abs(value - 3.14) < 0.001
        assert new_offset == 4

    def test_encode_decode_double(self):
        buffer = bytearray(16)
        offset = FixSizedTypesCodec.encode_double(buffer, 0, 3.141592653589793)
        assert offset == 8

        value, new_offset = FixSizedTypesCodec.decode_double(buffer, 0)
        assert abs(value - 3.141592653589793) < 1e-10
        assert new_offset == 8

    def test_encode_decode_uuid(self):
        buffer = bytearray(20)
        test_uuid = uuid.uuid4()
        offset = FixSizedTypesCodec.encode_uuid(buffer, 0, test_uuid)
        assert offset == UUID_SIZE

        value, new_offset = FixSizedTypesCodec.decode_uuid(buffer, 0)
        assert value == test_uuid
        assert new_offset == UUID_SIZE

    def test_encode_decode_null_uuid(self):
        buffer = bytearray(20)
        offset = FixSizedTypesCodec.encode_uuid(buffer, 0, None)
        assert offset == UUID_SIZE

        value, new_offset = FixSizedTypesCodec.decode_uuid(buffer, 0)
        assert value is None
        assert new_offset == UUID_SIZE


class TestLeBytes:
    """Tests for LeBytes utility class."""

    def test_int_to_bytes(self):
        result = LeBytes.int_to_bytes(12345)
        assert len(result) == 4
        assert LeBytes.bytes_to_int(result) == 12345

    def test_int_to_bytes_negative(self):
        result = LeBytes.int_to_bytes(-12345)
        assert LeBytes.bytes_to_int(result) == -12345

    def test_long_to_bytes(self):
        result = LeBytes.long_to_bytes(123456789012345)
        assert len(result) == 8
        assert LeBytes.bytes_to_long(result) == 123456789012345

    def test_long_to_bytes_negative(self):
        result = LeBytes.long_to_bytes(-123456789012345)
        assert LeBytes.bytes_to_long(result) == -123456789012345

    def test_bytes_to_int_with_offset(self):
        data = b"\x00\x00" + LeBytes.int_to_bytes(42) + b"\x00\x00"
        assert LeBytes.bytes_to_int(data, 2) == 42

    def test_bytes_to_long_with_offset(self):
        data = b"\x00\x00" + LeBytes.long_to_bytes(42) + b"\x00\x00"
        assert LeBytes.bytes_to_long(data, 2) == 42


class TestBuiltinCodecs:
    """Tests for builtin codec classes."""

    def test_string_codec_encode_decode(self):
        msg = ClientMessage.create_for_encode()
        BuiltinStringCodec.encode(msg, "hello world")

        msg.reset_read_index()
        result = BuiltinStringCodec.decode(msg)
        assert result == "hello world"

    def test_string_codec_unicode(self):
        msg = ClientMessage.create_for_encode()
        BuiltinStringCodec.encode(msg, "こんにちは")

        msg.reset_read_index()
        result = BuiltinStringCodec.decode(msg)
        assert result == "こんにちは"

    def test_string_codec_empty(self):
        msg = ClientMessage.create_for_encode()
        BuiltinStringCodec.encode(msg, "")

        msg.reset_read_index()
        result = BuiltinStringCodec.decode(msg)
        assert result == ""

    def test_byte_array_codec(self):
        msg = ClientMessage.create_for_encode()
        data = b"\x00\x01\x02\xff"
        ByteArrayCodec.encode(msg, data)

        msg.reset_read_index()
        result = ByteArrayCodec.decode(msg)
        assert result == data

    def test_data_codec(self):
        msg = ClientMessage.create_for_encode()
        data = b"serialized_data"
        DataCodec.encode(msg, data)

        msg.reset_read_index()
        result = DataCodec.decode(msg)
        assert result == data

    def test_data_codec_nullable(self):
        msg = ClientMessage.create_for_encode()
        DataCodec.encode_nullable(msg, None)

        msg.reset_read_index()
        result = DataCodec.decode_nullable(msg)
        assert result is None

    def test_long_codec(self):
        msg = ClientMessage.create_for_encode()
        LongCodec.encode(msg, 9876543210)

        msg.reset_read_index()
        result = LongCodec.decode(msg)
        assert result == 9876543210

    def test_integer_codec(self):
        msg = ClientMessage.create_for_encode()
        IntegerCodec.encode(msg, 42)

        msg.reset_read_index()
        result = IntegerCodec.decode(msg)
        assert result == 42

    def test_boolean_codec(self):
        msg = ClientMessage.create_for_encode()
        BooleanCodec.encode(msg, True)
        BooleanCodec.encode(msg, False)

        msg.reset_read_index()
        assert BooleanCodec.decode(msg) is True
        assert BooleanCodec.decode(msg) is False

    def test_uuid_codec(self):
        msg = ClientMessage.create_for_encode()
        test_uuid = uuid.uuid4()
        UUIDCodec.encode(msg, test_uuid)

        msg.reset_read_index()
        result = UUIDCodec.decode(msg)
        assert result == test_uuid

    def test_uuid_codec_nullable(self):
        msg = ClientMessage.create_for_encode()
        UUIDCodec.encode_nullable(msg, None)

        msg.reset_read_index()
        result = UUIDCodec.decode_nullable(msg)
        assert result is None

    def test_list_integer_codec(self):
        msg = ClientMessage.create_for_encode()
        items = [1, 2, 3, 4, 5]
        ListIntegerCodec.encode(msg, items)

        msg.reset_read_index()
        result = ListIntegerCodec.decode(msg)
        assert result == items

    def test_list_long_codec(self):
        msg = ClientMessage.create_for_encode()
        items = [100000000000, 200000000000, 300000000000]
        ListLongCodec.encode(msg, items)

        msg.reset_read_index()
        result = ListLongCodec.decode(msg)
        assert result == items

    def test_list_multi_frame_codec(self):
        msg = ClientMessage.create_for_encode()
        items = ["one", "two", "three"]
        ListMultiFrameCodec.encode(msg, items, BuiltinStringCodec.encode)

        msg.reset_read_index()
        result = ListMultiFrameCodec.decode(msg, BuiltinStringCodec.decode)
        assert result == items

    def test_list_multi_frame_codec_nullable(self):
        msg = ClientMessage.create_for_encode()
        ListMultiFrameCodec.encode_nullable(msg, None, BuiltinStringCodec.encode)

        msg.reset_read_index()
        result = ListMultiFrameCodec.decode_nullable(msg, BuiltinStringCodec.decode)
        assert result is None

    def test_entry_list_codec(self):
        msg = ClientMessage.create_for_encode()
        entries = [(b"key1", b"val1"), (b"key2", b"val2")]
        EntryListCodec.encode(msg, entries, ByteArrayCodec.encode, ByteArrayCodec.encode)

        msg.reset_read_index()
        result = EntryListCodec.decode(msg, ByteArrayCodec.decode, ByteArrayCodec.decode)
        assert result == entries

    def test_map_codec(self):
        msg = ClientMessage.create_for_encode()
        items = {"key1": "val1", "key2": "val2"}
        BuiltinMapCodec.encode(msg, items, BuiltinStringCodec.encode, BuiltinStringCodec.encode)

        msg.reset_read_index()
        result = BuiltinMapCodec.decode(msg, BuiltinStringCodec.decode, BuiltinStringCodec.decode)
        assert result == items

    def test_map_codec_nullable(self):
        msg = ClientMessage.create_for_encode()
        BuiltinMapCodec.encode_nullable(msg, None, BuiltinStringCodec.encode, BuiltinStringCodec.encode)

        msg.reset_read_index()
        result = BuiltinMapCodec.decode_nullable(msg, BuiltinStringCodec.decode, BuiltinStringCodec.decode)
        assert result is None


class TestCodecUtil:
    """Tests for CodecUtil class."""

    def test_fast_forward_to_end_frame(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(BEGIN_FRAME)
        msg.add_frame(Frame(b"inner1", 0))
        msg.add_frame(BEGIN_FRAME)
        msg.add_frame(Frame(b"nested", 0))
        msg.add_frame(END_FRAME)
        msg.add_frame(Frame(b"inner2", 0))
        msg.add_frame(END_FRAME)
        msg.add_frame(Frame(b"after", 0))

        msg.reset_read_index()
        msg.next_frame()
        CodecUtil.fast_forward_to_end_frame(msg)

        remaining = msg.next_frame()
        assert remaining.content == b"after"

    def test_encode_nullable_with_value(self):
        msg = ClientMessage.create_for_encode()
        CodecUtil.encode_nullable(msg, "test", BuiltinStringCodec.encode)

        msg.reset_read_index()
        frame = msg.next_frame()
        assert not frame.is_null_frame

    def test_encode_nullable_with_none(self):
        msg = ClientMessage.create_for_encode()
        CodecUtil.encode_nullable(msg, None, BuiltinStringCodec.encode)

        msg.reset_read_index()
        frame = msg.peek_next_frame()
        assert frame.is_null_frame

    def test_decode_nullable_with_value(self):
        msg = ClientMessage.create_for_encode()
        BuiltinStringCodec.encode(msg, "test")

        msg.reset_read_index()
        result = CodecUtil.decode_nullable(msg, BuiltinStringCodec.decode)
        assert result == "test"

    def test_decode_nullable_with_null(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(NULL_FRAME)

        msg.reset_read_index()
        result = CodecUtil.decode_nullable(msg, BuiltinStringCodec.decode)
        assert result is None


class TestMapCodec:
    """Tests for MapCodec protocol messages."""

    def test_encode_put_request(self):
        msg = MapCodec.encode_put_request("test-map", b"key", b"value", 1, 0)

        assert msg.get_message_type() == MAP_PUT
        assert len(msg) == 4

    def test_encode_get_request(self):
        msg = MapCodec.encode_get_request("test-map", b"key", 1)

        assert msg.get_message_type() == MAP_GET
        assert len(msg) == 3

    def test_encode_remove_request(self):
        msg = MapCodec.encode_remove_request("test-map", b"key", 1)

        assert msg.get_message_type() == MAP_REMOVE

    def test_encode_contains_key_request(self):
        msg = MapCodec.encode_contains_key_request("test-map", b"key", 1)

        assert msg.get_message_type() == MAP_CONTAINS_KEY

    def test_encode_size_request(self):
        msg = MapCodec.encode_size_request("test-map")

        assert msg.get_message_type() == MAP_SIZE

    def test_encode_clear_request(self):
        msg = MapCodec.encode_clear_request("test-map")

        assert msg.get_message_type() == MAP_CLEAR

    def test_encode_key_set_request(self):
        msg = MapCodec.encode_key_set_request("test-map")

        assert msg.get_message_type() == MAP_KEY_SET

    def test_encode_values_request(self):
        msg = MapCodec.encode_values_request("test-map")

        assert msg.get_message_type() == MAP_VALUES

    def test_encode_entry_set_request(self):
        msg = MapCodec.encode_entry_set_request("test-map")

        assert msg.get_message_type() == MAP_ENTRY_SET

    def test_decode_put_response_with_value(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytearray(RESPONSE_HEADER_SIZE), 0))
        msg.add_frame(Frame(b"old_value", 0))

        msg.reset_read_index()
        result = MapCodec.decode_put_response(msg)
        assert result == b"old_value"

    def test_decode_put_response_null(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytearray(RESPONSE_HEADER_SIZE), 0))
        msg.add_frame(NULL_FRAME)

        msg.reset_read_index()
        result = MapCodec.decode_put_response(msg)
        assert result is None

    def test_decode_contains_key_response(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + 1)
        buffer[RESPONSE_HEADER_SIZE] = 1
        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        result = MapCodec.decode_contains_key_response(msg)
        assert result is True

    def test_decode_size_response(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + 4)
        struct.pack_into("<i", buffer, RESPONSE_HEADER_SIZE, 42)
        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        result = MapCodec.decode_size_response(msg)
        assert result == 42


class TestQueueCodec:
    """Tests for QueueCodec protocol messages."""

    def test_encode_offer_request(self):
        msg = QueueCodec.encode_offer_request("test-queue", b"item", 1000)

        assert msg.get_message_type() == QUEUE_OFFER

    def test_encode_poll_request(self):
        msg = QueueCodec.encode_poll_request("test-queue", 1000)

        assert msg.get_message_type() == QUEUE_POLL

    def test_encode_size_request(self):
        msg = QueueCodec.encode_size_request("test-queue")

        assert msg.get_message_type() == QUEUE_SIZE

    def test_encode_peek_request(self):
        msg = QueueCodec.encode_peek_request("test-queue")

        assert msg.get_message_type() == QUEUE_PEEK

    def test_encode_clear_request(self):
        msg = QueueCodec.encode_clear_request("test-queue")

        assert msg.get_message_type() == QUEUE_CLEAR

    def test_decode_offer_response_true(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + 1)
        buffer[RESPONSE_HEADER_SIZE] = 1
        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        result = QueueCodec.decode_offer_response(msg)
        assert result is True

    def test_decode_offer_response_false(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + 1)
        buffer[RESPONSE_HEADER_SIZE] = 0
        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        result = QueueCodec.decode_offer_response(msg)
        assert result is False

    def test_decode_poll_response(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytearray(RESPONSE_HEADER_SIZE), 0))
        msg.add_frame(Frame(b"polled_item", 0))

        msg.reset_read_index()
        result = QueueCodec.decode_poll_response(msg)
        assert result == b"polled_item"

    def test_decode_size_response(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + 4)
        struct.pack_into("<i", buffer, RESPONSE_HEADER_SIZE, 10)
        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        result = QueueCodec.decode_size_response(msg)
        assert result == 10


class TestListCodec:
    """Tests for ListCodec protocol messages."""

    def test_encode_add_request(self):
        msg = ListCodec.encode_add_request("test-list", b"item")

        assert msg.get_message_type() == LIST_ADD

    def test_encode_get_request(self):
        msg = ListCodec.encode_get_request("test-list", 5)

        assert msg.get_message_type() == LIST_GET

    def test_encode_remove_request(self):
        msg = ListCodec.encode_remove_request("test-list", 3)

        assert msg.get_message_type() == LIST_REMOVE

    def test_encode_size_request(self):
        msg = ListCodec.encode_size_request("test-list")

        assert msg.get_message_type() == LIST_SIZE

    def test_encode_clear_request(self):
        msg = ListCodec.encode_clear_request("test-list")

        assert msg.get_message_type() == LIST_CLEAR

    def test_encode_contains_request(self):
        msg = ListCodec.encode_contains_request("test-list", b"item")

        assert msg.get_message_type() == LIST_CONTAINS

    def test_decode_add_response(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + 1)
        buffer[RESPONSE_HEADER_SIZE] = 1
        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        result = ListCodec.decode_add_response(msg)
        assert result is True

    def test_decode_get_response(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytearray(RESPONSE_HEADER_SIZE), 0))
        msg.add_frame(Frame(b"list_item", 0))

        msg.reset_read_index()
        result = ListCodec.decode_get_response(msg)
        assert result == b"list_item"

    def test_decode_size_response(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + 4)
        struct.pack_into("<i", buffer, RESPONSE_HEADER_SIZE, 25)
        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        result = ListCodec.decode_size_response(msg)
        assert result == 25


class TestSetCodec:
    """Tests for SetCodec protocol messages."""

    def test_encode_add_request(self):
        msg = SetCodec.encode_add_request("test-set", b"item")

        assert msg.get_message_type() == SET_ADD

    def test_encode_remove_request(self):
        msg = SetCodec.encode_remove_request("test-set", b"item")

        assert msg.get_message_type() == SET_REMOVE

    def test_encode_size_request(self):
        msg = SetCodec.encode_size_request("test-set")

        assert msg.get_message_type() == SET_SIZE

    def test_encode_contains_request(self):
        msg = SetCodec.encode_contains_request("test-set", b"item")

        assert msg.get_message_type() == SET_CONTAINS

    def test_encode_clear_request(self):
        msg = SetCodec.encode_clear_request("test-set")

        assert msg.get_message_type() == SET_CLEAR

    def test_encode_get_all_request(self):
        msg = SetCodec.encode_get_all_request("test-set")

        assert msg.get_message_type() == SET_GET_ALL

    def test_decode_add_response(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + 1)
        buffer[RESPONSE_HEADER_SIZE] = 1
        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        result = SetCodec.decode_add_response(msg)
        assert result is True

    def test_decode_remove_response(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + 1)
        buffer[RESPONSE_HEADER_SIZE] = 0
        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        result = SetCodec.decode_remove_response(msg)
        assert result is False

    def test_decode_size_response(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + 4)
        struct.pack_into("<i", buffer, RESPONSE_HEADER_SIZE, 100)
        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        result = SetCodec.decode_size_response(msg)
        assert result == 100

    def test_decode_contains_response(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + 1)
        buffer[RESPONSE_HEADER_SIZE] = 1
        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        result = SetCodec.decode_contains_response(msg)
        assert result is True


class TestTopicCodec:
    """Tests for TopicCodec protocol messages."""

    def test_encode_publish_request(self):
        msg = TopicCodec.encode_publish_request("test-topic", b"message")

        assert msg.get_message_type() == TOPIC_PUBLISH

    def test_encode_add_listener_request(self):
        msg = TopicCodec.encode_add_listener_request("test-topic", True)

        assert msg.get_message_type() == TOPIC_ADD_LISTENER

    def test_encode_remove_listener_request(self):
        test_uuid = uuid.uuid4()
        msg = TopicCodec.encode_remove_listener_request("test-topic", test_uuid)

        assert msg.get_message_type() == TOPIC_REMOVE_LISTENER

    def test_decode_add_listener_response(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + UUID_SIZE)
        test_uuid = uuid.uuid4()
        FixSizedTypesCodec.encode_uuid(buffer, RESPONSE_HEADER_SIZE, test_uuid)
        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        result = TopicCodec.decode_add_listener_response(msg)
        assert result == test_uuid

    def test_decode_remove_listener_response(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + 1)
        buffer[RESPONSE_HEADER_SIZE] = 1
        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        result = TopicCodec.decode_remove_listener_response(msg)
        assert result is True


class TestIdBatch:
    """Tests for IdBatch class."""

    def test_id_batch_properties(self):
        batch = IdBatch(1000, 10, 5)

        assert batch.base == 1000
        assert batch.increment == 10
        assert batch.batch_size == 5

    def test_next_id(self):
        batch = IdBatch(100, 2, 3)

        assert batch.next_id() == 100
        assert batch.next_id() == 102
        assert batch.next_id() == 104

    def test_next_id_exhausted(self):
        batch = IdBatch(100, 1, 2)

        batch.next_id()
        batch.next_id()

        with pytest.raises(StopIteration):
            batch.next_id()

    def test_remaining(self):
        batch = IdBatch(100, 1, 5)

        assert batch.remaining() == 5
        batch.next_id()
        assert batch.remaining() == 4
        batch.next_id()
        batch.next_id()
        assert batch.remaining() == 2

    def test_is_exhausted(self):
        batch = IdBatch(100, 1, 2)

        assert not batch.is_exhausted()
        batch.next_id()
        assert not batch.is_exhausted()
        batch.next_id()
        assert batch.is_exhausted()


class TestCardinalityEstimatorCodec:
    """Tests for CardinalityEstimatorCodec."""

    def test_encode_add_request(self):
        msg = CardinalityEstimatorCodec.encode_add_request("test-estimator", b"data")

        assert msg.get_message_type() == 0x190100

    def test_encode_estimate_request(self):
        msg = CardinalityEstimatorCodec.encode_estimate_request("test-estimator")

        assert msg.get_message_type() == 0x190200

    def test_decode_estimate_response(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + 8)
        struct.pack_into("<q", buffer, RESPONSE_HEADER_SIZE, 12345678)
        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        result = CardinalityEstimatorCodec.decode_estimate_response(msg)
        assert result == 12345678


class TestClientCodec:
    """Tests for ClientCodec protocol messages."""

    def test_encode_authentication_request(self):
        client_uuid = uuid.uuid4()
        msg = ClientCodec.encode_authentication_request(
            cluster_name="dev",
            username="admin",
            password="secret",
            client_uuid=client_uuid,
            client_type="PY",
            serialization_version=1,
            client_hazelcast_version="5.0.0",
            client_name="test-client",
            labels=["label1", "label2"],
        )

        assert msg.get_message_type() == CLIENT_AUTHENTICATION
        assert len(msg) >= 1

    def test_encode_authentication_request_null_credentials(self):
        client_uuid = uuid.uuid4()
        msg = ClientCodec.encode_authentication_request(
            cluster_name="dev",
            username=None,
            password=None,
            client_uuid=client_uuid,
            client_type="PY",
            serialization_version=1,
            client_hazelcast_version="5.0.0",
            client_name="test-client",
            labels=[],
        )

        assert msg.get_message_type() == CLIENT_AUTHENTICATION

    def test_decode_authentication_response_success(self):
        member_uuid = uuid.uuid4()
        cluster_id = uuid.uuid4()

        buffer = bytearray(RESPONSE_HEADER_SIZE + BYTE_SIZE + UUID_SIZE + BYTE_SIZE + INT_SIZE + UUID_SIZE + BOOLEAN_SIZE)
        offset = RESPONSE_HEADER_SIZE
        struct.pack_into("<B", buffer, offset, AUTH_STATUS_AUTHENTICATED)
        offset += BYTE_SIZE
        FixSizedTypesCodec.encode_uuid(buffer, offset, member_uuid)
        offset += UUID_SIZE
        struct.pack_into("<B", buffer, offset, 1)
        offset += BYTE_SIZE
        struct.pack_into("<i", buffer, offset, 271)
        offset += INT_SIZE
        FixSizedTypesCodec.encode_uuid(buffer, offset, cluster_id)
        offset += UUID_SIZE
        struct.pack_into("<B", buffer, offset, 1)

        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        status, resp_member_uuid, ser_ver, part_count, resp_cluster_id, failover = (
            ClientCodec.decode_authentication_response(msg)
        )

        assert status == AUTH_STATUS_AUTHENTICATED
        assert resp_member_uuid == member_uuid
        assert ser_ver == 1
        assert part_count == 271
        assert resp_cluster_id == cluster_id
        assert failover is True

    def test_decode_authentication_response_failed(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + BYTE_SIZE)
        struct.pack_into("<B", buffer, RESPONSE_HEADER_SIZE, AUTH_STATUS_CREDENTIALS_FAILED)

        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        status, member_uuid, ser_ver, part_count, cluster_id, failover = (
            ClientCodec.decode_authentication_response(msg)
        )

        assert status == AUTH_STATUS_CREDENTIALS_FAILED

    def test_decode_authentication_response_empty(self):
        msg = ClientMessage.create_for_encode()

        status, member_uuid, ser_ver, part_count, cluster_id, failover = (
            ClientCodec.decode_authentication_response(msg)
        )

        assert status == AUTH_STATUS_CREDENTIALS_FAILED

    def test_encode_authentication_custom_request(self):
        client_uuid = uuid.uuid4()
        credentials = b"custom_token_data"

        msg = ClientCodec.encode_authentication_custom_request(
            cluster_name="dev",
            credentials=credentials,
            client_uuid=client_uuid,
            client_type="PY",
            serialization_version=1,
            client_hazelcast_version="5.0.0",
            client_name="test-client",
            labels=["secure"],
        )

        assert msg.get_message_type() == CLIENT_AUTHENTICATION_CUSTOM
        assert len(msg) >= 1

    def test_decode_authentication_custom_response(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + BYTE_SIZE)
        struct.pack_into("<B", buffer, RESPONSE_HEADER_SIZE, AUTH_STATUS_AUTHENTICATED)

        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        status, _, _, _, _, _ = ClientCodec.decode_authentication_custom_response(msg)
        assert status == AUTH_STATUS_AUTHENTICATED

    def test_encode_add_cluster_view_listener_request(self):
        msg = ClientCodec.encode_add_cluster_view_listener_request()

        assert msg.get_message_type() == CLIENT_ADD_CLUSTER_VIEW_LISTENER
        assert len(msg) == 1

    def test_encode_create_proxy_request(self):
        msg = ClientCodec.encode_create_proxy_request("my-map", "hz:impl:mapService")

        assert msg.get_message_type() == CLIENT_CREATE_PROXY
        assert len(msg) == 3

    def test_encode_destroy_proxy_request(self):
        msg = ClientCodec.encode_destroy_proxy_request("my-map", "hz:impl:mapService")

        assert msg.get_message_type() == CLIENT_DESTROY_PROXY
        assert len(msg) == 3

    def test_encode_get_distributed_objects_request(self):
        msg = ClientCodec.encode_get_distributed_objects_request()

        assert msg.get_message_type() == CLIENT_GET_DISTRIBUTED_OBJECTS
        assert len(msg) == 1

    def test_decode_get_distributed_objects_response_with_items(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytearray(RESPONSE_HEADER_SIZE), 0))
        msg.add_frame(BEGIN_FRAME)
        msg.add_frame(Frame(b"hz:impl:mapService", 0))
        msg.add_frame(Frame(b"my-map", 0))
        msg.add_frame(Frame(b"hz:impl:queueService", 0))
        msg.add_frame(Frame(b"my-queue", 0))
        msg.add_frame(END_FRAME)

        msg.reset_read_index()
        result = ClientCodec.decode_get_distributed_objects_response(msg)

        assert len(result) == 2
        assert result[0] == ("hz:impl:mapService", "my-map")
        assert result[1] == ("hz:impl:queueService", "my-queue")

    def test_decode_get_distributed_objects_response_empty(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytearray(RESPONSE_HEADER_SIZE), 0))

        msg.reset_read_index()
        result = ClientCodec.decode_get_distributed_objects_response(msg)

        assert result == []

    def test_encode_ping_request(self):
        msg = ClientCodec.encode_ping_request()

        assert msg.get_message_type() == CLIENT_PING
        assert len(msg) == 1

    def test_encode_get_partitions_request(self):
        msg = ClientCodec.encode_get_partitions_request()

        assert msg.get_message_type() == CLIENT_GET_PARTITIONS
        assert len(msg) == 1

    def test_decode_get_partitions_response_with_data(self):
        member_uuid = uuid.uuid4()

        buffer = bytearray(RESPONSE_HEADER_SIZE + INT_SIZE)
        struct.pack_into("<i", buffer, RESPONSE_HEADER_SIZE, 42)

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytes(buffer), 0))
        msg.add_frame(BEGIN_FRAME)

        uuid_buffer = bytearray(UUID_SIZE)
        FixSizedTypesCodec.encode_uuid(uuid_buffer, 0, member_uuid)
        msg.add_frame(Frame(bytes(uuid_buffer), 0))

        partition_buffer = bytearray(INT_SIZE + 3 * INT_SIZE)
        struct.pack_into("<i", partition_buffer, 0, 3)
        struct.pack_into("<i", partition_buffer, INT_SIZE, 0)
        struct.pack_into("<i", partition_buffer, 2 * INT_SIZE, 1)
        struct.pack_into("<i", partition_buffer, 3 * INT_SIZE, 2)
        msg.add_frame(Frame(bytes(partition_buffer), 0))

        msg.add_frame(END_FRAME)

        msg.reset_read_index()
        partitions, state_version = ClientCodec.decode_get_partitions_response(msg)

        assert state_version == 42
        assert len(partitions) == 1
        assert partitions[0][0] == member_uuid
        assert partitions[0][1] == [0, 1, 2]

    def test_decode_get_partitions_response_empty(self):
        buffer = bytearray(RESPONSE_HEADER_SIZE + INT_SIZE)
        struct.pack_into("<i", buffer, RESPONSE_HEADER_SIZE, 0)

        msg = ClientMessage.create_for_decode([Frame(bytes(buffer), 0)])

        partitions, state_version = ClientCodec.decode_get_partitions_response(msg)

        assert state_version == 0
        assert partitions == []

    def test_encode_send_schema_request(self):
        schema_data = b"compact_schema_binary_data"
        msg = ClientCodec.encode_send_schema_request(schema_data)

        assert msg.get_message_type() == CLIENT_SEND_SCHEMA
        assert len(msg) == 2

    def test_decode_send_schema_response_with_uuids(self):
        uuid1 = uuid.uuid4()
        uuid2 = uuid.uuid4()

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytearray(RESPONSE_HEADER_SIZE), 0))
        msg.add_frame(BEGIN_FRAME)

        uuid_buffer1 = bytearray(UUID_SIZE)
        FixSizedTypesCodec.encode_uuid(uuid_buffer1, 0, uuid1)
        msg.add_frame(Frame(bytes(uuid_buffer1), 0))

        uuid_buffer2 = bytearray(UUID_SIZE)
        FixSizedTypesCodec.encode_uuid(uuid_buffer2, 0, uuid2)
        msg.add_frame(Frame(bytes(uuid_buffer2), 0))

        msg.add_frame(END_FRAME)

        msg.reset_read_index()
        result = ClientCodec.decode_send_schema_response(msg)

        assert len(result) == 2
        assert uuid1 in result
        assert uuid2 in result

    def test_decode_send_schema_response_empty(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytearray(RESPONSE_HEADER_SIZE), 0))

        msg.reset_read_index()
        result = ClientCodec.decode_send_schema_response(msg)

        assert result == []

    def test_encode_fetch_schema_request(self):
        schema_id = 123456789
        msg = ClientCodec.encode_fetch_schema_request(schema_id)

        assert msg.get_message_type() == CLIENT_FETCH_SCHEMA
        assert len(msg) == 1

        msg.reset_read_index()
        frame = msg.next_frame()
        decoded_id = struct.unpack_from("<q", frame.content, REQUEST_HEADER_SIZE)[0]
        assert decoded_id == schema_id

    def test_decode_fetch_schema_response_with_data(self):
        schema_data = b"fetched_schema_binary"

        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytearray(RESPONSE_HEADER_SIZE), 0))
        msg.add_frame(Frame(schema_data, 0))

        msg.reset_read_index()
        result = ClientCodec.decode_fetch_schema_response(msg)

        assert result == schema_data

    def test_decode_fetch_schema_response_null(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytearray(RESPONSE_HEADER_SIZE), 0))
        msg.add_frame(NULL_FRAME)

        msg.reset_read_index()
        result = ClientCodec.decode_fetch_schema_response(msg)

        assert result is None

    def test_decode_fetch_schema_response_empty(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(bytearray(RESPONSE_HEADER_SIZE), 0))

        msg.reset_read_index()
        result = ClientCodec.decode_fetch_schema_response(msg)

        assert result is None


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_decode_empty_message(self):
        msg = ClientMessage.create_for_encode()
        msg.reset_read_index()

        frame = msg.next_frame()
        assert frame is None

    def test_decode_from_incomplete_bytes(self):
        data = b"\x00\x00"
        msg = ClientMessage.from_bytes(data)
        assert len(msg) == 0

    def test_string_codec_decode_empty_frame(self):
        msg = ClientMessage.create_for_encode()
        msg.reset_read_index()

        result = BuiltinStringCodec.decode(msg)
        assert result == ""

    def test_byte_array_codec_decode_empty_frame(self):
        msg = ClientMessage.create_for_encode()
        msg.reset_read_index()

        result = ByteArrayCodec.decode(msg)
        assert result == b""

    def test_list_integer_codec_empty(self):
        msg = ClientMessage.create_for_encode()
        ListIntegerCodec.encode(msg, [])

        msg.reset_read_index()
        result = ListIntegerCodec.decode(msg)
        assert result == []

    def test_list_long_codec_empty(self):
        msg = ClientMessage.create_for_encode()
        ListLongCodec.encode(msg, [])

        msg.reset_read_index()
        result = ListLongCodec.decode(msg)
        assert result == []

    def test_integer_codec_short_frame(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(b"\x00", 0))

        msg.reset_read_index()
        result = IntegerCodec.decode(msg)
        assert result == 0

    def test_long_codec_short_frame(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(b"\x00", 0))

        msg.reset_read_index()
        result = LongCodec.decode(msg)
        assert result == 0

    def test_boolean_codec_short_frame(self):
        msg = ClientMessage.create_for_encode()
        msg.reset_read_index()

        result = BooleanCodec.decode(msg)
        assert result is False

    def test_uuid_codec_short_frame(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(b"\x00", 0))

        msg.reset_read_index()
        result = UUIDCodec.decode(msg)
        assert result is None

    def test_map_codec_response_short_frame(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(b"short", 0))

        msg.reset_read_index()
        result = MapCodec.decode_size_response(msg)
        assert result == 0

    def test_queue_codec_response_short_frame(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(b"short", 0))

        msg.reset_read_index()
        result = QueueCodec.decode_offer_response(msg)
        assert result is False

    def test_message_fragmentation_id(self):
        content = bytearray(22)
        struct.pack_into("<q", content, 0, 987654321)
        msg = ClientMessage.create_for_decode([Frame(bytes(content), BEGIN_FLAG)])

        assert msg.get_fragmentation_id() == 987654321

    def test_message_merge(self):
        msg1 = ClientMessage.create_for_decode([
            Frame(b"frame1", BEGIN_FLAG),
            Frame(b"frame2", 0)
        ])
        msg2 = ClientMessage.create_for_decode([
            Frame(b"frag_header", 0),
            Frame(b"frame3", 0)
        ])

        msg1.merge(msg2)
        assert len(msg1) == 3
        assert msg1.frames[2].content == b"frame3"

    def test_message_drop_fragmentation_frame(self):
        msg = ClientMessage.create_for_decode([
            Frame(b"frag_header", 0),
            Frame(b"actual_data", 0)
        ])

        msg.drop_fragmentation_frame()
        assert len(msg) == 1
        assert msg.frames[0].content == b"actual_data"
