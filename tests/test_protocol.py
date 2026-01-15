"""Unit tests for Hazelcast protocol layer."""

import uuid

import pytest

from hazelcast.protocol.codec import (
    FixSizedTypesCodec,
    LeBytes,
    BYTE_SIZE,
    INT_SIZE,
    LONG_SIZE,
    DOUBLE_SIZE,
)
from hazelcast.protocol.client_message import (
    ClientMessage,
    Frame,
    NULL_FRAME,
    BEGIN_FRAME,
    END_FRAME,
    BEGIN_FLAG,
    END_FLAG,
    UNFRAGMENTED_FLAG,
    SIZE_OF_FRAME_LENGTH_AND_FLAGS,
)
from hazelcast.protocol.builtin_codecs import (
    CodecUtil,
    StringCodec,
    ByteArrayCodec,
    DataCodec,
    LongCodec,
    IntegerCodec,
    ListMultiFrameCodec,
    ListIntegerCodec,
    ListLongCodec,
    EntryListCodec,
)
from hazelcast.invocation import Invocation, InvocationService


class TestFixSizedTypesCodec:
    """Tests for FixSizedTypesCodec."""

    def test_encode_decode_byte(self):
        buffer = bytearray(BYTE_SIZE)
        FixSizedTypesCodec.encode_byte(buffer, 0, 42)
        value, _ = FixSizedTypesCodec.decode_byte(bytes(buffer), 0)
        assert value == 42

    def test_encode_decode_byte_negative(self):
        buffer = bytearray(BYTE_SIZE)
        FixSizedTypesCodec.encode_byte(buffer, 0, -128)
        value, _ = FixSizedTypesCodec.decode_byte(bytes(buffer), 0)
        assert value == -128

    def test_encode_decode_boolean_true(self):
        buffer = bytearray(1)
        FixSizedTypesCodec.encode_boolean(buffer, 0, True)
        value, _ = FixSizedTypesCodec.decode_boolean(bytes(buffer), 0)
        assert value is True

    def test_encode_decode_boolean_false(self):
        buffer = bytearray(1)
        FixSizedTypesCodec.encode_boolean(buffer, 0, False)
        value, _ = FixSizedTypesCodec.decode_boolean(bytes(buffer), 0)
        assert value is False

    def test_encode_decode_short(self):
        buffer = bytearray(2)
        FixSizedTypesCodec.encode_short(buffer, 0, 12345)
        value, _ = FixSizedTypesCodec.decode_short(bytes(buffer), 0)
        assert value == 12345

    def test_encode_decode_short_negative(self):
        buffer = bytearray(2)
        FixSizedTypesCodec.encode_short(buffer, 0, -32768)
        value, _ = FixSizedTypesCodec.decode_short(bytes(buffer), 0)
        assert value == -32768

    def test_encode_decode_int(self):
        buffer = bytearray(INT_SIZE)
        FixSizedTypesCodec.encode_int(buffer, 0, 123456789)
        value, _ = FixSizedTypesCodec.decode_int(bytes(buffer), 0)
        assert value == 123456789

    def test_encode_decode_int_negative(self):
        buffer = bytearray(INT_SIZE)
        FixSizedTypesCodec.encode_int(buffer, 0, -2147483648)
        value, _ = FixSizedTypesCodec.decode_int(bytes(buffer), 0)
        assert value == -2147483648

    def test_encode_decode_long(self):
        buffer = bytearray(LONG_SIZE)
        FixSizedTypesCodec.encode_long(buffer, 0, 9223372036854775807)
        value, _ = FixSizedTypesCodec.decode_long(bytes(buffer), 0)
        assert value == 9223372036854775807

    def test_encode_decode_long_negative(self):
        buffer = bytearray(LONG_SIZE)
        FixSizedTypesCodec.encode_long(buffer, 0, -9223372036854775808)
        value, _ = FixSizedTypesCodec.decode_long(bytes(buffer), 0)
        assert value == -9223372036854775808

    def test_encode_decode_float(self):
        buffer = bytearray(4)
        FixSizedTypesCodec.encode_float(buffer, 0, 3.14)
        value, _ = FixSizedTypesCodec.decode_float(bytes(buffer), 0)
        assert abs(value - 3.14) < 0.001

    def test_encode_decode_double(self):
        buffer = bytearray(DOUBLE_SIZE)
        FixSizedTypesCodec.encode_double(buffer, 0, 3.141592653589793)
        value, _ = FixSizedTypesCodec.decode_double(bytes(buffer), 0)
        assert value == 3.141592653589793

    def test_encode_decode_uuid(self):
        test_uuid = uuid.uuid4()
        buffer = bytearray(17)
        FixSizedTypesCodec.encode_uuid(buffer, 0, test_uuid)
        decoded, _ = FixSizedTypesCodec.decode_uuid(bytes(buffer), 0)
        assert decoded == test_uuid

    def test_encode_decode_uuid_none(self):
        buffer = bytearray(17)
        FixSizedTypesCodec.encode_uuid(buffer, 0, None)
        decoded, _ = FixSizedTypesCodec.decode_uuid(bytes(buffer), 0)
        assert decoded is None

    def test_multiple_values_at_offsets(self):
        buffer = bytearray(INT_SIZE + LONG_SIZE + DOUBLE_SIZE)
        offset = 0
        offset = FixSizedTypesCodec.encode_int(buffer, offset, 42)
        offset = FixSizedTypesCodec.encode_long(buffer, offset, 123456789)
        FixSizedTypesCodec.encode_double(buffer, offset, 99.99)

        data = bytes(buffer)
        val1, off1 = FixSizedTypesCodec.decode_int(data, 0)
        val2, off2 = FixSizedTypesCodec.decode_long(data, off1)
        val3, _ = FixSizedTypesCodec.decode_double(data, off2)

        assert val1 == 42
        assert val2 == 123456789
        assert val3 == 99.99


class TestLeBytes:
    """Tests for LeBytes utility."""

    def test_int_to_bytes(self):
        data = LeBytes.int_to_bytes(12345)
        assert len(data) == 4
        assert LeBytes.bytes_to_int(data) == 12345

    def test_long_to_bytes(self):
        data = LeBytes.long_to_bytes(123456789012345)
        assert len(data) == 8
        assert LeBytes.bytes_to_long(data) == 123456789012345


class TestFrame:
    """Tests for Frame class."""

    def test_frame_creation(self):
        frame = Frame(b"hello", BEGIN_FLAG)
        assert frame.content == b"hello"
        assert frame.flags == BEGIN_FLAG

    def test_frame_is_begin(self):
        frame = Frame(b"", BEGIN_FLAG)
        assert frame.is_begin_frame is True
        assert frame.is_end_frame is False

    def test_frame_is_end(self):
        frame = Frame(b"", END_FLAG)
        assert frame.is_begin_frame is False
        assert frame.is_end_frame is True

    def test_frame_is_null(self):
        assert NULL_FRAME.is_null_frame is True

    def test_frame_copy_with_new_flags(self):
        frame = Frame(b"test", BEGIN_FLAG)
        copied = frame.copy_with_new_flags(END_FLAG)
        assert copied.content == b"test"
        assert copied.flags == END_FLAG

    def test_frame_length(self):
        frame = Frame(b"hello world")
        assert len(frame) == 11

    def test_frame_equality(self):
        f1 = Frame(b"test", BEGIN_FLAG)
        f2 = Frame(b"test", BEGIN_FLAG)
        f3 = Frame(b"test", END_FLAG)
        assert f1 == f2
        assert f1 != f3


class TestClientMessage:
    """Tests for ClientMessage class."""

    def test_create_empty_message(self):
        msg = ClientMessage.create_for_encode()
        assert len(msg) == 0

    def test_add_frame(self):
        msg = ClientMessage.create_for_encode()
        msg.add_frame(Frame(b"test"))
        assert len(msg) == 1

    def test_message_type(self):
        initial = ClientMessage.create_initial_frame(0)
        msg = ClientMessage([initial])
        msg.set_message_type(0x000100)
        assert msg.get_message_type() == 0x000100

    def test_correlation_id(self):
        initial = ClientMessage.create_initial_frame(0)
        msg = ClientMessage([initial])
        msg.set_correlation_id(12345)
        assert msg.get_correlation_id() == 12345

    def test_partition_id(self):
        initial = ClientMessage.create_initial_frame(0)
        msg = ClientMessage([initial])
        msg.set_partition_id(42)
        assert msg.get_partition_id() == 42

    def test_frame_iteration(self):
        msg = ClientMessage(
            [
                Frame(b"one"),
                Frame(b"two"),
                Frame(b"three"),
            ]
        )

        assert msg.has_next_frame()
        assert msg.next_frame().content == b"one"
        assert msg.next_frame().content == b"two"
        assert msg.peek_next_frame().content == b"three"
        assert msg.next_frame().content == b"three"
        assert not msg.has_next_frame()

    def test_skip_frame(self):
        msg = ClientMessage([Frame(b"one"), Frame(b"two")])
        msg.skip_frame()
        assert msg.next_frame().content == b"two"

    def test_reset_read_index(self):
        msg = ClientMessage([Frame(b"test")])
        msg.next_frame()
        assert not msg.has_next_frame()
        msg.reset_read_index()
        assert msg.has_next_frame()

    def test_to_bytes_and_from_bytes(self):
        original = ClientMessage.create_for_encode()
        initial = ClientMessage.create_initial_frame(0, UNFRAGMENTED_FLAG)
        original.add_frame(initial)
        original.set_message_type(256)
        original.set_correlation_id(999)
        original.set_partition_id(5)
        original.add_frame(Frame(b"payload"))

        data = original.to_bytes()
        restored = ClientMessage.from_bytes(data)

        assert restored.get_message_type() == 256
        assert restored.get_correlation_id() == 999
        assert restored.get_partition_id() == 5
        assert len(restored) == 2

    def test_total_length(self):
        msg = ClientMessage(
            [
                Frame(b"hello"),
                Frame(b"world"),
            ]
        )
        expected = SIZE_OF_FRAME_LENGTH_AND_FLAGS + 5 + SIZE_OF_FRAME_LENGTH_AND_FLAGS + 5
        assert msg.get_total_length() == expected


class TestStringCodec:
    """Tests for StringCodec."""

    def test_encode_decode_string(self):
        msg = ClientMessage.create_for_encode()
        StringCodec.encode(msg, "hello world")

        msg.reset_read_index()
        decoded = StringCodec.decode(msg)
        assert decoded == "hello world"

    def test_encode_decode_unicode(self):
        msg = ClientMessage.create_for_encode()
        text = "Hello \u4e16\u754c \U0001f30d"
        StringCodec.encode(msg, text)

        msg.reset_read_index()
        decoded = StringCodec.decode(msg)
        assert decoded == text

    def test_encode_decode_empty_string(self):
        msg = ClientMessage.create_for_encode()
        StringCodec.encode(msg, "")

        msg.reset_read_index()
        decoded = StringCodec.decode(msg)
        assert decoded == ""


class TestByteArrayCodec:
    """Tests for ByteArrayCodec."""

    def test_encode_decode_bytes(self):
        msg = ClientMessage.create_for_encode()
        data = b"\x00\x01\x02\x03\xff"
        ByteArrayCodec.encode(msg, data)

        msg.reset_read_index()
        decoded = ByteArrayCodec.decode(msg)
        assert decoded == data

    def test_encode_decode_empty_bytes(self):
        msg = ClientMessage.create_for_encode()
        ByteArrayCodec.encode(msg, b"")

        msg.reset_read_index()
        decoded = ByteArrayCodec.decode(msg)
        assert decoded == b""


class TestDataCodec:
    """Tests for DataCodec."""

    def test_encode_decode_nullable_with_value(self):
        msg = ClientMessage.create_for_encode()
        DataCodec.encode_nullable(msg, b"data")

        msg.reset_read_index()
        decoded = DataCodec.decode_nullable(msg)
        assert decoded == b"data"

    def test_encode_decode_nullable_none(self):
        msg = ClientMessage.create_for_encode()
        DataCodec.encode_nullable(msg, None)

        msg.reset_read_index()
        decoded = DataCodec.decode_nullable(msg)
        assert decoded is None


class TestLongCodec:
    """Tests for LongCodec."""

    def test_encode_decode_long(self):
        msg = ClientMessage.create_for_encode()
        LongCodec.encode(msg, 9876543210)

        msg.reset_read_index()
        decoded = LongCodec.decode(msg)
        assert decoded == 9876543210


class TestIntegerCodec:
    """Tests for IntegerCodec."""

    def test_encode_decode_int(self):
        msg = ClientMessage.create_for_encode()
        IntegerCodec.encode(msg, 42)

        msg.reset_read_index()
        decoded = IntegerCodec.decode(msg)
        assert decoded == 42


class TestListIntegerCodec:
    """Tests for ListIntegerCodec."""

    def test_encode_decode_list(self):
        msg = ClientMessage.create_for_encode()
        items = [1, 2, 3, 42, -100]
        ListIntegerCodec.encode(msg, items)

        msg.reset_read_index()
        decoded = ListIntegerCodec.decode(msg)
        assert decoded == items

    def test_encode_decode_empty_list(self):
        msg = ClientMessage.create_for_encode()
        ListIntegerCodec.encode(msg, [])

        msg.reset_read_index()
        decoded = ListIntegerCodec.decode(msg)
        assert decoded == []


class TestListLongCodec:
    """Tests for ListLongCodec."""

    def test_encode_decode_list(self):
        msg = ClientMessage.create_for_encode()
        items = [100000000000, 200000000000, -300000000000]
        ListLongCodec.encode(msg, items)

        msg.reset_read_index()
        decoded = ListLongCodec.decode(msg)
        assert decoded == items


class TestListMultiFrameCodec:
    """Tests for ListMultiFrameCodec."""

    def test_encode_decode_string_list(self):
        msg = ClientMessage.create_for_encode()
        items = ["one", "two", "three"]
        ListMultiFrameCodec.encode(msg, items, StringCodec.encode)

        msg.reset_read_index()
        decoded = ListMultiFrameCodec.decode(msg, StringCodec.decode)
        assert decoded == items

    def test_encode_decode_empty_list(self):
        msg = ClientMessage.create_for_encode()
        ListMultiFrameCodec.encode(msg, [], StringCodec.encode)

        msg.reset_read_index()
        decoded = ListMultiFrameCodec.decode(msg, StringCodec.decode)
        assert decoded == []

    def test_encode_decode_nullable_with_value(self):
        msg = ClientMessage.create_for_encode()
        items = ["a", "b"]
        ListMultiFrameCodec.encode_nullable(msg, items, StringCodec.encode)

        msg.reset_read_index()
        decoded = ListMultiFrameCodec.decode_nullable(msg, StringCodec.decode)
        assert decoded == items

    def test_encode_decode_nullable_none(self):
        msg = ClientMessage.create_for_encode()
        ListMultiFrameCodec.encode_nullable(msg, None, StringCodec.encode)

        msg.reset_read_index()
        decoded = ListMultiFrameCodec.decode_nullable(msg, StringCodec.decode)
        assert decoded is None


class TestEntryListCodec:
    """Tests for EntryListCodec."""

    def test_encode_decode_entries(self):
        msg = ClientMessage.create_for_encode()
        entries = [("key1", "value1"), ("key2", "value2")]
        EntryListCodec.encode(msg, entries, StringCodec.encode, StringCodec.encode)

        msg.reset_read_index()
        decoded = EntryListCodec.decode(msg, StringCodec.decode, StringCodec.decode)
        assert decoded == entries

    def test_encode_decode_empty_entries(self):
        msg = ClientMessage.create_for_encode()
        EntryListCodec.encode(msg, [], StringCodec.encode, StringCodec.encode)

        msg.reset_read_index()
        decoded = EntryListCodec.decode(msg, StringCodec.decode, StringCodec.decode)
        assert decoded == []


class TestCodecUtil:
    """Tests for CodecUtil."""

    def test_fast_forward_to_end_frame(self):
        msg = ClientMessage(
            [
                BEGIN_FRAME,
                Frame(b"nested"),
                END_FRAME,
                Frame(b"after"),
            ]
        )
        msg.next_frame()
        CodecUtil.fast_forward_to_end_frame(msg)
        assert msg.next_frame().content == b"after"

    def test_encode_nullable_with_value(self):
        msg = ClientMessage.create_for_encode()
        CodecUtil.encode_nullable(msg, "test", StringCodec.encode)

        msg.reset_read_index()
        decoded = CodecUtil.decode_nullable(msg, StringCodec.decode)
        assert decoded == "test"

    def test_encode_nullable_none(self):
        msg = ClientMessage.create_for_encode()
        CodecUtil.encode_nullable(msg, None, StringCodec.encode)

        msg.reset_read_index()
        decoded = CodecUtil.decode_nullable(msg, StringCodec.decode)
        assert decoded is None


class TestInvocation:
    """Tests for Invocation class."""

    def test_invocation_creation(self):
        initial = ClientMessage.create_initial_frame(0)
        msg = ClientMessage([initial])
        inv = Invocation(msg, partition_id=5, timeout=30.0)

        assert inv.request is msg
        assert inv.partition_id == 5
        assert inv.timeout == 30.0
        assert inv.sent_time is None

    def test_correlation_id_set(self):
        initial = ClientMessage.create_initial_frame(0)
        msg = ClientMessage([initial])
        inv = Invocation(msg)
        inv.correlation_id = 12345

        assert inv.correlation_id == 12345
        assert msg.get_correlation_id() == 12345

    def test_mark_sent(self):
        initial = ClientMessage.create_initial_frame(0)
        inv = Invocation(ClientMessage([initial]))
        assert inv.sent_time is None
        inv.mark_sent()
        assert inv.sent_time is not None

    def test_is_expired_before_sent(self):
        initial = ClientMessage.create_initial_frame(0)
        inv = Invocation(ClientMessage([initial]), timeout=0.001)
        assert inv.is_expired() is False

    def test_set_response(self):
        initial = ClientMessage.create_initial_frame(0)
        inv = Invocation(ClientMessage([initial]))
        response = ClientMessage([Frame(b"response")])
        inv.set_response(response)

        result = inv.future.result(timeout=1.0)
        assert result is response

    def test_set_exception(self):
        initial = ClientMessage.create_initial_frame(0)
        inv = Invocation(ClientMessage([initial]))
        inv.set_exception(ValueError("test error"))

        with pytest.raises(ValueError):
            inv.future.result(timeout=1.0)


class TestInvocationService:
    """Tests for InvocationService class."""

    def test_service_lifecycle(self):
        service = InvocationService()
        assert service.is_running is False
        service.start()
        assert service.is_running is True
        service.shutdown()
        assert service.is_running is False

    def test_invoke_assigns_correlation_id(self):
        service = InvocationService()
        service.start()

        initial = ClientMessage.create_initial_frame(0)
        inv = Invocation(ClientMessage([initial]))
        service.invoke(inv)

        assert inv.correlation_id > 0
        assert service.get_pending_count() == 1
        service.shutdown()

    def test_correlation_ids_increment(self):
        service = InvocationService()
        service.start()

        initial1 = ClientMessage.create_initial_frame(0)
        initial2 = ClientMessage.create_initial_frame(0)
        inv1 = Invocation(ClientMessage([initial1]))
        inv2 = Invocation(ClientMessage([initial2]))

        service.invoke(inv1)
        service.invoke(inv2)

        assert inv2.correlation_id > inv1.correlation_id
        service.shutdown()

    def test_handle_response(self):
        service = InvocationService()
        service.start()

        initial = ClientMessage.create_initial_frame(0)
        inv = Invocation(ClientMessage([initial]))
        future = service.invoke(inv)

        response = ClientMessage.create_for_encode()
        resp_initial = ClientMessage.create_initial_frame(0)
        response.add_frame(resp_initial)
        response.set_correlation_id(inv.correlation_id)

        result = service.handle_response(response)
        assert result is True
        assert future.result(timeout=1.0) is response
        assert service.get_pending_count() == 0
        service.shutdown()

    def test_handle_unknown_response(self):
        service = InvocationService()
        service.start()

        response = ClientMessage.create_for_encode()
        initial = ClientMessage.create_initial_frame(0)
        response.add_frame(initial)
        response.set_correlation_id(99999)

        result = service.handle_response(response)
        assert result is False
        service.shutdown()

    def test_remove_invocation(self):
        service = InvocationService()
        service.start()

        initial = ClientMessage.create_initial_frame(0)
        inv = Invocation(ClientMessage([initial]))
        service.invoke(inv)

        removed = service.remove_invocation(inv.correlation_id)
        assert removed is inv
        assert service.get_pending_count() == 0

        removed_again = service.remove_invocation(inv.correlation_id)
        assert removed_again is None
        service.shutdown()

    def test_shutdown_completes_pending(self):
        service = InvocationService()
        service.start()

        initial = ClientMessage.create_initial_frame(0)
        inv = Invocation(ClientMessage([initial]))
        future = service.invoke(inv)

        service.shutdown()

        with pytest.raises(Exception):
            future.result(timeout=1.0)
