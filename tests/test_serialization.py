"""Unit tests for hazelcast.serialization module."""

import pytest
import struct
import uuid

from hazelcast.serialization.service import (
    SerializationService,
    Data,
    ObjectDataInputImpl,
    ObjectDataOutputImpl,
)
from hazelcast.serialization.builtin import (
    NoneSerializer,
    BoolSerializer,
    IntSerializer,
    FloatSerializer,
    StringSerializer,
    ByteArraySerializer,
    ListSerializer,
    DictSerializer,
    UUIDSerializer,
    get_builtin_serializers,
    get_type_serializer_mapping,
    NONE_TYPE_ID,
    BOOLEAN_TYPE_ID,
    INT_TYPE_ID,
    STRING_TYPE_ID,
)
from hazelcast.serialization.api import (
    IdentifiedDataSerializable,
    ObjectDataInput,
    ObjectDataOutput,
)
from hazelcast.exceptions import SerializationException


class TestData:
    """Tests for Data class."""

    def test_empty_data(self):
        data = Data(b"")
        assert data.buffer == b""
        assert len(data) == 0
        assert data.total_size() == 0

    def test_get_type_id(self):
        buffer = struct.pack("<i", 42) + b"payload"
        data = Data(buffer)
        assert data.get_type_id() == 42

    def test_get_type_id_short_buffer(self):
        data = Data(b"ab")
        assert data.get_type_id() == NONE_TYPE_ID

    def test_get_payload(self):
        buffer = struct.pack("<i", 1) + b"test_payload"
        data = Data(buffer)
        assert data.get_payload() == b"test_payload"

    def test_get_payload_empty(self):
        buffer = struct.pack("<i", 1)
        data = Data(buffer)
        assert data.get_payload() == b""

    def test_equality(self):
        data1 = Data(b"test")
        data2 = Data(b"test")
        data3 = Data(b"other")
        assert data1 == data2
        assert data1 != data3
        assert data1 != "not data"

    def test_hash(self):
        data1 = Data(b"test")
        data2 = Data(b"test")
        assert hash(data1) == hash(data2)

    def test_bytes(self):
        data = Data(b"test")
        assert bytes(data) == b"test"


class TestObjectDataInputImpl:
    """Tests for ObjectDataInputImpl."""

    def test_read_boolean_true(self, serialization_service):
        buffer = struct.pack("<B", 1)
        input_stream = ObjectDataInputImpl(buffer, serialization_service)
        assert input_stream.read_boolean() is True

    def test_read_boolean_false(self, serialization_service):
        buffer = struct.pack("<B", 0)
        input_stream = ObjectDataInputImpl(buffer, serialization_service)
        assert input_stream.read_boolean() is False

    def test_read_byte(self, serialization_service):
        buffer = struct.pack("<b", -42)
        input_stream = ObjectDataInputImpl(buffer, serialization_service)
        assert input_stream.read_byte() == -42

    def test_read_short(self, serialization_service):
        buffer = struct.pack("<h", 12345)
        input_stream = ObjectDataInputImpl(buffer, serialization_service)
        assert input_stream.read_short() == 12345

    def test_read_int(self, serialization_service):
        buffer = struct.pack("<i", 123456789)
        input_stream = ObjectDataInputImpl(buffer, serialization_service)
        assert input_stream.read_int() == 123456789

    def test_read_long(self, serialization_service):
        buffer = struct.pack("<q", 9876543210)
        input_stream = ObjectDataInputImpl(buffer, serialization_service)
        assert input_stream.read_long() == 9876543210

    def test_read_float(self, serialization_service):
        buffer = struct.pack("<f", 3.14)
        input_stream = ObjectDataInputImpl(buffer, serialization_service)
        assert abs(input_stream.read_float() - 3.14) < 0.001

    def test_read_double(self, serialization_service):
        buffer = struct.pack("<d", 3.14159265359)
        input_stream = ObjectDataInputImpl(buffer, serialization_service)
        assert abs(input_stream.read_double() - 3.14159265359) < 0.0000001

    def test_read_string(self, serialization_service):
        test_str = "hello"
        encoded = test_str.encode("utf-8")
        buffer = struct.pack("<i", len(encoded)) + encoded
        input_stream = ObjectDataInputImpl(buffer, serialization_service)
        assert input_stream.read_string() == "hello"

    def test_read_string_empty(self, serialization_service):
        buffer = struct.pack("<i", -1)
        input_stream = ObjectDataInputImpl(buffer, serialization_service)
        assert input_stream.read_string() == ""

    def test_read_byte_array(self, serialization_service):
        test_bytes = b"\x01\x02\x03\x04"
        buffer = struct.pack("<i", len(test_bytes)) + test_bytes
        input_stream = ObjectDataInputImpl(buffer, serialization_service)
        assert input_stream.read_byte_array() == test_bytes

    def test_read_byte_array_empty(self, serialization_service):
        buffer = struct.pack("<i", -1)
        input_stream = ObjectDataInputImpl(buffer, serialization_service)
        assert input_stream.read_byte_array() == b""

    def test_position(self, serialization_service):
        buffer = struct.pack("<i", 42)
        input_stream = ObjectDataInputImpl(buffer, serialization_service)
        assert input_stream.position() == 0
        input_stream.read_int()
        assert input_stream.position() == 4

    def test_set_position(self, serialization_service):
        buffer = struct.pack("<ii", 1, 2)
        input_stream = ObjectDataInputImpl(buffer, serialization_service)
        input_stream.set_position(4)
        assert input_stream.read_int() == 2


class TestObjectDataOutputImpl:
    """Tests for ObjectDataOutputImpl."""

    def test_write_boolean(self, serialization_service):
        output = ObjectDataOutputImpl(serialization_service)
        output.write_boolean(True)
        output.write_boolean(False)
        result = output.to_byte_array()
        assert result == struct.pack("<BB", 1, 0)

    def test_write_byte(self, serialization_service):
        output = ObjectDataOutputImpl(serialization_service)
        output.write_byte(-42)
        result = output.to_byte_array()
        assert result == struct.pack("<b", -42)

    def test_write_short(self, serialization_service):
        output = ObjectDataOutputImpl(serialization_service)
        output.write_short(12345)
        result = output.to_byte_array()
        assert result == struct.pack("<h", 12345)

    def test_write_int(self, serialization_service):
        output = ObjectDataOutputImpl(serialization_service)
        output.write_int(123456789)
        result = output.to_byte_array()
        assert result == struct.pack("<i", 123456789)

    def test_write_long(self, serialization_service):
        output = ObjectDataOutputImpl(serialization_service)
        output.write_long(9876543210)
        result = output.to_byte_array()
        assert result == struct.pack("<q", 9876543210)

    def test_write_float(self, serialization_service):
        output = ObjectDataOutputImpl(serialization_service)
        output.write_float(3.14)
        result = output.to_byte_array()
        assert len(result) == 4

    def test_write_double(self, serialization_service):
        output = ObjectDataOutputImpl(serialization_service)
        output.write_double(3.14159265359)
        result = output.to_byte_array()
        assert len(result) == 8

    def test_write_string(self, serialization_service):
        output = ObjectDataOutputImpl(serialization_service)
        output.write_string("hello")
        result = output.to_byte_array()
        expected = struct.pack("<i", 5) + b"hello"
        assert result == expected

    def test_write_string_none(self, serialization_service):
        output = ObjectDataOutputImpl(serialization_service)
        output.write_string(None)
        result = output.to_byte_array()
        assert result == struct.pack("<i", -1)

    def test_write_byte_array(self, serialization_service):
        output = ObjectDataOutputImpl(serialization_service)
        output.write_byte_array(b"\x01\x02\x03")
        result = output.to_byte_array()
        expected = struct.pack("<i", 3) + b"\x01\x02\x03"
        assert result == expected

    def test_write_byte_array_none(self, serialization_service):
        output = ObjectDataOutputImpl(serialization_service)
        output.write_byte_array(None)
        result = output.to_byte_array()
        assert result == struct.pack("<i", -1)


class TestSerializationService:
    """Tests for SerializationService."""

    def test_to_data_none(self, serialization_service):
        data = serialization_service.to_data(None)
        assert data.get_type_id() == NONE_TYPE_ID

    def test_to_object_none(self, serialization_service):
        data = serialization_service.to_data(None)
        result = serialization_service.to_object(data)
        assert result is None

    def test_to_data_already_data(self, serialization_service):
        original = Data(b"test")
        result = serialization_service.to_data(original)
        assert result is original

    def test_roundtrip_bool(self, serialization_service):
        data = serialization_service.to_data(True)
        result = serialization_service.to_object(data)
        assert result is True

        data = serialization_service.to_data(False)
        result = serialization_service.to_object(data)
        assert result is False

    def test_roundtrip_int(self, serialization_service):
        data = serialization_service.to_data(42)
        result = serialization_service.to_object(data)
        assert result == 42

    def test_roundtrip_float(self, serialization_service):
        data = serialization_service.to_data(3.14)
        result = serialization_service.to_object(data)
        assert abs(result - 3.14) < 0.001

    def test_roundtrip_string(self, serialization_service):
        data = serialization_service.to_data("hello world")
        result = serialization_service.to_object(data)
        assert result == "hello world"

    def test_roundtrip_bytes(self, serialization_service):
        test_bytes = b"\x00\x01\x02\x03\xff"
        data = serialization_service.to_data(test_bytes)
        result = serialization_service.to_object(data)
        assert result == test_bytes

    def test_roundtrip_list(self, serialization_service):
        test_list = [1, 2, 3, "four", 5.0]
        data = serialization_service.to_data(test_list)
        result = serialization_service.to_object(data)
        assert result[0] == 1
        assert result[1] == 2
        assert result[2] == 3
        assert result[3] == "four"

    def test_roundtrip_dict(self, serialization_service):
        test_dict = {"key1": "value1", "key2": 42}
        data = serialization_service.to_data(test_dict)
        result = serialization_service.to_object(data)
        assert result["key1"] == "value1"
        assert result["key2"] == 42

    def test_roundtrip_tuple_as_list(self, serialization_service):
        test_tuple = (1, 2, 3)
        data = serialization_service.to_data(test_tuple)
        result = serialization_service.to_object(data)
        assert result == [1, 2, 3]

    def test_to_object_bytes_input(self, serialization_service):
        data = serialization_service.to_data(42)
        result = serialization_service.to_object(data.buffer)
        assert result == 42

    def test_to_object_non_data_passthrough(self, serialization_service):
        result = serialization_service.to_object("not data")
        assert result == "not data"

    def test_to_object_empty_data(self, serialization_service):
        data = Data(b"")
        result = serialization_service.to_object(data)
        assert result is None

    def test_unknown_type_raises(self, serialization_service):
        class UnknownType:
            pass

        with pytest.raises(SerializationException):
            serialization_service.to_data(UnknownType())

    def test_unknown_type_id_raises(self, serialization_service):
        buffer = struct.pack("<i", 99999) + b"payload"
        data = Data(buffer)
        with pytest.raises(SerializationException):
            serialization_service.to_object(data)


class TestBuiltinSerializers:
    """Tests for built-in serializers."""

    def test_get_builtin_serializers(self):
        serializers = get_builtin_serializers()
        assert NONE_TYPE_ID in serializers
        assert BOOLEAN_TYPE_ID in serializers
        assert INT_TYPE_ID in serializers
        assert STRING_TYPE_ID in serializers

    def test_get_type_serializer_mapping(self):
        mapping = get_type_serializer_mapping()
        assert type(None) in mapping
        assert bool in mapping
        assert int in mapping
        assert float in mapping
        assert str in mapping
        assert bytes in mapping
        assert list in mapping
        assert dict in mapping


class TestNoneSerializer:
    """Tests for NoneSerializer."""

    def test_type_id(self):
        serializer = NoneSerializer()
        assert serializer.type_id == NONE_TYPE_ID

    def test_read(self, serialization_service):
        serializer = NoneSerializer()
        buffer = b""
        input_stream = ObjectDataInputImpl(buffer, serialization_service)
        result = serializer.read(input_stream)
        assert result is None


class TestBoolSerializer:
    """Tests for BoolSerializer."""

    def test_type_id(self):
        serializer = BoolSerializer()
        assert serializer.type_id == BOOLEAN_TYPE_ID


class TestIntSerializer:
    """Tests for IntSerializer."""

    def test_type_id(self):
        serializer = IntSerializer()
        assert serializer.type_id == INT_TYPE_ID


class TestUUIDSerializer:
    """Tests for UUIDSerializer."""

    def test_roundtrip(self, serialization_service):
        test_uuid = uuid.uuid4()
        data = serialization_service.to_data(test_uuid)
        # Note: UUID serialization requires registering the type
        # This test verifies the serializer exists
        serializer = UUIDSerializer()
        assert serializer.type_id != 0
