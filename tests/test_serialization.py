"""Unit tests for hazelcast.serialization module."""

import pytest
import struct
from unittest.mock import Mock, MagicMock, patch

from hazelcast.serialization.service import (
    Data,
    ObjectDataInputImpl,
    ObjectDataOutputImpl,
    SerializationService,
    IdentifiedDataSerializableSerializer,
    PortableSerializer,
    TYPE_ID_SIZE,
    DATA_OFFSET,
)
from hazelcast.serialization.builtin import NONE_TYPE_ID
from hazelcast.serialization.api import IdentifiedDataSerializable, Portable
from hazelcast.exceptions import SerializationException


class TestData:
    """Tests for the Data class."""

    def test_empty_buffer(self):
        data = Data(b"")
        assert data.buffer == b""
        assert data.total_size() == 0
        assert len(data) == 0
        assert data.get_type_id() == NONE_TYPE_ID
        assert data.get_payload() == b""

    def test_buffer_too_short_for_type_id(self):
        data = Data(b"\x01\x02")
        assert data.get_type_id() == NONE_TYPE_ID

    def test_valid_type_id(self):
        type_id = 42
        buffer = struct.pack("<i", type_id) + b"payload"
        data = Data(buffer)
        assert data.get_type_id() == type_id
        assert data.get_payload() == b"payload"

    def test_negative_type_id(self):
        type_id = -5
        buffer = struct.pack("<i", type_id)
        data = Data(buffer)
        assert data.get_type_id() == type_id

    def test_equality(self):
        buffer = b"\x01\x02\x03\x04"
        data1 = Data(buffer)
        data2 = Data(buffer)
        data3 = Data(b"\x05\x06\x07\x08")
        
        assert data1 == data2
        assert data1 != data3
        assert data1 != "not a data"
        assert data1 != None

    def test_hash(self):
        buffer = b"\x01\x02\x03\x04"
        data1 = Data(buffer)
        data2 = Data(buffer)
        assert hash(data1) == hash(data2)

    def test_bytes_conversion(self):
        buffer = b"\x01\x02\x03\x04"
        data = Data(buffer)
        assert bytes(data) == buffer

    def test_payload_with_only_type_id(self):
        buffer = struct.pack("<i", 10)
        data = Data(buffer)
        assert data.get_payload() == b""


class TestObjectDataInputImpl:
    """Tests for ObjectDataInputImpl."""

    def setup_method(self):
        self.service = Mock(spec=SerializationService)

    def test_read_boolean_true(self):
        buffer = struct.pack("<B", 1)
        inp = ObjectDataInputImpl(buffer, self.service)
        assert inp.read_boolean() is True
        assert inp.position() == 1

    def test_read_boolean_false(self):
        buffer = struct.pack("<B", 0)
        inp = ObjectDataInputImpl(buffer, self.service)
        assert inp.read_boolean() is False

    def test_read_byte(self):
        buffer = struct.pack("<b", -42)
        inp = ObjectDataInputImpl(buffer, self.service)
        assert inp.read_byte() == -42

    def test_read_short(self):
        buffer = struct.pack("<h", -1234)
        inp = ObjectDataInputImpl(buffer, self.service)
        assert inp.read_short() == -1234

    def test_read_int(self):
        buffer = struct.pack("<i", 123456789)
        inp = ObjectDataInputImpl(buffer, self.service)
        assert inp.read_int() == 123456789

    def test_read_long(self):
        buffer = struct.pack("<q", 9876543210)
        inp = ObjectDataInputImpl(buffer, self.service)
        assert inp.read_long() == 9876543210

    def test_read_float(self):
        buffer = struct.pack("<f", 3.14)
        inp = ObjectDataInputImpl(buffer, self.service)
        assert abs(inp.read_float() - 3.14) < 0.001

    def test_read_double(self):
        buffer = struct.pack("<d", 3.14159265359)
        inp = ObjectDataInputImpl(buffer, self.service)
        assert abs(inp.read_double() - 3.14159265359) < 0.0000001

    def test_read_string(self):
        text = "Hello, World!"
        encoded = text.encode("utf-8")
        buffer = struct.pack("<i", len(encoded)) + encoded
        inp = ObjectDataInputImpl(buffer, self.service)
        assert inp.read_string() == text

    def test_read_string_empty(self):
        buffer = struct.pack("<i", 0)
        inp = ObjectDataInputImpl(buffer, self.service)
        assert inp.read_string() == ""

    def test_read_string_negative_length(self):
        buffer = struct.pack("<i", -1)
        inp = ObjectDataInputImpl(buffer, self.service)
        assert inp.read_string() == ""

    def test_read_string_unicode(self):
        text = "こんにちは世界"
        encoded = text.encode("utf-8")
        buffer = struct.pack("<i", len(encoded)) + encoded
        inp = ObjectDataInputImpl(buffer, self.service)
        assert inp.read_string() == text

    def test_read_byte_array(self):
        data = b"\x01\x02\x03\x04\x05"
        buffer = struct.pack("<i", len(data)) + data
        inp = ObjectDataInputImpl(buffer, self.service)
        assert inp.read_byte_array() == data

    def test_read_byte_array_empty(self):
        buffer = struct.pack("<i", 0)
        inp = ObjectDataInputImpl(buffer, self.service)
        assert inp.read_byte_array() == b""

    def test_read_byte_array_negative_length(self):
        buffer = struct.pack("<i", -1)
        inp = ObjectDataInputImpl(buffer, self.service)
        assert inp.read_byte_array() == b""

    def test_read_object(self):
        self.service._read_object = Mock(return_value="test_object")
        buffer = b""
        inp = ObjectDataInputImpl(buffer, self.service)
        result = inp.read_object()
        assert result == "test_object"
        self.service._read_object.assert_called_once_with(inp)

    def test_set_position(self):
        buffer = b"\x01\x02\x03\x04\x05"
        inp = ObjectDataInputImpl(buffer, self.service)
        inp.set_position(3)
        assert inp.position() == 3

    def test_sequential_reads(self):
        buffer = struct.pack("<i", 42) + struct.pack("<h", 100)
        inp = ObjectDataInputImpl(buffer, self.service)
        assert inp.read_int() == 42
        assert inp.read_short() == 100
        assert inp.position() == 6


class TestObjectDataOutputImpl:
    """Tests for ObjectDataOutputImpl."""

    def setup_method(self):
        self.service = Mock(spec=SerializationService)

    def test_write_boolean_true(self):
        out = ObjectDataOutputImpl(self.service)
        out.write_boolean(True)
        assert out.to_byte_array() == struct.pack("<B", 1)

    def test_write_boolean_false(self):
        out = ObjectDataOutputImpl(self.service)
        out.write_boolean(False)
        assert out.to_byte_array() == struct.pack("<B", 0)

    def test_write_byte(self):
        out = ObjectDataOutputImpl(self.service)
        out.write_byte(-42)
        assert out.to_byte_array() == struct.pack("<b", -42)

    def test_write_short(self):
        out = ObjectDataOutputImpl(self.service)
        out.write_short(-1234)
        assert out.to_byte_array() == struct.pack("<h", -1234)

    def test_write_int(self):
        out = ObjectDataOutputImpl(self.service)
        out.write_int(123456789)
        assert out.to_byte_array() == struct.pack("<i", 123456789)

    def test_write_long(self):
        out = ObjectDataOutputImpl(self.service)
        out.write_long(9876543210)
        assert out.to_byte_array() == struct.pack("<q", 9876543210)

    def test_write_float(self):
        out = ObjectDataOutputImpl(self.service)
        out.write_float(3.14)
        result = struct.unpack_from("<f", out.to_byte_array(), 0)[0]
        assert abs(result - 3.14) < 0.001

    def test_write_double(self):
        out = ObjectDataOutputImpl(self.service)
        out.write_double(3.14159265359)
        result = struct.unpack_from("<d", out.to_byte_array(), 0)[0]
        assert abs(result - 3.14159265359) < 0.0000001

    def test_write_string(self):
        out = ObjectDataOutputImpl(self.service)
        out.write_string("Hello")
        expected = struct.pack("<i", 5) + b"Hello"
        assert out.to_byte_array() == expected

    def test_write_string_none(self):
        out = ObjectDataOutputImpl(self.service)
        out.write_string(None)
        assert out.to_byte_array() == struct.pack("<i", -1)

    def test_write_string_unicode(self):
        out = ObjectDataOutputImpl(self.service)
        text = "日本語"
        out.write_string(text)
        encoded = text.encode("utf-8")
        expected = struct.pack("<i", len(encoded)) + encoded
        assert out.to_byte_array() == expected

    def test_write_byte_array(self):
        out = ObjectDataOutputImpl(self.service)
        data = b"\x01\x02\x03"
        out.write_byte_array(data)
        expected = struct.pack("<i", 3) + data
        assert out.to_byte_array() == expected

    def test_write_byte_array_none(self):
        out = ObjectDataOutputImpl(self.service)
        out.write_byte_array(None)
        assert out.to_byte_array() == struct.pack("<i", -1)

    def test_write_object(self):
        self.service._write_object = Mock()
        out = ObjectDataOutputImpl(self.service)
        out.write_object("test")
        self.service._write_object.assert_called_once_with(out, "test")

    def test_sequential_writes(self):
        out = ObjectDataOutputImpl(self.service)
        out.write_int(42)
        out.write_short(100)
        result = out.to_byte_array()
        assert len(result) == 6
        assert struct.unpack_from("<i", result, 0)[0] == 42
        assert struct.unpack_from("<h", result, 4)[0] == 100


class TestSerializationService:
    """Tests for SerializationService."""

    def test_to_data_none(self):
        service = SerializationService()
        data = service.to_data(None)
        assert data.get_type_id() == NONE_TYPE_ID

    def test_to_data_already_data(self):
        service = SerializationService()
        original = Data(b"\x01\x02\x03\x04")
        result = service.to_data(original)
        assert result is original

    def test_to_object_none(self):
        service = SerializationService()
        assert service.to_object(None) is None

    def test_to_object_bytes(self):
        service = SerializationService()
        buffer = struct.pack("<i", NONE_TYPE_ID)
        result = service.to_object(buffer)
        assert result is None

    def test_to_object_empty_data(self):
        service = SerializationService()
        data = Data(b"")
        assert service.to_object(data) is None

    def test_to_object_none_type_id(self):
        service = SerializationService()
        data = Data(struct.pack("<i", NONE_TYPE_ID))
        assert service.to_object(data) is None

    def test_to_object_passthrough_non_data(self):
        service = SerializationService()
        obj = {"key": "value"}
        assert service.to_object(obj) is obj

    def test_round_trip_string(self):
        service = SerializationService()
        original = "Hello, World!"
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == original

    def test_round_trip_int(self):
        service = SerializationService()
        original = 42
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == original

    def test_round_trip_float(self):
        service = SerializationService()
        original = 3.14159
        data = service.to_data(original)
        result = service.to_object(data)
        assert abs(result - original) < 0.0001

    def test_round_trip_bool(self):
        service = SerializationService()
        for original in [True, False]:
            data = service.to_data(original)
            result = service.to_object(data)
            assert result == original

    def test_round_trip_bytes(self):
        service = SerializationService()
        original = b"\x01\x02\x03\x04\x05"
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == original

    def test_round_trip_list(self):
        service = SerializationService()
        original = [1, 2, 3, "four", 5.0]
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == original

    def test_round_trip_dict(self):
        service = SerializationService()
        original = {"key1": "value1", "key2": 42}
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == original

    def test_no_serializer_found(self):
        service = SerializationService()
        
        class UnknownType:
            pass
        
        with pytest.raises(SerializationException) as exc_info:
            service.to_data(UnknownType())
        assert "No serializer found" in str(exc_info.value)

    def test_no_deserializer_found(self):
        service = SerializationService()
        unknown_type_id = 99999
        data = Data(struct.pack("<i", unknown_type_id) + b"payload")
        
        with pytest.raises(SerializationException) as exc_info:
            service.to_object(data)
        assert "No serializer found for type ID" in str(exc_info.value)

    def test_register_custom_serializer(self):
        class CustomClass:
            def __init__(self, value):
                self.value = value

        class CustomSerializer:
            @property
            def type_id(self):
                return 1000

            def write(self, output, obj):
                output.write_int(obj.value)

            def read(self, inp):
                return CustomClass(inp.read_int())

        service = SerializationService()
        service.register_serializer(CustomClass, CustomSerializer())
        
        original = CustomClass(42)
        data = service.to_data(original)
        result = service.to_object(data)
        assert result.value == 42

    def test_compact_service_property(self):
        service = SerializationService()
        compact = service.compact_service
        assert compact is not None
        assert compact is service.compact_service


class TestIdentifiedDataSerializableSerializer:
    """Tests for IdentifiedDataSerializableSerializer."""

    def test_write_read_round_trip(self):
        class TestDataSerializable(IdentifiedDataSerializable):
            def __init__(self, value=0):
                self.value = value

            @property
            def factory_id(self):
                return 1

            @property
            def class_id(self):
                return 1

            def write_data(self, output):
                output.write_int(self.value)

            def read_data(self, inp):
                self.value = inp.read_int()

        class TestFactory:
            def create(self, class_id):
                if class_id == 1:
                    return TestDataSerializable()
                return None

        factories = {1: TestFactory()}
        service = SerializationService(data_serializable_factories=factories)
        
        original = TestDataSerializable(42)
        data = service.to_data(original)
        result = service.to_object(data)
        assert result.value == 42

    def test_missing_factory(self):
        serializer = IdentifiedDataSerializableSerializer({})
        inp = Mock()
        inp.read_int = Mock(side_effect=[999, 1])
        
        with pytest.raises(SerializationException) as exc_info:
            serializer.read(inp)
        assert "No factory registered" in str(exc_info.value)

    def test_factory_returns_none(self):
        factory = Mock()
        factory.create = Mock(return_value=None)
        
        serializer = IdentifiedDataSerializableSerializer({1: factory})
        inp = Mock()
        inp.read_int = Mock(side_effect=[1, 1])
        
        with pytest.raises(SerializationException) as exc_info:
            serializer.read(inp)
        assert "returned None" in str(exc_info.value)


class TestPortableSerializer:
    """Tests for PortableSerializer."""

    def test_missing_factory(self):
        serializer = PortableSerializer(0, {})
        inp = Mock()
        inp.read_int = Mock(side_effect=[999, 1, 0])
        
        with pytest.raises(SerializationException) as exc_info:
            serializer.read(inp)
        assert "No factory registered" in str(exc_info.value)

    def test_factory_returns_none(self):
        factory = Mock()
        factory.create = Mock(return_value=None)
        
        serializer = PortableSerializer(0, {1: factory})
        inp = Mock()
        inp.read_int = Mock(side_effect=[1, 1, 0])
        
        with pytest.raises(SerializationException) as exc_info:
            serializer.read(inp)
        assert "returned None" in str(exc_info.value)

    def test_type_id(self):
        serializer = PortableSerializer(0, {})
        assert serializer.type_id == SerializationService.PORTABLE_ID


class TestNestedObjectSerialization:
    """Tests for nested object serialization."""

    def test_write_nested_none(self):
        service = SerializationService()
        out = ObjectDataOutputImpl(service)
        service._write_object(out, None)
        
        result = out.to_byte_array()
        assert struct.unpack_from("<i", result, 0)[0] == NONE_TYPE_ID

    def test_read_nested_none(self):
        service = SerializationService()
        buffer = struct.pack("<i", NONE_TYPE_ID)
        inp = ObjectDataInputImpl(buffer, service)
        
        result = service._read_object(inp)
        assert result is None

    def test_nested_unknown_type_write(self):
        service = SerializationService()
        out = ObjectDataOutputImpl(service)
        
        class UnknownType:
            pass
        
        with pytest.raises(SerializationException):
            service._write_object(out, UnknownType())

    def test_nested_unknown_type_read(self):
        service = SerializationService()
        buffer = struct.pack("<i", 99999)
        inp = ObjectDataInputImpl(buffer, service)
        
        with pytest.raises(SerializationException):
            service._read_object(inp)
