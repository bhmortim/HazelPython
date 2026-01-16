"""Comprehensive unit tests for hazelcast/serialization/service.py."""

import struct
import pytest
from unittest.mock import MagicMock, patch

from hazelcast.serialization.service import (
    FieldType,
    FieldDefinition,
    ClassDefinition,
    ClassDefinitionBuilder,
    ClassDefinitionContext,
    Data,
    ObjectDataInputImpl,
    ObjectDataOutputImpl,
    SerializationService,
    IdentifiedDataSerializableSerializer,
    DefaultPortableWriter,
    DefaultPortableReader,
    PortableSerializer,
    TYPE_ID_SIZE,
    DATA_OFFSET,
)
from hazelcast.serialization.api import (
    IdentifiedDataSerializable,
    Portable,
    PortableReader,
    PortableWriter,
)
from hazelcast.serialization.builtin import NONE_TYPE_ID
from hazelcast.exceptions import SerializationException


class TestFieldType:
    def test_enum_values(self):
        assert FieldType.BOOLEAN.value == 0
        assert FieldType.BYTE.value == 1
        assert FieldType.SHORT.value == 2
        assert FieldType.INT.value == 3
        assert FieldType.LONG.value == 4
        assert FieldType.FLOAT.value == 5
        assert FieldType.DOUBLE.value == 6
        assert FieldType.STRING.value == 7
        assert FieldType.PORTABLE.value == 8
        assert FieldType.BYTE_ARRAY.value == 9
        assert FieldType.BOOLEAN_ARRAY.value == 10
        assert FieldType.SHORT_ARRAY.value == 11
        assert FieldType.INT_ARRAY.value == 12
        assert FieldType.LONG_ARRAY.value == 13
        assert FieldType.FLOAT_ARRAY.value == 14
        assert FieldType.DOUBLE_ARRAY.value == 15
        assert FieldType.STRING_ARRAY.value == 16
        assert FieldType.PORTABLE_ARRAY.value == 17


class TestFieldDefinition:
    def test_init(self):
        fd = FieldDefinition(0, "name", FieldType.STRING)
        
        assert fd.index == 0
        assert fd.name == "name"
        assert fd.field_type == FieldType.STRING
        assert fd.factory_id == 0
        assert fd.class_id == 0
        assert fd.version == 0

    def test_init_with_nested(self):
        fd = FieldDefinition(
            1, "nested", FieldType.PORTABLE,
            factory_id=10, class_id=20, version=1
        )
        
        assert fd.factory_id == 10
        assert fd.class_id == 20
        assert fd.version == 1

    def test_equality(self):
        fd1 = FieldDefinition(0, "name", FieldType.STRING)
        fd2 = FieldDefinition(0, "name", FieldType.STRING)
        fd3 = FieldDefinition(0, "different", FieldType.STRING)
        
        assert fd1 == fd2
        assert fd1 != fd3

    def test_equality_different_type(self):
        fd = FieldDefinition(0, "name", FieldType.STRING)
        assert fd != "not a field"

    def test_hash(self):
        fd1 = FieldDefinition(0, "name", FieldType.STRING)
        fd2 = FieldDefinition(0, "name", FieldType.STRING)
        
        assert hash(fd1) == hash(fd2)


class TestClassDefinition:
    def test_init(self):
        cd = ClassDefinition(1, 2, 0)
        
        assert cd.factory_id == 1
        assert cd.class_id == 2
        assert cd.version == 0
        assert cd.get_field_count() == 0

    def test_add_field(self):
        cd = ClassDefinition(1, 1, 0)
        fd = FieldDefinition(0, "name", FieldType.STRING)
        
        result = cd.add_field(fd)
        
        assert result is cd
        assert cd.get_field_count() == 1

    def test_get_field(self):
        cd = ClassDefinition(1, 1, 0)
        fd = FieldDefinition(0, "name", FieldType.STRING)
        cd.add_field(fd)
        
        result = cd.get_field("name")
        
        assert result is fd

    def test_get_field_not_found(self):
        cd = ClassDefinition(1, 1, 0)
        
        result = cd.get_field("nonexistent")
        
        assert result is None

    def test_get_field_by_index(self):
        cd = ClassDefinition(1, 1, 0)
        fd = FieldDefinition(0, "name", FieldType.STRING)
        cd.add_field(fd)
        
        result = cd.get_field_by_index(0)
        
        assert result is fd

    def test_get_field_by_index_out_of_range(self):
        cd = ClassDefinition(1, 1, 0)
        
        assert cd.get_field_by_index(-1) is None
        assert cd.get_field_by_index(0) is None
        assert cd.get_field_by_index(100) is None

    def test_has_field(self):
        cd = ClassDefinition(1, 1, 0)
        fd = FieldDefinition(0, "name", FieldType.STRING)
        cd.add_field(fd)
        
        assert cd.has_field("name") is True
        assert cd.has_field("other") is False

    def test_get_field_names(self):
        cd = ClassDefinition(1, 1, 0)
        cd.add_field(FieldDefinition(0, "name", FieldType.STRING))
        cd.add_field(FieldDefinition(1, "age", FieldType.INT))
        
        names = cd.get_field_names()
        
        assert "name" in names
        assert "age" in names

    def test_get_field_type(self):
        cd = ClassDefinition(1, 1, 0)
        cd.add_field(FieldDefinition(0, "name", FieldType.STRING))
        
        assert cd.get_field_type("name") == FieldType.STRING
        assert cd.get_field_type("nonexistent") is None

    def test_fields_property(self):
        cd = ClassDefinition(1, 1, 0)
        fd1 = FieldDefinition(0, "name", FieldType.STRING)
        fd2 = FieldDefinition(1, "age", FieldType.INT)
        cd.add_field(fd1)
        cd.add_field(fd2)
        
        fields = cd.fields
        
        assert len(fields) == 2
        assert fd1 in fields
        assert fd2 in fields

    def test_equality(self):
        cd1 = ClassDefinition(1, 1, 0)
        cd1.add_field(FieldDefinition(0, "name", FieldType.STRING))
        
        cd2 = ClassDefinition(1, 1, 0)
        cd2.add_field(FieldDefinition(0, "name", FieldType.STRING))
        
        cd3 = ClassDefinition(1, 2, 0)
        
        assert cd1 == cd2
        assert cd1 != cd3

    def test_equality_different_type(self):
        cd = ClassDefinition(1, 1, 0)
        assert cd != "not a class def"

    def test_hash(self):
        cd1 = ClassDefinition(1, 1, 0)
        cd2 = ClassDefinition(1, 1, 0)
        
        assert hash(cd1) == hash(cd2)


class TestClassDefinitionBuilder:
    def test_init(self):
        builder = ClassDefinitionBuilder(1, 2, 0)
        
        cd = builder.build()
        
        assert cd.factory_id == 1
        assert cd.class_id == 2
        assert cd.version == 0

    def test_add_boolean_field(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        result = builder.add_boolean_field("active")
        
        assert result is builder
        cd = builder.build()
        assert cd.get_field_type("active") == FieldType.BOOLEAN

    def test_add_byte_field(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_byte_field("byte_val")
        
        cd = builder.build()
        assert cd.get_field_type("byte_val") == FieldType.BYTE

    def test_add_short_field(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_short_field("short_val")
        
        cd = builder.build()
        assert cd.get_field_type("short_val") == FieldType.SHORT

    def test_add_int_field(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_int_field("int_val")
        
        cd = builder.build()
        assert cd.get_field_type("int_val") == FieldType.INT

    def test_add_long_field(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_long_field("long_val")
        
        cd = builder.build()
        assert cd.get_field_type("long_val") == FieldType.LONG

    def test_add_float_field(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_float_field("float_val")
        
        cd = builder.build()
        assert cd.get_field_type("float_val") == FieldType.FLOAT

    def test_add_double_field(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_double_field("double_val")
        
        cd = builder.build()
        assert cd.get_field_type("double_val") == FieldType.DOUBLE

    def test_add_string_field(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_string_field("string_val")
        
        cd = builder.build()
        assert cd.get_field_type("string_val") == FieldType.STRING

    def test_add_byte_array_field(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_byte_array_field("bytes")
        
        cd = builder.build()
        assert cd.get_field_type("bytes") == FieldType.BYTE_ARRAY

    def test_add_boolean_array_field(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_boolean_array_field("bools")
        
        cd = builder.build()
        assert cd.get_field_type("bools") == FieldType.BOOLEAN_ARRAY

    def test_add_int_array_field(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_int_array_field("ints")
        
        cd = builder.build()
        assert cd.get_field_type("ints") == FieldType.INT_ARRAY

    def test_add_long_array_field(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_long_array_field("longs")
        
        cd = builder.build()
        assert cd.get_field_type("longs") == FieldType.LONG_ARRAY

    def test_add_string_array_field(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_string_array_field("strings")
        
        cd = builder.build()
        assert cd.get_field_type("strings") == FieldType.STRING_ARRAY

    def test_add_portable_field(self):
        nested_cd = ClassDefinition(2, 1, 0)
        
        builder = ClassDefinitionBuilder(1, 1, 0)
        result = builder.add_portable_field("nested", nested_cd)
        
        assert result is builder
        cd = builder.build()
        field = cd.get_field("nested")
        assert field.field_type == FieldType.PORTABLE
        assert field.factory_id == 2
        assert field.class_id == 1

    def test_add_portable_array_field(self):
        nested_cd = ClassDefinition(2, 1, 0)
        
        builder = ClassDefinitionBuilder(1, 1, 0)
        result = builder.add_portable_array_field("nested_array", nested_cd)
        
        assert result is builder
        cd = builder.build()
        field = cd.get_field("nested_array")
        assert field.field_type == FieldType.PORTABLE_ARRAY


class TestClassDefinitionContext:
    def test_init(self):
        ctx = ClassDefinitionContext(0)

    def test_register_and_lookup(self):
        ctx = ClassDefinitionContext(0)
        cd = ClassDefinition(1, 1, 0)
        
        result = ctx.register(cd)
        
        assert result is cd
        
        found = ctx.lookup(1, 1, 0)
        assert found is cd

    def test_register_returns_existing(self):
        ctx = ClassDefinitionContext(0)
        cd1 = ClassDefinition(1, 1, 0)
        cd2 = ClassDefinition(1, 1, 0)
        
        ctx.register(cd1)
        result = ctx.register(cd2)
        
        assert result is cd1

    def test_lookup_not_found(self):
        ctx = ClassDefinitionContext(0)
        
        result = ctx.lookup(1, 1, 0)
        
        assert result is None

    def test_lookup_wrong_factory(self):
        ctx = ClassDefinitionContext(0)
        cd = ClassDefinition(1, 1, 0)
        ctx.register(cd)
        
        result = ctx.lookup(2, 1, 0)
        
        assert result is None


class TestData:
    def test_init(self):
        buffer = b"\x00\x00\x00\x00payload"
        data = Data(buffer)
        
        assert data.buffer == buffer

    def test_buffer_property(self):
        buffer = b"test"
        data = Data(buffer)
        
        assert data.buffer == buffer

    def test_get_type_id(self):
        buffer = struct.pack("<i", 42) + b"payload"
        data = Data(buffer)
        
        assert data.get_type_id() == 42

    def test_get_type_id_too_small(self):
        data = Data(b"ab")
        
        assert data.get_type_id() == NONE_TYPE_ID

    def test_get_payload(self):
        buffer = struct.pack("<i", 42) + b"payload"
        data = Data(buffer)
        
        assert data.get_payload() == b"payload"

    def test_get_payload_empty(self):
        buffer = struct.pack("<i", 42)
        data = Data(buffer)
        
        assert data.get_payload() == b""

    def test_get_payload_too_small(self):
        data = Data(b"ab")
        
        assert data.get_payload() == b""

    def test_total_size(self):
        buffer = b"12345678"
        data = Data(buffer)
        
        assert data.total_size() == 8

    def test_len(self):
        buffer = b"12345"
        data = Data(buffer)
        
        assert len(data) == 5

    def test_equality(self):
        data1 = Data(b"test")
        data2 = Data(b"test")
        data3 = Data(b"other")
        
        assert data1 == data2
        assert data1 != data3

    def test_equality_different_type(self):
        data = Data(b"test")
        assert data != "test"

    def test_hash(self):
        data1 = Data(b"test")
        data2 = Data(b"test")
        
        assert hash(data1) == hash(data2)

    def test_bytes(self):
        buffer = b"test"
        data = Data(buffer)
        
        assert bytes(data) == buffer


class TestObjectDataInputImpl:
    def create_service(self):
        return MagicMock()

    def test_read_boolean_true(self):
        service = self.create_service()
        inp = ObjectDataInputImpl(struct.pack("<B", 1), service)
        
        assert inp.read_boolean() is True

    def test_read_boolean_false(self):
        service = self.create_service()
        inp = ObjectDataInputImpl(struct.pack("<B", 0), service)
        
        assert inp.read_boolean() is False

    def test_read_byte(self):
        service = self.create_service()
        inp = ObjectDataInputImpl(struct.pack("<b", -42), service)
        
        assert inp.read_byte() == -42

    def test_read_short(self):
        service = self.create_service()
        inp = ObjectDataInputImpl(struct.pack("<h", 12345), service)
        
        assert inp.read_short() == 12345

    def test_read_int(self):
        service = self.create_service()
        inp = ObjectDataInputImpl(struct.pack("<i", 123456789), service)
        
        assert inp.read_int() == 123456789

    def test_read_long(self):
        service = self.create_service()
        inp = ObjectDataInputImpl(struct.pack("<q", 9876543210), service)
        
        assert inp.read_long() == 9876543210

    def test_read_float(self):
        service = self.create_service()
        inp = ObjectDataInputImpl(struct.pack("<f", 3.14), service)
        
        result = inp.read_float()
        assert abs(result - 3.14) < 0.001

    def test_read_double(self):
        service = self.create_service()
        inp = ObjectDataInputImpl(struct.pack("<d", 3.14159265359), service)
        
        result = inp.read_double()
        assert abs(result - 3.14159265359) < 0.0000001

    def test_read_string(self):
        service = self.create_service()
        text = "Hello"
        encoded = text.encode("utf-8")
        buffer = struct.pack("<i", len(encoded)) + encoded
        inp = ObjectDataInputImpl(buffer, service)
        
        assert inp.read_string() == "Hello"

    def test_read_string_empty(self):
        service = self.create_service()
        buffer = struct.pack("<i", -1)
        inp = ObjectDataInputImpl(buffer, service)
        
        assert inp.read_string() == ""

    def test_read_byte_array(self):
        service = self.create_service()
        data = b"\x01\x02\x03"
        buffer = struct.pack("<i", len(data)) + data
        inp = ObjectDataInputImpl(buffer, service)
        
        assert inp.read_byte_array() == data

    def test_read_byte_array_empty(self):
        service = self.create_service()
        buffer = struct.pack("<i", -1)
        inp = ObjectDataInputImpl(buffer, service)
        
        assert inp.read_byte_array() == b""

    def test_read_object(self):
        service = self.create_service()
        service._read_object.return_value = "test_object"
        inp = ObjectDataInputImpl(b"", service)
        
        result = inp.read_object()
        
        service._read_object.assert_called_once_with(inp)
        assert result == "test_object"

    def test_position(self):
        service = self.create_service()
        inp = ObjectDataInputImpl(b"\x00\x00\x00\x00", service)
        
        assert inp.position() == 0
        inp.read_int()
        assert inp.position() == 4

    def test_set_position(self):
        service = self.create_service()
        inp = ObjectDataInputImpl(b"\x00\x00\x00\x00\x00\x00\x00\x00", service)
        
        inp.set_position(4)
        assert inp.position() == 4


class TestObjectDataOutputImpl:
    def create_service(self):
        return MagicMock()

    def test_write_boolean_true(self):
        service = self.create_service()
        out = ObjectDataOutputImpl(service)
        
        out.write_boolean(True)
        
        assert out.to_byte_array() == struct.pack("<B", 1)

    def test_write_boolean_false(self):
        service = self.create_service()
        out = ObjectDataOutputImpl(service)
        
        out.write_boolean(False)
        
        assert out.to_byte_array() == struct.pack("<B", 0)

    def test_write_byte(self):
        service = self.create_service()
        out = ObjectDataOutputImpl(service)
        
        out.write_byte(-42)
        
        assert out.to_byte_array() == struct.pack("<b", -42)

    def test_write_short(self):
        service = self.create_service()
        out = ObjectDataOutputImpl(service)
        
        out.write_short(12345)
        
        assert out.to_byte_array() == struct.pack("<h", 12345)

    def test_write_int(self):
        service = self.create_service()
        out = ObjectDataOutputImpl(service)
        
        out.write_int(123456789)
        
        assert out.to_byte_array() == struct.pack("<i", 123456789)

    def test_write_long(self):
        service = self.create_service()
        out = ObjectDataOutputImpl(service)
        
        out.write_long(9876543210)
        
        assert out.to_byte_array() == struct.pack("<q", 9876543210)

    def test_write_float(self):
        service = self.create_service()
        out = ObjectDataOutputImpl(service)
        
        out.write_float(3.14)
        
        result = struct.unpack("<f", out.to_byte_array())[0]
        assert abs(result - 3.14) < 0.001

    def test_write_double(self):
        service = self.create_service()
        out = ObjectDataOutputImpl(service)
        
        out.write_double(3.14159265359)
        
        result = struct.unpack("<d", out.to_byte_array())[0]
        assert abs(result - 3.14159265359) < 0.0000001

    def test_write_string(self):
        service = self.create_service()
        out = ObjectDataOutputImpl(service)
        
        out.write_string("Hello")
        
        result = out.to_byte_array()
        length = struct.unpack("<i", result[:4])[0]
        assert length == 5
        assert result[4:] == b"Hello"

    def test_write_string_none(self):
        service = self.create_service()
        out = ObjectDataOutputImpl(service)
        
        out.write_string(None)
        
        result = out.to_byte_array()
        length = struct.unpack("<i", result[:4])[0]
        assert length == -1

    def test_write_byte_array(self):
        service = self.create_service()
        out = ObjectDataOutputImpl(service)
        
        out.write_byte_array(b"\x01\x02\x03")
        
        result = out.to_byte_array()
        length = struct.unpack("<i", result[:4])[0]
        assert length == 3
        assert result[4:] == b"\x01\x02\x03"

    def test_write_byte_array_none(self):
        service = self.create_service()
        out = ObjectDataOutputImpl(service)
        
        out.write_byte_array(None)
        
        result = out.to_byte_array()
        length = struct.unpack("<i", result[:4])[0]
        assert length == -1

    def test_write_object(self):
        service = self.create_service()
        out = ObjectDataOutputImpl(service)
        
        out.write_object("test")
        
        service._write_object.assert_called_once_with(out, "test")


class TestSerializationService:
    def test_init_default(self):
        service = SerializationService()
        
        assert service.class_definition_context is not None
        assert service.compact_service is not None

    def test_init_with_params(self):
        factory = MagicMock()
        data_factory = MagicMock()
        
        service = SerializationService(
            portable_version=1,
            portable_factories={1: factory},
            data_serializable_factories={2: data_factory},
        )
        
        assert service._portable_version == 1
        assert service._portable_factories[1] is factory
        assert service._data_serializable_factories[2] is data_factory

    def test_init_with_custom_serializers(self):
        class MyClass:
            pass
        
        serializer = MagicMock()
        serializer.type_id = 100
        
        service = SerializationService(
            custom_serializers={MyClass: serializer}
        )
        
        assert MyClass in service._type_to_serializer

    def test_to_data_none(self):
        service = SerializationService()
        
        data = service.to_data(None)
        
        assert data.get_type_id() == NONE_TYPE_ID

    def test_to_data_already_data(self):
        service = SerializationService()
        original = Data(b"test")
        
        result = service.to_data(original)
        
        assert result is original

    def test_to_data_primitive(self):
        service = SerializationService()
        
        data = service.to_data(42)
        
        assert data is not None
        assert data.total_size() > 0

    def test_to_data_string(self):
        service = SerializationService()
        
        data = service.to_data("hello")
        
        assert data is not None
        assert data.total_size() > 0

    def test_to_data_no_serializer(self):
        service = SerializationService()
        
        class UnknownClass:
            pass
        
        with pytest.raises(SerializationException, match="No serializer found"):
            service.to_data(UnknownClass())

    def test_to_object_none(self):
        service = SerializationService()
        
        result = service.to_object(None)
        
        assert result is None

    def test_to_object_bytes(self):
        service = SerializationService()
        
        data = service.to_data(42)
        result = service.to_object(bytes(data))
        
        assert result == 42

    def test_to_object_not_data(self):
        service = SerializationService()
        
        result = service.to_object("plain string")
        
        assert result == "plain string"

    def test_to_object_empty_data(self):
        service = SerializationService()
        data = Data(b"")
        
        result = service.to_object(data)
        
        assert result is None

    def test_to_object_none_type_id(self):
        service = SerializationService()
        data = Data(struct.pack("<i", NONE_TYPE_ID))
        
        result = service.to_object(data)
        
        assert result is None

    def test_to_object_unknown_type_id(self):
        service = SerializationService()
        data = Data(struct.pack("<i", 99999) + b"payload")
        
        with pytest.raises(SerializationException, match="No serializer found for type ID"):
            service.to_object(data)

    def test_roundtrip_int(self):
        service = SerializationService()
        
        original = 12345
        data = service.to_data(original)
        result = service.to_object(data)
        
        assert result == original

    def test_roundtrip_string(self):
        service = SerializationService()
        
        original = "Hello, World!"
        data = service.to_data(original)
        result = service.to_object(data)
        
        assert result == original

    def test_roundtrip_list(self):
        service = SerializationService()
        
        original = [1, 2, 3]
        data = service.to_data(original)
        result = service.to_object(data)
        
        assert result == original

    def test_roundtrip_dict(self):
        service = SerializationService()
        
        original = {"key": "value"}
        data = service.to_data(original)
        result = service.to_object(data)
        
        assert result == original

    def test_register_serializer(self):
        service = SerializationService()
        
        class MyClass:
            pass
        
        serializer = MagicMock()
        serializer.type_id = 200
        
        service.register_serializer(MyClass, serializer)
        
        assert service._type_to_serializer[MyClass] is serializer
        assert service._type_id_to_serializer[200] is serializer

    def test_register_compact_serializer(self):
        service = SerializationService()
        
        class MyClass:
            pass
        
        serializer = MagicMock()
        serializer.clazz = MyClass
        
        service.register_compact_serializer(serializer)
        
        assert MyClass in service._compact_classes

    def test_register_class_definition(self):
        service = SerializationService()
        cd = ClassDefinition(1, 1, 0)
        
        service.register_class_definition(cd)
        
        found = service._class_definition_context.lookup(1, 1, 0)
        assert found is cd


class TestIdentifiedDataSerializableSerializer:
    def test_type_id(self):
        serializer = IdentifiedDataSerializableSerializer({})
        assert serializer.type_id == SerializationService.IDENTIFIED_DATA_SERIALIZABLE_ID

    def test_write(self):
        serializer = IdentifiedDataSerializableSerializer({})
        output = MagicMock()
        
        obj = MagicMock(spec=IdentifiedDataSerializable)
        obj.factory_id = 1
        obj.class_id = 2
        
        serializer.write(output, obj)
        
        output.write_int.assert_any_call(1)
        output.write_int.assert_any_call(2)
        obj.write_data.assert_called_once_with(output)

    def test_read(self):
        factory = MagicMock()
        obj = MagicMock(spec=IdentifiedDataSerializable)
        factory.create.return_value = obj
        
        serializer = IdentifiedDataSerializableSerializer({1: factory})
        input_stream = MagicMock()
        input_stream.read_int.side_effect = [1, 2]
        
        result = serializer.read(input_stream)
        
        factory.create.assert_called_once_with(2)
        obj.read_data.assert_called_once_with(input_stream)
        assert result is obj

    def test_read_no_factory(self):
        serializer = IdentifiedDataSerializableSerializer({})
        input_stream = MagicMock()
        input_stream.read_int.side_effect = [1, 2]
        
        with pytest.raises(SerializationException, match="No factory registered"):
            serializer.read(input_stream)

    def test_read_factory_returns_none(self):
        factory = MagicMock()
        factory.create.return_value = None
        
        serializer = IdentifiedDataSerializableSerializer({1: factory})
        input_stream = MagicMock()
        input_stream.read_int.side_effect = [1, 2]
        
        with pytest.raises(SerializationException, match="returned None"):
            serializer.read(input_stream)


class TestPortableSerializer:
    def test_type_id(self):
        serializer = PortableSerializer(0, {})
        assert serializer.type_id == SerializationService.PORTABLE_ID

    def test_context_property(self):
        ctx = ClassDefinitionContext(0)
        serializer = PortableSerializer(0, {}, ctx)
        
        assert serializer.context is ctx

    def test_register_class_definition(self):
        serializer = PortableSerializer(0, {})
        cd = ClassDefinition(1, 1, 0)
        
        serializer.register_class_definition(cd)
        
        found = serializer._context.lookup(1, 1, 0)
        assert found is cd

    def test_get_or_create_class_definition_existing(self):
        serializer = PortableSerializer(0, {})
        cd = ClassDefinition(1, 1, 0)
        serializer.register_class_definition(cd)
        
        portable = MagicMock(spec=Portable)
        portable.factory_id = 1
        portable.class_id = 1
        
        result = serializer.get_or_create_class_definition(portable)
        
        assert result is cd

    def test_get_or_create_class_definition_new(self):
        serializer = PortableSerializer(0, {})
        
        portable = MagicMock(spec=Portable)
        portable.factory_id = 1
        portable.class_id = 1
        
        result = serializer.get_or_create_class_definition(portable)
        
        assert result.factory_id == 1
        assert result.class_id == 1


class TestDefaultPortableWriter:
    def create_writer(self):
        serializer = MagicMock()
        output = MagicMock()
        class_def = ClassDefinition(1, 1, 0)
        return DefaultPortableWriter(serializer, output, class_def), output

    def test_write_int(self):
        writer, output = self.create_writer()
        
        writer.write_int("age", 25)
        
        output.write_int.assert_called_with(25)

    def test_write_long(self):
        writer, output = self.create_writer()
        
        writer.write_long("timestamp", 123456789)
        
        output.write_long.assert_called_with(123456789)

    def test_write_string(self):
        writer, output = self.create_writer()
        
        writer.write_string("name", "Alice")
        
        output.write_string.assert_called_with("Alice")

    def test_write_boolean(self):
        writer, output = self.create_writer()
        
        writer.write_boolean("active", True)
        
        output.write_boolean.assert_called_with(True)

    def test_write_float(self):
        writer, output = self.create_writer()
        
        writer.write_float("score", 3.14)
        
        output.write_float.assert_called_with(3.14)

    def test_write_double(self):
        writer, output = self.create_writer()
        
        writer.write_double("precise", 3.14159265359)
        
        output.write_double.assert_called_with(3.14159265359)

    def test_write_byte_array(self):
        writer, output = self.create_writer()
        
        writer.write_byte_array("data", b"\x01\x02\x03")
        
        output.write_byte_array.assert_called_with(b"\x01\x02\x03")

    def test_write_portable_null(self):
        writer, output = self.create_writer()
        
        writer.write_portable("nested", None)
        
        output.write_boolean.assert_called_with(True)

    def test_write_portable_array_null(self):
        writer, output = self.create_writer()
        
        writer.write_portable_array("items", None)
        
        output.write_int.assert_called_with(-1)

    def test_write_duplicate_field_raises(self):
        writer, output = self.create_writer()
        
        writer.write_int("age", 25)
        
        with pytest.raises(SerializationException, match="already written"):
            writer.write_int("age", 30)

    def test_write_type_mismatch_raises(self):
        serializer = MagicMock()
        output = MagicMock()
        class_def = ClassDefinition(1, 1, 0)
        class_def.add_field(FieldDefinition(0, "age", FieldType.STRING))
        writer = DefaultPortableWriter(serializer, output, class_def)
        
        with pytest.raises(SerializationException, match="type mismatch"):
            writer.write_int("age", 25)


class TestDefaultPortableReader:
    def create_reader(self):
        serializer = MagicMock()
        input_stream = MagicMock()
        class_def = ClassDefinition(1, 1, 0)
        return DefaultPortableReader(serializer, input_stream, class_def), input_stream

    def test_read_int(self):
        reader, inp = self.create_reader()
        inp.read_int.return_value = 25
        
        result = reader.read_int("age")
        
        assert result == 25

    def test_read_long(self):
        reader, inp = self.create_reader()
        inp.read_long.return_value = 123456789
        
        result = reader.read_long("timestamp")
        
        assert result == 123456789

    def test_read_string(self):
        reader, inp = self.create_reader()
        inp.read_string.return_value = "Alice"
        
        result = reader.read_string("name")
        
        assert result == "Alice"

    def test_read_boolean(self):
        reader, inp = self.create_reader()
        inp.read_boolean.return_value = True
        
        result = reader.read_boolean("active")
        
        assert result is True

    def test_read_float(self):
        reader, inp = self.create_reader()
        inp.read_float.return_value = 3.14
        
        result = reader.read_float("score")
        
        assert abs(result - 3.14) < 0.001

    def test_read_double(self):
        reader, inp = self.create_reader()
        inp.read_double.return_value = 3.14159265359
        
        result = reader.read_double("precise")
        
        assert abs(result - 3.14159265359) < 0.0000001

    def test_read_byte_array(self):
        reader, inp = self.create_reader()
        inp.read_byte_array.return_value = b"\x01\x02\x03"
        
        result = reader.read_byte_array("data")
        
        assert result == b"\x01\x02\x03"

    def test_read_portable_null(self):
        reader, inp = self.create_reader()
        reader._class_def.add_field(
            FieldDefinition(0, "nested", FieldType.PORTABLE, 1, 1, 0)
        )
        inp.read_boolean.return_value = True
        
        result = reader.read_portable("nested")
        
        assert result is None

    def test_read_portable_array_empty(self):
        reader, inp = self.create_reader()
        reader._class_def.add_field(
            FieldDefinition(0, "items", FieldType.PORTABLE_ARRAY, 1, 1, 0)
        )
        inp.read_int.return_value = -1
        
        result = reader.read_portable_array("items")
        
        assert result == []

    def test_read_type_mismatch_raises(self):
        serializer = MagicMock()
        inp = MagicMock()
        class_def = ClassDefinition(1, 1, 0)
        class_def.add_field(FieldDefinition(0, "age", FieldType.STRING))
        reader = DefaultPortableReader(serializer, inp, class_def)
        
        with pytest.raises(SerializationException, match="type mismatch"):
            reader.read_int("age")


class TestSerializationServiceInternals:
    def test_write_object_none(self):
        service = SerializationService()
        output = ObjectDataOutputImpl(service)
        
        service._write_object(output, None)
        
        result = output.to_byte_array()
        type_id = struct.unpack("<i", result[:4])[0]
        assert type_id == NONE_TYPE_ID

    def test_write_object_no_serializer(self):
        service = SerializationService()
        output = ObjectDataOutputImpl(service)
        
        class UnknownClass:
            pass
        
        with pytest.raises(SerializationException, match="No serializer"):
            service._write_object(output, UnknownClass())

    def test_read_object_none(self):
        service = SerializationService()
        buffer = struct.pack("<i", NONE_TYPE_ID)
        inp = ObjectDataInputImpl(buffer, service)
        
        result = service._read_object(inp)
        
        assert result is None

    def test_read_object_unknown_type(self):
        service = SerializationService()
        buffer = struct.pack("<i", 99999)
        inp = ObjectDataInputImpl(buffer, service)
        
        with pytest.raises(SerializationException, match="No serializer"):
            service._read_object(inp)

    def test_is_compact_object_generic_record(self):
        service = SerializationService()
        
        from hazelcast.serialization.compact import GenericRecord
        record = MagicMock(spec=GenericRecord)
        
        assert service._is_compact_object(record) is True

    def test_is_compact_object_registered_class(self):
        service = SerializationService()
        
        class MyClass:
            pass
        
        service._compact_classes.add(MyClass)
        
        obj = MyClass()
        assert service._is_compact_object(obj) is True

    def test_is_compact_object_not_compact(self):
        service = SerializationService()
        
        assert service._is_compact_object("string") is False
        assert service._is_compact_object(42) is False


class TestConstants:
    def test_type_id_size(self):
        assert TYPE_ID_SIZE == 4

    def test_data_offset(self):
        assert DATA_OFFSET == TYPE_ID_SIZE


class TestCompactIntegration:
    """Tests for compact serialization integration with SerializationService."""

    def test_register_and_use_compact_serializer(self):
        from hazelcast.serialization.api import CompactSerializer, CompactWriter, CompactReader
        
        class Product:
            def __init__(self, name="", price=0.0):
                self.name = name
                self.price = price
        
        class ProductSerializer(CompactSerializer[Product]):
            @property
            def type_name(self) -> str:
                return "Product"
            
            @property
            def clazz(self) -> type:
                return Product
            
            def write(self, writer: CompactWriter, obj: Product) -> None:
                writer.write_string("name", obj.name)
                writer.write_float64("price", obj.price)
            
            def read(self, reader: CompactReader) -> Product:
                return Product(
                    name=reader.read_string("name"),
                    price=reader.read_float64("price"),
                )
        
        service = SerializationService(compact_serializers=[ProductSerializer()])
        
        product = Product(name="Widget", price=19.99)
        data = service.to_data(product)
        
        assert data is not None
        assert data.get_type_id() == SerializationService.COMPACT_TYPE_ID

    def test_serialize_generic_record_through_service(self):
        from hazelcast.serialization.compact import GenericRecord, GenericRecordBuilder
        
        record = (GenericRecordBuilder("TestRecord")
                  .set_string("name", "Test")
                  .set_int32("value", 42)
                  .build())
        
        service = SerializationService()
        data = service.to_data(record)
        
        assert data is not None
        assert data.get_type_id() == SerializationService.COMPACT_TYPE_ID

    def test_deserialize_compact_to_generic_record(self):
        from hazelcast.serialization.api import CompactSerializer, CompactWriter, CompactReader
        from hazelcast.serialization.compact import GenericRecord
        
        class Item:
            def __init__(self, id=0, name=""):
                self.id = id
                self.name = name
        
        class ItemSerializer(CompactSerializer[Item]):
            @property
            def type_name(self) -> str:
                return "Item"
            
            @property
            def clazz(self) -> type:
                return Item
            
            def write(self, writer: CompactWriter, obj: Item) -> None:
                writer.write_int32("id", obj.id)
                writer.write_string("name", obj.name)
            
            def read(self, reader: CompactReader) -> Item:
                return Item(
                    id=reader.read_int32("id"),
                    name=reader.read_string("name"),
                )
        
        service = SerializationService(compact_serializers=[ItemSerializer()])
        
        item = Item(id=1, name="Test Item")
        data = service.to_data(item)
        
        result = service.to_object(data)
        
        assert isinstance(result, GenericRecord)

    def test_compact_service_property(self):
        service = SerializationService()
        
        assert service.compact_service is not None


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_register_same_serializer_twice(self):
        service = SerializationService()
        
        class MyClass:
            pass
        
        class MySerializer(Serializer[MyClass]):
            @property
            def type_id(self) -> int:
                return 3000
            
            def write(self, output, obj):
                pass
            
            def read(self, input_stream):
                return MyClass()
        
        serializer = MySerializer()
        service.register_serializer(MyClass, serializer)
        service.register_serializer(MyClass, serializer)
        
        assert service._type_to_serializer[MyClass] is serializer

    def test_empty_class_definition(self):
        cd = ClassDefinition(1, 1, 0)
        
        assert cd.get_field_count() == 0
        assert cd.get_field_names() == []
        assert cd.fields == []

    def test_class_definition_builder_all_array_types(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_boolean_array_field("bools")
        builder.add_int_array_field("ints")
        builder.add_long_array_field("longs")
        builder.add_string_array_field("strings")
        
        cd = builder.build()
        
        assert cd.get_field_type("bools") == FieldType.BOOLEAN_ARRAY
        assert cd.get_field_type("ints") == FieldType.INT_ARRAY
        assert cd.get_field_type("longs") == FieldType.LONG_ARRAY
        assert cd.get_field_type("strings") == FieldType.STRING_ARRAY

    def test_data_with_only_type_id(self):
        buffer = struct.pack("<i", 42)
        data = Data(buffer)
        
        assert data.get_type_id() == 42
        assert data.get_payload() == b""
        assert data.total_size() == 4

    def test_portable_writer_with_non_null_portable(self):
        serializer = MagicMock()
        output = MagicMock()
        class_def = ClassDefinition(1, 1, 0)
        writer = DefaultPortableWriter(serializer, output, class_def)
        
        nested = MagicMock(spec=Portable)
        writer.write_portable("nested", nested)
        
        output.write_boolean.assert_called_with(False)
        serializer.write_internal.assert_called_once()

    def test_portable_writer_array_with_items(self):
        serializer = MagicMock()
        output = MagicMock()
        class_def = ClassDefinition(1, 1, 0)
        writer = DefaultPortableWriter(serializer, output, class_def)
        
        items = [MagicMock(spec=Portable), MagicMock(spec=Portable)]
        writer.write_portable_array("items", items)
        
        output.write_int.assert_called_with(2)
        assert serializer.write_internal.call_count == 2

    def test_portable_reader_array_with_items(self):
        serializer = MagicMock()
        input_stream = MagicMock()
        class_def = ClassDefinition(1, 1, 0)
        class_def.add_field(FieldDefinition(0, "items", FieldType.PORTABLE_ARRAY, 1, 2, 0))
        reader = DefaultPortableReader(serializer, input_stream, class_def)
        
        input_stream.read_int.return_value = 2
        serializer.read_internal.side_effect = [MagicMock(), MagicMock()]
        
        result = reader.read_portable_array("items")
        
        assert len(result) == 2

    def test_portable_reader_non_null_portable(self):
        serializer = MagicMock()
        input_stream = MagicMock()
        class_def = ClassDefinition(1, 1, 0)
        class_def.add_field(FieldDefinition(0, "nested", FieldType.PORTABLE, 1, 2, 0))
        reader = DefaultPortableReader(serializer, input_stream, class_def)
        
        input_stream.read_boolean.return_value = False
        nested = MagicMock(spec=Portable)
        serializer.read_internal.return_value = nested
        
        result = reader.read_portable("nested")
        
        assert result is nested

    def test_serialization_service_thread_safety(self):
        import threading
        
        service = SerializationService()
        errors = []
        
        def register_and_use():
            try:
                class LocalClass:
                    pass
                
                class LocalSerializer(Serializer[LocalClass]):
                    @property
                    def type_id(self) -> int:
                        return threading.current_thread().ident % 10000 + 5000
                    
                    def write(self, output, obj):
                        pass
                    
                    def read(self, input_stream):
                        return LocalClass()
                
                service.register_serializer(LocalClass, LocalSerializer())
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=register_and_use) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0

    def test_nested_object_serialization(self):
        service = SerializationService()
        
        nested = {"level1": {"level2": {"level3": "deep"}}}
        data = service.to_data(nested)
        result = service.to_object(data)
        
        assert result == nested

    def test_list_with_mixed_types(self):
        service = SerializationService()
        
        mixed = [1, "two", 3.0, True, None]
        data = service.to_data(mixed)
        result = service.to_object(data)
        
        assert result == mixed
