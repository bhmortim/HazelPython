"""Comprehensive tests for serialization service."""

import struct
import uuid
from datetime import datetime, date, time
from decimal import Decimal
from typing import Any, Dict

import pytest

from hazelcast.serialization.api import (
    IdentifiedDataSerializable,
    ObjectDataInput,
    ObjectDataOutput,
    Portable,
    PortableReader,
    PortableWriter,
)
from hazelcast.serialization.service import (
    SerializationService,
    Data,
    ObjectDataInputImpl,
    ObjectDataOutputImpl,
    ClassDefinition,
    ClassDefinitionBuilder,
    FieldType,
    FieldDefinition,
    ClassDefinitionContext,
)
from hazelcast.serialization.builtin import (
    NONE_TYPE_ID,
    INT_TYPE_ID,
    STRING_TYPE_ID,
    DATETIME_TYPE_ID,
    BIG_DECIMAL_TYPE_ID,
)
from hazelcast.exceptions import SerializationException


class SampleIdentifiedDataSerializable(IdentifiedDataSerializable):
    """Sample IdentifiedDataSerializable for testing."""

    FACTORY_ID = 1
    CLASS_ID = 1

    def __init__(self, id: int = 0, name: str = "", value: float = 0.0):
        self.id = id
        self.name = name
        self.value = value

    @property
    def factory_id(self) -> int:
        return self.FACTORY_ID

    @property
    def class_id(self) -> int:
        return self.CLASS_ID

    def write_data(self, output: ObjectDataOutput) -> None:
        output.write_int(self.id)
        output.write_string(self.name)
        output.write_double(self.value)

    def read_data(self, input: ObjectDataInput) -> None:
        self.id = input.read_int()
        self.name = input.read_string()
        self.value = input.read_double()


class NestedIdentifiedDataSerializable(IdentifiedDataSerializable):
    """Nested IdentifiedDataSerializable for testing."""

    FACTORY_ID = 1
    CLASS_ID = 2

    def __init__(self, data: str = "", nested: SampleIdentifiedDataSerializable = None):
        self.data = data
        self.nested = nested

    @property
    def factory_id(self) -> int:
        return self.FACTORY_ID

    @property
    def class_id(self) -> int:
        return self.CLASS_ID

    def write_data(self, output: ObjectDataOutput) -> None:
        output.write_string(self.data)
        output.write_object(self.nested)

    def read_data(self, input: ObjectDataInput) -> None:
        self.data = input.read_string()
        self.nested = input.read_object()


class SampleDataSerializableFactory:
    """Factory for creating IdentifiedDataSerializable instances."""

    def create(self, class_id: int) -> IdentifiedDataSerializable:
        if class_id == SampleIdentifiedDataSerializable.CLASS_ID:
            return SampleIdentifiedDataSerializable()
        elif class_id == NestedIdentifiedDataSerializable.CLASS_ID:
            return NestedIdentifiedDataSerializable()
        return None


class SamplePortable(Portable):
    """Sample Portable for testing."""

    FACTORY_ID = 2
    CLASS_ID = 1

    def __init__(self, id: int = 0, name: str = "", active: bool = False):
        self.id = id
        self.name = name
        self.active = active

    @property
    def factory_id(self) -> int:
        return self.FACTORY_ID

    @property
    def class_id(self) -> int:
        return self.CLASS_ID

    def write_portable(self, writer: PortableWriter) -> None:
        writer.write_int("id", self.id)
        writer.write_string("name", self.name)
        writer.write_boolean("active", self.active)

    def read_portable(self, reader: PortableReader) -> None:
        self.id = reader.read_int("id")
        self.name = reader.read_string("name")
        self.active = reader.read_boolean("active")


class NestedPortable(Portable):
    """Nested Portable for testing."""

    FACTORY_ID = 2
    CLASS_ID = 2

    def __init__(self, label: str = "", child: SamplePortable = None):
        self.label = label
        self.child = child

    @property
    def factory_id(self) -> int:
        return self.FACTORY_ID

    @property
    def class_id(self) -> int:
        return self.CLASS_ID

    def write_portable(self, writer: PortableWriter) -> None:
        writer.write_string("label", self.label)
        writer.write_portable("child", self.child)

    def read_portable(self, reader: PortableReader) -> None:
        self.label = reader.read_string("label")
        self.child = reader.read_portable("child")


class SamplePortableFactory:
    """Factory for creating Portable instances."""

    def create(self, class_id: int) -> Portable:
        if class_id == SamplePortable.CLASS_ID:
            return SamplePortable()
        elif class_id == NestedPortable.CLASS_ID:
            return NestedPortable()
        return None


class TestData:
    """Tests for Data class."""

    def test_empty_buffer(self):
        data = Data(b"")
        assert data.total_size() == 0
        assert len(data) == 0
        assert data.get_type_id() == NONE_TYPE_ID

    def test_type_id_extraction(self):
        buffer = struct.pack("<i", INT_TYPE_ID) + b"\x00\x00\x00\x2A"
        data = Data(buffer)
        assert data.get_type_id() == INT_TYPE_ID

    def test_payload_extraction(self):
        payload = b"\x00\x00\x00\x2A"
        buffer = struct.pack("<i", INT_TYPE_ID) + payload
        data = Data(buffer)
        assert data.get_payload() == payload

    def test_equality(self):
        buffer = b"\x01\x02\x03\x04"
        data1 = Data(buffer)
        data2 = Data(buffer)
        data3 = Data(b"\x05\x06\x07\x08")
        assert data1 == data2
        assert data1 != data3

    def test_hash(self):
        buffer = b"\x01\x02\x03\x04"
        data1 = Data(buffer)
        data2 = Data(buffer)
        assert hash(data1) == hash(data2)

    def test_bytes_conversion(self):
        buffer = b"\x01\x02\x03\x04"
        data = Data(buffer)
        assert bytes(data) == buffer


class TestObjectDataIO:
    """Tests for ObjectDataInput and ObjectDataOutput implementations."""

    def test_boolean_roundtrip(self):
        service = SerializationService()
        output = ObjectDataOutputImpl(service)
        output.write_boolean(True)
        output.write_boolean(False)

        input_stream = ObjectDataInputImpl(output.to_byte_array(), service)
        assert input_stream.read_boolean() is True
        assert input_stream.read_boolean() is False

    def test_byte_roundtrip(self):
        service = SerializationService()
        output = ObjectDataOutputImpl(service)
        output.write_byte(127)
        output.write_byte(-128)

        input_stream = ObjectDataInputImpl(output.to_byte_array(), service)
        assert input_stream.read_byte() == 127
        assert input_stream.read_byte() == -128

    def test_short_roundtrip(self):
        service = SerializationService()
        output = ObjectDataOutputImpl(service)
        output.write_short(32767)
        output.write_short(-32768)

        input_stream = ObjectDataInputImpl(output.to_byte_array(), service)
        assert input_stream.read_short() == 32767
        assert input_stream.read_short() == -32768

    def test_int_roundtrip(self):
        service = SerializationService()
        output = ObjectDataOutputImpl(service)
        output.write_int(2147483647)
        output.write_int(-2147483648)

        input_stream = ObjectDataInputImpl(output.to_byte_array(), service)
        assert input_stream.read_int() == 2147483647
        assert input_stream.read_int() == -2147483648

    def test_long_roundtrip(self):
        service = SerializationService()
        output = ObjectDataOutputImpl(service)
        output.write_long(9223372036854775807)
        output.write_long(-9223372036854775808)

        input_stream = ObjectDataInputImpl(output.to_byte_array(), service)
        assert input_stream.read_long() == 9223372036854775807
        assert input_stream.read_long() == -9223372036854775808

    def test_float_roundtrip(self):
        service = SerializationService()
        output = ObjectDataOutputImpl(service)
        output.write_float(3.14)

        input_stream = ObjectDataInputImpl(output.to_byte_array(), service)
        assert abs(input_stream.read_float() - 3.14) < 0.001

    def test_double_roundtrip(self):
        service = SerializationService()
        output = ObjectDataOutputImpl(service)
        output.write_double(3.141592653589793)

        input_stream = ObjectDataInputImpl(output.to_byte_array(), service)
        assert input_stream.read_double() == 3.141592653589793

    def test_string_roundtrip(self):
        service = SerializationService()
        output = ObjectDataOutputImpl(service)
        output.write_string("Hello, World!")
        output.write_string("")
        output.write_string("日本語テスト")

        input_stream = ObjectDataInputImpl(output.to_byte_array(), service)
        assert input_stream.read_string() == "Hello, World!"
        assert input_stream.read_string() == ""
        assert input_stream.read_string() == "日本語テスト"

    def test_byte_array_roundtrip(self):
        service = SerializationService()
        output = ObjectDataOutputImpl(service)
        output.write_byte_array(b"\x01\x02\x03\x04")
        output.write_byte_array(b"")

        input_stream = ObjectDataInputImpl(output.to_byte_array(), service)
        assert input_stream.read_byte_array() == b"\x01\x02\x03\x04"
        assert input_stream.read_byte_array() == b""

    def test_position_operations(self):
        service = SerializationService()
        output = ObjectDataOutputImpl(service)
        output.write_int(100)
        output.write_int(200)

        input_stream = ObjectDataInputImpl(output.to_byte_array(), service)
        assert input_stream.position() == 0
        input_stream.read_int()
        assert input_stream.position() == 4
        input_stream.set_position(0)
        assert input_stream.position() == 0
        assert input_stream.read_int() == 100


class TestSerializationServicePrimitives:
    """Tests for primitive type serialization."""

    def test_none_serialization(self):
        service = SerializationService()
        data = service.to_data(None)
        result = service.to_object(data)
        assert result is None

    def test_bool_serialization(self):
        service = SerializationService()
        for value in [True, False]:
            data = service.to_data(value)
            result = service.to_object(data)
            assert result == value

    def test_int_serialization(self):
        service = SerializationService()
        for value in [0, 1, -1, 2147483647, -2147483648]:
            data = service.to_data(value)
            result = service.to_object(data)
            assert result == value

    def test_float_serialization(self):
        service = SerializationService()
        for value in [0.0, 1.5, -1.5, 3.141592653589793]:
            data = service.to_data(value)
            result = service.to_object(data)
            assert result == value

    def test_string_serialization(self):
        service = SerializationService()
        for value in ["", "hello", "世界", "emoji: 🎉"]:
            data = service.to_data(value)
            result = service.to_object(data)
            assert result == value

    def test_bytes_serialization(self):
        service = SerializationService()
        for value in [b"", b"\x00\x01\x02", b"hello"]:
            data = service.to_data(value)
            result = service.to_object(data)
            assert result == value

    def test_list_serialization(self):
        service = SerializationService()
        original = [1, "two", 3.0, True, None]
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == original

    def test_nested_list_serialization(self):
        service = SerializationService()
        original = [[1, 2], [3, 4], ["a", "b"]]
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == original

    def test_dict_serialization(self):
        service = SerializationService()
        original = {"name": "test", "value": 42, "active": True}
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == original

    def test_nested_dict_serialization(self):
        service = SerializationService()
        original = {"outer": {"inner": {"deep": 123}}}
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == original

    def test_uuid_serialization(self):
        service = SerializationService()
        original = uuid.uuid4()
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == original

    def test_datetime_serialization(self):
        service = SerializationService()
        original = datetime(2024, 6, 15, 10, 30, 45, 123456)
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == original

    def test_date_serialization(self):
        service = SerializationService()
        original = date(2024, 6, 15)
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == original

    def test_time_serialization(self):
        service = SerializationService()
        original = time(10, 30, 45, 123456)
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == original

    def test_decimal_serialization(self):
        service = SerializationService()
        for value in [Decimal("0"), Decimal("123.456"), Decimal("-999.999"), Decimal("1E+10")]:
            data = service.to_data(value)
            result = service.to_object(data)
            assert result == value


class TestIdentifiedDataSerializableSerialization:
    """Tests for IdentifiedDataSerializable serialization."""

    def test_simple_roundtrip(self):
        factory = SampleDataSerializableFactory()
        service = SerializationService(
            data_serializable_factories={SampleIdentifiedDataSerializable.FACTORY_ID: factory}
        )

        original = SampleIdentifiedDataSerializable(id=42, name="test", value=3.14)
        data = service.to_data(original)
        result = service.to_object(data)

        assert isinstance(result, SampleIdentifiedDataSerializable)
        assert result.id == original.id
        assert result.name == original.name
        assert result.value == original.value

    def test_nested_roundtrip(self):
        factory = SampleDataSerializableFactory()
        service = SerializationService(
            data_serializable_factories={SampleIdentifiedDataSerializable.FACTORY_ID: factory}
        )

        inner = SampleIdentifiedDataSerializable(id=1, name="inner", value=1.0)
        original = NestedIdentifiedDataSerializable(data="outer", nested=inner)
        data = service.to_data(original)
        result = service.to_object(data)

        assert isinstance(result, NestedIdentifiedDataSerializable)
        assert result.data == original.data
        assert isinstance(result.nested, SampleIdentifiedDataSerializable)
        assert result.nested.id == inner.id
        assert result.nested.name == inner.name

    def test_missing_factory_raises(self):
        service = SerializationService()
        original = SampleIdentifiedDataSerializable(id=1, name="test", value=1.0)
        data = service.to_data(original)

        with pytest.raises(SerializationException):
            service.to_object(data)


class TestPortableSerialization:
    """Tests for Portable serialization."""

    def test_simple_roundtrip(self):
        factory = SamplePortableFactory()
        service = SerializationService(
            portable_factories={SamplePortable.FACTORY_ID: factory}
        )

        original = SamplePortable(id=42, name="test", active=True)
        data = service.to_data(original)
        result = service.to_object(data)

        assert isinstance(result, SamplePortable)
        assert result.id == original.id
        assert result.name == original.name
        assert result.active == original.active

    def test_nested_roundtrip(self):
        factory = SamplePortableFactory()
        service = SerializationService(
            portable_factories={SamplePortable.FACTORY_ID: factory}
        )

        child_def = ClassDefinitionBuilder(SamplePortable.FACTORY_ID, SamplePortable.CLASS_ID, 0) \
            .add_int_field("id") \
            .add_string_field("name") \
            .add_boolean_field("active") \
            .build()

        parent_def = ClassDefinitionBuilder(NestedPortable.FACTORY_ID, NestedPortable.CLASS_ID, 0) \
            .add_string_field("label") \
            .add_portable_field("child", child_def) \
            .build()

        service.register_class_definition(child_def)
        service.register_class_definition(parent_def)

        child = SamplePortable(id=1, name="child", active=True)
        original = NestedPortable(label="parent", child=child)
        data = service.to_data(original)
        result = service.to_object(data)

        assert isinstance(result, NestedPortable)
        assert result.label == original.label
        assert isinstance(result.child, SamplePortable)
        assert result.child.id == child.id

    def test_missing_factory_raises(self):
        service = SerializationService()
        original = SamplePortable(id=1, name="test", active=True)
        data = service.to_data(original)

        with pytest.raises(SerializationException):
            service.to_object(data)


class TestClassDefinition:
    """Tests for ClassDefinition and related classes."""

    def test_field_definition(self):
        field = FieldDefinition(0, "test", FieldType.INT)
        assert field.index == 0
        assert field.name == "test"
        assert field.field_type == FieldType.INT

    def test_class_definition_builder(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_int_field("id")
        builder.add_string_field("name")
        builder.add_boolean_field("active")

        class_def = builder.build()
        assert class_def.factory_id == 1
        assert class_def.class_id == 1
        assert class_def.version == 0
        assert class_def.get_field_count() == 3
        assert class_def.has_field("id")
        assert class_def.has_field("name")
        assert class_def.has_field("active")
        assert not class_def.has_field("nonexistent")

    def test_class_definition_field_types(self):
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_int_field("int_field")
        builder.add_long_field("long_field")
        builder.add_string_field("string_field")
        builder.add_boolean_field("bool_field")
        builder.add_float_field("float_field")
        builder.add_double_field("double_field")
        builder.add_byte_array_field("bytes_field")

        class_def = builder.build()
        assert class_def.get_field_type("int_field") == FieldType.INT
        assert class_def.get_field_type("long_field") == FieldType.LONG
        assert class_def.get_field_type("string_field") == FieldType.STRING
        assert class_def.get_field_type("bool_field") == FieldType.BOOLEAN
        assert class_def.get_field_type("float_field") == FieldType.FLOAT
        assert class_def.get_field_type("double_field") == FieldType.DOUBLE
        assert class_def.get_field_type("bytes_field") == FieldType.BYTE_ARRAY

    def test_class_definition_context(self):
        context = ClassDefinitionContext(0)
        class_def = ClassDefinitionBuilder(1, 1, 0).add_int_field("id").build()

        registered = context.register(class_def)
        assert registered == class_def

        found = context.lookup(1, 1, 0)
        assert found == class_def

        not_found = context.lookup(1, 2, 0)
        assert not_found is None


class TestCustomSerializer:
    """Tests for custom serializer registration."""

    def test_register_custom_serializer(self):
        from hazelcast.serialization.api import Serializer

        class CustomObject:
            def __init__(self, value: int = 0):
                self.value = value

        class CustomSerializer(Serializer):
            @property
            def type_id(self) -> int:
                return 100

            def write(self, output: ObjectDataOutput, obj: CustomObject) -> None:
                output.write_int(obj.value)

            def read(self, input: ObjectDataInput) -> CustomObject:
                return CustomObject(input.read_int())

        service = SerializationService(
            custom_serializers={CustomObject: CustomSerializer()}
        )

        original = CustomObject(42)
        data = service.to_data(original)
        result = service.to_object(data)

        assert isinstance(result, CustomObject)
        assert result.value == original.value


class TestDataPassthrough:
    """Tests for Data object passthrough."""

    def test_data_to_data_returns_same(self):
        service = SerializationService()
        original_data = service.to_data("test")
        result = service.to_data(original_data)
        assert result is original_data

    def test_bytes_to_object(self):
        service = SerializationService()
        data = service.to_data(42)
        buffer = bytes(data)
        result = service.to_object(buffer)
        assert result == 42


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_string(self):
        service = SerializationService()
        data = service.to_data("")
        result = service.to_object(data)
        assert result == ""

    def test_empty_list(self):
        service = SerializationService()
        data = service.to_data([])
        result = service.to_object(data)
        assert result == []

    def test_empty_dict(self):
        service = SerializationService()
        data = service.to_data({})
        result = service.to_object(data)
        assert result == {}

    def test_tuple_converted_to_list(self):
        service = SerializationService()
        original = (1, 2, 3)
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == [1, 2, 3]

    def test_unknown_type_raises(self):
        service = SerializationService()

        class UnknownType:
            pass

        with pytest.raises(SerializationException):
            service.to_data(UnknownType())
