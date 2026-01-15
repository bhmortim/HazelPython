"""Unit tests for serialization implementation."""

import unittest
import uuid
from dataclasses import dataclass
from typing import Any

from hazelcast.serialization.api import (
    ObjectDataInput,
    ObjectDataOutput,
    Serializer,
    IdentifiedDataSerializable,
    CompactSerializer,
    CompactReader,
    CompactWriter,
)
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
    get_builtin_serializers,
)
from hazelcast.serialization.compact import (
    CompactSerializationService,
    DefaultCompactReader,
    DefaultCompactWriter,
    Schema,
    ReflectiveCompactSerializer,
)


class TestData(unittest.TestCase):
    """Tests for Data class."""

    def test_empty_data(self):
        """Test empty data handling."""
        data = Data(b"")
        self.assertEqual(data.total_size(), 0)
        self.assertEqual(len(data), 0)

    def test_data_with_type_id(self):
        """Test data with type ID."""
        import struct
        buffer = struct.pack("<i", 42) + b"payload"
        data = Data(buffer)

        self.assertEqual(data.get_type_id(), 42)
        self.assertEqual(data.get_payload(), b"payload")

    def test_data_equality(self):
        """Test data equality comparison."""
        data1 = Data(b"test")
        data2 = Data(b"test")
        data3 = Data(b"other")

        self.assertEqual(data1, data2)
        self.assertNotEqual(data1, data3)

    def test_data_hash(self):
        """Test data hashing."""
        data1 = Data(b"test")
        data2 = Data(b"test")

        self.assertEqual(hash(data1), hash(data2))


class TestObjectDataIO(unittest.TestCase):
    """Tests for ObjectDataInput and ObjectDataOutput."""

    def setUp(self):
        """Set up test fixtures."""
        self.service = SerializationService()

    def test_write_read_boolean(self):
        """Test boolean serialization."""
        output = ObjectDataOutputImpl(self.service)
        output.write_boolean(True)
        output.write_boolean(False)

        input_stream = ObjectDataInputImpl(output.to_byte_array(), self.service)
        self.assertTrue(input_stream.read_boolean())
        self.assertFalse(input_stream.read_boolean())

    def test_write_read_byte(self):
        """Test byte serialization."""
        output = ObjectDataOutputImpl(self.service)
        output.write_byte(127)
        output.write_byte(-128)

        input_stream = ObjectDataInputImpl(output.to_byte_array(), self.service)
        self.assertEqual(input_stream.read_byte(), 127)
        self.assertEqual(input_stream.read_byte(), -128)

    def test_write_read_int(self):
        """Test integer serialization."""
        output = ObjectDataOutputImpl(self.service)
        output.write_int(12345)
        output.write_int(-67890)

        input_stream = ObjectDataInputImpl(output.to_byte_array(), self.service)
        self.assertEqual(input_stream.read_int(), 12345)
        self.assertEqual(input_stream.read_int(), -67890)

    def test_write_read_long(self):
        """Test long serialization."""
        output = ObjectDataOutputImpl(self.service)
        output.write_long(9223372036854775807)
        output.write_long(-9223372036854775808)

        input_stream = ObjectDataInputImpl(output.to_byte_array(), self.service)
        self.assertEqual(input_stream.read_long(), 9223372036854775807)
        self.assertEqual(input_stream.read_long(), -9223372036854775808)

    def test_write_read_float(self):
        """Test float serialization."""
        output = ObjectDataOutputImpl(self.service)
        output.write_float(3.14)

        input_stream = ObjectDataInputImpl(output.to_byte_array(), self.service)
        self.assertAlmostEqual(input_stream.read_float(), 3.14, places=5)

    def test_write_read_double(self):
        """Test double serialization."""
        output = ObjectDataOutputImpl(self.service)
        output.write_double(3.141592653589793)

        input_stream = ObjectDataInputImpl(output.to_byte_array(), self.service)
        self.assertAlmostEqual(
            input_stream.read_double(), 3.141592653589793, places=10
        )

    def test_write_read_string(self):
        """Test string serialization."""
        output = ObjectDataOutputImpl(self.service)
        output.write_string("Hello, World!")
        output.write_string("")

        input_stream = ObjectDataInputImpl(output.to_byte_array(), self.service)
        self.assertEqual(input_stream.read_string(), "Hello, World!")
        self.assertEqual(input_stream.read_string(), "")

    def test_write_read_byte_array(self):
        """Test byte array serialization."""
        output = ObjectDataOutputImpl(self.service)
        output.write_byte_array(b"\x00\x01\x02\x03")

        input_stream = ObjectDataInputImpl(output.to_byte_array(), self.service)
        self.assertEqual(input_stream.read_byte_array(), b"\x00\x01\x02\x03")

    def test_position_tracking(self):
        """Test position tracking in input stream."""
        output = ObjectDataOutputImpl(self.service)
        output.write_int(42)
        output.write_int(84)

        input_stream = ObjectDataInputImpl(output.to_byte_array(), self.service)
        self.assertEqual(input_stream.position(), 0)

        input_stream.read_int()
        self.assertEqual(input_stream.position(), 4)

        input_stream.set_position(0)
        self.assertEqual(input_stream.read_int(), 42)


class TestSerializationService(unittest.TestCase):
    """Tests for SerializationService."""

    def setUp(self):
        """Set up test fixtures."""
        self.service = SerializationService()

    def test_serialize_none(self):
        """Test None serialization."""
        data = self.service.to_data(None)
        result = self.service.to_object(data)
        self.assertIsNone(result)

    def test_serialize_boolean(self):
        """Test boolean serialization."""
        data = self.service.to_data(True)
        result = self.service.to_object(data)
        self.assertTrue(result)

        data = self.service.to_data(False)
        result = self.service.to_object(data)
        self.assertFalse(result)

    def test_serialize_integer(self):
        """Test integer serialization."""
        for value in [0, 1, -1, 2147483647, -2147483648]:
            data = self.service.to_data(value)
            result = self.service.to_object(data)
            self.assertEqual(result, value)

    def test_serialize_float(self):
        """Test float serialization."""
        data = self.service.to_data(3.14)
        result = self.service.to_object(data)
        self.assertAlmostEqual(result, 3.14, places=5)

    def test_serialize_string(self):
        """Test string serialization."""
        test_strings = ["", "hello", "Hello, World!", "Unicode: \u00e9\u00e8"]
        for value in test_strings:
            data = self.service.to_data(value)
            result = self.service.to_object(data)
            self.assertEqual(result, value)

    def test_serialize_bytes(self):
        """Test bytes serialization."""
        value = b"\x00\x01\x02\xff"
        data = self.service.to_data(value)
        result = self.service.to_object(data)
        self.assertEqual(result, value)

    def test_serialize_list(self):
        """Test list serialization."""
        value = [1, "two", 3.0, None, True]
        data = self.service.to_data(value)
        result = self.service.to_object(data)
        self.assertEqual(result, value)

    def test_serialize_nested_list(self):
        """Test nested list serialization."""
        value = [[1, 2], [3, [4, 5]]]
        data = self.service.to_data(value)
        result = self.service.to_object(data)
        self.assertEqual(result, value)

    def test_serialize_dict(self):
        """Test dictionary serialization."""
        value = {"key1": "value1", "key2": 42, "key3": True}
        data = self.service.to_data(value)
        result = self.service.to_object(data)
        self.assertEqual(result, value)

    def test_serialize_nested_dict(self):
        """Test nested dictionary serialization."""
        value = {"outer": {"inner": {"deep": 42}}}
        data = self.service.to_data(value)
        result = self.service.to_object(data)
        self.assertEqual(result, value)

    def test_data_passthrough(self):
        """Test Data objects pass through unchanged."""
        original = self.service.to_data("test")
        result = self.service.to_data(original)
        self.assertIs(result, original)

    def test_bytes_to_object(self):
        """Test deserializing raw bytes."""
        data = self.service.to_data("test")
        result = self.service.to_object(bytes(data))
        self.assertEqual(result, "test")


class TestBuiltinSerializers(unittest.TestCase):
    """Tests for built-in serializers."""

    def test_get_builtin_serializers(self):
        """Test getting all builtin serializers."""
        serializers = get_builtin_serializers()

        self.assertIsInstance(serializers, dict)
        self.assertGreater(len(serializers), 0)

    def test_serializer_type_ids(self):
        """Test serializers have correct type IDs."""
        self.assertEqual(NoneSerializer().type_id, 0)
        self.assertEqual(BoolSerializer().type_id, -1)
        self.assertEqual(IntSerializer().type_id, -4)
        self.assertEqual(StringSerializer().type_id, -8)


class TestCustomSerializer(unittest.TestCase):
    """Tests for custom serializer registration."""

    def test_register_custom_serializer(self):
        """Test registering a custom serializer."""

        class CustomObject:
            def __init__(self, value: int):
                self.value = value

        class CustomSerializer(Serializer):
            @property
            def type_id(self) -> int:
                return 1000

            def write(self, output: ObjectDataOutput, obj: CustomObject) -> None:
                output.write_int(obj.value)

            def read(self, input_data: ObjectDataInput) -> CustomObject:
                return CustomObject(input_data.read_int())

        service = SerializationService(
            custom_serializers={CustomObject: CustomSerializer()}
        )

        original = CustomObject(42)
        data = service.to_data(original)
        result = service.to_object(data)

        self.assertIsInstance(result, CustomObject)
        self.assertEqual(result.value, 42)


class TestCompactSerialization(unittest.TestCase):
    """Tests for compact serialization."""

    def test_schema_creation(self):
        """Test schema creation."""
        schema = Schema(type_name="TestType")
        self.assertEqual(schema.type_name, "TestType")
        self.assertNotEqual(schema.schema_id, 0)

    def test_compact_writer_reader(self):
        """Test compact writer and reader."""
        schema = Schema(type_name="TestType")
        writer = DefaultCompactWriter(schema)

        writer.write_boolean("bool_field", True)
        writer.write_int32("int_field", 42)
        writer.write_string("string_field", "hello")

        reader = DefaultCompactReader(schema, writer.fields)

        self.assertTrue(reader.read_boolean("bool_field"))
        self.assertEqual(reader.read_int32("int_field"), 42)
        self.assertEqual(reader.read_string("string_field"), "hello")

    def test_compact_service_registration(self):
        """Test compact serializer registration."""

        @dataclass
        class Person:
            name: str
            age: int

        class PersonSerializer(CompactSerializer):
            @property
            def type_name(self) -> str:
                return "Person"

            @property
            def clazz(self) -> type:
                return Person

            def write(self, writer: CompactWriter, obj: Person) -> None:
                writer.write_string("name", obj.name)
                writer.write_int32("age", obj.age)

            def read(self, reader: CompactReader) -> Person:
                return Person(
                    name=reader.read_string("name"),
                    age=reader.read_int32("age"),
                )

        service = CompactSerializationService()
        service.register_serializer(PersonSerializer())

        person = Person(name="Alice", age=30)
        data = service.serialize(person)
        result = service.deserialize(data, "Person")

        self.assertEqual(result.name, "Alice")
        self.assertEqual(result.age, 30)

    def test_reflective_compact_serializer(self):
        """Test reflective compact serializer with dataclass."""

        @dataclass
        class Product:
            id: int
            name: str
            price: float
            in_stock: bool

        serializer = ReflectiveCompactSerializer(Product)
        schema = Schema(type_name="Product")

        product = Product(id=1, name="Widget", price=9.99, in_stock=True)
        writer = DefaultCompactWriter(schema)
        serializer.write(writer, product)

        reader = DefaultCompactReader(schema, writer.fields)
        result = serializer.read(reader)

        self.assertEqual(result.id, 1)
        self.assertEqual(result.name, "Widget")
        self.assertAlmostEqual(result.price, 9.99, places=2)
        self.assertTrue(result.in_stock)


class TestSerializationConfig(unittest.TestCase):
    """Tests for serialization configuration."""

    def test_custom_serializers_from_config(self):
        """Test custom serializers are loaded from config."""
        from hazelcast.config import SerializationConfig

        config = SerializationConfig()
        self.assertEqual(config.custom_serializers, {})

        class DummySerializer:
            pass

        class DummyClass:
            pass

        config.add_custom_serializer(DummyClass, DummySerializer())
        self.assertIn(DummyClass, config.custom_serializers)

    def test_compact_serializers_from_config(self):
        """Test compact serializers can be added to config."""
        from hazelcast.config import SerializationConfig

        config = SerializationConfig()
        self.assertEqual(config.compact_serializers, [])

        class DummyCompactSerializer:
            pass

        config.add_compact_serializer(DummyCompactSerializer())
        self.assertEqual(len(config.compact_serializers), 1)


if __name__ == "__main__":
    unittest.main()
