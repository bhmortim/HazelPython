"""Tests for hazelcast/serialization/service.py module."""

import pytest
import struct
from unittest.mock import Mock, MagicMock

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
    PortableSerializer,
    DefaultPortableWriter,
    DefaultPortableReader,
    TYPE_ID_SIZE,
    DATA_OFFSET,
)
from hazelcast.serialization.api import (
    IdentifiedDataSerializable,
    Portable,
    PortableReader,
    PortableWriter,
    Serializer,
    ObjectDataInput,
    ObjectDataOutput,
)
from hazelcast.serialization.builtin import NONE_TYPE_ID
from hazelcast.exceptions import SerializationException


class TestFieldType:
    """Tests for FieldType enum."""

    def test_all_field_types_exist(self):
        """Test all field types exist."""
        expected = [
            "BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE",
            "STRING", "PORTABLE", "BYTE_ARRAY", "BOOLEAN_ARRAY", "SHORT_ARRAY",
            "INT_ARRAY", "LONG_ARRAY", "FLOAT_ARRAY", "DOUBLE_ARRAY",
            "STRING_ARRAY", "PORTABLE_ARRAY",
        ]
        for field_type in expected:
            assert hasattr(FieldType, field_type)

    @pytest.mark.parametrize("field_type,value", [
        (FieldType.BOOLEAN, 0),
        (FieldType.BYTE, 1),
        (FieldType.SHORT, 2),
        (FieldType.INT, 3),
        (FieldType.LONG, 4),
        (FieldType.FLOAT, 5),
        (FieldType.DOUBLE, 6),
        (FieldType.STRING, 7),
        (FieldType.PORTABLE, 8),
    ])
    def test_field_type_values(self, field_type, value):
        """Test field type values."""
        assert field_type.value == value


class TestFieldDefinition:
    """Tests for FieldDefinition class."""

    @pytest.fixture
    def field_def(self):
        """Create a FieldDefinition."""
        return FieldDefinition(0, "name", FieldType.STRING)

    def test_init(self, field_def):
        """Test initialization."""
        assert field_def.index == 0
        assert field_def.name == "name"
        assert field_def.field_type == FieldType.STRING

    def test_init_with_nested(self):
        """Test initialization with nested portable info."""
        field_def = FieldDefinition(
            0, "nested", FieldType.PORTABLE, factory_id=1, class_id=2, version=3
        )
        assert field_def.factory_id == 1
        assert field_def.class_id == 2
        assert field_def.version == 3

    def test_equality(self):
        """Test __eq__."""
        fd1 = FieldDefinition(0, "name", FieldType.STRING)
        fd2 = FieldDefinition(0, "name", FieldType.STRING)
        fd3 = FieldDefinition(0, "name", FieldType.INT)
        
        assert fd1 == fd2
        assert fd1 != fd3
        assert fd1 != "not a field"

    def test_hash(self, field_def):
        """Test __hash__."""
        h = hash(field_def)
        assert isinstance(h, int)


class TestClassDefinition:
    """Tests for ClassDefinition class."""

    @pytest.fixture
    def class_def(self):
        """Create a ClassDefinition."""
        cd = ClassDefinition(1, 2, 0)
        cd.add_field(FieldDefinition(0, "id", FieldType.INT))
        cd.add_field(FieldDefinition(1, "name", FieldType.STRING))
        return cd

    def test_init(self):
        """Test initialization."""
        cd = ClassDefinition(1, 2, 3)
        assert cd.factory_id == 1
        assert cd.class_id == 2
        assert cd.version == 3

    def test_add_field(self, class_def):
        """Test add_field method."""
        result = class_def.add_field(FieldDefinition(2, "active", FieldType.BOOLEAN))
        assert result is class_def
        assert class_def.get_field_count() == 3

    def test_get_field(self, class_def):
        """Test get_field method."""
        field = class_def.get_field("name")
        assert field is not None
        assert field.name == "name"

    def test_get_field_not_found(self, class_def):
        """Test get_field returns None for unknown field."""
        assert class_def.get_field("unknown") is None

    def test_get_field_by_index(self, class_def):
        """Test get_field_by_index method."""
        field = class_def.get_field_by_index(0)
        assert field.name == "id"

    def test_get_field_by_index_out_of_range(self, class_def):
        """Test get_field_by_index returns None for invalid index."""
        assert class_def.get_field_by_index(10) is None

    def test_has_field(self, class_def):
        """Test has_field method."""
        assert class_def.has_field("id") is True
        assert class_def.has_field("unknown") is False

    def test_get_field_count(self, class_def):
        """Test get_field_count method."""
        assert class_def.get_field_count() == 2

    def test_get_field_names(self, class_def):
        """Test get_field_names method."""
        names = class_def.get_field_names()
        assert "id" in names
        assert "name" in names

    def test_get_field_type(self, class_def):
        """Test get_field_type method."""
        assert class_def.get_field_type("id") == FieldType.INT
        assert class_def.get_field_type("unknown") is None

    def test_fields_property(self, class_def):
        """Test fields property."""
        fields = class_def.fields
        assert len(fields) == 2

    def test_equality(self):
        """Test __eq__."""
        cd1 = ClassDefinition(1, 2, 0)
        cd2 = ClassDefinition(1, 2, 0)
        cd3 = ClassDefinition(1, 3, 0)
        
        assert cd1 == cd2
        assert cd1 != cd3
        assert cd1 != "not a class def"

    def test_hash(self, class_def):
        """Test __hash__."""
        h = hash(class_def)
        assert isinstance(h, int)


class TestClassDefinitionBuilder:
    """Tests for ClassDefinitionBuilder class."""

    @pytest.fixture
    def builder(self):
        """Create a ClassDefinitionBuilder."""
        return ClassDefinitionBuilder(1, 2, 0)

    def test_add_boolean_field(self, builder):
        """Test add_boolean_field method."""
        result = builder.add_boolean_field("active")
        assert result is builder

    def test_add_byte_field(self, builder):
        """Test add_byte_field method."""
        builder.add_byte_field("flags")
        cd = builder.build()
        assert cd.get_field("flags").field_type == FieldType.BYTE

    def test_add_short_field(self, builder):
        """Test add_short_field method."""
        builder.add_short_field("count")
        cd = builder.build()
        assert cd.get_field("count").field_type == FieldType.SHORT

    def test_add_int_field(self, builder):
        """Test add_int_field method."""
        builder.add_int_field("id")
        cd = builder.build()
        assert cd.get_field("id").field_type == FieldType.INT

    def test_add_long_field(self, builder):
        """Test add_long_field method."""
        builder.add_long_field("timestamp")
        cd = builder.build()
        assert cd.get_field("timestamp").field_type == FieldType.LONG

    def test_add_float_field(self, builder):
        """Test add_float_field method."""
        builder.add_float_field("score")
        cd = builder.build()
        assert cd.get_field("score").field_type == FieldType.FLOAT

    def test_add_double_field(self, builder):
        """Test add_double_field method."""
        builder.add_double_field("price")
        cd = builder.build()
        assert cd.get_field("price").field_type == FieldType.DOUBLE

    def test_add_string_field(self, builder):
        """Test add_string_field method."""
        builder.add_string_field("name")
        cd = builder.build()
        assert cd.get_field("name").field_type == FieldType.STRING

    def test_add_byte_array_field(self, builder):
        """Test add_byte_array_field method."""
        builder.add_byte_array_field("data")
        cd = builder.build()
        assert cd.get_field("data").field_type == FieldType.BYTE_ARRAY

    def test_add_portable_field(self, builder):
        """Test add_portable_field method."""
        nested_def = ClassDefinition(1, 3, 0)
        builder.add_portable_field("nested", nested_def)
        cd = builder.build()
        field = cd.get_field("nested")
        assert field.field_type == FieldType.PORTABLE
        assert field.factory_id == 1
        assert field.class_id == 3

    def test_add_portable_array_field(self, builder):
        """Test add_portable_array_field method."""
        nested_def = ClassDefinition(1, 3, 0)
        builder.add_portable_array_field("items", nested_def)
        cd = builder.build()
        field = cd.get_field("items")
        assert field.field_type == FieldType.PORTABLE_ARRAY

    def test_build(self, builder):
        """Test build method."""
        builder.add_int_field("id")
        builder.add_string_field("name")
        cd = builder.build()
        
        assert cd.factory_id == 1
        assert cd.class_id == 2
        assert cd.version == 0
        assert cd.get_field_count() == 2

    def test_field_indices(self, builder):
        """Test that field indices are assigned correctly."""
        builder.add_int_field("first")
        builder.add_string_field("second")
        builder.add_boolean_field("third")
        cd = builder.build()
        
        assert cd.get_field("first").index == 0
        assert cd.get_field("second").index == 1
        assert cd.get_field("third").index == 2


class TestClassDefinitionContext:
    """Tests for ClassDefinitionContext class."""

    @pytest.fixture
    def context(self):
        """Create a ClassDefinitionContext."""
        return ClassDefinitionContext(0)

    def test_register(self, context):
        """Test register method."""
        cd = ClassDefinition(1, 2, 0)
        result = context.register(cd)
        assert result is cd

    def test_register_returns_existing(self, context):
        """Test register returns existing definition."""
        cd1 = ClassDefinition(1, 2, 0)
        cd2 = ClassDefinition(1, 2, 0)
        context.register(cd1)
        result = context.register(cd2)
        assert result is cd1

    def test_lookup(self, context):
        """Test lookup method."""
        cd = ClassDefinition(1, 2, 0)
        context.register(cd)
        result = context.lookup(1, 2, 0)
        assert result is cd

    def test_lookup_not_found(self, context):
        """Test lookup returns None for unknown class."""
        assert context.lookup(99, 99, 0) is None


class TestData:
    """Tests for Data class."""

    @pytest.fixture
    def data(self):
        """Create a Data instance with type ID and payload."""
        type_id = 1
        payload = b"test payload"
        buffer = struct.pack("<i", type_id) + payload
        return Data(buffer)

    def test_buffer_property(self, data):
        """Test buffer property."""
        assert len(data.buffer) > 0

    def test_get_type_id(self, data):
        """Test get_type_id method."""
        assert data.get_type_id() == 1

    def test_get_type_id_small_buffer(self):
        """Test get_type_id with buffer too small."""
        data = Data(b"ab")
        assert data.get_type_id() == NONE_TYPE_ID

    def test_get_payload(self, data):
        """Test get_payload method."""
        payload = data.get_payload()
        assert payload == b"test payload"

    def test_get_payload_empty(self):
        """Test get_payload with no payload."""
        buffer = struct.pack("<i", 1)
        data = Data(buffer)
        assert data.get_payload() == b""

    def test_total_size(self, data):
        """Test total_size method."""
        assert data.total_size() == TYPE_ID_SIZE + len(b"test payload")

    def test_len(self, data):
        """Test __len__."""
        assert len(data) == data.total_size()

    def test_equality(self):
        """Test __eq__."""
        buffer1 = struct.pack("<i", 1) + b"test"
        buffer2 = struct.pack("<i", 1) + b"test"
        buffer3 = struct.pack("<i", 2) + b"test"
        
        data1 = Data(buffer1)
        data2 = Data(buffer2)
        data3 = Data(buffer3)
        
        assert data1 == data2
        assert data1 != data3
        assert data1 != "not data"

    def test_hash(self, data):
        """Test __hash__."""
        h = hash(data)
        assert isinstance(h, int)

    def test_bytes(self, data):
        """Test __bytes__."""
        assert bytes(data) == data.buffer


class TestObjectDataOutputImpl:
    """Tests for ObjectDataOutputImpl class."""

    @pytest.fixture
    def service(self):
        """Create a mock SerializationService."""
        return Mock(spec=SerializationService)

    @pytest.fixture
    def output(self, service):
        """Create an ObjectDataOutputImpl instance."""
        return ObjectDataOutputImpl(service)

    def test_write_boolean_true(self, output):
        """Test write_boolean with True."""
        output.write_boolean(True)
        result = output.to_byte_array()
        assert result == b"\x01"

    def test_write_boolean_false(self, output):
        """Test write_boolean with False."""
        output.write_boolean(False)
        result = output.to_byte_array()
        assert result == b"\x00"

    def test_write_byte(self, output):
        """Test write_byte."""
        output.write_byte(42)
        result = output.to_byte_array()
        assert struct.unpack("<b", result)[0] == 42

    def test_write_short(self, output):
        """Test write_short."""
        output.write_short(1000)
        result = output.to_byte_array()
        assert struct.unpack("<h", result)[0] == 1000

    def test_write_int(self, output):
        """Test write_int."""
        output.write_int(100000)
        result = output.to_byte_array()
        assert struct.unpack("<i", result)[0] == 100000

    def test_write_long(self, output):
        """Test write_long."""
        output.write_long(10000000000)
        result = output.to_byte_array()
        assert struct.unpack("<q", result)[0] == 10000000000

    def test_write_float(self, output):
        """Test write_float."""
        output.write_float(3.14)
        result = output.to_byte_array()
        value = struct.unpack("<f", result)[0]
        assert abs(value - 3.14) < 0.001

    def test_write_double(self, output):
        """Test write_double."""
        output.write_double(3.14159265359)
        result = output.to_byte_array()
        value = struct.unpack("<d", result)[0]
        assert abs(value - 3.14159265359) < 0.0000001

    def test_write_string(self, output):
        """Test write_string."""
        output.write_string("hello")
        result = output.to_byte_array()
        length = struct.unpack("<i", result[:4])[0]
        assert length == 5
        assert result[4:] == b"hello"

    def test_write_string_none(self, output):
        """Test write_string with None."""
        output.write_string(None)
        result = output.to_byte_array()
        length = struct.unpack("<i", result)[0]
        assert length == -1

    def test_write_byte_array(self, output):
        """Test write_byte_array."""
        output.write_byte_array(b"test data")
        result = output.to_byte_array()
        length = struct.unpack("<i", result[:4])[0]
        assert length == 9
        assert result[4:] == b"test data"

    def test_write_byte_array_none(self, output):
        """Test write_byte_array with None."""
        output.write_byte_array(None)
        result = output.to_byte_array()
        length = struct.unpack("<i", result)[0]
        assert length == -1

    def test_to_byte_array(self, output):
        """Test to_byte_array returns bytes."""
        output.write_int(42)
        result = output.to_byte_array()
        assert isinstance(result, bytes)


class TestObjectDataInputImpl:
    """Tests for ObjectDataInputImpl class."""

    @pytest.fixture
    def service(self):
        """Create a mock SerializationService."""
        return Mock(spec=SerializationService)

    def test_read_boolean_true(self, service):
        """Test read_boolean True."""
        input_stream = ObjectDataInputImpl(b"\x01", service)
        assert input_stream.read_boolean() is True

    def test_read_boolean_false(self, service):
        """Test read_boolean False."""
        input_stream = ObjectDataInputImpl(b"\x00", service)
        assert input_stream.read_boolean() is False

    def test_read_byte(self, service):
        """Test read_byte."""
        buffer = struct.pack("<b", -42)
        input_stream = ObjectDataInputImpl(buffer, service)
        assert input_stream.read_byte() == -42

    def test_read_short(self, service):
        """Test read_short."""
        buffer = struct.pack("<h", 1000)
        input_stream = ObjectDataInputImpl(buffer, service)
        assert input_stream.read_short() == 1000

    def test_read_int(self, service):
        """Test read_int."""
        buffer = struct.pack("<i", 100000)
        input_stream = ObjectDataInputImpl(buffer, service)
        assert input_stream.read_int() == 100000

    def test_read_long(self, service):
        """Test read_long."""
        buffer = struct.pack("<q", 10000000000)
        input_stream = ObjectDataInputImpl(buffer, service)
        assert input_stream.read_long() == 10000000000

    def test_read_float(self, service):
        """Test read_float."""
        buffer = struct.pack("<f", 3.14)
        input_stream = ObjectDataInputImpl(buffer, service)
        value = input_stream.read_float()
        assert abs(value - 3.14) < 0.001

    def test_read_double(self, service):
        """Test read_double."""
        buffer = struct.pack("<d", 3.14159265359)
        input_stream = ObjectDataInputImpl(buffer, service)
        value = input_stream.read_double()
        assert abs(value - 3.14159265359) < 0.0000001

    def test_read_string(self, service):
        """Test read_string."""
        text = "hello"
        encoded = text.encode("utf-8")
        buffer = struct.pack("<i", len(encoded)) + encoded
        input_stream = ObjectDataInputImpl(buffer, service)
        assert input_stream.read_string() == "hello"

    def test_read_string_empty(self, service):
        """Test read_string with negative length."""
        buffer = struct.pack("<i", -1)
        input_stream = ObjectDataInputImpl(buffer, service)
        assert input_stream.read_string() == ""

    def test_read_byte_array(self, service):
        """Test read_byte_array."""
        data = b"test data"
        buffer = struct.pack("<i", len(data)) + data
        input_stream = ObjectDataInputImpl(buffer, service)
        assert input_stream.read_byte_array() == b"test data"

    def test_read_byte_array_empty(self, service):
        """Test read_byte_array with negative length."""
        buffer = struct.pack("<i", -1)
        input_stream = ObjectDataInputImpl(buffer, service)
        assert input_stream.read_byte_array() == b""

    def test_position(self, service):
        """Test position method."""
        buffer = struct.pack("<ii", 1, 2)
        input_stream = ObjectDataInputImpl(buffer, service)
        assert input_stream.position() == 0
        input_stream.read_int()
        assert input_stream.position() == 4

    def test_set_position(self, service):
        """Test set_position method."""
        buffer = struct.pack("<ii", 1, 2)
        input_stream = ObjectDataInputImpl(buffer, service)
        input_stream.set_position(4)
        assert input_stream.position() == 4
        assert input_stream.read_int() == 2


class TestSerializationService:
    """Tests for SerializationService class."""

    @pytest.fixture
    def service(self):
        """Create a SerializationService instance."""
        return SerializationService()

    def test_init_defaults(self):
        """Test initialization with defaults."""
        service = SerializationService()
        assert service.class_definition_context is not None
        assert service.compact_service is not None

    def test_to_data_none(self, service):
        """Test to_data with None."""
        data = service.to_data(None)
        assert data.get_type_id() == NONE_TYPE_ID

    def test_to_data_already_data(self, service):
        """Test to_data with Data object."""
        original = Data(struct.pack("<i", 1) + b"test")
        result = service.to_data(original)
        assert result is original

    def test_to_object_none(self, service):
        """Test to_object with None."""
        assert service.to_object(None) is None

    def test_to_object_bytes(self, service):
        """Test to_object with bytes."""
        buffer = struct.pack("<i", NONE_TYPE_ID)
        result = service.to_object(buffer)
        assert result is None

    def test_to_object_data_none_type(self, service):
        """Test to_object with NONE_TYPE_ID."""
        data = Data(struct.pack("<i", NONE_TYPE_ID))
        result = service.to_object(data)
        assert result is None

    def test_to_object_empty_data(self, service):
        """Test to_object with empty Data."""
        data = Data(b"")
        result = service.to_object(data)
        assert result is None

    def test_to_object_passthrough(self, service):
        """Test to_object with non-Data object."""
        obj = {"key": "value"}
        result = service.to_object(obj)
        assert result is obj

    @pytest.mark.parametrize("value,expected_type", [
        (True, bool),
        (False, bool),
        (42, int),
        (-100, int),
        (3.14, float),
        ("hello", str),
        (b"bytes", bytes),
    ])
    def test_roundtrip_primitives(self, service, value, expected_type):
        """Test serialization roundtrip for primitives."""
        data = service.to_data(value)
        result = service.to_object(data)
        assert type(result) == expected_type
        assert result == value

    def test_roundtrip_list(self, service):
        """Test serialization roundtrip for list."""
        original = [1, 2, 3, "test"]
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == original

    def test_roundtrip_dict(self, service):
        """Test serialization roundtrip for dict."""
        original = {"key": "value", "number": 42}
        data = service.to_data(original)
        result = service.to_object(data)
        assert result == original

    def test_register_serializer(self, service):
        """Test register_serializer method."""
        class CustomType:
            pass
        
        class CustomSerializer(Serializer):
            @property
            def type_id(self):
                return 1000
            
            def write(self, output, obj):
                output.write_string("custom")
            
            def read(self, input_stream):
                return CustomType()
        
        serializer = CustomSerializer()
        service.register_serializer(CustomType, serializer)

    def test_to_data_unknown_type_raises(self, service):
        """Test to_data raises for unknown type."""
        class UnknownType:
            pass
        
        with pytest.raises(SerializationException, match="No serializer found"):
            service.to_data(UnknownType())

    def test_to_object_unknown_type_id_raises(self, service):
        """Test to_object raises for unknown type ID."""
        data = Data(struct.pack("<i", 99999) + b"payload")
        with pytest.raises(SerializationException, match="No serializer found"):
            service.to_object(data)


class SampleIdentifiedDataSerializable(IdentifiedDataSerializable):
    """Sample IdentifiedDataSerializable for testing."""

    def __init__(self, id_value: int = 0, name: str = ""):
        self.id_value = id_value
        self.name = name

    @property
    def factory_id(self) -> int:
        return 1

    @property
    def class_id(self) -> int:
        return 1

    def write_data(self, output: ObjectDataOutput) -> None:
        output.write_int(self.id_value)
        output.write_string(self.name)

    def read_data(self, input_stream: ObjectDataInput) -> None:
        self.id_value = input_stream.read_int()
        self.name = input_stream.read_string()


class SampleFactory:
    """Sample factory for IdentifiedDataSerializable."""

    def create(self, class_id: int):
        if class_id == 1:
            return SampleIdentifiedDataSerializable()
        return None


class TestIdentifiedDataSerializableSerializer:
    """Tests for IdentifiedDataSerializableSerializer."""

    @pytest.fixture
    def serializer(self):
        """Create an IdentifiedDataSerializableSerializer."""
        factories = {1: SampleFactory()}
        return IdentifiedDataSerializableSerializer(factories)

    def test_type_id(self, serializer):
        """Test type_id property."""
        assert serializer.type_id == SerializationService.IDENTIFIED_DATA_SERIALIZABLE_ID

    def test_write_read_roundtrip(self, serializer):
        """Test write and read roundtrip."""
        obj = SampleIdentifiedDataSerializable(42, "Alice")
        
        service = Mock()
        output = ObjectDataOutputImpl(service)
        serializer.write(output, obj)
        
        buffer = output.to_byte_array()
        input_stream = ObjectDataInputImpl(buffer, service)
        result = serializer.read(input_stream)
        
        assert result.id_value == 42
        assert result.name == "Alice"

    def test_read_unknown_factory_raises(self, serializer):
        """Test read raises for unknown factory ID."""
        buffer = struct.pack("<ii", 999, 1)
        input_stream = ObjectDataInputImpl(buffer, Mock())
        
        with pytest.raises(SerializationException, match="No factory registered"):
            serializer.read(input_stream)


class SamplePortable(Portable):
    """Sample Portable for testing."""

    def __init__(self, id_value: int = 0, name: str = ""):
        self.id_value = id_value
        self.name = name

    @property
    def factory_id(self) -> int:
        return 1

    @property
    def class_id(self) -> int:
        return 1

    def write_portable(self, writer: PortableWriter) -> None:
        writer.write_int("id_value", self.id_value)
        writer.write_string("name", self.name)

    def read_portable(self, reader: PortableReader) -> None:
        self.id_value = reader.read_int("id_value")
        self.name = reader.read_string("name")


class SamplePortableFactory:
    """Sample factory for Portable."""

    def create(self, class_id: int):
        if class_id == 1:
            return SamplePortable()
        return None


class TestPortableSerializer:
    """Tests for PortableSerializer."""

    @pytest.fixture
    def serializer(self):
        """Create a PortableSerializer."""
        factories = {1: SamplePortableFactory()}
        return PortableSerializer(0, factories)

    def test_type_id(self, serializer):
        """Test type_id property."""
        assert serializer.type_id == SerializationService.PORTABLE_ID

    def test_register_class_definition(self, serializer):
        """Test register_class_definition method."""
        cd = ClassDefinition(1, 2, 0)
        serializer.register_class_definition(cd)
        assert serializer.context.lookup(1, 2, 0) is cd

    def test_get_or_create_class_definition(self, serializer):
        """Test get_or_create_class_definition method."""
        obj = SamplePortable(1, "test")
        cd = serializer.get_or_create_class_definition(obj)
        assert cd.factory_id == 1
        assert cd.class_id == 1


class TestSerializationServiceWithFactories:
    """Tests for SerializationService with factories configured."""

    @pytest.fixture
    def service(self):
        """Create SerializationService with factories."""
        return SerializationService(
            portable_version=0,
            portable_factories={1: SamplePortableFactory()},
            data_serializable_factories={1: SampleFactory()},
        )

    def test_roundtrip_identified_data_serializable(self, service):
        """Test roundtrip for IdentifiedDataSerializable."""
        original = SampleIdentifiedDataSerializable(100, "Bob")
        data = service.to_data(original)
        result = service.to_object(data)
        
        assert isinstance(result, SampleIdentifiedDataSerializable)
        assert result.id_value == 100
        assert result.name == "Bob"

    def test_register_class_definition(self, service):
        """Test register_class_definition method."""
        builder = ClassDefinitionBuilder(1, 1, 0)
        builder.add_int_field("id_value")
        builder.add_string_field("name")
        cd = builder.build()
        
        service.register_class_definition(cd)
        assert service.class_definition_context.lookup(1, 1, 0) is cd
