Serialization
=============

Hazelcast needs to serialize objects before sending them across the network
or storing them in memory. The client supports multiple serialization mechanisms.

Default Serialization
---------------------

By default, Hazelcast serializes common Python types:

.. code-block:: python

   my_map = client.get_map("my-map")
   
   # Primitives
   my_map.put("string", "Hello")
   my_map.put("int", 42)
   my_map.put("float", 3.14)
   my_map.put("bool", True)
   my_map.put("none", None)
   
   # Collections
   my_map.put("list", [1, 2, 3])
   my_map.put("dict", {"key": "value"})
   my_map.put("set", {1, 2, 3})

JSON Serialization
------------------

Use ``HazelcastJsonValue`` for JSON data:

.. code-block:: python

   from hazelcast.serialization.json import HazelcastJsonValue
   import json

   my_map = client.get_map("json-map")
   
   # Store JSON
   user = {"name": "Alice", "age": 30, "roles": ["admin", "user"]}
   my_map.put("user:1", HazelcastJsonValue(json.dumps(user)))
   
   # Retrieve JSON
   json_value = my_map.get("user:1")
   user = json.loads(json_value.to_string())

JSON values can be queried with SQL and predicates:

.. code-block:: python

   from hazelcast.predicate import sql

   # Query JSON fields
   adults = my_map.values(sql("JSON_VALUE(this, '$.age') > 18"))

Compact Serialization (Recommended)
-----------------------------------

Compact serialization is the recommended approach for custom types.
It provides schema evolution, type safety, and good performance.

.. code-block:: python

   from hazelcast.serialization.compact import CompactSerializer

   class User:
       def __init__(self, id: int = 0, name: str = "", email: str = ""):
           self.id = id
           self.name = name
           self.email = email

   class UserSerializer(CompactSerializer):
       def read(self, reader):
           return User(
               id=reader.read_int32("id"),
               name=reader.read_string("name"),
               email=reader.read_string("email"),
           )
       
       def write(self, writer, obj):
           writer.write_int32("id", obj.id)
           writer.write_string("name", obj.name)
           writer.write_string("email", obj.email)
       
       def get_type_name(self):
           return "User"
       
       def get_class(self):
           return User

   # Register serializer
   config = ClientConfig()
   config.serialization.add_compact_serializer(UserSerializer())
   
   with HazelcastClient(config) as client:
       users = client.get_map("users")
       users.put("user:1", User(1, "Alice", "alice@example.com"))
       user = users.get("user:1")

Compact Types
~~~~~~~~~~~~~

Available field types for compact serialization:

.. list-table::
   :header-rows: 1
   :widths: 30 30 40

   * - Read Method
     - Write Method
     - Python Type
   * - ``read_boolean``
     - ``write_boolean``
     - ``bool``
   * - ``read_int8``
     - ``write_int8``
     - ``int`` (-128 to 127)
   * - ``read_int16``
     - ``write_int16``
     - ``int`` (16-bit)
   * - ``read_int32``
     - ``write_int32``
     - ``int`` (32-bit)
   * - ``read_int64``
     - ``write_int64``
     - ``int`` (64-bit)
   * - ``read_float32``
     - ``write_float32``
     - ``float``
   * - ``read_float64``
     - ``write_float64``
     - ``float``
   * - ``read_string``
     - ``write_string``
     - ``str``
   * - ``read_decimal``
     - ``write_decimal``
     - ``Decimal``
   * - ``read_date``
     - ``write_date``
     - ``date``
   * - ``read_time``
     - ``write_time``
     - ``time``
   * - ``read_timestamp``
     - ``write_timestamp``
     - ``datetime``
   * - ``read_compact``
     - ``write_compact``
     - Nested compact object

Nullable and Array variants are also available (e.g., ``read_nullable_int32``,
``read_array_of_string``).

GenericRecord
~~~~~~~~~~~~~

Access compact data without a class:

.. code-block:: python

   from hazelcast.serialization.compact import GenericRecord

   # Read any compact object as GenericRecord
   record = users.get("user:1")
   
   if isinstance(record, GenericRecord):
       name = record.get_string("name")
       age = record.get_int32("age")

Portable Serialization
----------------------

Portable serialization provides cross-language compatibility:

.. code-block:: python

   from hazelcast.serialization.portable import Portable, PortableFactory

   class Employee(Portable):
       FACTORY_ID = 1
       CLASS_ID = 1
       
       def __init__(self, id=0, name=""):
           self.id = id
           self.name = name
       
       def write_portable(self, writer):
           writer.write_int("id", self.id)
           writer.write_string("name", self.name)
       
       def read_portable(self, reader):
           self.id = reader.read_int("id")
           self.name = reader.read_string("name")
       
       def get_factory_id(self):
           return self.FACTORY_ID
       
       def get_class_id(self):
           return self.CLASS_ID

   class EmployeeFactory(PortableFactory):
       def create(self, class_id):
           if class_id == Employee.CLASS_ID:
               return Employee()
           return None
       
       def get_factory_id(self):
           return Employee.FACTORY_ID

   # Register factory
   config = ClientConfig()
   config.serialization.add_portable_factory(1, EmployeeFactory())

Custom Serializers
------------------

Implement custom serializers for complete control:

.. code-block:: python

   from hazelcast.serialization import StreamSerializer

   class MyObject:
       def __init__(self, data=""):
           self.data = data

   class MyObjectSerializer(StreamSerializer):
       TYPE_ID = 1000  # Must be unique and > 0
       
       def write(self, out, obj):
           out.write_string(obj.data)
       
       def read(self, inp):
           return MyObject(inp.read_string())
       
       def get_type_id(self):
           return self.TYPE_ID
       
       def destroy(self):
           pass

   config = ClientConfig()
   config.serialization.add_custom_serializer(MyObject, MyObjectSerializer())

Global Serializer
-----------------

Handle unknown types with a global serializer:

.. code-block:: python

   import pickle

   class PickleSerializer(StreamSerializer):
       TYPE_ID = 999
       
       def write(self, out, obj):
           data = pickle.dumps(obj)
           out.write_byte_array(data)
       
       def read(self, inp):
           data = inp.read_byte_array()
           return pickle.loads(data)
       
       def get_type_id(self):
           return self.TYPE_ID

   config.serialization.global_serializer = PickleSerializer()

Best Practices
--------------

1. **Use Compact Serialization**
   
   For new projects, use compact serialization for the best combination
   of performance, schema evolution, and type safety.

2. **Keep Serializers Stateless**
   
   Serializers should not hold state between calls.

3. **Version Your Schemas**
   
   Plan for schema evolution by adding optional fields with defaults.

4. **Avoid Large Objects**
   
   Break large objects into smaller pieces or use references.

5. **Test Serialization**
   
   Write unit tests to verify serialization/deserialization roundtrips.
