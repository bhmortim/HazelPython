hazelcast.serialization.compact
===============================

Compact serialization implementation.

.. automodule:: hazelcast.serialization.compact
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Overview
--------

Compact serialization is the recommended serialization mechanism for
Hazelcast. It provides:

* Schema-based serialization with automatic schema evolution
* Efficient binary format with minimal overhead
* Partial deserialization support
* Cross-language compatibility

Example
-------

.. code-block:: python

   from hazelcast.serialization.compact import CompactSerializer

   class PersonSerializer(CompactSerializer):
       def read(self, reader):
           name = reader.read_string("name")
           age = reader.read_int32("age")
           return Person(name, age)

       def write(self, writer, obj):
           writer.write_string("name", obj.name)
           writer.write_int32("age", obj.age)

       def get_type_name(self):
           return "Person"

       def get_class(self):
           return Person
