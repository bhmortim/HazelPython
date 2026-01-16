Serialization
=============

Configuration
-------------

.. module:: hazelcast.config

.. autoclass:: SerializationConfig
   :members:
   :undoc-members:
   :show-inheritance:

Service
-------

.. module:: hazelcast.serialization.service

.. autoclass:: SerializationService
   :members:
   :undoc-members:
   :show-inheritance:

Compact Serialization
---------------------

.. module:: hazelcast.serialization.compact

For high-performance serialization with schema evolution support.

Usage Example
~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.config import SerializationConfig

   class Employee:
       def __init__(self, id: int, name: str):
           self.id = id
           self.name = name

   class EmployeeSerializer:
       def get_type_name(self) -> str:
           return "Employee"

       def get_class(self) -> type:
           return Employee

       def write(self, writer, obj: Employee) -> None:
           writer.write_int32("id", obj.id)
           writer.write_string("name", obj.name)

       def read(self, reader) -> Employee:
           id = reader.read_int32("id")
           name = reader.read_string("name")
           return Employee(id, name)

   config = SerializationConfig()
   config.add_compact_serializer(EmployeeSerializer())
