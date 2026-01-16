Serialization API
=================

.. module:: hazelcast.serialization.api
   :synopsis: Serialization API interfaces.

This module defines the core interfaces for Hazelcast serialization.

Data Input/Output
-----------------

.. autoclass:: ObjectDataInput
   :members:

.. autoclass:: ObjectDataOutput
   :members:

Serializer Interfaces
---------------------

.. autoclass:: Serializer
   :members:

.. autoclass:: StreamSerializer
   :members:

IdentifiedDataSerializable
--------------------------

.. autoclass:: IdentifiedDataSerializable
   :members:

Portable Serialization
----------------------

.. autoclass:: PortableReader
   :members:

.. autoclass:: PortableWriter
   :members:

.. autoclass:: Portable
   :members:

Compact Serialization
---------------------

.. autoclass:: CompactReader
   :members:

.. autoclass:: CompactWriter
   :members:

.. autoclass:: CompactSerializer
   :members:

Example Usage
-------------

Implementing a custom serializer::

    from hazelcast.serialization.api import Serializer, ObjectDataInput, ObjectDataOutput

    class PersonSerializer(Serializer[Person]):
        @property
        def type_id(self) -> int:
            return 1000

        def write(self, output: ObjectDataOutput, person: Person) -> None:
            output.write_string(person.name)
            output.write_int(person.age)

        def read(self, input: ObjectDataInput) -> Person:
            name = input.read_string()
            age = input.read_int()
            return Person(name, age)

IdentifiedDataSerializable example::

    from hazelcast.serialization.api import IdentifiedDataSerializable

    class Employee(IdentifiedDataSerializable):
        FACTORY_ID = 1
        CLASS_ID = 1

        def __init__(self, id: int = 0, name: str = ""):
            self.id = id
            self.name = name

        @property
        def factory_id(self) -> int:
            return self.FACTORY_ID

        @property
        def class_id(self) -> int:
            return self.CLASS_ID

        def write_data(self, output) -> None:
            output.write_int(self.id)
            output.write_string(self.name)

        def read_data(self, input) -> None:
            self.id = input.read_int()
            self.name = input.read_string()
