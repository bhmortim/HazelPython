Serialization Service
=====================

.. module:: hazelcast.serialization.service
   :synopsis: Serialization service implementation.

This module provides the main serialization service for the Hazelcast client.

Field Types
-----------

.. autoclass:: FieldType
   :members:
   :undoc-members:

Class Definitions
-----------------

.. autoclass:: FieldDefinition
   :members:
   :special-members: __init__

.. autoclass:: ClassDefinition
   :members:
   :special-members: __init__

.. autoclass:: ClassDefinitionBuilder
   :members:
   :special-members: __init__

.. autoclass:: ClassDefinitionContext
   :members:
   :special-members: __init__

Data Class
----------

.. autoclass:: Data
   :members:
   :special-members: __init__

Input/Output Implementation
---------------------------

.. autoclass:: ObjectDataInputImpl
   :members:
   :special-members: __init__

.. autoclass:: ObjectDataOutputImpl
   :members:
   :special-members: __init__

Serialization Service
---------------------

.. autoclass:: SerializationService
   :members:
   :special-members: __init__

Portable Support
----------------

.. autoclass:: DefaultPortableWriter
   :members:
   :special-members: __init__

.. autoclass:: DefaultPortableReader
   :members:
   :special-members: __init__

.. autoclass:: PortableSerializer
   :members:
   :special-members: __init__

Example Usage
-------------

Basic serialization::

    from hazelcast.serialization.service import SerializationService

    service = SerializationService()

    # Serialize an object
    data = service.to_data({"name": "Alice", "age": 30})

    # Deserialize back
    obj = service.to_object(data)

Registering custom serializers::

    service = SerializationService(
        custom_serializers={Person: PersonSerializer()}
    )

Building class definitions for Portable::

    from hazelcast.serialization.service import ClassDefinitionBuilder

    builder = ClassDefinitionBuilder(1, 1, 0)
    class_def = (builder
        .add_string_field("name")
        .add_int_field("age")
        .add_boolean_field("active")
        .build())
