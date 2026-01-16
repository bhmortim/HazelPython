hazelcast.serialization.api
===========================

Serialization API interfaces and base classes.

.. automodule:: hazelcast.serialization.api
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Interfaces
----------

This module defines the abstract interfaces for implementing custom
serializers that integrate with Hazelcast's serialization framework.

Portable
~~~~~~~~

Interface for portable serialization. Portable objects can be read
partially without full deserialization.

IdentifiedDataSerializable
~~~~~~~~~~~~~~~~~~~~~~~~~~

Interface for identified data serializable objects. Provides high
performance serialization for objects that are identified by factory
and class IDs.

StreamSerializer
~~~~~~~~~~~~~~~~

Base interface for custom stream serializers that read and write
objects to a binary stream.

CompactSerializer
~~~~~~~~~~~~~~~~~

Interface for compact serialization. Compact serializers provide
schema-based serialization with automatic schema evolution.
