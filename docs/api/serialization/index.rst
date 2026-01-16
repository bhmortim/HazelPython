hazelcast.serialization
=======================

The ``hazelcast.serialization`` package provides serialization services
and configuration for converting objects to and from binary format.

.. toctree::
   :maxdepth: 2
   :caption: Serialization Components:

   api
   service
   compact
   builtin

Overview
--------

Hazelcast supports multiple serialization mechanisms:

* **Compact Serialization** (recommended): Schema-based, efficient serialization
* **Portable Serialization**: Cross-language, schema-evolution support
* **IdentifiedDataSerializable**: High-performance Java-compatible serialization
* **Custom Serializers**: User-defined serialization for specific types
* **Built-in Serializers**: Support for common Python types

Example
-------

.. code-block:: python

   from hazelcast import ClientConfig

   config = ClientConfig()

   # Add a compact serializer
   config.serialization.add_compact_serializer(MyCompactSerializer())

   # Add a custom serializer
   config.serialization.add_custom_serializer(MyClass, MySerializer())
