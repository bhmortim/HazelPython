hazelcast.serialization.builtin
===============================

Built-in serializers for common Python types.

.. automodule:: hazelcast.serialization.builtin
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Overview
--------

Hazelcast provides built-in serializers for common Python types:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Python Type
     - Notes
   * - ``None``
     - Null/nil value
   * - ``bool``
     - Boolean values
   * - ``int``
     - Integer values (various sizes)
   * - ``float``
     - Floating point values
   * - ``str``
     - Unicode strings
   * - ``bytes``
     - Binary data
   * - ``list``
     - Ordered collections
   * - ``dict``
     - Key-value mappings
   * - ``datetime``
     - Date and time values
   * - ``Decimal``
     - Arbitrary precision decimals
   * - ``UUID``
     - Universally unique identifiers
