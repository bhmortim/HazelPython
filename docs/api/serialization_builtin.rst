Built-in Serializers
====================

.. module:: hazelcast.serialization.builtin
   :synopsis: Built-in serializers for Python primitive types.

This module provides serializers for Python's built-in types.

Type IDs
--------

Built-in serializers use negative type IDs:

.. list-table:: Built-in Type IDs
   :header-rows: 1
   :widths: 30 20 50

   * - Constant
     - Value
     - Description
   * - ``NONE_TYPE_ID``
     - 0
     - None/null values
   * - ``BOOLEAN_TYPE_ID``
     - -1
     - Boolean values
   * - ``BYTE_TYPE_ID``
     - -2
     - Byte values (-128 to 127)
   * - ``SHORT_TYPE_ID``
     - -3
     - 16-bit short integers
   * - ``INT_TYPE_ID``
     - -4
     - 32-bit integers
   * - ``LONG_TYPE_ID``
     - -5
     - 64-bit long integers
   * - ``FLOAT_TYPE_ID``
     - -6
     - 32-bit floats
   * - ``DOUBLE_TYPE_ID``
     - -7
     - 64-bit doubles
   * - ``STRING_TYPE_ID``
     - -8
     - UTF-8 strings

Primitive Serializers
---------------------

.. autoclass:: NoneSerializer
   :members:

.. autoclass:: BoolSerializer
   :members:

.. autoclass:: ByteSerializer
   :members:

.. autoclass:: ShortSerializer
   :members:

.. autoclass:: IntSerializer
   :members:

.. autoclass:: LongSerializer
   :members:

.. autoclass:: FloatSerializer
   :members:

.. autoclass:: DoubleSerializer
   :members:

.. autoclass:: StringSerializer
   :members:

Array Serializers
-----------------

.. autoclass:: ByteArraySerializer
   :members:

.. autoclass:: BooleanArraySerializer
   :members:

.. autoclass:: ShortArraySerializer
   :members:

.. autoclass:: IntArraySerializer
   :members:

.. autoclass:: LongArraySerializer
   :members:

.. autoclass:: FloatArraySerializer
   :members:

.. autoclass:: DoubleArraySerializer
   :members:

.. autoclass:: StringArraySerializer
   :members:

Collection Serializers
----------------------

.. autoclass:: ListSerializer
   :members:

.. autoclass:: DictSerializer
   :members:

Date/Time Serializers
---------------------

.. autoclass:: DateTimeSerializer
   :members:

.. autoclass:: DateSerializer
   :members:

.. autoclass:: TimeSerializer
   :members:

Other Serializers
-----------------

.. autoclass:: UUIDSerializer
   :members:

.. autoclass:: BigDecimalSerializer
   :members:

Helper Functions
----------------

.. autofunction:: get_builtin_serializers

.. autofunction:: get_type_serializer_mapping
