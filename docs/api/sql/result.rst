hazelcast.sql.result
====================

SQL result handling and iteration.

.. automodule:: hazelcast.sql.result
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

SqlResult
~~~~~~~~~

.. autoclass:: hazelcast.sql.result.SqlResult
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__, __iter__, __next__, __enter__, __exit__

SqlRow
~~~~~~

.. autoclass:: hazelcast.sql.result.SqlRow
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__, __getitem__, __len__, __iter__

SqlRowMetadata
~~~~~~~~~~~~~~

.. autoclass:: hazelcast.sql.result.SqlRowMetadata
   :members:
   :undoc-members:
   :show-inheritance:

SqlColumnMetadata
~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.sql.result.SqlColumnMetadata
   :members:
   :undoc-members:
   :show-inheritance:

SqlPage
~~~~~~~

.. autoclass:: hazelcast.sql.result.SqlPage
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__, __iter__, __len__, __getitem__

Enumerations
------------

SqlColumnType
~~~~~~~~~~~~~

.. autoclass:: hazelcast.sql.result.SqlColumnType
   :members:
   :undoc-members:
