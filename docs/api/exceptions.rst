Exceptions API
==============

.. automodule:: hazelcast.exceptions
   :members:
   :undoc-members:
   :show-inheritance:

Base Exception
--------------

.. autoclass:: hazelcast.exceptions.HazelcastException
   :members:
   :undoc-members:

Connection Exceptions
---------------------

.. autoclass:: hazelcast.exceptions.ClientOfflineException
   :members:
   :undoc-members:

.. autoclass:: hazelcast.exceptions.TargetDisconnectedException
   :members:
   :undoc-members:

State Exceptions
----------------

.. autoclass:: hazelcast.exceptions.IllegalStateException
   :members:
   :undoc-members:

.. autoclass:: hazelcast.exceptions.IllegalArgumentException
   :members:
   :undoc-members:

Timeout Exceptions
------------------

.. autoclass:: hazelcast.exceptions.TimeoutException
   :members:
   :undoc-members:

.. autoclass:: hazelcast.exceptions.OperationTimeoutException
   :members:
   :undoc-members:

Configuration Exceptions
------------------------

.. autoclass:: hazelcast.exceptions.ConfigurationException
   :members:
   :undoc-members:

Authentication Exceptions
-------------------------

.. autoclass:: hazelcast.exceptions.AuthenticationException
   :members:
   :undoc-members:

Data Structure Exceptions
-------------------------

.. autoclass:: hazelcast.exceptions.StaleSequenceException
   :members:
   :undoc-members:

Exception Hierarchy
-------------------

.. code-block:: text

   HazelcastException
   ├── ClientOfflineException
   ├── TargetDisconnectedException
   ├── IllegalStateException
   ├── IllegalArgumentException
   ├── TimeoutException
   │   └── OperationTimeoutException
   ├── ConfigurationException
   ├── AuthenticationException
   └── StaleSequenceException
