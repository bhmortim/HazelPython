Configuration API
=================

.. automodule:: hazelcast.config
   :members:
   :undoc-members:
   :show-inheritance:

Configuration Classes
---------------------

ClientConfig
~~~~~~~~~~~~

Main configuration class for the Hazelcast client.

NetworkConfig
~~~~~~~~~~~~~

Network-related configuration including addresses and timeouts.

SecurityConfig
~~~~~~~~~~~~~~

Security configuration including credentials and TLS settings.

SerializationConfig
~~~~~~~~~~~~~~~~~~~

Serialization settings including portable and custom serializers.

NearCacheConfig
~~~~~~~~~~~~~~~

Near cache configuration for client-side caching.

DiscoveryConfig
~~~~~~~~~~~~~~~

Cloud discovery configuration for automatic cluster member discovery.

Enumerations
------------

.. autoclass:: hazelcast.config.ReconnectMode
   :members:
   :undoc-members:

.. autoclass:: hazelcast.config.EvictionPolicy
   :members:
   :undoc-members:

.. autoclass:: hazelcast.config.InMemoryFormat
   :members:
   :undoc-members:

.. autoclass:: hazelcast.config.DiscoveryStrategyType
   :members:
   :undoc-members:
