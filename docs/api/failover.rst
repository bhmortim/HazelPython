Failover Configuration
======================

.. module:: hazelcast.failover
   :synopsis: Failover configuration for high availability.

This module provides failover configuration for Hazelcast clients to
support high availability across multiple clusters.

Classes
-------

.. autoclass:: ClusterConfig
   :members:
   :special-members: __init__

.. autoclass:: CNAMEResolver
   :members:
   :special-members: __init__

.. autoclass:: FailoverConfig
   :members:
   :special-members: __init__

Example Usage
-------------

Basic failover configuration::

    from hazelcast.failover import FailoverConfig

    failover = FailoverConfig(try_count=3)
    failover.add_cluster("primary", ["node1:5701", "node2:5701"])
    failover.add_cluster("backup", ["backup1:5701"], priority=1)

CNAME-based discovery::

    failover = FailoverConfig()
    failover.add_cname_cluster(
        "production",
        dns_name="hazelcast.example.com",
        port=5701,
        refresh_interval=60.0
    )

Creating from multiple client configs::

    from hazelcast.config import ClientConfig
    from hazelcast.failover import FailoverConfig

    config1 = ClientConfig()
    config1.cluster_name = "primary"
    config2 = ClientConfig()
    config2.cluster_name = "backup"

    failover = FailoverConfig.from_configs([config1, config2])
