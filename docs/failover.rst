Failover
========

Hazelcast client supports automatic failover across multiple clusters
for high availability.

Configuration
-------------

Configure failover with multiple cluster configurations:

.. code-block:: python

   from hazelcast.failover import FailoverConfig

   failover = FailoverConfig(try_count=3)

   # Primary cluster (priority 0 = highest)
   failover.add_cluster(
       cluster_name="production-primary",
       addresses=["primary-node1:5701", "primary-node2:5701"],
       priority=0,
   )

   # Backup cluster
   failover.add_cluster(
       cluster_name="production-backup",
       addresses=["backup-node1:5701", "backup-node2:5701"],
       priority=1,
   )


Configuration Options
---------------------

FailoverConfig
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Option
     - Default
     - Description
   * - ``try_count``
     - 3
     - Number of connection attempts per cluster before switching


Adding Clusters
~~~~~~~~~~~~~~~

.. code-block:: python

   # Static addresses
   failover.add_cluster(
       cluster_name="my-cluster",
       addresses=["node1:5701", "node2:5701"],
       priority=0,
   )

   # CNAME-based discovery
   failover.add_cname_cluster(
       cluster_name="dynamic-cluster",
       dns_name="hazelcast.internal.example.com",
       port=5701,
       priority=1,
       refresh_interval=60.0,  # Re-resolve DNS every 60 seconds
   )


Using Failover
--------------

Converting to ClientConfig
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   failover = FailoverConfig(try_count=3)
   failover.add_cluster("primary", ["node1:5701"], priority=0)
   failover.add_cluster("backup", ["node2:5701"], priority=1)

   # Get config for current cluster
   config = failover.to_client_config(failover.current_cluster_index)

   # Connect
   client = HazelcastClient(config)
   client.start()


Manual Failover Logic
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient, ClientOfflineException
   from hazelcast.failover import FailoverConfig

   failover = FailoverConfig(try_count=3)
   failover.add_cluster("primary", ["primary:5701"], priority=0)
   failover.add_cluster("backup", ["backup:5701"], priority=1)

   connected = False
   while not connected and failover.current_cluster_index < failover.cluster_count:
       try:
           config = failover.to_client_config(failover.current_cluster_index)
           client = HazelcastClient(config)
           client.start()
           connected = True
           print(f"Connected to {config.cluster_name}")
       except ClientOfflineException:
           print(f"Failed to connect to cluster {failover.current_cluster_index}")
           failover.switch_to_next_cluster()

   if not connected:
       raise Exception("Could not connect to any cluster")


Failover State Management
-------------------------

.. code-block:: python

   failover = FailoverConfig(try_count=3)
   failover.add_cluster("cluster-a", ["node-a:5701"], priority=0)
   failover.add_cluster("cluster-b", ["node-b:5701"], priority=1)

   # Get current cluster
   current = failover.get_current_cluster()
   print(f"Current: {current.cluster_name}")

   # Switch to next cluster
   next_cluster = failover.switch_to_next_cluster()
   print(f"Switched to: {next_cluster.cluster_name}")

   # Reset to primary
   failover.reset()


Creating from ClientConfigs
---------------------------

Create a FailoverConfig from existing ClientConfig objects:

.. code-block:: python

   from hazelcast import ClientConfig
   from hazelcast.failover import FailoverConfig

   config1 = ClientConfig()
   config1.cluster_name = "east-coast"
   config1.cluster_members = ["east-node1:5701"]

   config2 = ClientConfig()
   config2.cluster_name = "west-coast"
   config2.cluster_members = ["west-node1:5701"]

   failover = FailoverConfig.from_configs([config1, config2])


CNAME Resolution
----------------

For dynamic cluster discovery using DNS:

.. code-block:: python

   from hazelcast.failover import CNAMEResolver

   resolver = CNAMEResolver(
       dns_name="hazelcast.internal.example.com",
       port=5701,
       refresh_interval=30.0,
   )

   # Resolve addresses
   addresses = resolver.resolve()

   # Force refresh
   addresses = resolver.refresh()


Multi-Region Example
--------------------

.. code-block:: python

   failover = FailoverConfig(try_count=5)

   # US East (primary)
   failover.add_cluster(
       cluster_name="us-east",
       addresses=[
           "us-east-1a.hz.internal:5701",
           "us-east-1b.hz.internal:5701",
       ],
       priority=0,
   )

   # US West (secondary)
   failover.add_cluster(
       cluster_name="us-west",
       addresses=["us-west-2a.hz.internal:5701"],
       priority=1,
   )

   # Europe (DR)
   failover.add_cname_cluster(
       cluster_name="eu-dr",
       dns_name="hz-dr.eu.internal",
       port=5701,
       priority=2,
       refresh_interval=120.0,
   )


Best Practices
--------------

1. **Order by latency**: Primary cluster should be geographically closest.

2. **Set appropriate try_count**: Balance between quick failover and
   transient failure tolerance.

3. **Use CNAME for dynamic clusters**: Enables DNS-based cluster
   membership updates.

4. **Test failover regularly**: Ensure backup clusters are healthy and
   accessible.

5. **Monitor cluster health**: Alert when operating on non-primary cluster.
