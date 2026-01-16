Partition Service
=================

.. module:: hazelcast.service.partition
   :synopsis: Partition service for cluster partition information.

This module provides access to the cluster's partition table.

Classes
-------

.. autoclass:: Partition
   :members:
   :special-members: __init__

.. autoclass:: PartitionTable
   :members:
   :special-members: __init__

.. autoclass:: PartitionService
   :members:
   :special-members: __init__

Example Usage
-------------

Using the partition service::

    from hazelcast.service.partition import PartitionService

    partition_service = PartitionService(partition_count=271)

    # Get partition for a key
    partition_id = partition_service.get_partition_id("my-key")
    owner = partition_service.get_partition_owner(partition_id)

    # Get all partitions for a member
    member_partitions = partition_service.get_partitions_for_member(member_uuid)
