"""Partition service for cluster partition information."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING
import threading
import hashlib

if TYPE_CHECKING:
    from hazelcast.listener import MemberInfo
    from hazelcast.serialization.service import SerializationService


@dataclass
class Partition:
    """Represents a single partition in the cluster."""

    partition_id: int
    owner_uuid: Optional[str] = None
    replica_uuids: List[str] = field(default_factory=list)

    @property
    def is_assigned(self) -> bool:
        """Check if this partition has an owner."""
        return self.owner_uuid is not None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Partition):
            return False
        return self.partition_id == other.partition_id

    def __hash__(self) -> int:
        return hash(self.partition_id)


@dataclass
class PartitionTable:
    """Represents the complete partition table for the cluster."""

    partitions: Dict[int, Partition] = field(default_factory=dict)
    version: int = 0

    @property
    def partition_count(self) -> int:
        """Get the total number of partitions."""
        return len(self.partitions)

    def get_partition(self, partition_id: int) -> Optional[Partition]:
        """Get a partition by ID."""
        return self.partitions.get(partition_id)

    def get_partitions_for_owner(self, owner_uuid: str) -> List[Partition]:
        """Get all partitions owned by a member."""
        return [
            p for p in self.partitions.values()
            if p.owner_uuid == owner_uuid
        ]


class PartitionService:
    """Service for managing partition information.

    Provides methods to query partition ownership, compute partition IDs
    for keys, and monitor partition table changes.
    """

    DEFAULT_PARTITION_COUNT = 271

    def __init__(
        self,
        partition_count: int = DEFAULT_PARTITION_COUNT,
        serialization_service: Optional["SerializationService"] = None,
    ):
        self._partition_count = partition_count
        self._serialization_service = serialization_service
        self._partition_table = PartitionTable()
        self._lock = threading.RLock()
        self._initialized = False

        self._initialize_partitions()

    def _initialize_partitions(self) -> None:
        """Initialize the partition table with empty partitions."""
        with self._lock:
            for i in range(self._partition_count):
                self._partition_table.partitions[i] = Partition(partition_id=i)

    @property
    def partition_count(self) -> int:
        """Get the total number of partitions."""
        return self._partition_count

    @property
    def is_initialized(self) -> bool:
        """Check if the partition table has been initialized from the cluster."""
        return self._initialized

    def get_partition_id(self, key: Any) -> int:
        """Compute the partition ID for a key.

        Args:
            key: The key to compute partition for.

        Returns:
            The partition ID (0 to partition_count - 1).
        """
        key_hash = self._compute_key_hash(key)
        return abs(key_hash) % self._partition_count

    def _compute_key_hash(self, key: Any) -> int:
        """Compute a hash value for a key."""
        if self._serialization_service:
            data = self._serialization_service.to_data(key)
            key_bytes = bytes(data)
        elif isinstance(key, bytes):
            key_bytes = key
        elif isinstance(key, str):
            key_bytes = key.encode("utf-8")
        else:
            key_bytes = str(key).encode("utf-8")

        hash_value = int(hashlib.md5(key_bytes).hexdigest()[:8], 16)
        return hash_value

    def get_partition(self, partition_id: int) -> Optional[Partition]:
        """Get partition information by ID.

        Args:
            partition_id: The partition ID.

        Returns:
            Partition information, or None if invalid ID.
        """
        if 0 <= partition_id < self._partition_count:
            with self._lock:
                return self._partition_table.get_partition(partition_id)
        return None

    def get_partition_owner(self, partition_id: int) -> Optional[str]:
        """Get the owner UUID for a partition.

        Args:
            partition_id: The partition ID.

        Returns:
            Owner member UUID, or None if not assigned.
        """
        partition = self.get_partition(partition_id)
        return partition.owner_uuid if partition else None

    def get_partition_owner_for_key(self, key: Any) -> Optional[str]:
        """Get the owner UUID for a key's partition.

        Args:
            key: The key to look up.

        Returns:
            Owner member UUID, or None if not assigned.
        """
        partition_id = self.get_partition_id(key)
        return self.get_partition_owner(partition_id)

    def get_partitions(self) -> List[Partition]:
        """Get all partitions.

        Returns:
            List of all partitions.
        """
        with self._lock:
            return list(self._partition_table.partitions.values())

    def get_partitions_for_member(self, member_uuid: str) -> List[Partition]:
        """Get all partitions owned by a member.

        Args:
            member_uuid: The member UUID.

        Returns:
            List of partitions owned by the member.
        """
        with self._lock:
            return self._partition_table.get_partitions_for_owner(member_uuid)

    def get_partition_count_for_member(self, member_uuid: str) -> int:
        """Get the number of partitions owned by a member.

        Args:
            member_uuid: The member UUID.

        Returns:
            Number of partitions owned.
        """
        return len(self.get_partitions_for_member(member_uuid))

    def update_partition_table(
        self,
        partitions: Dict[int, str],
        version: int,
    ) -> bool:
        """Update the partition table with new ownership information.

        Args:
            partitions: Mapping of partition ID to owner UUID.
            version: The partition table version.

        Returns:
            True if the table was updated, False if version is older.
        """
        with self._lock:
            if version <= self._partition_table.version and self._initialized:
                return False

            for partition_id, owner_uuid in partitions.items():
                if 0 <= partition_id < self._partition_count:
                    partition = self._partition_table.partitions.get(partition_id)
                    if partition:
                        partition.owner_uuid = owner_uuid

            self._partition_table.version = version
            self._initialized = True
            return True

    def get_partition_table_version(self) -> int:
        """Get the current partition table version.

        Returns:
            The partition table version.
        """
        with self._lock:
            return self._partition_table.version

    def reset(self) -> None:
        """Reset the partition table to uninitialized state."""
        with self._lock:
            self._initialized = False
            for partition in self._partition_table.partitions.values():
                partition.owner_uuid = None
                partition.replica_uuids.clear()
            self._partition_table.version = 0

    def __repr__(self) -> str:
        return (
            f"PartitionService(partition_count={self._partition_count}, "
            f"initialized={self._initialized}, "
            f"version={self._partition_table.version})"
        )
