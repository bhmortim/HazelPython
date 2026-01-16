"""WAN replication support for Hazelcast client.

Provides classes for configuring and managing WAN (Wide Area Network)
replication between geographically distributed Hazelcast clusters.
"""

from enum import Enum
from typing import List, Optional

from hazelcast.exceptions import ConfigurationException


class WanSyncState(Enum):
    """State of WAN synchronization operation."""

    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class WanPublisherState(Enum):
    """State of a WAN publisher."""

    REPLICATING = "REPLICATING"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"


class MergePolicy(Enum):
    """Built-in merge policies for WAN replication conflicts.

    When the same key exists in both source and target clusters,
    the merge policy determines which value wins.
    """

    PASS_THROUGH = "com.hazelcast.spi.merge.PassThroughMergePolicy"
    PUT_IF_ABSENT = "com.hazelcast.spi.merge.PutIfAbsentMergePolicy"
    HIGHER_HITS = "com.hazelcast.spi.merge.HigherHitsMergePolicy"
    LATEST_UPDATE = "com.hazelcast.spi.merge.LatestUpdateMergePolicy"
    LATEST_ACCESS = "com.hazelcast.spi.merge.LatestAccessMergePolicy"
    EXPIRATION_TIME = "com.hazelcast.spi.merge.ExpirationTimeMergePolicy"


class WanReplicationRef:
    """Reference to a WAN replication configuration for a data structure.

    Links a map or cache to a WAN replication configuration, specifying
    how data should be replicated to remote clusters.

    Attributes:
        name: Name of the WAN replication configuration.
        merge_policy_class_name: Fully qualified class name of merge policy.
        republishing_enabled: Whether republishing received events is enabled.
        filters: List of filter class names for filtering events.

    Example:
        Basic WAN reference::

            wan_ref = WanReplicationRef("my-wan-replication")
            wan_ref.merge_policy_class_name = MergePolicy.LATEST_UPDATE.value
            wan_ref.republishing_enabled = True

        With filters::

            wan_ref = WanReplicationRef(
                name="filtered-wan",
                filters=["com.example.MyWanFilter"]
            )
    """

    def __init__(
        self,
        name: str,
        merge_policy_class_name: str = MergePolicy.PASS_THROUGH.value,
        republishing_enabled: bool = False,
        filters: Optional[List[str]] = None,
    ):
        self._name = name
        self._merge_policy_class_name = merge_policy_class_name
        self._republishing_enabled = republishing_enabled
        self._filters = filters or []
        self._validate()

    def _validate(self) -> None:
        if not self._name:
            raise ConfigurationException(
                "WAN replication reference name cannot be empty"
            )

    @property
    def name(self) -> str:
        """Get the WAN replication configuration name."""
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value
        self._validate()

    @property
    def merge_policy_class_name(self) -> str:
        """Get the merge policy class name."""
        return self._merge_policy_class_name

    @merge_policy_class_name.setter
    def merge_policy_class_name(self, value: str) -> None:
        self._merge_policy_class_name = value

    @property
    def republishing_enabled(self) -> bool:
        """Get whether republishing is enabled.

        When enabled, events received from other clusters via WAN
        replication will be republished to this reference's targets.
        """
        return self._republishing_enabled

    @republishing_enabled.setter
    def republishing_enabled(self, value: bool) -> None:
        self._republishing_enabled = value

    @property
    def filters(self) -> List[str]:
        """Get the list of filter class names."""
        return self._filters

    @filters.setter
    def filters(self, value: List[str]) -> None:
        self._filters = value or []

    def add_filter(self, filter_class_name: str) -> "WanReplicationRef":
        """Add a filter class name.

        Filters can selectively exclude events from WAN replication
        based on custom logic.

        Args:
            filter_class_name: Fully qualified class name of the filter.

        Returns:
            This instance for method chaining.
        """
        self._filters.append(filter_class_name)
        return self

    @classmethod
    def from_dict(cls, data: dict) -> "WanReplicationRef":
        """Create WanReplicationRef from a dictionary.

        Args:
            data: Dictionary containing WAN reference configuration.

        Returns:
            A new WanReplicationRef instance.
        """
        return cls(
            name=data.get("name", ""),
            merge_policy_class_name=data.get(
                "merge_policy_class_name",
                MergePolicy.PASS_THROUGH.value,
            ),
            republishing_enabled=data.get("republishing_enabled", False),
            filters=data.get("filters", []),
        )

    def to_dict(self) -> dict:
        """Convert to a dictionary representation.

        Returns:
            Dictionary containing the WAN reference configuration.
        """
        return {
            "name": self._name,
            "merge_policy_class_name": self._merge_policy_class_name,
            "republishing_enabled": self._republishing_enabled,
            "filters": self._filters,
        }

    def __repr__(self) -> str:
        return (
            f"WanReplicationRef(name={self._name!r}, "
            f"merge_policy={self._merge_policy_class_name!r}, "
            f"republishing={self._republishing_enabled})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, WanReplicationRef):
            return False
        return (
            self._name == other._name
            and self._merge_policy_class_name == other._merge_policy_class_name
            and self._republishing_enabled == other._republishing_enabled
            and self._filters == other._filters
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._name,
                self._merge_policy_class_name,
                self._republishing_enabled,
                tuple(self._filters),
            )
        )


class WanSyncResult:
    """Result of a WAN synchronization operation.

    Contains information about a completed or in-progress WAN sync,
    including the number of entries synchronized.

    Attributes:
        state: Current state of the synchronization.
        partition_id: The partition being synchronized (-1 for all).
        entries_synced: Number of entries synchronized so far.
    """

    def __init__(
        self,
        state: WanSyncState = WanSyncState.IN_PROGRESS,
        partition_id: int = -1,
        entries_synced: int = 0,
    ):
        self._state = state
        self._partition_id = partition_id
        self._entries_synced = entries_synced

    @property
    def state(self) -> WanSyncState:
        """Get the synchronization state."""
        return self._state

    @property
    def partition_id(self) -> int:
        """Get the partition ID (-1 means all partitions)."""
        return self._partition_id

    @property
    def entries_synced(self) -> int:
        """Get the number of entries synchronized."""
        return self._entries_synced

    @property
    def is_completed(self) -> bool:
        """Check if synchronization is completed."""
        return self._state == WanSyncState.COMPLETED

    @property
    def is_failed(self) -> bool:
        """Check if synchronization failed."""
        return self._state == WanSyncState.FAILED

    def __repr__(self) -> str:
        return (
            f"WanSyncResult(state={self._state.value}, "
            f"partition={self._partition_id}, "
            f"entries_synced={self._entries_synced})"
        )
