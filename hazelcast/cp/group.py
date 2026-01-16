"""CP Subsystem group management."""

import threading
from typing import Dict, List, Optional, Set, TYPE_CHECKING
from enum import Enum

from hazelcast.exceptions import IllegalStateException

if TYPE_CHECKING:
    from hazelcast.proxy.base import ProxyContext


class CPGroupStatus(Enum):
    """Status of a CP group."""
    ACTIVE = "ACTIVE"
    DESTROYING = "DESTROYING"
    DESTROYED = "DESTROYED"


class CPMember:
    """Represents a member of a CP group.

    Attributes:
        uuid: The unique identifier of the member.
        address: The network address of the member.
    """

    def __init__(self, uuid: str, address: str = ""):
        self._uuid = uuid
        self._address = address

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def address(self) -> str:
        return self._address

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CPMember):
            return False
        return self._uuid == other._uuid

    def __hash__(self) -> int:
        return hash(self._uuid)

    def __repr__(self) -> str:
        return f"CPMember(uuid={self._uuid!r}, address={self._address!r})"


class CPGroup:
    """Represents a CP group in the cluster.

    A CP group is a cluster of CP members that run the Raft consensus
    algorithm. CP data structures are bound to CP groups.

    Attributes:
        name: The name of the CP group.
        group_id: The unique identifier of the group.
        status: Current status of the group.
        members: Set of members in this group.
    """

    DEFAULT_GROUP_NAME = "default"
    METADATA_GROUP_NAME = "METADATA"

    def __init__(
        self,
        name: str,
        group_id: str,
        members: Optional[Set[CPMember]] = None,
    ):
        self._name = name
        self._group_id = group_id
        self._members = members or set()
        self._status = CPGroupStatus.ACTIVE

    @property
    def name(self) -> str:
        return self._name

    @property
    def group_id(self) -> str:
        return self._group_id

    @property
    def status(self) -> CPGroupStatus:
        return self._status

    @property
    def members(self) -> Set[CPMember]:
        return self._members.copy()

    @property
    def member_count(self) -> int:
        return len(self._members)

    def is_active(self) -> bool:
        return self._status == CPGroupStatus.ACTIVE

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CPGroup):
            return False
        return self._group_id == other._group_id

    def __hash__(self) -> int:
        return hash(self._group_id)

    def __repr__(self) -> str:
        return (
            f"CPGroup(name={self._name!r}, id={self._group_id!r}, "
            f"status={self._status.value}, members={self.member_count})"
        )


class CPGroupManager:
    """Manages CP groups for the client.

    Provides APIs for querying and managing CP groups in the cluster.

    Example:
        >>> group_mgr = client.get_cp_subsystem().get_group_manager()
        >>> groups = group_mgr.get_groups()
        >>> default_group = group_mgr.get_group("default")
    """

    def __init__(self, context: Optional["ProxyContext"] = None):
        self._context = context
        self._groups: Dict[str, CPGroup] = {}
        self._lock = threading.Lock()
        self._closed = False

    def get_group(self, name: str) -> Optional[CPGroup]:
        """Get a CP group by name.

        Args:
            name: The name of the CP group.

        Returns:
            The CP group if it exists, None otherwise.
        """
        with self._lock:
            return self._groups.get(name)

    def get_groups(self) -> List[CPGroup]:
        """Get all known CP groups.

        Returns:
            List of all CP groups.
        """
        with self._lock:
            return list(self._groups.values())

    def get_active_groups(self) -> List[CPGroup]:
        """Get all active CP groups.

        Returns:
            List of active CP groups.
        """
        with self._lock:
            return [g for g in self._groups.values() if g.is_active()]

    def register_group(self, group: CPGroup) -> None:
        """Register a CP group.

        Args:
            group: The CP group to register.

        Raises:
            IllegalStateException: If manager is closed.
        """
        self._check_not_closed()
        with self._lock:
            self._groups[group.name] = group

    def get_default_group(self) -> Optional[CPGroup]:
        """Get the default CP group.

        Returns:
            The default CP group if it exists.
        """
        return self.get_group(CPGroup.DEFAULT_GROUP_NAME)

    def get_metadata_group(self) -> Optional[CPGroup]:
        """Get the metadata CP group.

        Returns:
            The metadata CP group if it exists.
        """
        return self.get_group(CPGroup.METADATA_GROUP_NAME)

    def contains_group(self, name: str) -> bool:
        """Check if a group exists.

        Args:
            name: The group name.

        Returns:
            True if the group exists.
        """
        with self._lock:
            return name in self._groups

    def remove_group(self, name: str) -> Optional[CPGroup]:
        """Remove a CP group from the manager.

        Args:
            name: The group name.

        Returns:
            The removed group if it existed.
        """
        with self._lock:
            return self._groups.pop(name, None)

    def clear(self) -> None:
        """Clear all groups from the manager."""
        with self._lock:
            self._groups.clear()

    def shutdown(self) -> None:
        """Shutdown the group manager."""
        with self._lock:
            self._closed = True
            self._groups.clear()

    @property
    def is_closed(self) -> bool:
        """Check if the manager is closed."""
        with self._lock:
            return self._closed

    def _check_not_closed(self) -> None:
        if self._closed:
            raise IllegalStateException("CPGroupManager is closed")

    def __repr__(self) -> str:
        with self._lock:
            active = sum(1 for g in self._groups.values() if g.is_active())
            return f"CPGroupManager(groups={len(self._groups)}, active={active})"
