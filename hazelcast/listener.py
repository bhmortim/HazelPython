"""Listener support for Hazelcast client events."""

import threading
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from typing import Callable, Dict, List, Optional, Set, Any


class LifecycleState(Enum):
    """Client lifecycle states."""

    STARTING = "STARTING"
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    SHUTTING_DOWN = "SHUTTING_DOWN"
    SHUTDOWN = "SHUTDOWN"
    CLIENT_CONNECTED = "CLIENT_CONNECTED"
    CLIENT_DISCONNECTED = "CLIENT_DISCONNECTED"


class LifecycleEvent:
    """Event fired when client lifecycle state changes."""

    def __init__(self, state: LifecycleState, previous_state: Optional[LifecycleState] = None):
        self._state = state
        self._previous_state = previous_state

    @property
    def state(self) -> LifecycleState:
        return self._state

    @property
    def previous_state(self) -> Optional[LifecycleState]:
        return self._previous_state

    def __str__(self) -> str:
        if self._previous_state:
            return f"LifecycleEvent({self._previous_state.value} -> {self._state.value})"
        return f"LifecycleEvent({self._state.value})"

    def __repr__(self) -> str:
        return self.__str__()


class LifecycleListener(ABC):
    """Listener for client lifecycle events."""

    @abstractmethod
    def on_state_changed(self, event: LifecycleEvent) -> None:
        """Called when lifecycle state changes."""
        pass


class FunctionLifecycleListener(LifecycleListener):
    """Lifecycle listener that delegates to a function."""

    def __init__(self, callback: Callable[[LifecycleEvent], None]):
        self._callback = callback

    def on_state_changed(self, event: LifecycleEvent) -> None:
        self._callback(event)


class MemberInfo:
    """Information about a cluster member."""

    def __init__(
        self,
        member_uuid: str,
        address: str,
        is_lite_member: bool = False,
        attributes: Optional[Dict[str, str]] = None,
        version: Optional[str] = None,
    ):
        self._uuid = member_uuid
        self._address = address
        self._is_lite_member = is_lite_member
        self._attributes = attributes or {}
        self._version = version

    @property
    def uuid(self) -> str:
        return self._uuid

    @property
    def address(self) -> str:
        return self._address

    @property
    def is_lite_member(self) -> bool:
        return self._is_lite_member

    @property
    def attributes(self) -> Dict[str, str]:
        return self._attributes

    @property
    def version(self) -> Optional[str]:
        return self._version

    def __str__(self) -> str:
        return f"Member[uuid={self._uuid}, address={self._address}]"

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MemberInfo):
            return False
        return self._uuid == other._uuid

    def __hash__(self) -> int:
        return hash(self._uuid)


class MembershipEventType(Enum):
    """Type of membership event."""

    MEMBER_ADDED = "MEMBER_ADDED"
    MEMBER_REMOVED = "MEMBER_REMOVED"


class MembershipEvent:
    """Event fired when cluster membership changes."""

    def __init__(
        self,
        event_type: MembershipEventType,
        member: MemberInfo,
        members: List[MemberInfo],
    ):
        self._event_type = event_type
        self._member = member
        self._members = members

    @property
    def event_type(self) -> MembershipEventType:
        return self._event_type

    @property
    def member(self) -> MemberInfo:
        return self._member

    @property
    def members(self) -> List[MemberInfo]:
        return list(self._members)

    def __str__(self) -> str:
        return f"MembershipEvent({self._event_type.value}, {self._member})"

    def __repr__(self) -> str:
        return self.__str__()


class MembershipListener(ABC):
    """Listener for cluster membership events."""

    @abstractmethod
    def on_member_added(self, event: MembershipEvent) -> None:
        """Called when a member is added to the cluster."""
        pass

    @abstractmethod
    def on_member_removed(self, event: MembershipEvent) -> None:
        """Called when a member is removed from the cluster."""
        pass


class FunctionMembershipListener(MembershipListener):
    """Membership listener that delegates to functions."""

    def __init__(
        self,
        on_added: Optional[Callable[[MembershipEvent], None]] = None,
        on_removed: Optional[Callable[[MembershipEvent], None]] = None,
    ):
        self._on_added = on_added
        self._on_removed = on_removed

    def on_member_added(self, event: MembershipEvent) -> None:
        if self._on_added:
            self._on_added(event)

    def on_member_removed(self, event: MembershipEvent) -> None:
        if self._on_removed:
            self._on_removed(event)


class DistributedObjectEventType(Enum):
    """Type of distributed object event."""

    CREATED = "CREATED"
    DESTROYED = "DESTROYED"


class DistributedObjectEvent:
    """Event fired when a distributed object is created or destroyed."""

    def __init__(
        self,
        event_type: DistributedObjectEventType,
        service_name: str,
        object_name: str,
        source: Any = None,
    ):
        self._event_type = event_type
        self._service_name = service_name
        self._object_name = object_name
        self._source = source

    @property
    def event_type(self) -> DistributedObjectEventType:
        return self._event_type

    @property
    def service_name(self) -> str:
        return self._service_name

    @property
    def object_name(self) -> str:
        return self._object_name

    @property
    def source(self) -> Any:
        return self._source

    def __str__(self) -> str:
        return (
            f"DistributedObjectEvent({self._event_type.value}, "
            f"service={self._service_name}, name={self._object_name})"
        )

    def __repr__(self) -> str:
        return self.__str__()


class DistributedObjectListener(ABC):
    """Listener for distributed object events."""

    @abstractmethod
    def on_distributed_object_created(self, event: DistributedObjectEvent) -> None:
        """Called when a distributed object is created."""
        pass

    @abstractmethod
    def on_distributed_object_destroyed(self, event: DistributedObjectEvent) -> None:
        """Called when a distributed object is destroyed."""
        pass


class FunctionDistributedObjectListener(DistributedObjectListener):
    """Distributed object listener that delegates to functions."""

    def __init__(
        self,
        on_created: Optional[Callable[[DistributedObjectEvent], None]] = None,
        on_destroyed: Optional[Callable[[DistributedObjectEvent], None]] = None,
    ):
        self._on_created = on_created
        self._on_destroyed = on_destroyed

    def on_distributed_object_created(self, event: DistributedObjectEvent) -> None:
        if self._on_created:
            self._on_created(event)

    def on_distributed_object_destroyed(self, event: DistributedObjectEvent) -> None:
        if self._on_destroyed:
            self._on_destroyed(event)


class PartitionLostEvent:
    """Event fired when a partition loses data."""

    def __init__(
        self,
        partition_id: int,
        lost_backup_count: int,
        source: Optional[MemberInfo] = None,
    ):
        self._partition_id = partition_id
        self._lost_backup_count = lost_backup_count
        self._source = source

    @property
    def partition_id(self) -> int:
        return self._partition_id

    @property
    def lost_backup_count(self) -> int:
        return self._lost_backup_count

    @property
    def source(self) -> Optional[MemberInfo]:
        return self._source

    def __str__(self) -> str:
        return f"PartitionLostEvent(partition_id={self._partition_id}, lost_backup_count={self._lost_backup_count})"

    def __repr__(self) -> str:
        return self.__str__()


class PartitionLostListener(ABC):
    """Listener for partition lost events."""

    @abstractmethod
    def on_partition_lost(self, event: PartitionLostEvent) -> None:
        """Called when a partition loses data."""
        pass


class FunctionPartitionLostListener(PartitionLostListener):
    """Partition lost listener that delegates to a function."""

    def __init__(self, callback: Callable[[PartitionLostEvent], None]):
        self._callback = callback

    def on_partition_lost(self, event: PartitionLostEvent) -> None:
        self._callback(event)


class MigrationState(Enum):
    """State of a partition migration."""

    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class MigrationEvent:
    """Event fired during partition migration."""

    def __init__(
        self,
        partition_id: int,
        old_owner: Optional[MemberInfo],
        new_owner: Optional[MemberInfo],
        migration_state: MigrationState,
    ):
        self._partition_id = partition_id
        self._old_owner = old_owner
        self._new_owner = new_owner
        self._migration_state = migration_state

    @property
    def partition_id(self) -> int:
        return self._partition_id

    @property
    def old_owner(self) -> Optional[MemberInfo]:
        return self._old_owner

    @property
    def new_owner(self) -> Optional[MemberInfo]:
        return self._new_owner

    @property
    def migration_state(self) -> MigrationState:
        return self._migration_state

    def __str__(self) -> str:
        return (
            f"MigrationEvent(partition_id={self._partition_id}, "
            f"state={self._migration_state.value}, "
            f"old_owner={self._old_owner}, new_owner={self._new_owner})"
        )

    def __repr__(self) -> str:
        return self.__str__()


class MigrationListener(ABC):
    """Listener for partition migration events."""

    @abstractmethod
    def on_migration_started(self, event: MigrationEvent) -> None:
        """Called when a partition migration starts."""
        pass

    @abstractmethod
    def on_migration_completed(self, event: MigrationEvent) -> None:
        """Called when a partition migration completes successfully."""
        pass

    @abstractmethod
    def on_migration_failed(self, event: MigrationEvent) -> None:
        """Called when a partition migration fails."""
        pass


class FunctionMigrationListener(MigrationListener):
    """Migration listener that delegates to functions."""

    def __init__(
        self,
        on_started: Optional[Callable[[MigrationEvent], None]] = None,
        on_completed: Optional[Callable[[MigrationEvent], None]] = None,
        on_failed: Optional[Callable[[MigrationEvent], None]] = None,
    ):
        self._on_started = on_started
        self._on_completed = on_completed
        self._on_failed = on_failed

    def on_migration_started(self, event: MigrationEvent) -> None:
        if self._on_started:
            self._on_started(event)

    def on_migration_completed(self, event: MigrationEvent) -> None:
        if self._on_completed:
            self._on_completed(event)

    def on_migration_failed(self, event: MigrationEvent) -> None:
        if self._on_failed:
            self._on_failed(event)


class InitialMembershipEvent:
    """Event containing the initial set of cluster members."""

    def __init__(self, members: List[MemberInfo]):
        self._members = list(members)

    @property
    def members(self) -> List[MemberInfo]:
        return list(self._members)

    def __str__(self) -> str:
        return f"InitialMembershipEvent(members={len(self._members)})"

    def __repr__(self) -> str:
        return self.__str__()


class InitialMembershipListener(MembershipListener):
    """Membership listener that receives initial members on registration.

    When registered, the init() method is immediately called with the
    current set of cluster members before any membership events are fired.
    """

    @abstractmethod
    def init(self, event: InitialMembershipEvent) -> None:
        """Called with the initial set of members when listener is registered."""
        pass


class FunctionInitialMembershipListener(InitialMembershipListener):
    """Initial membership listener that delegates to functions."""

    def __init__(
        self,
        on_init: Optional[Callable[[InitialMembershipEvent], None]] = None,
        on_added: Optional[Callable[[MembershipEvent], None]] = None,
        on_removed: Optional[Callable[[MembershipEvent], None]] = None,
    ):
        self._on_init = on_init
        self._on_added = on_added
        self._on_removed = on_removed

    def init(self, event: InitialMembershipEvent) -> None:
        if self._on_init:
            self._on_init(event)

    def on_member_added(self, event: MembershipEvent) -> None:
        if self._on_added:
            self._on_added(event)

    def on_member_removed(self, event: MembershipEvent) -> None:
        if self._on_removed:
            self._on_removed(event)


class ListenerRegistration:
    """Holds registration information for a listener."""

    def __init__(
        self,
        registration_id: str,
        listener: Any,
        listener_type: str,
    ):
        self._registration_id = registration_id
        self._listener = listener
        self._listener_type = listener_type

    @property
    def registration_id(self) -> str:
        return self._registration_id

    @property
    def listener(self) -> Any:
        return self._listener

    @property
    def listener_type(self) -> str:
        return self._listener_type


class ListenerService:
    """Service for managing client-side listeners."""

    LIFECYCLE_LISTENER = "lifecycle"
    MEMBERSHIP_LISTENER = "membership"
    DISTRIBUTED_OBJECT_LISTENER = "distributed_object"
    PARTITION_LOST_LISTENER = "partition_lost"
    MIGRATION_LISTENER = "migration"

    def __init__(self):
        self._registrations: Dict[str, ListenerRegistration] = {}
        self._lifecycle_listeners: Dict[str, LifecycleListener] = {}
        self._membership_listeners: Dict[str, MembershipListener] = {}
        self._distributed_object_listeners: Dict[str, DistributedObjectListener] = {}
        self._partition_lost_listeners: Dict[str, PartitionLostListener] = {}
        self._migration_listeners: Dict[str, MigrationListener] = {}
        self._current_members: List[MemberInfo] = []
        self._lock = threading.Lock()

    def add_lifecycle_listener(
        self,
        listener: LifecycleListener,
    ) -> str:
        """Register a lifecycle listener.

        Args:
            listener: The listener to register.

        Returns:
            Registration ID for removing the listener.
        """
        registration_id = str(uuid.uuid4())
        with self._lock:
            self._lifecycle_listeners[registration_id] = listener
            self._registrations[registration_id] = ListenerRegistration(
                registration_id, listener, self.LIFECYCLE_LISTENER
            )
        return registration_id

    def add_membership_listener(
        self,
        listener: MembershipListener,
    ) -> str:
        """Register a membership listener.

        Args:
            listener: The listener to register.

        Returns:
            Registration ID for removing the listener.
        """
        registration_id = str(uuid.uuid4())
        with self._lock:
            self._membership_listeners[registration_id] = listener
            self._registrations[registration_id] = ListenerRegistration(
                registration_id, listener, self.MEMBERSHIP_LISTENER
            )
        return registration_id

    def add_distributed_object_listener(
        self,
        listener: DistributedObjectListener,
    ) -> str:
        """Register a distributed object listener.

        Args:
            listener: The listener to register.

        Returns:
            Registration ID for removing the listener.
        """
        registration_id = str(uuid.uuid4())
        with self._lock:
            self._distributed_object_listeners[registration_id] = listener
            self._registrations[registration_id] = ListenerRegistration(
                registration_id, listener, self.DISTRIBUTED_OBJECT_LISTENER
            )
        return registration_id

    def set_current_members(self, members: List[MemberInfo]) -> None:
        """Set the current cluster members for initial membership listeners."""
        with self._lock:
            self._current_members = list(members)

    def add_partition_lost_listener(
        self,
        listener: PartitionLostListener,
    ) -> str:
        """Register a partition lost listener.

        Args:
            listener: The listener to register.

        Returns:
            Registration ID for removing the listener.
        """
        registration_id = str(uuid.uuid4())
        with self._lock:
            self._partition_lost_listeners[registration_id] = listener
            self._registrations[registration_id] = ListenerRegistration(
                registration_id, listener, self.PARTITION_LOST_LISTENER
            )
        return registration_id

    def add_migration_listener(
        self,
        listener: MigrationListener,
    ) -> str:
        """Register a migration listener.

        Args:
            listener: The listener to register.

        Returns:
            Registration ID for removing the listener.
        """
        registration_id = str(uuid.uuid4())
        with self._lock:
            self._migration_listeners[registration_id] = listener
            self._registrations[registration_id] = ListenerRegistration(
                registration_id, listener, self.MIGRATION_LISTENER
            )
        return registration_id

    def add_initial_membership_listener(
        self,
        listener: InitialMembershipListener,
    ) -> str:
        """Register an initial membership listener.

        The listener's init() method is called immediately with current members,
        then on_member_added/on_member_removed are called for future changes.

        Args:
            listener: The listener to register.

        Returns:
            Registration ID for removing the listener.
        """
        registration_id = str(uuid.uuid4())
        with self._lock:
            self._membership_listeners[registration_id] = listener
            self._registrations[registration_id] = ListenerRegistration(
                registration_id, listener, self.MEMBERSHIP_LISTENER
            )
            current_members = list(self._current_members)

        try:
            event = InitialMembershipEvent(current_members)
            listener.init(event)
        except Exception:
            pass

        return registration_id

    def remove_listener(self, registration_id: str) -> bool:
        """Remove a registered listener.

        Args:
            registration_id: The registration ID returned when adding the listener.

        Returns:
            True if the listener was removed, False if not found.
        """
        with self._lock:
            registration = self._registrations.pop(registration_id, None)
            if not registration:
                return False

            if registration.listener_type == self.LIFECYCLE_LISTENER:
                self._lifecycle_listeners.pop(registration_id, None)
            elif registration.listener_type == self.MEMBERSHIP_LISTENER:
                self._membership_listeners.pop(registration_id, None)
            elif registration.listener_type == self.DISTRIBUTED_OBJECT_LISTENER:
                self._distributed_object_listeners.pop(registration_id, None)
            elif registration.listener_type == self.PARTITION_LOST_LISTENER:
                self._partition_lost_listeners.pop(registration_id, None)
            elif registration.listener_type == self.MIGRATION_LISTENER:
                self._migration_listeners.pop(registration_id, None)

            return True

    def fire_lifecycle_event(self, event: LifecycleEvent) -> None:
        """Fire a lifecycle event to all registered listeners."""
        with self._lock:
            listeners = list(self._lifecycle_listeners.values())

        for listener in listeners:
            try:
                listener.on_state_changed(event)
            except Exception:
                pass

    def fire_membership_event(self, event: MembershipEvent) -> None:
        """Fire a membership event to all registered listeners."""
        with self._lock:
            listeners = list(self._membership_listeners.values())

        for listener in listeners:
            try:
                if event.event_type == MembershipEventType.MEMBER_ADDED:
                    listener.on_member_added(event)
                elif event.event_type == MembershipEventType.MEMBER_REMOVED:
                    listener.on_member_removed(event)
            except Exception:
                pass

    def fire_distributed_object_event(self, event: DistributedObjectEvent) -> None:
        """Fire a distributed object event to all registered listeners."""
        with self._lock:
            listeners = list(self._distributed_object_listeners.values())

        for listener in listeners:
            try:
                if event.event_type == DistributedObjectEventType.CREATED:
                    listener.on_distributed_object_created(event)
                elif event.event_type == DistributedObjectEventType.DESTROYED:
                    listener.on_distributed_object_destroyed(event)
            except Exception:
                pass

    def fire_partition_lost_event(self, event: PartitionLostEvent) -> None:
        """Fire a partition lost event to all registered listeners."""
        with self._lock:
            listeners = list(self._partition_lost_listeners.values())

        for listener in listeners:
            try:
                listener.on_partition_lost(event)
            except Exception:
                pass

    def fire_migration_event(self, event: MigrationEvent) -> None:
        """Fire a migration event to all registered listeners."""
        with self._lock:
            listeners = list(self._migration_listeners.values())

        for listener in listeners:
            try:
                if event.migration_state == MigrationState.STARTED:
                    listener.on_migration_started(event)
                elif event.migration_state == MigrationState.COMPLETED:
                    listener.on_migration_completed(event)
                elif event.migration_state == MigrationState.FAILED:
                    listener.on_migration_failed(event)
            except Exception:
                pass

    def clear(self) -> None:
        """Remove all registered listeners."""
        with self._lock:
            self._registrations.clear()
            self._lifecycle_listeners.clear()
            self._membership_listeners.clear()
            self._distributed_object_listeners.clear()
            self._partition_lost_listeners.clear()
            self._migration_listeners.clear()
            self._current_members.clear()
