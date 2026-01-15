"""Unit tests for hazelcast.listener module."""

import pytest

from hazelcast.listener import (
    LifecycleState,
    LifecycleEvent,
    LifecycleListener,
    FunctionLifecycleListener,
    MemberInfo,
    MembershipEventType,
    MembershipEvent,
    MembershipListener,
    FunctionMembershipListener,
    DistributedObjectEventType,
    DistributedObjectEvent,
    DistributedObjectListener,
    FunctionDistributedObjectListener,
    ListenerRegistration,
    ListenerService,
)


class TestLifecycleState:
    """Tests for LifecycleState enum."""

    def test_values(self):
        assert LifecycleState.STARTING.value == "STARTING"
        assert LifecycleState.CONNECTED.value == "CONNECTED"
        assert LifecycleState.DISCONNECTED.value == "DISCONNECTED"
        assert LifecycleState.SHUTTING_DOWN.value == "SHUTTING_DOWN"
        assert LifecycleState.SHUTDOWN.value == "SHUTDOWN"


class TestLifecycleEvent:
    """Tests for LifecycleEvent class."""

    def test_init(self):
        event = LifecycleEvent(LifecycleState.CONNECTED)
        assert event.state == LifecycleState.CONNECTED
        assert event.previous_state is None

    def test_with_previous_state(self):
        event = LifecycleEvent(
            LifecycleState.CONNECTED,
            previous_state=LifecycleState.STARTING,
        )
        assert event.state == LifecycleState.CONNECTED
        assert event.previous_state == LifecycleState.STARTING

    def test_str(self):
        event = LifecycleEvent(LifecycleState.CONNECTED)
        assert "CONNECTED" in str(event)

    def test_str_with_previous(self):
        event = LifecycleEvent(
            LifecycleState.CONNECTED,
            previous_state=LifecycleState.STARTING,
        )
        s = str(event)
        assert "STARTING" in s
        assert "CONNECTED" in s

    def test_repr(self):
        event = LifecycleEvent(LifecycleState.CONNECTED)
        assert repr(event) == str(event)


class TestFunctionLifecycleListener:
    """Tests for FunctionLifecycleListener."""

    def test_callback_called(self):
        events = []
        listener = FunctionLifecycleListener(lambda e: events.append(e))
        event = LifecycleEvent(LifecycleState.CONNECTED)
        listener.on_state_changed(event)
        assert len(events) == 1
        assert events[0] is event


class TestMemberInfo:
    """Tests for MemberInfo class."""

    def test_init(self):
        member = MemberInfo(
            member_uuid="uuid-123",
            address="192.168.1.1:5701",
        )
        assert member.uuid == "uuid-123"
        assert member.address == "192.168.1.1:5701"
        assert member.is_lite_member is False
        assert member.attributes == {}
        assert member.version is None

    def test_full_init(self):
        member = MemberInfo(
            member_uuid="uuid-123",
            address="192.168.1.1:5701",
            is_lite_member=True,
            attributes={"zone": "us-east"},
            version="5.3.0",
        )
        assert member.is_lite_member is True
        assert member.attributes == {"zone": "us-east"}
        assert member.version == "5.3.0"

    def test_equality(self):
        m1 = MemberInfo("uuid-1", "addr1")
        m2 = MemberInfo("uuid-1", "addr2")
        m3 = MemberInfo("uuid-2", "addr1")
        assert m1 == m2
        assert m1 != m3
        assert m1 != "not a member"

    def test_hash(self):
        m1 = MemberInfo("uuid-1", "addr1")
        m2 = MemberInfo("uuid-1", "addr2")
        assert hash(m1) == hash(m2)

    def test_str_repr(self):
        member = MemberInfo("uuid-123", "192.168.1.1:5701")
        s = str(member)
        assert "uuid-123" in s
        assert "192.168.1.1:5701" in s


class TestMembershipEvent:
    """Tests for MembershipEvent class."""

    def test_init(self):
        member = MemberInfo("uuid-1", "addr1")
        members = [member]
        event = MembershipEvent(
            MembershipEventType.MEMBER_ADDED,
            member,
            members,
        )
        assert event.event_type == MembershipEventType.MEMBER_ADDED
        assert event.member is member
        assert len(event.members) == 1

    def test_members_copy(self):
        member = MemberInfo("uuid-1", "addr1")
        members = [member]
        event = MembershipEvent(MembershipEventType.MEMBER_ADDED, member, members)
        returned_members = event.members
        returned_members.append(MemberInfo("uuid-2", "addr2"))
        assert len(event.members) == 1

    def test_str(self):
        member = MemberInfo("uuid-1", "addr1")
        event = MembershipEvent(MembershipEventType.MEMBER_ADDED, member, [member])
        s = str(event)
        assert "MEMBER_ADDED" in s


class TestFunctionMembershipListener:
    """Tests for FunctionMembershipListener."""

    def test_on_added(self):
        added_events = []
        listener = FunctionMembershipListener(
            on_added=lambda e: added_events.append(e)
        )
        member = MemberInfo("uuid-1", "addr1")
        event = MembershipEvent(MembershipEventType.MEMBER_ADDED, member, [member])
        listener.on_member_added(event)
        assert len(added_events) == 1

    def test_on_removed(self):
        removed_events = []
        listener = FunctionMembershipListener(
            on_removed=lambda e: removed_events.append(e)
        )
        member = MemberInfo("uuid-1", "addr1")
        event = MembershipEvent(MembershipEventType.MEMBER_REMOVED, member, [])
        listener.on_member_removed(event)
        assert len(removed_events) == 1

    def test_none_callbacks(self):
        listener = FunctionMembershipListener()
        member = MemberInfo("uuid-1", "addr1")
        event = MembershipEvent(MembershipEventType.MEMBER_ADDED, member, [member])
        listener.on_member_added(event)
        listener.on_member_removed(event)


class TestDistributedObjectEvent:
    """Tests for DistributedObjectEvent class."""

    def test_init(self):
        event = DistributedObjectEvent(
            DistributedObjectEventType.CREATED,
            "hz:impl:mapService",
            "my-map",
        )
        assert event.event_type == DistributedObjectEventType.CREATED
        assert event.service_name == "hz:impl:mapService"
        assert event.object_name == "my-map"
        assert event.source is None

    def test_with_source(self):
        source = object()
        event = DistributedObjectEvent(
            DistributedObjectEventType.DESTROYED,
            "hz:impl:mapService",
            "my-map",
            source=source,
        )
        assert event.source is source

    def test_str(self):
        event = DistributedObjectEvent(
            DistributedObjectEventType.CREATED,
            "hz:impl:mapService",
            "my-map",
        )
        s = str(event)
        assert "CREATED" in s
        assert "my-map" in s


class TestFunctionDistributedObjectListener:
    """Tests for FunctionDistributedObjectListener."""

    def test_on_created(self):
        created_events = []
        listener = FunctionDistributedObjectListener(
            on_created=lambda e: created_events.append(e)
        )
        event = DistributedObjectEvent(
            DistributedObjectEventType.CREATED, "service", "name"
        )
        listener.on_distributed_object_created(event)
        assert len(created_events) == 1

    def test_on_destroyed(self):
        destroyed_events = []
        listener = FunctionDistributedObjectListener(
            on_destroyed=lambda e: destroyed_events.append(e)
        )
        event = DistributedObjectEvent(
            DistributedObjectEventType.DESTROYED, "service", "name"
        )
        listener.on_distributed_object_destroyed(event)
        assert len(destroyed_events) == 1


class TestListenerService:
    """Tests for ListenerService class."""

    def test_add_lifecycle_listener(self, listener_service):
        class TestListener(LifecycleListener):
            def on_state_changed(self, event):
                pass

        reg_id = listener_service.add_lifecycle_listener(TestListener())
        assert reg_id is not None
        assert len(reg_id) > 0

    def test_add_membership_listener(self, listener_service):
        class TestListener(MembershipListener):
            def on_member_added(self, event):
                pass

            def on_member_removed(self, event):
                pass

        reg_id = listener_service.add_membership_listener(TestListener())
        assert reg_id is not None

    def test_add_distributed_object_listener(self, listener_service):
        class TestListener(DistributedObjectListener):
            def on_distributed_object_created(self, event):
                pass

            def on_distributed_object_destroyed(self, event):
                pass

        reg_id = listener_service.add_distributed_object_listener(TestListener())
        assert reg_id is not None

    def test_remove_listener(self, listener_service):
        listener = FunctionLifecycleListener(lambda e: None)
        reg_id = listener_service.add_lifecycle_listener(listener)
        assert listener_service.remove_listener(reg_id) is True
        assert listener_service.remove_listener(reg_id) is False

    def test_remove_nonexistent_listener(self, listener_service):
        assert listener_service.remove_listener("nonexistent") is False

    def test_fire_lifecycle_event(self, listener_service):
        events = []
        listener = FunctionLifecycleListener(lambda e: events.append(e))
        listener_service.add_lifecycle_listener(listener)
        event = LifecycleEvent(LifecycleState.CONNECTED)
        listener_service.fire_lifecycle_event(event)
        assert len(events) == 1
        assert events[0].state == LifecycleState.CONNECTED

    def test_fire_membership_event_added(self, listener_service):
        events = []
        listener = FunctionMembershipListener(
            on_added=lambda e: events.append(e)
        )
        listener_service.add_membership_listener(listener)
        member = MemberInfo("uuid-1", "addr1")
        event = MembershipEvent(MembershipEventType.MEMBER_ADDED, member, [member])
        listener_service.fire_membership_event(event)
        assert len(events) == 1

    def test_fire_membership_event_removed(self, listener_service):
        events = []
        listener = FunctionMembershipListener(
            on_removed=lambda e: events.append(e)
        )
        listener_service.add_membership_listener(listener)
        member = MemberInfo("uuid-1", "addr1")
        event = MembershipEvent(MembershipEventType.MEMBER_REMOVED, member, [])
        listener_service.fire_membership_event(event)
        assert len(events) == 1

    def test_fire_distributed_object_event_created(self, listener_service):
        events = []
        listener = FunctionDistributedObjectListener(
            on_created=lambda e: events.append(e)
        )
        listener_service.add_distributed_object_listener(listener)
        event = DistributedObjectEvent(
            DistributedObjectEventType.CREATED, "service", "name"
        )
        listener_service.fire_distributed_object_event(event)
        assert len(events) == 1

    def test_fire_distributed_object_event_destroyed(self, listener_service):
        events = []
        listener = FunctionDistributedObjectListener(
            on_destroyed=lambda e: events.append(e)
        )
        listener_service.add_distributed_object_listener(listener)
        event = DistributedObjectEvent(
            DistributedObjectEventType.DESTROYED, "service", "name"
        )
        listener_service.fire_distributed_object_event(event)
        assert len(events) == 1

    def test_clear(self, listener_service):
        listener = FunctionLifecycleListener(lambda e: None)
        reg_id = listener_service.add_lifecycle_listener(listener)
        listener_service.clear()
        assert listener_service.remove_listener(reg_id) is False

    def test_listener_exception_does_not_propagate(self, listener_service):
        def bad_callback(e):
            raise RuntimeError("Listener error")

        listener = FunctionLifecycleListener(bad_callback)
        listener_service.add_lifecycle_listener(listener)
        event = LifecycleEvent(LifecycleState.CONNECTED)
        listener_service.fire_lifecycle_event(event)
