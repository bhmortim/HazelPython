"""Unit tests for listener infrastructure."""

import threading
import pytest
from unittest.mock import Mock, MagicMock

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
    def test_all_states_exist(self):
        assert LifecycleState.STARTING.value == "STARTING"
        assert LifecycleState.CONNECTED.value == "CONNECTED"
        assert LifecycleState.DISCONNECTED.value == "DISCONNECTED"
        assert LifecycleState.SHUTTING_DOWN.value == "SHUTTING_DOWN"
        assert LifecycleState.SHUTDOWN.value == "SHUTDOWN"
        assert LifecycleState.CLIENT_CONNECTED.value == "CLIENT_CONNECTED"
        assert LifecycleState.CLIENT_DISCONNECTED.value == "CLIENT_DISCONNECTED"


class TestLifecycleEvent:
    def test_event_with_state_only(self):
        event = LifecycleEvent(LifecycleState.STARTING)
        assert event.state == LifecycleState.STARTING
        assert event.previous_state is None

    def test_event_with_previous_state(self):
        event = LifecycleEvent(LifecycleState.CONNECTED, LifecycleState.STARTING)
        assert event.state == LifecycleState.CONNECTED
        assert event.previous_state == LifecycleState.STARTING

    def test_str_without_previous_state(self):
        event = LifecycleEvent(LifecycleState.STARTING)
        assert str(event) == "LifecycleEvent(STARTING)"

    def test_str_with_previous_state(self):
        event = LifecycleEvent(LifecycleState.CONNECTED, LifecycleState.STARTING)
        assert str(event) == "LifecycleEvent(STARTING -> CONNECTED)"

    def test_repr(self):
        event = LifecycleEvent(LifecycleState.SHUTDOWN)
        assert repr(event) == str(event)


class TestFunctionLifecycleListener:
    def test_callback_is_invoked(self):
        callback = Mock()
        listener = FunctionLifecycleListener(callback)
        event = LifecycleEvent(LifecycleState.STARTING)

        listener.on_state_changed(event)

        callback.assert_called_once_with(event)


class TestMemberInfo:
    def test_basic_properties(self):
        member = MemberInfo("uuid-123", "127.0.0.1:5701")
        assert member.uuid == "uuid-123"
        assert member.address == "127.0.0.1:5701"
        assert member.is_lite_member is False
        assert member.attributes == {}
        assert member.version is None

    def test_all_properties(self):
        attrs = {"role": "worker"}
        member = MemberInfo(
            "uuid-456",
            "192.168.1.1:5701",
            is_lite_member=True,
            attributes=attrs,
            version="5.3.0",
        )
        assert member.uuid == "uuid-456"
        assert member.address == "192.168.1.1:5701"
        assert member.is_lite_member is True
        assert member.attributes == {"role": "worker"}
        assert member.version == "5.3.0"

    def test_str(self):
        member = MemberInfo("uuid-123", "127.0.0.1:5701")
        assert str(member) == "Member[uuid=uuid-123, address=127.0.0.1:5701]"

    def test_repr(self):
        member = MemberInfo("uuid-123", "127.0.0.1:5701")
        assert repr(member) == str(member)

    def test_equality_same_uuid(self):
        member1 = MemberInfo("uuid-123", "127.0.0.1:5701")
        member2 = MemberInfo("uuid-123", "192.168.1.1:5702")
        assert member1 == member2

    def test_equality_different_uuid(self):
        member1 = MemberInfo("uuid-123", "127.0.0.1:5701")
        member2 = MemberInfo("uuid-456", "127.0.0.1:5701")
        assert member1 != member2

    def test_equality_with_non_member(self):
        member = MemberInfo("uuid-123", "127.0.0.1:5701")
        assert member != "not a member"
        assert member != 123
        assert member != None

    def test_hash(self):
        member1 = MemberInfo("uuid-123", "127.0.0.1:5701")
        member2 = MemberInfo("uuid-123", "192.168.1.1:5702")
        assert hash(member1) == hash(member2)

        members_set = {member1, member2}
        assert len(members_set) == 1


class TestMembershipEvent:
    def test_properties(self):
        member = MemberInfo("uuid-123", "127.0.0.1:5701")
        members = [member]
        event = MembershipEvent(MembershipEventType.MEMBER_ADDED, member, members)

        assert event.event_type == MembershipEventType.MEMBER_ADDED
        assert event.member == member
        assert event.members == members

    def test_members_returns_copy(self):
        member = MemberInfo("uuid-123", "127.0.0.1:5701")
        members = [member]
        event = MembershipEvent(MembershipEventType.MEMBER_ADDED, member, members)

        returned_members = event.members
        returned_members.append(MemberInfo("uuid-456", "127.0.0.1:5702"))

        assert len(event.members) == 1

    def test_str(self):
        member = MemberInfo("uuid-123", "127.0.0.1:5701")
        event = MembershipEvent(MembershipEventType.MEMBER_ADDED, member, [member])
        assert "MEMBER_ADDED" in str(event)
        assert "uuid-123" in str(event)

    def test_repr(self):
        member = MemberInfo("uuid-123", "127.0.0.1:5701")
        event = MembershipEvent(MembershipEventType.MEMBER_REMOVED, member, [])
        assert repr(event) == str(event)


class TestFunctionMembershipListener:
    def test_on_added_callback(self):
        on_added = Mock()
        listener = FunctionMembershipListener(on_added=on_added)
        member = MemberInfo("uuid-123", "127.0.0.1:5701")
        event = MembershipEvent(MembershipEventType.MEMBER_ADDED, member, [member])

        listener.on_member_added(event)

        on_added.assert_called_once_with(event)

    def test_on_removed_callback(self):
        on_removed = Mock()
        listener = FunctionMembershipListener(on_removed=on_removed)
        member = MemberInfo("uuid-123", "127.0.0.1:5701")
        event = MembershipEvent(MembershipEventType.MEMBER_REMOVED, member, [])

        listener.on_member_removed(event)

        on_removed.assert_called_once_with(event)

    def test_no_callback_does_not_raise(self):
        listener = FunctionMembershipListener()
        member = MemberInfo("uuid-123", "127.0.0.1:5701")
        event = MembershipEvent(MembershipEventType.MEMBER_ADDED, member, [member])

        listener.on_member_added(event)
        listener.on_member_removed(event)


class TestDistributedObjectEvent:
    def test_properties(self):
        event = DistributedObjectEvent(
            DistributedObjectEventType.CREATED,
            "hz:impl:mapService",
            "my-map",
            source="client",
        )
        assert event.event_type == DistributedObjectEventType.CREATED
        assert event.service_name == "hz:impl:mapService"
        assert event.object_name == "my-map"
        assert event.source == "client"

    def test_default_source(self):
        event = DistributedObjectEvent(
            DistributedObjectEventType.DESTROYED,
            "hz:impl:mapService",
            "my-map",
        )
        assert event.source is None

    def test_str(self):
        event = DistributedObjectEvent(
            DistributedObjectEventType.CREATED,
            "hz:impl:mapService",
            "my-map",
        )
        result = str(event)
        assert "CREATED" in result
        assert "hz:impl:mapService" in result
        assert "my-map" in result

    def test_repr(self):
        event = DistributedObjectEvent(
            DistributedObjectEventType.DESTROYED,
            "service",
            "name",
        )
        assert repr(event) == str(event)


class TestFunctionDistributedObjectListener:
    def test_on_created_callback(self):
        on_created = Mock()
        listener = FunctionDistributedObjectListener(on_created=on_created)
        event = DistributedObjectEvent(
            DistributedObjectEventType.CREATED,
            "service",
            "name",
        )

        listener.on_distributed_object_created(event)

        on_created.assert_called_once_with(event)

    def test_on_destroyed_callback(self):
        on_destroyed = Mock()
        listener = FunctionDistributedObjectListener(on_destroyed=on_destroyed)
        event = DistributedObjectEvent(
            DistributedObjectEventType.DESTROYED,
            "service",
            "name",
        )

        listener.on_distributed_object_destroyed(event)

        on_destroyed.assert_called_once_with(event)

    def test_no_callback_does_not_raise(self):
        listener = FunctionDistributedObjectListener()
        event = DistributedObjectEvent(
            DistributedObjectEventType.CREATED,
            "service",
            "name",
        )

        listener.on_distributed_object_created(event)
        listener.on_distributed_object_destroyed(event)


class TestListenerRegistration:
    def test_properties(self):
        listener = Mock()
        reg = ListenerRegistration("reg-123", listener, "lifecycle")

        assert reg.registration_id == "reg-123"
        assert reg.listener == listener
        assert reg.listener_type == "lifecycle"


class TestListenerService:
    def test_add_lifecycle_listener(self):
        service = ListenerService()
        listener = FunctionLifecycleListener(lambda e: None)

        reg_id = service.add_lifecycle_listener(listener)

        assert reg_id is not None
        assert len(reg_id) > 0

    def test_add_membership_listener(self):
        service = ListenerService()
        listener = FunctionMembershipListener()

        reg_id = service.add_membership_listener(listener)

        assert reg_id is not None
        assert len(reg_id) > 0

    def test_add_distributed_object_listener(self):
        service = ListenerService()
        listener = FunctionDistributedObjectListener()

        reg_id = service.add_distributed_object_listener(listener)

        assert reg_id is not None
        assert len(reg_id) > 0

    def test_remove_lifecycle_listener(self):
        service = ListenerService()
        listener = FunctionLifecycleListener(lambda e: None)
        reg_id = service.add_lifecycle_listener(listener)

        result = service.remove_listener(reg_id)

        assert result is True

    def test_remove_membership_listener(self):
        service = ListenerService()
        listener = FunctionMembershipListener()
        reg_id = service.add_membership_listener(listener)

        result = service.remove_listener(reg_id)

        assert result is True

    def test_remove_distributed_object_listener(self):
        service = ListenerService()
        listener = FunctionDistributedObjectListener()
        reg_id = service.add_distributed_object_listener(listener)

        result = service.remove_listener(reg_id)

        assert result is True

    def test_remove_nonexistent_listener(self):
        service = ListenerService()

        result = service.remove_listener("nonexistent-id")

        assert result is False

    def test_fire_lifecycle_event(self):
        service = ListenerService()
        callback = Mock()
        listener = FunctionLifecycleListener(callback)
        service.add_lifecycle_listener(listener)
        event = LifecycleEvent(LifecycleState.STARTING)

        service.fire_lifecycle_event(event)

        callback.assert_called_once_with(event)

    def test_fire_lifecycle_event_to_multiple_listeners(self):
        service = ListenerService()
        callback1 = Mock()
        callback2 = Mock()
        service.add_lifecycle_listener(FunctionLifecycleListener(callback1))
        service.add_lifecycle_listener(FunctionLifecycleListener(callback2))
        event = LifecycleEvent(LifecycleState.CONNECTED)

        service.fire_lifecycle_event(event)

        callback1.assert_called_once_with(event)
        callback2.assert_called_once_with(event)

    def test_fire_lifecycle_event_exception_does_not_stop_others(self):
        service = ListenerService()
        callback1 = Mock(side_effect=Exception("error"))
        callback2 = Mock()
        service.add_lifecycle_listener(FunctionLifecycleListener(callback1))
        service.add_lifecycle_listener(FunctionLifecycleListener(callback2))
        event = LifecycleEvent(LifecycleState.SHUTDOWN)

        service.fire_lifecycle_event(event)

        callback1.assert_called_once()
        callback2.assert_called_once()

    def test_fire_membership_event_added(self):
        service = ListenerService()
        on_added = Mock()
        on_removed = Mock()
        listener = FunctionMembershipListener(on_added=on_added, on_removed=on_removed)
        service.add_membership_listener(listener)
        member = MemberInfo("uuid-123", "127.0.0.1:5701")
        event = MembershipEvent(MembershipEventType.MEMBER_ADDED, member, [member])

        service.fire_membership_event(event)

        on_added.assert_called_once_with(event)
        on_removed.assert_not_called()

    def test_fire_membership_event_removed(self):
        service = ListenerService()
        on_added = Mock()
        on_removed = Mock()
        listener = FunctionMembershipListener(on_added=on_added, on_removed=on_removed)
        service.add_membership_listener(listener)
        member = MemberInfo("uuid-123", "127.0.0.1:5701")
        event = MembershipEvent(MembershipEventType.MEMBER_REMOVED, member, [])

        service.fire_membership_event(event)

        on_added.assert_not_called()
        on_removed.assert_called_once_with(event)

    def test_fire_membership_event_exception_does_not_stop_others(self):
        service = ListenerService()
        on_added1 = Mock(side_effect=Exception("error"))
        on_added2 = Mock()
        service.add_membership_listener(FunctionMembershipListener(on_added=on_added1))
        service.add_membership_listener(FunctionMembershipListener(on_added=on_added2))
        member = MemberInfo("uuid-123", "127.0.0.1:5701")
        event = MembershipEvent(MembershipEventType.MEMBER_ADDED, member, [member])

        service.fire_membership_event(event)

        on_added1.assert_called_once()
        on_added2.assert_called_once()

    def test_fire_distributed_object_event_created(self):
        service = ListenerService()
        on_created = Mock()
        on_destroyed = Mock()
        listener = FunctionDistributedObjectListener(
            on_created=on_created, on_destroyed=on_destroyed
        )
        service.add_distributed_object_listener(listener)
        event = DistributedObjectEvent(
            DistributedObjectEventType.CREATED,
            "service",
            "name",
        )

        service.fire_distributed_object_event(event)

        on_created.assert_called_once_with(event)
        on_destroyed.assert_not_called()

    def test_fire_distributed_object_event_destroyed(self):
        service = ListenerService()
        on_created = Mock()
        on_destroyed = Mock()
        listener = FunctionDistributedObjectListener(
            on_created=on_created, on_destroyed=on_destroyed
        )
        service.add_distributed_object_listener(listener)
        event = DistributedObjectEvent(
            DistributedObjectEventType.DESTROYED,
            "service",
            "name",
        )

        service.fire_distributed_object_event(event)

        on_created.assert_not_called()
        on_destroyed.assert_called_once_with(event)

    def test_fire_distributed_object_event_exception_does_not_stop_others(self):
        service = ListenerService()
        on_created1 = Mock(side_effect=Exception("error"))
        on_created2 = Mock()
        service.add_distributed_object_listener(
            FunctionDistributedObjectListener(on_created=on_created1)
        )
        service.add_distributed_object_listener(
            FunctionDistributedObjectListener(on_created=on_created2)
        )
        event = DistributedObjectEvent(
            DistributedObjectEventType.CREATED,
            "service",
            "name",
        )

        service.fire_distributed_object_event(event)

        on_created1.assert_called_once()
        on_created2.assert_called_once()

    def test_clear_removes_all_listeners(self):
        service = ListenerService()
        callback = Mock()
        service.add_lifecycle_listener(FunctionLifecycleListener(callback))
        service.add_membership_listener(FunctionMembershipListener())
        service.add_distributed_object_listener(FunctionDistributedObjectListener())

        service.clear()

        service.fire_lifecycle_event(LifecycleEvent(LifecycleState.STARTING))
        callback.assert_not_called()

    def test_removed_listener_not_notified(self):
        service = ListenerService()
        callback = Mock()
        listener = FunctionLifecycleListener(callback)
        reg_id = service.add_lifecycle_listener(listener)

        service.remove_listener(reg_id)
        service.fire_lifecycle_event(LifecycleEvent(LifecycleState.STARTING))

        callback.assert_not_called()

    def test_thread_safety_add_and_fire(self):
        service = ListenerService()
        results = []
        lock = threading.Lock()

        def callback(event):
            with lock:
                results.append(event)

        def add_listeners():
            for _ in range(10):
                service.add_lifecycle_listener(FunctionLifecycleListener(callback))

        def fire_events():
            for _ in range(10):
                service.fire_lifecycle_event(LifecycleEvent(LifecycleState.STARTING))

        threads = []
        for _ in range(5):
            threads.append(threading.Thread(target=add_listeners))
            threads.append(threading.Thread(target=fire_events))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) > 0
