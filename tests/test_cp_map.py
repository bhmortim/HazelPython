"""Unit tests for CP Subsystem components."""

import threading
import time
import unittest
from concurrent.futures import Future
from unittest.mock import Mock, MagicMock, patch

from hazelcast.cp.cp_map import CPMap
from hazelcast.cp.session import CPSession, CPSessionState, CPSessionManager
from hazelcast.cp.group import CPGroup, CPGroupStatus, CPMember, CPGroupManager
from hazelcast.proxy.base import ProxyContext
from hazelcast.exceptions import IllegalStateException


class TestCPMap(unittest.TestCase):
    """Tests for CPMap proxy."""

    def setUp(self):
        self.mock_context = Mock(spec=ProxyContext)
        self.mock_invocation = Mock()
        self.mock_serialization = Mock()

        self.mock_context.invocation_service = self.mock_invocation
        self.mock_context.serialization_service = self.mock_serialization

        self.mock_serialization.to_data = Mock(
            side_effect=lambda x: x.encode() if isinstance(x, str) else x
        )
        self.mock_serialization.to_object = Mock(
            side_effect=lambda x: x.decode() if isinstance(x, bytes) else x
        )

    def test_init_default_group(self):
        """Test CPMap initialization with default group."""
        cp_map = CPMap("hz:raft:mapService", "test-map", self.mock_context)
        self.assertEqual(cp_map.name, "test-map")
        self.assertEqual(cp_map._group_id, "default")
        self.assertEqual(cp_map._get_object_name(), "test-map")

    def test_init_custom_group(self):
        """Test CPMap initialization with custom group."""
        cp_map = CPMap("hz:raft:mapService", "test-map@custom-group", self.mock_context)
        self.assertEqual(cp_map.name, "test-map@custom-group")
        self.assertEqual(cp_map._group_id, "custom-group")
        self.assertEqual(cp_map._get_object_name(), "test-map")

    def test_get_async_returns_future(self):
        """Test that get_async returns a Future."""
        cp_map = CPMap("hz:raft:mapService", "test-map", self.mock_context)

        future = Future()
        future.set_result(None)
        self.mock_invocation.invoke = Mock(return_value=future)

        result = cp_map.get_async("key")
        self.assertIsInstance(result, Future)

    def test_put_async_returns_future(self):
        """Test that put_async returns a Future."""
        cp_map = CPMap("hz:raft:mapService", "test-map", self.mock_context)

        future = Future()
        future.set_result(None)
        self.mock_invocation.invoke = Mock(return_value=future)

        result = cp_map.put_async("key", "value")
        self.assertIsInstance(result, Future)

    def test_set_async_returns_future(self):
        """Test that set_async returns a Future."""
        cp_map = CPMap("hz:raft:mapService", "test-map", self.mock_context)

        future = Future()
        future.set_result(None)
        self.mock_invocation.invoke = Mock(return_value=future)

        result = cp_map.set_async("key", "value")
        self.assertIsInstance(result, Future)

    def test_remove_async_returns_future(self):
        """Test that remove_async returns a Future."""
        cp_map = CPMap("hz:raft:mapService", "test-map", self.mock_context)

        future = Future()
        future.set_result(None)
        self.mock_invocation.invoke = Mock(return_value=future)

        result = cp_map.remove_async("key")
        self.assertIsInstance(result, Future)

    def test_delete_async_returns_future(self):
        """Test that delete_async returns a Future."""
        cp_map = CPMap("hz:raft:mapService", "test-map", self.mock_context)

        future = Future()
        future.set_result(None)
        self.mock_invocation.invoke = Mock(return_value=future)

        result = cp_map.delete_async("key")
        self.assertIsInstance(result, Future)

    def test_compare_and_set_async_returns_future(self):
        """Test that compare_and_set_async returns a Future."""
        cp_map = CPMap("hz:raft:mapService", "test-map", self.mock_context)

        future = Future()
        future.set_result(None)
        self.mock_invocation.invoke = Mock(return_value=future)

        result = cp_map.compare_and_set_async("key", "old", "new")
        self.assertIsInstance(result, Future)

    def test_destroyed_map_raises(self):
        """Test that operations on destroyed map raise exception."""
        cp_map = CPMap("hz:raft:mapService", "test-map", self.mock_context)
        cp_map._destroyed = True

        with self.assertRaises(IllegalStateException):
            cp_map.get("key")

    def test_repr(self):
        """Test CPMap string representation."""
        cp_map = CPMap("hz:raft:mapService", "test-map", self.mock_context)
        repr_str = repr(cp_map)
        self.assertIn("CPMap", repr_str)
        self.assertIn("test-map", repr_str)


class TestCPSession(unittest.TestCase):
    """Tests for CPSession."""

    def test_init(self):
        """Test CPSession initialization."""
        session = CPSession(12345, "default")
        self.assertEqual(session.session_id, 12345)
        self.assertEqual(session.group_id, "default")
        self.assertEqual(session.state, CPSessionState.ACTIVE)
        self.assertTrue(session.is_active())

    def test_close(self):
        """Test session closure."""
        session = CPSession(12345, "default")
        session._close()
        self.assertEqual(session.state, CPSessionState.CLOSED)
        self.assertFalse(session.is_active())

    def test_expire(self):
        """Test session expiration."""
        session = CPSession(12345, "default")
        session._expire()
        self.assertEqual(session.state, CPSessionState.EXPIRED)
        self.assertFalse(session.is_active())

    def test_heartbeat_update(self):
        """Test heartbeat update."""
        session = CPSession(12345, "default")
        initial_heartbeat = session.last_heartbeat
        time.sleep(0.01)
        session._update_heartbeat()
        self.assertGreater(session.last_heartbeat, initial_heartbeat)

    def test_repr(self):
        """Test string representation."""
        session = CPSession(12345, "default")
        repr_str = repr(session)
        self.assertIn("CPSession", repr_str)
        self.assertIn("12345", repr_str)
        self.assertIn("default", repr_str)
        self.assertIn("ACTIVE", repr_str)


class TestCPSessionManager(unittest.TestCase):
    """Tests for CPSessionManager."""

    def test_get_or_create_session(self):
        """Test session creation."""
        manager = CPSessionManager()
        session = manager.get_or_create_session("default")
        self.assertIsNotNone(session)
        self.assertEqual(session.group_id, "default")
        self.assertTrue(session.is_active())

    def test_get_existing_session(self):
        """Test getting existing session."""
        manager = CPSessionManager()
        session1 = manager.get_or_create_session("default")
        session2 = manager.get_session("default")
        self.assertEqual(session1, session2)

    def test_get_nonexistent_session(self):
        """Test getting non-existent session."""
        manager = CPSessionManager()
        session = manager.get_session("nonexistent")
        self.assertIsNone(session)

    def test_close_session(self):
        """Test session closure."""
        manager = CPSessionManager()
        manager.get_or_create_session("default")
        result = manager.close_session("default")
        self.assertTrue(result)

        session = manager.get_session("default")
        self.assertIsNone(session)

    def test_close_nonexistent_session(self):
        """Test closing non-existent session."""
        manager = CPSessionManager()
        result = manager.close_session("nonexistent")
        self.assertFalse(result)

    def test_heartbeat(self):
        """Test heartbeat functionality."""
        manager = CPSessionManager()
        manager.get_or_create_session("default")
        result = manager.heartbeat("default")
        self.assertTrue(result)

    def test_heartbeat_nonexistent(self):
        """Test heartbeat for non-existent session."""
        manager = CPSessionManager()
        result = manager.heartbeat("nonexistent")
        self.assertFalse(result)

    def test_get_all_sessions(self):
        """Test getting all sessions."""
        manager = CPSessionManager()
        manager.get_or_create_session("group1")
        manager.get_or_create_session("group2")
        sessions = manager.get_all_sessions()
        self.assertEqual(len(sessions), 2)

    def test_get_active_sessions(self):
        """Test getting active sessions."""
        manager = CPSessionManager()
        manager.get_or_create_session("group1")
        manager.get_or_create_session("group2")
        manager.close_session("group1")
        active = manager.get_active_sessions()
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0].group_id, "group2")

    def test_close_all(self):
        """Test closing all sessions."""
        manager = CPSessionManager()
        manager.get_or_create_session("group1")
        manager.get_or_create_session("group2")
        count = manager.close_all()
        self.assertEqual(count, 2)
        self.assertEqual(len(manager.get_active_sessions()), 0)

    def test_shutdown(self):
        """Test manager shutdown."""
        manager = CPSessionManager()
        manager.get_or_create_session("default")
        manager.shutdown()
        self.assertTrue(manager.is_closed)

        with self.assertRaises(IllegalStateException):
            manager.get_or_create_session("new")

    def test_repr(self):
        """Test string representation."""
        manager = CPSessionManager()
        manager.get_or_create_session("default")
        repr_str = repr(manager)
        self.assertIn("CPSessionManager", repr_str)


class TestCPMember(unittest.TestCase):
    """Tests for CPMember."""

    def test_init(self):
        """Test CPMember initialization."""
        member = CPMember("uuid-123", "127.0.0.1:5701")
        self.assertEqual(member.uuid, "uuid-123")
        self.assertEqual(member.address, "127.0.0.1:5701")

    def test_equality(self):
        """Test member equality."""
        member1 = CPMember("uuid-123", "127.0.0.1:5701")
        member2 = CPMember("uuid-123", "127.0.0.1:5702")
        member3 = CPMember("uuid-456", "127.0.0.1:5701")

        self.assertEqual(member1, member2)
        self.assertNotEqual(member1, member3)

    def test_hash(self):
        """Test member hashing."""
        member1 = CPMember("uuid-123", "127.0.0.1:5701")
        member2 = CPMember("uuid-123", "127.0.0.1:5702")

        self.assertEqual(hash(member1), hash(member2))

    def test_repr(self):
        """Test string representation."""
        member = CPMember("uuid-123", "127.0.0.1:5701")
        repr_str = repr(member)
        self.assertIn("CPMember", repr_str)
        self.assertIn("uuid-123", repr_str)


class TestCPGroup(unittest.TestCase):
    """Tests for CPGroup."""

    def test_init(self):
        """Test CPGroup initialization."""
        members = {CPMember("uuid-1", ""), CPMember("uuid-2", "")}
        group = CPGroup("default", "group-id-123", members)

        self.assertEqual(group.name, "default")
        self.assertEqual(group.group_id, "group-id-123")
        self.assertEqual(group.member_count, 2)
        self.assertTrue(group.is_active())

    def test_default_group_name(self):
        """Test default group name constant."""
        self.assertEqual(CPGroup.DEFAULT_GROUP_NAME, "default")

    def test_metadata_group_name(self):
        """Test metadata group name constant."""
        self.assertEqual(CPGroup.METADATA_GROUP_NAME, "METADATA")

    def test_members_copy(self):
        """Test that members returns a copy."""
        members = {CPMember("uuid-1", "")}
        group = CPGroup("test", "id", members)

        returned_members = group.members
        returned_members.add(CPMember("uuid-2", ""))

        self.assertEqual(group.member_count, 1)

    def test_equality(self):
        """Test group equality."""
        group1 = CPGroup("default", "id-1")
        group2 = CPGroup("default", "id-1")
        group3 = CPGroup("default", "id-2")

        self.assertEqual(group1, group2)
        self.assertNotEqual(group1, group3)

    def test_repr(self):
        """Test string representation."""
        group = CPGroup("default", "id-123")
        repr_str = repr(group)
        self.assertIn("CPGroup", repr_str)
        self.assertIn("default", repr_str)


class TestCPGroupManager(unittest.TestCase):
    """Tests for CPGroupManager."""

    def test_register_and_get_group(self):
        """Test group registration and retrieval."""
        manager = CPGroupManager()
        group = CPGroup("test", "id-123")
        manager.register_group(group)

        retrieved = manager.get_group("test")
        self.assertEqual(retrieved, group)

    def test_get_nonexistent_group(self):
        """Test getting non-existent group."""
        manager = CPGroupManager()
        group = manager.get_group("nonexistent")
        self.assertIsNone(group)

    def test_get_groups(self):
        """Test getting all groups."""
        manager = CPGroupManager()
        manager.register_group(CPGroup("group1", "id-1"))
        manager.register_group(CPGroup("group2", "id-2"))

        groups = manager.get_groups()
        self.assertEqual(len(groups), 2)

    def test_get_default_group(self):
        """Test getting default group."""
        manager = CPGroupManager()
        default = CPGroup(CPGroup.DEFAULT_GROUP_NAME, "id")
        manager.register_group(default)

        retrieved = manager.get_default_group()
        self.assertEqual(retrieved, default)

    def test_contains_group(self):
        """Test group existence check."""
        manager = CPGroupManager()
        manager.register_group(CPGroup("test", "id"))

        self.assertTrue(manager.contains_group("test"))
        self.assertFalse(manager.contains_group("nonexistent"))

    def test_remove_group(self):
        """Test group removal."""
        manager = CPGroupManager()
        group = CPGroup("test", "id")
        manager.register_group(group)

        removed = manager.remove_group("test")
        self.assertEqual(removed, group)
        self.assertFalse(manager.contains_group("test"))

    def test_clear(self):
        """Test clearing all groups."""
        manager = CPGroupManager()
        manager.register_group(CPGroup("group1", "id-1"))
        manager.register_group(CPGroup("group2", "id-2"))
        manager.clear()

        self.assertEqual(len(manager.get_groups()), 0)

    def test_shutdown(self):
        """Test manager shutdown."""
        manager = CPGroupManager()
        manager.register_group(CPGroup("test", "id"))
        manager.shutdown()

        self.assertTrue(manager.is_closed)
        self.assertEqual(len(manager.get_groups()), 0)

        with self.assertRaises(IllegalStateException):
            manager.register_group(CPGroup("new", "id"))

    def test_repr(self):
        """Test string representation."""
        manager = CPGroupManager()
        manager.register_group(CPGroup("test", "id"))
        repr_str = repr(manager)
        self.assertIn("CPGroupManager", repr_str)


class TestCPSessionState(unittest.TestCase):
    """Tests for CPSessionState enum."""

    def test_values(self):
        """Test enum values."""
        self.assertEqual(CPSessionState.ACTIVE.value, "ACTIVE")
        self.assertEqual(CPSessionState.CLOSED.value, "CLOSED")
        self.assertEqual(CPSessionState.EXPIRED.value, "EXPIRED")


class TestCPGroupStatus(unittest.TestCase):
    """Tests for CPGroupStatus enum."""

    def test_values(self):
        """Test enum values."""
        self.assertEqual(CPGroupStatus.ACTIVE.value, "ACTIVE")
        self.assertEqual(CPGroupStatus.DESTROYING.value, "DESTROYING")
        self.assertEqual(CPGroupStatus.DESTROYED.value, "DESTROYED")


if __name__ == "__main__":
    unittest.main()
