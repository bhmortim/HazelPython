"""Comprehensive unit tests for CP Subsystem session and group management."""

import threading
import time
import unittest
from unittest.mock import MagicMock

from hazelcast.cp.session import CPSession, CPSessionState, CPSessionManager
from hazelcast.cp.group import CPMember, CPGroup, CPGroupStatus, CPGroupManager
from hazelcast.exceptions import IllegalStateException


class TestCPSession(unittest.TestCase):
    """Tests for CPSession class."""

    def test_init(self):
        """Test CPSession initialization."""
        session = CPSession(12345, "default")
        self.assertEqual(session.session_id, 12345)
        self.assertEqual(session.group_id, "default")
        self.assertEqual(session.state, CPSessionState.ACTIVE)
        self.assertIsNotNone(session.creation_time)
        self.assertIsNotNone(session.last_heartbeat)

    def test_is_active(self):
        """Test is_active() returns True for active session."""
        session = CPSession(1, "group1")
        self.assertTrue(session.is_active())

    def test_close_session(self):
        """Test _close() changes state to CLOSED."""
        session = CPSession(1, "group1")
        session._close()
        self.assertEqual(session.state, CPSessionState.CLOSED)
        self.assertFalse(session.is_active())

    def test_expire_session(self):
        """Test _expire() changes state to EXPIRED."""
        session = CPSession(1, "group1")
        session._expire()
        self.assertEqual(session.state, CPSessionState.EXPIRED)
        self.assertFalse(session.is_active())

    def test_update_heartbeat(self):
        """Test _update_heartbeat() updates last_heartbeat."""
        session = CPSession(1, "group1")
        old_heartbeat = session.last_heartbeat
        time.sleep(0.01)
        session._update_heartbeat()
        self.assertGreater(session.last_heartbeat, old_heartbeat)

    def test_repr(self):
        """Test __repr__ output."""
        session = CPSession(42, "my-group")
        repr_str = repr(session)
        self.assertIn("42", repr_str)
        self.assertIn("my-group", repr_str)
        self.assertIn("ACTIVE", repr_str)


class TestCPSessionManager(unittest.TestCase):
    """Tests for CPSessionManager class."""

    def setUp(self):
        self.manager = CPSessionManager()

    def tearDown(self):
        self.manager.shutdown()

    def test_init(self):
        """Test CPSessionManager initialization."""
        manager = CPSessionManager()
        self.assertFalse(manager.is_closed)
        self.assertEqual(len(manager.get_all_sessions()), 0)
        manager.shutdown()

    def test_get_session_returns_none_when_empty(self):
        """Test get_session() returns None when no session exists."""
        session = self.manager.get_session("nonexistent")
        self.assertIsNone(session)

    def test_get_or_create_session(self):
        """Test get_or_create_session() creates new session."""
        session = self.manager.get_or_create_session("group1")
        self.assertIsNotNone(session)
        self.assertEqual(session.group_id, "group1")
        self.assertTrue(session.is_active())

    def test_get_or_create_session_returns_existing(self):
        """Test get_or_create_session() returns existing active session."""
        session1 = self.manager.get_or_create_session("group1")
        session2 = self.manager.get_or_create_session("group1")
        self.assertEqual(session1.session_id, session2.session_id)

    def test_get_or_create_session_creates_new_after_close(self):
        """Test get_or_create_session() creates new session after close."""
        session1 = self.manager.get_or_create_session("group1")
        self.manager.close_session("group1")
        session2 = self.manager.get_or_create_session("group1")
        self.assertNotEqual(session1.session_id, session2.session_id)

    def test_get_session_returns_active_session(self):
        """Test get_session() returns active session."""
        self.manager.get_or_create_session("group1")
        session = self.manager.get_session("group1")
        self.assertIsNotNone(session)
        self.assertTrue(session.is_active())

    def test_get_session_returns_none_for_closed(self):
        """Test get_session() returns None for closed session."""
        self.manager.get_or_create_session("group1")
        self.manager.close_session("group1")
        session = self.manager.get_session("group1")
        self.assertIsNone(session)

    def test_close_session_success(self):
        """Test close_session() returns True when session closed."""
        self.manager.get_or_create_session("group1")
        result = self.manager.close_session("group1")
        self.assertTrue(result)

    def test_close_session_returns_false_when_nonexistent(self):
        """Test close_session() returns False for nonexistent session."""
        result = self.manager.close_session("nonexistent")
        self.assertFalse(result)

    def test_close_session_returns_false_when_already_closed(self):
        """Test close_session() returns False for already closed session."""
        self.manager.get_or_create_session("group1")
        self.manager.close_session("group1")
        result = self.manager.close_session("group1")
        self.assertFalse(result)

    def test_heartbeat_success(self):
        """Test heartbeat() returns True for active session."""
        self.manager.get_or_create_session("group1")
        result = self.manager.heartbeat("group1")
        self.assertTrue(result)

    def test_heartbeat_returns_false_when_nonexistent(self):
        """Test heartbeat() returns False for nonexistent session."""
        result = self.manager.heartbeat("nonexistent")
        self.assertFalse(result)

    def test_heartbeat_returns_false_when_closed(self):
        """Test heartbeat() returns False for closed session."""
        self.manager.get_or_create_session("group1")
        self.manager.close_session("group1")
        result = self.manager.heartbeat("group1")
        self.assertFalse(result)

    def test_get_all_sessions(self):
        """Test get_all_sessions() returns all sessions."""
        self.manager.get_or_create_session("group1")
        self.manager.get_or_create_session("group2")
        sessions = self.manager.get_all_sessions()
        self.assertEqual(len(sessions), 2)

    def test_get_active_sessions(self):
        """Test get_active_sessions() returns only active sessions."""
        self.manager.get_or_create_session("group1")
        self.manager.get_or_create_session("group2")
        self.manager.close_session("group1")
        active = self.manager.get_active_sessions()
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0].group_id, "group2")

    def test_close_all(self):
        """Test close_all() closes all sessions."""
        self.manager.get_or_create_session("group1")
        self.manager.get_or_create_session("group2")
        count = self.manager.close_all()
        self.assertEqual(count, 2)
        self.assertEqual(len(self.manager.get_active_sessions()), 0)

    def test_close_all_returns_zero_when_empty(self):
        """Test close_all() returns 0 when no sessions."""
        count = self.manager.close_all()
        self.assertEqual(count, 0)

    def test_shutdown(self):
        """Test shutdown() closes manager."""
        self.manager.get_or_create_session("group1")
        self.manager.shutdown()
        self.assertTrue(self.manager.is_closed)

    def test_get_or_create_raises_when_closed(self):
        """Test get_or_create_session() raises when manager closed."""
        self.manager.shutdown()
        with self.assertRaises(IllegalStateException):
            self.manager.get_or_create_session("group1")

    def test_repr(self):
        """Test __repr__ output."""
        self.manager.get_or_create_session("group1")
        self.manager.get_or_create_session("group2")
        self.manager.close_session("group1")
        repr_str = repr(self.manager)
        self.assertIn("sessions=2", repr_str)
        self.assertIn("active=1", repr_str)

    def test_concurrent_session_creation(self):
        """Test concurrent session creation is thread-safe."""
        sessions = []
        errors = []

        def create_session(group_id):
            try:
                session = self.manager.get_or_create_session(group_id)
                sessions.append(session)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=create_session, args=(f"group{i}",))
            for i in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0)
        self.assertEqual(len(sessions), 10)


class TestCPMember(unittest.TestCase):
    """Tests for CPMember class."""

    def test_init(self):
        """Test CPMember initialization."""
        member = CPMember("uuid-123", "192.168.1.1:5701")
        self.assertEqual(member.uuid, "uuid-123")
        self.assertEqual(member.address, "192.168.1.1:5701")

    def test_init_default_address(self):
        """Test CPMember initialization with default address."""
        member = CPMember("uuid-456")
        self.assertEqual(member.uuid, "uuid-456")
        self.assertEqual(member.address, "")

    def test_equality(self):
        """Test CPMember equality based on UUID."""
        member1 = CPMember("uuid-123", "addr1")
        member2 = CPMember("uuid-123", "addr2")
        member3 = CPMember("uuid-456", "addr1")
        self.assertEqual(member1, member2)
        self.assertNotEqual(member1, member3)

    def test_equality_with_non_member(self):
        """Test CPMember equality with non-CPMember."""
        member = CPMember("uuid-123")
        self.assertNotEqual(member, "uuid-123")
        self.assertNotEqual(member, None)

    def test_hash(self):
        """Test CPMember hash based on UUID."""
        member1 = CPMember("uuid-123", "addr1")
        member2 = CPMember("uuid-123", "addr2")
        self.assertEqual(hash(member1), hash(member2))

    def test_repr(self):
        """Test CPMember __repr__ output."""
        member = CPMember("uuid-123", "192.168.1.1:5701")
        repr_str = repr(member)
        self.assertIn("uuid-123", repr_str)
        self.assertIn("192.168.1.1:5701", repr_str)


class TestCPGroup(unittest.TestCase):
    """Tests for CPGroup class."""

    def test_init(self):
        """Test CPGroup initialization."""
        group = CPGroup("my-group", "group-id-123")
        self.assertEqual(group.name, "my-group")
        self.assertEqual(group.group_id, "group-id-123")
        self.assertEqual(group.status, CPGroupStatus.ACTIVE)
        self.assertEqual(group.member_count, 0)

    def test_init_with_members(self):
        """Test CPGroup initialization with members."""
        members = {CPMember("uuid1"), CPMember("uuid2")}
        group = CPGroup("my-group", "id-123", members)
        self.assertEqual(group.member_count, 2)

    def test_members_returns_copy(self):
        """Test members property returns a copy."""
        members = {CPMember("uuid1")}
        group = CPGroup("my-group", "id-123", members)
        returned_members = group.members
        returned_members.add(CPMember("uuid2"))
        self.assertEqual(group.member_count, 1)

    def test_is_active(self):
        """Test is_active() returns True for active group."""
        group = CPGroup("my-group", "id-123")
        self.assertTrue(group.is_active())

    def test_equality(self):
        """Test CPGroup equality based on group_id."""
        group1 = CPGroup("name1", "id-123")
        group2 = CPGroup("name2", "id-123")
        group3 = CPGroup("name1", "id-456")
        self.assertEqual(group1, group2)
        self.assertNotEqual(group1, group3)

    def test_equality_with_non_group(self):
        """Test CPGroup equality with non-CPGroup."""
        group = CPGroup("my-group", "id-123")
        self.assertNotEqual(group, "id-123")
        self.assertNotEqual(group, None)

    def test_hash(self):
        """Test CPGroup hash based on group_id."""
        group1 = CPGroup("name1", "id-123")
        group2 = CPGroup("name2", "id-123")
        self.assertEqual(hash(group1), hash(group2))

    def test_default_group_name(self):
        """Test DEFAULT_GROUP_NAME constant."""
        self.assertEqual(CPGroup.DEFAULT_GROUP_NAME, "default")

    def test_metadata_group_name(self):
        """Test METADATA_GROUP_NAME constant."""
        self.assertEqual(CPGroup.METADATA_GROUP_NAME, "METADATA")

    def test_repr(self):
        """Test CPGroup __repr__ output."""
        group = CPGroup("my-group", "id-123")
        repr_str = repr(group)
        self.assertIn("my-group", repr_str)
        self.assertIn("id-123", repr_str)
        self.assertIn("ACTIVE", repr_str)


class TestCPGroupManager(unittest.TestCase):
    """Tests for CPGroupManager class."""

    def setUp(self):
        self.manager = CPGroupManager()

    def tearDown(self):
        self.manager.shutdown()

    def test_init(self):
        """Test CPGroupManager initialization."""
        manager = CPGroupManager()
        self.assertFalse(manager.is_closed)
        self.assertEqual(len(manager.get_groups()), 0)
        manager.shutdown()

    def test_get_group_returns_none_when_nonexistent(self):
        """Test get_group() returns None for nonexistent group."""
        group = self.manager.get_group("nonexistent")
        self.assertIsNone(group)

    def test_register_group(self):
        """Test register_group() adds group."""
        group = CPGroup("my-group", "id-123")
        self.manager.register_group(group)
        retrieved = self.manager.get_group("my-group")
        self.assertEqual(retrieved, group)

    def test_get_groups(self):
        """Test get_groups() returns all groups."""
        self.manager.register_group(CPGroup("group1", "id1"))
        self.manager.register_group(CPGroup("group2", "id2"))
        groups = self.manager.get_groups()
        self.assertEqual(len(groups), 2)

    def test_get_active_groups(self):
        """Test get_active_groups() returns active groups."""
        group1 = CPGroup("group1", "id1")
        group2 = CPGroup("group2", "id2")
        self.manager.register_group(group1)
        self.manager.register_group(group2)
        active = self.manager.get_active_groups()
        self.assertEqual(len(active), 2)

    def test_get_default_group(self):
        """Test get_default_group() returns default group."""
        group = CPGroup(CPGroup.DEFAULT_GROUP_NAME, "default-id")
        self.manager.register_group(group)
        default = self.manager.get_default_group()
        self.assertEqual(default, group)

    def test_get_default_group_returns_none(self):
        """Test get_default_group() returns None when not exists."""
        default = self.manager.get_default_group()
        self.assertIsNone(default)

    def test_get_metadata_group(self):
        """Test get_metadata_group() returns metadata group."""
        group = CPGroup(CPGroup.METADATA_GROUP_NAME, "meta-id")
        self.manager.register_group(group)
        meta = self.manager.get_metadata_group()
        self.assertEqual(meta, group)

    def test_get_metadata_group_returns_none(self):
        """Test get_metadata_group() returns None when not exists."""
        meta = self.manager.get_metadata_group()
        self.assertIsNone(meta)

    def test_contains_group(self):
        """Test contains_group() returns True when group exists."""
        self.manager.register_group(CPGroup("my-group", "id"))
        self.assertTrue(self.manager.contains_group("my-group"))
        self.assertFalse(self.manager.contains_group("other"))

    def test_remove_group(self):
        """Test remove_group() removes and returns group."""
        group = CPGroup("my-group", "id-123")
        self.manager.register_group(group)
        removed = self.manager.remove_group("my-group")
        self.assertEqual(removed, group)
        self.assertIsNone(self.manager.get_group("my-group"))

    def test_remove_group_returns_none_when_nonexistent(self):
        """Test remove_group() returns None for nonexistent group."""
        removed = self.manager.remove_group("nonexistent")
        self.assertIsNone(removed)

    def test_clear(self):
        """Test clear() removes all groups."""
        self.manager.register_group(CPGroup("group1", "id1"))
        self.manager.register_group(CPGroup("group2", "id2"))
        self.manager.clear()
        self.assertEqual(len(self.manager.get_groups()), 0)

    def test_shutdown(self):
        """Test shutdown() closes manager and clears groups."""
        self.manager.register_group(CPGroup("my-group", "id"))
        self.manager.shutdown()
        self.assertTrue(self.manager.is_closed)
        self.assertEqual(len(self.manager.get_groups()), 0)

    def test_register_group_raises_when_closed(self):
        """Test register_group() raises when manager closed."""
        self.manager.shutdown()
        with self.assertRaises(IllegalStateException):
            self.manager.register_group(CPGroup("my-group", "id"))

    def test_repr(self):
        """Test __repr__ output."""
        self.manager.register_group(CPGroup("group1", "id1"))
        self.manager.register_group(CPGroup("group2", "id2"))
        repr_str = repr(self.manager)
        self.assertIn("groups=2", repr_str)
        self.assertIn("active=2", repr_str)

    def test_concurrent_group_registration(self):
        """Test concurrent group registration is thread-safe."""
        errors = []

        def register_group(name):
            try:
                self.manager.register_group(CPGroup(name, f"id-{name}"))
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=register_group, args=(f"group{i}",))
            for i in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0)
        self.assertEqual(len(self.manager.get_groups()), 10)


if __name__ == "__main__":
    unittest.main()
