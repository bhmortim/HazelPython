"""Tests for CP Subsystem functionality."""

import struct
import time
import threading
import unittest

from hazelcast.cp.session import CPSession, CPSessionManager, CPSessionState
from hazelcast.cp.group import CPGroup, CPGroupManager, CPGroupStatus, CPMember
from hazelcast.protocol.codec import (
    CPSessionCodec,
    CPGroupCodec,
    CP_SESSION_CREATE,
    CP_SESSION_CLOSE,
    CP_SESSION_HEARTBEAT,
    CP_SESSION_GENERATE_THREAD_ID,
    CP_SESSION_GET_SESSIONS,
    CP_GROUP_CREATE_CP_GROUP,
    CP_GROUP_DESTROY_CP_OBJECT,
    CP_GROUP_GET_CP_GROUP_IDS,
    CP_GROUP_GET_CP_OBJECT_INFOS,
    RESPONSE_HEADER_SIZE,
    LONG_SIZE,
    BOOLEAN_SIZE,
)


class TestCPSession(unittest.TestCase):
    """Tests for CPSession class."""

    def test_session_creation(self):
        """Test basic session creation."""
        session = CPSession(12345, "default")
        self.assertEqual(session.session_id, 12345)
        self.assertEqual(session.group_id, "default")
        self.assertEqual(session.state, CPSessionState.ACTIVE)
        self.assertTrue(session.is_active())

    def test_session_with_ttl(self):
        """Test session creation with TTL and heartbeat settings."""
        session = CPSession(12345, "default", ttl_millis=300000, heartbeat_millis=5000)
        self.assertEqual(session.ttl_millis, 300000)
        self.assertEqual(session.heartbeat_millis, 5000)

    def test_session_close(self):
        """Test session close."""
        session = CPSession(12345, "default")
        self.assertTrue(session.is_active())
        session._close()
        self.assertEqual(session.state, CPSessionState.CLOSED)
        self.assertFalse(session.is_active())

    def test_session_expire(self):
        """Test session expiration."""
        session = CPSession(12345, "default")
        session._expire()
        self.assertEqual(session.state, CPSessionState.EXPIRED)
        self.assertFalse(session.is_active())

    def test_session_heartbeat_update(self):
        """Test heartbeat update."""
        session = CPSession(12345, "default")
        initial_heartbeat = session.last_heartbeat
        time.sleep(0.01)
        session._update_heartbeat()
        self.assertGreater(session.last_heartbeat, initial_heartbeat)

    def test_session_acquire_count(self):
        """Test acquire count management."""
        session = CPSession(12345, "default")
        self.assertEqual(session.acquire_count, 0)

        self.assertEqual(session._increment_acquire(), 1)
        self.assertEqual(session.acquire_count, 1)

        self.assertEqual(session._increment_acquire(), 2)
        self.assertEqual(session.acquire_count, 2)

        self.assertEqual(session._decrement_acquire(), 1)
        self.assertEqual(session.acquire_count, 1)

        self.assertEqual(session._decrement_acquire(), 0)
        self.assertEqual(session.acquire_count, 0)

        self.assertEqual(session._decrement_acquire(), 0)
        self.assertEqual(session.acquire_count, 0)

    def test_session_repr(self):
        """Test session string representation."""
        session = CPSession(12345, "default")
        repr_str = repr(session)
        self.assertIn("12345", repr_str)
        self.assertIn("default", repr_str)
        self.assertIn("ACTIVE", repr_str)


class TestCPSessionManager(unittest.TestCase):
    """Tests for CPSessionManager class."""

    def test_manager_creation(self):
        """Test manager creation."""
        manager = CPSessionManager()
        self.assertFalse(manager.is_closed)
        self.assertEqual(len(manager.get_all_sessions()), 0)

    def test_get_or_create_session(self):
        """Test session creation via manager."""
        manager = CPSessionManager()
        session = manager.get_or_create_session("default")
        self.assertIsNotNone(session)
        self.assertEqual(session.group_id, "default")
        self.assertTrue(session.is_active())

        session2 = manager.get_or_create_session("default")
        self.assertEqual(session.session_id, session2.session_id)

    def test_get_session(self):
        """Test getting existing session."""
        manager = CPSessionManager()
        self.assertIsNone(manager.get_session("default"))

        manager.get_or_create_session("default")
        session = manager.get_session("default")
        self.assertIsNotNone(session)

    def test_close_session(self):
        """Test session closure."""
        manager = CPSessionManager()
        manager.get_or_create_session("default")

        self.assertTrue(manager.close_session("default"))
        self.assertFalse(manager.close_session("default"))

        session = manager.get_session("default")
        self.assertIsNone(session)

    def test_heartbeat(self):
        """Test session heartbeat."""
        manager = CPSessionManager()
        self.assertFalse(manager.heartbeat("default"))

        session = manager.get_or_create_session("default")
        initial_heartbeat = session.last_heartbeat
        time.sleep(0.01)

        self.assertTrue(manager.heartbeat("default"))
        self.assertGreater(session.last_heartbeat, initial_heartbeat)

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
        self.assertEqual(len(manager.get_active_sessions()), 0)

    def test_operations_after_shutdown(self):
        """Test that operations fail after shutdown."""
        manager = CPSessionManager()
        manager.shutdown()

        from hazelcast.exceptions import IllegalStateException
        with self.assertRaises(IllegalStateException):
            manager.get_or_create_session("default")

    def test_acquire_release_session(self):
        """Test session acquire and release."""
        manager = CPSessionManager()

        session_id, count = manager.acquire_session("default")
        self.assertGreater(session_id, 0)
        self.assertEqual(count, 1)

        session_id2, count2 = manager.acquire_session("default")
        self.assertEqual(session_id, session_id2)
        self.assertEqual(count2, 2)

        manager.release_session("default", session_id)
        session = manager.get_session("default")
        self.assertEqual(session.acquire_count, 1)

    def test_invalidate_session(self):
        """Test session invalidation."""
        manager = CPSessionManager()
        session = manager.get_or_create_session("default")
        session_id = session.session_id

        manager.invalidate_session("default", session_id)
        self.assertIsNone(manager.get_session("default"))

    def test_generate_thread_id(self):
        """Test thread ID generation."""
        manager = CPSessionManager()
        thread_id = manager.generate_thread_id("default")
        self.assertGreater(thread_id, 0)

        thread_id2 = manager.generate_thread_id("default")
        self.assertEqual(thread_id, thread_id2)

    def test_manager_repr(self):
        """Test manager string representation."""
        manager = CPSessionManager()
        manager.get_or_create_session("default")
        repr_str = repr(manager)
        self.assertIn("CPSessionManager", repr_str)
        self.assertIn("sessions=1", repr_str)

    def test_thread_safety(self):
        """Test thread-safe session management."""
        manager = CPSessionManager()
        sessions_created = []
        errors = []

        def create_session(group_id):
            try:
                session = manager.get_or_create_session(group_id)
                sessions_created.append(session.session_id)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=create_session, args=("default",))
            for _ in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0)
        self.assertEqual(len(set(sessions_created)), 1)


class TestCPSessionCodec(unittest.TestCase):
    """Tests for CPSessionCodec class."""

    def test_encode_create_session_request(self):
        """Test encoding create session request."""
        msg = CPSessionCodec.encode_create_session_request("default", "python-client")
        self.assertIsNotNone(msg)

        frame = msg.next_frame()
        self.assertIsNotNone(frame)
        msg_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(msg_type, CP_SESSION_CREATE)

    def test_decode_create_session_response(self):
        """Test decoding create session response."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(RESPONSE_HEADER_SIZE + 3 * LONG_SIZE)
        struct.pack_into("<q", buffer, RESPONSE_HEADER_SIZE, 12345)
        struct.pack_into("<q", buffer, RESPONSE_HEADER_SIZE + LONG_SIZE, 300000)
        struct.pack_into("<q", buffer, RESPONSE_HEADER_SIZE + 2 * LONG_SIZE, 5000)

        msg = ClientMessage.create_for_decode()
        msg.add_frame(Frame(bytes(buffer)))

        session_id, ttl, heartbeat = CPSessionCodec.decode_create_session_response(msg)
        self.assertEqual(session_id, 12345)
        self.assertEqual(ttl, 300000)
        self.assertEqual(heartbeat, 5000)

    def test_encode_close_session_request(self):
        """Test encoding close session request."""
        msg = CPSessionCodec.encode_close_session_request("default", 12345)
        self.assertIsNotNone(msg)

        frame = msg.next_frame()
        msg_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(msg_type, CP_SESSION_CLOSE)

    def test_decode_close_session_response(self):
        """Test decoding close session response."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(RESPONSE_HEADER_SIZE + BOOLEAN_SIZE)
        struct.pack_into("<B", buffer, RESPONSE_HEADER_SIZE, 1)

        msg = ClientMessage.create_for_decode()
        msg.add_frame(Frame(bytes(buffer)))

        result = CPSessionCodec.decode_close_session_response(msg)
        self.assertTrue(result)

    def test_encode_heartbeat_request(self):
        """Test encoding heartbeat request."""
        msg = CPSessionCodec.encode_heartbeat_request("default", 12345)
        self.assertIsNotNone(msg)

        frame = msg.next_frame()
        msg_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(msg_type, CP_SESSION_HEARTBEAT)

    def test_encode_generate_thread_id_request(self):
        """Test encoding generate thread ID request."""
        msg = CPSessionCodec.encode_generate_thread_id_request("default")
        self.assertIsNotNone(msg)

        frame = msg.next_frame()
        msg_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(msg_type, CP_SESSION_GENERATE_THREAD_ID)

    def test_decode_generate_thread_id_response(self):
        """Test decoding generate thread ID response."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(RESPONSE_HEADER_SIZE + LONG_SIZE)
        struct.pack_into("<q", buffer, RESPONSE_HEADER_SIZE, 98765)

        msg = ClientMessage.create_for_decode()
        msg.add_frame(Frame(bytes(buffer)))

        thread_id = CPSessionCodec.decode_generate_thread_id_response(msg)
        self.assertEqual(thread_id, 98765)

    def test_encode_get_sessions_request(self):
        """Test encoding get sessions request."""
        msg = CPSessionCodec.encode_get_sessions_request("default")
        self.assertIsNotNone(msg)

        frame = msg.next_frame()
        msg_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(msg_type, CP_SESSION_GET_SESSIONS)


class TestCPMember(unittest.TestCase):
    """Tests for CPMember class."""

    def test_member_creation(self):
        """Test basic member creation."""
        member = CPMember("uuid-123", "127.0.0.1:5701")
        self.assertEqual(member.uuid, "uuid-123")
        self.assertEqual(member.address, "127.0.0.1:5701")

    def test_member_equality(self):
        """Test member equality based on UUID."""
        member1 = CPMember("uuid-123", "127.0.0.1:5701")
        member2 = CPMember("uuid-123", "127.0.0.1:5702")
        member3 = CPMember("uuid-456", "127.0.0.1:5701")

        self.assertEqual(member1, member2)
        self.assertNotEqual(member1, member3)

    def test_member_hash(self):
        """Test member hashing."""
        member1 = CPMember("uuid-123", "127.0.0.1:5701")
        member2 = CPMember("uuid-123", "127.0.0.1:5702")

        self.assertEqual(hash(member1), hash(member2))

    def test_member_repr(self):
        """Test member string representation."""
        member = CPMember("uuid-123", "127.0.0.1:5701")
        repr_str = repr(member)
        self.assertIn("uuid-123", repr_str)
        self.assertIn("127.0.0.1:5701", repr_str)


class TestCPGroup(unittest.TestCase):
    """Tests for CPGroup class."""

    def test_group_creation(self):
        """Test basic group creation."""
        group = CPGroup("default", "group-id-123")
        self.assertEqual(group.name, "default")
        self.assertEqual(group.group_id, "group-id-123")
        self.assertEqual(group.status, CPGroupStatus.ACTIVE)
        self.assertTrue(group.is_active())

    def test_group_with_members(self):
        """Test group creation with members."""
        members = {CPMember("uuid-1", "addr1"), CPMember("uuid-2", "addr2")}
        group = CPGroup("default", "group-id-123", members)
        self.assertEqual(group.member_count, 2)
        self.assertEqual(len(group.members), 2)

    def test_group_members_copy(self):
        """Test that members property returns a copy."""
        members = {CPMember("uuid-1", "addr1")}
        group = CPGroup("default", "group-id-123", members)
        returned_members = group.members
        returned_members.add(CPMember("uuid-2", "addr2"))
        self.assertEqual(group.member_count, 1)

    def test_group_default_name(self):
        """Test default group name constant."""
        self.assertEqual(CPGroup.DEFAULT_GROUP_NAME, "default")

    def test_group_metadata_name(self):
        """Test metadata group name constant."""
        self.assertEqual(CPGroup.METADATA_GROUP_NAME, "METADATA")

    def test_group_equality(self):
        """Test group equality based on group_id."""
        group1 = CPGroup("default", "group-id-123")
        group2 = CPGroup("default", "group-id-123")
        group3 = CPGroup("default", "group-id-456")

        self.assertEqual(group1, group2)
        self.assertNotEqual(group1, group3)

    def test_group_hash(self):
        """Test group hashing."""
        group1 = CPGroup("default", "group-id-123")
        group2 = CPGroup("other", "group-id-123")

        self.assertEqual(hash(group1), hash(group2))

    def test_group_repr(self):
        """Test group string representation."""
        group = CPGroup("default", "group-id-123")
        repr_str = repr(group)
        self.assertIn("default", repr_str)
        self.assertIn("group-id-123", repr_str)
        self.assertIn("ACTIVE", repr_str)


class TestCPGroupManager(unittest.TestCase):
    """Tests for CPGroupManager class."""

    def test_manager_creation(self):
        """Test manager creation."""
        manager = CPGroupManager()
        self.assertFalse(manager.is_closed)
        self.assertEqual(len(manager.get_groups()), 0)

    def test_register_group(self):
        """Test group registration."""
        manager = CPGroupManager()
        group = CPGroup("default", "group-id-123")
        manager.register_group(group)

        self.assertTrue(manager.contains_group("default"))
        self.assertEqual(manager.get_group("default"), group)

    def test_get_group(self):
        """Test getting a group by name."""
        manager = CPGroupManager()
        self.assertIsNone(manager.get_group("default"))

        group = CPGroup("default", "group-id-123")
        manager.register_group(group)
        self.assertEqual(manager.get_group("default"), group)

    def test_get_groups(self):
        """Test getting all groups."""
        manager = CPGroupManager()
        manager.register_group(CPGroup("group1", "id1"))
        manager.register_group(CPGroup("group2", "id2"))

        groups = manager.get_groups()
        self.assertEqual(len(groups), 2)

    def test_get_active_groups(self):
        """Test getting active groups."""
        manager = CPGroupManager()
        group1 = CPGroup("group1", "id1")
        group2 = CPGroup("group2", "id2")
        manager.register_group(group1)
        manager.register_group(group2)

        active = manager.get_active_groups()
        self.assertEqual(len(active), 2)

    def test_get_default_group(self):
        """Test getting default group."""
        manager = CPGroupManager()
        self.assertIsNone(manager.get_default_group())

        group = CPGroup(CPGroup.DEFAULT_GROUP_NAME, "id1")
        manager.register_group(group)
        self.assertEqual(manager.get_default_group(), group)

    def test_get_metadata_group(self):
        """Test getting metadata group."""
        manager = CPGroupManager()
        self.assertIsNone(manager.get_metadata_group())

        group = CPGroup(CPGroup.METADATA_GROUP_NAME, "id1")
        manager.register_group(group)
        self.assertEqual(manager.get_metadata_group(), group)

    def test_remove_group(self):
        """Test removing a group."""
        manager = CPGroupManager()
        group = CPGroup("default", "id1")
        manager.register_group(group)

        removed = manager.remove_group("default")
        self.assertEqual(removed, group)
        self.assertIsNone(manager.get_group("default"))
        self.assertIsNone(manager.remove_group("default"))

    def test_clear(self):
        """Test clearing all groups."""
        manager = CPGroupManager()
        manager.register_group(CPGroup("group1", "id1"))
        manager.register_group(CPGroup("group2", "id2"))

        manager.clear()
        self.assertEqual(len(manager.get_groups()), 0)

    def test_shutdown(self):
        """Test manager shutdown."""
        manager = CPGroupManager()
        manager.register_group(CPGroup("default", "id1"))

        manager.shutdown()
        self.assertTrue(manager.is_closed)
        self.assertEqual(len(manager.get_groups()), 0)

    def test_operations_after_shutdown(self):
        """Test that operations fail after shutdown."""
        manager = CPGroupManager()
        manager.shutdown()

        from hazelcast.exceptions import IllegalStateException
        with self.assertRaises(IllegalStateException):
            manager.register_group(CPGroup("default", "id1"))

    def test_create_cp_group(self):
        """Test CP group creation."""
        manager = CPGroupManager()
        group = manager.create_cp_group("test-group")

        self.assertIsNotNone(group)
        self.assertEqual(group.name, "test-group")
        self.assertTrue(manager.contains_group("test-group"))

    def test_destroy_cp_object(self):
        """Test CP object destruction (no error without invocation service)."""
        manager = CPGroupManager()
        manager.destroy_cp_object("group-id", "AtomicLong", "counter")

    def test_get_cp_group_ids_from_cluster(self):
        """Test getting CP group IDs (empty without invocation service)."""
        manager = CPGroupManager()
        ids = manager.get_cp_group_ids_from_cluster()
        self.assertEqual(ids, [])

    def test_get_cp_object_infos_from_cluster(self):
        """Test getting CP object infos (empty without invocation service)."""
        manager = CPGroupManager()
        infos = manager.get_cp_object_infos_from_cluster("group-id")
        self.assertEqual(infos, [])

    def test_manager_repr(self):
        """Test manager string representation."""
        manager = CPGroupManager()
        manager.register_group(CPGroup("default", "id1"))
        repr_str = repr(manager)
        self.assertIn("CPGroupManager", repr_str)
        self.assertIn("groups=1", repr_str)

    def test_thread_safety(self):
        """Test thread-safe group management."""
        manager = CPGroupManager()
        errors = []

        def register_group(name):
            try:
                group = CPGroup(name, f"id-{name}")
                manager.register_group(group)
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
        self.assertEqual(len(manager.get_groups()), 10)


class TestCPGroupCodec(unittest.TestCase):
    """Tests for CPGroupCodec class."""

    def test_encode_create_cp_group_request(self):
        """Test encoding create CP group request."""
        msg = CPGroupCodec.encode_create_cp_group_request("test-group")
        self.assertIsNotNone(msg)

        frame = msg.next_frame()
        self.assertIsNotNone(frame)
        msg_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(msg_type, CP_GROUP_CREATE_CP_GROUP)

    def test_decode_create_cp_group_response(self):
        """Test decoding create CP group response."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(RESPONSE_HEADER_SIZE + 2 * LONG_SIZE)
        struct.pack_into("<q", buffer, RESPONSE_HEADER_SIZE, 12345)
        struct.pack_into("<q", buffer, RESPONSE_HEADER_SIZE + LONG_SIZE, 67890)

        msg = ClientMessage.create_for_decode()
        msg.add_frame(Frame(bytes(buffer)))
        msg.add_frame(Frame(b"test-group"))

        group_name, seed, group_id = CPGroupCodec.decode_create_cp_group_response(msg)
        self.assertEqual(group_name, "test-group")
        self.assertEqual(seed, 12345)
        self.assertEqual(group_id, 67890)

    def test_encode_destroy_cp_object_request(self):
        """Test encoding destroy CP object request."""
        msg = CPGroupCodec.encode_destroy_cp_object_request(
            "group-id", "AtomicLong", "counter"
        )
        self.assertIsNotNone(msg)

        frame = msg.next_frame()
        msg_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(msg_type, CP_GROUP_DESTROY_CP_OBJECT)

    def test_encode_get_cp_group_ids_request(self):
        """Test encoding get CP group IDs request."""
        msg = CPGroupCodec.encode_get_cp_group_ids_request()
        self.assertIsNotNone(msg)

        frame = msg.next_frame()
        msg_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(msg_type, CP_GROUP_GET_CP_GROUP_IDS)

    def test_encode_get_cp_object_infos_request(self):
        """Test encoding get CP object infos request."""
        msg = CPGroupCodec.encode_get_cp_object_infos_request("group-id")
        self.assertIsNotNone(msg)

        frame = msg.next_frame()
        msg_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(msg_type, CP_GROUP_GET_CP_OBJECT_INFOS)

    def test_decode_empty_get_cp_group_ids_response(self):
        """Test decoding empty get CP group IDs response."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(RESPONSE_HEADER_SIZE)
        msg = ClientMessage.create_for_decode()
        msg.add_frame(Frame(bytes(buffer)))

        ids = CPGroupCodec.decode_get_cp_group_ids_response(msg)
        self.assertEqual(ids, [])

    def test_decode_empty_get_cp_object_infos_response(self):
        """Test decoding empty get CP object infos response."""
        from hazelcast.protocol.client_message import ClientMessage, Frame

        buffer = bytearray(RESPONSE_HEADER_SIZE)
        msg = ClientMessage.create_for_decode()
        msg.add_frame(Frame(bytes(buffer)))

        infos = CPGroupCodec.decode_get_cp_object_infos_response(msg)
        self.assertEqual(infos, [])


class TestCPSessionLifecycle(unittest.TestCase):
    """Integration tests for CP session lifecycle."""

    def test_full_session_lifecycle(self):
        """Test complete session lifecycle."""
        manager = CPSessionManager()

        session_id, _ = manager.acquire_session("default")
        self.assertGreater(session_id, 0)

        session = manager.get_session("default")
        self.assertIsNotNone(session)
        self.assertTrue(session.is_active())
        self.assertEqual(session.acquire_count, 1)

        self.assertTrue(manager.heartbeat("default"))

        manager.release_session("default", session_id)
        self.assertEqual(session.acquire_count, 0)

        self.assertTrue(manager.close_session("default"))
        self.assertIsNone(manager.get_session("default"))

    def test_multiple_groups(self):
        """Test managing sessions for multiple CP groups."""
        manager = CPSessionManager()

        groups = ["group1", "group2", "group3"]
        session_ids = {}

        for group in groups:
            session_id, _ = manager.acquire_session(group)
            session_ids[group] = session_id

        self.assertEqual(len(manager.get_active_sessions()), 3)

        for group, session_id in session_ids.items():
            session = manager.get_session(group)
            self.assertEqual(session.session_id, session_id)

        manager.shutdown()
        self.assertEqual(len(manager.get_active_sessions()), 0)

    def test_session_reuse(self):
        """Test that sessions are reused for the same group."""
        manager = CPSessionManager()

        session1 = manager.get_or_create_session("default")
        session2 = manager.get_or_create_session("default")

        self.assertIs(session1, session2)

    def test_session_recreation_after_close(self):
        """Test that a new session is created after closing."""
        manager = CPSessionManager()

        session1 = manager.get_or_create_session("default")
        session_id1 = session1.session_id

        manager.close_session("default")

        session2 = manager.get_or_create_session("default")
        self.assertNotEqual(session_id1, session2.session_id)


if __name__ == "__main__":
    unittest.main()
