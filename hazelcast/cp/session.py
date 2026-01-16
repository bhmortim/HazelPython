"""CP Subsystem session management."""

import threading
import time
from typing import Callable, Dict, List, Optional, Tuple, TYPE_CHECKING
from enum import Enum

from hazelcast.exceptions import IllegalStateException

if TYPE_CHECKING:
    from hazelcast.proxy.base import ProxyContext
    from hazelcast.protocol.client_message import ClientMessage


class CPSessionState(Enum):
    """State of a CP session."""
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"
    EXPIRED = "EXPIRED"


class CPSession:
    """Represents a CP Subsystem session.

    A CP session is used to track client presence in CP groups for
    distributed primitives like FencedLock and Semaphore.

    Attributes:
        session_id: The unique session identifier.
        group_id: The CP group this session belongs to.
        state: Current state of the session.
        creation_time: When the session was created.
        ttl_millis: Session time-to-live in milliseconds.
        heartbeat_millis: Heartbeat interval in milliseconds.
    """

    def __init__(
        self,
        session_id: int,
        group_id: str,
        ttl_millis: int = 0,
        heartbeat_millis: int = 0,
    ):
        self._session_id = session_id
        self._group_id = group_id
        self._state = CPSessionState.ACTIVE
        self._creation_time = time.time()
        self._last_heartbeat = self._creation_time
        self._ttl_millis = ttl_millis
        self._heartbeat_millis = heartbeat_millis
        self._acquire_count = 0

    @property
    def session_id(self) -> int:
        return self._session_id

    @property
    def group_id(self) -> str:
        return self._group_id

    @property
    def state(self) -> CPSessionState:
        return self._state

    @property
    def creation_time(self) -> float:
        return self._creation_time

    @property
    def last_heartbeat(self) -> float:
        return self._last_heartbeat

    @property
    def ttl_millis(self) -> int:
        return self._ttl_millis

    @property
    def heartbeat_millis(self) -> int:
        return self._heartbeat_millis

    @property
    def acquire_count(self) -> int:
        return self._acquire_count

    def is_active(self) -> bool:
        return self._state == CPSessionState.ACTIVE

    def _update_heartbeat(self) -> None:
        self._last_heartbeat = time.time()

    def _close(self) -> None:
        self._state = CPSessionState.CLOSED

    def _expire(self) -> None:
        self._state = CPSessionState.EXPIRED

    def _increment_acquire(self) -> int:
        self._acquire_count += 1
        return self._acquire_count

    def _decrement_acquire(self) -> int:
        self._acquire_count = max(0, self._acquire_count - 1)
        return self._acquire_count

    def __repr__(self) -> str:
        return f"CPSession(id={self._session_id}, group={self._group_id!r}, state={self._state.value})"


class CPSessionManager:
    """Manages CP sessions for the client.

    The session manager maintains sessions for CP groups and handles
    session lifecycle including creation, heartbeats, and cleanup.

    Example:
        >>> session_mgr = client.get_cp_subsystem().get_session_manager()
        >>> session = session_mgr.get_session("default")
        >>> session_mgr.close_session("default")
    """

    def __init__(
        self,
        context: Optional["ProxyContext"] = None,
        invocation_service: Optional[Callable[["ClientMessage"], "ClientMessage"]] = None,
    ):
        self._context = context
        self._invocation_service = invocation_service
        self._sessions: Dict[str, CPSession] = {}
        self._thread_ids: Dict[str, int] = {}
        self._lock = threading.Lock()
        self._session_counter = 0
        self._closed = False
        self._endpoint_name = "python-client"

    def get_session(self, group_id: str) -> Optional[CPSession]:
        """Get an existing session for a CP group.

        Args:
            group_id: The CP group identifier.

        Returns:
            The session if it exists and is active, None otherwise.
        """
        with self._lock:
            session = self._sessions.get(group_id)
            if session and session.is_active():
                return session
            return None

    def get_or_create_session(self, group_id: str) -> CPSession:
        """Get or create a session for a CP group.

        Args:
            group_id: The CP group identifier.

        Returns:
            An active session for the group.

        Raises:
            IllegalStateException: If the manager is closed.
        """
        self._check_not_closed()

        with self._lock:
            session = self._sessions.get(group_id)
            if session and session.is_active():
                return session

            session_id, ttl_millis, heartbeat_millis = self._create_session_on_cluster(group_id)
            session = CPSession(session_id, group_id, ttl_millis, heartbeat_millis)
            self._sessions[group_id] = session
            return session

    def _create_session_on_cluster(self, group_id: str) -> Tuple[int, int, int]:
        """Create a session on the cluster via protocol.

        Args:
            group_id: The CP group identifier.

        Returns:
            Tuple of (session_id, ttl_millis, heartbeat_millis).
        """
        if self._invocation_service is not None:
            from hazelcast.protocol.codec import CPSessionCodec

            request = CPSessionCodec.encode_create_session_request(
                group_id, self._endpoint_name
            )
            response = self._invocation_service(request)
            return CPSessionCodec.decode_create_session_response(response)

        self._session_counter += 1
        session_id = int(time.time() * 1000) + self._session_counter
        return session_id, 300000, 5000

    def close_session(self, group_id: str) -> bool:
        """Close a session for a CP group.

        Args:
            group_id: The CP group identifier.

        Returns:
            True if a session was closed, False if no active session existed.
        """
        with self._lock:
            session = self._sessions.get(group_id)
            if session and session.is_active():
                self._close_session_on_cluster(group_id, session.session_id)
                session._close()
                return True
            return False

    def _close_session_on_cluster(self, group_id: str, session_id: int) -> bool:
        """Close a session on the cluster via protocol.

        Args:
            group_id: The CP group identifier.
            session_id: The session identifier.

        Returns:
            True if closed successfully.
        """
        if self._invocation_service is not None:
            from hazelcast.protocol.codec import CPSessionCodec

            request = CPSessionCodec.encode_close_session_request(group_id, session_id)
            response = self._invocation_service(request)
            return CPSessionCodec.decode_close_session_response(response)
        return True

    def heartbeat(self, group_id: str) -> bool:
        """Send a heartbeat for a session.

        Args:
            group_id: The CP group identifier.

        Returns:
            True if heartbeat was recorded, False if no active session.
        """
        with self._lock:
            session = self._sessions.get(group_id)
            if session and session.is_active():
                self._send_heartbeat_to_cluster(group_id, session.session_id)
                session._update_heartbeat()
                return True
            return False

    def _send_heartbeat_to_cluster(self, group_id: str, session_id: int) -> None:
        """Send a heartbeat to the cluster via protocol.

        Args:
            group_id: The CP group identifier.
            session_id: The session identifier.
        """
        if self._invocation_service is not None:
            from hazelcast.protocol.codec import CPSessionCodec

            request = CPSessionCodec.encode_heartbeat_request(group_id, session_id)
            self._invocation_service(request)

    def generate_thread_id(self, group_id: str) -> int:
        """Generate a unique thread ID for a CP group.

        Args:
            group_id: The CP group identifier.

        Returns:
            A unique thread ID.
        """
        self._check_not_closed()

        with self._lock:
            if group_id in self._thread_ids:
                return self._thread_ids[group_id]

            thread_id = self._generate_thread_id_on_cluster(group_id)
            self._thread_ids[group_id] = thread_id
            return thread_id

    def _generate_thread_id_on_cluster(self, group_id: str) -> int:
        """Generate a thread ID on the cluster via protocol.

        Args:
            group_id: The CP group identifier.

        Returns:
            The generated thread ID.
        """
        if self._invocation_service is not None:
            from hazelcast.protocol.codec import CPSessionCodec

            request = CPSessionCodec.encode_generate_thread_id_request(group_id)
            response = self._invocation_service(request)
            return CPSessionCodec.decode_generate_thread_id_response(response)

        return int(time.time() * 1000000) + threading.current_thread().ident

    def get_sessions_from_cluster(self, group_id: str) -> List[Tuple[int, int, str, str]]:
        """Get all sessions for a CP group from the cluster.

        Args:
            group_id: The CP group identifier.

        Returns:
            List of (session_id, creation_time, endpoint_name, endpoint_type) tuples.
        """
        self._check_not_closed()

        if self._invocation_service is not None:
            from hazelcast.protocol.codec import CPSessionCodec

            request = CPSessionCodec.encode_get_sessions_request(group_id)
            response = self._invocation_service(request)
            return CPSessionCodec.decode_get_sessions_response(response)

        return []

    def acquire_session(self, group_id: str) -> Tuple[int, int]:
        """Acquire a session for use by a CP primitive.

        Args:
            group_id: The CP group identifier.

        Returns:
            Tuple of (session_id, acquire_count).
        """
        session = self.get_or_create_session(group_id)
        count = session._increment_acquire()
        return session.session_id, count

    def release_session(self, group_id: str, session_id: int) -> None:
        """Release a session after use by a CP primitive.

        Args:
            group_id: The CP group identifier.
            session_id: The session identifier.
        """
        with self._lock:
            session = self._sessions.get(group_id)
            if session and session.session_id == session_id:
                session._decrement_acquire()

    def invalidate_session(self, group_id: str, session_id: int) -> None:
        """Invalidate a session (e.g., due to session expiry on server).

        Args:
            group_id: The CP group identifier.
            session_id: The session identifier.
        """
        with self._lock:
            session = self._sessions.get(group_id)
            if session and session.session_id == session_id:
                session._expire()
                del self._sessions[group_id]

    def get_all_sessions(self) -> List[CPSession]:
        """Get all sessions managed by this manager.

        Returns:
            List of all sessions (active and inactive).
        """
        with self._lock:
            return list(self._sessions.values())

    def get_active_sessions(self) -> List[CPSession]:
        """Get all active sessions.

        Returns:
            List of active sessions.
        """
        with self._lock:
            return [s for s in self._sessions.values() if s.is_active()]

    def close_all(self) -> int:
        """Close all sessions.

        Returns:
            Number of sessions closed.
        """
        with self._lock:
            count = 0
            for session in self._sessions.values():
                if session.is_active():
                    session._close()
                    count += 1
            return count

    def shutdown(self) -> None:
        """Shutdown the session manager."""
        self.close_all()
        with self._lock:
            self._closed = True

    @property
    def is_closed(self) -> bool:
        """Check if the manager is closed."""
        with self._lock:
            return self._closed

    def _check_not_closed(self) -> None:
        if self._closed:
            raise IllegalStateException("CPSessionManager is closed")

    def __repr__(self) -> str:
        with self._lock:
            active = sum(1 for s in self._sessions.values() if s.is_active())
            return f"CPSessionManager(sessions={len(self._sessions)}, active={active})"
