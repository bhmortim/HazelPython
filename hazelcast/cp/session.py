"""CP Subsystem session management."""

import threading
import time
from typing import Dict, List, Optional, TYPE_CHECKING
from enum import Enum

from hazelcast.exceptions import IllegalStateException

if TYPE_CHECKING:
    from hazelcast.proxy.base import ProxyContext


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
    """

    def __init__(self, session_id: int, group_id: str):
        self._session_id = session_id
        self._group_id = group_id
        self._state = CPSessionState.ACTIVE
        self._creation_time = time.time()
        self._last_heartbeat = self._creation_time

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

    def is_active(self) -> bool:
        return self._state == CPSessionState.ACTIVE

    def _update_heartbeat(self) -> None:
        self._last_heartbeat = time.time()

    def _close(self) -> None:
        self._state = CPSessionState.CLOSED

    def _expire(self) -> None:
        self._state = CPSessionState.EXPIRED

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

    def __init__(self, context: Optional["ProxyContext"] = None):
        self._context = context
        self._sessions: Dict[str, CPSession] = {}
        self._lock = threading.Lock()
        self._session_counter = 0
        self._closed = False

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

            self._session_counter += 1
            session_id = int(time.time() * 1000) + self._session_counter
            session = CPSession(session_id, group_id)
            self._sessions[group_id] = session
            return session

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
                session._close()
                return True
            return False

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
                session._update_heartbeat()
                return True
            return False

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
