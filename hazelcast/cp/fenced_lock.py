"""CP FencedLock distributed data structure."""

from concurrent.futures import Future
from typing import Optional, TYPE_CHECKING
import threading
import time

from hazelcast.cp.base import CPProxy, CPGroupId

if TYPE_CHECKING:
    from hazelcast.invocation import InvocationService


class FencedLock(CPProxy):
    """A distributed reentrant lock backed by CP subsystem.

    Provides linearizable mutual exclusion with fencing tokens
    to prevent zombie processes from corrupting state.
    """

    SERVICE_NAME = "hz:raft:lockService"
    INVALID_FENCE = 0

    def __init__(
        self,
        name: str,
        group_id: Optional[CPGroupId] = None,
        invocation_service: Optional["InvocationService"] = None,
        direct_to_leader: bool = True,
    ):
        super().__init__(
            self.SERVICE_NAME,
            name,
            group_id or CPGroupId(CPGroupId.DEFAULT_GROUP_NAME),
            invocation_service,
            direct_to_leader,
        )
        self._lock = threading.RLock()
        self._fence: int = 0
        self._owner: Optional[int] = None
        self._lock_count: int = 0

    def lock(self) -> int:
        """Acquire the lock.

        Blocks until the lock is acquired.

        Returns:
            The fence token for this lock acquisition.
        """
        return self.lock_async().result()

    def lock_async(self) -> Future:
        """Acquire the lock asynchronously.

        Returns:
            Future containing the fence token.
        """
        self._check_not_destroyed()
        future: Future = Future()

        self._lock.acquire()
        self._fence += 1
        self._owner = threading.current_thread().ident
        self._lock_count += 1
        future.set_result(self._fence)

        return future

    def try_lock(self, timeout: float = 0) -> int:
        """Try to acquire the lock.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            The fence token if acquired, INVALID_FENCE otherwise.
        """
        return self.try_lock_async(timeout).result()

    def try_lock_async(self, timeout: float = 0) -> Future:
        """Try to acquire the lock asynchronously.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            Future containing the fence token or INVALID_FENCE.
        """
        self._check_not_destroyed()
        future: Future = Future()

        acquired = self._lock.acquire(blocking=True, timeout=max(0, timeout))
        if acquired:
            self._fence += 1
            self._owner = threading.current_thread().ident
            self._lock_count += 1
            future.set_result(self._fence)
        else:
            future.set_result(self.INVALID_FENCE)

        return future

    def unlock(self) -> None:
        """Release the lock.

        Raises:
            IllegalStateException: If the lock is not held by this thread.
        """
        self.unlock_async().result()

    def unlock_async(self) -> Future:
        """Release the lock asynchronously.

        Returns:
            Future that completes when the lock is released.
        """
        self._check_not_destroyed()
        future: Future = Future()

        try:
            self._lock_count -= 1
            self._lock.release()
            if self._lock_count == 0:
                self._owner = None
            future.set_result(None)
        except RuntimeError as e:
            from hazelcast.exceptions import IllegalStateException
            future.set_exception(
                IllegalStateException(f"Lock not held: {e}")
            )

        return future

    def get_fence(self) -> int:
        """Get the current fence token.

        Returns:
            The current fence token, or INVALID_FENCE if not locked.
        """
        return self.get_fence_async().result()

    def get_fence_async(self) -> Future:
        """Get the current fence asynchronously.

        Returns:
            Future containing the fence token.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(self._fence if self._owner else self.INVALID_FENCE)
        return future

    def is_locked(self) -> bool:
        """Check if the lock is held by any thread.

        Returns:
            True if the lock is held.
        """
        return self.is_locked_async().result()

    def is_locked_async(self) -> Future:
        """Check if locked asynchronously.

        Returns:
            Future containing True if locked.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(self._owner is not None)
        return future

    def is_locked_by_current_thread(self) -> bool:
        """Check if the lock is held by the current thread.

        Returns:
            True if the current thread holds the lock.
        """
        return self.is_locked_by_current_thread_async().result()

    def is_locked_by_current_thread_async(self) -> Future:
        """Check if locked by current thread asynchronously.

        Returns:
            Future containing True if held by current thread.
        """
        self._check_not_destroyed()
        future: Future = Future()
        current = threading.current_thread().ident
        future.set_result(self._owner == current)
        return future

    def get_lock_count(self) -> int:
        """Get the number of holds on this lock.

        Returns:
            The lock count (for reentrant locks).
        """
        return self.get_lock_count_async().result()

    def get_lock_count_async(self) -> Future:
        """Get the lock count asynchronously.

        Returns:
            Future containing the lock count.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(self._lock_count)
        return future

    def __enter__(self) -> int:
        return self.lock()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.unlock()
