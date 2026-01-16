"""CP Subsystem synchronization primitives."""

import threading
import time
from concurrent.futures import Future
from typing import Optional, TYPE_CHECKING

from hazelcast.proxy.base import Proxy, ProxyContext
from hazelcast.exceptions import IllegalStateException
from hazelcast.protocol.codec import (
    FencedLockCodec,
    SemaphoreCodec,
    CountDownLatchCodec,
)

if TYPE_CHECKING:
    from hazelcast.protocol.client_message import ClientMessage


INVALID_FENCE = 0


class FencedLock(Proxy):
    """A distributed reentrant mutex with fencing token support.

    FencedLock provides mutual exclusion with strong consistency using
    the CP Subsystem. Each lock acquisition returns a monotonically
    increasing fencing token that can be used to detect stale lock
    holders in distributed scenarios.

    The lock is reentrant - a thread that holds the lock can acquire
    it again without blocking. Each acquisition must be matched with
    a corresponding release.

    Example:
        >>> lock = client.get_fenced_lock("my-lock")
        >>> fence = lock.lock()
        >>> try:
        ...     # Critical section
        ...     print(f"Lock acquired with fence: {fence}")
        ... finally:
        ...     lock.unlock()

        Context manager usage::

            with lock as fence:
                # Critical section
                pass
    """

    def __init__(
        self,
        service_name: str,
        name: str,
        context: Optional[ProxyContext] = None,
    ):
        super().__init__(service_name, name, context)
        self._group_id = self._parse_group_id(name)
        self._lock = threading.RLock()
        self._thread_id = 0
        self._lock_count = 0
        self._fence = INVALID_FENCE
        self._session_id = -1

    def _parse_group_id(self, name: str) -> str:
        """Parse CP group ID from name."""
        if "@" in name:
            return name.split("@")[1]
        return "default"

    def _get_object_name(self) -> str:
        """Get the object name without group suffix."""
        if "@" in self._name:
            return self._name.split("@")[0]
        return self._name

    def _get_thread_id(self) -> int:
        """Get a unique thread identifier."""
        return threading.current_thread().ident or 0

    def _get_session_id(self) -> int:
        """Get or create session ID."""
        if self._session_id < 0:
            self._session_id = int(time.time() * 1000) % (2**31)
        return self._session_id

    def lock(self) -> int:
        """Acquire the lock, blocking until available.

        Returns:
            The fencing token for this lock acquisition.

        Raises:
            IllegalStateException: If the lock is destroyed.
        """
        return self.lock_async().result()

    def lock_async(self) -> Future:
        """Acquire the lock asynchronously.

        Returns:
            A Future that will contain the fencing token.
        """
        thread_id = self._get_thread_id()
        session_id = self._get_session_id()
        invocation_uid = int(time.time() * 1000000) % (2**63)

        request = FencedLockCodec.encode_lock_request(
            self._group_id,
            self._get_object_name(),
            session_id,
            thread_id,
            invocation_uid,
        )

        def handle_response(msg: "ClientMessage") -> int:
            fence = FencedLockCodec.decode_lock_response(msg)
            with self._lock:
                self._fence = fence
                self._thread_id = thread_id
                self._lock_count += 1
            return fence

        return self._invoke(request, handle_response)

    def try_lock(self, timeout: float = 0) -> int:
        """Try to acquire the lock with an optional timeout.

        Args:
            timeout: Maximum time to wait in seconds. 0 means no waiting.

        Returns:
            The fencing token if acquired, INVALID_FENCE (0) otherwise.
        """
        return self.try_lock_async(timeout).result()

    def try_lock_async(self, timeout: float = 0) -> Future:
        """Try to acquire the lock asynchronously.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            A Future that will contain the fencing token or INVALID_FENCE.
        """
        thread_id = self._get_thread_id()
        session_id = self._get_session_id()
        invocation_uid = int(time.time() * 1000000) % (2**63)
        timeout_ms = int(timeout * 1000)

        request = FencedLockCodec.encode_try_lock_request(
            self._group_id,
            self._get_object_name(),
            session_id,
            thread_id,
            invocation_uid,
            timeout_ms,
        )

        def handle_response(msg: "ClientMessage") -> int:
            fence = FencedLockCodec.decode_try_lock_response(msg)
            if fence != INVALID_FENCE:
                with self._lock:
                    self._fence = fence
                    self._thread_id = thread_id
                    self._lock_count += 1
            return fence

        return self._invoke(request, handle_response)

    def unlock(self) -> None:
        """Release the lock.

        Raises:
            IllegalStateException: If the current thread doesn't hold the lock.
        """
        self.unlock_async().result()

    def unlock_async(self) -> Future:
        """Release the lock asynchronously.

        Returns:
            A Future that completes when the lock is released.

        Raises:
            IllegalStateException: If the current thread doesn't hold the lock.
        """
        thread_id = self._get_thread_id()
        session_id = self._get_session_id()
        invocation_uid = int(time.time() * 1000000) % (2**63)

        with self._lock:
            if self._thread_id != thread_id or self._lock_count == 0:
                future: Future = Future()
                future.set_exception(
                    IllegalStateException("Current thread does not hold this lock")
                )
                return future

        request = FencedLockCodec.encode_unlock_request(
            self._group_id,
            self._get_object_name(),
            session_id,
            thread_id,
            invocation_uid,
        )

        def handle_response(msg: "ClientMessage") -> None:
            FencedLockCodec.decode_unlock_response(msg)
            with self._lock:
                self._lock_count -= 1
                if self._lock_count == 0:
                    self._fence = INVALID_FENCE
                    self._thread_id = 0

        return self._invoke(request, handle_response)

    def is_locked(self) -> bool:
        """Check if the lock is currently held by any thread.

        Returns:
            True if the lock is held, False otherwise.
        """
        return self.is_locked_async().result()

    def is_locked_async(self) -> Future:
        """Check if the lock is currently held asynchronously.

        Returns:
            A Future that will contain True if the lock is held.
        """
        request = FencedLockCodec.encode_get_lock_ownership_state_request(
            self._group_id, self._get_object_name()
        )

        def handle_response(msg: "ClientMessage") -> bool:
            fence, lock_count, _, _ = FencedLockCodec.decode_get_lock_ownership_state_response(msg)
            return fence != INVALID_FENCE and lock_count > 0

        return self._invoke(request, handle_response)

    def is_locked_by_current_thread(self) -> bool:
        """Check if the lock is held by the current thread.

        Returns:
            True if the current thread holds the lock.
        """
        with self._lock:
            return (
                self._thread_id == self._get_thread_id()
                and self._lock_count > 0
            )

    def get_lock_count(self) -> int:
        """Get the number of times the lock has been acquired.

        Returns:
            The reentrant lock count.
        """
        return self.get_lock_count_async().result()

    def get_lock_count_async(self) -> Future:
        """Get the reentrant lock count asynchronously.

        Returns:
            A Future that will contain the lock count.
        """
        request = FencedLockCodec.encode_get_lock_ownership_state_request(
            self._group_id, self._get_object_name()
        )

        def handle_response(msg: "ClientMessage") -> int:
            _, lock_count, _, _ = FencedLockCodec.decode_get_lock_ownership_state_response(msg)
            return lock_count

        return self._invoke(request, handle_response)

    def get_fence(self) -> int:
        """Get the current fencing token.

        Returns:
            The fencing token, or INVALID_FENCE if not locked.
        """
        with self._lock:
            return self._fence

    def __enter__(self) -> int:
        """Context manager entry - acquires the lock.

        Returns:
            The fencing token.
        """
        return self.lock()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - releases the lock."""
        self.unlock()


class Semaphore(Proxy):
    """A distributed counting semaphore with strong consistency.

    Semaphore maintains a set of permits that can be acquired and released.
    Acquire operations block when no permits are available.

    The semaphore is initialized with a number of permits. The permit
    count can be dynamically increased or decreased.

    Example:
        >>> sem = client.get_semaphore("my-semaphore")
        >>> sem.init(5)  # Initialize with 5 permits
        >>> sem.acquire()  # Acquire 1 permit
        >>> sem.acquire(2)  # Acquire 2 permits
        >>> print(sem.available_permits())  # 2
        >>> sem.release(3)  # Release 3 permits
    """

    def __init__(
        self,
        service_name: str,
        name: str,
        context: Optional[ProxyContext] = None,
    ):
        super().__init__(service_name, name, context)
        self._group_id = self._parse_group_id(name)
        self._session_id = -1

    def _parse_group_id(self, name: str) -> str:
        """Parse CP group ID from name."""
        if "@" in name:
            return name.split("@")[1]
        return "default"

    def _get_object_name(self) -> str:
        """Get the object name without group suffix."""
        if "@" in self._name:
            return self._name.split("@")[0]
        return self._name

    def _get_thread_id(self) -> int:
        """Get a unique thread identifier."""
        return threading.current_thread().ident or 0

    def _get_session_id(self) -> int:
        """Get or create session ID."""
        if self._session_id < 0:
            self._session_id = int(time.time() * 1000) % (2**31)
        return self._session_id

    def init(self, permits: int) -> bool:
        """Initialize the semaphore with the given number of permits.

        This operation is idempotent - it only initializes if not already done.

        Args:
            permits: The initial number of permits. Must be non-negative.

        Returns:
            True if initialization was successful, False if already initialized.
        """
        return self.init_async(permits).result()

    def init_async(self, permits: int) -> Future:
        """Initialize the semaphore asynchronously.

        Args:
            permits: The initial number of permits.

        Returns:
            A Future that will contain True if initialization was successful.
        """
        if permits < 0:
            future: Future = Future()
            future.set_exception(
                IllegalStateException("Permits cannot be negative")
            )
            return future

        request = SemaphoreCodec.encode_init_request(
            self._group_id, self._get_object_name(), permits
        )
        return self._invoke(request, SemaphoreCodec.decode_init_response)

    def acquire(self, permits: int = 1) -> None:
        """Acquire permits, blocking until available.

        Args:
            permits: The number of permits to acquire. Default is 1.
        """
        self.acquire_async(permits).result()

    def acquire_async(self, permits: int = 1) -> Future:
        """Acquire permits asynchronously.

        Args:
            permits: The number of permits to acquire.

        Returns:
            A Future that completes when permits are acquired.
        """
        if permits < 1:
            future: Future = Future()
            future.set_exception(
                IllegalStateException("Permits must be positive")
            )
            return future

        thread_id = self._get_thread_id()
        session_id = self._get_session_id()
        invocation_uid = int(time.time() * 1000000) % (2**63)

        request = SemaphoreCodec.encode_acquire_request(
            self._group_id,
            self._get_object_name(),
            session_id,
            thread_id,
            invocation_uid,
            permits,
            -1,  # No timeout
        )
        return self._invoke(request)

    def try_acquire(self, permits: int = 1, timeout: float = 0) -> bool:
        """Try to acquire permits with an optional timeout.

        Args:
            permits: The number of permits to acquire. Default is 1.
            timeout: Maximum time to wait in seconds. 0 means no waiting.

        Returns:
            True if permits were acquired, False otherwise.
        """
        return self.try_acquire_async(permits, timeout).result()

    def try_acquire_async(self, permits: int = 1, timeout: float = 0) -> Future:
        """Try to acquire permits asynchronously.

        Args:
            permits: The number of permits to acquire.
            timeout: Maximum time to wait in seconds.

        Returns:
            A Future that will contain True if permits were acquired.
        """
        if permits < 1:
            future: Future = Future()
            future.set_exception(
                IllegalStateException("Permits must be positive")
            )
            return future

        thread_id = self._get_thread_id()
        session_id = self._get_session_id()
        invocation_uid = int(time.time() * 1000000) % (2**63)
        timeout_ms = int(timeout * 1000)

        request = SemaphoreCodec.encode_acquire_request(
            self._group_id,
            self._get_object_name(),
            session_id,
            thread_id,
            invocation_uid,
            permits,
            timeout_ms,
        )
        return self._invoke(request, SemaphoreCodec.decode_acquire_response)

    def release(self, permits: int = 1) -> None:
        """Release permits back to the semaphore.

        Args:
            permits: The number of permits to release. Default is 1.
        """
        self.release_async(permits).result()

    def release_async(self, permits: int = 1) -> Future:
        """Release permits asynchronously.

        Args:
            permits: The number of permits to release.

        Returns:
            A Future that completes when permits are released.
        """
        if permits < 1:
            future: Future = Future()
            future.set_exception(
                IllegalStateException("Permits must be positive")
            )
            return future

        thread_id = self._get_thread_id()
        session_id = self._get_session_id()
        invocation_uid = int(time.time() * 1000000) % (2**63)

        request = SemaphoreCodec.encode_release_request(
            self._group_id,
            self._get_object_name(),
            session_id,
            thread_id,
            invocation_uid,
            permits,
        )
        return self._invoke(request)

    def available_permits(self) -> int:
        """Get the number of available permits.

        Returns:
            The number of permits currently available.
        """
        return self.available_permits_async().result()

    def available_permits_async(self) -> Future:
        """Get available permits asynchronously.

        Returns:
            A Future that will contain the available permit count.
        """
        request = SemaphoreCodec.encode_available_permits_request(
            self._group_id, self._get_object_name()
        )
        return self._invoke(request, SemaphoreCodec.decode_available_permits_response)

    def drain_permits(self) -> int:
        """Acquire and return all immediately available permits.

        Returns:
            The number of permits acquired.
        """
        return self.drain_permits_async().result()

    def drain_permits_async(self) -> Future:
        """Drain all available permits asynchronously.

        Returns:
            A Future that will contain the number of permits acquired.
        """
        thread_id = self._get_thread_id()
        session_id = self._get_session_id()
        invocation_uid = int(time.time() * 1000000) % (2**63)

        request = SemaphoreCodec.encode_drain_request(
            self._group_id,
            self._get_object_name(),
            session_id,
            thread_id,
            invocation_uid,
        )
        return self._invoke(request, SemaphoreCodec.decode_drain_response)

    def reduce_permits(self, reduction: int) -> None:
        """Reduce the number of available permits.

        Args:
            reduction: The number of permits to reduce by.
        """
        self.reduce_permits_async(reduction).result()

    def reduce_permits_async(self, reduction: int) -> Future:
        """Reduce permits asynchronously.

        Args:
            reduction: The number of permits to reduce by.

        Returns:
            A Future that completes when the operation is done.
        """
        if reduction < 0:
            future: Future = Future()
            future.set_exception(
                IllegalStateException("Reduction cannot be negative")
            )
            return future

        thread_id = self._get_thread_id()
        session_id = self._get_session_id()
        invocation_uid = int(time.time() * 1000000) % (2**63)

        request = SemaphoreCodec.encode_change_request(
            self._group_id,
            self._get_object_name(),
            session_id,
            thread_id,
            invocation_uid,
            -reduction,
        )
        return self._invoke(request)

    def increase_permits(self, increase: int) -> None:
        """Increase the number of available permits.

        Args:
            increase: The number of permits to add.
        """
        self.increase_permits_async(increase).result()

    def increase_permits_async(self, increase: int) -> Future:
        """Increase permits asynchronously.

        Args:
            increase: The number of permits to add.

        Returns:
            A Future that completes when the operation is done.
        """
        if increase < 0:
            future: Future = Future()
            future.set_exception(
                IllegalStateException("Increase cannot be negative")
            )
            return future

        thread_id = self._get_thread_id()
        session_id = self._get_session_id()
        invocation_uid = int(time.time() * 1000000) % (2**63)

        request = SemaphoreCodec.encode_change_request(
            self._group_id,
            self._get_object_name(),
            session_id,
            thread_id,
            invocation_uid,
            increase,
        )
        return self._invoke(request)


class CountDownLatch(Proxy):
    """A distributed synchronization aid with strong consistency.

    CountDownLatch allows one or more threads to wait until a set of
    operations being performed in other threads completes.

    A CountDownLatch is initialized with a count. The await methods
    block until the count reaches zero due to invocations of the
    count_down method, after which all waiting threads are released.

    This is a one-shot phenomenon - the count cannot be reset.

    Example:
        >>> latch = client.get_count_down_latch("my-latch")
        >>> latch.try_set_count(3)
        >>> # In worker threads:
        >>> latch.count_down()
        >>> # In waiting thread:
        >>> latch.await(timeout=60)
    """

    def __init__(
        self,
        service_name: str,
        name: str,
        context: Optional[ProxyContext] = None,
    ):
        super().__init__(service_name, name, context)
        self._group_id = self._parse_group_id(name)

    def _parse_group_id(self, name: str) -> str:
        """Parse CP group ID from name."""
        if "@" in name:
            return name.split("@")[1]
        return "default"

    def _get_object_name(self) -> str:
        """Get the object name without group suffix."""
        if "@" in self._name:
            return self._name.split("@")[0]
        return self._name

    def try_set_count(self, count: int) -> bool:
        """Set the count if it hasn't been set yet.

        This operation can only be performed once. Subsequent calls
        will return False.

        Args:
            count: The count value. Must be positive.

        Returns:
            True if the count was set, False if already set.
        """
        return self.try_set_count_async(count).result()

    def try_set_count_async(self, count: int) -> Future:
        """Set the count asynchronously.

        Args:
            count: The count value. Must be positive.

        Returns:
            A Future that will contain True if the count was set.
        """
        if count < 1:
            future: Future = Future()
            future.set_exception(
                IllegalStateException("Count must be positive")
            )
            return future

        request = CountDownLatchCodec.encode_try_set_count_request(
            self._group_id, self._get_object_name(), count
        )
        return self._invoke(request, CountDownLatchCodec.decode_try_set_count_response)

    def get_count(self) -> int:
        """Get the current count.

        Returns:
            The current count value.
        """
        return self.get_count_async().result()

    def get_count_async(self) -> Future:
        """Get the current count asynchronously.

        Returns:
            A Future that will contain the current count.
        """
        request = CountDownLatchCodec.encode_get_count_request(
            self._group_id, self._get_object_name()
        )
        return self._invoke(request, CountDownLatchCodec.decode_get_count_response)

    def count_down(self) -> None:
        """Decrement the count by one.

        If the count reaches zero, all waiting threads are released.
        """
        self.count_down_async().result()

    def count_down_async(self) -> Future:
        """Decrement the count asynchronously.

        Returns:
            A Future that completes when the operation is done.
        """
        invocation_uid = int(time.time() * 1000000) % (2**63)
        request = CountDownLatchCodec.encode_count_down_request(
            self._group_id, self._get_object_name(), invocation_uid, 1
        )
        return self._invoke(request)

    def await_(self, timeout: float = -1) -> bool:
        """Wait for the count to reach zero.

        Args:
            timeout: Maximum time to wait in seconds.
                     -1 means wait indefinitely.

        Returns:
            True if the count reached zero, False if timed out.
        """
        return self.await_async(timeout).result()

    # Alias for Python keyword conflict
    wait = await_

    def await_async(self, timeout: float = -1) -> Future:
        """Wait for the count to reach zero asynchronously.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            A Future that will contain True if count reached zero.
        """
        invocation_uid = int(time.time() * 1000000) % (2**63)
        timeout_ms = int(timeout * 1000) if timeout >= 0 else -1

        request = CountDownLatchCodec.encode_await_request(
            self._group_id, self._get_object_name(), invocation_uid, timeout_ms
        )
        return self._invoke(request, CountDownLatchCodec.decode_await_response)

    def get_round(self) -> int:
        """Get the current round number.

        The round is incremented each time the latch is reset after
        reaching zero.

        Returns:
            The current round number.
        """
        return self.get_round_async().result()

    def get_round_async(self) -> Future:
        """Get the round number asynchronously.

        Returns:
            A Future that will contain the round number.
        """
        request = CountDownLatchCodec.encode_get_round_request(
            self._group_id, self._get_object_name()
        )
        return self._invoke(request, CountDownLatchCodec.decode_get_round_response)
