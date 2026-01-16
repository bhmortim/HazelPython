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

    Attributes:
        name: The name of this FencedLock instance.

    Note:
        Requires the CP Subsystem to be enabled on the Hazelcast cluster
        with at least 3 members for fault tolerance.

    Example:
        Basic lock usage::

            lock = client.get_fenced_lock("my-lock")
            fence = lock.lock()
            try:
                # Critical section
                print(f"Lock acquired with fence: {fence}")
            finally:
                lock.unlock()

        Context manager usage (recommended)::

            with lock as fence:
                # Critical section - auto-releases on exit
                pass

        Using fencing token for zombie detection::

            fence = lock.lock()
            # Pass fence to external systems
            external_service.write(data, fence_token=fence)
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

        Blocks the current thread until the lock becomes available. If the
        lock is already held by the current thread, the lock count is
        incremented (reentrant behavior).

        Returns:
            int: The fencing token for this lock acquisition. The token is
                monotonically increasing and unique per acquisition.

        Raises:
            IllegalStateException: If the lock is destroyed.
            HazelcastException: If the operation fails.

        Example:
            >>> fence = lock.lock()
            >>> try:
            ...     # Critical section
            ...     process_with_fence(fence)
            ... finally:
            ...     lock.unlock()
        """
        return self.lock_async().result()

    def lock_async(self) -> Future:
        """Acquire the lock asynchronously.

        Returns:
            Future: A Future that will contain the fencing token as int.

        Example:
            >>> future = lock.lock_async()
            >>> fence = future.result()
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

        Attempts to acquire the lock, waiting up to the specified timeout
        if the lock is not immediately available.

        Args:
            timeout: Maximum time to wait in seconds. 0 means no waiting
                (returns immediately).

        Returns:
            int: The fencing token if acquired, INVALID_FENCE (0) if the lock
                could not be acquired within the timeout.

        Raises:
            HazelcastException: If the operation fails.

        Example:
            >>> fence = lock.try_lock(timeout=5.0)
            >>> if fence != INVALID_FENCE:
            ...     try:
            ...         # Critical section
            ...         pass
            ...     finally:
            ...         lock.unlock()
            ... else:
            ...     print("Could not acquire lock")
        """
        return self.try_lock_async(timeout).result()

    def try_lock_async(self, timeout: float = 0) -> Future:
        """Try to acquire the lock asynchronously.

        Args:
            timeout: Maximum time to wait in seconds. 0 means no waiting.

        Returns:
            Future: A Future that will contain the fencing token or INVALID_FENCE (0).

        Example:
            >>> future = lock.try_lock_async(timeout=5.0)
            >>> fence = future.result()
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

        Decrements the lock count. If the count reaches zero, the lock is
        released and other threads can acquire it.

        Raises:
            IllegalStateException: If the current thread does not hold the lock.
            HazelcastException: If the operation fails.

        Example:
            >>> lock.lock()
            >>> try:
            ...     # Critical section
            ...     pass
            ... finally:
            ...     lock.unlock()
        """
        self.unlock_async().result()

    def unlock_async(self) -> Future:
        """Release the lock asynchronously.

        Returns:
            Future: A Future that completes when the lock is released.

        Raises:
            IllegalStateException: If the current thread does not hold the lock.

        Example:
            >>> future = lock.unlock_async()
            >>> future.result()
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
            bool: True if the lock is held by any thread, False otherwise.

        Raises:
            HazelcastException: If the operation fails.

        Example:
            >>> if lock.is_locked():
            ...     print("Lock is currently held")
        """
        return self.is_locked_async().result()

    def is_locked_async(self) -> Future:
        """Check if the lock is currently held asynchronously.

        Returns:
            Future: A Future that will contain True if the lock is held.

        Example:
            >>> future = lock.is_locked_async()
            >>> is_held = future.result()
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

        This is a local check that does not involve network communication.

        Returns:
            bool: True if the current thread holds the lock.

        Example:
            >>> if lock.is_locked_by_current_thread():
            ...     lock.unlock()
        """
        with self._lock:
            return (
                self._thread_id == self._get_thread_id()
                and self._lock_count > 0
            )

    def get_lock_count(self) -> int:
        """Get the number of times the lock has been acquired.

        For reentrant locks, this returns the number of times the lock
        has been acquired without being released.

        Returns:
            int: The reentrant lock count, or 0 if not locked.

        Raises:
            HazelcastException: If the operation fails.

        Example:
            >>> count = lock.get_lock_count()
            >>> print(f"Lock held {count} times")
        """
        return self.get_lock_count_async().result()

    def get_lock_count_async(self) -> Future:
        """Get the reentrant lock count asynchronously.

        Returns:
            Future: A Future that will contain the lock count as int.

        Example:
            >>> future = lock.get_lock_count_async()
            >>> count = future.result()
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

        Returns the fencing token from the most recent lock acquisition
        by the current thread. This is a local operation.

        Returns:
            int: The fencing token, or INVALID_FENCE (0) if not locked.

        Example:
            >>> fence = lock.get_fence()
            >>> if fence != INVALID_FENCE:
            ...     use_fence(fence)
        """
        with self._lock:
            return self._fence

    def __enter__(self) -> int:
        """Context manager entry - acquires the lock.

        Returns:
            int: The fencing token.

        Example:
            >>> with lock as fence:
            ...     # Critical section
            ...     print(f"Fence: {fence}")
        """
        return self.lock()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - releases the lock.

        The lock is released even if an exception occurred in the with block.
        """
        self.unlock()


class Semaphore(Proxy):
    """A distributed counting semaphore with strong consistency.

    Semaphore maintains a set of permits that can be acquired and released.
    Acquire operations block when no permits are available.

    The semaphore is initialized with a number of permits. The permit
    count can be dynamically increased or decreased.

    Attributes:
        name: The name of this Semaphore instance.

    Note:
        Requires the CP Subsystem to be enabled on the Hazelcast cluster
        with at least 3 members for fault tolerance.

    Example:
        Basic semaphore usage::

            sem = client.get_semaphore("my-semaphore")
            sem.init(5)  # Initialize with 5 permits
            sem.acquire()  # Acquire 1 permit
            sem.acquire(2)  # Acquire 2 permits
            print(sem.available_permits())  # 2
            sem.release(3)  # Release 3 permits

        Rate limiting::

            rate_limiter = client.get_semaphore("api-rate-limit")
            rate_limiter.init(100)  # 100 requests per interval
            if rate_limiter.try_acquire():
                try:
                    process_request()
                finally:
                    rate_limiter.release()
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
        Subsequent calls after successful initialization return False.

        Args:
            permits: The initial number of permits. Must be non-negative.

        Returns:
            bool: True if initialization was successful, False if already initialized.

        Raises:
            IllegalStateException: If permits is negative.
            HazelcastException: If the operation fails.

        Example:
            >>> if sem.init(10):
            ...     print("Semaphore initialized with 10 permits")
            ... else:
            ...     print("Semaphore was already initialized")
        """
        return self.init_async(permits).result()

    def init_async(self, permits: int) -> Future:
        """Initialize the semaphore asynchronously.

        Args:
            permits: The initial number of permits. Must be non-negative.

        Returns:
            Future: A Future that will contain True if initialization was successful.

        Raises:
            IllegalStateException: If permits is negative.

        Example:
            >>> future = sem.init_async(10)
            >>> was_init = future.result()
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

        Blocks until the requested number of permits become available.

        Args:
            permits: The number of permits to acquire. Default is 1.
                Must be positive.

        Raises:
            IllegalStateException: If permits is less than 1.
            HazelcastException: If the operation fails.

        Example:
            >>> sem.acquire()  # Acquire 1 permit
            >>> sem.acquire(3)  # Acquire 3 permits
        """
        self.acquire_async(permits).result()

    def acquire_async(self, permits: int = 1) -> Future:
        """Acquire permits asynchronously.

        Args:
            permits: The number of permits to acquire. Must be positive.

        Returns:
            Future: A Future that completes when permits are acquired.

        Raises:
            IllegalStateException: If permits is less than 1.

        Example:
            >>> future = sem.acquire_async(2)
            >>> future.result()  # Wait for acquisition
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

        Attempts to acquire the specified number of permits, waiting up to
        the timeout if permits are not immediately available.

        Args:
            permits: The number of permits to acquire. Default is 1.
                Must be positive.
            timeout: Maximum time to wait in seconds. 0 means no waiting
                (returns immediately if permits not available).

        Returns:
            bool: True if permits were acquired, False if timeout expired.

        Raises:
            IllegalStateException: If permits is less than 1.
            HazelcastException: If the operation fails.

        Example:
            >>> if sem.try_acquire(2, timeout=5.0):
            ...     try:
            ...         # Use resources
            ...         pass
            ...     finally:
            ...         sem.release(2)
            ... else:
            ...     print("Could not acquire permits")
        """
        return self.try_acquire_async(permits, timeout).result()

    def try_acquire_async(self, permits: int = 1, timeout: float = 0) -> Future:
        """Try to acquire permits asynchronously.

        Args:
            permits: The number of permits to acquire. Must be positive.
            timeout: Maximum time to wait in seconds. 0 means no waiting.

        Returns:
            Future: A Future that will contain True if permits were acquired.

        Raises:
            IllegalStateException: If permits is less than 1.

        Example:
            >>> future = sem.try_acquire_async(2, timeout=5.0)
            >>> success = future.result()
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

        Releases the specified number of permits, making them available
        to other threads waiting to acquire.

        Args:
            permits: The number of permits to release. Default is 1.
                Must be positive.

        Raises:
            IllegalStateException: If permits is less than 1.
            HazelcastException: If the operation fails.

        Example:
            >>> sem.release()  # Release 1 permit
            >>> sem.release(3)  # Release 3 permits
        """
        self.release_async(permits).result()

    def release_async(self, permits: int = 1) -> Future:
        """Release permits asynchronously.

        Args:
            permits: The number of permits to release. Must be positive.

        Returns:
            Future: A Future that completes when permits are released.

        Raises:
            IllegalStateException: If permits is less than 1.

        Example:
            >>> future = sem.release_async(2)
            >>> future.result()
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
            int: The number of permits currently available.

        Raises:
            HazelcastException: If the operation fails.

        Example:
            >>> available = sem.available_permits()
            >>> print(f"{available} permits available")
        """
        return self.available_permits_async().result()

    def available_permits_async(self) -> Future:
        """Get available permits asynchronously.

        Returns:
            Future: A Future that will contain the available permit count as int.

        Example:
            >>> future = sem.available_permits_async()
            >>> count = future.result()
        """
        request = SemaphoreCodec.encode_available_permits_request(
            self._group_id, self._get_object_name()
        )
        return self._invoke(request, SemaphoreCodec.decode_available_permits_response)

    def drain_permits(self) -> int:
        """Acquire and return all immediately available permits.

        Acquires all permits that are immediately available without blocking.
        This is useful for resetting the semaphore.

        Returns:
            int: The number of permits acquired (could be 0).

        Raises:
            HazelcastException: If the operation fails.

        Example:
            >>> drained = sem.drain_permits()
            >>> print(f"Drained {drained} permits")
        """
        return self.drain_permits_async().result()

    def drain_permits_async(self) -> Future:
        """Drain all available permits asynchronously.

        Returns:
            Future: A Future that will contain the number of permits acquired.

        Example:
            >>> future = sem.drain_permits_async()
            >>> count = future.result()
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

        Reduces the available permits by the specified amount. Can make
        the permit count negative.

        Args:
            reduction: The number of permits to reduce by. Must be non-negative.

        Raises:
            IllegalStateException: If reduction is negative.
            HazelcastException: If the operation fails.

        Example:
            >>> sem.reduce_permits(5)  # Reduce by 5
        """
        self.reduce_permits_async(reduction).result()

    def reduce_permits_async(self, reduction: int) -> Future:
        """Reduce permits asynchronously.

        Args:
            reduction: The number of permits to reduce by. Must be non-negative.

        Returns:
            Future: A Future that completes when the operation is done.

        Raises:
            IllegalStateException: If reduction is negative.

        Example:
            >>> future = sem.reduce_permits_async(5)
            >>> future.result()
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

        Adds the specified number of permits. This may release waiting threads.

        Args:
            increase: The number of permits to add. Must be non-negative.

        Raises:
            IllegalStateException: If increase is negative.
            HazelcastException: If the operation fails.

        Example:
            >>> sem.increase_permits(10)  # Add 10 permits
        """
        self.increase_permits_async(increase).result()

    def increase_permits_async(self, increase: int) -> Future:
        """Increase permits asynchronously.

        Args:
            increase: The number of permits to add. Must be non-negative.

        Returns:
            Future: A Future that completes when the operation is done.

        Raises:
            IllegalStateException: If increase is negative.

        Example:
            >>> future = sem.increase_permits_async(10)
            >>> future.result()
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

    This is a one-shot phenomenon - the count cannot be reset after
    reaching zero without creating a new latch.

    Attributes:
        name: The name of this CountDownLatch instance.

    Note:
        Requires the CP Subsystem to be enabled on the Hazelcast cluster
        with at least 3 members for fault tolerance.

    Example:
        Coordinating worker completion::

            latch = client.get_count_down_latch("batch-complete")
            latch.try_set_count(3)  # Wait for 3 workers

            # In worker threads:
            # ... do work ...
            latch.count_down()

            # In coordinator thread:
            if latch.await_(timeout=60):
                print("All workers completed")
            else:
                print("Timeout waiting for workers")
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

        This operation can only be performed once per latch lifecycle.
        Subsequent calls will return False until the latch counts down
        to zero and is reset.

        Args:
            count: The count value. Must be positive.

        Returns:
            bool: True if the count was set, False if already set.

        Raises:
            IllegalStateException: If count is less than 1.
            HazelcastException: If the operation fails.

        Example:
            >>> if latch.try_set_count(5):
            ...     print("Latch initialized to count 5")
            ... else:
            ...     print("Latch already initialized")
        """
        return self.try_set_count_async(count).result()

    def try_set_count_async(self, count: int) -> Future:
        """Set the count asynchronously.

        Args:
            count: The count value. Must be positive.

        Returns:
            Future: A Future that will contain True if the count was set.

        Raises:
            IllegalStateException: If count is less than 1.

        Example:
            >>> future = latch.try_set_count_async(5)
            >>> was_set = future.result()
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
            int: The current count value, or 0 if the latch has been released.

        Raises:
            HazelcastException: If the operation fails.

        Example:
            >>> remaining = latch.get_count()
            >>> print(f"{remaining} more count_down() calls needed")
        """
        return self.get_count_async().result()

    def get_count_async(self) -> Future:
        """Get the current count asynchronously.

        Returns:
            Future: A Future that will contain the current count as int.

        Example:
            >>> future = latch.get_count_async()
            >>> count = future.result()
        """
        request = CountDownLatchCodec.encode_get_count_request(
            self._group_id, self._get_object_name()
        )
        return self._invoke(request, CountDownLatchCodec.decode_get_count_response)

    def count_down(self) -> None:
        """Decrement the count by one.

        If the count reaches zero, all waiting threads are released.
        If the count is already zero, this operation has no effect.

        Raises:
            HazelcastException: If the operation fails.

        Example:
            >>> # Worker signals completion
            >>> latch.count_down()
        """
        self.count_down_async().result()

    def count_down_async(self) -> Future:
        """Decrement the count asynchronously.

        Returns:
            Future: A Future that completes when the operation is done.

        Example:
            >>> future = latch.count_down_async()
            >>> future.result()
        """
        invocation_uid = int(time.time() * 1000000) % (2**63)
        request = CountDownLatchCodec.encode_count_down_request(
            self._group_id, self._get_object_name(), invocation_uid, 1
        )
        return self._invoke(request)

    def await_(self, timeout: float = -1) -> bool:
        """Wait for the count to reach zero.

        Blocks until the count reaches zero or the timeout expires.

        Args:
            timeout: Maximum time to wait in seconds.
                -1 means wait indefinitely.

        Returns:
            bool: True if the count reached zero, False if timed out.

        Raises:
            HazelcastException: If the operation fails.

        Example:
            >>> # Wait up to 60 seconds for all workers
            >>> if latch.await_(timeout=60):
            ...     print("All workers done")
            ... else:
            ...     print("Timeout!")

        Note:
            The method is named ``await_`` with a trailing underscore because
            ``await`` is a reserved keyword in Python. The ``wait`` alias is
            also available.
        """
        return self.await_async(timeout).result()

    wait = await_

    def await_async(self, timeout: float = -1) -> Future:
        """Wait for the count to reach zero asynchronously.

        Args:
            timeout: Maximum time to wait in seconds. -1 means wait indefinitely.

        Returns:
            Future: A Future that will contain True if count reached zero,
                False if timeout expired.

        Example:
            >>> future = latch.await_async(timeout=60)
            >>> completed = future.result()
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
        reaching zero. This can be used to detect if the latch has
        been reused.

        Returns:
            int: The current round number (starts at 0).

        Raises:
            HazelcastException: If the operation fails.

        Example:
            >>> round_num = latch.get_round()
            >>> print(f"Latch is in round {round_num}")
        """
        return self.get_round_async().result()

    def get_round_async(self) -> Future:
        """Get the round number asynchronously.

        Returns:
            Future: A Future that will contain the round number as int.

        Example:
            >>> future = latch.get_round_async()
            >>> round_num = future.result()
        """
        request = CountDownLatchCodec.encode_get_round_request(
            self._group_id, self._get_object_name()
        )
        return self._invoke(request, CountDownLatchCodec.decode_get_round_response)
