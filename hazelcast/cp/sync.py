"""CP Subsystem Synchronization primitives."""

import threading
import time
from concurrent.futures import Future
from typing import Optional

from hazelcast.proxy.base import Proxy, ProxyContext
from hazelcast.exceptions import IllegalArgumentException, IllegalStateException


class CountDownLatch(Proxy):
    """CP Subsystem CountDownLatch.

    A linearizable, distributed countdown latch for synchronization.
    Uses the Raft consensus algorithm for strong consistency guarantees.
    """

    SERVICE_NAME = "hz:raft:countDownLatchService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)
        self._count: int = 0
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def await_(self) -> bool:
        """Wait for the count to reach zero.

        Blocks until the count reaches zero.

        Returns:
            True when the count reaches zero.
        """
        return self.await_async().result()

    def await_async(self) -> Future:
        """Wait for the count to reach zero asynchronously.

        Returns:
            A Future that completes when the count reaches zero.
        """
        self._check_not_destroyed()
        future: Future = Future()
        with self._condition:
            while self._count > 0:
                self._condition.wait()
        future.set_result(True)
        return future

    def await_with_timeout(self, timeout: float) -> bool:
        """Wait for the count to reach zero with a timeout.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            True if the count reached zero, False if timeout expired.
        """
        return self.await_with_timeout_async(timeout).result()

    def await_with_timeout_async(self, timeout: float) -> Future:
        """Wait for the count to reach zero with a timeout asynchronously.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            A Future that will contain True if count reached zero.
        """
        self._check_not_destroyed()
        if timeout < 0:
            raise IllegalArgumentException("timeout cannot be negative")
        future: Future = Future()
        with self._condition:
            if self._count == 0:
                future.set_result(True)
                return future
            end_time = time.monotonic() + timeout
            while self._count > 0:
                remaining = end_time - time.monotonic()
                if remaining <= 0:
                    future.set_result(False)
                    return future
                self._condition.wait(remaining)
        future.set_result(True)
        return future

    def count_down(self) -> None:
        """Decrement the count by one.

        If the count reaches zero, all waiting threads are released.
        """
        self.count_down_async().result()

    def count_down_async(self) -> Future:
        """Decrement the count by one asynchronously.

        Returns:
            A Future that completes when the operation is done.
        """
        self._check_not_destroyed()
        future: Future = Future()
        with self._condition:
            if self._count > 0:
                self._count -= 1
                if self._count == 0:
                    self._condition.notify_all()
        future.set_result(None)
        return future

    def get_count(self) -> int:
        """Get the current count.

        Returns:
            The current count.
        """
        return self.get_count_async().result()

    def get_count_async(self) -> Future:
        """Get the current count asynchronously.

        Returns:
            A Future that will contain the current count.
        """
        self._check_not_destroyed()
        future: Future = Future()
        with self._lock:
            future.set_result(self._count)
        return future

    def try_set_count(self, count: int) -> bool:
        """Try to set the count.

        The count can only be set if the current count is zero.

        Args:
            count: The new count value.

        Returns:
            True if the count was set, False otherwise.
        """
        return self.try_set_count_async(count).result()

    def try_set_count_async(self, count: int) -> Future:
        """Try to set the count asynchronously.

        Args:
            count: The new count value.

        Returns:
            A Future that will contain True if the count was set.
        """
        self._check_not_destroyed()
        if count < 0:
            raise IllegalArgumentException("count cannot be negative")
        future: Future = Future()
        with self._lock:
            if self._count == 0:
                self._count = count
                future.set_result(True)
            else:
                future.set_result(False)
        return future


class Semaphore(Proxy):
    """CP Subsystem Semaphore.

    A linearizable, distributed semaphore for controlling access to resources.
    Uses the Raft consensus algorithm for strong consistency guarantees.
    """

    SERVICE_NAME = "hz:raft:semaphoreService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)
        self._permits: int = 0
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def init(self, permits: int) -> bool:
        """Initialize the semaphore with the given number of permits.

        This can only be done once when permits are zero.

        Args:
            permits: The initial number of permits.

        Returns:
            True if initialization was successful.
        """
        return self.init_async(permits).result()

    def init_async(self, permits: int) -> Future:
        """Initialize the semaphore asynchronously.

        Args:
            permits: The initial number of permits.

        Returns:
            A Future that will contain True if successful.
        """
        self._check_not_destroyed()
        if permits < 0:
            raise IllegalArgumentException("permits cannot be negative")
        future: Future = Future()
        with self._lock:
            if self._permits == 0:
                self._permits = permits
                future.set_result(True)
            else:
                future.set_result(False)
        return future

    def acquire(self, permits: int = 1) -> None:
        """Acquire the given number of permits.

        Blocks until permits are available.

        Args:
            permits: Number of permits to acquire.
        """
        self.acquire_async(permits).result()

    def acquire_async(self, permits: int = 1) -> Future:
        """Acquire permits asynchronously.

        Args:
            permits: Number of permits to acquire.

        Returns:
            A Future that completes when permits are acquired.
        """
        self._check_not_destroyed()
        if permits < 0:
            raise IllegalArgumentException("permits cannot be negative")
        future: Future = Future()
        with self._condition:
            while self._permits < permits:
                self._condition.wait()
            self._permits -= permits
        future.set_result(None)
        return future

    def try_acquire(self, permits: int = 1, timeout: Optional[float] = None) -> bool:
        """Try to acquire permits without blocking indefinitely.

        Args:
            permits: Number of permits to acquire.
            timeout: Optional timeout in seconds. If None, returns immediately.

        Returns:
            True if permits were acquired, False otherwise.
        """
        return self.try_acquire_async(permits, timeout).result()

    def try_acquire_async(
        self, permits: int = 1, timeout: Optional[float] = None
    ) -> Future:
        """Try to acquire permits asynchronously.

        Args:
            permits: Number of permits to acquire.
            timeout: Optional timeout in seconds.

        Returns:
            A Future that will contain True if permits were acquired.
        """
        self._check_not_destroyed()
        if permits < 0:
            raise IllegalArgumentException("permits cannot be negative")
        if timeout is not None and timeout < 0:
            raise IllegalArgumentException("timeout cannot be negative")
        future: Future = Future()
        with self._condition:
            if timeout is None:
                if self._permits >= permits:
                    self._permits -= permits
                    future.set_result(True)
                else:
                    future.set_result(False)
            else:
                end_time = time.monotonic() + timeout
                while self._permits < permits:
                    remaining = end_time - time.monotonic()
                    if remaining <= 0:
                        future.set_result(False)
                        return future
                    self._condition.wait(remaining)
                self._permits -= permits
                future.set_result(True)
        return future

    def release(self, permits: int = 1) -> None:
        """Release the given number of permits.

        Args:
            permits: Number of permits to release.
        """
        self.release_async(permits).result()

    def release_async(self, permits: int = 1) -> Future:
        """Release permits asynchronously.

        Args:
            permits: Number of permits to release.

        Returns:
            A Future that completes when the operation is done.
        """
        self._check_not_destroyed()
        if permits < 0:
            raise IllegalArgumentException("permits cannot be negative")
        future: Future = Future()
        with self._condition:
            self._permits += permits
            self._condition.notify_all()
        future.set_result(None)
        return future

    def available_permits(self) -> int:
        """Get the number of available permits.

        Returns:
            The number of available permits.
        """
        return self.available_permits_async().result()

    def available_permits_async(self) -> Future:
        """Get available permits asynchronously.

        Returns:
            A Future that will contain the available permits count.
        """
        self._check_not_destroyed()
        future: Future = Future()
        with self._lock:
            future.set_result(self._permits)
        return future

    def drain_permits(self) -> int:
        """Acquire all available permits.

        Returns:
            The number of permits acquired.
        """
        return self.drain_permits_async().result()

    def drain_permits_async(self) -> Future:
        """Drain all permits asynchronously.

        Returns:
            A Future that will contain the number of permits drained.
        """
        self._check_not_destroyed()
        future: Future = Future()
        with self._lock:
            drained = self._permits
            self._permits = 0
            future.set_result(drained)
        return future

    def reduce_permits(self, reduction: int) -> None:
        """Reduce the number of available permits.

        Args:
            reduction: The number of permits to reduce.
        """
        self.reduce_permits_async(reduction).result()

    def reduce_permits_async(self, reduction: int) -> Future:
        """Reduce permits asynchronously.

        Args:
            reduction: The number of permits to reduce.

        Returns:
            A Future that completes when the operation is done.
        """
        self._check_not_destroyed()
        if reduction < 0:
            raise IllegalArgumentException("reduction cannot be negative")
        future: Future = Future()
        with self._lock:
            self._permits -= reduction
        future.set_result(None)
        return future

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
        self._check_not_destroyed()
        if increase < 0:
            raise IllegalArgumentException("increase cannot be negative")
        future: Future = Future()
        with self._condition:
            self._permits += increase
            self._condition.notify_all()
        future.set_result(None)
        return future


class FencedLock(Proxy):
    """CP Subsystem FencedLock.

    A linearizable, distributed reentrant lock with fencing token support.
    Uses the Raft consensus algorithm for strong consistency guarantees.

    The fencing token is a monotonically increasing value that changes
    each time the lock is acquired, providing protection against
    stale lock holders in distributed systems.
    """

    SERVICE_NAME = "hz:raft:lockService"
    INVALID_FENCE = 0

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._owner_thread: Optional[int] = None
        self._lock_count: int = 0
        self._fence: int = 0
        self._next_fence: int = 1

    def lock(self) -> int:
        """Acquire the lock.

        If the lock is held by another thread, this method blocks until
        the lock becomes available. Supports reentrant locking.

        Returns:
            The fencing token for this lock acquisition.
        """
        return self.lock_async().result()

    def lock_async(self) -> Future:
        """Acquire the lock asynchronously.

        Returns:
            A Future that will contain the fencing token.
        """
        self._check_not_destroyed()
        future: Future = Future()
        current_thread = threading.current_thread().ident
        with self._condition:
            while self._owner_thread is not None and self._owner_thread != current_thread:
                self._condition.wait()
            if self._owner_thread is None:
                self._owner_thread = current_thread
                self._fence = self._next_fence
                self._next_fence += 1
            self._lock_count += 1
            future.set_result(self._fence)
        return future

    def try_lock(self, timeout: Optional[float] = None) -> int:
        """Try to acquire the lock.

        Args:
            timeout: Optional timeout in seconds. If None, returns immediately.

        Returns:
            The fencing token if lock was acquired, INVALID_FENCE (0) otherwise.
        """
        return self.try_lock_async(timeout).result()

    def try_lock_async(self, timeout: Optional[float] = None) -> Future:
        """Try to acquire the lock asynchronously.

        Args:
            timeout: Optional timeout in seconds.

        Returns:
            A Future that will contain the fencing token or INVALID_FENCE.
        """
        self._check_not_destroyed()
        if timeout is not None and timeout < 0:
            raise IllegalArgumentException("timeout cannot be negative")
        future: Future = Future()
        current_thread = threading.current_thread().ident
        with self._condition:
            if timeout is None:
                if self._owner_thread is None or self._owner_thread == current_thread:
                    if self._owner_thread is None:
                        self._owner_thread = current_thread
                        self._fence = self._next_fence
                        self._next_fence += 1
                    self._lock_count += 1
                    future.set_result(self._fence)
                else:
                    future.set_result(self.INVALID_FENCE)
            else:
                end_time = time.monotonic() + timeout
                while self._owner_thread is not None and self._owner_thread != current_thread:
                    remaining = end_time - time.monotonic()
                    if remaining <= 0:
                        future.set_result(self.INVALID_FENCE)
                        return future
                    self._condition.wait(remaining)
                if self._owner_thread is None:
                    self._owner_thread = current_thread
                    self._fence = self._next_fence
                    self._next_fence += 1
                self._lock_count += 1
                future.set_result(self._fence)
        return future

    def unlock(self) -> None:
        """Release the lock.

        Raises:
            IllegalStateException: If the current thread does not hold the lock.
        """
        self.unlock_async().result()

    def unlock_async(self) -> Future:
        """Release the lock asynchronously.

        Returns:
            A Future that completes when the operation is done.

        Raises:
            IllegalStateException: If the current thread does not hold the lock.
        """
        self._check_not_destroyed()
        future: Future = Future()
        current_thread = threading.current_thread().ident
        with self._condition:
            if self._owner_thread != current_thread:
                raise IllegalStateException(
                    "Current thread does not hold this lock"
                )
            self._lock_count -= 1
            if self._lock_count == 0:
                self._owner_thread = None
                self._fence = self.INVALID_FENCE
                self._condition.notify_all()
        future.set_result(None)
        return future

    def is_locked(self) -> bool:
        """Check if the lock is currently held by any thread.

        Returns:
            True if the lock is held.
        """
        return self.is_locked_async().result()

    def is_locked_async(self) -> Future:
        """Check if locked asynchronously.

        Returns:
            A Future that will contain True if the lock is held.
        """
        self._check_not_destroyed()
        future: Future = Future()
        with self._condition:
            future.set_result(self._owner_thread is not None)
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
            A Future that will contain True if current thread holds the lock.
        """
        self._check_not_destroyed()
        future: Future = Future()
        current_thread = threading.current_thread().ident
        with self._condition:
            future.set_result(self._owner_thread == current_thread)
        return future

    def get_lock_count(self) -> int:
        """Get the reentrant lock count.

        Returns:
            The number of times the lock has been acquired by the owner.
        """
        return self.get_lock_count_async().result()

    def get_lock_count_async(self) -> Future:
        """Get the lock count asynchronously.

        Returns:
            A Future that will contain the lock count.
        """
        self._check_not_destroyed()
        future: Future = Future()
        with self._condition:
            future.set_result(self._lock_count)
        return future

    def get_fence(self) -> int:
        """Get the current fencing token.

        Returns:
            The fencing token, or INVALID_FENCE if not locked.
        """
        return self.get_fence_async().result()

    def get_fence_async(self) -> Future:
        """Get the fencing token asynchronously.

        Returns:
            A Future that will contain the fencing token.
        """
        self._check_not_destroyed()
        future: Future = Future()
        with self._condition:
            future.set_result(self._fence)
        return future
