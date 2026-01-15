"""CP Semaphore distributed data structure."""

from concurrent.futures import Future
from typing import Optional, TYPE_CHECKING
import threading

from hazelcast.cp.base import CPProxy, CPGroupId

if TYPE_CHECKING:
    from hazelcast.invocation import InvocationService


class Semaphore(CPProxy):
    """A distributed semaphore backed by CP subsystem.

    Provides linearizable counting semaphore operations
    with support for acquiring and releasing permits.
    """

    SERVICE_NAME = "hz:raft:semaphoreService"

    def __init__(
        self,
        name: str,
        group_id: Optional[CPGroupId] = None,
        invocation_service: Optional["InvocationService"] = None,
        direct_to_leader: bool = True,
        initial_permits: int = 0,
    ):
        super().__init__(
            self.SERVICE_NAME,
            name,
            group_id or CPGroupId(CPGroupId.DEFAULT_GROUP_NAME),
            invocation_service,
            direct_to_leader,
        )
        self._permits = initial_permits
        self._semaphore = threading.Semaphore(max(0, initial_permits))
        self._lock = threading.Lock()

    def init(self, permits: int) -> bool:
        """Initialize the semaphore with a number of permits.

        Args:
            permits: The initial number of permits.

        Returns:
            True if initialized, False if already initialized.
        """
        return self.init_async(permits).result()

    def init_async(self, permits: int) -> Future:
        """Initialize the semaphore asynchronously.

        Args:
            permits: The initial number of permits.

        Returns:
            Future containing True if initialized.
        """
        self._check_not_destroyed()
        future: Future = Future()

        with self._lock:
            if self._permits == 0:
                self._permits = permits
                self._semaphore = threading.Semaphore(max(0, permits))
                future.set_result(True)
            else:
                future.set_result(False)

        return future

    def acquire(self, permits: int = 1) -> None:
        """Acquire permits from the semaphore.

        Blocks until the permits are available.

        Args:
            permits: Number of permits to acquire.
        """
        self.acquire_async(permits).result()

    def acquire_async(self, permits: int = 1) -> Future:
        """Acquire permits asynchronously.

        Args:
            permits: Number of permits to acquire.

        Returns:
            Future that completes when permits are acquired.
        """
        self._check_not_destroyed()
        future: Future = Future()

        for _ in range(permits):
            self._semaphore.acquire()

        with self._lock:
            self._permits -= permits

        future.set_result(None)
        return future

    def try_acquire(self, permits: int = 1, timeout: float = 0) -> bool:
        """Try to acquire permits.

        Args:
            permits: Number of permits to acquire.
            timeout: Maximum time to wait in seconds.

        Returns:
            True if permits were acquired, False otherwise.
        """
        return self.try_acquire_async(permits, timeout).result()

    def try_acquire_async(self, permits: int = 1, timeout: float = 0) -> Future:
        """Try to acquire permits asynchronously.

        Args:
            permits: Number of permits to acquire.
            timeout: Maximum time to wait in seconds.

        Returns:
            Future containing True if acquired.
        """
        self._check_not_destroyed()
        future: Future = Future()

        acquired_count = 0
        try:
            for _ in range(permits):
                if self._semaphore.acquire(blocking=True, timeout=max(0, timeout)):
                    acquired_count += 1
                else:
                    break

            if acquired_count == permits:
                with self._lock:
                    self._permits -= permits
                future.set_result(True)
            else:
                for _ in range(acquired_count):
                    self._semaphore.release()
                future.set_result(False)
        except Exception:
            for _ in range(acquired_count):
                self._semaphore.release()
            future.set_result(False)

        return future

    def release(self, permits: int = 1) -> None:
        """Release permits to the semaphore.

        Args:
            permits: Number of permits to release.
        """
        self.release_async(permits).result()

    def release_async(self, permits: int = 1) -> Future:
        """Release permits asynchronously.

        Args:
            permits: Number of permits to release.

        Returns:
            Future that completes when permits are released.
        """
        self._check_not_destroyed()
        future: Future = Future()

        with self._lock:
            self._permits += permits

        for _ in range(permits):
            self._semaphore.release()

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
            Future containing the number of available permits.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(max(0, self._permits))
        return future

    def drain_permits(self) -> int:
        """Acquire all available permits.

        Returns:
            The number of permits acquired.
        """
        return self.drain_permits_async().result()

    def drain_permits_async(self) -> Future:
        """Drain permits asynchronously.

        Returns:
            Future containing the number of permits drained.
        """
        self._check_not_destroyed()
        future: Future = Future()

        with self._lock:
            drained = max(0, self._permits)
            for _ in range(drained):
                acquired = self._semaphore.acquire(blocking=False)
                if not acquired:
                    break
            self._permits = 0

        future.set_result(drained)
        return future

    def reduce_permits(self, reduction: int) -> None:
        """Reduce the number of available permits.

        Args:
            reduction: Number of permits to reduce.
        """
        self.reduce_permits_async(reduction).result()

    def reduce_permits_async(self, reduction: int) -> Future:
        """Reduce permits asynchronously.

        Args:
            reduction: Number of permits to reduce.

        Returns:
            Future that completes when permits are reduced.
        """
        self._check_not_destroyed()
        future: Future = Future()

        with self._lock:
            self._permits -= reduction

        future.set_result(None)
        return future

    def increase_permits(self, increase: int) -> None:
        """Increase the number of available permits.

        Args:
            increase: Number of permits to add.
        """
        self.increase_permits_async(increase).result()

    def increase_permits_async(self, increase: int) -> Future:
        """Increase permits asynchronously.

        Args:
            increase: Number of permits to add.

        Returns:
            Future that completes when permits are increased.
        """
        self._check_not_destroyed()
        future: Future = Future()

        with self._lock:
            self._permits += increase

        for _ in range(increase):
            self._semaphore.release()

        future.set_result(None)
        return future
