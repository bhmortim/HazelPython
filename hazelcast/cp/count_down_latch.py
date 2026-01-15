"""CP CountDownLatch distributed data structure."""

from concurrent.futures import Future
from typing import Optional, TYPE_CHECKING
import threading
import time

from hazelcast.cp.base import CPProxy, CPGroupId

if TYPE_CHECKING:
    from hazelcast.invocation import InvocationService


class CountDownLatch(CPProxy):
    """A distributed countdown latch backed by CP subsystem.

    Allows one or more threads to wait until a set of operations
    completes. The latch can be initialized with a count and then
    decremented until it reaches zero.
    """

    SERVICE_NAME = "hz:raft:countDownLatchService"

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
        self._count: int = 0
        self._condition = threading.Condition()

    def try_set_count(self, count: int) -> bool:
        """Try to set the initial count.

        The count can only be set if the current count is zero.

        Args:
            count: The initial count value.

        Returns:
            True if the count was set, False otherwise.
        """
        return self.try_set_count_async(count).result()

    def try_set_count_async(self, count: int) -> Future:
        """Try to set the count asynchronously.

        Args:
            count: The initial count value.

        Returns:
            Future containing True if set, False otherwise.
        """
        self._check_not_destroyed()
        future: Future = Future()

        with self._condition:
            if self._count == 0 and count > 0:
                self._count = count
                future.set_result(True)
            else:
                future.set_result(False)

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
            Future containing the current count.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(self._count)
        return future

    def count_down(self) -> None:
        """Decrement the count.

        If the count reaches zero, all waiting threads are released.
        """
        self.count_down_async().result()

    def count_down_async(self) -> Future:
        """Decrement the count asynchronously.

        Returns:
            Future that completes when the count is decremented.
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

    def await_(self, timeout: float = -1) -> bool:
        """Wait until the count reaches zero.

        Args:
            timeout: Maximum time to wait in seconds. -1 for infinite.

        Returns:
            True if the count reached zero, False if timeout expired.
        """
        return self.await_async(timeout).result()

    def await_async(self, timeout: float = -1) -> Future:
        """Wait asynchronously until the count reaches zero.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            Future containing True if count reached zero.
        """
        self._check_not_destroyed()
        future: Future = Future()

        with self._condition:
            if self._count == 0:
                future.set_result(True)
                return future

            if timeout < 0:
                while self._count > 0:
                    self._condition.wait()
                future.set_result(True)
            else:
                start = time.time()
                remaining = timeout
                while self._count > 0 and remaining > 0:
                    self._condition.wait(timeout=remaining)
                    remaining = timeout - (time.time() - start)
                future.set_result(self._count == 0)

        return future

    def get_round(self) -> int:
        """Get the current round number.

        The round number is incremented each time the latch is reset.

        Returns:
            The current round number.
        """
        return self.get_round_async().result()

    def get_round_async(self) -> Future:
        """Get the current round asynchronously.

        Returns:
            Future containing the round number.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(0)
        return future
