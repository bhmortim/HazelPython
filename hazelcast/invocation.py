"""Request/response invocation handling."""

import threading
import time
from typing import Dict, Optional
from concurrent.futures import Future

from hazelcast.protocol.client_message import ClientMessage
from hazelcast.exceptions import HazelcastException, OperationTimeoutException


class Invocation:
    """Represents a pending request awaiting a response."""

    def __init__(
        self,
        request: ClientMessage,
        partition_id: int = -1,
        timeout: float = 120.0,
    ):
        self._request = request
        self._partition_id = partition_id
        self._timeout = timeout
        self._future: Future = Future()
        self._sent_time: Optional[float] = None
        self._correlation_id: int = 0

    @property
    def request(self) -> ClientMessage:
        return self._request

    @property
    def partition_id(self) -> int:
        return self._partition_id

    @property
    def timeout(self) -> float:
        return self._timeout

    @property
    def future(self) -> Future:
        return self._future

    @property
    def correlation_id(self) -> int:
        return self._correlation_id

    @correlation_id.setter
    def correlation_id(self, value: int) -> None:
        self._correlation_id = value
        self._request.set_correlation_id(value)

    @property
    def sent_time(self) -> Optional[float]:
        return self._sent_time

    def mark_sent(self) -> None:
        self._sent_time = time.time()

    def is_expired(self) -> bool:
        if self._sent_time is None:
            return False
        return (time.time() - self._sent_time) > self._timeout

    def set_response(self, response: ClientMessage) -> None:
        if not self._future.done():
            self._future.set_result(response)

    def set_exception(self, exception: Exception) -> None:
        if not self._future.done():
            self._future.set_exception(exception)


class InvocationService:
    """Service for managing invocations and correlation IDs."""

    def __init__(self):
        self._pending: Dict[int, Invocation] = {}
        self._correlation_id_counter = 0
        self._lock = threading.Lock()
        self._running = False

    def start(self) -> None:
        self._running = True

    def shutdown(self) -> None:
        self._running = False
        with self._lock:
            for invocation in self._pending.values():
                invocation.set_exception(
                    HazelcastException("Client is shutting down")
                )
            self._pending.clear()

    @property
    def is_running(self) -> bool:
        return self._running

    def invoke(self, invocation: Invocation) -> Future:
        with self._lock:
            self._correlation_id_counter += 1
            correlation_id = self._correlation_id_counter
            invocation.correlation_id = correlation_id
            self._pending[correlation_id] = invocation

        return invocation.future

    def handle_response(self, response: ClientMessage) -> bool:
        correlation_id = response.get_correlation_id()

        with self._lock:
            invocation = self._pending.pop(correlation_id, None)

        if invocation is None:
            return False

        invocation.set_response(response)
        return True

    def get_pending_count(self) -> int:
        with self._lock:
            return len(self._pending)

    def check_timeouts(self) -> int:
        timed_out = []

        with self._lock:
            for cid, invocation in list(self._pending.items()):
                if invocation.is_expired():
                    timed_out.append((cid, invocation))

            for cid, _ in timed_out:
                del self._pending[cid]

        for _, invocation in timed_out:
            invocation.set_exception(
                OperationTimeoutException(
                    f"Operation timed out after {invocation.timeout}s"
                )
            )

        return len(timed_out)

    def remove_invocation(self, correlation_id: int) -> Optional[Invocation]:
        with self._lock:
            return self._pending.pop(correlation_id, None)
