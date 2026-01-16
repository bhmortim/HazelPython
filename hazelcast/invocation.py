"""Request/response invocation handling."""

import asyncio
import logging
import threading
import time
from typing import TYPE_CHECKING, Callable, Dict, Optional
from concurrent.futures import Future

from hazelcast.protocol.client_message import ClientMessage
from hazelcast.exceptions import (
    HazelcastException,
    OperationTimeoutException,
    TargetDisconnectedException,
    ClientOfflineException,
)
from hazelcast.logging import get_logger

if TYPE_CHECKING:
    from hazelcast.network.connection_manager import ConnectionManager
    from hazelcast.network.connection import Connection
    from hazelcast.config import ClientConfig

_logger = get_logger("invocation")


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

    def __init__(
        self,
        connection_manager: "ConnectionManager" = None,
        config: "ClientConfig" = None,
    ):
        self._connection_manager = connection_manager
        self._config = config
        self._pending: Dict[int, Invocation] = {}
        self._correlation_id_counter = 0
        self._lock = threading.Lock()
        self._running = False
        self._smart_routing = True
        self._retry_count = 3
        self._retry_pause = 1.0

        if config:
            self._smart_routing = getattr(config, 'smart_routing', True)
            retry_config = getattr(config, 'retry', None)
            if retry_config:
                self._retry_count = getattr(retry_config, 'max_attempts', 3)
                self._retry_pause = getattr(retry_config, 'initial_backoff', 1.0)

        if connection_manager:
            connection_manager.set_message_callback(self._handle_message)

    def start(self) -> None:
        self._running = True
        _logger.info("Invocation service started (smart_routing=%s)", self._smart_routing)

    def shutdown(self) -> None:
        self._running = False
        with self._lock:
            pending_count = len(self._pending)
            for invocation in self._pending.values():
                invocation.set_exception(
                    HazelcastException("Client is shutting down")
                )
            self._pending.clear()
        _logger.info("Invocation service shutdown, cancelled %d pending invocations", pending_count)

    @property
    def is_running(self) -> bool:
        return self._running

    def invoke(self, invocation: Invocation) -> Future:
        if not self._running:
            invocation.set_exception(ClientOfflineException("Invocation service not running"))
            return invocation.future

        with self._lock:
            self._correlation_id_counter += 1
            correlation_id = self._correlation_id_counter
            invocation.correlation_id = correlation_id
            self._pending[correlation_id] = invocation

        _logger.debug(
            "Invocation started: correlation_id=%d, partition_id=%d",
            correlation_id,
            invocation.partition_id,
        )

        self._send_invocation(invocation)
        return invocation.future

    def _send_invocation(self, invocation: Invocation, retry_count: int = 0) -> None:
        """Send an invocation to the cluster."""
        if not self._running:
            invocation.set_exception(ClientOfflineException("Client is shutting down"))
            return

        connection = self._get_connection_for_invocation(invocation)
        if connection is None:
            if retry_count < self._retry_count:
                _logger.debug(
                    "No connection available for correlation_id=%d, retrying (%d/%d)",
                    invocation.correlation_id,
                    retry_count + 1,
                    self._retry_count,
                )
                self._schedule_retry(invocation, retry_count + 1)
            else:
                self._fail_invocation(
                    invocation,
                    ClientOfflineException("No connection available after retries"),
                )
            return

        try:
            invocation.mark_sent()
            connection.send_sync(invocation.request)
            _logger.debug(
                "Invocation sent: correlation_id=%d via connection %d",
                invocation.correlation_id,
                connection.connection_id,
            )
        except TargetDisconnectedException as e:
            _logger.warning(
                "Failed to send invocation correlation_id=%d: %s",
                invocation.correlation_id,
                e,
            )
            if retry_count < self._retry_count:
                self._schedule_retry(invocation, retry_count + 1)
            else:
                self._fail_invocation(invocation, e)
        except Exception as e:
            _logger.error(
                "Unexpected error sending invocation correlation_id=%d: %s",
                invocation.correlation_id,
                e,
            )
            self._fail_invocation(invocation, HazelcastException(str(e)))

    def _get_connection_for_invocation(self, invocation: Invocation) -> Optional["Connection"]:
        """Get appropriate connection for the invocation."""
        if self._connection_manager is None:
            return None

        if self._smart_routing and invocation.partition_id >= 0:
            return self._connection_manager.get_connection(invocation.partition_id)

        return self._connection_manager.get_connection()

    def _schedule_retry(self, invocation: Invocation, retry_count: int) -> None:
        """Schedule a retry for the invocation."""
        backoff = self._retry_pause * (2 ** (retry_count - 1))

        def do_retry():
            time.sleep(backoff)
            if self._running and not invocation.future.done():
                self._send_invocation(invocation, retry_count)

        thread = threading.Thread(target=do_retry, daemon=True)
        thread.start()

    def _fail_invocation(self, invocation: Invocation, exception: Exception) -> None:
        """Fail an invocation and remove from pending."""
        with self._lock:
            self._pending.pop(invocation.correlation_id, None)
        invocation.set_exception(exception)

    def _handle_message(self, message: ClientMessage) -> None:
        """Handle an incoming message from a connection."""
        if message.is_event():
            _logger.debug("Received event message type=0x%06x", message.get_message_type())
            return

        self.handle_response(message)

    def handle_response(self, response: ClientMessage) -> bool:
        correlation_id = response.get_correlation_id()

        with self._lock:
            invocation = self._pending.pop(correlation_id, None)

        if invocation is None:
            _logger.warning("Received response for unknown correlation_id=%d", correlation_id)
            return False

        _logger.debug("Invocation completed: correlation_id=%d", correlation_id)
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

        for cid, invocation in timed_out:
            _logger.warning(
                "Invocation timed out: correlation_id=%d, timeout=%.1fs",
                cid,
                invocation.timeout,
            )
            invocation.set_exception(
                OperationTimeoutException(
                    f"Operation timed out after {invocation.timeout}s"
                )
            )

        return len(timed_out)

    def remove_invocation(self, correlation_id: int) -> Optional[Invocation]:
        with self._lock:
            return self._pending.pop(correlation_id, None)

    def invoke_on_connection(
        self,
        invocation: Invocation,
        connection: "Connection",
    ) -> Future:
        """Invoke on a specific connection."""
        if not self._running:
            invocation.set_exception(ClientOfflineException("Invocation service not running"))
            return invocation.future

        with self._lock:
            self._correlation_id_counter += 1
            correlation_id = self._correlation_id_counter
            invocation.correlation_id = correlation_id
            self._pending[correlation_id] = invocation

        try:
            invocation.mark_sent()
            connection.send_sync(invocation.request)
        except Exception as e:
            self._fail_invocation(invocation, TargetDisconnectedException(str(e)))

        return invocation.future
