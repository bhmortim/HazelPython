"""Unit tests for hazelcast/invocation.py."""

import threading
import time
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from unittest.mock import MagicMock, Mock, patch, PropertyMock

import pytest

from hazelcast.invocation import Invocation, InvocationService
from hazelcast.exceptions import (
    HazelcastException,
    OperationTimeoutException,
    TargetDisconnectedException,
    ClientOfflineException,
)


class TestInvocation:
    """Tests for the Invocation class."""

    def test_init_defaults(self):
        """Test Invocation initialization with default values."""
        request = MagicMock()
        inv = Invocation(request)

        assert inv.request is request
        assert inv.partition_id == -1
        assert inv.timeout == 120.0
        assert inv.correlation_id == 0
        assert inv.sent_time is None
        assert isinstance(inv.future, Future)
        assert not inv.future.done()

    def test_init_with_custom_values(self):
        """Test Invocation initialization with custom values."""
        request = MagicMock()
        inv = Invocation(request, partition_id=5, timeout=30.0)

        assert inv.partition_id == 5
        assert inv.timeout == 30.0

    def test_correlation_id_setter(self):
        """Test correlation_id setter updates request."""
        request = MagicMock()
        inv = Invocation(request)

        inv.correlation_id = 42

        assert inv.correlation_id == 42
        request.set_correlation_id.assert_called_once_with(42)

    def test_mark_sent(self):
        """Test mark_sent sets sent_time."""
        request = MagicMock()
        inv = Invocation(request)

        assert inv.sent_time is None
        before = time.time()
        inv.mark_sent()
        after = time.time()

        assert inv.sent_time is not None
        assert before <= inv.sent_time <= after

    def test_is_expired_when_not_sent(self):
        """Test is_expired returns False when not yet sent."""
        request = MagicMock()
        inv = Invocation(request, timeout=0.001)

        assert not inv.is_expired()

    def test_is_expired_when_not_expired(self):
        """Test is_expired returns False when within timeout."""
        request = MagicMock()
        inv = Invocation(request, timeout=60.0)
        inv.mark_sent()

        assert not inv.is_expired()

    def test_is_expired_when_expired(self):
        """Test is_expired returns True when timeout exceeded."""
        request = MagicMock()
        inv = Invocation(request, timeout=0.001)
        inv.mark_sent()

        time.sleep(0.01)

        assert inv.is_expired()

    def test_set_response(self):
        """Test set_response completes future with result."""
        request = MagicMock()
        inv = Invocation(request)
        response = MagicMock()

        inv.set_response(response)

        assert inv.future.done()
        assert inv.future.result() is response

    def test_set_response_when_already_done(self):
        """Test set_response does nothing when future already done."""
        request = MagicMock()
        inv = Invocation(request)
        first_response = MagicMock()
        second_response = MagicMock()

        inv.set_response(first_response)
        inv.set_response(second_response)

        assert inv.future.result() is first_response

    def test_set_exception(self):
        """Test set_exception completes future with exception."""
        request = MagicMock()
        inv = Invocation(request)
        error = HazelcastException("test error")

        inv.set_exception(error)

        assert inv.future.done()
        with pytest.raises(HazelcastException, match="test error"):
            inv.future.result()

    def test_set_exception_when_already_done(self):
        """Test set_exception does nothing when future already done."""
        request = MagicMock()
        inv = Invocation(request)
        response = MagicMock()

        inv.set_response(response)
        inv.set_exception(HazelcastException("ignored"))

        assert inv.future.result() is response


class TestInvocationService:
    """Tests for the InvocationService class."""

    def test_init_without_dependencies(self):
        """Test InvocationService initialization without dependencies."""
        service = InvocationService()

        assert service._connection_manager is None
        assert service._config is None
        assert service._smart_routing is True
        assert service._retry_count == 3
        assert service._retry_pause == 1.0
        assert not service.is_running

    def test_init_with_config(self):
        """Test InvocationService initialization with config."""
        config = MagicMock()
        config.smart_routing = False
        retry = MagicMock()
        retry.max_attempts = 5
        retry.initial_backoff = 2.0
        config.retry = retry

        service = InvocationService(config=config)

        assert service._smart_routing is False
        assert service._retry_count == 5
        assert service._retry_pause == 2.0

    def test_init_with_connection_manager(self):
        """Test InvocationService sets message callback."""
        conn_mgr = MagicMock()
        service = InvocationService(connection_manager=conn_mgr)

        conn_mgr.set_message_callback.assert_called_once_with(service._handle_message)

    def test_start(self):
        """Test start sets running state."""
        service = InvocationService()

        assert not service.is_running
        service.start()
        assert service.is_running

    def test_shutdown(self):
        """Test shutdown cancels pending invocations."""
        service = InvocationService()
        service.start()

        request = MagicMock()
        inv = Invocation(request)
        inv.correlation_id = 1
        service._pending[1] = inv

        service.shutdown()

        assert not service.is_running
        assert len(service._pending) == 0
        assert inv.future.done()
        with pytest.raises(HazelcastException, match="shutting down"):
            inv.future.result()

    def test_invoke_when_not_running(self):
        """Test invoke fails when service not running."""
        service = InvocationService()
        request = MagicMock()
        inv = Invocation(request)

        future = service.invoke(inv)

        assert future.done()
        with pytest.raises(ClientOfflineException, match="not running"):
            future.result()

    def test_invoke_assigns_correlation_id(self):
        """Test invoke assigns unique correlation IDs."""
        conn_mgr = MagicMock()
        conn_mgr.get_connection.return_value = None
        service = InvocationService(connection_manager=conn_mgr)
        service._retry_count = 0
        service.start()

        inv1 = Invocation(MagicMock())
        inv2 = Invocation(MagicMock())

        service.invoke(inv1)
        service.invoke(inv2)

        assert inv1.correlation_id == 1
        assert inv2.correlation_id == 2

    def test_invoke_adds_to_pending(self):
        """Test invoke adds invocation to pending map."""
        conn_mgr = MagicMock()
        connection = MagicMock()
        conn_mgr.get_connection.return_value = connection
        service = InvocationService(connection_manager=conn_mgr)
        service.start()

        request = MagicMock()
        inv = Invocation(request)

        service.invoke(inv)

        assert inv.correlation_id in service._pending

    def test_send_invocation_no_connection_retries(self):
        """Test _send_invocation retries when no connection available."""
        conn_mgr = MagicMock()
        conn_mgr.get_connection.return_value = None
        service = InvocationService(connection_manager=conn_mgr)
        service._retry_count = 2
        service._retry_pause = 0.001
        service.start()

        request = MagicMock()
        inv = Invocation(request)
        inv.correlation_id = 1
        service._pending[1] = inv

        service._send_invocation(inv)

        time.sleep(0.1)

        assert inv.future.done()
        with pytest.raises(ClientOfflineException, match="No connection"):
            inv.future.result()

    def test_send_invocation_success(self):
        """Test _send_invocation sends message via connection."""
        conn_mgr = MagicMock()
        connection = MagicMock()
        connection.connection_id = 123
        conn_mgr.get_connection.return_value = connection
        service = InvocationService(connection_manager=conn_mgr)
        service.start()

        request = MagicMock()
        inv = Invocation(request)
        inv.correlation_id = 1
        service._pending[1] = inv

        service._send_invocation(inv)

        connection.send_sync.assert_called_once_with(request)
        assert inv.sent_time is not None

    def test_send_invocation_disconnect_retries(self):
        """Test _send_invocation retries on TargetDisconnectedException."""
        conn_mgr = MagicMock()
        connection = MagicMock()
        connection.connection_id = 123
        connection.send_sync.side_effect = TargetDisconnectedException("disconnected")
        conn_mgr.get_connection.return_value = connection
        service = InvocationService(connection_manager=conn_mgr)
        service._retry_count = 1
        service._retry_pause = 0.001
        service.start()

        request = MagicMock()
        inv = Invocation(request)
        inv.correlation_id = 1
        service._pending[1] = inv

        service._send_invocation(inv)

        time.sleep(0.1)

        assert inv.future.done()
        with pytest.raises(TargetDisconnectedException):
            inv.future.result()

    def test_send_invocation_unexpected_error(self):
        """Test _send_invocation handles unexpected exceptions."""
        conn_mgr = MagicMock()
        connection = MagicMock()
        connection.send_sync.side_effect = RuntimeError("unexpected")
        conn_mgr.get_connection.return_value = connection
        service = InvocationService(connection_manager=conn_mgr)
        service.start()

        request = MagicMock()
        inv = Invocation(request)
        inv.correlation_id = 1
        service._pending[1] = inv

        service._send_invocation(inv)

        assert inv.future.done()
        with pytest.raises(HazelcastException, match="unexpected"):
            inv.future.result()

    def test_get_connection_smart_routing(self):
        """Test _get_connection_for_invocation uses partition for smart routing."""
        conn_mgr = MagicMock()
        service = InvocationService(connection_manager=conn_mgr)
        service._smart_routing = True

        inv = Invocation(MagicMock(), partition_id=5)
        service._get_connection_for_invocation(inv)

        conn_mgr.get_connection.assert_called_once_with(5)

    def test_get_connection_non_smart_routing(self):
        """Test _get_connection_for_invocation ignores partition without smart routing."""
        conn_mgr = MagicMock()
        service = InvocationService(connection_manager=conn_mgr)
        service._smart_routing = False

        inv = Invocation(MagicMock(), partition_id=5)
        service._get_connection_for_invocation(inv)

        conn_mgr.get_connection.assert_called_once_with()

    def test_get_connection_no_partition(self):
        """Test _get_connection_for_invocation without partition."""
        conn_mgr = MagicMock()
        service = InvocationService(connection_manager=conn_mgr)
        service._smart_routing = True

        inv = Invocation(MagicMock(), partition_id=-1)
        service._get_connection_for_invocation(inv)

        conn_mgr.get_connection.assert_called_once_with()

    def test_schedule_retry_exponential_backoff(self):
        """Test _schedule_retry uses exponential backoff."""
        service = InvocationService()
        service._retry_pause = 0.01
        service.start()

        inv = Invocation(MagicMock())
        inv.correlation_id = 1
        service._pending[1] = inv

        send_times = []
        original_send = service._send_invocation

        def mock_send(invocation, retry_count=0):
            send_times.append((time.time(), retry_count))
            if retry_count < 2:
                service._schedule_retry(invocation, retry_count + 1)

        service._send_invocation = mock_send
        service._schedule_retry(inv, 1)

        time.sleep(0.2)

        assert len(send_times) >= 2

    def test_handle_message_event(self):
        """Test _handle_message ignores event messages."""
        service = InvocationService()
        service.start()

        message = MagicMock()
        message.is_event.return_value = True
        message.get_message_type.return_value = 0x000001

        service._handle_message(message)

    def test_handle_response_success(self):
        """Test handle_response completes matching invocation."""
        service = InvocationService()
        service.start()

        request = MagicMock()
        inv = Invocation(request)
        inv.correlation_id = 42
        service._pending[42] = inv

        response = MagicMock()
        response.get_correlation_id.return_value = 42

        result = service.handle_response(response)

        assert result is True
        assert inv.future.done()
        assert inv.future.result() is response
        assert 42 not in service._pending

    def test_handle_response_unknown_correlation_id(self):
        """Test handle_response returns False for unknown correlation ID."""
        service = InvocationService()
        service.start()

        response = MagicMock()
        response.get_correlation_id.return_value = 999

        result = service.handle_response(response)

        assert result is False

    def test_get_pending_count(self):
        """Test get_pending_count returns correct count."""
        service = InvocationService()

        assert service.get_pending_count() == 0

        service._pending[1] = MagicMock()
        service._pending[2] = MagicMock()

        assert service.get_pending_count() == 2

    def test_check_timeouts(self):
        """Test check_timeouts expires old invocations."""
        service = InvocationService()
        service.start()

        inv1 = Invocation(MagicMock(), timeout=0.001)
        inv1.correlation_id = 1
        inv1.mark_sent()

        inv2 = Invocation(MagicMock(), timeout=60.0)
        inv2.correlation_id = 2
        inv2.mark_sent()

        service._pending[1] = inv1
        service._pending[2] = inv2

        time.sleep(0.01)

        timed_out = service.check_timeouts()

        assert timed_out == 1
        assert 1 not in service._pending
        assert 2 in service._pending
        assert inv1.future.done()
        with pytest.raises(OperationTimeoutException):
            inv1.future.result()
        assert not inv2.future.done()

    def test_remove_invocation(self):
        """Test remove_invocation removes and returns invocation."""
        service = InvocationService()

        inv = Invocation(MagicMock())
        inv.correlation_id = 42
        service._pending[42] = inv

        removed = service.remove_invocation(42)

        assert removed is inv
        assert 42 not in service._pending

    def test_remove_invocation_not_found(self):
        """Test remove_invocation returns None for unknown ID."""
        service = InvocationService()

        removed = service.remove_invocation(999)

        assert removed is None

    def test_invoke_on_connection(self):
        """Test invoke_on_connection sends to specific connection."""
        service = InvocationService()
        service.start()

        connection = MagicMock()
        request = MagicMock()
        inv = Invocation(request)

        future = service.invoke_on_connection(inv, connection)

        connection.send_sync.assert_called_once_with(request)
        assert inv.correlation_id == 1
        assert inv.sent_time is not None

    def test_invoke_on_connection_when_not_running(self):
        """Test invoke_on_connection fails when not running."""
        service = InvocationService()

        connection = MagicMock()
        inv = Invocation(MagicMock())

        future = service.invoke_on_connection(inv, connection)

        assert future.done()
        with pytest.raises(ClientOfflineException):
            future.result()

    def test_invoke_on_connection_send_failure(self):
        """Test invoke_on_connection handles send failure."""
        service = InvocationService()
        service.start()

        connection = MagicMock()
        connection.send_sync.side_effect = RuntimeError("send failed")

        inv = Invocation(MagicMock())

        future = service.invoke_on_connection(inv, connection)

        assert future.done()
        with pytest.raises(TargetDisconnectedException):
            future.result()

    def test_concurrent_invocations(self):
        """Test concurrent invocations get unique correlation IDs."""
        conn_mgr = MagicMock()
        connection = MagicMock()
        connection.connection_id = 1
        conn_mgr.get_connection.return_value = connection
        service = InvocationService(connection_manager=conn_mgr)
        service.start()

        correlation_ids = []
        lock = threading.Lock()

        def invoke_thread():
            inv = Invocation(MagicMock())
            service.invoke(inv)
            with lock:
                correlation_ids.append(inv.correlation_id)

        threads = [threading.Thread(target=invoke_thread) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(correlation_ids) == 10
        assert len(set(correlation_ids)) == 10

    def test_shutdown_while_pending(self):
        """Test shutdown cancels all pending invocations."""
        service = InvocationService()
        service.start()

        invocations = []
        for i in range(5):
            inv = Invocation(MagicMock())
            inv.correlation_id = i
            service._pending[i] = inv
            invocations.append(inv)

        service.shutdown()

        assert service.get_pending_count() == 0
        for inv in invocations:
            assert inv.future.done()
            with pytest.raises(HazelcastException, match="shutting down"):
                inv.future.result()

    def test_network_partition_simulation(self):
        """Test behavior during simulated network partition."""
        conn_mgr = MagicMock()
        connection = MagicMock()
        connection.connection_id = 1

        call_count = [0]

        def get_connection(*args):
            call_count[0] += 1
            if call_count[0] <= 2:
                return None
            return connection

        conn_mgr.get_connection.side_effect = get_connection
        service = InvocationService(connection_manager=conn_mgr)
        service._retry_count = 3
        service._retry_pause = 0.001
        service.start()

        inv = Invocation(MagicMock())
        service.invoke(inv)

        time.sleep(0.1)

        connection.send_sync.assert_called()

    def test_send_when_shutting_down(self):
        """Test _send_invocation fails gracefully during shutdown."""
        service = InvocationService()
        service.start()

        inv = Invocation(MagicMock())
        inv.correlation_id = 1

        service._running = False
        service._send_invocation(inv)

        assert inv.future.done()
        with pytest.raises(ClientOfflineException, match="shutting down"):
            inv.future.result()
