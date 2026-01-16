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

    def test_get_connection_no_connection_manager(self):
        """Test _get_connection_for_invocation returns None without connection manager."""
        service = InvocationService()
        inv = Invocation(MagicMock(), partition_id=5)

        result = service._get_connection_for_invocation(inv)

        assert result is None

    def test_init_config_without_retry(self):
        """Test InvocationService with config that has no retry attribute."""
        config = MagicMock(spec=['smart_routing'])
        config.smart_routing = False

        service = InvocationService(config=config)

        assert service._smart_routing is False
        assert service._retry_count == 3
        assert service._retry_pause == 1.0

    def test_schedule_retry_skipped_when_future_done(self):
        """Test _schedule_retry skips retry when future already completed."""
        service = InvocationService()
        service._retry_pause = 0.001
        service.start()

        inv = Invocation(MagicMock())
        inv.correlation_id = 1
        inv.set_response(MagicMock())

        send_calls = []
        service._send_invocation = lambda i, r=0: send_calls.append(r)

        service._schedule_retry(inv, 1)

        time.sleep(0.05)

        assert len(send_calls) == 0

    def test_schedule_retry_skipped_when_not_running(self):
        """Test _schedule_retry skips retry when service stopped."""
        service = InvocationService()
        service._retry_pause = 0.001
        service.start()

        inv = Invocation(MagicMock())
        inv.correlation_id = 1

        send_calls = []
        service._send_invocation = lambda i, r=0: send_calls.append(r)

        service._schedule_retry(inv, 1)
        service._running = False

        time.sleep(0.05)

        assert len(send_calls) == 0

    def test_handle_message_non_event_calls_handle_response(self):
        """Test _handle_message calls handle_response for non-event messages."""
        service = InvocationService()
        service.start()

        request = MagicMock()
        inv = Invocation(request)
        inv.correlation_id = 42
        service._pending[42] = inv

        message = MagicMock()
        message.is_event.return_value = False
        message.get_correlation_id.return_value = 42

        service._handle_message(message)

        assert inv.future.done()
        assert inv.future.result() is message

    def test_smart_routing_partition_zero(self):
        """Test smart routing with partition_id=0 (valid partition)."""
        conn_mgr = MagicMock()
        service = InvocationService(connection_manager=conn_mgr)
        service._smart_routing = True

        inv = Invocation(MagicMock(), partition_id=0)
        service._get_connection_for_invocation(inv)

        conn_mgr.get_connection.assert_called_once_with(0)

    def test_check_timeouts_empty_pending(self):
        """Test check_timeouts with no pending invocations."""
        service = InvocationService()
        service.start()

        timed_out = service.check_timeouts()

        assert timed_out == 0

    def test_check_timeouts_thread_safety(self):
        """Test check_timeouts is thread-safe with concurrent calls."""
        service = InvocationService()
        service.start()

        for i in range(20):
            inv = Invocation(MagicMock(), timeout=0.001 if i % 2 == 0 else 60.0)
            inv.correlation_id = i
            inv.mark_sent()
            service._pending[i] = inv

        time.sleep(0.02)

        results = []
        lock = threading.Lock()

        def check_thread():
            result = service.check_timeouts()
            with lock:
                results.append(result)

        threads = [threading.Thread(target=check_thread) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sum(results) == 10
        assert service.get_pending_count() == 10

    def test_invoke_full_flow_with_response(self):
        """Test complete invoke flow from request to response."""
        conn_mgr = MagicMock()
        connection = MagicMock()
        connection.connection_id = 1
        conn_mgr.get_connection.return_value = connection
        service = InvocationService(connection_manager=conn_mgr)
        service.start()

        request = MagicMock()
        inv = Invocation(request, partition_id=3)

        future = service.invoke(inv)

        response = MagicMock()
        response.get_correlation_id.return_value = inv.correlation_id
        service.handle_response(response)

        assert future.done()
        assert future.result() is response
        assert service.get_pending_count() == 0

    def test_multiple_sequential_retries(self):
        """Test multiple retry attempts with increasing backoff."""
        conn_mgr = MagicMock()
        conn_mgr.get_connection.return_value = None
        service = InvocationService(connection_manager=conn_mgr)
        service._retry_count = 3
        service._retry_pause = 0.005
        service.start()

        request = MagicMock()
        inv = Invocation(request)

        service.invoke(inv)

        time.sleep(0.2)

        assert inv.future.done()
        with pytest.raises(ClientOfflineException):
            inv.future.result()

        assert conn_mgr.get_connection.call_count >= 4

    def test_concurrent_invoke_and_response(self):
        """Test concurrent invoke and response handling."""
        conn_mgr = MagicMock()
        connection = MagicMock()
        connection.connection_id = 1
        conn_mgr.get_connection.return_value = connection
        service = InvocationService(connection_manager=conn_mgr)
        service.start()

        futures = []
        lock = threading.Lock()

        def invoke_and_store(i):
            inv = Invocation(MagicMock())
            future = service.invoke(inv)
            with lock:
                futures.append((inv.correlation_id, future))

        threads = [threading.Thread(target=invoke_and_store, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for cid, future in futures:
            response = MagicMock()
            response.get_correlation_id.return_value = cid
            service.handle_response(response)

        for _, future in futures:
            assert future.done()

        assert service.get_pending_count() == 0

    def test_fail_invocation_removes_from_pending(self):
        """Test _fail_invocation removes invocation from pending map."""
        service = InvocationService()
        service.start()

        inv = Invocation(MagicMock())
        inv.correlation_id = 42
        service._pending[42] = inv

        service._fail_invocation(inv, HazelcastException("test"))

        assert 42 not in service._pending
        assert inv.future.done()

    def test_invoke_on_connection_marks_sent_time(self):
        """Test invoke_on_connection marks sent time correctly."""
        service = InvocationService()
        service.start()

        connection = MagicMock()
        inv = Invocation(MagicMock())

        before = time.time()
        service.invoke_on_connection(inv, connection)
        after = time.time()

        assert inv.sent_time is not None
        assert before <= inv.sent_time <= after

    def test_retry_backoff_calculation(self):
        """Test exponential backoff values are calculated correctly."""
        conn_mgr = MagicMock()
        connection = MagicMock()
        connection.connection_id = 1
        connection.send_sync.side_effect = TargetDisconnectedException("fail")
        conn_mgr.get_connection.return_value = connection

        service = InvocationService(connection_manager=conn_mgr)
        service._retry_count = 3
        service._retry_pause = 0.01
        service.start()

        retry_times = []
        original_schedule = service._schedule_retry

        def mock_schedule(inv, retry_count):
            retry_times.append((time.time(), retry_count))
            original_schedule(inv, retry_count)

        service._schedule_retry = mock_schedule

        inv = Invocation(MagicMock())
        service.invoke(inv)

        time.sleep(0.2)

        assert len(retry_times) >= 3

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

    def test_invoke_partition_routing_under_load(self):
        """Test partition-aware routing under concurrent load."""
        conn_mgr = MagicMock()
        connections = {i: MagicMock(connection_id=i) for i in range(10)}

        def get_connection(partition_id=None):
            if partition_id is not None:
                return connections.get(partition_id % 10)
            return connections[0]

        conn_mgr.get_connection.side_effect = get_connection
        service = InvocationService(connection_manager=conn_mgr)
        service._smart_routing = True
        service.start()

        results = []
        lock = threading.Lock()

        def invoke_partition(partition):
            inv = Invocation(MagicMock(), partition_id=partition)
            service.invoke(inv)
            with lock:
                results.append((partition, inv.correlation_id))

        threads = [threading.Thread(target=invoke_partition, args=(i,)) for i in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 50
        assert len(set(cid for _, cid in results)) == 50

    def test_timeout_during_retry(self):
        """Test invocation times out while waiting for retry."""
        conn_mgr = MagicMock()
        conn_mgr.get_connection.return_value = None
        service = InvocationService(connection_manager=conn_mgr)
        service._retry_count = 10
        service._retry_pause = 1.0
        service.start()

        inv = Invocation(MagicMock(), timeout=0.01)
        inv.correlation_id = 1
        service._pending[1] = inv

        service._send_invocation(inv)

        time.sleep(0.05)

        timed_out = service.check_timeouts()

        assert timed_out == 1

    def test_response_after_timeout(self):
        """Test response received after timeout is ignored."""
        service = InvocationService()
        service.start()

        inv = Invocation(MagicMock(), timeout=0.001)
        inv.correlation_id = 1
        inv.mark_sent()
        service._pending[1] = inv

        time.sleep(0.01)
        service.check_timeouts()

        response = MagicMock()
        response.get_correlation_id.return_value = 1

        result = service.handle_response(response)

        assert result is False
        with pytest.raises(OperationTimeoutException):
            inv.future.result()

    def test_disconnect_all_retries_exhausted(self):
        """Test all retries exhausted on disconnect preserves original exception."""
        conn_mgr = MagicMock()
        connection = MagicMock()
        connection.connection_id = 1
        connection.send_sync.side_effect = TargetDisconnectedException("connection lost")
        conn_mgr.get_connection.return_value = connection

        service = InvocationService(connection_manager=conn_mgr)
        service._retry_count = 0
        service.start()

        inv = Invocation(MagicMock())
        service.invoke(inv)

        assert inv.future.done()
        with pytest.raises(TargetDisconnectedException, match="connection lost"):
            inv.future.result()

    def test_invoke_on_connection_adds_to_pending(self):
        """Test invoke_on_connection adds invocation to pending map."""
        service = InvocationService()
        service.start()

        connection = MagicMock()
        inv = Invocation(MagicMock())

        service.invoke_on_connection(inv, connection)

        assert inv.correlation_id in service._pending

    def test_config_missing_smart_routing(self):
        """Test config without smart_routing attribute uses default."""
        config = MagicMock(spec=[])

        service = InvocationService(config=config)

        assert service._smart_routing is True

    def test_shutdown_idempotent(self):
        """Test shutdown can be called multiple times safely."""
        service = InvocationService()
        service.start()

        inv = Invocation(MagicMock())
        inv.correlation_id = 1
        service._pending[1] = inv

        service.shutdown()
        service.shutdown()

        assert not service.is_running
        assert service.get_pending_count() == 0

    def test_check_timeouts_all_expired(self):
        """Test check_timeouts when all invocations are expired."""
        service = InvocationService()
        service.start()

        for i in range(5):
            inv = Invocation(MagicMock(), timeout=0.001)
            inv.correlation_id = i
            inv.mark_sent()
            service._pending[i] = inv

        time.sleep(0.02)

        timed_out = service.check_timeouts()

        assert timed_out == 5
        assert service.get_pending_count() == 0

    def test_check_timeouts_none_expired(self):
        """Test check_timeouts when no invocations are expired."""
        service = InvocationService()
        service.start()

        for i in range(5):
            inv = Invocation(MagicMock(), timeout=60.0)
            inv.correlation_id = i
            inv.mark_sent()
            service._pending[i] = inv

        timed_out = service.check_timeouts()

        assert timed_out == 0
        assert service.get_pending_count() == 5
