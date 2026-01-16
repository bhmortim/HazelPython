"""Tests for hazelcast/invocation.py module."""

import pytest
import time
import threading
from unittest.mock import Mock, MagicMock, patch
from concurrent.futures import Future

from hazelcast.invocation import Invocation, InvocationService
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.exceptions import (
    HazelcastException,
    OperationTimeoutException,
    TargetDisconnectedException,
    ClientOfflineException,
)


class TestInvocation:
    """Tests for the Invocation class."""

    @pytest.fixture
    def mock_request(self):
        """Create a mock ClientMessage request."""
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        return request

    @pytest.fixture
    def invocation(self, mock_request):
        """Create an Invocation instance."""
        return Invocation(mock_request, partition_id=5, timeout=30.0)

    def test_init_defaults(self, mock_request):
        """Test Invocation initialization with defaults."""
        inv = Invocation(mock_request)
        assert inv.request is mock_request
        assert inv.partition_id == -1
        assert inv.timeout == 120.0
        assert inv.correlation_id == 0
        assert inv.sent_time is None
        assert isinstance(inv.future, Future)
        assert not inv.future.done()

    def test_init_with_params(self, mock_request):
        """Test Invocation initialization with parameters."""
        inv = Invocation(mock_request, partition_id=10, timeout=60.0)
        assert inv.partition_id == 10
        assert inv.timeout == 60.0

    def test_correlation_id_setter(self, invocation, mock_request):
        """Test setting correlation_id also sets it on the request."""
        invocation.correlation_id = 42
        assert invocation.correlation_id == 42
        mock_request.set_correlation_id.assert_called_once_with(42)

    def test_mark_sent(self, invocation):
        """Test mark_sent sets the sent_time."""
        assert invocation.sent_time is None
        invocation.mark_sent()
        assert invocation.sent_time is not None
        assert isinstance(invocation.sent_time, float)

    def test_is_expired_not_sent(self, invocation):
        """Test is_expired returns False if not sent."""
        assert invocation.is_expired() is False

    def test_is_expired_not_expired(self, invocation):
        """Test is_expired returns False if within timeout."""
        invocation.mark_sent()
        assert invocation.is_expired() is False

    def test_is_expired_expired(self, mock_request):
        """Test is_expired returns True after timeout."""
        inv = Invocation(mock_request, timeout=0.001)
        inv.mark_sent()
        time.sleep(0.01)
        assert inv.is_expired() is True

    @pytest.mark.parametrize("timeout,sleep_time,expected", [
        (0.1, 0.0, False),
        (0.01, 0.02, True),
        (1.0, 0.0, False),
    ])
    def test_is_expired_parametrized(self, mock_request, timeout, sleep_time, expected):
        """Parametrized test for is_expired."""
        inv = Invocation(mock_request, timeout=timeout)
        inv.mark_sent()
        if sleep_time > 0:
            time.sleep(sleep_time)
        assert inv.is_expired() == expected

    def test_set_response(self, invocation):
        """Test set_response sets the future result."""
        response = Mock(spec=ClientMessage)
        invocation.set_response(response)
        assert invocation.future.done()
        assert invocation.future.result() is response

    def test_set_response_already_done(self, invocation):
        """Test set_response does nothing if future already done."""
        response1 = Mock(spec=ClientMessage)
        response2 = Mock(spec=ClientMessage)
        invocation.set_response(response1)
        invocation.set_response(response2)
        assert invocation.future.result() is response1

    def test_set_exception(self, invocation):
        """Test set_exception sets the future exception."""
        exc = HazelcastException("test error")
        invocation.set_exception(exc)
        assert invocation.future.done()
        with pytest.raises(HazelcastException, match="test error"):
            invocation.future.result()

    def test_set_exception_already_done(self, invocation):
        """Test set_exception does nothing if future already done."""
        response = Mock(spec=ClientMessage)
        invocation.set_response(response)
        invocation.set_exception(HazelcastException("test"))
        assert invocation.future.result() is response


class TestInvocationService:
    """Tests for the InvocationService class."""

    @pytest.fixture
    def mock_connection_manager(self):
        """Create a mock ConnectionManager."""
        manager = Mock()
        manager.set_message_callback = Mock()
        manager.get_connection = Mock(return_value=None)
        return manager

    @pytest.fixture
    def mock_connection(self):
        """Create a mock Connection."""
        conn = Mock()
        conn.connection_id = 1
        conn.send_sync = Mock()
        return conn

    @pytest.fixture
    def mock_config(self):
        """Create a mock ClientConfig."""
        config = Mock()
        config.smart_routing = True
        config.retry = Mock()
        config.retry.max_attempts = 3
        config.retry.initial_backoff = 0.1
        return config

    @pytest.fixture
    def service(self, mock_connection_manager, mock_config):
        """Create an InvocationService instance."""
        return InvocationService(
            connection_manager=mock_connection_manager,
            config=mock_config,
        )

    @pytest.fixture
    def service_no_deps(self):
        """Create an InvocationService without dependencies."""
        return InvocationService()

    def test_init_defaults(self):
        """Test InvocationService initialization with defaults."""
        service = InvocationService()
        assert not service.is_running
        assert service.get_pending_count() == 0

    def test_init_with_config(self, mock_connection_manager, mock_config):
        """Test InvocationService initialization with config."""
        service = InvocationService(
            connection_manager=mock_connection_manager,
            config=mock_config,
        )
        mock_connection_manager.set_message_callback.assert_called_once()

    def test_start(self, service):
        """Test starting the service."""
        assert not service.is_running
        service.start()
        assert service.is_running

    def test_shutdown(self, service):
        """Test shutting down the service."""
        service.start()
        
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request)
        service.invoke(inv)
        
        service.shutdown()
        
        assert not service.is_running
        assert service.get_pending_count() == 0
        with pytest.raises(HazelcastException, match="shutting down"):
            inv.future.result()

    def test_invoke_not_running(self, service):
        """Test invoke when service is not running."""
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request)
        
        future = service.invoke(inv)
        
        with pytest.raises(ClientOfflineException):
            future.result()

    def test_invoke_success(self, service, mock_connection_manager, mock_connection):
        """Test successful invocation."""
        mock_connection_manager.get_connection.return_value = mock_connection
        service.start()
        
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request)
        
        future = service.invoke(inv)
        
        assert inv.correlation_id > 0
        assert service.get_pending_count() == 1
        mock_connection.send_sync.assert_called_once_with(request)

    def test_invoke_assigns_correlation_id(self, service, mock_connection_manager, mock_connection):
        """Test that invoke assigns unique correlation IDs."""
        mock_connection_manager.get_connection.return_value = mock_connection
        service.start()
        
        request1 = Mock(spec=ClientMessage)
        request1.set_correlation_id = Mock()
        inv1 = Invocation(request1)
        
        request2 = Mock(spec=ClientMessage)
        request2.set_correlation_id = Mock()
        inv2 = Invocation(request2)
        
        service.invoke(inv1)
        service.invoke(inv2)
        
        assert inv1.correlation_id != inv2.correlation_id
        assert inv1.correlation_id > 0
        assert inv2.correlation_id > 0

    def test_handle_response_success(self, service, mock_connection_manager, mock_connection):
        """Test handling a successful response."""
        mock_connection_manager.get_connection.return_value = mock_connection
        service.start()
        
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request)
        service.invoke(inv)
        
        response = Mock(spec=ClientMessage)
        response.get_correlation_id = Mock(return_value=inv.correlation_id)
        
        result = service.handle_response(response)
        
        assert result is True
        assert service.get_pending_count() == 0
        assert inv.future.result() is response

    def test_handle_response_unknown_correlation(self, service):
        """Test handling response with unknown correlation ID."""
        service.start()
        
        response = Mock(spec=ClientMessage)
        response.get_correlation_id = Mock(return_value=999)
        
        result = service.handle_response(response)
        
        assert result is False

    def test_check_timeouts(self, service, mock_connection_manager, mock_connection):
        """Test checking for timed out invocations."""
        mock_connection_manager.get_connection.return_value = mock_connection
        service.start()
        
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request, timeout=0.001)
        service.invoke(inv)
        
        time.sleep(0.02)
        
        timed_out = service.check_timeouts()
        
        assert timed_out == 1
        assert service.get_pending_count() == 0
        with pytest.raises(OperationTimeoutException):
            inv.future.result()

    def test_check_timeouts_none_expired(self, service, mock_connection_manager, mock_connection):
        """Test check_timeouts when no invocations have expired."""
        mock_connection_manager.get_connection.return_value = mock_connection
        service.start()
        
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request, timeout=10.0)
        service.invoke(inv)
        
        timed_out = service.check_timeouts()
        
        assert timed_out == 0
        assert service.get_pending_count() == 1

    def test_remove_invocation(self, service, mock_connection_manager, mock_connection):
        """Test removing an invocation."""
        mock_connection_manager.get_connection.return_value = mock_connection
        service.start()
        
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request)
        service.invoke(inv)
        
        removed = service.remove_invocation(inv.correlation_id)
        
        assert removed is inv
        assert service.get_pending_count() == 0

    def test_remove_invocation_not_found(self, service):
        """Test removing non-existent invocation."""
        service.start()
        removed = service.remove_invocation(999)
        assert removed is None

    def test_invoke_on_connection(self, service, mock_connection):
        """Test invoking on a specific connection."""
        service.start()
        
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request)
        
        future = service.invoke_on_connection(inv, mock_connection)
        
        assert inv.correlation_id > 0
        mock_connection.send_sync.assert_called_once_with(request)

    def test_invoke_on_connection_not_running(self, service, mock_connection):
        """Test invoke_on_connection when service not running."""
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request)
        
        future = service.invoke_on_connection(inv, mock_connection)
        
        with pytest.raises(ClientOfflineException):
            future.result()

    def test_invoke_on_connection_send_fails(self, service, mock_connection):
        """Test invoke_on_connection when send fails."""
        service.start()
        mock_connection.send_sync.side_effect = Exception("connection lost")
        
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request)
        
        future = service.invoke_on_connection(inv, mock_connection)
        
        with pytest.raises(TargetDisconnectedException):
            future.result()

    def test_get_connection_for_invocation_smart_routing(
        self, service, mock_connection_manager, mock_connection
    ):
        """Test connection selection with smart routing."""
        mock_connection_manager.get_connection.return_value = mock_connection
        service.start()
        
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request, partition_id=5)
        
        conn = service._get_connection_for_invocation(inv)
        
        mock_connection_manager.get_connection.assert_called_with(5)

    def test_get_connection_for_invocation_no_partition(
        self, service, mock_connection_manager, mock_connection
    ):
        """Test connection selection without partition."""
        mock_connection_manager.get_connection.return_value = mock_connection
        service.start()
        
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request, partition_id=-1)
        
        conn = service._get_connection_for_invocation(inv)
        
        mock_connection_manager.get_connection.assert_called_with()

    def test_get_connection_for_invocation_no_manager(self, service_no_deps):
        """Test connection selection without connection manager."""
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request)
        
        conn = service_no_deps._get_connection_for_invocation(inv)
        
        assert conn is None

    def test_send_invocation_no_connection_retries(
        self, service, mock_connection_manager
    ):
        """Test that send retries when no connection available."""
        mock_connection_manager.get_connection.return_value = None
        service.start()
        service._retry_pause = 0.001
        
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request)
        inv.correlation_id = 1
        
        service._send_invocation(inv, retry_count=3)
        
        with pytest.raises(ClientOfflineException):
            inv.future.result(timeout=0.5)

    def test_send_invocation_disconnect_retries(
        self, service, mock_connection_manager, mock_connection
    ):
        """Test retry on disconnect."""
        mock_connection_manager.get_connection.return_value = mock_connection
        mock_connection.send_sync.side_effect = TargetDisconnectedException("lost")
        service.start()
        service._retry_pause = 0.001
        service._retry_count = 1
        
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request)
        inv.correlation_id = 1
        
        service._send_invocation(inv, retry_count=1)
        
        with pytest.raises(TargetDisconnectedException):
            inv.future.result(timeout=0.5)

    def test_handle_message_event(self, service):
        """Test handling event messages (should be ignored)."""
        service.start()
        
        message = Mock(spec=ClientMessage)
        message.is_event.return_value = True
        message.get_message_type.return_value = 0x000100
        
        service._handle_message(message)

    def test_handle_message_response(self, service, mock_connection_manager, mock_connection):
        """Test handling response messages."""
        mock_connection_manager.get_connection.return_value = mock_connection
        service.start()
        
        request = Mock(spec=ClientMessage)
        request.set_correlation_id = Mock()
        inv = Invocation(request)
        service.invoke(inv)
        
        response = Mock(spec=ClientMessage)
        response.is_event.return_value = False
        response.get_correlation_id.return_value = inv.correlation_id
        
        service._handle_message(response)
        
        assert inv.future.result() is response

    def test_concurrent_invocations(self, service, mock_connection_manager, mock_connection):
        """Test concurrent invocations."""
        mock_connection_manager.get_connection.return_value = mock_connection
        service.start()
        
        invocations = []
        for _ in range(10):
            request = Mock(spec=ClientMessage)
            request.set_correlation_id = Mock()
            inv = Invocation(request)
            service.invoke(inv)
            invocations.append(inv)
        
        assert service.get_pending_count() == 10
        
        correlation_ids = [inv.correlation_id for inv in invocations]
        assert len(set(correlation_ids)) == 10
