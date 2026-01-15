"""Unit tests for hazelcast.invocation module."""

import pytest
import time
from concurrent.futures import Future
from unittest.mock import MagicMock

from hazelcast.invocation import Invocation, InvocationService
from hazelcast.protocol.client_message import ClientMessage
from hazelcast.exceptions import HazelcastException, OperationTimeoutException


class TestInvocation:
    """Tests for Invocation class."""

    def test_init(self):
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request)
        assert inv.request is request
        assert inv.partition_id == -1
        assert inv.timeout == 120.0
        assert inv.correlation_id == 0
        assert inv.sent_time is None

    def test_init_with_partition(self):
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request, partition_id=5)
        assert inv.partition_id == 5

    def test_init_with_timeout(self):
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request, timeout=30.0)
        assert inv.timeout == 30.0

    def test_future(self):
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request)
        assert isinstance(inv.future, Future)

    def test_correlation_id_setter(self):
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request)
        inv.correlation_id = 42
        assert inv.correlation_id == 42
        request.set_correlation_id.assert_called_with(42)

    def test_mark_sent(self):
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request)
        assert inv.sent_time is None
        inv.mark_sent()
        assert inv.sent_time is not None
        assert inv.sent_time <= time.time()

    def test_is_expired_not_sent(self):
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request, timeout=1.0)
        assert inv.is_expired() is False

    def test_is_expired_not_timed_out(self):
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request, timeout=60.0)
        inv.mark_sent()
        assert inv.is_expired() is False

    def test_is_expired_timed_out(self):
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request, timeout=0.001)
        inv.mark_sent()
        time.sleep(0.01)
        assert inv.is_expired() is True

    def test_set_response(self):
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request)
        response = MagicMock(spec=ClientMessage)
        inv.set_response(response)
        assert inv.future.result() is response

    def test_set_response_when_already_done(self):
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request)
        inv.set_response("first")
        inv.set_response("second")
        assert inv.future.result() == "first"

    def test_set_exception(self):
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request)
        exc = ValueError("test error")
        inv.set_exception(exc)
        with pytest.raises(ValueError):
            inv.future.result()

    def test_set_exception_when_already_done(self):
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request)
        inv.set_response("result")
        inv.set_exception(ValueError("ignored"))
        assert inv.future.result() == "result"


class TestInvocationService:
    """Tests for InvocationService class."""

    def test_init(self):
        service = InvocationService()
        assert service.is_running is False
        assert service.get_pending_count() == 0

    def test_start(self):
        service = InvocationService()
        service.start()
        assert service.is_running is True

    def test_shutdown(self):
        service = InvocationService()
        service.start()
        service.shutdown()
        assert service.is_running is False

    def test_shutdown_cancels_pending(self):
        service = InvocationService()
        service.start()
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request)
        service.invoke(inv)
        service.shutdown()
        with pytest.raises(HazelcastException):
            inv.future.result()

    def test_invoke_assigns_correlation_id(self):
        service = InvocationService()
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request)
        service.invoke(inv)
        assert inv.correlation_id > 0

    def test_invoke_increments_correlation_id(self):
        service = InvocationService()
        request1 = MagicMock(spec=ClientMessage)
        request2 = MagicMock(spec=ClientMessage)
        inv1 = Invocation(request1)
        inv2 = Invocation(request2)
        service.invoke(inv1)
        service.invoke(inv2)
        assert inv2.correlation_id == inv1.correlation_id + 1

    def test_invoke_returns_future(self):
        service = InvocationService()
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request)
        result = service.invoke(inv)
        assert result is inv.future

    def test_get_pending_count(self):
        service = InvocationService()
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request)
        service.invoke(inv)
        assert service.get_pending_count() == 1

    def test_handle_response(self):
        service = InvocationService()
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request)
        service.invoke(inv)
        cid = inv.correlation_id

        response = MagicMock(spec=ClientMessage)
        response.get_correlation_id.return_value = cid

        result = service.handle_response(response)
        assert result is True
        assert inv.future.result() is response
        assert service.get_pending_count() == 0

    def test_handle_response_unknown_correlation_id(self):
        service = InvocationService()
        response = MagicMock(spec=ClientMessage)
        response.get_correlation_id.return_value = 9999
        result = service.handle_response(response)
        assert result is False

    def test_check_timeouts_no_expired(self):
        service = InvocationService()
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request, timeout=60.0)
        service.invoke(inv)
        inv.mark_sent()
        timed_out = service.check_timeouts()
        assert timed_out == 0
        assert service.get_pending_count() == 1

    def test_check_timeouts_expired(self):
        service = InvocationService()
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request, timeout=0.001)
        service.invoke(inv)
        inv.mark_sent()
        time.sleep(0.02)
        timed_out = service.check_timeouts()
        assert timed_out == 1
        assert service.get_pending_count() == 0
        with pytest.raises(OperationTimeoutException):
            inv.future.result()

    def test_remove_invocation(self):
        service = InvocationService()
        request = MagicMock(spec=ClientMessage)
        inv = Invocation(request)
        service.invoke(inv)
        cid = inv.correlation_id
        removed = service.remove_invocation(cid)
        assert removed is inv
        assert service.get_pending_count() == 0

    def test_remove_invocation_nonexistent(self):
        service = InvocationService()
        removed = service.remove_invocation(9999)
        assert removed is None
