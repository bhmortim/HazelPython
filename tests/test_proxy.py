"""Tests for hazelcast/proxy/base.py module."""

import pytest
from unittest.mock import Mock, MagicMock, AsyncMock
from concurrent.futures import Future
import asyncio

from hazelcast.proxy.base import DistributedObject, ProxyContext, Proxy
from hazelcast.invocation import Invocation, InvocationService
from hazelcast.exceptions import IllegalStateException


class TestProxyContext:
    """Tests for the ProxyContext class."""

    @pytest.fixture
    def mock_invocation_service(self):
        """Create a mock InvocationService."""
        return Mock(spec=InvocationService)

    @pytest.fixture
    def mock_serialization_service(self):
        """Create a mock serialization service."""
        service = Mock()
        service.to_data = Mock(return_value=b"serialized")
        service.to_object = Mock(return_value={"key": "value"})
        return service

    @pytest.fixture
    def mock_partition_service(self):
        """Create a mock partition service."""
        service = Mock()
        service.get_partition_id = Mock(return_value=5)
        return service

    @pytest.fixture
    def mock_listener_service(self):
        """Create a mock listener service."""
        return Mock()

    @pytest.fixture
    def context(
        self,
        mock_invocation_service,
        mock_serialization_service,
        mock_partition_service,
        mock_listener_service,
    ):
        """Create a ProxyContext with all services."""
        return ProxyContext(
            invocation_service=mock_invocation_service,
            serialization_service=mock_serialization_service,
            partition_service=mock_partition_service,
            listener_service=mock_listener_service,
        )

    def test_init(self, mock_invocation_service):
        """Test ProxyContext initialization."""
        context = ProxyContext(invocation_service=mock_invocation_service)
        assert context.invocation_service is mock_invocation_service
        assert context.serialization_service is None
        assert context.partition_service is None
        assert context.listener_service is None

    def test_invocation_service_property(self, context, mock_invocation_service):
        """Test invocation_service property."""
        assert context.invocation_service is mock_invocation_service

    def test_serialization_service_property(self, context, mock_serialization_service):
        """Test serialization_service property."""
        assert context.serialization_service is mock_serialization_service

    def test_partition_service_property(self, context, mock_partition_service):
        """Test partition_service property."""
        assert context.partition_service is mock_partition_service

    def test_listener_service_property(self, context, mock_listener_service):
        """Test listener_service property."""
        assert context.listener_service is mock_listener_service


class TestProxy:
    """Tests for the Proxy class."""

    @pytest.fixture
    def mock_invocation_service(self):
        """Create a mock InvocationService."""
        service = Mock(spec=InvocationService)
        future = Future()
        future.set_result(Mock())
        service.invoke = Mock(return_value=future)
        return service

    @pytest.fixture
    def mock_serialization_service(self):
        """Create a mock serialization service."""
        service = Mock()
        service.to_data = Mock(return_value=b"serialized")
        service.to_object = Mock(return_value={"key": "value"})
        return service

    @pytest.fixture
    def mock_partition_service(self):
        """Create a mock partition service."""
        service = Mock()
        service.get_partition_id = Mock(return_value=5)
        return service

    @pytest.fixture
    def context(
        self,
        mock_invocation_service,
        mock_serialization_service,
        mock_partition_service,
    ):
        """Create a ProxyContext."""
        return ProxyContext(
            invocation_service=mock_invocation_service,
            serialization_service=mock_serialization_service,
            partition_service=mock_partition_service,
        )

    @pytest.fixture
    def proxy(self, context):
        """Create a Proxy instance."""
        return Proxy("hz:impl:testService", "test-proxy", context)

    @pytest.fixture
    def proxy_no_context(self):
        """Create a Proxy without context."""
        return Proxy("hz:impl:testService", "test-proxy-no-ctx", None)

    def test_init(self, context):
        """Test Proxy initialization."""
        proxy = Proxy("hz:impl:mapService", "my-map", context)
        assert proxy.name == "my-map"
        assert proxy.service_name == "hz:impl:mapService"
        assert not proxy.is_destroyed

    def test_name_property(self, proxy):
        """Test name property."""
        assert proxy.name == "test-proxy"

    def test_service_name_property(self, proxy):
        """Test service_name property."""
        assert proxy.service_name == "hz:impl:testService"

    def test_is_destroyed_property(self, proxy):
        """Test is_destroyed property."""
        assert proxy.is_destroyed is False
        proxy.destroy()
        assert proxy.is_destroyed is True

    def test_check_not_destroyed(self, proxy):
        """Test _check_not_destroyed when not destroyed."""
        proxy._check_not_destroyed()

    def test_check_not_destroyed_raises(self, proxy):
        """Test _check_not_destroyed raises when destroyed."""
        proxy.destroy()
        with pytest.raises(IllegalStateException, match="has been destroyed"):
            proxy._check_not_destroyed()

    def test_destroy(self, proxy):
        """Test destroy method."""
        assert not proxy.is_destroyed
        proxy.destroy()
        assert proxy.is_destroyed

    def test_destroy_twice_raises(self, proxy):
        """Test destroying twice raises exception."""
        proxy.destroy()
        with pytest.raises(IllegalStateException):
            proxy.destroy()

    @pytest.mark.asyncio
    async def test_destroy_async(self, proxy):
        """Test async destroy method."""
        assert not proxy.is_destroyed
        await proxy.destroy_async()
        assert proxy.is_destroyed

    @pytest.mark.asyncio
    async def test_destroy_async_twice_raises(self, proxy):
        """Test async destroying twice raises."""
        await proxy.destroy_async()
        with pytest.raises(IllegalStateException):
            await proxy.destroy_async()

    def test_invoke(self, proxy, mock_invocation_service):
        """Test _invoke method."""
        request = Mock()
        future = proxy._invoke(request)
        
        assert mock_invocation_service.invoke.called
        call_args = mock_invocation_service.invoke.call_args
        invocation = call_args[0][0]
        assert invocation.request is request

    def test_invoke_with_response_handler(self, proxy, mock_invocation_service):
        """Test _invoke with response handler."""
        response = Mock()
        response_future = Future()
        response_future.set_result(response)
        mock_invocation_service.invoke.return_value = response_future
        
        handler = Mock(return_value="processed")
        request = Mock()
        
        future = proxy._invoke(request, response_handler=handler)
        result = future.result()
        
        assert result == "processed"
        handler.assert_called_once_with(response)

    def test_invoke_response_handler_exception(self, proxy, mock_invocation_service):
        """Test _invoke when response handler raises."""
        response = Mock()
        response_future = Future()
        response_future.set_result(response)
        mock_invocation_service.invoke.return_value = response_future
        
        handler = Mock(side_effect=ValueError("handler error"))
        request = Mock()
        
        future = proxy._invoke(request, response_handler=handler)
        
        with pytest.raises(ValueError, match="handler error"):
            future.result()

    def test_invoke_when_destroyed(self, proxy):
        """Test _invoke raises when destroyed."""
        proxy.destroy()
        request = Mock()
        
        with pytest.raises(IllegalStateException):
            proxy._invoke(request)

    def test_invoke_no_context(self, proxy_no_context):
        """Test _invoke with no context returns None result."""
        request = Mock()
        future = proxy_no_context._invoke(request)
        assert future.result() is None

    def test_invoke_on_partition(self, proxy, mock_invocation_service):
        """Test _invoke_on_partition method."""
        request = Mock()
        future = proxy._invoke_on_partition(request, partition_id=3)
        
        assert mock_invocation_service.invoke.called
        call_args = mock_invocation_service.invoke.call_args
        invocation = call_args[0][0]
        assert invocation.request is request
        assert invocation.partition_id == 3

    def test_invoke_on_partition_with_handler(self, proxy, mock_invocation_service):
        """Test _invoke_on_partition with response handler."""
        response = Mock()
        response_future = Future()
        response_future.set_result(response)
        mock_invocation_service.invoke.return_value = response_future
        
        handler = Mock(return_value=42)
        request = Mock()
        
        future = proxy._invoke_on_partition(request, 1, response_handler=handler)
        
        assert future.result() == 42

    def test_invoke_on_partition_when_destroyed(self, proxy):
        """Test _invoke_on_partition raises when destroyed."""
        proxy.destroy()
        with pytest.raises(IllegalStateException):
            proxy._invoke_on_partition(Mock(), 0)

    def test_invoke_on_partition_no_context(self, proxy_no_context):
        """Test _invoke_on_partition with no context."""
        future = proxy_no_context._invoke_on_partition(Mock(), 0)
        assert future.result() is None

    def test_to_data_with_service(self, proxy, mock_serialization_service):
        """Test _to_data with serialization service."""
        result = proxy._to_data({"key": "value"})
        mock_serialization_service.to_data.assert_called_once_with({"key": "value"})
        assert result == b"serialized"

    def test_to_data_none(self, proxy_no_context):
        """Test _to_data with None."""
        result = proxy_no_context._to_data(None)
        assert result == b""

    def test_to_data_bytes(self, proxy_no_context):
        """Test _to_data with bytes."""
        result = proxy_no_context._to_data(b"test")
        assert result == b"test"

    def test_to_data_string(self, proxy_no_context):
        """Test _to_data with string (no service)."""
        result = proxy_no_context._to_data("hello")
        assert result == b"hello"

    def test_to_object_with_service(self, proxy, mock_serialization_service):
        """Test _to_object with serialization service."""
        result = proxy._to_object(b"data")
        mock_serialization_service.to_object.assert_called_once_with(b"data")
        assert result == {"key": "value"}

    def test_to_object_none(self, proxy_no_context):
        """Test _to_object with None."""
        result = proxy_no_context._to_object(None)
        assert result is None

    def test_to_object_empty(self, proxy_no_context):
        """Test _to_object with empty bytes."""
        result = proxy_no_context._to_object(b"")
        assert result is None

    def test_to_object_no_service(self, proxy_no_context):
        """Test _to_object without service returns data."""
        result = proxy_no_context._to_object(b"test")
        assert result == b"test"

    def test_get_partition_id_with_service(self, proxy, mock_partition_service):
        """Test _get_partition_id with partition service."""
        result = proxy._get_partition_id("my-key")
        mock_partition_service.get_partition_id.assert_called_once_with("my-key")
        assert result == 5

    def test_get_partition_id_no_service(self, proxy_no_context):
        """Test _get_partition_id without service uses hash."""
        result = proxy_no_context._get_partition_id("test-key")
        expected = hash("test-key") % 271
        assert result == expected

    def test_repr(self, proxy):
        """Test __repr__ method."""
        repr_str = repr(proxy)
        assert "Proxy" in repr_str
        assert "test-proxy" in repr_str

    def test_str(self, proxy):
        """Test __str__ method."""
        str_val = str(proxy)
        assert "Proxy" in str_val
        assert "test-proxy" in str_val

    @pytest.mark.parametrize("name,service_name", [
        ("map1", "hz:impl:mapService"),
        ("queue-test", "hz:impl:queueService"),
        ("my_set", "hz:impl:setService"),
    ])
    def test_proxy_names(self, context, name, service_name):
        """Test proxy with various names and service names."""
        proxy = Proxy(service_name, name, context)
        assert proxy.name == name
        assert proxy.service_name == service_name


class ConcreteDistributedObject(DistributedObject):
    """Concrete implementation for testing DistributedObject interface."""

    def __init__(self, name: str, service_name: str):
        self._name = name
        self._service_name = service_name
        self._destroyed = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def service_name(self) -> str:
        return self._service_name

    def destroy(self) -> None:
        self._destroyed = True

    async def destroy_async(self) -> None:
        self._destroyed = True


class TestDistributedObject:
    """Tests for the DistributedObject interface."""

    def test_interface_implementation(self):
        """Test that DistributedObject can be implemented."""
        obj = ConcreteDistributedObject("test", "hz:impl:test")
        assert obj.name == "test"
        assert obj.service_name == "hz:impl:test"

    def test_destroy(self):
        """Test destroy method."""
        obj = ConcreteDistributedObject("test", "hz:impl:test")
        obj.destroy()
        assert obj._destroyed is True

    @pytest.mark.asyncio
    async def test_destroy_async(self):
        """Test async destroy method."""
        obj = ConcreteDistributedObject("test", "hz:impl:test")
        await obj.destroy_async()
        assert obj._destroyed is True
