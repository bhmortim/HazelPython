"""Unit tests for hazelcast.proxy.base module."""

import pytest
from concurrent.futures import Future

from hazelcast.proxy.base import Proxy, DistributedObject, ProxyContext
from hazelcast.exceptions import IllegalStateException


class ConcreteProxy(Proxy):
    """Concrete implementation of Proxy for testing."""

    def __init__(self, name, context=None):
        super().__init__("test:service", name, context)


class TestProxyContext:
    """Tests for ProxyContext class."""

    def test_init(self, mock_invocation_service, serialization_service, listener_service):
        context = ProxyContext(
            invocation_service=mock_invocation_service,
            serialization_service=serialization_service,
            partition_service=None,
            listener_service=listener_service,
        )
        assert context.invocation_service is mock_invocation_service
        assert context.serialization_service is serialization_service
        assert context.partition_service is None
        assert context.listener_service is listener_service


class TestProxy:
    """Tests for Proxy base class."""

    def test_init(self):
        proxy = ConcreteProxy("test-name")
        assert proxy.name == "test-name"
        assert proxy.service_name == "test:service"
        assert proxy.is_destroyed is False

    def test_init_with_context(self, proxy_context):
        proxy = ConcreteProxy("test-name", proxy_context)
        assert proxy._context is proxy_context

    def test_destroy(self):
        proxy = ConcreteProxy("test-name")
        proxy.destroy()
        assert proxy.is_destroyed is True

    def test_destroy_twice_raises(self):
        proxy = ConcreteProxy("test-name")
        proxy.destroy()
        with pytest.raises(IllegalStateException):
            proxy.destroy()

    def test_check_not_destroyed(self):
        proxy = ConcreteProxy("test-name")
        proxy._check_not_destroyed()
        proxy.destroy()
        with pytest.raises(IllegalStateException):
            proxy._check_not_destroyed()

    @pytest.mark.asyncio
    async def test_destroy_async(self):
        proxy = ConcreteProxy("test-name")
        await proxy.destroy_async()
        assert proxy.is_destroyed is True

    def test_invoke_no_context(self):
        proxy = ConcreteProxy("test-name")
        future = proxy._invoke(None)
        assert isinstance(future, Future)
        assert future.result() is None

    def test_invoke_on_partition_no_context(self):
        proxy = ConcreteProxy("test-name")
        future = proxy._invoke_on_partition(None, partition_id=0)
        assert isinstance(future, Future)
        assert future.result() is None

    def test_to_data_no_context(self):
        proxy = ConcreteProxy("test-name")
        result = proxy._to_data(b"test")
        assert result == b"test"

    def test_to_data_none(self):
        proxy = ConcreteProxy("test-name")
        result = proxy._to_data(None)
        assert result == b""

    def test_to_data_string(self):
        proxy = ConcreteProxy("test-name")
        result = proxy._to_data("hello")
        assert result == b"hello"

    def test_to_object_no_context(self):
        proxy = ConcreteProxy("test-name")
        result = proxy._to_object(b"test")
        assert result == b"test"

    def test_to_object_none(self):
        proxy = ConcreteProxy("test-name")
        result = proxy._to_object(None)
        assert result is None

    def test_to_object_empty(self):
        proxy = ConcreteProxy("test-name")
        result = proxy._to_object(b"")
        assert result is None

    def test_get_partition_id_no_context(self):
        proxy = ConcreteProxy("test-name")
        pid = proxy._get_partition_id("key")
        assert isinstance(pid, int)
        assert 0 <= pid < 271

    def test_repr(self):
        proxy = ConcreteProxy("test-name")
        r = repr(proxy)
        assert "ConcreteProxy" in r
        assert "test-name" in r

    def test_str(self):
        proxy = ConcreteProxy("test-name")
        s = str(proxy)
        assert "ConcreteProxy" in s


class TestDistributedObjectInterface:
    """Tests for DistributedObject interface compliance."""

    def test_proxy_is_distributed_object(self):
        proxy = ConcreteProxy("test")
        assert isinstance(proxy, DistributedObject)

    def test_has_required_properties(self):
        proxy = ConcreteProxy("test")
        assert hasattr(proxy, "name")
        assert hasattr(proxy, "service_name")

    def test_has_required_methods(self):
        proxy = ConcreteProxy("test")
        assert callable(proxy.destroy)
        assert callable(proxy.destroy_async)
