"""Shared pytest fixtures for Hazelcast client tests."""

import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock

from hazelcast.config import (
    ClientConfig,
    NetworkConfig,
    SecurityConfig,
    NearCacheConfig,
    SerializationConfig,
    RetryConfig,
    ReconnectMode,
    EvictionPolicy,
    InMemoryFormat,
)
from hazelcast.network.address import Address
from hazelcast.network.ssl_config import SSLConfig, SSLProtocol
from hazelcast.serialization.service import SerializationService
from hazelcast.invocation import InvocationService
from hazelcast.proxy.base import ProxyContext
from hazelcast.listener import ListenerService


@pytest.fixture
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def default_config():
    """Create a default ClientConfig."""
    return ClientConfig()


@pytest.fixture
def custom_config():
    """Create a custom ClientConfig with all options set."""
    config = ClientConfig()
    config.cluster_name = "test-cluster"
    config.client_name = "test-client"
    config.network.addresses = ["127.0.0.1:5701", "127.0.0.1:5702"]
    config.network.connection_timeout = 10.0
    config.network.smart_routing = True
    config.security.username = "admin"
    config.security.password = "secret"
    config.labels = ["test", "dev"]
    return config


@pytest.fixture
def network_config():
    """Create a NetworkConfig."""
    return NetworkConfig(
        addresses=["localhost:5701"],
        connection_timeout=5.0,
        smart_routing=True,
    )


@pytest.fixture
def security_config():
    """Create a SecurityConfig."""
    return SecurityConfig(
        username="testuser",
        password="testpass",
    )


@pytest.fixture
def near_cache_config():
    """Create a NearCacheConfig."""
    return NearCacheConfig(
        name="test-cache",
        max_size=1000,
        time_to_live_seconds=300,
        max_idle_seconds=60,
        eviction_policy=EvictionPolicy.LRU,
        in_memory_format=InMemoryFormat.BINARY,
    )


@pytest.fixture
def retry_config():
    """Create a RetryConfig."""
    return RetryConfig(
        initial_backoff=1.0,
        max_backoff=30.0,
        multiplier=2.0,
        jitter=0.1,
    )


@pytest.fixture
def ssl_config():
    """Create an SSLConfig."""
    return SSLConfig(
        enabled=True,
        protocol=SSLProtocol.TLSv1_2,
        check_hostname=True,
    )


@pytest.fixture
def address():
    """Create an Address."""
    return Address("localhost", 5701)


@pytest.fixture
def address_ipv6():
    """Create an IPv6 Address."""
    return Address("::1", 5701)


@pytest.fixture
def serialization_service():
    """Create a SerializationService."""
    return SerializationService()


@pytest.fixture
def mock_invocation_service():
    """Create a mock InvocationService."""
    mock = MagicMock(spec=InvocationService)
    mock.is_running = True
    return mock


@pytest.fixture
def listener_service():
    """Create a ListenerService."""
    return ListenerService()


@pytest.fixture
def proxy_context(mock_invocation_service, serialization_service, listener_service):
    """Create a ProxyContext with mocked services."""
    return ProxyContext(
        invocation_service=mock_invocation_service,
        serialization_service=serialization_service,
        partition_service=None,
        listener_service=listener_service,
    )


@pytest.fixture
def mock_connection():
    """Create a mock Connection."""
    mock = MagicMock()
    mock.connection_id = 1
    mock.is_alive = True
    mock.last_read_time = 0.0
    mock.last_write_time = 0.0
    mock.address = Address("localhost", 5701)
    return mock


@pytest.fixture
def mock_async_connection():
    """Create a mock async Connection."""
    mock = AsyncMock()
    mock.connection_id = 1
    mock.is_alive = True
    mock.last_read_time = 0.0
    mock.last_write_time = 0.0
    mock.address = Address("localhost", 5701)
    mock.connect = AsyncMock()
    mock.close = AsyncMock()
    mock.send = AsyncMock()
    return mock
