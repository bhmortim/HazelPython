"""Unit tests for hazelcast.config module."""

import pytest
import tempfile
import os

from hazelcast.config import (
    ClientConfig,
    NetworkConfig,
    SecurityConfig,
    NearCacheConfig,
    SerializationConfig,
    RetryConfig,
    ConnectionStrategyConfig,
    ReconnectMode,
    EvictionPolicy,
    InMemoryFormat,
)
from hazelcast.exceptions import ConfigurationException


class TestRetryConfig:
    """Tests for RetryConfig."""

    def test_default_values(self):
        config = RetryConfig()
        assert config.initial_backoff == 1.0
        assert config.max_backoff == 30.0
        assert config.multiplier == 2.0
        assert config.jitter == 0.0

    def test_custom_values(self):
        config = RetryConfig(
            initial_backoff=0.5,
            max_backoff=60.0,
            multiplier=1.5,
            jitter=0.2,
        )
        assert config.initial_backoff == 0.5
        assert config.max_backoff == 60.0
        assert config.multiplier == 1.5
        assert config.jitter == 0.2

    def test_invalid_initial_backoff(self):
        with pytest.raises(ConfigurationException):
            RetryConfig(initial_backoff=0)

        with pytest.raises(ConfigurationException):
            RetryConfig(initial_backoff=-1)

    def test_invalid_max_backoff(self):
        with pytest.raises(ConfigurationException):
            RetryConfig(initial_backoff=10.0, max_backoff=5.0)

    def test_invalid_multiplier(self):
        with pytest.raises(ConfigurationException):
            RetryConfig(multiplier=0.5)

    def test_invalid_jitter(self):
        with pytest.raises(ConfigurationException):
            RetryConfig(jitter=-0.1)

        with pytest.raises(ConfigurationException):
            RetryConfig(jitter=1.5)

    def test_from_dict(self):
        data = {
            "initial_backoff": 2.0,
            "max_backoff": 45.0,
            "multiplier": 3.0,
            "jitter": 0.3,
        }
        config = RetryConfig.from_dict(data)
        assert config.initial_backoff == 2.0
        assert config.max_backoff == 45.0
        assert config.multiplier == 3.0
        assert config.jitter == 0.3

    def test_setters(self):
        config = RetryConfig()
        config.initial_backoff = 2.0
        config.max_backoff = 60.0
        config.multiplier = 3.0
        config.jitter = 0.5
        assert config.initial_backoff == 2.0
        assert config.max_backoff == 60.0
        assert config.multiplier == 3.0
        assert config.jitter == 0.5


class TestNetworkConfig:
    """Tests for NetworkConfig."""

    def test_default_values(self):
        config = NetworkConfig()
        assert config.addresses == ["localhost:5701"]
        assert config.connection_timeout == 5.0
        assert config.smart_routing is True

    def test_custom_values(self):
        config = NetworkConfig(
            addresses=["192.168.1.1:5701", "192.168.1.2:5701"],
            connection_timeout=10.0,
            smart_routing=False,
        )
        assert config.addresses == ["192.168.1.1:5701", "192.168.1.2:5701"]
        assert config.connection_timeout == 10.0
        assert config.smart_routing is False

    def test_empty_addresses_raises(self):
        with pytest.raises(ConfigurationException):
            NetworkConfig(addresses=[])

    def test_invalid_timeout_raises(self):
        with pytest.raises(ConfigurationException):
            NetworkConfig(connection_timeout=0)

        with pytest.raises(ConfigurationException):
            NetworkConfig(connection_timeout=-1)

    def test_from_dict(self):
        data = {
            "addresses": ["host1:5701", "host2:5702"],
            "connection_timeout": 15.0,
            "smart_routing": False,
        }
        config = NetworkConfig.from_dict(data)
        assert config.addresses == ["host1:5701", "host2:5702"]
        assert config.connection_timeout == 15.0
        assert config.smart_routing is False

    def test_setters(self):
        config = NetworkConfig()
        config.addresses = ["new-host:5701"]
        config.connection_timeout = 20.0
        config.smart_routing = False
        assert config.addresses == ["new-host:5701"]
        assert config.connection_timeout == 20.0
        assert config.smart_routing is False


class TestSecurityConfig:
    """Tests for SecurityConfig."""

    def test_default_values(self):
        config = SecurityConfig()
        assert config.username is None
        assert config.password is None
        assert config.token is None
        assert config.is_configured is False

    def test_username_password(self):
        config = SecurityConfig(username="user", password="pass")
        assert config.username == "user"
        assert config.password == "pass"
        assert config.is_configured is True

    def test_token(self):
        config = SecurityConfig(token="my-token")
        assert config.token == "my-token"
        assert config.is_configured is True

    def test_from_dict(self):
        data = {
            "username": "admin",
            "password": "secret",
        }
        config = SecurityConfig.from_dict(data)
        assert config.username == "admin"
        assert config.password == "secret"

    def test_setters(self):
        config = SecurityConfig()
        config.username = "newuser"
        config.password = "newpass"
        config.token = "newtoken"
        assert config.username == "newuser"
        assert config.password == "newpass"
        assert config.token == "newtoken"


class TestNearCacheConfig:
    """Tests for NearCacheConfig."""

    def test_default_values(self):
        config = NearCacheConfig()
        assert config.name == "default"
        assert config.max_idle_seconds == 0
        assert config.time_to_live_seconds == 0
        assert config.eviction_policy == EvictionPolicy.LRU
        assert config.max_size == 10000
        assert config.in_memory_format == InMemoryFormat.BINARY
        assert config.invalidate_on_change is True

    def test_custom_values(self):
        config = NearCacheConfig(
            name="my-cache",
            max_idle_seconds=60,
            time_to_live_seconds=300,
            eviction_policy=EvictionPolicy.LFU,
            max_size=5000,
            in_memory_format=InMemoryFormat.OBJECT,
            invalidate_on_change=False,
        )
        assert config.name == "my-cache"
        assert config.max_idle_seconds == 60
        assert config.time_to_live_seconds == 300
        assert config.eviction_policy == EvictionPolicy.LFU
        assert config.max_size == 5000
        assert config.in_memory_format == InMemoryFormat.OBJECT
        assert config.invalidate_on_change is False

    def test_empty_name_raises(self):
        with pytest.raises(ConfigurationException):
            NearCacheConfig(name="")

    def test_negative_max_idle_raises(self):
        with pytest.raises(ConfigurationException):
            NearCacheConfig(max_idle_seconds=-1)

    def test_negative_ttl_raises(self):
        with pytest.raises(ConfigurationException):
            NearCacheConfig(time_to_live_seconds=-1)

    def test_invalid_max_size_raises(self):
        with pytest.raises(ConfigurationException):
            NearCacheConfig(max_size=0)

        with pytest.raises(ConfigurationException):
            NearCacheConfig(max_size=-1)

    def test_from_dict(self):
        data = {
            "max_idle_seconds": 120,
            "time_to_live_seconds": 600,
            "eviction_policy": "LFU",
            "max_size": 2000,
            "in_memory_format": "OBJECT",
            "invalidate_on_change": False,
        }
        config = NearCacheConfig.from_dict("test-cache", data)
        assert config.name == "test-cache"
        assert config.max_idle_seconds == 120
        assert config.time_to_live_seconds == 600
        assert config.eviction_policy == EvictionPolicy.LFU
        assert config.max_size == 2000
        assert config.in_memory_format == InMemoryFormat.OBJECT
        assert config.invalidate_on_change is False


class TestSerializationConfig:
    """Tests for SerializationConfig."""

    def test_default_values(self):
        config = SerializationConfig()
        assert config.portable_version == 0
        assert config.default_integer_type == "INT"
        assert config.portable_factories == {}
        assert config.data_serializable_factories == {}
        assert config.custom_serializers == {}
        assert config.compact_serializers == []

    def test_add_portable_factory(self):
        config = SerializationConfig()
        factory = object()
        config.add_portable_factory(1, factory)
        assert config.portable_factories[1] is factory

    def test_add_data_serializable_factory(self):
        config = SerializationConfig()
        factory = object()
        config.add_data_serializable_factory(1, factory)
        assert config.data_serializable_factories[1] is factory

    def test_add_custom_serializer(self):
        config = SerializationConfig()
        serializer = object()
        config.add_custom_serializer(str, serializer)
        assert config.custom_serializers[str] is serializer

    def test_add_compact_serializer(self):
        config = SerializationConfig()
        serializer = object()
        config.add_compact_serializer(serializer)
        assert serializer in config.compact_serializers

    def test_from_dict(self):
        data = {
            "portable_version": 2,
            "default_integer_type": "LONG",
        }
        config = SerializationConfig.from_dict(data)
        assert config.portable_version == 2
        assert config.default_integer_type == "LONG"


class TestConnectionStrategyConfig:
    """Tests for ConnectionStrategyConfig."""

    def test_default_values(self):
        config = ConnectionStrategyConfig()
        assert config.async_start is False
        assert config.reconnect_mode == ReconnectMode.ON
        assert config.retry is not None

    def test_custom_values(self):
        retry = RetryConfig(initial_backoff=2.0)
        config = ConnectionStrategyConfig(
            async_start=True,
            reconnect_mode=ReconnectMode.ASYNC,
            retry=retry,
        )
        assert config.async_start is True
        assert config.reconnect_mode == ReconnectMode.ASYNC
        assert config.retry.initial_backoff == 2.0

    def test_from_dict(self):
        data = {
            "async_start": True,
            "reconnect_mode": "ASYNC",
            "retry": {"initial_backoff": 3.0},
        }
        config = ConnectionStrategyConfig.from_dict(data)
        assert config.async_start is True
        assert config.reconnect_mode == ReconnectMode.ASYNC
        assert config.retry.initial_backoff == 3.0

    def test_invalid_reconnect_mode_raises(self):
        with pytest.raises(ConfigurationException):
            ConnectionStrategyConfig.from_dict({"reconnect_mode": "INVALID"})


class TestClientConfig:
    """Tests for ClientConfig."""

    def test_default_values(self):
        config = ClientConfig()
        assert config.cluster_name == "dev"
        assert config.client_name is None
        assert config.network is not None
        assert config.security is not None
        assert config.serialization is not None
        assert config.labels == []

    def test_cluster_name(self):
        config = ClientConfig()
        config.cluster_name = "my-cluster"
        assert config.cluster_name == "my-cluster"

    def test_empty_cluster_name_raises(self):
        config = ClientConfig()
        with pytest.raises(ConfigurationException):
            config.cluster_name = ""

    def test_cluster_members_alias(self):
        config = ClientConfig()
        config.cluster_members = ["host1:5701", "host2:5702"]
        assert config.network.addresses == ["host1:5701", "host2:5702"]
        assert config.cluster_members == ["host1:5701", "host2:5702"]

    def test_connection_timeout_alias(self):
        config = ClientConfig()
        config.connection_timeout = 15.0
        assert config.network.connection_timeout == 15.0
        assert config.connection_timeout == 15.0

    def test_smart_routing_alias(self):
        config = ClientConfig()
        config.smart_routing = False
        assert config.network.smart_routing is False
        assert config.smart_routing is False

    def test_credentials(self):
        config = ClientConfig()
        config.credentials = {"username": "user", "password": "pass"}
        assert config.security.username == "user"
        assert config.security.password == "pass"
        assert config.credentials == {"username": "user", "password": "pass"}

    def test_add_near_cache(self):
        config = ClientConfig()
        nc_config = NearCacheConfig(name="test-map")
        config.add_near_cache(nc_config)
        assert "test-map" in config.near_caches
        assert config.near_caches["test-map"] is nc_config

    def test_from_dict(self):
        data = {
            "cluster_name": "test-cluster",
            "client_name": "test-client",
            "network": {
                "addresses": ["192.168.1.1:5701"],
                "connection_timeout": 10.0,
            },
            "security": {
                "username": "admin",
                "password": "secret",
            },
            "labels": ["env:test"],
        }
        config = ClientConfig.from_dict(data)
        assert config.cluster_name == "test-cluster"
        assert config.client_name == "test-client"
        assert config.network.addresses == ["192.168.1.1:5701"]
        assert config.network.connection_timeout == 10.0
        assert config.security.username == "admin"
        assert config.security.password == "secret"
        assert "env:test" in config.labels

    def test_from_yaml_string(self):
        yaml_content = """
        cluster_name: yaml-cluster
        client_name: yaml-client
        network:
          addresses:
            - 10.0.0.1:5701
        """
        try:
            import yaml
            config = ClientConfig.from_yaml_string(yaml_content)
            assert config.cluster_name == "yaml-cluster"
            assert config.client_name == "yaml-client"
        except ImportError:
            pytest.skip("PyYAML not installed")

    def test_from_yaml_file(self):
        yaml_content = """
        cluster_name: file-cluster
        """
        try:
            import yaml
            with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
                f.write(yaml_content)
                f.flush()
                config = ClientConfig.from_yaml(f.name)
                assert config.cluster_name == "file-cluster"
            os.unlink(f.name)
        except ImportError:
            pytest.skip("PyYAML not installed")

    def test_from_yaml_file_not_found(self):
        try:
            import yaml
            with pytest.raises(ConfigurationException):
                ClientConfig.from_yaml("/nonexistent/path/config.yaml")
        except ImportError:
            pytest.skip("PyYAML not installed")

    def test_from_yaml_with_hazelcast_client_wrapper(self):
        yaml_content = """
        hazelcast_client:
          cluster_name: wrapped-cluster
        """
        try:
            import yaml
            config = ClientConfig.from_yaml_string(yaml_content)
            assert config.cluster_name == "wrapped-cluster"
        except ImportError:
            pytest.skip("PyYAML not installed")


class TestReconnectMode:
    """Tests for ReconnectMode enum."""

    def test_values(self):
        assert ReconnectMode.OFF.value == "OFF"
        assert ReconnectMode.ON.value == "ON"
        assert ReconnectMode.ASYNC.value == "ASYNC"


class TestEvictionPolicy:
    """Tests for EvictionPolicy enum."""

    def test_values(self):
        assert EvictionPolicy.NONE.value == "NONE"
        assert EvictionPolicy.LRU.value == "LRU"
        assert EvictionPolicy.LFU.value == "LFU"
        assert EvictionPolicy.RANDOM.value == "RANDOM"


class TestInMemoryFormat:
    """Tests for InMemoryFormat enum."""

    def test_values(self):
        assert InMemoryFormat.BINARY.value == "BINARY"
        assert InMemoryFormat.OBJECT.value == "OBJECT"
