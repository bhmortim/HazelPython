"""Unit tests for Hazelcast client configuration."""

import os
import tempfile

import pytest

from hazelcast.config import (
    ClientConfig,
    ConnectionStrategyConfig,
    EvictionPolicy,
    InMemoryFormat,
    NearCacheConfig,
    NetworkConfig,
    ReconnectMode,
    RetryConfig,
    SecurityConfig,
    SerializationConfig,
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

    def test_invalid_max_backoff(self):
        with pytest.raises(ConfigurationException):
            RetryConfig(initial_backoff=10.0, max_backoff=5.0)

    def test_invalid_multiplier(self):
        with pytest.raises(ConfigurationException):
            RetryConfig(multiplier=0.5)

    def test_invalid_jitter_negative(self):
        with pytest.raises(ConfigurationException):
            RetryConfig(jitter=-0.1)

    def test_invalid_jitter_too_large(self):
        with pytest.raises(ConfigurationException):
            RetryConfig(jitter=1.5)

    def test_from_dict(self):
        data = {
            "initial_backoff": 2.0,
            "max_backoff": 120.0,
            "multiplier": 3.0,
            "jitter": 0.5,
        }
        config = RetryConfig.from_dict(data)
        assert config.initial_backoff == 2.0
        assert config.max_backoff == 120.0
        assert config.multiplier == 3.0
        assert config.jitter == 0.5

    def test_from_dict_with_defaults(self):
        config = RetryConfig.from_dict({})
        assert config.initial_backoff == 1.0
        assert config.max_backoff == 30.0


class TestNetworkConfig:
    """Tests for NetworkConfig."""

    def test_default_values(self):
        config = NetworkConfig()
        assert config.addresses == ["localhost:5701"]
        assert config.connection_timeout == 5.0
        assert config.smart_routing is True

    def test_custom_addresses(self):
        config = NetworkConfig(addresses=["192.168.1.1:5701", "192.168.1.2:5701"])
        assert len(config.addresses) == 2
        assert "192.168.1.1:5701" in config.addresses

    def test_empty_addresses_raises(self):
        with pytest.raises(ConfigurationException):
            NetworkConfig(addresses=[])

    def test_invalid_connection_timeout(self):
        with pytest.raises(ConfigurationException):
            NetworkConfig(connection_timeout=0)

    def test_from_dict(self):
        data = {
            "addresses": ["10.0.0.1:5701"],
            "connection_timeout": 10.0,
            "smart_routing": False,
        }
        config = NetworkConfig.from_dict(data)
        assert config.addresses == ["10.0.0.1:5701"]
        assert config.connection_timeout == 10.0
        assert config.smart_routing is False


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
            "retry": {"initial_backoff": 5.0},
        }
        config = ConnectionStrategyConfig.from_dict(data)
        assert config.async_start is True
        assert config.reconnect_mode == ReconnectMode.ASYNC
        assert config.retry.initial_backoff == 5.0

    def test_from_dict_invalid_reconnect_mode(self):
        with pytest.raises(ConfigurationException):
            ConnectionStrategyConfig.from_dict({"reconnect_mode": "INVALID"})


class TestSecurityConfig:
    """Tests for SecurityConfig."""

    def test_default_values(self):
        config = SecurityConfig()
        assert config.username is None
        assert config.password is None
        assert config.token is None
        assert config.is_configured is False

    def test_username_password(self):
        config = SecurityConfig(username="admin", password="secret")
        assert config.username == "admin"
        assert config.password == "secret"
        assert config.is_configured is True

    def test_token_auth(self):
        config = SecurityConfig(token="my-token")
        assert config.token == "my-token"
        assert config.is_configured is True

    def test_from_dict(self):
        data = {"username": "user", "password": "pass", "token": "tok"}
        config = SecurityConfig.from_dict(data)
        assert config.username == "user"
        assert config.password == "pass"
        assert config.token == "tok"


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
            max_idle_seconds=300,
            time_to_live_seconds=600,
            eviction_policy=EvictionPolicy.LFU,
            max_size=5000,
            in_memory_format=InMemoryFormat.OBJECT,
            invalidate_on_change=False,
        )
        assert config.name == "my-cache"
        assert config.max_idle_seconds == 300
        assert config.time_to_live_seconds == 600
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

    def test_from_dict(self):
        data = {
            "max_idle_seconds": 100,
            "eviction_policy": "LFU",
            "in_memory_format": "OBJECT",
        }
        config = NearCacheConfig.from_dict("test-cache", data)
        assert config.name == "test-cache"
        assert config.max_idle_seconds == 100
        assert config.eviction_policy == EvictionPolicy.LFU
        assert config.in_memory_format == InMemoryFormat.OBJECT

    def test_from_dict_invalid_eviction_policy(self):
        with pytest.raises(ConfigurationException):
            NearCacheConfig.from_dict("cache", {"eviction_policy": "INVALID"})

    def test_from_dict_invalid_in_memory_format(self):
        with pytest.raises(ConfigurationException):
            NearCacheConfig.from_dict("cache", {"in_memory_format": "INVALID"})


class TestSerializationConfig:
    """Tests for SerializationConfig."""

    def test_default_values(self):
        config = SerializationConfig()
        assert config.portable_version == 0
        assert config.default_integer_type == "INT"

    def test_add_factories(self):
        config = SerializationConfig()
        config.add_portable_factory(1, "factory1")
        config.add_data_serializable_factory(2, "factory2")
        assert config.portable_factories[1] == "factory1"
        assert config.data_serializable_factories[2] == "factory2"

    def test_from_dict(self):
        data = {"portable_version": 1, "default_integer_type": "LONG"}
        config = SerializationConfig.from_dict(data)
        assert config.portable_version == 1
        assert config.default_integer_type == "LONG"


class TestClientConfig:
    """Tests for ClientConfig."""

    def test_default_values(self):
        config = ClientConfig()
        assert config.cluster_name == "dev"
        assert config.client_name is None
        assert config.cluster_members == ["localhost:5701"]
        assert config.connection_timeout == 5.0
        assert config.smart_routing is True

    def test_cluster_name_setter(self):
        config = ClientConfig()
        config.cluster_name = "production"
        assert config.cluster_name == "production"

    def test_empty_cluster_name_raises(self):
        config = ClientConfig()
        with pytest.raises(ConfigurationException):
            config.cluster_name = ""

    def test_cluster_members_alias(self):
        config = ClientConfig()
        config.cluster_members = ["host1:5701", "host2:5701"]
        assert config.network.addresses == ["host1:5701", "host2:5701"]

    def test_credentials_property(self):
        config = ClientConfig()
        config.credentials = {"username": "admin", "password": "secret"}
        assert config.security.username == "admin"
        assert config.security.password == "secret"

    def test_add_near_cache(self):
        config = ClientConfig()
        nc = NearCacheConfig(name="my-map")
        config.add_near_cache(nc)
        assert "my-map" in config.near_caches
        assert config.near_caches["my-map"] == nc

    def test_labels(self):
        config = ClientConfig()
        config.labels = ["web-server", "production"]
        assert len(config.labels) == 2

    def test_from_dict_minimal(self):
        data = {"cluster_name": "test-cluster"}
        config = ClientConfig.from_dict(data)
        assert config.cluster_name == "test-cluster"

    def test_from_dict_full(self):
        data = {
            "cluster_name": "my-cluster",
            "client_name": "my-client",
            "network": {
                "addresses": ["10.0.0.1:5701", "10.0.0.2:5701"],
                "connection_timeout": 15.0,
                "smart_routing": False,
            },
            "connection_strategy": {
                "async_start": True,
                "reconnect_mode": "ASYNC",
                "retry": {
                    "initial_backoff": 2.0,
                    "max_backoff": 60.0,
                },
            },
            "security": {
                "username": "admin",
                "password": "secret",
            },
            "near_caches": {
                "map1": {"max_size": 1000},
                "map2": {"eviction_policy": "LFU"},
            },
            "labels": ["app1", "env-prod"],
        }
        config = ClientConfig.from_dict(data)
        assert config.cluster_name == "my-cluster"
        assert config.client_name == "my-client"
        assert len(config.network.addresses) == 2
        assert config.network.connection_timeout == 15.0
        assert config.connection_strategy.async_start is True
        assert config.connection_strategy.reconnect_mode == ReconnectMode.ASYNC
        assert config.security.username == "admin"
        assert len(config.near_caches) == 2
        assert config.near_caches["map1"].max_size == 1000
        assert len(config.labels) == 2


class TestClientConfigYaml:
    """Tests for YAML configuration loading."""

    def test_from_yaml_string(self):
        yaml_content = """
        cluster_name: yaml-cluster
        network:
          addresses:
            - 192.168.1.1:5701
            - 192.168.1.2:5701
          connection_timeout: 10.0
        """
        config = ClientConfig.from_yaml_string(yaml_content)
        assert config.cluster_name == "yaml-cluster"
        assert len(config.network.addresses) == 2
        assert config.network.connection_timeout == 10.0

    def test_from_yaml_string_with_wrapper(self):
        yaml_content = """
        hazelcast_client:
          cluster_name: wrapped-cluster
          network:
            addresses:
              - localhost:5701
        """
        config = ClientConfig.from_yaml_string(yaml_content)
        assert config.cluster_name == "wrapped-cluster"

    def test_from_yaml_string_empty(self):
        config = ClientConfig.from_yaml_string("")
        assert config.cluster_name == "dev"

    def test_from_yaml_file(self):
        yaml_content = """
cluster_name: file-cluster
network:
  addresses:
    - 10.0.0.1:5701
connection_strategy:
  async_start: true
  reconnect_mode: ASYNC
  retry:
    initial_backoff: 2.0
    max_backoff: 120.0
    multiplier: 2.5
    jitter: 0.1
security:
  username: test-user
  password: test-pass
near_caches:
  my-map:
    max_size: 5000
    eviction_policy: LFU
    time_to_live_seconds: 300
labels:
  - production
  - region-us
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            config = ClientConfig.from_yaml(temp_path)
            assert config.cluster_name == "file-cluster"
            assert config.network.addresses == ["10.0.0.1:5701"]
            assert config.connection_strategy.async_start is True
            assert config.connection_strategy.reconnect_mode == ReconnectMode.ASYNC
            assert config.connection_strategy.retry.initial_backoff == 2.0
            assert config.connection_strategy.retry.max_backoff == 120.0
            assert config.connection_strategy.retry.multiplier == 2.5
            assert config.connection_strategy.retry.jitter == 0.1
            assert config.security.username == "test-user"
            assert config.security.password == "test-pass"
            assert "my-map" in config.near_caches
            assert config.near_caches["my-map"].max_size == 5000
            assert config.near_caches["my-map"].eviction_policy == EvictionPolicy.LFU
            assert config.near_caches["my-map"].time_to_live_seconds == 300
            assert len(config.labels) == 2
        finally:
            os.unlink(temp_path)

    def test_from_yaml_file_not_found(self):
        with pytest.raises(ConfigurationException) as exc_info:
            ClientConfig.from_yaml("/nonexistent/path/config.yaml")
        assert "not found" in str(exc_info.value)

    def test_from_yaml_string_invalid_yaml(self):
        invalid_yaml = "cluster_name: [invalid: yaml: content"
        with pytest.raises(ConfigurationException) as exc_info:
            ClientConfig.from_yaml_string(invalid_yaml)
        assert "Failed to parse YAML" in str(exc_info.value)


class TestReconnectModeEnum:
    """Tests for ReconnectMode enum."""

    def test_values(self):
        assert ReconnectMode.OFF.value == "OFF"
        assert ReconnectMode.ON.value == "ON"
        assert ReconnectMode.ASYNC.value == "ASYNC"


class TestEvictionPolicyEnum:
    """Tests for EvictionPolicy enum."""

    def test_values(self):
        assert EvictionPolicy.NONE.value == "NONE"
        assert EvictionPolicy.LRU.value == "LRU"
        assert EvictionPolicy.LFU.value == "LFU"
        assert EvictionPolicy.RANDOM.value == "RANDOM"


class TestInMemoryFormatEnum:
    """Tests for InMemoryFormat enum."""

    def test_values(self):
        assert InMemoryFormat.BINARY.value == "BINARY"
        assert InMemoryFormat.OBJECT.value == "OBJECT"
