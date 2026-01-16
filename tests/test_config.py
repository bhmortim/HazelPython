"""Unit tests for hazelcast.config module."""

import pytest
import os
import tempfile
from unittest.mock import patch, mock_open

from hazelcast.config import (
    ClientConfig,
    NetworkConfig,
    SecurityConfig,
    SerializationConfig,
    RetryConfig,
    ConnectionStrategyConfig,
    NearCacheConfig,
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
            multiplier=3.0,
            jitter=0.2,
        )
        assert config.initial_backoff == 0.5
        assert config.max_backoff == 60.0
        assert config.multiplier == 3.0
        assert config.jitter == 0.2

    def test_invalid_initial_backoff(self):
        with pytest.raises(ConfigurationException) as exc_info:
            RetryConfig(initial_backoff=0)
        assert "initial_backoff must be positive" in str(exc_info.value)

    def test_invalid_initial_backoff_negative(self):
        with pytest.raises(ConfigurationException):
            RetryConfig(initial_backoff=-1)

    def test_invalid_max_backoff(self):
        with pytest.raises(ConfigurationException) as exc_info:
            RetryConfig(initial_backoff=10.0, max_backoff=5.0)
        assert "max_backoff must be >= initial_backoff" in str(exc_info.value)

    def test_invalid_multiplier(self):
        with pytest.raises(ConfigurationException) as exc_info:
            RetryConfig(multiplier=0.5)
        assert "multiplier must be >= 1.0" in str(exc_info.value)

    def test_invalid_jitter_negative(self):
        with pytest.raises(ConfigurationException) as exc_info:
            RetryConfig(jitter=-0.1)
        assert "jitter must be between" in str(exc_info.value)

    def test_invalid_jitter_too_large(self):
        with pytest.raises(ConfigurationException):
            RetryConfig(jitter=1.5)

    def test_setter_validation(self):
        config = RetryConfig()
        
        with pytest.raises(ConfigurationException):
            config.initial_backoff = 0
        
        with pytest.raises(ConfigurationException):
            config.max_backoff = 0.5
        
        with pytest.raises(ConfigurationException):
            config.multiplier = 0.5
        
        with pytest.raises(ConfigurationException):
            config.jitter = -1

    def test_from_dict(self):
        data = {
            "initial_backoff": 2.0,
            "max_backoff": 60.0,
            "multiplier": 1.5,
            "jitter": 0.1,
        }
        config = RetryConfig.from_dict(data)
        assert config.initial_backoff == 2.0
        assert config.max_backoff == 60.0
        assert config.multiplier == 1.5
        assert config.jitter == 0.1

    def test_from_dict_defaults(self):
        config = RetryConfig.from_dict({})
        assert config.initial_backoff == 1.0


class TestNetworkConfig:
    """Tests for NetworkConfig."""

    def test_default_values(self):
        config = NetworkConfig()
        assert config.addresses == ["localhost:5701"]
        assert config.connection_timeout == 5.0
        assert config.smart_routing is True

    def test_custom_addresses(self):
        config = NetworkConfig(addresses=["node1:5701", "node2:5702"])
        assert len(config.addresses) == 2

    def test_empty_addresses_validation(self):
        with pytest.raises(ConfigurationException) as exc_info:
            NetworkConfig(addresses=[])
        assert "At least one cluster address" in str(exc_info.value)

    def test_invalid_connection_timeout(self):
        with pytest.raises(ConfigurationException) as exc_info:
            NetworkConfig(connection_timeout=0)
        assert "connection_timeout must be positive" in str(exc_info.value)

    def test_negative_connection_timeout(self):
        with pytest.raises(ConfigurationException):
            NetworkConfig(connection_timeout=-1)

    def test_setter_validation(self):
        config = NetworkConfig()
        
        with pytest.raises(ConfigurationException):
            config.addresses = []
        
        with pytest.raises(ConfigurationException):
            config.connection_timeout = 0

    def test_from_dict(self):
        data = {
            "addresses": ["node1:5701"],
            "connection_timeout": 10.0,
            "smart_routing": False,
        }
        config = NetworkConfig.from_dict(data)
        assert config.addresses == ["node1:5701"]
        assert config.connection_timeout == 10.0
        assert config.smart_routing is False


class TestSecurityConfig:
    """Tests for SecurityConfig."""

    def test_default_values(self):
        config = SecurityConfig()
        assert config.username is None
        assert config.password is None
        assert config.token is None
        assert not config.is_configured

    def test_with_username_password(self):
        config = SecurityConfig(username="admin", password="secret")
        assert config.username == "admin"
        assert config.password == "secret"
        assert config.is_configured

    def test_with_token(self):
        config = SecurityConfig(token="my-token")
        assert config.token == "my-token"
        assert config.is_configured

    def test_from_dict(self):
        data = {"username": "user", "password": "pass", "token": "tok"}
        config = SecurityConfig.from_dict(data)
        assert config.username == "user"
        assert config.password == "pass"
        assert config.token == "tok"


class TestConnectionStrategyConfig:
    """Tests for ConnectionStrategyConfig."""

    def test_default_values(self):
        config = ConnectionStrategyConfig()
        assert config.async_start is False
        assert config.reconnect_mode == ReconnectMode.ON
        assert config.retry is not None

    def test_custom_values(self):
        config = ConnectionStrategyConfig(
            async_start=True,
            reconnect_mode=ReconnectMode.OFF,
            retry=RetryConfig(initial_backoff=2.0),
        )
        assert config.async_start is True
        assert config.reconnect_mode == ReconnectMode.OFF
        assert config.retry.initial_backoff == 2.0

    def test_from_dict(self):
        data = {
            "async_start": True,
            "reconnect_mode": "OFF",
            "retry": {"initial_backoff": 5.0},
        }
        config = ConnectionStrategyConfig.from_dict(data)
        assert config.async_start is True
        assert config.reconnect_mode == ReconnectMode.OFF
        assert config.retry.initial_backoff == 5.0

    def test_from_dict_invalid_reconnect_mode(self):
        data = {"reconnect_mode": "INVALID"}
        with pytest.raises(ConfigurationException) as exc_info:
            ConnectionStrategyConfig.from_dict(data)
        assert "Invalid reconnect_mode" in str(exc_info.value)


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

    def test_empty_name_validation(self):
        with pytest.raises(ConfigurationException) as exc_info:
            NearCacheConfig(name="")
        assert "name cannot be empty" in str(exc_info.value)

    def test_negative_max_idle_seconds(self):
        with pytest.raises(ConfigurationException) as exc_info:
            NearCacheConfig(max_idle_seconds=-1)
        assert "max_idle_seconds cannot be negative" in str(exc_info.value)

    def test_negative_ttl(self):
        with pytest.raises(ConfigurationException):
            NearCacheConfig(time_to_live_seconds=-1)

    def test_invalid_max_size(self):
        with pytest.raises(ConfigurationException) as exc_info:
            NearCacheConfig(max_size=0)
        assert "max_size must be positive" in str(exc_info.value)

    def test_from_dict(self):
        data = {
            "max_idle_seconds": 300,
            "time_to_live_seconds": 600,
            "eviction_policy": "LFU",
            "max_size": 5000,
            "in_memory_format": "OBJECT",
            "invalidate_on_change": False,
        }
        config = NearCacheConfig.from_dict("test-cache", data)
        assert config.name == "test-cache"
        assert config.max_idle_seconds == 300
        assert config.time_to_live_seconds == 600
        assert config.eviction_policy == EvictionPolicy.LFU
        assert config.max_size == 5000
        assert config.in_memory_format == InMemoryFormat.OBJECT
        assert config.invalidate_on_change is False

    def test_from_dict_invalid_eviction_policy(self):
        data = {"eviction_policy": "INVALID"}
        with pytest.raises(ConfigurationException) as exc_info:
            NearCacheConfig.from_dict("test", data)
        assert "Invalid eviction_policy" in str(exc_info.value)

    def test_from_dict_invalid_in_memory_format(self):
        data = {"in_memory_format": "INVALID"}
        with pytest.raises(ConfigurationException):
            NearCacheConfig.from_dict("test", data)


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
        data = {"portable_version": 2, "default_integer_type": "LONG"}
        config = SerializationConfig.from_dict(data)
        assert config.portable_version == 2
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
        assert config.labels == []

    def test_set_cluster_name(self):
        config = ClientConfig()
        config.cluster_name = "production"
        assert config.cluster_name == "production"

    def test_empty_cluster_name_validation(self):
        config = ClientConfig()
        with pytest.raises(ConfigurationException) as exc_info:
            config.cluster_name = ""
        assert "cluster_name cannot be empty" in str(exc_info.value)

    def test_cluster_members_alias(self):
        config = ClientConfig()
        config.cluster_members = ["node1:5701", "node2:5702"]
        assert config.network.addresses == ["node1:5701", "node2:5702"]

    def test_connection_timeout_alias(self):
        config = ClientConfig()
        config.connection_timeout = 10.0
        assert config.network.connection_timeout == 10.0

    def test_smart_routing_alias(self):
        config = ClientConfig()
        config.smart_routing = False
        assert config.network.smart_routing is False

    def test_credentials_property(self):
        config = ClientConfig()
        assert config.credentials is None
        
        config.security.username = "admin"
        config.security.password = "secret"
        
        creds = config.credentials
        assert creds["username"] == "admin"
        assert creds["password"] == "secret"

    def test_credentials_setter(self):
        config = ClientConfig()
        config.credentials = {"username": "user", "password": "pass"}
        
        assert config.security.username == "user"
        assert config.security.password == "pass"

    def test_add_near_cache(self):
        config = ClientConfig()
        nc_config = NearCacheConfig(name="test-map")
        config.add_near_cache(nc_config)
        
        assert "test-map" in config.near_caches
        assert config.near_caches["test-map"] is nc_config

    def test_from_dict_full(self):
        data = {
            "cluster_name": "test-cluster",
            "client_name": "test-client",
            "network": {
                "addresses": ["node1:5701"],
                "connection_timeout": 10.0,
            },
            "security": {
                "username": "admin",
            },
            "labels": ["tag1", "tag2"],
            "near_caches": {
                "my-map": {"max_size": 5000},
            },
        }
        config = ClientConfig.from_dict(data)
        
        assert config.cluster_name == "test-cluster"
        assert config.client_name == "test-client"
        assert config.network.addresses == ["node1:5701"]
        assert config.network.connection_timeout == 10.0
        assert config.security.username == "admin"
        assert config.labels == ["tag1", "tag2"]
        assert "my-map" in config.near_caches

    def test_from_yaml_file_not_found(self):
        with pytest.raises(ConfigurationException) as exc_info:
            ClientConfig.from_yaml("/nonexistent/path.yml")
        assert "not found" in str(exc_info.value)

    def test_from_yaml_success(self):
        yaml_content = """
cluster_name: yaml-cluster
client_name: yaml-client
network:
  addresses:
    - node1:5701
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            
            try:
                config = ClientConfig.from_yaml(f.name)
                assert config.cluster_name == "yaml-cluster"
                assert config.client_name == "yaml-client"
                assert config.network.addresses == ["node1:5701"]
            finally:
                os.unlink(f.name)

    def test_from_yaml_with_hazelcast_client_root(self):
        yaml_content = """
hazelcast_client:
  cluster_name: root-cluster
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            
            try:
                config = ClientConfig.from_yaml(f.name)
                assert config.cluster_name == "root-cluster"
            finally:
                os.unlink(f.name)

    def test_from_yaml_empty_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("")
            f.flush()
            
            try:
                config = ClientConfig.from_yaml(f.name)
                assert config.cluster_name == "dev"
            finally:
                os.unlink(f.name)

    def test_from_yaml_string(self):
        yaml_content = """
cluster_name: string-cluster
"""
        config = ClientConfig.from_yaml_string(yaml_content)
        assert config.cluster_name == "string-cluster"

    def test_from_yaml_string_empty(self):
        config = ClientConfig.from_yaml_string("")
        assert config.cluster_name == "dev"

    def test_from_yaml_string_with_root(self):
        yaml_content = """
hazelcast_client:
  cluster_name: nested-cluster
"""
        config = ClientConfig.from_yaml_string(yaml_content)
        assert config.cluster_name == "nested-cluster"


class TestReconnectMode:
    """Tests for ReconnectMode enum."""

    def test_all_modes_defined(self):
        assert ReconnectMode.OFF.value == "OFF"
        assert ReconnectMode.ON.value == "ON"
        assert ReconnectMode.ASYNC.value == "ASYNC"


class TestEvictionPolicy:
    """Tests for EvictionPolicy enum."""

    def test_all_policies_defined(self):
        assert EvictionPolicy.NONE.value == "NONE"
        assert EvictionPolicy.LRU.value == "LRU"
        assert EvictionPolicy.LFU.value == "LFU"
        assert EvictionPolicy.RANDOM.value == "RANDOM"


class TestInMemoryFormat:
    """Tests for InMemoryFormat enum."""

    def test_all_formats_defined(self):
        assert InMemoryFormat.BINARY.value == "BINARY"
        assert InMemoryFormat.OBJECT.value == "OBJECT"
