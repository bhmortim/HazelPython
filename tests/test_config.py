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
    IndexType,
    IndexConfig,
    BitmapIndexOptions,
    UniqueKeyTransformation,
    QueryCacheConfig,
    WanReplicationConfig,
    ClientUserCodeDeploymentConfig,
    LocalUpdatePolicy,
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


class TestIndexType:
    """Tests for IndexType enum."""

    def test_all_types_defined(self):
        assert IndexType.SORTED.value == "SORTED"
        assert IndexType.HASH.value == "HASH"
        assert IndexType.BITMAP.value == "BITMAP"


class TestLocalUpdatePolicy:
    """Tests for LocalUpdatePolicy enum."""

    def test_all_policies_defined(self):
        assert LocalUpdatePolicy.INVALIDATE.value == "INVALIDATE"
        assert LocalUpdatePolicy.CACHE_ON_UPDATE.value == "CACHE_ON_UPDATE"


class TestUniqueKeyTransformation:
    """Tests for UniqueKeyTransformation enum."""

    def test_all_transformations_defined(self):
        assert UniqueKeyTransformation.OBJECT.value == "OBJECT"
        assert UniqueKeyTransformation.LONG.value == "LONG"
        assert UniqueKeyTransformation.RAW.value == "RAW"


class TestBitmapIndexOptions:
    """Tests for BitmapIndexOptions."""

    def test_default_values(self):
        options = BitmapIndexOptions()
        assert options.unique_key == "__key"
        assert options.unique_key_transformation == UniqueKeyTransformation.OBJECT

    def test_custom_values(self):
        options = BitmapIndexOptions(
            unique_key="myKey",
            unique_key_transformation=UniqueKeyTransformation.LONG,
        )
        assert options.unique_key == "myKey"
        assert options.unique_key_transformation == UniqueKeyTransformation.LONG

    def test_empty_unique_key_validation(self):
        with pytest.raises(ConfigurationException) as exc_info:
            BitmapIndexOptions(unique_key="")
        assert "unique_key cannot be empty" in str(exc_info.value)

    def test_from_dict(self):
        data = {"unique_key": "id", "unique_key_transformation": "RAW"}
        options = BitmapIndexOptions.from_dict(data)
        assert options.unique_key == "id"
        assert options.unique_key_transformation == UniqueKeyTransformation.RAW

    def test_from_dict_invalid_transformation(self):
        data = {"unique_key_transformation": "INVALID"}
        with pytest.raises(ConfigurationException) as exc_info:
            BitmapIndexOptions.from_dict(data)
        assert "Invalid unique_key_transformation" in str(exc_info.value)


class TestIndexConfig:
    """Tests for IndexConfig."""

    def test_default_values(self):
        config = IndexConfig(attributes=["name"])
        assert config.name is None
        assert config.type == IndexType.SORTED
        assert config.attributes == ["name"]
        assert config.bitmap_index_options is None

    def test_custom_values(self):
        config = IndexConfig(
            name="my-index",
            type=IndexType.HASH,
            attributes=["field1", "field2"],
        )
        assert config.name == "my-index"
        assert config.type == IndexType.HASH
        assert config.attributes == ["field1", "field2"]

    def test_empty_attributes_validation(self):
        with pytest.raises(ConfigurationException) as exc_info:
            IndexConfig(attributes=[])
        assert "At least one attribute" in str(exc_info.value)

    def test_add_attribute(self):
        config = IndexConfig(attributes=["field1"])
        config.add_attribute("field2")
        assert "field2" in config.attributes

    def test_bitmap_index_with_options(self):
        options = BitmapIndexOptions(unique_key="id")
        config = IndexConfig(
            type=IndexType.BITMAP,
            attributes=["status"],
            bitmap_index_options=options,
        )
        assert config.type == IndexType.BITMAP
        assert config.bitmap_index_options.unique_key == "id"

    def test_from_dict(self):
        data = {
            "name": "idx",
            "type": "HASH",
            "attributes": ["col1"],
        }
        config = IndexConfig.from_dict(data)
        assert config.name == "idx"
        assert config.type == IndexType.HASH
        assert config.attributes == ["col1"]

    def test_from_dict_with_bitmap_options(self):
        data = {
            "type": "BITMAP",
            "attributes": ["flag"],
            "bitmap_index_options": {"unique_key": "pk"},
        }
        config = IndexConfig.from_dict(data)
        assert config.type == IndexType.BITMAP
        assert config.bitmap_index_options.unique_key == "pk"

    def test_from_dict_invalid_type(self):
        data = {"type": "INVALID", "attributes": ["a"]}
        with pytest.raises(ConfigurationException) as exc_info:
            IndexConfig.from_dict(data)
        assert "Invalid index type" in str(exc_info.value)


class TestQueryCacheConfig:
    """Tests for QueryCacheConfig."""

    def test_default_values(self):
        config = QueryCacheConfig()
        assert config.name == "default"
        assert config.predicate is None
        assert config.batch_size == 1
        assert config.buffer_size == 16
        assert config.delay_seconds == 0
        assert config.in_memory_format == InMemoryFormat.BINARY
        assert config.include_value is True
        assert config.populate is True
        assert config.coalesce is False
        assert config.eviction_max_size == 10000
        assert config.eviction_policy == EvictionPolicy.LRU

    def test_custom_values(self):
        config = QueryCacheConfig(
            name="my-cache",
            predicate="active = true",
            batch_size=100,
            buffer_size=32,
            delay_seconds=5,
            in_memory_format=InMemoryFormat.OBJECT,
            include_value=False,
            populate=False,
            coalesce=True,
        )
        assert config.name == "my-cache"
        assert config.predicate == "active = true"
        assert config.batch_size == 100
        assert config.in_memory_format == InMemoryFormat.OBJECT

    def test_empty_name_validation(self):
        with pytest.raises(ConfigurationException) as exc_info:
            QueryCacheConfig(name="")
        assert "name cannot be empty" in str(exc_info.value)

    def test_invalid_batch_size(self):
        with pytest.raises(ConfigurationException) as exc_info:
            QueryCacheConfig(batch_size=0)
        assert "batch_size must be at least 1" in str(exc_info.value)

    def test_invalid_buffer_size(self):
        with pytest.raises(ConfigurationException) as exc_info:
            QueryCacheConfig(buffer_size=0)
        assert "buffer_size must be at least 1" in str(exc_info.value)

    def test_negative_delay_seconds(self):
        with pytest.raises(ConfigurationException) as exc_info:
            QueryCacheConfig(delay_seconds=-1)
        assert "delay_seconds cannot be negative" in str(exc_info.value)

    def test_invalid_eviction_max_size(self):
        with pytest.raises(ConfigurationException) as exc_info:
            QueryCacheConfig(eviction_max_size=0)
        assert "eviction_max_size must be positive" in str(exc_info.value)

    def test_from_dict(self):
        data = {
            "predicate": "status = 1",
            "batch_size": 50,
            "buffer_size": 64,
            "in_memory_format": "OBJECT",
            "coalesce": True,
            "eviction_policy": "LFU",
        }
        config = QueryCacheConfig.from_dict("test-qc", data)
        assert config.name == "test-qc"
        assert config.predicate == "status = 1"
        assert config.batch_size == 50
        assert config.in_memory_format == InMemoryFormat.OBJECT
        assert config.coalesce is True
        assert config.eviction_policy == EvictionPolicy.LFU

    def test_from_dict_invalid_in_memory_format(self):
        data = {"in_memory_format": "INVALID"}
        with pytest.raises(ConfigurationException) as exc_info:
            QueryCacheConfig.from_dict("test", data)
        assert "Invalid in_memory_format" in str(exc_info.value)


class TestWanReplicationConfig:
    """Tests for WanReplicationConfig."""

    def test_default_values(self):
        config = WanReplicationConfig()
        assert config.cluster_name == "dev"
        assert config.endpoints == []
        assert config.queue_capacity == 10000
        assert config.batch_size == 500
        assert config.batch_max_delay_millis == 1000
        assert config.response_timeout_millis == 60000
        assert config.acknowledge_type == "ACK_ON_OPERATION_COMPLETE"

    def test_custom_values(self):
        config = WanReplicationConfig(
            cluster_name="remote-dc",
            endpoints=["10.0.0.1:5701", "10.0.0.2:5701"],
            queue_capacity=50000,
            batch_size=1000,
            acknowledge_type="ACK_ON_RECEIPT",
        )
        assert config.cluster_name == "remote-dc"
        assert len(config.endpoints) == 2
        assert config.queue_capacity == 50000
        assert config.acknowledge_type == "ACK_ON_RECEIPT"

    def test_empty_cluster_name_validation(self):
        with pytest.raises(ConfigurationException) as exc_info:
            WanReplicationConfig(cluster_name="")
        assert "cluster_name cannot be empty" in str(exc_info.value)

    def test_invalid_queue_capacity(self):
        with pytest.raises(ConfigurationException) as exc_info:
            WanReplicationConfig(queue_capacity=0)
        assert "queue_capacity must be positive" in str(exc_info.value)

    def test_invalid_batch_size(self):
        with pytest.raises(ConfigurationException) as exc_info:
            WanReplicationConfig(batch_size=0)
        assert "batch_size must be positive" in str(exc_info.value)

    def test_negative_batch_max_delay(self):
        with pytest.raises(ConfigurationException) as exc_info:
            WanReplicationConfig(batch_max_delay_millis=-1)
        assert "batch_max_delay_millis cannot be negative" in str(exc_info.value)

    def test_invalid_response_timeout(self):
        with pytest.raises(ConfigurationException) as exc_info:
            WanReplicationConfig(response_timeout_millis=0)
        assert "response_timeout_millis must be positive" in str(exc_info.value)

    def test_invalid_acknowledge_type(self):
        with pytest.raises(ConfigurationException) as exc_info:
            WanReplicationConfig(acknowledge_type="INVALID")
        assert "Invalid acknowledge_type" in str(exc_info.value)

    def test_add_endpoint(self):
        config = WanReplicationConfig()
        config.add_endpoint("node1:5701")
        config.add_endpoint("node2:5701")
        assert len(config.endpoints) == 2
        assert "node1:5701" in config.endpoints

    def test_from_dict(self):
        data = {
            "cluster_name": "wan-cluster",
            "endpoints": ["host1:5701"],
            "queue_capacity": 20000,
            "batch_size": 250,
            "acknowledge_type": "ACK_ON_RECEIPT",
        }
        config = WanReplicationConfig.from_dict(data)
        assert config.cluster_name == "wan-cluster"
        assert config.endpoints == ["host1:5701"]
        assert config.queue_capacity == 20000
        assert config.acknowledge_type == "ACK_ON_RECEIPT"


class TestClientUserCodeDeploymentConfig:
    """Tests for ClientUserCodeDeploymentConfig."""

    def test_default_values(self):
        config = ClientUserCodeDeploymentConfig()
        assert config.enabled is False
        assert config.class_names == []
        assert config.jar_paths == []

    def test_custom_values(self):
        config = ClientUserCodeDeploymentConfig(
            enabled=True,
            class_names=["com.example.MyClass"],
            jar_paths=["/path/to/my.jar"],
        )
        assert config.enabled is True
        assert "com.example.MyClass" in config.class_names
        assert "/path/to/my.jar" in config.jar_paths

    def test_add_class_name(self):
        config = ClientUserCodeDeploymentConfig()
        config.add_class_name("com.example.Class1")
        config.add_class_name("com.example.Class2")
        assert len(config.class_names) == 2

    def test_add_jar_path(self):
        config = ClientUserCodeDeploymentConfig()
        config.add_jar_path("/path/a.jar")
        config.add_jar_path("/path/b.jar")
        assert len(config.jar_paths) == 2

    def test_fluent_api(self):
        config = (
            ClientUserCodeDeploymentConfig()
            .add_class_name("com.example.A")
            .add_jar_path("/path/x.jar")
        )
        assert "com.example.A" in config.class_names
        assert "/path/x.jar" in config.jar_paths

    def test_from_dict(self):
        data = {
            "enabled": True,
            "class_names": ["com.example.Foo", "com.example.Bar"],
            "jar_paths": ["/lib/foo.jar"],
        }
        config = ClientUserCodeDeploymentConfig.from_dict(data)
        assert config.enabled is True
        assert len(config.class_names) == 2
        assert config.jar_paths == ["/lib/foo.jar"]


class TestNearCacheConfigExtended:
    """Extended tests for NearCacheConfig with new options."""

    def test_new_default_values(self):
        config = NearCacheConfig()
        assert config.serialize_keys is False
        assert config.local_update_policy == LocalUpdatePolicy.INVALIDATE
        assert config.preloader_enabled is False
        assert config.preloader_directory == ""
        assert config.preloader_store_initial_delay_seconds == 600
        assert config.preloader_store_interval_seconds == 600

    def test_serialize_keys(self):
        config = NearCacheConfig(serialize_keys=True)
        assert config.serialize_keys is True

    def test_local_update_policy(self):
        config = NearCacheConfig(local_update_policy=LocalUpdatePolicy.CACHE_ON_UPDATE)
        assert config.local_update_policy == LocalUpdatePolicy.CACHE_ON_UPDATE

    def test_preloader_options(self):
        config = NearCacheConfig(
            preloader_enabled=True,
            preloader_directory="/tmp/preload",
            preloader_store_initial_delay_seconds=300,
            preloader_store_interval_seconds=120,
        )
        assert config.preloader_enabled is True
        assert config.preloader_directory == "/tmp/preload"
        assert config.preloader_store_initial_delay_seconds == 300
        assert config.preloader_store_interval_seconds == 120

    def test_negative_preloader_initial_delay_validation(self):
        with pytest.raises(ConfigurationException) as exc_info:
            NearCacheConfig(preloader_store_initial_delay_seconds=-1)
        assert "preloader_store_initial_delay_seconds cannot be negative" in str(
            exc_info.value
        )

    def test_negative_preloader_interval_validation(self):
        with pytest.raises(ConfigurationException) as exc_info:
            NearCacheConfig(preloader_store_interval_seconds=-1)
        assert "preloader_store_interval_seconds cannot be negative" in str(
            exc_info.value
        )

    def test_from_dict_with_new_options(self):
        data = {
            "serialize_keys": True,
            "local_update_policy": "CACHE_ON_UPDATE",
            "preloader_enabled": True,
            "preloader_directory": "/data/near-cache",
            "preloader_store_initial_delay_seconds": 60,
            "preloader_store_interval_seconds": 30,
        }
        config = NearCacheConfig.from_dict("extended-nc", data)
        assert config.name == "extended-nc"
        assert config.serialize_keys is True
        assert config.local_update_policy == LocalUpdatePolicy.CACHE_ON_UPDATE
        assert config.preloader_enabled is True
        assert config.preloader_directory == "/data/near-cache"
        assert config.preloader_store_initial_delay_seconds == 60
        assert config.preloader_store_interval_seconds == 30

    def test_from_dict_invalid_local_update_policy(self):
        data = {"local_update_policy": "INVALID"}
        with pytest.raises(ConfigurationException) as exc_info:
            NearCacheConfig.from_dict("test", data)
        assert "Invalid local_update_policy" in str(exc_info.value)
