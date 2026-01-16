"""Unit tests for hazelcast.config module."""

import pytest
import tempfile
import os

from hazelcast.config import (
    ClientConfig,
    NetworkConfig,
    SecurityConfig,
    ConnectionStrategyConfig,
    SerializationConfig,
    NearCacheConfig,
    RetryConfig,
    IndexConfig,
    QueryCacheConfig,
    WanReplicationConfig,
    ClientUserCodeDeploymentConfig,
    BitmapIndexOptions,
    ReconnectMode,
    EvictionPolicy,
    InMemoryFormat,
    IndexType,
    LocalUpdatePolicy,
    UniqueKeyTransformation,
)
from hazelcast.exceptions import ConfigurationException
from hazelcast.network.ssl_config import TlsConfig


class TestRetryConfig:
    """Tests for RetryConfig."""

    def test_defaults(self):
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
            jitter=0.1,
        )
        assert config.initial_backoff == 0.5
        assert config.max_backoff == 60.0
        assert config.multiplier == 1.5
        assert config.jitter == 0.1

    def test_invalid_initial_backoff(self):
        with pytest.raises(ConfigurationException):
            RetryConfig(initial_backoff=0)
        with pytest.raises(ConfigurationException):
            RetryConfig(initial_backoff=-1)

    def test_invalid_max_backoff(self):
        with pytest.raises(ConfigurationException):
            RetryConfig(initial_backoff=10, max_backoff=5)

    def test_invalid_multiplier(self):
        with pytest.raises(ConfigurationException):
            RetryConfig(multiplier=0.5)

    def test_invalid_jitter(self):
        with pytest.raises(ConfigurationException):
            RetryConfig(jitter=-0.1)
        with pytest.raises(ConfigurationException):
            RetryConfig(jitter=1.1)

    def test_from_dict(self):
        config = RetryConfig.from_dict({
            "initial_backoff": 2.0,
            "max_backoff": 45.0,
            "multiplier": 3.0,
            "jitter": 0.2,
        })
        assert config.initial_backoff == 2.0
        assert config.max_backoff == 45.0

    def test_setters_with_validation(self):
        config = RetryConfig()
        config.initial_backoff = 2.0
        assert config.initial_backoff == 2.0
        
        with pytest.raises(ConfigurationException):
            config.initial_backoff = 0


class TestNetworkConfig:
    """Tests for NetworkConfig."""

    def test_defaults(self):
        config = NetworkConfig()
        assert config.addresses == ["localhost:5701"]
        assert config.connection_timeout == 5.0
        assert config.smart_routing is True

    def test_custom_addresses(self):
        config = NetworkConfig(addresses=["node1:5701", "node2:5701"])
        assert len(config.addresses) == 2
        assert "node1:5701" in config.addresses

    def test_empty_addresses_raises(self):
        with pytest.raises(ConfigurationException):
            NetworkConfig(addresses=[])

    def test_invalid_timeout(self):
        with pytest.raises(ConfigurationException):
            NetworkConfig(connection_timeout=0)
        with pytest.raises(ConfigurationException):
            NetworkConfig(connection_timeout=-1)

    def test_from_dict(self):
        config = NetworkConfig.from_dict({
            "addresses": ["server:5701"],
            "connection_timeout": 10.0,
            "smart_routing": False,
        })
        assert config.addresses == ["server:5701"]
        assert config.connection_timeout == 10.0
        assert config.smart_routing is False

    def test_setters(self):
        config = NetworkConfig()
        config.addresses = ["new:5701"]
        config.connection_timeout = 15.0
        config.smart_routing = False
        
        assert config.addresses == ["new:5701"]
        assert config.connection_timeout == 15.0
        assert config.smart_routing is False


class TestConnectionStrategyConfig:
    """Tests for ConnectionStrategyConfig."""

    def test_defaults(self):
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
        config = ConnectionStrategyConfig.from_dict({
            "async_start": True,
            "reconnect_mode": "OFF",
            "retry": {"initial_backoff": 3.0},
        })
        assert config.async_start is True
        assert config.reconnect_mode == ReconnectMode.OFF
        assert config.retry.initial_backoff == 3.0

    def test_invalid_reconnect_mode(self):
        with pytest.raises(ConfigurationException):
            ConnectionStrategyConfig.from_dict({"reconnect_mode": "INVALID"})


class TestSecurityConfig:
    """Tests for SecurityConfig."""

    def test_defaults(self):
        config = SecurityConfig()
        assert config.username is None
        assert config.password is None
        assert config.token is None
        assert config.is_configured is False

    def test_with_credentials(self):
        config = SecurityConfig(username="admin", password="secret")
        assert config.username == "admin"
        assert config.password == "secret"
        assert config.is_configured is True

    def test_with_token(self):
        config = SecurityConfig(token="my-token")
        assert config.token == "my-token"
        assert config.is_configured is True

    def test_tls_config(self):
        tls = TlsConfig(enabled=True)
        config = SecurityConfig(tls=tls)
        assert config.tls_enabled is True

    def test_from_dict(self):
        config = SecurityConfig.from_dict({
            "username": "user",
            "password": "pass",
            "token": "tok",
        })
        assert config.username == "user"
        assert config.password == "pass"
        assert config.token == "tok"


class TestBitmapIndexOptions:
    """Tests for BitmapIndexOptions."""

    def test_defaults(self):
        options = BitmapIndexOptions()
        assert options.unique_key == "__key"
        assert options.unique_key_transformation == UniqueKeyTransformation.OBJECT

    def test_custom_values(self):
        options = BitmapIndexOptions(
            unique_key="id",
            unique_key_transformation=UniqueKeyTransformation.LONG,
        )
        assert options.unique_key == "id"
        assert options.unique_key_transformation == UniqueKeyTransformation.LONG

    def test_empty_unique_key_raises(self):
        with pytest.raises(ConfigurationException):
            BitmapIndexOptions(unique_key="")

    def test_from_dict(self):
        options = BitmapIndexOptions.from_dict({
            "unique_key": "myKey",
            "unique_key_transformation": "RAW",
        })
        assert options.unique_key == "myKey"
        assert options.unique_key_transformation == UniqueKeyTransformation.RAW


class TestIndexConfig:
    """Tests for IndexConfig."""

    def test_defaults(self):
        config = IndexConfig(attributes=["name"])
        assert config.name is None
        assert config.type == IndexType.SORTED
        assert config.attributes == ["name"]

    def test_custom_values(self):
        config = IndexConfig(
            name="my_index",
            type=IndexType.HASH,
            attributes=["a", "b"],
        )
        assert config.name == "my_index"
        assert config.type == IndexType.HASH
        assert len(config.attributes) == 2

    def test_empty_attributes_raises(self):
        with pytest.raises(ConfigurationException):
            IndexConfig(attributes=[])

    def test_add_attribute(self):
        config = IndexConfig(attributes=["a"])
        config.add_attribute("b")
        assert "b" in config.attributes

    def test_with_bitmap_options(self):
        bitmap = BitmapIndexOptions(unique_key="id")
        config = IndexConfig(
            attributes=["status"],
            type=IndexType.BITMAP,
            bitmap_index_options=bitmap,
        )
        assert config.bitmap_index_options is not None
        assert config.bitmap_index_options.unique_key == "id"

    def test_from_dict(self):
        config = IndexConfig.from_dict({
            "name": "idx",
            "type": "HASH",
            "attributes": ["x", "y"],
        })
        assert config.name == "idx"
        assert config.type == IndexType.HASH


class TestQueryCacheConfig:
    """Tests for QueryCacheConfig."""

    def test_defaults(self):
        config = QueryCacheConfig()
        assert config.name == "default"
        assert config.batch_size == 1
        assert config.buffer_size == 16
        assert config.include_value is True
        assert config.populate is True
        assert config.coalesce is False
        assert config.eviction_policy == EvictionPolicy.LRU

    def test_custom_values(self):
        config = QueryCacheConfig(
            name="my-cache",
            predicate="age > 18",
            batch_size=10,
            eviction_max_size=5000,
        )
        assert config.name == "my-cache"
        assert config.predicate == "age > 18"
        assert config.batch_size == 10
        assert config.eviction_max_size == 5000

    def test_invalid_batch_size(self):
        with pytest.raises(ConfigurationException):
            QueryCacheConfig(batch_size=0)

    def test_invalid_buffer_size(self):
        with pytest.raises(ConfigurationException):
            QueryCacheConfig(buffer_size=0)

    def test_invalid_delay_seconds(self):
        with pytest.raises(ConfigurationException):
            QueryCacheConfig(delay_seconds=-1)

    def test_from_dict(self):
        config = QueryCacheConfig.from_dict("test", {
            "predicate": "active = true",
            "batch_size": 5,
            "in_memory_format": "OBJECT",
        })
        assert config.name == "test"
        assert config.predicate == "active = true"
        assert config.in_memory_format == InMemoryFormat.OBJECT


class TestWanReplicationConfig:
    """Tests for WanReplicationConfig."""

    def test_defaults(self):
        config = WanReplicationConfig()
        assert config.cluster_name == "dev"
        assert config.endpoints == []
        assert config.queue_capacity == 10000
        assert config.batch_size == 500

    def test_custom_values(self):
        config = WanReplicationConfig(
            cluster_name="target",
            endpoints=["target1:5701"],
            queue_capacity=20000,
        )
        assert config.cluster_name == "target"
        assert len(config.endpoints) == 1

    def test_empty_cluster_name_raises(self):
        with pytest.raises(ConfigurationException):
            WanReplicationConfig(cluster_name="")

    def test_invalid_queue_capacity(self):
        with pytest.raises(ConfigurationException):
            WanReplicationConfig(queue_capacity=0)

    def test_invalid_acknowledge_type(self):
        with pytest.raises(ConfigurationException):
            WanReplicationConfig(acknowledge_type="INVALID")

    def test_add_endpoint(self):
        config = WanReplicationConfig()
        config.add_endpoint("node:5701")
        assert "node:5701" in config.endpoints

    def test_from_dict(self):
        config = WanReplicationConfig.from_dict({
            "cluster_name": "dr",
            "endpoints": ["dr1:5701", "dr2:5701"],
        })
        assert config.cluster_name == "dr"
        assert len(config.endpoints) == 2


class TestClientUserCodeDeploymentConfig:
    """Tests for ClientUserCodeDeploymentConfig."""

    def test_defaults(self):
        config = ClientUserCodeDeploymentConfig()
        assert config.enabled is False
        assert config.class_names == []
        assert config.jar_paths == []

    def test_custom_values(self):
        config = ClientUserCodeDeploymentConfig(
            enabled=True,
            class_names=["com.example.MyClass"],
            jar_paths=["/path/to/jar.jar"],
        )
        assert config.enabled is True
        assert len(config.class_names) == 1
        assert len(config.jar_paths) == 1

    def test_add_class_name(self):
        config = ClientUserCodeDeploymentConfig()
        config.add_class_name("MyClass")
        assert "MyClass" in config.class_names

    def test_add_jar_path(self):
        config = ClientUserCodeDeploymentConfig()
        config.add_jar_path("/path/to/jar")
        assert "/path/to/jar" in config.jar_paths

    def test_from_dict(self):
        config = ClientUserCodeDeploymentConfig.from_dict({
            "enabled": True,
            "class_names": ["A", "B"],
        })
        assert config.enabled is True
        assert len(config.class_names) == 2


class TestNearCacheConfig:
    """Tests for NearCacheConfig."""

    def test_defaults(self):
        config = NearCacheConfig()
        assert config.name == "default"
        assert config.max_size == 10000
        assert config.eviction_policy == EvictionPolicy.LRU
        assert config.invalidate_on_change is True

    def test_custom_values(self):
        config = NearCacheConfig(
            name="my-cache",
            max_idle_seconds=60,
            time_to_live_seconds=300,
            max_size=5000,
        )
        assert config.name == "my-cache"
        assert config.max_idle_seconds == 60
        assert config.time_to_live_seconds == 300
        assert config.max_size == 5000

    def test_empty_name_raises(self):
        with pytest.raises(ConfigurationException):
            NearCacheConfig(name="")

    def test_negative_max_idle_raises(self):
        with pytest.raises(ConfigurationException):
            NearCacheConfig(max_idle_seconds=-1)

    def test_negative_ttl_raises(self):
        with pytest.raises(ConfigurationException):
            NearCacheConfig(time_to_live_seconds=-1)

    def test_invalid_max_size(self):
        with pytest.raises(ConfigurationException):
            NearCacheConfig(max_size=0)

    def test_from_dict(self):
        config = NearCacheConfig.from_dict("test", {
            "max_size": 2000,
            "eviction_policy": "LFU",
            "in_memory_format": "OBJECT",
        })
        assert config.name == "test"
        assert config.max_size == 2000
        assert config.eviction_policy == EvictionPolicy.LFU
        assert config.in_memory_format == InMemoryFormat.OBJECT


class TestSerializationConfig:
    """Tests for SerializationConfig."""

    def test_defaults(self):
        config = SerializationConfig()
        assert config.portable_version == 0
        assert config.default_integer_type == "INT"

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
        config = SerializationConfig.from_dict({
            "portable_version": 1,
            "default_integer_type": "LONG",
        })
        assert config.portable_version == 1
        assert config.default_integer_type == "LONG"


class TestClientConfig:
    """Tests for ClientConfig."""

    def test_defaults(self):
        config = ClientConfig()
        assert config.cluster_name == "dev"
        assert config.client_name is None
        assert config.cluster_members == ["localhost:5701"]
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
        config.cluster_members = ["node1:5701", "node2:5701"]
        assert config.network.addresses == ["node1:5701", "node2:5701"]

    def test_connection_timeout_alias(self):
        config = ClientConfig()
        config.connection_timeout = 10.0
        assert config.network.connection_timeout == 10.0

    def test_credentials_getter_none(self):
        config = ClientConfig()
        assert config.credentials is None

    def test_credentials_getter_with_user(self):
        config = ClientConfig()
        config.security.username = "admin"
        config.security.password = "pass"
        creds = config.credentials
        assert creds["username"] == "admin"
        assert creds["password"] == "pass"

    def test_credentials_setter(self):
        config = ClientConfig()
        config.credentials = {"username": "user", "password": "secret"}
        assert config.security.username == "user"
        assert config.security.password == "secret"

    def test_add_near_cache(self):
        config = ClientConfig()
        nc = NearCacheConfig(name="my-map")
        config.add_near_cache(nc)
        assert "my-map" in config.near_caches

    def test_labels(self):
        config = ClientConfig()
        config.labels = ["label1", "label2"]
        assert len(config.labels) == 2

    def test_from_dict(self):
        config = ClientConfig.from_dict({
            "cluster_name": "test-cluster",
            "client_name": "test-client",
            "network": {
                "addresses": ["server:5701"],
                "connection_timeout": 15.0,
            },
            "labels": ["env:test"],
        })
        assert config.cluster_name == "test-cluster"
        assert config.client_name == "test-client"
        assert config.cluster_members == ["server:5701"]
        assert "env:test" in config.labels

    def test_from_yaml_string(self):
        yaml_content = """
        cluster_name: yaml-cluster
        network:
          addresses:
            - node1:5701
        """
        try:
            config = ClientConfig.from_yaml_string(yaml_content)
            assert config.cluster_name == "yaml-cluster"
        except ConfigurationException as e:
            if "PyYAML" in str(e):
                pytest.skip("PyYAML not installed")

    def test_from_yaml_file_not_found(self):
        with pytest.raises(ConfigurationException) as exc_info:
            ClientConfig.from_yaml("/nonexistent/path.yml")
        assert "not found" in str(exc_info.value)

    def test_from_yaml_file(self):
        try:
            import yaml
        except ImportError:
            pytest.skip("PyYAML not installed")
            
        yaml_content = """
        hazelcast_client:
          cluster_name: file-cluster
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
            f.write(yaml_content)
            f.flush()
            try:
                config = ClientConfig.from_yaml(f.name)
                assert config.cluster_name == "file-cluster"
            finally:
                os.unlink(f.name)
