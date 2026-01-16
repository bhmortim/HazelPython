"""Comprehensive unit tests for hazelcast/config.py."""

import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock

from hazelcast.config import (
    DiscoveryStrategyType,
    DiscoveryConfig,
    SplitBrainProtectionOn,
    SplitBrainProtectionFunctionType,
    SplitBrainProtectionEvent,
    SplitBrainProtectionListener,
    SplitBrainProtectionFunction,
    SplitBrainProtectionConfig,
    InitialLoadMode,
    MapStoreConfig,
    ReconnectMode,
    EvictionPolicy,
    InMemoryFormat,
    IndexType,
    LocalUpdatePolicy,
    UniqueKeyTransformation,
    RetryConfig,
    NetworkConfig,
    ConnectionStrategyConfig,
    SecurityConfig,
    BitmapIndexOptions,
    IndexConfig,
    QueryCacheConfig,
    WanReplicationConfig,
    ClientUserCodeDeploymentConfig,
    StatisticsConfig,
    EventJournalConfig,
    NearCacheConfig,
    SerializationConfig,
    ClientConfig,
)
from hazelcast.exceptions import ConfigurationException
from hazelcast.network.ssl_config import TlsConfig


class TestDiscoveryStrategyType:
    def test_enum_values(self):
        assert DiscoveryStrategyType.AWS.value == "AWS"
        assert DiscoveryStrategyType.AZURE.value == "AZURE"
        assert DiscoveryStrategyType.GCP.value == "GCP"
        assert DiscoveryStrategyType.KUBERNETES.value == "KUBERNETES"
        assert DiscoveryStrategyType.CLOUD.value == "CLOUD"
        assert DiscoveryStrategyType.MULTICAST.value == "MULTICAST"


class TestDiscoveryConfig:
    def test_default_init(self):
        config = DiscoveryConfig()
        assert config.enabled is False
        assert config.strategy_type is None
        assert config.aws is None
        assert config.azure is None
        assert config.gcp is None
        assert config.kubernetes is None
        assert config.cloud is None
        assert config.multicast is None

    def test_init_with_params(self):
        config = DiscoveryConfig(enabled=True, strategy_type=DiscoveryStrategyType.AWS)
        assert config.enabled is True
        assert config.strategy_type == DiscoveryStrategyType.AWS

    def test_enabled_setter(self):
        config = DiscoveryConfig()
        config.enabled = True
        assert config.enabled is True

    def test_strategy_type_setter(self):
        config = DiscoveryConfig()
        config.strategy_type = DiscoveryStrategyType.GCP
        assert config.strategy_type == DiscoveryStrategyType.GCP

    def test_aws_setter_auto_strategy(self):
        config = DiscoveryConfig()
        config.aws = {"region": "us-west-2"}
        assert config.aws == {"region": "us-west-2"}
        assert config.strategy_type == DiscoveryStrategyType.AWS

    def test_aws_setter_no_override(self):
        config = DiscoveryConfig(strategy_type=DiscoveryStrategyType.GCP)
        config.aws = {"region": "us-west-2"}
        assert config.strategy_type == DiscoveryStrategyType.GCP

    def test_azure_setter_auto_strategy(self):
        config = DiscoveryConfig()
        config.azure = {"subscription_id": "sub-123"}
        assert config.azure == {"subscription_id": "sub-123"}
        assert config.strategy_type == DiscoveryStrategyType.AZURE

    def test_gcp_setter_auto_strategy(self):
        config = DiscoveryConfig()
        config.gcp = {"project": "my-project"}
        assert config.gcp == {"project": "my-project"}
        assert config.strategy_type == DiscoveryStrategyType.GCP

    def test_kubernetes_setter_auto_strategy(self):
        config = DiscoveryConfig()
        config.kubernetes = {"namespace": "default"}
        assert config.kubernetes == {"namespace": "default"}
        assert config.strategy_type == DiscoveryStrategyType.KUBERNETES

    def test_cloud_setter_auto_strategy(self):
        config = DiscoveryConfig()
        config.cloud = {"cluster_name": "prod"}
        assert config.cloud == {"cluster_name": "prod"}
        assert config.strategy_type == DiscoveryStrategyType.CLOUD

    def test_multicast_setter_auto_strategy(self):
        config = DiscoveryConfig()
        config.multicast = {"group": "224.0.0.1"}
        assert config.multicast == {"group": "224.0.0.1"}
        assert config.strategy_type == DiscoveryStrategyType.MULTICAST

    def test_create_strategy_not_enabled(self):
        config = DiscoveryConfig(enabled=False)
        with pytest.raises(ConfigurationException, match="Discovery is not enabled"):
            config.create_strategy()

    def test_create_strategy_no_type(self):
        config = DiscoveryConfig(enabled=True)
        with pytest.raises(ConfigurationException, match="Discovery strategy type is not set"):
            config.create_strategy()

    @patch("hazelcast.config.AwsDiscoveryStrategy")
    @patch("hazelcast.config.AwsConfig")
    def test_create_strategy_aws(self, mock_aws_config, mock_aws_strategy):
        config = DiscoveryConfig(enabled=True, strategy_type=DiscoveryStrategyType.AWS)
        config.aws = {"region": "us-west-2"}
        mock_aws_config.from_dict.return_value = MagicMock()
        mock_aws_strategy.return_value = MagicMock()
        
        strategy = config.create_strategy()
        
        mock_aws_config.from_dict.assert_called_once_with({"region": "us-west-2"})
        mock_aws_strategy.assert_called_once()

    @patch("hazelcast.config.AzureDiscoveryStrategy")
    @patch("hazelcast.config.AzureConfig")
    def test_create_strategy_azure(self, mock_azure_config, mock_azure_strategy):
        config = DiscoveryConfig(enabled=True, strategy_type=DiscoveryStrategyType.AZURE)
        config.azure = {"subscription_id": "sub-123"}
        mock_azure_config.from_dict.return_value = MagicMock()
        mock_azure_strategy.return_value = MagicMock()
        
        strategy = config.create_strategy()
        
        mock_azure_config.from_dict.assert_called_once()
        mock_azure_strategy.assert_called_once()

    @patch("hazelcast.config.GcpDiscoveryStrategy")
    @patch("hazelcast.config.GcpConfig")
    def test_create_strategy_gcp(self, mock_gcp_config, mock_gcp_strategy):
        config = DiscoveryConfig(enabled=True, strategy_type=DiscoveryStrategyType.GCP)
        mock_gcp_config.from_dict.return_value = MagicMock()
        mock_gcp_strategy.return_value = MagicMock()
        
        strategy = config.create_strategy()
        
        mock_gcp_config.from_dict.assert_called_once()
        mock_gcp_strategy.assert_called_once()

    @patch("hazelcast.config.KubernetesDiscoveryStrategy")
    @patch("hazelcast.config.KubernetesConfig")
    def test_create_strategy_kubernetes(self, mock_k8s_config, mock_k8s_strategy):
        config = DiscoveryConfig(enabled=True, strategy_type=DiscoveryStrategyType.KUBERNETES)
        mock_k8s_config.from_dict.return_value = MagicMock()
        mock_k8s_strategy.return_value = MagicMock()
        
        strategy = config.create_strategy()
        
        mock_k8s_config.from_dict.assert_called_once()
        mock_k8s_strategy.assert_called_once()

    @patch("hazelcast.config.HazelcastCloudDiscovery")
    @patch("hazelcast.config.CloudConfig")
    def test_create_strategy_cloud(self, mock_cloud_config, mock_cloud_strategy):
        config = DiscoveryConfig(enabled=True, strategy_type=DiscoveryStrategyType.CLOUD)
        mock_cloud_config.from_dict.return_value = MagicMock()
        mock_cloud_strategy.return_value = MagicMock()
        
        strategy = config.create_strategy()
        
        mock_cloud_config.from_dict.assert_called_once()
        mock_cloud_strategy.assert_called_once()

    @patch("hazelcast.config.MulticastDiscoveryStrategy")
    @patch("hazelcast.config.MulticastConfig")
    def test_create_strategy_multicast(self, mock_multicast_config, mock_multicast_strategy):
        config = DiscoveryConfig(enabled=True, strategy_type=DiscoveryStrategyType.MULTICAST)
        mock_multicast_config.from_dict.return_value = MagicMock()
        mock_multicast_strategy.return_value = MagicMock()
        
        strategy = config.create_strategy()
        
        mock_multicast_config.from_dict.assert_called_once()
        mock_multicast_strategy.assert_called_once()

    def test_from_dict_basic(self):
        data = {"enabled": True}
        config = DiscoveryConfig.from_dict(data)
        assert config.enabled is True

    def test_from_dict_with_strategy_type(self):
        data = {"enabled": True, "strategy_type": "aws"}
        config = DiscoveryConfig.from_dict(data)
        assert config.strategy_type == DiscoveryStrategyType.AWS

    def test_from_dict_invalid_strategy_type(self):
        data = {"enabled": True, "strategy_type": "invalid"}
        with pytest.raises(ConfigurationException, match="Invalid discovery strategy type"):
            DiscoveryConfig.from_dict(data)

    def test_from_dict_all_providers(self):
        data = {
            "enabled": True,
            "aws": {"region": "us-west-2"},
            "azure": {"subscription_id": "sub"},
            "gcp": {"project": "proj"},
            "kubernetes": {"namespace": "ns"},
            "cloud": {"cluster": "cl"},
            "multicast": {"group": "224.0.0.1"},
        }
        config = DiscoveryConfig.from_dict(data)
        assert config.aws == {"region": "us-west-2"}
        assert config.azure == {"subscription_id": "sub"}
        assert config.gcp == {"project": "proj"}
        assert config.kubernetes == {"namespace": "ns"}
        assert config.cloud == {"cluster": "cl"}
        assert config.multicast == {"group": "224.0.0.1"}


class TestSplitBrainProtectionEnums:
    def test_split_brain_protection_on_values(self):
        assert SplitBrainProtectionOn.READ.value == "READ"
        assert SplitBrainProtectionOn.WRITE.value == "WRITE"
        assert SplitBrainProtectionOn.READ_WRITE.value == "READ_WRITE"

    def test_split_brain_protection_function_type_values(self):
        assert SplitBrainProtectionFunctionType.MEMBER_COUNT.value == "MEMBER_COUNT"
        assert SplitBrainProtectionFunctionType.PROBABILISTIC.value == "PROBABILISTIC"
        assert SplitBrainProtectionFunctionType.RECENTLY_ACTIVE.value == "RECENTLY_ACTIVE"


class TestSplitBrainProtectionEvent:
    def test_init(self):
        event = SplitBrainProtectionEvent(
            name="test-quorum",
            threshold=3,
            current_members=["member1", "member2"],
            is_present=True,
        )
        assert event.name == "test-quorum"
        assert event.threshold == 3
        assert event.current_members == ["member1", "member2"]
        assert event.is_present is True

    def test_current_members_is_copy(self):
        original = ["member1", "member2"]
        event = SplitBrainProtectionEvent("test", 2, original, True)
        original.append("member3")
        assert len(event.current_members) == 2


class TestSplitBrainProtectionConfig:
    def test_default_init(self):
        config = SplitBrainProtectionConfig()
        assert config.name == "default"
        assert config.enabled is True
        assert config.min_cluster_size == 2
        assert config.protect_on == SplitBrainProtectionOn.READ_WRITE
        assert config.function_type == SplitBrainProtectionFunctionType.MEMBER_COUNT
        assert config.function is None
        assert config.listeners == []

    def test_init_with_params(self):
        config = SplitBrainProtectionConfig(
            name="custom",
            enabled=False,
            min_cluster_size=5,
            protect_on=SplitBrainProtectionOn.WRITE,
            function_type=SplitBrainProtectionFunctionType.PROBABILISTIC,
        )
        assert config.name == "custom"
        assert config.enabled is False
        assert config.min_cluster_size == 5
        assert config.protect_on == SplitBrainProtectionOn.WRITE
        assert config.function_type == SplitBrainProtectionFunctionType.PROBABILISTIC

    def test_validate_empty_name(self):
        with pytest.raises(ConfigurationException, match="name cannot be empty"):
            SplitBrainProtectionConfig(name="")

    def test_validate_min_cluster_size(self):
        with pytest.raises(ConfigurationException, match="min_cluster_size must be at least 1"):
            SplitBrainProtectionConfig(min_cluster_size=0)

    def test_setters(self):
        config = SplitBrainProtectionConfig()
        config.enabled = False
        config.min_cluster_size = 5
        config.protect_on = SplitBrainProtectionOn.READ
        config.function_type = SplitBrainProtectionFunctionType.RECENTLY_ACTIVE
        
        assert config.enabled is False
        assert config.min_cluster_size == 5
        assert config.protect_on == SplitBrainProtectionOn.READ
        assert config.function_type == SplitBrainProtectionFunctionType.RECENTLY_ACTIVE

    def test_custom_function(self):
        class CustomFunction(SplitBrainProtectionFunction):
            def apply(self, members):
                return len(members) >= 2

        func = CustomFunction()
        config = SplitBrainProtectionConfig(function=func)
        config.function = func
        assert config.function is func

    def test_add_listener(self):
        class TestListener(SplitBrainProtectionListener):
            def on_present(self, event):
                pass
            def on_absent(self, event):
                pass

        config = SplitBrainProtectionConfig()
        listener = TestListener()
        result = config.add_listener(listener)
        
        assert result is config
        assert listener in config.listeners

    def test_probabilistic_settings(self):
        config = SplitBrainProtectionConfig()
        config.probabilistic_suspected_epsilon_millis = 20000
        config.probabilistic_max_sample_size = 300
        config.probabilistic_acceptable_heartbeat_pause_millis = 120000
        config.probabilistic_heartbeat_interval_millis = 10000
        
        assert config.probabilistic_suspected_epsilon_millis == 20000
        assert config.probabilistic_max_sample_size == 300
        assert config.probabilistic_acceptable_heartbeat_pause_millis == 120000
        assert config.probabilistic_heartbeat_interval_millis == 10000

    def test_recently_active_settings(self):
        config = SplitBrainProtectionConfig()
        config.recently_active_heartbeat_tolerance_millis = 90000
        assert config.recently_active_heartbeat_tolerance_millis == 90000

    def test_validate_probabilistic_epsilon_negative(self):
        config = SplitBrainProtectionConfig()
        with pytest.raises(ConfigurationException, match="probabilistic_suspected_epsilon_millis cannot be negative"):
            config.probabilistic_suspected_epsilon_millis = -1

    def test_validate_probabilistic_sample_size(self):
        config = SplitBrainProtectionConfig()
        with pytest.raises(ConfigurationException, match="probabilistic_max_sample_size must be at least 1"):
            config.probabilistic_max_sample_size = 0

    def test_validate_probabilistic_heartbeat_pause_negative(self):
        config = SplitBrainProtectionConfig()
        with pytest.raises(ConfigurationException, match="probabilistic_acceptable_heartbeat_pause_millis cannot be negative"):
            config.probabilistic_acceptable_heartbeat_pause_millis = -1

    def test_validate_probabilistic_heartbeat_interval(self):
        config = SplitBrainProtectionConfig()
        with pytest.raises(ConfigurationException, match="probabilistic_heartbeat_interval_millis must be positive"):
            config.probabilistic_heartbeat_interval_millis = 0

    def test_validate_recently_active_tolerance_negative(self):
        config = SplitBrainProtectionConfig()
        with pytest.raises(ConfigurationException, match="recently_active_heartbeat_tolerance_millis cannot be negative"):
            config.recently_active_heartbeat_tolerance_millis = -1

    def test_from_dict(self):
        data = {
            "enabled": True,
            "min_cluster_size": 4,
            "protect_on": "WRITE",
            "function_type": "PROBABILISTIC",
            "probabilistic_suspected_epsilon_millis": 15000,
            "probabilistic_max_sample_size": 250,
        }
        config = SplitBrainProtectionConfig.from_dict("test-quorum", data)
        
        assert config.name == "test-quorum"
        assert config.enabled is True
        assert config.min_cluster_size == 4
        assert config.protect_on == SplitBrainProtectionOn.WRITE
        assert config.function_type == SplitBrainProtectionFunctionType.PROBABILISTIC
        assert config.probabilistic_suspected_epsilon_millis == 15000
        assert config.probabilistic_max_sample_size == 250

    def test_from_dict_invalid_protect_on(self):
        data = {"protect_on": "INVALID"}
        with pytest.raises(ConfigurationException, match="Invalid protect_on"):
            SplitBrainProtectionConfig.from_dict("test", data)

    def test_from_dict_invalid_function_type(self):
        data = {"function_type": "INVALID"}
        with pytest.raises(ConfigurationException, match="Invalid function_type"):
            SplitBrainProtectionConfig.from_dict("test", data)


class TestMapStoreConfig:
    def test_default_init(self):
        config = MapStoreConfig()
        assert config.enabled is True
        assert config.class_name is None
        assert config.factory_class_name is None
        assert config.write_coalescing is True
        assert config.write_delay_seconds == 0
        assert config.write_batch_size == 1
        assert config.initial_load_mode == InitialLoadMode.LAZY
        assert config.properties == {}

    def test_init_with_params(self):
        config = MapStoreConfig(
            enabled=False,
            class_name="com.example.MyStore",
            factory_class_name="com.example.MyFactory",
            write_coalescing=False,
            write_delay_seconds=5,
            write_batch_size=100,
            initial_load_mode=InitialLoadMode.EAGER,
            properties={"key": "value"},
        )
        assert config.enabled is False
        assert config.class_name == "com.example.MyStore"
        assert config.factory_class_name == "com.example.MyFactory"
        assert config.write_coalescing is False
        assert config.write_delay_seconds == 5
        assert config.write_batch_size == 100
        assert config.initial_load_mode == InitialLoadMode.EAGER
        assert config.properties == {"key": "value"}

    def test_validate_write_delay_negative(self):
        with pytest.raises(ConfigurationException, match="write_delay_seconds cannot be negative"):
            MapStoreConfig(write_delay_seconds=-1)

    def test_validate_batch_size_zero(self):
        with pytest.raises(ConfigurationException, match="write_batch_size must be at least 1"):
            MapStoreConfig(write_batch_size=0)

    def test_setters(self):
        config = MapStoreConfig()
        config.enabled = False
        config.class_name = "MyStore"
        config.factory_class_name = "MyFactory"
        config.write_coalescing = False
        config.write_delay_seconds = 10
        config.write_batch_size = 50
        config.initial_load_mode = InitialLoadMode.EAGER
        config.properties = {"a": "b"}
        
        assert config.enabled is False
        assert config.class_name == "MyStore"
        assert config.factory_class_name == "MyFactory"
        assert config.write_coalescing is False
        assert config.write_delay_seconds == 10
        assert config.write_batch_size == 50
        assert config.initial_load_mode == InitialLoadMode.EAGER
        assert config.properties == {"a": "b"}

    def test_set_property(self):
        config = MapStoreConfig()
        result = config.set_property("key1", "value1")
        assert result is config
        assert config.properties["key1"] == "value1"

    def test_is_write_through(self):
        config = MapStoreConfig(write_delay_seconds=0)
        assert config.is_write_through is True
        assert config.is_write_behind is False

    def test_is_write_behind(self):
        config = MapStoreConfig(write_delay_seconds=5)
        assert config.is_write_through is False
        assert config.is_write_behind is True

    def test_from_dict(self):
        data = {
            "enabled": True,
            "class_name": "MyStore",
            "write_delay_seconds": 10,
            "initial_load_mode": "EAGER",
            "properties": {"prop1": "val1"},
        }
        config = MapStoreConfig.from_dict(data)
        
        assert config.enabled is True
        assert config.class_name == "MyStore"
        assert config.write_delay_seconds == 10
        assert config.initial_load_mode == InitialLoadMode.EAGER
        assert config.properties == {"prop1": "val1"}

    def test_from_dict_invalid_load_mode(self):
        data = {"initial_load_mode": "INVALID"}
        with pytest.raises(ConfigurationException, match="Invalid initial_load_mode"):
            MapStoreConfig.from_dict(data)


class TestRetryConfig:
    def test_default_init(self):
        config = RetryConfig()
        assert config.initial_backoff == 1.0
        assert config.max_backoff == 30.0
        assert config.multiplier == 2.0
        assert config.jitter == 0.0

    def test_init_with_params(self):
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

    def test_validate_initial_backoff_zero(self):
        with pytest.raises(ConfigurationException, match="initial_backoff must be positive"):
            RetryConfig(initial_backoff=0)

    def test_validate_max_backoff_less_than_initial(self):
        with pytest.raises(ConfigurationException, match="max_backoff must be >= initial_backoff"):
            RetryConfig(initial_backoff=10.0, max_backoff=5.0)

    def test_validate_multiplier_less_than_one(self):
        with pytest.raises(ConfigurationException, match="multiplier must be >= 1.0"):
            RetryConfig(multiplier=0.5)

    def test_validate_jitter_negative(self):
        with pytest.raises(ConfigurationException, match="jitter must be between 0.0 and 1.0"):
            RetryConfig(jitter=-0.1)

    def test_validate_jitter_greater_than_one(self):
        with pytest.raises(ConfigurationException, match="jitter must be between 0.0 and 1.0"):
            RetryConfig(jitter=1.5)

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

    def test_from_dict(self):
        data = {
            "initial_backoff": 0.5,
            "max_backoff": 120.0,
            "multiplier": 2.5,
            "jitter": 0.1,
        }
        config = RetryConfig.from_dict(data)
        
        assert config.initial_backoff == 0.5
        assert config.max_backoff == 120.0
        assert config.multiplier == 2.5
        assert config.jitter == 0.1


class TestNetworkConfig:
    def test_default_init(self):
        config = NetworkConfig()
        assert config.addresses == ["localhost:5701"]
        assert config.connection_timeout == 5.0
        assert config.smart_routing is True

    def test_init_with_params(self):
        config = NetworkConfig(
            addresses=["node1:5701", "node2:5701"],
            connection_timeout=10.0,
            smart_routing=False,
        )
        assert config.addresses == ["node1:5701", "node2:5701"]
        assert config.connection_timeout == 10.0
        assert config.smart_routing is False

    def test_validate_empty_addresses(self):
        with pytest.raises(ConfigurationException, match="At least one cluster address is required"):
            NetworkConfig(addresses=[])

    def test_validate_connection_timeout_zero(self):
        with pytest.raises(ConfigurationException, match="connection_timeout must be positive"):
            NetworkConfig(connection_timeout=0)

    def test_setters(self):
        config = NetworkConfig()
        config.addresses = ["new-node:5701"]
        config.connection_timeout = 15.0
        config.smart_routing = False
        
        assert config.addresses == ["new-node:5701"]
        assert config.connection_timeout == 15.0
        assert config.smart_routing is False

    def test_from_dict(self):
        data = {
            "addresses": ["host1:5701", "host2:5701"],
            "connection_timeout": 20.0,
            "smart_routing": False,
        }
        config = NetworkConfig.from_dict(data)
        
        assert config.addresses == ["host1:5701", "host2:5701"]
        assert config.connection_timeout == 20.0
        assert config.smart_routing is False


class TestConnectionStrategyConfig:
    def test_default_init(self):
        config = ConnectionStrategyConfig()
        assert config.async_start is False
        assert config.reconnect_mode == ReconnectMode.ON
        assert isinstance(config.retry, RetryConfig)

    def test_init_with_params(self):
        retry = RetryConfig(initial_backoff=2.0)
        config = ConnectionStrategyConfig(
            async_start=True,
            reconnect_mode=ReconnectMode.ASYNC,
            retry=retry,
        )
        assert config.async_start is True
        assert config.reconnect_mode == ReconnectMode.ASYNC
        assert config.retry.initial_backoff == 2.0

    def test_setters(self):
        config = ConnectionStrategyConfig()
        config.async_start = True
        config.reconnect_mode = ReconnectMode.OFF
        config.retry = RetryConfig(max_backoff=60.0)
        
        assert config.async_start is True
        assert config.reconnect_mode == ReconnectMode.OFF
        assert config.retry.max_backoff == 60.0

    def test_from_dict(self):
        data = {
            "async_start": True,
            "reconnect_mode": "ASYNC",
            "retry": {"initial_backoff": 2.0},
        }
        config = ConnectionStrategyConfig.from_dict(data)
        
        assert config.async_start is True
        assert config.reconnect_mode == ReconnectMode.ASYNC
        assert config.retry.initial_backoff == 2.0

    def test_from_dict_invalid_reconnect_mode(self):
        data = {"reconnect_mode": "INVALID"}
        with pytest.raises(ConfigurationException, match="Invalid reconnect_mode"):
            ConnectionStrategyConfig.from_dict(data)


class TestSecurityConfig:
    def test_default_init(self):
        config = SecurityConfig()
        assert config.username is None
        assert config.password is None
        assert config.token is None
        assert isinstance(config.tls, TlsConfig)
        assert config.is_configured is False

    def test_init_with_params(self):
        tls = TlsConfig(enabled=True)
        config = SecurityConfig(
            username="admin",
            password="secret",
            token="token123",
            tls=tls,
        )
        assert config.username == "admin"
        assert config.password == "secret"
        assert config.token == "token123"
        assert config.tls.enabled is True

    def test_is_configured_with_username(self):
        config = SecurityConfig(username="user")
        assert config.is_configured is True

    def test_is_configured_with_token(self):
        config = SecurityConfig(token="token")
        assert config.is_configured is True

    def test_tls_enabled(self):
        config = SecurityConfig()
        assert config.tls_enabled is False
        
        config.tls = TlsConfig(enabled=True)
        assert config.tls_enabled is True

    def test_setters(self):
        config = SecurityConfig()
        config.username = "user1"
        config.password = "pass1"
        config.token = "tok1"
        config.tls = TlsConfig(enabled=True)
        
        assert config.username == "user1"
        assert config.password == "pass1"
        assert config.token == "tok1"
        assert config.tls.enabled is True

    def test_from_dict(self):
        data = {
            "username": "admin",
            "password": "secret",
            "token": "my-token",
            "tls": {"enabled": True},
        }
        config = SecurityConfig.from_dict(data)
        
        assert config.username == "admin"
        assert config.password == "secret"
        assert config.token == "my-token"


class TestBitmapIndexOptions:
    def test_default_init(self):
        options = BitmapIndexOptions()
        assert options.unique_key == "__key"
        assert options.unique_key_transformation == UniqueKeyTransformation.OBJECT

    def test_init_with_params(self):
        options = BitmapIndexOptions(
            unique_key="id",
            unique_key_transformation=UniqueKeyTransformation.LONG,
        )
        assert options.unique_key == "id"
        assert options.unique_key_transformation == UniqueKeyTransformation.LONG

    def test_validate_empty_unique_key(self):
        with pytest.raises(ConfigurationException, match="unique_key cannot be empty"):
            BitmapIndexOptions(unique_key="")

    def test_setters(self):
        options = BitmapIndexOptions()
        options.unique_key = "custom_key"
        options.unique_key_transformation = UniqueKeyTransformation.RAW
        
        assert options.unique_key == "custom_key"
        assert options.unique_key_transformation == UniqueKeyTransformation.RAW

    def test_from_dict(self):
        data = {
            "unique_key": "my_id",
            "unique_key_transformation": "LONG",
        }
        options = BitmapIndexOptions.from_dict(data)
        
        assert options.unique_key == "my_id"
        assert options.unique_key_transformation == UniqueKeyTransformation.LONG

    def test_from_dict_invalid_transformation(self):
        data = {"unique_key_transformation": "INVALID"}
        with pytest.raises(ConfigurationException, match="Invalid unique_key_transformation"):
            BitmapIndexOptions.from_dict(data)


class TestIndexConfig:
    def test_default_init(self):
        config = IndexConfig(attributes=["name"])
        assert config.name is None
        assert config.type == IndexType.SORTED
        assert config.attributes == ["name"]
        assert config.bitmap_index_options is None

    def test_init_with_params(self):
        bitmap_opts = BitmapIndexOptions(unique_key="id")
        config = IndexConfig(
            name="my-index",
            type=IndexType.BITMAP,
            attributes=["field1", "field2"],
            bitmap_index_options=bitmap_opts,
        )
        assert config.name == "my-index"
        assert config.type == IndexType.BITMAP
        assert config.attributes == ["field1", "field2"]
        assert config.bitmap_index_options.unique_key == "id"

    def test_validate_empty_attributes(self):
        with pytest.raises(ConfigurationException, match="At least one attribute is required"):
            IndexConfig(attributes=[])

    def test_setters(self):
        config = IndexConfig(attributes=["a"])
        config.name = "idx"
        config.type = IndexType.HASH
        config.attributes = ["b", "c"]
        config.bitmap_index_options = BitmapIndexOptions()
        
        assert config.name == "idx"
        assert config.type == IndexType.HASH
        assert config.attributes == ["b", "c"]
        assert config.bitmap_index_options is not None

    def test_add_attribute(self):
        config = IndexConfig(attributes=["a"])
        result = config.add_attribute("b")
        
        assert result is config
        assert "b" in config.attributes

    def test_from_dict(self):
        data = {
            "name": "my-idx",
            "type": "HASH",
            "attributes": ["field1"],
            "bitmap_index_options": {"unique_key": "id"},
        }
        config = IndexConfig.from_dict(data)
        
        assert config.name == "my-idx"
        assert config.type == IndexType.HASH
        assert config.attributes == ["field1"]
        assert config.bitmap_index_options.unique_key == "id"

    def test_from_dict_invalid_type(self):
        data = {"type": "INVALID", "attributes": ["a"]}
        with pytest.raises(ConfigurationException, match="Invalid index type"):
            IndexConfig.from_dict(data)


class TestQueryCacheConfig:
    def test_default_init(self):
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

    def test_validate_empty_name(self):
        with pytest.raises(ConfigurationException, match="Query cache name cannot be empty"):
            QueryCacheConfig(name="")

    def test_validate_batch_size(self):
        with pytest.raises(ConfigurationException, match="batch_size must be at least 1"):
            QueryCacheConfig(batch_size=0)

    def test_validate_buffer_size(self):
        with pytest.raises(ConfigurationException, match="buffer_size must be at least 1"):
            QueryCacheConfig(buffer_size=0)

    def test_validate_delay_seconds(self):
        with pytest.raises(ConfigurationException, match="delay_seconds cannot be negative"):
            QueryCacheConfig(delay_seconds=-1)

    def test_validate_eviction_max_size(self):
        with pytest.raises(ConfigurationException, match="eviction_max_size must be positive"):
            QueryCacheConfig(eviction_max_size=0)

    def test_setters(self):
        config = QueryCacheConfig()
        config.predicate = "age > 18"
        config.batch_size = 10
        config.buffer_size = 32
        config.delay_seconds = 5
        config.in_memory_format = InMemoryFormat.OBJECT
        config.include_value = False
        config.populate = False
        config.coalesce = True
        config.eviction_max_size = 5000
        config.eviction_policy = EvictionPolicy.LFU
        
        assert config.predicate == "age > 18"
        assert config.batch_size == 10
        assert config.buffer_size == 32
        assert config.delay_seconds == 5
        assert config.in_memory_format == InMemoryFormat.OBJECT
        assert config.include_value is False
        assert config.populate is False
        assert config.coalesce is True
        assert config.eviction_max_size == 5000
        assert config.eviction_policy == EvictionPolicy.LFU

    def test_from_dict(self):
        data = {
            "predicate": "status = 'active'",
            "batch_size": 5,
            "in_memory_format": "OBJECT",
            "eviction_policy": "LFU",
        }
        config = QueryCacheConfig.from_dict("my-cache", data)
        
        assert config.name == "my-cache"
        assert config.predicate == "status = 'active'"
        assert config.batch_size == 5
        assert config.in_memory_format == InMemoryFormat.OBJECT
        assert config.eviction_policy == EvictionPolicy.LFU

    def test_from_dict_invalid_format(self):
        data = {"in_memory_format": "INVALID"}
        with pytest.raises(ConfigurationException, match="Invalid in_memory_format"):
            QueryCacheConfig.from_dict("test", data)

    def test_from_dict_invalid_eviction(self):
        data = {"eviction_policy": "INVALID"}
        with pytest.raises(ConfigurationException, match="Invalid eviction_policy"):
            QueryCacheConfig.from_dict("test", data)


class TestWanReplicationConfig:
    def test_default_init(self):
        config = WanReplicationConfig()
        assert config.cluster_name == "dev"
        assert config.endpoints == []
        assert config.queue_capacity == 10000
        assert config.batch_size == 500
        assert config.batch_max_delay_millis == 1000
        assert config.response_timeout_millis == 60000
        assert config.acknowledge_type == "ACK_ON_OPERATION_COMPLETE"

    def test_validate_empty_cluster_name(self):
        with pytest.raises(ConfigurationException, match="WAN cluster_name cannot be empty"):
            WanReplicationConfig(cluster_name="")

    def test_validate_queue_capacity(self):
        with pytest.raises(ConfigurationException, match="queue_capacity must be positive"):
            WanReplicationConfig(queue_capacity=0)

    def test_validate_batch_size(self):
        with pytest.raises(ConfigurationException, match="batch_size must be positive"):
            WanReplicationConfig(batch_size=0)

    def test_validate_batch_max_delay(self):
        with pytest.raises(ConfigurationException, match="batch_max_delay_millis cannot be negative"):
            WanReplicationConfig(batch_max_delay_millis=-1)

    def test_validate_response_timeout(self):
        with pytest.raises(ConfigurationException, match="response_timeout_millis must be positive"):
            WanReplicationConfig(response_timeout_millis=0)

    def test_validate_acknowledge_type_invalid(self):
        with pytest.raises(ConfigurationException, match="Invalid acknowledge_type"):
            WanReplicationConfig(acknowledge_type="INVALID")

    def test_valid_acknowledge_types(self):
        config1 = WanReplicationConfig(acknowledge_type="ACK_ON_RECEIPT")
        assert config1.acknowledge_type == "ACK_ON_RECEIPT"
        
        config2 = WanReplicationConfig(acknowledge_type="ACK_ON_OPERATION_COMPLETE")
        assert config2.acknowledge_type == "ACK_ON_OPERATION_COMPLETE"

    def test_setters(self):
        config = WanReplicationConfig()
        config.cluster_name = "prod"
        config.endpoints = ["host1:5701"]
        config.queue_capacity = 20000
        config.batch_size = 1000
        config.batch_max_delay_millis = 2000
        config.response_timeout_millis = 120000
        config.acknowledge_type = "ACK_ON_RECEIPT"
        
        assert config.cluster_name == "prod"
        assert config.endpoints == ["host1:5701"]
        assert config.queue_capacity == 20000
        assert config.batch_size == 1000
        assert config.batch_max_delay_millis == 2000
        assert config.response_timeout_millis == 120000
        assert config.acknowledge_type == "ACK_ON_RECEIPT"

    def test_add_endpoint(self):
        config = WanReplicationConfig()
        result = config.add_endpoint("new-host:5701")
        
        assert result is config
        assert "new-host:5701" in config.endpoints

    def test_from_dict(self):
        data = {
            "cluster_name": "target",
            "endpoints": ["h1:5701", "h2:5701"],
            "queue_capacity": 5000,
        }
        config = WanReplicationConfig.from_dict(data)
        
        assert config.cluster_name == "target"
        assert config.endpoints == ["h1:5701", "h2:5701"]
        assert config.queue_capacity == 5000


class TestClientUserCodeDeploymentConfig:
    def test_default_init(self):
        config = ClientUserCodeDeploymentConfig()
        assert config.enabled is False
        assert config.class_names == []
        assert config.jar_paths == []

    def test_init_with_params(self):
        config = ClientUserCodeDeploymentConfig(
            enabled=True,
            class_names=["com.example.MyClass"],
            jar_paths=["/path/to/jar.jar"],
        )
        assert config.enabled is True
        assert config.class_names == ["com.example.MyClass"]
        assert config.jar_paths == ["/path/to/jar.jar"]

    def test_setters(self):
        config = ClientUserCodeDeploymentConfig()
        config.enabled = True
        config.class_names = ["Class1"]
        config.jar_paths = ["/path1.jar"]
        
        assert config.enabled is True
        assert config.class_names == ["Class1"]
        assert config.jar_paths == ["/path1.jar"]

    def test_add_class_name(self):
        config = ClientUserCodeDeploymentConfig()
        result = config.add_class_name("MyClass")
        
        assert result is config
        assert "MyClass" in config.class_names

    def test_add_jar_path(self):
        config = ClientUserCodeDeploymentConfig()
        result = config.add_jar_path("/my.jar")
        
        assert result is config
        assert "/my.jar" in config.jar_paths

    def test_from_dict(self):
        data = {
            "enabled": True,
            "class_names": ["A", "B"],
            "jar_paths": ["/x.jar"],
        }
        config = ClientUserCodeDeploymentConfig.from_dict(data)
        
        assert config.enabled is True
        assert config.class_names == ["A", "B"]
        assert config.jar_paths == ["/x.jar"]


class TestStatisticsConfig:
    def test_default_init(self):
        config = StatisticsConfig()
        assert config.enabled is False
        assert config.period_seconds == 3.0

    def test_init_with_params(self):
        config = StatisticsConfig(enabled=True, period_seconds=5.0)
        assert config.enabled is True
        assert config.period_seconds == 5.0

    def test_validate_period_zero(self):
        with pytest.raises(ConfigurationException, match="period_seconds must be positive"):
            StatisticsConfig(period_seconds=0)

    def test_setters(self):
        config = StatisticsConfig()
        config.enabled = True
        config.period_seconds = 10.0
        
        assert config.enabled is True
        assert config.period_seconds == 10.0

    def test_from_dict(self):
        data = {"enabled": True, "period_seconds": 5.0}
        config = StatisticsConfig.from_dict(data)
        
        assert config.enabled is True
        assert config.period_seconds == 5.0


class TestEventJournalConfig:
    def test_default_init(self):
        config = EventJournalConfig()
        assert config.enabled is False
        assert config.capacity == 10000
        assert config.time_to_live_seconds == 0

    def test_init_with_params(self):
        config = EventJournalConfig(
            enabled=True,
            capacity=50000,
            time_to_live_seconds=3600,
        )
        assert config.enabled is True
        assert config.capacity == 50000
        assert config.time_to_live_seconds == 3600

    def test_validate_capacity_zero(self):
        with pytest.raises(ConfigurationException, match="capacity must be positive"):
            EventJournalConfig(capacity=0)

    def test_validate_ttl_negative(self):
        with pytest.raises(ConfigurationException, match="time_to_live_seconds cannot be negative"):
            EventJournalConfig(time_to_live_seconds=-1)

    def test_setters(self):
        config = EventJournalConfig()
        config.enabled = True
        config.capacity = 25000
        config.time_to_live_seconds = 7200
        
        assert config.enabled is True
        assert config.capacity == 25000
        assert config.time_to_live_seconds == 7200

    def test_from_dict(self):
        data = {
            "enabled": True,
            "capacity": 100000,
            "time_to_live_seconds": 1800,
        }
        config = EventJournalConfig.from_dict(data)
        
        assert config.enabled is True
        assert config.capacity == 100000
        assert config.time_to_live_seconds == 1800


class TestNearCacheConfig:
    def test_default_init(self):
        config = NearCacheConfig()
        assert config.name == "default"
        assert config.max_idle_seconds == 0
        assert config.time_to_live_seconds == 0
        assert config.eviction_policy == EvictionPolicy.LRU
        assert config.max_size == 10000
        assert config.in_memory_format == InMemoryFormat.BINARY
        assert config.invalidate_on_change is True
        assert config.serialize_keys is False
        assert config.local_update_policy == LocalUpdatePolicy.INVALIDATE
        assert config.preloader_enabled is False

    def test_validate_empty_name(self):
        with pytest.raises(ConfigurationException, match="Near cache name cannot be empty"):
            NearCacheConfig(name="")

    def test_validate_max_idle_negative(self):
        with pytest.raises(ConfigurationException, match="max_idle_seconds cannot be negative"):
            NearCacheConfig(max_idle_seconds=-1)

    def test_validate_ttl_negative(self):
        with pytest.raises(ConfigurationException, match="time_to_live_seconds cannot be negative"):
            NearCacheConfig(time_to_live_seconds=-1)

    def test_validate_max_size_zero(self):
        with pytest.raises(ConfigurationException, match="max_size must be positive"):
            NearCacheConfig(max_size=0)

    def test_validate_preloader_initial_delay_negative(self):
        with pytest.raises(ConfigurationException, match="preloader_store_initial_delay_seconds cannot be negative"):
            NearCacheConfig(preloader_store_initial_delay_seconds=-1)

    def test_validate_preloader_interval_negative(self):
        with pytest.raises(ConfigurationException, match="preloader_store_interval_seconds cannot be negative"):
            NearCacheConfig(preloader_store_interval_seconds=-1)

    def test_setters(self):
        config = NearCacheConfig()
        config.max_idle_seconds = 300
        config.time_to_live_seconds = 600
        config.eviction_policy = EvictionPolicy.LFU
        config.max_size = 5000
        config.in_memory_format = InMemoryFormat.OBJECT
        config.invalidate_on_change = False
        config.serialize_keys = True
        config.local_update_policy = LocalUpdatePolicy.CACHE_ON_UPDATE
        config.preloader_enabled = True
        config.preloader_directory = "/tmp/preloader"
        config.preloader_store_initial_delay_seconds = 300
        config.preloader_store_interval_seconds = 300
        
        assert config.max_idle_seconds == 300
        assert config.time_to_live_seconds == 600
        assert config.eviction_policy == EvictionPolicy.LFU
        assert config.max_size == 5000
        assert config.in_memory_format == InMemoryFormat.OBJECT
        assert config.invalidate_on_change is False
        assert config.serialize_keys is True
        assert config.local_update_policy == LocalUpdatePolicy.CACHE_ON_UPDATE
        assert config.preloader_enabled is True
        assert config.preloader_directory == "/tmp/preloader"

    def test_from_dict(self):
        data = {
            "max_idle_seconds": 100,
            "eviction_policy": "LFU",
            "in_memory_format": "OBJECT",
            "local_update_policy": "CACHE_ON_UPDATE",
        }
        config = NearCacheConfig.from_dict("my-cache", data)
        
        assert config.name == "my-cache"
        assert config.max_idle_seconds == 100
        assert config.eviction_policy == EvictionPolicy.LFU
        assert config.in_memory_format == InMemoryFormat.OBJECT
        assert config.local_update_policy == LocalUpdatePolicy.CACHE_ON_UPDATE

    def test_from_dict_invalid_eviction(self):
        data = {"eviction_policy": "INVALID"}
        with pytest.raises(ConfigurationException, match="Invalid eviction_policy"):
            NearCacheConfig.from_dict("test", data)

    def test_from_dict_invalid_format(self):
        data = {"in_memory_format": "INVALID"}
        with pytest.raises(ConfigurationException, match="Invalid in_memory_format"):
            NearCacheConfig.from_dict("test", data)

    def test_from_dict_invalid_update_policy(self):
        data = {"local_update_policy": "INVALID"}
        with pytest.raises(ConfigurationException, match="Invalid local_update_policy"):
            NearCacheConfig.from_dict("test", data)


class TestSerializationConfig:
    def test_default_init(self):
        config = SerializationConfig()
        assert config.portable_version == 0
        assert config.default_integer_type == "INT"
        assert config.portable_factories == {}
        assert config.data_serializable_factories == {}
        assert config.custom_serializers == {}
        assert config.compact_serializers == []

    def test_init_with_params(self):
        config = SerializationConfig(
            portable_version=1,
            default_integer_type="LONG",
        )
        assert config.portable_version == 1
        assert config.default_integer_type == "LONG"

    def test_add_portable_factory(self):
        config = SerializationConfig()
        factory = MagicMock()
        config.add_portable_factory(1, factory)
        
        assert config.portable_factories[1] is factory

    def test_add_data_serializable_factory(self):
        config = SerializationConfig()
        factory = MagicMock()
        config.add_data_serializable_factory(2, factory)
        
        assert config.data_serializable_factories[2] is factory

    def test_add_custom_serializer(self):
        config = SerializationConfig()
        serializer = MagicMock()
        
        class MyClass:
            pass
        
        config.add_custom_serializer(MyClass, serializer)
        assert config.custom_serializers[MyClass] is serializer

    def test_add_compact_serializer(self):
        config = SerializationConfig()
        serializer = MagicMock()
        config.add_compact_serializer(serializer)
        
        assert serializer in config.compact_serializers

    def test_setters(self):
        config = SerializationConfig()
        config.portable_version = 2
        config.default_integer_type = "BYTE"
        
        assert config.portable_version == 2
        assert config.default_integer_type == "BYTE"

    def test_from_dict(self):
        data = {
            "portable_version": 3,
            "default_integer_type": "SHORT",
        }
        config = SerializationConfig.from_dict(data)
        
        assert config.portable_version == 3
        assert config.default_integer_type == "SHORT"


class TestClientConfig:
    def test_default_init(self):
        config = ClientConfig()
        assert config.cluster_name == "dev"
        assert config.client_name is None
        assert isinstance(config.network, NetworkConfig)
        assert isinstance(config.connection_strategy, ConnectionStrategyConfig)
        assert isinstance(config.security, SecurityConfig)
        assert isinstance(config.serialization, SerializationConfig)
        assert config.near_caches == {}
        assert config.labels == []

    def test_cluster_name_setter(self):
        config = ClientConfig()
        config.cluster_name = "production"
        assert config.cluster_name == "production"

    def test_cluster_name_empty(self):
        config = ClientConfig()
        with pytest.raises(ConfigurationException, match="cluster_name cannot be empty"):
            config.cluster_name = ""

    def test_client_name_setter(self):
        config = ClientConfig()
        config.client_name = "my-client"
        assert config.client_name == "my-client"

    def test_cluster_members_alias(self):
        config = ClientConfig()
        config.cluster_members = ["host1:5701", "host2:5701"]
        assert config.cluster_members == ["host1:5701", "host2:5701"]
        assert config.network.addresses == ["host1:5701", "host2:5701"]

    def test_connection_timeout_alias(self):
        config = ClientConfig()
        config.connection_timeout = 15.0
        assert config.connection_timeout == 15.0
        assert config.network.connection_timeout == 15.0

    def test_smart_routing_alias(self):
        config = ClientConfig()
        config.smart_routing = False
        assert config.smart_routing is False
        assert config.network.smart_routing is False

    def test_credentials_getter_not_configured(self):
        config = ClientConfig()
        assert config.credentials is None

    def test_credentials_getter_configured(self):
        config = ClientConfig()
        config.security.username = "admin"
        config.security.password = "secret"
        
        creds = config.credentials
        assert creds["username"] == "admin"
        assert creds["password"] == "secret"

    def test_credentials_setter(self):
        config = ClientConfig()
        config.credentials = {"username": "user1", "password": "pass1"}
        
        assert config.security.username == "user1"
        assert config.security.password == "pass1"

    def test_network_setter(self):
        config = ClientConfig()
        new_network = NetworkConfig(addresses=["new:5701"])
        config.network = new_network
        assert config.network.addresses == ["new:5701"]

    def test_connection_strategy_setter(self):
        config = ClientConfig()
        new_strategy = ConnectionStrategyConfig(async_start=True)
        config.connection_strategy = new_strategy
        assert config.connection_strategy.async_start is True

    def test_security_setter(self):
        config = ClientConfig()
        new_security = SecurityConfig(username="admin")
        config.security = new_security
        assert config.security.username == "admin"

    def test_serialization_setter(self):
        config = ClientConfig()
        new_serialization = SerializationConfig(portable_version=2)
        config.serialization = new_serialization
        assert config.serialization.portable_version == 2

    def test_add_near_cache(self):
        config = ClientConfig()
        nc = NearCacheConfig(name="my-map")
        config.add_near_cache(nc)
        
        assert "my-map" in config.near_caches
        assert config.near_caches["my-map"].name == "my-map"

    def test_labels_setter(self):
        config = ClientConfig()
        config.labels = ["label1", "label2"]
        assert config.labels == ["label1", "label2"]

    def test_discovery_setter(self):
        config = ClientConfig()
        discovery = DiscoveryConfig(enabled=True)
        config.discovery = discovery
        assert config.discovery.enabled is True

    def test_statistics_setter(self):
        config = ClientConfig()
        stats = StatisticsConfig(enabled=True)
        config.statistics = stats
        assert config.statistics.enabled is True

    def test_add_split_brain_protection(self):
        config = ClientConfig()
        sbp = SplitBrainProtectionConfig(name="my-quorum", min_cluster_size=3)
        result = config.add_split_brain_protection(sbp)
        
        assert result is config
        assert "my-quorum" in config.split_brain_protections
        assert config.split_brain_protections["my-quorum"].min_cluster_size == 3

    def test_get_split_brain_protection(self):
        config = ClientConfig()
        sbp = SplitBrainProtectionConfig(name="quorum1")
        config.add_split_brain_protection(sbp)
        
        retrieved = config.get_split_brain_protection("quorum1")
        assert retrieved is sbp
        
        not_found = config.get_split_brain_protection("nonexistent")
        assert not_found is None

    def test_from_dict(self):
        data = {
            "cluster_name": "test-cluster",
            "client_name": "test-client",
            "labels": ["env:test"],
            "network": {"addresses": ["h1:5701"]},
            "security": {"username": "admin"},
            "statistics": {"enabled": True},
            "near_caches": {
                "map1": {"max_size": 5000},
            },
            "split_brain_protections": {
                "quorum1": {"min_cluster_size": 3},
            },
        }
        config = ClientConfig.from_dict(data)
        
        assert config.cluster_name == "test-cluster"
        assert config.client_name == "test-client"
        assert config.labels == ["env:test"]
        assert config.network.addresses == ["h1:5701"]
        assert config.security.username == "admin"
        assert config.statistics.enabled is True
        assert "map1" in config.near_caches
        assert "quorum1" in config.split_brain_protections

    def test_from_yaml_file_not_found(self):
        with pytest.raises(ConfigurationException, match="Configuration file not found"):
            ClientConfig.from_yaml("/nonexistent/path.yaml")

    def test_from_yaml_success(self):
        yaml_content = """
hazelcast_client:
  cluster_name: yaml-cluster
  client_name: yaml-client
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(yaml_content)
            f.flush()
            
            try:
                config = ClientConfig.from_yaml(f.name)
                assert config.cluster_name == "yaml-cluster"
                assert config.client_name == "yaml-client"
            finally:
                os.unlink(f.name)

    def test_from_yaml_empty_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
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
client_name: string-client
"""
        config = ClientConfig.from_yaml_string(yaml_content)
        assert config.cluster_name == "string-cluster"
        assert config.client_name == "string-client"

    def test_from_yaml_string_empty(self):
        config = ClientConfig.from_yaml_string("")
        assert config.cluster_name == "dev"

    def test_from_yaml_string_with_hazelcast_client_root(self):
        yaml_content = """
hazelcast_client:
  cluster_name: nested-cluster
"""
        config = ClientConfig.from_yaml_string(yaml_content)
        assert config.cluster_name == "nested-cluster"


class TestEnumValues:
    def test_reconnect_mode(self):
        assert ReconnectMode.OFF.value == "OFF"
        assert ReconnectMode.ON.value == "ON"
        assert ReconnectMode.ASYNC.value == "ASYNC"

    def test_eviction_policy(self):
        assert EvictionPolicy.NONE.value == "NONE"
        assert EvictionPolicy.LRU.value == "LRU"
        assert EvictionPolicy.LFU.value == "LFU"
        assert EvictionPolicy.RANDOM.value == "RANDOM"

    def test_in_memory_format(self):
        assert InMemoryFormat.BINARY.value == "BINARY"
        assert InMemoryFormat.OBJECT.value == "OBJECT"

    def test_index_type(self):
        assert IndexType.SORTED.value == "SORTED"
        assert IndexType.HASH.value == "HASH"
        assert IndexType.BITMAP.value == "BITMAP"

    def test_local_update_policy(self):
        assert LocalUpdatePolicy.INVALIDATE.value == "INVALIDATE"
        assert LocalUpdatePolicy.CACHE_ON_UPDATE.value == "CACHE_ON_UPDATE"

    def test_unique_key_transformation(self):
        assert UniqueKeyTransformation.OBJECT.value == "OBJECT"
        assert UniqueKeyTransformation.LONG.value == "LONG"
        assert UniqueKeyTransformation.RAW.value == "RAW"

    def test_initial_load_mode(self):
        assert InitialLoadMode.LAZY.value == "LAZY"
        assert InitialLoadMode.EAGER.value == "EAGER"
