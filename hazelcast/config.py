"""Hazelcast client configuration."""

from enum import Enum
from typing import Dict, List, Optional
import os

from hazelcast.exceptions import ConfigurationException


class ReconnectMode(Enum):
    """Reconnection behavior mode."""
    OFF = "OFF"
    ON = "ON"
    ASYNC = "ASYNC"


class EvictionPolicy(Enum):
    """Eviction policy for near cache."""
    NONE = "NONE"
    LRU = "LRU"
    LFU = "LFU"
    RANDOM = "RANDOM"


class InMemoryFormat(Enum):
    """In-memory storage format."""
    BINARY = "BINARY"
    OBJECT = "OBJECT"


class RetryConfig:
    """Configuration for exponential backoff retry strategy."""

    def __init__(
        self,
        initial_backoff: float = 1.0,
        max_backoff: float = 30.0,
        multiplier: float = 2.0,
        jitter: float = 0.0,
    ):
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._multiplier = multiplier
        self._jitter = jitter
        self._validate()

    def _validate(self) -> None:
        if self._initial_backoff <= 0:
            raise ConfigurationException("initial_backoff must be positive")
        if self._max_backoff < self._initial_backoff:
            raise ConfigurationException("max_backoff must be >= initial_backoff")
        if self._multiplier < 1.0:
            raise ConfigurationException("multiplier must be >= 1.0")
        if self._jitter < 0.0 or self._jitter > 1.0:
            raise ConfigurationException("jitter must be between 0.0 and 1.0")

    @property
    def initial_backoff(self) -> float:
        """Get the initial backoff duration in seconds."""
        return self._initial_backoff

    @initial_backoff.setter
    def initial_backoff(self, value: float) -> None:
        self._initial_backoff = value
        self._validate()

    @property
    def max_backoff(self) -> float:
        """Get the maximum backoff duration in seconds."""
        return self._max_backoff

    @max_backoff.setter
    def max_backoff(self, value: float) -> None:
        self._max_backoff = value
        self._validate()

    @property
    def multiplier(self) -> float:
        """Get the backoff multiplier."""
        return self._multiplier

    @multiplier.setter
    def multiplier(self, value: float) -> None:
        self._multiplier = value
        self._validate()

    @property
    def jitter(self) -> float:
        """Get the jitter factor (0.0 to 1.0)."""
        return self._jitter

    @jitter.setter
    def jitter(self, value: float) -> None:
        self._jitter = value
        self._validate()

    @classmethod
    def from_dict(cls, data: dict) -> "RetryConfig":
        """Create RetryConfig from a dictionary."""
        return cls(
            initial_backoff=data.get("initial_backoff", 1.0),
            max_backoff=data.get("max_backoff", 30.0),
            multiplier=data.get("multiplier", 2.0),
            jitter=data.get("jitter", 0.0),
        )


class NetworkConfig:
    """Network configuration for connecting to the cluster."""

    def __init__(
        self,
        addresses: List[str] = None,
        connection_timeout: float = 5.0,
        smart_routing: bool = True,
    ):
        self._addresses = addresses or ["localhost:5701"]
        self._connection_timeout = connection_timeout
        self._smart_routing = smart_routing
        self._validate()

    def _validate(self) -> None:
        if not self._addresses:
            raise ConfigurationException("At least one cluster address is required")
        if self._connection_timeout <= 0:
            raise ConfigurationException("connection_timeout must be positive")

    @property
    def addresses(self) -> List[str]:
        """Get the list of cluster member addresses."""
        return self._addresses

    @addresses.setter
    def addresses(self, value: List[str]) -> None:
        self._addresses = value
        self._validate()

    @property
    def connection_timeout(self) -> float:
        """Get the connection timeout in seconds."""
        return self._connection_timeout

    @connection_timeout.setter
    def connection_timeout(self, value: float) -> None:
        self._connection_timeout = value
        self._validate()

    @property
    def smart_routing(self) -> bool:
        """Get whether smart routing is enabled."""
        return self._smart_routing

    @smart_routing.setter
    def smart_routing(self, value: bool) -> None:
        self._smart_routing = value

    @classmethod
    def from_dict(cls, data: dict) -> "NetworkConfig":
        """Create NetworkConfig from a dictionary."""
        return cls(
            addresses=data.get("addresses", ["localhost:5701"]),
            connection_timeout=data.get("connection_timeout", 5.0),
            smart_routing=data.get("smart_routing", True),
        )


class ConnectionStrategyConfig:
    """Configuration for connection strategy."""

    def __init__(
        self,
        async_start: bool = False,
        reconnect_mode: ReconnectMode = ReconnectMode.ON,
        retry: RetryConfig = None,
    ):
        self._async_start = async_start
        self._reconnect_mode = reconnect_mode
        self._retry = retry or RetryConfig()

    @property
    def async_start(self) -> bool:
        """Get whether to start connection asynchronously."""
        return self._async_start

    @async_start.setter
    def async_start(self, value: bool) -> None:
        self._async_start = value

    @property
    def reconnect_mode(self) -> ReconnectMode:
        """Get the reconnection mode."""
        return self._reconnect_mode

    @reconnect_mode.setter
    def reconnect_mode(self, value: ReconnectMode) -> None:
        self._reconnect_mode = value

    @property
    def retry(self) -> RetryConfig:
        """Get the retry configuration."""
        return self._retry

    @retry.setter
    def retry(self, value: RetryConfig) -> None:
        self._retry = value

    @classmethod
    def from_dict(cls, data: dict) -> "ConnectionStrategyConfig":
        """Create ConnectionStrategyConfig from a dictionary."""
        reconnect_mode_str = data.get("reconnect_mode", "ON")
        try:
            reconnect_mode = ReconnectMode(reconnect_mode_str.upper())
        except ValueError:
            raise ConfigurationException(f"Invalid reconnect_mode: {reconnect_mode_str}")

        retry_data = data.get("retry", {})
        return cls(
            async_start=data.get("async_start", False),
            reconnect_mode=reconnect_mode,
            retry=RetryConfig.from_dict(retry_data),
        )


class SecurityConfig:
    """Security configuration for authentication."""

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
    ):
        self._username = username
        self._password = password
        self._token = token

    @property
    def username(self) -> Optional[str]:
        """Get the username for authentication."""
        return self._username

    @username.setter
    def username(self, value: Optional[str]) -> None:
        self._username = value

    @property
    def password(self) -> Optional[str]:
        """Get the password for authentication."""
        return self._password

    @password.setter
    def password(self, value: Optional[str]) -> None:
        self._password = value

    @property
    def token(self) -> Optional[str]:
        """Get the authentication token."""
        return self._token

    @token.setter
    def token(self, value: Optional[str]) -> None:
        self._token = value

    @property
    def is_configured(self) -> bool:
        """Check if any security credentials are configured."""
        return bool(self._username or self._token)

    @classmethod
    def from_dict(cls, data: dict) -> "SecurityConfig":
        """Create SecurityConfig from a dictionary."""
        return cls(
            username=data.get("username"),
            password=data.get("password"),
            token=data.get("token"),
        )


class NearCacheConfig:
    """Configuration for near cache."""

    def __init__(
        self,
        name: str = "default",
        max_idle_seconds: int = 0,
        time_to_live_seconds: int = 0,
        eviction_policy: EvictionPolicy = EvictionPolicy.LRU,
        max_size: int = 10000,
        in_memory_format: InMemoryFormat = InMemoryFormat.BINARY,
        invalidate_on_change: bool = True,
    ):
        self._name = name
        self._max_idle_seconds = max_idle_seconds
        self._time_to_live_seconds = time_to_live_seconds
        self._eviction_policy = eviction_policy
        self._max_size = max_size
        self._in_memory_format = in_memory_format
        self._invalidate_on_change = invalidate_on_change
        self._validate()

    def _validate(self) -> None:
        if not self._name:
            raise ConfigurationException("Near cache name cannot be empty")
        if self._max_idle_seconds < 0:
            raise ConfigurationException("max_idle_seconds cannot be negative")
        if self._time_to_live_seconds < 0:
            raise ConfigurationException("time_to_live_seconds cannot be negative")
        if self._max_size <= 0:
            raise ConfigurationException("max_size must be positive")

    @property
    def name(self) -> str:
        """Get the near cache name."""
        return self._name

    @property
    def max_idle_seconds(self) -> int:
        """Get max idle time before eviction."""
        return self._max_idle_seconds

    @max_idle_seconds.setter
    def max_idle_seconds(self, value: int) -> None:
        self._max_idle_seconds = value
        self._validate()

    @property
    def time_to_live_seconds(self) -> int:
        """Get time to live in seconds."""
        return self._time_to_live_seconds

    @time_to_live_seconds.setter
    def time_to_live_seconds(self, value: int) -> None:
        self._time_to_live_seconds = value
        self._validate()

    @property
    def eviction_policy(self) -> EvictionPolicy:
        """Get the eviction policy."""
        return self._eviction_policy

    @eviction_policy.setter
    def eviction_policy(self, value: EvictionPolicy) -> None:
        self._eviction_policy = value

    @property
    def max_size(self) -> int:
        """Get the maximum size of the near cache."""
        return self._max_size

    @max_size.setter
    def max_size(self, value: int) -> None:
        self._max_size = value
        self._validate()

    @property
    def in_memory_format(self) -> InMemoryFormat:
        """Get the in-memory storage format."""
        return self._in_memory_format

    @in_memory_format.setter
    def in_memory_format(self, value: InMemoryFormat) -> None:
        self._in_memory_format = value

    @property
    def invalidate_on_change(self) -> bool:
        """Get whether to invalidate on change."""
        return self._invalidate_on_change

    @invalidate_on_change.setter
    def invalidate_on_change(self, value: bool) -> None:
        self._invalidate_on_change = value

    @classmethod
    def from_dict(cls, name: str, data: dict) -> "NearCacheConfig":
        """Create NearCacheConfig from a dictionary."""
        eviction_str = data.get("eviction_policy", "LRU")
        try:
            eviction_policy = EvictionPolicy(eviction_str.upper())
        except ValueError:
            raise ConfigurationException(f"Invalid eviction_policy: {eviction_str}")

        format_str = data.get("in_memory_format", "BINARY")
        try:
            in_memory_format = InMemoryFormat(format_str.upper())
        except ValueError:
            raise ConfigurationException(f"Invalid in_memory_format: {format_str}")

        return cls(
            name=name,
            max_idle_seconds=data.get("max_idle_seconds", 0),
            time_to_live_seconds=data.get("time_to_live_seconds", 0),
            eviction_policy=eviction_policy,
            max_size=data.get("max_size", 10000),
            in_memory_format=in_memory_format,
            invalidate_on_change=data.get("invalidate_on_change", True),
        )


class SerializationConfig:
    """Configuration for serialization."""

    def __init__(
        self,
        portable_version: int = 0,
        default_integer_type: str = "INT",
    ):
        self._portable_version = portable_version
        self._default_integer_type = default_integer_type
        self._portable_factories: Dict[int, object] = {}
        self._data_serializable_factories: Dict[int, object] = {}

    @property
    def portable_version(self) -> int:
        """Get the portable serialization version."""
        return self._portable_version

    @portable_version.setter
    def portable_version(self, value: int) -> None:
        self._portable_version = value

    @property
    def default_integer_type(self) -> str:
        """Get the default integer type."""
        return self._default_integer_type

    @default_integer_type.setter
    def default_integer_type(self, value: str) -> None:
        self._default_integer_type = value

    @property
    def portable_factories(self) -> Dict[int, object]:
        """Get portable factories."""
        return self._portable_factories

    @property
    def data_serializable_factories(self) -> Dict[int, object]:
        """Get data serializable factories."""
        return self._data_serializable_factories

    def add_portable_factory(self, factory_id: int, factory: object) -> None:
        """Add a portable factory."""
        self._portable_factories[factory_id] = factory

    def add_data_serializable_factory(self, factory_id: int, factory: object) -> None:
        """Add a data serializable factory."""
        self._data_serializable_factories[factory_id] = factory

    @classmethod
    def from_dict(cls, data: dict) -> "SerializationConfig":
        """Create SerializationConfig from a dictionary."""
        return cls(
            portable_version=data.get("portable_version", 0),
            default_integer_type=data.get("default_integer_type", "INT"),
        )


class ClientConfig:
    """Configuration for the Hazelcast client."""

    def __init__(self):
        """Initialize client configuration with defaults."""
        self._cluster_name: str = "dev"
        self._client_name: Optional[str] = None
        self._network: NetworkConfig = NetworkConfig()
        self._connection_strategy: ConnectionStrategyConfig = ConnectionStrategyConfig()
        self._security: SecurityConfig = SecurityConfig()
        self._serialization: SerializationConfig = SerializationConfig()
        self._near_caches: Dict[str, NearCacheConfig] = {}
        self._labels: List[str] = []

    @property
    def cluster_name(self) -> str:
        """Get the cluster name."""
        return self._cluster_name

    @cluster_name.setter
    def cluster_name(self, value: str) -> None:
        """Set the cluster name."""
        if not value:
            raise ConfigurationException("cluster_name cannot be empty")
        self._cluster_name = value

    @property
    def client_name(self) -> Optional[str]:
        """Get the client name."""
        return self._client_name

    @client_name.setter
    def client_name(self, value: Optional[str]) -> None:
        """Set the client name."""
        self._client_name = value

    @property
    def cluster_members(self) -> List[str]:
        """Get the list of cluster member addresses (alias for network.addresses)."""
        return self._network.addresses

    @cluster_members.setter
    def cluster_members(self, value: List[str]) -> None:
        """Set the list of cluster member addresses."""
        self._network.addresses = value

    @property
    def connection_timeout(self) -> float:
        """Get the connection timeout in seconds."""
        return self._network.connection_timeout

    @connection_timeout.setter
    def connection_timeout(self, value: float) -> None:
        """Set the connection timeout in seconds."""
        self._network.connection_timeout = value

    @property
    def smart_routing(self) -> bool:
        """Get whether smart routing is enabled."""
        return self._network.smart_routing

    @smart_routing.setter
    def smart_routing(self, value: bool) -> None:
        """Set whether smart routing is enabled."""
        self._network.smart_routing = value

    @property
    def credentials(self) -> Optional[dict]:
        """Get the authentication credentials."""
        if not self._security.is_configured:
            return None
        return {
            "username": self._security.username,
            "password": self._security.password,
        }

    @credentials.setter
    def credentials(self, value: Optional[dict]) -> None:
        """Set the authentication credentials."""
        if value:
            self._security.username = value.get("username")
            self._security.password = value.get("password")

    @property
    def network(self) -> NetworkConfig:
        """Get the network configuration."""
        return self._network

    @network.setter
    def network(self, value: NetworkConfig) -> None:
        """Set the network configuration."""
        self._network = value

    @property
    def connection_strategy(self) -> ConnectionStrategyConfig:
        """Get the connection strategy configuration."""
        return self._connection_strategy

    @connection_strategy.setter
    def connection_strategy(self, value: ConnectionStrategyConfig) -> None:
        """Set the connection strategy configuration."""
        self._connection_strategy = value

    @property
    def security(self) -> SecurityConfig:
        """Get the security configuration."""
        return self._security

    @security.setter
    def security(self, value: SecurityConfig) -> None:
        """Set the security configuration."""
        self._security = value

    @property
    def serialization(self) -> SerializationConfig:
        """Get the serialization configuration."""
        return self._serialization

    @serialization.setter
    def serialization(self, value: SerializationConfig) -> None:
        """Set the serialization configuration."""
        self._serialization = value

    @property
    def near_caches(self) -> Dict[str, NearCacheConfig]:
        """Get near cache configurations."""
        return self._near_caches

    def add_near_cache(self, config: NearCacheConfig) -> None:
        """Add a near cache configuration."""
        self._near_caches[config.name] = config

    @property
    def labels(self) -> List[str]:
        """Get client labels."""
        return self._labels

    @labels.setter
    def labels(self, value: List[str]) -> None:
        """Set client labels."""
        self._labels = value

    @classmethod
    def from_dict(cls, data: dict) -> "ClientConfig":
        """Create ClientConfig from a dictionary."""
        config = cls()

        if "cluster_name" in data:
            config.cluster_name = data["cluster_name"]

        if "client_name" in data:
            config.client_name = data["client_name"]

        if "network" in data:
            config.network = NetworkConfig.from_dict(data["network"])

        if "connection_strategy" in data:
            config.connection_strategy = ConnectionStrategyConfig.from_dict(
                data["connection_strategy"]
            )

        if "security" in data:
            config.security = SecurityConfig.from_dict(data["security"])

        if "serialization" in data:
            config.serialization = SerializationConfig.from_dict(data["serialization"])

        if "near_caches" in data:
            for name, nc_data in data["near_caches"].items():
                config.add_near_cache(NearCacheConfig.from_dict(name, nc_data))

        if "labels" in data:
            config.labels = data["labels"]

        return config

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "ClientConfig":
        """Load configuration from a YAML file.

        Args:
            yaml_path: Path to the YAML configuration file.

        Returns:
            ClientConfig instance.

        Raises:
            ConfigurationException: If the file cannot be read or parsed.
        """
        try:
            import yaml
        except ImportError:
            raise ConfigurationException(
                "PyYAML is required for YAML configuration loading. "
                "Install it with: pip install pyyaml"
            )

        if not os.path.exists(yaml_path):
            raise ConfigurationException(f"Configuration file not found: {yaml_path}")

        try:
            with open(yaml_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationException(f"Failed to parse YAML: {e}")
        except IOError as e:
            raise ConfigurationException(f"Failed to read configuration file: {e}")

        if data is None:
            data = {}

        if "hazelcast_client" in data:
            data = data["hazelcast_client"]

        return cls.from_dict(data)

    @classmethod
    def from_yaml_string(cls, yaml_content: str) -> "ClientConfig":
        """Load configuration from a YAML string.

        Args:
            yaml_content: YAML configuration as a string.

        Returns:
            ClientConfig instance.

        Raises:
            ConfigurationException: If the YAML cannot be parsed.
        """
        try:
            import yaml
        except ImportError:
            raise ConfigurationException(
                "PyYAML is required for YAML configuration loading. "
                "Install it with: pip install pyyaml"
            )

        try:
            data = yaml.safe_load(yaml_content)
        except yaml.YAMLError as e:
            raise ConfigurationException(f"Failed to parse YAML: {e}")

        if data is None:
            data = {}

        if "hazelcast_client" in data:
            data = data["hazelcast_client"]

        return cls.from_dict(data)
