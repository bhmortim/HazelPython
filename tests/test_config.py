"""Unit tests for hazelcast.config module."""

import pytest

from hazelcast.config import (
    NetworkConfig,
    ConfigurationException,
)


class TestNetworkConfig:
    """Tests for NetworkConfig class."""

    def test_default_values(self):
        """Test default configuration values."""
        config = NetworkConfig()
        assert config.addresses == ["localhost:5701"]
        assert config.connection_timeout == 5.0
        assert config.smart_routing is True
        assert config.tcp_no_delay is True
        assert config.socket_keep_alive is True
        assert config.socket_send_buffer_size is None
        assert config.socket_receive_buffer_size is None
        assert config.socket_linger_seconds is None

    def test_custom_socket_options(self):
        """Test custom socket option configuration."""
        config = NetworkConfig(
            tcp_no_delay=False,
            socket_keep_alive=False,
            socket_send_buffer_size=65536,
            socket_receive_buffer_size=131072,
            socket_linger_seconds=5,
        )
        assert config.tcp_no_delay is False
        assert config.socket_keep_alive is False
        assert config.socket_send_buffer_size == 65536
        assert config.socket_receive_buffer_size == 131072
        assert config.socket_linger_seconds == 5

    def test_socket_option_setters(self):
        """Test socket option setters."""
        config = NetworkConfig()

        config.tcp_no_delay = False
        assert config.tcp_no_delay is False

        config.socket_keep_alive = False
        assert config.socket_keep_alive is False

        config.socket_send_buffer_size = 32768
        assert config.socket_send_buffer_size == 32768

        config.socket_receive_buffer_size = 65536
        assert config.socket_receive_buffer_size == 65536

        config.socket_linger_seconds = 10
        assert config.socket_linger_seconds == 10

    def test_invalid_socket_send_buffer_size(self):
        """Test validation of socket_send_buffer_size."""
        with pytest.raises(ConfigurationException) as exc_info:
            NetworkConfig(socket_send_buffer_size=0)
        assert "socket_send_buffer_size must be positive" in str(exc_info.value)

        with pytest.raises(ConfigurationException) as exc_info:
            NetworkConfig(socket_send_buffer_size=-1)
        assert "socket_send_buffer_size must be positive" in str(exc_info.value)

    def test_invalid_socket_receive_buffer_size(self):
        """Test validation of socket_receive_buffer_size."""
        with pytest.raises(ConfigurationException) as exc_info:
            NetworkConfig(socket_receive_buffer_size=0)
        assert "socket_receive_buffer_size must be positive" in str(exc_info.value)

        with pytest.raises(ConfigurationException) as exc_info:
            NetworkConfig(socket_receive_buffer_size=-1)
        assert "socket_receive_buffer_size must be positive" in str(exc_info.value)

    def test_invalid_socket_linger_seconds(self):
        """Test validation of socket_linger_seconds."""
        with pytest.raises(ConfigurationException) as exc_info:
            NetworkConfig(socket_linger_seconds=-1)
        assert "socket_linger_seconds cannot be negative" in str(exc_info.value)

    def test_socket_linger_seconds_zero_valid(self):
        """Test that socket_linger_seconds=0 is valid (immediate close)."""
        config = NetworkConfig(socket_linger_seconds=0)
        assert config.socket_linger_seconds == 0

    def test_from_dict_with_socket_options(self):
        """Test creating NetworkConfig from dictionary with socket options."""
        data = {
            "addresses": ["192.168.1.1:5701"],
            "connection_timeout": 10.0,
            "smart_routing": False,
            "tcp_no_delay": False,
            "socket_keep_alive": False,
            "socket_send_buffer_size": 65536,
            "socket_receive_buffer_size": 131072,
            "socket_linger_seconds": 5,
        }
        config = NetworkConfig.from_dict(data)

        assert config.addresses == ["192.168.1.1:5701"]
        assert config.connection_timeout == 10.0
        assert config.smart_routing is False
        assert config.tcp_no_delay is False
        assert config.socket_keep_alive is False
        assert config.socket_send_buffer_size == 65536
        assert config.socket_receive_buffer_size == 131072
        assert config.socket_linger_seconds == 5

    def test_from_dict_defaults(self):
        """Test that from_dict uses defaults for missing socket options."""
        config = NetworkConfig.from_dict({})

        assert config.tcp_no_delay is True
        assert config.socket_keep_alive is True
        assert config.socket_send_buffer_size is None
        assert config.socket_receive_buffer_size is None
        assert config.socket_linger_seconds is None

    def test_setter_validation(self):
        """Test that setters trigger validation."""
        config = NetworkConfig()

        with pytest.raises(ConfigurationException):
            config.socket_send_buffer_size = -1

        with pytest.raises(ConfigurationException):
            config.socket_receive_buffer_size = 0

        with pytest.raises(ConfigurationException):
            config.socket_linger_seconds = -5
