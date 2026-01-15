"""Unit tests for hazelcast.failover module."""

import pytest
from unittest.mock import patch, MagicMock

from hazelcast.failover import (
    ClusterConfig,
    CNAMEResolver,
    FailoverConfig,
)
from hazelcast.config import ClientConfig


class TestClusterConfig:
    """Tests for ClusterConfig class."""

    def test_init(self):
        config = ClusterConfig(
            cluster_name="test-cluster",
            addresses=["192.168.1.1:5701"],
            priority=0,
        )
        assert config.cluster_name == "test-cluster"
        assert config.addresses == ["192.168.1.1:5701"]
        assert config.priority == 0

    def test_default_addresses(self):
        config = ClusterConfig(cluster_name="test")
        assert config.addresses == ["localhost:5701"]


class TestCNAMEResolver:
    """Tests for CNAMEResolver class."""

    def test_init(self):
        resolver = CNAMEResolver("cluster.example.com", port=5702)
        assert resolver.dns_name == "cluster.example.com"
        assert resolver.port == 5702

    def test_resolve_caches_result(self):
        resolver = CNAMEResolver("localhost", port=5701)
        with patch("socket.gethostbyname_ex") as mock_dns:
            mock_dns.return_value = ("localhost", [], ["127.0.0.1"])
            result1 = resolver.resolve()
            result2 = resolver.resolve()
            assert mock_dns.call_count == 1

    def test_resolve_returns_addresses(self):
        resolver = CNAMEResolver("test.example.com", port=5701)
        with patch("socket.gethostbyname_ex") as mock_dns:
            mock_dns.return_value = ("test.example.com", [], ["10.0.0.1", "10.0.0.2"])
            result = resolver.resolve()
            assert "10.0.0.1:5701" in result
            assert "10.0.0.2:5701" in result

    def test_resolve_failure_returns_cached(self):
        resolver = CNAMEResolver("test.example.com", port=5701)
        resolver._resolved_addresses = ["cached:5701"]
        import socket
        with patch("socket.gethostbyname_ex", side_effect=socket.gaierror):
            result = resolver.resolve()
            assert result == ["cached:5701"]

    def test_refresh(self):
        resolver = CNAMEResolver("test.example.com", port=5701)
        resolver._last_refresh = 9999999999
        resolver._resolved_addresses = ["old:5701"]
        with patch("socket.gethostbyname_ex") as mock_dns:
            mock_dns.return_value = ("test", [], ["10.0.0.1"])
            result = resolver.refresh()
            assert "10.0.0.1:5701" in result


class TestFailoverConfig:
    """Tests for FailoverConfig class."""

    def test_init(self):
        config = FailoverConfig()
        assert config.try_count == FailoverConfig.DEFAULT_TRY_COUNT
        assert config.clusters == []
        assert config.cluster_count == 0

    def test_init_with_try_count(self):
        config = FailoverConfig(try_count=5)
        assert config.try_count == 5

    def test_try_count_setter(self):
        config = FailoverConfig()
        config.try_count = 10
        assert config.try_count == 10

    def test_try_count_invalid(self):
        config = FailoverConfig()
        with pytest.raises(ValueError):
            config.try_count = 0

    def test_add_cluster(self):
        config = FailoverConfig()
        config.add_cluster("cluster1", ["addr1:5701"])
        assert config.cluster_count == 1
        assert config.clusters[0].cluster_name == "cluster1"

    def test_add_cluster_chaining(self):
        config = FailoverConfig()
        result = config.add_cluster("c1", ["a1"]).add_cluster("c2", ["a2"])
        assert result is config
        assert config.cluster_count == 2

    def test_add_cluster_priority_sorting(self):
        config = FailoverConfig()
        config.add_cluster("low", ["a1"], priority=10)
        config.add_cluster("high", ["a2"], priority=1)
        assert config.clusters[0].cluster_name == "high"
        assert config.clusters[1].cluster_name == "low"

    def test_add_cname_cluster(self):
        config = FailoverConfig()
        config.add_cname_cluster(
            "cluster1",
            "cluster.example.com",
            port=5701,
        )
        assert config.cluster_count == 1
        assert "cluster1" in config._cname_resolvers

    def test_get_current_cluster(self):
        config = FailoverConfig()
        config.add_cluster("cluster1", ["addr1"])
        current = config.get_current_cluster()
        assert current.cluster_name == "cluster1"

    def test_get_current_cluster_empty(self):
        config = FailoverConfig()
        assert config.get_current_cluster() is None

    def test_get_cluster_addresses_static(self):
        config = FailoverConfig()
        config.add_cluster("cluster1", ["addr1:5701", "addr2:5702"])
        addresses = config.get_cluster_addresses("cluster1")
        assert addresses == ["addr1:5701", "addr2:5702"]

    def test_get_cluster_addresses_cname(self):
        config = FailoverConfig()
        config.add_cname_cluster("cluster1", "test.example.com")
        with patch("socket.gethostbyname_ex") as mock_dns:
            mock_dns.return_value = ("test", [], ["10.0.0.1"])
            addresses = config.get_cluster_addresses("cluster1")
            assert "10.0.0.1:5701" in addresses

    def test_get_cluster_addresses_not_found(self):
        config = FailoverConfig()
        addresses = config.get_cluster_addresses("nonexistent")
        assert addresses == []

    def test_switch_to_next_cluster(self):
        config = FailoverConfig()
        config.add_cluster("c1", ["a1"])
        config.add_cluster("c2", ["a2"])
        assert config.current_cluster_index == 0
        next_cluster = config.switch_to_next_cluster()
        assert next_cluster.cluster_name == "c2"
        assert config.current_cluster_index == 1

    def test_switch_to_next_cluster_wraps(self):
        config = FailoverConfig()
        config.add_cluster("c1", ["a1"])
        config.add_cluster("c2", ["a2"])
        config.switch_to_next_cluster()
        next_cluster = config.switch_to_next_cluster()
        assert next_cluster.cluster_name == "c1"
        assert config.current_cluster_index == 0

    def test_switch_to_next_cluster_empty(self):
        config = FailoverConfig()
        assert config.switch_to_next_cluster() is None

    def test_reset(self):
        config = FailoverConfig()
        config.add_cluster("c1", ["a1"])
        config.add_cluster("c2", ["a2"])
        config.switch_to_next_cluster()
        config.reset()
        assert config.current_cluster_index == 0

    def test_to_client_config(self):
        config = FailoverConfig()
        config.add_cluster("test-cluster", ["addr1:5701"])
        client_config = config.to_client_config(0)
        assert client_config.cluster_name == "test-cluster"
        assert "addr1:5701" in client_config.cluster_members

    def test_to_client_config_empty(self):
        config = FailoverConfig()
        client_config = config.to_client_config(0)
        assert client_config.cluster_name == "dev"

    def test_to_client_config_out_of_range(self):
        config = FailoverConfig()
        config.add_cluster("c1", ["a1"])
        client_config = config.to_client_config(10)
        assert client_config.cluster_name == "dev"

    def test_from_configs(self):
        config1 = ClientConfig()
        config1.cluster_name = "cluster1"
        config1.cluster_members = ["addr1:5701"]

        config2 = ClientConfig()
        config2.cluster_name = "cluster2"
        config2.cluster_members = ["addr2:5702"]

        failover = FailoverConfig.from_configs([config1, config2])
        assert failover.cluster_count == 2
        assert failover.clusters[0].cluster_name == "cluster1"
        assert failover.clusters[1].cluster_name == "cluster2"

    def test_repr(self):
        config = FailoverConfig(try_count=5)
        config.add_cluster("c1", ["a1"])
        r = repr(config)
        assert "FailoverConfig" in r
        assert "try_count=5" in r
        assert "c1" in r
