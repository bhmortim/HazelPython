"""Unit tests for hazelcast/failover.py."""

import socket
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from hazelcast.failover import ClusterConfig, CNAMEResolver, FailoverConfig


class TestClusterConfig:
    """Tests for the ClusterConfig dataclass."""

    def test_init_with_all_fields(self):
        """Test ClusterConfig initialization with all fields."""
        config = ClusterConfig(
            cluster_name="production",
            addresses=["node1:5701", "node2:5701"],
            priority=1,
        )

        assert config.cluster_name == "production"
        assert config.addresses == ["node1:5701", "node2:5701"]
        assert config.priority == 1

    def test_init_default_addresses(self):
        """Test ClusterConfig uses default address when empty."""
        config = ClusterConfig(cluster_name="dev", addresses=[])

        assert config.addresses == ["localhost:5701"]

    def test_init_with_none_addresses(self):
        """Test ClusterConfig default_factory for addresses."""
        config = ClusterConfig(cluster_name="test")

        assert config.addresses == ["localhost:5701"]

    def test_init_default_priority(self):
        """Test ClusterConfig default priority is 0."""
        config = ClusterConfig(cluster_name="test", addresses=["node:5701"])

        assert config.priority == 0


class TestCNAMEResolver:
    """Tests for the CNAMEResolver class."""

    def test_init(self):
        """Test CNAMEResolver initialization."""
        resolver = CNAMEResolver(
            dns_name="hazelcast.example.com",
            port=5702,
            refresh_interval=30.0,
        )

        assert resolver.dns_name == "hazelcast.example.com"
        assert resolver.port == 5702
        assert resolver._refresh_interval == 30.0

    def test_init_defaults(self):
        """Test CNAMEResolver default values."""
        resolver = CNAMEResolver(dns_name="test.example.com")

        assert resolver.port == 5701
        assert resolver._refresh_interval == 60.0

    @patch("socket.gethostbyname_ex")
    def test_resolve_success(self, mock_dns):
        """Test resolve returns formatted addresses."""
        mock_dns.return_value = (
            "hazelcast.example.com",
            [],
            ["192.168.1.10", "192.168.1.11"],
        )
        resolver = CNAMEResolver(dns_name="hazelcast.example.com", port=5701)

        addresses = resolver.resolve()

        assert addresses == ["192.168.1.10:5701", "192.168.1.11:5701"]
        mock_dns.assert_called_once_with("hazelcast.example.com")

    @patch("socket.gethostbyname_ex")
    def test_resolve_caching(self, mock_dns):
        """Test resolve uses cache within refresh interval."""
        mock_dns.return_value = ("test", [], ["10.0.0.1"])
        resolver = CNAMEResolver(
            dns_name="test.example.com",
            refresh_interval=60.0,
        )

        resolver.resolve()
        resolver.resolve()
        resolver.resolve()

        mock_dns.assert_called_once()

    @patch("socket.gethostbyname_ex")
    def test_resolve_cache_expired(self, mock_dns):
        """Test resolve refreshes after interval expires."""
        mock_dns.return_value = ("test", [], ["10.0.0.1"])
        resolver = CNAMEResolver(
            dns_name="test.example.com",
            refresh_interval=0.01,
        )

        resolver.resolve()
        time.sleep(0.02)
        resolver.resolve()

        assert mock_dns.call_count == 2

    @patch("socket.gethostbyname_ex")
    def test_resolve_dns_failure_returns_cached(self, mock_dns):
        """Test resolve returns cached addresses on DNS failure."""
        mock_dns.return_value = ("test", [], ["10.0.0.1"])
        resolver = CNAMEResolver(
            dns_name="test.example.com",
            refresh_interval=0.001,
        )

        first_result = resolver.resolve()
        time.sleep(0.01)

        mock_dns.side_effect = socket.gaierror("DNS lookup failed")
        second_result = resolver.resolve()

        assert first_result == ["10.0.0.1:5701"]
        assert second_result == ["10.0.0.1:5701"]

    @patch("socket.gethostbyname_ex")
    def test_resolve_dns_failure_no_cache(self, mock_dns):
        """Test resolve returns empty list on DNS failure with no cache."""
        mock_dns.side_effect = socket.gaierror("DNS lookup failed")
        resolver = CNAMEResolver(dns_name="test.example.com")

        addresses = resolver.resolve()

        assert addresses == []

    @patch("socket.gethostbyname_ex")
    def test_refresh_forces_lookup(self, mock_dns):
        """Test refresh ignores cache and performs new lookup."""
        mock_dns.return_value = ("test", [], ["10.0.0.1"])
        resolver = CNAMEResolver(
            dns_name="test.example.com",
            refresh_interval=60.0,
        )

        resolver.resolve()
        mock_dns.return_value = ("test", [], ["10.0.0.2"])
        result = resolver.refresh()

        assert result == ["10.0.0.2:5701"]
        assert mock_dns.call_count == 2

    @patch("socket.gethostbyname_ex")
    def test_resolve_thread_safety(self, mock_dns):
        """Test resolve is thread-safe."""
        mock_dns.return_value = ("test", [], ["10.0.0.1"])
        resolver = CNAMEResolver(
            dns_name="test.example.com",
            refresh_interval=0.001,
        )

        results = []
        errors = []

        def resolve_thread():
            try:
                for _ in range(10):
                    result = resolver.resolve()
                    results.append(result)
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=resolve_thread) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert all(r == ["10.0.0.1:5701"] for r in results)


class TestFailoverConfig:
    """Tests for the FailoverConfig class."""

    def test_init_defaults(self):
        """Test FailoverConfig default initialization."""
        config = FailoverConfig()

        assert config.try_count == 3
        assert config.clusters == []
        assert config.cluster_count == 0
        assert config.current_cluster_index == 0

    def test_init_with_try_count(self):
        """Test FailoverConfig with custom try_count."""
        config = FailoverConfig(try_count=5)

        assert config.try_count == 5

    def test_init_with_clusters(self):
        """Test FailoverConfig with pre-configured clusters."""
        clusters = [
            ClusterConfig("cluster1", ["node1:5701"], priority=1),
            ClusterConfig("cluster2", ["node2:5701"], priority=0),
        ]
        config = FailoverConfig(clusters=clusters)

        assert config.cluster_count == 2
        assert config.clusters[0].cluster_name == "cluster2"
        assert config.clusters[1].cluster_name == "cluster1"

    def test_try_count_setter_valid(self):
        """Test try_count setter with valid value."""
        config = FailoverConfig()
        config.try_count = 10

        assert config.try_count == 10

    def test_try_count_setter_invalid(self):
        """Test try_count setter rejects invalid values."""
        config = FailoverConfig()

        with pytest.raises(ValueError, match="must be at least 1"):
            config.try_count = 0

        with pytest.raises(ValueError, match="must be at least 1"):
            config.try_count = -1

    def test_add_cluster(self):
        """Test add_cluster adds and sorts by priority."""
        config = FailoverConfig()

        config.add_cluster("backup", ["backup:5701"], priority=2)
        config.add_cluster("primary", ["primary:5701"], priority=0)
        config.add_cluster("secondary", ["secondary:5701"], priority=1)

        assert config.cluster_count == 3
        assert config.clusters[0].cluster_name == "primary"
        assert config.clusters[1].cluster_name == "secondary"
        assert config.clusters[2].cluster_name == "backup"

    def test_add_cluster_chaining(self):
        """Test add_cluster supports method chaining."""
        config = FailoverConfig()

        result = config.add_cluster("a", ["a:5701"]).add_cluster("b", ["b:5701"])

        assert result is config
        assert config.cluster_count == 2

    @patch("socket.gethostbyname_ex")
    def test_add_cname_cluster(self, mock_dns):
        """Test add_cname_cluster registers resolver."""
        mock_dns.return_value = ("test", [], ["10.0.0.1"])
        config = FailoverConfig()

        config.add_cname_cluster(
            "dynamic",
            dns_name="hazelcast.example.com",
            port=5702,
            priority=0,
            refresh_interval=30.0,
        )

        assert config.cluster_count == 1
        assert "dynamic" in config._cname_resolvers
        addresses = config.get_cluster_addresses("dynamic")
        assert addresses == ["10.0.0.1:5702"]

    def test_get_current_cluster_empty(self):
        """Test get_current_cluster returns None when empty."""
        config = FailoverConfig()

        assert config.get_current_cluster() is None

    def test_get_current_cluster(self):
        """Test get_current_cluster returns current cluster."""
        config = FailoverConfig()
        config.add_cluster("primary", ["primary:5701"])
        config.add_cluster("backup", ["backup:5701"], priority=1)

        current = config.get_current_cluster()

        assert current.cluster_name == "primary"

    def test_get_cluster_addresses_static(self):
        """Test get_cluster_addresses for static cluster."""
        config = FailoverConfig()
        config.add_cluster("test", ["node1:5701", "node2:5701"])

        addresses = config.get_cluster_addresses("test")

        assert addresses == ["node1:5701", "node2:5701"]

    @patch("socket.gethostbyname_ex")
    def test_get_cluster_addresses_cname(self, mock_dns):
        """Test get_cluster_addresses for CNAME cluster."""
        mock_dns.return_value = ("test", [], ["192.168.1.1", "192.168.1.2"])
        config = FailoverConfig()
        config.add_cname_cluster("dynamic", dns_name="test.example.com")

        addresses = config.get_cluster_addresses("dynamic")

        assert addresses == ["192.168.1.1:5701", "192.168.1.2:5701"]

    def test_get_cluster_addresses_not_found(self):
        """Test get_cluster_addresses returns empty for unknown cluster."""
        config = FailoverConfig()

        addresses = config.get_cluster_addresses("unknown")

        assert addresses == []

    def test_switch_to_next_cluster_empty(self):
        """Test switch_to_next_cluster returns None when empty."""
        config = FailoverConfig()

        result = config.switch_to_next_cluster()

        assert result is None

    def test_switch_to_next_cluster_cycles(self):
        """Test switch_to_next_cluster cycles through clusters."""
        config = FailoverConfig()
        config.add_cluster("a", ["a:5701"], priority=0)
        config.add_cluster("b", ["b:5701"], priority=1)
        config.add_cluster("c", ["c:5701"], priority=2)

        assert config.current_cluster_index == 0

        next_cluster = config.switch_to_next_cluster()
        assert next_cluster.cluster_name == "b"
        assert config.current_cluster_index == 1

        next_cluster = config.switch_to_next_cluster()
        assert next_cluster.cluster_name == "c"
        assert config.current_cluster_index == 2

        next_cluster = config.switch_to_next_cluster()
        assert next_cluster.cluster_name == "a"
        assert config.current_cluster_index == 0

    def test_reset(self):
        """Test reset returns to first cluster."""
        config = FailoverConfig()
        config.add_cluster("a", ["a:5701"])
        config.add_cluster("b", ["b:5701"], priority=1)

        config.switch_to_next_cluster()
        assert config.current_cluster_index == 1

        config.reset()
        assert config.current_cluster_index == 0

    def test_to_client_config_empty(self):
        """Test to_client_config returns empty config when no clusters."""
        config = FailoverConfig()

        client_config = config.to_client_config()

        assert client_config.cluster_name == "dev"

    def test_to_client_config(self):
        """Test to_client_config generates correct configuration."""
        config = FailoverConfig()
        config.add_cluster("production", ["prod1:5701", "prod2:5701"])

        client_config = config.to_client_config(0)

        assert client_config.cluster_name == "production"
        assert client_config.cluster_members == ["prod1:5701", "prod2:5701"]

    def test_to_client_config_invalid_index(self):
        """Test to_client_config handles invalid index."""
        config = FailoverConfig()
        config.add_cluster("test", ["test:5701"])

        client_config = config.to_client_config(99)

        assert client_config.cluster_name == "dev"

    def test_from_configs(self):
        """Test from_configs factory method."""
        config1 = MagicMock()
        config1.cluster_name = "primary"
        config1.cluster_members = ["primary:5701"]

        config2 = MagicMock()
        config2.cluster_name = "backup"
        config2.cluster_members = ["backup:5701"]

        failover = FailoverConfig.from_configs([config1, config2])

        assert failover.cluster_count == 2
        assert failover.clusters[0].cluster_name == "primary"
        assert failover.clusters[0].priority == 0
        assert failover.clusters[1].cluster_name == "backup"
        assert failover.clusters[1].priority == 1

    def test_repr(self):
        """Test __repr__ output."""
        config = FailoverConfig(try_count=5)
        config.add_cluster("a", ["a:5701"])
        config.add_cluster("b", ["b:5701"], priority=1)

        result = repr(config)

        assert "try_count=5" in result
        assert "clusters=" in result
        assert "'a'" in result
        assert "'b'" in result

    def test_cluster_switching_simulation(self):
        """Test full cluster switching scenario."""
        config = FailoverConfig(try_count=2)
        config.add_cluster("primary", ["primary1:5701", "primary2:5701"], priority=0)
        config.add_cluster("dr_site", ["dr1:5701"], priority=1)
        config.add_cluster("cloud", ["cloud1:5701"], priority=2)

        assert config.get_current_cluster().cluster_name == "primary"
        assert config.get_cluster_addresses("primary") == ["primary1:5701", "primary2:5701"]

        config.switch_to_next_cluster()
        assert config.get_current_cluster().cluster_name == "dr_site"

        config.switch_to_next_cluster()
        assert config.get_current_cluster().cluster_name == "cloud"

        config.switch_to_next_cluster()
        assert config.get_current_cluster().cluster_name == "primary"

        config.reset()
        assert config.get_current_cluster().cluster_name == "primary"

    def test_concurrent_cluster_access(self):
        """Test thread-safe cluster operations."""
        config = FailoverConfig()
        config.add_cluster("a", ["a:5701"], priority=0)
        config.add_cluster("b", ["b:5701"], priority=1)
        config.add_cluster("c", ["c:5701"], priority=2)

        errors = []
        results = []

        def switch_thread():
            try:
                for _ in range(20):
                    cluster = config.switch_to_next_cluster()
                    results.append(cluster.cluster_name)
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        def read_thread():
            try:
                for _ in range(20):
                    cluster = config.get_current_cluster()
                    if cluster:
                        results.append(cluster.cluster_name)
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        threads = []
        for _ in range(3):
            threads.append(threading.Thread(target=switch_thread))
            threads.append(threading.Thread(target=read_thread))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert all(name in ["a", "b", "c"] for name in results)

    def test_leader_election_scenario(self):
        """Test failover during simulated leader election."""
        config = FailoverConfig(try_count=3)
        config.add_cluster("main", ["leader:5701", "follower1:5701", "follower2:5701"])

        current = config.get_current_cluster()
        addresses = config.get_cluster_addresses(current.cluster_name)

        assert len(addresses) == 3
        assert "leader:5701" in addresses

    @patch("socket.gethostbyname_ex")
    def test_cname_failover_with_dns_change(self, mock_dns):
        """Test CNAME resolution during cluster membership change."""
        mock_dns.return_value = ("test", [], ["10.0.0.1", "10.0.0.2"])

        config = FailoverConfig()
        config.add_cname_cluster(
            "dynamic",
            dns_name="hazelcast.example.com",
            refresh_interval=0.01,
        )

        addresses1 = config.get_cluster_addresses("dynamic")
        assert addresses1 == ["10.0.0.1:5701", "10.0.0.2:5701"]

        mock_dns.return_value = ("test", [], ["10.0.0.3", "10.0.0.4", "10.0.0.5"])
        time.sleep(0.02)

        addresses2 = config.get_cluster_addresses("dynamic")
        assert addresses2 == ["10.0.0.3:5701", "10.0.0.4:5701", "10.0.0.5:5701"]

    def test_static_addresses_immutable(self):
        """Test get_cluster_addresses returns copy for static clusters."""
        config = FailoverConfig()
        config.add_cluster("test", ["node:5701"])

        addresses = config.get_cluster_addresses("test")
        addresses.append("hacker:5701")

        fresh_addresses = config.get_cluster_addresses("test")
        assert fresh_addresses == ["node:5701"]
