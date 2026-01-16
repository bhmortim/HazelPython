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


class TestCNAMEResolverEdgeCases:
    """Additional edge case tests for CNAMEResolver."""

    @patch("socket.gethostbyname_ex")
    def test_resolve_empty_response(self, mock_dns):
        """Test resolve handles empty DNS response."""
        mock_dns.return_value = ("test.example.com", [], [])
        resolver = CNAMEResolver(dns_name="test.example.com")

        addresses = resolver.resolve()

        assert addresses == []

    @patch("socket.gethostbyname_ex")
    def test_resolve_single_ip(self, mock_dns):
        """Test resolve handles single IP response."""
        mock_dns.return_value = ("test", [], ["10.0.0.1"])
        resolver = CNAMEResolver(dns_name="test.example.com", port=5703)

        addresses = resolver.resolve()

        assert addresses == ["10.0.0.1:5703"]

    @patch("socket.gethostbyname_ex")
    def test_resolve_many_ips(self, mock_dns):
        """Test resolve handles many IPs in response."""
        ips = [f"10.0.0.{i}" for i in range(1, 11)]
        mock_dns.return_value = ("test", [], ips)
        resolver = CNAMEResolver(dns_name="test.example.com")

        addresses = resolver.resolve()

        assert len(addresses) == 10
        assert addresses[0] == "10.0.0.1:5701"
        assert addresses[9] == "10.0.0.10:5701"

    @patch("socket.gethostbyname_ex")
    def test_resolve_updates_on_dns_change(self, mock_dns):
        """Test resolver picks up DNS changes after refresh interval."""
        mock_dns.return_value = ("test", [], ["10.0.0.1"])
        resolver = CNAMEResolver(dns_name="test.example.com", refresh_interval=0.01)

        first = resolver.resolve()
        assert first == ["10.0.0.1:5701"]

        mock_dns.return_value = ("test", [], ["10.0.0.2", "10.0.0.3"])
        time.sleep(0.02)

        second = resolver.resolve()
        assert second == ["10.0.0.2:5701", "10.0.0.3:5701"]

    @patch("socket.gethostbyname_ex")
    def test_resolve_recovers_after_transient_failure(self, mock_dns):
        """Test resolver recovers after transient DNS failure."""
        mock_dns.return_value = ("test", [], ["10.0.0.1"])
        resolver = CNAMEResolver(dns_name="test.example.com", refresh_interval=0.01)

        first = resolver.resolve()
        assert first == ["10.0.0.1:5701"]

        mock_dns.side_effect = socket.gaierror("Temporary failure")
        time.sleep(0.02)
        cached = resolver.resolve()
        assert cached == ["10.0.0.1:5701"]

        mock_dns.side_effect = None
        mock_dns.return_value = ("test", [], ["10.0.0.2"])
        time.sleep(0.02)
        recovered = resolver.resolve()
        assert recovered == ["10.0.0.2:5701"]

    @patch("socket.gethostbyname_ex")
    def test_refresh_during_concurrent_resolve(self, mock_dns):
        """Test refresh and resolve can run concurrently."""
        call_count = [0]

        def slow_dns(*args):
            call_count[0] += 1
            time.sleep(0.01)
            return ("test", [], [f"10.0.0.{call_count[0]}"])

        mock_dns.side_effect = slow_dns
        resolver = CNAMEResolver(dns_name="test.example.com", refresh_interval=60.0)

        results = []
        errors = []

        def resolve_task():
            try:
                results.append(resolver.resolve())
            except Exception as e:
                errors.append(e)

        def refresh_task():
            try:
                results.append(resolver.refresh())
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=resolve_task),
            threading.Thread(target=refresh_task),
            threading.Thread(target=resolve_task),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 3


class TestFailoverStateMachine:
    """Tests for failover state machine transitions."""

    def test_initial_state(self):
        """Test initial state is first cluster."""
        config = FailoverConfig(try_count=3)
        config.add_cluster("primary", ["p:5701"], priority=0)
        config.add_cluster("secondary", ["s:5701"], priority=1)

        assert config.current_cluster_index == 0
        assert config.get_current_cluster().cluster_name == "primary"

    def test_state_transition_on_switch(self):
        """Test state transitions correctly on each switch."""
        config = FailoverConfig()
        config.add_cluster("a", ["a:5701"], priority=0)
        config.add_cluster("b", ["b:5701"], priority=1)
        config.add_cluster("c", ["c:5701"], priority=2)

        states = [config.current_cluster_index]

        for _ in range(6):
            config.switch_to_next_cluster()
            states.append(config.current_cluster_index)

        assert states == [0, 1, 2, 0, 1, 2, 0]

    def test_reset_from_any_state(self):
        """Test reset works from any cluster index."""
        config = FailoverConfig()
        for i in range(5):
            config.add_cluster(f"cluster{i}", [f"node{i}:5701"], priority=i)

        for target_index in range(5):
            for _ in range(target_index):
                config.switch_to_next_cluster()

            config.reset()
            assert config.current_cluster_index == 0
            assert config.get_current_cluster().cluster_name == "cluster0"

    def test_single_cluster_switch_stays_on_same(self):
        """Test switching with single cluster stays on same cluster."""
        config = FailoverConfig()
        config.add_cluster("only", ["only:5701"])

        config.switch_to_next_cluster()
        assert config.current_cluster_index == 0
        assert config.get_current_cluster().cluster_name == "only"

        config.switch_to_next_cluster()
        assert config.current_cluster_index == 0

    def test_state_preserved_after_get_operations(self):
        """Test get operations do not change state."""
        config = FailoverConfig()
        config.add_cluster("a", ["a:5701"], priority=0)
        config.add_cluster("b", ["b:5701"], priority=1)

        config.switch_to_next_cluster()
        initial_index = config.current_cluster_index

        config.get_current_cluster()
        config.get_cluster_addresses("a")
        config.get_cluster_addresses("b")
        _ = config.clusters
        _ = config.cluster_count

        assert config.current_cluster_index == initial_index


class TestFailoverRetryExhaustion:
    """Tests for retry exhaustion and failover triggering."""

    def test_try_count_configuration(self):
        """Test try_count is correctly configured."""
        config = FailoverConfig(try_count=5)
        assert config.try_count == 5

        config.try_count = 10
        assert config.try_count == 10

    def test_try_count_minimum_boundary(self):
        """Test try_count boundary at minimum value."""
        config = FailoverConfig(try_count=1)
        assert config.try_count == 1

        with pytest.raises(ValueError):
            config.try_count = 0

    def test_simulated_retry_exhaustion_triggers_switch(self):
        """Simulate retry exhaustion leading to cluster switch."""
        config = FailoverConfig(try_count=3)
        config.add_cluster("primary", ["p1:5701", "p2:5701"], priority=0)
        config.add_cluster("backup", ["b1:5701"], priority=1)

        connection_attempts = 0
        max_attempts = config.try_count

        for attempt in range(max_attempts):
            connection_attempts += 1
            connection_failed = True

            if connection_failed and attempt == max_attempts - 1:
                config.switch_to_next_cluster()

        assert connection_attempts == 3
        assert config.get_current_cluster().cluster_name == "backup"

    def test_full_failover_cycle_with_exhaustion(self):
        """Test complete failover cycle through all clusters."""
        config = FailoverConfig(try_count=2)
        config.add_cluster("dc1", ["dc1:5701"], priority=0)
        config.add_cluster("dc2", ["dc2:5701"], priority=1)
        config.add_cluster("dc3", ["dc3:5701"], priority=2)

        visited_clusters = []

        for cycle in range(2):
            for cluster_idx in range(config.cluster_count):
                current = config.get_current_cluster()
                visited_clusters.append(current.cluster_name)

                for _ in range(config.try_count):
                    pass

                config.switch_to_next_cluster()

        assert visited_clusters == ["dc1", "dc2", "dc3", "dc1", "dc2", "dc3"]


class TestMultiClusterIntegration:
    """Integration-style tests for multi-cluster failover scenarios."""

    def test_three_tier_failover_hierarchy(self):
        """Test three-tier failover: local -> regional -> global."""
        config = FailoverConfig(try_count=2)
        config.add_cluster(
            "local",
            ["local1:5701", "local2:5701", "local3:5701"],
            priority=0,
        )
        config.add_cluster(
            "regional",
            ["regional1:5701", "regional2:5701"],
            priority=1,
        )
        config.add_cluster(
            "global",
            ["global1:5701"],
            priority=2,
        )

        assert config.get_current_cluster().cluster_name == "local"
        assert len(config.get_cluster_addresses("local")) == 3

        config.switch_to_next_cluster()
        assert config.get_current_cluster().cluster_name == "regional"

        config.switch_to_next_cluster()
        assert config.get_current_cluster().cluster_name == "global"

        config.switch_to_next_cluster()
        assert config.get_current_cluster().cluster_name == "local"

    @patch("socket.gethostbyname_ex")
    def test_mixed_static_and_cname_clusters(self, mock_dns):
        """Test failover between static and CNAME-based clusters."""
        mock_dns.return_value = ("dynamic", [], ["10.0.0.1", "10.0.0.2"])

        config = FailoverConfig(try_count=3)
        config.add_cluster("static-primary", ["static1:5701", "static2:5701"], priority=0)
        config.add_cname_cluster(
            "dynamic-backup",
            dns_name="backup.example.com",
            priority=1,
        )
        config.add_cluster("static-dr", ["dr1:5701"], priority=2)

        assert config.get_current_cluster().cluster_name == "static-primary"
        static_addrs = config.get_cluster_addresses("static-primary")
        assert static_addrs == ["static1:5701", "static2:5701"]

        config.switch_to_next_cluster()
        assert config.get_current_cluster().cluster_name == "dynamic-backup"
        dynamic_addrs = config.get_cluster_addresses("dynamic-backup")
        assert dynamic_addrs == ["10.0.0.1:5701", "10.0.0.2:5701"]

        config.switch_to_next_cluster()
        assert config.get_current_cluster().cluster_name == "static-dr"

    @patch("socket.gethostbyname_ex")
    def test_cname_cluster_membership_change_during_failover(self, mock_dns):
        """Test CNAME cluster membership changes are picked up."""
        mock_dns.return_value = ("test", [], ["10.0.0.1"])

        config = FailoverConfig(try_count=2)
        config.add_cname_cluster(
            "dynamic",
            dns_name="cluster.example.com",
            refresh_interval=0.01,
        )
        config.add_cluster("static", ["static:5701"], priority=1)

        addrs1 = config.get_cluster_addresses("dynamic")
        assert addrs1 == ["10.0.0.1:5701"]

        config.switch_to_next_cluster()
        assert config.get_current_cluster().cluster_name == "static"

        mock_dns.return_value = ("test", [], ["10.0.0.2", "10.0.0.3"])
        time.sleep(0.02)

        config.switch_to_next_cluster()
        assert config.get_current_cluster().cluster_name == "dynamic"

        addrs2 = config.get_cluster_addresses("dynamic")
        assert addrs2 == ["10.0.0.2:5701", "10.0.0.3:5701"]

    def test_failover_and_recovery_to_primary(self):
        """Test failover to backup and recovery to primary."""
        config = FailoverConfig(try_count=3)
        config.add_cluster("primary", ["p1:5701", "p2:5701"], priority=0)
        config.add_cluster("backup", ["b1:5701"], priority=1)

        assert config.get_current_cluster().cluster_name == "primary"

        config.switch_to_next_cluster()
        assert config.get_current_cluster().cluster_name == "backup"

        config.reset()
        assert config.get_current_cluster().cluster_name == "primary"

    def test_client_config_generation_for_each_cluster(self):
        """Test ClientConfig generation for all clusters in failover."""
        config = FailoverConfig(try_count=2)
        config.add_cluster("prod", ["prod1:5701", "prod2:5701"], priority=0)
        config.add_cluster("staging", ["staging1:5701"], priority=1)

        prod_config = config.to_client_config(0)
        assert prod_config.cluster_name == "prod"
        assert prod_config.cluster_members == ["prod1:5701", "prod2:5701"]

        staging_config = config.to_client_config(1)
        assert staging_config.cluster_name == "staging"
        assert staging_config.cluster_members == ["staging1:5701"]

    def test_concurrent_failover_operations(self):
        """Test thread-safe concurrent failover operations."""
        config = FailoverConfig(try_count=2)
        for i in range(5):
            config.add_cluster(f"cluster{i}", [f"node{i}:5701"], priority=i)

        errors = []
        operations_completed = []

        def switch_worker():
            try:
                for _ in range(10):
                    config.switch_to_next_cluster()
                    operations_completed.append("switch")
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        def reset_worker():
            try:
                for _ in range(5):
                    time.sleep(0.005)
                    config.reset()
                    operations_completed.append("reset")
            except Exception as e:
                errors.append(e)

        def read_worker():
            try:
                for _ in range(15):
                    _ = config.get_current_cluster()
                    _ = config.current_cluster_index
                    operations_completed.append("read")
                    time.sleep(0.001)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=switch_worker),
            threading.Thread(target=switch_worker),
            threading.Thread(target=reset_worker),
            threading.Thread(target=read_worker),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(operations_completed) > 0

    def test_failover_with_priority_reordering(self):
        """Test clusters are correctly ordered by priority."""
        config = FailoverConfig()

        config.add_cluster("low-priority", ["low:5701"], priority=10)
        config.add_cluster("high-priority", ["high:5701"], priority=0)
        config.add_cluster("medium-priority", ["med:5701"], priority=5)

        cluster_order = [c.cluster_name for c in config.clusters]
        assert cluster_order == ["high-priority", "medium-priority", "low-priority"]

        assert config.get_current_cluster().cluster_name == "high-priority"

    def test_from_configs_preserves_order_as_priority(self):
        """Test from_configs uses list order as priority."""
        configs = []
        for name in ["first", "second", "third"]:
            mock_config = MagicMock()
            mock_config.cluster_name = name
            mock_config.cluster_members = [f"{name}:5701"]
            configs.append(mock_config)

        failover = FailoverConfig.from_configs(configs)

        assert failover.clusters[0].cluster_name == "first"
        assert failover.clusters[0].priority == 0
        assert failover.clusters[1].cluster_name == "second"
        assert failover.clusters[1].priority == 1
        assert failover.clusters[2].cluster_name == "third"
        assert failover.clusters[2].priority == 2

    @patch("socket.gethostbyname_ex")
    def test_all_cname_clusters_failover(self, mock_dns):
        """Test failover between multiple CNAME-based clusters."""
        dns_responses = {
            "primary.example.com": ["10.0.1.1", "10.0.1.2"],
            "backup.example.com": ["10.0.2.1"],
            "dr.example.com": ["10.0.3.1", "10.0.3.2", "10.0.3.3"],
        }

        def dns_lookup(name):
            return (name, [], dns_responses.get(name, []))

        mock_dns.side_effect = dns_lookup

        config = FailoverConfig(try_count=2)
        config.add_cname_cluster("primary", dns_name="primary.example.com", priority=0)
        config.add_cname_cluster("backup", dns_name="backup.example.com", priority=1)
        config.add_cname_cluster("dr", dns_name="dr.example.com", priority=2)

        primary_addrs = config.get_cluster_addresses("primary")
        assert len(primary_addrs) == 2

        config.switch_to_next_cluster()
        backup_addrs = config.get_cluster_addresses("backup")
        assert len(backup_addrs) == 1

        config.switch_to_next_cluster()
        dr_addrs = config.get_cluster_addresses("dr")
        assert len(dr_addrs) == 3

    def test_empty_failover_config_edge_cases(self):
        """Test edge cases with empty failover configuration."""
        config = FailoverConfig()

        assert config.get_current_cluster() is None
        assert config.switch_to_next_cluster() is None
        assert config.get_cluster_addresses("nonexistent") == []
        assert config.cluster_count == 0

        config.reset()
        assert config.current_cluster_index == 0
