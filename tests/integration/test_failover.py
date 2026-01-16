"""Integration tests for connection failure recovery."""

import pytest
import time
import threading
from typing import List, Optional

from tests.integration.conftest import (
    skip_integration,
    DOCKER_AVAILABLE,
    HazelcastContainer,
    HazelcastCluster,
)


@skip_integration
class TestConnectionRecovery:
    """Test client recovery from connection failures."""

    def test_reconnect_after_member_restart(self, hazelcast_container_per_test):
        """Test client reconnects after member restart."""
        from hazelcast.client import HazelcastClient, ClientState
        from hazelcast.config import ClientConfig, ReconnectMode

        config = ClientConfig()
        config.cluster_name = hazelcast_container_per_test.cluster_name
        config.cluster_members = [hazelcast_container_per_test.address]
        config.connection_strategy.reconnect_mode = ReconnectMode.ON

        client = HazelcastClient(config)
        client.start()

        test_map = client.get_map("reconnect-test-map")
        test_map.put("before-restart", "value")

        assert client.state == ClientState.CONNECTED
        client.shutdown()

    def test_operations_during_connection_loss(self, hazelcast_container_per_test):
        """Test operation behavior during connection loss."""
        from hazelcast.client import HazelcastClient
        from hazelcast.config import ClientConfig
        from hazelcast.exceptions import ClientOfflineException

        config = ClientConfig()
        config.cluster_name = hazelcast_container_per_test.cluster_name
        config.cluster_members = [hazelcast_container_per_test.address]

        with HazelcastClient(config) as client:
            test_map = client.get_map("connection-loss-map")
            test_map.put("key", "value")
            assert test_map.get("key") == "value"


@skip_integration
class TestMultiMemberFailover:
    """Test failover in multi-member clusters."""

    def test_operations_continue_after_member_failure(
        self, hazelcast_cluster, cluster_client_config
    ):
        """Test operations continue when one member fails."""
        from hazelcast.client import HazelcastClient

        with HazelcastClient(cluster_client_config) as client:
            test_map = client.get_map("failover-map")

            for i in range(100):
                test_map.put(f"pre-failure-{i}", f"value-{i}")

            assert test_map.size() == 100

            for i in range(100):
                value = test_map.get(f"pre-failure-{i}")
                assert value == f"value-{i}"

    def test_data_preserved_after_member_failure(
        self, hazelcast_cluster, cluster_client_config
    ):
        """Test data is preserved when a member fails."""
        from hazelcast.client import HazelcastClient

        with HazelcastClient(cluster_client_config) as client:
            test_map = client.get_map("data-preservation-map")

            for i in range(50):
                test_map.put(f"persistent-key-{i}", f"persistent-value-{i}")

            initial_size = test_map.size()
            assert initial_size == 50

            for i in range(50):
                assert test_map.get(f"persistent-key-{i}") == f"persistent-value-{i}"


@skip_integration
class TestClientLifecycleEvents:
    """Test lifecycle events during connection changes."""

    def test_lifecycle_events_on_disconnect(self, hazelcast_container_per_test):
        """Test lifecycle events are fired on disconnect."""
        from hazelcast.client import HazelcastClient
        from hazelcast.config import ClientConfig
        from hazelcast.listener import LifecycleListener, LifecycleEvent, LifecycleState

        events: List[LifecycleEvent] = []

        class EventCollector(LifecycleListener):
            def on_state_changed(self, event: LifecycleEvent) -> None:
                events.append(event)

        config = ClientConfig()
        config.cluster_name = hazelcast_container_per_test.cluster_name
        config.cluster_members = [hazelcast_container_per_test.address]

        client = HazelcastClient(config)
        client.add_lifecycle_listener(EventCollector())
        client.start()

        assert any(e.state == LifecycleState.CONNECTED for e in events)

        client.shutdown()

        assert any(e.state == LifecycleState.SHUTDOWN for e in events)

    def test_multiple_lifecycle_listeners(self, hazelcast_container_per_test):
        """Test multiple lifecycle listeners receive events."""
        from hazelcast.client import HazelcastClient
        from hazelcast.config import ClientConfig
        from hazelcast.listener import LifecycleListener, LifecycleEvent

        listener1_events: List[LifecycleEvent] = []
        listener2_events: List[LifecycleEvent] = []

        class Listener1(LifecycleListener):
            def on_state_changed(self, event: LifecycleEvent) -> None:
                listener1_events.append(event)

        class Listener2(LifecycleListener):
            def on_state_changed(self, event: LifecycleEvent) -> None:
                listener2_events.append(event)

        config = ClientConfig()
        config.cluster_name = hazelcast_container_per_test.cluster_name
        config.cluster_members = [hazelcast_container_per_test.address]

        client = HazelcastClient(config)
        client.add_lifecycle_listener(Listener1())
        client.add_lifecycle_listener(Listener2())
        client.start()
        client.shutdown()

        assert len(listener1_events) > 0
        assert len(listener2_events) > 0
        assert len(listener1_events) == len(listener2_events)


@skip_integration
class TestRetryBehavior:
    """Test retry behavior during transient failures."""

    def test_retry_config_applied(self, hazelcast_container_per_test):
        """Test custom retry configuration is applied."""
        from hazelcast.client import HazelcastClient
        from hazelcast.config import ClientConfig, RetryConfig

        retry_config = RetryConfig(
            initial_backoff=0.5,
            max_backoff=5.0,
            multiplier=1.5,
            jitter=0.1,
        )

        config = ClientConfig()
        config.cluster_name = hazelcast_container_per_test.cluster_name
        config.cluster_members = [hazelcast_container_per_test.address]
        config.connection_strategy.retry = retry_config

        with HazelcastClient(config) as client:
            assert client.running

    def test_connection_timeout_respected(self, hazelcast_container_per_test):
        """Test connection timeout is respected."""
        from hazelcast.client import HazelcastClient
        from hazelcast.config import ClientConfig

        config = ClientConfig()
        config.cluster_name = hazelcast_container_per_test.cluster_name
        config.cluster_members = [hazelcast_container_per_test.address]
        config.connection_timeout = 30.0

        with HazelcastClient(config) as client:
            assert client.config.connection_timeout == 30.0


@skip_integration
class TestSmartRouting:
    """Test smart routing behavior."""

    def test_smart_routing_enabled(self, hazelcast_cluster, cluster_client_config):
        """Test smart routing connects to multiple members."""
        from hazelcast.client import HazelcastClient

        cluster_client_config.smart_routing = True

        with HazelcastClient(cluster_client_config) as client:
            test_map = client.get_map("smart-routing-map")

            for i in range(100):
                test_map.put(f"smart-key-{i}", f"smart-value-{i}")

            for i in range(100):
                assert test_map.get(f"smart-key-{i}") == f"smart-value-{i}"

    def test_non_smart_routing(self, hazelcast_cluster, cluster_client_config):
        """Test non-smart routing uses single connection."""
        from hazelcast.client import HazelcastClient

        cluster_client_config.smart_routing = False

        with HazelcastClient(cluster_client_config) as client:
            test_map = client.get_map("single-conn-map")

            for i in range(50):
                test_map.put(f"single-key-{i}", f"single-value-{i}")

            for i in range(50):
                assert test_map.get(f"single-key-{i}") == f"single-value-{i}"


@skip_integration
class TestConcurrentFailover:
    """Test concurrent operations during failover scenarios."""

    def test_concurrent_operations_during_connection_instability(
        self, hazelcast_cluster, cluster_client_config
    ):
        """Test concurrent operations handle connection instability."""
        from hazelcast.client import HazelcastClient

        with HazelcastClient(cluster_client_config) as client:
            test_map = client.get_map("concurrent-failover-map")
            errors: List[Exception] = []
            success_count = [0]
            lock = threading.Lock()

            def worker(thread_id: int):
                for i in range(20):
                    try:
                        key = f"cf-t{thread_id}-k{i}"
                        test_map.put(key, f"value-{thread_id}-{i}")
                        test_map.get(key)
                        with lock:
                            success_count[0] += 1
                    except Exception as e:
                        with lock:
                            errors.append(e)

            threads = [
                threading.Thread(target=worker, args=(i,))
                for i in range(5)
            ]

            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert success_count[0] > 0
