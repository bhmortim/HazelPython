"""Integration tests for cluster failover and reconnection scenarios."""

import pytest
import time
import threading
from typing import List

from tests.integration.conftest import (
    skip_integration,
    DOCKER_AVAILABLE,
    HazelcastCluster,
)


@skip_integration
class TestClientReconnection:
    """Test client reconnection behavior."""

    def test_reconnect_after_connection_loss(self, hazelcast_cluster, cluster_client_config):
        """Test client reconnects after temporary connection loss."""
        from hazelcast.client import HazelcastClient, ClientState
        
        client = HazelcastClient(cluster_client_config)
        client.start()
        
        try:
            test_map = client.get_map("reconnect-test")
            test_map.put("key1", "value1")
            
            assert client.state == ClientState.CONNECTED
            
            assert test_map.get("key1") == "value1"
        finally:
            client.shutdown()

    def test_operations_during_reconnection(self, hazelcast_cluster, cluster_client_config):
        """Test operations queue during reconnection."""
        from hazelcast.client import HazelcastClient
        
        client = HazelcastClient(cluster_client_config)
        client.start()
        
        try:
            test_map = client.get_map("operations-during-reconnect")
            
            for i in range(100):
                test_map.put(f"key-{i}", f"value-{i}")
            
            assert test_map.size() == 100
        finally:
            client.shutdown()


@skip_integration
class TestClusterFailover:
    """Test cluster failover scenarios."""

    def test_failover_to_another_member(self, hazelcast_cluster, cluster_client_config):
        """Test client fails over to another cluster member."""
        from hazelcast.client import HazelcastClient
        
        client = HazelcastClient(cluster_client_config)
        client.start()
        
        try:
            test_map = client.get_map("failover-test")
            test_map.put("key1", "value1")
            
            assert test_map.get("key1") == "value1"
        finally:
            client.shutdown()

    def test_data_consistency_after_failover(self, hazelcast_cluster, cluster_client_config):
        """Test data consistency is maintained after failover."""
        from hazelcast.client import HazelcastClient
        
        client = HazelcastClient(cluster_client_config)
        client.start()
        
        try:
            test_map = client.get_map("consistency-test")
            
            for i in range(50):
                test_map.put(f"key-{i}", f"value-{i}")
            
            for i in range(50):
                value = test_map.get(f"key-{i}")
                assert value == f"value-{i}"
        finally:
            client.shutdown()


@skip_integration
class TestMembershipEvents:
    """Test membership event handling during failover."""

    def test_member_left_event(self, hazelcast_cluster, cluster_client_config):
        """Test member left event is received."""
        from hazelcast.client import HazelcastClient
        from hazelcast.listener import MembershipListener, MembershipEvent
        
        events: List[MembershipEvent] = []
        
        class TestListener(MembershipListener):
            def on_member_added(self, event: MembershipEvent) -> None:
                events.append(event)
            
            def on_member_removed(self, event: MembershipEvent) -> None:
                events.append(event)
        
        client = HazelcastClient(cluster_client_config)
        client.start()
        
        try:
            client.add_membership_listener(TestListener())
            time.sleep(1)
        finally:
            client.shutdown()

    def test_member_added_event(self, hazelcast_cluster, cluster_client_config):
        """Test member added event is received."""
        from hazelcast.client import HazelcastClient
        from hazelcast.listener import MembershipListener, MembershipEvent
        
        events: List[MembershipEvent] = []
        
        class TestListener(MembershipListener):
            def on_member_added(self, event: MembershipEvent) -> None:
                events.append(event)
            
            def on_member_removed(self, event: MembershipEvent) -> None:
                events.append(event)
        
        client = HazelcastClient(cluster_client_config)
        client.start()
        
        try:
            client.add_membership_listener(TestListener())
            time.sleep(1)
        finally:
            client.shutdown()


@skip_integration
class TestPartitionAwareness:
    """Test partition-aware operations during failover."""

    def test_partition_redistribution(self, hazelcast_cluster, cluster_client_config):
        """Test operations continue after partition redistribution."""
        from hazelcast.client import HazelcastClient
        
        client = HazelcastClient(cluster_client_config)
        client.start()
        
        try:
            test_map = client.get_map("partition-test")
            
            for i in range(100):
                test_map.put(f"key-{i}", f"value-{i}")
            
            for i in range(100):
                assert test_map.get(f"key-{i}") == f"value-{i}"
        finally:
            client.shutdown()

    def test_operations_across_partitions(self, hazelcast_cluster, cluster_client_config):
        """Test operations distributed across partitions."""
        from hazelcast.client import HazelcastClient
        
        client = HazelcastClient(cluster_client_config)
        client.start()
        
        try:
            test_map = client.get_map("multi-partition-test")
            
            for i in range(271):
                test_map.put(f"key-{i}", f"value-{i}")
            
            assert test_map.size() == 271
        finally:
            client.shutdown()


@skip_integration
class TestRetryBehavior:
    """Test operation retry behavior."""

    def test_operation_retry_on_failure(self, hazelcast_cluster, cluster_client_config):
        """Test operations are retried on transient failures."""
        from hazelcast.client import HazelcastClient
        
        cluster_client_config.retry = type('RetryConfig', (), {
            'max_attempts': 5,
            'initial_backoff': 0.5,
        })()
        
        client = HazelcastClient(cluster_client_config)
        client.start()
        
        try:
            test_map = client.get_map("retry-test")
            
            for i in range(50):
                test_map.put(f"key-{i}", f"value-{i}")
            
            for i in range(50):
                assert test_map.get(f"key-{i}") == f"value-{i}"
        finally:
            client.shutdown()

    def test_timeout_handling(self, hazelcast_cluster, cluster_client_config):
        """Test operation timeout handling."""
        from hazelcast.client import HazelcastClient
        
        client = HazelcastClient(cluster_client_config)
        client.start()
        
        try:
            test_map = client.get_map("timeout-test")
            test_map.put("key", "value")
            assert test_map.get("key") == "value"
        finally:
            client.shutdown()


@skip_integration
class TestConcurrentFailover:
    """Test concurrent operations during failover."""

    def test_concurrent_operations_during_failover(
        self, hazelcast_cluster, cluster_client_config
    ):
        """Test concurrent operations continue during failover."""
        from hazelcast.client import HazelcastClient
        
        client = HazelcastClient(cluster_client_config)
        client.start()
        
        try:
            test_map = client.get_map("concurrent-failover-test")
            errors: List[Exception] = []
            results: List[bool] = []
            
            def worker(thread_id: int):
                try:
                    for i in range(50):
                        key = f"thread-{thread_id}-key-{i}"
                        value = f"thread-{thread_id}-value-{i}"
                        test_map.put(key, value)
                        retrieved = test_map.get(key)
                        results.append(retrieved == value)
                except Exception as e:
                    errors.append(e)
            
            threads = [
                threading.Thread(target=worker, args=(i,))
                for i in range(5)
            ]
            
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            
            assert len(errors) == 0, f"Errors occurred: {errors}"
            assert all(results), "Some operations returned unexpected values"
        finally:
            client.shutdown()


@skip_integration
class TestSmartRoutingFailover:
    """Test smart routing behavior during failover."""

    def test_smart_routing_enabled(self, hazelcast_cluster, cluster_client_config):
        """Test smart routing routes to correct partition owners."""
        from hazelcast.client import HazelcastClient
        
        cluster_client_config.smart_routing = True
        
        client = HazelcastClient(cluster_client_config)
        client.start()
        
        try:
            test_map = client.get_map("smart-routing-test")
            
            for i in range(100):
                test_map.put(f"key-{i}", f"value-{i}")
            
            for i in range(100):
                assert test_map.get(f"key-{i}") == f"value-{i}"
        finally:
            client.shutdown()

    def test_smart_routing_disabled(self, hazelcast_cluster, cluster_client_config):
        """Test operations work with smart routing disabled."""
        from hazelcast.client import HazelcastClient
        
        cluster_client_config.smart_routing = False
        
        client = HazelcastClient(cluster_client_config)
        client.start()
        
        try:
            test_map = client.get_map("non-smart-routing-test")
            
            for i in range(100):
                test_map.put(f"key-{i}", f"value-{i}")
            
            for i in range(100):
                assert test_map.get(f"key-{i}") == f"value-{i}"
        finally:
            client.shutdown()
