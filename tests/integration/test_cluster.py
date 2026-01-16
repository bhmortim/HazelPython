"""Integration tests for multi-member cluster scenarios."""

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
class TestMultiMemberCluster:
    """Test client behavior with multi-member clusters."""

    def test_connect_to_cluster(self, hazelcast_cluster, cluster_client_config):
        """Test connecting to a multi-member cluster."""
        from hazelcast.client import HazelcastClient, ClientState

        client = HazelcastClient(cluster_client_config)
        client.start()

        assert client.state == ClientState.CONNECTED
        assert client.running

        client.shutdown()

    def test_operations_across_cluster(self, hazelcast_cluster, cluster_client_config):
        """Test operations are distributed across cluster members."""
        from hazelcast.client import HazelcastClient

        with HazelcastClient(cluster_client_config) as client:
            test_map = client.get_map("cluster-test-map")

            for i in range(100):
                test_map.put(f"key-{i}", f"value-{i}")

            assert test_map.size() == 100

            for i in range(100):
                value = test_map.get(f"key-{i}")
                assert value == f"value-{i}"

    def test_multiple_clients_same_cluster(self, hazelcast_cluster, cluster_client_config):
        """Test multiple clients connecting to the same cluster."""
        from hazelcast.client import HazelcastClient

        clients: List[HazelcastClient] = []
        try:
            for i in range(3):
                client = HazelcastClient(cluster_client_config)
                client.start()
                clients.append(client)

            assert all(c.running for c in clients)

            test_map = clients[0].get_map("shared-map")
            test_map.put("shared-key", "shared-value")

            for client in clients[1:]:
                client_map = client.get_map("shared-map")
                value = client_map.get("shared-key")
                assert value == "shared-value"

        finally:
            for client in clients:
                client.shutdown()

    def test_concurrent_operations(self, hazelcast_cluster, cluster_client_config):
        """Test concurrent operations from multiple threads."""
        from hazelcast.client import HazelcastClient

        with HazelcastClient(cluster_client_config) as client:
            test_map = client.get_map("concurrent-map")
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


@skip_integration
class TestClusterMembership:
    """Test cluster membership awareness."""

    def test_membership_listener(self, hazelcast_cluster, cluster_client_config):
        """Test membership listener receives events."""
        from hazelcast.client import HazelcastClient
        from hazelcast.listener import MembershipListener, MembershipEvent

        events: List[MembershipEvent] = []

        class TestMembershipListener(MembershipListener):
            def on_member_added(self, event: MembershipEvent) -> None:
                events.append(event)

            def on_member_removed(self, event: MembershipEvent) -> None:
                events.append(event)

        with HazelcastClient(cluster_client_config) as client:
            client.add_membership_listener(TestMembershipListener())
            time.sleep(1)


@skip_integration
class TestClusterPartitioning:
    """Test partition-aware operations."""

    def test_partition_aware_operations(self, hazelcast_cluster, cluster_client_config):
        """Test operations are partition-aware."""
        from hazelcast.client import HazelcastClient

        with HazelcastClient(cluster_client_config) as client:
            test_map = client.get_map("partition-test-map")

            for i in range(100):
                test_map.put(f"partition-key-{i}", {"data": i})

            for i in range(100):
                value = test_map.get(f"partition-key-{i}")
                assert value is not None
                assert value.get("data") == i


@skip_integration
class TestDistributedObjects:
    """Test distributed object management across cluster."""

    def test_distributed_objects_visible_to_all_clients(
        self, hazelcast_cluster, cluster_client_config
    ):
        """Test distributed objects created by one client are visible to others."""
        from hazelcast.client import HazelcastClient

        with HazelcastClient(cluster_client_config) as client1:
            map1 = client1.get_map("visible-map")
            map1.put("test", "value")

            with HazelcastClient(cluster_client_config) as client2:
                map2 = client2.get_map("visible-map")
                assert map2.get("test") == "value"

    def test_different_data_structures(self, hazelcast_cluster, cluster_client_config):
        """Test different data structures work correctly in cluster."""
        from hazelcast.client import HazelcastClient

        with HazelcastClient(cluster_client_config) as client:
            test_map = client.get_map("test-map")
            test_queue = client.get_queue("test-queue")
            test_set = client.get_set("test-set")
            test_list = client.get_list("test-list")

            test_map.put("key", "map-value")
            test_queue.offer("queue-item")
            test_set.add("set-item")
            test_list.add("list-item")

            assert test_map.get("key") == "map-value"
            assert test_queue.poll() == "queue-item"
            assert test_set.contains("set-item")
            assert test_list.get(0) == "list-item"
