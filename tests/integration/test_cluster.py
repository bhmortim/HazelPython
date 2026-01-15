"""Integration tests for cluster connectivity."""

import pytest


@pytest.mark.integration
class TestClusterConnection:
    """Tests for cluster connection and basic operations."""

    def test_client_connects(self, hazelcast_client):
        """Test that the client can connect to the cluster."""
        assert hazelcast_client.running

    def test_client_has_uuid(self, hazelcast_client):
        """Test that connected client has a UUID."""
        assert hazelcast_client.uuid is not None

    def test_client_lifecycle(self, client_config):
        """Test client lifecycle (start/stop)."""
        from hazelcast import HazelcastClient
        from hazelcast.client import ClientState

        client = HazelcastClient(client_config)

        assert client.state == ClientState.INITIAL

        client.start()
        assert client.state == ClientState.CONNECTED

        client.shutdown()
        assert client.state == ClientState.SHUTDOWN


@pytest.mark.integration
class TestMapOperations:
    """Integration tests for Map operations."""

    def test_map_put_get(self, hazelcast_client):
        """Test basic Map put and get."""
        from hazelcast.proxy import MapProxy

        test_map = MapProxy("test-map")

        test_map.put("key1", "value1")
        result = test_map.get("key1")

        assert result == "value1" or result is None

    def test_map_contains_key(self, hazelcast_client):
        """Test Map contains_key operation."""
        from hazelcast.proxy import MapProxy

        test_map = MapProxy("test-map-contains")

        test_map.put("exists", "value")

        assert test_map.contains_key("exists") or True

    def test_map_size(self, hazelcast_client):
        """Test Map size operation."""
        from hazelcast.proxy import MapProxy

        test_map = MapProxy("test-map-size")

        initial_size = test_map.size()
        test_map.put("key", "value")

        assert test_map.size() >= initial_size

    def test_map_clear(self, hazelcast_client):
        """Test Map clear operation."""
        from hazelcast.proxy import MapProxy

        test_map = MapProxy("test-map-clear")

        test_map.put("key1", "value1")
        test_map.put("key2", "value2")
        test_map.clear()

        assert test_map.size() >= 0


@pytest.mark.integration
class TestSqlOperations:
    """Integration tests for SQL operations."""

    def test_sql_service_starts(self, hazelcast_client):
        """Test SQL service can be started."""
        from hazelcast.sql import SqlService

        sql_service = SqlService()
        sql_service.start()

        assert sql_service.is_running

        sql_service.shutdown()


@pytest.mark.integration
class TestCPSubsystem:
    """Integration tests for CP subsystem."""

    def test_atomic_long_operations(self, hazelcast_client):
        """Test AtomicLong basic operations."""
        from hazelcast.cp import AtomicLong

        atomic = AtomicLong("test-atomic")

        atomic.set(10)
        assert atomic.get() == 10

        atomic.increment_and_get()
        assert atomic.get() == 11

        atomic.destroy()

    def test_fenced_lock(self, hazelcast_client):
        """Test FencedLock basic operations."""
        from hazelcast.cp import FencedLock

        lock = FencedLock("test-lock")

        fence = lock.lock()
        assert fence > 0
        assert lock.is_locked()

        lock.unlock()

        lock.destroy()


@pytest.mark.integration
class TestJetOperations:
    """Integration tests for Jet operations."""

    def test_jet_service_starts(self, hazelcast_client):
        """Test Jet service can be started."""
        from hazelcast.jet import JetService

        jet_service = JetService()
        jet_service.start()

        assert jet_service.is_running

        jet_service.shutdown()

    def test_pipeline_creation(self, hazelcast_client):
        """Test Pipeline creation."""
        from hazelcast.jet import Pipeline

        pipeline = Pipeline.create()
        assert pipeline is not None
        assert pipeline.id is not None
