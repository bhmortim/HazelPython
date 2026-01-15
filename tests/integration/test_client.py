"""Integration tests for HazelcastClient."""

import pytest
import threading
from unittest.mock import Mock, patch, MagicMock

from hazelcast.client import (
    HazelcastClient,
    ClientState,
    SERVICE_NAME_MAP,
    SERVICE_NAME_QUEUE,
    SERVICE_NAME_SET,
    SERVICE_NAME_LIST,
)
from hazelcast.config import ClientConfig
from hazelcast.exceptions import ClientOfflineException, IllegalStateException
from hazelcast.listener import (
    LifecycleState,
    LifecycleEvent,
    LifecycleListener,
    DistributedObjectEvent,
    DistributedObjectEventType,
    DistributedObjectListener,
)
from hazelcast.proxy.base import Proxy


class MockProxy(Proxy):
    """Mock proxy for testing."""

    def __init__(self, service_name: str, name: str, context=None):
        super().__init__(service_name, name, context)


@pytest.fixture
def mock_services():
    """Fixture to mock all service imports."""
    with patch("hazelcast.client.SerializationService") as mock_ser, \
         patch("hazelcast.client.PartitionService") as mock_part, \
         patch("hazelcast.client.ConnectionManager") as mock_conn, \
         patch("hazelcast.client.InvocationService") as mock_inv, \
         patch("hazelcast.client.MetricsRegistry") as mock_metrics:

        mock_ser.return_value = MagicMock()
        mock_part.return_value = MagicMock()
        mock_conn.return_value = MagicMock()
        mock_inv.return_value = MagicMock()
        mock_metrics.return_value = MagicMock()

        yield {
            "serialization": mock_ser,
            "partition": mock_part,
            "connection": mock_conn,
            "invocation": mock_inv,
            "metrics": mock_metrics,
        }


class TestClientLifecycle:
    """Test client lifecycle and state management."""

    def test_client_initial_state(self):
        """Test client starts in INITIAL state."""
        client = HazelcastClient()
        assert client.state == ClientState.INITIAL
        assert not client.running

    def test_client_start_transitions_to_connected(self, mock_services):
        """Test starting client transitions to CONNECTED state."""
        client = HazelcastClient()
        client.start()

        assert client.state == ClientState.CONNECTED
        assert client.running

        client.shutdown()

    def test_client_shutdown_transitions_to_shutdown(self, mock_services):
        """Test shutdown transitions through SHUTTING_DOWN to SHUTDOWN."""
        client = HazelcastClient()
        client.start()
        client.shutdown()

        assert client.state == ClientState.SHUTDOWN
        assert not client.running

    def test_client_context_manager(self, mock_services):
        """Test client as context manager."""
        with HazelcastClient() as client:
            assert client.state == ClientState.CONNECTED

        assert client.state == ClientState.SHUTDOWN

    def test_double_shutdown_is_safe(self, mock_services):
        """Test calling shutdown multiple times is safe."""
        client = HazelcastClient()
        client.start()
        client.shutdown()
        client.shutdown()  # Should not raise

        assert client.state == ClientState.SHUTDOWN

    def test_lifecycle_events_fired(self, mock_services):
        """Test lifecycle events are fired on state transitions."""
        events = []

        class TestListener(LifecycleListener):
            def on_state_changed(self, event: LifecycleEvent) -> None:
                events.append(event)

        client = HazelcastClient()
        client.add_lifecycle_listener(TestListener())

        client.start()
        client.shutdown()

        states = [e.state for e in events]
        assert LifecycleState.STARTING in states
        assert LifecycleState.CONNECTED in states
        assert LifecycleState.SHUTTING_DOWN in states
        assert LifecycleState.SHUTDOWN in states


class TestClientConfiguration:
    """Test client configuration."""

    def test_default_config(self):
        """Test client with default configuration."""
        client = HazelcastClient()
        assert client.config is not None
        assert client.name.startswith("hz.client_")

    def test_custom_client_name(self):
        """Test client with custom name."""
        config = ClientConfig()
        config.client_name = "my-client"

        client = HazelcastClient(config)
        assert client.name == "my-client"

    def test_client_uuid_is_generated(self):
        """Test client UUID is generated."""
        client = HazelcastClient()
        assert client.uuid is not None
        assert len(client.uuid) > 0

    def test_client_labels(self):
        """Test client labels from config."""
        config = ClientConfig()
        config.labels = ["label1", "label2"]

        client = HazelcastClient(config)
        assert client.labels == ["label1", "label2"]


class TestDistributedObjects:
    """Test distributed object creation and lifecycle."""

    def test_get_map_requires_running_client(self):
        """Test getting map requires client to be running."""
        client = HazelcastClient()

        with pytest.raises(ClientOfflineException):
            client.get_map("test-map")

    def test_get_map_returns_proxy(self, mock_services):
        """Test get_map returns a Map proxy."""
        with patch("hazelcast.client.Map", MockProxy):
            client = HazelcastClient()
            client.start()

            map_proxy = client.get_map("test-map")
            assert map_proxy is not None
            assert map_proxy.name == "test-map"
            assert map_proxy.service_name == SERVICE_NAME_MAP

            client.shutdown()

    def test_get_same_map_returns_same_instance(self, mock_services):
        """Test getting same map name returns same instance."""
        with patch("hazelcast.client.Map", MockProxy):
            client = HazelcastClient()
            client.start()

            map1 = client.get_map("test-map")
            map2 = client.get_map("test-map")

            assert map1 is map2

            client.shutdown()

    def test_get_different_maps_returns_different_instances(self, mock_services):
        """Test getting different map names returns different instances."""
        with patch("hazelcast.client.Map", MockProxy):
            client = HazelcastClient()
            client.start()

            map1 = client.get_map("map1")
            map2 = client.get_map("map2")

            assert map1 is not map2
            assert map1.name == "map1"
            assert map2.name == "map2"

            client.shutdown()

    def test_get_queue_returns_proxy(self, mock_services):
        """Test get_queue returns a Queue proxy."""
        with patch("hazelcast.client.Queue", MockProxy):
            client = HazelcastClient()
            client.start()

            queue = client.get_queue("test-queue")
            assert queue is not None
            assert queue.name == "test-queue"
            assert queue.service_name == SERVICE_NAME_QUEUE

            client.shutdown()

    def test_get_set_returns_proxy(self, mock_services):
        """Test get_set returns a Set proxy."""
        with patch("hazelcast.client.Set", MockProxy):
            client = HazelcastClient()
            client.start()

            set_proxy = client.get_set("test-set")
            assert set_proxy is not None
            assert set_proxy.name == "test-set"
            assert set_proxy.service_name == SERVICE_NAME_SET

            client.shutdown()

    def test_get_list_returns_proxy(self, mock_services):
        """Test get_list returns a List proxy."""
        with patch("hazelcast.client.HzList", MockProxy):
            client = HazelcastClient()
            client.start()

            list_proxy = client.get_list("test-list")
            assert list_proxy is not None
            assert list_proxy.name == "test-list"
            assert list_proxy.service_name == SERVICE_NAME_LIST

            client.shutdown()

    def test_get_distributed_objects_empty_initially(self, mock_services):
        """Test get_distributed_objects returns empty list initially."""
        client = HazelcastClient()
        client.start()

        objects = client.get_distributed_objects()
        assert objects == []

        client.shutdown()

    def test_get_distributed_objects_returns_created_objects(self, mock_services):
        """Test get_distributed_objects returns all created objects."""
        with patch("hazelcast.client.Map", MockProxy), \
             patch("hazelcast.client.Queue", MockProxy):
            client = HazelcastClient()
            client.start()

            map_proxy = client.get_map("test-map")
            queue_proxy = client.get_queue("test-queue")

            objects = client.get_distributed_objects()
            assert len(objects) == 2
            assert map_proxy in objects
            assert queue_proxy in objects

            client.shutdown()

    def test_distributed_object_created_event(self, mock_services):
        """Test DistributedObjectEvent is fired on creation."""
        events = []

        class TestListener(DistributedObjectListener):
            def on_created(self, event: DistributedObjectEvent) -> None:
                events.append(event)

            def on_destroyed(self, event: DistributedObjectEvent) -> None:
                pass

        with patch("hazelcast.client.Map", MockProxy):
            client = HazelcastClient()
            client.add_distributed_object_listener(TestListener())
            client.start()

            client.get_map("test-map")

            assert len(events) == 1
            assert events[0].event_type == DistributedObjectEventType.CREATED
            assert events[0].name == "test-map"
            assert events[0].service_name == SERVICE_NAME_MAP

            client.shutdown()


class TestServicesIntegration:
    """Test service wiring and integration."""

    def test_services_initialized_on_start(self, mock_services):
        """Test all services are initialized when client starts."""
        client = HazelcastClient()
        client.start()

        mock_services["serialization"].assert_called_once()
        mock_services["partition"].assert_called_once()
        mock_services["connection"].assert_called_once()
        mock_services["invocation"].assert_called_once()
        mock_services["metrics"].assert_called_once()

        client.shutdown()

    def test_invocation_service_shutdown_called(self, mock_services):
        """Test invocation service shutdown is called."""
        client = HazelcastClient()
        client.start()

        inv_service = client._invocation_service
        client.shutdown()

        inv_service.shutdown.assert_called_once()

    def test_connection_manager_shutdown_called(self, mock_services):
        """Test connection manager shutdown is called."""
        client = HazelcastClient()
        client.start()

        conn_manager = client._connection_manager
        client.shutdown()

        conn_manager.shutdown.assert_called_once()

    def test_proxy_context_has_services(self, mock_services):
        """Test proxy context contains all services."""
        client = HazelcastClient()
        client.start()

        ctx = client._proxy_context
        assert ctx is not None
        assert ctx.invocation_service is not None
        assert ctx.serialization_service is not None
        assert ctx.partition_service is not None
        assert ctx.listener_service is not None

        client.shutdown()


class TestSqlAndJetServices:
    """Test SQL and Jet service access."""

    def test_get_sql_requires_running_client(self):
        """Test get_sql requires client to be running."""
        client = HazelcastClient()

        with pytest.raises(ClientOfflineException):
            client.get_sql()

    def test_get_jet_requires_running_client(self):
        """Test get_jet requires client to be running."""
        client = HazelcastClient()

        with pytest.raises(ClientOfflineException):
            client.get_jet()

    def test_get_sql_returns_service(self, mock_services):
        """Test get_sql returns SqlService."""
        mock_sql_service = MagicMock()

        with patch("hazelcast.client.SqlService", return_value=mock_sql_service):
            client = HazelcastClient()
            client.start()

            sql = client.get_sql()
            assert sql is mock_sql_service

            client.shutdown()

    def test_get_sql_returns_same_instance(self, mock_services):
        """Test get_sql returns same instance on multiple calls."""
        mock_sql_service = MagicMock()

        with patch("hazelcast.client.SqlService", return_value=mock_sql_service):
            client = HazelcastClient()
            client.start()

            sql1 = client.get_sql()
            sql2 = client.get_sql()
            assert sql1 is sql2

            client.shutdown()

    def test_get_jet_returns_service(self, mock_services):
        """Test get_jet returns JetService."""
        mock_jet_service = MagicMock()

        with patch("hazelcast.client.JetService", return_value=mock_jet_service):
            client = HazelcastClient()
            client.start()

            jet = client.get_jet()
            assert jet is mock_jet_service

            client.shutdown()


class TestCPProxies:
    """Test CP subsystem proxy creation."""

    def test_get_atomic_long(self, mock_services):
        """Test get_atomic_long returns AtomicLong proxy."""
        with patch("hazelcast.client.AtomicLong", MockProxy):
            client = HazelcastClient()
            client.start()

            atomic = client.get_atomic_long("test-atomic")
            assert atomic is not None
            assert atomic.name == "test-atomic"

            client.shutdown()

    def test_get_atomic_reference(self, mock_services):
        """Test get_atomic_reference returns AtomicReference proxy."""
        with patch("hazelcast.client.AtomicReference", MockProxy):
            client = HazelcastClient()
            client.start()

            ref = client.get_atomic_reference("test-ref")
            assert ref is not None
            assert ref.name == "test-ref"

            client.shutdown()

    def test_get_count_down_latch(self, mock_services):
        """Test get_count_down_latch returns CountDownLatch proxy."""
        with patch("hazelcast.client.CountDownLatch", MockProxy):
            client = HazelcastClient()
            client.start()

            latch = client.get_count_down_latch("test-latch")
            assert latch is not None
            assert latch.name == "test-latch"

            client.shutdown()

    def test_get_semaphore(self, mock_services):
        """Test get_semaphore returns Semaphore proxy."""
        with patch("hazelcast.client.Semaphore", MockProxy):
            client = HazelcastClient()
            client.start()

            sem = client.get_semaphore("test-sem")
            assert sem is not None
            assert sem.name == "test-sem"

            client.shutdown()

    def test_get_fenced_lock(self, mock_services):
        """Test get_fenced_lock returns FencedLock proxy."""
        with patch("hazelcast.client.FencedLock", MockProxy):
            client = HazelcastClient()
            client.start()

            lock = client.get_fenced_lock("test-lock")
            assert lock is not None
            assert lock.name == "test-lock"

            client.shutdown()


class TestThreadSafety:
    """Test thread safety of client operations."""

    def test_concurrent_proxy_creation(self, mock_services):
        """Test concurrent proxy creation is thread-safe."""
        with patch("hazelcast.client.Map", MockProxy):
            client = HazelcastClient()
            client.start()

            results = []
            errors = []

            def create_map():
                try:
                    proxy = client.get_map("shared-map")
                    results.append(proxy)
                except Exception as e:
                    errors.append(e)

            threads = [threading.Thread(target=create_map) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert len(errors) == 0
            assert len(results) == 10
            assert all(r is results[0] for r in results)

            client.shutdown()
