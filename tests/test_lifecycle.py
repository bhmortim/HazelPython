"""Unit tests for hazelcast.service modules (partition, executor, client_service)."""

import pytest
import time
from unittest.mock import MagicMock, patch

from hazelcast.service.partition import (
    Partition,
    PartitionTable,
    PartitionService,
)
from hazelcast.service.executor import (
    ExecutorService,
    ExecutorTask,
    ExecutorCallback,
    FunctionExecutorCallback,
)
from hazelcast.service.client_service import (
    ClientService,
    ConnectionStatus,
    ConnectionInfo,
    ClusterInfo,
    ClientConnectionListener,
    FunctionConnectionListener,
)
from hazelcast.metrics import (
    MetricsRegistry,
    GaugeMetric,
    CounterMetric,
    TimerMetric,
    ConnectionMetrics,
    InvocationMetrics,
    NearCacheMetrics,
)
from hazelcast.failover import (
    FailoverConfig,
    ClusterConfig,
    CNAMEResolver,
)


class TestPartition:
    """Tests for Partition."""

    def test_create(self):
        p = Partition(partition_id=5)
        assert p.partition_id == 5
        assert p.owner_uuid is None

    def test_with_owner(self):
        p = Partition(partition_id=10, owner_uuid="member-123")
        assert p.owner_uuid == "member-123"

    def test_is_assigned(self):
        p1 = Partition(partition_id=0)
        p2 = Partition(partition_id=1, owner_uuid="uuid")
        
        assert p1.is_assigned is False
        assert p2.is_assigned is True

    def test_equality(self):
        p1 = Partition(partition_id=5)
        p2 = Partition(partition_id=5)
        p3 = Partition(partition_id=6)
        
        assert p1 == p2
        assert p1 != p3

    def test_hash(self):
        p1 = Partition(partition_id=5)
        p2 = Partition(partition_id=5)
        assert hash(p1) == hash(p2)


class TestPartitionTable:
    """Tests for PartitionTable."""

    def test_create(self):
        table = PartitionTable()
        assert table.partition_count == 0
        assert table.version == 0

    def test_get_partition(self):
        table = PartitionTable()
        p = Partition(partition_id=0, owner_uuid="uuid")
        table.partitions[0] = p
        
        assert table.get_partition(0) is p
        assert table.get_partition(1) is None

    def test_get_partitions_for_owner(self):
        table = PartitionTable()
        table.partitions[0] = Partition(0, owner_uuid="a")
        table.partitions[1] = Partition(1, owner_uuid="b")
        table.partitions[2] = Partition(2, owner_uuid="a")
        
        owned = table.get_partitions_for_owner("a")
        assert len(owned) == 2


class TestPartitionService:
    """Tests for PartitionService."""

    @pytest.fixture
    def service(self):
        return PartitionService(partition_count=271)

    def test_partition_count(self, service):
        assert service.partition_count == 271

    def test_not_initialized_initially(self, service):
        assert service.is_initialized is False

    def test_get_partition_id(self, service):
        pid = service.get_partition_id("key")
        assert 0 <= pid < 271

    def test_same_key_same_partition(self, service):
        pid1 = service.get_partition_id("test-key")
        pid2 = service.get_partition_id("test-key")
        assert pid1 == pid2

    def test_get_partition(self, service):
        p = service.get_partition(0)
        assert p is not None
        assert p.partition_id == 0

    def test_get_partition_invalid(self, service):
        assert service.get_partition(-1) is None
        assert service.get_partition(1000) is None

    def test_update_partition_table(self, service):
        updates = {0: "uuid-a", 1: "uuid-b"}
        result = service.update_partition_table(updates, version=1)
        
        assert result is True
        assert service.is_initialized is True
        assert service.get_partition_owner(0) == "uuid-a"

    def test_update_old_version_rejected(self, service):
        service.update_partition_table({0: "a"}, version=2)
        result = service.update_partition_table({0: "b"}, version=1)
        
        assert result is False
        assert service.get_partition_owner(0) == "a"

    def test_get_partitions(self, service):
        partitions = service.get_partitions()
        assert len(partitions) == 271

    def test_get_partitions_for_member(self, service):
        service.update_partition_table({0: "m1", 1: "m1", 2: "m2"}, version=1)
        
        owned = service.get_partitions_for_member("m1")
        assert len(owned) == 2

    def test_get_partition_count_for_member(self, service):
        service.update_partition_table({0: "m1", 1: "m1"}, version=1)
        assert service.get_partition_count_for_member("m1") == 2

    def test_reset(self, service):
        service.update_partition_table({0: "uuid"}, version=1)
        service.reset()
        
        assert service.is_initialized is False
        assert service.get_partition_owner(0) is None

    def test_repr(self, service):
        repr_str = repr(service)
        assert "PartitionService" in repr_str
        assert "271" in repr_str


class TestExecutorTask:
    """Tests for ExecutorTask."""

    def test_create_callable(self):
        task = ExecutorTask(callable=lambda: 42)
        assert task.is_callable is True
        assert task.is_runnable is False

    def test_create_runnable(self):
        task = ExecutorTask(runnable=lambda: None)
        assert task.is_callable is False
        assert task.is_runnable is True

    def test_auto_id(self):
        task1 = ExecutorTask()
        task2 = ExecutorTask()
        assert task1.task_id != task2.task_id

    def test_target_member(self):
        task = ExecutorTask(target_member="member-uuid")
        assert task.target_member == "member-uuid"

    def test_target_partition_key(self):
        task = ExecutorTask(target_partition_key="key123")
        assert task.target_partition_key == "key123"


class TestFunctionExecutorCallback:
    """Tests for FunctionExecutorCallback."""

    def test_on_response(self):
        results = []
        callback = FunctionExecutorCallback(
            on_success=lambda r: results.append(r)
        )
        
        callback.on_response(42)
        assert results == [42]

    def test_on_failure(self):
        errors = []
        callback = FunctionExecutorCallback(
            on_error=lambda e: errors.append(e)
        )
        
        error = Exception("test")
        callback.on_failure(error)
        assert errors[0] is error

    def test_no_callbacks(self):
        callback = FunctionExecutorCallback()
        callback.on_response(42)
        callback.on_failure(Exception())


class TestExecutorService:
    """Tests for ExecutorService."""

    @pytest.fixture
    def executor(self):
        return ExecutorService("test-executor")

    def test_name(self, executor):
        assert executor.name == "test-executor"

    def test_not_shutdown_initially(self, executor):
        assert executor.is_shutdown is False

    def test_submit(self, executor):
        future = executor.submit(lambda: 42)
        result = future.result(timeout=1)
        assert result == 42

    def test_submit_with_callback(self, executor):
        results = []
        callback = FunctionExecutorCallback(
            on_success=lambda r: results.append(r)
        )
        
        future = executor.submit(lambda: "done", callback)
        future.result(timeout=1)
        
        time.sleep(0.1)
        assert "done" in results

    def test_submit_to_member(self, executor):
        future = executor.submit_to_member(lambda: 100, "member-uuid")
        result = future.result(timeout=1)
        assert result == 100

    def test_submit_to_key_owner(self, executor):
        future = executor.submit_to_key_owner(lambda: "result", "my-key")
        result = future.result(timeout=1)
        assert result == "result"

    def test_submit_to_members(self, executor):
        futures = executor.submit_to_members(lambda: 1, {"m1", "m2"})
        assert len(futures) == 2

    def test_execute(self, executor):
        executed = []
        executor.execute(lambda: executed.append(1))
        time.sleep(0.1)
        assert 1 in executed

    def test_execute_on_key_owner(self, executor):
        executed = []
        executor.execute_on_key_owner(lambda: executed.append(1), "key")
        time.sleep(0.1)
        assert 1 in executed

    def test_shutdown(self, executor):
        executor.shutdown()
        assert executor.is_shutdown is True

    def test_is_terminated(self, executor):
        executor.shutdown()
        time.sleep(0.1)
        assert executor.is_terminated() is True

    def test_pending_task_count(self, executor):
        assert executor.get_pending_task_count() == 0

    def test_submit_after_shutdown_raises(self, executor):
        executor.shutdown()
        
        from hazelcast.exceptions import IllegalStateException
        with pytest.raises(IllegalStateException):
            executor.submit(lambda: 1)

    def test_repr(self, executor):
        repr_str = repr(executor)
        assert "ExecutorService" in repr_str
        assert "test-executor" in repr_str


class TestConnectionInfo:
    """Tests for ConnectionInfo."""

    def test_create(self):
        info = ConnectionInfo(
            connection_id=1,
            remote_address=("localhost", 5701),
        )
        assert info.connection_id == 1
        assert info.remote_address == ("localhost", 5701)
        assert info.is_alive is True

    def test_defaults(self):
        info = ConnectionInfo(connection_id=1)
        assert info.local_address is None
        assert info.member_uuid is None
        assert info.last_read_time == 0.0
        assert info.last_write_time == 0.0


class TestClusterInfo:
    """Tests for ClusterInfo."""

    def test_defaults(self):
        info = ClusterInfo()
        assert info.cluster_uuid is None
        assert info.cluster_name == ""
        assert info.member_count == 0
        assert info.partition_count == 0
        assert info.version == ""


class TestFunctionConnectionListener:
    """Tests for FunctionConnectionListener."""

    def test_on_opened(self):
        connections = []
        listener = FunctionConnectionListener(
            on_opened=lambda c: connections.append(c)
        )
        
        conn = MagicMock()
        listener.on_connection_opened(conn)
        
        assert connections[0] is conn

    def test_on_closed(self):
        events = []
        listener = FunctionConnectionListener(
            on_closed=lambda c, r: events.append((c, r))
        )
        
        conn = MagicMock()
        listener.on_connection_closed(conn, "timeout")
        
        assert events[0] == (conn, "timeout")

    def test_no_callbacks(self):
        listener = FunctionConnectionListener()
        listener.on_connection_opened(MagicMock())
        listener.on_connection_closed(MagicMock(), "reason")


class TestClientService:
    """Tests for ClientService."""

    @pytest.fixture
    def service(self):
        return ClientService()

    def test_initial_state(self, service):
        assert service.status == ConnectionStatus.DISCONNECTED
        assert service.is_connected is False
        assert service.connection_count == 0

    def test_set_status(self, service):
        service.set_status(ConnectionStatus.CONNECTING)
        assert service.status == ConnectionStatus.CONNECTING

    def test_add_connection(self, service):
        conn = MagicMock()
        conn.connection_id = 1
        conn.remote_address = ("localhost", 5701)
        conn.local_address = None
        conn.member_uuid = "uuid"
        conn.last_read_time = 0
        conn.last_write_time = 0
        conn.is_alive = True
        
        service.add_connection(conn)
        
        assert service.connection_count == 1
        assert service.status == ConnectionStatus.CONNECTED

    def test_remove_connection(self, service):
        conn = MagicMock()
        conn.connection_id = 1
        conn.remote_address = ("localhost", 5701)
        conn.local_address = None
        conn.member_uuid = "uuid"
        conn.last_read_time = 0
        conn.last_write_time = 0
        conn.is_alive = True
        
        service.add_connection(conn)
        service.remove_connection(conn, "closed")
        
        assert service.status == ConnectionStatus.DISCONNECTED

    def test_get_connections(self, service):
        conn = MagicMock()
        conn.connection_id = 1
        conn.remote_address = ("localhost", 5701)
        conn.local_address = None
        conn.member_uuid = "uuid"
        conn.last_read_time = 0
        conn.last_write_time = 0
        conn.is_alive = True
        
        service.add_connection(conn)
        
        connections = service.get_connections()
        assert len(connections) == 1

    def test_update_cluster_info(self, service):
        service.update_cluster_info(
            cluster_uuid="c-uuid",
            cluster_name="test-cluster",
            member_count=3,
        )
        
        info = service.cluster_info
        assert info.cluster_uuid == "c-uuid"
        assert info.cluster_name == "test-cluster"
        assert info.member_count == 3

    def test_add_connection_listener(self, service):
        listener = MagicMock()
        reg_id = service.add_connection_listener(listener)
        
        assert reg_id is not None

    def test_remove_connection_listener(self, service):
        listener = MagicMock()
        reg_id = service.add_connection_listener(listener)
        
        removed = service.remove_connection_listener(reg_id)
        assert removed is True
        
        removed_again = service.remove_connection_listener(reg_id)
        assert removed_again is False

    def test_get_statistics(self, service):
        stats = service.get_statistics()
        assert "status" in stats
        assert "active_connections" in stats

    def test_reset(self, service):
        service.set_status(ConnectionStatus.CONNECTED)
        service.reset()
        
        assert service.status == ConnectionStatus.DISCONNECTED

    def test_repr(self, service):
        repr_str = repr(service)
        assert "ClientService" in repr_str


class TestMetricsRegistry:
    """Tests for MetricsRegistry."""

    @pytest.fixture
    def registry(self):
        return MetricsRegistry()

    def test_uptime(self, registry):
        time.sleep(0.1)
        assert registry.uptime_seconds >= 0.1

    def test_register_gauge(self, registry):
        gauge = registry.register_gauge("test.gauge", lambda: 42)
        assert gauge.get_value() == 42

    def test_register_counter(self, registry):
        counter = registry.register_counter("test.counter")
        counter.increment()
        counter.increment(5)
        assert counter.get_value() == 6

    def test_register_timer(self, registry):
        timer = registry.register_timer("test.timer")
        timer.record(10.0)
        timer.record(20.0)
        assert timer.get_average() == 15.0

    def test_connection_metrics(self, registry):
        registry.record_connection_opened()
        registry.record_connection_opened()
        registry.record_connection_closed()
        
        metrics = registry.connection_metrics
        assert metrics.active_connections == 1
        assert metrics.total_connections_opened == 2
        assert metrics.total_connections_closed == 1

    def test_connection_attempt(self, registry):
        registry.record_connection_attempt(success=True)
        registry.record_connection_attempt(success=False)
        
        metrics = registry.connection_metrics
        assert metrics.connection_attempts == 2
        assert metrics.failed_connection_attempts == 1

    def test_invocation_metrics(self, registry):
        registry.record_invocation_start()
        registry.record_invocation_start()
        registry.record_invocation_end(10.0, success=True)
        registry.record_invocation_end(20.0, success=False, timed_out=True)
        
        metrics = registry.invocation_metrics
        assert metrics.total_invocations == 2
        assert metrics.completed_invocations == 2
        assert metrics.failed_invocations == 1
        assert metrics.timed_out_invocations == 1

    def test_invocation_retry(self, registry):
        registry.record_invocation_retry()
        assert registry.invocation_metrics.retried_invocations == 1

    def test_near_cache_metrics(self, registry):
        registry.record_near_cache_hit("map1")
        registry.record_near_cache_hit("map1")
        registry.record_near_cache_miss("map1")
        registry.record_near_cache_eviction("map1")
        registry.record_near_cache_invalidation("map1")
        
        metrics = registry.get_near_cache_metrics("map1")
        assert metrics.hits == 2
        assert metrics.misses == 1
        assert metrics.evictions == 1
        assert metrics.invalidations == 1
        assert metrics.hit_ratio == pytest.approx(2/3)

    def test_to_dict(self, registry):
        registry.record_connection_opened()
        registry.record_invocation_start()
        
        d = registry.to_dict()
        assert "uptime_seconds" in d
        assert "connections" in d
        assert "invocations" in d
        assert "near_caches" in d

    def test_reset(self, registry):
        registry.record_connection_opened()
        registry.register_gauge("g", lambda: 1)
        registry.reset()
        
        assert registry.connection_metrics.active_connections == 0

    def test_repr(self, registry):
        repr_str = repr(registry)
        assert "MetricsRegistry" in repr_str


class TestGaugeMetric:
    """Tests for GaugeMetric."""

    def test_get_value(self):
        gauge = GaugeMetric(name="test", value_fn=lambda: 42)
        assert gauge.get_value() == 42

    def test_exception_returns_zero(self):
        gauge = GaugeMetric(name="test", value_fn=lambda: 1/0)
        assert gauge.get_value() == 0.0


class TestCounterMetric:
    """Tests for CounterMetric."""

    def test_increment(self):
        counter = CounterMetric(name="test")
        counter.increment()
        assert counter.get_value() == 1

    def test_increment_amount(self):
        counter = CounterMetric(name="test")
        counter.increment(10)
        assert counter.get_value() == 10


class TestTimerMetric:
    """Tests for TimerMetric."""

    def test_record(self):
        timer = TimerMetric(name="test")
        timer.record(10)
        timer.record(20)
        timer.record(30)
        
        assert timer.count == 3
        assert timer.total_time == 60
        assert timer.min_time == 10
        assert timer.max_time == 30
        assert timer.get_average() == 20

    def test_average_empty(self):
        timer = TimerMetric(name="test")
        assert timer.get_average() == 0.0


class TestNearCacheMetrics:
    """Tests for NearCacheMetrics."""

    def test_hit_ratio(self):
        metrics = NearCacheMetrics(name="test", hits=80, misses=20)
        assert metrics.hit_ratio == 0.8
        assert metrics.miss_ratio == 0.2

    def test_hit_ratio_empty(self):
        metrics = NearCacheMetrics(name="test")
        assert metrics.hit_ratio == 0.0
        assert metrics.miss_ratio == 0.0


class TestClusterConfig:
    """Tests for ClusterConfig."""

    def test_create(self):
        config = ClusterConfig(
            cluster_name="prod",
            addresses=["node1:5701"],
            priority=0,
        )
        assert config.cluster_name == "prod"
        assert config.addresses == ["node1:5701"]
        assert config.priority == 0

    def test_defaults(self):
        config = ClusterConfig(cluster_name="test")
        assert config.addresses == ["localhost:5701"]
        assert config.priority == 0


class TestFailoverConfig:
    """Tests for FailoverConfig."""

    @pytest.fixture
    def failover(self):
        return FailoverConfig()

    def test_defaults(self, failover):
        assert failover.try_count == 3
        assert failover.cluster_count == 0

    def test_add_cluster(self, failover):
        failover.add_cluster("primary", ["node1:5701"])
        failover.add_cluster("backup", ["node2:5701"], priority=1)
        
        assert failover.cluster_count == 2

    def test_clusters_sorted_by_priority(self, failover):
        failover.add_cluster("backup", ["b:5701"], priority=1)
        failover.add_cluster("primary", ["p:5701"], priority=0)
        
        clusters = failover.clusters
        assert clusters[0].cluster_name == "primary"
        assert clusters[1].cluster_name == "backup"

    def test_get_current_cluster(self, failover):
        failover.add_cluster("test", ["node:5701"])
        
        current = failover.get_current_cluster()
        assert current.cluster_name == "test"

    def test_get_current_cluster_empty(self, failover):
        assert failover.get_current_cluster() is None

    def test_switch_to_next_cluster(self, failover):
        failover.add_cluster("a", ["a:5701"], priority=0)
        failover.add_cluster("b", ["b:5701"], priority=1)
        
        assert failover.get_current_cluster().cluster_name == "a"
        
        failover.switch_to_next_cluster()
        assert failover.get_current_cluster().cluster_name == "b"
        
        failover.switch_to_next_cluster()
        assert failover.get_current_cluster().cluster_name == "a"

    def test_reset(self, failover):
        failover.add_cluster("a", ["a:5701"])
        failover.add_cluster("b", ["b:5701"])
        failover.switch_to_next_cluster()
        
        failover.reset()
        
        assert failover.current_cluster_index == 0

    def test_try_count_setter(self, failover):
        failover.try_count = 5
        assert failover.try_count == 5

    def test_try_count_invalid(self, failover):
        with pytest.raises(ValueError):
            failover.try_count = 0

    def test_get_cluster_addresses(self, failover):
        failover.add_cluster("test", ["n1:5701", "n2:5701"])
        
        addresses = failover.get_cluster_addresses("test")
        assert len(addresses) == 2

    def test_repr(self, failover):
        failover.add_cluster("test", ["node:5701"])
        repr_str = repr(failover)
        assert "FailoverConfig" in repr_str
        assert "test" in repr_str
