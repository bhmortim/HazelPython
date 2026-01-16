"""Unit tests for connection_manager.py"""

import asyncio
import pytest
from unittest.mock import MagicMock, patch
from typing import List

from hazelcast.network.connection_manager import (
    ConnectionManager,
    LoadBalancer,
    RoundRobinLoadBalancer,
    RandomLoadBalancer,
    RoutingMode,
)
from hazelcast.network.connection import ConnectionState
from hazelcast.network.address import Address
from hazelcast.exceptions import ClientOfflineException


class MockConnection:
    """Mock connection for testing."""

    def __init__(self, connection_id: int, address: Address, alive: bool = True):
        self._connection_id = connection_id
        self._address = address
        self._alive = alive
        self._state = ConnectionState.CONNECTED if alive else ConnectionState.CLOSED

    @property
    def connection_id(self) -> int:
        return self._connection_id

    @property
    def address(self) -> Address:
        return self._address

    @property
    def is_alive(self) -> bool:
        return self._alive

    @is_alive.setter
    def is_alive(self, value: bool) -> None:
        self._alive = value
        self._state = ConnectionState.CONNECTED if value else ConnectionState.CLOSED

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def last_read_time(self) -> float:
        return 0.0

    @property
    def last_write_time(self) -> float:
        return 0.0


def create_mock_connection(
    connection_id: int,
    host: str = "localhost",
    port: int = 5701,
    alive: bool = True,
) -> MockConnection:
    """Create a mock connection for testing."""
    address = Address(host, port)
    return MockConnection(connection_id, address, alive)


class TestRoundRobinLoadBalancer:
    """Tests for RoundRobinLoadBalancer."""

    def test_init_with_empty_connections(self):
        """Test initialization with no connections."""
        lb = RoundRobinLoadBalancer()
        lb.init([])

        assert lb.next() is None
        assert not lb.can_get_next()

    def test_init_filters_dead_connections(self):
        """Test that init only keeps alive connections."""
        lb = RoundRobinLoadBalancer()

        alive = create_mock_connection(1, alive=True)
        dead = create_mock_connection(2, alive=False)

        lb.init([alive, dead])

        assert lb.can_get_next()
        assert lb.next() == alive

    def test_round_robin_distribution(self):
        """Test that connections are returned in round-robin order."""
        lb = RoundRobinLoadBalancer()

        conn1 = create_mock_connection(1, "host1")
        conn2 = create_mock_connection(2, "host2")
        conn3 = create_mock_connection(3, "host3")

        lb.init([conn1, conn2, conn3])

        assert lb.next() == conn1
        assert lb.next() == conn2
        assert lb.next() == conn3

        assert lb.next() == conn1
        assert lb.next() == conn2
        assert lb.next() == conn3

    def test_round_robin_skips_dead_connections(self):
        """Test that round-robin skips dead connections."""
        lb = RoundRobinLoadBalancer()

        conn1 = create_mock_connection(1, "host1")
        conn2 = create_mock_connection(2, "host2")
        conn3 = create_mock_connection(3, "host3")

        lb.init([conn1, conn2, conn3])

        conn2.is_alive = False

        results = [lb.next() for _ in range(4)]

        assert conn2 not in results
        assert all(c in [conn1, conn3] for c in results)

    def test_can_get_next_with_alive_connections(self):
        """Test can_get_next returns True when alive connections exist."""
        lb = RoundRobinLoadBalancer()

        conn = create_mock_connection(1)
        lb.init([conn])

        assert lb.can_get_next()

    def test_can_get_next_all_dead(self):
        """Test can_get_next returns False when all connections are dead."""
        lb = RoundRobinLoadBalancer()

        conn = create_mock_connection(1)
        lb.init([conn])

        conn.is_alive = False

        assert not lb.can_get_next()

    def test_next_returns_none_when_all_dead(self):
        """Test next returns None when all connections become dead."""
        lb = RoundRobinLoadBalancer()

        conn1 = create_mock_connection(1)
        conn2 = create_mock_connection(2)

        lb.init([conn1, conn2])

        conn1.is_alive = False
        conn2.is_alive = False

        assert lb.next() is None

    def test_single_connection_round_robin(self):
        """Test round-robin with single connection."""
        lb = RoundRobinLoadBalancer()

        conn = create_mock_connection(1)
        lb.init([conn])

        for _ in range(5):
            assert lb.next() == conn

    def test_index_wraps_correctly(self):
        """Test that index wraps around correctly after full cycle."""
        lb = RoundRobinLoadBalancer()

        connections = [create_mock_connection(i) for i in range(3)]
        lb.init(connections)

        for _ in range(10):
            result = lb.next()
            assert result in connections


class TestRandomLoadBalancer:
    """Tests for RandomLoadBalancer."""

    def test_init_with_empty_connections(self):
        """Test initialization with no connections."""
        lb = RandomLoadBalancer()
        lb.init([])

        assert lb.next() is None
        assert not lb.can_get_next()

    def test_init_filters_dead_connections(self):
        """Test that init only keeps alive connections."""
        lb = RandomLoadBalancer()

        alive = create_mock_connection(1, alive=True)
        dead = create_mock_connection(2, alive=False)

        lb.init([alive, dead])

        assert lb.can_get_next()
        assert lb.next() == alive

    def test_random_returns_alive_connections(self):
        """Test that random selection only returns alive connections."""
        lb = RandomLoadBalancer()

        conn1 = create_mock_connection(1, "host1")
        conn2 = create_mock_connection(2, "host2")
        conn3 = create_mock_connection(3, "host3")

        lb.init([conn1, conn2, conn3])

        for _ in range(20):
            result = lb.next()
            assert result in [conn1, conn2, conn3]
            assert result.is_alive

    def test_random_skips_dead_connections(self):
        """Test that random selection skips dead connections."""
        lb = RandomLoadBalancer()

        conn1 = create_mock_connection(1, "host1")
        conn2 = create_mock_connection(2, "host2")

        lb.init([conn1, conn2])

        conn2.is_alive = False

        for _ in range(10):
            assert lb.next() == conn1

    def test_random_distribution(self):
        """Test that random selection distributes across connections."""
        lb = RandomLoadBalancer()

        conn1 = create_mock_connection(1, "host1")
        conn2 = create_mock_connection(2, "host2")

        lb.init([conn1, conn2])

        counts = {conn1: 0, conn2: 0}
        for _ in range(100):
            result = lb.next()
            counts[result] += 1

        assert counts[conn1] > 0
        assert counts[conn2] > 0

    def test_single_connection_random(self):
        """Test random with single connection."""
        lb = RandomLoadBalancer()

        conn = create_mock_connection(1)
        lb.init([conn])

        for _ in range(5):
            assert lb.next() == conn

    def test_can_get_next_reflects_alive_state(self):
        """Test can_get_next updates based on connection alive state."""
        lb = RandomLoadBalancer()

        conn = create_mock_connection(1)
        lb.init([conn])

        assert lb.can_get_next()

        conn.is_alive = False

        assert not lb.can_get_next()


class TestConnectionManagerRouting:
    """Tests for ConnectionManager routing logic."""

    @pytest.fixture
    def manager(self):
        """Create a ConnectionManager for testing."""
        return ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.ALL_MEMBERS,
            connection_timeout=1.0,
        )

    def test_routing_mode_property(self, manager):
        """Test routing_mode property returns correct value."""
        assert manager.routing_mode == RoutingMode.ALL_MEMBERS

    def test_single_member_routing_mode(self):
        """Test SINGLE_MEMBER routing mode."""
        manager = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )
        assert manager.routing_mode == RoutingMode.SINGLE_MEMBER

    def test_multi_member_routing_mode(self):
        """Test MULTI_MEMBER routing mode."""
        manager = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.MULTI_MEMBER,
        )
        assert manager.routing_mode == RoutingMode.MULTI_MEMBER

    def test_all_members_uses_load_balancer(self):
        """Test that ALL_MEMBERS mode uses the load balancer."""
        lb = MagicMock(spec=LoadBalancer)
        expected_conn = create_mock_connection(1)
        lb.next.return_value = expected_conn

        manager = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.ALL_MEMBERS,
            load_balancer=lb,
        )

        result = manager.get_connection()

        lb.next.assert_called_once()
        assert result == expected_conn


class TestConnectionManagerPoolManagement:
    """Tests for ConnectionManager connection pool management."""

    @pytest.fixture
    def manager(self):
        """Create a ConnectionManager for testing."""
        return ConnectionManager(
            addresses=["localhost:5701", "localhost:5702"],
            routing_mode=RoutingMode.ALL_MEMBERS,
            connection_timeout=1.0,
        )

    def test_initial_connection_count_is_zero(self, manager):
        """Test that initial connection count is zero."""
        assert manager.connection_count == 0

    def test_is_running_initially_false(self, manager):
        """Test that manager is not running initially."""
        assert not manager.is_running

    def test_get_connection_returns_none_when_empty(self, manager):
        """Test get_connection returns None when no connections exist."""
        assert manager.get_connection() is None

    def test_get_all_connections_returns_empty_when_no_connections(self, manager):
        """Test get_all_connections returns empty list initially."""
        assert manager.get_all_connections() == []

    def test_get_connection_for_address_returns_none_when_not_found(self, manager):
        """Test get_connection_for_address returns None for unknown address."""
        address = Address("unknown", 5701)
        assert manager.get_connection_for_address(address) is None

    def test_connection_count_reflects_alive_connections(self, manager):
        """Test connection_count only counts alive connections."""
        conn1 = create_mock_connection(1, alive=True)
        conn2 = create_mock_connection(2, alive=False)

        manager._connections[1] = conn1
        manager._connections[2] = conn2

        assert manager.connection_count == 1

    def test_get_all_connections_returns_only_alive(self, manager):
        """Test get_all_connections filters dead connections."""
        conn1 = create_mock_connection(1, alive=True)
        conn2 = create_mock_connection(2, alive=False)
        conn3 = create_mock_connection(3, alive=True)

        manager._connections[1] = conn1
        manager._connections[2] = conn2
        manager._connections[3] = conn3

        result = manager.get_all_connections()

        assert len(result) == 2
        assert conn1 in result
        assert conn3 in result
        assert conn2 not in result

    def test_get_connection_for_address_returns_alive_connection(self, manager):
        """Test get_connection_for_address returns alive connection."""
        address = Address("localhost", 5701)
        conn = create_mock_connection(1, "localhost", 5701)

        manager._address_connections[address] = conn

        result = manager.get_connection_for_address(address)

        assert result == conn

    def test_get_connection_for_address_returns_none_for_dead(self, manager):
        """Test get_connection_for_address returns None for dead connection."""
        address = Address("localhost", 5701)
        conn = create_mock_connection(1, "localhost", 5701, alive=False)

        manager._address_connections[address] = conn

        result = manager.get_connection_for_address(address)

        assert result is None


class TestConnectionManagerBackoff:
    """Tests for ConnectionManager backoff calculation."""

    def test_default_backoff_calculation(self):
        """Test default backoff calculation without retry config."""
        manager = ConnectionManager(
            addresses=["localhost:5701"],
            retry_config=None,
        )

        assert manager._calculate_backoff(0) == 1.0
        assert manager._calculate_backoff(1) == 2.0
        assert manager._calculate_backoff(2) == 4.0
        assert manager._calculate_backoff(5) == 30.0

    def test_backoff_with_retry_config(self):
        """Test backoff calculation with retry config."""

        class MockRetryConfig:
            initial_backoff = 0.5
            multiplier = 2.0
            max_backoff = 10.0
            jitter = 0.0

        manager = ConnectionManager(
            addresses=["localhost:5701"],
            retry_config=MockRetryConfig(),
        )

        assert manager._calculate_backoff(0) == 0.5
        assert manager._calculate_backoff(1) == 1.0
        assert manager._calculate_backoff(2) == 2.0
        assert manager._calculate_backoff(5) == 10.0

    def test_backoff_with_jitter(self):
        """Test backoff calculation applies jitter."""

        class MockRetryConfig:
            initial_backoff = 1.0
            multiplier = 1.0
            max_backoff = 10.0
            jitter = 0.5

        manager = ConnectionManager(
            addresses=["localhost:5701"],
            retry_config=MockRetryConfig(),
        )

        results = [manager._calculate_backoff(0) for _ in range(10)]

        assert all(1.0 <= r <= 1.5 for r in results)

    def test_backoff_capped_at_max(self):
        """Test backoff is capped at max value."""

        class MockRetryConfig:
            initial_backoff = 1.0
            multiplier = 10.0
            max_backoff = 5.0
            jitter = 0.0

        manager = ConnectionManager(
            addresses=["localhost:5701"],
            retry_config=MockRetryConfig(),
        )

        assert manager._calculate_backoff(0) == 1.0
        assert manager._calculate_backoff(1) == 5.0
        assert manager._calculate_backoff(10) == 5.0


class TestConnectionManagerLifecycle:
    """Tests for ConnectionManager lifecycle (start/shutdown)."""

    @pytest.mark.asyncio
    async def test_start_fails_with_no_cluster_members(self):
        """Test that start raises exception when no members can be reached."""
        manager = ConnectionManager(
            addresses=["nonexistent.invalid:5701"],
            routing_mode=RoutingMode.ALL_MEMBERS,
            connection_timeout=0.1,
        )

        with pytest.raises(ClientOfflineException):
            await manager.start()

    @pytest.mark.asyncio
    async def test_shutdown_when_not_started(self):
        """Test that shutdown works even when not started."""
        manager = ConnectionManager(
            addresses=["localhost:5701"],
        )

        await manager.shutdown()
        assert not manager.is_running

    @pytest.mark.asyncio
    async def test_shutdown_clears_connections(self):
        """Test that shutdown clears all connections."""
        manager = ConnectionManager(
            addresses=["localhost:5701"],
        )

        conn = MagicMock()
        conn.is_alive = True
        conn.connection_id = 1
        conn.close = MagicMock(return_value=asyncio.coroutine(lambda: None)())

        manager._connections[1] = conn
        manager._running = True

        await manager.shutdown()

        assert len(manager._connections) == 0
        assert len(manager._address_connections) == 0
        assert not manager.is_running


class TestConnectionManagerEdgeCases:
    """Tests for edge cases in ConnectionManager."""

    def test_empty_cluster_membership(self):
        """Test handling of empty cluster membership."""
        manager = ConnectionManager(
            addresses=[],
            routing_mode=RoutingMode.ALL_MEMBERS,
        )

        assert manager.connection_count == 0
        assert manager.get_connection() is None
        assert manager.get_all_connections() == []

    def test_set_connection_listener(self):
        """Test setting connection listeners."""
        manager = ConnectionManager(addresses=["localhost:5701"])

        opened_called = []
        closed_called = []

        def on_opened(conn):
            opened_called.append(conn)

        def on_closed(conn, reason):
            closed_called.append((conn, reason))

        manager.set_connection_listener(on_opened, on_closed)

        assert manager._on_connection_opened is not None
        assert manager._on_connection_closed is not None

    def test_set_message_callback(self):
        """Test setting message callback."""
        manager = ConnectionManager(addresses=["localhost:5701"])

        def callback(msg):
            pass

        manager.set_message_callback(callback)

        assert manager._message_callback == callback

    def test_custom_load_balancer(self):
        """Test using a custom load balancer."""
        custom_lb = RoundRobinLoadBalancer()

        manager = ConnectionManager(
            addresses=["localhost:5701"],
            load_balancer=custom_lb,
        )

        assert manager._load_balancer is custom_lb

    def test_partition_id_parameter_accepted(self):
        """Test get_connection accepts partition_id parameter."""
        manager = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.ALL_MEMBERS,
        )

        result = manager.get_connection(partition_id=5)
        assert result is None


class TestRapidMemberChanges:
    """Tests for rapid member join/leave scenarios."""

    def test_load_balancer_handles_rapid_alive_changes(self):
        """Test load balancer handles rapid connection state changes."""
        lb = RoundRobinLoadBalancer()

        connections = [create_mock_connection(i) for i in range(5)]
        lb.init(connections)

        for _ in range(10):
            for i, conn in enumerate(connections):
                conn.is_alive = (i % 2 == 0)

            result = lb.next()
            if result:
                assert result.is_alive

        for conn in connections:
            conn.is_alive = False

        assert lb.next() is None

        connections[2].is_alive = True

        assert lb.next() == connections[2]

    def test_load_balancer_reinit_during_operations(self):
        """Test reinitializing load balancer during operations."""
        lb = RoundRobinLoadBalancer()

        initial_conns = [create_mock_connection(i) for i in range(3)]
        lb.init(initial_conns)

        lb.next()

        new_conns = [create_mock_connection(i + 10) for i in range(2)]
        lb.init(new_conns)

        result = lb.next()
        assert result in new_conns

    def test_manager_handles_all_connections_dying(self):
        """Test manager handles all connections dying gracefully."""
        manager = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.ALL_MEMBERS,
        )

        connections = [create_mock_connection(i) for i in range(3)]
        for conn in connections:
            manager._connections[conn.connection_id] = conn

        manager._load_balancer.init(list(manager._connections.values()))

        for conn in connections:
            conn.is_alive = False

        assert manager.connection_count == 0
        assert manager.get_connection() is None

    def test_manager_handles_connection_revival(self):
        """Test manager handles connection revival."""
        manager = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.ALL_MEMBERS,
        )

        conn = create_mock_connection(1)
        manager._connections[1] = conn
        manager._load_balancer.init([conn])

        conn.is_alive = False
        assert manager.connection_count == 0

        conn.is_alive = True
        assert manager.connection_count == 1

    def test_rapid_add_remove_addresses(self):
        """Test handling rapid additions to pending addresses."""
        manager = ConnectionManager(
            addresses=["localhost:5701"],
        )

        for i in range(10):
            address = Address(f"host{i}", 5701)
            manager._pending_addresses.append(address)

        assert len(manager._pending_addresses) == 10

        manager._pending_addresses.clear()

        assert len(manager._pending_addresses) == 0


class TestSingleMemberRoutingMode:
    """Tests specific to SINGLE_MEMBER routing mode."""

    def test_single_member_get_connection_returns_first_alive(self):
        """Test that SINGLE_MEMBER mode returns first alive connection."""
        manager = ConnectionManager(
            addresses=["localhost:5701", "localhost:5702"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )

        conn1 = create_mock_connection(1, "localhost", 5701)
        conn2 = create_mock_connection(2, "localhost", 5702)

        manager._connections[1] = conn1
        manager._connections[2] = conn2

        result = manager.get_connection()
        assert result in [conn1, conn2]
        assert result.is_alive

    def test_single_member_returns_none_when_all_dead(self):
        """Test that SINGLE_MEMBER returns None when all connections dead."""
        manager = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )

        conn = create_mock_connection(1, alive=False)
        manager._connections[1] = conn

        assert manager.get_connection() is None

    def test_single_member_ignores_load_balancer(self):
        """Test that SINGLE_MEMBER mode doesn't use load balancer."""
        lb = MagicMock(spec=LoadBalancer)

        manager = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
            load_balancer=lb,
        )

        conn = create_mock_connection(1)
        manager._connections[1] = conn

        result = manager.get_connection()

        lb.next.assert_not_called()
        assert result == conn


class TestHeartbeatIntegration:
    """Tests for heartbeat integration with ConnectionManager."""

    def test_heartbeat_manager_created_on_config(self):
        """Test heartbeat manager uses configured intervals."""
        manager = ConnectionManager(
            addresses=["localhost:5701"],
            heartbeat_interval=10.0,
            heartbeat_timeout=120.0,
        )

        assert manager._failure_detector.heartbeat_interval == 10.0
        assert manager._failure_detector.heartbeat_timeout == 120.0

    def test_default_heartbeat_config(self):
        """Test default heartbeat configuration."""
        manager = ConnectionManager(
            addresses=["localhost:5701"],
        )

        assert manager._failure_detector.heartbeat_interval == 5.0
        assert manager._failure_detector.heartbeat_timeout == 60.0
