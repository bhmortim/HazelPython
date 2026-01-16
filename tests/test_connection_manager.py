"""Unit tests for hazelcast.network.connection_manager module."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest

from hazelcast.network.address import Address
from hazelcast.network.connection import Connection, ConnectionState
from hazelcast.network.connection_manager import (
    ConnectionManager,
    LoadBalancer,
    RandomLoadBalancer,
    RoundRobinLoadBalancer,
    RoutingMode,
)
from hazelcast.exceptions import ClientOfflineException


class MockConnection:
    """Mock connection for testing."""

    def __init__(self, connection_id: int, is_alive: bool = True, address: Address = None):
        self.connection_id = connection_id
        self._is_alive = is_alive
        self.address = address or Address("127.0.0.1", 5701)
        self._last_read_time = time.time()
        self._last_write_time = time.time()
        self._message_callback = None
        self._closed = False
        self._close_reason = None

    @property
    def is_alive(self) -> bool:
        return self._is_alive and not self._closed

    @property
    def last_read_time(self) -> float:
        return self._last_read_time

    @property
    def last_write_time(self) -> float:
        return self._last_write_time

    def set_message_callback(self, callback):
        self._message_callback = callback

    async def connect(self):
        pass

    async def close(self, reason: str = ""):
        self._closed = True
        self._close_reason = reason

    def start_reading(self):
        pass

    def send_sync(self, message):
        pass


class TestRoundRobinLoadBalancer:
    """Tests for RoundRobinLoadBalancer class."""

    def test_init(self):
        lb = RoundRobinLoadBalancer()
        assert lb._connections == []
        assert lb._index == 0

    def test_init_with_connections(self):
        lb = RoundRobinLoadBalancer()
        conn1 = MockConnection(1)
        conn2 = MockConnection(2)
        lb.init([conn1, conn2])
        assert len(lb._connections) == 2

    def test_init_filters_dead_connections(self):
        lb = RoundRobinLoadBalancer()
        conn1 = MockConnection(1, is_alive=True)
        conn2 = MockConnection(2, is_alive=False)
        lb.init([conn1, conn2])
        assert len(lb._connections) == 1
        assert lb._connections[0].connection_id == 1

    def test_next_empty(self):
        lb = RoundRobinLoadBalancer()
        lb.init([])
        assert lb.next() is None

    def test_next_single_connection(self):
        lb = RoundRobinLoadBalancer()
        conn = MockConnection(1)
        lb.init([conn])
        assert lb.next() is conn
        assert lb.next() is conn

    def test_next_multiple_connections(self):
        lb = RoundRobinLoadBalancer()
        conn1 = MockConnection(1)
        conn2 = MockConnection(2)
        conn3 = MockConnection(3)
        lb.init([conn1, conn2, conn3])

        assert lb.next() is conn1
        assert lb.next() is conn2
        assert lb.next() is conn3
        assert lb.next() is conn1

    def test_next_skips_dead_connections(self):
        lb = RoundRobinLoadBalancer()
        conn1 = MockConnection(1)
        conn2 = MockConnection(2, is_alive=False)
        conn3 = MockConnection(3)
        lb.init([conn1, conn2, conn3])

        results = [lb.next() for _ in range(4)]
        assert conn2 not in results

    def test_next_all_dead(self):
        lb = RoundRobinLoadBalancer()
        conn1 = MockConnection(1)
        lb.init([conn1])
        conn1._is_alive = False
        assert lb.next() is None

    def test_can_get_next_empty(self):
        lb = RoundRobinLoadBalancer()
        lb.init([])
        assert lb.can_get_next() is False

    def test_can_get_next_with_alive(self):
        lb = RoundRobinLoadBalancer()
        conn = MockConnection(1)
        lb.init([conn])
        assert lb.can_get_next() is True

    def test_can_get_next_all_dead(self):
        lb = RoundRobinLoadBalancer()
        conn = MockConnection(1)
        lb.init([conn])
        conn._is_alive = False
        assert lb.can_get_next() is False


class TestRandomLoadBalancer:
    """Tests for RandomLoadBalancer class."""

    def test_init(self):
        lb = RandomLoadBalancer()
        assert lb._connections == []

    def test_init_with_connections(self):
        lb = RandomLoadBalancer()
        conn1 = MockConnection(1)
        conn2 = MockConnection(2)
        lb.init([conn1, conn2])
        assert len(lb._connections) == 2

    def test_init_filters_dead_connections(self):
        lb = RandomLoadBalancer()
        conn1 = MockConnection(1, is_alive=True)
        conn2 = MockConnection(2, is_alive=False)
        lb.init([conn1, conn2])
        assert len(lb._connections) == 1

    def test_next_empty(self):
        lb = RandomLoadBalancer()
        lb.init([])
        assert lb.next() is None

    def test_next_single_connection(self):
        lb = RandomLoadBalancer()
        conn = MockConnection(1)
        lb.init([conn])
        assert lb.next() is conn

    def test_next_multiple_connections(self):
        lb = RandomLoadBalancer()
        conn1 = MockConnection(1)
        conn2 = MockConnection(2)
        lb.init([conn1, conn2])

        results = set()
        for _ in range(100):
            result = lb.next()
            results.add(result.connection_id)

        assert 1 in results or 2 in results

    def test_next_skips_dead_connections(self):
        lb = RandomLoadBalancer()
        conn1 = MockConnection(1)
        conn2 = MockConnection(2, is_alive=False)
        lb.init([conn1, conn2])

        for _ in range(10):
            result = lb.next()
            assert result is conn1

    def test_next_all_dead(self):
        lb = RandomLoadBalancer()
        conn = MockConnection(1)
        lb.init([conn])
        conn._is_alive = False
        assert lb.next() is None

    def test_can_get_next_empty(self):
        lb = RandomLoadBalancer()
        lb.init([])
        assert lb.can_get_next() is False

    def test_can_get_next_with_alive(self):
        lb = RandomLoadBalancer()
        conn = MockConnection(1)
        lb.init([conn])
        assert lb.can_get_next() is True

    def test_can_get_next_all_dead(self):
        lb = RandomLoadBalancer()
        conn = MockConnection(1)
        lb.init([conn])
        conn._is_alive = False
        assert lb.can_get_next() is False


class TestRoutingMode:
    """Tests for RoutingMode enum."""

    def test_all_members(self):
        assert RoutingMode.ALL_MEMBERS.value == "ALL_MEMBERS"

    def test_single_member(self):
        assert RoutingMode.SINGLE_MEMBER.value == "SINGLE_MEMBER"

    def test_multi_member(self):
        assert RoutingMode.MULTI_MEMBER.value == "MULTI_MEMBER"


class TestConnectionManager:
    """Tests for ConnectionManager class."""

    def test_init_defaults(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        assert cm.routing_mode == RoutingMode.ALL_MEMBERS
        assert cm._connection_timeout == 5.0
        assert cm.is_running is False
        assert cm.connection_count == 0

    def test_init_custom_routing_mode(self):
        cm = ConnectionManager(
            addresses=["127.0.0.1:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )
        assert cm.routing_mode == RoutingMode.SINGLE_MEMBER

    def test_init_with_custom_load_balancer(self):
        lb = RandomLoadBalancer()
        cm = ConnectionManager(
            addresses=["127.0.0.1:5701"],
            load_balancer=lb,
        )
        assert cm._load_balancer is lb

    def test_init_parses_addresses(self):
        cm = ConnectionManager(
            addresses=["192.168.1.1:5701", "192.168.1.2:5702"]
        )
        assert len(cm._addresses) == 2
        assert cm._addresses[0].host == "192.168.1.1"
        assert cm._addresses[0].port == 5701
        assert cm._addresses[1].host == "192.168.1.2"
        assert cm._addresses[1].port == 5702

    def test_set_connection_listener(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        on_opened = MagicMock()
        on_closed = MagicMock()
        cm.set_connection_listener(on_opened, on_closed)
        assert cm._on_connection_opened is on_opened
        assert cm._on_connection_closed is on_closed

    def test_set_message_callback(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        callback = MagicMock()
        cm.set_message_callback(callback)
        assert cm._message_callback is callback

    def test_get_connection_single_member_mode(self):
        cm = ConnectionManager(
            addresses=["127.0.0.1:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )
        conn = MockConnection(1)
        cm._connections[1] = conn
        result = cm.get_connection()
        assert result is conn

    def test_get_connection_single_member_mode_no_alive(self):
        cm = ConnectionManager(
            addresses=["127.0.0.1:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )
        conn = MockConnection(1, is_alive=False)
        cm._connections[1] = conn
        result = cm.get_connection()
        assert result is None

    def test_get_connection_uses_load_balancer(self):
        lb = MagicMock(spec=LoadBalancer)
        lb.next.return_value = MockConnection(1)
        cm = ConnectionManager(
            addresses=["127.0.0.1:5701"],
            routing_mode=RoutingMode.ALL_MEMBERS,
            load_balancer=lb,
        )
        result = cm.get_connection()
        lb.next.assert_called_once()

    def test_get_connection_for_address_found(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        addr = Address("127.0.0.1", 5701)
        conn = MockConnection(1, address=addr)
        cm._address_connections[addr] = conn
        result = cm.get_connection_for_address(addr)
        assert result is conn

    def test_get_connection_for_address_not_found(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        addr = Address("127.0.0.1", 5701)
        result = cm.get_connection_for_address(addr)
        assert result is None

    def test_get_connection_for_address_dead(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        addr = Address("127.0.0.1", 5701)
        conn = MockConnection(1, is_alive=False, address=addr)
        cm._address_connections[addr] = conn
        result = cm.get_connection_for_address(addr)
        assert result is None

    def test_get_all_connections_empty(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        assert cm.get_all_connections() == []

    def test_get_all_connections_filters_dead(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        conn1 = MockConnection(1, is_alive=True)
        conn2 = MockConnection(2, is_alive=False)
        conn3 = MockConnection(3, is_alive=True)
        cm._connections = {1: conn1, 2: conn2, 3: conn3}
        result = cm.get_all_connections()
        assert len(result) == 2
        assert conn2 not in result

    def test_calculate_backoff_no_config(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        backoff0 = cm._calculate_backoff(0)
        backoff1 = cm._calculate_backoff(1)
        backoff2 = cm._calculate_backoff(2)

        assert backoff0 == 1.0
        assert backoff1 == 2.0
        assert backoff2 == 4.0

    def test_calculate_backoff_max_cap(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        backoff = cm._calculate_backoff(10)
        assert backoff <= 30.0

    def test_calculate_backoff_with_config(self):
        retry_config = MagicMock()
        retry_config.initial_backoff = 0.5
        retry_config.multiplier = 2.0
        retry_config.max_backoff = 10.0
        retry_config.jitter = 0.0

        cm = ConnectionManager(
            addresses=["127.0.0.1:5701"],
            retry_config=retry_config,
        )

        backoff0 = cm._calculate_backoff(0)
        backoff1 = cm._calculate_backoff(1)

        assert backoff0 == 0.5
        assert backoff1 == 1.0

    def test_calculate_backoff_with_jitter(self):
        retry_config = MagicMock()
        retry_config.initial_backoff = 1.0
        retry_config.multiplier = 1.0
        retry_config.max_backoff = 10.0
        retry_config.jitter = 0.5

        cm = ConnectionManager(
            addresses=["127.0.0.1:5701"],
            retry_config=retry_config,
        )

        backoffs = [cm._calculate_backoff(0) for _ in range(10)]
        assert any(b != 1.0 for b in backoffs)
        assert all(b >= 1.0 for b in backoffs)
        assert all(b <= 1.5 for b in backoffs)

    @pytest.mark.asyncio
    async def test_start_already_running(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        cm._running = True
        await cm.start()

    @pytest.mark.asyncio
    async def test_start_no_connections_raises(self):
        cm = ConnectionManager(addresses=["invalid.host.test:5701"])

        with patch.object(cm, "_connect_to_cluster", new_callable=AsyncMock):
            with pytest.raises(ClientOfflineException):
                await cm.start()

    @pytest.mark.asyncio
    async def test_shutdown_not_running(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        await cm.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_cancels_reconnect_task(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        cm._running = True
        cm._reconnect_task = asyncio.create_task(asyncio.sleep(10))

        await cm.shutdown()

        assert cm._running is False
        assert cm._reconnect_task is None

    @pytest.mark.asyncio
    async def test_shutdown_closes_connections(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        cm._running = True

        conn1 = MockConnection(1)
        conn2 = MockConnection(2)
        cm._connections = {1: conn1, 2: conn2}

        await cm.shutdown()

        assert conn1._closed
        assert conn2._closed
        assert len(cm._connections) == 0

    @pytest.mark.asyncio
    async def test_connect_to_address_reuses_existing(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        addr = Address("127.0.0.1", 5701)
        existing_conn = MockConnection(1, address=addr)
        cm._address_connections[addr] = existing_conn

        result = await cm._connect_to_address(addr)
        assert result is existing_conn

    @pytest.mark.asyncio
    async def test_connect_to_cluster_single_member_stops_on_first(self):
        cm = ConnectionManager(
            addresses=["127.0.0.1:5701", "127.0.0.1:5702"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )

        connect_count = 0

        async def mock_connect(addr):
            nonlocal connect_count
            connect_count += 1
            conn = MockConnection(connect_count, address=addr)
            cm._connections[connect_count] = conn
            return conn

        with patch.object(cm, "_connect_to_address", side_effect=mock_connect):
            await cm._connect_to_cluster()
            assert connect_count == 1

    @pytest.mark.asyncio
    async def test_connect_to_cluster_all_members_connects_all(self):
        cm = ConnectionManager(
            addresses=["127.0.0.1:5701", "127.0.0.1:5702"],
            routing_mode=RoutingMode.ALL_MEMBERS,
        )

        connect_count = 0

        async def mock_connect(addr):
            nonlocal connect_count
            connect_count += 1
            conn = MockConnection(connect_count, address=addr)
            cm._connections[connect_count] = conn
            return conn

        with patch.object(cm, "_connect_to_address", side_effect=mock_connect):
            await cm._connect_to_cluster()
            assert connect_count >= 2

    @pytest.mark.asyncio
    async def test_try_reconnect_single_member_skips_when_connected(self):
        cm = ConnectionManager(
            addresses=["127.0.0.1:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )
        cm._connections[1] = MockConnection(1)

        connect_mock = AsyncMock()
        with patch.object(cm, "_connect_to_address", connect_mock):
            await cm._try_reconnect()
            connect_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_try_reconnect_adds_failed_to_pending(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        cm._addresses = [Address("127.0.0.1", 5701)]

        async def fail_connect(addr):
            raise Exception("Connection failed")

        with patch.object(cm, "_connect_to_address", side_effect=fail_connect):
            await cm._try_reconnect()
            assert len(cm._pending_addresses) > 0

    @pytest.mark.asyncio
    async def test_close_connection_removes_from_collections(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        addr = Address("127.0.0.1", 5701)
        conn = MockConnection(1, address=addr)
        cm._connections[1] = conn
        cm._address_connections[addr] = conn

        await cm._close_connection(conn, "Test close")

        assert 1 not in cm._connections
        assert addr not in cm._address_connections
        assert addr in cm._pending_addresses

    @pytest.mark.asyncio
    async def test_close_connection_calls_listener(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        on_closed = MagicMock()
        cm._on_connection_closed = on_closed

        conn = MockConnection(1)
        cm._connections[1] = conn

        await cm._close_connection(conn, "Test close")

        on_closed.assert_called_once_with(conn, "Test close")

    def test_on_connection_closed_schedules_close(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        conn = MockConnection(1)

        with patch("asyncio.create_task") as mock_create_task:
            cm.on_connection_closed(conn, "External close")
            mock_create_task.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_heartbeat_failure_closes_connection(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        conn = MockConnection(1)
        cm._connections[1] = conn

        with patch.object(cm, "_close_connection", new_callable=AsyncMock) as mock_close:
            cm._on_heartbeat_failure(conn, "Heartbeat timeout")
            await asyncio.sleep(0.01)
            mock_close.assert_called()

    def test_send_heartbeat(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        conn = MockConnection(1)

        with patch.object(conn, "send_sync") as mock_send:
            cm._send_heartbeat(conn)
            mock_send.assert_called_once()

    def test_send_heartbeat_exception_caught(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        conn = MockConnection(1)

        with patch.object(conn, "send_sync", side_effect=Exception("Send failed")):
            cm._send_heartbeat(conn)

    @pytest.mark.asyncio
    async def test_reconnect_loop_cancellation(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        cm._running = True

        async def slow_reconnect():
            await asyncio.sleep(10)

        with patch.object(cm, "_try_reconnect", side_effect=slow_reconnect):
            task = asyncio.create_task(cm._reconnect_loop())
            await asyncio.sleep(0.01)
            cm._running = False
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_reconnect_loop_exception_handling(self):
        cm = ConnectionManager(addresses=["127.0.0.1:5701"])
        cm._running = True

        call_count = 0

        async def failing_reconnect():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Reconnect failed")
            cm._running = False

        with patch.object(cm, "_try_reconnect", side_effect=failing_reconnect):
            with patch.object(cm, "_calculate_backoff", return_value=0.001):
                await cm._reconnect_loop()

        assert call_count >= 2
