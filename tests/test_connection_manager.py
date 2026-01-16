"""Comprehensive unit tests for hazelcast/network/connection_manager.py."""

import asyncio
import pytest
from unittest.mock import MagicMock, patch, AsyncMock, PropertyMock

from hazelcast.network.connection_manager import (
    RoutingMode,
    LoadBalancer,
    RoundRobinLoadBalancer,
    RandomLoadBalancer,
    ConnectionManager,
)
from hazelcast.network.address import Address
from hazelcast.network.connection import Connection, ConnectionState
from hazelcast.exceptions import ClientOfflineException


class TestRoutingMode:
    def test_enum_values(self):
        assert RoutingMode.ALL_MEMBERS.value == "ALL_MEMBERS"
        assert RoutingMode.SINGLE_MEMBER.value == "SINGLE_MEMBER"
        assert RoutingMode.MULTI_MEMBER.value == "MULTI_MEMBER"


class TestRoundRobinLoadBalancer:
    def test_init(self):
        lb = RoundRobinLoadBalancer()
        assert lb._connections == []
        assert lb._index == 0

    def test_init_with_connections(self):
        lb = RoundRobinLoadBalancer()
        
        conn1 = MagicMock()
        conn1.is_alive = True
        conn2 = MagicMock()
        conn2.is_alive = True
        conn3 = MagicMock()
        conn3.is_alive = False
        
        lb.init([conn1, conn2, conn3])
        
        assert len(lb._connections) == 2
        assert conn1 in lb._connections
        assert conn2 in lb._connections
        assert conn3 not in lb._connections

    def test_next_empty(self):
        lb = RoundRobinLoadBalancer()
        lb.init([])
        
        assert lb.next() is None

    def test_next_round_robin(self):
        lb = RoundRobinLoadBalancer()
        
        conn1 = MagicMock()
        conn1.is_alive = True
        conn2 = MagicMock()
        conn2.is_alive = True
        
        lb.init([conn1, conn2])
        
        result1 = lb.next()
        result2 = lb.next()
        result3 = lb.next()
        
        assert result1 is conn1
        assert result2 is conn2
        assert result3 is conn1

    def test_next_skips_dead_connections(self):
        lb = RoundRobinLoadBalancer()
        
        conn1 = MagicMock()
        conn1.is_alive = True
        conn2 = MagicMock()
        conn2.is_alive = False
        
        lb.init([conn1, conn2])
        
        conn2.is_alive = False
        
        result = lb.next()
        assert result is conn1

    def test_next_all_dead(self):
        lb = RoundRobinLoadBalancer()
        
        conn1 = MagicMock()
        conn1.is_alive = True
        
        lb.init([conn1])
        
        conn1.is_alive = False
        
        assert lb.next() is None

    def test_can_get_next_true(self):
        lb = RoundRobinLoadBalancer()
        
        conn = MagicMock()
        conn.is_alive = True
        
        lb.init([conn])
        
        assert lb.can_get_next() is True

    def test_can_get_next_false_empty(self):
        lb = RoundRobinLoadBalancer()
        lb.init([])
        
        assert lb.can_get_next() is False

    def test_can_get_next_false_all_dead(self):
        lb = RoundRobinLoadBalancer()
        
        conn = MagicMock()
        conn.is_alive = True
        
        lb.init([conn])
        conn.is_alive = False
        
        assert lb.can_get_next() is False


class TestRandomLoadBalancer:
    def test_init(self):
        lb = RandomLoadBalancer()
        assert lb._connections == []

    def test_init_with_connections(self):
        lb = RandomLoadBalancer()
        
        conn1 = MagicMock()
        conn1.is_alive = True
        conn2 = MagicMock()
        conn2.is_alive = False
        
        lb.init([conn1, conn2])
        
        assert len(lb._connections) == 1
        assert conn1 in lb._connections

    def test_next_empty(self):
        lb = RandomLoadBalancer()
        lb.init([])
        
        assert lb.next() is None

    def test_next_returns_alive_connection(self):
        lb = RandomLoadBalancer()
        
        conn1 = MagicMock()
        conn1.is_alive = True
        
        lb.init([conn1])
        
        result = lb.next()
        assert result is conn1

    def test_next_all_dead(self):
        lb = RandomLoadBalancer()
        
        conn = MagicMock()
        conn.is_alive = True
        
        lb.init([conn])
        conn.is_alive = False
        
        assert lb.next() is None

    def test_can_get_next_true(self):
        lb = RandomLoadBalancer()
        
        conn = MagicMock()
        conn.is_alive = True
        
        lb.init([conn])
        
        assert lb.can_get_next() is True

    def test_can_get_next_false(self):
        lb = RandomLoadBalancer()
        lb.init([])
        
        assert lb.can_get_next() is False


class TestConnectionManagerInit:
    def test_default_init(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        assert cm.routing_mode == RoutingMode.ALL_MEMBERS
        assert cm._connection_timeout == 5.0
        assert cm.is_running is False
        assert cm.connection_count == 0

    def test_init_with_params(self):
        retry_config = MagicMock()
        ssl_config = MagicMock()
        load_balancer = MagicMock()
        
        cm = ConnectionManager(
            addresses=["host1:5701", "host2:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
            connection_timeout=10.0,
            retry_config=retry_config,
            ssl_config=ssl_config,
            load_balancer=load_balancer,
            heartbeat_interval=10.0,
            heartbeat_timeout=120.0,
        )
        
        assert cm.routing_mode == RoutingMode.SINGLE_MEMBER
        assert cm._connection_timeout == 10.0
        assert cm._retry_config is retry_config
        assert cm._ssl_config is ssl_config
        assert cm._load_balancer is load_balancer


class TestConnectionManagerProperties:
    def test_routing_mode_property(self):
        cm = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.MULTI_MEMBER,
        )
        assert cm.routing_mode == RoutingMode.MULTI_MEMBER

    def test_is_running_property(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        assert cm.is_running is False
        
        cm._running = True
        assert cm.is_running is True

    def test_connection_count_property(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        conn1 = MagicMock()
        conn1.is_alive = True
        conn2 = MagicMock()
        conn2.is_alive = False
        
        cm._connections = {1: conn1, 2: conn2}
        
        assert cm.connection_count == 1


class TestConnectionManagerListeners:
    def test_set_connection_listener(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        on_opened = MagicMock()
        on_closed = MagicMock()
        
        cm.set_connection_listener(on_opened, on_closed)
        
        assert cm._on_connection_opened is on_opened
        assert cm._on_connection_closed is on_closed

    def test_set_message_callback(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        callback = MagicMock()
        cm.set_message_callback(callback)
        
        assert cm._message_callback is callback


class TestConnectionManagerGetConnection:
    def test_get_connection_single_member_mode(self):
        cm = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )
        
        conn = MagicMock()
        conn.is_alive = True
        cm._connections = {1: conn}
        
        result = cm.get_connection()
        assert result is conn

    def test_get_connection_single_member_no_alive(self):
        cm = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )
        
        conn = MagicMock()
        conn.is_alive = False
        cm._connections = {1: conn}
        
        result = cm.get_connection()
        assert result is None

    def test_get_connection_uses_load_balancer(self):
        load_balancer = MagicMock()
        conn = MagicMock()
        load_balancer.next.return_value = conn
        
        cm = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.ALL_MEMBERS,
            load_balancer=load_balancer,
        )
        
        result = cm.get_connection()
        
        load_balancer.next.assert_called_once()
        assert result is conn

    def test_get_connection_for_address_exists(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        addr = Address("localhost", 5701)
        conn = MagicMock()
        conn.is_alive = True
        cm._address_connections = {addr: conn}
        
        result = cm.get_connection_for_address(addr)
        assert result is conn

    def test_get_connection_for_address_dead(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        addr = Address("localhost", 5701)
        conn = MagicMock()
        conn.is_alive = False
        cm._address_connections = {addr: conn}
        
        result = cm.get_connection_for_address(addr)
        assert result is None

    def test_get_connection_for_address_not_found(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        addr = Address("localhost", 5701)
        result = cm.get_connection_for_address(addr)
        assert result is None

    def test_get_all_connections(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        conn1 = MagicMock()
        conn1.is_alive = True
        conn2 = MagicMock()
        conn2.is_alive = False
        conn3 = MagicMock()
        conn3.is_alive = True
        
        cm._connections = {1: conn1, 2: conn2, 3: conn3}
        
        result = cm.get_all_connections()
        
        assert len(result) == 2
        assert conn1 in result
        assert conn3 in result
        assert conn2 not in result


class TestConnectionManagerBackoff:
    def test_calculate_backoff_no_config(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        backoff0 = cm._calculate_backoff(0)
        backoff1 = cm._calculate_backoff(1)
        backoff2 = cm._calculate_backoff(2)
        
        assert backoff0 == 1.0
        assert backoff1 == 2.0
        assert backoff2 == 4.0

    def test_calculate_backoff_max_cap(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        backoff = cm._calculate_backoff(10)
        
        assert backoff <= 30.0

    def test_calculate_backoff_with_config(self):
        retry_config = MagicMock()
        retry_config.initial_backoff = 0.5
        retry_config.multiplier = 2.0
        retry_config.max_backoff = 10.0
        retry_config.jitter = 0.0
        
        cm = ConnectionManager(
            addresses=["localhost:5701"],
            retry_config=retry_config,
        )
        
        backoff = cm._calculate_backoff(0)
        assert backoff == 0.5
        
        backoff = cm._calculate_backoff(1)
        assert backoff == 1.0

    def test_calculate_backoff_with_jitter(self):
        retry_config = MagicMock()
        retry_config.initial_backoff = 1.0
        retry_config.multiplier = 1.0
        retry_config.max_backoff = 10.0
        retry_config.jitter = 0.5
        
        cm = ConnectionManager(
            addresses=["localhost:5701"],
            retry_config=retry_config,
        )
        
        backoff = cm._calculate_backoff(0)
        
        assert backoff >= 1.0
        assert backoff <= 1.5


class TestConnectionManagerHeartbeat:
    def test_send_heartbeat(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        conn = MagicMock()
        conn.connection_id = 1
        conn.send_sync = MagicMock()
        
        cm._send_heartbeat(conn)
        
        conn.send_sync.assert_called_once()

    def test_send_heartbeat_failure(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        conn = MagicMock()
        conn.connection_id = 1
        conn.send_sync.side_effect = Exception("Send failed")
        
        cm._send_heartbeat(conn)

    def test_on_heartbeat_failure(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        conn = MagicMock()
        
        with patch("asyncio.create_task") as mock_create_task:
            cm._on_heartbeat_failure(conn, "Heartbeat timeout")
            mock_create_task.assert_called_once()


class TestConnectionManagerAsync:
    @pytest.mark.asyncio
    @patch.object(ConnectionManager, "_connect_to_cluster", new_callable=AsyncMock)
    async def test_start_success(self, mock_connect):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        conn = MagicMock()
        conn.is_alive = True
        cm._connections = {1: conn}
        
        await cm.start()
        
        assert cm.is_running is True
        mock_connect.assert_called_once()

    @pytest.mark.asyncio
    @patch.object(ConnectionManager, "_connect_to_cluster", new_callable=AsyncMock)
    async def test_start_no_connections(self, mock_connect):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        with pytest.raises(ClientOfflineException, match="Could not connect"):
            await cm.start()
        
        assert cm.is_running is False

    @pytest.mark.asyncio
    async def test_start_already_running(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        cm._running = True
        
        await cm.start()

    @pytest.mark.asyncio
    async def test_shutdown(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        cm._running = True
        
        conn = MagicMock()
        conn.close = AsyncMock()
        cm._connections = {1: conn}
        
        await cm.shutdown()
        
        assert cm.is_running is False
        assert len(cm._connections) == 0
        conn.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_cancels_reconnect_task(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        cm._running = True
        
        mock_task = MagicMock()
        mock_task.cancel = MagicMock()
        
        async def mock_await():
            raise asyncio.CancelledError()
        
        mock_task.__await__ = lambda self: mock_await().__await__()
        cm._reconnect_task = mock_task
        
        await cm.shutdown()
        
        mock_task.cancel.assert_called_once()

    @pytest.mark.asyncio
    @patch("hazelcast.network.connection_manager.Connection")
    async def test_connect_to_address_new(self, mock_conn_class):
        cm = ConnectionManager(addresses=["localhost:5701"])
        cm._running = True
        
        mock_conn = MagicMock()
        mock_conn.connect = AsyncMock()
        mock_conn.start_reading = MagicMock()
        mock_conn.is_alive = True
        mock_conn_class.return_value = mock_conn
        
        addr = Address("localhost", 5701)
        result = await cm._connect_to_address(addr)
        
        assert result is mock_conn
        mock_conn.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_connect_to_address_existing(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        addr = Address("localhost", 5701)
        existing_conn = MagicMock()
        existing_conn.is_alive = True
        cm._address_connections = {addr: existing_conn}
        
        result = await cm._connect_to_address(addr)
        
        assert result is existing_conn

    @pytest.mark.asyncio
    async def test_close_connection(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        addr = Address("localhost", 5701)
        conn = MagicMock()
        conn.connection_id = 1
        conn.address = addr
        conn.close = AsyncMock()
        
        cm._connections = {1: conn}
        cm._address_connections = {addr: conn}
        
        await cm._close_connection(conn, "Test reason")
        
        conn.close.assert_called_once_with("Test reason")
        assert 1 not in cm._connections
        assert addr not in cm._address_connections

    @pytest.mark.asyncio
    async def test_close_connection_notifies_listener(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        on_closed = MagicMock()
        cm._on_connection_closed = on_closed
        
        addr = Address("localhost", 5701)
        conn = MagicMock()
        conn.connection_id = 1
        conn.address = addr
        conn.close = AsyncMock()
        
        cm._connections = {1: conn}
        cm._address_connections = {addr: conn}
        
        await cm._close_connection(conn, "Test reason")
        
        on_closed.assert_called_once_with(conn, "Test reason")


class TestConnectionManagerReconnect:
    @pytest.mark.asyncio
    async def test_try_reconnect_single_member_has_connection(self):
        cm = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )
        cm._running = True
        
        conn = MagicMock()
        conn.is_alive = True
        cm._connections = {1: conn}
        
        await cm._try_reconnect()

    @pytest.mark.asyncio
    @patch.object(ConnectionManager, "_connect_to_address", new_callable=AsyncMock)
    async def test_try_reconnect_processes_pending(self, mock_connect):
        cm = ConnectionManager(addresses=["localhost:5701"])
        cm._running = True
        
        addr = Address("localhost", 5701)
        cm._pending_addresses = [addr]
        
        mock_connect.return_value = MagicMock()
        
        await cm._try_reconnect()
        
        mock_connect.assert_called()

    def test_on_connection_closed_external(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        conn = MagicMock()
        
        with patch("asyncio.create_task") as mock_create_task:
            cm.on_connection_closed(conn, "External close")
            mock_create_task.assert_called_once()


class TestConnectionManagerConnectToCluster:
    @pytest.mark.asyncio
    @patch.object(ConnectionManager, "_connect_to_address", new_callable=AsyncMock)
    async def test_connect_single_member_success(self, mock_connect):
        cm = ConnectionManager(
            addresses=["host1:5701", "host2:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )
        
        mock_connect.return_value = MagicMock()
        
        await cm._connect_to_cluster()
        
        assert mock_connect.call_count == 1

    @pytest.mark.asyncio
    @patch.object(ConnectionManager, "_connect_to_address", new_callable=AsyncMock)
    async def test_connect_single_member_fallback(self, mock_connect):
        cm = ConnectionManager(
            addresses=["host1:5701", "host2:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )
        
        mock_connect.side_effect = [Exception("Failed"), MagicMock()]
        
        await cm._connect_to_cluster()
        
        assert mock_connect.call_count == 2

    @pytest.mark.asyncio
    @patch.object(ConnectionManager, "_connect_to_address", new_callable=AsyncMock)
    async def test_connect_all_members(self, mock_connect):
        cm = ConnectionManager(
            addresses=["host1:5701", "host2:5701"],
            routing_mode=RoutingMode.ALL_MEMBERS,
        )
        
        mock_connect.return_value = MagicMock()
        
        await cm._connect_to_cluster()
        
        assert mock_connect.call_count >= 1

    @pytest.mark.asyncio
    @patch.object(ConnectionManager, "_connect_to_address", new_callable=AsyncMock)
    async def test_connect_tracks_failed_addresses(self, mock_connect):
        cm = ConnectionManager(
            addresses=["host1:5701"],
            routing_mode=RoutingMode.ALL_MEMBERS,
        )
        
        mock_connect.side_effect = Exception("Connection failed")
        
        await cm._connect_to_cluster()
        
        assert len(cm._pending_addresses) > 0
