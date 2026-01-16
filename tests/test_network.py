"""Unit tests for hazelcast.network module."""

import asyncio
import pytest
from unittest.mock import Mock, MagicMock, AsyncMock, patch, PropertyMock

from hazelcast.network.connection import Connection, ConnectionState
from hazelcast.network.connection_manager import (
    ConnectionManager,
    RoutingMode,
    RoundRobinLoadBalancer,
    RandomLoadBalancer,
)
from hazelcast.exceptions import (
    HazelcastException,
    TargetDisconnectedException,
    ClientOfflineException,
)


class TestConnectionState:
    """Tests for ConnectionState enum."""

    def test_all_states_defined(self):
        expected = ["CREATED", "CONNECTING", "CONNECTED", "AUTHENTICATED", "CLOSING", "CLOSED"]
        for state_name in expected:
            assert hasattr(ConnectionState, state_name)


class TestConnection:
    """Tests for Connection class."""

    def setup_method(self):
        self.address = Mock()
        self.address.resolve.return_value = [("127.0.0.1", 5701)]
        
    def test_initial_state(self):
        conn = Connection(
            address=self.address,
            connection_id=1,
            connection_timeout=5.0,
        )
        assert conn.state == ConnectionState.CREATED
        assert conn.connection_id == 1
        assert conn.address is self.address
        assert not conn.is_alive

    def test_is_alive_states(self):
        conn = Connection(address=self.address, connection_id=1)
        
        conn._state = ConnectionState.CREATED
        assert not conn.is_alive
        
        conn._state = ConnectionState.CONNECTING
        assert not conn.is_alive
        
        conn._state = ConnectionState.CONNECTED
        assert conn.is_alive
        
        conn._state = ConnectionState.AUTHENTICATED
        assert conn.is_alive
        
        conn._state = ConnectionState.CLOSING
        assert not conn.is_alive
        
        conn._state = ConnectionState.CLOSED
        assert not conn.is_alive

    def test_member_uuid_property(self):
        conn = Connection(address=self.address, connection_id=1)
        assert conn.member_uuid is None
        
        conn.member_uuid = "test-uuid"
        assert conn.member_uuid == "test-uuid"

    def test_mark_authenticated(self):
        conn = Connection(address=self.address, connection_id=1)
        conn._state = ConnectionState.CONNECTED
        
        conn.mark_authenticated()
        
        assert conn.state == ConnectionState.AUTHENTICATED

    def test_mark_authenticated_wrong_state(self):
        conn = Connection(address=self.address, connection_id=1)
        conn._state = ConnectionState.CREATED
        
        conn.mark_authenticated()
        
        assert conn.state == ConnectionState.CREATED

    def test_str_repr(self):
        conn = Connection(address=self.address, connection_id=42)
        s = str(conn)
        assert "42" in s
        assert "CREATED" in s

    @pytest.mark.asyncio
    async def test_connect_wrong_initial_state(self):
        conn = Connection(address=self.address, connection_id=1)
        conn._state = ConnectionState.CONNECTED
        
        with pytest.raises(HazelcastException) as exc_info:
            await conn.connect()
        assert "Cannot connect" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_connect_unresolvable_address(self):
        self.address.resolve.return_value = []
        conn = Connection(address=self.address, connection_id=1)
        
        with pytest.raises(HazelcastException) as exc_info:
            await conn.connect()
        assert "Could not resolve" in str(exc_info.value)
        assert conn.state == ConnectionState.CLOSED

    @pytest.mark.asyncio
    async def test_connect_timeout(self):
        conn = Connection(
            address=self.address,
            connection_id=1,
            connection_timeout=0.001,
        )
        
        async def slow_connect(*args, **kwargs):
            await asyncio.sleep(10)
            return Mock(), Mock()
        
        with patch("asyncio.open_connection", side_effect=slow_connect):
            with pytest.raises(HazelcastException) as exc_info:
                await conn.connect()
            assert "timeout" in str(exc_info.value).lower()
            assert conn.state == ConnectionState.CLOSED

    @pytest.mark.asyncio
    async def test_connect_failure(self):
        conn = Connection(address=self.address, connection_id=1)
        
        with patch("asyncio.open_connection", side_effect=ConnectionRefusedError("refused")):
            with pytest.raises(HazelcastException) as exc_info:
                await conn.connect()
            assert conn.state == ConnectionState.CLOSED

    @pytest.mark.asyncio
    async def test_connect_success(self):
        conn = Connection(address=self.address, connection_id=1)
        
        mock_reader = AsyncMock()
        mock_writer = Mock()
        mock_socket = Mock()
        mock_socket.getpeername.return_value = ("127.0.0.1", 5701)
        mock_socket.getsockname.return_value = ("127.0.0.1", 12345)
        mock_writer.get_extra_info.return_value = mock_socket
        
        with patch("asyncio.open_connection", return_value=(mock_reader, mock_writer)):
            with patch("asyncio.wait_for", return_value=(mock_reader, mock_writer)):
                await conn.connect()
        
        assert conn.state == ConnectionState.CONNECTED
        assert conn.remote_address == ("127.0.0.1", 5701)
        assert conn.local_address == ("127.0.0.1", 12345)

    @pytest.mark.asyncio
    async def test_send_not_alive(self):
        conn = Connection(address=self.address, connection_id=1)
        conn._state = ConnectionState.CLOSED
        
        message = Mock()
        with pytest.raises(TargetDisconnectedException):
            await conn.send(message)

    @pytest.mark.asyncio
    async def test_send_no_writer(self):
        conn = Connection(address=self.address, connection_id=1)
        conn._state = ConnectionState.CONNECTED
        conn._writer = None
        
        message = Mock()
        with pytest.raises(TargetDisconnectedException):
            await conn.send(message)

    @pytest.mark.asyncio
    async def test_close_idempotent(self):
        conn = Connection(address=self.address, connection_id=1)
        conn._state = ConnectionState.CLOSED
        
        await conn.close("test reason")
        
        assert conn.state == ConnectionState.CLOSED

    @pytest.mark.asyncio
    async def test_close_with_read_task(self):
        conn = Connection(address=self.address, connection_id=1)
        conn._state = ConnectionState.CONNECTED
        
        mock_task = AsyncMock()
        conn._read_task = mock_task
        
        mock_writer = Mock()
        mock_writer.close = Mock()
        mock_writer.wait_closed = AsyncMock()
        conn._writer = mock_writer
        
        await conn.close("test")
        
        assert conn.state == ConnectionState.CLOSED
        assert conn._read_task is None


class TestRoundRobinLoadBalancer:
    """Tests for RoundRobinLoadBalancer."""

    def test_init_empty(self):
        lb = RoundRobinLoadBalancer()
        lb.init([])
        
        assert lb.next() is None
        assert not lb.can_get_next()

    def test_single_connection(self):
        lb = RoundRobinLoadBalancer()
        
        conn = Mock()
        conn.is_alive = True
        lb.init([conn])
        
        assert lb.next() is conn
        assert lb.can_get_next()

    def test_round_robin_distribution(self):
        lb = RoundRobinLoadBalancer()
        
        conns = [Mock(is_alive=True) for _ in range(3)]
        lb.init(conns)
        
        results = [lb.next() for _ in range(6)]
        
        for conn in conns:
            assert results.count(conn) == 2

    def test_skips_dead_connections(self):
        lb = RoundRobinLoadBalancer()
        
        alive_conn = Mock(is_alive=True)
        dead_conn = Mock(is_alive=False)
        lb.init([alive_conn, dead_conn])
        
        for _ in range(5):
            assert lb.next() is alive_conn

    def test_can_get_next_all_dead(self):
        lb = RoundRobinLoadBalancer()
        
        dead_conns = [Mock(is_alive=False) for _ in range(3)]
        lb.init(dead_conns)
        
        assert not lb.can_get_next()
        assert lb.next() is None


class TestRandomLoadBalancer:
    """Tests for RandomLoadBalancer."""

    def test_init_empty(self):
        lb = RandomLoadBalancer()
        lb.init([])
        
        assert lb.next() is None
        assert not lb.can_get_next()

    def test_single_connection(self):
        lb = RandomLoadBalancer()
        
        conn = Mock(is_alive=True)
        lb.init([conn])
        
        assert lb.next() is conn
        assert lb.can_get_next()

    def test_skips_dead_connections(self):
        lb = RandomLoadBalancer()
        
        alive_conn = Mock(is_alive=True)
        dead_conn = Mock(is_alive=False)
        lb.init([alive_conn, dead_conn])
        
        for _ in range(10):
            assert lb.next() is alive_conn

    def test_can_get_next_all_dead(self):
        lb = RandomLoadBalancer()
        
        dead_conns = [Mock(is_alive=False) for _ in range(3)]
        lb.init(dead_conns)
        
        assert not lb.can_get_next()

    def test_random_distribution(self):
        lb = RandomLoadBalancer()
        
        conns = [Mock(is_alive=True) for _ in range(10)]
        lb.init(conns)
        
        results = set()
        for _ in range(100):
            results.add(lb.next())
        
        assert len(results) > 1


class TestConnectionManager:
    """Tests for ConnectionManager."""

    def test_routing_mode_property(self):
        cm = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.ALL_MEMBERS,
        )
        assert cm.routing_mode == RoutingMode.ALL_MEMBERS

    def test_initial_state(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        assert not cm.is_running
        assert cm.connection_count == 0

    def test_set_connection_listener(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        on_opened = Mock()
        on_closed = Mock()
        cm.set_connection_listener(on_opened, on_closed)
        
        assert cm._on_connection_opened is on_opened
        assert cm._on_connection_closed is on_closed

    def test_set_message_callback(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        callback = Mock()
        cm.set_message_callback(callback)
        
        assert cm._message_callback is callback

    def test_get_connection_single_member_mode(self):
        cm = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )
        
        conn = Mock(is_alive=True)
        cm._connections = {1: conn}
        
        result = cm.get_connection()
        assert result is conn

    def test_get_connection_single_member_no_alive(self):
        cm = ConnectionManager(
            addresses=["localhost:5701"],
            routing_mode=RoutingMode.SINGLE_MEMBER,
        )
        
        conn = Mock(is_alive=False)
        cm._connections = {1: conn}
        
        result = cm.get_connection()
        assert result is None

    def test_get_all_connections(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        alive = Mock(is_alive=True)
        dead = Mock(is_alive=False)
        cm._connections = {1: alive, 2: dead}
        
        result = cm.get_all_connections()
        assert result == [alive]

    def test_calculate_backoff_no_config(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        backoff_0 = cm._calculate_backoff(0)
        backoff_1 = cm._calculate_backoff(1)
        backoff_2 = cm._calculate_backoff(2)
        
        assert backoff_0 == 1.0
        assert backoff_1 == 2.0
        assert backoff_2 == 4.0

    def test_calculate_backoff_max_cap(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        backoff = cm._calculate_backoff(10)
        assert backoff <= 30.0

    def test_calculate_backoff_with_config(self):
        from hazelcast.config import RetryConfig
        
        retry_config = RetryConfig(
            initial_backoff=0.5,
            max_backoff=10.0,
            multiplier=2.0,
            jitter=0.0,
        )
        cm = ConnectionManager(
            addresses=["localhost:5701"],
            retry_config=retry_config,
        )
        
        backoff_0 = cm._calculate_backoff(0)
        backoff_1 = cm._calculate_backoff(1)
        
        assert backoff_0 == 0.5
        assert backoff_1 == 1.0

    def test_calculate_backoff_with_jitter(self):
        from hazelcast.config import RetryConfig
        
        retry_config = RetryConfig(
            initial_backoff=1.0,
            max_backoff=30.0,
            multiplier=2.0,
            jitter=0.5,
        )
        cm = ConnectionManager(
            addresses=["localhost:5701"],
            retry_config=retry_config,
        )
        
        backoffs = [cm._calculate_backoff(0) for _ in range(100)]
        
        assert min(backoffs) >= 1.0
        assert max(backoffs) <= 1.5

    @pytest.mark.asyncio
    async def test_start_already_running(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        cm._running = True
        
        await cm.start()

    @pytest.mark.asyncio
    async def test_shutdown_when_not_running(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        cm._running = False
        
        await cm.shutdown()
        
        assert not cm.is_running


class TestConnectionManagerEdgeCases:
    """Edge case tests for ConnectionManager."""

    def test_connection_count_thread_safety(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        conns = [Mock(is_alive=True) for _ in range(5)]
        dead_conns = [Mock(is_alive=False) for _ in range(3)]
        
        cm._connections = {i: c for i, c in enumerate(conns + dead_conns)}
        
        assert cm.connection_count == 5

    def test_get_connection_for_missing_address(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        address = Mock()
        result = cm.get_connection_for_address(address)
        
        assert result is None

    def test_get_connection_for_address_dead(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        address = Mock()
        dead_conn = Mock(is_alive=False)
        cm._address_connections = {address: dead_conn}
        
        result = cm.get_connection_for_address(address)
        assert result is None

    def test_get_connection_for_address_alive(self):
        cm = ConnectionManager(addresses=["localhost:5701"])
        
        address = Mock()
        alive_conn = Mock(is_alive=True)
        cm._address_connections = {address: alive_conn}
        
        result = cm.get_connection_for_address(address)
        assert result is alive_conn


class TestRoutingMode:
    """Tests for RoutingMode enum."""

    def test_all_modes_defined(self):
        assert RoutingMode.ALL_MEMBERS.value == "ALL_MEMBERS"
        assert RoutingMode.SINGLE_MEMBER.value == "SINGLE_MEMBER"
        assert RoutingMode.MULTI_MEMBER.value == "MULTI_MEMBER"
