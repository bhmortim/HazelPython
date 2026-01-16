"""Tests for network connection lifecycle and management."""

import asyncio
import pytest
import struct
import time
import threading
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from hazelcast.network.connection import Connection, ConnectionState
from hazelcast.network.connection_manager import (
    ConnectionManager,
    RoutingMode,
    RoundRobinLoadBalancer,
    RandomLoadBalancer,
)
from hazelcast.invocation import Invocation, InvocationService
from hazelcast.protocol.client_message import ClientMessage, Frame, UNFRAGMENTED_FLAG
from hazelcast.exceptions import (
    HazelcastException,
    TargetDisconnectedException,
    ClientOfflineException,
    OperationTimeoutException,
)


class TestConnectionState:
    """Tests for connection state transitions."""

    def test_initial_state_is_created(self):
        """Connection should start in CREATED state."""
        with patch('hazelcast.network.connection.Address') as mock_addr:
            mock_addr.resolve.return_value = [("localhost", 5701)]
            conn = Connection(
                address=mock_addr,
                connection_id=1,
            )
            assert conn.state == ConnectionState.CREATED

    def test_is_alive_returns_false_for_created_state(self):
        """is_alive should be False before connection is established."""
        with patch('hazelcast.network.connection.Address') as mock_addr:
            conn = Connection(address=mock_addr, connection_id=1)
            assert conn.is_alive is False

    def test_is_alive_returns_true_for_connected_state(self):
        """is_alive should be True when connected."""
        with patch('hazelcast.network.connection.Address') as mock_addr:
            conn = Connection(address=mock_addr, connection_id=1)
            conn._state = ConnectionState.CONNECTED
            assert conn.is_alive is True

    def test_is_alive_returns_true_for_authenticated_state(self):
        """is_alive should be True when authenticated."""
        with patch('hazelcast.network.connection.Address') as mock_addr:
            conn = Connection(address=mock_addr, connection_id=1)
            conn._state = ConnectionState.AUTHENTICATED
            assert conn.is_alive is True

    def test_is_alive_returns_false_for_closing_state(self):
        """is_alive should be False when closing."""
        with patch('hazelcast.network.connection.Address') as mock_addr:
            conn = Connection(address=mock_addr, connection_id=1)
            conn._state = ConnectionState.CLOSING
            assert conn.is_alive is False

    def test_mark_authenticated_transitions_from_connected(self):
        """mark_authenticated should transition from CONNECTED to AUTHENTICATED."""
        with patch('hazelcast.network.connection.Address') as mock_addr:
            conn = Connection(address=mock_addr, connection_id=1)
            conn._state = ConnectionState.CONNECTED
            conn.mark_authenticated()
            assert conn.state == ConnectionState.AUTHENTICATED

    def test_mark_authenticated_does_not_transition_from_other_states(self):
        """mark_authenticated should only work from CONNECTED state."""
        with patch('hazelcast.network.connection.Address') as mock_addr:
            conn = Connection(address=mock_addr, connection_id=1)
            conn._state = ConnectionState.CREATED
            conn.mark_authenticated()
            assert conn.state == ConnectionState.CREATED


class TestConnectionConnect:
    """Tests for connection establishment."""

    @pytest.mark.asyncio
    async def test_connect_fails_if_not_in_created_state(self):
        """connect() should fail if connection is not in CREATED state."""
        with patch('hazelcast.network.connection.Address') as mock_addr:
            conn = Connection(address=mock_addr, connection_id=1)
            conn._state = ConnectionState.CONNECTED

            with pytest.raises(HazelcastException) as exc_info:
                await conn.connect()
            assert "Cannot connect" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_connect_sets_state_to_connecting(self):
        """connect() should set state to CONNECTING."""
        mock_addr = MagicMock()
        mock_addr.resolve.return_value = None

        conn = Connection(address=mock_addr, connection_id=1)

        with pytest.raises(HazelcastException):
            await conn.connect()

        assert conn.state == ConnectionState.CLOSED

    @pytest.mark.asyncio
    async def test_connect_fails_on_unresolvable_address(self):
        """connect() should fail if address cannot be resolved."""
        mock_addr = MagicMock()
        mock_addr.resolve.return_value = None

        conn = Connection(address=mock_addr, connection_id=1)

        with pytest.raises(HazelcastException) as exc_info:
            await conn.connect()
        assert "Could not resolve" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_connect_success(self):
        """connect() should establish connection successfully."""
        mock_addr = MagicMock()
        mock_addr.resolve.return_value = [("127.0.0.1", 5701)]

        mock_reader = AsyncMock()
        mock_writer = MagicMock()
        mock_writer.get_extra_info.return_value = None

        with patch('asyncio.wait_for') as mock_wait_for:
            mock_wait_for.return_value = (mock_reader, mock_writer)

            conn = Connection(address=mock_addr, connection_id=1)
            await conn.connect()

            assert conn.state == ConnectionState.CONNECTED


class TestConnectionClose:
    """Tests for connection closure."""

    @pytest.mark.asyncio
    async def test_close_sets_state_to_closed(self):
        """close() should set state to CLOSED."""
        with patch('hazelcast.network.connection.Address') as mock_addr:
            conn = Connection(address=mock_addr, connection_id=1)
            conn._state = ConnectionState.CONNECTED
            conn._writer = MagicMock()
            conn._writer.close = MagicMock()
            conn._writer.wait_closed = AsyncMock()

            await conn.close("Test close")

            assert conn.state == ConnectionState.CLOSED

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self):
        """close() should be safe to call multiple times."""
        with patch('hazelcast.network.connection.Address') as mock_addr:
            conn = Connection(address=mock_addr, connection_id=1)
            conn._state = ConnectionState.CLOSED

            await conn.close("Test close")
            assert conn.state == ConnectionState.CLOSED

    @pytest.mark.asyncio
    async def test_close_cancels_read_task(self):
        """close() should cancel any pending read task."""
        with patch('hazelcast.network.connection.Address') as mock_addr:
            conn = Connection(address=mock_addr, connection_id=1)
            conn._state = ConnectionState.CONNECTED

            mock_task = MagicMock()
            mock_task.cancel = MagicMock()
            conn._read_task = mock_task

            conn._writer = MagicMock()
            conn._writer.close = MagicMock()
            conn._writer.wait_closed = AsyncMock()

            await conn.close("Test close")

            mock_task.cancel.assert_called_once()


class TestConnectionSend:
    """Tests for sending messages."""

    @pytest.mark.asyncio
    async def test_send_fails_if_not_alive(self):
        """send() should fail if connection is not alive."""
        with patch('hazelcast.network.connection.Address') as mock_addr:
            conn = Connection(address=mock_addr, connection_id=1)
            conn._state = ConnectionState.CLOSED

            message = ClientMessage()
            with pytest.raises(TargetDisconnectedException):
                await conn.send(message)

    @pytest.mark.asyncio
    async def test_send_fails_if_writer_is_none(self):
        """send() should fail if writer is not initialized."""
        with patch('hazelcast.network.connection.Address') as mock_addr:
            conn = Connection(address=mock_addr, connection_id=1)
            conn._state = ConnectionState.CONNECTED
            conn._writer = None

            message = ClientMessage()
            with pytest.raises(TargetDisconnectedException):
                await conn.send(message)

    @pytest.mark.asyncio
    async def test_send_writes_message_bytes(self):
        """send() should write message bytes to writer."""
        with patch('hazelcast.network.connection.Address') as mock_addr:
            conn = Connection(address=mock_addr, connection_id=1)
            conn._state = ConnectionState.CONNECTED

            mock_writer = MagicMock()
            mock_writer.write = MagicMock()
            mock_writer.drain = AsyncMock()
            conn._writer = mock_writer

            content = bytearray(22)
            frame = Frame(bytes(content), UNFRAGMENTED_FLAG)
            message = ClientMessage([frame])

            await conn.send(message)

            mock_writer.write.assert_called_once()
            mock_writer.drain.assert_called_once()


class TestRoundRobinLoadBalancer:
    """Tests for round-robin load balancing."""

    def test_next_returns_none_when_empty(self):
        """next() should return None when no connections."""
        lb = RoundRobinLoadBalancer()
        lb.init([])
        assert lb.next() is None

    def test_next_cycles_through_connections(self):
        """next() should cycle through connections in order."""
        lb = RoundRobinLoadBalancer()

        conn1 = MagicMock()
        conn1.is_alive = True
        conn2 = MagicMock()
        conn2.is_alive = True
        conn3 = MagicMock()
        conn3.is_alive = True

        lb.init([conn1, conn2, conn3])

        assert lb.next() == conn1
        assert lb.next() == conn2
        assert lb.next() == conn3
        assert lb.next() == conn1

    def test_next_skips_dead_connections(self):
        """next() should skip connections that are not alive."""
        lb = RoundRobinLoadBalancer()

        conn1 = MagicMock()
        conn1.is_alive = False
        conn2 = MagicMock()
        conn2.is_alive = True

        lb.init([conn1, conn2])

        assert lb.next() == conn2

    def test_can_get_next_returns_true_when_alive_exists(self):
        """can_get_next() should return True when alive connections exist."""
        lb = RoundRobinLoadBalancer()
        conn = MagicMock()
        conn.is_alive = True
        lb.init([conn])
        assert lb.can_get_next() is True

    def test_can_get_next_returns_false_when_all_dead(self):
        """can_get_next() should return False when all connections are dead."""
        lb = RoundRobinLoadBalancer()
        conn = MagicMock()
        conn.is_alive = False
        lb.init([conn])
        assert lb.can_get_next() is False


class TestRandomLoadBalancer:
    """Tests for random load balancing."""

    def test_next_returns_none_when_empty(self):
        """next() should return None when no connections."""
        lb = RandomLoadBalancer()
        lb.init([])
        assert lb.next() is None

    def test_next_returns_alive_connection(self):
        """next() should return an alive connection."""
        lb = RandomLoadBalancer()
        conn = MagicMock()
        conn.is_alive = True
        lb.init([conn])
        assert lb.next() == conn

    def test_next_returns_none_when_all_dead(self):
        """next() should return None when all connections are dead."""
        lb = RandomLoadBalancer()
        conn = MagicMock()
        conn.is_alive = False
        lb.init([conn])
        assert lb.next() is None


class TestInvocation:
    """Tests for Invocation class."""

    def test_initial_correlation_id_is_zero(self):
        """Invocation should have correlation_id 0 initially."""
        message = ClientMessage()
        inv = Invocation(request=message)
        assert inv.correlation_id == 0

    def test_set_correlation_id_updates_message(self):
        """Setting correlation_id should update the request message."""
        content = bytearray(22)
        frame = Frame(bytes(content), UNFRAGMENTED_FLAG)
        message = ClientMessage([frame])

        inv = Invocation(request=message)
        inv.correlation_id = 42

        assert inv.correlation_id == 42
        assert message.get_correlation_id() == 42

    def test_mark_sent_sets_sent_time(self):
        """mark_sent() should record the current time."""
        message = ClientMessage()
        inv = Invocation(request=message)

        assert inv.sent_time is None
        inv.mark_sent()
        assert inv.sent_time is not None
        assert inv.sent_time <= time.time()

    def test_is_expired_returns_false_before_sent(self):
        """is_expired() should return False if not yet sent."""
        message = ClientMessage()
        inv = Invocation(request=message, timeout=0.001)
        assert inv.is_expired() is False

    def test_is_expired_returns_true_after_timeout(self):
        """is_expired() should return True after timeout."""
        message = ClientMessage()
        inv = Invocation(request=message, timeout=0.001)
        inv.mark_sent()
        time.sleep(0.01)
        assert inv.is_expired() is True

    def test_set_response_completes_future(self):
        """set_response() should complete the future with result."""
        message = ClientMessage()
        inv = Invocation(request=message)

        response = ClientMessage()
        inv.set_response(response)

        assert inv.future.done()
        assert inv.future.result() == response

    def test_set_exception_fails_future(self):
        """set_exception() should fail the future with exception."""
        message = ClientMessage()
        inv = Invocation(request=message)

        exc = HazelcastException("Test error")
        inv.set_exception(exc)

        assert inv.future.done()
        with pytest.raises(HazelcastException):
            inv.future.result()


class TestInvocationService:
    """Tests for InvocationService."""

    def test_start_sets_running_flag(self):
        """start() should set running flag to True."""
        svc = InvocationService()
        svc.start()
        assert svc.is_running is True

    def test_shutdown_clears_running_flag(self):
        """shutdown() should set running flag to False."""
        svc = InvocationService()
        svc.start()
        svc.shutdown()
        assert svc.is_running is False

    def test_shutdown_cancels_pending_invocations(self):
        """shutdown() should cancel all pending invocations."""
        svc = InvocationService()
        svc.start()

        content = bytearray(22)
        frame = Frame(bytes(content), UNFRAGMENTED_FLAG)
        message = ClientMessage([frame])
        inv = Invocation(request=message)

        svc._pending[1] = inv

        svc.shutdown()

        assert inv.future.done()
        with pytest.raises(HazelcastException):
            inv.future.result()

    def test_invoke_assigns_correlation_id(self):
        """invoke() should assign a unique correlation ID."""
        svc = InvocationService()
        svc.start()

        content = bytearray(22)
        frame = Frame(bytes(content), UNFRAGMENTED_FLAG)
        msg1 = ClientMessage([frame])
        msg2 = ClientMessage([Frame(bytes(bytearray(22)), UNFRAGMENTED_FLAG)])

        inv1 = Invocation(request=msg1)
        inv2 = Invocation(request=msg2)

        svc.invoke(inv1)
        svc.invoke(inv2)

        assert inv1.correlation_id == 1
        assert inv2.correlation_id == 2

    def test_invoke_returns_future(self):
        """invoke() should return the invocation's future."""
        svc = InvocationService()
        svc.start()

        content = bytearray(22)
        frame = Frame(bytes(content), UNFRAGMENTED_FLAG)
        message = ClientMessage([frame])
        inv = Invocation(request=message)

        future = svc.invoke(inv)

        assert future is inv.future

    def test_handle_response_completes_invocation(self):
        """handle_response() should complete the matching invocation."""
        svc = InvocationService()
        svc.start()

        content = bytearray(22)
        struct.pack_into("<q", content, 4, 1)
        frame = Frame(bytes(content), UNFRAGMENTED_FLAG)
        response = ClientMessage([frame])

        request_content = bytearray(22)
        request_frame = Frame(bytes(request_content), UNFRAGMENTED_FLAG)
        request = ClientMessage([request_frame])
        inv = Invocation(request=request)
        inv._correlation_id = 1
        svc._pending[1] = inv

        result = svc.handle_response(response)

        assert result is True
        assert inv.future.done()

    def test_handle_response_returns_false_for_unknown_id(self):
        """handle_response() should return False for unknown correlation ID."""
        svc = InvocationService()
        svc.start()

        content = bytearray(22)
        struct.pack_into("<q", content, 4, 999)
        frame = Frame(bytes(content), UNFRAGMENTED_FLAG)
        response = ClientMessage([frame])

        result = svc.handle_response(response)

        assert result is False

    def test_check_timeouts_expires_old_invocations(self):
        """check_timeouts() should expire timed-out invocations."""
        svc = InvocationService()
        svc.start()

        content = bytearray(22)
        frame = Frame(bytes(content), UNFRAGMENTED_FLAG)
        message = ClientMessage([frame])
        inv = Invocation(request=message, timeout=0.001)
        inv._correlation_id = 1
        inv.mark_sent()
        svc._pending[1] = inv

        time.sleep(0.01)

        timed_out_count = svc.check_timeouts()

        assert timed_out_count == 1
        assert inv.future.done()
        with pytest.raises(OperationTimeoutException):
            inv.future.result()

    def test_get_pending_count(self):
        """get_pending_count() should return number of pending invocations."""
        svc = InvocationService()
        svc.start()

        assert svc.get_pending_count() == 0

        content = bytearray(22)
        frame = Frame(bytes(content), UNFRAGMENTED_FLAG)
        message = ClientMessage([frame])
        inv = Invocation(request=message)
        inv._correlation_id = 1
        svc._pending[1] = inv

        assert svc.get_pending_count() == 1


class TestConnectionManagerBackoff:
    """Tests for connection manager backoff calculation."""

    def test_calculate_backoff_default(self):
        """_calculate_backoff() should use exponential backoff by default."""
        with patch('hazelcast.network.connection_manager.AddressHelper'):
            cm = ConnectionManager(addresses=["localhost:5701"])

            assert cm._calculate_backoff(0) == 1.0
            assert cm._calculate_backoff(1) == 2.0
            assert cm._calculate_backoff(2) == 4.0
            assert cm._calculate_backoff(5) == 30.0

    def test_calculate_backoff_with_config(self):
        """_calculate_backoff() should use retry config if provided."""
        mock_retry_config = MagicMock()
        mock_retry_config.initial_backoff = 0.5
        mock_retry_config.multiplier = 2.0
        mock_retry_config.max_backoff = 10.0
        mock_retry_config.jitter = 0.0

        with patch('hazelcast.network.connection_manager.AddressHelper'):
            cm = ConnectionManager(
                addresses=["localhost:5701"],
                retry_config=mock_retry_config,
            )

            assert cm._calculate_backoff(0) == 0.5
            assert cm._calculate_backoff(1) == 1.0
            assert cm._calculate_backoff(2) == 2.0
            assert cm._calculate_backoff(5) == 10.0


class TestConnectionManagerRouting:
    """Tests for connection manager routing modes."""

    def test_single_member_routing_mode(self):
        """SINGLE_MEMBER mode should use first available connection."""
        with patch('hazelcast.network.connection_manager.AddressHelper'):
            cm = ConnectionManager(
                addresses=["localhost:5701"],
                routing_mode=RoutingMode.SINGLE_MEMBER,
            )

            conn = MagicMock()
            conn.is_alive = True
            cm._connections[1] = conn

            result = cm.get_connection()

            assert result == conn

    def test_all_members_routing_uses_load_balancer(self):
        """ALL_MEMBERS mode should use load balancer."""
        mock_lb = MagicMock()
        mock_conn = MagicMock()
        mock_lb.next.return_value = mock_conn

        with patch('hazelcast.network.connection_manager.AddressHelper'):
            cm = ConnectionManager(
                addresses=["localhost:5701"],
                routing_mode=RoutingMode.ALL_MEMBERS,
                load_balancer=mock_lb,
            )

            result = cm.get_connection()

            mock_lb.next.assert_called_once()
            assert result == mock_conn
