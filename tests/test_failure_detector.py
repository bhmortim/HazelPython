"""Unit tests for hazelcast.network.failure_detector module."""

import asyncio
import socket
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hazelcast.network.failure_detector import (
    FailureDetector,
    HeartbeatHistory,
    HeartbeatManager,
    PhiAccrualFailureDetector,
    PingFailureDetector,
    _calculate_checksum,
    _can_use_raw_sockets,
)


class MockConnection:
    """Mock connection for testing."""

    def __init__(self, connection_id: int, is_alive: bool = True):
        self.connection_id = connection_id
        self._is_alive = is_alive
        self._last_read_time = time.time()
        self._last_write_time = time.time()

    @property
    def is_alive(self) -> bool:
        return self._is_alive

    @property
    def last_read_time(self) -> float:
        return self._last_read_time

    @property
    def last_write_time(self) -> float:
        return self._last_write_time


class TestCalculateChecksum:
    """Tests for _calculate_checksum function."""

    def test_checksum_even_length(self):
        data = b"\x08\x00\x00\x00\x00\x01\x00\x01"
        result = _calculate_checksum(data)
        assert isinstance(result, int)
        assert 0 <= result <= 0xFFFF

    def test_checksum_odd_length(self):
        data = b"\x08\x00\x00\x00\x00\x01\x00"
        result = _calculate_checksum(data)
        assert isinstance(result, int)
        assert 0 <= result <= 0xFFFF

    def test_checksum_empty(self):
        result = _calculate_checksum(b"")
        assert result == 0xFFFF

    def test_checksum_consistency(self):
        data = b"hazelcast-ping"
        result1 = _calculate_checksum(data)
        result2 = _calculate_checksum(data)
        assert result1 == result2


class TestCanUseRawSockets:
    """Tests for _can_use_raw_sockets function."""

    @patch("platform.system")
    def test_windows_returns_true(self, mock_system):
        mock_system.return_value = "Windows"
        assert _can_use_raw_sockets() is True

    @patch("platform.system")
    @patch("os.geteuid")
    def test_root_user_returns_true(self, mock_geteuid, mock_system):
        mock_system.return_value = "Linux"
        mock_geteuid.return_value = 0
        assert _can_use_raw_sockets() is True

    @patch("platform.system")
    @patch("os.geteuid")
    @patch("socket.socket")
    def test_raw_socket_available(self, mock_socket, mock_geteuid, mock_system):
        mock_system.return_value = "Linux"
        mock_geteuid.return_value = 1000
        mock_sock = MagicMock()
        mock_socket.return_value = mock_sock
        assert _can_use_raw_sockets() is True
        mock_sock.close.assert_called()

    @patch("platform.system")
    @patch("os.geteuid")
    @patch("socket.socket")
    def test_no_raw_socket_permission(self, mock_socket, mock_geteuid, mock_system):
        mock_system.return_value = "Linux"
        mock_geteuid.return_value = 1000
        mock_socket.side_effect = PermissionError("No permission")
        assert _can_use_raw_sockets() is False


class TestFailureDetector:
    """Tests for FailureDetector class."""

    def test_init_defaults(self):
        fd = FailureDetector()
        assert fd.heartbeat_interval == 5.0
        assert fd.heartbeat_timeout == 60.0

    def test_init_custom_values(self):
        fd = FailureDetector(heartbeat_interval=10.0, heartbeat_timeout=120.0)
        assert fd.heartbeat_interval == 10.0
        assert fd.heartbeat_timeout == 120.0

    def test_heartbeat_interval_setter(self):
        fd = FailureDetector()
        fd.heartbeat_interval = 15.0
        assert fd.heartbeat_interval == 15.0

    def test_heartbeat_timeout_setter(self):
        fd = FailureDetector()
        fd.heartbeat_timeout = 90.0
        assert fd.heartbeat_timeout == 90.0

    def test_register_connection(self):
        fd = FailureDetector()
        conn = MockConnection(1)
        fd.register_connection(conn)
        assert 1 in fd._last_heartbeat

    def test_unregister_connection(self):
        fd = FailureDetector()
        conn = MockConnection(1)
        fd.register_connection(conn)
        fd.unregister_connection(conn)
        assert 1 not in fd._last_heartbeat

    def test_unregister_nonexistent_connection(self):
        fd = FailureDetector()
        conn = MockConnection(999)
        fd.unregister_connection(conn)

    def test_on_heartbeat_received(self):
        fd = FailureDetector()
        conn = MockConnection(1)
        fd.register_connection(conn)
        old_time = fd._last_heartbeat[1]
        time.sleep(0.01)
        fd.on_heartbeat_received(conn)
        assert fd._last_heartbeat[1] >= old_time

    def test_is_alive_dead_connection(self):
        fd = FailureDetector()
        conn = MockConnection(1, is_alive=False)
        assert fd.is_alive(conn) is False

    def test_is_alive_no_read_time(self):
        fd = FailureDetector()
        conn = MockConnection(1)
        conn._last_read_time = 0
        assert fd.is_alive(conn) is True

    def test_is_alive_within_timeout(self):
        fd = FailureDetector(heartbeat_timeout=60.0)
        conn = MockConnection(1)
        conn._last_read_time = time.time()
        assert fd.is_alive(conn) is True

    def test_is_alive_exceeded_timeout(self):
        fd = FailureDetector(heartbeat_timeout=0.01)
        conn = MockConnection(1)
        conn._last_read_time = time.time() - 1.0
        assert fd.is_alive(conn) is False

    def test_needs_heartbeat_dead_connection(self):
        fd = FailureDetector()
        conn = MockConnection(1, is_alive=False)
        assert fd.needs_heartbeat(conn) is False

    def test_needs_heartbeat_no_write_time(self):
        fd = FailureDetector()
        conn = MockConnection(1)
        conn._last_write_time = 0
        assert fd.needs_heartbeat(conn) is True

    def test_needs_heartbeat_recent_write(self):
        fd = FailureDetector(heartbeat_interval=60.0)
        conn = MockConnection(1)
        conn._last_write_time = time.time()
        assert fd.needs_heartbeat(conn) is False

    def test_needs_heartbeat_interval_elapsed(self):
        fd = FailureDetector(heartbeat_interval=0.01)
        conn = MockConnection(1)
        conn._last_write_time = time.time() - 1.0
        assert fd.needs_heartbeat(conn) is True

    def test_get_suspect_connections_empty(self):
        fd = FailureDetector()
        suspects = fd.get_suspect_connections({})
        assert suspects == []

    def test_get_suspect_connections_all_healthy(self):
        fd = FailureDetector(heartbeat_timeout=60.0)
        conn1 = MockConnection(1)
        conn2 = MockConnection(2)
        connections = {1: conn1, 2: conn2}
        suspects = fd.get_suspect_connections(connections)
        assert suspects == []

    def test_get_suspect_connections_skips_dead(self):
        fd = FailureDetector(heartbeat_timeout=0.01)
        conn = MockConnection(1, is_alive=False)
        conn._last_read_time = time.time() - 100
        connections = {1: conn}
        suspects = fd.get_suspect_connections(connections)
        assert suspects == []

    def test_get_suspect_connections_finds_suspects(self):
        fd = FailureDetector(heartbeat_timeout=0.01)
        conn = MockConnection(1)
        conn._last_read_time = time.time() - 1.0
        connections = {1: conn}
        suspects = fd.get_suspect_connections(connections)
        assert conn in suspects


class TestPingFailureDetector:
    """Tests for PingFailureDetector class."""

    def test_init_defaults(self):
        pfd = PingFailureDetector()
        assert pfd.ping_timeout == 5.0
        assert pfd.ping_interval == 10.0
        assert pfd.max_failures == 3

    def test_init_custom_values(self):
        pfd = PingFailureDetector(
            ping_timeout=2.0, ping_interval=5.0, max_failures=5
        )
        assert pfd.ping_timeout == 2.0
        assert pfd.ping_interval == 5.0
        assert pfd.max_failures == 5

    def test_is_available_property(self):
        pfd = PingFailureDetector()
        assert isinstance(pfd.is_available, bool)

    def test_register_host(self):
        pfd = PingFailureDetector()
        pfd.register_host("192.168.1.1")
        assert pfd._failure_counts["192.168.1.1"] == 0
        assert pfd._last_ping_times["192.168.1.1"] == 0.0

    def test_unregister_host(self):
        pfd = PingFailureDetector()
        pfd.register_host("192.168.1.1")
        pfd.unregister_host("192.168.1.1")
        assert "192.168.1.1" not in pfd._failure_counts
        assert "192.168.1.1" not in pfd._last_ping_times

    def test_unregister_nonexistent_host(self):
        pfd = PingFailureDetector()
        pfd.unregister_host("nonexistent")

    @patch.object(PingFailureDetector, "_can_ping", False)
    def test_needs_ping_when_unavailable(self):
        pfd = PingFailureDetector()
        pfd._can_ping = False
        assert pfd.needs_ping("192.168.1.1") is False

    def test_needs_ping_first_time(self):
        pfd = PingFailureDetector()
        pfd._can_ping = True
        pfd.register_host("192.168.1.1")
        assert pfd.needs_ping("192.168.1.1") is True

    def test_needs_ping_interval_not_elapsed(self):
        pfd = PingFailureDetector(ping_interval=60.0)
        pfd._can_ping = True
        pfd.register_host("192.168.1.1")
        pfd._last_ping_times["192.168.1.1"] = time.time()
        assert pfd.needs_ping("192.168.1.1") is False

    def test_needs_ping_interval_elapsed(self):
        pfd = PingFailureDetector(ping_interval=0.01)
        pfd._can_ping = True
        pfd.register_host("192.168.1.1")
        pfd._last_ping_times["192.168.1.1"] = time.time() - 1.0
        assert pfd.needs_ping("192.168.1.1") is True

    @pytest.mark.asyncio
    async def test_ping_when_unavailable(self):
        pfd = PingFailureDetector()
        pfd._can_ping = False
        success, latency = await pfd.ping("192.168.1.1")
        assert success is False
        assert latency == -1.0

    def test_record_success(self):
        pfd = PingFailureDetector()
        pfd.register_host("192.168.1.1")
        pfd._failure_counts["192.168.1.1"] = 5
        pfd._record_success("192.168.1.1")
        assert pfd._failure_counts["192.168.1.1"] == 0

    def test_record_failure(self):
        pfd = PingFailureDetector()
        pfd.register_host("192.168.1.1")
        pfd._record_failure("192.168.1.1")
        assert pfd._failure_counts["192.168.1.1"] == 1
        pfd._record_failure("192.168.1.1")
        assert pfd._failure_counts["192.168.1.1"] == 2

    def test_is_host_suspect_below_threshold(self):
        pfd = PingFailureDetector(max_failures=3)
        pfd.register_host("192.168.1.1")
        pfd._failure_counts["192.168.1.1"] = 2
        assert pfd.is_host_suspect("192.168.1.1") is False

    def test_is_host_suspect_at_threshold(self):
        pfd = PingFailureDetector(max_failures=3)
        pfd.register_host("192.168.1.1")
        pfd._failure_counts["192.168.1.1"] = 3
        assert pfd.is_host_suspect("192.168.1.1") is True

    def test_is_host_suspect_above_threshold(self):
        pfd = PingFailureDetector(max_failures=3)
        pfd.register_host("192.168.1.1")
        pfd._failure_counts["192.168.1.1"] = 5
        assert pfd.is_host_suspect("192.168.1.1") is True

    def test_get_failure_count(self):
        pfd = PingFailureDetector()
        pfd.register_host("192.168.1.1")
        pfd._failure_counts["192.168.1.1"] = 7
        assert pfd.get_failure_count("192.168.1.1") == 7

    def test_get_failure_count_unknown_host(self):
        pfd = PingFailureDetector()
        assert pfd.get_failure_count("unknown") == 0

    @pytest.mark.asyncio
    async def test_ping_dns_failure(self):
        pfd = PingFailureDetector()
        pfd._can_ping = True
        pfd.register_host("invalid.nonexistent.host.test")

        with patch("socket.gethostbyname", side_effect=socket.gaierror("DNS failed")):
            with patch("socket.socket"):
                success, latency = await pfd.ping("invalid.nonexistent.host.test")
                assert success is False
                assert latency == -1.0


class TestHeartbeatHistory:
    """Tests for HeartbeatHistory class."""

    def test_init_defaults(self):
        hh = HeartbeatHistory()
        assert hh._max_sample_size == 200
        assert not hh.is_empty

    def test_init_custom_max_size(self):
        hh = HeartbeatHistory(max_sample_size=100)
        assert hh._max_sample_size == 100

    def test_init_with_estimate(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=1000.0)
        assert len(hh._intervals) == 2
        assert hh.mean == 1000.0

    def test_init_no_estimate(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0.0)
        assert len(hh._intervals) == 0

    def test_is_empty_after_init_no_estimate(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0.0)
        assert hh.is_empty is True

    def test_is_empty_after_init_with_estimate(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=500.0)
        assert hh.is_empty is False

    def test_last_timestamp_none(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0.0)
        assert hh.last_timestamp is None

    def test_last_timestamp_after_add(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0.0)
        hh.add(1000.0)
        assert hh.last_timestamp == 1000.0

    def test_add_first_heartbeat(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0.0)
        hh.add(1000.0)
        assert hh.last_timestamp == 1000.0
        assert len(hh._intervals) == 0

    def test_add_second_heartbeat(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0.0)
        hh.add(1000.0)
        hh.add(1500.0)
        assert hh.last_timestamp == 1500.0
        assert len(hh._intervals) == 1
        assert hh._intervals[0] == 500.0

    def test_add_negative_interval_ignored(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0.0)
        hh.add(1000.0)
        hh.add(500.0)
        assert len(hh._intervals) == 0

    def test_mean_calculation(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0.0)
        hh.add(0.0)
        hh.add(100.0)
        hh.add(200.0)
        hh.add(300.0)
        assert hh.mean == 100.0

    def test_mean_empty_history(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0.0)
        assert hh.mean == 0.0

    def test_variance_calculation(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0.0)
        hh.add(0.0)
        hh.add(100.0)
        hh.add(200.0)
        hh.add(300.0)
        assert hh.variance == 0.0

    def test_variance_empty_history(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0.0)
        assert hh.variance == 0.0

    def test_std_deviation_calculation(self):
        hh = HeartbeatHistory(first_heartbeat_estimate_ms=0.0)
        hh.add(0.0)
        hh.add(100.0)
        hh.add(200.0)
        hh.add(400.0)
        assert hh.std_deviation >= 0.0

    def test_max_sample_size_eviction(self):
        hh = HeartbeatHistory(max_sample_size=3, first_heartbeat_estimate_ms=0.0)
        hh.add(0.0)
        hh.add(100.0)
        hh.add(200.0)
        hh.add(300.0)
        hh.add(400.0)
        assert len(hh._intervals) == 3


class TestPhiAccrualFailureDetector:
    """Tests for PhiAccrualFailureDetector class."""

    def test_init_defaults(self):
        pafd = PhiAccrualFailureDetector()
        assert pafd.threshold == 8.0
        assert pafd._max_sample_size == 200
        assert pafd._min_std_deviation_ms == 100.0

    def test_init_custom_values(self):
        pafd = PhiAccrualFailureDetector(
            threshold=10.0,
            max_sample_size=100,
            min_std_deviation_ms=50.0,
            acceptable_heartbeat_pause_ms=1000.0,
            first_heartbeat_estimate_ms=250.0,
        )
        assert pafd.threshold == 10.0
        assert pafd._max_sample_size == 100
        assert pafd._min_std_deviation_ms == 50.0

    def test_threshold_setter(self):
        pafd = PhiAccrualFailureDetector()
        pafd.threshold = 12.0
        assert pafd.threshold == 12.0

    def test_register_connection(self):
        pafd = PhiAccrualFailureDetector()
        pafd.register_connection(1)
        assert 1 in pafd._heartbeat_history

    def test_unregister_connection(self):
        pafd = PhiAccrualFailureDetector()
        pafd.register_connection(1)
        pafd.unregister_connection(1)
        assert 1 not in pafd._heartbeat_history

    def test_unregister_nonexistent_connection(self):
        pafd = PhiAccrualFailureDetector()
        pafd.unregister_connection(999)

    def test_heartbeat_unregistered_connection(self):
        pafd = PhiAccrualFailureDetector()
        pafd.heartbeat(999)

    def test_heartbeat_registered_connection(self):
        pafd = PhiAccrualFailureDetector()
        pafd.register_connection(1)
        pafd.heartbeat(1, 1000.0)
        assert pafd._heartbeat_history[1].last_timestamp == 1000.0

    def test_heartbeat_auto_timestamp(self):
        pafd = PhiAccrualFailureDetector()
        pafd.register_connection(1)
        pafd.heartbeat(1)
        assert pafd._heartbeat_history[1].last_timestamp is not None

    def test_phi_unregistered_connection(self):
        pafd = PhiAccrualFailureDetector()
        assert pafd.phi(999) == 0.0

    def test_phi_no_heartbeat(self):
        pafd = PhiAccrualFailureDetector(first_heartbeat_estimate_ms=0.0)
        pafd.register_connection(1)
        pafd._heartbeat_history[1] = HeartbeatHistory(first_heartbeat_estimate_ms=0.0)
        assert pafd.phi(1) == 0.0

    def test_phi_recent_heartbeat(self):
        pafd = PhiAccrualFailureDetector()
        pafd.register_connection(1)
        now = time.time() * 1000
        pafd.heartbeat(1, now - 100)
        pafd.heartbeat(1, now)
        phi = pafd.phi(1, now + 50)
        assert phi >= 0.0

    def test_phi_stale_heartbeat(self):
        pafd = PhiAccrualFailureDetector()
        pafd.register_connection(1)
        now = time.time() * 1000
        pafd.heartbeat(1, now - 10000)
        pafd.heartbeat(1, now - 5000)
        phi = pafd.phi(1, now)
        assert phi > 0.0

    def test_calculate_phi_zero_time_diff(self):
        pafd = PhiAccrualFailureDetector()
        result = pafd._calculate_phi(0.0, 500.0, 100.0)
        assert result == 0.0

    def test_calculate_phi_negative_time_diff(self):
        pafd = PhiAccrualFailureDetector()
        result = pafd._calculate_phi(-100.0, 500.0, 100.0)
        assert result == 0.0

    def test_calculate_phi_above_mean(self):
        pafd = PhiAccrualFailureDetector()
        result = pafd._calculate_phi(1000.0, 500.0, 100.0)
        assert result > 0.0

    def test_calculate_phi_below_mean(self):
        pafd = PhiAccrualFailureDetector()
        result = pafd._calculate_phi(100.0, 500.0, 100.0)
        assert result >= 0.0

    def test_is_available_true(self):
        pafd = PhiAccrualFailureDetector(threshold=8.0)
        pafd.register_connection(1)
        now = time.time() * 1000
        pafd.heartbeat(1, now - 100)
        pafd.heartbeat(1, now)
        assert pafd.is_available(1, now + 50) is True

    def test_is_available_false(self):
        pafd = PhiAccrualFailureDetector(threshold=1.0)
        pafd.register_connection(1)
        now = time.time() * 1000
        pafd.heartbeat(1, now - 10000)
        pafd.heartbeat(1, now - 5000)
        assert pafd.is_available(1, now) is False


class TestHeartbeatManager:
    """Tests for HeartbeatManager class."""

    def test_init(self):
        fd = FailureDetector()
        send_hb = MagicMock()
        on_failed = MagicMock()
        hm = HeartbeatManager(fd, send_hb, on_failed)
        assert hm.failure_detector is fd
        assert not hm._running

    def test_add_connection(self):
        fd = FailureDetector()
        send_hb = MagicMock()
        on_failed = MagicMock()
        hm = HeartbeatManager(fd, send_hb, on_failed)

        conn = MockConnection(1)
        hm.add_connection(conn)
        assert 1 in hm._connections
        assert 1 in fd._last_heartbeat

    def test_remove_connection(self):
        fd = FailureDetector()
        send_hb = MagicMock()
        on_failed = MagicMock()
        hm = HeartbeatManager(fd, send_hb, on_failed)

        conn = MockConnection(1)
        hm.add_connection(conn)
        hm.remove_connection(conn)
        assert 1 not in hm._connections

    @pytest.mark.asyncio
    async def test_start_stop(self):
        fd = FailureDetector(heartbeat_interval=0.01)
        send_hb = MagicMock()
        on_failed = MagicMock()
        hm = HeartbeatManager(fd, send_hb, on_failed)

        hm.start()
        assert hm._running is True
        assert hm._task is not None

        await asyncio.sleep(0.02)

        await hm.stop()
        assert hm._running is False
        assert hm._task is None

    @pytest.mark.asyncio
    async def test_start_idempotent(self):
        fd = FailureDetector(heartbeat_interval=0.1)
        send_hb = MagicMock()
        on_failed = MagicMock()
        hm = HeartbeatManager(fd, send_hb, on_failed)

        hm.start()
        task1 = hm._task
        hm.start()
        task2 = hm._task

        assert task1 is task2

        await hm.stop()

    @pytest.mark.asyncio
    async def test_heartbeat_sent_when_needed(self):
        fd = FailureDetector(heartbeat_interval=0.01, heartbeat_timeout=60.0)
        send_hb = MagicMock()
        on_failed = MagicMock()
        hm = HeartbeatManager(fd, send_hb, on_failed)

        conn = MockConnection(1)
        conn._last_write_time = time.time() - 1.0
        hm.add_connection(conn)

        hm.start()
        await asyncio.sleep(0.05)
        await hm.stop()

        assert send_hb.called

    @pytest.mark.asyncio
    async def test_connection_failed_on_timeout(self):
        fd = FailureDetector(heartbeat_interval=0.01, heartbeat_timeout=0.001)
        send_hb = MagicMock()
        on_failed = MagicMock()
        hm = HeartbeatManager(fd, send_hb, on_failed)

        conn = MockConnection(1)
        conn._last_read_time = time.time() - 1.0
        hm.add_connection(conn)

        hm.start()
        await asyncio.sleep(0.05)
        await hm.stop()

        assert on_failed.called

    @pytest.mark.asyncio
    async def test_dead_connection_skipped(self):
        fd = FailureDetector(heartbeat_interval=0.01)
        send_hb = MagicMock()
        on_failed = MagicMock()
        hm = HeartbeatManager(fd, send_hb, on_failed)

        conn = MockConnection(1, is_alive=False)
        hm.add_connection(conn)

        hm.start()
        await asyncio.sleep(0.05)
        await hm.stop()

        assert not send_hb.called

    @pytest.mark.asyncio
    async def test_send_heartbeat_exception_caught(self):
        fd = FailureDetector(heartbeat_interval=0.01)
        send_hb = MagicMock(side_effect=Exception("Send failed"))
        on_failed = MagicMock()
        hm = HeartbeatManager(fd, send_hb, on_failed)

        conn = MockConnection(1)
        conn._last_write_time = time.time() - 1.0
        hm.add_connection(conn)

        hm.start()
        await asyncio.sleep(0.05)
        await hm.stop()
