"""Unit tests for hazelcast.network.failure_detector module."""

import pytest
import time
import asyncio
from unittest.mock import Mock, MagicMock, AsyncMock, patch

from hazelcast.network.failure_detector import (
    FailureDetector,
    PingFailureDetector,
    PhiAccrualFailureDetector,
    HeartbeatHistory,
    HeartbeatManager,
    _can_use_raw_sockets,
    _calculate_checksum,
)


class TestFailureDetector:
    """Tests for FailureDetector class."""

    def test_default_intervals(self):
        detector = FailureDetector()
        assert detector.heartbeat_interval == 5.0
        assert detector.heartbeat_timeout == 60.0

    def test_custom_intervals(self):
        detector = FailureDetector(
            heartbeat_interval=10.0,
            heartbeat_timeout=120.0,
        )
        assert detector.heartbeat_interval == 10.0
        assert detector.heartbeat_timeout == 120.0

    def test_heartbeat_interval_setter(self):
        detector = FailureDetector()
        detector.heartbeat_interval = 15.0
        assert detector.heartbeat_interval == 15.0

    def test_heartbeat_timeout_setter(self):
        detector = FailureDetector()
        detector.heartbeat_timeout = 90.0
        assert detector.heartbeat_timeout == 90.0

    def test_register_connection(self):
        detector = FailureDetector()
        connection = Mock()
        connection.connection_id = 1
        
        detector.register_connection(connection)
        
        assert 1 in detector._last_heartbeat
        assert detector._last_heartbeat[1] > 0

    def test_unregister_connection(self):
        detector = FailureDetector()
        connection = Mock()
        connection.connection_id = 1
        
        detector.register_connection(connection)
        detector.unregister_connection(connection)
        
        assert 1 not in detector._last_heartbeat

    def test_unregister_connection_not_registered(self):
        detector = FailureDetector()
        connection = Mock()
        connection.connection_id = 999
        
        detector.unregister_connection(connection)

    def test_on_heartbeat_received_updates_timestamp(self):
        detector = FailureDetector()
        connection = Mock()
        connection.connection_id = 1
        
        detector.register_connection(connection)
        old_time = detector._last_heartbeat[1]
        
        time.sleep(0.01)
        detector.on_heartbeat_received(connection)
        
        assert detector._last_heartbeat[1] > old_time

    def test_is_alive_with_alive_connection(self):
        detector = FailureDetector()
        connection = Mock()
        connection.is_alive = True
        connection.last_read_time = time.time()
        
        assert detector.is_alive(connection) is True

    def test_is_alive_with_dead_connection(self):
        detector = FailureDetector()
        connection = Mock()
        connection.is_alive = False
        
        assert detector.is_alive(connection) is False

    def test_is_alive_with_timeout_exceeded(self):
        detector = FailureDetector(heartbeat_timeout=0.01)
        connection = Mock()
        connection.is_alive = True
        connection.last_read_time = time.time() - 1.0
        
        assert detector.is_alive(connection) is False

    def test_is_alive_with_zero_last_read_time(self):
        detector = FailureDetector()
        connection = Mock()
        connection.is_alive = True
        connection.last_read_time = 0
        
        assert detector.is_alive(connection) is True

    def test_needs_heartbeat_when_interval_not_reached(self):
        detector = FailureDetector(heartbeat_interval=100.0)
        connection = Mock()
        connection.is_alive = True
        connection.last_write_time = time.time()
        
        assert detector.needs_heartbeat(connection) is False

    def test_needs_heartbeat_when_interval_exceeded(self):
        detector = FailureDetector(heartbeat_interval=0.01)
        connection = Mock()
        connection.is_alive = True
        connection.last_write_time = time.time() - 1.0
        
        assert detector.needs_heartbeat(connection) is True

    def test_needs_heartbeat_with_dead_connection(self):
        detector = FailureDetector()
        connection = Mock()
        connection.is_alive = False
        
        assert detector.needs_heartbeat(connection) is False

    def test_needs_heartbeat_with_zero_last_write_time(self):
        detector = FailureDetector()
        connection = Mock()
        connection.is_alive = True
        connection.last_write_time = 0
        
        assert detector.needs_heartbeat(connection) is True

    def test_get_suspect_connections_empty_when_healthy(self):
        detector = FailureDetector(heartbeat_timeout=100.0)
        
        conn1 = Mock()
        conn1.is_alive = True
        conn1.last_read_time = time.time()
        
        conn2 = Mock()
        conn2.is_alive = True
        conn2.last_read_time = time.time()
        
        connections = {1: conn1, 2: conn2}
        suspects = detector.get_suspect_connections(connections)
        
        assert suspects == []

    def test_get_suspect_connections_returns_timed_out(self):
        detector = FailureDetector(heartbeat_timeout=0.01)
        
        healthy = Mock()
        healthy.is_alive = True
        healthy.last_read_time = time.time()
        
        suspect = Mock()
        suspect.is_alive = True
        suspect.last_read_time = time.time() - 1.0
        
        connections = {1: healthy, 2: suspect}
        suspects = detector.get_suspect_connections(connections)
        
        assert len(suspects) == 1
        assert suspects[0] is suspect

    def test_get_suspect_connections_skips_dead(self):
        detector = FailureDetector(heartbeat_timeout=0.01)
        
        dead = Mock()
        dead.is_alive = False
        
        connections = {1: dead}
        suspects = detector.get_suspect_connections(connections)
        
        assert suspects == []

    def test_get_suspect_connections_skips_zero_last_read(self):
        detector = FailureDetector(heartbeat_timeout=0.01)
        
        conn = Mock()
        conn.is_alive = True
        conn.last_read_time = 0
        
        connections = {1: conn}
        suspects = detector.get_suspect_connections(connections)
        
        assert suspects == []


class TestPingFailureDetector:
    """Tests for PingFailureDetector class."""

    def test_default_values(self):
        detector = PingFailureDetector()
        assert detector.ping_timeout == 5.0
        assert detector.ping_interval == 10.0
        assert detector.max_failures == 3

    def test_custom_values(self):
        detector = PingFailureDetector(
            ping_timeout=10.0,
            ping_interval=20.0,
            max_failures=5,
        )
        assert detector.ping_timeout == 10.0
        assert detector.ping_interval == 20.0
        assert detector.max_failures == 5

    def test_is_available_property(self):
        detector = PingFailureDetector()
        assert isinstance(detector.is_available, bool)

    def test_register_host(self):
        detector = PingFailureDetector()
        detector.register_host("192.168.1.1")
        
        assert "192.168.1.1" in detector._failure_counts
        assert detector._failure_counts["192.168.1.1"] == 0
        assert detector._last_ping_times["192.168.1.1"] == 0.0

    def test_unregister_host(self):
        detector = PingFailureDetector()
        detector.register_host("192.168.1.1")
        detector.unregister_host("192.168.1.1")
        
        assert "192.168.1.1" not in detector._failure_counts
        assert "192.168.1.1" not in detector._last_ping_times

    def test_unregister_host_not_registered(self):
        detector = PingFailureDetector()
        detector.unregister_host("192.168.1.1")

    def test_needs_ping_when_not_available(self):
        detector = PingFailureDetector()
        detector._can_ping = False
        detector.register_host("192.168.1.1")
        
        assert detector.needs_ping("192.168.1.1") is False

    def test_needs_ping_when_never_pinged(self):
        detector = PingFailureDetector()
        detector._can_ping = True
        detector.register_host("192.168.1.1")
        
        assert detector.needs_ping("192.168.1.1") is True

    def test_needs_ping_when_interval_not_reached(self):
        detector = PingFailureDetector(ping_interval=100.0)
        detector._can_ping = True
        detector.register_host("192.168.1.1")
        detector._last_ping_times["192.168.1.1"] = time.time()
        
        assert detector.needs_ping("192.168.1.1") is False

    def test_needs_ping_when_interval_exceeded(self):
        detector = PingFailureDetector(ping_interval=0.01)
        detector._can_ping = True
        detector.register_host("192.168.1.1")
        detector._last_ping_times["192.168.1.1"] = time.time() - 1.0
        
        assert detector.needs_ping("192.168.1.1") is True

    def test_is_host_suspect_below_max_failures(self):
        detector = PingFailureDetector(max_failures=3)
        detector.register_host("192.168.1.1")
        detector._failure_counts["192.168.1.1"] = 2
        
        assert detector.is_host_suspect("192.168.1.1") is False

    def test_is_host_suspect_at_max_failures(self):
        detector = PingFailureDetector(max_failures=3)
        detector.register_host("192.168.1.1")
        detector._failure_counts["192.168.1.1"] = 3
        
        assert detector.is_host_suspect("192.168.1.1") is True

    def test_is_host_suspect_above_max_failures(self):
        detector = PingFailureDetector(max_failures=3)
        detector.register_host("192.168.1.1")
        detector._failure_counts["192.168.1.1"] = 5
        
        assert detector.is_host_suspect("192.168.1.1") is True

    def test_get_failure_count(self):
        detector = PingFailureDetector()
        detector.register_host("192.168.1.1")
        detector._failure_counts["192.168.1.1"] = 2
        
        assert detector.get_failure_count("192.168.1.1") == 2

    def test_get_failure_count_unregistered(self):
        detector = PingFailureDetector()
        assert detector.get_failure_count("unregistered") == 0

    def test_record_success(self):
        detector = PingFailureDetector()
        detector.register_host("192.168.1.1")
        detector._failure_counts["192.168.1.1"] = 5
        
        detector._record_success("192.168.1.1")
        
        assert detector._failure_counts["192.168.1.1"] == 0

    def test_record_failure(self):
        detector = PingFailureDetector()
        detector.register_host("192.168.1.1")
        
        detector._record_failure("192.168.1.1")
        detector._record_failure("192.168.1.1")
        
        assert detector._failure_counts["192.168.1.1"] == 2

    @pytest.mark.asyncio
    async def test_ping_not_available(self):
        detector = PingFailureDetector()
        detector._can_ping = False
        
        success, latency = await detector.ping("192.168.1.1")
        
        assert success is False
        assert latency == -1.0


class TestPhiAccrualFailureDetector:
    """Tests for PhiAccrualFailureDetector class."""

    def test_default_threshold(self):
        detector = PhiAccrualFailureDetector()
        assert detector.threshold == 8.0

    def test_custom_threshold(self):
        detector = PhiAccrualFailureDetector(threshold=10.0)
        assert detector.threshold == 10.0

    def test_threshold_setter(self):
        detector = PhiAccrualFailureDetector()
        detector.threshold = 12.0
        assert detector.threshold == 12.0

    def test_register_connection_creates_history(self):
        detector = PhiAccrualFailureDetector()
        detector.register_connection(1)
        
        assert 1 in detector._heartbeat_history
        assert isinstance(detector._heartbeat_history[1], HeartbeatHistory)

    def test_unregister_connection_removes_history(self):
        detector = PhiAccrualFailureDetector()
        detector.register_connection(1)
        detector.unregister_connection(1)
        
        assert 1 not in detector._heartbeat_history

    def test_unregister_connection_not_registered(self):
        detector = PhiAccrualFailureDetector()
        detector.unregister_connection(999)

    def test_heartbeat_updates_history(self):
        detector = PhiAccrualFailureDetector()
        detector.register_connection(1)
        
        timestamp = 1000.0
        detector.heartbeat(1, timestamp)
        
        assert detector._heartbeat_history[1].last_timestamp == timestamp

    def test_heartbeat_not_registered(self):
        detector = PhiAccrualFailureDetector()
        detector.heartbeat(999, 1000.0)

    def test_phi_with_no_history(self):
        detector = PhiAccrualFailureDetector()
        
        phi = detector.phi(999)
        
        assert phi == 0.0

    def test_phi_with_empty_history(self):
        detector = PhiAccrualFailureDetector(first_heartbeat_estimate_ms=0)
        detector.register_connection(1)
        detector._heartbeat_history[1]._intervals.clear()
        detector._heartbeat_history[1]._last_timestamp = None
        
        phi = detector.phi(1)
        
        assert phi == 0.0

    def test_phi_increases_over_time(self):
        detector = PhiAccrualFailureDetector(
            first_heartbeat_estimate_ms=100.0,
            min_std_deviation_ms=10.0,
        )
        detector.register_connection(1)
        
        base_time = 1000.0
        detector.heartbeat(1, base_time)
        
        phi_short = detector.phi(1, base_time + 100)
        phi_long = detector.phi(1, base_time + 1000)
        
        assert phi_long > phi_short

    def test_is_available_when_phi_below_threshold(self):
        detector = PhiAccrualFailureDetector(threshold=100.0)
        detector.register_connection(1)
        
        base_time = 1000.0
        detector.heartbeat(1, base_time)
        
        assert detector.is_available(1, base_time + 50) is True

    def test_is_available_when_phi_above_threshold(self):
        detector = PhiAccrualFailureDetector(
            threshold=1.0,
            first_heartbeat_estimate_ms=10.0,
        )
        detector.register_connection(1)
        
        base_time = 1000.0
        detector.heartbeat(1, base_time)
        
        assert detector.is_available(1, base_time + 100000) is False

    def test_phi_calculation_with_recent_heartbeat(self):
        detector = PhiAccrualFailureDetector()
        detector.register_connection(1)
        
        base_time = 1000.0
        detector.heartbeat(1, base_time)
        
        phi = detector.phi(1, base_time)
        assert phi >= 0


class TestHeartbeatHistory:
    """Tests for HeartbeatHistory class."""

    def test_initial_state_with_estimate(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=500.0)
        assert not history.is_empty
        assert len(history._intervals) == 2

    def test_initial_state_without_estimate(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        assert history.is_empty
        assert len(history._intervals) == 0

    def test_is_empty_initially_without_estimate(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        assert history.is_empty is True

    def test_is_empty_after_estimate(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=500.0)
        assert history.is_empty is False

    def test_add_updates_last_timestamp(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        
        history.add(1000.0)
        
        assert history.last_timestamp == 1000.0

    def test_add_records_interval(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        
        history.add(1000.0)
        history.add(1100.0)
        
        assert len(history._intervals) == 1
        assert 100.0 in history._intervals

    def test_add_ignores_first_timestamp(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        
        history.add(1000.0)
        
        assert len(history._intervals) == 0

    def test_add_ignores_zero_interval(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        
        history.add(1000.0)
        history.add(1000.0)
        
        assert len(history._intervals) == 0

    def test_mean_calculation(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        
        history.add(0)
        history.add(100)
        history.add(200)
        history.add(300)
        
        assert history.mean == 100.0

    def test_mean_empty_history(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        assert history.mean == 0.0

    def test_variance_calculation(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        
        history.add(0)
        history.add(100)
        history.add(200)
        history.add(300)
        
        variance = history.variance
        assert variance >= 0

    def test_variance_empty_history(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        assert history.variance == 0.0

    def test_std_deviation_calculation(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        
        history.add(0)
        history.add(100)
        history.add(200)
        history.add(300)
        
        std_dev = history.std_deviation
        assert std_dev >= 0

    def test_std_deviation_empty_history(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        assert history.std_deviation == 0.0

    def test_max_sample_size_limit(self):
        max_size = 5
        history = HeartbeatHistory(
            max_sample_size=max_size,
            first_heartbeat_estimate_ms=0,
        )
        
        for i in range(10):
            history.add(i * 100.0)
        
        assert len(history._intervals) == max_size

    def test_max_sample_size_maintains_stats(self):
        max_size = 3
        history = HeartbeatHistory(
            max_sample_size=max_size,
            first_heartbeat_estimate_ms=0,
        )
        
        history.add(0)
        history.add(100)
        history.add(200)
        history.add(300)
        history.add(400)
        
        assert history.mean > 0


class TestHeartbeatManager:
    """Tests for HeartbeatManager class."""

    def test_add_connection(self):
        detector = FailureDetector()
        manager = HeartbeatManager(
            failure_detector=detector,
            send_heartbeat=Mock(),
            on_connection_failed=Mock(),
        )
        
        connection = Mock()
        connection.connection_id = 1
        
        manager.add_connection(connection)
        
        assert 1 in manager._connections

    def test_remove_connection(self):
        detector = FailureDetector()
        manager = HeartbeatManager(
            failure_detector=detector,
            send_heartbeat=Mock(),
            on_connection_failed=Mock(),
        )
        
        connection = Mock()
        connection.connection_id = 1
        
        manager.add_connection(connection)
        manager.remove_connection(connection)
        
        assert 1 not in manager._connections

    def test_remove_connection_not_added(self):
        detector = FailureDetector()
        manager = HeartbeatManager(
            failure_detector=detector,
            send_heartbeat=Mock(),
            on_connection_failed=Mock(),
        )
        
        connection = Mock()
        connection.connection_id = 999
        
        manager.remove_connection(connection)

    def test_failure_detector_property(self):
        detector = FailureDetector()
        manager = HeartbeatManager(
            failure_detector=detector,
            send_heartbeat=Mock(),
            on_connection_failed=Mock(),
        )
        
        assert manager.failure_detector is detector

    def test_start_sets_running(self):
        detector = FailureDetector()
        manager = HeartbeatManager(
            failure_detector=detector,
            send_heartbeat=Mock(),
            on_connection_failed=Mock(),
        )
        
        manager.start()
        
        assert manager._running is True
        assert manager._task is not None

    def test_start_idempotent(self):
        detector = FailureDetector()
        manager = HeartbeatManager(
            failure_detector=detector,
            send_heartbeat=Mock(),
            on_connection_failed=Mock(),
        )
        
        manager.start()
        task1 = manager._task
        manager.start()
        task2 = manager._task
        
        assert task1 is task2

    @pytest.mark.asyncio
    async def test_stop_clears_running(self):
        detector = FailureDetector()
        manager = HeartbeatManager(
            failure_detector=detector,
            send_heartbeat=Mock(),
            on_connection_failed=Mock(),
        )
        
        manager.start()
        await manager.stop()
        
        assert manager._running is False
        assert manager._task is None

    @pytest.mark.asyncio
    async def test_stop_when_not_started(self):
        detector = FailureDetector()
        manager = HeartbeatManager(
            failure_detector=detector,
            send_heartbeat=Mock(),
            on_connection_failed=Mock(),
        )
        
        await manager.stop()
        
        assert manager._running is False


class TestChecksumCalculation:
    """Tests for checksum calculation utility."""

    def test_checksum_even_length(self):
        data = b"\x00\x01\x00\x02"
        checksum = _calculate_checksum(data)
        assert isinstance(checksum, int)
        assert 0 <= checksum <= 0xFFFF

    def test_checksum_odd_length(self):
        data = b"\x00\x01\x02"
        checksum = _calculate_checksum(data)
        assert isinstance(checksum, int)
        assert 0 <= checksum <= 0xFFFF

    def test_checksum_empty(self):
        data = b""
        checksum = _calculate_checksum(data)
        assert checksum == 0xFFFF

    def test_checksum_consistency(self):
        data = b"\x08\x00\x00\x00\x00\x01\x00\x01"
        checksum1 = _calculate_checksum(data)
        checksum2 = _calculate_checksum(data)
        assert checksum1 == checksum2
