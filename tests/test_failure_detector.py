"""Unit tests for failure detection mechanisms."""

import asyncio
import time
import unittest
from unittest.mock import MagicMock, patch, AsyncMock

from hazelcast.network.failure_detector import (
    FailureDetector,
    HeartbeatManager,
    HeartbeatHistory,
    PhiAccrualFailureDetector,
    PingFailureDetector,
    _calculate_checksum,
    _can_use_raw_sockets,
)


class MockConnection:
    """Mock connection for testing."""

    def __init__(
        self,
        connection_id: int = 1,
        is_alive: bool = True,
        last_read_time: float = 0.0,
        last_write_time: float = 0.0,
    ):
        self.connection_id = connection_id
        self.is_alive = is_alive
        self.last_read_time = last_read_time
        self.last_write_time = last_write_time
        self.address = MagicMock()
        self.address.host = "127.0.0.1"


class TestFailureDetector(unittest.TestCase):
    """Tests for the FailureDetector class."""

    def setUp(self):
        self.detector = FailureDetector(
            heartbeat_interval=5.0,
            heartbeat_timeout=60.0,
        )

    def test_init_default_values(self):
        detector = FailureDetector()
        self.assertEqual(detector.heartbeat_interval, 5.0)
        self.assertEqual(detector.heartbeat_timeout, 60.0)

    def test_init_custom_values(self):
        detector = FailureDetector(
            heartbeat_interval=10.0,
            heartbeat_timeout=120.0,
        )
        self.assertEqual(detector.heartbeat_interval, 10.0)
        self.assertEqual(detector.heartbeat_timeout, 120.0)

    def test_heartbeat_interval_setter(self):
        self.detector.heartbeat_interval = 15.0
        self.assertEqual(self.detector.heartbeat_interval, 15.0)

    def test_heartbeat_timeout_setter(self):
        self.detector.heartbeat_timeout = 90.0
        self.assertEqual(self.detector.heartbeat_timeout, 90.0)

    def test_register_connection(self):
        conn = MockConnection(connection_id=1)
        self.detector.register_connection(conn)
        self.assertIn(1, self.detector._last_heartbeat)

    def test_unregister_connection(self):
        conn = MockConnection(connection_id=1)
        self.detector.register_connection(conn)
        self.detector.unregister_connection(conn)
        self.assertNotIn(1, self.detector._last_heartbeat)

    def test_on_heartbeat_received(self):
        conn = MockConnection(connection_id=1)
        self.detector.register_connection(conn)
        old_time = self.detector._last_heartbeat[1]

        time.sleep(0.01)
        self.detector.on_heartbeat_received(conn)

        self.assertGreater(self.detector._last_heartbeat[1], old_time)

    def test_is_alive_dead_connection(self):
        conn = MockConnection(connection_id=1, is_alive=False)
        self.assertFalse(self.detector.is_alive(conn))

    def test_is_alive_no_read_time(self):
        conn = MockConnection(connection_id=1, is_alive=True, last_read_time=0)
        self.assertTrue(self.detector.is_alive(conn))

    def test_is_alive_recent_read(self):
        conn = MockConnection(
            connection_id=1,
            is_alive=True,
            last_read_time=time.time(),
        )
        self.assertTrue(self.detector.is_alive(conn))

    def test_is_alive_timeout_exceeded(self):
        detector = FailureDetector(heartbeat_timeout=1.0)
        conn = MockConnection(
            connection_id=1,
            is_alive=True,
            last_read_time=time.time() - 2.0,
        )
        self.assertFalse(detector.is_alive(conn))

    def test_needs_heartbeat_dead_connection(self):
        conn = MockConnection(connection_id=1, is_alive=False)
        self.assertFalse(self.detector.needs_heartbeat(conn))

    def test_needs_heartbeat_no_write_time(self):
        conn = MockConnection(
            connection_id=1,
            is_alive=True,
            last_write_time=0,
        )
        self.assertTrue(self.detector.needs_heartbeat(conn))

    def test_needs_heartbeat_recent_write(self):
        conn = MockConnection(
            connection_id=1,
            is_alive=True,
            last_write_time=time.time(),
        )
        self.assertFalse(self.detector.needs_heartbeat(conn))

    def test_needs_heartbeat_interval_exceeded(self):
        detector = FailureDetector(heartbeat_interval=1.0)
        conn = MockConnection(
            connection_id=1,
            is_alive=True,
            last_write_time=time.time() - 2.0,
        )
        self.assertTrue(detector.needs_heartbeat(conn))

    def test_get_suspect_connections_empty(self):
        suspects = self.detector.get_suspect_connections({})
        self.assertEqual(suspects, [])

    def test_get_suspect_connections_no_suspects(self):
        conn = MockConnection(
            connection_id=1,
            is_alive=True,
            last_read_time=time.time(),
        )
        suspects = self.detector.get_suspect_connections({1: conn})
        self.assertEqual(suspects, [])

    def test_get_suspect_connections_with_suspect(self):
        detector = FailureDetector(heartbeat_timeout=1.0)
        conn = MockConnection(
            connection_id=1,
            is_alive=True,
            last_read_time=time.time() - 2.0,
        )
        suspects = detector.get_suspect_connections({1: conn})
        self.assertEqual(len(suspects), 1)
        self.assertEqual(suspects[0], conn)

    def test_get_suspect_connections_skips_dead(self):
        detector = FailureDetector(heartbeat_timeout=1.0)
        conn = MockConnection(
            connection_id=1,
            is_alive=False,
            last_read_time=time.time() - 2.0,
        )
        suspects = detector.get_suspect_connections({1: conn})
        self.assertEqual(suspects, [])


class TestPingFailureDetector(unittest.TestCase):
    """Tests for the PingFailureDetector class."""

    def setUp(self):
        self.detector = PingFailureDetector(
            ping_timeout=5.0,
            ping_interval=10.0,
            max_failures=3,
        )

    def test_init_default_values(self):
        detector = PingFailureDetector()
        self.assertEqual(detector.ping_timeout, 5.0)
        self.assertEqual(detector.ping_interval, 10.0)
        self.assertEqual(detector.max_failures, 3)

    def test_init_custom_values(self):
        detector = PingFailureDetector(
            ping_timeout=2.0,
            ping_interval=5.0,
            max_failures=5,
        )
        self.assertEqual(detector.ping_timeout, 2.0)
        self.assertEqual(detector.ping_interval, 5.0)
        self.assertEqual(detector.max_failures, 5)

    def test_register_host(self):
        self.detector.register_host("192.168.1.1")
        self.assertIn("192.168.1.1", self.detector._failure_counts)
        self.assertEqual(self.detector._failure_counts["192.168.1.1"], 0)

    def test_unregister_host(self):
        self.detector.register_host("192.168.1.1")
        self.detector.unregister_host("192.168.1.1")
        self.assertNotIn("192.168.1.1", self.detector._failure_counts)

    def test_needs_ping_no_raw_sockets(self):
        self.detector._can_ping = False
        self.assertFalse(self.detector.needs_ping("192.168.1.1"))

    def test_needs_ping_first_time(self):
        self.detector._can_ping = True
        self.detector.register_host("192.168.1.1")
        self.assertTrue(self.detector.needs_ping("192.168.1.1"))

    def test_needs_ping_interval_not_reached(self):
        self.detector._can_ping = True
        self.detector.register_host("192.168.1.1")
        self.detector._last_ping_times["192.168.1.1"] = time.time()
        self.assertFalse(self.detector.needs_ping("192.168.1.1"))

    def test_needs_ping_interval_exceeded(self):
        detector = PingFailureDetector(ping_interval=1.0)
        detector._can_ping = True
        detector.register_host("192.168.1.1")
        detector._last_ping_times["192.168.1.1"] = time.time() - 2.0
        self.assertTrue(detector.needs_ping("192.168.1.1"))

    def test_is_host_suspect_below_threshold(self):
        self.detector.register_host("192.168.1.1")
        self.detector._failure_counts["192.168.1.1"] = 2
        self.assertFalse(self.detector.is_host_suspect("192.168.1.1"))

    def test_is_host_suspect_at_threshold(self):
        self.detector.register_host("192.168.1.1")
        self.detector._failure_counts["192.168.1.1"] = 3
        self.assertTrue(self.detector.is_host_suspect("192.168.1.1"))

    def test_get_failure_count(self):
        self.detector.register_host("192.168.1.1")
        self.detector._failure_counts["192.168.1.1"] = 5
        self.assertEqual(self.detector.get_failure_count("192.168.1.1"), 5)

    def test_get_failure_count_unknown_host(self):
        self.assertEqual(self.detector.get_failure_count("unknown"), 0)

    def test_record_success_resets_count(self):
        self.detector.register_host("192.168.1.1")
        self.detector._failure_counts["192.168.1.1"] = 5
        self.detector._record_success("192.168.1.1")
        self.assertEqual(self.detector._failure_counts["192.168.1.1"], 0)

    def test_record_failure_increments_count(self):
        self.detector.register_host("192.168.1.1")
        self.detector._record_failure("192.168.1.1")
        self.assertEqual(self.detector._failure_counts["192.168.1.1"], 1)


class TestPhiAccrualFailureDetector(unittest.TestCase):
    """Tests for the PhiAccrualFailureDetector class."""

    def setUp(self):
        self.detector = PhiAccrualFailureDetector(
            threshold=8.0,
            max_sample_size=200,
            min_std_deviation_ms=100.0,
        )

    def test_init_default_values(self):
        detector = PhiAccrualFailureDetector()
        self.assertEqual(detector.threshold, 8.0)

    def test_init_custom_threshold(self):
        detector = PhiAccrualFailureDetector(threshold=10.0)
        self.assertEqual(detector.threshold, 10.0)

    def test_threshold_setter(self):
        self.detector.threshold = 12.0
        self.assertEqual(self.detector.threshold, 12.0)

    def test_register_connection(self):
        self.detector.register_connection(1)
        self.assertIn(1, self.detector._heartbeat_history)

    def test_unregister_connection(self):
        self.detector.register_connection(1)
        self.detector.unregister_connection(1)
        self.assertNotIn(1, self.detector._heartbeat_history)

    def test_phi_unregistered_connection(self):
        phi = self.detector.phi(999)
        self.assertEqual(phi, 0.0)

    def test_phi_new_connection(self):
        self.detector.register_connection(1)
        phi = self.detector.phi(1)
        self.assertEqual(phi, 0.0)

    def test_heartbeat_updates_history(self):
        self.detector.register_connection(1)
        ts = time.time() * 1000
        self.detector.heartbeat(1, ts)
        history = self.detector._heartbeat_history[1]
        self.assertEqual(history.last_timestamp, ts)

    def test_is_available_new_connection(self):
        self.detector.register_connection(1)
        self.assertTrue(self.detector.is_available(1))

    def test_is_available_after_heartbeats(self):
        self.detector.register_connection(1)
        ts = time.time() * 1000
        self.detector.heartbeat(1, ts)
        self.detector.heartbeat(1, ts + 1000)
        self.assertTrue(self.detector.is_available(1, ts + 2000))


class TestHeartbeatHistory(unittest.TestCase):
    """Tests for the HeartbeatHistory class."""

    def test_init_empty(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        self.assertTrue(history.is_empty)
        self.assertIsNone(history.last_timestamp)

    def test_init_with_estimate(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=500.0)
        self.assertFalse(history.is_empty)

    def test_add_first_timestamp(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        history.add(1000.0)
        self.assertEqual(history.last_timestamp, 1000.0)

    def test_add_second_timestamp_creates_interval(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        history.add(1000.0)
        history.add(1500.0)
        self.assertFalse(history.is_empty)
        self.assertEqual(history.last_timestamp, 1500.0)

    def test_mean_calculation(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        history.add(0)
        history.add(100)
        history.add(200)
        history.add(300)
        self.assertEqual(history.mean, 100.0)

    def test_std_deviation_calculation(self):
        history = HeartbeatHistory(first_heartbeat_estimate_ms=0)
        history.add(0)
        history.add(100)
        history.add(200)
        history.add(300)
        self.assertAlmostEqual(history.std_deviation, 0.0, delta=0.01)

    def test_max_sample_size_respected(self):
        history = HeartbeatHistory(max_sample_size=5, first_heartbeat_estimate_ms=0)
        for i in range(10):
            history.add(i * 100)
        self.assertEqual(len(history._intervals), 5)


class TestHeartbeatManager(unittest.TestCase):
    """Tests for the HeartbeatManager class."""

    def setUp(self):
        self.failure_detector = FailureDetector(
            heartbeat_interval=0.1,
            heartbeat_timeout=1.0,
        )
        self.send_heartbeat = MagicMock()
        self.on_connection_failed = MagicMock()
        self.manager = HeartbeatManager(
            failure_detector=self.failure_detector,
            send_heartbeat=self.send_heartbeat,
            on_connection_failed=self.on_connection_failed,
        )

    def test_init(self):
        self.assertEqual(self.manager.failure_detector, self.failure_detector)
        self.assertFalse(self.manager._running)

    def test_add_connection(self):
        conn = MockConnection(connection_id=1)
        self.manager.add_connection(conn)
        self.assertIn(1, self.manager._connections)

    def test_remove_connection(self):
        conn = MockConnection(connection_id=1)
        self.manager.add_connection(conn)
        self.manager.remove_connection(conn)
        self.assertNotIn(1, self.manager._connections)

    def test_start(self):
        self.manager.start()
        self.assertTrue(self.manager._running)
        self.assertIsNotNone(self.manager._task)

        asyncio.get_event_loop().run_until_complete(self.manager.stop())

    def test_stop(self):
        self.manager.start()
        asyncio.get_event_loop().run_until_complete(self.manager.stop())
        self.assertFalse(self.manager._running)
        self.assertIsNone(self.manager._task)


class TestUtilityFunctions(unittest.TestCase):
    """Tests for utility functions."""

    def test_calculate_checksum(self):
        data = b'\x08\x00\x00\x00\x00\x01\x00\x01'
        checksum = _calculate_checksum(data)
        self.assertIsInstance(checksum, int)
        self.assertGreaterEqual(checksum, 0)
        self.assertLessEqual(checksum, 0xFFFF)

    def test_calculate_checksum_odd_length(self):
        data = b'\x08\x00\x00'
        checksum = _calculate_checksum(data)
        self.assertIsInstance(checksum, int)

    @patch('hazelcast.network.failure_detector.platform')
    @patch('hazelcast.network.failure_detector.os')
    def test_can_use_raw_sockets_windows(self, mock_os, mock_platform):
        mock_platform.system.return_value = "Windows"
        self.assertTrue(_can_use_raw_sockets())

    @patch('hazelcast.network.failure_detector.platform')
    @patch('hazelcast.network.failure_detector.os')
    def test_can_use_raw_sockets_root(self, mock_os, mock_platform):
        mock_platform.system.return_value = "Linux"
        mock_os.geteuid.return_value = 0
        self.assertTrue(_can_use_raw_sockets())


if __name__ == "__main__":
    unittest.main()
