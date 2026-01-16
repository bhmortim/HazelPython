"""Tests for CP Subsystem module structure and imports."""

import unittest


class TestCPImports(unittest.TestCase):
    """Test CP subsystem module imports."""

    def test_import_cp_module(self):
        """Test that CP module can be imported."""
        from hazelcast import cp
        self.assertIsNotNone(cp)

    def test_import_atomic_long(self):
        """Test AtomicLong import."""
        from hazelcast.cp import AtomicLong
        self.assertIsNotNone(AtomicLong)

    def test_import_atomic_reference(self):
        """Test AtomicReference import."""
        from hazelcast.cp import AtomicReference
        self.assertIsNotNone(AtomicReference)

    def test_import_fenced_lock(self):
        """Test FencedLock import."""
        from hazelcast.cp import FencedLock
        self.assertIsNotNone(FencedLock)

    def test_import_semaphore(self):
        """Test Semaphore import."""
        from hazelcast.cp import Semaphore
        self.assertIsNotNone(Semaphore)

    def test_import_count_down_latch(self):
        """Test CountDownLatch import."""
        from hazelcast.cp import CountDownLatch
        self.assertIsNotNone(CountDownLatch)

    def test_all_exports(self):
        """Test that __all__ contains expected exports."""
        from hazelcast import cp
        expected = [
            "AtomicLong",
            "AtomicReference",
            "FencedLock",
            "Semaphore",
            "CountDownLatch",
        ]
        for name in expected:
            self.assertIn(name, cp.__all__)


if __name__ == "__main__":
    unittest.main()
