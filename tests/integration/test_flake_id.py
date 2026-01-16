"""Integration tests for FlakeIdGenerator cluster-wide uniqueness."""

import threading
import unittest
from typing import List, Set
from unittest.mock import MagicMock

from hazelcast.protocol.codec import IdBatch
from hazelcast.proxy.flake_id import FlakeIdGenerator
from hazelcast.proxy.base import ProxyContext


class MockClusterIdService:
    """Simulates a cluster-side FlakeIdGenerator service for integration testing."""

    def __init__(self):
        self._lock = threading.Lock()
        self._next_base = 0
        self._increment = 1
        self._allocated_ranges: List[tuple] = []

    def allocate_batch(self, batch_size: int) -> IdBatch:
        """Allocate a new batch of IDs, simulating cluster behavior."""
        with self._lock:
            base = self._next_base
            self._next_base += batch_size * self._increment
            self._allocated_ranges.append((base, base + batch_size * self._increment))
            return IdBatch(base, self._increment, batch_size)

    def get_all_allocated_ids(self) -> Set[int]:
        """Get all IDs that could be generated from allocated ranges."""
        ids = set()
        for start, end in self._allocated_ranges:
            for i in range(start, end, self._increment):
                ids.add(i)
        return ids


class TestClusterWideUniqueness(unittest.TestCase):
    """Tests verifying cluster-wide unique ID generation."""

    def setUp(self):
        self.cluster_service = MockClusterIdService()

    def _create_generator(self, name: str, batch_size: int = 100) -> FlakeIdGenerator:
        """Create a FlakeIdGenerator that uses the mock cluster service."""
        mock_invocation = MagicMock()
        context = ProxyContext(invocation_service=mock_invocation)
        generator = FlakeIdGenerator(name, context, batch_size)

        generator._fetch_new_batch = lambda size=None: self.cluster_service.allocate_batch(
            size if size is not None else batch_size
        )
        return generator

    def test_single_generator_uniqueness(self):
        """Verify IDs from a single generator are unique."""
        generator = self._create_generator("gen1", batch_size=100)

        ids = set()
        for _ in range(1000):
            id_value = generator.new_id()
            self.assertNotIn(id_value, ids, "Duplicate ID generated")
            ids.add(id_value)

        self.assertEqual(1000, len(ids))

    def test_multiple_generators_uniqueness(self):
        """Verify IDs from multiple generators sharing a cluster service are unique."""
        generators = [
            self._create_generator(f"gen{i}", batch_size=50)
            for i in range(5)
        ]

        all_ids: Set[int] = set()
        ids_per_generator = 200

        for gen in generators:
            for _ in range(ids_per_generator):
                id_value = gen.new_id()
                self.assertNotIn(
                    id_value, all_ids,
                    f"Duplicate ID {id_value} from generator {gen.name}"
                )
                all_ids.add(id_value)

        expected_total = len(generators) * ids_per_generator
        self.assertEqual(expected_total, len(all_ids))

    def test_concurrent_generation_uniqueness(self):
        """Verify concurrent ID generation produces unique IDs."""
        generator = self._create_generator("concurrent-gen", batch_size=100)

        all_ids: List[int] = []
        lock = threading.Lock()
        errors: List[Exception] = []

        def generate_ids(count: int):
            try:
                local_ids = []
                for _ in range(count):
                    id_value = generator.new_id()
                    local_ids.append(id_value)
                with lock:
                    all_ids.extend(local_ids)
            except Exception as e:
                with lock:
                    errors.append(e)

        threads = [
            threading.Thread(target=generate_ids, args=(100,))
            for _ in range(10)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual([], errors, f"Errors during generation: {errors}")
        self.assertEqual(1000, len(all_ids))
        self.assertEqual(1000, len(set(all_ids)), "Duplicate IDs found")

    def test_batch_generation_uniqueness(self):
        """Verify batch generation produces unique IDs."""
        generator = self._create_generator("batch-gen", batch_size=50)

        batch1 = generator.new_id_batch(100)
        batch2 = generator.new_id_batch(100)

        self.assertEqual(100, len(batch1))
        self.assertEqual(100, len(batch2))

        all_ids = set(batch1 + batch2)
        self.assertEqual(200, len(all_ids), "Duplicate IDs in batches")

    def test_mixed_single_and_batch_uniqueness(self):
        """Verify mixing single and batch ID generation produces unique IDs."""
        generator = self._create_generator("mixed-gen", batch_size=25)

        all_ids: Set[int] = set()

        for _ in range(50):
            id_value = generator.new_id()
            self.assertNotIn(id_value, all_ids)
            all_ids.add(id_value)

        batch = generator.new_id_batch(50)
        for id_value in batch:
            self.assertNotIn(id_value, all_ids)
            all_ids.add(id_value)

        for _ in range(50):
            id_value = generator.new_id()
            self.assertNotIn(id_value, all_ids)
            all_ids.add(id_value)

        self.assertEqual(150, len(all_ids))

    def test_id_ordering(self):
        """Verify IDs are monotonically increasing within a generator."""
        generator = self._create_generator("ordered-gen", batch_size=100)

        previous_id = -1
        for _ in range(500):
            id_value = generator.new_id()
            self.assertGreater(id_value, previous_id)
            previous_id = id_value

    def test_generator_isolation(self):
        """Verify different generator instances get different ID ranges."""
        gen1 = self._create_generator("isolated-gen1", batch_size=100)
        gen2 = self._create_generator("isolated-gen2", batch_size=100)

        ids1 = set(gen1.new_id_batch(100))
        ids2 = set(gen2.new_id_batch(100))

        overlap = ids1 & ids2
        self.assertEqual(set(), overlap, f"Overlapping IDs: {overlap}")


class TestIdBatchIntegration(unittest.TestCase):
    """Integration tests for IdBatch behavior."""

    def test_large_batch_iteration(self):
        """Test iterating through a large batch."""
        batch = IdBatch(base=0, increment=1, batch_size=10000)

        ids = []
        while not batch.is_exhausted():
            ids.append(batch.next_id())

        self.assertEqual(10000, len(ids))
        self.assertEqual(10000, len(set(ids)))
        self.assertEqual(list(range(10000)), ids)

    def test_increment_spacing(self):
        """Test that increment properly spaces IDs."""
        batch = IdBatch(base=1000, increment=100, batch_size=10)

        ids = [batch.next_id() for _ in range(10)]
        expected = [1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900]
        self.assertEqual(expected, ids)


if __name__ == "__main__":
    unittest.main()
