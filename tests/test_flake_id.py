"""Unit tests for FlakeIdGenerator proxy."""

import threading
import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from hazelcast.protocol.codec import IdBatch, FlakeIdGeneratorCodec
from hazelcast.proxy.flake_id import FlakeIdGenerator, DEFAULT_BATCH_SIZE
from hazelcast.proxy.base import ProxyContext


class TestIdBatch(unittest.TestCase):
    """Tests for the IdBatch class."""

    def test_basic_iteration(self):
        batch = IdBatch(base=100, increment=2, batch_size=5)
        expected = [100, 102, 104, 106, 108]
        actual = [batch.next_id() for _ in range(5)]
        self.assertEqual(expected, actual)

    def test_remaining(self):
        batch = IdBatch(base=0, increment=1, batch_size=10)
        self.assertEqual(10, batch.remaining())
        batch.next_id()
        self.assertEqual(9, batch.remaining())
        for _ in range(9):
            batch.next_id()
        self.assertEqual(0, batch.remaining())

    def test_is_exhausted(self):
        batch = IdBatch(base=0, increment=1, batch_size=2)
        self.assertFalse(batch.is_exhausted())
        batch.next_id()
        self.assertFalse(batch.is_exhausted())
        batch.next_id()
        self.assertTrue(batch.is_exhausted())

    def test_exhausted_raises_stop_iteration(self):
        batch = IdBatch(base=0, increment=1, batch_size=1)
        batch.next_id()
        with self.assertRaises(StopIteration):
            batch.next_id()

    def test_properties(self):
        batch = IdBatch(base=500, increment=10, batch_size=50)
        self.assertEqual(500, batch.base)
        self.assertEqual(10, batch.increment)
        self.assertEqual(50, batch.batch_size)


class TestFlakeIdGenerator(unittest.TestCase):
    """Tests for the FlakeIdGenerator proxy."""

    def setUp(self):
        self.mock_invocation_service = MagicMock()
        self.context = ProxyContext(
            invocation_service=self.mock_invocation_service
        )

    def _create_generator(self, batch_size=DEFAULT_BATCH_SIZE):
        return FlakeIdGenerator("test-generator", self.context, batch_size)

    def _mock_batch_response(self, base, increment, batch_size):
        """Create a mock future that returns an IdBatch."""
        future = Future()
        future.set_result(IdBatch(base, increment, batch_size))
        return future

    def test_new_id_fetches_batch_on_first_call(self):
        generator = self._create_generator(batch_size=10)
        self.mock_invocation_service.invoke.return_value = self._mock_batch_response(
            base=1000, increment=1, batch_size=10
        )

        with patch.object(
            FlakeIdGeneratorCodec,
            "decode_new_id_batch_response",
            return_value=IdBatch(1000, 1, 10),
        ):
            id_value = generator.new_id()

        self.assertEqual(1000, id_value)
        self.mock_invocation_service.invoke.assert_called_once()

    def test_new_id_uses_cached_batch(self):
        generator = self._create_generator(batch_size=10)
        generator._batch = IdBatch(base=500, increment=1, batch_size=5)

        id1 = generator.new_id()
        id2 = generator.new_id()

        self.assertEqual(500, id1)
        self.assertEqual(501, id2)
        self.mock_invocation_service.invoke.assert_not_called()

    def test_new_id_batch_returns_requested_size(self):
        generator = self._create_generator(batch_size=10)
        generator._batch = IdBatch(base=100, increment=1, batch_size=20)

        ids = generator.new_id_batch(5)

        self.assertEqual(5, len(ids))
        self.assertEqual([100, 101, 102, 103, 104], ids)

    def test_new_id_batch_invalid_size(self):
        generator = self._create_generator()

        with self.assertRaises(ValueError):
            generator.new_id_batch(0)

        with self.assertRaises(ValueError):
            generator.new_id_batch(-1)

    def test_batch_size_property(self):
        generator = self._create_generator(batch_size=50)
        self.assertEqual(50, generator.batch_size)

    def test_destroyed_generator_raises_error(self):
        generator = self._create_generator()
        generator.destroy()

        with self.assertRaises(Exception):
            generator.new_id()

    def test_thread_safety(self):
        generator = self._create_generator(batch_size=1000)
        generator._batch = IdBatch(base=0, increment=1, batch_size=1000)

        ids_collected = []
        lock = threading.Lock()

        def collect_ids():
            for _ in range(100):
                id_value = generator.new_id()
                with lock:
                    ids_collected.append(id_value)

        threads = [threading.Thread(target=collect_ids) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(1000, len(ids_collected))
        self.assertEqual(1000, len(set(ids_collected)))

    def test_new_id_async_with_cached_batch(self):
        generator = self._create_generator()
        generator._batch = IdBatch(base=200, increment=1, batch_size=5)

        future = generator.new_id_async()
        id_value = future.result()

        self.assertEqual(200, id_value)

    def test_repr(self):
        generator = self._create_generator()
        self.assertIn("test-generator", repr(generator))


class TestFlakeIdGeneratorCodec(unittest.TestCase):
    """Tests for the FlakeIdGeneratorCodec."""

    def test_encode_request_sets_batch_size(self):
        with patch("hazelcast.protocol.codec.ClientMessage") as MockMessage:
            mock_msg = MagicMock()
            MockMessage.create_for_encode.return_value = mock_msg

            FlakeIdGeneratorCodec.encode_new_id_batch_request("gen1", 50)

            mock_msg.add_frame.assert_called()
            MockMessage.create_for_encode.assert_called_once()


if __name__ == "__main__":
    unittest.main()
