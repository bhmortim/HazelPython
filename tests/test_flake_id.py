"""Unit tests for FlakeIdGenerator proxy."""

import pytest
import threading
import time
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from hazelcast.proxy.flake_id import (
    FlakeIdGeneratorProxy,
    FlakeIdGenerator,
    AutoBatcher,
    SERVICE_NAME_FLAKE_ID,
    DEFAULT_BATCH_SIZE,
    DEFAULT_PREFETCH_VALIDITY_MILLIS,
)
from hazelcast.protocol.codec import IdBatch


class TestIdBatch:
    """Test IdBatch functionality."""

    def test_basic_id_generation(self):
        """Test basic ID generation from a batch."""
        batch = IdBatch(base=1000, increment=1, batch_size=5)

        ids = [batch.next_id() for _ in range(5)]
        assert ids == [1000, 1001, 1002, 1003, 1004]

    def test_batch_exhaustion(self):
        """Test batch exhaustion behavior."""
        batch = IdBatch(base=100, increment=1, batch_size=3)

        batch.next_id()
        batch.next_id()
        batch.next_id()

        with pytest.raises(StopIteration):
            batch.next_id()

    def test_remaining_count(self):
        """Test remaining count tracking."""
        batch = IdBatch(base=0, increment=1, batch_size=5)

        assert batch.remaining() == 5
        batch.next_id()
        assert batch.remaining() == 4
        batch.next_id()
        batch.next_id()
        assert batch.remaining() == 2

    def test_is_exhausted(self):
        """Test exhaustion check."""
        batch = IdBatch(base=0, increment=1, batch_size=2)

        assert not batch.is_exhausted()
        batch.next_id()
        assert not batch.is_exhausted()
        batch.next_id()
        assert batch.is_exhausted()

    def test_custom_increment(self):
        """Test batch with custom increment."""
        batch = IdBatch(base=100, increment=10, batch_size=4)

        ids = [batch.next_id() for _ in range(4)]
        assert ids == [100, 110, 120, 130]

    def test_large_base(self):
        """Test batch with large base value."""
        large_base = 2 ** 40
        batch = IdBatch(base=large_base, increment=1, batch_size=3)

        ids = [batch.next_id() for _ in range(3)]
        assert ids == [large_base, large_base + 1, large_base + 2]


class TestAutoBatcher:
    """Test AutoBatcher functionality."""

    def test_local_batch_creation(self):
        """Test local batch creation without fetch function."""
        batcher = AutoBatcher(batch_size=10)

        id1 = batcher.next_id()
        id2 = batcher.next_id()

        assert id1 != id2
        assert id2 > id1

    def test_batch_refetch_on_exhaustion(self):
        """Test automatic batch refetch when exhausted."""
        batcher = AutoBatcher(batch_size=3)

        ids = [batcher.next_id() for _ in range(10)]

        assert len(set(ids)) == 10

    def test_custom_fetch_function(self):
        """Test batcher with custom fetch function."""
        fetch_count = [0]

        def mock_fetch():
            fetch_count[0] += 1
            future = Future()
            future.set_result(IdBatch(base=fetch_count[0] * 100, increment=1, batch_size=3))
            return future

        batcher = AutoBatcher(batch_size=3, fetch_batch_fn=mock_fetch)

        batcher.next_id()
        batcher.next_id()
        batcher.next_id()
        batcher.next_id()

        assert fetch_count[0] == 2

    def test_batch_expiration(self):
        """Test batch expiration based on validity time."""
        batcher = AutoBatcher(
            batch_size=100,
            prefetch_validity_millis=50,
        )

        batcher.next_id()

        time.sleep(0.1)

        batcher.next_id()

    def test_next_id_async(self):
        """Test async ID generation."""
        batcher = AutoBatcher(batch_size=10)

        future = batcher.next_id_async()
        id_value = future.result()

        assert isinstance(id_value, int)

    def test_reset(self):
        """Test batcher reset."""
        batcher = AutoBatcher(batch_size=10)

        batcher.next_id()
        batcher.reset()

        assert batcher._current_batch is None
        assert batcher._batch_fetch_time == 0

    def test_concurrent_id_generation(self):
        """Test thread-safe ID generation."""
        batcher = AutoBatcher(batch_size=100)
        ids = []
        lock = threading.Lock()

        def generate_ids():
            for _ in range(50):
                id_val = batcher.next_id()
                with lock:
                    ids.append(id_val)

        threads = [threading.Thread(target=generate_ids) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(ids) == 250
        assert len(set(ids)) == 250


class TestFlakeIdGeneratorProxy:
    """Test FlakeIdGeneratorProxy functionality."""

    def test_initialization(self):
        """Test proxy initialization."""
        proxy = FlakeIdGeneratorProxy(SERVICE_NAME_FLAKE_ID, "test-gen")

        assert proxy.name == "test-gen"
        assert proxy.service_name == SERVICE_NAME_FLAKE_ID
        assert proxy.batch_size == DEFAULT_BATCH_SIZE

    def test_custom_batch_size(self):
        """Test proxy with custom batch size."""
        proxy = FlakeIdGeneratorProxy(
            SERVICE_NAME_FLAKE_ID,
            "test-gen",
            batch_size=50,
        )

        assert proxy.batch_size == 50

    def test_new_id(self):
        """Test new_id generation."""
        proxy = FlakeIdGeneratorProxy(SERVICE_NAME_FLAKE_ID, "test-gen")

        id1 = proxy.new_id()
        id2 = proxy.new_id()

        assert isinstance(id1, int)
        assert isinstance(id2, int)
        assert id1 != id2

    def test_new_id_async(self):
        """Test async new_id generation."""
        proxy = FlakeIdGeneratorProxy(SERVICE_NAME_FLAKE_ID, "test-gen")

        future = proxy.new_id_async()
        id_val = future.result()

        assert isinstance(id_val, int)

    def test_id_uniqueness(self):
        """Test that generated IDs are unique."""
        proxy = FlakeIdGeneratorProxy(
            SERVICE_NAME_FLAKE_ID,
            "test-gen",
            batch_size=100,
        )

        ids = [proxy.new_id() for _ in range(500)]

        assert len(set(ids)) == 500

    def test_destroyed_proxy(self):
        """Test operations on destroyed proxy."""
        proxy = FlakeIdGeneratorProxy(SERVICE_NAME_FLAKE_ID, "test-gen")
        proxy.destroy()

        with pytest.raises(Exception):
            proxy.new_id()

    def test_on_destroy_clears_batcher(self):
        """Test that destroy clears the auto batcher."""
        proxy = FlakeIdGeneratorProxy(SERVICE_NAME_FLAKE_ID, "test-gen")
        proxy.new_id()

        proxy._on_destroy()

        assert proxy._auto_batcher._current_batch is None

    def test_concurrent_new_id(self):
        """Test concurrent ID generation from proxy."""
        proxy = FlakeIdGeneratorProxy(
            SERVICE_NAME_FLAKE_ID,
            "test-gen",
            batch_size=200,
        )
        ids = []
        lock = threading.Lock()

        def generate():
            for _ in range(100):
                id_val = proxy.new_id()
                with lock:
                    ids.append(id_val)

        threads = [threading.Thread(target=generate) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(ids) == 1000
        assert len(set(ids)) == 1000

    def test_repr(self):
        """Test string representation."""
        proxy = FlakeIdGeneratorProxy(SERVICE_NAME_FLAKE_ID, "my-generator")
        repr_str = repr(proxy)

        assert "my-generator" in repr_str


class TestFlakeIdGeneratorAlias:
    """Test FlakeIdGenerator alias."""

    def test_alias_is_proxy_class(self):
        """Test that FlakeIdGenerator is an alias for the proxy."""
        assert FlakeIdGenerator is FlakeIdGeneratorProxy

    def test_alias_instantiation(self):
        """Test instantiation through alias."""
        gen = FlakeIdGenerator(SERVICE_NAME_FLAKE_ID, "alias-test")
        assert isinstance(gen, FlakeIdGeneratorProxy)


class TestFlakeIdGeneratorWithContext:
    """Test FlakeIdGenerator with mock context."""

    def test_with_invocation_service(self):
        """Test proxy with mock invocation service."""
        mock_context = MagicMock()
        mock_inv_service = MagicMock()
        mock_context.invocation_service = mock_inv_service

        future = Future()
        future.set_result(IdBatch(base=5000, increment=1, batch_size=10))
        mock_inv_service.invoke.return_value = future

        proxy = FlakeIdGeneratorProxy(
            SERVICE_NAME_FLAKE_ID,
            "test-gen",
            context=mock_context,
        )

        id_val = proxy.new_id()
        assert isinstance(id_val, int)

    def test_fallback_to_local_batch_on_error(self):
        """Test fallback to local batch when fetch fails."""
        mock_context = MagicMock()
        mock_inv_service = MagicMock()
        mock_context.invocation_service = mock_inv_service

        error_future = Future()
        error_future.set_exception(RuntimeError("Network error"))
        mock_inv_service.invoke.return_value = error_future

        proxy = FlakeIdGeneratorProxy(
            SERVICE_NAME_FLAKE_ID,
            "test-gen",
            context=mock_context,
        )

        id_val = proxy.new_id()
        assert isinstance(id_val, int)
