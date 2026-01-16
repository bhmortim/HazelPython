"""Unit tests for CardinalityEstimator proxy."""

import pytest
import math
from concurrent.futures import Future
from unittest.mock import MagicMock

from hazelcast.proxy.cardinality_estimator import (
    CardinalityEstimator,
    CardinalityEstimatorProxy,
    HyperLogLog,
    SERVICE_NAME_CARDINALITY_ESTIMATOR,
    DEFAULT_HLL_PRECISION,
)


class TestHyperLogLog:
    """Test HyperLogLog implementation."""

    def test_initialization(self):
        """Test HyperLogLog initialization."""
        hll = HyperLogLog()

        assert hll.precision == DEFAULT_HLL_PRECISION
        assert hll.register_count == 2 ** DEFAULT_HLL_PRECISION

    def test_custom_precision(self):
        """Test HyperLogLog with custom precision."""
        hll = HyperLogLog(precision=10)

        assert hll.precision == 10
        assert hll.register_count == 1024

    def test_invalid_precision_low(self):
        """Test that low precision raises error."""
        with pytest.raises(ValueError):
            HyperLogLog(precision=3)

    def test_invalid_precision_high(self):
        """Test that high precision raises error."""
        with pytest.raises(ValueError):
            HyperLogLog(precision=17)

    def test_add_string(self):
        """Test adding string values."""
        hll = HyperLogLog()

        hll.add("hello")
        hll.add("world")

        estimate = hll.estimate()
        assert estimate >= 1

    def test_add_integers(self):
        """Test adding integer values."""
        hll = HyperLogLog()

        for i in range(100):
            hll.add(i)

        estimate = hll.estimate()
        assert 80 <= estimate <= 120

    def test_add_bytes(self):
        """Test adding bytes values."""
        hll = HyperLogLog()

        hll.add(b"bytes1")
        hll.add(b"bytes2")

        estimate = hll.estimate()
        assert estimate >= 1

    def test_duplicate_values(self):
        """Test that duplicates don't increase cardinality."""
        hll = HyperLogLog()

        for _ in range(100):
            hll.add("same-value")

        estimate = hll.estimate()
        assert estimate <= 5

    def test_estimate_empty(self):
        """Test estimate on empty HyperLogLog."""
        hll = HyperLogLog()

        estimate = hll.estimate()
        assert estimate == 0

    def test_estimate_accuracy(self):
        """Test estimation accuracy for known cardinality."""
        hll = HyperLogLog(precision=14)

        actual_count = 10000
        for i in range(actual_count):
            hll.add(f"item-{i}")

        estimate = hll.estimate()
        error_rate = abs(estimate - actual_count) / actual_count

        assert error_rate < 0.05

    def test_merge(self):
        """Test merging two HyperLogLogs."""
        hll1 = HyperLogLog()
        hll2 = HyperLogLog()

        for i in range(100):
            hll1.add(f"hll1-{i}")

        for i in range(100):
            hll2.add(f"hll2-{i}")

        hll1.merge(hll2)
        estimate = hll1.estimate()

        assert 150 <= estimate <= 250

    def test_merge_different_precision_raises(self):
        """Test that merging different precisions raises error."""
        hll1 = HyperLogLog(precision=10)
        hll2 = HyperLogLog(precision=12)

        with pytest.raises(ValueError):
            hll1.merge(hll2)

    def test_merge_overlapping(self):
        """Test merging HyperLogLogs with overlapping values."""
        hll1 = HyperLogLog()
        hll2 = HyperLogLog()

        for i in range(100):
            hll1.add(f"item-{i}")

        for i in range(50, 150):
            hll2.add(f"item-{i}")

        hll1.merge(hll2)
        estimate = hll1.estimate()

        assert 120 <= estimate <= 180

    def test_clear(self):
        """Test clearing the HyperLogLog."""
        hll = HyperLogLog()

        for i in range(100):
            hll.add(f"item-{i}")

        assert hll.estimate() > 0

        hll.clear()

        assert hll.estimate() == 0

    def test_hash_determinism(self):
        """Test that hashing is deterministic."""
        hll = HyperLogLog()

        hll.add("test-value")
        estimate1 = hll.estimate()

        hll.clear()
        hll.add("test-value")
        estimate2 = hll.estimate()

        assert estimate1 == estimate2


class TestCardinalityEstimatorProxy:
    """Test CardinalityEstimator proxy functionality."""

    def test_initialization(self):
        """Test proxy initialization."""
        estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
        )

        assert estimator.name == "test-estimator"
        assert estimator.service_name == SERVICE_NAME_CARDINALITY_ESTIMATOR

    def test_add_and_estimate(self):
        """Test add and estimate operations."""
        estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
        )

        estimator.add("user-1")
        estimator.add("user-2")
        estimator.add("user-3")

        estimate = estimator.estimate().result()
        assert 2 <= estimate <= 5

    def test_add_async(self):
        """Test async add operation."""
        estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
        )

        future = estimator.add_async("value")
        assert isinstance(future, Future)
        future.result()

    def test_estimate_async(self):
        """Test async estimate operation."""
        estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
        )

        estimator.add("value")
        future = estimator.estimate_async()

        assert isinstance(future, Future)
        result = future.result()
        assert result >= 0

    def test_add_all(self):
        """Test add_all operation."""
        estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
        )

        values = [f"user-{i}" for i in range(50)]
        estimator.add_all(values)

        estimate = estimator.estimate().result()
        assert 40 <= estimate <= 60

    def test_get_local_estimate(self):
        """Test get_local_estimate operation."""
        estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
        )

        for i in range(100):
            estimator.add(f"item-{i}")

        local_estimate = estimator.get_local_estimate()
        assert 80 <= local_estimate <= 120

    def test_duplicate_handling(self):
        """Test that duplicates are handled correctly."""
        estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
        )

        for _ in range(100):
            estimator.add("same-value")

        estimate = estimator.estimate().result()
        assert estimate <= 5

    def test_destroyed_proxy(self):
        """Test operations on destroyed proxy."""
        estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
        )
        estimator.destroy()

        with pytest.raises(Exception):
            estimator.add("value")

    def test_on_destroy_clears_hll(self):
        """Test that destroy clears local HLL."""
        estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
        )

        estimator.add("value")
        estimator._on_destroy()

        assert estimator._local_hll.estimate() == 0


class TestCardinalityEstimatorAlias:
    """Test CardinalityEstimator alias."""

    def test_alias_is_proxy_class(self):
        """Test that CardinalityEstimatorProxy is an alias."""
        assert CardinalityEstimatorProxy is CardinalityEstimator


class TestCardinalityEstimatorWithContext:
    """Test CardinalityEstimator with mock context."""

    def test_add_invokes_service(self):
        """Test that add invokes the service."""
        mock_context = MagicMock()
        mock_inv_service = MagicMock()
        mock_ser_service = MagicMock()
        mock_context.invocation_service = mock_inv_service
        mock_context.serialization_service = mock_ser_service
        mock_ser_service.to_data.return_value = b"serialized"

        future = Future()
        future.set_result(None)
        mock_inv_service.invoke.return_value = future

        estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
            context=mock_context,
        )

        estimator.add("value")

        mock_inv_service.invoke.assert_called()

    def test_estimate_invokes_service(self):
        """Test that estimate invokes the service."""
        mock_context = MagicMock()
        mock_inv_service = MagicMock()
        mock_context.invocation_service = mock_inv_service

        future = Future()
        future.set_result(100)
        mock_inv_service.invoke.return_value = future

        estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
            context=mock_context,
        )

        result_future = estimator.estimate()
        assert isinstance(result_future, Future)


class TestCardinalityEstimatorAccuracy:
    """Test CardinalityEstimator accuracy."""

    def test_small_cardinality(self):
        """Test accuracy for small cardinality."""
        estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
        )

        for i in range(10):
            estimator.add(f"item-{i}")

        estimate = estimator.estimate().result()
        assert 7 <= estimate <= 15

    def test_medium_cardinality(self):
        """Test accuracy for medium cardinality."""
        estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
        )

        for i in range(1000):
            estimator.add(f"item-{i}")

        estimate = estimator.estimate().result()
        error_rate = abs(estimate - 1000) / 1000
        assert error_rate < 0.1

    def test_large_cardinality(self):
        """Test accuracy for large cardinality."""
        estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
        )

        for i in range(50000):
            estimator.add(f"item-{i}")

        estimate = estimator.estimate().result()
        error_rate = abs(estimate - 50000) / 50000
        assert error_rate < 0.05

    def test_mixed_types(self):
        """Test with mixed types of values."""
        estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
        )

        estimator.add("string")
        estimator.add(123)
        estimator.add(45.67)
        estimator.add(b"bytes")
        estimator.add({"key": "value"})

        estimate = estimator.estimate().result()
        assert 3 <= estimate <= 7
