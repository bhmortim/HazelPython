"""Tests for CardinalityEstimator proxy."""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future

from hazelcast.proxy.cardinality_estimator import (
    CardinalityEstimator,
    CardinalityEstimatorProxy,
    SERVICE_NAME_CARDINALITY_ESTIMATOR,
)
from hazelcast.proxy.base import ProxyContext
from hazelcast.protocol.builtin_codecs import CardinalityEstimatorCodec


class TestCardinalityEstimator(unittest.TestCase):
    """Unit tests for CardinalityEstimator proxy."""

    def setUp(self):
        self.mock_invocation_service = MagicMock()
        self.mock_serialization_service = MagicMock()
        self.mock_serialization_service.to_data = lambda x: str(x).encode("utf-8")

        self.context = ProxyContext(
            invocation_service=self.mock_invocation_service,
            serialization_service=self.mock_serialization_service,
        )

        self.estimator = CardinalityEstimator(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "test-estimator",
            self.context,
        )

    def test_service_name(self):
        self.assertEqual(
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
            "hz:impl:cardinalityEstimatorService",
        )

    def test_proxy_creation(self):
        self.assertEqual(self.estimator.name, "test-estimator")
        self.assertEqual(
            self.estimator.service_name,
            SERVICE_NAME_CARDINALITY_ESTIMATOR,
        )

    def test_proxy_alias(self):
        self.assertIs(CardinalityEstimatorProxy, CardinalityEstimator)

    def test_add_invokes_service(self):
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result = self.estimator.add("test-value")

        self.mock_invocation_service.invoke.assert_called_once()
        self.assertIsInstance(result, Future)

    def test_add_serializes_value(self):
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        self.estimator.add("user-123")

        self.mock_serialization_service.to_data.assert_called_with("user-123")

    def test_estimate_invokes_service(self):
        response_future = Future()
        mock_response = MagicMock()
        response_future.set_result(mock_response)
        self.mock_invocation_service.invoke.return_value = response_future

        result = self.estimator.estimate()

        self.mock_invocation_service.invoke.assert_called_once()
        self.assertIsInstance(result, Future)

    def test_add_async_delegates_to_add(self):
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result = self.estimator.add_async("value")

        self.assertIsInstance(result, Future)

    def test_estimate_async_delegates_to_estimate(self):
        response_future = Future()
        mock_response = MagicMock()
        response_future.set_result(mock_response)
        self.mock_invocation_service.invoke.return_value = response_future

        result = self.estimator.estimate_async()

        self.assertIsInstance(result, Future)

    def test_destroyed_estimator_raises(self):
        self.estimator.destroy()

        with self.assertRaises(Exception):
            self.estimator.add("value")

    def test_repr(self):
        repr_str = repr(self.estimator)
        self.assertIn("CardinalityEstimator", repr_str)
        self.assertIn("test-estimator", repr_str)


class TestCardinalityEstimatorCodec(unittest.TestCase):
    """Unit tests for CardinalityEstimator codec."""

    def test_encode_add_request(self):
        request = CardinalityEstimatorCodec.encode_add_request(
            "my-estimator",
            b"serialized-data",
        )

        self.assertIsNotNone(request)
        self.assertEqual(request.get_message_type(), 0x190100)

    def test_encode_estimate_request(self):
        request = CardinalityEstimatorCodec.encode_estimate_request("my-estimator")

        self.assertIsNotNone(request)
        self.assertEqual(request.get_message_type(), 0x190200)

    def test_decode_estimate_response_empty(self):
        mock_msg = MagicMock()
        mock_msg.next_frame.return_value = None

        result = CardinalityEstimatorCodec.decode_estimate_response(mock_msg)

        self.assertEqual(result, 0)

    def test_decode_estimate_response_short_frame(self):
        mock_msg = MagicMock()
        mock_frame = MagicMock()
        mock_frame.content = b"\x00" * 10
        mock_msg.next_frame.return_value = mock_frame

        result = CardinalityEstimatorCodec.decode_estimate_response(mock_msg)

        self.assertEqual(result, 0)


if __name__ == "__main__":
    unittest.main()
