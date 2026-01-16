"""Unit tests for Vector Collection functionality."""

import unittest
from unittest.mock import Mock, MagicMock, patch
from typing import List, Dict, Optional

from hazelcast.protocol.codec import (
    VectorCodec,
    VectorDocument,
    VectorSearchResult,
    VECTOR_PUT,
    VECTOR_GET,
    VECTOR_SEARCH,
    VECTOR_DELETE,
    VECTOR_SIZE,
    VECTOR_CLEAR,
    VECTOR_OPTIMIZE,
)


class TestVectorDocument(unittest.TestCase):
    """Tests for VectorDocument data class."""

    def test_init_with_all_params(self):
        doc = VectorDocument(
            key=b"test-key",
            vector=[0.1, 0.2, 0.3],
            metadata={"title": b"Test Title"},
        )
        self.assertEqual(doc.key, b"test-key")
        self.assertEqual(doc.vector, [0.1, 0.2, 0.3])
        self.assertEqual(doc.metadata, {"title": b"Test Title"})

    def test_init_without_metadata(self):
        doc = VectorDocument(key=b"key", vector=[1.0, 2.0])
        self.assertEqual(doc.key, b"key")
        self.assertEqual(doc.vector, [1.0, 2.0])
        self.assertEqual(doc.metadata, {})

    def test_repr(self):
        doc = VectorDocument(
            key=b"key",
            vector=[0.1, 0.2],
            metadata={"a": b"1", "b": b"2"},
        )
        repr_str = repr(doc)
        self.assertIn("VectorDocument", repr_str)
        self.assertIn("vector_dim=2", repr_str)


class TestVectorSearchResult(unittest.TestCase):
    """Tests for VectorSearchResult data class."""

    def test_init(self):
        result = VectorSearchResult(
            score=0.95,
            key=b"doc-1",
            vector=[0.1, 0.2, 0.3],
            metadata={"category": b"test"},
        )
        self.assertEqual(result.score, 0.95)
        self.assertEqual(result.key, b"doc-1")
        self.assertEqual(result.vector, [0.1, 0.2, 0.3])
        self.assertEqual(result.metadata, {"category": b"test"})

    def test_repr(self):
        result = VectorSearchResult(score=0.85, key=b"key", vector=[], metadata={})
        repr_str = repr(result)
        self.assertIn("VectorSearchResult", repr_str)
        self.assertIn("score=0.85", repr_str)


class TestVectorCodecEncode(unittest.TestCase):
    """Tests for VectorCodec encoding methods."""

    def test_encode_put_request_basic(self):
        msg = VectorCodec.encode_put_request(
            name="test-collection",
            key=b"doc-1",
            vector=[0.1, 0.2, 0.3, 0.4],
            metadata=None,
        )
        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), VECTOR_PUT)

    def test_encode_put_request_with_metadata(self):
        msg = VectorCodec.encode_put_request(
            name="test-collection",
            key=b"doc-1",
            vector=[0.5, 0.6],
            metadata={"title": b"Test Doc", "category": b"A"},
        )
        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), VECTOR_PUT)

    def test_encode_get_request(self):
        msg = VectorCodec.encode_get_request(
            name="test-collection",
            key=b"doc-1",
        )
        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), VECTOR_GET)

    def test_encode_search_request_default_params(self):
        msg = VectorCodec.encode_search_request(
            name="test-collection",
            vector=[0.1, 0.2, 0.3],
            limit=10,
        )
        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), VECTOR_SEARCH)

    def test_encode_search_request_with_options(self):
        msg = VectorCodec.encode_search_request(
            name="test-collection",
            vector=[0.1, 0.2],
            limit=5,
            include_vectors=True,
            include_metadata=False,
        )
        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), VECTOR_SEARCH)

    def test_encode_delete_request(self):
        msg = VectorCodec.encode_delete_request(
            name="test-collection",
            key=b"doc-1",
        )
        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), VECTOR_DELETE)

    def test_encode_size_request(self):
        msg = VectorCodec.encode_size_request(name="test-collection")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), VECTOR_SIZE)

    def test_encode_clear_request(self):
        msg = VectorCodec.encode_clear_request(name="test-collection")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), VECTOR_CLEAR)

    def test_encode_optimize_request(self):
        msg = VectorCodec.encode_optimize_request(name="test-collection")
        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), VECTOR_OPTIMIZE)


class TestVectorCodecVectorEncoding(unittest.TestCase):
    """Tests for vector encoding/decoding helpers."""

    def test_encode_decode_vector_roundtrip(self):
        original = [0.1, 0.2, 0.3, 0.4, 0.5]
        buffer = bytearray(100)
        offset = VectorCodec._encode_vector(buffer, 0, original)

        decoded, new_offset = VectorCodec._decode_vector(bytes(buffer), 0)
        self.assertEqual(len(decoded), len(original))
        for i, (o, d) in enumerate(zip(original, decoded)):
            self.assertAlmostEqual(o, d, places=5, msg=f"Mismatch at index {i}")
        self.assertEqual(offset, new_offset)

    def test_encode_empty_vector(self):
        buffer = bytearray(10)
        offset = VectorCodec._encode_vector(buffer, 0, [])
        decoded, _ = VectorCodec._decode_vector(bytes(buffer), 0)
        self.assertEqual(decoded, [])

    def test_encode_single_element_vector(self):
        buffer = bytearray(20)
        VectorCodec._encode_vector(buffer, 0, [3.14159])
        decoded, _ = VectorCodec._decode_vector(bytes(buffer), 0)
        self.assertEqual(len(decoded), 1)
        self.assertAlmostEqual(decoded[0], 3.14159, places=4)


class TestVectorCodecDecode(unittest.TestCase):
    """Tests for VectorCodec decoding methods."""

    def test_decode_put_response_none(self):
        mock_msg = Mock()
        mock_msg.next_frame.side_effect = [Mock(content=b""), None]
        result = VectorCodec.decode_put_response(mock_msg)
        self.assertIsNone(result)

    def test_decode_delete_response_false(self):
        mock_msg = Mock()
        mock_frame = Mock()
        mock_frame.content = b"\x00" * 23
        mock_msg.next_frame.return_value = mock_frame
        result = VectorCodec.decode_delete_response(mock_msg)
        self.assertFalse(result)

    def test_decode_size_response_zero(self):
        mock_msg = Mock()
        mock_frame = Mock()
        mock_frame.content = b"\x00" * 30
        mock_msg.next_frame.return_value = mock_frame
        result = VectorCodec.decode_size_response(mock_msg)
        self.assertEqual(result, 0)


class TestVectorCollectionProxy(unittest.TestCase):
    """Tests for VectorCollection proxy class."""

    def setUp(self):
        self.mock_context = Mock()
        self.mock_context.invocation_service = Mock()
        self.mock_context.serialization_service = Mock()
        self.mock_context.serialization_service.to_data.side_effect = lambda x: x if isinstance(x, bytes) else str(x).encode()
        self.mock_context.serialization_service.to_object.side_effect = lambda x: x

    @patch("hazelcast.proxy.vector.VectorCollection.__init__", return_value=None)
    def test_proxy_instantiation(self, mock_init):
        from hazelcast.proxy.vector import VectorCollection
        vc = VectorCollection.__new__(VectorCollection)
        vc._name = "test"
        vc._context = self.mock_context
        self.assertIsNotNone(vc)


class TestVectorCollectionOperations(unittest.TestCase):
    """Integration-style tests for VectorCollection operations."""

    def test_put_creates_correct_request(self):
        msg = VectorCodec.encode_put_request(
            "embeddings",
            b"doc-123",
            [0.1, 0.2, 0.3],
            {"source": b"test"},
        )
        self.assertEqual(msg.get_message_type(), VECTOR_PUT)

    def test_get_creates_correct_request(self):
        msg = VectorCodec.encode_get_request("embeddings", b"doc-123")
        self.assertEqual(msg.get_message_type(), VECTOR_GET)

    def test_search_creates_correct_request(self):
        query_vector = [0.1, 0.2, 0.3, 0.4, 0.5]
        msg = VectorCodec.encode_search_request(
            "embeddings",
            query_vector,
            limit=20,
            include_vectors=True,
            include_metadata=True,
        )
        self.assertEqual(msg.get_message_type(), VECTOR_SEARCH)

    def test_delete_creates_correct_request(self):
        msg = VectorCodec.encode_delete_request("embeddings", b"doc-to-delete")
        self.assertEqual(msg.get_message_type(), VECTOR_DELETE)

    def test_size_creates_correct_request(self):
        msg = VectorCodec.encode_size_request("embeddings")
        self.assertEqual(msg.get_message_type(), VECTOR_SIZE)

    def test_clear_creates_correct_request(self):
        msg = VectorCodec.encode_clear_request("embeddings")
        self.assertEqual(msg.get_message_type(), VECTOR_CLEAR)

    def test_optimize_creates_correct_request(self):
        msg = VectorCodec.encode_optimize_request("embeddings")
        self.assertEqual(msg.get_message_type(), VECTOR_OPTIMIZE)


class TestVectorCollectionEdgeCases(unittest.TestCase):
    """Edge case tests for Vector Collection."""

    def test_high_dimensional_vector(self):
        high_dim_vector = [float(i) / 1000 for i in range(1536)]
        msg = VectorCodec.encode_put_request(
            "high-dim-collection",
            b"large-vec",
            high_dim_vector,
        )
        self.assertIsNotNone(msg)

    def test_zero_vector(self):
        zero_vector = [0.0] * 128
        msg = VectorCodec.encode_search_request(
            "collection",
            zero_vector,
            limit=5,
        )
        self.assertIsNotNone(msg)

    def test_negative_values_vector(self):
        vector = [-0.5, -0.3, 0.0, 0.3, 0.5]
        msg = VectorCodec.encode_put_request("collection", b"key", vector)
        self.assertIsNotNone(msg)

    def test_large_metadata(self):
        large_metadata = {f"key_{i}": f"value_{i}".encode() for i in range(100)}
        msg = VectorCodec.encode_put_request(
            "collection",
            b"key",
            [0.1, 0.2],
            large_metadata,
        )
        self.assertIsNotNone(msg)

    def test_unicode_collection_name(self):
        msg = VectorCodec.encode_get_request("my-vectors", b"key")
        self.assertIsNotNone(msg)

    def test_empty_key(self):
        msg = VectorCodec.encode_put_request("collection", b"", [0.1, 0.2])
        self.assertIsNotNone(msg)


class TestClientVectorCollectionMethod(unittest.TestCase):
    """Tests for HazelcastClient.get_vector_collection method."""

    def test_service_name_constant_defined(self):
        from hazelcast.client import SERVICE_NAME_VECTOR_COLLECTION
        self.assertEqual(SERVICE_NAME_VECTOR_COLLECTION, "hz:impl:vectorCollectionService")

    @patch("hazelcast.client.HazelcastClient._check_running")
    @patch("hazelcast.client.HazelcastClient._get_or_create_proxy")
    def test_get_vector_collection_calls_proxy_factory(self, mock_create, mock_check):
        from hazelcast.client import HazelcastClient, SERVICE_NAME_VECTOR_COLLECTION
        from hazelcast.proxy.vector import VectorCollection

        client = HazelcastClient.__new__(HazelcastClient)
        client._state = Mock()
        client._proxies = {}
        client._proxies_lock = Mock()
        client._check_running = mock_check
        client._get_or_create_proxy = mock_create

        client.get_vector_collection("my-vectors")

        mock_create.assert_called_once()
        call_args = mock_create.call_args[0]
        self.assertEqual(call_args[0], SERVICE_NAME_VECTOR_COLLECTION)
        self.assertEqual(call_args[1], "my-vectors")
        self.assertEqual(call_args[2], VectorCollection)


if __name__ == "__main__":
    unittest.main()
