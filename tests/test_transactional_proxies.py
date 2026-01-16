"""Tests for Transactional Proxy implementations."""

import unittest
from unittest.mock import MagicMock

from hazelcast.transaction import (
    TransactionContext,
    TransactionNotActiveException,
)
from hazelcast.proxy.transactional import (
    TransactionalProxy,
    TransactionalMap,
    TransactionalSet,
    TransactionalList,
    TransactionalQueue,
    TransactionalMultiMap,
)
from hazelcast.proxy.base import ProxyContext


class TestTransactionalProxy(unittest.TestCase):
    """Tests for TransactionalProxy base class."""

    def setUp(self):
        self.mock_context = ProxyContext(
            invocation_service=MagicMock(),
            serialization_service=MagicMock(),
        )
        self.txn_ctx = TransactionContext(self.mock_context)

    def test_name_property(self):
        self.txn_ctx.begin()
        txn_map = TransactionalMap("test-map", self.txn_ctx)
        self.assertEqual(txn_map.name, "test-map")

    def test_transaction_context_property(self):
        self.txn_ctx.begin()
        txn_map = TransactionalMap("test-map", self.txn_ctx)
        self.assertIs(txn_map.transaction_context, self.txn_ctx)

    def test_repr(self):
        self.txn_ctx.begin()
        txn_map = TransactionalMap("test-map", self.txn_ctx)
        self.assertIn("TransactionalMap", repr(txn_map))
        self.assertIn("test-map", repr(txn_map))


class TestTransactionalMapOperations(unittest.TestCase):
    """Tests for TransactionalMap operations."""

    def setUp(self):
        self.mock_context = ProxyContext(
            invocation_service=MagicMock(),
            serialization_service=MagicMock(),
        )
        self.txn_ctx = TransactionContext(self.mock_context)
        self.txn_ctx.begin()
        self.txn_map = self.txn_ctx.get_map("test-map")

    def test_put_and_get(self):
        self.txn_map.put("key1", "value1")
        self.assertEqual(self.txn_map.get("key1"), "value1")

    def test_put_returns_old_value(self):
        self.txn_map.put("key1", "value1")
        old = self.txn_map.put("key1", "value2")
        self.assertEqual(old, "value1")
        self.assertEqual(self.txn_map.get("key1"), "value2")

    def test_get_nonexistent_returns_none(self):
        self.assertIsNone(self.txn_map.get("nonexistent"))

    def test_remove_returns_old_value(self):
        self.txn_map.put("key1", "value1")
        old = self.txn_map.remove("key1")
        self.assertEqual(old, "value1")
        self.assertIsNone(self.txn_map.get("key1"))

    def test_remove_nonexistent_returns_none(self):
        self.assertIsNone(self.txn_map.remove("nonexistent"))

    def test_contains_key(self):
        self.assertFalse(self.txn_map.contains_key("key1"))
        self.txn_map.put("key1", "value1")
        self.assertTrue(self.txn_map.contains_key("key1"))

    def test_contains_key_after_delete(self):
        self.txn_map.put("key1", "value1")
        self.txn_map.delete("key1")
        self.assertFalse(self.txn_map.contains_key("key1"))

    def test_size(self):
        self.assertEqual(self.txn_map.size(), 0)
        self.txn_map.put("key1", "value1")
        self.assertEqual(self.txn_map.size(), 1)
        self.txn_map.put("key2", "value2")
        self.assertEqual(self.txn_map.size(), 2)

    def test_is_empty(self):
        self.assertTrue(self.txn_map.is_empty())
        self.txn_map.put("key1", "value1")
        self.assertFalse(self.txn_map.is_empty())

    def test_key_set(self):
        self.txn_map.put("key1", "value1")
        self.txn_map.put("key2", "value2")
        keys = self.txn_map.key_set()
        self.assertEqual(keys, {"key1", "key2"})

    def test_values(self):
        self.txn_map.put("key1", "value1")
        self.txn_map.put("key2", "value2")
        values = self.txn_map.values()
        self.assertEqual(sorted(values), ["value1", "value2"])

    def test_put_if_absent_when_absent(self):
        result = self.txn_map.put_if_absent("key1", "value1")
        self.assertIsNone(result)
        self.assertEqual(self.txn_map.get("key1"), "value1")

    def test_put_if_absent_when_present(self):
        self.txn_map.put("key1", "value1")
        result = self.txn_map.put_if_absent("key1", "value2")
        self.assertEqual(result, "value1")
        self.assertEqual(self.txn_map.get("key1"), "value1")

    def test_replace_when_exists(self):
        self.txn_map.put("key1", "value1")
        old = self.txn_map.replace("key1", "value2")
        self.assertEqual(old, "value1")
        self.assertEqual(self.txn_map.get("key1"), "value2")

    def test_replace_when_not_exists(self):
        old = self.txn_map.replace("key1", "value1")
        self.assertIsNone(old)

    def test_replace_if_same_success(self):
        self.txn_map.put("key1", "value1")
        result = self.txn_map.replace_if_same("key1", "value1", "value2")
        self.assertTrue(result)
        self.assertEqual(self.txn_map.get("key1"), "value2")

    def test_replace_if_same_failure(self):
        self.txn_map.put("key1", "value1")
        result = self.txn_map.replace_if_same("key1", "wrong", "value2")
        self.assertFalse(result)
        self.assertEqual(self.txn_map.get("key1"), "value1")

    def test_set(self):
        self.txn_map.set("key1", "value1")
        self.assertEqual(self.txn_map.get("key1"), "value1")

    def test_delete(self):
        self.txn_map.put("key1", "value1")
        self.txn_map.delete("key1")
        self.assertIsNone(self.txn_map.get("key1"))

    def test_get_for_update(self):
        self.txn_map.put("key1", "value1")
        value = self.txn_map.get_for_update("key1")
        self.assertEqual(value, "value1")


class TestTransactionalSetOperations(unittest.TestCase):
    """Tests for TransactionalSet operations."""

    def setUp(self):
        self.mock_context = ProxyContext(
            invocation_service=MagicMock(),
            serialization_service=MagicMock(),
        )
        self.txn_ctx = TransactionContext(self.mock_context)
        self.txn_ctx.begin()
        self.txn_set = self.txn_ctx.get_set("test-set")

    def test_add_new_item(self):
        result = self.txn_set.add("item1")
        self.assertTrue(result)

    def test_add_duplicate_item(self):
        self.txn_set.add("item1")
        result = self.txn_set.add("item1")
        self.assertFalse(result)

    def test_remove_existing_item(self):
        self.txn_set.add("item1")
        result = self.txn_set.remove("item1")
        self.assertTrue(result)

    def test_remove_nonexistent_item(self):
        result = self.txn_set.remove("nonexistent")
        self.assertFalse(result)

    def test_contains(self):
        self.assertFalse(self.txn_set.contains("item1"))
        self.txn_set.add("item1")
        self.assertTrue(self.txn_set.contains("item1"))

    def test_size(self):
        self.assertEqual(self.txn_set.size(), 0)
        self.txn_set.add("item1")
        self.assertEqual(self.txn_set.size(), 1)
        self.txn_set.add("item2")
        self.assertEqual(self.txn_set.size(), 2)
        self.txn_set.add("item1")  # Duplicate
        self.assertEqual(self.txn_set.size(), 2)

    def test_is_empty(self):
        self.assertTrue(self.txn_set.is_empty())
        self.txn_set.add("item1")
        self.assertFalse(self.txn_set.is_empty())


class TestTransactionalListOperations(unittest.TestCase):
    """Tests for TransactionalList operations."""

    def setUp(self):
        self.mock_context = ProxyContext(
            invocation_service=MagicMock(),
            serialization_service=MagicMock(),
        )
        self.txn_ctx = TransactionContext(self.mock_context)
        self.txn_ctx.begin()
        self.txn_list = self.txn_ctx.get_list("test-list")

    def test_add(self):
        result = self.txn_list.add("item1")
        self.assertTrue(result)

    def test_add_duplicates_allowed(self):
        self.txn_list.add("item1")
        result = self.txn_list.add("item1")
        self.assertTrue(result)
        self.assertEqual(self.txn_list.size(), 2)

    def test_remove_existing_item(self):
        self.txn_list.add("item1")
        result = self.txn_list.remove("item1")
        self.assertTrue(result)

    def test_remove_nonexistent_item(self):
        result = self.txn_list.remove("nonexistent")
        self.assertFalse(result)

    def test_remove_only_first_occurrence(self):
        self.txn_list.add("item1")
        self.txn_list.add("item1")
        self.txn_list.remove("item1")
        self.assertEqual(self.txn_list.size(), 1)

    def test_contains(self):
        self.assertFalse(self.txn_list.contains("item1"))
        self.txn_list.add("item1")
        self.assertTrue(self.txn_list.contains("item1"))

    def test_size(self):
        self.assertEqual(self.txn_list.size(), 0)
        self.txn_list.add("item1")
        self.assertEqual(self.txn_list.size(), 1)

    def test_is_empty(self):
        self.assertTrue(self.txn_list.is_empty())
        self.txn_list.add("item1")
        self.assertFalse(self.txn_list.is_empty())


class TestTransactionalQueueOperations(unittest.TestCase):
    """Tests for TransactionalQueue operations."""

    def setUp(self):
        self.mock_context = ProxyContext(
            invocation_service=MagicMock(),
            serialization_service=MagicMock(),
        )
        self.txn_ctx = TransactionContext(self.mock_context)
        self.txn_ctx.begin()
        self.txn_queue = self.txn_ctx.get_queue("test-queue")

    def test_offer(self):
        result = self.txn_queue.offer("item1")
        self.assertTrue(result)

    def test_poll_returns_head(self):
        self.txn_queue.offer("item1")
        self.txn_queue.offer("item2")
        result = self.txn_queue.poll()
        self.assertEqual(result, "item1")

    def test_poll_empty_returns_none(self):
        result = self.txn_queue.poll()
        self.assertIsNone(result)

    def test_peek_returns_head_without_removing(self):
        self.txn_queue.offer("item1")
        result = self.txn_queue.peek()
        self.assertEqual(result, "item1")
        self.assertEqual(self.txn_queue.size(), 1)

    def test_peek_empty_returns_none(self):
        result = self.txn_queue.peek()
        self.assertIsNone(result)

    def test_take(self):
        self.txn_queue.offer("item1")
        result = self.txn_queue.take()
        self.assertEqual(result, "item1")

    def test_size(self):
        self.assertEqual(self.txn_queue.size(), 0)
        self.txn_queue.offer("item1")
        self.assertEqual(self.txn_queue.size(), 1)

    def test_is_empty(self):
        self.assertTrue(self.txn_queue.is_empty())
        self.txn_queue.offer("item1")
        self.assertFalse(self.txn_queue.is_empty())


class TestTransactionalMultiMapOperations(unittest.TestCase):
    """Tests for TransactionalMultiMap operations."""

    def setUp(self):
        self.mock_context = ProxyContext(
            invocation_service=MagicMock(),
            serialization_service=MagicMock(),
        )
        self.txn_ctx = TransactionContext(self.mock_context)
        self.txn_ctx.begin()
        self.txn_mm = self.txn_ctx.get_multi_map("test-multimap")

    def test_put(self):
        result = self.txn_mm.put("key1", "value1")
        self.assertTrue(result)

    def test_put_multiple_values_same_key(self):
        self.txn_mm.put("key1", "value1")
        self.txn_mm.put("key1", "value2")
        values = self.txn_mm.get("key1")
        self.assertEqual(values, ["value1", "value2"])

    def test_get_nonexistent_returns_empty_list(self):
        values = self.txn_mm.get("nonexistent")
        self.assertEqual(values, [])

    def test_remove_specific_entry(self):
        self.txn_mm.put("key1", "value1")
        self.txn_mm.put("key1", "value2")
        result = self.txn_mm.remove("key1", "value1")
        self.assertTrue(result)
        self.assertEqual(self.txn_mm.get("key1"), ["value2"])

    def test_remove_nonexistent_entry(self):
        result = self.txn_mm.remove("key1", "value1")
        self.assertFalse(result)

    def test_remove_all(self):
        self.txn_mm.put("key1", "value1")
        self.txn_mm.put("key1", "value2")
        removed = self.txn_mm.remove_all("key1")
        self.assertEqual(removed, ["value1", "value2"])
        self.assertEqual(self.txn_mm.get("key1"), [])

    def test_contains_key(self):
        self.assertFalse(self.txn_mm.contains_key("key1"))
        self.txn_mm.put("key1", "value1")
        self.assertTrue(self.txn_mm.contains_key("key1"))

    def test_contains_value(self):
        self.assertFalse(self.txn_mm.contains_value("value1"))
        self.txn_mm.put("key1", "value1")
        self.assertTrue(self.txn_mm.contains_value("value1"))

    def test_contains_entry(self):
        self.assertFalse(self.txn_mm.contains_entry("key1", "value1"))
        self.txn_mm.put("key1", "value1")
        self.assertTrue(self.txn_mm.contains_entry("key1", "value1"))
        self.assertFalse(self.txn_mm.contains_entry("key1", "value2"))

    def test_value_count(self):
        self.assertEqual(self.txn_mm.value_count("key1"), 0)
        self.txn_mm.put("key1", "value1")
        self.assertEqual(self.txn_mm.value_count("key1"), 1)
        self.txn_mm.put("key1", "value2")
        self.assertEqual(self.txn_mm.value_count("key1"), 2)

    def test_size(self):
        self.assertEqual(self.txn_mm.size(), 0)
        self.txn_mm.put("key1", "value1")
        self.assertEqual(self.txn_mm.size(), 1)
        self.txn_mm.put("key1", "value2")
        self.assertEqual(self.txn_mm.size(), 2)
        self.txn_mm.put("key2", "value3")
        self.assertEqual(self.txn_mm.size(), 3)

    def test_is_empty(self):
        self.assertTrue(self.txn_mm.is_empty())
        self.txn_mm.put("key1", "value1")
        self.assertFalse(self.txn_mm.is_empty())

    def test_key_set(self):
        self.txn_mm.put("key1", "value1")
        self.txn_mm.put("key2", "value2")
        keys = self.txn_mm.key_set()
        self.assertEqual(keys, {"key1", "key2"})

    def test_values(self):
        self.txn_mm.put("key1", "value1")
        self.txn_mm.put("key2", "value2")
        values = self.txn_mm.values()
        self.assertEqual(sorted(values), ["value1", "value2"])


class TestTransactionalProxyIsolation(unittest.TestCase):
    """Tests for transaction isolation."""

    def setUp(self):
        self.mock_context = ProxyContext(
            invocation_service=MagicMock(),
            serialization_service=MagicMock(),
        )

    def test_operations_require_active_transaction(self):
        txn_ctx = TransactionContext(self.mock_context)
        txn_ctx.begin()
        txn_map = txn_ctx.get_map("test-map")
        txn_ctx.commit()

        with self.assertRaises(TransactionNotActiveException):
            txn_map.put("key", "value")

    def test_changes_visible_within_transaction(self):
        txn_ctx = TransactionContext(self.mock_context)
        txn_ctx.begin()
        txn_map = txn_ctx.get_map("test-map")

        txn_map.put("key1", "value1")
        self.assertEqual(txn_map.get("key1"), "value1")

    def test_deleted_keys_not_visible(self):
        txn_ctx = TransactionContext(self.mock_context)
        txn_ctx.begin()
        txn_map = txn_ctx.get_map("test-map")

        txn_map.put("key1", "value1")
        txn_map.delete("key1")
        self.assertIsNone(txn_map.get("key1"))
        self.assertFalse(txn_map.contains_key("key1"))


class TestTransactionalMapCodec(unittest.TestCase):
    """Tests for TransactionalMapCodec encode/decode methods."""

    def test_encode_put_request_message_type(self):
        """Test that put request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalMapCodec, TXN_MAP_PUT

        txn_id = uuid.uuid4()
        msg = TransactionalMapCodec.encode_put_request(
            name="test-map",
            txn_id=txn_id,
            thread_id=12345,
            key=b"key",
            value=b"value",
            ttl=-1,
        )

        frame = msg.next_frame()
        assert frame is not None
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_MAP_PUT)

    def test_encode_get_request_message_type(self):
        """Test that get request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalMapCodec, TXN_MAP_GET

        txn_id = uuid.uuid4()
        msg = TransactionalMapCodec.encode_get_request(
            name="test-map",
            txn_id=txn_id,
            thread_id=12345,
            key=b"key",
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_MAP_GET)

    def test_encode_remove_request_message_type(self):
        """Test that remove request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalMapCodec, TXN_MAP_REMOVE

        txn_id = uuid.uuid4()
        msg = TransactionalMapCodec.encode_remove_request(
            name="test-map",
            txn_id=txn_id,
            thread_id=12345,
            key=b"key",
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_MAP_REMOVE)

    def test_encode_size_request_message_type(self):
        """Test that size request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalMapCodec, TXN_MAP_SIZE

        txn_id = uuid.uuid4()
        msg = TransactionalMapCodec.encode_size_request(
            name="test-map",
            txn_id=txn_id,
            thread_id=12345,
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_MAP_SIZE)

    def test_encode_contains_key_request_message_type(self):
        """Test that containsKey request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalMapCodec, TXN_MAP_CONTAINS_KEY

        txn_id = uuid.uuid4()
        msg = TransactionalMapCodec.encode_contains_key_request(
            name="test-map",
            txn_id=txn_id,
            thread_id=12345,
            key=b"key",
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_MAP_CONTAINS_KEY)

    def test_encode_replace_if_same_request_message_type(self):
        """Test that replaceIfSame request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalMapCodec, TXN_MAP_REPLACE_IF_SAME

        txn_id = uuid.uuid4()
        msg = TransactionalMapCodec.encode_replace_if_same_request(
            name="test-map",
            txn_id=txn_id,
            thread_id=12345,
            key=b"key",
            old_value=b"old",
            new_value=b"new",
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_MAP_REPLACE_IF_SAME)


class TestTransactionalListCodec(unittest.TestCase):
    """Tests for TransactionalListCodec encode/decode methods."""

    def test_encode_add_request_message_type(self):
        """Test that add request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalListCodec, TXN_LIST_ADD

        txn_id = uuid.uuid4()
        msg = TransactionalListCodec.encode_add_request(
            name="test-list",
            txn_id=txn_id,
            thread_id=12345,
            value=b"item",
        )

        frame = msg.next_frame()
        assert frame is not None
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_LIST_ADD)

    def test_encode_remove_request_message_type(self):
        """Test that remove request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalListCodec, TXN_LIST_REMOVE

        txn_id = uuid.uuid4()
        msg = TransactionalListCodec.encode_remove_request(
            name="test-list",
            txn_id=txn_id,
            thread_id=12345,
            value=b"item",
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_LIST_REMOVE)

    def test_encode_size_request_message_type(self):
        """Test that size request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalListCodec, TXN_LIST_SIZE

        txn_id = uuid.uuid4()
        msg = TransactionalListCodec.encode_size_request(
            name="test-list",
            txn_id=txn_id,
            thread_id=12345,
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_LIST_SIZE)


class TestTransactionalSetCodec(unittest.TestCase):
    """Tests for TransactionalSetCodec encode/decode methods."""

    def test_encode_add_request_message_type(self):
        """Test that add request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalSetCodec, TXN_SET_ADD

        txn_id = uuid.uuid4()
        msg = TransactionalSetCodec.encode_add_request(
            name="test-set",
            txn_id=txn_id,
            thread_id=12345,
            value=b"item",
        )

        frame = msg.next_frame()
        assert frame is not None
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_SET_ADD)

    def test_encode_remove_request_message_type(self):
        """Test that remove request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalSetCodec, TXN_SET_REMOVE

        txn_id = uuid.uuid4()
        msg = TransactionalSetCodec.encode_remove_request(
            name="test-set",
            txn_id=txn_id,
            thread_id=12345,
            value=b"item",
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_SET_REMOVE)

    def test_encode_size_request_message_type(self):
        """Test that size request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalSetCodec, TXN_SET_SIZE

        txn_id = uuid.uuid4()
        msg = TransactionalSetCodec.encode_size_request(
            name="test-set",
            txn_id=txn_id,
            thread_id=12345,
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_SET_SIZE)


class TestTransactionalQueueCodec(unittest.TestCase):
    """Tests for TransactionalQueueCodec encode/decode methods."""

    def test_encode_offer_request_message_type(self):
        """Test that offer request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalQueueCodec, TXN_QUEUE_OFFER

        txn_id = uuid.uuid4()
        msg = TransactionalQueueCodec.encode_offer_request(
            name="test-queue",
            txn_id=txn_id,
            thread_id=12345,
            value=b"item",
            timeout_millis=0,
        )

        frame = msg.next_frame()
        assert frame is not None
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_QUEUE_OFFER)

    def test_encode_poll_request_message_type(self):
        """Test that poll request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalQueueCodec, TXN_QUEUE_POLL

        txn_id = uuid.uuid4()
        msg = TransactionalQueueCodec.encode_poll_request(
            name="test-queue",
            txn_id=txn_id,
            thread_id=12345,
            timeout_millis=0,
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_QUEUE_POLL)

    def test_encode_take_request_message_type(self):
        """Test that take request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalQueueCodec, TXN_QUEUE_TAKE

        txn_id = uuid.uuid4()
        msg = TransactionalQueueCodec.encode_take_request(
            name="test-queue",
            txn_id=txn_id,
            thread_id=12345,
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_QUEUE_TAKE)

    def test_encode_peek_request_message_type(self):
        """Test that peek request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalQueueCodec, TXN_QUEUE_PEEK

        txn_id = uuid.uuid4()
        msg = TransactionalQueueCodec.encode_peek_request(
            name="test-queue",
            txn_id=txn_id,
            thread_id=12345,
            timeout_millis=0,
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_QUEUE_PEEK)

    def test_encode_size_request_message_type(self):
        """Test that size request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalQueueCodec, TXN_QUEUE_SIZE

        txn_id = uuid.uuid4()
        msg = TransactionalQueueCodec.encode_size_request(
            name="test-queue",
            txn_id=txn_id,
            thread_id=12345,
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_QUEUE_SIZE)


class TestTransactionalMultiMapCodec(unittest.TestCase):
    """Tests for TransactionalMultiMapCodec encode/decode methods."""

    def test_encode_put_request_message_type(self):
        """Test that put request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalMultiMapCodec, TXN_MULTI_MAP_PUT

        txn_id = uuid.uuid4()
        msg = TransactionalMultiMapCodec.encode_put_request(
            name="test-multimap",
            txn_id=txn_id,
            thread_id=12345,
            key=b"key",
            value=b"value",
        )

        frame = msg.next_frame()
        assert frame is not None
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_MULTI_MAP_PUT)

    def test_encode_get_request_message_type(self):
        """Test that get request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalMultiMapCodec, TXN_MULTI_MAP_GET

        txn_id = uuid.uuid4()
        msg = TransactionalMultiMapCodec.encode_get_request(
            name="test-multimap",
            txn_id=txn_id,
            thread_id=12345,
            key=b"key",
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_MULTI_MAP_GET)

    def test_encode_remove_request_message_type(self):
        """Test that remove request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalMultiMapCodec, TXN_MULTI_MAP_REMOVE

        txn_id = uuid.uuid4()
        msg = TransactionalMultiMapCodec.encode_remove_request(
            name="test-multimap",
            txn_id=txn_id,
            thread_id=12345,
            key=b"key",
            value=b"value",
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_MULTI_MAP_REMOVE)

    def test_encode_remove_all_request_message_type(self):
        """Test that removeAll request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalMultiMapCodec, TXN_MULTI_MAP_REMOVE_ALL

        txn_id = uuid.uuid4()
        msg = TransactionalMultiMapCodec.encode_remove_all_request(
            name="test-multimap",
            txn_id=txn_id,
            thread_id=12345,
            key=b"key",
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_MULTI_MAP_REMOVE_ALL)

    def test_encode_value_count_request_message_type(self):
        """Test that valueCount request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalMultiMapCodec, TXN_MULTI_MAP_VALUE_COUNT

        txn_id = uuid.uuid4()
        msg = TransactionalMultiMapCodec.encode_value_count_request(
            name="test-multimap",
            txn_id=txn_id,
            thread_id=12345,
            key=b"key",
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_MULTI_MAP_VALUE_COUNT)

    def test_encode_size_request_message_type(self):
        """Test that size request has correct message type."""
        import struct
        import uuid
        from hazelcast.protocol.codec import TransactionalMultiMapCodec, TXN_MULTI_MAP_SIZE

        txn_id = uuid.uuid4()
        msg = TransactionalMultiMapCodec.encode_size_request(
            name="test-multimap",
            txn_id=txn_id,
            thread_id=12345,
        )

        frame = msg.next_frame()
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, TXN_MULTI_MAP_SIZE)


if __name__ == "__main__":
    unittest.main()
