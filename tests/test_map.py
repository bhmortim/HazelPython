"""Unit tests for MapProxy operations."""

import struct
import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from hazelcast.protocol.codec import (
    MapCodec,
    REQUEST_HEADER_SIZE,
    RESPONSE_HEADER_SIZE,
    LONG_SIZE,
    INT_SIZE,
    BOOLEAN_SIZE,
    MAP_AGGREGATE,
    MAP_AGGREGATE_WITH_PREDICATE,
    MAP_PROJECT,
    MAP_PROJECT_WITH_PREDICATE,
    MAP_PUT_WITH_MAX_IDLE,
    MAP_SET_WITH_MAX_IDLE,
    MAP_FETCH_ENTRIES,
    MAP_IS_LOADED,
)
from hazelcast.protocol.client_message import ClientMessage, Frame
from hazelcast.proxy.map import MapProxy, IndexType, IndexConfig, EntryView, EntryEvent
from hazelcast.proxy.base import ProxyContext


class TestMapCodec(unittest.TestCase):
    """Test MapCodec encode/decode methods."""

    def test_encode_aggregate_request(self):
        """Test encoding a Map.aggregate request."""
        aggregator_data = b"test_aggregator"
        msg = MapCodec.encode_aggregate_request("test-map", aggregator_data)

        self.assertIsInstance(msg, ClientMessage)
        frame = msg._frames[0]
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, MAP_AGGREGATE)

    def test_encode_aggregate_with_predicate_request(self):
        """Test encoding a Map.aggregateWithPredicate request."""
        aggregator_data = b"test_aggregator"
        predicate_data = b"test_predicate"
        msg = MapCodec.encode_aggregate_with_predicate_request(
            "test-map", aggregator_data, predicate_data
        )

        self.assertIsInstance(msg, ClientMessage)
        frame = msg._frames[0]
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, MAP_AGGREGATE_WITH_PREDICATE)

    def test_encode_project_request(self):
        """Test encoding a Map.project request."""
        projection_data = b"test_projection"
        msg = MapCodec.encode_project_request("test-map", projection_data)

        self.assertIsInstance(msg, ClientMessage)
        frame = msg._frames[0]
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, MAP_PROJECT)

    def test_encode_project_with_predicate_request(self):
        """Test encoding a Map.projectWithPredicate request."""
        projection_data = b"test_projection"
        predicate_data = b"test_predicate"
        msg = MapCodec.encode_project_with_predicate_request(
            "test-map", projection_data, predicate_data
        )

        self.assertIsInstance(msg, ClientMessage)
        frame = msg._frames[0]
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, MAP_PROJECT_WITH_PREDICATE)

    def test_encode_put_with_max_idle_request(self):
        """Test encoding a Map.putWithMaxIdle request."""
        msg = MapCodec.encode_put_with_max_idle_request(
            "test-map", b"key", b"value", 0, 5000, 1000
        )

        self.assertIsInstance(msg, ClientMessage)
        frame = msg._frames[0]
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, MAP_PUT_WITH_MAX_IDLE)

        thread_id = struct.unpack_from("<q", frame.buf, REQUEST_HEADER_SIZE)[0]
        self.assertEqual(thread_id, 0)

        ttl = struct.unpack_from("<q", frame.buf, REQUEST_HEADER_SIZE + LONG_SIZE)[0]
        self.assertEqual(ttl, 5000)

        max_idle = struct.unpack_from("<q", frame.buf, REQUEST_HEADER_SIZE + 2 * LONG_SIZE)[0]
        self.assertEqual(max_idle, 1000)

    def test_encode_set_with_max_idle_request(self):
        """Test encoding a Map.setWithMaxIdle request."""
        msg = MapCodec.encode_set_with_max_idle_request(
            "test-map", b"key", b"value", 0, 3000, 500
        )

        self.assertIsInstance(msg, ClientMessage)
        frame = msg._frames[0]
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, MAP_SET_WITH_MAX_IDLE)

    def test_encode_fetch_entries_request(self):
        """Test encoding a Map.fetchEntries request."""
        msg = MapCodec.encode_fetch_entries_request("test-map", 5, 10, 100)

        self.assertIsInstance(msg, ClientMessage)
        frame = msg._frames[0]
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, MAP_FETCH_ENTRIES)

        partition_id = struct.unpack_from("<i", frame.buf, 12)[0]
        self.assertEqual(partition_id, 5)

        table_index = struct.unpack_from("<i", frame.buf, REQUEST_HEADER_SIZE)[0]
        self.assertEqual(table_index, 10)

        batch_size = struct.unpack_from("<i", frame.buf, REQUEST_HEADER_SIZE + INT_SIZE)[0]
        self.assertEqual(batch_size, 100)

    def test_encode_is_loaded_request(self):
        """Test encoding a Map.isLoaded request."""
        msg = MapCodec.encode_is_loaded_request("test-map")

        self.assertIsInstance(msg, ClientMessage)
        frame = msg._frames[0]
        message_type = struct.unpack_from("<I", frame.buf, 0)[0]
        self.assertEqual(message_type, MAP_IS_LOADED)

    def test_decode_aggregate_response(self):
        """Test decoding a Map.aggregate response."""
        msg = ClientMessage.create_for_encode()
        header = bytearray(RESPONSE_HEADER_SIZE)
        msg.add_frame(Frame(bytes(header)))
        msg.add_frame(Frame(b"result_data"))

        msg._read_index = 0
        result = MapCodec.decode_aggregate_response(msg)
        self.assertEqual(result, b"result_data")

    def test_decode_aggregate_response_null(self):
        """Test decoding a null Map.aggregate response."""
        msg = ClientMessage.create_for_encode()
        header = bytearray(RESPONSE_HEADER_SIZE)
        msg.add_frame(Frame(bytes(header)))

        from hazelcast.protocol.client_message import NULL_FRAME
        msg.add_frame(NULL_FRAME)

        msg._read_index = 0
        result = MapCodec.decode_aggregate_response(msg)
        self.assertIsNone(result)

    def test_decode_is_loaded_response_true(self):
        """Test decoding a Map.isLoaded response (true)."""
        msg = ClientMessage.create_for_encode()
        header = bytearray(RESPONSE_HEADER_SIZE + BOOLEAN_SIZE)
        struct.pack_into("<B", header, RESPONSE_HEADER_SIZE, 1)
        msg.add_frame(Frame(bytes(header)))

        msg._read_index = 0
        result = MapCodec.decode_is_loaded_response(msg)
        self.assertTrue(result)

    def test_decode_is_loaded_response_false(self):
        """Test decoding a Map.isLoaded response (false)."""
        msg = ClientMessage.create_for_encode()
        header = bytearray(RESPONSE_HEADER_SIZE + BOOLEAN_SIZE)
        struct.pack_into("<B", header, RESPONSE_HEADER_SIZE, 0)
        msg.add_frame(Frame(bytes(header)))

        msg._read_index = 0
        result = MapCodec.decode_is_loaded_response(msg)
        self.assertFalse(result)

    def test_decode_put_with_max_idle_response(self):
        """Test decoding a Map.putWithMaxIdle response."""
        msg = ClientMessage.create_for_encode()
        header = bytearray(RESPONSE_HEADER_SIZE)
        msg.add_frame(Frame(bytes(header)))
        msg.add_frame(Frame(b"old_value"))

        msg._read_index = 0
        result = MapCodec.decode_put_with_max_idle_response(msg)
        self.assertEqual(result, b"old_value")

    def test_decode_fetch_entries_response(self):
        """Test decoding a Map.fetchEntries response."""
        msg = ClientMessage.create_for_encode()

        header = bytearray(RESPONSE_HEADER_SIZE + INT_SIZE)
        struct.pack_into("<i", header, RESPONSE_HEADER_SIZE, 42)
        msg.add_frame(Frame(bytes(header)))

        from hazelcast.protocol.client_message import BEGIN_FRAME, END_FRAME
        msg.add_frame(BEGIN_FRAME)
        msg.add_frame(Frame(b"key1"))
        msg.add_frame(Frame(b"value1"))
        msg.add_frame(END_FRAME)

        msg._read_index = 0
        entries, next_index = MapCodec.decode_fetch_entries_response(msg)

        self.assertEqual(next_index, 42)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0], (b"key1", b"value1"))


class TestIndexConfig(unittest.TestCase):
    """Test IndexConfig class."""

    def test_index_config_creation(self):
        """Test creating an IndexConfig."""
        config = IndexConfig(["attr1", "attr2"], IndexType.HASH, "my-index")

        self.assertEqual(config.name, "my-index")
        self.assertEqual(config.type, IndexType.HASH)
        self.assertEqual(config.attributes, ["attr1", "attr2"])

    def test_index_config_defaults(self):
        """Test IndexConfig default values."""
        config = IndexConfig(["attr"])

        self.assertIsNone(config.name)
        self.assertEqual(config.type, IndexType.SORTED)
        self.assertEqual(config.attributes, ["attr"])


class TestEntryView(unittest.TestCase):
    """Test EntryView class."""

    def test_entry_view_creation(self):
        """Test creating an EntryView."""
        view = EntryView(
            key="test-key",
            value="test-value",
            cost=100,
            creation_time=1000,
            expiration_time=2000,
            hits=5,
            last_access_time=1500,
            last_stored_time=1200,
            last_update_time=1400,
            version=3,
            ttl=1000,
            max_idle=500,
        )

        self.assertEqual(view.key, "test-key")
        self.assertEqual(view.value, "test-value")
        self.assertEqual(view.cost, 100)
        self.assertEqual(view.creation_time, 1000)
        self.assertEqual(view.expiration_time, 2000)
        self.assertEqual(view.hits, 5)
        self.assertEqual(view.last_access_time, 1500)
        self.assertEqual(view.last_stored_time, 1200)
        self.assertEqual(view.last_update_time, 1400)
        self.assertEqual(view.version, 3)
        self.assertEqual(view.ttl, 1000)
        self.assertEqual(view.max_idle, 500)

    def test_entry_view_repr(self):
        """Test EntryView string representation."""
        view = EntryView(key="k", value="v", hits=10, version=2)
        repr_str = repr(view)
        self.assertIn("k", repr_str)
        self.assertIn("v", repr_str)
        self.assertIn("10", repr_str)
        self.assertIn("2", repr_str)


class TestEntryEvent(unittest.TestCase):
    """Test EntryEvent class."""

    def test_entry_event_creation(self):
        """Test creating an EntryEvent."""
        event = EntryEvent(
            event_type=EntryEvent.ADDED,
            key="test-key",
            value="new-value",
            old_value="old-value",
        )

        self.assertEqual(event.event_type, EntryEvent.ADDED)
        self.assertEqual(event.key, "test-key")
        self.assertEqual(event.value, "new-value")
        self.assertEqual(event.old_value, "old-value")

    def test_entry_event_types(self):
        """Test EntryEvent type constants."""
        self.assertEqual(EntryEvent.ADDED, 1)
        self.assertEqual(EntryEvent.REMOVED, 2)
        self.assertEqual(EntryEvent.UPDATED, 4)
        self.assertEqual(EntryEvent.EVICTED, 8)
        self.assertEqual(EntryEvent.EXPIRED, 16)


class TestMapProxy(unittest.TestCase):
    """Test MapProxy methods."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_invocation = MagicMock()
        self.mock_serialization = MagicMock()

        self.mock_serialization.to_data = lambda x: str(x).encode("utf-8") if x else b""
        self.mock_serialization.to_object = lambda x: x.decode("utf-8") if isinstance(x, bytes) else x

        self.context = ProxyContext(
            invocation_service=self.mock_invocation,
            serialization_service=self.mock_serialization,
        )
        self.proxy = MapProxy("test-map", self.context)

    def test_proxy_name(self):
        """Test proxy name property."""
        self.assertEqual(self.proxy.name, "test-map")

    def test_proxy_service_name(self):
        """Test proxy service name."""
        self.assertEqual(self.proxy.service_name, "hz:impl:mapService")

    def test_destroy_marks_proxy_destroyed(self):
        """Test that destroy marks the proxy as destroyed."""
        self.proxy.destroy()
        self.assertTrue(self.proxy._destroyed)

    def test_check_not_destroyed_raises_after_destroy(self):
        """Test that operations raise after proxy is destroyed."""
        self.proxy.destroy()

        from hazelcast.exceptions import IllegalStateException
        with self.assertRaises(IllegalStateException):
            self.proxy._check_not_destroyed()

    def test_len_returns_size(self):
        """Test __len__ returns map size."""
        future = Future()
        future.set_result(5)
        self.mock_invocation.invoke.return_value = future

        with patch.object(self.proxy, 'size', return_value=5):
            self.assertEqual(len(self.proxy), 5)

    def test_contains_checks_key(self):
        """Test __contains__ checks for key."""
        with patch.object(self.proxy, 'contains_key', return_value=True):
            self.assertTrue("key" in self.proxy)

        with patch.object(self.proxy, 'contains_key', return_value=False):
            self.assertFalse("key" in self.proxy)

    def test_getitem_calls_get(self):
        """Test __getitem__ calls get."""
        with patch.object(self.proxy, 'get', return_value="value"):
            self.assertEqual(self.proxy["key"], "value")

    def test_setitem_calls_put(self):
        """Test __setitem__ calls put."""
        with patch.object(self.proxy, 'put') as mock_put:
            self.proxy["key"] = "value"
            mock_put.assert_called_once_with("key", "value")

    def test_delitem_calls_remove(self):
        """Test __delitem__ calls remove."""
        with patch.object(self.proxy, 'remove') as mock_remove:
            del self.proxy["key"]
            mock_remove.assert_called_once_with("key")

    def test_iter_returns_keys(self):
        """Test __iter__ returns key iterator."""
        with patch.object(self.proxy, 'key_set', return_value={"a", "b", "c"}):
            keys = list(self.proxy)
            self.assertEqual(set(keys), {"a", "b", "c"})

    def test_repr(self):
        """Test proxy string representation."""
        repr_str = repr(self.proxy)
        self.assertIn("MapProxy", repr_str)
        self.assertIn("test-map", repr_str)


class TestMapProxyWithMaxIdle(unittest.TestCase):
    """Test MapProxy max idle operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_invocation = MagicMock()
        self.mock_serialization = MagicMock()

        self.mock_serialization.to_data = lambda x: str(x).encode("utf-8") if x else b""
        self.mock_serialization.to_object = lambda x: x.decode("utf-8") if isinstance(x, bytes) else x

        self.context = ProxyContext(
            invocation_service=self.mock_invocation,
            serialization_service=self.mock_serialization,
        )
        self.proxy = MapProxy("test-map", self.context)

    def test_put_with_max_idle_async_creates_request(self):
        """Test put_with_max_idle_async creates proper request."""
        future = Future()
        future.set_result(None)
        self.mock_invocation.invoke.return_value = future

        result_future = self.proxy.put_with_max_idle_async("key", "value", ttl=5.0, max_idle=2.0)

        self.mock_invocation.invoke.assert_called_once()
        call_args = self.mock_invocation.invoke.call_args
        request = call_args[0][0]
        self.assertIsInstance(request, ClientMessage)

    def test_set_with_max_idle_async_creates_request(self):
        """Test set_with_max_idle_async creates proper request."""
        future = Future()
        future.set_result(None)
        self.mock_invocation.invoke.return_value = future

        result_future = self.proxy.set_with_max_idle_async("key", "value", ttl=10.0, max_idle=3.0)

        self.mock_invocation.invoke.assert_called_once()


class TestMapProxyFetchEntries(unittest.TestCase):
    """Test MapProxy fetch entries operation."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_invocation = MagicMock()
        self.mock_serialization = MagicMock()

        self.mock_serialization.to_data = lambda x: str(x).encode("utf-8") if x else b""
        self.mock_serialization.to_object = lambda x: x.decode("utf-8") if isinstance(x, bytes) else x

        self.context = ProxyContext(
            invocation_service=self.mock_invocation,
            serialization_service=self.mock_serialization,
        )
        self.proxy = MapProxy("test-map", self.context)

    def test_fetch_entries_async_creates_request(self):
        """Test fetch_entries_async creates proper request."""
        future = Future()
        future.set_result(None)
        self.mock_invocation.invoke.return_value = future

        result_future = self.proxy.fetch_entries_async(
            partition_id=0, table_index=10, batch_size=50
        )

        self.mock_invocation.invoke.assert_called_once()
        call_args = self.mock_invocation.invoke.call_args
        request = call_args[0][0]
        self.assertIsInstance(request, ClientMessage)


class TestMapProxyAggregation(unittest.TestCase):
    """Test MapProxy aggregation operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_invocation = MagicMock()
        self.mock_serialization = MagicMock()

        self.mock_serialization.to_data = lambda x: str(x).encode("utf-8") if x else b""
        self.mock_serialization.to_object = lambda x: x.decode("utf-8") if isinstance(x, bytes) else x

        self.context = ProxyContext(
            invocation_service=self.mock_invocation,
            serialization_service=self.mock_serialization,
        )
        self.proxy = MapProxy("test-map", self.context)

    def test_aggregate_async_without_predicate(self):
        """Test aggregate_async without predicate."""
        future = Future()
        future.set_result(None)
        self.mock_invocation.invoke.return_value = future

        mock_aggregator = MagicMock()
        result_future = self.proxy.aggregate_async(mock_aggregator)

        self.mock_invocation.invoke.assert_called_once()

    def test_aggregate_async_with_predicate(self):
        """Test aggregate_async with predicate."""
        future = Future()
        future.set_result(None)
        self.mock_invocation.invoke.return_value = future

        mock_aggregator = MagicMock()
        mock_predicate = MagicMock()
        result_future = self.proxy.aggregate_async(mock_aggregator, mock_predicate)

        self.mock_invocation.invoke.assert_called_once()


class TestMapProxyProjection(unittest.TestCase):
    """Test MapProxy projection operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_invocation = MagicMock()
        self.mock_serialization = MagicMock()

        self.mock_serialization.to_data = lambda x: str(x).encode("utf-8") if x else b""
        self.mock_serialization.to_object = lambda x: x.decode("utf-8") if isinstance(x, bytes) else x

        self.context = ProxyContext(
            invocation_service=self.mock_invocation,
            serialization_service=self.mock_serialization,
        )
        self.proxy = MapProxy("test-map", self.context)

    def test_project_async_without_predicate(self):
        """Test project_async without predicate."""
        future = Future()
        future.set_result(None)
        self.mock_invocation.invoke.return_value = future

        mock_projection = MagicMock()
        result_future = self.proxy.project_async(mock_projection)

        self.mock_invocation.invoke.assert_called_once()

    def test_project_async_with_predicate(self):
        """Test project_async with predicate."""
        future = Future()
        future.set_result(None)
        self.mock_invocation.invoke.return_value = future

        mock_projection = MagicMock()
        mock_predicate = MagicMock()
        result_future = self.proxy.project_async(mock_projection, mock_predicate)

        self.mock_invocation.invoke.assert_called_once()


if __name__ == "__main__":
    unittest.main()
