"""Tests for MapProxy with mocked invocations."""

import struct
import uuid
from concurrent.futures import Future
from typing import Any, Callable, Optional
from unittest import TestCase
from unittest.mock import MagicMock, patch

from hazelcast.protocol.client_message import ClientMessage, Frame
from hazelcast.protocol.codec import (
    BOOLEAN_SIZE,
    INT_SIZE,
    LONG_SIZE,
    RESPONSE_HEADER_SIZE,
    UUID_SIZE,
    MapCodec,
)
from hazelcast.proxy.base import ProxyContext
from hazelcast.proxy.map import EntryEvent, EntryListener, MapProxy


def _create_mock_response(content: bytes = b"") -> ClientMessage:
    """Create a mock response ClientMessage."""
    msg = ClientMessage.create_for_encode()
    msg.add_frame(Frame(content))
    return msg


def _create_bool_response(value: bool) -> ClientMessage:
    """Create a response with a boolean value."""
    buffer = bytearray(RESPONSE_HEADER_SIZE + BOOLEAN_SIZE)
    struct.pack_into("<B", buffer, RESPONSE_HEADER_SIZE, 1 if value else 0)
    msg = ClientMessage.create_for_encode()
    msg.add_frame(Frame(bytes(buffer)))
    return msg


def _create_int_response(value: int) -> ClientMessage:
    """Create a response with an int value."""
    buffer = bytearray(RESPONSE_HEADER_SIZE + INT_SIZE)
    struct.pack_into("<i", buffer, RESPONSE_HEADER_SIZE, value)
    msg = ClientMessage.create_for_encode()
    msg.add_frame(Frame(bytes(buffer)))
    return msg


def _create_data_response(data: Optional[bytes]) -> ClientMessage:
    """Create a response with data in a second frame."""
    msg = ClientMessage.create_for_encode()
    msg.add_frame(Frame(b""))  # initial frame
    if data is not None:
        msg.add_frame(Frame(data))
    return msg


def _create_uuid_response(uuid_val: uuid.UUID) -> ClientMessage:
    """Create a response with a UUID value."""
    from hazelcast.protocol.codec import FixSizedTypesCodec

    buffer = bytearray(RESPONSE_HEADER_SIZE + UUID_SIZE)
    FixSizedTypesCodec.encode_uuid(buffer, RESPONSE_HEADER_SIZE, uuid_val)
    msg = ClientMessage.create_for_encode()
    msg.add_frame(Frame(bytes(buffer)))
    return msg


class MockInvocationService:
    """Mock invocation service for testing."""

    def __init__(self) -> None:
        self.last_request: Optional[ClientMessage] = None
        self.response_provider: Optional[Callable[[], ClientMessage]] = None

    def invoke(self, invocation: Any) -> Future:
        self.last_request = invocation.request
        future: Future = Future()
        if self.response_provider:
            future.set_result(self.response_provider())
        else:
            future.set_result(_create_mock_response())
        return future

    def set_response(self, response: ClientMessage) -> None:
        self.response_provider = lambda: response


class MockSerializationService:
    """Mock serialization service for testing."""

    def to_data(self, obj: Any) -> bytes:
        if obj is None:
            return b""
        if isinstance(obj, bytes):
            return obj
        return str(obj).encode("utf-8")

    def to_object(self, data: bytes) -> Any:
        if not data:
            return None
        return data.decode("utf-8")


class TestMapProxyBasicOperations(TestCase):
    """Tests for basic Map operations."""

    def setUp(self) -> None:
        self.invocation_service = MockInvocationService()
        self.serialization_service = MockSerializationService()
        self.context = ProxyContext(
            invocation_service=self.invocation_service,
            serialization_service=self.serialization_service,
        )
        self.map_proxy: MapProxy[str, str] = MapProxy("test-map", self.context)

    def test_put_returns_old_value(self) -> None:
        self.invocation_service.set_response(_create_data_response(b"old_value"))
        result = self.map_proxy.put("key", "value")
        self.assertEqual("old_value", result)

    def test_put_returns_none_when_no_old_value(self) -> None:
        self.invocation_service.set_response(_create_data_response(None))
        result = self.map_proxy.put("key", "value")
        self.assertIsNone(result)

    def test_put_with_ttl(self) -> None:
        self.invocation_service.set_response(_create_data_response(None))
        self.map_proxy.put("key", "value", ttl=60)
        self.assertIsNotNone(self.invocation_service.last_request)

    def test_get_returns_value(self) -> None:
        self.invocation_service.set_response(_create_data_response(b"value"))
        result = self.map_proxy.get("key")
        self.assertEqual("value", result)

    def test_get_returns_none_when_not_found(self) -> None:
        self.invocation_service.set_response(_create_data_response(None))
        result = self.map_proxy.get("key")
        self.assertIsNone(result)

    def test_remove_returns_old_value(self) -> None:
        self.invocation_service.set_response(_create_data_response(b"removed"))
        result = self.map_proxy.remove("key")
        self.assertEqual("removed", result)

    def test_remove_returns_none_when_not_found(self) -> None:
        self.invocation_service.set_response(_create_data_response(None))
        result = self.map_proxy.remove("key")
        self.assertIsNone(result)

    def test_delete(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        self.map_proxy.delete("key")
        self.assertIsNotNone(self.invocation_service.last_request)

    def test_contains_key_true(self) -> None:
        self.invocation_service.set_response(_create_bool_response(True))
        result = self.map_proxy.contains_key("key")
        self.assertTrue(result)

    def test_contains_key_false(self) -> None:
        self.invocation_service.set_response(_create_bool_response(False))
        result = self.map_proxy.contains_key("key")
        self.assertFalse(result)

    def test_contains_value_true(self) -> None:
        self.invocation_service.set_response(_create_bool_response(True))
        result = self.map_proxy.contains_value("value")
        self.assertTrue(result)

    def test_contains_value_false(self) -> None:
        self.invocation_service.set_response(_create_bool_response(False))
        result = self.map_proxy.contains_value("value")
        self.assertFalse(result)

    def test_size(self) -> None:
        self.invocation_service.set_response(_create_int_response(42))
        result = self.map_proxy.size()
        self.assertEqual(42, result)

    def test_is_empty_true(self) -> None:
        self.invocation_service.set_response(_create_bool_response(True))
        result = self.map_proxy.is_empty()
        self.assertTrue(result)

    def test_is_empty_false(self) -> None:
        self.invocation_service.set_response(_create_bool_response(False))
        result = self.map_proxy.is_empty()
        self.assertFalse(result)

    def test_clear(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        self.map_proxy.clear()
        self.assertIsNotNone(self.invocation_service.last_request)


class TestMapProxyConditionalOperations(TestCase):
    """Tests for conditional Map operations."""

    def setUp(self) -> None:
        self.invocation_service = MockInvocationService()
        self.serialization_service = MockSerializationService()
        self.context = ProxyContext(
            invocation_service=self.invocation_service,
            serialization_service=self.serialization_service,
        )
        self.map_proxy: MapProxy[str, str] = MapProxy("test-map", self.context)

    def test_put_if_absent_returns_none(self) -> None:
        self.invocation_service.set_response(_create_data_response(None))
        result = self.map_proxy.put_if_absent("key", "value")
        self.assertIsNone(result)

    def test_put_if_absent_returns_existing(self) -> None:
        self.invocation_service.set_response(_create_data_response(b"existing"))
        result = self.map_proxy.put_if_absent("key", "value")
        self.assertEqual("existing", result)

    def test_replace_returns_old(self) -> None:
        self.invocation_service.set_response(_create_data_response(b"old"))
        result = self.map_proxy.replace("key", "new")
        self.assertEqual("old", result)

    def test_replace_returns_none(self) -> None:
        self.invocation_service.set_response(_create_data_response(None))
        result = self.map_proxy.replace("key", "new")
        self.assertIsNone(result)

    def test_replace_if_same_true(self) -> None:
        self.invocation_service.set_response(_create_bool_response(True))
        result = self.map_proxy.replace_if_same("key", "old", "new")
        self.assertTrue(result)

    def test_replace_if_same_false(self) -> None:
        self.invocation_service.set_response(_create_bool_response(False))
        result = self.map_proxy.replace_if_same("key", "old", "new")
        self.assertFalse(result)

    def test_set(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        self.map_proxy.set("key", "value")
        self.assertIsNotNone(self.invocation_service.last_request)

    def test_set_with_ttl(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        self.map_proxy.set("key", "value", ttl=30)
        self.assertIsNotNone(self.invocation_service.last_request)


class TestMapProxyBulkOperations(TestCase):
    """Tests for bulk Map operations."""

    def setUp(self) -> None:
        self.invocation_service = MockInvocationService()
        self.serialization_service = MockSerializationService()
        self.context = ProxyContext(
            invocation_service=self.invocation_service,
            serialization_service=self.serialization_service,
        )
        self.map_proxy: MapProxy[str, str] = MapProxy("test-map", self.context)

    def test_get_all_empty_keys(self) -> None:
        result = self.map_proxy.get_all(set())
        self.assertEqual({}, result)

    def test_put_all_empty(self) -> None:
        self.map_proxy.put_all({})
        # Should not invoke when empty

    def test_put_all(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        self.map_proxy.put_all({"k1": "v1", "k2": "v2"})
        self.assertIsNotNone(self.invocation_service.last_request)


class TestMapProxyCollectionViews(TestCase):
    """Tests for collection view operations."""

    def setUp(self) -> None:
        self.invocation_service = MockInvocationService()
        self.serialization_service = MockSerializationService()
        self.context = ProxyContext(
            invocation_service=self.invocation_service,
            serialization_service=self.serialization_service,
        )
        self.map_proxy: MapProxy[str, str] = MapProxy("test-map", self.context)

    def test_key_set(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        result = self.map_proxy.key_set()
        self.assertIsInstance(result, set)

    def test_values(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        result = self.map_proxy.values()
        self.assertIsInstance(result, list)

    def test_entry_set(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        result = self.map_proxy.entry_set()
        self.assertIsInstance(result, set)


class TestMapProxyLockOperations(TestCase):
    """Tests for lock operations."""

    def setUp(self) -> None:
        self.invocation_service = MockInvocationService()
        self.serialization_service = MockSerializationService()
        self.context = ProxyContext(
            invocation_service=self.invocation_service,
            serialization_service=self.serialization_service,
        )
        self.map_proxy: MapProxy[str, str] = MapProxy("test-map", self.context)

    def test_lock(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        self.map_proxy.lock("key")
        self.assertIsNotNone(self.invocation_service.last_request)

    def test_lock_with_ttl(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        self.map_proxy.lock("key", ttl=30)
        self.assertIsNotNone(self.invocation_service.last_request)

    def test_try_lock_success(self) -> None:
        self.invocation_service.set_response(_create_bool_response(True))
        result = self.map_proxy.try_lock("key")
        self.assertTrue(result)

    def test_try_lock_failure(self) -> None:
        self.invocation_service.set_response(_create_bool_response(False))
        result = self.map_proxy.try_lock("key")
        self.assertFalse(result)

    def test_try_lock_with_timeout(self) -> None:
        self.invocation_service.set_response(_create_bool_response(True))
        result = self.map_proxy.try_lock("key", timeout=5)
        self.assertTrue(result)

    def test_unlock(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        self.map_proxy.unlock("key")
        self.assertIsNotNone(self.invocation_service.last_request)

    def test_is_locked_true(self) -> None:
        self.invocation_service.set_response(_create_bool_response(True))
        result = self.map_proxy.is_locked("key")
        self.assertTrue(result)

    def test_is_locked_false(self) -> None:
        self.invocation_service.set_response(_create_bool_response(False))
        result = self.map_proxy.is_locked("key")
        self.assertFalse(result)

    def test_force_unlock(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        self.map_proxy.force_unlock("key")
        self.assertIsNotNone(self.invocation_service.last_request)


class TestMapProxyEvictionOperations(TestCase):
    """Tests for eviction operations."""

    def setUp(self) -> None:
        self.invocation_service = MockInvocationService()
        self.serialization_service = MockSerializationService()
        self.context = ProxyContext(
            invocation_service=self.invocation_service,
            serialization_service=self.serialization_service,
        )
        self.map_proxy: MapProxy[str, str] = MapProxy("test-map", self.context)

    def test_evict_true(self) -> None:
        self.invocation_service.set_response(_create_bool_response(True))
        result = self.map_proxy.evict("key")
        self.assertTrue(result)

    def test_evict_false(self) -> None:
        self.invocation_service.set_response(_create_bool_response(False))
        result = self.map_proxy.evict("key")
        self.assertFalse(result)

    def test_evict_all(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        self.map_proxy.evict_all()
        self.assertIsNotNone(self.invocation_service.last_request)

    def test_flush(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        self.map_proxy.flush()
        self.assertIsNotNone(self.invocation_service.last_request)

    def test_load_all(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        self.map_proxy.load_all()
        self.assertIsNotNone(self.invocation_service.last_request)

    def test_load_all_with_keys(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        self.map_proxy.load_all(keys={"k1", "k2"})
        self.assertIsNotNone(self.invocation_service.last_request)


class TestMapProxyEntryProcessor(TestCase):
    """Tests for entry processor operations."""

    def setUp(self) -> None:
        self.invocation_service = MockInvocationService()
        self.serialization_service = MockSerializationService()
        self.context = ProxyContext(
            invocation_service=self.invocation_service,
            serialization_service=self.serialization_service,
        )
        self.map_proxy: MapProxy[str, str] = MapProxy("test-map", self.context)

    def test_execute_on_key(self) -> None:
        self.invocation_service.set_response(_create_data_response(b"result"))
        from hazelcast.processor import EntryProcessor

        class TestProcessor(EntryProcessor[str, str]):
            pass

        result = self.map_proxy.execute_on_key("key", TestProcessor())
        self.assertEqual("result", result)

    def test_execute_on_keys_empty(self) -> None:
        from hazelcast.processor import EntryProcessor

        class TestProcessor(EntryProcessor[str, str]):
            pass

        result = self.map_proxy.execute_on_keys(set(), TestProcessor())
        self.assertEqual({}, result)

    def test_execute_on_entries(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        from hazelcast.processor import EntryProcessor

        class TestProcessor(EntryProcessor[str, str]):
            pass

        result = self.map_proxy.execute_on_entries(TestProcessor())
        self.assertIsInstance(result, dict)

    def test_execute_on_all_entries(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        from hazelcast.processor import EntryProcessor

        class TestProcessor(EntryProcessor[str, str]):
            pass

        result = self.map_proxy.execute_on_all_entries(TestProcessor())
        self.assertIsInstance(result, dict)


class TestMapProxyEntryListener(TestCase):
    """Tests for entry listener operations."""

    def setUp(self) -> None:
        self.invocation_service = MockInvocationService()
        self.serialization_service = MockSerializationService()
        self.context = ProxyContext(
            invocation_service=self.invocation_service,
            serialization_service=self.serialization_service,
        )
        self.map_proxy: MapProxy[str, str] = MapProxy("test-map", self.context)

    def test_add_entry_listener(self) -> None:
        test_uuid = uuid.uuid4()
        self.invocation_service.set_response(_create_uuid_response(test_uuid))
        listener = EntryListener[str, str]()
        reg_id = self.map_proxy.add_entry_listener(listener)
        self.assertIsNotNone(reg_id)

    def test_add_entry_listener_to_key(self) -> None:
        test_uuid = uuid.uuid4()
        self.invocation_service.set_response(_create_uuid_response(test_uuid))
        listener = EntryListener[str, str]()
        reg_id = self.map_proxy.add_entry_listener(listener, key="specific_key")
        self.assertIsNotNone(reg_id)

    def test_remove_entry_listener(self) -> None:
        test_uuid = uuid.uuid4()
        self.invocation_service.set_response(_create_uuid_response(test_uuid))
        listener = EntryListener[str, str]()
        reg_id = self.map_proxy.add_entry_listener(listener)

        self.invocation_service.set_response(_create_bool_response(True))
        result = self.map_proxy.remove_entry_listener(reg_id)
        self.assertTrue(result)

    def test_remove_nonexistent_listener(self) -> None:
        result = self.map_proxy.remove_entry_listener("nonexistent")
        self.assertFalse(result)


class TestMapProxyDunderMethods(TestCase):
    """Tests for dunder methods."""

    def setUp(self) -> None:
        self.invocation_service = MockInvocationService()
        self.serialization_service = MockSerializationService()
        self.context = ProxyContext(
            invocation_service=self.invocation_service,
            serialization_service=self.serialization_service,
        )
        self.map_proxy: MapProxy[str, str] = MapProxy("test-map", self.context)

    def test_len(self) -> None:
        self.invocation_service.set_response(_create_int_response(5))
        self.assertEqual(5, len(self.map_proxy))

    def test_contains(self) -> None:
        self.invocation_service.set_response(_create_bool_response(True))
        self.assertTrue("key" in self.map_proxy)

    def test_getitem(self) -> None:
        self.invocation_service.set_response(_create_data_response(b"value"))
        self.assertEqual("value", self.map_proxy["key"])

    def test_setitem(self) -> None:
        self.invocation_service.set_response(_create_data_response(None))
        self.map_proxy["key"] = "value"
        self.assertIsNotNone(self.invocation_service.last_request)

    def test_delitem(self) -> None:
        self.invocation_service.set_response(_create_data_response(None))
        del self.map_proxy["key"]
        self.assertIsNotNone(self.invocation_service.last_request)

    def test_repr(self) -> None:
        self.assertIn("test-map", repr(self.map_proxy))

    def test_str(self) -> None:
        self.assertIn("test-map", str(self.map_proxy))


class TestMapProxyDestroyedState(TestCase):
    """Tests for destroyed proxy state."""

    def setUp(self) -> None:
        self.invocation_service = MockInvocationService()
        self.context = ProxyContext(invocation_service=self.invocation_service)
        self.map_proxy: MapProxy[str, str] = MapProxy("test-map", self.context)

    def test_put_after_destroy_raises(self) -> None:
        self.map_proxy.destroy()
        from hazelcast.exceptions import IllegalStateException

        with self.assertRaises(IllegalStateException):
            self.map_proxy.put("key", "value")

    def test_get_after_destroy_raises(self) -> None:
        self.map_proxy.destroy()
        from hazelcast.exceptions import IllegalStateException

        with self.assertRaises(IllegalStateException):
            self.map_proxy.get("key")

    def test_is_destroyed(self) -> None:
        self.assertFalse(self.map_proxy.is_destroyed)
        self.map_proxy.destroy()
        self.assertTrue(self.map_proxy.is_destroyed)


class TestMapProxyNearCache(TestCase):
    """Tests for near cache integration."""

    def setUp(self) -> None:
        self.invocation_service = MockInvocationService()
        self.serialization_service = MockSerializationService()
        self.context = ProxyContext(
            invocation_service=self.invocation_service,
            serialization_service=self.serialization_service,
        )
        self.near_cache = MagicMock()
        self.map_proxy: MapProxy[str, str] = MapProxy(
            "test-map", self.context, near_cache=self.near_cache
        )

    def test_get_from_near_cache(self) -> None:
        self.near_cache.get.return_value = "cached_value"
        result = self.map_proxy.get("key")
        self.assertEqual("cached_value", result)
        self.near_cache.get.assert_called_once_with("key")

    def test_get_populates_near_cache(self) -> None:
        self.near_cache.get.return_value = None
        self.invocation_service.set_response(_create_data_response(b"value"))
        self.map_proxy.get("key")
        self.near_cache.put.assert_called()

    def test_put_invalidates_near_cache(self) -> None:
        self.invocation_service.set_response(_create_data_response(None))
        self.map_proxy.put("key", "value")
        self.near_cache.invalidate.assert_called_once_with("key")

    def test_remove_invalidates_near_cache(self) -> None:
        self.invocation_service.set_response(_create_data_response(None))
        self.map_proxy.remove("key")
        self.near_cache.invalidate.assert_called_once_with("key")

    def test_clear_invalidates_all(self) -> None:
        self.invocation_service.set_response(_create_mock_response())
        self.map_proxy.clear()
        self.near_cache.invalidate_all.assert_called_once()

    def test_set_near_cache(self) -> None:
        new_cache = MagicMock()
        self.map_proxy.set_near_cache(new_cache)
        self.assertEqual(new_cache, self.map_proxy.near_cache)


class TestEntryEvent(TestCase):
    """Tests for EntryEvent class."""

    def test_event_properties(self) -> None:
        event = EntryEvent[str, str](
            event_type=EntryEvent.ADDED,
            key="key",
            value="value",
            old_value="old",
            merging_value="merging",
            member=None,
        )
        self.assertEqual(EntryEvent.ADDED, event.event_type)
        self.assertEqual("key", event.key)
        self.assertEqual("value", event.value)
        self.assertEqual("old", event.old_value)
        self.assertEqual("merging", event.merging_value)
        self.assertIsNone(event.member)

    def test_event_types(self) -> None:
        self.assertEqual(1, EntryEvent.ADDED)
        self.assertEqual(2, EntryEvent.REMOVED)
        self.assertEqual(4, EntryEvent.UPDATED)
        self.assertEqual(8, EntryEvent.EVICTED)
        self.assertEqual(16, EntryEvent.EXPIRED)
        self.assertEqual(32, EntryEvent.EVICT_ALL)
        self.assertEqual(64, EntryEvent.CLEAR_ALL)
        self.assertEqual(128, EntryEvent.MERGED)
        self.assertEqual(256, EntryEvent.INVALIDATION)
        self.assertEqual(512, EntryEvent.LOADED)


class TestEntryListener(TestCase):
    """Tests for EntryListener class."""

    def test_listener_methods_callable(self) -> None:
        listener = EntryListener[str, str]()
        event = EntryEvent[str, str](EntryEvent.ADDED, "key", "value")

        listener.entry_added(event)
        listener.entry_removed(event)
        listener.entry_updated(event)
        listener.entry_evicted(event)
        listener.entry_expired(event)
        listener.map_evicted(event)
        listener.map_cleared(event)


class TestMapCodecEncodeDecode(TestCase):
    """Tests for MapCodec encode/decode methods."""

    def test_encode_put_request(self) -> None:
        msg = MapCodec.encode_put_request("test", b"key", b"value", 0, -1)
        self.assertIsNotNone(msg)

    def test_encode_get_request(self) -> None:
        msg = MapCodec.encode_get_request("test", b"key", 0)
        self.assertIsNotNone(msg)

    def test_encode_remove_request(self) -> None:
        msg = MapCodec.encode_remove_request("test", b"key", 0)
        self.assertIsNotNone(msg)

    def test_encode_contains_key_request(self) -> None:
        msg = MapCodec.encode_contains_key_request("test", b"key", 0)
        self.assertIsNotNone(msg)

    def test_encode_size_request(self) -> None:
        msg = MapCodec.encode_size_request("test")
        self.assertIsNotNone(msg)

    def test_encode_clear_request(self) -> None:
        msg = MapCodec.encode_clear_request("test")
        self.assertIsNotNone(msg)

    def test_decode_size_response(self) -> None:
        response = _create_int_response(42)
        result = MapCodec.decode_size_response(response)
        self.assertEqual(42, result)

    def test_decode_contains_key_response_true(self) -> None:
        response = _create_bool_response(True)
        result = MapCodec.decode_contains_key_response(response)
        self.assertTrue(result)

    def test_decode_contains_key_response_false(self) -> None:
        response = _create_bool_response(False)
        result = MapCodec.decode_contains_key_response(response)
        self.assertFalse(result)
