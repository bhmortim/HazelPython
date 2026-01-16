"""Unit tests for JCache (JSR-107) implementation.

Tests cover the Cache proxy, CacheManager, expiry policies, and
entry processor functionality.
"""

import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from hazelcast.proxy.cache import (
    Cache,
    CacheConfig,
    CacheEntryProcessor,
    Duration,
    ExpiryPolicy,
    MutableEntry,
)
from hazelcast.cache_manager import CacheManager
from hazelcast.proxy.base import ProxyContext
from hazelcast.exceptions import IllegalStateException


class TestDuration(unittest.TestCase):
    """Tests for the Duration class."""

    def test_duration_seconds(self):
        """Test creating a duration in seconds."""
        duration = Duration.seconds(30)
        self.assertEqual(duration.amount, 30)
        self.assertEqual(duration.time_unit, Duration.TimeUnit.SECONDS)

    def test_duration_minutes(self):
        """Test creating a duration in minutes."""
        duration = Duration.minutes(5)
        self.assertEqual(duration.amount, 5)
        self.assertEqual(duration.time_unit, Duration.TimeUnit.MINUTES)

    def test_duration_hours(self):
        """Test creating a duration in hours."""
        duration = Duration.hours(2)
        self.assertEqual(duration.amount, 2)
        self.assertEqual(duration.time_unit, Duration.TimeUnit.HOURS)

    def test_duration_days(self):
        """Test creating a duration in days."""
        duration = Duration.days(1)
        self.assertEqual(duration.amount, 1)
        self.assertEqual(duration.time_unit, Duration.TimeUnit.DAYS)

    def test_duration_milliseconds(self):
        """Test creating a duration in milliseconds."""
        duration = Duration.milliseconds(500)
        self.assertEqual(duration.amount, 500)
        self.assertEqual(duration.time_unit, Duration.TimeUnit.MILLISECONDS)

    def test_to_millis_seconds(self):
        """Test converting seconds to milliseconds."""
        duration = Duration.seconds(5)
        self.assertEqual(duration.to_millis(), 5000)

    def test_to_millis_minutes(self):
        """Test converting minutes to milliseconds."""
        duration = Duration.minutes(2)
        self.assertEqual(duration.to_millis(), 120000)

    def test_to_millis_hours(self):
        """Test converting hours to milliseconds."""
        duration = Duration.hours(1)
        self.assertEqual(duration.to_millis(), 3600000)

    def test_eternal(self):
        """Test eternal duration sentinel."""
        eternal = Duration.eternal()
        self.assertIsNone(eternal)

    def test_repr(self):
        """Test duration string representation."""
        duration = Duration.seconds(30)
        self.assertIn("30", repr(duration))
        self.assertIn("SECONDS", repr(duration))


class TestExpiryPolicy(unittest.TestCase):
    """Tests for the ExpiryPolicy class."""

    def test_created_expiry_policy(self):
        """Test creating a creation-based expiry policy."""
        policy = ExpiryPolicy.created_expiry_policy(Duration.minutes(30))
        self.assertIsNotNone(policy.creation)
        self.assertIsNone(policy.access)
        self.assertIsNone(policy.update)
        self.assertEqual(policy.creation.amount, 30)

    def test_accessed_expiry_policy(self):
        """Test creating an access-based expiry policy."""
        policy = ExpiryPolicy.accessed_expiry_policy(Duration.minutes(10))
        self.assertIsNone(policy.creation)
        self.assertIsNotNone(policy.access)
        self.assertIsNone(policy.update)
        self.assertEqual(policy.access.amount, 10)

    def test_modified_expiry_policy(self):
        """Test creating a modification-based expiry policy."""
        policy = ExpiryPolicy.modified_expiry_policy(Duration.minutes(15))
        self.assertIsNone(policy.creation)
        self.assertIsNone(policy.access)
        self.assertIsNotNone(policy.update)
        self.assertEqual(policy.update.amount, 15)

    def test_touched_expiry_policy(self):
        """Test creating a touch-based expiry policy."""
        policy = ExpiryPolicy.touched_expiry_policy(Duration.minutes(20))
        self.assertIsNotNone(policy.creation)
        self.assertIsNotNone(policy.access)
        self.assertIsNotNone(policy.update)
        self.assertEqual(policy.creation.amount, 20)
        self.assertEqual(policy.access.amount, 20)
        self.assertEqual(policy.update.amount, 20)

    def test_eternal_expiry_policy(self):
        """Test creating an eternal expiry policy."""
        policy = ExpiryPolicy.eternal_expiry_policy()
        self.assertIsNone(policy.creation)
        self.assertIsNone(policy.access)
        self.assertIsNone(policy.update)

    def test_custom_expiry_policy(self):
        """Test creating a custom expiry policy."""
        policy = ExpiryPolicy(
            creation=Duration.minutes(30),
            access=Duration.minutes(10),
            update=Duration.minutes(15),
        )
        self.assertEqual(policy.creation.amount, 30)
        self.assertEqual(policy.access.amount, 10)
        self.assertEqual(policy.update.amount, 15)

    def test_repr(self):
        """Test expiry policy string representation."""
        policy = ExpiryPolicy.created_expiry_policy(Duration.minutes(30))
        self.assertIn("ExpiryPolicy", repr(policy))


class TestMutableEntry(unittest.TestCase):
    """Tests for the MutableEntry class."""

    def test_entry_exists(self):
        """Test entry that exists."""
        entry = MutableEntry("key", "value", exists=True)
        self.assertEqual(entry.key, "key")
        self.assertEqual(entry.value, "value")
        self.assertTrue(entry.exists)

    def test_entry_not_exists(self):
        """Test entry that does not exist."""
        entry = MutableEntry("key", None, exists=False)
        self.assertEqual(entry.key, "key")
        self.assertIsNone(entry.value)
        self.assertFalse(entry.exists)

    def test_set_value(self):
        """Test setting entry value."""
        entry = MutableEntry("key", None, exists=False)
        entry.value = "new_value"
        self.assertEqual(entry.value, "new_value")
        self.assertTrue(entry.exists)

    def test_remove_entry(self):
        """Test removing entry."""
        entry = MutableEntry("key", "value", exists=True)
        entry.remove()
        self.assertFalse(entry.exists)


class TestCacheConfig(unittest.TestCase):
    """Tests for the CacheConfig class."""

    def test_default_config(self):
        """Test default cache configuration."""
        config = CacheConfig("test-cache")
        self.assertEqual(config.name, "test-cache")
        self.assertFalse(config.statistics_enabled)
        self.assertFalse(config.management_enabled)
        self.assertFalse(config.read_through)
        self.assertFalse(config.write_through)
        self.assertTrue(config.store_by_value)
        self.assertIsNone(config.expiry_policy)

    def test_custom_config(self):
        """Test custom cache configuration."""
        expiry = ExpiryPolicy.created_expiry_policy(Duration.minutes(30))
        config = CacheConfig(
            name="test-cache",
            key_type="java.lang.String",
            value_type="java.lang.Object",
            statistics_enabled=True,
            management_enabled=True,
            read_through=True,
            write_through=True,
            store_by_value=False,
            expiry_policy=expiry,
        )
        self.assertEqual(config.name, "test-cache")
        self.assertEqual(config.key_type, "java.lang.String")
        self.assertEqual(config.value_type, "java.lang.Object")
        self.assertTrue(config.statistics_enabled)
        self.assertTrue(config.management_enabled)
        self.assertTrue(config.read_through)
        self.assertTrue(config.write_through)
        self.assertFalse(config.store_by_value)
        self.assertIsNotNone(config.expiry_policy)

    def test_repr(self):
        """Test config string representation."""
        config = CacheConfig("test-cache")
        self.assertIn("test-cache", repr(config))


class TestCache(unittest.TestCase):
    """Tests for the Cache proxy class."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_context = MagicMock(spec=ProxyContext)
        self.mock_invocation_service = MagicMock()
        self.mock_serialization_service = MagicMock()

        self.mock_context.invocation_service = self.mock_invocation_service
        self.mock_context.serialization_service = self.mock_serialization_service

        self.mock_serialization_service.to_data.side_effect = lambda x: str(x).encode()
        self.mock_serialization_service.to_object.side_effect = lambda x: x.decode() if x else None

        self.cache = Cache(
            Cache.SERVICE_NAME,
            "test-cache",
            self.mock_context,
        )

    def test_cache_name(self):
        """Test cache name property."""
        self.assertEqual(self.cache.name, "test-cache")

    def test_cache_service_name(self):
        """Test cache service name."""
        self.assertEqual(self.cache.service_name, Cache.SERVICE_NAME)

    def test_cache_config(self):
        """Test cache configuration."""
        self.assertEqual(self.cache.config.name, "test-cache")

    def test_get_async(self):
        """Test async get operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.get_async("key")
        self.assertIsInstance(result_future, Future)

    def test_put_async(self):
        """Test async put operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.put_async("key", "value")
        self.assertIsInstance(result_future, Future)

    def test_remove_async(self):
        """Test async remove operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.remove_async("key")
        self.assertIsInstance(result_future, Future)

    def test_contains_key_async(self):
        """Test async contains_key operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.contains_key_async("key")
        self.assertIsInstance(result_future, Future)

    def test_clear_async(self):
        """Test async clear operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.clear_async()
        self.assertIsInstance(result_future, Future)

    def test_size_async(self):
        """Test async size operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.size_async()
        self.assertIsInstance(result_future, Future)

    def test_get_all_empty_keys(self):
        """Test get_all with empty keys set."""
        result = self.cache.get_all(set())
        self.assertEqual(result, {})

    def test_put_all_empty_entries(self):
        """Test put_all with empty entries dict."""
        self.cache.put_all({})

    def test_remove_all_empty_keys(self):
        """Test remove_all with empty keys set."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        self.cache.remove_all(set())

    def test_destroyed_cache_raises(self):
        """Test that operations on destroyed cache raise exception."""
        self.cache.destroy()
        with self.assertRaises(IllegalStateException):
            self.cache.get_async("key")

    def test_len(self):
        """Test __len__ method."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        with patch.object(self.cache, 'size', return_value=5):
            self.assertEqual(len(self.cache), 5)

    def test_contains(self):
        """Test __contains__ method."""
        with patch.object(self.cache, 'contains_key', return_value=True):
            self.assertTrue("key" in self.cache)

    def test_getitem(self):
        """Test __getitem__ method."""
        with patch.object(self.cache, 'get', return_value="value"):
            self.assertEqual(self.cache["key"], "value")

    def test_setitem(self):
        """Test __setitem__ method."""
        with patch.object(self.cache, 'put') as mock_put:
            self.cache["key"] = "value"
            mock_put.assert_called_once_with("key", "value")

    def test_delitem(self):
        """Test __delitem__ method."""
        with patch.object(self.cache, 'remove') as mock_remove:
            del self.cache["key"]
            mock_remove.assert_called_once_with("key")


class TestCacheWithExpiryPolicy(unittest.TestCase):
    """Tests for Cache with expiry policy."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_context = MagicMock(spec=ProxyContext)
        self.mock_invocation_service = MagicMock()
        self.mock_serialization_service = MagicMock()

        self.mock_context.invocation_service = self.mock_invocation_service
        self.mock_context.serialization_service = self.mock_serialization_service

        self.mock_serialization_service.to_data.side_effect = lambda x: str(x).encode()

        expiry = ExpiryPolicy.created_expiry_policy(Duration.minutes(30))
        config = CacheConfig("test-cache", expiry_policy=expiry)

        self.cache = Cache(
            Cache.SERVICE_NAME,
            "test-cache",
            self.mock_context,
            config,
        )

    def test_default_expiry_policy_used(self):
        """Test that default expiry policy is used when none specified."""
        self.assertIsNotNone(self.cache.config.expiry_policy)
        self.assertEqual(self.cache.config.expiry_policy.creation.amount, 30)


class TestCacheManager(unittest.TestCase):
    """Tests for the CacheManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_context = MagicMock(spec=ProxyContext)
        self.manager = CacheManager("test-cluster", self.mock_context)

    def test_uri(self):
        """Test cache manager URI."""
        self.assertEqual(self.manager.uri, "test-cluster")

    def test_create_cache(self):
        """Test creating a new cache."""
        cache = self.manager.create_cache("test-cache")
        self.assertIsNotNone(cache)
        self.assertEqual(cache.name, "test-cache")

    def test_create_cache_with_config(self):
        """Test creating a cache with configuration."""
        config = CacheConfig("test-cache", statistics_enabled=True)
        cache = self.manager.create_cache("test-cache", config)
        self.assertTrue(cache.config.statistics_enabled)

    def test_create_cache_already_exists(self):
        """Test creating a cache that already exists raises exception."""
        self.manager.create_cache("test-cache")
        with self.assertRaises(IllegalStateException):
            self.manager.create_cache("test-cache")

    def test_get_cache(self):
        """Test getting an existing cache."""
        self.manager.create_cache("test-cache")
        cache = self.manager.get_cache("test-cache")
        self.assertIsNotNone(cache)
        self.assertEqual(cache.name, "test-cache")

    def test_get_cache_not_exists(self):
        """Test getting a non-existent cache returns None."""
        cache = self.manager.get_cache("nonexistent")
        self.assertIsNone(cache)

    def test_get_or_create_cache_new(self):
        """Test get_or_create_cache creates new cache."""
        cache = self.manager.get_or_create_cache("test-cache")
        self.assertIsNotNone(cache)
        self.assertEqual(cache.name, "test-cache")

    def test_get_or_create_cache_existing(self):
        """Test get_or_create_cache returns existing cache."""
        cache1 = self.manager.create_cache("test-cache")
        cache2 = self.manager.get_or_create_cache("test-cache")
        self.assertIs(cache1, cache2)

    def test_destroy_cache(self):
        """Test destroying a cache."""
        self.manager.create_cache("test-cache")
        self.manager.destroy_cache("test-cache")
        cache = self.manager.get_cache("test-cache")
        self.assertIsNone(cache)

    def test_destroy_cache_nonexistent(self):
        """Test destroying a non-existent cache does not raise."""
        self.manager.destroy_cache("nonexistent")

    def test_get_cache_names(self):
        """Test getting all cache names."""
        self.manager.create_cache("cache1")
        self.manager.create_cache("cache2")
        names = self.manager.get_cache_names()
        self.assertEqual(names, frozenset(["cache1", "cache2"]))

    def test_get_cache_names_empty(self):
        """Test getting cache names when empty."""
        names = self.manager.get_cache_names()
        self.assertEqual(names, frozenset())

    def test_close(self):
        """Test closing the cache manager."""
        self.manager.create_cache("test-cache")
        self.manager.close()
        self.assertTrue(self.manager.is_closed)

    def test_close_idempotent(self):
        """Test closing multiple times is safe."""
        self.manager.close()
        self.manager.close()
        self.assertTrue(self.manager.is_closed)

    def test_operations_after_close_raise(self):
        """Test operations after close raise exception."""
        self.manager.close()
        with self.assertRaises(IllegalStateException):
            self.manager.create_cache("test-cache")
        with self.assertRaises(IllegalStateException):
            self.manager.get_cache("test-cache")
        with self.assertRaises(IllegalStateException):
            self.manager.get_cache_names()

    def test_context_manager(self):
        """Test using cache manager as context manager."""
        with CacheManager("test-cluster", self.mock_context) as mgr:
            mgr.create_cache("test-cache")
        self.assertTrue(mgr.is_closed)

    def test_repr(self):
        """Test cache manager string representation."""
        self.assertIn("test-cluster", repr(self.manager))


class TestCacheEntryProcessor(unittest.TestCase):
    """Tests for CacheEntryProcessor base class."""

    def test_process_not_implemented(self):
        """Test that process raises NotImplementedError."""
        processor = CacheEntryProcessor()
        entry = MutableEntry("key", "value", exists=True)
        with self.assertRaises(NotImplementedError):
            processor.process(entry)


class TestCacheInvoke(unittest.TestCase):
    """Tests for Cache invoke operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_context = MagicMock(spec=ProxyContext)
        self.mock_invocation_service = MagicMock()
        self.mock_serialization_service = MagicMock()

        self.mock_context.invocation_service = self.mock_invocation_service
        self.mock_context.serialization_service = self.mock_serialization_service

        self.mock_serialization_service.to_data.side_effect = lambda x: str(x).encode()

        self.cache = Cache(
            Cache.SERVICE_NAME,
            "test-cache",
            self.mock_context,
        )

    def test_invoke_async(self):
        """Test async invoke operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        processor = MagicMock(spec=CacheEntryProcessor)
        result_future = self.cache.invoke_async("key", processor, "arg1", "arg2")
        self.assertIsInstance(result_future, Future)

    def test_invoke_all_empty_keys(self):
        """Test invoke_all with empty keys set."""
        processor = MagicMock(spec=CacheEntryProcessor)
        result = self.cache.invoke_all(set(), processor)
        self.assertEqual(result, {})


class TestCacheBulkOperations(unittest.TestCase):
    """Tests for Cache bulk operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_context = MagicMock(spec=ProxyContext)
        self.mock_invocation_service = MagicMock()
        self.mock_serialization_service = MagicMock()

        self.mock_context.invocation_service = self.mock_invocation_service
        self.mock_context.serialization_service = self.mock_serialization_service

        self.mock_serialization_service.to_data.side_effect = lambda x: str(x).encode()

        self.cache = Cache(
            Cache.SERVICE_NAME,
            "test-cache",
            self.mock_context,
        )

    def test_get_all_async(self):
        """Test async get_all operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.get_all_async({"key1", "key2"})
        self.assertIsInstance(result_future, Future)

    def test_put_all_async(self):
        """Test async put_all operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.put_all_async({"key1": "value1", "key2": "value2"})
        self.assertIsInstance(result_future, Future)

    def test_remove_all_async_with_keys(self):
        """Test async remove_all with specific keys."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.remove_all_async({"key1", "key2"})
        self.assertIsInstance(result_future, Future)

    def test_remove_all_async_all_entries(self):
        """Test async remove_all for all entries."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.remove_all_async()
        self.assertIsInstance(result_future, Future)


class TestCacheConditionalOperations(unittest.TestCase):
    """Tests for Cache conditional operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_context = MagicMock(spec=ProxyContext)
        self.mock_invocation_service = MagicMock()
        self.mock_serialization_service = MagicMock()

        self.mock_context.invocation_service = self.mock_invocation_service
        self.mock_context.serialization_service = self.mock_serialization_service

        self.mock_serialization_service.to_data.side_effect = lambda x: str(x).encode()

        self.cache = Cache(
            Cache.SERVICE_NAME,
            "test-cache",
            self.mock_context,
        )

    def test_put_if_absent_async(self):
        """Test async put_if_absent operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.put_if_absent_async("key", "value")
        self.assertIsInstance(result_future, Future)

    def test_replace_async(self):
        """Test async replace operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.replace_async("key", "value")
        self.assertIsInstance(result_future, Future)

    def test_replace_if_same_async(self):
        """Test async replace_if_same operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.replace_if_same_async("key", "old_value", "new_value")
        self.assertIsInstance(result_future, Future)

    def test_remove_if_same_async(self):
        """Test async remove_if_same operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.remove_if_same_async("key", "value")
        self.assertIsInstance(result_future, Future)


class TestCacheGetAndOperations(unittest.TestCase):
    """Tests for Cache get-and-* operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_context = MagicMock(spec=ProxyContext)
        self.mock_invocation_service = MagicMock()
        self.mock_serialization_service = MagicMock()

        self.mock_context.invocation_service = self.mock_invocation_service
        self.mock_context.serialization_service = self.mock_serialization_service

        self.mock_serialization_service.to_data.side_effect = lambda x: str(x).encode()

        self.cache = Cache(
            Cache.SERVICE_NAME,
            "test-cache",
            self.mock_context,
        )

    def test_get_and_put_async(self):
        """Test async get_and_put operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.get_and_put_async("key", "value")
        self.assertIsInstance(result_future, Future)

    def test_get_and_remove_async(self):
        """Test async get_and_remove operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.get_and_remove_async("key")
        self.assertIsInstance(result_future, Future)

    def test_get_and_replace_async(self):
        """Test async get_and_replace operation."""
        future = Future()
        future.set_result(None)
        self.mock_invocation_service.invoke.return_value = future

        result_future = self.cache.get_and_replace_async("key", "value")
        self.assertIsInstance(result_future, Future)


if __name__ == "__main__":
    unittest.main()
