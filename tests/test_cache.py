"""Unit tests for Cache proxy."""

import unittest
from unittest.mock import MagicMock, patch
from concurrent.futures import Future

from hazelcast.proxy.cache import (
    Cache,
    Duration,
    ExpiryPolicy,
    CacheEntryProcessor,
    MutableEntry,
    CacheConfig,
)


class TestDuration(unittest.TestCase):
    """Tests for Duration class."""

    def test_duration_initialization(self):
        """Test Duration initialization."""
        duration = Duration(5, Duration.TimeUnit.SECONDS)
        self.assertEqual(duration.amount, 5)
        self.assertEqual(duration.time_unit, Duration.TimeUnit.SECONDS)

    def test_to_millis_nanoseconds(self):
        """Test conversion from nanoseconds to millis."""
        duration = Duration(1_000_000, Duration.TimeUnit.NANOSECONDS)
        self.assertEqual(duration.to_millis(), 1)

    def test_to_millis_microseconds(self):
        """Test conversion from microseconds to millis."""
        duration = Duration(1000, Duration.TimeUnit.MICROSECONDS)
        self.assertEqual(duration.to_millis(), 1)

    def test_to_millis_milliseconds(self):
        """Test conversion from milliseconds to millis."""
        duration = Duration(100, Duration.TimeUnit.MILLISECONDS)
        self.assertEqual(duration.to_millis(), 100)

    def test_to_millis_seconds(self):
        """Test conversion from seconds to millis."""
        duration = Duration(5, Duration.TimeUnit.SECONDS)
        self.assertEqual(duration.to_millis(), 5000)

    def test_to_millis_minutes(self):
        """Test conversion from minutes to millis."""
        duration = Duration(2, Duration.TimeUnit.MINUTES)
        self.assertEqual(duration.to_millis(), 120000)

    def test_to_millis_hours(self):
        """Test conversion from hours to millis."""
        duration = Duration(1, Duration.TimeUnit.HOURS)
        self.assertEqual(duration.to_millis(), 3600000)

    def test_to_millis_days(self):
        """Test conversion from days to millis."""
        duration = Duration(1, Duration.TimeUnit.DAYS)
        self.assertEqual(duration.to_millis(), 86400000)

    def test_factory_nanoseconds(self):
        """Test nanoseconds factory method."""
        duration = Duration.nanoseconds(1000)
        self.assertEqual(duration.amount, 1000)
        self.assertEqual(duration.time_unit, Duration.TimeUnit.NANOSECONDS)

    def test_factory_microseconds(self):
        """Test microseconds factory method."""
        duration = Duration.microseconds(1000)
        self.assertEqual(duration.amount, 1000)
        self.assertEqual(duration.time_unit, Duration.TimeUnit.MICROSECONDS)

    def test_factory_milliseconds(self):
        """Test milliseconds factory method."""
        duration = Duration.milliseconds(1000)
        self.assertEqual(duration.amount, 1000)
        self.assertEqual(duration.time_unit, Duration.TimeUnit.MILLISECONDS)

    def test_factory_seconds(self):
        """Test seconds factory method."""
        duration = Duration.seconds(60)
        self.assertEqual(duration.amount, 60)
        self.assertEqual(duration.time_unit, Duration.TimeUnit.SECONDS)

    def test_factory_minutes(self):
        """Test minutes factory method."""
        duration = Duration.minutes(30)
        self.assertEqual(duration.amount, 30)
        self.assertEqual(duration.time_unit, Duration.TimeUnit.MINUTES)

    def test_factory_hours(self):
        """Test hours factory method."""
        duration = Duration.hours(24)
        self.assertEqual(duration.amount, 24)
        self.assertEqual(duration.time_unit, Duration.TimeUnit.HOURS)

    def test_factory_days(self):
        """Test days factory method."""
        duration = Duration.days(7)
        self.assertEqual(duration.amount, 7)
        self.assertEqual(duration.time_unit, Duration.TimeUnit.DAYS)

    def test_eternal(self):
        """Test eternal factory method."""
        duration = Duration.eternal()
        self.assertIsNone(duration)

    def test_repr(self):
        """Test Duration __repr__."""
        duration = Duration(5, Duration.TimeUnit.SECONDS)
        repr_str = repr(duration)
        self.assertIn("5", repr_str)
        self.assertIn("SECONDS", repr_str)


class TestExpiryPolicy(unittest.TestCase):
    """Tests for ExpiryPolicy class."""

    def test_expiry_policy_initialization(self):
        """Test ExpiryPolicy initialization."""
        creation = Duration.minutes(30)
        access = Duration.minutes(10)
        update = Duration.minutes(15)
        policy = ExpiryPolicy(creation=creation, access=access, update=update)
        self.assertEqual(policy.creation, creation)
        self.assertEqual(policy.access, access)
        self.assertEqual(policy.update, update)

    def test_expiry_policy_defaults(self):
        """Test ExpiryPolicy with default values."""
        policy = ExpiryPolicy()
        self.assertIsNone(policy.creation)
        self.assertIsNone(policy.access)
        self.assertIsNone(policy.update)

    def test_created_expiry_policy(self):
        """Test created_expiry_policy factory."""
        duration = Duration.minutes(30)
        policy = ExpiryPolicy.created_expiry_policy(duration)
        self.assertEqual(policy.creation, duration)
        self.assertIsNone(policy.access)
        self.assertIsNone(policy.update)

    def test_accessed_expiry_policy(self):
        """Test accessed_expiry_policy factory."""
        duration = Duration.minutes(10)
        policy = ExpiryPolicy.accessed_expiry_policy(duration)
        self.assertIsNone(policy.creation)
        self.assertEqual(policy.access, duration)
        self.assertIsNone(policy.update)

    def test_modified_expiry_policy(self):
        """Test modified_expiry_policy factory."""
        duration = Duration.minutes(15)
        policy = ExpiryPolicy.modified_expiry_policy(duration)
        self.assertIsNone(policy.creation)
        self.assertIsNone(policy.access)
        self.assertEqual(policy.update, duration)

    def test_touched_expiry_policy(self):
        """Test touched_expiry_policy factory."""
        duration = Duration.minutes(20)
        policy = ExpiryPolicy.touched_expiry_policy(duration)
        self.assertEqual(policy.creation, duration)
        self.assertEqual(policy.access, duration)
        self.assertEqual(policy.update, duration)

    def test_eternal_expiry_policy(self):
        """Test eternal_expiry_policy factory."""
        policy = ExpiryPolicy.eternal_expiry_policy()
        self.assertIsNone(policy.creation)
        self.assertIsNone(policy.access)
        self.assertIsNone(policy.update)

    def test_repr(self):
        """Test ExpiryPolicy __repr__."""
        policy = ExpiryPolicy(creation=Duration.minutes(30))
        repr_str = repr(policy)
        self.assertIn("creation", repr_str)


class TestMutableEntry(unittest.TestCase):
    """Tests for MutableEntry class."""

    def test_mutable_entry_initialization(self):
        """Test MutableEntry initialization."""
        entry = MutableEntry(key="test_key", value="test_value", exists=True)
        self.assertEqual(entry.key, "test_key")
        self.assertEqual(entry.value, "test_value")
        self.assertTrue(entry.exists)

    def test_mutable_entry_set_value(self):
        """Test setting value on MutableEntry."""
        entry = MutableEntry(key="key", value=None, exists=False)
        entry.value = "new_value"
        self.assertEqual(entry.value, "new_value")
        self.assertTrue(entry.exists)

    def test_mutable_entry_remove(self):
        """Test removing MutableEntry."""
        entry = MutableEntry(key="key", value="value", exists=True)
        entry.remove()
        self.assertFalse(entry.exists)


class TestCacheConfig(unittest.TestCase):
    """Tests for CacheConfig class."""

    def test_cache_config_initialization(self):
        """Test CacheConfig initialization."""
        config = CacheConfig(
            name="test-cache",
            key_type="java.lang.String",
            value_type="java.lang.Integer",
            statistics_enabled=True,
            management_enabled=True,
            read_through=True,
            write_through=True,
            store_by_value=False,
        )
        self.assertEqual(config.name, "test-cache")
        self.assertEqual(config.key_type, "java.lang.String")
        self.assertEqual(config.value_type, "java.lang.Integer")
        self.assertTrue(config.statistics_enabled)
        self.assertTrue(config.management_enabled)
        self.assertTrue(config.read_through)
        self.assertTrue(config.write_through)
        self.assertFalse(config.store_by_value)

    def test_cache_config_defaults(self):
        """Test CacheConfig with default values."""
        config = CacheConfig(name="test")
        self.assertEqual(config.name, "test")
        self.assertIsNone(config.key_type)
        self.assertIsNone(config.value_type)
        self.assertFalse(config.statistics_enabled)
        self.assertFalse(config.management_enabled)
        self.assertFalse(config.read_through)
        self.assertFalse(config.write_through)
        self.assertTrue(config.store_by_value)

    def test_cache_config_repr(self):
        """Test CacheConfig __repr__."""
        config = CacheConfig(name="my-cache")
        repr_str = repr(config)
        self.assertIn("my-cache", repr_str)


class TestCache(unittest.TestCase):
    """Tests for Cache class."""

    def setUp(self):
        """Set up test fixtures."""
        self.cache = Cache(
            service_name="hz:impl:cacheService",
            name="test-cache",
            context=None,
        )

    def test_service_name_constant(self):
        """Test SERVICE_NAME is defined correctly."""
        self.assertEqual(Cache.SERVICE_NAME, "hz:impl:cacheService")

    def test_config_property(self):
        """Test config property returns CacheConfig."""
        config = self.cache.config
        self.assertIsInstance(config, CacheConfig)
        self.assertEqual(config.name, "test-cache")

    def test_get_async_returns_future(self):
        """Test get_async returns a Future."""
        future = self.cache.get_async("key")
        self.assertIsInstance(future, Future)

    def test_put_async_returns_future(self):
        """Test put_async returns a Future."""
        future = self.cache.put_async("key", "value")
        self.assertIsInstance(future, Future)

    def test_put_if_absent_async_returns_future(self):
        """Test put_if_absent_async returns a Future."""
        future = self.cache.put_if_absent_async("key", "value")
        self.assertIsInstance(future, Future)

    def test_get_and_put_async_returns_future(self):
        """Test get_and_put_async returns a Future."""
        future = self.cache.get_and_put_async("key", "value")
        self.assertIsInstance(future, Future)

    def test_remove_async_returns_future(self):
        """Test remove_async returns a Future."""
        future = self.cache.remove_async("key")
        self.assertIsInstance(future, Future)

    def test_remove_if_same_async_returns_future(self):
        """Test remove_if_same_async returns a Future."""
        future = self.cache.remove_if_same_async("key", "old_value")
        self.assertIsInstance(future, Future)

    def test_get_and_remove_async_returns_future(self):
        """Test get_and_remove_async returns a Future."""
        future = self.cache.get_and_remove_async("key")
        self.assertIsInstance(future, Future)

    def test_replace_async_returns_future(self):
        """Test replace_async returns a Future."""
        future = self.cache.replace_async("key", "value")
        self.assertIsInstance(future, Future)

    def test_replace_if_same_async_returns_future(self):
        """Test replace_if_same_async returns a Future."""
        future = self.cache.replace_if_same_async("key", "old", "new")
        self.assertIsInstance(future, Future)

    def test_get_and_replace_async_returns_future(self):
        """Test get_and_replace_async returns a Future."""
        future = self.cache.get_and_replace_async("key", "value")
        self.assertIsInstance(future, Future)

    def test_contains_key_async_returns_future(self):
        """Test contains_key_async returns a Future."""
        future = self.cache.contains_key_async("key")
        self.assertIsInstance(future, Future)

    def test_clear_async_returns_future(self):
        """Test clear_async returns a Future."""
        future = self.cache.clear_async()
        self.assertIsInstance(future, Future)

    def test_size_async_returns_future(self):
        """Test size_async returns a Future."""
        future = self.cache.size_async()
        self.assertIsInstance(future, Future)

    def test_get_all_async_empty_keys(self):
        """Test get_all_async with empty keys returns empty dict."""
        future = self.cache.get_all_async(set())
        self.assertEqual(future.result(), {})

    def test_get_all_async_returns_future(self):
        """Test get_all_async returns a Future."""
        future = self.cache.get_all_async({"key1", "key2"})
        self.assertIsInstance(future, Future)

    def test_put_all_async_empty_entries(self):
        """Test put_all_async with empty entries completes."""
        future = self.cache.put_all_async({})
        self.assertIsNone(future.result())

    def test_put_all_async_returns_future(self):
        """Test put_all_async returns a Future."""
        future = self.cache.put_all_async({"key1": "value1"})
        self.assertIsInstance(future, Future)

    def test_remove_all_async_returns_future(self):
        """Test remove_all_async returns a Future."""
        future = self.cache.remove_all_async()
        self.assertIsInstance(future, Future)

    def test_remove_all_async_empty_keys(self):
        """Test remove_all_async with empty keys completes."""
        future = self.cache.remove_all_async(set())
        self.assertIsNone(future.result())

    def test_invoke_async_returns_future(self):
        """Test invoke_async returns a Future."""
        class TestProcessor(CacheEntryProcessor):
            def process(self, entry, *args):
                return entry.value

        future = self.cache.invoke_async("key", TestProcessor())
        self.assertIsInstance(future, Future)

    def test_invoke_all_async_empty_keys(self):
        """Test invoke_all_async with empty keys returns empty dict."""
        class TestProcessor(CacheEntryProcessor):
            def process(self, entry, *args):
                return entry.value

        future = self.cache.invoke_all_async(set(), TestProcessor())
        self.assertEqual(future.result(), {})

    def test_len_returns_size(self):
        """Test __len__ returns size."""
        self.assertEqual(len(self.cache), 0)

    def test_contains(self):
        """Test __contains__."""
        self.assertFalse("key" in self.cache)

    def test_getitem(self):
        """Test __getitem__."""
        result = self.cache["key"]
        self.assertIsNone(result)

    def test_setitem(self):
        """Test __setitem__."""
        self.cache["key"] = "value"

    def test_delitem(self):
        """Test __delitem__."""
        del self.cache["key"]


class TestCacheWithExpiryPolicy(unittest.TestCase):
    """Tests for Cache with ExpiryPolicy."""

    def setUp(self):
        """Set up test fixtures."""
        expiry = ExpiryPolicy(creation=Duration.minutes(30))
        config = CacheConfig(name="test-cache", expiry_policy=expiry)
        self.cache = Cache(
            service_name="hz:impl:cacheService",
            name="test-cache",
            context=None,
            config=config,
        )

    def test_default_expiry_policy_is_set(self):
        """Test default expiry policy is set from config."""
        self.assertIsNotNone(self.cache._default_expiry_policy)

    def test_get_with_custom_expiry(self):
        """Test get_async with custom expiry policy."""
        custom_expiry = ExpiryPolicy(access=Duration.minutes(10))
        future = self.cache.get_async("key", expiry_policy=custom_expiry)
        self.assertIsInstance(future, Future)


if __name__ == "__main__":
    unittest.main()
