"""Tests for Map Store/Loader functionality."""

import unittest
from typing import Dict, Iterable, List, Optional
from unittest.mock import MagicMock, patch

from hazelcast.store import (
    MapLoader,
    MapStore,
    MapLoaderLifecycleSupport,
    EntryLoader,
    EntryLoaderEntry,
    LoadAllKeysCallback,
)
from hazelcast.config import (
    MapStoreConfig,
    InitialLoadMode,
)
from hazelcast.exceptions import ConfigurationException


class InMemoryMapLoader(MapLoader[str, str]):
    """In-memory MapLoader implementation for testing."""

    def __init__(self, data: Optional[Dict[str, str]] = None):
        self._data = data or {}

    def load(self, key: str) -> Optional[str]:
        return self._data.get(key)

    def load_all(self, keys: Iterable[str]) -> Dict[str, str]:
        return {k: self._data[k] for k in keys if k in self._data}

    def load_all_keys(self) -> Iterable[str]:
        return list(self._data.keys())


class InMemoryMapStore(MapStore[str, str]):
    """In-memory MapStore implementation for testing."""

    def __init__(self, data: Optional[Dict[str, str]] = None):
        self._data = data.copy() if data else {}
        self._deleted: List[str] = []

    def load(self, key: str) -> Optional[str]:
        return self._data.get(key)

    def load_all(self, keys: Iterable[str]) -> Dict[str, str]:
        return {k: self._data[k] for k in keys if k in self._data}

    def load_all_keys(self) -> Iterable[str]:
        return list(self._data.keys())

    def store(self, key: str, value: str) -> None:
        self._data[key] = value

    def store_all(self, entries: Dict[str, str]) -> None:
        self._data.update(entries)

    def delete(self, key: str) -> None:
        self._data.pop(key, None)
        self._deleted.append(key)

    def delete_all(self, keys: Iterable[str]) -> None:
        for key in keys:
            self.delete(key)


class LifecycleAwareMapStore(InMemoryMapStore, MapLoaderLifecycleSupport):
    """MapStore with lifecycle support for testing."""

    def __init__(self):
        super().__init__()
        self.initialized = False
        self.destroyed = False
        self.init_properties: Dict[str, str] = {}
        self.init_map_name: str = ""

    def init(self, properties: Dict[str, str], map_name: str) -> None:
        self.initialized = True
        self.init_properties = properties
        self.init_map_name = map_name

    def destroy(self) -> None:
        self.destroyed = True


class TestMapLoader(unittest.TestCase):
    """Tests for MapLoader interface."""

    def test_load_single_key(self):
        data = {"key1": "value1", "key2": "value2"}
        loader = InMemoryMapLoader(data)

        self.assertEqual("value1", loader.load("key1"))
        self.assertEqual("value2", loader.load("key2"))
        self.assertIsNone(loader.load("nonexistent"))

    def test_load_all_keys(self):
        data = {"key1": "value1", "key2": "value2", "key3": "value3"}
        loader = InMemoryMapLoader(data)

        result = loader.load_all(["key1", "key3"])
        self.assertEqual({"key1": "value1", "key3": "value3"}, result)

    def test_load_all_with_missing_keys(self):
        data = {"key1": "value1"}
        loader = InMemoryMapLoader(data)

        result = loader.load_all(["key1", "nonexistent"])
        self.assertEqual({"key1": "value1"}, result)

    def test_load_all_keys_returns_all(self):
        data = {"key1": "value1", "key2": "value2"}
        loader = InMemoryMapLoader(data)

        all_keys = list(loader.load_all_keys())
        self.assertEqual(sorted(["key1", "key2"]), sorted(all_keys))

    def test_empty_loader(self):
        loader = InMemoryMapLoader()

        self.assertIsNone(loader.load("any"))
        self.assertEqual({}, loader.load_all(["any"]))
        self.assertEqual([], list(loader.load_all_keys()))


class TestMapStore(unittest.TestCase):
    """Tests for MapStore interface."""

    def test_store_single_entry(self):
        store = InMemoryMapStore()

        store.store("key1", "value1")

        self.assertEqual("value1", store.load("key1"))

    def test_store_overwrites_existing(self):
        store = InMemoryMapStore({"key1": "old"})

        store.store("key1", "new")

        self.assertEqual("new", store.load("key1"))

    def test_store_all_entries(self):
        store = InMemoryMapStore()

        store.store_all({"key1": "value1", "key2": "value2"})

        self.assertEqual("value1", store.load("key1"))
        self.assertEqual("value2", store.load("key2"))

    def test_delete_single_key(self):
        store = InMemoryMapStore({"key1": "value1", "key2": "value2"})

        store.delete("key1")

        self.assertIsNone(store.load("key1"))
        self.assertEqual("value2", store.load("key2"))
        self.assertIn("key1", store._deleted)

    def test_delete_nonexistent_key(self):
        store = InMemoryMapStore()

        store.delete("nonexistent")

        self.assertIn("nonexistent", store._deleted)

    def test_delete_all_keys(self):
        store = InMemoryMapStore({"key1": "v1", "key2": "v2", "key3": "v3"})

        store.delete_all(["key1", "key3"])

        self.assertIsNone(store.load("key1"))
        self.assertEqual("v2", store.load("key2"))
        self.assertIsNone(store.load("key3"))


class TestMapLoaderLifecycleSupport(unittest.TestCase):
    """Tests for MapLoaderLifecycleSupport interface."""

    def test_init_called(self):
        store = LifecycleAwareMapStore()
        properties = {"prop1": "val1"}

        store.init(properties, "test-map")

        self.assertTrue(store.initialized)
        self.assertEqual(properties, store.init_properties)
        self.assertEqual("test-map", store.init_map_name)

    def test_destroy_called(self):
        store = LifecycleAwareMapStore()

        store.destroy()

        self.assertTrue(store.destroyed)

    def test_lifecycle_order(self):
        store = LifecycleAwareMapStore()

        self.assertFalse(store.initialized)
        self.assertFalse(store.destroyed)

        store.init({}, "map")
        self.assertTrue(store.initialized)

        store.store("key", "value")
        self.assertEqual("value", store.load("key"))

        store.destroy()
        self.assertTrue(store.destroyed)


class TestEntryLoader(unittest.TestCase):
    """Tests for EntryLoader and EntryLoaderEntry."""

    def test_entry_loader_entry_properties(self):
        entry = EntryLoaderEntry("key1", "value1", expiration_time=1000)

        self.assertEqual("key1", entry.key)
        self.assertEqual("value1", entry.value)
        self.assertEqual(1000, entry.expiration_time)

    def test_entry_loader_entry_without_expiration(self):
        entry = EntryLoaderEntry("key1", "value1")

        self.assertEqual("key1", entry.key)
        self.assertEqual("value1", entry.value)
        self.assertIsNone(entry.expiration_time)

    def test_entry_loader_entry_repr(self):
        entry = EntryLoaderEntry("key1", "value1", expiration_time=1000)

        repr_str = repr(entry)

        self.assertIn("key1", repr_str)
        self.assertIn("value1", repr_str)
        self.assertIn("1000", repr_str)


class TestLoadAllKeysCallback(unittest.TestCase):
    """Tests for LoadAllKeysCallback interface."""

    def test_callback_interface(self):
        callback = MagicMock(spec=LoadAllKeysCallback)

        callback.on_keys(["key1", "key2"])
        callback.on_keys(["key3"])
        callback.on_complete()

        self.assertEqual(2, callback.on_keys.call_count)
        callback.on_complete.assert_called_once()

    def test_callback_error_handling(self):
        callback = MagicMock(spec=LoadAllKeysCallback)
        error = Exception("Test error")

        callback.on_error(error)

        callback.on_error.assert_called_once_with(error)


class TestMapStoreConfig(unittest.TestCase):
    """Tests for MapStoreConfig."""

    def test_default_values(self):
        config = MapStoreConfig()

        self.assertTrue(config.enabled)
        self.assertIsNone(config.class_name)
        self.assertIsNone(config.factory_class_name)
        self.assertTrue(config.write_coalescing)
        self.assertEqual(0, config.write_delay_seconds)
        self.assertEqual(1, config.write_batch_size)
        self.assertEqual(InitialLoadMode.LAZY, config.initial_load_mode)
        self.assertEqual({}, config.properties)

    def test_write_through_detection(self):
        config = MapStoreConfig(write_delay_seconds=0)

        self.assertTrue(config.is_write_through)
        self.assertFalse(config.is_write_behind)

    def test_write_behind_detection(self):
        config = MapStoreConfig(write_delay_seconds=5)

        self.assertFalse(config.is_write_through)
        self.assertTrue(config.is_write_behind)

    def test_custom_configuration(self):
        config = MapStoreConfig(
            enabled=True,
            class_name="com.example.MyMapStore",
            write_coalescing=False,
            write_delay_seconds=10,
            write_batch_size=50,
            initial_load_mode=InitialLoadMode.EAGER,
            properties={"db.url": "jdbc:mysql://localhost/db"},
        )

        self.assertTrue(config.enabled)
        self.assertEqual("com.example.MyMapStore", config.class_name)
        self.assertFalse(config.write_coalescing)
        self.assertEqual(10, config.write_delay_seconds)
        self.assertEqual(50, config.write_batch_size)
        self.assertEqual(InitialLoadMode.EAGER, config.initial_load_mode)
        self.assertEqual("jdbc:mysql://localhost/db", config.properties["db.url"])

    def test_set_property_chaining(self):
        config = MapStoreConfig()

        result = config.set_property("key1", "val1").set_property("key2", "val2")

        self.assertIs(config, result)
        self.assertEqual("val1", config.properties["key1"])
        self.assertEqual("val2", config.properties["key2"])

    def test_negative_write_delay_raises(self):
        with self.assertRaises(ConfigurationException) as ctx:
            MapStoreConfig(write_delay_seconds=-1)

        self.assertIn("write_delay_seconds", str(ctx.exception))

    def test_zero_batch_size_raises(self):
        with self.assertRaises(ConfigurationException) as ctx:
            MapStoreConfig(write_batch_size=0)

        self.assertIn("write_batch_size", str(ctx.exception))

    def test_from_dict(self):
        data = {
            "enabled": True,
            "class_name": "MyStore",
            "write_coalescing": False,
            "write_delay_seconds": 5,
            "write_batch_size": 100,
            "initial_load_mode": "EAGER",
            "properties": {"key": "value"},
        }

        config = MapStoreConfig.from_dict(data)

        self.assertTrue(config.enabled)
        self.assertEqual("MyStore", config.class_name)
        self.assertFalse(config.write_coalescing)
        self.assertEqual(5, config.write_delay_seconds)
        self.assertEqual(100, config.write_batch_size)
        self.assertEqual(InitialLoadMode.EAGER, config.initial_load_mode)
        self.assertEqual("value", config.properties["key"])

    def test_from_dict_defaults(self):
        config = MapStoreConfig.from_dict({})

        self.assertTrue(config.enabled)
        self.assertEqual(0, config.write_delay_seconds)
        self.assertEqual(InitialLoadMode.LAZY, config.initial_load_mode)

    def test_from_dict_invalid_load_mode(self):
        with self.assertRaises(ConfigurationException) as ctx:
            MapStoreConfig.from_dict({"initial_load_mode": "INVALID"})

        self.assertIn("initial_load_mode", str(ctx.exception))

    def test_setters(self):
        config = MapStoreConfig()

        config.enabled = False
        config.class_name = "NewClass"
        config.factory_class_name = "NewFactory"
        config.write_coalescing = False
        config.write_delay_seconds = 15
        config.write_batch_size = 200
        config.initial_load_mode = InitialLoadMode.EAGER
        config.properties = {"new": "props"}

        self.assertFalse(config.enabled)
        self.assertEqual("NewClass", config.class_name)
        self.assertEqual("NewFactory", config.factory_class_name)
        self.assertFalse(config.write_coalescing)
        self.assertEqual(15, config.write_delay_seconds)
        self.assertEqual(200, config.write_batch_size)
        self.assertEqual(InitialLoadMode.EAGER, config.initial_load_mode)
        self.assertEqual({"new": "props"}, config.properties)


class TestInitialLoadMode(unittest.TestCase):
    """Tests for InitialLoadMode enum."""

    def test_lazy_mode(self):
        self.assertEqual("LAZY", InitialLoadMode.LAZY.value)

    def test_eager_mode(self):
        self.assertEqual("EAGER", InitialLoadMode.EAGER.value)

    def test_from_string(self):
        self.assertEqual(InitialLoadMode.LAZY, InitialLoadMode("LAZY"))
        self.assertEqual(InitialLoadMode.EAGER, InitialLoadMode("EAGER"))


if __name__ == "__main__":
    unittest.main()
