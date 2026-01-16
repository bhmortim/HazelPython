"""Tests for entry processor implementations."""

import unittest
from typing import Any

from hazelcast.processor import (
    AbstractEntryProcessor,
    CompositeEntryProcessor,
    DecrementProcessor,
    DeleteEntryProcessor,
    EntryBackupProcessor,
    EntryProcessor,
    EntryProcessorEntry,
    IncrementProcessor,
    SimpleEntryProcessorEntry,
    UpdateEntryProcessor,
)


class TestSimpleEntryProcessorEntry(unittest.TestCase):
    """Tests for SimpleEntryProcessorEntry."""

    def test_get_key(self):
        entry = SimpleEntryProcessorEntry("test_key", "test_value")
        self.assertEqual("test_key", entry.get_key())

    def test_get_value(self):
        entry = SimpleEntryProcessorEntry("key", "value")
        self.assertEqual("value", entry.get_value())

    def test_get_value_none(self):
        entry = SimpleEntryProcessorEntry("key")
        self.assertIsNone(entry.get_value())

    def test_set_value(self):
        entry = SimpleEntryProcessorEntry("key", "old")
        result = entry.set_value("new")
        self.assertEqual("new", result)
        self.assertEqual("new", entry.get_value())

    def test_delete(self):
        entry = SimpleEntryProcessorEntry("key", "value")
        entry.delete()
        self.assertIsNone(entry.get_value())
        self.assertTrue(entry.is_deleted)

    def test_set_value_after_delete(self):
        entry = SimpleEntryProcessorEntry("key", "value")
        entry.delete()
        entry.set_value("new_value")
        self.assertEqual("new_value", entry.get_value())
        self.assertFalse(entry.is_deleted)


class TestIncrementProcessor(unittest.TestCase):
    """Tests for IncrementProcessor."""

    def test_default_increment(self):
        processor = IncrementProcessor()
        entry = SimpleEntryProcessorEntry("counter", 10)
        old_value = processor.process(entry)
        self.assertEqual(10, old_value)
        self.assertEqual(11, entry.get_value())

    def test_custom_increment(self):
        processor = IncrementProcessor(5)
        entry = SimpleEntryProcessorEntry("counter", 10)
        old_value = processor.process(entry)
        self.assertEqual(10, old_value)
        self.assertEqual(15, entry.get_value())

    def test_increment_none_value(self):
        processor = IncrementProcessor(3)
        entry = SimpleEntryProcessorEntry("counter")
        old_value = processor.process(entry)
        self.assertEqual(0, old_value)
        self.assertEqual(3, entry.get_value())

    def test_increment_negative(self):
        processor = IncrementProcessor(-5)
        entry = SimpleEntryProcessorEntry("counter", 10)
        old_value = processor.process(entry)
        self.assertEqual(10, old_value)
        self.assertEqual(5, entry.get_value())

    def test_increment_property(self):
        processor = IncrementProcessor(7)
        self.assertEqual(7, processor.increment)

    def test_apply_on_backup_default(self):
        processor = IncrementProcessor()
        self.assertTrue(processor.apply_on_backup)

    def test_apply_on_backup_disabled(self):
        processor = IncrementProcessor(apply_on_backup=False)
        self.assertFalse(processor.apply_on_backup)

    def test_process_backup(self):
        processor = IncrementProcessor(5)
        entry = SimpleEntryProcessorEntry("counter", 10)
        processor.process_backup(entry)
        self.assertEqual(15, entry.get_value())

    def test_process_backup_disabled(self):
        processor = IncrementProcessor(5, apply_on_backup=False)
        entry = SimpleEntryProcessorEntry("counter", 10)
        processor.process_backup(entry)
        self.assertEqual(10, entry.get_value())


class TestDecrementProcessor(unittest.TestCase):
    """Tests for DecrementProcessor."""

    def test_default_decrement(self):
        processor = DecrementProcessor()
        entry = SimpleEntryProcessorEntry("counter", 10)
        old_value = processor.process(entry)
        self.assertEqual(10, old_value)
        self.assertEqual(9, entry.get_value())

    def test_custom_decrement(self):
        processor = DecrementProcessor(3)
        entry = SimpleEntryProcessorEntry("counter", 10)
        old_value = processor.process(entry)
        self.assertEqual(10, old_value)
        self.assertEqual(7, entry.get_value())

    def test_decrement_none_value(self):
        processor = DecrementProcessor(2)
        entry = SimpleEntryProcessorEntry("counter")
        old_value = processor.process(entry)
        self.assertEqual(0, old_value)
        self.assertEqual(-2, entry.get_value())

    def test_decrement_to_negative(self):
        processor = DecrementProcessor(15)
        entry = SimpleEntryProcessorEntry("counter", 10)
        old_value = processor.process(entry)
        self.assertEqual(10, old_value)
        self.assertEqual(-5, entry.get_value())

    def test_decrement_property(self):
        processor = DecrementProcessor(4)
        self.assertEqual(4, processor.decrement)

    def test_process_backup(self):
        processor = DecrementProcessor(3)
        entry = SimpleEntryProcessorEntry("counter", 10)
        processor.process_backup(entry)
        self.assertEqual(7, entry.get_value())


class TestUpdateEntryProcessor(unittest.TestCase):
    """Tests for UpdateEntryProcessor."""

    def test_update_existing_value(self):
        processor = UpdateEntryProcessor("new_value")
        entry = SimpleEntryProcessorEntry("key", "old_value")
        old_value = processor.process(entry)
        self.assertEqual("old_value", old_value)
        self.assertEqual("new_value", entry.get_value())

    def test_update_none_value(self):
        processor = UpdateEntryProcessor("new_value")
        entry = SimpleEntryProcessorEntry("key")
        old_value = processor.process(entry)
        self.assertIsNone(old_value)
        self.assertEqual("new_value", entry.get_value())

    def test_update_with_none(self):
        processor = UpdateEntryProcessor(None)
        entry = SimpleEntryProcessorEntry("key", "old_value")
        old_value = processor.process(entry)
        self.assertEqual("old_value", old_value)
        self.assertIsNone(entry.get_value())

    def test_update_complex_object(self):
        new_data = {"name": "test", "values": [1, 2, 3]}
        processor = UpdateEntryProcessor(new_data)
        entry = SimpleEntryProcessorEntry("key", {"old": "data"})
        processor.process(entry)
        self.assertEqual(new_data, entry.get_value())

    def test_new_value_property(self):
        processor = UpdateEntryProcessor("test")
        self.assertEqual("test", processor.new_value)

    def test_process_backup(self):
        processor = UpdateEntryProcessor("backup_value")
        entry = SimpleEntryProcessorEntry("key", "original")
        processor.process_backup(entry)
        self.assertEqual("backup_value", entry.get_value())


class TestDeleteEntryProcessor(unittest.TestCase):
    """Tests for DeleteEntryProcessor."""

    def test_delete_existing_entry(self):
        processor = DeleteEntryProcessor()
        entry = SimpleEntryProcessorEntry("key", "value")
        existed = processor.process(entry)
        self.assertTrue(existed)
        self.assertIsNone(entry.get_value())
        self.assertTrue(entry.is_deleted)

    def test_delete_nonexistent_entry(self):
        processor = DeleteEntryProcessor()
        entry = SimpleEntryProcessorEntry("key")
        existed = processor.process(entry)
        self.assertFalse(existed)
        self.assertIsNone(entry.get_value())

    def test_delete_apply_on_backup(self):
        processor = DeleteEntryProcessor()
        self.assertTrue(processor.apply_on_backup)

    def test_delete_backup_disabled(self):
        processor = DeleteEntryProcessor(apply_on_backup=False)
        self.assertFalse(processor.apply_on_backup)

    def test_process_backup(self):
        processor = DeleteEntryProcessor()
        entry = SimpleEntryProcessorEntry("key", "value")
        processor.process_backup(entry)
        self.assertIsNone(entry.get_value())


class TestCompositeEntryProcessor(unittest.TestCase):
    """Tests for CompositeEntryProcessor."""

    def test_empty_composite(self):
        processor = CompositeEntryProcessor()
        entry = SimpleEntryProcessorEntry("key", 10)
        results = processor.process(entry)
        self.assertEqual([], results)
        self.assertEqual(10, entry.get_value())

    def test_single_processor(self):
        processor = CompositeEntryProcessor(IncrementProcessor(5))
        entry = SimpleEntryProcessorEntry("counter", 10)
        results = processor.process(entry)
        self.assertEqual([10], results)
        self.assertEqual(15, entry.get_value())

    def test_multiple_processors(self):
        processor = CompositeEntryProcessor(
            IncrementProcessor(5),
            IncrementProcessor(10)
        )
        entry = SimpleEntryProcessorEntry("counter", 0)
        results = processor.process(entry)
        self.assertEqual([0, 5], results)
        self.assertEqual(15, entry.get_value())

    def test_mixed_processors(self):
        processor = CompositeEntryProcessor(
            IncrementProcessor(10),
            DecrementProcessor(3)
        )
        entry = SimpleEntryProcessorEntry("counter", 5)
        results = processor.process(entry)
        self.assertEqual([5, 15], results)
        self.assertEqual(12, entry.get_value())

    def test_add_processor(self):
        processor = CompositeEntryProcessor()
        processor.add_processor(IncrementProcessor(1))
        processor.add_processor(IncrementProcessor(2))
        entry = SimpleEntryProcessorEntry("counter", 0)
        results = processor.process(entry)
        self.assertEqual([0, 1], results)
        self.assertEqual(3, entry.get_value())

    def test_add_processor_returns_self(self):
        processor = CompositeEntryProcessor()
        result = processor.add_processor(IncrementProcessor(1))
        self.assertIs(processor, result)

    def test_method_chaining(self):
        processor = (
            CompositeEntryProcessor()
            .add_processor(IncrementProcessor(1))
            .add_processor(IncrementProcessor(2))
            .add_processor(IncrementProcessor(3))
        )
        entry = SimpleEntryProcessorEntry("counter", 0)
        processor.process(entry)
        self.assertEqual(6, entry.get_value())

    def test_processors_property(self):
        inc = IncrementProcessor(1)
        dec = DecrementProcessor(1)
        processor = CompositeEntryProcessor(inc, dec)
        processors = processor.processors
        self.assertEqual(2, len(processors))
        self.assertIsInstance(processors[0], IncrementProcessor)
        self.assertIsInstance(processors[1], DecrementProcessor)

    def test_processors_property_returns_copy(self):
        processor = CompositeEntryProcessor(IncrementProcessor(1))
        processors = processor.processors
        processors.append(DecrementProcessor(1))
        self.assertEqual(1, len(processor.processors))

    def test_process_backup(self):
        processor = CompositeEntryProcessor(
            IncrementProcessor(5),
            IncrementProcessor(3)
        )
        entry = SimpleEntryProcessorEntry("counter", 0)
        processor.process_backup(entry)
        self.assertEqual(8, entry.get_value())

    def test_process_backup_disabled(self):
        processor = CompositeEntryProcessor(
            IncrementProcessor(5),
            apply_on_backup=False
        )
        entry = SimpleEntryProcessorEntry("counter", 10)
        processor.process_backup(entry)
        self.assertEqual(10, entry.get_value())


class TestAbstractEntryProcessor(unittest.TestCase):
    """Tests for AbstractEntryProcessor base class behavior."""

    def test_is_entry_processor(self):
        processor = IncrementProcessor()
        self.assertIsInstance(processor, EntryProcessor)

    def test_is_entry_backup_processor(self):
        processor = IncrementProcessor()
        self.assertIsInstance(processor, EntryBackupProcessor)

    def test_apply_on_backup_true_by_default(self):
        processor = IncrementProcessor()
        self.assertTrue(processor.apply_on_backup)

    def test_apply_on_backup_can_be_disabled(self):
        processor = IncrementProcessor(apply_on_backup=False)
        self.assertFalse(processor.apply_on_backup)


class CustomProcessor(AbstractEntryProcessor[str, int]):
    """Custom processor for testing abstract base class."""

    def __init__(self, multiplier: int = 2, apply_on_backup: bool = True):
        super().__init__(apply_on_backup)
        self._multiplier = multiplier

    def process(self, entry: EntryProcessorEntry[str, int]) -> int:
        old_value = entry.get_value() or 0
        entry.set_value(old_value * self._multiplier)
        return old_value


class TestCustomEntryProcessor(unittest.TestCase):
    """Tests for custom entry processor implementations."""

    def test_custom_processor(self):
        processor = CustomProcessor(3)
        entry = SimpleEntryProcessorEntry("key", 5)
        old_value = processor.process(entry)
        self.assertEqual(5, old_value)
        self.assertEqual(15, entry.get_value())

    def test_custom_processor_backup(self):
        processor = CustomProcessor(2)
        entry = SimpleEntryProcessorEntry("key", 4)
        processor.process_backup(entry)
        self.assertEqual(8, entry.get_value())

    def test_custom_processor_backup_disabled(self):
        processor = CustomProcessor(2, apply_on_backup=False)
        entry = SimpleEntryProcessorEntry("key", 4)
        processor.process_backup(entry)
        self.assertEqual(4, entry.get_value())


class TestEntryProcessorIntegration(unittest.TestCase):
    """Integration tests for entry processors."""

    def test_update_then_delete(self):
        composite = CompositeEntryProcessor(
            UpdateEntryProcessor("temp"),
            DeleteEntryProcessor()
        )
        entry = SimpleEntryProcessorEntry("key", "original")
        results = composite.process(entry)
        self.assertEqual("original", results[0])
        self.assertTrue(results[1])
        self.assertIsNone(entry.get_value())

    def test_increment_multiple_times(self):
        composite = CompositeEntryProcessor(
            IncrementProcessor(1),
            IncrementProcessor(1),
            IncrementProcessor(1)
        )
        entry = SimpleEntryProcessorEntry("counter", 0)
        results = composite.process(entry)
        self.assertEqual([0, 1, 2], results)
        self.assertEqual(3, entry.get_value())

    def test_nested_composite(self):
        inner = CompositeEntryProcessor(
            IncrementProcessor(5),
            IncrementProcessor(5)
        )
        outer = CompositeEntryProcessor(
            IncrementProcessor(10),
            inner
        )
        entry = SimpleEntryProcessorEntry("counter", 0)
        results = outer.process(entry)
        self.assertEqual(10, results[0])
        self.assertEqual([10, 15], results[1])
        self.assertEqual(20, entry.get_value())


if __name__ == "__main__":
    unittest.main()
