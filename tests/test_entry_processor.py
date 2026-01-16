"""Unit tests for hazelcast/processor.py"""

import unittest
from abc import ABC

from hazelcast.processor import (
    EntryProcessorEntry,
    EntryProcessor,
    EntryBackupProcessor,
    AbstractEntryProcessor,
    SimpleEntryProcessorEntry,
    IncrementProcessor,
    DecrementProcessor,
    UpdateEntryProcessor,
    DeleteEntryProcessor,
    CompositeEntryProcessor,
)


class TestEntryProcessorEntryABC(unittest.TestCase):
    def test_is_abstract(self):
        self.assertTrue(issubclass(EntryProcessorEntry, ABC))

    def test_cannot_instantiate(self):
        with self.assertRaises(TypeError):
            EntryProcessorEntry()

    def test_has_abstract_methods(self):
        self.assertIn("get_key", EntryProcessorEntry.__abstractmethods__)
        self.assertIn("get_value", EntryProcessorEntry.__abstractmethods__)
        self.assertIn("set_value", EntryProcessorEntry.__abstractmethods__)


class TestEntryProcessorABC(unittest.TestCase):
    def test_is_abstract(self):
        self.assertTrue(issubclass(EntryProcessor, ABC))

    def test_cannot_instantiate(self):
        with self.assertRaises(TypeError):
            EntryProcessor()

    def test_has_process_abstract_method(self):
        self.assertIn("process", EntryProcessor.__abstractmethods__)


class TestEntryBackupProcessorABC(unittest.TestCase):
    def test_is_abstract(self):
        self.assertTrue(issubclass(EntryBackupProcessor, ABC))

    def test_cannot_instantiate(self):
        with self.assertRaises(TypeError):
            EntryBackupProcessor()

    def test_has_process_backup_abstract_method(self):
        self.assertIn("process_backup", EntryBackupProcessor.__abstractmethods__)


class TestAbstractEntryProcessor(unittest.TestCase):
    def test_init_default_apply_on_backup(self):
        class TestProcessor(AbstractEntryProcessor):
            def process(self, entry):
                return entry.get_value()

        proc = TestProcessor()
        self.assertTrue(proc.apply_on_backup)

    def test_init_custom_apply_on_backup(self):
        class TestProcessor(AbstractEntryProcessor):
            def process(self, entry):
                return entry.get_value()

        proc = TestProcessor(apply_on_backup=False)
        self.assertFalse(proc.apply_on_backup)

    def test_apply_on_backup_property(self):
        class TestProcessor(AbstractEntryProcessor):
            def process(self, entry):
                return entry.get_value()

        proc = TestProcessor(apply_on_backup=True)
        self.assertTrue(proc.apply_on_backup)

    def test_process_backup_calls_process_when_enabled(self):
        class TestProcessor(AbstractEntryProcessor):
            def __init__(self):
                super().__init__(apply_on_backup=True)
                self.process_called = False

            def process(self, entry):
                self.process_called = True
                return None

        proc = TestProcessor()
        entry = SimpleEntryProcessorEntry("key", "value")
        proc.process_backup(entry)
        self.assertTrue(proc.process_called)

    def test_process_backup_skips_when_disabled(self):
        class TestProcessor(AbstractEntryProcessor):
            def __init__(self):
                super().__init__(apply_on_backup=False)
                self.process_called = False

            def process(self, entry):
                self.process_called = True
                return None

        proc = TestProcessor()
        entry = SimpleEntryProcessorEntry("key", "value")
        proc.process_backup(entry)
        self.assertFalse(proc.process_called)


class TestSimpleEntryProcessorEntry(unittest.TestCase):
    def test_init_with_key_only(self):
        entry = SimpleEntryProcessorEntry("key")
        self.assertEqual(entry.get_key(), "key")
        self.assertIsNone(entry.get_value())

    def test_init_with_key_and_value(self):
        entry = SimpleEntryProcessorEntry("key", "value")
        self.assertEqual(entry.get_key(), "key")
        self.assertEqual(entry.get_value(), "value")

    def test_get_key(self):
        entry = SimpleEntryProcessorEntry("my-key", "my-value")
        self.assertEqual(entry.get_key(), "my-key")

    def test_get_value(self):
        entry = SimpleEntryProcessorEntry("key", 42)
        self.assertEqual(entry.get_value(), 42)

    def test_set_value(self):
        entry = SimpleEntryProcessorEntry("key", "old")
        result = entry.set_value("new")
        self.assertEqual(result, "new")
        self.assertEqual(entry.get_value(), "new")

    def test_delete(self):
        entry = SimpleEntryProcessorEntry("key", "value")
        entry.delete()
        self.assertIsNone(entry.get_value())
        self.assertTrue(entry.is_deleted)

    def test_is_deleted_property_false(self):
        entry = SimpleEntryProcessorEntry("key", "value")
        self.assertFalse(entry.is_deleted)

    def test_is_deleted_property_true(self):
        entry = SimpleEntryProcessorEntry("key", "value")
        entry.delete()
        self.assertTrue(entry.is_deleted)

    def test_get_value_returns_none_when_deleted(self):
        entry = SimpleEntryProcessorEntry("key", "value")
        entry.delete()
        self.assertIsNone(entry.get_value())

    def test_set_value_clears_deleted_flag(self):
        entry = SimpleEntryProcessorEntry("key", "value")
        entry.delete()
        self.assertTrue(entry.is_deleted)
        entry.set_value("new_value")
        self.assertFalse(entry.is_deleted)
        self.assertEqual(entry.get_value(), "new_value")


class TestIncrementProcessor(unittest.TestCase):
    def test_init_default_increment(self):
        proc = IncrementProcessor()
        self.assertEqual(proc.increment, 1)

    def test_init_custom_increment(self):
        proc = IncrementProcessor(increment=5)
        self.assertEqual(proc.increment, 5)

    def test_increment_property(self):
        proc = IncrementProcessor(increment=10)
        self.assertEqual(proc.increment, 10)

    def test_process_returns_old_value(self):
        proc = IncrementProcessor(increment=5)
        entry = SimpleEntryProcessorEntry("counter", 10)
        result = proc.process(entry)
        self.assertEqual(result, 10)

    def test_process_increments_value(self):
        proc = IncrementProcessor(increment=5)
        entry = SimpleEntryProcessorEntry("counter", 10)
        proc.process(entry)
        self.assertEqual(entry.get_value(), 15)

    def test_process_handles_none_as_zero(self):
        proc = IncrementProcessor(increment=3)
        entry = SimpleEntryProcessorEntry("counter")
        result = proc.process(entry)
        self.assertEqual(result, 0)
        self.assertEqual(entry.get_value(), 3)

    def test_apply_on_backup_default(self):
        proc = IncrementProcessor()
        self.assertTrue(proc.apply_on_backup)

    def test_apply_on_backup_custom(self):
        proc = IncrementProcessor(apply_on_backup=False)
        self.assertFalse(proc.apply_on_backup)


class TestDecrementProcessor(unittest.TestCase):
    def test_init_default_decrement(self):
        proc = DecrementProcessor()
        self.assertEqual(proc.decrement, 1)

    def test_init_custom_decrement(self):
        proc = DecrementProcessor(decrement=5)
        self.assertEqual(proc.decrement, 5)

    def test_decrement_property(self):
        proc = DecrementProcessor(decrement=10)
        self.assertEqual(proc.decrement, 10)

    def test_process_returns_old_value(self):
        proc = DecrementProcessor(decrement=3)
        entry = SimpleEntryProcessorEntry("counter", 10)
        result = proc.process(entry)
        self.assertEqual(result, 10)

    def test_process_decrements_value(self):
        proc = DecrementProcessor(decrement=3)
        entry = SimpleEntryProcessorEntry("counter", 10)
        proc.process(entry)
        self.assertEqual(entry.get_value(), 7)

    def test_process_handles_none_as_zero(self):
        proc = DecrementProcessor(decrement=5)
        entry = SimpleEntryProcessorEntry("counter")
        result = proc.process(entry)
        self.assertEqual(result, 0)
        self.assertEqual(entry.get_value(), -5)


class TestUpdateEntryProcessor(unittest.TestCase):
    def test_init(self):
        proc = UpdateEntryProcessor("new_value")
        self.assertEqual(proc.new_value, "new_value")

    def test_new_value_property(self):
        proc = UpdateEntryProcessor(42)
        self.assertEqual(proc.new_value, 42)

    def test_process_returns_old_value(self):
        proc = UpdateEntryProcessor("new")
        entry = SimpleEntryProcessorEntry("key", "old")
        result = proc.process(entry)
        self.assertEqual(result, "old")

    def test_process_sets_new_value(self):
        proc = UpdateEntryProcessor("new")
        entry = SimpleEntryProcessorEntry("key", "old")
        proc.process(entry)
        self.assertEqual(entry.get_value(), "new")

    def test_process_returns_none_if_no_old_value(self):
        proc = UpdateEntryProcessor("new")
        entry = SimpleEntryProcessorEntry("key")
        result = proc.process(entry)
        self.assertIsNone(result)

    def test_apply_on_backup_default(self):
        proc = UpdateEntryProcessor("value")
        self.assertTrue(proc.apply_on_backup)


class TestDeleteEntryProcessor(unittest.TestCase):
    def test_init(self):
        proc = DeleteEntryProcessor()
        self.assertTrue(proc.apply_on_backup)

    def test_process_returns_true_if_existed(self):
        proc = DeleteEntryProcessor()
        entry = SimpleEntryProcessorEntry("key", "value")
        result = proc.process(entry)
        self.assertTrue(result)

    def test_process_returns_false_if_not_existed(self):
        proc = DeleteEntryProcessor()
        entry = SimpleEntryProcessorEntry("key")
        result = proc.process(entry)
        self.assertFalse(result)

    def test_process_deletes_entry(self):
        proc = DeleteEntryProcessor()
        entry = SimpleEntryProcessorEntry("key", "value")
        proc.process(entry)
        self.assertIsNone(entry.get_value())

    def test_process_calls_delete_method(self):
        proc = DeleteEntryProcessor()
        entry = SimpleEntryProcessorEntry("key", "value")
        proc.process(entry)
        self.assertTrue(entry.is_deleted)


class TestCompositeEntryProcessor(unittest.TestCase):
    def test_init_with_processors(self):
        p1 = IncrementProcessor(5)
        p2 = IncrementProcessor(10)
        composite = CompositeEntryProcessor(p1, p2)
        self.assertEqual(len(composite.processors), 2)

    def test_init_empty(self):
        composite = CompositeEntryProcessor()
        self.assertEqual(composite.processors, [])

    def test_processors_property_returns_copy(self):
        p1 = IncrementProcessor(1)
        composite = CompositeEntryProcessor(p1)
        procs = composite.processors
        procs.append(IncrementProcessor(2))
        self.assertEqual(len(composite.processors), 1)

    def test_add_processor(self):
        composite = CompositeEntryProcessor()
        p1 = IncrementProcessor(1)
        result = composite.add_processor(p1)
        self.assertIs(result, composite)
        self.assertEqual(len(composite.processors), 1)

    def test_process_executes_all_processors(self):
        p1 = IncrementProcessor(5)
        p2 = IncrementProcessor(10)
        composite = CompositeEntryProcessor(p1, p2)
        entry = SimpleEntryProcessorEntry("counter", 0)
        results = composite.process(entry)
        self.assertEqual(results, [0, 5])
        self.assertEqual(entry.get_value(), 15)

    def test_process_returns_list_of_results(self):
        p1 = UpdateEntryProcessor("first")
        p2 = UpdateEntryProcessor("second")
        composite = CompositeEntryProcessor(p1, p2)
        entry = SimpleEntryProcessorEntry("key", "initial")
        results = composite.process(entry)
        self.assertEqual(results[0], "initial")
        self.assertEqual(results[1], "first")
        self.assertEqual(entry.get_value(), "second")

    def test_process_backup_applies_all_child_backup_processors(self):
        p1 = IncrementProcessor(5)
        p2 = IncrementProcessor(10)
        composite = CompositeEntryProcessor(p1, p2)
        entry = SimpleEntryProcessorEntry("counter", 0)
        composite.process_backup(entry)
        self.assertEqual(entry.get_value(), 15)

    def test_process_backup_handles_non_backup_processors(self):
        class SimpleProcessor(EntryProcessor):
            def process(self, entry):
                entry.set_value((entry.get_value() or 0) + 1)
                return None

        p1 = SimpleProcessor()
        composite = CompositeEntryProcessor(p1)
        entry = SimpleEntryProcessorEntry("counter", 0)
        composite.process_backup(entry)
        self.assertEqual(entry.get_value(), 1)

    def test_apply_on_backup_default(self):
        composite = CompositeEntryProcessor()
        self.assertTrue(composite.apply_on_backup)

    def test_apply_on_backup_custom(self):
        composite = CompositeEntryProcessor(apply_on_backup=False)
        self.assertFalse(composite.apply_on_backup)


if __name__ == "__main__":
    unittest.main()
