"""Integration tests for ISet and IList operations against a real Hazelcast cluster."""

import pytest
import time
import threading
from typing import List as PyList

from tests.integration.conftest import skip_integration, DOCKER_AVAILABLE


@skip_integration
class TestSetBasicOperations:
    """Test basic ISet operations."""

    def test_add_and_contains(self, test_set):
        """Test add and contains operations."""
        result = test_set.add("item1")
        assert result is True
        assert test_set.contains("item1") is True

    def test_add_duplicate(self, test_set):
        """Test adding duplicate item."""
        test_set.add("item1")
        result = test_set.add("item1")
        assert result is False
        assert test_set.size() == 1

    def test_remove(self, test_set):
        """Test remove operation."""
        test_set.add("item1")
        
        result = test_set.remove("item1")
        assert result is True
        assert test_set.contains("item1") is False
        
        result = test_set.remove("item1")
        assert result is False

    def test_size_and_is_empty(self, test_set):
        """Test size and is_empty operations."""
        assert test_set.is_empty() is True
        assert test_set.size() == 0
        
        test_set.add("item1")
        
        assert test_set.is_empty() is False
        assert test_set.size() == 1

    def test_clear(self, test_set):
        """Test clear operation."""
        for i in range(10):
            test_set.add(f"item{i}")
        
        assert test_set.size() == 10
        test_set.clear()
        assert test_set.size() == 0

    def test_to_array(self, test_set):
        """Test to_array operation."""
        items = {"a", "b", "c"}
        for item in items:
            test_set.add(item)
        
        array = test_set.to_array()
        assert set(array) == items

    def test_add_all(self, test_set):
        """Test add_all operation."""
        items = ["a", "b", "c", "d"]
        test_set.add_all(items)
        
        assert test_set.size() == 4
        for item in items:
            assert test_set.contains(item) is True

    def test_remove_all(self, test_set):
        """Test remove_all operation."""
        test_set.add_all(["a", "b", "c", "d"])
        test_set.remove_all(["a", "c"])
        
        assert test_set.size() == 2
        assert test_set.contains("a") is False
        assert test_set.contains("b") is True

    def test_retain_all(self, test_set):
        """Test retain_all operation."""
        test_set.add_all(["a", "b", "c", "d"])
        test_set.retain_all(["b", "d"])
        
        assert test_set.size() == 2
        assert test_set.contains("a") is False
        assert test_set.contains("b") is True

    def test_contains_all(self, test_set):
        """Test contains_all operation."""
        test_set.add_all(["a", "b", "c"])
        
        assert test_set.contains_all(["a", "b"]) is True
        assert test_set.contains_all(["a", "x"]) is False


@skip_integration
class TestSetUniqueness:
    """Test ISet uniqueness guarantees."""

    def test_no_duplicates(self, test_set):
        """Test set maintains uniqueness."""
        for _ in range(10):
            test_set.add("same-item")
        
        assert test_set.size() == 1

    def test_uniqueness_with_concurrent_adds(self, test_set):
        """Test uniqueness under concurrent adds."""
        errors: PyList[Exception] = []
        
        def worker():
            try:
                for i in range(100):
                    test_set.add(f"item-{i % 10}")
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=worker) for _ in range(5)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert test_set.size() == 10


@skip_integration
class TestListBasicOperations:
    """Test basic IList operations."""

    def test_add_and_get(self, test_list):
        """Test add and get operations."""
        test_list.add("item1")
        result = test_list.get(0)
        assert result == "item1"

    def test_add_at_index(self, test_list):
        """Test add at specific index."""
        test_list.add("a")
        test_list.add("c")
        test_list.add_at(1, "b")
        
        assert test_list.get(0) == "a"
        assert test_list.get(1) == "b"
        assert test_list.get(2) == "c"

    def test_set(self, test_list):
        """Test set operation."""
        test_list.add("old")
        old = test_list.set(0, "new")
        
        assert old == "old"
        assert test_list.get(0) == "new"

    def test_remove_at(self, test_list):
        """Test remove at index."""
        test_list.add_all(["a", "b", "c"])
        removed = test_list.remove_at(1)
        
        assert removed == "b"
        assert test_list.size() == 2
        assert test_list.get(1) == "c"

    def test_remove_item(self, test_list):
        """Test remove specific item."""
        test_list.add_all(["a", "b", "c", "b"])
        result = test_list.remove("b")
        
        assert result is True
        assert test_list.size() == 3

    def test_index_of(self, test_list):
        """Test index_of operation."""
        test_list.add_all(["a", "b", "c", "b"])
        
        assert test_list.index_of("b") == 1
        assert test_list.index_of("x") == -1

    def test_last_index_of(self, test_list):
        """Test last_index_of operation."""
        test_list.add_all(["a", "b", "c", "b"])
        
        assert test_list.last_index_of("b") == 3
        assert test_list.last_index_of("x") == -1

    def test_size_and_is_empty(self, test_list):
        """Test size and is_empty operations."""
        assert test_list.is_empty() is True
        assert test_list.size() == 0
        
        test_list.add("item1")
        
        assert test_list.is_empty() is False
        assert test_list.size() == 1

    def test_clear(self, test_list):
        """Test clear operation."""
        test_list.add_all(["a", "b", "c"])
        test_list.clear()
        assert test_list.size() == 0

    def test_contains(self, test_list):
        """Test contains operation."""
        test_list.add("item1")
        
        assert test_list.contains("item1") is True
        assert test_list.contains("nonexistent") is False

    def test_to_array(self, test_list):
        """Test to_array operation."""
        items = ["a", "b", "c"]
        test_list.add_all(items)
        
        array = test_list.to_array()
        assert array == items

    def test_sub_list(self, test_list):
        """Test sub_list operation."""
        test_list.add_all(["a", "b", "c", "d", "e"])
        sub = test_list.sub_list(1, 4)
        
        assert sub == ["b", "c", "d"]


@skip_integration
class TestListOrdering:
    """Test IList ordering."""

    def test_maintains_insertion_order(self, test_list):
        """Test list maintains insertion order."""
        items = ["first", "second", "third", "fourth"]
        
        for item in items:
            test_list.add(item)
        
        for i, expected in enumerate(items):
            assert test_list.get(i) == expected

    def test_allows_duplicates(self, test_list):
        """Test list allows duplicate items."""
        for _ in range(5):
            test_list.add("duplicate")
        
        assert test_list.size() == 5


@skip_integration
class TestListConcurrency:
    """Test concurrent IList operations."""

    def test_concurrent_adds(self, test_list):
        """Test concurrent add operations."""
        errors: PyList[Exception] = []
        
        def worker(thread_id: int):
            try:
                for i in range(50):
                    test_list.add(f"thread-{thread_id}-item-{i}")
            except Exception as e:
                errors.append(e)
        
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert len(errors) == 0
        assert test_list.size() == 250


@skip_integration
class TestCollectionListeners:
    """Test collection item listeners."""

    def test_set_item_listener(self, test_set):
        """Test set item added listener."""
        events: PyList[dict] = []
        
        def on_item_added(event):
            events.append({"item": event.item})
        
        test_set.add_item_listener(on_item_added=on_item_added)
        test_set.add("test-item")
        
        time.sleep(0.5)
        assert len(events) >= 1

    def test_list_item_listener(self, test_list):
        """Test list item added listener."""
        events: PyList[dict] = []
        
        def on_item_added(event):
            events.append({"item": event.item})
        
        test_list.add_item_listener(on_item_added=on_item_added)
        test_list.add("test-item")
        
        time.sleep(0.5)
        assert len(events) >= 1
