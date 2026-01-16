"""Tests for hazelcast/cp/__init__.py module."""

import pytest

import hazelcast.cp as cp_module
from hazelcast.cp import (
    AtomicLong,
    AtomicReference,
    CPMap,
    FencedLock,
    Semaphore,
    CountDownLatch,
    CPSession,
    CPSessionState,
    CPSessionManager,
    CPGroup,
    CPGroupStatus,
    CPMember,
    CPGroupManager,
)


class TestCPModuleExports:
    """Tests for CP module exports."""

    def test_all_list_completeness(self):
        """Test that __all__ contains all expected exports."""
        expected_exports = [
            "AtomicLong",
            "AtomicReference",
            "CPMap",
            "FencedLock",
            "Semaphore",
            "CountDownLatch",
            "CPSession",
            "CPSessionState",
            "CPSessionManager",
            "CPGroup",
            "CPGroupStatus",
            "CPMember",
            "CPGroupManager",
        ]
        
        for export in expected_exports:
            assert export in cp_module.__all__, f"{export} should be in __all__"

    def test_all_list_no_extra_items(self):
        """Test that __all__ doesn't contain unexpected items."""
        expected_exports = {
            "AtomicLong",
            "AtomicReference",
            "CPMap",
            "FencedLock",
            "Semaphore",
            "CountDownLatch",
            "CPSession",
            "CPSessionState",
            "CPSessionManager",
            "CPGroup",
            "CPGroupStatus",
            "CPMember",
            "CPGroupManager",
        }
        
        for item in cp_module.__all__:
            assert item in expected_exports, f"Unexpected item {item} in __all__"

    @pytest.mark.parametrize("class_name", [
        "AtomicLong",
        "AtomicReference",
        "CPMap",
        "FencedLock",
        "Semaphore",
        "CountDownLatch",
        "CPSession",
        "CPSessionState",
        "CPSessionManager",
        "CPGroup",
        "CPGroupStatus",
        "CPMember",
        "CPGroupManager",
    ])
    def test_class_is_importable(self, class_name):
        """Test that each class can be imported from the module."""
        cls = getattr(cp_module, class_name)
        assert cls is not None

    def test_atomic_long_import(self):
        """Test AtomicLong can be imported."""
        assert AtomicLong is not None

    def test_atomic_reference_import(self):
        """Test AtomicReference can be imported."""
        assert AtomicReference is not None

    def test_cp_map_import(self):
        """Test CPMap can be imported."""
        assert CPMap is not None

    def test_fenced_lock_import(self):
        """Test FencedLock can be imported."""
        assert FencedLock is not None

    def test_semaphore_import(self):
        """Test Semaphore can be imported."""
        assert Semaphore is not None

    def test_count_down_latch_import(self):
        """Test CountDownLatch can be imported."""
        assert CountDownLatch is not None

    def test_cp_session_import(self):
        """Test CPSession can be imported."""
        assert CPSession is not None

    def test_cp_session_state_import(self):
        """Test CPSessionState can be imported."""
        assert CPSessionState is not None

    def test_cp_session_manager_import(self):
        """Test CPSessionManager can be imported."""
        assert CPSessionManager is not None

    def test_cp_group_import(self):
        """Test CPGroup can be imported."""
        assert CPGroup is not None

    def test_cp_group_status_import(self):
        """Test CPGroupStatus can be imported."""
        assert CPGroupStatus is not None

    def test_cp_member_import(self):
        """Test CPMember can be imported."""
        assert CPMember is not None

    def test_cp_group_manager_import(self):
        """Test CPGroupManager can be imported."""
        assert CPGroupManager is not None


class TestCPModuleStructure:
    """Tests for CP module structure."""

    def test_atomic_structures_category(self):
        """Test atomic data structures are exported."""
        assert hasattr(cp_module, 'AtomicLong')
        assert hasattr(cp_module, 'AtomicReference')

    def test_sync_primitives_category(self):
        """Test synchronization primitives are exported."""
        assert hasattr(cp_module, 'FencedLock')
        assert hasattr(cp_module, 'Semaphore')
        assert hasattr(cp_module, 'CountDownLatch')

    def test_session_management_category(self):
        """Test session management classes are exported."""
        assert hasattr(cp_module, 'CPSession')
        assert hasattr(cp_module, 'CPSessionState')
        assert hasattr(cp_module, 'CPSessionManager')

    def test_group_management_category(self):
        """Test group management classes are exported."""
        assert hasattr(cp_module, 'CPGroup')
        assert hasattr(cp_module, 'CPGroupStatus')
        assert hasattr(cp_module, 'CPMember')
        assert hasattr(cp_module, 'CPGroupManager')

    def test_map_category(self):
        """Test map is exported."""
        assert hasattr(cp_module, 'CPMap')
