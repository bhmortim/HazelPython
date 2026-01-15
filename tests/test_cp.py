"""Unit tests for CP subsystem proxies."""

import pytest
from concurrent.futures import Future
import threading
import time

from hazelcast.cp import (
    CPGroupId,
    AtomicLong,
    AtomicReference,
    CountDownLatch,
    FencedLock,
    Semaphore,
)
from hazelcast.exceptions import IllegalStateException


class TestCPGroupId:
    """Tests for CPGroupId."""

    def test_create_default(self):
        group = CPGroupId(CPGroupId.DEFAULT_GROUP_NAME)
        assert group.name == "default"
        assert group.seed == 0
        assert group.group_id == 0

    def test_create_custom(self):
        group = CPGroupId("custom", seed=1, group_id=42)
        assert group.name == "custom"
        assert group.seed == 1
        assert group.group_id == 42

    def test_equality(self):
        g1 = CPGroupId("test", 1, 2)
        g2 = CPGroupId("test", 1, 2)
        g3 = CPGroupId("test", 1, 3)
        assert g1 == g2
        assert g1 != g3

    def test_hash(self):
        g1 = CPGroupId("test", 1, 2)
        g2 = CPGroupId("test", 1, 2)
        assert hash(g1) == hash(g2)


class TestAtomicLong:
    """Tests for AtomicLong."""

    def test_create(self):
        atomic = AtomicLong("test")
        assert atomic.name == "test"
        assert atomic.get() == 0

    def test_set_get(self):
        atomic = AtomicLong("test")
        atomic.set(42)
        assert atomic.get() == 42

    def test_get_and_set(self):
        atomic = AtomicLong("test")
        atomic.set(10)
        old = atomic.get_and_set(20)
        assert old == 10
        assert atomic.get() == 20

    def test_compare_and_set_success(self):
        atomic = AtomicLong("test")
        atomic.set(10)
        assert atomic.compare_and_set(10, 20)
        assert atomic.get() == 20

    def test_compare_and_set_failure(self):
        atomic = AtomicLong("test")
        atomic.set(10)
        assert not atomic.compare_and_set(15, 20)
        assert atomic.get() == 10

    def test_increment_and_get(self):
        atomic = AtomicLong("test")
        atomic.set(5)
        result = atomic.increment_and_get()
        assert result == 6
        assert atomic.get() == 6

    def test_decrement_and_get(self):
        atomic = AtomicLong("test")
        atomic.set(5)
        result = atomic.decrement_and_get()
        assert result == 4
        assert atomic.get() == 4

    def test_get_and_increment(self):
        atomic = AtomicLong("test")
        atomic.set(5)
        result = atomic.get_and_increment()
        assert result == 5
        assert atomic.get() == 6

    def test_get_and_decrement(self):
        atomic = AtomicLong("test")
        atomic.set(5)
        result = atomic.get_and_decrement()
        assert result == 5
        assert atomic.get() == 4

    def test_add_and_get(self):
        atomic = AtomicLong("test")
        atomic.set(10)
        result = atomic.add_and_get(5)
        assert result == 15
        assert atomic.get() == 15

    def test_get_and_add(self):
        atomic = AtomicLong("test")
        atomic.set(10)
        result = atomic.get_and_add(5)
        assert result == 10
        assert atomic.get() == 15

    def test_alter(self):
        atomic = AtomicLong("test")
        atomic.set(10)
        atomic.alter(lambda x: x * 2)
        assert atomic.get() == 20

    def test_alter_and_get(self):
        atomic = AtomicLong("test")
        atomic.set(10)
        result = atomic.alter_and_get(lambda x: x * 2)
        assert result == 20

    def test_get_and_alter(self):
        atomic = AtomicLong("test")
        atomic.set(10)
        result = atomic.get_and_alter(lambda x: x * 2)
        assert result == 10
        assert atomic.get() == 20

    def test_apply(self):
        atomic = AtomicLong("test")
        atomic.set(10)
        result = atomic.apply(lambda x: x * 3)
        assert result == 30
        assert atomic.get() == 10

    def test_async_get(self):
        atomic = AtomicLong("test")
        atomic.set(42)
        future = atomic.get_async()
        assert isinstance(future, Future)
        assert future.result() == 42

    def test_destroyed_raises(self):
        atomic = AtomicLong("test")
        atomic.destroy()
        with pytest.raises(IllegalStateException):
            atomic.get()


class TestAtomicReference:
    """Tests for AtomicReference."""

    def test_create(self):
        ref = AtomicReference("test")
        assert ref.name == "test"
        assert ref.get() is None

    def test_set_get(self):
        ref = AtomicReference("test")
        ref.set("hello")
        assert ref.get() == "hello"

    def test_get_and_set(self):
        ref = AtomicReference("test")
        ref.set("old")
        old = ref.get_and_set("new")
        assert old == "old"
        assert ref.get() == "new"

    def test_compare_and_set_success(self):
        ref = AtomicReference("test")
        ref.set("old")
        assert ref.compare_and_set("old", "new")
        assert ref.get() == "new"

    def test_compare_and_set_failure(self):
        ref = AtomicReference("test")
        ref.set("old")
        assert not ref.compare_and_set("wrong", "new")
        assert ref.get() == "old"

    def test_is_null(self):
        ref = AtomicReference("test")
        assert ref.is_null()
        ref.set("value")
        assert not ref.is_null()

    def test_clear(self):
        ref = AtomicReference("test")
        ref.set("value")
        ref.clear()
        assert ref.is_null()

    def test_contains(self):
        ref = AtomicReference("test")
        ref.set("hello")
        assert ref.contains("hello")
        assert not ref.contains("world")

    def test_alter(self):
        ref = AtomicReference("test")
        ref.set("hello")
        ref.alter(lambda x: x.upper() if x else None)
        assert ref.get() == "HELLO"

    def test_apply(self):
        ref = AtomicReference("test")
        ref.set("hello")
        result = ref.apply(lambda x: len(x) if x else 0)
        assert result == 5


class TestCountDownLatch:
    """Tests for CountDownLatch."""

    def test_create(self):
        latch = CountDownLatch("test")
        assert latch.name == "test"
        assert latch.get_count() == 0

    def test_try_set_count_success(self):
        latch = CountDownLatch("test")
        assert latch.try_set_count(3)
        assert latch.get_count() == 3

    def test_try_set_count_failure_when_nonzero(self):
        latch = CountDownLatch("test")
        latch.try_set_count(3)
        assert not latch.try_set_count(5)
        assert latch.get_count() == 3

    def test_count_down(self):
        latch = CountDownLatch("test")
        latch.try_set_count(3)
        latch.count_down()
        assert latch.get_count() == 2

    def test_count_down_to_zero(self):
        latch = CountDownLatch("test")
        latch.try_set_count(2)
        latch.count_down()
        latch.count_down()
        assert latch.get_count() == 0

    def test_await_immediate(self):
        latch = CountDownLatch("test")
        assert latch.await_(timeout=0)

    def test_await_with_timeout(self):
        latch = CountDownLatch("test")
        latch.try_set_count(1)
        assert not latch.await_(timeout=0.1)


class TestFencedLock:
    """Tests for FencedLock."""

    def test_create(self):
        lock = FencedLock("test")
        assert lock.name == "test"
        assert not lock.is_locked()

    def test_lock_unlock(self):
        lock = FencedLock("test")
        fence = lock.lock()
        assert fence > 0
        assert lock.is_locked()
        lock.unlock()
        assert lock.get_lock_count() == 0

    def test_try_lock_success(self):
        lock = FencedLock("test")
        fence = lock.try_lock()
        assert fence > 0
        lock.unlock()

    def test_try_lock_timeout(self):
        lock = FencedLock("test")
        fence = lock.try_lock(timeout=0.1)
        assert fence > 0
        lock.unlock()

    def test_get_fence(self):
        lock = FencedLock("test")
        lock.lock()
        fence = lock.get_fence()
        assert fence > 0
        lock.unlock()

    def test_is_locked_by_current_thread(self):
        lock = FencedLock("test")
        lock.lock()
        assert lock.is_locked_by_current_thread()
        lock.unlock()

    def test_context_manager(self):
        lock = FencedLock("test")
        with lock as fence:
            assert fence > 0
            assert lock.is_locked()
        assert lock.get_lock_count() == 0


class TestSemaphore:
    """Tests for Semaphore."""

    def test_create(self):
        sem = Semaphore("test")
        assert sem.name == "test"

    def test_init(self):
        sem = Semaphore("test")
        assert sem.init(5)
        assert sem.available_permits() == 5

    def test_init_already_initialized(self):
        sem = Semaphore("test", initial_permits=3)
        assert not sem.init(5)

    def test_acquire_release(self):
        sem = Semaphore("test", initial_permits=5)
        sem.acquire(2)
        assert sem.available_permits() == 3
        sem.release(2)
        assert sem.available_permits() == 5

    def test_try_acquire_success(self):
        sem = Semaphore("test", initial_permits=5)
        assert sem.try_acquire(3)
        assert sem.available_permits() == 2

    def test_try_acquire_failure(self):
        sem = Semaphore("test", initial_permits=2)
        assert not sem.try_acquire(5, timeout=0.1)
        assert sem.available_permits() == 2

    def test_drain_permits(self):
        sem = Semaphore("test", initial_permits=5)
        drained = sem.drain_permits()
        assert drained == 5
        assert sem.available_permits() == 0

    def test_reduce_permits(self):
        sem = Semaphore("test", initial_permits=10)
        sem.reduce_permits(3)
        assert sem.available_permits() == 7

    def test_increase_permits(self):
        sem = Semaphore("test", initial_permits=5)
        sem.increase_permits(3)
        assert sem.available_permits() == 8


class TestDirectToLeaderRouting:
    """Tests for direct-to-leader routing option."""

    def test_default_enabled(self):
        atomic = AtomicLong("test")
        assert atomic.direct_to_leader

    def test_disable(self):
        atomic = AtomicLong("test", direct_to_leader=False)
        assert not atomic.direct_to_leader

    def test_change_at_runtime(self):
        atomic = AtomicLong("test")
        assert atomic.direct_to_leader
        atomic.direct_to_leader = False
        assert not atomic.direct_to_leader
