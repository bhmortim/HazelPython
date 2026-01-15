"""Hazelcast CP subsystem proxies."""

from hazelcast.cp.base import CPProxy, CPGroupId
from hazelcast.cp.atomic_long import AtomicLong
from hazelcast.cp.atomic_reference import AtomicReference
from hazelcast.cp.count_down_latch import CountDownLatch
from hazelcast.cp.fenced_lock import FencedLock
from hazelcast.cp.semaphore import Semaphore

__all__ = [
    "CPProxy",
    "CPGroupId",
    "AtomicLong",
    "AtomicReference",
    "CountDownLatch",
    "FencedLock",
    "Semaphore",
]
