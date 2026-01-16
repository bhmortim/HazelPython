"""CP Subsystem for Hazelcast.

The CP Subsystem provides strongly consistent distributed data structures
using the Raft consensus algorithm. It includes:

- AtomicLong: Distributed atomic long counter
- AtomicReference: Distributed atomic reference
- FencedLock: Distributed mutex with fencing tokens
- Semaphore: Distributed semaphore
- CountDownLatch: Distributed countdown synchronization

These primitives require a CP Subsystem to be enabled on the Hazelcast
cluster with at least 3 members for fault tolerance.
"""

from hazelcast.cp.atomic import AtomicLong, AtomicReference
from hazelcast.cp.sync import FencedLock, Semaphore, CountDownLatch

__all__ = [
    "AtomicLong",
    "AtomicReference",
    "FencedLock",
    "Semaphore",
    "CountDownLatch",
]
