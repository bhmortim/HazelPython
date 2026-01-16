"""CP Subsystem for Hazelcast.

The CP Subsystem provides strongly consistent distributed data structures
using the Raft consensus algorithm. It includes:

- AtomicLong: Distributed atomic long counter
- AtomicReference: Distributed atomic reference
- CPMap: Distributed map with strong consistency
- FencedLock: Distributed mutex with fencing tokens
- Semaphore: Distributed semaphore
- CountDownLatch: Distributed countdown synchronization
- CPSession: CP session representation
- CPSessionManager: Session lifecycle management
- CPGroup: CP group representation
- CPGroupManager: Group management

These primitives require a CP Subsystem to be enabled on the Hazelcast
cluster with at least 3 members for fault tolerance.
"""

from hazelcast.cp.atomic import AtomicLong, AtomicReference
from hazelcast.cp.cp_map import CPMap
from hazelcast.cp.sync import FencedLock, Semaphore, CountDownLatch
from hazelcast.cp.session import CPSession, CPSessionState, CPSessionManager
from hazelcast.cp.group import CPGroup, CPGroupStatus, CPMember, CPGroupManager

__all__ = [
    # Atomic data structures
    "AtomicLong",
    "AtomicReference",
    # Map
    "CPMap",
    # Synchronization primitives
    "FencedLock",
    "Semaphore",
    "CountDownLatch",
    # Session management
    "CPSession",
    "CPSessionState",
    "CPSessionManager",
    # Group management
    "CPGroup",
    "CPGroupStatus",
    "CPMember",
    "CPGroupManager",
]
