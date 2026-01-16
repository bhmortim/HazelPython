"""Hazelcast distributed data structure proxies.

This package provides proxy classes for interacting with Hazelcast
distributed data structures. Each proxy represents a client-side
handle to a server-side distributed object.

Available Data Structures:
    - MapProxy: Distributed key-value map
    - MultiMapProxy: Map with multiple values per key
    - ReplicatedMapProxy: Fully replicated map
    - QueueProxy: Distributed blocking queue
    - SetProxy: Distributed set (no duplicates)
    - ListProxy: Distributed ordered list
    - RingbufferProxy: Bounded circular buffer
    - TopicProxy: Publish-subscribe messaging
    - ReliableTopicProxy: Reliable pub-sub with ordering
    - PNCounterProxy: CRDT counter
    - CardinalityEstimator: HyperLogLog cardinality estimation
    - ExecutorServiceProxy: Distributed task execution
    - ScheduledExecutorServiceProxy: Scheduled task execution
    - DurableExecutorService: Durable task execution
    - Cache: JCache-compliant distributed cache
    - VectorCollection: Vector similarity search

Example:
    >>> from hazelcast import HazelcastClient
    >>> client = HazelcastClient()
    >>> my_map = client.get_map("my-map")
    >>> my_map.put("key", "value")
"""

from hazelcast.proxy.base import Proxy, DistributedObject, ProxyContext
from hazelcast.proxy.map import MapProxy, EntryEvent, EntryListener
from hazelcast.proxy.multi_map import MultiMapProxy
from hazelcast.proxy.replicated_map import (
    ReplicatedMapProxy,
    EntryEvent as ReplicatedMapEntryEvent,
    EntryListener as ReplicatedMapEntryListener,
)
from hazelcast.proxy.queue import QueueProxy, ItemEvent, ItemEventType, ItemListener
from hazelcast.proxy.collections import SetProxy, ListProxy
from hazelcast.proxy.ringbuffer import RingbufferProxy, OverflowPolicy, ReadResultSet
from hazelcast.proxy.topic import TopicProxy, TopicMessage, MessageListener, LocalTopicStats
from hazelcast.proxy.reliable_topic import (
    ReliableTopicProxy,
    ReliableTopicConfig,
    ReliableMessageListener,
    TopicOverloadPolicy,
)
from hazelcast.proxy.pn_counter import PNCounterProxy
from hazelcast.proxy.cardinality_estimator import (
    CardinalityEstimator,
    CardinalityEstimatorProxy,
)
from hazelcast.proxy.executor import (
    IExecutorService,
    ExecutorService,
    ExecutorServiceProxy,
    Callable,
    Runnable,
    ExecutionCallback,
    MultiExecutionCallback,
    Member,
)
from hazelcast.processor import (
    EntryProcessor,
    EntryProcessorEntry,
    SimpleEntryProcessorEntry,
)

__all__ = [
    # Base
    "Proxy",
    "DistributedObject",
    "ProxyContext",
    # Map
    "MapProxy",
    "EntryEvent",
    "EntryListener",
    # MultiMap
    "MultiMapProxy",
    # ReplicatedMap
    "ReplicatedMapProxy",
    "ReplicatedMapEntryEvent",
    "ReplicatedMapEntryListener",
    # Queue
    "QueueProxy",
    "ItemEvent",
    "ItemEventType",
    "ItemListener",
    # Collections
    "SetProxy",
    "ListProxy",
    # Ringbuffer
    "RingbufferProxy",
    "OverflowPolicy",
    "ReadResultSet",
    # Topic
    "TopicProxy",
    "TopicMessage",
    "MessageListener",
    "LocalTopicStats",
    # Reliable Topic
    "ReliableTopicProxy",
    "ReliableTopicConfig",
    "ReliableMessageListener",
    "TopicOverloadPolicy",
    # PNCounter
    "PNCounterProxy",
    # CardinalityEstimator
    "CardinalityEstimator",
    "CardinalityEstimatorProxy",
    # Executor Service
    "IExecutorService",
    "ExecutorService",
    "ExecutorServiceProxy",
    "Callable",
    "Runnable",
    "ExecutionCallback",
    "MultiExecutionCallback",
    "Member",
    # Entry Processor
    "EntryProcessor",
    "EntryProcessorEntry",
    "SimpleEntryProcessorEntry",
]
