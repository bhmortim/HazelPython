"""Hazelcast distributed data structure proxies."""

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
