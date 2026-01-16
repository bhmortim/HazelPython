"""Hazelcast Python Client."""

from hazelcast.client import HazelcastClient
from hazelcast.config import (
    ClientConfig,
    NearCacheConfig,
    SerializationConfig,
    NetworkConfig,
    SecurityConfig,
    RetryConfig,
    ReconnectMode,
)
from hazelcast.failover import FailoverConfig
from hazelcast.exceptions import (
    HazelcastException,
    ClientOfflineException,
    IllegalStateException,
    IllegalArgumentException,
    TimeoutException,
    OperationTimeoutException,
    TargetDisconnectedException,
    ConfigurationException,
    AuthenticationException,
    StaleSequenceException,
)
from hazelcast.sql.service import SqlService
from hazelcast.sql.result import (
    SqlResult,
    SqlRow,
    SqlRowMetadata,
    SqlColumnMetadata,
    SqlColumnType,
)
from hazelcast.sql.statement import SqlStatement, SqlExpectedResultType
from hazelcast.jet.service import JetService
from hazelcast.jet.pipeline import Pipeline
from hazelcast.jet.job import Job, JobConfig, JobStatus, ProcessingGuarantee
from hazelcast.serialization.json import HazelcastJsonValue
from hazelcast.serialization.compact import CompactSerializer, GenericRecord
from hazelcast.near_cache import NearCache, NearCacheManager, NearCacheStats
from hazelcast.proxy import (
    Proxy,
    DistributedObject,
    MapProxy,
    EntryEvent,
    EntryListener,
    EntryProcessor,
    EntryProcessorEntry,
    MultiMapProxy,
    ReplicatedMapProxy,
    QueueProxy,
    ItemEvent,
    ItemEventType,
    ItemListener,
    SetProxy,
    ListProxy,
    RingbufferProxy,
    OverflowPolicy,
    TopicProxy,
    TopicMessage,
    MessageListener,
    ReliableTopicProxy,
    ReliableTopicConfig,
    PNCounterProxy,
    CardinalityEstimatorProxy,
    IExecutorService,
    ExecutorServiceProxy,
    Callable,
    Runnable,
    ExecutionCallback,
    MultiExecutionCallback,
    Member,
)
from hazelcast.predicate import (
    Predicate,
    SqlPredicate,
    TruePredicate,
    FalsePredicate,
    EqualPredicate,
    NotEqualPredicate,
    GreaterLessPredicate,
    BetweenPredicate,
    InPredicate,
    LikePredicate,
    ILikePredicate,
    RegexPredicate,
    AndPredicate,
    OrPredicate,
    NotPredicate,
    InstanceOfPredicate,
    PagingPredicate,
    PredicateBuilder,
)
from hazelcast.aggregator import (
    Aggregator,
    CountAggregator,
    SumAggregator,
    AverageAggregator,
    MinAggregator,
    MaxAggregator,
    DistinctValuesAggregator,
)
from hazelcast.projection import (
    Projection,
    SingleAttributeProjection,
    MultiAttributeProjection,
)
from hazelcast.listener import (
    LifecycleState,
    LifecycleEvent,
    LifecycleListener,
    MembershipListener,
    MembershipEvent,
    DistributedObjectListener,
    DistributedObjectEvent,
)
from hazelcast.transaction import (
    TransactionType,
    TransactionState,
    TransactionOptions,
    TransactionContext,
    TransactionException,
    TransactionNotActiveException,
    TransactionTimedOutException,
    TransactionalProxy,
    TransactionalMap,
    TransactionalSet,
    TransactionalList,
    TransactionalQueue,
    TransactionalMultiMap,
)
from hazelcast.store import (
    MapLoader,
    MapStore,
    MapLoaderLifecycleSupport,
    EntryLoader,
    EntryLoaderEntry,
    LoadAllKeysCallback,
)
from hazelcast.config import (
    MapStoreConfig,
    InitialLoadMode,
)

__all__ = [
    # Core
    "HazelcastClient",
    # Configuration
    "ClientConfig",
    "NearCacheConfig",
    "SerializationConfig",
    "NetworkConfig",
    "SecurityConfig",
    "RetryConfig",
    "ReconnectMode",
    "FailoverConfig",
    # Exceptions
    "HazelcastException",
    "ClientOfflineException",
    "IllegalStateException",
    "IllegalArgumentException",
    "TimeoutException",
    "OperationTimeoutException",
    "TargetDisconnectedException",
    "ConfigurationException",
    "AuthenticationException",
    "StaleSequenceException",
    # Proxy base
    "Proxy",
    "DistributedObject",
    # Map
    "MapProxy",
    "EntryEvent",
    "EntryListener",
    # Entry Processor
    "EntryProcessor",
    "EntryProcessorEntry",
    # MultiMap
    "MultiMapProxy",
    # ReplicatedMap
    "ReplicatedMapProxy",
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
    # Topic
    "TopicProxy",
    "TopicMessage",
    "MessageListener",
    "ReliableTopicProxy",
    "ReliableTopicConfig",
    # PNCounter
    "PNCounterProxy",
    # CardinalityEstimator
    "CardinalityEstimatorProxy",
    # Executor Service
    "IExecutorService",
    "ExecutorServiceProxy",
    "Callable",
    "Runnable",
    "ExecutionCallback",
    "MultiExecutionCallback",
    "Member",
    # Predicates
    "Predicate",
    "SqlPredicate",
    "TruePredicate",
    "FalsePredicate",
    "EqualPredicate",
    "NotEqualPredicate",
    "GreaterLessPredicate",
    "BetweenPredicate",
    "InPredicate",
    "LikePredicate",
    "ILikePredicate",
    "RegexPredicate",
    "AndPredicate",
    "OrPredicate",
    "NotPredicate",
    "InstanceOfPredicate",
    "PagingPredicate",
    "PredicateBuilder",
    # Aggregators
    "Aggregator",
    "CountAggregator",
    "SumAggregator",
    "AverageAggregator",
    "MinAggregator",
    "MaxAggregator",
    "DistinctValuesAggregator",
    # Projections
    "Projection",
    "SingleAttributeProjection",
    "MultiAttributeProjection",
    # Listeners
    "LifecycleState",
    "LifecycleEvent",
    "LifecycleListener",
    "MembershipListener",
    "MembershipEvent",
    "DistributedObjectListener",
    "DistributedObjectEvent",
    # Transactions
    "TransactionType",
    "TransactionState",
    "TransactionOptions",
    "TransactionContext",
    "TransactionException",
    "TransactionNotActiveException",
    "TransactionTimedOutException",
    "TransactionalProxy",
    "TransactionalMap",
    "TransactionalSet",
    "TransactionalList",
    "TransactionalQueue",
    "TransactionalMultiMap",
    # SQL
    "SqlService",
    "SqlResult",
    "SqlRow",
    "SqlRowMetadata",
    "SqlColumnMetadata",
    "SqlColumnType",
    "SqlStatement",
    "SqlExpectedResultType",
    # Jet
    "JetService",
    "Pipeline",
    "Job",
    "JobConfig",
    "JobStatus",
    "ProcessingGuarantee",
    # Serialization
    "HazelcastJsonValue",
    "CompactSerializer",
    "GenericRecord",
    # Near Cache
    "NearCache",
    "NearCacheManager",
    "NearCacheStats",
    # Map Store/Loader
    "MapLoader",
    "MapStore",
    "MapLoaderLifecycleSupport",
    "EntryLoader",
    "EntryLoaderEntry",
    "LoadAllKeysCallback",
    "MapStoreConfig",
    "InitialLoadMode",
]

__version__ = "0.1.0"
