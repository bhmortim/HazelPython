# Changelog

All notable changes to the Hazelcast Python Client are documented in this file.

## [0.1.0] - 2024

### Added

#### Core Client
- HazelcastClient with full lifecycle management (start, shutdown, reconnection)
- ClientConfig with comprehensive configuration options
- NetworkConfig for cluster connection settings
- SecurityConfig with username/password, token, and TLS authentication
- RetryConfig with exponential backoff strategy
- FailoverConfig for multi-cluster failover support
- ReconnectMode enum (OFF, ON, ASYNC)

#### Data Structures
- IMap (MapProxy) with full CRUD operations, TTL, entry views, indexes, interceptors
- IQueue (QueueProxy) with blocking operations
- ISet (SetProxy) and IList (ListProxy) collections
- MultiMap (MultiMapProxy) for multi-value maps
- ReplicatedMap (ReplicatedMapProxy) for eventually consistent maps
- Ringbuffer (RingbufferProxy) for circular buffer operations
- ITopic (TopicProxy) and ReliableTopic (ReliableTopicProxy) for pub/sub
- PNCounter (PNCounterProxy) for CRDT counters
- CardinalityEstimator for HyperLogLog cardinality estimation
- FlakeIdGenerator for cluster-wide unique IDs

#### CP Subsystem
- AtomicLong and AtomicReference for atomic operations
- FencedLock for distributed mutex with fencing tokens
- Semaphore for distributed semaphore
- CountDownLatch for distributed countdown
- CPMap for strongly consistent distributed map
- CPSession and CPSessionManager for session management
- CPGroup and CPGroupManager for group management

#### Executor Services
- IExecutorService for distributed task execution
- IScheduledExecutorService for scheduled task execution
- IScheduledFuture for scheduled task handles
- TimeUnit enum for time conversions

#### SQL Service
- SqlService for executing SQL queries
- SqlStatement for query configuration
- SqlResult with sync and async iteration
- SqlRow, SqlRowMetadata, SqlColumnMetadata
- SqlColumnType enum with full type mappings
- SqlPage for paginated results
- EXPLAIN query support
- Streaming results with backpressure

#### Jet Service
- JetService for pipeline submission and job management
- Pipeline API with sources, sinks, and transformations
- Stage transformations: map, filter, flatMap, groupBy, aggregate
- WindowDefinition for tumbling, sliding, session windows
- AggregateOperation with built-in aggregations
- Source connectors: Map, List, File, Socket, JDBC, Kafka
- Sink connectors: Map, List, File, Socket, JDBC, Kafka, Logger
- Job lifecycle management (start, suspend, resume, cancel)
- JobConfig with processing guarantees and snapshots
- JobMetrics for runtime metrics collection

#### Serialization
- SerializationService with pluggable serializers
- Built-in serializers for Python primitives and collections
- IdentifiedDataSerializable for efficient custom serialization
- Portable serialization with schema evolution
- Compact serialization with GenericRecord support
- HazelcastJsonValue for JSON data
- JSON predicates (JsonPathPredicate, JsonContainsPredicate, JsonValuePredicate)

#### Near Cache
- NearCache with local caching
- NearCacheConfig with TTL, max-idle, eviction policies
- NearCacheManager for cache lifecycle
- NearCacheStats for cache statistics
- Eviction strategies: LRU, LFU, RANDOM, NONE
- Invalidation listeners

#### Listeners and Events
- LifecycleListener and LifecycleEvent
- MembershipListener and MembershipEvent
- DistributedObjectListener and DistributedObjectEvent
- PartitionLostListener and PartitionLostEvent
- MigrationListener and MigrationEvent
- InitialMembershipListener for member snapshots
- EntryListener for map events

#### Predicates and Queries
- Predicate base class and implementations
- SqlPredicate for SQL-like queries
- Comparison predicates (Equal, NotEqual, GreaterLess, Between, In)
- Pattern predicates (Like, ILike, Regex)
- Logical predicates (And, Or, Not)
- PagingPredicate for paginated queries
- PredicateBuilder for fluent predicate construction

#### Aggregators
- Aggregator base class
- CountAggregator, SumAggregator, AverageAggregator
- MinAggregator, MaxAggregator
- DistinctValuesAggregator

#### Projections
- Projection base class
- SingleAttributeProjection
- MultiAttributeProjection

#### Transactions
- TransactionContext for transactional operations
- TransactionOptions with timeout and durability settings
- TransactionType (ONE_PHASE, TWO_PHASE)
- Transactional proxies: Map, Set, List, Queue, MultiMap

#### Authentication
- UsernamePasswordCredentials
- TokenCredentials
- KerberosCredentials
- LdapCredentials
- CustomCredentials with factory support

#### TLS/SSL
- TlsConfig for SSL context configuration
- Mutual TLS (mTLS) support
- Certificate verification modes
- Cipher suite configuration
- TLS protocol version selection

#### Configuration Classes
- IndexConfig for map indexes (SORTED, HASH, BITMAP)
- QueryCacheConfig for continuous query caches
- WanReplicationConfig for WAN replication
- NearCacheConfig with full options
- ClientUserCodeDeploymentConfig

#### Exceptions
- HazelcastException base class
- IllegalStateException, IllegalArgumentException
- ConfigurationException
- TimeoutException, OperationTimeoutException
- AuthenticationException
- TargetDisconnectedException
- ClientOfflineException
- HazelcastSerializationException
- StaleSequenceException

### Testing
- Comprehensive unit tests with pytest
- 95%+ code coverage for core modules
- Integration test suite for cluster testing
- Fixtures and mocks for isolated testing

### Documentation
- Google-style docstrings for all public APIs
- Module-level documentation
- Configuration examples
- API usage examples
