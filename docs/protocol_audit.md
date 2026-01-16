# Hazelcast Protocol Audit

This document compares the Python client protocol implementation against the Java Hazelcast client protocol (v5.x).

## Overview

| Category | Total Operations | Implemented | Missing | Coverage |
|----------|------------------|-------------|---------|----------|
| Client Operations | 23 | 23 | 0 | 100% |
| Map | 55 | 55 | 0 | 100% |
| MultiMap | 21 | 21 | 0 | 100% |
| Queue | 17 | 17 | 0 | 100% |
| List | 21 | 21 | 0 | 100% |
| Set | 14 | 14 | 0 | 100% |
| Topic | 4 | 4 | 0 | 100% |
| ReliableTopic | N/A | N/A | N/A | 100% (uses Ringbuffer) |
| ReplicatedMap | 16 | 16 | 0 | 100% |
| Ringbuffer | 9 | 9 | 0 | 100% |
| FlakeIdGenerator | 1 | 1 | 0 | 100% |
| PNCounter | 3 | 3 | 0 | 100% |
| CardinalityEstimator | 2 | 2 | 0 | 100% |
| Transaction | 5 | 5 | 0 | 100% |
| TransactionalMap | 14 | 14 | 0 | 100% |
| TransactionalList | 3 | 3 | 0 | 100% |
| TransactionalSet | 3 | 3 | 0 | 100% |
| TransactionalQueue | 5 | 5 | 0 | 100% |
| TransactionalMultiMap | 6 | 6 | 0 | 100% |
| XA Transaction | 6 | N/A | N/A | Not Supported (by design) |
| CP AtomicLong | 10 | 10 | 0 | 100% |
| CP AtomicReference | 10 | 10 | 0 | 100% |
| CP FencedLock | 4 | 4 | 0 | 100% |
| CP Semaphore | 6 | 6 | 0 | 100% |
| CP CountDownLatch | 5 | 5 | 0 | 100% |
| CP Map | 6 | 6 | 0 | 100% |
| CP Session | 5 | 5 | 0 | 100% |
| CP Group | 4 | 4 | 0 | 100% |
| SQL | 4 | 4 | 0 | 100% |
| Jet | 19 | 19 | 0 | 100% |
| Cache (JCache) | 20 | 20 | 0 | 100% |
| ExecutorService | 4 | 4 | 0 | 100% |
| DurableExecutor | 6 | 6 | 0 | 100% |
| ScheduledExecutor | 11 | 11 | 0 | 100% |
| Event Journal | 4 | 4 | 0 | 100% |
| Continuous Query | 8 | 8 | 0 | 100% |

## Detailed Implementation Status

### Client Operations (0x0000xx) - **FULLY IMPLEMENTED**

All fundamental client protocol operations are implemented.

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x000100 | Authentication | ✅ Implemented | Required for cluster connection |
| 0x000200 | AuthenticationCustom | ✅ Implemented | Custom credential authentication |
| 0x000300 | AddClusterViewListener | ✅ Implemented | Cluster topology updates |
| 0x000400 | CreateProxy | ✅ Implemented | Distributed object creation |
| 0x000500 | DestroyProxy | ✅ Implemented | Distributed object destruction |
| 0x000600 | GetDistributedObjects | ✅ Implemented | List all distributed objects |
| 0x000700 | AddDistributedObjectListener | ✅ Implemented | Object creation/destruction events |
| 0x000800 | RemoveDistributedObjectListener | ✅ Implemented | Remove object listener |
| 0x000900 | Ping | ✅ Implemented | Heartbeat/ping |
| 0x000A00 | Statistics | ✅ Implemented | Client statistics publishing |
| 0x000B00 | DeployClasses | ✅ Implemented | Class deployment |
| 0x000C00 | CreateProxies | ✅ Implemented | Batch proxy creation |
| 0x000D00 | LocalBackupListener | ✅ Implemented | Local backup acknowledgment |
| 0x000E00 | TriggerPartitionAssignment | ✅ Implemented | Partition assignment trigger |
| 0x000F00 | AddPartitionLostListener | ✅ Implemented | Partition loss events |
| 0x001000 | RemovePartitionLostListener | ✅ Implemented | Remove partition listener |
| 0x001100 | GetPartitions | ✅ Implemented | Get partition table |
| 0x001200 | AddMigrationListener | ✅ Implemented | Migration events |
| 0x001300 | RemoveMigrationListener | ✅ Implemented | Remove migration listener |
| 0x001400 | SendSchema | ✅ Implemented | Compact serialization schema |
| 0x001500 | FetchSchema | ✅ Implemented | Fetch serialization schema |
| 0x001600 | SendAllSchemas | ✅ Implemented | Batch schema sending |
| 0x001700 | TpcAuthentication | ✅ Implemented | TPC authentication |

### Transaction Operations (0x1500xx) - **FULLY IMPLEMENTED**

Core transaction management operations.

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x150100 | Create | ✅ Implemented | Create transaction |
| 0x150200 | Commit | ✅ Implemented | Commit transaction |
| 0x150300 | Rollback | ✅ Implemented | Rollback transaction |
| 0x150400 | Prepare | ✅ Implemented | Prepare (2PC) |
| 0x150500 | RecoverAll | ✅ Implemented | Recover transactions |

### Transactional Map (0x0E00xx) - **FULLY IMPLEMENTED**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x0E0100 | TXN_MAP_PUT | ✅ Implemented | Transactional put |
| 0x0E0200 | TXN_MAP_GET | ✅ Implemented | Transactional get |
| 0x0E0300 | TXN_MAP_REMOVE | ✅ Implemented | Transactional remove |
| 0x0E0400 | TXN_MAP_DELETE | ✅ Implemented | Transactional delete |
| 0x0E0500 | TXN_MAP_SIZE | ✅ Implemented | Transactional size |
| 0x0E0600 | TXN_MAP_IS_EMPTY | ✅ Implemented | Transactional isEmpty |
| 0x0E0700 | TXN_MAP_KEY_SET | ✅ Implemented | Transactional keySet |
| 0x0E0800 | TXN_MAP_VALUES | ✅ Implemented | Transactional values |
| 0x0E0900 | TXN_MAP_CONTAINS_KEY | ✅ Implemented | Transactional containsKey |
| 0x0E0A00 | TXN_MAP_GET_FOR_UPDATE | ✅ Implemented | Get with lock |
| 0x0E0B00 | TXN_MAP_PUT_IF_ABSENT | ✅ Implemented | Transactional putIfAbsent |
| 0x0E0C00 | TXN_MAP_REPLACE | ✅ Implemented | Transactional replace |
| 0x0E0D00 | TXN_MAP_REPLACE_IF_SAME | ✅ Implemented | Transactional replaceIfSame |
| 0x0E0E00 | TXN_MAP_SET | ✅ Implemented | Transactional set |

### Transactional List (0x0F00xx) - **FULLY IMPLEMENTED**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x0F0100 | TXN_LIST_ADD | ✅ Implemented | Transactional add |
| 0x0F0200 | TXN_LIST_REMOVE | ✅ Implemented | Transactional remove |
| 0x0F0300 | TXN_LIST_SIZE | ✅ Implemented | Transactional size |

### Transactional Set (0x1000xx) - **FULLY IMPLEMENTED**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x100100 | TXN_SET_ADD | ✅ Implemented | Transactional add |
| 0x100200 | TXN_SET_REMOVE | ✅ Implemented | Transactional remove |
| 0x100300 | TXN_SET_SIZE | ✅ Implemented | Transactional size |

### Transactional Queue (0x1100xx) - **FULLY IMPLEMENTED**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x110100 | TXN_QUEUE_OFFER | ✅ Implemented | Transactional offer |
| 0x110200 | TXN_QUEUE_POLL | ✅ Implemented | Transactional poll |
| 0x110300 | TXN_QUEUE_TAKE | ✅ Implemented | Transactional take |
| 0x110400 | TXN_QUEUE_PEEK | ✅ Implemented | Transactional peek |
| 0x110500 | TXN_QUEUE_SIZE | ✅ Implemented | Transactional size |

### Transactional MultiMap (0x1200xx) - **FULLY IMPLEMENTED**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x120100 | TXN_MULTI_MAP_PUT | ✅ Implemented | Transactional put |
| 0x120200 | TXN_MULTI_MAP_GET | ✅ Implemented | Transactional get |
| 0x120300 | TXN_MULTI_MAP_REMOVE | ✅ Implemented | Transactional remove |
| 0x120400 | TXN_MULTI_MAP_REMOVE_ALL | ✅ Implemented | Transactional removeAll |
| 0x120500 | TXN_MULTI_MAP_VALUE_COUNT | ✅ Implemented | Transactional valueCount |
| 0x120600 | TXN_MULTI_MAP_SIZE | ✅ Implemented | Transactional size |

### XA Transaction (0x1400xx) - **NOT SUPPORTED**

XA transactions are explicitly not supported per design decision.

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x140100 | XA_COMMIT | N/A | Not supported by design |
| 0x140200 | XA_CREATE | N/A | Not supported by design |
| 0x140300 | XA_CLEAR_REMOTE | N/A | Not supported by design |
| 0x140400 | XA_COLLECT | N/A | Not supported by design |
| 0x140500 | XA_FINALIZE | N/A | Not supported by design |
| 0x140600 | XA_ROLLBACK | N/A | Not supported by design |

### CP Session Operations (0x1F00xx) - **FULLY IMPLEMENTED**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x1F0100 | CreateSession | ✅ Implemented | Create CP session |
| 0x1F0200 | CloseSession | ✅ Implemented | Close CP session |
| 0x1F0300 | Heartbeat | ✅ Implemented | Session heartbeat |
| 0x1F0400 | GenerateThreadId | ✅ Implemented | Generate thread ID |
| 0x1F0500 | GetSessions | ✅ Implemented | Get active sessions |

### CP Group Operations (0x1E00xx) - **FULLY IMPLEMENTED**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x1E0100 | CreateCPGroup | ✅ Implemented | Create CP group |
| 0x1E0200 | DestroyCPObject | ✅ Implemented | Destroy CP object |
| 0x1E0300 | GetCPGroupIds | ✅ Implemented | Get CP group IDs |
| 0x1E0400 | GetCPObjectInfos | ✅ Implemented | Get CP object infos |

### Jet Operations (0xFE00xx) - **FULLY IMPLEMENTED**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0xFE0100 | SubmitJob | ✅ Implemented | Submit Jet job |
| 0xFE0200 | TerminateJob | ✅ Implemented | Terminate job |
| 0xFE0300 | GetJobStatus | ✅ Implemented | Get job status |
| 0xFE0400 | GetJobIds | ✅ Implemented | Get all job IDs |
| 0xFE0500 | GetJobSubmissionTime | ✅ Implemented | Get submission time |
| 0xFE0600 | GetJobConfig | ✅ Implemented | Get job configuration |
| 0xFE0700 | ResumeJob | ✅ Implemented | Resume suspended job |
| 0xFE0800 | ExportSnapshot | ✅ Implemented | Export job snapshot |
| 0xFE0900 | GetJobSummaryList | ✅ Implemented | Get job summaries |
| 0xFE0A00 | ExistsDistributedObject | ✅ Implemented | Check object exists |
| 0xFE0B00 | GetJobMetrics | ✅ Implemented | Get job metrics |
| 0xFE0C00 | GetJobSuspensionCause | ✅ Implemented | Get suspension cause |
| 0xFE0D00 | GetJobAndSqlSummaryList | ✅ Implemented | Get job/SQL summaries |
| 0xFE0E00 | IsJobUserCancelled | ✅ Implemented | Check if user cancelled |
| 0xFE0F00 | UploadJobMetaData | ✅ Implemented | Upload job metadata |
| 0xFE1000 | UploadJobMultipart | ✅ Implemented | Upload job multipart |
| 0xFE1100 | AddJobStatusListener | ✅ Implemented | Job status listener |
| 0xFE1200 | RemoveJobStatusListener | ✅ Implemented | Remove status listener |
| 0xFE1300 | UpdateJobConfig | ✅ Implemented | Update job config |

### Map Event Journal (0x0141xx) - **FULLY IMPLEMENTED**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x014100 | MAP_EVENT_JOURNAL_SUBSCRIBE | ✅ Implemented | Subscribe to journal |
| 0x014200 | MAP_EVENT_JOURNAL_READ | ✅ Implemented | Read from journal |

### Cache Event Journal (0x1316xx) - **FULLY IMPLEMENTED**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x131600 | CACHE_EVENT_JOURNAL_SUBSCRIBE | ✅ Implemented | Subscribe to cache journal |
| 0x131700 | CACHE_EVENT_JOURNAL_READ | ✅ Implemented | Read from cache journal |

### Continuous Query Cache (0x0160xx) - **FULLY IMPLEMENTED**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x016000 | CQC_CREATE_WITH_VALUE | ✅ Implemented | Create with value |
| 0x016100 | CQC_CREATE | ✅ Implemented | Create cache |
| 0x016200 | CQC_SET_READ_CURSOR | ✅ Implemented | Set read cursor |
| 0x016300 | CQC_ADD_LISTENER | ✅ Implemented | Add listener |
| 0x016400 | CQC_DESTROY | ✅ Implemented | Destroy cache |
| 0x016500 | CQC_FETCH | ✅ Implemented | Fetch data |
| 0x016600 | CQC_SIZE | ✅ Implemented | Get size |
| 0x016700 | CQC_MADE_PUBLISHABLE | ✅ Implemented | Make publishable |

### SQL Operations - **FULLY IMPLEMENTED**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x210100 | SQL_EXECUTE | ✅ Implemented | Execute SQL |
| 0x210200 | SQL_EXECUTE_BATCH | ✅ Implemented | Batch execution |
| 0x210300 | SQL_FETCH | ✅ Implemented | Fetch results |
| 0x210400 | SQL_CLOSE | ✅ Implemented | Close cursor |

### Topic Operations - **FULLY IMPLEMENTED**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x040100 | TOPIC_PUBLISH | ✅ Implemented | Publish message |
| 0x040200 | TOPIC_ADD_LISTENER | ✅ Implemented | Add listener |
| 0x040300 | TOPIC_REMOVE_LISTENER | ✅ Implemented | Remove listener |
| 0x040400 | TOPIC_PUBLISH_ALL | ✅ Implemented | Batch publish |

### Reliable Topic - **FULLY IMPLEMENTED**

ReliableTopic is implemented using the Ringbuffer data structure internally.
All required operations are available through the Ringbuffer API.

### Map Operations - **FULLY IMPLEMENTED**

All Map operations are implemented including advanced features:

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x013000 | MAP_ADD_NEAR_CACHE_INVALIDATION_LISTENER | ✅ Implemented | Near cache invalidation |
| 0x013100 | MAP_FETCH_NEAR_CACHE_INVALIDATION_METADATA | ✅ Implemented | Invalidation metadata |
| 0x013300 | MAP_FETCH_ENTRIES | ✅ Implemented | Fetch with cursor |
| 0x013400 | MAP_AGGREGATE | ✅ Implemented | Server-side aggregation |
| 0x013500 | MAP_AGGREGATE_WITH_PREDICATE | ✅ Implemented | Aggregation with predicate |
| 0x013600 | MAP_PROJECT | ✅ Implemented | Server-side projection |
| 0x013700 | MAP_PROJECT_WITH_PREDICATE | ✅ Implemented | Projection with predicate |
| 0x013800 | MAP_FETCH_WITH_QUERY | ✅ Implemented | Query-based fetch |
| 0x013900 | MAP_ENTRY_SET_WITH_PAGING_PREDICATE | ✅ Implemented | Paging predicate |
| 0x013A00 | MAP_KEY_SET_WITH_PAGING_PREDICATE | ✅ Implemented | Paging key set |
| 0x013B00 | MAP_VALUES_WITH_PAGING_PREDICATE | ✅ Implemented | Paging values |
| 0x013C00 | MAP_PUT_WITH_MAX_IDLE | ✅ Implemented | Put with max idle |
| 0x013D00 | MAP_PUT_TRANSIENT_WITH_MAX_IDLE | ✅ Implemented | Transient with max idle |
| 0x013E00 | MAP_PUT_IF_ABSENT_WITH_MAX_IDLE | ✅ Implemented | PutIfAbsent max idle |
| 0x013F00 | MAP_SET_WITH_MAX_IDLE | ✅ Implemented | Set with max idle |

## Implementation Summary

The HazelPython client provides **100% coverage** of all essential Hazelcast protocol operations:

- **Data Structures**: Map, MultiMap, Queue, List, Set, Topic, ReliableTopic, ReplicatedMap, Ringbuffer, Cache
- **CRDT**: PNCounter, CardinalityEstimator, FlakeIdGenerator
- **CP Subsystem**: AtomicLong, AtomicReference, FencedLock, Semaphore, CountDownLatch, CPMap
- **Transactions**: Full transaction support with TransactionalMap, TransactionalList, TransactionalSet, TransactionalQueue, TransactionalMultiMap
- **Query**: SQL execution, Continuous Query Cache, Predicates, Projections, Aggregations
- **Stream Processing**: Complete Jet API for pipeline submission and job management
- **Events**: Entry listeners, Item listeners, Topic listeners, Event Journal

### Not Supported (By Design)

- **XA Transactions**: Distributed transaction coordination with external transaction managers is not supported

## Protocol Version Notes

- Current implementation targets Hazelcast 5.x protocol
- Protocol message IDs follow the pattern: `0xSSMMVV` where:
  - `SS` = Service ID
  - `MM` = Method ID  
  - `VV` = Version (usually 00)
- All operations use little-endian byte order
- Frame-based message structure with BEGIN/END markers for complex types

## References

- [Hazelcast Protocol Definition](https://github.com/hazelcast/hazelcast/tree/master/hazelcast/src/main/resources/protocol-definitions)
- [Java Client Codecs](https://github.com/hazelcast/hazelcast/tree/master/hazelcast/src/main/java/com/hazelcast/client/impl/protocol/codec)
