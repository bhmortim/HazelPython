# Hazelcast Protocol Audit

This document compares the Python client protocol implementation against the Java Hazelcast client protocol (v5.x).

## Overview

| Category | Total Operations | Implemented | Missing | Coverage |
|----------|------------------|-------------|---------|----------|
| Client Operations | 15+ | 10 | 13 | ~43% |
| Map | 50+ | 45 | 5+ | ~90% |
| MultiMap | 21 | 21 | 0 | 100% |
| Queue | 17 | 17 | 0 | 100% |
| List | 21 | 21 | 0 | 100% |
| Set | 14 | 14 | 0 | 100% |
| Topic | 4 | 3 | 1 | 75% |
| ReplicatedMap | 16 | 16 | 0 | 100% |
| Ringbuffer | 9 | 9 | 0 | 100% |
| FlakeIdGenerator | 1 | 1 | 0 | 100% |
| PNCounter | 3 | 3 | 0 | 100% |
| CardinalityEstimator | 2 | 2 | 0 | 100% |
| Transaction | 5 | 0 | 5 | 0% |
| Transactional DS | 30+ | 0 | 30+ | 0% |
| XA Transaction | 6 | 0 | 6 | 0% |
| CP AtomicLong | 10 | 10 | 0 | 100% |
| CP AtomicReference | 10 | 10 | 0 | 100% |
| CP FencedLock | 4 | 4 | 0 | 100% |
| CP Semaphore | 6 | 6 | 0 | 100% |
| CP CountDownLatch | 5 | 5 | 0 | 100% |
| CP Map | 6 | 6 | 0 | 100% |
| CP Session | 5 | 5 | 0 | 100% |
| CP Group | 4 | 0 | 4 | 0% |
| SQL | 4 | 3 | 1 | 75% |
| Jet | 15+ | 0 | 15+ | 0% |
| Cache (JCache) | 20 | 20 | 0 | 100% |
| ExecutorService | 4 | 4 | 0 | 100% |
| DurableExecutor | 6 | 6 | 0 | 100% |
| ScheduledExecutor | 11 | 11 | 0 | 100% |
| Event Journal | 4 | 0 | 4 | 0% |
| Continuous Query | 8 | 0 | 8 | 0% |

## Detailed Gap Analysis

### Client Operations (0x0000xx) - **PARTIALLY IMPLEMENTED**

These are fundamental client protocol operations required for proper client lifecycle.

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x000100 | Authentication | ✅ Implemented | Required for cluster connection |
| 0x000200 | AuthenticationCustom | ✅ Implemented | Custom credential authentication |
| 0x000300 | AddClusterViewListener | ✅ Implemented | Cluster topology updates |
| 0x000400 | CreateProxy | ✅ Implemented | Distributed object creation |
| 0x000500 | DestroyProxy | ✅ Implemented | Distributed object destruction |
| 0x000600 | GetDistributedObjects | ✅ Implemented | List all distributed objects |
| 0x000700 | AddDistributedObjectListener | ❌ Missing | Object creation/destruction events |
| 0x000800 | RemoveDistributedObjectListener | ❌ Missing | Remove object listener |
| 0x000900 | Ping | ✅ Implemented | Heartbeat/ping |
| 0x000A00 | Statistics | ❌ Missing | Client statistics publishing |
| 0x000B00 | DeployClasses | ❌ Missing | Class deployment |
| 0x000C00 | CreateProxies | ❌ Missing | Batch proxy creation |
| 0x000D00 | LocalBackupListener | ❌ Missing | Local backup acknowledgment |
| 0x000E00 | TriggerPartitionAssignment | ❌ Missing | Partition assignment trigger |
| 0x000F00 | AddPartitionLostListener | ❌ Missing | Partition loss events |
| 0x001000 | RemovePartitionLostListener | ❌ Missing | Remove partition listener |
| 0x001100 | GetPartitions | ✅ Implemented | Get partition table |
| 0x001200 | AddMigrationListener | ❌ Missing | Migration events |
| 0x001300 | RemoveMigrationListener | ❌ Missing | Remove migration listener |
| 0x001400 | SendSchema | ✅ Implemented | Compact serialization schema |
| 0x001500 | FetchSchema | ✅ Implemented | Fetch serialization schema |
| 0x001600 | SendAllSchemas | ❌ Missing | Batch schema sending |
| 0x001700 | TpcAuthentication | ❌ Missing | TPC authentication |

### Transaction Operations (0x1500xx) - **MISSING**

Core transaction management operations.

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x150100 | Create | ❌ Missing | Create transaction |
| 0x150200 | Commit | ❌ Missing | Commit transaction |
| 0x150300 | Rollback | ❌ Missing | Rollback transaction |

### Transactional Map (0x0E00xx) - **MISSING**

Constants defined but no codec implementation.

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x0E0100 | TXN_MAP_PUT | ❌ Missing | Transactional put |
| 0x0E0200 | TXN_MAP_GET | ❌ Missing | Transactional get |
| 0x0E0300 | TXN_MAP_REMOVE | ❌ Missing | Transactional remove |
| 0x0E0400 | TXN_MAP_DELETE | ❌ Missing | Transactional delete |
| 0x0E0500 | TXN_MAP_SIZE | ❌ Missing | Transactional size |
| 0x0E0600 | TXN_MAP_IS_EMPTY | ❌ Missing | Transactional isEmpty |
| 0x0E0700 | TXN_MAP_KEY_SET | ❌ Missing | Transactional keySet |
| 0x0E0800 | TXN_MAP_VALUES | ❌ Missing | Transactional values |
| 0x0E0900 | TXN_MAP_CONTAINS_KEY | ❌ Missing | Transactional containsKey |
| 0x0E0A00 | TXN_MAP_GET_FOR_UPDATE | ❌ Missing | Get with lock |
| 0x0E0B00 | TXN_MAP_PUT_IF_ABSENT | ❌ Missing | Transactional putIfAbsent |
| 0x0E0C00 | TXN_MAP_REPLACE | ❌ Missing | Transactional replace |
| 0x0E0D00 | TXN_MAP_REPLACE_IF_SAME | ❌ Missing | Transactional replaceIfSame |
| 0x0E0E00 | TXN_MAP_SET | ❌ Missing | Transactional set |

### Transactional List (0x0F00xx) - **MISSING**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x0F0100 | TXN_LIST_ADD | ❌ Missing | Transactional add |
| 0x0F0200 | TXN_LIST_REMOVE | ❌ Missing | Transactional remove |
| 0x0F0300 | TXN_LIST_SIZE | ❌ Missing | Transactional size |

### Transactional Set (0x1000xx) - **MISSING**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x100100 | TXN_SET_ADD | ❌ Missing | Transactional add |
| 0x100200 | TXN_SET_REMOVE | ❌ Missing | Transactional remove |
| 0x100300 | TXN_SET_SIZE | ❌ Missing | Transactional size |

### Transactional Queue (0x1100xx) - **MISSING**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x110100 | TXN_QUEUE_OFFER | ❌ Missing | Transactional offer |
| 0x110200 | TXN_QUEUE_POLL | ❌ Missing | Transactional poll |
| 0x110300 | TXN_QUEUE_TAKE | ❌ Missing | Transactional take |
| 0x110400 | TXN_QUEUE_PEEK | ❌ Missing | Transactional peek |
| 0x110500 | TXN_QUEUE_SIZE | ❌ Missing | Transactional size |

### Transactional MultiMap (0x1200xx) - **MISSING**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x120100 | TXN_MULTI_MAP_PUT | ❌ Missing | Transactional put |
| 0x120200 | TXN_MULTI_MAP_GET | ❌ Missing | Transactional get |
| 0x120300 | TXN_MULTI_MAP_REMOVE | ❌ Missing | Transactional remove |
| 0x120400 | TXN_MULTI_MAP_REMOVE_ALL | ❌ Missing | Transactional removeAll |
| 0x120500 | TXN_MULTI_MAP_VALUE_COUNT | ❌ Missing | Transactional valueCount |
| 0x120600 | TXN_MULTI_MAP_SIZE | ❌ Missing | Transactional size |

### XA Transaction (0x1400xx) - **NOT SUPPORTED**

Per README, XA transactions are explicitly not supported.

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x140100 | XA_COMMIT | ❌ Not Supported | XA commit |
| 0x140200 | XA_CREATE | ❌ Not Supported | XA create |
| 0x140300 | XA_CLEAR_REMOTE | ❌ Not Supported | XA clear remote |
| 0x140400 | XA_COLLECT | ❌ Not Supported | XA collect |
| 0x140500 | XA_FINALIZE | ❌ Not Supported | XA finalize |
| 0x140600 | XA_ROLLBACK | ❌ Not Supported | XA rollback |

### CP Session Operations (0x1F00xx) - **IMPLEMENTED**

Required for proper CP subsystem functionality.

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x1F0100 | CreateSession | ✅ Implemented | Create CP session |
| 0x1F0200 | CloseSession | ✅ Implemented | Close CP session |
| 0x1F0300 | Heartbeat | ✅ Implemented | Session heartbeat |
| 0x1F0400 | GenerateThreadId | ✅ Implemented | Generate thread ID |
| 0x1F0500 | GetSessions | ✅ Implemented | Get active sessions |

### CP Group Operations (0x1E00xx) - **MISSING**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x1E0100 | CreateCPGroup | ❌ Missing | Create CP group |
| 0x1E0200 | DestroyCPObject | ❌ Missing | Destroy CP object |
| 0x1E0300 | GetCPGroupIds | ❌ Missing | Get CP group IDs |
| 0x1E0400 | GetCPObjectInfos | ❌ Missing | Get CP object infos |

### Jet Operations (0x FE00xx) - **MISSING**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0xFE0100 | SubmitJob | ❌ Missing | Submit Jet job |
| 0xFE0200 | TerminateJob | ❌ Missing | Terminate job |
| 0xFE0300 | GetJobStatus | ❌ Missing | Get job status |
| 0xFE0400 | GetJobIds | ❌ Missing | Get all job IDs |
| 0xFE0500 | GetJobSubmissionTime | ❌ Missing | Get submission time |
| 0xFE0600 | GetJobConfig | ❌ Missing | Get job configuration |
| 0xFE0700 | ResumeJob | ❌ Missing | Resume suspended job |
| 0xFE0800 | ExportSnapshot | ❌ Missing | Export job snapshot |
| 0xFE0900 | GetJobSummaryList | ❌ Missing | Get job summaries |
| 0xFE0A00 | ExistsDistributedObject | ❌ Missing | Check object exists |
| 0xFE0B00 | GetJobMetrics | ❌ Missing | Get job metrics |
| 0xFE0C00 | GetJobSuspensionCause | ❌ Missing | Get suspension cause |
| 0xFE0D00 | GetJobAndSqlSummaryList | ❌ Missing | Get job/SQL summaries |
| 0xFE0E00 | IsJobUserCancelled | ❌ Missing | Check if user cancelled |
| 0xFE0F00 | UploadJobMetaData | ❌ Missing | Upload job metadata |
| 0xFE1000 | UploadJobMultipart | ❌ Missing | Upload job multipart |
| 0xFE1100 | AddJobStatusListener | ❌ Missing | Job status listener |
| 0xFE1200 | RemoveJobStatusListener | ❌ Missing | Remove status listener |
| 0xFE1300 | UpdateJobConfig | ❌ Missing | Update job config |

### Map Event Journal (0x0141xx) - **MISSING**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x014100 | MAP_EVENT_JOURNAL_SUBSCRIBE | ❌ Missing | Subscribe to journal |
| 0x014200 | MAP_EVENT_JOURNAL_READ | ❌ Missing | Read from journal |

### Cache Event Journal (0x1316xx) - **MISSING**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x131600 | CACHE_EVENT_JOURNAL_SUBSCRIBE | ❌ Missing | Subscribe to cache journal |
| 0x131700 | CACHE_EVENT_JOURNAL_READ | ❌ Missing | Read from cache journal |

### Continuous Query Cache (0x0160xx) - **MISSING**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x016000 | CQC_CREATE_WITH_VALUE | ❌ Missing | Create with value |
| 0x016100 | CQC_CREATE | ❌ Missing | Create cache |
| 0x016200 | CQC_SET_READ_CURSOR | ❌ Missing | Set read cursor |
| 0x016300 | CQC_ADD_LISTENER | ❌ Missing | Add listener |
| 0x016400 | CQC_DESTROY | ❌ Missing | Destroy cache |
| 0x016500 | CQC_FETCH | ❌ Missing | Fetch data |
| 0x016600 | CQC_SIZE | ❌ Missing | Get size |
| 0x016700 | CQC_MADE_PUBLISHABLE | ❌ Missing | Make publishable |

### SQL Operations - **PARTIAL**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x210100 | SQL_EXECUTE | ✅ Implemented | Execute SQL |
| 0x210200 | SQL_EXECUTE_BATCH | ❌ Missing | Batch execution |
| 0x210300 | SQL_FETCH | ✅ Implemented | Fetch results |
| 0x210400 | SQL_CLOSE | ✅ Implemented | Close cursor |
| 0x210500 | SQL_MAPPING_DDL | ❌ Missing | DDL operations |

### Topic Operations - **PARTIAL**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x040100 | TOPIC_PUBLISH | ✅ Implemented | Publish message |
| 0x040200 | TOPIC_ADD_LISTENER | ✅ Implemented | Add listener |
| 0x040300 | TOPIC_REMOVE_LISTENER | ✅ Implemented | Remove listener |
| 0x040400 | TOPIC_PUBLISH_ALL | ❌ Missing | Batch publish |

### Reliable Topic (0x0A00xx) - **MISSING**

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| N/A | ReliableTopic operations | ❌ Missing | Uses Ringbuffer internally |

### Map Operations - **MOSTLY COMPLETE**

Missing advanced operations:

| Message Type ID | Operation | Python Status | Notes |
|-----------------|-----------|---------------|-------|
| 0x013000 | MAP_ADD_NEAR_CACHE_INVALIDATION_LISTENER | ❌ Missing | Near cache invalidation |
| 0x013100 | MAP_FETCH_NEAR_CACHE_INVALIDATION_METADATA | ❌ Missing | Invalidation metadata |
| 0x013300 | MAP_FETCH_ENTRIES | ❌ Missing | Fetch with cursor |
| 0x013400 | MAP_AGGREGATE | ❌ Missing | Server-side aggregation |
| 0x013500 | MAP_AGGREGATE_WITH_PREDICATE | ❌ Missing | Aggregation with predicate |
| 0x013600 | MAP_PROJECT | ❌ Missing | Server-side projection |
| 0x013700 | MAP_PROJECT_WITH_PREDICATE | ❌ Missing | Projection with predicate |
| 0x013800 | MAP_FETCH_WITH_QUERY | ❌ Missing | Query-based fetch |
| 0x013900 | MAP_ENTRY_SET_WITH_PAGING_PREDICATE | ❌ Missing | Paging predicate |
| 0x013A00 | MAP_KEY_SET_WITH_PAGING_PREDICATE | ❌ Missing | Paging key set |
| 0x013B00 | MAP_VALUES_WITH_PAGING_PREDICATE | ❌ Missing | Paging values |
| 0x013C00 | MAP_PUT_WITH_MAX_IDLE | ❌ Missing | Put with max idle |
| 0x013D00 | MAP_PUT_TRANSIENT_WITH_MAX_IDLE | ❌ Missing | Transient with max idle |
| 0x013E00 | MAP_PUT_IF_ABSENT_WITH_MAX_IDLE | ❌ Missing | PutIfAbsent max idle |
| 0x013F00 | MAP_SET_WITH_MAX_IDLE | ❌ Missing | Set with max idle |

## Implementation Priority Recommendations

### P0 - Critical (Required for basic functionality)

1. **Client Authentication** - Without this, the client cannot connect to a secured cluster
2. **CreateProxy/DestroyProxy** - Required for distributed object lifecycle
3. **GetPartitions** - Required for smart routing
4. **AddClusterViewListener** - Required for topology awareness
5. **CP Session Operations** - Required for proper CP subsystem usage

### P1 - High (Transaction support)

1. **Transaction Create/Commit/Rollback** - Core transaction management
2. **Transactional Map Codec** - Most commonly used transactional DS
3. **Transactional Queue Codec** - Common transactional use case

### P2 - Medium (Advanced features)

1. **Jet Operations** - Required for stream processing
2. **Event Journal** - Required for CDC-style applications
3. **Map Near Cache Invalidation** - Required for proper near cache
4. **SQL Batch Execute** - Performance optimization

### P3 - Low (Specialized features)

1. **Continuous Query Cache** - Advanced caching
2. **XA Transactions** - Enterprise integration
3. **Remaining transactional DS codecs**

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
