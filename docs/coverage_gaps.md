# Coverage Gaps Analysis

This document tracks modules below 95% coverage and identifies specific lines/branches needing tests.

**Target Modules:**
- `hazelcast/network/` (all submodules)
- `hazelcast/invocation.py`
- `hazelcast/failover.py`
- `hazelcast/serialization/compact.py`

---

## Summary Table

| Module | Estimated Coverage | Priority | Test File |
|--------|-------------------|----------|-----------|
| `hazelcast/network/connection.py` | < 30% | **HIGH** | `tests/test_network.py` |
| `hazelcast/network/connection_manager.py` | < 25% | **HIGH** | `tests/test_connection_manager.py` |
| `hazelcast/network/failure_detector.py` | < 20% | **HIGH** | `tests/test_network.py` |
| `hazelcast/invocation.py` | ~85% | **DONE** | `tests/test_invocation.py` |
| `hazelcast/failover.py` | ~90% | **DONE** | `tests/test_failover.py` |
| `hazelcast/serialization/compact.py` | < 30% | **HIGH** | `tests/test_compact.py` |
| `hazelcast/network/address.py` | < 50% | **MEDIUM** | `tests/test_network.py` |
| `hazelcast/network/ssl_config.py` | < 45% | **MEDIUM** | `tests/test_network.py` |
| `hazelcast/network/socket_interceptor.py` | < 60% | **LOW** | `tests/test_network.py` |

---

## Detailed Gap Analysis

### 1. `hazelcast/network/connection.py`

**Uncovered Lines/Branches:**

| Lines | Description | Test Needed |
|-------|-------------|-------------|
| 68-72 | `member_uuid` property setter | Unit test for UUID assignment |
| 78-82 | `connect()` state validation | Test connecting when not in CREATED state |
| 89-92 | Address resolution failure | Test with unresolvable hostname |
| 97-100 | SSL context creation branch | Test SSL-enabled connection |
| 102-109 | `asyncio.open_connection` timeout | Test connection timeout scenario |
| 111-120 | Socket options application | Test `_apply_socket_options()` |
| 123-125 | Socket interceptor `on_connect` | Test with custom interceptor |
| 131-137 | `asyncio.TimeoutError` handling | Test connection timeout exception |
| 139-145 | General connection exception | Test network failure scenarios |
| 148-165 | `_apply_socket_options()` branches | Test each socket option individually |
| 173-196 | `_read_loop()` async task | Test read loop with mock data |
| 177-179 | Remote close detection | Test server-initiated close |
| 185-188 | Read error exception handling | Test read failure |
| 190-210 | `_process_buffer()` frame parsing | Test partial/complete messages |
| 212-230 | `_find_message_end()` logic | Test fragmented messages, END_FLAG |
| 232-254 | `send()` method | Test send success/failure |
| 246-249 | Write error handling | Test write failure |
| 256-261 | `send_sync()` branches | Test sync send in running/not running loop |
| 263-290 | `close()` method | Test close with interceptor, cancel task |
| 276-283 | Writer close and wait | Test graceful close |

**Branch Coverage Gaps:**
- Line 78: `if self._state != ConnectionState.CREATED`
- Line 97: `if self._ssl_config and self._ssl_config.enabled`
- Line 111: `if sock`
- Line 123: `if self._socket_interceptor`
- Line 148-165: All socket option conditionals
- Line 177: `if not data`
- Line 232: `if not self.is_alive`
- Line 234: `if self._writer is None`

---

### 2. `hazelcast/network/connection_manager.py`

**Uncovered Lines/Branches:**

| Lines | Description | Test Needed |
|-------|-------------|-------------|
| 78-89 | `RoundRobinLoadBalancer.next()` | Test with empty/alive connections |
| 91-96 | `RoundRobinLoadBalancer.can_get_next()` | Test availability check |
| 99-117 | `RandomLoadBalancer` methods | Test random selection |
| 145-152 | Constructor initialization | Test with all parameter combinations |
| 175-180 | `set_connection_listener()` | Test listener registration |
| 185-215 | `start()` method | Test startup sequence |
| 198-200 | Connection count check after connect | Test failed cluster connect |
| 206-208 | Reconnect task creation | Test reconnect mode handling |
| 210-230 | `shutdown()` method | Test graceful shutdown |
| 232-250 | `_connect_to_cluster()` | Test SINGLE_MEMBER vs ALL_MEMBERS modes |
| 252-285 | `_connect_to_address()` | Test connection reuse, new connection |
| 287-300 | `get_connection()` | Test routing modes |
| 302-310 | `get_connection_for_address()` | Test address lookup |
| 315-345 | `_reconnect_loop()` | Test background reconnection |
| 347-375 | `_try_reconnect()` | Test reconnection logic |
| 377-395 | `_calculate_backoff()` | Test backoff calculation with/without config |
| 397-420 | `_send_heartbeat()` | Test heartbeat message construction |
| 422-428 | `_on_heartbeat_failure()` | Test failure callback |
| 430-460 | `_close_connection()` | Test connection cleanup |
| 462-465 | `on_connection_closed()` | Test external close notification |

**Branch Coverage Gaps:**
- Line 78-89: Connection alive filtering in load balancer
- Line 198: `if self.connection_count == 0`
- Line 206: `if self._reconnect_mode and self._reconnect_mode.value != "OFF"`
- Line 232-238: Routing mode switch
- Line 252-258: Existing connection reuse
- Line 347-350: Single member reconnect skip
- Line 377-395: Retry config presence

---

### 3. `hazelcast/network/failure_detector.py`

**Uncovered Lines/Branches:**

| Lines | Description | Test Needed |
|-------|-------------|-------------|
| 22-45 | `_can_use_raw_sockets()` | Test on different platforms |
| 48-58 | `_calculate_checksum()` | Test ICMP checksum calculation |
| 61-105 | `FailureDetector` class | Full class coverage |
| 108-170 | `PingFailureDetector` class | Test ICMP ping functionality |
| 143-148 | `register_host()` / `unregister_host()` | Test host management |
| 150-160 | `needs_ping()` | Test ping timing logic |
| 162-185 | `ping()` async method | Test successful/failed ping |
| 187-250 | `_send_ping()` | Test ICMP packet send/receive |
| 252-265 | Success/failure recording | Test counter management |
| 267-275 | `is_host_suspect()` | Test suspicion threshold |
| 278-340 | `PhiAccrualFailureDetector` | Full class coverage |
| 295-305 | `register_connection()` / `unregister_connection()` | Test connection tracking |
| 307-315 | `heartbeat()` method | Test heartbeat recording |
| 317-345 | `phi()` calculation | Test phi value computation |
| 347-360 | `_calculate_phi()` | Test CDF calculation |
| 362-370 | `is_available()` | Test availability check |
| 373-420 | `HeartbeatHistory` class | Test statistics tracking |
| 423-500 | `HeartbeatManager` class | Test heartbeat loop |
| 450-470 | `_heartbeat_loop()` | Test async loop |
| 472-495 | `_check_connections()` | Test connection health checks |

**Branch Coverage Gaps:**
- Line 22-45: Platform-specific raw socket checks
- Line 150: `if not self._can_ping`
- Line 187-195: Socket type fallback (RAW vs DGRAM)
- Line 317-325: History presence check
- Line 347-360: Time difference edge cases

---

### 4. `hazelcast/invocation.py`

**Uncovered Lines/Branches:**

| Lines | Description | Test Needed |
|-------|-------------|-------------|
| 30-75 | `Invocation` class | Full class coverage |
| 58-62 | `correlation_id` setter | Test ID assignment |
| 68-72 | `mark_sent()` / `is_expired()` | Test timing logic |
| 74-80 | `set_response()` / `set_exception()` | Test future completion |
| 83-115 | `InvocationService.__init__()` | Test with various configs |
| 117-130 | `start()` / `shutdown()` | Test lifecycle |
| 135-155 | `invoke()` | Test invocation flow |
| 157-190 | `_send_invocation()` | Test send with retries |
| 165-175 | Retry scheduling | Test retry logic |
| 177-185 | Exception handling | Test send failures |
| 192-200 | `_get_connection_for_invocation()` | Test smart routing |
| 202-215 | `_schedule_retry()` | Test backoff scheduling |
| 217-225 | `_fail_invocation()` | Test failure handling |
| 227-240 | `_handle_message()` | Test message routing |
| 242-260 | `handle_response()` | Test response matching |
| 262-285 | `check_timeouts()` | Test timeout detection |
| 287-310 | `invoke_on_connection()` | Test targeted invocation |

**Branch Coverage Gaps:**
- Line 135: `if not self._running`
- Line 157: `if not self._running`
- Line 162-175: Connection availability and retry
- Line 192-200: Smart routing branch
- Line 227-230: Event message handling
- Line 242-250: Unknown correlation ID

---

### 5. `hazelcast/failover.py`

**Uncovered Lines/Branches:**

| Lines | Description | Test Needed |
|-------|-------------|-------------|
| 50-65 | `ClusterConfig` dataclass | Test initialization |
| 68-120 | `CNAMEResolver` class | Full class coverage |
| 90-115 | `resolve()` method | Test DNS resolution, caching |
| 117-125 | `refresh()` method | Test forced refresh |
| 128-230 | `FailoverConfig` class | Full class coverage |
| 155-165 | `try_count` setter validation | Test invalid values |
| 180-210 | `add_cluster()` | Test cluster addition, sorting |
| 212-235 | `add_cname_cluster()` | Test CNAME cluster registration |
| 237-250 | `get_current_cluster()` | Test current cluster retrieval |
| 252-265 | `get_cluster_addresses()` | Test address retrieval |
| 267-285 | `switch_to_next_cluster()` | Test cluster cycling |
| 287-295 | `reset()` | Test reset functionality |
| 297-315 | `to_client_config()` | Test config generation |
| 317-335 | `from_configs()` class method | Test factory method |

**Branch Coverage Gaps:**
- Line 90-100: Cache hit vs miss in resolver
- Line 105-115: DNS resolution failure
- Line 155: `if value < 1`
- Line 237-240: Empty clusters check
- Line 252-260: CNAME vs static resolution
- Line 267-275: Empty cluster list

---

### 6. `hazelcast/serialization/compact.py`

**Uncovered Lines/Branches:**

| Lines | Description | Test Needed |
|-------|-------------|-------------|
| 95-130 | `FieldDescriptor` class | Test field type mapping |
| 132-175 | `Schema` class | Test schema operations |
| 145-155 | `_compute_schema_id()` | Test ID computation |
| 157-170 | `get_field()` / `add_field()` | Test field management |
| 173-220 | `SchemaService` class | Full class coverage |
| 190-210 | `is_compatible()` | Test schema compatibility |
| 223-320 | `DefaultCompactWriter` class | Test all write methods |
| 323-450 | `DefaultCompactReader` class | Test all read methods |
| 360-380 | `_check_field_kind()` | Test type mismatch errors |
| 453-510 | `CompactStreamWriter` class | Test binary writing |
| 513-570 | `CompactStreamReader` class | Test binary reading |
| 573-750 | `CompactSerializationService` class | Full class coverage |
| 620-650 | `serialize()` | Test serialization flow |
| 652-690 | `deserialize()` | Test deserialization flow |
| 692-720 | `_encode()` / `_decode()` | Test encoding/decoding |
| 722-800 | `_write_field_value()` | Test all field type writes |
| 802-880 | `_read_field_value()` | Test all field type reads |
| 882-970 | `GenericRecord` class | Full class coverage |
| 973-1100 | `GenericRecordBuilder` class | Test all setter methods |
| 1103-1180 | `GenericRecordSerializer` class | Test serialization |
| 1183-1250 | `ReflectiveCompactSerializer` class | Test reflection-based serialization |

**Branch Coverage Gaps:**
- Line 360-380: Field kind mismatch exceptions
- Line 722-800: All nullable field branches
- Line 802-880: All field type read branches
- Line 882-920: All GenericRecord getter validations
- Line 1183-1250: Dataclass vs regular class branches

---

## Test Implementation Priority

### Phase 1 (Critical - Connection Layer)
1. `tests/test_network.py` - Connection lifecycle, socket options
2. `tests/test_connection_manager.py` - Connection management, routing

### Phase 2 (High - Request/Response)
3. `tests/test_invocation.py` - Invocation service, retries, timeouts
4. `tests/test_compact.py` - Serialization, schema evolution

### Phase 3 (Medium - Resilience)
5. `tests/test_failover.py` - Failover configuration, CNAME resolution
6. Additional failure detector tests

---

## Missing Test Files

The following test files need to be created or populated:

```
tests/
├── test_network.py          # Connection, Address, SSL, SocketInterceptor
├── test_connection_manager.py  # ConnectionManager, LoadBalancers
├── test_invocation.py       # ✅ CREATED - Invocation, InvocationService
├── test_failover.py         # ✅ CREATED - FailoverConfig, CNAMEResolver
├── test_compact.py          # Compact serialization, Schema, GenericRecord
└── test_failure_detector.py # FailureDetector, HeartbeatManager, PhiAccrual
```

---

## Commands to Run

```bash
# Generate coverage report
pytest tests/ --cov=hazelcast --cov-report=term-missing --cov-report=html

# Run specific module tests
pytest tests/test_network.py -v --cov=hazelcast/network --cov-report=term-missing

# Generate HTML report
pytest tests/ --cov=hazelcast --cov-report=html
# Open htmlcov/index.html in browser
```

---

## Notes

- All async methods need `pytest-asyncio` for testing
- Socket operations should be mocked using `unittest.mock`
- SSL tests require test certificates or mocked SSL contexts
- ICMP ping tests require mocking due to permission requirements
