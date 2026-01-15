# HazelPython

A comprehensive Python client for [Hazelcast](https://hazelcast.com/) in-memory data grid.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-green.svg)](https://www.python.org/)

## Features

- **Distributed Data Structures**: Map, Queue, Set, List, Topic, MultiMap, Ringbuffer, PN Counter
- **SQL Support**: Execute SQL queries against your cluster data
- **CP Subsystem**: AtomicLong, AtomicReference, FencedLock, CountDownLatch, Semaphore
- **Jet Integration**: Submit and manage streaming/batch processing pipelines
- **Near Cache**: Local caching with configurable eviction and invalidation
- **Failover**: Automatic failover across multiple clusters with CNAME-based discovery
- **Metrics**: Comprehensive client metrics for monitoring
- **Async Support**: Both synchronous and asynchronous APIs

## Installation

```bash
pip install hazelcast-python-client
```

For development:

```bash
pip install -e ".[dev]"
```

## Quick Start

### Basic Connection

```python
from hazelcast import HazelcastClient, ClientConfig

# Create configuration
config = ClientConfig()
config.cluster_name = "dev"
config.cluster_members = ["localhost:5701"]

# Create and start the client
client = HazelcastClient(config)
client.start()

# Use the client...

# Shutdown when done
client.shutdown()
```

### Using Context Manager

```python
from hazelcast import HazelcastClient, ClientConfig

config = ClientConfig()
config.cluster_name = "dev"

with HazelcastClient(config) as client:
    # Client automatically started and will shutdown on exit
    pass
```

### Async Usage

```python
import asyncio
from hazelcast import HazelcastClient, ClientConfig

async def main():
    config = ClientConfig()
    config.cluster_name = "dev"
    
    async with HazelcastClient(config) as client:
        # Use async operations
        pass

asyncio.run(main())
```

## Distributed Data Structures

### Map

```python
from hazelcast.proxy import MapProxy

# Get a distributed map
my_map = MapProxy("my-map")

# Basic operations
my_map.put("key1", "value1")
value = my_map.get("key1")
my_map.remove("key1")

# Conditional operations
my_map.put_if_absent("key2", "value2")
my_map.replace("key2", "new_value")

# Bulk operations
my_map.put_all({"a": 1, "b": 2, "c": 3})
values = my_map.get_all({"a", "b"})

# Entry listeners
from hazelcast.proxy import EntryListener

class MyListener(EntryListener):
    def entry_added(self, event):
        print(f"Added: {event.key}")

listener = MyListener()
registration_id = my_map.add_entry_listener(listener)
```

### Queue, Set, List

```python
from hazelcast.proxy import QueueProxy, SetProxy, ListProxy

# Queue operations
queue = QueueProxy("my-queue")
queue.offer("item1")
item = queue.poll()

# Set operations
my_set = SetProxy("my-set")
my_set.add("item1")
contains = my_set.contains("item1")

# List operations
my_list = ListProxy("my-list")
my_list.add("item1")
item = my_list.get(0)
```

## SQL Queries

```python
from hazelcast.sql import SqlService, SqlStatement

sql_service = SqlService()
sql_service.start()

# Simple query
result = sql_service.execute("SELECT * FROM employees")
for row in result:
    print(row.to_dict())

# Parameterized query
result = sql_service.execute(
    "SELECT * FROM employees WHERE department = ? AND salary > ?",
    "Engineering",
    50000
)

# Using SqlStatement for more control
stmt = SqlStatement("SELECT * FROM employees WHERE active = ?")
stmt.add_parameter(True)
stmt.timeout = 30.0
stmt.cursor_buffer_size = 1000

result = sql_service.execute_statement(stmt)
```

## CP Subsystem

### AtomicLong

```python
from hazelcast.cp import AtomicLong

counter = AtomicLong("my-counter")

counter.set(0)
counter.increment_and_get()
old_value = counter.get_and_add(10)

# Compare and set
success = counter.compare_and_set(11, 20)

# Alter with function
counter.alter(lambda x: x * 2)
```

### FencedLock

```python
from hazelcast.cp import FencedLock

lock = FencedLock("my-lock")

# Manual locking
fence = lock.lock()
try:
    # Critical section
    pass
finally:
    lock.unlock()

# Context manager
with lock as fence:
    # Critical section with fence token
    print(f"Fence: {fence}")
```

### Semaphore

```python
from hazelcast.cp import Semaphore

sem = Semaphore("my-semaphore", initial_permits=5)

sem.acquire(2)
try:
    # Use resources
    pass
finally:
    sem.release(2)
```

## Jet Pipelines

```python
from hazelcast.jet import JetService, Pipeline, JobConfig

jet_service = JetService()
jet_service.start()

# Create a pipeline
pipeline = Pipeline.create()

# Build pipeline stages
source = Pipeline.from_list("source", [1, 2, 3, 4, 5])
sink = Pipeline.to_list("sink")

pipeline.read_from(source).map(lambda x: x * 2).write_to(sink)

# Submit job
config = JobConfig()
config.name = "my-job"

job = jet_service.submit(pipeline, config)
print(f"Job status: {job.status}")

# Manage job
job.suspend()
job.resume()
job.cancel()
```

## Failover Configuration

```python
from hazelcast.failover import FailoverConfig

failover = FailoverConfig(try_count=3)

# Add clusters in priority order
failover.add_cluster(
    cluster_name="primary",
    addresses=["primary-node:5701"],
    priority=0
)
failover.add_cluster(
    cluster_name="secondary",
    addresses=["secondary-node:5701"],
    priority=1
)

# Use CNAME-based discovery
failover.add_cname_cluster(
    cluster_name="dynamic",
    dns_name="hazelcast.example.com",
    port=5701,
    refresh_interval=60.0
)

# Create client config from failover
config = failover.to_client_config()
```

## Metrics

```python
from hazelcast.metrics import MetricsRegistry

metrics = MetricsRegistry()

# Connection metrics
metrics.record_connection_opened()
metrics.record_connection_closed()

# Invocation metrics
metrics.record_invocation_start()
metrics.record_invocation_end(response_time=15.5, success=True)

# Near cache metrics
metrics.record_near_cache_hit("my-map")
metrics.record_near_cache_miss("my-map")

# Export all metrics
all_metrics = metrics.to_dict()
print(f"Active connections: {all_metrics['connections']['active']}")
print(f"Invocation avg response: {all_metrics['invocations']['average_response_time_ms']}ms")
```

## Logging

The client uses Python's standard `logging` module with a hierarchical logger structure under the `hazelcast` namespace.

### Basic Configuration

```python
import logging
from hazelcast.logging import configure_logging

# Configure with INFO level (default)
configure_logging(level=logging.INFO)

# Configure with DEBUG level for detailed output
configure_logging(level=logging.DEBUG)

# Custom format
configure_logging(
    level=logging.DEBUG,
    format_string="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
```

### Using Standard Logging

You can also configure Hazelcast logging using Python's standard logging configuration:

```python
import logging

# Configure the hazelcast logger directly
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("hazelcast").setLevel(logging.INFO)

# Or configure specific components
logging.getLogger("hazelcast.client").setLevel(logging.DEBUG)
logging.getLogger("hazelcast.connection").setLevel(logging.WARNING)
```

### Logger Hierarchy

| Logger Name | Description |
|-------------|-------------|
| `hazelcast` | Root logger for all Hazelcast components |
| `hazelcast.client` | Client lifecycle and state transitions |
| `hazelcast.connection` | Individual connection handling |
| `hazelcast.connection.manager` | Connection management and routing |
| `hazelcast.invocation` | Request/response invocation handling |
| `hazelcast.proxy` | Distributed object proxy operations |

### Log Levels

| Level | Description |
|-------|-------------|
| `DEBUG` | Detailed diagnostic information (connections, messages, proxy operations) |
| `INFO` | General operational information (startup, shutdown, connections established) |
| `WARNING` | Potentially harmful situations (timeouts, reconnection attempts) |
| `ERROR` | Error events that might still allow the client to continue running |

### File Logging Example

```python
import logging
from hazelcast.logging import configure_logging

# Create a file handler
file_handler = logging.FileHandler("hazelcast.log")
file_handler.setLevel(logging.DEBUG)

configure_logging(
    level=logging.DEBUG,
    handler=file_handler
)
```

### Disabling Logging

```python
from hazelcast.logging import HazelcastLoggerFactory

# Disable all Hazelcast logging
HazelcastLoggerFactory.disable()

# Re-enable logging
HazelcastLoggerFactory.enable()
```

## Configuration

### YAML Configuration

```yaml
# hazelcast-client.yml
hazelcast_client:
  cluster_name: production
  
  network:
    addresses:
      - node1.example.com:5701
      - node2.example.com:5701
    connection_timeout: 10.0
    smart_routing: true
  
  connection_strategy:
    async_start: false
    reconnect_mode: ON
    retry:
      initial_backoff: 1.0
      max_backoff: 30.0
      multiplier: 2.0
      jitter: 0.1
  
  security:
    username: admin
    password: secret
  
  near_caches:
    my-map:
      max_size: 10000
      time_to_live_seconds: 300
      eviction_policy: LRU
```

Load from YAML:

```python
config = ClientConfig.from_yaml("hazelcast-client.yml")
```

### Programmatic Configuration

```python
from hazelcast.config import (
    ClientConfig,
    NetworkConfig,
    ConnectionStrategyConfig,
    RetryConfig,
    SecurityConfig,
    NearCacheConfig,
    ReconnectMode,
    EvictionPolicy,
)

config = ClientConfig()
config.cluster_name = "production"

# Network settings
config.network = NetworkConfig(
    addresses=["node1:5701", "node2:5701"],
    connection_timeout=10.0,
    smart_routing=True
)

# Connection strategy
config.connection_strategy = ConnectionStrategyConfig(
    async_start=False,
    reconnect_mode=ReconnectMode.ON,
    retry=RetryConfig(
        initial_backoff=1.0,
        max_backoff=30.0,
        multiplier=2.0,
        jitter=0.1
    )
)

# Security
config.security = SecurityConfig(
    username="admin",
    password="secret"
)

# Near cache
config.add_near_cache(NearCacheConfig(
    name="my-map",
    max_size=10000,
    time_to_live_seconds=300,
    eviction_policy=EvictionPolicy.LRU
))
```

## Event Listeners

### Lifecycle Listener

```python
from hazelcast.listener import LifecycleEvent

def on_lifecycle(event: LifecycleEvent):
    print(f"State changed: {event.previous_state} -> {event.state}")

client.add_lifecycle_listener(on_state_changed=on_lifecycle)
```

### Membership Listener

```python
from hazelcast.listener import MembershipEvent

client.add_membership_listener(
    on_member_added=lambda e: print(f"Member joined: {e.member}"),
    on_member_removed=lambda e: print(f"Member left: {e.member}")
)
```

## Project Structure

```
HazelPython/
├── hazelcast/
│   ├── __init__.py          # Package entry point
│   ├── client.py             # HazelcastClient
│   ├── config.py             # Configuration classes
│   ├── exceptions.py         # Exception hierarchy
│   ├── auth.py               # Authentication
│   ├── listener.py           # Event listeners
│   ├── invocation.py         # Request/response handling
│   ├── failover.py           # Failover configuration
│   ├── metrics.py            # Client metrics
│   ├── protocol/             # Wire protocol
│   ├── network/              # Connection management
│   ├── serialization/        # Serialization service
│   ├── proxy/                # Distributed data structures
│   ├── sql/                  # SQL support
│   ├── cp/                   # CP subsystem
│   ├── jet/                  # Jet integration
│   └── service/              # Internal services
├── tests/
│   ├── unit/                 # Unit tests
│   └── integration/          # Integration tests
├── examples/                 # Code examples
├── docs/                     # Documentation
├── setup.py                  # Package configuration
├── LICENSE
└── README.md
```

## Running Tests

### Unit Tests

```bash
pytest tests/ -v
```

### Integration Tests

Integration tests require a running Hazelcast cluster:

```bash
# Start test cluster with Docker
docker-compose -f tests/integration/docker-compose.yml up -d

# Run integration tests
pytest tests/integration/ -v --integration

# Stop test cluster
docker-compose -f tests/integration/docker-compose.yml down
```

## API Reference

### Core Classes

| Class | Description |
|-------|-------------|
| `HazelcastClient` | Main client class for connecting to clusters |
| `ClientConfig` | Client configuration |
| `MapProxy` | Distributed Map implementation |
| `SqlService` | SQL query execution |
| `JetService` | Jet pipeline submission |
| `AtomicLong` | CP AtomicLong |
| `FencedLock` | CP FencedLock |
| `FailoverConfig` | Multi-cluster failover |
| `MetricsRegistry` | Client metrics |

### Services

| Service | Description |
|---------|-------------|
| `ExecutorService` | Distributed task execution |
| `PartitionService` | Partition information |
| `ClientService` | Connection monitoring |
| `SerializationService` | Object serialization |
| `InvocationService` | Request/response handling |

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests for your changes
4. Submit a pull request

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.

## Resources

- [Hazelcast Documentation](https://docs.hazelcast.com/)
- [Hazelcast Community](https://hazelcast.com/community/)
- [Issue Tracker](https://github.com/hazelcast/HazelPython/issues)