# HazelPython

A comprehensive Python client for [Hazelcast](https://hazelcast.com/) in-memory data grid.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-green.svg)](https://www.python.org/)

## Features

- **Distributed Data Structures**: Map, Queue, Set, List, Topic, MultiMap, Ringbuffer, PN Counter
- **SQL Support**: Execute SQL queries against cluster data
- **CP Subsystem**: AtomicLong, AtomicReference, FencedLock, Semaphore, CountDownLatch
- **Jet Integration**: Submit and manage streaming/batch processing pipelines
- **Near Cache**: Local caching with configurable eviction (LRU, LFU) and TTL
- **Failover**: Automatic failover across multiple clusters with CNAME-based discovery
- **Async Support**: Both synchronous and asynchronous APIs
- **Metrics**: Comprehensive client metrics for monitoring
- **Logging**: Hierarchical logging with configurable levels

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

config = ClientConfig()
config.cluster_name = "dev"
config.cluster_members = ["localhost:5701"]

# Using context manager (recommended)
with HazelcastClient(config) as client:
    my_map = client.get_map("my-map")
    my_map.put("key", "value")
    value = my_map.get("key")
    print(f"Retrieved: {value}")
```

### Async Usage

```python
import asyncio
from hazelcast import HazelcastClient, ClientConfig

async def main():
    config = ClientConfig()
    config.cluster_name = "dev"

    async with HazelcastClient(config) as client:
        my_map = client.get_map("my-map")
        await my_map.put_async("key", "value")
        value = await my_map.get_async("key")
        print(f"Retrieved: {value.result()}")

asyncio.run(main())
```

## Configuration Reference

### ClientConfig

The main configuration class with the following properties:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `cluster_name` | `str` | `"dev"` | Name of the Hazelcast cluster |
| `client_name` | `str` | Auto-generated | Identifier for this client instance |
| `cluster_members` | `List[str]` | `["localhost:5701"]` | List of cluster member addresses |
| `connection_timeout` | `float` | `5.0` | Connection timeout in seconds |
| `smart_routing` | `bool` | `True` | Enable smart routing to partition owners |
| `labels` | `List[str]` | `[]` | Client labels for identification |

### NetworkConfig

Network-related settings:

```python
from hazelcast.config import NetworkConfig

config.network = NetworkConfig(
    addresses=["node1:5701", "node2:5701"],
    connection_timeout=10.0,
    smart_routing=True,
)
```

### SecurityConfig

Authentication credentials:

```python
from hazelcast.config import SecurityConfig

config.security = SecurityConfig(
    username="admin",
    password="secret",
)

# Or token-based
config.security = SecurityConfig(
    token="your-auth-token",
)
```

### NearCacheConfig

Local caching configuration:

```python
from hazelcast.config import NearCacheConfig, EvictionPolicy

config.add_near_cache(NearCacheConfig(
    name="my-map",
    max_size=10000,
    time_to_live_seconds=300,
    max_idle_seconds=60,
    eviction_policy=EvictionPolicy.LRU,
    invalidate_on_change=True,
))
```

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `name` | `str` | Required | Map name to cache |
| `max_size` | `int` | `10000` | Maximum entries |
| `time_to_live_seconds` | `int` | `0` | Entry TTL (0 = infinite) |
| `max_idle_seconds` | `int` | `0` | Max idle time (0 = infinite) |
| `eviction_policy` | `EvictionPolicy` | `LRU` | `LRU`, `LFU`, `RANDOM`, `NONE` |
| `invalidate_on_change` | `bool` | `True` | Invalidate on server changes |

### ConnectionStrategyConfig

Reconnection behavior:

```python
from hazelcast.config import ConnectionStrategyConfig, ReconnectMode, RetryConfig

config.connection_strategy = ConnectionStrategyConfig(
    async_start=False,
    reconnect_mode=ReconnectMode.ON,
    retry=RetryConfig(
        initial_backoff=1.0,
        max_backoff=30.0,
        multiplier=2.0,
        jitter=0.1,
    ),
)
```

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

  security:
    username: admin
    password: secret

  connection_strategy:
    async_start: false
    reconnect_mode: ON
    retry:
      initial_backoff: 1.0
      max_backoff: 30.0
      multiplier: 2.0

  near_caches:
    users:
      max_size: 10000
      time_to_live_seconds: 300
      eviction_policy: LRU
```

Load from YAML:

```python
config = ClientConfig.from_yaml("hazelcast-client.yml")
```

## API Overview

### Distributed Data Structures

#### Map

```python
my_map = client.get_map("my-map")
my_map.put("key", "value", ttl=300)
value = my_map.get("key")
my_map.remove("key")
my_map.put_if_absent("key", "default")
entries = my_map.get_all({"key1", "key2"})
```

#### Queue

```python
queue = client.get_queue("my-queue")
queue.offer("item")
item = queue.poll(timeout=5.0)
queue.put("blocking-item")
item = queue.take()
```

#### Set and List

```python
my_set = client.get_set("my-set")
my_set.add("item")
exists = my_set.contains("item")

my_list = client.get_list("my-list")
my_list.add("item")
item = my_list.get(0)
```

#### MultiMap

```python
mm = client.get_multi_map("my-multimap")
mm.put("key", "value1")
mm.put("key", "value2")
values = mm.get("key")  # ["value1", "value2"]
```

#### Ringbuffer

```python
rb = client.get_ringbuffer("my-ringbuffer")
sequence = rb.add("item")
item = rb.read_one(sequence)
items = rb.read_many(start_sequence=0, min_count=1, max_count=100)
```

#### Topic

```python
topic = client.get_topic("my-topic")

def on_message(message):
    print(f"Received: {message.message}")

topic.add_message_listener(on_message=on_message)
topic.publish("Hello!")
```

#### PN Counter

```python
counter = client.get_pn_counter("my-counter")
counter.increment_and_get()
counter.add_and_get(10)
value = counter.get()
```

### CP Subsystem

Requires a cluster with 3+ members for CP subsystem:

#### AtomicLong

```python
counter = client.get_atomic_long("my-counter")
counter.set(0)
counter.increment_and_get()
counter.compare_and_set(1, 100)
```

#### FencedLock

```python
lock = client.get_fenced_lock("my-lock")

with lock as fence:
    # Critical section
    print(f"Fence token: {fence}")
```

#### Semaphore

```python
sem = client.get_semaphore("my-semaphore")
sem.acquire(2)
try:
    # Use resources
    pass
finally:
    sem.release(2)
```

#### CountDownLatch

```python
latch = client.get_count_down_latch("my-latch")
latch.try_set_count(3)
latch.count_down()
latch.await_latch(timeout=10.0)
```

### SQL Service

```python
sql = client.get_sql()

# Simple query
result = sql.execute("SELECT * FROM employees")
for row in result:
    print(row.to_dict())

# Parameterized query
result = sql.execute(
    "SELECT * FROM employees WHERE department = ? AND salary > ?",
    "Engineering", 50000
)
```

### Jet Service

```python
from hazelcast.jet import Pipeline, JobConfig

jet = client.get_jet()

# Build pipeline
pipeline = Pipeline.create()
source = Pipeline.from_list("numbers", [1, 2, 3, 4, 5])
sink = Pipeline.to_list("results")

pipeline.read_from(source).map(lambda x: x * 2).write_to(sink)

# Submit job
config = JobConfig()
config.name = "double-numbers"
job = jet.submit(pipeline, config)

print(f"Job status: {job.status}")
```

## Logging

Configure logging using Python's standard logging module:

```python
import logging
from hazelcast.logging import configure_logging

# Basic configuration
configure_logging(level=logging.INFO)

# Debug level for detailed output
configure_logging(level=logging.DEBUG)

# Custom format
configure_logging(
    level=logging.DEBUG,
    format_string="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
```

### Logger Hierarchy

| Logger Name | Description |
|-------------|-------------|
| `hazelcast` | Root logger |
| `hazelcast.client` | Client lifecycle |
| `hazelcast.connection` | Connection handling |
| `hazelcast.connection.manager` | Connection management |
| `hazelcast.invocation` | Request/response |
| `hazelcast.proxy` | Proxy operations |

### Per-Component Configuration

```python
import logging

logging.getLogger("hazelcast").setLevel(logging.INFO)
logging.getLogger("hazelcast.connection").setLevel(logging.DEBUG)
```

## Project Structure

```
HazelPython/
├── hazelcast/
│   ├── __init__.py           # Package exports
│   ├── client.py             # HazelcastClient
│   ├── config.py             # Configuration classes
│   ├── exceptions.py         # Exception hierarchy
│   ├── auth.py               # Authentication
│   ├── listener.py           # Event listeners
│   ├── invocation.py         # Request/response
│   ├── failover.py           # Multi-cluster failover
│   ├── metrics.py            # Client metrics
│   ├── logging.py            # Logging configuration
│   ├── near_cache.py         # Near cache implementation
│   ├── predicate.py          # Query predicates
│   ├── aggregator.py         # Aggregation functions
│   ├── projection.py         # Query projections
│   ├── network/              # Connection management
│   ├── serialization/        # Serialization service
│   ├── proxy/                # Data structure proxies
│   ├── sql/                  # SQL support
│   ├── cp/                   # CP subsystem
│   ├── jet/                  # Jet integration
│   └── service/              # Internal services
├── examples/                 # Example scripts
├── tests/                    # Test suite
├── setup.py
├── LICENSE
└── README.md
```

## Running Tests

### Unit Tests

```bash
pytest tests/ -v
```

### Integration Tests

Requires a running Hazelcast cluster:

```bash
# Start test cluster
docker run -d -p 5701:5701 hazelcast/hazelcast

# Run integration tests
pytest tests/integration/ -v --integration
```

## Examples

See the `examples/` directory for complete working examples:

- `basic_map.py` - Map operations and entry listeners
- `sql_queries.py` - SQL query execution
- `near_cache.py` - Near cache configuration and usage
- `cp_structures.py` - CP subsystem data structures
- `jet_pipeline.py` - Jet pipeline building and job management
- `failover.py` - Multi-cluster failover configuration

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.

## Resources

- [Hazelcast Documentation](https://docs.hazelcast.com/)
- [Hazelcast Community](https://hazelcast.com/community/)
- [Issue Tracker](https://github.com/hazelcast/HazelPython/issues)
