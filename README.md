# HazelPython

Hazelcast Python Client - A Python client for [Hazelcast](https://hazelcast.com/) in-memory data grid.

## Installation

```bash
pip install hazelcast-python-client
```

For development:

```bash
pip install -e ".[dev]"
```

## Quick Start

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

## Project Structure

```
HazelPython/
├── hazelcast/           # Main package
│   ├── __init__.py
│   ├── client.py        # HazelcastClient implementation
│   ├── config.py        # ClientConfig
│   ├── exceptions.py    # Exception hierarchy
│   └── logging_config.py
├── tests/               # Unit tests
├── docs/                # Documentation
├── examples/            # Example code
├── setup.py
├── LICENSE
└── README.md
```

## Running Tests

```bash
pytest tests/
```

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.