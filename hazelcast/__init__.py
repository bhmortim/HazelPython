"""Hazelcast Python Client."""

from hazelcast.client import HazelcastClient
from hazelcast.config import ClientConfig

__all__ = [
    "HazelcastClient",
    "ClientConfig",
]

__version__ = "0.1.0"
