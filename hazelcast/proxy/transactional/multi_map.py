"""Transactional MultiMap proxy implementation.

This module re-exports TransactionalMultiMap from multimap.py for
backward compatibility with alternative import paths.
"""

from hazelcast.proxy.transactional.multimap import TransactionalMultiMap

__all__ = ["TransactionalMultiMap"]
