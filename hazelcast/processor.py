"""Entry processor for distributed map operations."""

from abc import ABC, abstractmethod
from typing import Any, Generic, Optional, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class EntryProcessorEntry(ABC, Generic[K, V]):
    """Interface for map entry access within an EntryProcessor.

    Provides read and write access to a map entry during processing.
    """

    @abstractmethod
    def get_key(self) -> K:
        """Get the entry's key."""
        pass

    @abstractmethod
    def get_value(self) -> Optional[V]:
        """Get the entry's current value."""
        pass

    @abstractmethod
    def set_value(self, value: V) -> V:
        """Set the entry's value.

        Args:
            value: The new value.

        Returns:
            The new value.
        """
        pass


class EntryProcessor(ABC, Generic[K, V]):
    """Base class for entry processors.

    An EntryProcessor executes read and update operations on map entries
    atomically, at the data's location. This eliminates the need to
    serialize/deserialize the entry and send it across the network.

    Entry processors run on the partition thread of the target key,
    ensuring atomicity without explicit locking.

    Type Parameters:
        K: The key type.
        V: The value type.

    Example:
        Creating a custom entry processor::

            class IncrementProcessor(EntryProcessor[str, int]):
                def __init__(self, increment: int = 1):
                    self._increment = increment

                def process(self, entry):
                    current = entry.get_value() or 0
                    entry.set_value(current + self._increment)
                    return current
    """

    @abstractmethod
    def process(self, entry: EntryProcessorEntry[K, V]) -> Any:
        """Process the map entry.

        Args:
            entry: The entry to process, providing access to key and value.

        Returns:
            The result of processing, sent back to the caller.
        """
        pass


class SimpleEntryProcessorEntry(EntryProcessorEntry[K, V]):
    """Simple implementation of EntryProcessorEntry for testing."""

    def __init__(self, key: K, value: Optional[V] = None):
        self._key = key
        self._value = value

    def get_key(self) -> K:
        return self._key

    def get_value(self) -> Optional[V]:
        return self._value

    def set_value(self, value: V) -> V:
        self._value = value
        return value
