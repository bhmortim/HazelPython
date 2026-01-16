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


class EntryBackupProcessor(ABC, Generic[K, V]):
    """Interface for processing backup entries.

    When an EntryProcessor modifies an entry, the backup processor
    is invoked on backup replicas to keep them in sync.
    """

    @abstractmethod
    def process_backup(self, entry: EntryProcessorEntry[K, V]) -> None:
        """Process the backup entry.

        Args:
            entry: The backup entry to process.
        """
        pass


class AbstractEntryProcessor(EntryProcessor[K, V], EntryBackupProcessor[K, V]):
    """Abstract base class for entry processors with optional backup processing.

    Provides a default backup processor implementation that re-applies
    the same logic as the primary processor. Subclasses can override
    `process_backup` for custom backup behavior.

    Args:
        apply_on_backup: If True, changes are applied to backup entries.
                         Defaults to True.
    """

    def __init__(self, apply_on_backup: bool = True):
        self._apply_on_backup = apply_on_backup

    @property
    def apply_on_backup(self) -> bool:
        """Whether this processor should be applied to backup entries."""
        return self._apply_on_backup

    def process_backup(self, entry: EntryProcessorEntry[K, V]) -> None:
        """Process backup entry by re-applying the primary processor logic."""
        if self._apply_on_backup:
            self.process(entry)


class SimpleEntryProcessorEntry(EntryProcessorEntry[K, V]):
    """Simple implementation of EntryProcessorEntry for testing."""

    def __init__(self, key: K, value: Optional[V] = None):
        self._key = key
        self._value = value
        self._deleted = False

    def get_key(self) -> K:
        return self._key

    def get_value(self) -> Optional[V]:
        if self._deleted:
            return None
        return self._value

    def set_value(self, value: V) -> V:
        self._value = value
        self._deleted = False
        return value

    def delete(self) -> None:
        """Mark this entry as deleted."""
        self._value = None
        self._deleted = True

    @property
    def is_deleted(self) -> bool:
        """Check if this entry has been deleted."""
        return self._deleted


class IncrementProcessor(AbstractEntryProcessor[K, int]):
    """Entry processor that increments a numeric value.

    Args:
        increment: The amount to increment by. Defaults to 1.
        apply_on_backup: Whether to apply on backup entries. Defaults to True.

    Example:
        >>> processor = IncrementProcessor(5)
        >>> entry = SimpleEntryProcessorEntry("counter", 10)
        >>> result = processor.process(entry)
        >>> result  # Returns old value
        10
        >>> entry.get_value()
        15
    """

    def __init__(self, increment: int = 1, apply_on_backup: bool = True):
        super().__init__(apply_on_backup)
        self._increment = increment

    @property
    def increment(self) -> int:
        """The increment amount."""
        return self._increment

    def process(self, entry: EntryProcessorEntry[K, int]) -> int:
        """Increment the entry value and return the old value."""
        old_value = entry.get_value() or 0
        entry.set_value(old_value + self._increment)
        return old_value


class DecrementProcessor(AbstractEntryProcessor[K, int]):
    """Entry processor that decrements a numeric value.

    Args:
        decrement: The amount to decrement by. Defaults to 1.
        apply_on_backup: Whether to apply on backup entries. Defaults to True.

    Example:
        >>> processor = DecrementProcessor(3)
        >>> entry = SimpleEntryProcessorEntry("counter", 10)
        >>> result = processor.process(entry)
        >>> result  # Returns old value
        10
        >>> entry.get_value()
        7
    """

    def __init__(self, decrement: int = 1, apply_on_backup: bool = True):
        super().__init__(apply_on_backup)
        self._decrement = decrement

    @property
    def decrement(self) -> int:
        """The decrement amount."""
        return self._decrement

    def process(self, entry: EntryProcessorEntry[K, int]) -> int:
        """Decrement the entry value and return the old value."""
        old_value = entry.get_value() or 0
        entry.set_value(old_value - self._decrement)
        return old_value


class UpdateEntryProcessor(AbstractEntryProcessor[K, V]):
    """Entry processor that updates an entry with a new value.

    Args:
        new_value: The value to set.
        apply_on_backup: Whether to apply on backup entries. Defaults to True.

    Example:
        >>> processor = UpdateEntryProcessor("new_data")
        >>> entry = SimpleEntryProcessorEntry("key", "old_data")
        >>> result = processor.process(entry)
        >>> result  # Returns old value
        'old_data'
        >>> entry.get_value()
        'new_data'
    """

    def __init__(self, new_value: V, apply_on_backup: bool = True):
        super().__init__(apply_on_backup)
        self._new_value = new_value

    @property
    def new_value(self) -> V:
        """The new value to set."""
        return self._new_value

    def process(self, entry: EntryProcessorEntry[K, V]) -> Optional[V]:
        """Update the entry and return the old value."""
        old_value = entry.get_value()
        entry.set_value(self._new_value)
        return old_value


class DeleteEntryProcessor(AbstractEntryProcessor[K, V]):
    """Entry processor that deletes an entry.

    Args:
        apply_on_backup: Whether to apply on backup entries. Defaults to True.

    Example:
        >>> processor = DeleteEntryProcessor()
        >>> entry = SimpleEntryProcessorEntry("key", "value")
        >>> result = processor.process(entry)
        >>> result  # Returns True if entry existed
        True
        >>> entry.get_value()
        None
    """

    def __init__(self, apply_on_backup: bool = True):
        super().__init__(apply_on_backup)

    def process(self, entry: EntryProcessorEntry[K, V]) -> bool:
        """Delete the entry and return whether it existed."""
        existed = entry.get_value() is not None
        if hasattr(entry, "delete"):
            entry.delete()
        else:
            entry.set_value(None)
        return existed


class CompositeEntryProcessor(AbstractEntryProcessor[K, V]):
    """Entry processor that chains multiple processors together.

    Processors are executed in order, with each receiving the entry
    after previous processors have modified it.

    Args:
        processors: The processors to execute in sequence.
        apply_on_backup: Whether to apply on backup entries. Defaults to True.

    Example:
        >>> composite = CompositeEntryProcessor(
        ...     IncrementProcessor(5),
        ...     IncrementProcessor(10)
        ... )
        >>> entry = SimpleEntryProcessorEntry("counter", 0)
        >>> results = composite.process(entry)
        >>> results  # Returns list of results from each processor
        [0, 5]
        >>> entry.get_value()
        15
    """

    def __init__(
        self, *processors: EntryProcessor[K, V], apply_on_backup: bool = True
    ):
        super().__init__(apply_on_backup)
        self._processors = list(processors)

    @property
    def processors(self) -> list:
        """The list of processors to execute."""
        return list(self._processors)

    def add_processor(self, processor: EntryProcessor[K, V]) -> "CompositeEntryProcessor[K, V]":
        """Add a processor to the chain.

        Args:
            processor: The processor to add.

        Returns:
            This composite processor for method chaining.
        """
        self._processors.append(processor)
        return self

    def process(self, entry: EntryProcessorEntry[K, V]) -> list:
        """Execute all processors and return their results."""
        results = []
        for processor in self._processors:
            result = processor.process(entry)
            results.append(result)
        return results

    def process_backup(self, entry: EntryProcessorEntry[K, V]) -> None:
        """Process backup by applying all child processors' backup logic."""
        if self._apply_on_backup:
            for processor in self._processors:
                if isinstance(processor, EntryBackupProcessor):
                    processor.process_backup(entry)
                else:
                    processor.process(entry)
