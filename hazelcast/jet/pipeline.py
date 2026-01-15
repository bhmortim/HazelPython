"""Jet pipeline building for streaming and batch processing."""

from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, Iterator, List, Optional, TypeVar
import uuid

T = TypeVar("T")
R = TypeVar("R")


class Source(ABC, Generic[T]):
    """Base class for pipeline sources."""

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        """Get the source name."""
        return self._name

    @abstractmethod
    def items(self) -> Iterator[T]:
        """Get items from the source."""
        pass


class BatchSource(Source[T]):
    """A batch source that reads from a collection."""

    def __init__(self, name: str, data: List[T]):
        super().__init__(name)
        self._data = data

    def items(self) -> Iterator[T]:
        return iter(self._data)


class MapSource(Source[tuple]):
    """A source that reads from an IMap."""

    def __init__(self, map_name: str):
        super().__init__(f"map:{map_name}")
        self._map_name = map_name

    @property
    def map_name(self) -> str:
        """Get the map name."""
        return self._map_name

    def items(self) -> Iterator[tuple]:
        return iter([])


class Sink(ABC, Generic[T]):
    """Base class for pipeline sinks."""

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        """Get the sink name."""
        return self._name

    @abstractmethod
    def receive(self, item: T) -> None:
        """Receive an item."""
        pass


class ListSink(Sink[T]):
    """A sink that collects items into a list."""

    def __init__(self, name: str):
        super().__init__(name)
        self._items: List[T] = []

    @property
    def items(self) -> List[T]:
        """Get collected items."""
        return list(self._items)

    def receive(self, item: T) -> None:
        self._items.append(item)


class MapSink(Sink[tuple]):
    """A sink that writes to an IMap."""

    def __init__(self, map_name: str):
        super().__init__(f"map:{map_name}")
        self._map_name = map_name

    @property
    def map_name(self) -> str:
        """Get the map name."""
        return self._map_name

    def receive(self, item: tuple) -> None:
        pass


class Stage(Generic[T]):
    """A stage in the pipeline that can be transformed."""

    def __init__(self, pipeline: "Pipeline", source: Optional[Source[T]] = None):
        self._pipeline = pipeline
        self._source = source
        self._transforms: List[Callable[[Any], Any]] = []
        self._sink: Optional[Sink] = None

    @property
    def pipeline(self) -> "Pipeline":
        """Get the pipeline this stage belongs to."""
        return self._pipeline

    def map(self, fn: Callable[[T], R]) -> "Stage[R]":
        """Apply a mapping function to each item.

        Args:
            fn: The mapping function.

        Returns:
            A new stage with the mapping applied.
        """
        new_stage: Stage[R] = Stage(self._pipeline)
        new_stage._source = self._source
        new_stage._transforms = list(self._transforms)
        new_stage._transforms.append(fn)
        return new_stage

    def filter(self, predicate: Callable[[T], bool]) -> "Stage[T]":
        """Filter items using a predicate.

        Args:
            predicate: The filter predicate.

        Returns:
            A new stage with the filter applied.
        """
        def filter_fn(item: T) -> Optional[T]:
            return item if predicate(item) else None

        new_stage: Stage[T] = Stage(self._pipeline)
        new_stage._source = self._source
        new_stage._transforms = list(self._transforms)
        new_stage._transforms.append(("filter", predicate))
        return new_stage

    def flat_map(self, fn: Callable[[T], Iterator[R]]) -> "Stage[R]":
        """Flat map items.

        Args:
            fn: Function returning an iterator.

        Returns:
            A new stage with flat mapping applied.
        """
        new_stage: Stage[R] = Stage(self._pipeline)
        new_stage._source = self._source
        new_stage._transforms = list(self._transforms)
        new_stage._transforms.append(("flatmap", fn))
        return new_stage

    def write_to(self, sink: Sink[T]) -> "Pipeline":
        """Write this stage to a sink.

        Args:
            sink: The sink to write to.

        Returns:
            The pipeline for chaining.
        """
        self._sink = sink
        self._pipeline._add_stage(self)
        return self._pipeline

    def drain_to(self, sink: Sink[T]) -> "Pipeline":
        """Alias for write_to.

        Args:
            sink: The sink to drain to.

        Returns:
            The pipeline for chaining.
        """
        return self.write_to(sink)


class Pipeline:
    """Represents a Jet processing pipeline.

    Pipelines are built using a fluent API starting with a source,
    applying transformations, and writing to a sink.
    """

    def __init__(self):
        self._id = str(uuid.uuid4())
        self._stages: List[Stage] = []

    @classmethod
    def create(cls) -> "Pipeline":
        """Create a new pipeline.

        Returns:
            A new Pipeline instance.
        """
        return cls()

    @property
    def id(self) -> str:
        """Get the pipeline ID."""
        return self._id

    def read_from(self, source: Source[T]) -> Stage[T]:
        """Read from a source to start the pipeline.

        Args:
            source: The source to read from.

        Returns:
            A stage for further transformations.
        """
        stage: Stage[T] = Stage(self, source)
        return stage

    def draw_from(self, source: Source[T]) -> Stage[T]:
        """Alias for read_from.

        Args:
            source: The source to read from.

        Returns:
            A stage for further transformations.
        """
        return self.read_from(source)

    def _add_stage(self, stage: Stage) -> None:
        """Add a completed stage to the pipeline."""
        self._stages.append(stage)

    @staticmethod
    def from_list(name: str, items: List[T]) -> Source[T]:
        """Create a batch source from a list.

        Args:
            name: The source name.
            items: The items to read.

        Returns:
            A batch source.
        """
        return BatchSource(name, items)

    @staticmethod
    def from_map(map_name: str) -> Source[tuple]:
        """Create a source that reads from an IMap.

        Args:
            map_name: The map name.

        Returns:
            A map source.
        """
        return MapSource(map_name)

    @staticmethod
    def to_list(name: str) -> Sink:
        """Create a sink that collects to a list.

        Args:
            name: The sink name.

        Returns:
            A list sink.
        """
        return ListSink(name)

    @staticmethod
    def to_map(map_name: str) -> Sink[tuple]:
        """Create a sink that writes to an IMap.

        Args:
            map_name: The map name.

        Returns:
            A map sink.
        """
        return MapSink(map_name)

    def __repr__(self) -> str:
        return f"Pipeline(id={self._id!r}, stages={len(self._stages)})"
