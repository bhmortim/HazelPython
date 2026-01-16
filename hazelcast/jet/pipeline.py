"""Jet Pipeline API for building data processing pipelines."""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar, Union
import json

T = TypeVar("T")
R = TypeVar("R")
K = TypeVar("K")
V = TypeVar("V")


class ProcessingGuarantee(Enum):
    """Processing guarantee levels for Jet pipelines."""

    NONE = "NONE"
    AT_LEAST_ONCE = "AT_LEAST_ONCE"
    EXACTLY_ONCE = "EXACTLY_ONCE"


class Source(Generic[T], ABC):
    """Base class for pipeline sources.

    A Source produces items that flow through the pipeline.
    """

    def __init__(self, name: str):
        self._name = name
        self._partitioned = False

    @property
    def name(self) -> str:
        """Return the source name."""
        return self._name

    @property
    def is_partitioned(self) -> bool:
        """Return True if the source is partitioned."""
        return self._partitioned

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert the source to a dictionary representation."""
        pass


class MapSource(Source[tuple]):
    """Source that reads from a Hazelcast IMap."""

    def __init__(self, map_name: str):
        super().__init__(f"map-source-{map_name}")
        self._map_name = map_name
        self._partitioned = True

    @property
    def map_name(self) -> str:
        """Return the map name."""
        return self._map_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "map",
            "name": self._map_name,
        }


class ListSource(Source[T]):
    """Source that reads from a Hazelcast IList."""

    def __init__(self, list_name: str):
        super().__init__(f"list-source-{list_name}")
        self._list_name = list_name

    @property
    def list_name(self) -> str:
        """Return the list name."""
        return self._list_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "list",
            "name": self._list_name,
        }


class TestSource(Source[T]):
    """Source that emits test items for testing pipelines."""

    def __init__(self, items: List[T]):
        super().__init__("test-source")
        self._items = items

    @property
    def items(self) -> List[T]:
        """Return the test items."""
        return self._items

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "test",
            "items": self._items,
        }


class Sink(Generic[T], ABC):
    """Base class for pipeline sinks.

    A Sink consumes items from the pipeline.
    """

    def __init__(self, name: str):
        self._name = name
        self._partitioned = False

    @property
    def name(self) -> str:
        """Return the sink name."""
        return self._name

    @property
    def is_partitioned(self) -> bool:
        """Return True if the sink is partitioned."""
        return self._partitioned

    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """Convert the sink to a dictionary representation."""
        pass


class MapSink(Sink[tuple]):
    """Sink that writes to a Hazelcast IMap."""

    def __init__(self, map_name: str):
        super().__init__(f"map-sink-{map_name}")
        self._map_name = map_name
        self._partitioned = True

    @property
    def map_name(self) -> str:
        """Return the map name."""
        return self._map_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "map",
            "name": self._map_name,
        }


class ListSink(Sink[T]):
    """Sink that writes to a Hazelcast IList."""

    def __init__(self, list_name: str):
        super().__init__(f"list-sink-{list_name}")
        self._list_name = list_name

    @property
    def list_name(self) -> str:
        """Return the list name."""
        return self._list_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "list",
            "name": self._list_name,
        }


class LoggerSink(Sink[T]):
    """Sink that logs items for debugging."""

    def __init__(self, prefix: str = ""):
        super().__init__("logger-sink")
        self._prefix = prefix

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "logger",
            "prefix": self._prefix,
        }


class NoopSink(Sink[T]):
    """Sink that discards all items."""

    def __init__(self):
        super().__init__("noop-sink")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "noop",
        }


class Stage(Generic[T]):
    """Represents a stage in the pipeline.

    Stages can be transformed using various operations like map, filter, etc.
    """

    def __init__(self, pipeline: "Pipeline", name: str, upstream: Optional["Stage"] = None):
        self._pipeline = pipeline
        self._name = name
        self._upstream = upstream
        self._operations: List[Dict[str, Any]] = []

    @property
    def name(self) -> str:
        """Return the stage name."""
        return self._name

    def map(self, map_fn: Callable[[T], R]) -> "Stage[R]":
        """Apply a mapping function to each item.

        Args:
            map_fn: Function to transform each item.

        Returns:
            A new Stage with transformed items.
        """
        new_stage: Stage[R] = Stage(self._pipeline, f"{self._name}-map", self)
        new_stage._operations = self._operations + [{
            "type": "map",
            "function": map_fn.__name__ if hasattr(map_fn, "__name__") else "lambda",
        }]
        return new_stage

    def filter(self, predicate: Callable[[T], bool]) -> "Stage[T]":
        """Filter items based on a predicate.

        Args:
            predicate: Function that returns True for items to keep.

        Returns:
            A new Stage with filtered items.
        """
        new_stage: Stage[T] = Stage(self._pipeline, f"{self._name}-filter", self)
        new_stage._operations = self._operations + [{
            "type": "filter",
            "predicate": predicate.__name__ if hasattr(predicate, "__name__") else "lambda",
        }]
        return new_stage

    def flat_map(self, flat_map_fn: Callable[[T], List[R]]) -> "Stage[R]":
        """Apply a flat-mapping function to each item.

        Args:
            flat_map_fn: Function that returns a list of items for each input.

        Returns:
            A new Stage with flattened items.
        """
        new_stage: Stage[R] = Stage(self._pipeline, f"{self._name}-flatmap", self)
        new_stage._operations = self._operations + [{
            "type": "flatMap",
            "function": flat_map_fn.__name__ if hasattr(flat_map_fn, "__name__") else "lambda",
        }]
        return new_stage

    def group_by(self, key_fn: Callable[[T], K]) -> "Stage[tuple]":
        """Group items by a key function.

        Args:
            key_fn: Function to extract the grouping key.

        Returns:
            A new Stage with grouped items as (key, items) tuples.
        """
        new_stage: Stage[tuple] = Stage(self._pipeline, f"{self._name}-groupby", self)
        new_stage._operations = self._operations + [{
            "type": "groupBy",
            "keyFunction": key_fn.__name__ if hasattr(key_fn, "__name__") else "lambda",
        }]
        return new_stage

    def aggregate(
        self,
        aggregate_fn: Callable[[List[T]], R],
    ) -> "Stage[R]":
        """Aggregate all items using an aggregation function.

        Args:
            aggregate_fn: Function to aggregate items.

        Returns:
            A new Stage with aggregated result.
        """
        new_stage: Stage[R] = Stage(self._pipeline, f"{self._name}-aggregate", self)
        new_stage._operations = self._operations + [{
            "type": "aggregate",
            "function": aggregate_fn.__name__ if hasattr(aggregate_fn, "__name__") else "lambda",
        }]
        return new_stage

    def peek(self, peek_fn: Callable[[T], None]) -> "Stage[T]":
        """Peek at items without modifying them.

        Args:
            peek_fn: Function to call for each item.

        Returns:
            The same Stage for chaining.
        """
        new_stage: Stage[T] = Stage(self._pipeline, f"{self._name}-peek", self)
        new_stage._operations = self._operations + [{
            "type": "peek",
            "function": peek_fn.__name__ if hasattr(peek_fn, "__name__") else "lambda",
        }]
        return new_stage

    def distinct(self) -> "Stage[T]":
        """Remove duplicate items.

        Returns:
            A new Stage with distinct items.
        """
        new_stage: Stage[T] = Stage(self._pipeline, f"{self._name}-distinct", self)
        new_stage._operations = self._operations + [{
            "type": "distinct",
        }]
        return new_stage

    def sort(self, comparator: Optional[Callable[[T, T], int]] = None) -> "Stage[T]":
        """Sort items.

        Args:
            comparator: Optional comparison function.

        Returns:
            A new Stage with sorted items.
        """
        new_stage: Stage[T] = Stage(self._pipeline, f"{self._name}-sort", self)
        op: Dict[str, Any] = {"type": "sort"}
        if comparator:
            op["comparator"] = comparator.__name__ if hasattr(comparator, "__name__") else "lambda"
        new_stage._operations = self._operations + [op]
        return new_stage

    def drain_to(self, sink: Sink[T]) -> "SinkStage":
        """Drain items to a sink.

        Args:
            sink: The sink to write to.

        Returns:
            A SinkStage representing the terminal operation.
        """
        sink_stage = SinkStage(self._pipeline, sink, self)
        self._pipeline._add_sink_stage(sink_stage)
        return sink_stage

    def to_dict(self) -> Dict[str, Any]:
        """Convert the stage to a dictionary representation."""
        return {
            "name": self._name,
            "operations": self._operations,
            "upstream": self._upstream._name if self._upstream else None,
        }


class SinkStage:
    """Represents a terminal sink stage in the pipeline."""

    def __init__(self, pipeline: "Pipeline", sink: Sink, upstream: Stage):
        self._pipeline = pipeline
        self._sink = sink
        self._upstream = upstream

    @property
    def sink(self) -> Sink:
        """Return the sink."""
        return self._sink

    def to_dict(self) -> Dict[str, Any]:
        """Convert the sink stage to a dictionary representation."""
        return {
            "sink": self._sink.to_dict(),
            "upstream": self._upstream._name,
        }


class Pipeline:
    """Represents a Jet data processing pipeline.

    A Pipeline is a DAG (Directed Acyclic Graph) of processing stages.
    It starts with sources and ends with sinks.

    Example:
        >>> pipeline = Pipeline.create()
        >>> source = pipeline.read_from(Sources.map("my-map"))
        >>> source.map(lambda x: x[1]).filter(lambda x: x > 10).drain_to(Sinks.list("results"))
        >>> job = jet_service.new_job(pipeline)
    """

    def __init__(self):
        self._stages: List[Stage] = []
        self._sink_stages: List[SinkStage] = []
        self._sources: List[Source] = []

    @classmethod
    def create(cls) -> "Pipeline":
        """Create a new empty Pipeline.

        Returns:
            A new Pipeline instance.
        """
        return cls()

    def read_from(self, source: Source[T]) -> Stage[T]:
        """Add a source to the pipeline.

        Args:
            source: The source to read from.

        Returns:
            A Stage representing the source output.
        """
        stage: Stage[T] = Stage(self, f"source-{source.name}")
        self._sources.append(source)
        self._stages.append(stage)
        return stage

    def _add_sink_stage(self, sink_stage: SinkStage) -> None:
        """Add a sink stage to the pipeline."""
        self._sink_stages.append(sink_stage)

    def is_empty(self) -> bool:
        """Check if the pipeline has no stages.

        Returns:
            True if the pipeline has no stages.
        """
        return len(self._stages) == 0

    def to_dag(self) -> Dict[str, Any]:
        """Convert the pipeline to a DAG representation.

        Returns:
            A dictionary representing the DAG.
        """
        return {
            "sources": [s.to_dict() for s in self._sources],
            "stages": [s.to_dict() for s in self._stages],
            "sinks": [s.to_dict() for s in self._sink_stages],
        }

    def to_json(self) -> str:
        """Convert the pipeline to JSON.

        Returns:
            JSON string representation of the pipeline.
        """
        return json.dumps(self.to_dag())

    def __repr__(self) -> str:
        return f"Pipeline(sources={len(self._sources)}, stages={len(self._stages)}, sinks={len(self._sink_stages)})"


class Sources:
    """Factory for creating pipeline sources."""

    @staticmethod
    def map(map_name: str) -> MapSource:
        """Create a source that reads from a Hazelcast IMap.

        Args:
            map_name: The name of the map to read from.

        Returns:
            A MapSource instance.
        """
        return MapSource(map_name)

    @staticmethod
    def list(list_name: str) -> ListSource:
        """Create a source that reads from a Hazelcast IList.

        Args:
            list_name: The name of the list to read from.

        Returns:
            A ListSource instance.
        """
        return ListSource(list_name)

    @staticmethod
    def items(*items: T) -> TestSource[T]:
        """Create a source that emits the given items.

        Args:
            *items: The items to emit.

        Returns:
            A TestSource instance.
        """
        return TestSource(list(items))


class Sinks:
    """Factory for creating pipeline sinks."""

    @staticmethod
    def map(map_name: str) -> MapSink:
        """Create a sink that writes to a Hazelcast IMap.

        Args:
            map_name: The name of the map to write to.

        Returns:
            A MapSink instance.
        """
        return MapSink(map_name)

    @staticmethod
    def list(list_name: str) -> ListSink:
        """Create a sink that writes to a Hazelcast IList.

        Args:
            list_name: The name of the list to write to.

        Returns:
            A ListSink instance.
        """
        return ListSink(list_name)

    @staticmethod
    def logger(prefix: str = "") -> LoggerSink:
        """Create a sink that logs items.

        Args:
            prefix: Optional prefix for log messages.

        Returns:
            A LoggerSink instance.
        """
        return LoggerSink(prefix)

    @staticmethod
    def noop() -> NoopSink:
        """Create a sink that discards all items.

        Returns:
            A NoopSink instance.
        """
        return NoopSink()
