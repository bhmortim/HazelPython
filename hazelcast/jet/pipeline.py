"""Jet pipeline API for building distributed data processing pipelines."""

import uuid
from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, List, Optional, TypeVar, Union

T = TypeVar("T")
R = TypeVar("R")


class Source(ABC):
    """Base class for pipeline data sources."""

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        """Get the source name."""
        return self._name

    @property
    @abstractmethod
    def source_type(self) -> str:
        """Get the source type identifier."""
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self._name!r})"


class BatchSource(Source):
    """Source for bounded/batch data."""

    def __init__(self, name: str, items: Optional[List[Any]] = None):
        super().__init__(name)
        self._items = items or []

    @property
    def source_type(self) -> str:
        return "batch"

    @property
    def items(self) -> List[Any]:
        """Get the source items."""
        return self._items


class StreamSource(Source):
    """Source for unbounded/streaming data."""

    def __init__(self, name: str):
        super().__init__(name)

    @property
    def source_type(self) -> str:
        return "stream"


class MapSource(Source):
    """Source that reads from an IMap."""

    def __init__(self, name: str, map_name: Optional[str] = None):
        super().__init__(name)
        self._map_name = map_name or name

    @property
    def source_type(self) -> str:
        return "map"

    @property
    def map_name(self) -> str:
        """Get the IMap name."""
        return self._map_name


class ListSource(Source):
    """Source that reads from an IList."""

    def __init__(self, name: str, list_name: Optional[str] = None):
        super().__init__(name)
        self._list_name = list_name or name

    @property
    def source_type(self) -> str:
        return "list"

    @property
    def list_name(self) -> str:
        """Get the IList name."""
        return self._list_name


class Sink(ABC):
    """Base class for pipeline data sinks."""

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        """Get the sink name."""
        return self._name

    @property
    @abstractmethod
    def sink_type(self) -> str:
        """Get the sink type identifier."""
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self._name!r})"


class MapSink(Sink):
    """Sink that writes to an IMap."""

    def __init__(self, name: str, map_name: Optional[str] = None):
        super().__init__(name)
        self._map_name = map_name or name

    @property
    def sink_type(self) -> str:
        return "map"

    @property
    def map_name(self) -> str:
        """Get the IMap name."""
        return self._map_name


class ListSink(Sink):
    """Sink that writes to an IList."""

    def __init__(self, name: str, list_name: Optional[str] = None):
        super().__init__(name)
        self._list_name = list_name or name

    @property
    def sink_type(self) -> str:
        return "list"

    @property
    def list_name(self) -> str:
        """Get the IList name."""
        return self._list_name


class LoggerSink(Sink):
    """Sink that logs items."""

    def __init__(self, name: str = "logger"):
        super().__init__(name)

    @property
    def sink_type(self) -> str:
        return "logger"


class Stage(Generic[T]):
    """A stage in a pipeline representing a transformation.

    Stages are created by reading from a source and can be chained
    with transformations like map, filter, flatMap, etc.
    """

    def __init__(
        self,
        pipeline: "Pipeline",
        name: str,
        upstream: Optional["Stage"] = None,
    ):
        self._pipeline = pipeline
        self._name = name
        self._upstream = upstream
        self._transforms: List[tuple] = []

    @property
    def name(self) -> str:
        """Get the stage name."""
        return self._name

    @property
    def pipeline(self) -> "Pipeline":
        """Get the parent pipeline."""
        return self._pipeline

    def map(self, fn: Callable[[T], R]) -> "Stage[R]":
        """Apply a mapping function to each item.

        Args:
            fn: Function to apply to each item.

        Returns:
            A new stage with the mapped items.
        """
        new_stage: Stage[R] = Stage(
            self._pipeline,
            f"{self._name}-map",
            self,
        )
        new_stage._transforms = self._transforms + [("map", fn)]
        self._pipeline._add_stage(new_stage)
        return new_stage

    def filter(self, predicate: Callable[[T], bool]) -> "Stage[T]":
        """Filter items based on a predicate.

        Args:
            predicate: Function that returns True for items to keep.

        Returns:
            A new stage with filtered items.
        """
        new_stage: Stage[T] = Stage(
            self._pipeline,
            f"{self._name}-filter",
            self,
        )
        new_stage._transforms = self._transforms + [("filter", predicate)]
        self._pipeline._add_stage(new_stage)
        return new_stage

    def flat_map(self, fn: Callable[[T], List[R]]) -> "Stage[R]":
        """Apply a function that returns multiple items per input.

        Args:
            fn: Function that returns a list of items.

        Returns:
            A new stage with flattened results.
        """
        new_stage: Stage[R] = Stage(
            self._pipeline,
            f"{self._name}-flatmap",
            self,
        )
        new_stage._transforms = self._transforms + [("flat_map", fn)]
        self._pipeline._add_stage(new_stage)
        return new_stage

    def peek(self, fn: Callable[[T], None]) -> "Stage[T]":
        """Perform a side-effect for each item without modifying it.

        Args:
            fn: Function to call for each item.

        Returns:
            A new stage with unchanged items.
        """
        new_stage: Stage[T] = Stage(
            self._pipeline,
            f"{self._name}-peek",
            self,
        )
        new_stage._transforms = self._transforms + [("peek", fn)]
        self._pipeline._add_stage(new_stage)
        return new_stage

    def group_by(self, key_fn: Callable[[T], Any]) -> "Stage[tuple]":
        """Group items by a key function.

        Args:
            key_fn: Function to extract the grouping key.

        Returns:
            A new stage with (key, items) tuples.
        """
        new_stage: Stage[tuple] = Stage(
            self._pipeline,
            f"{self._name}-groupby",
            self,
        )
        new_stage._transforms = self._transforms + [("group_by", key_fn)]
        self._pipeline._add_stage(new_stage)
        return new_stage

    def distinct(self) -> "Stage[T]":
        """Remove duplicate items.

        Returns:
            A new stage with distinct items.
        """
        new_stage: Stage[T] = Stage(
            self._pipeline,
            f"{self._name}-distinct",
            self,
        )
        new_stage._transforms = self._transforms + [("distinct", None)]
        self._pipeline._add_stage(new_stage)
        return new_stage

    def sort(self, key_fn: Optional[Callable[[T], Any]] = None) -> "Stage[T]":
        """Sort items.

        Args:
            key_fn: Optional function to extract sort key.

        Returns:
            A new stage with sorted items.
        """
        new_stage: Stage[T] = Stage(
            self._pipeline,
            f"{self._name}-sort",
            self,
        )
        new_stage._transforms = self._transforms + [("sort", key_fn)]
        self._pipeline._add_stage(new_stage)
        return new_stage

    def aggregate(
        self,
        accumulator: Callable[[Any, T], Any],
        initial: Any = None,
    ) -> "Stage":
        """Aggregate items using an accumulator function.

        Args:
            accumulator: Function (acc, item) -> new_acc.
            initial: Initial accumulator value.

        Returns:
            A new stage with aggregated result.
        """
        new_stage: Stage = Stage(
            self._pipeline,
            f"{self._name}-aggregate",
            self,
        )
        new_stage._transforms = self._transforms + [
            ("aggregate", (accumulator, initial))
        ]
        self._pipeline._add_stage(new_stage)
        return new_stage

    def write_to(self, sink: Sink) -> "Pipeline":
        """Write the stage output to a sink.

        Args:
            sink: The sink to write to.

        Returns:
            The parent pipeline.
        """
        self._pipeline._set_sink(sink, self)
        return self._pipeline

    def __repr__(self) -> str:
        return f"Stage(name={self._name!r}, transforms={len(self._transforms)})"


class Pipeline:
    """A Jet pipeline for distributed data processing.

    Pipelines are built by defining sources, transformations, and sinks.
    Once built, they can be submitted to a JetService for execution.

    Example:
        >>> pipeline = Pipeline.create()
        >>> source = Pipeline.from_list("numbers", [1, 2, 3, 4, 5])
        >>> sink = Pipeline.to_list("results")
        >>> pipeline.read_from(source).map(lambda x: x * 2).write_to(sink)
        >>> job = jet_service.submit(pipeline)
    """

    def __init__(self):
        self._id = str(uuid.uuid4())
        self._source: Optional[Source] = None
        self._sink: Optional[Sink] = None
        self._stages: List[Stage] = []
        self._terminal_stage: Optional[Stage] = None

    @property
    def id(self) -> str:
        """Get the pipeline ID."""
        return self._id

    @property
    def source(self) -> Optional[Source]:
        """Get the pipeline source."""
        return self._source

    @property
    def sink(self) -> Optional[Sink]:
        """Get the pipeline sink."""
        return self._sink

    @property
    def stages(self) -> List[Stage]:
        """Get all stages in the pipeline."""
        return self._stages.copy()

    @classmethod
    def create(cls) -> "Pipeline":
        """Create a new empty pipeline.

        Returns:
            A new Pipeline instance.
        """
        return cls()

    def read_from(self, source: Source) -> Stage:
        """Read from a source to start the pipeline.

        Args:
            source: The source to read from.

        Returns:
            The initial stage.
        """
        self._source = source
        stage: Stage = Stage(self, f"source-{source.name}")
        self._stages.append(stage)
        return stage

    def _add_stage(self, stage: Stage) -> None:
        """Add a stage to the pipeline."""
        self._stages.append(stage)

    def _set_sink(self, sink: Sink, terminal_stage: Stage) -> None:
        """Set the pipeline sink."""
        self._sink = sink
        self._terminal_stage = terminal_stage

    @staticmethod
    def from_list(name: str, items: List[Any]) -> BatchSource:
        """Create a batch source from a list.

        Args:
            name: Source name.
            items: List of items.

        Returns:
            A BatchSource containing the items.
        """
        return BatchSource(name, items)

    @staticmethod
    def from_map(name: str) -> MapSource:
        """Create a source that reads from an IMap.

        Args:
            name: Name of the IMap.

        Returns:
            A MapSource for the IMap.
        """
        return MapSource(name)

    @staticmethod
    def to_list(name: str) -> ListSink:
        """Create a sink that writes to an IList.

        Args:
            name: Name of the IList.

        Returns:
            A ListSink for the IList.
        """
        return ListSink(name)

    @staticmethod
    def to_map(name: str) -> MapSink:
        """Create a sink that writes to an IMap.

        Args:
            name: Name of the IMap.

        Returns:
            A MapSink for the IMap.
        """
        return MapSink(name)

    @staticmethod
    def to_logger(name: str = "logger") -> LoggerSink:
        """Create a sink that logs items.

        Args:
            name: Logger name.

        Returns:
            A LoggerSink.
        """
        return LoggerSink(name)

    def is_complete(self) -> bool:
        """Check if the pipeline has both source and sink defined."""
        return self._source is not None and self._sink is not None

    def __repr__(self) -> str:
        return (
            f"Pipeline(id={self._id[:8]}..., "
            f"stages={len(self._stages)}, "
            f"source={self._source}, "
            f"sink={self._sink})"
        )
