"""Jet pipeline API for building distributed data processing pipelines."""

import uuid
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable, Dict, Generic, List, Optional, Tuple, TypeVar, Union

T = TypeVar("T")
R = TypeVar("R")
K = TypeVar("K")
A = TypeVar("A")


class WindowType(Enum):
    """Type of windowing for streaming operations."""

    TUMBLING = "TUMBLING"
    SLIDING = "SLIDING"
    SESSION = "SESSION"


class WindowDefinition:
    """Definition of a window for streaming aggregations."""

    def __init__(
        self,
        window_type: WindowType,
        size_millis: int,
        slide_millis: Optional[int] = None,
        gap_millis: Optional[int] = None,
    ):
        self._window_type = window_type
        self._size_millis = size_millis
        self._slide_millis = slide_millis or size_millis
        self._gap_millis = gap_millis

    @property
    def window_type(self) -> WindowType:
        """Get the window type."""
        return self._window_type

    @property
    def size_millis(self) -> int:
        """Get the window size in milliseconds."""
        return self._size_millis

    @property
    def slide_millis(self) -> int:
        """Get the slide interval in milliseconds."""
        return self._slide_millis

    @property
    def gap_millis(self) -> Optional[int]:
        """Get the session gap in milliseconds."""
        return self._gap_millis

    @classmethod
    def tumbling(cls, size_millis: int) -> "WindowDefinition":
        """Create a tumbling window definition."""
        return cls(WindowType.TUMBLING, size_millis)

    @classmethod
    def sliding(cls, size_millis: int, slide_millis: int) -> "WindowDefinition":
        """Create a sliding window definition."""
        return cls(WindowType.SLIDING, size_millis, slide_millis)

    @classmethod
    def session(cls, gap_millis: int) -> "WindowDefinition":
        """Create a session window definition."""
        return cls(WindowType.SESSION, 0, gap_millis=gap_millis)

    def __repr__(self) -> str:
        return f"WindowDefinition(type={self._window_type.value}, size={self._size_millis}ms)"


class AggregateOperation(Generic[T, A, R]):
    """Defines an aggregate operation with create, accumulate, combine, and finish functions.

    This is the core abstraction for defining how to aggregate items in a pipeline.

    Type Parameters:
        T: Input item type.
        A: Accumulator type.
        R: Result type.
    """

    def __init__(
        self,
        create_fn: Callable[[], A],
        accumulate_fn: Callable[[A, T], A],
        combine_fn: Optional[Callable[[A, A], A]] = None,
        finish_fn: Optional[Callable[[A], R]] = None,
        deduct_fn: Optional[Callable[[A, T], A]] = None,
    ):
        self._create_fn = create_fn
        self._accumulate_fn = accumulate_fn
        self._combine_fn = combine_fn
        self._finish_fn = finish_fn or (lambda a: a)
        self._deduct_fn = deduct_fn

    @property
    def create_fn(self) -> Callable[[], A]:
        """Get the accumulator creation function."""
        return self._create_fn

    @property
    def accumulate_fn(self) -> Callable[[A, T], A]:
        """Get the accumulate function."""
        return self._accumulate_fn

    @property
    def combine_fn(self) -> Optional[Callable[[A, A], A]]:
        """Get the combine function for parallel aggregation."""
        return self._combine_fn

    @property
    def finish_fn(self) -> Callable[[A], R]:
        """Get the finish function to produce final result."""
        return self._finish_fn

    @property
    def deduct_fn(self) -> Optional[Callable[[A, T], A]]:
        """Get the deduct function for sliding windows."""
        return self._deduct_fn

    @classmethod
    def counting(cls) -> "AggregateOperation[Any, int, int]":
        """Create an aggregate operation that counts items."""
        return cls(
            create_fn=lambda: 0,
            accumulate_fn=lambda acc, _: acc + 1,
            combine_fn=lambda a, b: a + b,
            finish_fn=lambda a: a,
            deduct_fn=lambda acc, _: acc - 1,
        )

    @classmethod
    def summing(
        cls, value_fn: Callable[[T], float] = None
    ) -> "AggregateOperation[T, float, float]":
        """Create an aggregate operation that sums values."""
        get_value = value_fn or (lambda x: x)
        return cls(
            create_fn=lambda: 0.0,
            accumulate_fn=lambda acc, item: acc + get_value(item),
            combine_fn=lambda a, b: a + b,
            finish_fn=lambda a: a,
            deduct_fn=lambda acc, item: acc - get_value(item),
        )

    @classmethod
    def averaging(
        cls, value_fn: Callable[[T], float] = None
    ) -> "AggregateOperation[T, Tuple[float, int], float]":
        """Create an aggregate operation that computes average."""
        get_value = value_fn or (lambda x: x)
        return cls(
            create_fn=lambda: (0.0, 0),
            accumulate_fn=lambda acc, item: (acc[0] + get_value(item), acc[1] + 1),
            combine_fn=lambda a, b: (a[0] + b[0], a[1] + b[1]),
            finish_fn=lambda a: a[0] / a[1] if a[1] > 0 else 0.0,
        )

    @classmethod
    def min_by(
        cls, compare_fn: Callable[[T], Any] = None
    ) -> "AggregateOperation[T, Optional[T], Optional[T]]":
        """Create an aggregate operation that finds minimum."""
        key_fn = compare_fn or (lambda x: x)
        return cls(
            create_fn=lambda: None,
            accumulate_fn=lambda acc, item: item if acc is None or key_fn(item) < key_fn(acc) else acc,
            combine_fn=lambda a, b: a if b is None or (a is not None and key_fn(a) <= key_fn(b)) else b,
            finish_fn=lambda a: a,
        )

    @classmethod
    def max_by(
        cls, compare_fn: Callable[[T], Any] = None
    ) -> "AggregateOperation[T, Optional[T], Optional[T]]":
        """Create an aggregate operation that finds maximum."""
        key_fn = compare_fn or (lambda x: x)
        return cls(
            create_fn=lambda: None,
            accumulate_fn=lambda acc, item: item if acc is None or key_fn(item) > key_fn(acc) else acc,
            combine_fn=lambda a, b: a if b is None or (a is not None and key_fn(a) >= key_fn(b)) else b,
            finish_fn=lambda a: a,
        )

    @classmethod
    def to_list(cls) -> "AggregateOperation[T, List[T], List[T]]":
        """Create an aggregate operation that collects items to a list."""
        return cls(
            create_fn=lambda: [],
            accumulate_fn=lambda acc, item: acc + [item],
            combine_fn=lambda a, b: a + b,
            finish_fn=lambda a: a,
        )

    @classmethod
    def to_set(cls) -> "AggregateOperation[T, set, set]":
        """Create an aggregate operation that collects items to a set."""
        def accumulate(acc: set, item: T) -> set:
            acc.add(item)
            return acc
        return cls(
            create_fn=lambda: set(),
            accumulate_fn=accumulate,
            combine_fn=lambda a, b: a | b,
            finish_fn=lambda a: a,
        )

    def __repr__(self) -> str:
        return f"AggregateOperation(create={self._create_fn}, accumulate={self._accumulate_fn})"


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


class FileSource(Source):
    """Source that reads from files."""

    def __init__(
        self,
        name: str,
        directory: str,
        glob: str = "*",
        shared_file_system: bool = False,
    ):
        super().__init__(name)
        self._directory = directory
        self._glob = glob
        self._shared_file_system = shared_file_system

    @property
    def source_type(self) -> str:
        return "file"

    @property
    def directory(self) -> str:
        """Get the source directory."""
        return self._directory

    @property
    def glob(self) -> str:
        """Get the file glob pattern."""
        return self._glob

    @property
    def shared_file_system(self) -> bool:
        """Get whether the file system is shared across nodes."""
        return self._shared_file_system


class SocketSource(Source):
    """Source that reads from a TCP socket."""

    def __init__(self, name: str, host: str, port: int):
        super().__init__(name)
        self._host = host
        self._port = port

    @property
    def source_type(self) -> str:
        return "socket"

    @property
    def host(self) -> str:
        """Get the host address."""
        return self._host

    @property
    def port(self) -> int:
        """Get the port number."""
        return self._port


class JdbcSource(Source):
    """Source that reads from a JDBC database."""

    def __init__(
        self,
        name: str,
        connection_url: str,
        query: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        super().__init__(name)
        self._connection_url = connection_url
        self._query = query
        self._username = username
        self._password = password

    @property
    def source_type(self) -> str:
        return "jdbc"

    @property
    def connection_url(self) -> str:
        """Get the JDBC connection URL."""
        return self._connection_url

    @property
    def query(self) -> str:
        """Get the SQL query."""
        return self._query


class KafkaSource(Source):
    """Source that reads from Apache Kafka."""

    def __init__(
        self,
        name: str,
        topic: str,
        bootstrap_servers: str,
        group_id: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
    ):
        super().__init__(name)
        self._topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._group_id = group_id
        self._properties = properties or {}

    @property
    def source_type(self) -> str:
        return "kafka"

    @property
    def topic(self) -> str:
        """Get the Kafka topic."""
        return self._topic

    @property
    def bootstrap_servers(self) -> str:
        """Get the bootstrap servers."""
        return self._bootstrap_servers

    @property
    def group_id(self) -> Optional[str]:
        """Get the consumer group ID."""
        return self._group_id

    @property
    def properties(self) -> Dict[str, str]:
        """Get additional Kafka properties."""
        return self._properties


class TestSource(Source):
    """Source for testing that generates items."""

    def __init__(
        self,
        name: str,
        items_per_second: int = 10,
        item_generator: Optional[Callable[[int], Any]] = None,
    ):
        super().__init__(name)
        self._items_per_second = items_per_second
        self._item_generator = item_generator or (lambda i: i)

    @property
    def source_type(self) -> str:
        return "test"

    @property
    def items_per_second(self) -> int:
        """Get the items per second rate."""
        return self._items_per_second

    @property
    def item_generator(self) -> Callable[[int], Any]:
        """Get the item generator function."""
        return self._item_generator


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


class FileSink(Sink):
    """Sink that writes to files."""

    def __init__(
        self,
        name: str,
        directory: str,
        to_string_fn: Optional[Callable[[Any], str]] = None,
    ):
        super().__init__(name)
        self._directory = directory
        self._to_string_fn = to_string_fn or str

    @property
    def sink_type(self) -> str:
        return "file"

    @property
    def directory(self) -> str:
        """Get the output directory."""
        return self._directory

    @property
    def to_string_fn(self) -> Callable[[Any], str]:
        """Get the string conversion function."""
        return self._to_string_fn


class SocketSink(Sink):
    """Sink that writes to a TCP socket."""

    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        to_string_fn: Optional[Callable[[Any], str]] = None,
    ):
        super().__init__(name)
        self._host = host
        self._port = port
        self._to_string_fn = to_string_fn or str

    @property
    def sink_type(self) -> str:
        return "socket"

    @property
    def host(self) -> str:
        """Get the host address."""
        return self._host

    @property
    def port(self) -> int:
        """Get the port number."""
        return self._port


class JdbcSink(Sink):
    """Sink that writes to a JDBC database."""

    def __init__(
        self,
        name: str,
        connection_url: str,
        update_query: str,
        bind_fn: Optional[Callable[[Any], tuple]] = None,
    ):
        super().__init__(name)
        self._connection_url = connection_url
        self._update_query = update_query
        self._bind_fn = bind_fn

    @property
    def sink_type(self) -> str:
        return "jdbc"

    @property
    def connection_url(self) -> str:
        """Get the JDBC connection URL."""
        return self._connection_url

    @property
    def update_query(self) -> str:
        """Get the update query template."""
        return self._update_query


class KafkaSink(Sink):
    """Sink that writes to Apache Kafka."""

    def __init__(
        self,
        name: str,
        topic: str,
        bootstrap_servers: str,
        properties: Optional[Dict[str, str]] = None,
        to_key_fn: Optional[Callable[[Any], Any]] = None,
        to_value_fn: Optional[Callable[[Any], Any]] = None,
    ):
        super().__init__(name)
        self._topic = topic
        self._bootstrap_servers = bootstrap_servers
        self._properties = properties or {}
        self._to_key_fn = to_key_fn
        self._to_value_fn = to_value_fn or (lambda x: x)

    @property
    def sink_type(self) -> str:
        return "kafka"

    @property
    def topic(self) -> str:
        """Get the Kafka topic."""
        return self._topic

    @property
    def bootstrap_servers(self) -> str:
        """Get the bootstrap servers."""
        return self._bootstrap_servers


class NoopSink(Sink):
    """Sink that discards all items (for testing)."""

    def __init__(self, name: str = "noop"):
        super().__init__(name)

    @property
    def sink_type(self) -> str:
        return "noop"


class StageWithKey(Generic[T, K]):
    """A stage that has a grouping key defined.

    This stage is created by calling grouping_key() on a Stage and
    allows key-aware operations like aggregation.
    """

    def __init__(
        self,
        stage: "Stage[T]",
        key_fn: Callable[[T], K],
    ):
        self._stage = stage
        self._key_fn = key_fn

    @property
    def key_fn(self) -> Callable[[T], K]:
        """Get the key extraction function."""
        return self._key_fn

    @property
    def pipeline(self) -> "Pipeline":
        """Get the parent pipeline."""
        return self._stage.pipeline

    def aggregate(
        self,
        operation: AggregateOperation[T, A, R],
    ) -> "Stage[Tuple[K, R]]":
        """Aggregate items by key using the given operation.

        Args:
            operation: The aggregate operation to apply.

        Returns:
            A new stage with (key, aggregated_result) tuples.
        """
        new_stage: Stage[Tuple[K, R]] = Stage(
            self._stage._pipeline,
            f"{self._stage._name}-aggregate-by-key",
            self._stage,
        )
        new_stage._transforms = self._stage._transforms + [
            ("aggregate_by_key", (self._key_fn, operation))
        ]
        self._stage._pipeline._add_stage(new_stage)
        return new_stage

    def window(
        self,
        window_def: WindowDefinition,
    ) -> "WindowedStage[T, K]":
        """Apply windowing to this keyed stage.

        Args:
            window_def: The window definition.

        Returns:
            A windowed stage for window-based aggregations.
        """
        return WindowedStage(self, window_def)

    def __repr__(self) -> str:
        return f"StageWithKey(stage={self._stage._name!r}, key_fn={self._key_fn})"


class WindowedStage(Generic[T, K]):
    """A stage with both grouping key and window defined."""

    def __init__(
        self,
        keyed_stage: StageWithKey[T, K],
        window_def: WindowDefinition,
    ):
        self._keyed_stage = keyed_stage
        self._window_def = window_def

    @property
    def window_definition(self) -> WindowDefinition:
        """Get the window definition."""
        return self._window_def

    def aggregate(
        self,
        operation: AggregateOperation[T, A, R],
    ) -> "Stage[Tuple[K, R]]":
        """Aggregate items in windows by key.

        Args:
            operation: The aggregate operation to apply.

        Returns:
            A new stage with (key, aggregated_result) tuples per window.
        """
        stage = self._keyed_stage._stage
        new_stage: Stage[Tuple[K, R]] = Stage(
            stage._pipeline,
            f"{stage._name}-windowed-aggregate",
            stage,
        )
        new_stage._transforms = stage._transforms + [
            ("windowed_aggregate", (self._keyed_stage._key_fn, self._window_def, operation))
        ]
        stage._pipeline._add_stage(new_stage)
        return new_stage

    def __repr__(self) -> str:
        return f"WindowedStage(window={self._window_def})"


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

    @property
    def upstream(self) -> Optional["Stage"]:
        """Get the upstream stage."""
        return self._upstream

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
        operation: Union[AggregateOperation[T, A, R], Callable[[Any, T], Any]],
        initial: Any = None,
    ) -> "Stage":
        """Aggregate items using an aggregate operation or accumulator function.

        Args:
            operation: An AggregateOperation or accumulator function (acc, item) -> new_acc.
            initial: Initial accumulator value (only used with function).

        Returns:
            A new stage with aggregated result.
        """
        new_stage: Stage = Stage(
            self._pipeline,
            f"{self._name}-aggregate",
            self,
        )
        if isinstance(operation, AggregateOperation):
            new_stage._transforms = self._transforms + [
                ("aggregate_op", operation)
            ]
        else:
            new_stage._transforms = self._transforms + [
                ("aggregate", (operation, initial))
            ]
        self._pipeline._add_stage(new_stage)
        return new_stage

    def grouping_key(self, key_fn: Callable[[T], K]) -> StageWithKey[T, K]:
        """Set a grouping key for this stage.

        Items with the same key will be grouped together for subsequent
        aggregation operations.

        Args:
            key_fn: Function to extract the grouping key from each item.

        Returns:
            A StageWithKey that allows keyed operations.

        Example:
            >>> stage.grouping_key(lambda x: x["category"]).aggregate(...)
        """
        return StageWithKey(self, key_fn)

    def add_timestamps(
        self,
        timestamp_fn: Callable[[T], int],
        allowed_lag_millis: int = 0,
    ) -> "Stage[T]":
        """Add event timestamps to items for windowing.

        Args:
            timestamp_fn: Function to extract timestamp (epoch millis) from item.
            allowed_lag_millis: Maximum allowed event lateness.

        Returns:
            A new stage with timestamped items.
        """
        new_stage: Stage[T] = Stage(
            self._pipeline,
            f"{self._name}-timestamps",
            self,
        )
        new_stage._transforms = self._transforms + [
            ("add_timestamps", (timestamp_fn, allowed_lag_millis))
        ]
        self._pipeline._add_stage(new_stage)
        return new_stage

    def rebalance(self, key_fn: Optional[Callable[[T], Any]] = None) -> "Stage[T]":
        """Rebalance items across processors.

        Args:
            key_fn: Optional key function for partitioned rebalancing.

        Returns:
            A new stage with rebalanced items.
        """
        new_stage: Stage[T] = Stage(
            self._pipeline,
            f"{self._name}-rebalance",
            self,
        )
        new_stage._transforms = self._transforms + [("rebalance", key_fn)]
        self._pipeline._add_stage(new_stage)
        return new_stage

    def merge(self, other: "Stage[T]") -> "Stage[T]":
        """Merge this stage with another stage.

        Args:
            other: The other stage to merge with.

        Returns:
            A new stage containing items from both stages.
        """
        new_stage: Stage[T] = Stage(
            self._pipeline,
            f"{self._name}-merge",
            self,
        )
        new_stage._transforms = self._transforms + [("merge", other)]
        self._pipeline._add_stage(new_stage)
        return new_stage

    def hash_join(
        self,
        other: "Stage",
        this_key_fn: Callable[[T], Any],
        other_key_fn: Callable[[Any], Any],
    ) -> "Stage[Tuple[T, Any]]":
        """Hash join this stage with another stage.

        Args:
            other: The stage to join with.
            this_key_fn: Key extractor for this stage.
            other_key_fn: Key extractor for the other stage.

        Returns:
            A new stage with joined items as tuples.
        """
        new_stage: Stage[Tuple[T, Any]] = Stage(
            self._pipeline,
            f"{self._name}-hashjoin",
            self,
        )
        new_stage._transforms = self._transforms + [
            ("hash_join", (other, this_key_fn, other_key_fn))
        ]
        self._pipeline._add_stage(new_stage)
        return new_stage

    def apply(self, transform_fn: Callable[["Stage[T]"], "Stage[R]"]) -> "Stage[R]":
        """Apply a custom transformation function to this stage.

        Args:
            transform_fn: Function that takes this stage and returns a new stage.

        Returns:
            The stage returned by the transform function.
        """
        return transform_fn(self)

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

    @staticmethod
    def to_file(name: str, directory: str) -> FileSink:
        """Create a sink that writes to files.

        Args:
            name: Sink name.
            directory: Output directory.

        Returns:
            A FileSink.
        """
        return FileSink(name, directory)

    @staticmethod
    def to_socket(name: str, host: str, port: int) -> SocketSink:
        """Create a sink that writes to a socket.

        Args:
            name: Sink name.
            host: Target host.
            port: Target port.

        Returns:
            A SocketSink.
        """
        return SocketSink(name, host, port)

    @staticmethod
    def to_kafka(
        name: str,
        topic: str,
        bootstrap_servers: str,
    ) -> KafkaSink:
        """Create a sink that writes to Kafka.

        Args:
            name: Sink name.
            topic: Kafka topic.
            bootstrap_servers: Bootstrap server addresses.

        Returns:
            A KafkaSink.
        """
        return KafkaSink(name, topic, bootstrap_servers)

    @staticmethod
    def from_file(
        name: str,
        directory: str,
        glob: str = "*",
    ) -> FileSource:
        """Create a source that reads from files.

        Args:
            name: Source name.
            directory: Source directory.
            glob: File pattern.

        Returns:
            A FileSource.
        """
        return FileSource(name, directory, glob)

    @staticmethod
    def from_socket(name: str, host: str, port: int) -> SocketSource:
        """Create a source that reads from a socket.

        Args:
            name: Source name.
            host: Source host.
            port: Source port.

        Returns:
            A SocketSource.
        """
        return SocketSource(name, host, port)

    @staticmethod
    def from_kafka(
        name: str,
        topic: str,
        bootstrap_servers: str,
        group_id: Optional[str] = None,
    ) -> KafkaSource:
        """Create a source that reads from Kafka.

        Args:
            name: Source name.
            topic: Kafka topic.
            bootstrap_servers: Bootstrap server addresses.
            group_id: Consumer group ID.

        Returns:
            A KafkaSource.
        """
        return KafkaSource(name, topic, bootstrap_servers, group_id)

    @staticmethod
    def noop() -> NoopSink:
        """Create a sink that discards all items.

        Returns:
            A NoopSink.
        """
        return NoopSink()

    @staticmethod
    def test_source(
        name: str,
        items_per_second: int = 10,
    ) -> TestSource:
        """Create a test source that generates items.

        Args:
            name: Source name.
            items_per_second: Rate of item generation.

        Returns:
            A TestSource.
        """
        return TestSource(name, items_per_second)

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
