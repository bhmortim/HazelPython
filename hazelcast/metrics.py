"""Client metrics collection and reporting.

This module provides metrics collection infrastructure for monitoring
Hazelcast client performance and health. It supports various metric types
including gauges, counters, and timers, as well as specialized metrics
for connections, invocations, and near caches.

The central ``MetricsRegistry`` collects all metrics and can export them
in various formats for monitoring systems.

Example:
    Basic metrics usage::

        from hazelcast.metrics import MetricsRegistry

        registry = MetricsRegistry()

        # Record connection events
        registry.record_connection_opened()
        registry.record_connection_attempt(success=True)

        # Record invocations
        registry.record_invocation_start()
        registry.record_invocation_end(response_time=15.5, success=True)

        # Get all metrics as dictionary
        metrics = registry.to_dict()
        print(f"Active connections: {metrics['connections']['active']}")

    Custom gauges and counters::

        registry = MetricsRegistry()

        # Register a gauge that reads from a function
        gauge = registry.register_gauge(
            "memory.used",
            lambda: get_memory_usage(),
            unit="bytes"
        )

        # Register and use a counter
        counter = registry.register_counter("requests.total")
        counter.increment()
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
import threading
import time


@dataclass
class GaugeMetric:
    """A gauge metric that represents a current value.

    Gauges represent point-in-time values that can increase or decrease,
    such as memory usage, active connections, or queue size.

    Attributes:
        name: The metric name (e.g., "connections.active").
        value_fn: A callable that returns the current metric value.
        unit: Optional unit of measurement (e.g., "bytes", "ms").
        description: Optional human-readable description.

    Example:
        >>> gauge = GaugeMetric(
        ...     name="queue.size",
        ...     value_fn=lambda: len(my_queue),
        ...     unit="items"
        ... )
        >>> gauge.get_value()
        42
    """

    name: str
    value_fn: Callable[[], float]
    unit: str = ""
    description: str = ""

    def get_value(self) -> float:
        """Get the current metric value.

        Invokes the value function and returns its result. Returns 0.0
        if the value function raises an exception.

        Returns:
            The current metric value.
        """
        try:
            return self.value_fn()
        except Exception:
            return 0.0


@dataclass
class CounterMetric:
    """A counter metric that tracks cumulative values.

    Counters represent monotonically increasing values such as total
    requests, bytes transferred, or errors encountered.

    Attributes:
        name: The metric name (e.g., "requests.total").
        value: The current counter value.
        unit: Optional unit of measurement.
        description: Optional human-readable description.

    Example:
        >>> counter = CounterMetric(name="requests.total")
        >>> counter.increment()
        >>> counter.increment(5)
        >>> counter.get_value()
        6.0
    """

    name: str
    value: float = 0.0
    unit: str = ""
    description: str = ""

    def increment(self, amount: float = 1.0) -> None:
        """Increment the counter by the specified amount.

        Args:
            amount: The value to add to the counter. Defaults to 1.0.
        """
        self.value += amount

    def get_value(self) -> float:
        """Get the current counter value.

        Returns:
            The current cumulative value.
        """
        return self.value


@dataclass
class TimerMetric:
    """A timer metric that tracks durations.

    Timers track the duration of operations, maintaining count, total,
    minimum, maximum, and average values.

    Attributes:
        name: The metric name (e.g., "operation.duration").
        count: Number of recorded durations.
        total_time: Sum of all recorded durations.
        min_time: Minimum recorded duration.
        max_time: Maximum recorded duration.
        unit: Unit of measurement. Defaults to "ms" (milliseconds).
        description: Optional human-readable description.

    Example:
        >>> timer = TimerMetric(name="operation.duration")
        >>> timer.record(15.5)
        >>> timer.record(20.3)
        >>> timer.get_average()
        17.9
    """

    name: str
    count: int = 0
    total_time: float = 0.0
    min_time: float = float("inf")
    max_time: float = 0.0
    unit: str = "ms"
    description: str = ""

    def record(self, duration: float) -> None:
        """Record a duration measurement.

        Updates count, total, min, and max statistics.

        Args:
            duration: The duration to record, in the configured unit.
        """
        self.count += 1
        self.total_time += duration
        self.min_time = min(self.min_time, duration)
        self.max_time = max(self.max_time, duration)

    def get_average(self) -> float:
        """Get the average duration across all recordings.

        Returns:
            Average duration, or 0.0 if no recordings.
        """
        return self.total_time / self.count if self.count > 0 else 0.0


@dataclass
class ConnectionMetrics:
    """Metrics related to client connections.

    Tracks connection lifecycle events including opens, closes,
    and connection attempts.

    Attributes:
        active_connections: Current number of active connections.
        total_connections_opened: Total connections opened since start.
        total_connections_closed: Total connections closed since start.
        connection_attempts: Total connection attempts.
        failed_connection_attempts: Number of failed connection attempts.
        last_connection_time: Timestamp of last successful connection.
    """

    active_connections: int = 0
    total_connections_opened: int = 0
    total_connections_closed: int = 0
    connection_attempts: int = 0
    failed_connection_attempts: int = 0
    last_connection_time: Optional[float] = None


@dataclass
class InvocationMetrics:
    """Metrics related to invocations/operations.

    Tracks operation invocations including counts, response times,
    and failure rates.

    Attributes:
        total_invocations: Total invocations started.
        pending_invocations: Currently pending invocations.
        completed_invocations: Total completed invocations.
        failed_invocations: Number of failed invocations.
        timed_out_invocations: Number of timed-out invocations.
        retried_invocations: Number of retried invocations.
        total_response_time: Sum of all response times in milliseconds.
        min_response_time: Minimum response time observed.
        max_response_time: Maximum response time observed.
    """

    total_invocations: int = 0
    pending_invocations: int = 0
    completed_invocations: int = 0
    failed_invocations: int = 0
    timed_out_invocations: int = 0
    retried_invocations: int = 0
    total_response_time: float = 0.0
    min_response_time: float = float("inf")
    max_response_time: float = 0.0

    def record_invocation(self, response_time: float, success: bool = True) -> None:
        """Record an invocation result.

        Args:
            response_time: The response time in milliseconds.
            success: Whether the invocation succeeded.
        """
        self.total_invocations += 1
        self.completed_invocations += 1
        self.total_response_time += response_time
        self.min_response_time = min(self.min_response_time, response_time)
        self.max_response_time = max(self.max_response_time, response_time)

        if not success:
            self.failed_invocations += 1

    @property
    def average_response_time(self) -> float:
        """Get the average response time in milliseconds.

        Returns:
            Average response time, or 0.0 if no completions.
        """
        if self.completed_invocations == 0:
            return 0.0
        return self.total_response_time / self.completed_invocations


@dataclass
class NearCacheMetrics:
    """Metrics for a single Near Cache.

    Tracks cache effectiveness including hits, misses, evictions,
    and memory usage for a specific near cache.

    Attributes:
        name: The near cache name.
        hits: Number of cache hits.
        misses: Number of cache misses.
        entries: Current number of entries in cache.
        evictions: Number of entries evicted.
        expirations: Number of entries expired.
        invalidations: Number of entries invalidated.
        owned_entry_memory_cost: Estimated memory cost of cached entries.
    """

    name: str
    hits: int = 0
    misses: int = 0
    entries: int = 0
    evictions: int = 0
    expirations: int = 0
    invalidations: int = 0
    owned_entry_memory_cost: int = 0

    @property
    def hit_ratio(self) -> float:
        """Calculate the cache hit ratio.

        Returns:
            Ratio of hits to total accesses (0.0 to 1.0).
        """
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

    @property
    def miss_ratio(self) -> float:
        """Calculate the cache miss ratio.

        Returns:
            Ratio of misses to total accesses (0.0 to 1.0).
        """
        return 1.0 - self.hit_ratio


class MetricsRegistry:
    """Central registry for all client metrics.

    Collects and manages all metrics for the Hazelcast client including
    connection metrics, invocation metrics, near cache metrics, and
    custom gauges/counters/timers.

    The registry is thread-safe and provides methods for recording
    events and exporting metrics data.

    Attributes:
        uptime_seconds: Time since the registry was created.
        connection_metrics: Connection-related metrics.
        invocation_metrics: Invocation-related metrics.

    Example:
        >>> registry = MetricsRegistry()
        >>> registry.record_connection_opened()
        >>> registry.record_invocation_start()
        >>> registry.record_invocation_end(15.5, success=True)
        >>> print(registry.to_dict())
    """

    def __init__(self):
        """Initialize the metrics registry.

        Creates a new registry with empty metrics collections and
        records the start time for uptime calculation.
        """
        self._gauges: Dict[str, GaugeMetric] = {}
        self._counters: Dict[str, CounterMetric] = {}
        self._timers: Dict[str, TimerMetric] = {}
        self._connection_metrics = ConnectionMetrics()
        self._invocation_metrics = InvocationMetrics()
        self._near_cache_metrics: Dict[str, NearCacheMetrics] = {}
        self._lock = threading.RLock()
        self._start_time = time.time()

    @property
    def uptime_seconds(self) -> float:
        """Get client uptime in seconds."""
        return time.time() - self._start_time

    @property
    def connection_metrics(self) -> ConnectionMetrics:
        """Get connection metrics."""
        return self._connection_metrics

    @property
    def invocation_metrics(self) -> InvocationMetrics:
        """Get invocation metrics."""
        return self._invocation_metrics

    def register_gauge(
        self,
        name: str,
        value_fn: Callable[[], float],
        unit: str = "",
        description: str = "",
    ) -> GaugeMetric:
        """Register a gauge metric.

        Creates a new gauge that reads its value from the provided function.
        Gauges are useful for measuring values that can go up or down.

        Args:
            name: Unique metric name (e.g., "memory.heap.used").
            value_fn: Callable that returns the current metric value.
            unit: Optional unit of measurement (e.g., "bytes", "count").
            description: Optional human-readable description.

        Returns:
            The registered GaugeMetric instance.

        Example:
            >>> def get_queue_size():
            ...     return len(my_queue)
            >>> gauge = registry.register_gauge(
            ...     "queue.size",
            ...     get_queue_size,
            ...     unit="items",
            ...     description="Number of items in queue"
            ... )
        """
        with self._lock:
            gauge = GaugeMetric(name, value_fn, unit, description)
            self._gauges[name] = gauge
            return gauge

    def register_counter(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> CounterMetric:
        """Register a counter metric.

        Creates a new counter for tracking cumulative, monotonically
        increasing values.

        Args:
            name: Unique metric name (e.g., "requests.total").
            unit: Optional unit of measurement.
            description: Optional human-readable description.

        Returns:
            The registered CounterMetric instance.

        Example:
            >>> errors = registry.register_counter(
            ...     "errors.total",
            ...     description="Total errors encountered"
            ... )
            >>> errors.increment()
        """
        with self._lock:
            counter = CounterMetric(name, unit=unit, description=description)
            self._counters[name] = counter
            return counter

    def register_timer(
        self,
        name: str,
        unit: str = "ms",
        description: str = "",
    ) -> TimerMetric:
        """Register a timer metric.

        Creates a new timer for tracking duration statistics including
        count, total, min, max, and average.

        Args:
            name: Unique metric name (e.g., "operation.duration").
            unit: Unit of measurement. Defaults to "ms" (milliseconds).
            description: Optional human-readable description.

        Returns:
            The registered TimerMetric instance.

        Example:
            >>> timer = registry.register_timer("db.query.duration")
            >>> start = time.time()
            >>> # ... perform operation ...
            >>> timer.record((time.time() - start) * 1000)
        """
        with self._lock:
            timer = TimerMetric(name, unit=unit, description=description)
            self._timers[name] = timer
            return timer

    def get_gauge(self, name: str) -> Optional[GaugeMetric]:
        """Get a gauge metric by name.

        Args:
            name: The metric name.

        Returns:
            The gauge metric, or ``None`` if not found.
        """
        with self._lock:
            return self._gauges.get(name)

    def get_counter(self, name: str) -> Optional[CounterMetric]:
        """Get a counter metric by name.

        Args:
            name: The metric name.

        Returns:
            The counter metric, or ``None`` if not found.
        """
        with self._lock:
            return self._counters.get(name)

    def get_timer(self, name: str) -> Optional[TimerMetric]:
        """Get a timer metric by name.

        Args:
            name: The metric name.

        Returns:
            The timer metric, or ``None`` if not found.
        """
        with self._lock:
            return self._timers.get(name)

    def get_or_create_near_cache_metrics(self, name: str) -> NearCacheMetrics:
        """Get or create metrics for a Near Cache.

        Creates a new NearCacheMetrics instance if one doesn't exist
        for the given name.

        Args:
            name: The Near Cache name.

        Returns:
            NearCacheMetrics instance for the cache.
        """
        with self._lock:
            if name not in self._near_cache_metrics:
                self._near_cache_metrics[name] = NearCacheMetrics(name)
            return self._near_cache_metrics[name]

    def get_near_cache_metrics(self, name: str) -> Optional[NearCacheMetrics]:
        """Get metrics for a Near Cache.

        Args:
            name: The Near Cache name.

        Returns:
            NearCacheMetrics instance, or ``None`` if not found.
        """
        with self._lock:
            return self._near_cache_metrics.get(name)

    def get_all_near_cache_metrics(self) -> Dict[str, NearCacheMetrics]:
        """Get metrics for all Near Caches.

        Returns:
            Dictionary mapping cache names to their metrics.
        """
        with self._lock:
            return dict(self._near_cache_metrics)

    def record_connection_opened(self) -> None:
        """Record a connection being opened.

        Increments active connections, total opened count, and updates
        the last connection timestamp.
        """
        with self._lock:
            self._connection_metrics.total_connections_opened += 1
            self._connection_metrics.active_connections += 1
            self._connection_metrics.last_connection_time = time.time()

    def record_connection_closed(self) -> None:
        """Record a connection being closed.

        Increments total closed count and decrements active connections.
        """
        with self._lock:
            self._connection_metrics.total_connections_closed += 1
            self._connection_metrics.active_connections = max(
                0, self._connection_metrics.active_connections - 1
            )

    def record_connection_attempt(self, success: bool = True) -> None:
        """Record a connection attempt.

        Args:
            success: Whether the connection attempt succeeded.
        """
        with self._lock:
            self._connection_metrics.connection_attempts += 1
            if not success:
                self._connection_metrics.failed_connection_attempts += 1

    def record_invocation_start(self) -> None:
        """Record an invocation starting.

        Increments total invocations and pending invocations count.
        """
        with self._lock:
            self._invocation_metrics.total_invocations += 1
            self._invocation_metrics.pending_invocations += 1

    def record_invocation_end(
        self,
        response_time: float,
        success: bool = True,
        timed_out: bool = False,
    ) -> None:
        """Record an invocation completing.

        Updates completion count, response time statistics, and
        failure/timeout counts as appropriate.

        Args:
            response_time: Response time in milliseconds.
            success: Whether the invocation succeeded. Defaults to ``True``.
            timed_out: Whether the invocation timed out. Defaults to ``False``.
        """
        with self._lock:
            self._invocation_metrics.pending_invocations = max(
                0, self._invocation_metrics.pending_invocations - 1
            )
            self._invocation_metrics.completed_invocations += 1
            self._invocation_metrics.total_response_time += response_time

            if response_time < self._invocation_metrics.min_response_time:
                self._invocation_metrics.min_response_time = response_time
            if response_time > self._invocation_metrics.max_response_time:
                self._invocation_metrics.max_response_time = response_time

            if not success:
                self._invocation_metrics.failed_invocations += 1
            if timed_out:
                self._invocation_metrics.timed_out_invocations += 1

    def record_invocation_retry(self) -> None:
        """Record an invocation being retried.

        Increments the retried invocations count.
        """
        with self._lock:
            self._invocation_metrics.retried_invocations += 1

    def record_near_cache_hit(self, cache_name: str) -> None:
        """Record a Near Cache hit.

        Args:
            cache_name: Name of the near cache.
        """
        metrics = self.get_or_create_near_cache_metrics(cache_name)
        metrics.hits += 1

    def record_near_cache_miss(self, cache_name: str) -> None:
        """Record a Near Cache miss.

        Args:
            cache_name: Name of the near cache.
        """
        metrics = self.get_or_create_near_cache_metrics(cache_name)
        metrics.misses += 1

    def record_near_cache_eviction(self, cache_name: str) -> None:
        """Record a Near Cache eviction.

        Args:
            cache_name: Name of the near cache.
        """
        metrics = self.get_or_create_near_cache_metrics(cache_name)
        metrics.evictions += 1

    def record_near_cache_invalidation(self, cache_name: str) -> None:
        """Record a Near Cache invalidation.

        Args:
            cache_name: Name of the near cache.
        """
        metrics = self.get_or_create_near_cache_metrics(cache_name)
        metrics.invalidations += 1

    def to_dict(self) -> Dict[str, Any]:
        """Export all metrics as a dictionary.

        Creates a comprehensive snapshot of all metrics suitable for
        JSON serialization or monitoring system integration.

        Returns:
            Dictionary containing all metric values organized by category.

        Example:
            >>> metrics = registry.to_dict()
            >>> print(metrics["connections"]["active"])
            3
            >>> print(metrics["invocations"]["average_response_time_ms"])
            15.5
        """
        with self._lock:
            result = {
                "uptime_seconds": self.uptime_seconds,
                "connections": {
                    "active": self._connection_metrics.active_connections,
                    "total_opened": self._connection_metrics.total_connections_opened,
                    "total_closed": self._connection_metrics.total_connections_closed,
                    "attempts": self._connection_metrics.connection_attempts,
                    "failed_attempts": self._connection_metrics.failed_connection_attempts,
                },
                "invocations": {
                    "total": self._invocation_metrics.total_invocations,
                    "pending": self._invocation_metrics.pending_invocations,
                    "completed": self._invocation_metrics.completed_invocations,
                    "failed": self._invocation_metrics.failed_invocations,
                    "timed_out": self._invocation_metrics.timed_out_invocations,
                    "retried": self._invocation_metrics.retried_invocations,
                    "average_response_time_ms": self._invocation_metrics.average_response_time,
                },
                "near_caches": {
                    name: {
                        "hits": m.hits,
                        "misses": m.misses,
                        "entries": m.entries,
                        "hit_ratio": m.hit_ratio,
                        "evictions": m.evictions,
                        "invalidations": m.invalidations,
                    }
                    for name, m in self._near_cache_metrics.items()
                },
                "gauges": {
                    name: g.get_value() for name, g in self._gauges.items()
                },
                "counters": {
                    name: c.get_value() for name, c in self._counters.items()
                },
            }
            return result

    def reset(self) -> None:
        """Reset all metrics to initial state.

        Clears all recorded metrics, custom gauges/counters/timers,
        and resets the start time. Useful for testing or periodic
        metric resets.
        """
        with self._lock:
            self._connection_metrics = ConnectionMetrics()
            self._invocation_metrics = InvocationMetrics()
            self._near_cache_metrics.clear()
            self._gauges.clear()
            self._counters.clear()
            self._timers.clear()
            self._start_time = time.time()

    def __repr__(self) -> str:
        return (
            f"MetricsRegistry(uptime={self.uptime_seconds:.1f}s, "
            f"connections={self._connection_metrics.active_connections})"
        )
