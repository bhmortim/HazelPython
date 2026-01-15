"""Client metrics collection and reporting."""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
import threading
import time


@dataclass
class GaugeMetric:
    """A gauge metric that represents a current value."""

    name: str
    value_fn: Callable[[], float]
    unit: str = ""
    description: str = ""

    def get_value(self) -> float:
        """Get the current value."""
        try:
            return self.value_fn()
        except Exception:
            return 0.0


@dataclass
class CounterMetric:
    """A counter metric that tracks cumulative values."""

    name: str
    value: float = 0.0
    unit: str = ""
    description: str = ""

    def increment(self, amount: float = 1.0) -> None:
        """Increment the counter."""
        self.value += amount

    def get_value(self) -> float:
        """Get the current value."""
        return self.value


@dataclass
class TimerMetric:
    """A timer metric that tracks durations."""

    name: str
    count: int = 0
    total_time: float = 0.0
    min_time: float = float("inf")
    max_time: float = 0.0
    unit: str = "ms"
    description: str = ""

    def record(self, duration: float) -> None:
        """Record a duration."""
        self.count += 1
        self.total_time += duration
        self.min_time = min(self.min_time, duration)
        self.max_time = max(self.max_time, duration)

    def get_average(self) -> float:
        """Get the average duration."""
        return self.total_time / self.count if self.count > 0 else 0.0


@dataclass
class ConnectionMetrics:
    """Metrics related to client connections."""

    active_connections: int = 0
    total_connections_opened: int = 0
    total_connections_closed: int = 0
    connection_attempts: int = 0
    failed_connection_attempts: int = 0
    last_connection_time: Optional[float] = None


@dataclass
class InvocationMetrics:
    """Metrics related to invocations/operations."""

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
        """Record an invocation result."""
        self.total_invocations += 1
        self.completed_invocations += 1
        self.total_response_time += response_time
        self.min_response_time = min(self.min_response_time, response_time)
        self.max_response_time = max(self.max_response_time, response_time)

        if not success:
            self.failed_invocations += 1

    @property
    def average_response_time(self) -> float:
        """Get the average response time."""
        if self.completed_invocations == 0:
            return 0.0
        return self.total_response_time / self.completed_invocations


@dataclass
class NearCacheMetrics:
    """Metrics for a single Near Cache."""

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
        """Calculate the cache hit ratio."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

    @property
    def miss_ratio(self) -> float:
        """Calculate the cache miss ratio."""
        return 1.0 - self.hit_ratio


class MetricsRegistry:
    """Central registry for all client metrics."""

    def __init__(self):
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

        Args:
            name: Metric name.
            value_fn: Function to get current value.
            unit: Unit of measurement.
            description: Description of the metric.

        Returns:
            The registered gauge.
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

        Args:
            name: Metric name.
            unit: Unit of measurement.
            description: Description of the metric.

        Returns:
            The registered counter.
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

        Args:
            name: Metric name.
            unit: Unit of measurement.
            description: Description of the metric.

        Returns:
            The registered timer.
        """
        with self._lock:
            timer = TimerMetric(name, unit=unit, description=description)
            self._timers[name] = timer
            return timer

    def get_gauge(self, name: str) -> Optional[GaugeMetric]:
        """Get a gauge by name."""
        with self._lock:
            return self._gauges.get(name)

    def get_counter(self, name: str) -> Optional[CounterMetric]:
        """Get a counter by name."""
        with self._lock:
            return self._counters.get(name)

    def get_timer(self, name: str) -> Optional[TimerMetric]:
        """Get a timer by name."""
        with self._lock:
            return self._timers.get(name)

    def get_or_create_near_cache_metrics(self, name: str) -> NearCacheMetrics:
        """Get or create metrics for a Near Cache.

        Args:
            name: Near Cache name.

        Returns:
            Near Cache metrics.
        """
        with self._lock:
            if name not in self._near_cache_metrics:
                self._near_cache_metrics[name] = NearCacheMetrics(name)
            return self._near_cache_metrics[name]

    def get_near_cache_metrics(self, name: str) -> Optional[NearCacheMetrics]:
        """Get metrics for a Near Cache.

        Args:
            name: Near Cache name.

        Returns:
            Near Cache metrics, or None if not found.
        """
        with self._lock:
            return self._near_cache_metrics.get(name)

    def get_all_near_cache_metrics(self) -> Dict[str, NearCacheMetrics]:
        """Get metrics for all Near Caches.

        Returns:
            Dictionary of name to metrics.
        """
        with self._lock:
            return dict(self._near_cache_metrics)

    def record_connection_opened(self) -> None:
        """Record a connection being opened."""
        with self._lock:
            self._connection_metrics.total_connections_opened += 1
            self._connection_metrics.active_connections += 1
            self._connection_metrics.last_connection_time = time.time()

    def record_connection_closed(self) -> None:
        """Record a connection being closed."""
        with self._lock:
            self._connection_metrics.total_connections_closed += 1
            self._connection_metrics.active_connections = max(
                0, self._connection_metrics.active_connections - 1
            )

    def record_connection_attempt(self, success: bool = True) -> None:
        """Record a connection attempt."""
        with self._lock:
            self._connection_metrics.connection_attempts += 1
            if not success:
                self._connection_metrics.failed_connection_attempts += 1

    def record_invocation_start(self) -> None:
        """Record an invocation starting."""
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

        Args:
            response_time: Response time in milliseconds.
            success: Whether the invocation succeeded.
            timed_out: Whether the invocation timed out.
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
        """Record an invocation being retried."""
        with self._lock:
            self._invocation_metrics.retried_invocations += 1

    def record_near_cache_hit(self, cache_name: str) -> None:
        """Record a Near Cache hit."""
        metrics = self.get_or_create_near_cache_metrics(cache_name)
        metrics.hits += 1

    def record_near_cache_miss(self, cache_name: str) -> None:
        """Record a Near Cache miss."""
        metrics = self.get_or_create_near_cache_metrics(cache_name)
        metrics.misses += 1

    def record_near_cache_eviction(self, cache_name: str) -> None:
        """Record a Near Cache eviction."""
        metrics = self.get_or_create_near_cache_metrics(cache_name)
        metrics.evictions += 1

    def record_near_cache_invalidation(self, cache_name: str) -> None:
        """Record a Near Cache invalidation."""
        metrics = self.get_or_create_near_cache_metrics(cache_name)
        metrics.invalidations += 1

    def to_dict(self) -> Dict[str, Any]:
        """Export all metrics as a dictionary.

        Returns:
            Dictionary representation of all metrics.
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
        """Reset all metrics."""
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
