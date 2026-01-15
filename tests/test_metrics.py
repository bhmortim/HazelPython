"""Unit tests for hazelcast.metrics module."""

import pytest
import time

from hazelcast.metrics import (
    GaugeMetric,
    CounterMetric,
    TimerMetric,
    ConnectionMetrics,
    InvocationMetrics,
    NearCacheMetrics,
    MetricsRegistry,
)


class TestGaugeMetric:
    """Tests for GaugeMetric class."""

    def test_init(self):
        gauge = GaugeMetric(
            name="test.gauge",
            value_fn=lambda: 42,
            unit="count",
            description="Test gauge",
        )
        assert gauge.name == "test.gauge"
        assert gauge.unit == "count"
        assert gauge.description == "Test gauge"

    def test_get_value(self):
        gauge = GaugeMetric(name="test", value_fn=lambda: 100)
        assert gauge.get_value() == 100

    def test_get_value_exception(self):
        def bad_fn():
            raise RuntimeError("error")

        gauge = GaugeMetric(name="test", value_fn=bad_fn)
        assert gauge.get_value() == 0.0


class TestCounterMetric:
    """Tests for CounterMetric class."""

    def test_init(self):
        counter = CounterMetric(name="test.counter")
        assert counter.name == "test.counter"
        assert counter.value == 0.0

    def test_increment(self):
        counter = CounterMetric(name="test")
        counter.increment()
        assert counter.value == 1.0
        counter.increment(5.0)
        assert counter.value == 6.0

    def test_get_value(self):
        counter = CounterMetric(name="test", value=10.0)
        assert counter.get_value() == 10.0


class TestTimerMetric:
    """Tests for TimerMetric class."""

    def test_init(self):
        timer = TimerMetric(name="test.timer")
        assert timer.name == "test.timer"
        assert timer.count == 0
        assert timer.total_time == 0.0

    def test_record(self):
        timer = TimerMetric(name="test")
        timer.record(10.0)
        timer.record(20.0)
        assert timer.count == 2
        assert timer.total_time == 30.0
        assert timer.min_time == 10.0
        assert timer.max_time == 20.0

    def test_get_average(self):
        timer = TimerMetric(name="test")
        timer.record(10.0)
        timer.record(30.0)
        assert timer.get_average() == 20.0

    def test_get_average_no_recordings(self):
        timer = TimerMetric(name="test")
        assert timer.get_average() == 0.0


class TestConnectionMetrics:
    """Tests for ConnectionMetrics class."""

    def test_default_values(self):
        metrics = ConnectionMetrics()
        assert metrics.active_connections == 0
        assert metrics.total_connections_opened == 0
        assert metrics.total_connections_closed == 0
        assert metrics.connection_attempts == 0
        assert metrics.failed_connection_attempts == 0


class TestInvocationMetrics:
    """Tests for InvocationMetrics class."""

    def test_default_values(self):
        metrics = InvocationMetrics()
        assert metrics.total_invocations == 0
        assert metrics.pending_invocations == 0
        assert metrics.completed_invocations == 0

    def test_record_invocation(self):
        metrics = InvocationMetrics()
        metrics.record_invocation(100.0, success=True)
        assert metrics.total_invocations == 1
        assert metrics.completed_invocations == 1
        assert metrics.failed_invocations == 0
        assert metrics.total_response_time == 100.0

    def test_record_invocation_failure(self):
        metrics = InvocationMetrics()
        metrics.record_invocation(50.0, success=False)
        assert metrics.failed_invocations == 1

    def test_average_response_time(self):
        metrics = InvocationMetrics()
        metrics.record_invocation(100.0)
        metrics.record_invocation(200.0)
        assert metrics.average_response_time == 150.0

    def test_average_response_time_no_completed(self):
        metrics = InvocationMetrics()
        assert metrics.average_response_time == 0.0


class TestNearCacheMetrics:
    """Tests for NearCacheMetrics class."""

    def test_init(self):
        metrics = NearCacheMetrics(name="test-map")
        assert metrics.name == "test-map"
        assert metrics.hits == 0
        assert metrics.misses == 0

    def test_hit_ratio(self):
        metrics = NearCacheMetrics(name="test", hits=80, misses=20)
        assert metrics.hit_ratio == 0.8

    def test_hit_ratio_no_accesses(self):
        metrics = NearCacheMetrics(name="test")
        assert metrics.hit_ratio == 0.0

    def test_miss_ratio(self):
        metrics = NearCacheMetrics(name="test", hits=80, misses=20)
        assert metrics.miss_ratio == 0.2


class TestMetricsRegistry:
    """Tests for MetricsRegistry class."""

    def test_init(self):
        registry = MetricsRegistry()
        assert registry.uptime_seconds >= 0

    def test_register_gauge(self):
        registry = MetricsRegistry()
        gauge = registry.register_gauge("test.gauge", lambda: 42)
        assert gauge.name == "test.gauge"
        assert registry.get_gauge("test.gauge") is gauge

    def test_register_counter(self):
        registry = MetricsRegistry()
        counter = registry.register_counter("test.counter")
        assert counter.name == "test.counter"
        assert registry.get_counter("test.counter") is counter

    def test_register_timer(self):
        registry = MetricsRegistry()
        timer = registry.register_timer("test.timer")
        assert timer.name == "test.timer"
        assert registry.get_timer("test.timer") is timer

    def test_get_nonexistent(self):
        registry = MetricsRegistry()
        assert registry.get_gauge("nonexistent") is None
        assert registry.get_counter("nonexistent") is None
        assert registry.get_timer("nonexistent") is None

    def test_get_or_create_near_cache_metrics(self):
        registry = MetricsRegistry()
        metrics = registry.get_or_create_near_cache_metrics("test-map")
        assert metrics.name == "test-map"
        same_metrics = registry.get_or_create_near_cache_metrics("test-map")
        assert metrics is same_metrics

    def test_get_near_cache_metrics(self):
        registry = MetricsRegistry()
        registry.get_or_create_near_cache_metrics("test-map")
        metrics = registry.get_near_cache_metrics("test-map")
        assert metrics is not None
        assert registry.get_near_cache_metrics("nonexistent") is None

    def test_get_all_near_cache_metrics(self):
        registry = MetricsRegistry()
        registry.get_or_create_near_cache_metrics("map1")
        registry.get_or_create_near_cache_metrics("map2")
        all_metrics = registry.get_all_near_cache_metrics()
        assert "map1" in all_metrics
        assert "map2" in all_metrics

    def test_record_connection_opened(self):
        registry = MetricsRegistry()
        registry.record_connection_opened()
        assert registry.connection_metrics.total_connections_opened == 1
        assert registry.connection_metrics.active_connections == 1

    def test_record_connection_closed(self):
        registry = MetricsRegistry()
        registry.record_connection_opened()
        registry.record_connection_closed()
        assert registry.connection_metrics.total_connections_closed == 1
        assert registry.connection_metrics.active_connections == 0

    def test_record_connection_closed_no_underflow(self):
        registry = MetricsRegistry()
        registry.record_connection_closed()
        assert registry.connection_metrics.active_connections == 0

    def test_record_connection_attempt(self):
        registry = MetricsRegistry()
        registry.record_connection_attempt(success=True)
        registry.record_connection_attempt(success=False)
        assert registry.connection_metrics.connection_attempts == 2
        assert registry.connection_metrics.failed_connection_attempts == 1

    def test_record_invocation_start(self):
        registry = MetricsRegistry()
        registry.record_invocation_start()
        assert registry.invocation_metrics.total_invocations == 1
        assert registry.invocation_metrics.pending_invocations == 1

    def test_record_invocation_end(self):
        registry = MetricsRegistry()
        registry.record_invocation_start()
        registry.record_invocation_end(100.0, success=True)
        assert registry.invocation_metrics.pending_invocations == 0
        assert registry.invocation_metrics.completed_invocations == 1

    def test_record_invocation_end_timeout(self):
        registry = MetricsRegistry()
        registry.record_invocation_start()
        registry.record_invocation_end(100.0, success=False, timed_out=True)
        assert registry.invocation_metrics.timed_out_invocations == 1

    def test_record_invocation_retry(self):
        registry = MetricsRegistry()
        registry.record_invocation_retry()
        assert registry.invocation_metrics.retried_invocations == 1

    def test_record_near_cache_hit(self):
        registry = MetricsRegistry()
        registry.record_near_cache_hit("test-map")
        metrics = registry.get_near_cache_metrics("test-map")
        assert metrics.hits == 1

    def test_record_near_cache_miss(self):
        registry = MetricsRegistry()
        registry.record_near_cache_miss("test-map")
        metrics = registry.get_near_cache_metrics("test-map")
        assert metrics.misses == 1

    def test_record_near_cache_eviction(self):
        registry = MetricsRegistry()
        registry.record_near_cache_eviction("test-map")
        metrics = registry.get_near_cache_metrics("test-map")
        assert metrics.evictions == 1

    def test_record_near_cache_invalidation(self):
        registry = MetricsRegistry()
        registry.record_near_cache_invalidation("test-map")
        metrics = registry.get_near_cache_metrics("test-map")
        assert metrics.invalidations == 1

    def test_to_dict(self):
        registry = MetricsRegistry()
        registry.record_connection_opened()
        registry.record_invocation_start()
        registry.record_invocation_end(100.0)
        d = registry.to_dict()
        assert "uptime_seconds" in d
        assert "connections" in d
        assert "invocations" in d
        assert "near_caches" in d
        assert "gauges" in d
        assert "counters" in d

    def test_reset(self):
        registry = MetricsRegistry()
        registry.record_connection_opened()
        registry.register_counter("test")
        registry.reset()
        assert registry.connection_metrics.active_connections == 0
        assert registry.get_counter("test") is None

    def test_repr(self):
        registry = MetricsRegistry()
        r = repr(registry)
        assert "MetricsRegistry" in r
