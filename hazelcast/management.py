"""Management Center integration for Hazelcast client.

This module provides client statistics collection and reporting to
Hazelcast Management Center (MC). It enables monitoring of client
health, performance, and resource usage through the MC dashboard.

The ``ManagementCenterService`` periodically collects statistics from
various client components and publishes them to the cluster, which
forwards them to Management Center.

Example:
    Statistics are enabled via configuration::

        from hazelcast import HazelcastClient, ClientConfig

        config = ClientConfig()
        config.statistics.enabled = True
        config.statistics.period_seconds = 5.0

        client = HazelcastClient(config)
        # Statistics are automatically published every 5 seconds

    Accessing statistics programmatically::

        service = client.get_management_center_service()
        stats = service.collect_statistics()
        print(f"Active connections: {stats.connections_active}")
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from hazelcast.config import StatisticsConfig
    from hazelcast.metrics import MetricsRegistry


@dataclass
class ClientStatistics:
    """Snapshot of client statistics at a point in time.

    Contains all statistics collected from the client including
    connection metrics, invocation metrics, near cache statistics,
    and custom metrics.

    Attributes:
        timestamp: Unix timestamp when statistics were collected.
        client_name: Name of the client instance.
        client_uuid: Unique identifier of the client.
        cluster_name: Name of the cluster the client is connected to.
        uptime_seconds: Time since client started in seconds.
        connections_active: Current number of active connections.
        connections_opened: Total connections opened since start.
        connections_closed: Total connections closed since start.
        invocations_total: Total invocations started.
        invocations_pending: Currently pending invocations.
        invocations_completed: Total completed invocations.
        invocations_failed: Number of failed invocations.
        invocations_retried: Number of retried invocations.
        average_response_time_ms: Average response time in milliseconds.
        near_cache_stats: Statistics for each near cache by name.
        custom_metrics: Custom gauge and counter values.

    Example:
        >>> stats = service.collect_statistics()
        >>> print(f"Uptime: {stats.uptime_seconds:.1f}s")
        >>> print(f"Pending invocations: {stats.invocations_pending}")
    """

    timestamp: float = 0.0
    client_name: str = ""
    client_uuid: str = ""
    cluster_name: str = ""
    uptime_seconds: float = 0.0

    connections_active: int = 0
    connections_opened: int = 0
    connections_closed: int = 0
    connection_attempts: int = 0
    failed_connection_attempts: int = 0

    invocations_total: int = 0
    invocations_pending: int = 0
    invocations_completed: int = 0
    invocations_failed: int = 0
    invocations_timed_out: int = 0
    invocations_retried: int = 0
    average_response_time_ms: float = 0.0
    min_response_time_ms: float = 0.0
    max_response_time_ms: float = 0.0

    near_cache_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    custom_metrics: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert statistics to a dictionary.

        Returns:
            Dictionary representation suitable for JSON serialization.

        Example:
            >>> stats_dict = stats.to_dict()
            >>> import json
            >>> print(json.dumps(stats_dict, indent=2))
        """
        return {
            "timestamp": self.timestamp,
            "client": {
                "name": self.client_name,
                "uuid": self.client_uuid,
                "cluster_name": self.cluster_name,
                "uptime_seconds": self.uptime_seconds,
            },
            "connections": {
                "active": self.connections_active,
                "opened": self.connections_opened,
                "closed": self.connections_closed,
                "attempts": self.connection_attempts,
                "failed_attempts": self.failed_connection_attempts,
            },
            "invocations": {
                "total": self.invocations_total,
                "pending": self.invocations_pending,
                "completed": self.invocations_completed,
                "failed": self.invocations_failed,
                "timed_out": self.invocations_timed_out,
                "retried": self.invocations_retried,
                "average_response_time_ms": self.average_response_time_ms,
                "min_response_time_ms": self.min_response_time_ms,
                "max_response_time_ms": self.max_response_time_ms,
            },
            "near_caches": self.near_cache_stats,
            "custom_metrics": self.custom_metrics,
        }

    def to_key_value_string(self) -> str:
        """Convert statistics to key=value format for MC protocol.

        Returns:
            String in key=value format separated by commas.

        Example:
            >>> kv_str = stats.to_key_value_string()
            >>> # "client.name=hz.client_abc,client.uuid=..."
        """
        pairs = [
            f"timestamp={int(self.timestamp * 1000)}",
            f"client.name={self.client_name}",
            f"client.uuid={self.client_uuid}",
            f"clusterName={self.cluster_name}",
            f"client.uptime={int(self.uptime_seconds * 1000)}",
            f"os.totalPhysicalMemorySize={_get_memory_info()}",
            f"runtime.availableProcessors={_get_cpu_count()}",
            f"connections.active={self.connections_active}",
            f"connections.opened={self.connections_opened}",
            f"connections.closed={self.connections_closed}",
            f"invocations.total={self.invocations_total}",
            f"invocations.pending={self.invocations_pending}",
            f"invocations.completed={self.invocations_completed}",
            f"invocations.failed={self.invocations_failed}",
            f"invocations.retried={self.invocations_retried}",
        ]

        for name, nc_stats in self.near_cache_stats.items():
            prefix = f"nc.{name}"
            pairs.append(f"{prefix}.hits={nc_stats.get('hits', 0)}")
            pairs.append(f"{prefix}.misses={nc_stats.get('misses', 0)}")
            pairs.append(f"{prefix}.entries={nc_stats.get('entries', 0)}")
            pairs.append(f"{prefix}.evictions={nc_stats.get('evictions', 0)}")

        for name, value in self.custom_metrics.items():
            pairs.append(f"custom.{name}={value}")

        return ",".join(pairs)


def _get_memory_info() -> int:
    """Get total physical memory in bytes."""
    try:
        import psutil
        return psutil.virtual_memory().total
    except ImportError:
        return 0


def _get_cpu_count() -> int:
    """Get number of available CPUs."""
    try:
        import os
        return os.cpu_count() or 1
    except Exception:
        return 1


class ManagementCenterService:
    """Service for collecting and publishing client statistics.

    Manages the lifecycle of statistics collection, including periodic
    publishing to Management Center through the Hazelcast cluster.

    The service runs a background thread that collects statistics at
    configurable intervals and sends them to the cluster.

    Attributes:
        config: The statistics configuration.
        running: Whether the service is currently running.
        last_published: Timestamp of last successful publish.

    Example:
        The service is typically accessed through the client::

            client = HazelcastClient(config)
            mc_service = client.get_management_center_service()

            # Check if statistics are being collected
            if mc_service.running:
                stats = mc_service.collect_statistics()
                print(f"Last published: {mc_service.last_published}")

        Manual control (advanced)::

            mc_service.start()
            # ... client operations ...
            mc_service.shutdown()
    """

    def __init__(
        self,
        config: "StatisticsConfig",
        metrics_registry: "MetricsRegistry",
        client_name: str = "",
        client_uuid: str = "",
        cluster_name: str = "",
        publish_callback: Optional[Callable[[ClientStatistics], bool]] = None,
    ):
        """Initialize the Management Center service.

        Args:
            config: Statistics configuration.
            metrics_registry: Registry containing all client metrics.
            client_name: Name of the client instance.
            client_uuid: UUID of the client instance.
            cluster_name: Name of the cluster.
            publish_callback: Optional callback for publishing statistics.
                If provided, called with ClientStatistics and should return
                True on success.
        """
        self._config = config
        self._metrics_registry = metrics_registry
        self._client_name = client_name
        self._client_uuid = client_uuid
        self._cluster_name = cluster_name
        self._publish_callback = publish_callback

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        self._last_published: Optional[float] = None
        self._publish_count = 0
        self._publish_failures = 0

        self._listeners: List[Callable[[ClientStatistics], None]] = []

    @property
    def config(self) -> "StatisticsConfig":
        """Get the statistics configuration."""
        return self._config

    @property
    def running(self) -> bool:
        """Check if the service is running."""
        with self._lock:
            return self._running

    @property
    def last_published(self) -> Optional[float]:
        """Get the timestamp of last successful publish."""
        with self._lock:
            return self._last_published

    @property
    def publish_count(self) -> int:
        """Get the total number of successful publishes."""
        with self._lock:
            return self._publish_count

    @property
    def publish_failures(self) -> int:
        """Get the total number of failed publishes."""
        with self._lock:
            return self._publish_failures

    def add_statistics_listener(
        self,
        listener: Callable[[ClientStatistics], None],
    ) -> None:
        """Add a listener to receive statistics on each collection.

        The listener is called with the collected ClientStatistics
        each time statistics are published.

        Args:
            listener: Callable that receives ClientStatistics.

        Example:
            >>> def on_stats(stats):
            ...     print(f"Connections: {stats.connections_active}")
            >>> mc_service.add_statistics_listener(on_stats)
        """
        with self._lock:
            self._listeners.append(listener)

    def remove_statistics_listener(
        self,
        listener: Callable[[ClientStatistics], None],
    ) -> bool:
        """Remove a previously added statistics listener.

        Args:
            listener: The listener to remove.

        Returns:
            True if the listener was found and removed.
        """
        with self._lock:
            try:
                self._listeners.remove(listener)
                return True
            except ValueError:
                return False

    def start(self) -> None:
        """Start the statistics collection service.

        Begins periodic collection and publishing of statistics.
        Does nothing if statistics are disabled in configuration
        or the service is already running.

        Example:
            >>> mc_service.start()
            >>> assert mc_service.running
        """
        if not self._config.enabled:
            return

        with self._lock:
            if self._running:
                return

            self._running = True
            self._stop_event.clear()

        self._thread = threading.Thread(
            target=self._collection_loop,
            name="hz-statistics-collector",
            daemon=True,
        )
        self._thread.start()

    def shutdown(self) -> None:
        """Shutdown the statistics collection service.

        Stops the background collection thread and waits for it
        to terminate. Safe to call multiple times.

        Example:
            >>> mc_service.shutdown()
            >>> assert not mc_service.running
        """
        with self._lock:
            if not self._running:
                return
            self._running = False

        self._stop_event.set()

        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def collect_statistics(self) -> ClientStatistics:
        """Collect current client statistics.

        Gathers statistics from the metrics registry and returns
        a snapshot of all client metrics.

        Returns:
            ClientStatistics snapshot of current metrics.

        Example:
            >>> stats = mc_service.collect_statistics()
            >>> print(f"Active connections: {stats.connections_active}")
            >>> print(f"Pending invocations: {stats.invocations_pending}")
        """
        conn_metrics = self._metrics_registry.connection_metrics
        inv_metrics = self._metrics_registry.invocation_metrics

        near_cache_stats = {}
        for name, nc_metrics in self._metrics_registry.get_all_near_cache_metrics().items():
            near_cache_stats[name] = {
                "hits": nc_metrics.hits,
                "misses": nc_metrics.misses,
                "entries": nc_metrics.entries,
                "evictions": nc_metrics.evictions,
                "expirations": nc_metrics.expirations,
                "invalidations": nc_metrics.invalidations,
                "hit_ratio": nc_metrics.hit_ratio,
                "owned_entry_memory_cost": nc_metrics.owned_entry_memory_cost,
            }

        metrics_dict = self._metrics_registry.to_dict()
        custom_metrics = {}
        custom_metrics.update(metrics_dict.get("gauges", {}))
        custom_metrics.update(metrics_dict.get("counters", {}))

        min_response = inv_metrics.min_response_time
        if min_response == float("inf"):
            min_response = 0.0

        return ClientStatistics(
            timestamp=time.time(),
            client_name=self._client_name,
            client_uuid=self._client_uuid,
            cluster_name=self._cluster_name,
            uptime_seconds=self._metrics_registry.uptime_seconds,
            connections_active=conn_metrics.active_connections,
            connections_opened=conn_metrics.total_connections_opened,
            connections_closed=conn_metrics.total_connections_closed,
            connection_attempts=conn_metrics.connection_attempts,
            failed_connection_attempts=conn_metrics.failed_connection_attempts,
            invocations_total=inv_metrics.total_invocations,
            invocations_pending=inv_metrics.pending_invocations,
            invocations_completed=inv_metrics.completed_invocations,
            invocations_failed=inv_metrics.failed_invocations,
            invocations_timed_out=inv_metrics.timed_out_invocations,
            invocations_retried=inv_metrics.retried_invocations,
            average_response_time_ms=inv_metrics.average_response_time,
            min_response_time_ms=min_response,
            max_response_time_ms=inv_metrics.max_response_time,
            near_cache_stats=near_cache_stats,
            custom_metrics=custom_metrics,
        )

    def publish_statistics(self) -> bool:
        """Publish current statistics to Management Center.

        Collects statistics and sends them to the cluster for
        forwarding to Management Center.

        Returns:
            True if publish was successful, False otherwise.

        Example:
            >>> if mc_service.publish_statistics():
            ...     print("Statistics published successfully")
        """
        try:
            stats = self.collect_statistics()

            with self._lock:
                listeners = list(self._listeners)

            for listener in listeners:
                try:
                    listener(stats)
                except Exception:
                    pass

            success = True
            if self._publish_callback is not None:
                try:
                    success = self._publish_callback(stats)
                except Exception:
                    success = False

            with self._lock:
                if success:
                    self._last_published = time.time()
                    self._publish_count += 1
                else:
                    self._publish_failures += 1

            return success

        except Exception:
            with self._lock:
                self._publish_failures += 1
            return False

    def _collection_loop(self) -> None:
        """Background loop for periodic statistics collection."""
        period = self._config.period_seconds

        while not self._stop_event.is_set():
            try:
                self.publish_statistics()
            except Exception:
                pass

            self._stop_event.wait(timeout=period)

    def get_statistics_summary(self) -> Dict[str, Any]:
        """Get a summary of the statistics service status.

        Returns:
            Dictionary with service status and counters.

        Example:
            >>> summary = mc_service.get_statistics_summary()
            >>> print(f"Published {summary['publish_count']} times")
        """
        with self._lock:
            return {
                "enabled": self._config.enabled,
                "running": self._running,
                "period_seconds": self._config.period_seconds,
                "publish_count": self._publish_count,
                "publish_failures": self._publish_failures,
                "last_published": self._last_published,
            }

    def __repr__(self) -> str:
        return (
            f"ManagementCenterService(enabled={self._config.enabled}, "
            f"running={self._running}, publishes={self._publish_count})"
        )
