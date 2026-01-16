"""Jet job management and configuration."""

from concurrent.futures import Future
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import threading
import time


class JobStatus(Enum):
    """Status of a Jet job."""

    NOT_RUNNING = "NOT_RUNNING"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    SUSPENDED = "SUSPENDED"
    SUSPENDING = "SUSPENDING"
    RESUMING = "RESUMING"
    COMPLETING = "COMPLETING"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"


class ProcessingGuarantee(Enum):
    """Processing guarantee for streaming jobs."""

    NONE = "NONE"
    AT_LEAST_ONCE = "AT_LEAST_ONCE"
    EXACTLY_ONCE = "EXACTLY_ONCE"


class JobConfig:
    """Configuration for a Jet job.

    Attributes:
        name: Optional job name for identification.
        auto_scaling_enabled: Whether to automatically scale the job.
        split_brain_protection_enabled: Whether to enable split-brain protection.
        snapshot_interval_millis: Interval between snapshots in milliseconds.
        processing_guarantee: Processing guarantee level.
        max_processor_accumulated_records: Max records accumulated per processor.
        timeout_millis: Job timeout in milliseconds (0 = no timeout).
        metrics_enabled: Whether to collect job metrics.
        store_metrics_after_job_completion: Whether to retain metrics after completion.
        suspend_on_failure: Whether to suspend job on failure instead of failing.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        auto_scaling_enabled: bool = True,
        split_brain_protection_enabled: bool = False,
        snapshot_interval_millis: int = 10000,
        processing_guarantee: ProcessingGuarantee = ProcessingGuarantee.NONE,
        max_processor_accumulated_records: int = -1,
        timeout_millis: int = 0,
        metrics_enabled: bool = True,
        store_metrics_after_job_completion: bool = False,
        suspend_on_failure: bool = False,
    ):
        self._name = name
        self._auto_scaling_enabled = auto_scaling_enabled
        self._split_brain_protection_enabled = split_brain_protection_enabled
        self._snapshot_interval_millis = snapshot_interval_millis
        self._processing_guarantee = processing_guarantee
        self._max_processor_accumulated_records = max_processor_accumulated_records
        self._initial_snapshot_name: Optional[str] = None
        self._timeout_millis = timeout_millis
        self._metrics_enabled = metrics_enabled
        self._store_metrics_after_job_completion = store_metrics_after_job_completion
        self._suspend_on_failure = suspend_on_failure
        self._arguments: Dict[str, Any] = {}

    @property
    def name(self) -> Optional[str]:
        """Get the job name."""
        return self._name

    @name.setter
    def name(self, value: Optional[str]) -> None:
        """Set the job name."""
        self._name = value

    @property
    def auto_scaling_enabled(self) -> bool:
        """Get whether auto-scaling is enabled."""
        return self._auto_scaling_enabled

    @auto_scaling_enabled.setter
    def auto_scaling_enabled(self, value: bool) -> None:
        """Set whether auto-scaling is enabled."""
        self._auto_scaling_enabled = value

    @property
    def split_brain_protection_enabled(self) -> bool:
        """Get whether split-brain protection is enabled."""
        return self._split_brain_protection_enabled

    @split_brain_protection_enabled.setter
    def split_brain_protection_enabled(self, value: bool) -> None:
        """Set whether split-brain protection is enabled."""
        self._split_brain_protection_enabled = value

    @property
    def snapshot_interval_millis(self) -> int:
        """Get the snapshot interval in milliseconds."""
        return self._snapshot_interval_millis

    @snapshot_interval_millis.setter
    def snapshot_interval_millis(self, value: int) -> None:
        """Set the snapshot interval in milliseconds."""
        self._snapshot_interval_millis = value

    @property
    def processing_guarantee(self) -> ProcessingGuarantee:
        """Get the processing guarantee level."""
        return self._processing_guarantee

    @processing_guarantee.setter
    def processing_guarantee(self, value: ProcessingGuarantee) -> None:
        """Set the processing guarantee level."""
        self._processing_guarantee = value

    @property
    def max_processor_accumulated_records(self) -> int:
        """Get max records accumulated per processor."""
        return self._max_processor_accumulated_records

    @max_processor_accumulated_records.setter
    def max_processor_accumulated_records(self, value: int) -> None:
        """Set max records accumulated per processor."""
        self._max_processor_accumulated_records = value

    @property
    def initial_snapshot_name(self) -> Optional[str]:
        """Get the initial snapshot name for job restart."""
        return self._initial_snapshot_name

    @initial_snapshot_name.setter
    def initial_snapshot_name(self, value: Optional[str]) -> None:
        """Set the initial snapshot name for job restart."""
        self._initial_snapshot_name = value

    @property
    def timeout_millis(self) -> int:
        """Get the job timeout in milliseconds."""
        return self._timeout_millis

    @timeout_millis.setter
    def timeout_millis(self, value: int) -> None:
        """Set the job timeout in milliseconds."""
        self._timeout_millis = value

    @property
    def metrics_enabled(self) -> bool:
        """Get whether metrics collection is enabled."""
        return self._metrics_enabled

    @metrics_enabled.setter
    def metrics_enabled(self, value: bool) -> None:
        """Set whether metrics collection is enabled."""
        self._metrics_enabled = value

    @property
    def store_metrics_after_job_completion(self) -> bool:
        """Get whether to store metrics after job completion."""
        return self._store_metrics_after_job_completion

    @store_metrics_after_job_completion.setter
    def store_metrics_after_job_completion(self, value: bool) -> None:
        """Set whether to store metrics after job completion."""
        self._store_metrics_after_job_completion = value

    @property
    def suspend_on_failure(self) -> bool:
        """Get whether to suspend on failure."""
        return self._suspend_on_failure

    @suspend_on_failure.setter
    def suspend_on_failure(self, value: bool) -> None:
        """Set whether to suspend on failure."""
        self._suspend_on_failure = value

    @property
    def arguments(self) -> Dict[str, Any]:
        """Get job arguments."""
        return self._arguments

    def set_argument(self, key: str, value: Any) -> "JobConfig":
        """Set a job argument.

        Args:
            key: Argument key.
            value: Argument value.

        Returns:
            This config for chaining.
        """
        self._arguments[key] = value
        return self

    def get_argument(self, key: str, default: Any = None) -> Any:
        """Get a job argument.

        Args:
            key: Argument key.
            default: Default value if not found.

        Returns:
            The argument value or default.
        """
        return self._arguments.get(key, default)

    def __repr__(self) -> str:
        return (
            f"JobConfig(name={self._name!r}, "
            f"auto_scaling={self._auto_scaling_enabled}, "
            f"guarantee={self._processing_guarantee.value})"
        )


class JobMetrics:
    """Metrics collected from a running or completed job."""

    def __init__(self):
        self._metrics: Dict[str, Any] = {}
        self._timestamp = datetime.now()
        self._items_in: int = 0
        self._items_out: int = 0
        self._snapshot_count: int = 0
        self._last_snapshot_time: Optional[datetime] = None

    @property
    def timestamp(self) -> datetime:
        """Get the timestamp when metrics were collected."""
        return self._timestamp

    @property
    def items_in(self) -> int:
        """Get the number of items received."""
        return self._items_in

    @items_in.setter
    def items_in(self, value: int) -> None:
        """Set the number of items received."""
        self._items_in = value

    @property
    def items_out(self) -> int:
        """Get the number of items emitted."""
        return self._items_out

    @items_out.setter
    def items_out(self, value: int) -> None:
        """Set the number of items emitted."""
        self._items_out = value

    @property
    def snapshot_count(self) -> int:
        """Get the number of snapshots taken."""
        return self._snapshot_count

    @property
    def last_snapshot_time(self) -> Optional[datetime]:
        """Get the time of the last snapshot."""
        return self._last_snapshot_time

    def record_snapshot(self) -> None:
        """Record a snapshot being taken."""
        self._snapshot_count += 1
        self._last_snapshot_time = datetime.now()

    def get(self, name: str, default: Any = None) -> Any:
        """Get a metric by name."""
        return self._metrics.get(name, default)

    def set(self, name: str, value: Any) -> None:
        """Set a metric value."""
        self._metrics[name] = value

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to a dictionary."""
        return {
            "timestamp": self._timestamp.isoformat(),
            "items_in": self._items_in,
            "items_out": self._items_out,
            "snapshot_count": self._snapshot_count,
            "last_snapshot_time": (
                self._last_snapshot_time.isoformat()
                if self._last_snapshot_time
                else None
            ),
            **self._metrics,
        }

    def __repr__(self) -> str:
        return f"JobMetrics(in={self._items_in}, out={self._items_out}, snapshots={self._snapshot_count})"


class Job:
    """Represents a Jet job.

    A Job is a unit of distributed computation submitted to the Jet
    cluster. Jobs can be monitored, suspended, resumed, and cancelled.

    Attributes:
        id: Unique job identifier.
        name: Optional job name.
        config: Job configuration.
        status: Current job status.
        submission_time: When the job was submitted.
    """

    _VALID_TRANSITIONS = {
        JobStatus.NOT_RUNNING: {JobStatus.STARTING},
        JobStatus.STARTING: {JobStatus.RUNNING, JobStatus.FAILED},
        JobStatus.RUNNING: {
            JobStatus.SUSPENDING,
            JobStatus.COMPLETING,
            JobStatus.FAILED,
        },
        JobStatus.SUSPENDING: {JobStatus.SUSPENDED, JobStatus.FAILED},
        JobStatus.SUSPENDED: {JobStatus.RESUMING, JobStatus.COMPLETING},
        JobStatus.RESUMING: {JobStatus.RUNNING, JobStatus.FAILED},
        JobStatus.COMPLETING: {JobStatus.COMPLETED, JobStatus.FAILED},
        JobStatus.FAILED: set(),
        JobStatus.COMPLETED: set(),
    }

    def __init__(
        self,
        job_id: int,
        name: Optional[str] = None,
        config: Optional[JobConfig] = None,
    ):
        self._id = job_id
        self._name = name
        self._config = config or JobConfig()
        self._status = JobStatus.NOT_RUNNING
        self._submission_time: Optional[datetime] = None
        self._completion_time: Optional[datetime] = None
        self._failure_reason: Optional[str] = None
        self._lock = threading.Lock()
        self._completion_future: Future = Future()
        self._metrics = JobMetrics()
        self._snapshots: List[str] = []
        self._light_job = False
        self._user_cancelled = False

    @property
    def id(self) -> int:
        """Get the job ID."""
        return self._id

    @property
    def name(self) -> Optional[str]:
        """Get the job name."""
        return self._name

    @property
    def config(self) -> JobConfig:
        """Get the job configuration."""
        return self._config

    @property
    def status(self) -> JobStatus:
        """Get the current job status."""
        with self._lock:
            return self._status

    @property
    def submission_time(self) -> Optional[datetime]:
        """Get the job submission time."""
        return self._submission_time

    @property
    def completion_time(self) -> Optional[datetime]:
        """Get the job completion time."""
        return self._completion_time

    @property
    def failure_reason(self) -> Optional[str]:
        """Get the failure reason if the job failed."""
        return self._failure_reason

    @property
    def metrics(self) -> JobMetrics:
        """Get the job metrics."""
        return self._metrics

    @property
    def snapshots(self) -> List[str]:
        """Get list of snapshot names."""
        return self._snapshots.copy()

    @property
    def is_light_job(self) -> bool:
        """Check if this is a light job (no fault tolerance)."""
        return self._light_job

    @property
    def is_user_cancelled(self) -> bool:
        """Check if the job was cancelled by user."""
        return self._user_cancelled

    def is_terminal(self) -> bool:
        """Check if the job is in a terminal state."""
        return self.status in (JobStatus.COMPLETED, JobStatus.FAILED)

    def _transition_status(self, new_status: JobStatus) -> bool:
        """Transition to a new status if valid.

        Args:
            new_status: The target status.

        Returns:
            True if transition was successful.
        """
        with self._lock:
            valid_targets = self._VALID_TRANSITIONS.get(self._status, set())
            if new_status not in valid_targets:
                return False
            self._status = new_status
            return True

    def _start(self) -> None:
        """Mark the job as started."""
        self._submission_time = datetime.now()
        self._transition_status(JobStatus.STARTING)
        self._transition_status(JobStatus.RUNNING)

    def _complete(self) -> None:
        """Mark the job as completed."""
        self._transition_status(JobStatus.COMPLETING)
        self._transition_status(JobStatus.COMPLETED)
        self._completion_time = datetime.now()
        if not self._completion_future.done():
            self._completion_future.set_result(self)

    def _fail(self, reason: str) -> None:
        """Mark the job as failed."""
        with self._lock:
            self._status = JobStatus.FAILED
            self._failure_reason = reason
            self._completion_time = datetime.now()
        if not self._completion_future.done():
            from hazelcast.exceptions import HazelcastException
            self._completion_future.set_exception(HazelcastException(reason))

    def suspend(self) -> None:
        """Suspend the job.

        Suspends a running job. The job can be resumed later.
        """
        self.suspend_async().result()

    def suspend_async(self) -> Future:
        """Suspend the job asynchronously.

        Returns:
            Future that completes when the job is suspended.
        """
        future: Future = Future()
        if self._transition_status(JobStatus.SUSPENDING):
            self._transition_status(JobStatus.SUSPENDED)
            future.set_result(None)
        else:
            from hazelcast.exceptions import IllegalStateException
            future.set_exception(
                IllegalStateException(
                    f"Cannot suspend job in status {self.status.value}"
                )
            )
        return future

    def resume(self) -> None:
        """Resume a suspended job."""
        self.resume_async().result()

    def resume_async(self) -> Future:
        """Resume a suspended job asynchronously.

        Returns:
            Future that completes when the job is resumed.
        """
        future: Future = Future()
        if self._transition_status(JobStatus.RESUMING):
            self._transition_status(JobStatus.RUNNING)
            future.set_result(None)
        else:
            from hazelcast.exceptions import IllegalStateException
            future.set_exception(
                IllegalStateException(
                    f"Cannot resume job in status {self.status.value}"
                )
            )
        return future

    def restart(self) -> None:
        """Restart the job.

        Restarts the job from the beginning or from a snapshot.
        """
        self.restart_async().result()

    def restart_async(self) -> Future:
        """Restart the job asynchronously.

        Returns:
            Future that completes when the job is restarted.
        """
        future: Future = Future()
        current = self.status
        if current in (JobStatus.RUNNING, JobStatus.SUSPENDED):
            with self._lock:
                self._status = JobStatus.STARTING
            self._transition_status(JobStatus.RUNNING)
            future.set_result(None)
        else:
            from hazelcast.exceptions import IllegalStateException
            future.set_exception(
                IllegalStateException(
                    f"Cannot restart job in status {current.value}"
                )
            )
        return future

    def cancel(self) -> None:
        """Cancel the job.

        Cancels a running or suspended job. This is a terminal operation.
        """
        self.cancel_async().result()

    def cancel_async(self) -> Future:
        """Cancel the job asynchronously.

        Returns:
            Future that completes when the job is cancelled.
        """
        future: Future = Future()
        current = self.status
        if current in (
            JobStatus.RUNNING,
            JobStatus.SUSPENDED,
            JobStatus.STARTING,
            JobStatus.SUSPENDING,
            JobStatus.RESUMING,
        ):
            with self._lock:
                self._status = JobStatus.COMPLETING
                self._user_cancelled = True
            self._transition_status(JobStatus.COMPLETED)
            self._completion_time = datetime.now()
            if not self._completion_future.done():
                self._completion_future.set_result(self)
            future.set_result(None)
        elif current in (JobStatus.COMPLETED, JobStatus.FAILED):
            future.set_result(None)
        else:
            from hazelcast.exceptions import IllegalStateException
            future.set_exception(
                IllegalStateException(
                    f"Cannot cancel job in status {current.value}"
                )
            )
        return future

    def cancel_and_export_snapshot(self, name: str) -> None:
        """Cancel the job and export a terminal snapshot.

        This allows the job to be restarted from this snapshot later.

        Args:
            name: Name for the exported snapshot.
        """
        self.cancel_and_export_snapshot_async(name).result()

    def cancel_and_export_snapshot_async(self, name: str) -> Future:
        """Cancel and export snapshot asynchronously.

        Args:
            name: Name for the exported snapshot.

        Returns:
            Future that completes when operation is done.
        """
        future: Future = Future()
        current = self.status
        if current in (JobStatus.RUNNING, JobStatus.SUSPENDED):
            self._snapshots.append(name)
            self._metrics.record_snapshot()
            with self._lock:
                self._status = JobStatus.COMPLETING
                self._user_cancelled = True
            self._transition_status(JobStatus.COMPLETED)
            self._completion_time = datetime.now()
            if not self._completion_future.done():
                self._completion_future.set_result(self)
            future.set_result(None)
        else:
            from hazelcast.exceptions import IllegalStateException
            future.set_exception(
                IllegalStateException(
                    f"Cannot cancel and export snapshot in status {current.value}"
                )
            )
        return future

    def join(self, timeout: Optional[float] = None) -> "Job":
        """Wait for the job to complete.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            This job instance.

        Raises:
            TimeoutException: If the timeout expires.
            HazelcastException: If the job fails.
        """
        return self.join_async().result(timeout=timeout)

    def join_async(self) -> Future:
        """Wait for the job to complete asynchronously.

        Returns:
            Future that completes when the job finishes.
        """
        return self._completion_future

    def get_metrics(self) -> dict:
        """Get job metrics.

        Returns:
            Dictionary of metric name to value.
        """
        base_metrics = {
            "status": self.status.value,
            "submission_time": (
                self._submission_time.isoformat() if self._submission_time else None
            ),
            "completion_time": (
                self._completion_time.isoformat() if self._completion_time else None
            ),
            "user_cancelled": self._user_cancelled,
        }
        if self._config.metrics_enabled:
            base_metrics.update(self._metrics.to_dict())
        return base_metrics

    def get_metrics_async(self) -> Future:
        """Get job metrics asynchronously.

        Returns:
            Future containing metrics dictionary.
        """
        future: Future = Future()
        future.set_result(self.get_metrics())
        return future

    def export_snapshot(self, name: str) -> Future:
        """Export a snapshot of the job state.

        Creates a named snapshot of the current job state that can be
        used to restart the job later.

        Args:
            name: Name for the snapshot.

        Returns:
            Future that completes when the snapshot is exported.

        Raises:
            IllegalStateException: If the job is not in a snapshotable state.
        """
        future: Future = Future()
        current = self.status
        if current in (JobStatus.RUNNING, JobStatus.SUSPENDED):
            self._snapshots.append(name)
            self._metrics.record_snapshot()
            future.set_result(None)
        else:
            from hazelcast.exceptions import IllegalStateException
            future.set_exception(
                IllegalStateException(
                    f"Cannot export snapshot in status {current.value}"
                )
            )
        return future

    def get_suspension_cause(self) -> Optional[str]:
        """Get the cause of job suspension.

        Returns:
            The suspension cause, or None if not suspended.
        """
        if self.status == JobStatus.SUSPENDED:
            return self._failure_reason
        return None

    def get_id_string(self) -> str:
        """Get a string representation of the job ID.

        Returns:
            Hex string of the job ID.
        """
        return f"{self._id:016x}"

    def __repr__(self) -> str:
        return (
            f"Job(id={self._id}, name={self._name!r}, "
            f"status={self._status.value})"
        )
