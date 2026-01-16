"""Jet job management and configuration."""

from concurrent.futures import Future
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional
import threading


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
    """

    def __init__(
        self,
        name: Optional[str] = None,
        auto_scaling_enabled: bool = True,
        split_brain_protection_enabled: bool = False,
        snapshot_interval_millis: int = 10000,
        processing_guarantee: ProcessingGuarantee = ProcessingGuarantee.NONE,
        max_processor_accumulated_records: int = -1,
    ):
        self._name = name
        self._auto_scaling_enabled = auto_scaling_enabled
        self._split_brain_protection_enabled = split_brain_protection_enabled
        self._snapshot_interval_millis = snapshot_interval_millis
        self._processing_guarantee = processing_guarantee
        self._max_processor_accumulated_records = max_processor_accumulated_records
        self._initial_snapshot_name: Optional[str] = None

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

    def __repr__(self) -> str:
        return (
            f"JobConfig(name={self._name!r}, "
            f"auto_scaling={self._auto_scaling_enabled}, "
            f"guarantee={self._processing_guarantee.value})"
        )


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
        return {
            "status": self.status.value,
            "submission_time": (
                self._submission_time.isoformat() if self._submission_time else None
            ),
            "completion_time": (
                self._completion_time.isoformat() if self._completion_time else None
            ),
        }

    def export_snapshot(self, name: str) -> Future:
        """Export a snapshot of the job state.

        Args:
            name: Name for the snapshot.

        Returns:
            Future that completes when the snapshot is exported.
        """
        future: Future = Future()
        future.set_result(None)
        return future

    def __repr__(self) -> str:
        return (
            f"Job(id={self._id}, name={self._name!r}, "
            f"status={self._status.value})"
        )
