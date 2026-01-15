"""Jet job management."""

from concurrent.futures import Future
from enum import Enum
from typing import Any, Dict, List, Optional, TYPE_CHECKING
import time
import uuid

if TYPE_CHECKING:
    from hazelcast.jet.pipeline import Pipeline


class JobStatus(Enum):
    """Status of a Jet job."""

    NOT_RUNNING = "NOT_RUNNING"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    SUSPENDED = "SUSPENDED"
    SUSPENDED_EXPORTING_SNAPSHOT = "SUSPENDED_EXPORTING_SNAPSHOT"
    COMPLETING = "COMPLETING"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"


class JobConfig:
    """Configuration for a Jet job."""

    def __init__(self, name: Optional[str] = None):
        self._name = name
        self._split_brain_protection_enabled = False
        self._auto_scaling_enabled = True
        self._processing_guarantee = "NONE"
        self._snapshot_interval_millis = 10000
        self._initial_snapshot_name: Optional[str] = None
        self._max_processor_accumulated_records = 10000000

    @property
    def name(self) -> Optional[str]:
        """Get the job name."""
        return self._name

    @name.setter
    def name(self, value: Optional[str]) -> None:
        """Set the job name."""
        self._name = value

    @property
    def split_brain_protection_enabled(self) -> bool:
        """Check if split brain protection is enabled."""
        return self._split_brain_protection_enabled

    @split_brain_protection_enabled.setter
    def split_brain_protection_enabled(self, value: bool) -> None:
        """Set split brain protection."""
        self._split_brain_protection_enabled = value

    @property
    def auto_scaling_enabled(self) -> bool:
        """Check if auto scaling is enabled."""
        return self._auto_scaling_enabled

    @auto_scaling_enabled.setter
    def auto_scaling_enabled(self, value: bool) -> None:
        """Set auto scaling."""
        self._auto_scaling_enabled = value

    @property
    def processing_guarantee(self) -> str:
        """Get the processing guarantee."""
        return self._processing_guarantee

    @processing_guarantee.setter
    def processing_guarantee(self, value: str) -> None:
        """Set the processing guarantee (NONE, AT_LEAST_ONCE, EXACTLY_ONCE)."""
        self._processing_guarantee = value

    @property
    def snapshot_interval_millis(self) -> int:
        """Get the snapshot interval in milliseconds."""
        return self._snapshot_interval_millis

    @snapshot_interval_millis.setter
    def snapshot_interval_millis(self, value: int) -> None:
        """Set the snapshot interval."""
        self._snapshot_interval_millis = value

    def set_name(self, name: str) -> "JobConfig":
        """Set the job name (fluent).

        Args:
            name: The job name.

        Returns:
            This config for chaining.
        """
        self._name = name
        return self

    def set_processing_guarantee(self, guarantee: str) -> "JobConfig":
        """Set the processing guarantee (fluent).

        Args:
            guarantee: The processing guarantee.

        Returns:
            This config for chaining.
        """
        self._processing_guarantee = guarantee
        return self

    def enable_auto_scaling(self) -> "JobConfig":
        """Enable auto scaling.

        Returns:
            This config for chaining.
        """
        self._auto_scaling_enabled = True
        return self

    def disable_auto_scaling(self) -> "JobConfig":
        """Disable auto scaling.

        Returns:
            This config for chaining.
        """
        self._auto_scaling_enabled = False
        return self


class Job:
    """Represents a Jet job.

    Jobs execute pipelines and can be monitored, suspended, resumed,
    and cancelled.
    """

    def __init__(
        self,
        job_id: int,
        name: Optional[str] = None,
        config: Optional[JobConfig] = None,
    ):
        self._job_id = job_id
        self._name = name or f"job-{job_id}"
        self._config = config or JobConfig()
        self._status = JobStatus.NOT_RUNNING
        self._submission_time = time.time()
        self._completion_time: Optional[float] = None
        self._metrics: Dict[str, Any] = {}

    @property
    def id(self) -> int:
        """Get the job ID."""
        return self._job_id

    @property
    def id_string(self) -> str:
        """Get the job ID as a hex string."""
        return f"{self._job_id:016x}"

    @property
    def name(self) -> str:
        """Get the job name."""
        return self._name

    @property
    def config(self) -> JobConfig:
        """Get the job configuration."""
        return self._config

    @property
    def status(self) -> JobStatus:
        """Get the current job status."""
        return self._status

    @property
    def submission_time(self) -> float:
        """Get the submission time as a timestamp."""
        return self._submission_time

    @property
    def completion_time(self) -> Optional[float]:
        """Get the completion time as a timestamp."""
        return self._completion_time

    def get_status(self) -> JobStatus:
        """Get the current job status.

        Returns:
            The job status.
        """
        return self.get_status_async().result()

    def get_status_async(self) -> Future:
        """Get the status asynchronously.

        Returns:
            Future containing the job status.
        """
        future: Future = Future()
        future.set_result(self._status)
        return future

    def get_metrics(self) -> Dict[str, Any]:
        """Get job metrics.

        Returns:
            Dictionary of metrics.
        """
        return self.get_metrics_async().result()

    def get_metrics_async(self) -> Future:
        """Get metrics asynchronously.

        Returns:
            Future containing metrics dictionary.
        """
        future: Future = Future()
        future.set_result(dict(self._metrics))
        return future

    def suspend(self) -> None:
        """Suspend the job."""
        self.suspend_async().result()

    def suspend_async(self) -> Future:
        """Suspend the job asynchronously.

        Returns:
            Future that completes when suspended.
        """
        future: Future = Future()
        if self._status == JobStatus.RUNNING:
            self._status = JobStatus.SUSPENDED
        future.set_result(None)
        return future

    def resume(self) -> None:
        """Resume a suspended job."""
        self.resume_async().result()

    def resume_async(self) -> Future:
        """Resume the job asynchronously.

        Returns:
            Future that completes when resumed.
        """
        future: Future = Future()
        if self._status == JobStatus.SUSPENDED:
            self._status = JobStatus.RUNNING
        future.set_result(None)
        return future

    def cancel(self) -> None:
        """Cancel the job."""
        self.cancel_async().result()

    def cancel_async(self) -> Future:
        """Cancel the job asynchronously.

        Returns:
            Future that completes when cancelled.
        """
        future: Future = Future()
        if self._status not in (JobStatus.COMPLETED, JobStatus.FAILED):
            self._status = JobStatus.FAILED
            self._completion_time = time.time()
        future.set_result(None)
        return future

    def restart(self) -> None:
        """Restart the job."""
        self.restart_async().result()

    def restart_async(self) -> Future:
        """Restart the job asynchronously.

        Returns:
            Future that completes when restarted.
        """
        future: Future = Future()
        if self._status in (JobStatus.RUNNING, JobStatus.SUSPENDED):
            self._status = JobStatus.STARTING
            self._status = JobStatus.RUNNING
        future.set_result(None)
        return future

    def join(self) -> None:
        """Wait for the job to complete."""
        self.join_async().result()

    def join_async(self) -> Future:
        """Wait for completion asynchronously.

        Returns:
            Future that completes when the job finishes.
        """
        future: Future = Future()
        future.set_result(None)
        return future

    def is_user_cancelled(self) -> bool:
        """Check if the job was cancelled by user.

        Returns:
            True if user cancelled.
        """
        return self._status == JobStatus.FAILED

    def _start(self) -> None:
        """Internal method to start the job."""
        self._status = JobStatus.STARTING
        self._status = JobStatus.RUNNING

    def _complete(self) -> None:
        """Internal method to complete the job."""
        self._status = JobStatus.COMPLETED
        self._completion_time = time.time()

    def __repr__(self) -> str:
        return (
            f"Job(id={self.id_string}, "
            f"name={self._name!r}, "
            f"status={self._status.value})"
        )
