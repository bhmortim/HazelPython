"""Jet Job representation and management."""

from enum import IntEnum
from typing import Any, Callable, Dict, List, Optional
import uuid as uuid_module


class JobStatus(IntEnum):
    """Represents the status of a Jet job."""

    NOT_RUNNING = 0
    STARTING = 1
    RUNNING = 2
    SUSPENDED = 3
    SUSPENDED_EXPORTING_SNAPSHOT = 4
    COMPLETING = 5
    FAILED = 6
    COMPLETED = 7

    @classmethod
    def from_code(cls, code: int) -> "JobStatus":
        """Convert an integer code to JobStatus."""
        try:
            return cls(code)
        except ValueError:
            return cls.NOT_RUNNING


class TerminationMode(IntEnum):
    """Specifies how to terminate a Jet job."""

    RESTART_GRACEFUL = 0
    RESTART_FORCEFUL = 1
    SUSPEND_GRACEFUL = 2
    SUSPEND_FORCEFUL = 3
    CANCEL_GRACEFUL = 4
    CANCEL_FORCEFUL = 5


class JobConfig:
    """Configuration for a Jet job.

    Attributes:
        name: The job name.
        split_brain_protection_enabled: Whether split-brain protection is enabled.
        auto_scaling: Whether auto-scaling is enabled.
        processing_guarantee: The processing guarantee level.
        snapshot_interval_millis: Interval between snapshots in milliseconds.
        initial_snapshot_name: Name of initial snapshot to restore from.
        max_processor_accumulated_records: Max records accumulated per processor.
        timeout_millis: Job timeout in milliseconds.
        metrics_enabled: Whether metrics collection is enabled.
        store_metrics_after_job_completion: Whether to store metrics after completion.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        split_brain_protection_enabled: bool = False,
        auto_scaling: bool = True,
        processing_guarantee: str = "NONE",
        snapshot_interval_millis: int = 10000,
        initial_snapshot_name: Optional[str] = None,
        max_processor_accumulated_records: int = -1,
        timeout_millis: int = 0,
        metrics_enabled: bool = True,
        store_metrics_after_job_completion: bool = False,
    ):
        self.name = name
        self.split_brain_protection_enabled = split_brain_protection_enabled
        self.auto_scaling = auto_scaling
        self.processing_guarantee = processing_guarantee
        self.snapshot_interval_millis = snapshot_interval_millis
        self.initial_snapshot_name = initial_snapshot_name
        self.max_processor_accumulated_records = max_processor_accumulated_records
        self.timeout_millis = timeout_millis
        self.metrics_enabled = metrics_enabled
        self.store_metrics_after_job_completion = store_metrics_after_job_completion

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "name": self.name,
            "splitBrainProtectionEnabled": self.split_brain_protection_enabled,
            "autoScaling": self.auto_scaling,
            "processingGuarantee": self.processing_guarantee,
            "snapshotIntervalMillis": self.snapshot_interval_millis,
            "initialSnapshotName": self.initial_snapshot_name,
            "maxProcessorAccumulatedRecords": self.max_processor_accumulated_records,
            "timeoutMillis": self.timeout_millis,
            "metricsEnabled": self.metrics_enabled,
            "storeMetricsAfterJobCompletion": self.store_metrics_after_job_completion,
        }


class JobMetrics:
    """Metrics for a Jet job.

    Attributes:
        timestamp: The timestamp when metrics were collected.
        metrics: Dictionary of metric name to value.
    """

    def __init__(self, timestamp: int = 0, metrics: Optional[Dict[str, Any]] = None):
        self.timestamp = timestamp
        self.metrics = metrics or {}

    def get(self, name: str, default: Any = None) -> Any:
        """Get a metric value by name."""
        return self.metrics.get(name, default)

    def __repr__(self) -> str:
        return f"JobMetrics(timestamp={self.timestamp}, metrics_count={len(self.metrics)})"


class Job:
    """Represents a Jet job.

    A Job is a unit of work submitted to the Jet engine for execution.
    Jobs can be monitored, suspended, resumed, and cancelled.

    Attributes:
        id: The unique job identifier.
        name: The job name.
        status: The current job status.
        submission_time: The job submission time in milliseconds since epoch.
        completion_time: The job completion time (0 if not completed).
    """

    def __init__(
        self,
        jet_service: Any,
        job_id: int,
        name: Optional[str] = None,
        light_job_coordinator: Optional[uuid_module.UUID] = None,
    ):
        self._jet_service = jet_service
        self._id = job_id
        self._name = name
        self._light_job_coordinator = light_job_coordinator
        self._status: JobStatus = JobStatus.NOT_RUNNING
        self._submission_time: int = 0
        self._completion_time: int = 0
        self._status_listeners: Dict[uuid_module.UUID, Callable[[JobStatus, JobStatus], None]] = {}

    @property
    def id(self) -> int:
        """Return the job ID."""
        return self._id

    @property
    def name(self) -> Optional[str]:
        """Return the job name."""
        return self._name

    @property
    def status(self) -> JobStatus:
        """Return the cached job status."""
        return self._status

    @property
    def submission_time(self) -> int:
        """Return the submission time."""
        return self._submission_time

    @property
    def completion_time(self) -> int:
        """Return the completion time."""
        return self._completion_time

    @property
    def is_light_job(self) -> bool:
        """Return True if this is a light job."""
        return self._light_job_coordinator is not None

    def get_status(self) -> JobStatus:
        """Get the current job status from the cluster.

        Returns:
            The current JobStatus.
        """
        status_code = self._jet_service._get_job_status(self._id)
        self._status = JobStatus.from_code(status_code)
        return self._status

    def get_submission_time(self) -> int:
        """Get the job submission time from the cluster.

        Returns:
            The submission time in milliseconds since epoch.
        """
        self._submission_time = self._jet_service._get_job_submission_time(
            self._id, self._light_job_coordinator
        )
        return self._submission_time

    def get_config(self) -> Optional[JobConfig]:
        """Get the job configuration.

        Returns:
            The JobConfig or None if unavailable.
        """
        config_data = self._jet_service._get_job_config(
            self._id, self._light_job_coordinator
        )
        if config_data is None:
            return None
        return JobConfig()

    def get_metrics(self) -> JobMetrics:
        """Get the job metrics.

        Returns:
            The JobMetrics for this job.
        """
        metrics_data = self._jet_service._get_job_metrics(self._id)
        return JobMetrics()

    def get_suspension_cause(self) -> Optional[str]:
        """Get the suspension cause if the job is suspended.

        Returns:
            The suspension cause message or None.
        """
        return self._jet_service._get_job_suspension_cause(self._id)

    def is_user_cancelled(self) -> bool:
        """Check if the job was cancelled by user.

        Returns:
            True if the job was cancelled by user.
        """
        return self._jet_service._is_job_user_cancelled(self._id)

    def cancel(self) -> None:
        """Cancel the job gracefully."""
        self._jet_service._terminate_job(
            self._id,
            TerminationMode.CANCEL_GRACEFUL,
            self._light_job_coordinator,
        )

    def cancel_forcefully(self) -> None:
        """Cancel the job forcefully."""
        self._jet_service._terminate_job(
            self._id,
            TerminationMode.CANCEL_FORCEFUL,
            self._light_job_coordinator,
        )

    def suspend(self) -> None:
        """Suspend the job gracefully."""
        self._jet_service._terminate_job(
            self._id,
            TerminationMode.SUSPEND_GRACEFUL,
            self._light_job_coordinator,
        )

    def resume(self) -> None:
        """Resume a suspended job."""
        self._jet_service._resume_job(self._id)

    def restart(self) -> None:
        """Restart the job gracefully."""
        self._jet_service._terminate_job(
            self._id,
            TerminationMode.RESTART_GRACEFUL,
            self._light_job_coordinator,
        )

    def export_snapshot(self, name: str, cancel_job: bool = False) -> None:
        """Export a snapshot of the job state.

        Args:
            name: The snapshot name.
            cancel_job: Whether to cancel the job after export.
        """
        self._jet_service._export_snapshot(self._id, name, cancel_job)

    def add_status_listener(
        self,
        listener: Callable[[JobStatus, JobStatus], None],
        local_only: bool = False,
    ) -> uuid_module.UUID:
        """Add a job status listener.

        Args:
            listener: Callback function receiving (old_status, new_status).
            local_only: Whether to listen only to local events.

        Returns:
            The listener registration UUID.
        """
        registration_id = self._jet_service._add_job_status_listener(
            self._id,
            self._light_job_coordinator,
            local_only,
        )
        if registration_id:
            self._status_listeners[registration_id] = listener
        return registration_id

    def remove_status_listener(self, registration_id: uuid_module.UUID) -> bool:
        """Remove a job status listener.

        Args:
            registration_id: The listener registration ID.

        Returns:
            True if the listener was removed.
        """
        result = self._jet_service._remove_job_status_listener(
            self._id, registration_id
        )
        if result:
            self._status_listeners.pop(registration_id, None)
        return result

    def join(self) -> None:
        """Wait for the job to complete.

        Blocks until the job reaches a terminal state (COMPLETED or FAILED).
        """
        import time

        while True:
            status = self.get_status()
            if status in (JobStatus.COMPLETED, JobStatus.FAILED):
                break
            time.sleep(0.1)

    def __repr__(self) -> str:
        return f"Job(id={self._id}, name={self._name!r}, status={self._status.name})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Job):
            return False
        return self._id == other._id

    def __hash__(self) -> int:
        return hash(self._id)
