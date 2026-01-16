"""Jet service for pipeline submission and job management."""

from concurrent.futures import Future
from typing import Any, Dict, List, Optional, TYPE_CHECKING
import threading

from hazelcast.jet.pipeline import Pipeline
from hazelcast.jet.job import Job, JobConfig, JobStatus
from hazelcast.exceptions import IllegalArgumentException
from hazelcast.exceptions import HazelcastException, IllegalStateException

if TYPE_CHECKING:
    from hazelcast.invocation import InvocationService


class JetService:
    """Service for submitting and managing Jet jobs.

    Provides methods to submit pipelines, retrieve jobs,
    and manage their lifecycle.
    """

    def __init__(
        self,
        invocation_service: Optional["InvocationService"] = None,
        serialization_service: Optional[Any] = None,
    ):
        self._invocation_service = invocation_service
        self._serialization_service = serialization_service
        self._running = False
        self._jobs: Dict[int, Job] = {}
        self._jobs_by_name: Dict[str, Job] = {}
        self._job_id_counter = 0
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the Jet service."""
        self._running = True

    def shutdown(self) -> None:
        """Shutdown the Jet service."""
        self._running = False

    @property
    def is_running(self) -> bool:
        """Check if the service is running."""
        return self._running

    def submit(
        self,
        pipeline: Pipeline,
        config: Optional[JobConfig] = None,
    ) -> Job:
        """Submit a pipeline for execution.

        Args:
            pipeline: The pipeline to execute.
            config: Optional job configuration.

        Returns:
            The submitted Job.

        Raises:
            IllegalStateException: If the service is not running.
        """
        return self.submit_async(pipeline, config).result()

    def submit_async(
        self,
        pipeline: Pipeline,
        config: Optional[JobConfig] = None,
    ) -> Future:
        """Submit a pipeline asynchronously.

        Args:
            pipeline: The pipeline to execute.
            config: Optional job configuration.

        Returns:
            Future containing the Job.
        """
        future: Future = Future()

        if not self._running:
            future.set_exception(
                IllegalStateException("Jet service is not running")
            )
            return future

        if pipeline is None:
            future.set_exception(
                IllegalArgumentException("Pipeline cannot be None")
            )
            return future

        if not pipeline.is_complete():
            future.set_exception(
                IllegalArgumentException(
                    "Pipeline must have both source and sink defined"
                )
            )
            return future

        with self._lock:
            self._job_id_counter += 1
            job_id = self._job_id_counter

        config = config or JobConfig()
        job = Job(job_id, config.name, config)
        job._start()

        with self._lock:
            self._jobs[job_id] = job
            if job.name:
                self._jobs_by_name[job.name] = job

        future.set_result(job)
        return future

    def new_job(
        self,
        pipeline: Pipeline,
        config: Optional[JobConfig] = None,
    ) -> Job:
        """Create and submit a new job.

        Alias for submit().

        Args:
            pipeline: The pipeline to execute.
            config: Optional job configuration.

        Returns:
            The submitted Job.
        """
        return self.submit(pipeline, config)

    def new_job_async(
        self,
        pipeline: Pipeline,
        config: Optional[JobConfig] = None,
    ) -> Future:
        """Create and submit a new job asynchronously.

        Args:
            pipeline: The pipeline to execute.
            config: Optional job configuration.

        Returns:
            Future containing the Job.
        """
        return self.submit_async(pipeline, config)

    def new_job_if_absent(
        self,
        pipeline: Pipeline,
        config: JobConfig,
    ) -> Job:
        """Submit a job only if a job with the same name doesn't exist.

        Args:
            pipeline: The pipeline to execute.
            config: Job configuration (name is required).

        Returns:
            The existing or new Job.

        Raises:
            ValueError: If config.name is not set.
        """
        return self.new_job_if_absent_async(pipeline, config).result()

    def new_job_if_absent_async(
        self,
        pipeline: Pipeline,
        config: JobConfig,
    ) -> Future:
        """Submit a job if absent asynchronously.

        Args:
            pipeline: The pipeline to execute.
            config: Job configuration.

        Returns:
            Future containing the Job.
        """
        future: Future = Future()

        if not config.name:
            future.set_exception(
                ValueError("JobConfig.name is required for new_job_if_absent")
            )
            return future

        if not self._running:
            future.set_exception(
                IllegalStateException("Jet service is not running")
            )
            return future

        with self._lock:
            existing = self._jobs_by_name.get(config.name)
            if existing:
                future.set_result(existing)
                return future

        return self.submit_async(pipeline, config)

    def get_job(self, job_id: int) -> Optional[Job]:
        """Get a job by ID.

        Args:
            job_id: The job ID.

        Returns:
            The Job, or None if not found.
        """
        return self.get_job_async(job_id).result()

    def get_job_async(self, job_id: int) -> Future:
        """Get a job by ID asynchronously.

        Args:
            job_id: The job ID.

        Returns:
            Future containing the Job or None.
        """
        future: Future = Future()
        with self._lock:
            future.set_result(self._jobs.get(job_id))
        return future

    def get_job_by_name(self, name: str) -> Optional[Job]:
        """Get a job by name.

        Args:
            name: The job name.

        Returns:
            The Job, or None if not found.
        """
        return self.get_job_by_name_async(name).result()

    def get_job_by_name_async(self, name: str) -> Future:
        """Get a job by name asynchronously.

        Args:
            name: The job name.

        Returns:
            Future containing the Job or None.
        """
        future: Future = Future()
        with self._lock:
            future.set_result(self._jobs_by_name.get(name))
        return future

    def get_jobs(self) -> List[Job]:
        """Get all jobs.

        Returns:
            List of all jobs.
        """
        return self.get_jobs_async().result()

    def get_jobs_async(self) -> Future:
        """Get all jobs asynchronously.

        Returns:
            Future containing list of jobs.
        """
        future: Future = Future()
        with self._lock:
            future.set_result(list(self._jobs.values()))
        return future

    def get_jobs_by_name(self, name: str) -> List[Job]:
        """Get all jobs with a specific name.

        Args:
            name: The job name.

        Returns:
            List of jobs with the given name.
        """
        return self.get_jobs_by_name_async(name).result()

    def get_jobs_by_name_async(self, name: str) -> Future:
        """Get jobs by name asynchronously.

        Args:
            name: The job name.

        Returns:
            Future containing list of matching jobs.
        """
        future: Future = Future()
        with self._lock:
            job = self._jobs_by_name.get(name)
            future.set_result([job] if job else [])
        return future

    def __repr__(self) -> str:
        with self._lock:
            job_count = len(self._jobs)
        return f"JetService(running={self._running}, jobs={job_count})"
