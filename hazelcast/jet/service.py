"""Jet service for managing Jet jobs."""

from typing import Any, Callable, Dict, List, Optional
import time
import uuid as uuid_module

from hazelcast.jet.job import Job, JobConfig, JobStatus, TerminationMode
from hazelcast.jet.pipeline import Pipeline


class JetService:
    """Service for managing Jet jobs.

    The JetService provides methods to submit, manage, and monitor Jet jobs.

    Attributes:
        client: The Hazelcast client instance.
    """

    def __init__(self, client: Any):
        self._client = client
        self._invocation_service = getattr(client, "_invocation_service", None)
        self._serialization_service = getattr(client, "_serialization_service", None)

    def new_job(
        self,
        pipeline: Pipeline,
        config: Optional[JobConfig] = None,
    ) -> Job:
        """Submit a new job from a pipeline.

        Args:
            pipeline: The pipeline to execute.
            config: Optional job configuration.

        Returns:
            A Job instance representing the submitted job.

        Raises:
            ValueError: If the pipeline is empty.
        """
        if pipeline.is_empty():
            raise ValueError("Cannot submit an empty pipeline")

        job_id = self._generate_job_id()
        dag_data = self._serialize_pipeline(pipeline)
        config_data = self._serialize_config(config or JobConfig())

        self._submit_job(job_id, dag_data, config_data)

        job = Job(self, job_id, config.name if config else None)
        return job

    def new_light_job(
        self,
        pipeline: Pipeline,
        config: Optional[JobConfig] = None,
    ) -> Job:
        """Submit a new light job from a pipeline.

        Light jobs are optimized for low-latency, short-lived computations.

        Args:
            pipeline: The pipeline to execute.
            config: Optional job configuration.

        Returns:
            A Job instance representing the submitted job.
        """
        if pipeline.is_empty():
            raise ValueError("Cannot submit an empty pipeline")

        job_id = self._generate_job_id()
        dag_data = self._serialize_pipeline(pipeline)
        config_data = self._serialize_config(config or JobConfig())

        coordinator = self._get_light_job_coordinator()
        self._submit_job(job_id, dag_data, config_data, coordinator)

        job = Job(self, job_id, config.name if config else None, coordinator)
        return job

    def get_job(self, job_id: int) -> Optional[Job]:
        """Get a job by ID.

        Args:
            job_id: The job identifier.

        Returns:
            The Job instance or None if not found.
        """
        job_ids = self._get_job_ids()
        if job_id not in job_ids:
            return None
        return Job(self, job_id)

    def get_job_by_name(self, name: str) -> Optional[Job]:
        """Get a job by name.

        Args:
            name: The job name.

        Returns:
            The Job instance or None if not found.
        """
        summaries = self.get_jobs()
        for job in summaries:
            if job.name == name:
                return job
        return None

    def get_jobs(self) -> List[Job]:
        """Get all jobs.

        Returns:
            List of Job instances.
        """
        summaries = self._get_job_summary_list()
        jobs = []
        for summary in summaries:
            job = Job(self, summary["job_id"], summary.get("name"))
            job._status = JobStatus.from_code(summary.get("status", 0))
            job._submission_time = summary.get("submission_time", 0)
            job._completion_time = summary.get("completion_time", 0)
            jobs.append(job)
        return jobs

    def get_job_ids(self) -> List[int]:
        """Get all job IDs.

        Returns:
            List of job IDs.
        """
        return self._get_job_ids()

    def exists_distributed_object(
        self,
        service_name: str,
        object_name: str,
    ) -> bool:
        """Check if a distributed object exists.

        Args:
            service_name: The service name.
            object_name: The object name.

        Returns:
            True if the object exists.
        """
        return self._exists_distributed_object(service_name, object_name)

    def _generate_job_id(self) -> int:
        """Generate a unique job ID."""
        return int(time.time() * 1000) ^ (uuid_module.uuid4().int & 0xFFFFFFFFFFFFFFFF)

    def _get_light_job_coordinator(self) -> uuid_module.UUID:
        """Get the coordinator UUID for light jobs."""
        return uuid_module.uuid4()

    def _serialize_pipeline(self, pipeline: Pipeline) -> bytes:
        """Serialize a pipeline to bytes."""
        import json
        return json.dumps(pipeline.to_dag()).encode("utf-8")

    def _serialize_config(self, config: JobConfig) -> bytes:
        """Serialize a job configuration to bytes."""
        import json
        return json.dumps(config.to_dict()).encode("utf-8")

    def _submit_job(
        self,
        job_id: int,
        dag_data: bytes,
        config_data: bytes,
        light_job_coordinator: Optional[uuid_module.UUID] = None,
    ) -> None:
        """Submit a job to the cluster."""
        from hazelcast.protocol.codec import JetCodec

        request = JetCodec.encode_submit_job_request(
            job_id, dag_data, config_data, light_job_coordinator
        )
        if self._invocation_service:
            self._invocation_service.invoke(request)

    def _terminate_job(
        self,
        job_id: int,
        terminate_mode: TerminationMode,
        light_job_coordinator: Optional[uuid_module.UUID] = None,
    ) -> None:
        """Terminate a job."""
        from hazelcast.protocol.codec import JetCodec

        request = JetCodec.encode_terminate_job_request(
            job_id, int(terminate_mode), light_job_coordinator
        )
        if self._invocation_service:
            self._invocation_service.invoke(request)

    def _get_job_status(self, job_id: int) -> int:
        """Get the status of a job."""
        from hazelcast.protocol.codec import JetCodec, JOB_STATUS_NOT_RUNNING

        request = JetCodec.encode_get_job_status_request(job_id)
        if self._invocation_service:
            response = self._invocation_service.invoke(request)
            if response:
                return JetCodec.decode_get_job_status_response(response)
        return JOB_STATUS_NOT_RUNNING

    def _get_job_ids(
        self,
        only_name: Optional[str] = None,
        only_job_id: int = -1,
    ) -> List[int]:
        """Get job IDs."""
        from hazelcast.protocol.codec import JetCodec

        request = JetCodec.encode_get_job_ids_request(only_name, only_job_id)
        if self._invocation_service:
            response = self._invocation_service.invoke(request)
            if response:
                return JetCodec.decode_get_job_ids_response(response)
        return []

    def _get_job_submission_time(
        self,
        job_id: int,
        light_job_coordinator: Optional[uuid_module.UUID] = None,
    ) -> int:
        """Get the submission time of a job."""
        from hazelcast.protocol.codec import JetCodec

        request = JetCodec.encode_get_job_submission_time_request(
            job_id, light_job_coordinator
        )
        if self._invocation_service:
            response = self._invocation_service.invoke(request)
            if response:
                return JetCodec.decode_get_job_submission_time_response(response)
        return 0

    def _get_job_config(
        self,
        job_id: int,
        light_job_coordinator: Optional[uuid_module.UUID] = None,
    ) -> Optional[bytes]:
        """Get the configuration of a job."""
        from hazelcast.protocol.codec import JetCodec

        request = JetCodec.encode_get_job_config_request(job_id, light_job_coordinator)
        if self._invocation_service:
            response = self._invocation_service.invoke(request)
            if response:
                return JetCodec.decode_get_job_config_response(response)
        return None

    def _resume_job(self, job_id: int) -> None:
        """Resume a suspended job."""
        from hazelcast.protocol.codec import JetCodec

        request = JetCodec.encode_resume_job_request(job_id)
        if self._invocation_service:
            self._invocation_service.invoke(request)

    def _export_snapshot(
        self,
        job_id: int,
        name: str,
        cancel_job: bool,
    ) -> None:
        """Export a job snapshot."""
        from hazelcast.protocol.codec import JetCodec

        request = JetCodec.encode_export_snapshot_request(job_id, name, cancel_job)
        if self._invocation_service:
            self._invocation_service.invoke(request)

    def _get_job_summary_list(self) -> List[dict]:
        """Get the list of job summaries."""
        from hazelcast.protocol.codec import JetCodec

        request = JetCodec.encode_get_job_summary_list_request()
        if self._invocation_service:
            response = self._invocation_service.invoke(request)
            if response:
                return JetCodec.decode_get_job_summary_list_response(response)
        return []

    def _exists_distributed_object(
        self,
        service_name: str,
        object_name: str,
    ) -> bool:
        """Check if a distributed object exists."""
        from hazelcast.protocol.codec import JetCodec

        request = JetCodec.encode_exists_distributed_object_request(
            service_name, object_name
        )
        if self._invocation_service:
            response = self._invocation_service.invoke(request)
            if response:
                return JetCodec.decode_exists_distributed_object_response(response)
        return False

    def _get_job_metrics(self, job_id: int) -> Optional[bytes]:
        """Get job metrics."""
        from hazelcast.protocol.codec import JetCodec

        request = JetCodec.encode_get_job_metrics_request(job_id)
        if self._invocation_service:
            response = self._invocation_service.invoke(request)
            if response:
                return JetCodec.decode_get_job_metrics_response(response)
        return None

    def _get_job_suspension_cause(self, job_id: int) -> Optional[str]:
        """Get the suspension cause for a job."""
        from hazelcast.protocol.codec import JetCodec

        request = JetCodec.encode_get_job_suspension_cause_request(job_id)
        if self._invocation_service:
            response = self._invocation_service.invoke(request)
            if response:
                return JetCodec.decode_get_job_suspension_cause_response(response)
        return None

    def _is_job_user_cancelled(self, job_id: int) -> bool:
        """Check if a job was cancelled by user."""
        from hazelcast.protocol.codec import JetCodec

        request = JetCodec.encode_is_job_user_cancelled_request(job_id)
        if self._invocation_service:
            response = self._invocation_service.invoke(request)
            if response:
                return JetCodec.decode_is_job_user_cancelled_response(response)
        return False

    def _add_job_status_listener(
        self,
        job_id: int,
        light_job_coordinator: Optional[uuid_module.UUID],
        local_only: bool,
    ) -> Optional[uuid_module.UUID]:
        """Add a job status listener."""
        from hazelcast.protocol.codec import JetCodec

        request = JetCodec.encode_add_job_status_listener_request(
            job_id, light_job_coordinator, local_only
        )
        if self._invocation_service:
            response = self._invocation_service.invoke(request)
            if response:
                return JetCodec.decode_add_job_status_listener_response(response)
        return None

    def _remove_job_status_listener(
        self,
        job_id: int,
        registration_id: uuid_module.UUID,
    ) -> bool:
        """Remove a job status listener."""
        from hazelcast.protocol.codec import JetCodec

        request = JetCodec.encode_remove_job_status_listener_request(
            job_id, registration_id
        )
        if self._invocation_service:
            response = self._invocation_service.invoke(request)
            if response:
                return JetCodec.decode_remove_job_status_listener_response(response)
        return False
