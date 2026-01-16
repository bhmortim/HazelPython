"""Unit tests for hazelcast.jet.service module."""

import pytest
from concurrent.futures import Future
from unittest.mock import Mock, MagicMock

from hazelcast.jet.service import JetService
from hazelcast.jet.pipeline import Pipeline
from hazelcast.jet.job import Job, JobConfig, JobStatus
from hazelcast.exceptions import IllegalStateException


class TestJetService:
    """Tests for JetService class."""

    def test_initial_state_not_running(self):
        service = JetService()
        assert service.is_running is False

    def test_initial_state_empty_jobs(self):
        service = JetService()
        assert len(service._jobs) == 0

    def test_start_sets_running(self):
        service = JetService()
        service.start()
        assert service.is_running is True

    def test_shutdown_sets_not_running(self):
        service = JetService()
        service.start()
        service.shutdown()
        assert service.is_running is False

    def test_is_running_property(self):
        service = JetService()
        assert service.is_running is False
        service._running = True
        assert service.is_running is True

    def test_submit_when_not_running_raises(self):
        service = JetService()
        pipeline = Mock(spec=Pipeline)
        with pytest.raises(IllegalStateException) as exc_info:
            service.submit(pipeline)
        assert "not running" in str(exc_info.value)

    def test_submit_async_when_not_running(self):
        service = JetService()
        pipeline = Mock(spec=Pipeline)
        future = service.submit_async(pipeline)
        with pytest.raises(IllegalStateException):
            future.result()

    def test_submit_success_creates_job(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        
        job = service.submit(pipeline)
        
        assert isinstance(job, Job)
        assert job.id == 1
        assert job.id in service._jobs

    def test_submit_increments_job_id(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        
        job1 = service.submit(pipeline)
        job2 = service.submit(pipeline)
        
        assert job1.id == 1
        assert job2.id == 2

    def test_submit_with_config_uses_job_name(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        config = JobConfig(name="my-job")
        
        job = service.submit(pipeline, config)
        
        assert job.name == "my-job"
        assert "my-job" in service._jobs_by_name

    def test_submit_async_success(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        
        future = service.submit_async(pipeline)
        job = future.result()
        
        assert isinstance(job, Job)

    def test_new_job_alias_for_submit(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        
        job = service.new_job(pipeline)
        
        assert isinstance(job, Job)

    def test_new_job_async(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        
        future = service.new_job_async(pipeline)
        job = future.result()
        
        assert isinstance(job, Job)

    def test_new_job_if_absent_creates_new_job(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        config = JobConfig(name="new-job")
        
        job = service.new_job_if_absent(pipeline, config)
        
        assert job.name == "new-job"

    def test_new_job_if_absent_returns_existing_job(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        config = JobConfig(name="existing-job")
        
        job1 = service.submit(pipeline, config)
        job2 = service.new_job_if_absent(pipeline, config)
        
        assert job1 is job2

    def test_new_job_if_absent_no_name_raises(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        config = JobConfig()
        
        with pytest.raises(ValueError) as exc_info:
            service.new_job_if_absent(pipeline, config)
        assert "name is required" in str(exc_info.value)

    def test_new_job_if_absent_async_no_name_raises(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        config = JobConfig()
        
        future = service.new_job_if_absent_async(pipeline, config)
        with pytest.raises(ValueError):
            future.result()

    def test_new_job_if_absent_not_running_raises(self):
        service = JetService()
        pipeline = Mock(spec=Pipeline)
        config = JobConfig(name="test")
        
        with pytest.raises(IllegalStateException):
            service.new_job_if_absent(pipeline, config)

    def test_new_job_if_absent_async_not_running(self):
        service = JetService()
        pipeline = Mock(spec=Pipeline)
        config = JobConfig(name="test")
        
        future = service.new_job_if_absent_async(pipeline, config)
        with pytest.raises(IllegalStateException):
            future.result()

    def test_get_job_found(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        
        submitted_job = service.submit(pipeline)
        found_job = service.get_job(submitted_job.id)
        
        assert found_job is submitted_job

    def test_get_job_not_found(self):
        service = JetService()
        service.start()
        
        job = service.get_job(999)
        
        assert job is None

    def test_get_job_async_found(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        
        submitted_job = service.submit(pipeline)
        future = service.get_job_async(submitted_job.id)
        
        assert future.result() is submitted_job

    def test_get_job_async_not_found(self):
        service = JetService()
        future = service.get_job_async(999)
        assert future.result() is None

    def test_get_job_by_name_found(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        config = JobConfig(name="named-job")
        
        submitted_job = service.submit(pipeline, config)
        found_job = service.get_job_by_name("named-job")
        
        assert found_job is submitted_job

    def test_get_job_by_name_not_found(self):
        service = JetService()
        service.start()
        
        job = service.get_job_by_name("nonexistent")
        
        assert job is None

    def test_get_job_by_name_async_found(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        config = JobConfig(name="async-job")
        
        submitted_job = service.submit(pipeline, config)
        future = service.get_job_by_name_async("async-job")
        
        assert future.result() is submitted_job

    def test_get_job_by_name_async_not_found(self):
        service = JetService()
        future = service.get_job_by_name_async("nonexistent")
        assert future.result() is None

    def test_get_jobs_returns_all_jobs(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        
        job1 = service.submit(pipeline)
        job2 = service.submit(pipeline)
        job3 = service.submit(pipeline)
        
        jobs = service.get_jobs()
        
        assert len(jobs) == 3
        assert job1 in jobs
        assert job2 in jobs
        assert job3 in jobs

    def test_get_jobs_empty(self):
        service = JetService()
        service.start()
        
        jobs = service.get_jobs()
        
        assert jobs == []

    def test_get_jobs_async(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        
        service.submit(pipeline)
        service.submit(pipeline)
        
        future = service.get_jobs_async()
        jobs = future.result()
        
        assert len(jobs) == 2

    def test_get_jobs_by_name_returns_matching(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        
        config = JobConfig(name="specific-name")
        submitted_job = service.submit(pipeline, config)
        
        jobs = service.get_jobs_by_name("specific-name")
        
        assert len(jobs) == 1
        assert jobs[0] is submitted_job

    def test_get_jobs_by_name_no_match(self):
        service = JetService()
        service.start()
        
        jobs = service.get_jobs_by_name("nonexistent")
        
        assert jobs == []

    def test_get_jobs_by_name_async(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        
        config = JobConfig(name="async-search")
        service.submit(pipeline, config)
        
        future = service.get_jobs_by_name_async("async-search")
        jobs = future.result()
        
        assert len(jobs) == 1

    def test_get_jobs_by_name_async_no_match(self):
        service = JetService()
        future = service.get_jobs_by_name_async("nonexistent")
        assert future.result() == []

    def test_repr(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        service.submit(pipeline)
        
        repr_str = repr(service)
        
        assert "JetService" in repr_str
        assert "running=True" in repr_str
        assert "jobs=1" in repr_str

    def test_repr_not_running(self):
        service = JetService()
        
        repr_str = repr(service)
        
        assert "running=False" in repr_str
        assert "jobs=0" in repr_str

    def test_submit_without_config(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        
        job = service.submit(pipeline)
        
        assert job.config is not None

    def test_job_started_on_submit(self):
        service = JetService()
        service.start()
        pipeline = Mock(spec=Pipeline)
        
        job = service.submit(pipeline)
        
        assert job.status == JobStatus.RUNNING
