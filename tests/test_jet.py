"""Unit tests for Jet pipeline and job management."""

import pytest
import time
from concurrent.futures import Future

from hazelcast.jet import (
    JetService,
    Pipeline,
    Stage,
    Source,
    Sink,
    BatchSource,
    MapSource,
    ListSource,
    MapSink,
    ListSink,
    Job,
    JobConfig,
    JobStatus,
)
from hazelcast.jet.job import ProcessingGuarantee
from hazelcast.exceptions import IllegalStateException, IllegalArgumentException


class TestJobConfig:
    """Tests for JobConfig."""

    def test_default_config(self):
        config = JobConfig()
        assert config.name is None
        assert config.auto_scaling_enabled is True
        assert config.split_brain_protection_enabled is False
        assert config.snapshot_interval_millis == 10000
        assert config.processing_guarantee == ProcessingGuarantee.NONE

    def test_config_with_name(self):
        config = JobConfig(name="test-job")
        assert config.name == "test-job"

    def test_config_setters(self):
        config = JobConfig()
        config.name = "my-job"
        config.auto_scaling_enabled = False
        config.split_brain_protection_enabled = True
        config.snapshot_interval_millis = 5000
        config.processing_guarantee = ProcessingGuarantee.EXACTLY_ONCE

        assert config.name == "my-job"
        assert config.auto_scaling_enabled is False
        assert config.split_brain_protection_enabled is True
        assert config.snapshot_interval_millis == 5000
        assert config.processing_guarantee == ProcessingGuarantee.EXACTLY_ONCE

    def test_config_repr(self):
        config = JobConfig(name="test")
        repr_str = repr(config)
        assert "test" in repr_str
        assert "JobConfig" in repr_str


class TestJobStatus:
    """Tests for JobStatus enum."""

    def test_status_values(self):
        assert JobStatus.NOT_RUNNING.value == "NOT_RUNNING"
        assert JobStatus.RUNNING.value == "RUNNING"
        assert JobStatus.SUSPENDED.value == "SUSPENDED"
        assert JobStatus.COMPLETED.value == "COMPLETED"
        assert JobStatus.FAILED.value == "FAILED"


class TestJob:
    """Tests for Job lifecycle."""

    def test_job_creation(self):
        job = Job(1, "test-job")
        assert job.id == 1
        assert job.name == "test-job"
        assert job.status == JobStatus.NOT_RUNNING
        assert job.submission_time is None

    def test_job_start(self):
        job = Job(1, "test-job")
        job._start()
        assert job.status == JobStatus.RUNNING
        assert job.submission_time is not None

    def test_job_suspend_resume(self):
        job = Job(1, "test-job")
        job._start()
        assert job.status == JobStatus.RUNNING

        job.suspend()
        assert job.status == JobStatus.SUSPENDED

        job.resume()
        assert job.status == JobStatus.RUNNING

    def test_job_cancel(self):
        job = Job(1, "test-job")
        job._start()
        job.cancel()
        assert job.status == JobStatus.COMPLETED
        assert job.is_terminal()

    def test_job_restart(self):
        job = Job(1, "test-job")
        job._start()
        job.restart()
        assert job.status == JobStatus.RUNNING

    def test_job_suspend_not_running_fails(self):
        job = Job(1, "test-job")
        with pytest.raises(IllegalStateException):
            job.suspend()

    def test_job_resume_not_suspended_fails(self):
        job = Job(1, "test-job")
        job._start()
        with pytest.raises(IllegalStateException):
            job.resume()

    def test_job_complete(self):
        job = Job(1, "test-job")
        job._start()
        job._complete()
        assert job.status == JobStatus.COMPLETED
        assert job.completion_time is not None
        assert job.is_terminal()

    def test_job_fail(self):
        job = Job(1, "test-job")
        job._start()
        job._fail("Something went wrong")
        assert job.status == JobStatus.FAILED
        assert job.failure_reason == "Something went wrong"
        assert job.is_terminal()

    def test_job_metrics(self):
        job = Job(1, "test-job")
        job._start()
        metrics = job.get_metrics()
        assert "status" in metrics
        assert "submission_time" in metrics
        assert metrics["status"] == "RUNNING"

    def test_job_repr(self):
        job = Job(1, "test-job")
        repr_str = repr(job)
        assert "Job" in repr_str
        assert "test-job" in repr_str


class TestSource:
    """Tests for Source classes."""

    def test_batch_source(self):
        source = BatchSource("numbers", [1, 2, 3])
        assert source.name == "numbers"
        assert source.source_type == "batch"
        assert source.items == [1, 2, 3]

    def test_map_source(self):
        source = MapSource("my-map")
        assert source.name == "my-map"
        assert source.source_type == "map"
        assert source.map_name == "my-map"

    def test_list_source(self):
        source = ListSource("my-list")
        assert source.name == "my-list"
        assert source.source_type == "list"

    def test_source_repr(self):
        source = BatchSource("test")
        repr_str = repr(source)
        assert "BatchSource" in repr_str
        assert "test" in repr_str


class TestSink:
    """Tests for Sink classes."""

    def test_map_sink(self):
        sink = MapSink("output-map")
        assert sink.name == "output-map"
        assert sink.sink_type == "map"
        assert sink.map_name == "output-map"

    def test_list_sink(self):
        sink = ListSink("output-list")
        assert sink.name == "output-list"
        assert sink.sink_type == "list"

    def test_sink_repr(self):
        sink = ListSink("test")
        repr_str = repr(sink)
        assert "ListSink" in repr_str
        assert "test" in repr_str


class TestPipeline:
    """Tests for Pipeline construction."""

    def test_create_pipeline(self):
        pipeline = Pipeline.create()
        assert pipeline.id is not None
        assert pipeline.source is None
        assert pipeline.sink is None
        assert len(pipeline.stages) == 0

    def test_from_list_source(self):
        source = Pipeline.from_list("numbers", [1, 2, 3, 4, 5])
        assert isinstance(source, BatchSource)
        assert source.name == "numbers"
        assert source.items == [1, 2, 3, 4, 5]

    def test_from_map_source(self):
        source = Pipeline.from_map("my-map")
        assert isinstance(source, MapSource)
        assert source.map_name == "my-map"

    def test_to_list_sink(self):
        sink = Pipeline.to_list("results")
        assert isinstance(sink, ListSink)
        assert sink.name == "results"

    def test_to_map_sink(self):
        sink = Pipeline.to_map("output")
        assert isinstance(sink, MapSink)
        assert sink.map_name == "output"

    def test_read_from_source(self):
        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3])
        stage = pipeline.read_from(source)

        assert pipeline.source == source
        assert isinstance(stage, Stage)
        assert len(pipeline.stages) == 1

    def test_pipeline_with_sink(self):
        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3])
        sink = Pipeline.to_list("output")

        pipeline.read_from(source).write_to(sink)

        assert pipeline.source == source
        assert pipeline.sink == sink
        assert pipeline.is_complete()

    def test_pipeline_repr(self):
        pipeline = Pipeline.create()
        repr_str = repr(pipeline)
        assert "Pipeline" in repr_str


class TestStage:
    """Tests for Stage transformations."""

    def test_map_transformation(self):
        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3])
        stage = pipeline.read_from(source)

        mapped = stage.map(lambda x: x * 2)
        assert isinstance(mapped, Stage)
        assert len(pipeline.stages) == 2

    def test_filter_transformation(self):
        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3, 4, 5])
        stage = pipeline.read_from(source)

        filtered = stage.filter(lambda x: x > 2)
        assert isinstance(filtered, Stage)

    def test_flat_map_transformation(self):
        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [[1, 2], [3, 4]])
        stage = pipeline.read_from(source)

        flat_mapped = stage.flat_map(lambda x: x)
        assert isinstance(flat_mapped, Stage)

    def test_peek_transformation(self):
        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3])
        stage = pipeline.read_from(source)

        peeked = stage.peek(lambda x: print(x))
        assert isinstance(peeked, Stage)

    def test_distinct_transformation(self):
        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 1, 2, 2, 3])
        stage = pipeline.read_from(source)

        distinct = stage.distinct()
        assert isinstance(distinct, Stage)

    def test_sort_transformation(self):
        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [3, 1, 2])
        stage = pipeline.read_from(source)

        sorted_stage = stage.sort()
        assert isinstance(sorted_stage, Stage)

    def test_chained_transformations(self):
        pipeline = Pipeline.create()
        source = Pipeline.from_list("numbers", list(range(10)))
        sink = Pipeline.to_list("results")

        result = (
            pipeline.read_from(source)
            .map(lambda x: x * 2)
            .filter(lambda x: x > 5)
            .distinct()
            .write_to(sink)
        )

        assert result == pipeline
        assert pipeline.is_complete()
        assert len(pipeline.stages) == 4

    def test_stage_repr(self):
        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3])
        stage = pipeline.read_from(source)
        repr_str = repr(stage)
        assert "Stage" in repr_str


class TestJetService:
    """Tests for JetService."""

    def test_service_creation(self):
        service = JetService()
        assert service.is_running is False

    def test_service_start_stop(self):
        service = JetService()
        service.start()
        assert service.is_running is True

        service.shutdown()
        assert service.is_running is False

    def test_submit_requires_running(self):
        service = JetService()
        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3])
        sink = Pipeline.to_list("output")
        pipeline.read_from(source).write_to(sink)

        with pytest.raises(IllegalStateException):
            service.submit(pipeline)

    def test_submit_job(self):
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3])
        sink = Pipeline.to_list("output")
        pipeline.read_from(source).write_to(sink)

        job = service.submit(pipeline)

        assert job is not None
        assert job.id == 1
        assert job.status == JobStatus.RUNNING

        service.shutdown()

    def test_submit_with_config(self):
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3])
        sink = Pipeline.to_list("output")
        pipeline.read_from(source).write_to(sink)

        config = JobConfig(name="test-job")
        job = service.submit(pipeline, config)

        assert job.name == "test-job"
        assert job.config == config

        service.shutdown()

    def test_submit_async(self):
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3])
        sink = Pipeline.to_list("output")
        pipeline.read_from(source).write_to(sink)

        future = service.submit_async(pipeline)
        assert isinstance(future, Future)

        job = future.result()
        assert job.status == JobStatus.RUNNING

        service.shutdown()

    def test_new_job(self):
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3])
        sink = Pipeline.to_list("output")
        pipeline.read_from(source).write_to(sink)

        job = service.new_job(pipeline)
        assert job is not None

        service.shutdown()

    def test_new_job_if_absent(self):
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3])
        sink = Pipeline.to_list("output")
        pipeline.read_from(source).write_to(sink)

        config = JobConfig(name="singleton-job")
        job1 = service.new_job_if_absent(pipeline, config)
        job2 = service.new_job_if_absent(pipeline, config)

        assert job1.id == job2.id

        service.shutdown()

    def test_new_job_if_absent_requires_name(self):
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3])
        sink = Pipeline.to_list("output")
        pipeline.read_from(source).write_to(sink)

        config = JobConfig()
        with pytest.raises(ValueError):
            service.new_job_if_absent(pipeline, config)

        service.shutdown()

    def test_get_job_by_id(self):
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3])
        sink = Pipeline.to_list("output")
        pipeline.read_from(source).write_to(sink)

        job = service.submit(pipeline)
        retrieved = service.get_job(job.id)

        assert retrieved is not None
        assert retrieved.id == job.id

        service.shutdown()

    def test_get_job_by_name(self):
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3])
        sink = Pipeline.to_list("output")
        pipeline.read_from(source).write_to(sink)

        config = JobConfig(name="named-job")
        job = service.submit(pipeline, config)
        retrieved = service.get_job_by_name("named-job")

        assert retrieved is not None
        assert retrieved.name == "named-job"

        service.shutdown()

    def test_get_jobs(self):
        service = JetService()
        service.start()

        for i in range(3):
            pipeline = Pipeline.create()
            source = Pipeline.from_list(f"data-{i}", [i])
            sink = Pipeline.to_list(f"output-{i}")
            pipeline.read_from(source).write_to(sink)
            service.submit(pipeline)

        jobs = service.get_jobs()
        assert len(jobs) == 3

        service.shutdown()

    def test_get_nonexistent_job(self):
        service = JetService()
        service.start()

        job = service.get_job(999)
        assert job is None

        job = service.get_job_by_name("nonexistent")
        assert job is None

        service.shutdown()

    def test_submit_incomplete_pipeline(self):
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", [1, 2, 3])
        pipeline.read_from(source)

        with pytest.raises(IllegalArgumentException):
            service.submit(pipeline)

        service.shutdown()

    def test_service_repr(self):
        service = JetService()
        repr_str = repr(service)
        assert "JetService" in repr_str


class TestPipelineIntegration:
    """Integration tests for complete pipeline flows."""

    def test_complete_batch_pipeline(self):
        pipeline = Pipeline.create()
        source = Pipeline.from_list("numbers", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        sink = Pipeline.to_list("results")

        result = (
            pipeline.read_from(source)
            .map(lambda x: x * 2)
            .filter(lambda x: x > 10)
            .write_to(sink)
        )

        assert result.is_complete()
        assert result.source.name == "numbers"
        assert result.sink.name == "results"

    def test_map_to_map_pipeline(self):
        pipeline = Pipeline.create()
        source = Pipeline.from_map("input-map")
        sink = Pipeline.to_map("output-map")

        result = (
            pipeline.read_from(source)
            .map(lambda entry: (entry[0], str(entry[1]).upper()))
            .write_to(sink)
        )

        assert result.is_complete()
        assert isinstance(result.source, MapSource)
        assert isinstance(result.sink, MapSink)

    def test_submit_and_manage_job(self):
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        source = Pipeline.from_list("data", list(range(100)))
        sink = Pipeline.to_list("output")
        pipeline.read_from(source).map(lambda x: x + 1).write_to(sink)

        config = JobConfig(name="lifecycle-test")
        job = service.submit(pipeline, config)

        assert job.status == JobStatus.RUNNING

        job.suspend()
        assert job.status == JobStatus.SUSPENDED

        job.resume()
        assert job.status == JobStatus.RUNNING

        job.cancel()
        assert job.status == JobStatus.COMPLETED

        service.shutdown()
