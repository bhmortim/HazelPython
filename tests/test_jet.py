"""Unit tests for the Jet module."""

import unittest
from concurrent.futures import Future

from hazelcast.jet.pipeline import (
    Pipeline,
    Source,
    Sink,
    BatchSource,
    MapSource,
    ListSink,
    MapSink,
    Stage,
)
from hazelcast.jet.job import Job, JobConfig, JobStatus
from hazelcast.jet.service import JetService
from hazelcast.exceptions import IllegalStateException


class TestPipeline(unittest.TestCase):
    """Tests for Pipeline class."""

    def test_create(self):
        """Test pipeline creation."""
        pipeline = Pipeline.create()
        self.assertIsInstance(pipeline, Pipeline)
        self.assertIsNotNone(pipeline.id)

    def test_pipeline_id_is_unique(self):
        """Test that each pipeline has a unique ID."""
        p1 = Pipeline.create()
        p2 = Pipeline.create()
        self.assertNotEqual(p1.id, p2.id)

    def test_read_from_returns_stage(self):
        """Test that read_from returns a Stage."""
        pipeline = Pipeline.create()
        source = BatchSource("test", [1, 2, 3])
        stage = pipeline.read_from(source)
        self.assertIsInstance(stage, Stage)
        self.assertIs(stage.pipeline, pipeline)

    def test_draw_from_alias(self):
        """Test that draw_from is an alias for read_from."""
        pipeline = Pipeline.create()
        source = BatchSource("test", [1, 2, 3])
        stage = pipeline.draw_from(source)
        self.assertIsInstance(stage, Stage)

    def test_from_list_creates_batch_source(self):
        """Test from_list factory method."""
        source = Pipeline.from_list("numbers", [1, 2, 3])
        self.assertIsInstance(source, BatchSource)
        self.assertEqual(source.name, "numbers")
        self.assertEqual(list(source.items()), [1, 2, 3])

    def test_from_map_creates_map_source(self):
        """Test from_map factory method."""
        source = Pipeline.from_map("my-map")
        self.assertIsInstance(source, MapSource)
        self.assertEqual(source.map_name, "my-map")

    def test_to_list_creates_list_sink(self):
        """Test to_list factory method."""
        sink = Pipeline.to_list("results")
        self.assertIsInstance(sink, ListSink)
        self.assertEqual(sink.name, "results")

    def test_to_map_creates_map_sink(self):
        """Test to_map factory method."""
        sink = Pipeline.to_map("output-map")
        self.assertIsInstance(sink, MapSink)
        self.assertEqual(sink.map_name, "output-map")

    def test_pipeline_repr(self):
        """Test pipeline string representation."""
        pipeline = Pipeline.create()
        repr_str = repr(pipeline)
        self.assertIn("Pipeline", repr_str)
        self.assertIn("stages=0", repr_str)


class TestSource(unittest.TestCase):
    """Tests for Source classes."""

    def test_batch_source_name(self):
        """Test BatchSource name property."""
        source = BatchSource("my-source", [1, 2, 3])
        self.assertEqual(source.name, "my-source")

    def test_batch_source_items(self):
        """Test BatchSource items iteration."""
        data = [1, 2, 3, 4, 5]
        source = BatchSource("test", data)
        self.assertEqual(list(source.items()), data)

    def test_map_source_name(self):
        """Test MapSource name format."""
        source = MapSource("test-map")
        self.assertEqual(source.name, "map:test-map")
        self.assertEqual(source.map_name, "test-map")

    def test_map_source_items_empty(self):
        """Test MapSource items returns empty iterator."""
        source = MapSource("test-map")
        self.assertEqual(list(source.items()), [])


class TestSink(unittest.TestCase):
    """Tests for Sink classes."""

    def test_list_sink_name(self):
        """Test ListSink name property."""
        sink = ListSink("my-sink")
        self.assertEqual(sink.name, "my-sink")

    def test_list_sink_receive_and_items(self):
        """Test ListSink receives and stores items."""
        sink = ListSink("collector")
        sink.receive(1)
        sink.receive(2)
        sink.receive(3)
        self.assertEqual(sink.items, [1, 2, 3])

    def test_list_sink_items_returns_copy(self):
        """Test that items property returns a copy."""
        sink = ListSink("collector")
        sink.receive(1)
        items = sink.items
        items.append(2)
        self.assertEqual(sink.items, [1])

    def test_map_sink_name(self):
        """Test MapSink name format."""
        sink = MapSink("output-map")
        self.assertEqual(sink.name, "map:output-map")
        self.assertEqual(sink.map_name, "output-map")


class TestStage(unittest.TestCase):
    """Tests for Stage class."""

    def test_map_transformation(self):
        """Test map creates a new stage with transformation."""
        pipeline = Pipeline.create()
        source = BatchSource("test", [1, 2, 3])
        stage1 = pipeline.read_from(source)
        stage2 = stage1.map(lambda x: x * 2)
        self.assertIsNot(stage1, stage2)
        self.assertIs(stage2.pipeline, pipeline)

    def test_filter_transformation(self):
        """Test filter creates a new stage."""
        pipeline = Pipeline.create()
        source = BatchSource("test", [1, 2, 3, 4, 5])
        stage1 = pipeline.read_from(source)
        stage2 = stage1.filter(lambda x: x > 2)
        self.assertIsNot(stage1, stage2)

    def test_flat_map_transformation(self):
        """Test flat_map creates a new stage."""
        pipeline = Pipeline.create()
        source = BatchSource("test", [[1, 2], [3, 4]])
        stage1 = pipeline.read_from(source)
        stage2 = stage1.flat_map(iter)
        self.assertIsNot(stage1, stage2)

    def test_write_to_returns_pipeline(self):
        """Test write_to returns the pipeline for chaining."""
        pipeline = Pipeline.create()
        source = BatchSource("test", [1, 2, 3])
        sink = ListSink("output")
        result = pipeline.read_from(source).write_to(sink)
        self.assertIs(result, pipeline)

    def test_drain_to_alias(self):
        """Test drain_to is an alias for write_to."""
        pipeline = Pipeline.create()
        source = BatchSource("test", [1, 2, 3])
        sink = ListSink("output")
        result = pipeline.read_from(source).drain_to(sink)
        self.assertIs(result, pipeline)

    def test_chained_transformations(self):
        """Test chaining multiple transformations."""
        pipeline = Pipeline.create()
        source = BatchSource("numbers", [1, 2, 3, 4, 5])
        sink = ListSink("results")

        result = (
            pipeline.read_from(source)
            .filter(lambda x: x > 2)
            .map(lambda x: x * 10)
            .write_to(sink)
        )

        self.assertIs(result, pipeline)


class TestJobConfig(unittest.TestCase):
    """Tests for JobConfig class."""

    def test_default_values(self):
        """Test default configuration values."""
        config = JobConfig()
        self.assertIsNone(config.name)
        self.assertFalse(config.split_brain_protection_enabled)
        self.assertTrue(config.auto_scaling_enabled)
        self.assertEqual(config.processing_guarantee, "NONE")
        self.assertEqual(config.snapshot_interval_millis, 10000)

    def test_name_constructor(self):
        """Test name in constructor."""
        config = JobConfig(name="my-job")
        self.assertEqual(config.name, "my-job")

    def test_name_setter(self):
        """Test name setter."""
        config = JobConfig()
        config.name = "updated-name"
        self.assertEqual(config.name, "updated-name")

    def test_split_brain_protection_setter(self):
        """Test split brain protection setter."""
        config = JobConfig()
        config.split_brain_protection_enabled = True
        self.assertTrue(config.split_brain_protection_enabled)

    def test_auto_scaling_setter(self):
        """Test auto scaling setter."""
        config = JobConfig()
        config.auto_scaling_enabled = False
        self.assertFalse(config.auto_scaling_enabled)

    def test_processing_guarantee_setter(self):
        """Test processing guarantee setter."""
        config = JobConfig()
        config.processing_guarantee = "EXACTLY_ONCE"
        self.assertEqual(config.processing_guarantee, "EXACTLY_ONCE")

    def test_snapshot_interval_setter(self):
        """Test snapshot interval setter."""
        config = JobConfig()
        config.snapshot_interval_millis = 5000
        self.assertEqual(config.snapshot_interval_millis, 5000)

    def test_fluent_set_name(self):
        """Test fluent set_name method."""
        config = JobConfig().set_name("fluent-job")
        self.assertEqual(config.name, "fluent-job")
        self.assertIsInstance(config, JobConfig)

    def test_fluent_set_processing_guarantee(self):
        """Test fluent set_processing_guarantee method."""
        config = JobConfig().set_processing_guarantee("AT_LEAST_ONCE")
        self.assertEqual(config.processing_guarantee, "AT_LEAST_ONCE")

    def test_fluent_enable_auto_scaling(self):
        """Test fluent enable_auto_scaling method."""
        config = JobConfig()
        config.auto_scaling_enabled = False
        config.enable_auto_scaling()
        self.assertTrue(config.auto_scaling_enabled)

    def test_fluent_disable_auto_scaling(self):
        """Test fluent disable_auto_scaling method."""
        config = JobConfig().disable_auto_scaling()
        self.assertFalse(config.auto_scaling_enabled)

    def test_fluent_chaining(self):
        """Test fluent method chaining."""
        config = (
            JobConfig()
            .set_name("chained-job")
            .set_processing_guarantee("EXACTLY_ONCE")
            .disable_auto_scaling()
        )
        self.assertEqual(config.name, "chained-job")
        self.assertEqual(config.processing_guarantee, "EXACTLY_ONCE")
        self.assertFalse(config.auto_scaling_enabled)


class TestJob(unittest.TestCase):
    """Tests for Job class."""

    def test_job_creation(self):
        """Test job creation with default values."""
        job = Job(123)
        self.assertEqual(job.id, 123)
        self.assertEqual(job.get_id(), 123)
        self.assertEqual(job.name, "job-123")

    def test_job_with_name(self):
        """Test job creation with name."""
        job = Job(456, name="my-job")
        self.assertEqual(job.name, "my-job")
        self.assertEqual(job.get_name(), "my-job")

    def test_job_with_config(self):
        """Test job creation with config."""
        config = JobConfig(name="configured-job")
        job = Job(789, config=config)
        self.assertIs(job.config, config)
        self.assertIs(job.get_config(), config)

    def test_job_id_string(self):
        """Test job ID string format."""
        job = Job(255)
        self.assertEqual(job.id_string, "00000000000000ff")

    def test_job_initial_status(self):
        """Test job initial status."""
        job = Job(1)
        self.assertEqual(job.status, JobStatus.NOT_RUNNING)

    def test_job_submission_time(self):
        """Test job submission time is set."""
        job = Job(1)
        self.assertIsNotNone(job.submission_time)
        self.assertIsNotNone(job.get_submission_time())

    def test_job_completion_time_initially_none(self):
        """Test completion time is None initially."""
        job = Job(1)
        self.assertIsNone(job.completion_time)

    def test_get_status(self):
        """Test get_status method."""
        job = Job(1)
        status = job.get_status()
        self.assertEqual(status, JobStatus.NOT_RUNNING)

    def test_get_status_async(self):
        """Test get_status_async method."""
        job = Job(1)
        future = job.get_status_async()
        self.assertIsInstance(future, Future)
        self.assertEqual(future.result(), JobStatus.NOT_RUNNING)

    def test_get_metrics(self):
        """Test get_metrics method."""
        job = Job(1)
        metrics = job.get_metrics()
        self.assertIsInstance(metrics, dict)

    def test_get_metrics_async(self):
        """Test get_metrics_async method."""
        job = Job(1)
        future = job.get_metrics_async()
        self.assertIsInstance(future, Future)
        self.assertIsInstance(future.result(), dict)

    def test_suspend(self):
        """Test suspend method."""
        job = Job(1)
        job._start()
        self.assertEqual(job.status, JobStatus.RUNNING)
        job.suspend()
        self.assertEqual(job.status, JobStatus.SUSPENDED)

    def test_suspend_async(self):
        """Test suspend_async method."""
        job = Job(1)
        job._start()
        future = job.suspend_async()
        self.assertIsInstance(future, Future)
        future.result()
        self.assertEqual(job.status, JobStatus.SUSPENDED)

    def test_resume(self):
        """Test resume method."""
        job = Job(1)
        job._start()
        job.suspend()
        job.resume()
        self.assertEqual(job.status, JobStatus.RUNNING)

    def test_resume_async(self):
        """Test resume_async method."""
        job = Job(1)
        job._start()
        job.suspend()
        future = job.resume_async()
        self.assertIsInstance(future, Future)
        future.result()
        self.assertEqual(job.status, JobStatus.RUNNING)

    def test_cancel(self):
        """Test cancel method."""
        job = Job(1)
        job._start()
        job.cancel()
        self.assertEqual(job.status, JobStatus.FAILED)
        self.assertIsNotNone(job.completion_time)

    def test_cancel_async(self):
        """Test cancel_async method."""
        job = Job(1)
        job._start()
        future = job.cancel_async()
        self.assertIsInstance(future, Future)
        future.result()
        self.assertEqual(job.status, JobStatus.FAILED)

    def test_restart(self):
        """Test restart method."""
        job = Job(1)
        job._start()
        job.suspend()
        job.restart()
        self.assertEqual(job.status, JobStatus.RUNNING)

    def test_restart_async(self):
        """Test restart_async method."""
        job = Job(1)
        job._start()
        future = job.restart_async()
        self.assertIsInstance(future, Future)
        future.result()
        self.assertEqual(job.status, JobStatus.RUNNING)

    def test_join(self):
        """Test join method."""
        job = Job(1)
        job.join()

    def test_join_async(self):
        """Test join_async method."""
        job = Job(1)
        future = job.join_async()
        self.assertIsInstance(future, Future)
        future.result()

    def test_export_snapshot(self):
        """Test export_snapshot method."""
        job = Job(1)
        job._start()
        job.export_snapshot("snapshot-1")
        self.assertEqual(job.status, JobStatus.RUNNING)

    def test_export_snapshot_async(self):
        """Test export_snapshot_async method."""
        job = Job(1)
        job._start()
        future = job.export_snapshot_async("snapshot-2")
        self.assertIsInstance(future, Future)
        future.result()

    def test_is_user_cancelled(self):
        """Test is_user_cancelled method."""
        job = Job(1)
        self.assertFalse(job.is_user_cancelled())
        job._start()
        job.cancel()
        self.assertTrue(job.is_user_cancelled())

    def test_job_repr(self):
        """Test job string representation."""
        job = Job(123, name="test-job")
        repr_str = repr(job)
        self.assertIn("Job", repr_str)
        self.assertIn("test-job", repr_str)
        self.assertIn("NOT_RUNNING", repr_str)


class TestJobStatus(unittest.TestCase):
    """Tests for JobStatus enum."""

    def test_all_statuses_exist(self):
        """Test all expected statuses exist."""
        self.assertEqual(JobStatus.NOT_RUNNING.value, "NOT_RUNNING")
        self.assertEqual(JobStatus.STARTING.value, "STARTING")
        self.assertEqual(JobStatus.RUNNING.value, "RUNNING")
        self.assertEqual(JobStatus.SUSPENDED.value, "SUSPENDED")
        self.assertEqual(
            JobStatus.SUSPENDED_EXPORTING_SNAPSHOT.value,
            "SUSPENDED_EXPORTING_SNAPSHOT",
        )
        self.assertEqual(JobStatus.COMPLETING.value, "COMPLETING")
        self.assertEqual(JobStatus.FAILED.value, "FAILED")
        self.assertEqual(JobStatus.COMPLETED.value, "COMPLETED")


class TestJetService(unittest.TestCase):
    """Tests for JetService class."""

    def setUp(self):
        """Set up test fixtures."""
        self.service = JetService()

    def tearDown(self):
        """Tear down test fixtures."""
        self.service.shutdown()

    def test_service_creation(self):
        """Test service creation."""
        self.assertIsInstance(self.service, JetService)
        self.assertFalse(self.service.is_running)

    def test_start_and_shutdown(self):
        """Test start and shutdown."""
        self.service.start()
        self.assertTrue(self.service.is_running)
        self.service.shutdown()
        self.assertFalse(self.service.is_running)

    def test_submit_when_not_running(self):
        """Test submit raises when service not running."""
        pipeline = Pipeline.create()
        with self.assertRaises(IllegalStateException):
            self.service.submit(pipeline)

    def test_submit(self):
        """Test submit method."""
        self.service.start()
        pipeline = Pipeline.create()
        job = self.service.submit(pipeline)
        self.assertIsInstance(job, Job)
        self.assertEqual(job.status, JobStatus.RUNNING)

    def test_submit_with_config(self):
        """Test submit with config."""
        self.service.start()
        pipeline = Pipeline.create()
        config = JobConfig(name="configured-job")
        job = self.service.submit(pipeline, config)
        self.assertEqual(job.name, "configured-job")

    def test_submit_async(self):
        """Test submit_async method."""
        self.service.start()
        pipeline = Pipeline.create()
        future = self.service.submit_async(pipeline)
        self.assertIsInstance(future, Future)
        job = future.result()
        self.assertIsInstance(job, Job)

    def test_new_job_alias(self):
        """Test new_job is alias for submit."""
        self.service.start()
        pipeline = Pipeline.create()
        job = self.service.new_job(pipeline)
        self.assertIsInstance(job, Job)

    def test_new_job_async(self):
        """Test new_job_async method."""
        self.service.start()
        pipeline = Pipeline.create()
        future = self.service.new_job_async(pipeline)
        self.assertIsInstance(future, Future)

    def test_new_job_if_absent_requires_name(self):
        """Test new_job_if_absent requires name in config."""
        self.service.start()
        pipeline = Pipeline.create()
        config = JobConfig()
        with self.assertRaises(ValueError):
            self.service.new_job_if_absent(pipeline, config)

    def test_new_job_if_absent_creates_new(self):
        """Test new_job_if_absent creates new job."""
        self.service.start()
        pipeline = Pipeline.create()
        config = JobConfig(name="unique-job")
        job = self.service.new_job_if_absent(pipeline, config)
        self.assertEqual(job.name, "unique-job")

    def test_new_job_if_absent_returns_existing(self):
        """Test new_job_if_absent returns existing job."""
        self.service.start()
        pipeline = Pipeline.create()
        config = JobConfig(name="existing-job")
        job1 = self.service.new_job_if_absent(pipeline, config)
        job2 = self.service.new_job_if_absent(pipeline, config)
        self.assertIs(job1, job2)

    def test_get_job(self):
        """Test get_job method."""
        self.service.start()
        pipeline = Pipeline.create()
        job = self.service.submit(pipeline)
        retrieved = self.service.get_job(job.id)
        self.assertIs(retrieved, job)

    def test_get_job_not_found(self):
        """Test get_job returns None for unknown ID."""
        self.service.start()
        result = self.service.get_job(99999)
        self.assertIsNone(result)

    def test_get_job_async(self):
        """Test get_job_async method."""
        self.service.start()
        pipeline = Pipeline.create()
        job = self.service.submit(pipeline)
        future = self.service.get_job_async(job.id)
        self.assertIsInstance(future, Future)
        self.assertIs(future.result(), job)

    def test_get_job_by_name(self):
        """Test get_job_by_name method."""
        self.service.start()
        pipeline = Pipeline.create()
        config = JobConfig(name="named-job")
        job = self.service.submit(pipeline, config)
        retrieved = self.service.get_job_by_name("named-job")
        self.assertIs(retrieved, job)

    def test_get_job_by_name_not_found(self):
        """Test get_job_by_name returns None."""
        self.service.start()
        result = self.service.get_job_by_name("nonexistent")
        self.assertIsNone(result)

    def test_get_job_by_name_async(self):
        """Test get_job_by_name_async method."""
        self.service.start()
        future = self.service.get_job_by_name_async("any")
        self.assertIsInstance(future, Future)

    def test_get_jobs(self):
        """Test get_jobs method."""
        self.service.start()
        pipeline = Pipeline.create()
        self.service.submit(pipeline)
        self.service.submit(pipeline)
        jobs = self.service.get_jobs()
        self.assertEqual(len(jobs), 2)

    def test_get_jobs_empty(self):
        """Test get_jobs returns empty list."""
        self.service.start()
        jobs = self.service.get_jobs()
        self.assertEqual(jobs, [])

    def test_get_jobs_async(self):
        """Test get_jobs_async method."""
        self.service.start()
        future = self.service.get_jobs_async()
        self.assertIsInstance(future, Future)
        self.assertIsInstance(future.result(), list)

    def test_get_jobs_by_name(self):
        """Test get_jobs_by_name method."""
        self.service.start()
        pipeline = Pipeline.create()
        config = JobConfig(name="batch-job")
        self.service.submit(pipeline, config)
        jobs = self.service.get_jobs_by_name("batch-job")
        self.assertEqual(len(jobs), 1)

    def test_get_jobs_by_name_not_found(self):
        """Test get_jobs_by_name returns empty list."""
        self.service.start()
        jobs = self.service.get_jobs_by_name("unknown")
        self.assertEqual(jobs, [])

    def test_get_jobs_by_name_async(self):
        """Test get_jobs_by_name_async method."""
        self.service.start()
        future = self.service.get_jobs_by_name_async("any")
        self.assertIsInstance(future, Future)

    def test_service_repr(self):
        """Test service string representation."""
        repr_str = repr(self.service)
        self.assertIn("JetService", repr_str)
        self.assertIn("running=False", repr_str)


class TestIntegration(unittest.TestCase):
    """Integration tests for Jet pipeline execution."""

    def test_full_pipeline_flow(self):
        """Test complete pipeline creation and submission."""
        service = JetService()
        service.start()

        try:
            source = Pipeline.from_list("numbers", [1, 2, 3, 4, 5])
            sink = Pipeline.to_list("results")

            pipeline = (
                Pipeline.create()
                .read_from(source)
                .filter(lambda x: x > 2)
                .map(lambda x: x * 10)
                .write_to(sink)
            )

            config = (
                JobConfig()
                .set_name("transform-job")
                .set_processing_guarantee("AT_LEAST_ONCE")
            )

            job = service.submit(pipeline, config)

            self.assertEqual(job.get_name(), "transform-job")
            self.assertEqual(job.get_status(), JobStatus.RUNNING)
            self.assertIsNotNone(job.get_submission_time())
            self.assertEqual(
                job.get_config().processing_guarantee, "AT_LEAST_ONCE"
            )
        finally:
            service.shutdown()

    def test_job_lifecycle(self):
        """Test full job lifecycle."""
        service = JetService()
        service.start()

        try:
            pipeline = Pipeline.create()
            job = service.submit(pipeline)

            self.assertEqual(job.get_status(), JobStatus.RUNNING)

            job.suspend()
            self.assertEqual(job.get_status(), JobStatus.SUSPENDED)

            job.resume()
            self.assertEqual(job.get_status(), JobStatus.RUNNING)

            job.export_snapshot("checkpoint-1")
            self.assertEqual(job.get_status(), JobStatus.RUNNING)

            job.cancel()
            self.assertEqual(job.get_status(), JobStatus.FAILED)
            self.assertTrue(job.is_user_cancelled())
        finally:
            service.shutdown()


if __name__ == "__main__":
    unittest.main()
