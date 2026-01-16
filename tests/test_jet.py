"""Unit tests for Jet service, pipeline, and job management."""

import struct
import unittest
import uuid

from hazelcast.protocol.codec import (
    JetCodec,
    JOB_STATUS_RUNNING,
    JOB_STATUS_COMPLETED,
    JOB_STATUS_FAILED,
    JOB_STATUS_SUSPENDED,
    TERMINATE_MODE_CANCEL_GRACEFUL,
    TERMINATE_MODE_SUSPEND_GRACEFUL,
    TERMINATE_MODE_RESTART_GRACEFUL,
    JET_SUBMIT_JOB,
    JET_TERMINATE_JOB,
    JET_GET_JOB_STATUS,
    JET_GET_JOB_IDS,
    JET_GET_JOB_SUMMARY_LIST,
    JET_RESUME_JOB,
    JET_EXPORT_SNAPSHOT,
    REQUEST_HEADER_SIZE,
    RESPONSE_HEADER_SIZE,
    LONG_SIZE,
    INT_SIZE,
    BOOLEAN_SIZE,
)
from hazelcast.jet.job import Job, JobConfig, JobStatus, TerminationMode, JobMetrics
from hazelcast.jet.pipeline import (
    Pipeline,
    Sources,
    Sinks,
    Stage,
    MapSource,
    ListSource,
    MapSink,
    ListSink,
    LoggerSink,
    NoopSink,
    ProcessingGuarantee,
)
from hazelcast.jet.service import JetService


class MockInvocationService:
    """Mock invocation service for testing."""

    def __init__(self):
        self.last_request = None
        self.response = None

    def invoke(self, request):
        self.last_request = request
        return self.response


class MockJetService:
    """Mock Jet service for testing Job class."""

    def __init__(self):
        self.terminated_jobs = []
        self.resumed_jobs = []
        self.exported_snapshots = []
        self.job_statuses = {}
        self.job_submission_times = {}

    def _terminate_job(self, job_id, terminate_mode, light_job_coordinator=None):
        self.terminated_jobs.append((job_id, terminate_mode, light_job_coordinator))

    def _resume_job(self, job_id):
        self.resumed_jobs.append(job_id)

    def _export_snapshot(self, job_id, name, cancel_job):
        self.exported_snapshots.append((job_id, name, cancel_job))

    def _get_job_status(self, job_id):
        return self.job_statuses.get(job_id, JOB_STATUS_RUNNING)

    def _get_job_submission_time(self, job_id, light_job_coordinator=None):
        return self.job_submission_times.get(job_id, 0)

    def _get_job_config(self, job_id, light_job_coordinator=None):
        return None

    def _get_job_metrics(self, job_id):
        return None

    def _get_job_suspension_cause(self, job_id):
        return None

    def _is_job_user_cancelled(self, job_id):
        return False

    def _add_job_status_listener(self, job_id, light_job_coordinator, local_only):
        return uuid.uuid4()

    def _remove_job_status_listener(self, job_id, registration_id):
        return True


class TestJetCodec(unittest.TestCase):
    """Tests for Jet codec encode/decode functions."""

    def test_encode_submit_job_request(self):
        """Test encoding a submit job request."""
        job_id = 12345
        dag_data = b'{"dag": "test"}'
        config_data = b'{"config": "test"}'

        msg = JetCodec.encode_submit_job_request(job_id, dag_data, config_data)

        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), JET_SUBMIT_JOB)

    def test_encode_submit_job_request_with_coordinator(self):
        """Test encoding a submit job request with light job coordinator."""
        job_id = 12345
        dag_data = b'{"dag": "test"}'
        config_data = b'{"config": "test"}'
        coordinator = uuid.uuid4()

        msg = JetCodec.encode_submit_job_request(
            job_id, dag_data, config_data, coordinator
        )

        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), JET_SUBMIT_JOB)

    def test_encode_terminate_job_request(self):
        """Test encoding a terminate job request."""
        job_id = 12345
        terminate_mode = TERMINATE_MODE_CANCEL_GRACEFUL

        msg = JetCodec.encode_terminate_job_request(job_id, terminate_mode)

        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), JET_TERMINATE_JOB)

    def test_encode_get_job_status_request(self):
        """Test encoding a get job status request."""
        job_id = 12345

        msg = JetCodec.encode_get_job_status_request(job_id)

        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), JET_GET_JOB_STATUS)

    def test_encode_get_job_ids_request(self):
        """Test encoding a get job IDs request."""
        msg = JetCodec.encode_get_job_ids_request()

        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), JET_GET_JOB_IDS)

    def test_encode_get_job_ids_request_with_filter(self):
        """Test encoding a get job IDs request with name filter."""
        msg = JetCodec.encode_get_job_ids_request(only_name="test-job")

        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), JET_GET_JOB_IDS)

    def test_encode_resume_job_request(self):
        """Test encoding a resume job request."""
        job_id = 12345

        msg = JetCodec.encode_resume_job_request(job_id)

        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), JET_RESUME_JOB)

    def test_encode_export_snapshot_request(self):
        """Test encoding an export snapshot request."""
        job_id = 12345
        name = "test-snapshot"

        msg = JetCodec.encode_export_snapshot_request(job_id, name, False)

        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), JET_EXPORT_SNAPSHOT)

    def test_encode_get_job_summary_list_request(self):
        """Test encoding a get job summary list request."""
        msg = JetCodec.encode_get_job_summary_list_request()

        self.assertIsNotNone(msg)
        self.assertEqual(msg.get_message_type(), JET_GET_JOB_SUMMARY_LIST)


class TestJobStatus(unittest.TestCase):
    """Tests for JobStatus enum."""

    def test_job_status_values(self):
        """Test JobStatus enum values."""
        self.assertEqual(JobStatus.NOT_RUNNING.value, 0)
        self.assertEqual(JobStatus.STARTING.value, 1)
        self.assertEqual(JobStatus.RUNNING.value, 2)
        self.assertEqual(JobStatus.SUSPENDED.value, 3)
        self.assertEqual(JobStatus.COMPLETED.value, 7)
        self.assertEqual(JobStatus.FAILED.value, 6)

    def test_job_status_from_code(self):
        """Test converting code to JobStatus."""
        self.assertEqual(JobStatus.from_code(0), JobStatus.NOT_RUNNING)
        self.assertEqual(JobStatus.from_code(2), JobStatus.RUNNING)
        self.assertEqual(JobStatus.from_code(7), JobStatus.COMPLETED)
        self.assertEqual(JobStatus.from_code(999), JobStatus.NOT_RUNNING)


class TestTerminationMode(unittest.TestCase):
    """Tests for TerminationMode enum."""

    def test_termination_mode_values(self):
        """Test TerminationMode enum values."""
        self.assertEqual(TerminationMode.RESTART_GRACEFUL.value, 0)
        self.assertEqual(TerminationMode.RESTART_FORCEFUL.value, 1)
        self.assertEqual(TerminationMode.SUSPEND_GRACEFUL.value, 2)
        self.assertEqual(TerminationMode.SUSPEND_FORCEFUL.value, 3)
        self.assertEqual(TerminationMode.CANCEL_GRACEFUL.value, 4)
        self.assertEqual(TerminationMode.CANCEL_FORCEFUL.value, 5)


class TestJobConfig(unittest.TestCase):
    """Tests for JobConfig class."""

    def test_job_config_defaults(self):
        """Test JobConfig default values."""
        config = JobConfig()

        self.assertIsNone(config.name)
        self.assertFalse(config.split_brain_protection_enabled)
        self.assertTrue(config.auto_scaling)
        self.assertEqual(config.processing_guarantee, "NONE")
        self.assertEqual(config.snapshot_interval_millis, 10000)
        self.assertTrue(config.metrics_enabled)

    def test_job_config_custom_values(self):
        """Test JobConfig with custom values."""
        config = JobConfig(
            name="test-job",
            auto_scaling=False,
            processing_guarantee="EXACTLY_ONCE",
            snapshot_interval_millis=5000,
        )

        self.assertEqual(config.name, "test-job")
        self.assertFalse(config.auto_scaling)
        self.assertEqual(config.processing_guarantee, "EXACTLY_ONCE")
        self.assertEqual(config.snapshot_interval_millis, 5000)

    def test_job_config_to_dict(self):
        """Test JobConfig serialization to dictionary."""
        config = JobConfig(name="test-job")
        d = config.to_dict()

        self.assertEqual(d["name"], "test-job")
        self.assertIn("autoScaling", d)
        self.assertIn("processingGuarantee", d)


class TestJob(unittest.TestCase):
    """Tests for Job class."""

    def setUp(self):
        self.mock_service = MockJetService()
        self.job = Job(self.mock_service, 12345, "test-job")

    def test_job_properties(self):
        """Test Job properties."""
        self.assertEqual(self.job.id, 12345)
        self.assertEqual(self.job.name, "test-job")
        self.assertEqual(self.job.status, JobStatus.NOT_RUNNING)
        self.assertFalse(self.job.is_light_job)

    def test_job_cancel(self):
        """Test Job cancel."""
        self.job.cancel()

        self.assertEqual(len(self.mock_service.terminated_jobs), 1)
        job_id, mode, _ = self.mock_service.terminated_jobs[0]
        self.assertEqual(job_id, 12345)
        self.assertEqual(mode, TerminationMode.CANCEL_GRACEFUL)

    def test_job_cancel_forcefully(self):
        """Test Job cancel forcefully."""
        self.job.cancel_forcefully()

        self.assertEqual(len(self.mock_service.terminated_jobs), 1)
        job_id, mode, _ = self.mock_service.terminated_jobs[0]
        self.assertEqual(mode, TerminationMode.CANCEL_FORCEFUL)

    def test_job_suspend(self):
        """Test Job suspend."""
        self.job.suspend()

        self.assertEqual(len(self.mock_service.terminated_jobs), 1)
        job_id, mode, _ = self.mock_service.terminated_jobs[0]
        self.assertEqual(mode, TerminationMode.SUSPEND_GRACEFUL)

    def test_job_resume(self):
        """Test Job resume."""
        self.job.resume()

        self.assertEqual(len(self.mock_service.resumed_jobs), 1)
        self.assertEqual(self.mock_service.resumed_jobs[0], 12345)

    def test_job_restart(self):
        """Test Job restart."""
        self.job.restart()

        self.assertEqual(len(self.mock_service.terminated_jobs), 1)
        job_id, mode, _ = self.mock_service.terminated_jobs[0]
        self.assertEqual(mode, TerminationMode.RESTART_GRACEFUL)

    def test_job_export_snapshot(self):
        """Test Job export snapshot."""
        self.job.export_snapshot("my-snapshot", cancel_job=True)

        self.assertEqual(len(self.mock_service.exported_snapshots), 1)
        job_id, name, cancel = self.mock_service.exported_snapshots[0]
        self.assertEqual(job_id, 12345)
        self.assertEqual(name, "my-snapshot")
        self.assertTrue(cancel)

    def test_job_get_status(self):
        """Test Job get status."""
        self.mock_service.job_statuses[12345] = JOB_STATUS_RUNNING

        status = self.job.get_status()

        self.assertEqual(status, JobStatus.RUNNING)

    def test_job_equality(self):
        """Test Job equality."""
        job1 = Job(self.mock_service, 12345, "job1")
        job2 = Job(self.mock_service, 12345, "job2")
        job3 = Job(self.mock_service, 99999, "job1")

        self.assertEqual(job1, job2)
        self.assertNotEqual(job1, job3)

    def test_job_hash(self):
        """Test Job hash."""
        job1 = Job(self.mock_service, 12345, "job1")
        job2 = Job(self.mock_service, 12345, "job2")

        self.assertEqual(hash(job1), hash(job2))

    def test_job_repr(self):
        """Test Job repr."""
        repr_str = repr(self.job)

        self.assertIn("12345", repr_str)
        self.assertIn("test-job", repr_str)

    def test_light_job(self):
        """Test light job detection."""
        coordinator = uuid.uuid4()
        light_job = Job(self.mock_service, 12345, "light-job", coordinator)

        self.assertTrue(light_job.is_light_job)


class TestPipeline(unittest.TestCase):
    """Tests for Pipeline class."""

    def test_create_pipeline(self):
        """Test creating a pipeline."""
        pipeline = Pipeline.create()

        self.assertIsNotNone(pipeline)
        self.assertTrue(pipeline.is_empty())

    def test_pipeline_read_from_map(self):
        """Test reading from a map source."""
        pipeline = Pipeline.create()
        source = Sources.map("test-map")
        stage = pipeline.read_from(source)

        self.assertIsNotNone(stage)
        self.assertFalse(pipeline.is_empty())

    def test_pipeline_read_from_list(self):
        """Test reading from a list source."""
        pipeline = Pipeline.create()
        source = Sources.list("test-list")
        stage = pipeline.read_from(source)

        self.assertIsNotNone(stage)
        self.assertFalse(pipeline.is_empty())

    def test_pipeline_read_from_items(self):
        """Test reading from test items."""
        pipeline = Pipeline.create()
        source = Sources.items(1, 2, 3, 4, 5)
        stage = pipeline.read_from(source)

        self.assertIsNotNone(stage)
        self.assertEqual(source.items, [1, 2, 3, 4, 5])

    def test_pipeline_to_dag(self):
        """Test converting pipeline to DAG."""
        pipeline = Pipeline.create()
        source = Sources.map("test-map")
        pipeline.read_from(source)

        dag = pipeline.to_dag()

        self.assertIn("sources", dag)
        self.assertIn("stages", dag)
        self.assertIn("sinks", dag)
        self.assertEqual(len(dag["sources"]), 1)

    def test_pipeline_to_json(self):
        """Test converting pipeline to JSON."""
        pipeline = Pipeline.create()
        source = Sources.map("test-map")
        pipeline.read_from(source)

        json_str = pipeline.to_json()

        self.assertIn("sources", json_str)
        self.assertIn("test-map", json_str)


class TestStage(unittest.TestCase):
    """Tests for Stage class."""

    def setUp(self):
        self.pipeline = Pipeline.create()
        self.stage = self.pipeline.read_from(Sources.map("test-map"))

    def test_stage_map(self):
        """Test stage map operation."""
        mapped = self.stage.map(lambda x: x * 2)

        self.assertIsNotNone(mapped)
        self.assertIn("map", mapped.name)

    def test_stage_filter(self):
        """Test stage filter operation."""
        filtered = self.stage.filter(lambda x: x > 0)

        self.assertIsNotNone(filtered)
        self.assertIn("filter", filtered.name)

    def test_stage_flat_map(self):
        """Test stage flat_map operation."""
        flat_mapped = self.stage.flat_map(lambda x: [x, x])

        self.assertIsNotNone(flat_mapped)
        self.assertIn("flatmap", flat_mapped.name)

    def test_stage_group_by(self):
        """Test stage group_by operation."""
        grouped = self.stage.group_by(lambda x: x[0])

        self.assertIsNotNone(grouped)
        self.assertIn("groupby", grouped.name)

    def test_stage_distinct(self):
        """Test stage distinct operation."""
        distinct = self.stage.distinct()

        self.assertIsNotNone(distinct)
        self.assertIn("distinct", distinct.name)

    def test_stage_sort(self):
        """Test stage sort operation."""
        sorted_stage = self.stage.sort()

        self.assertIsNotNone(sorted_stage)
        self.assertIn("sort", sorted_stage.name)

    def test_stage_peek(self):
        """Test stage peek operation."""
        peeked = self.stage.peek(lambda x: print(x))

        self.assertIsNotNone(peeked)
        self.assertIn("peek", peeked.name)

    def test_stage_drain_to_map(self):
        """Test draining to a map sink."""
        sink_stage = self.stage.drain_to(Sinks.map("result-map"))

        self.assertIsNotNone(sink_stage)
        self.assertEqual(sink_stage.sink.map_name, "result-map")

    def test_stage_drain_to_list(self):
        """Test draining to a list sink."""
        sink_stage = self.stage.drain_to(Sinks.list("result-list"))

        self.assertIsNotNone(sink_stage)
        self.assertEqual(sink_stage.sink.list_name, "result-list")

    def test_stage_drain_to_logger(self):
        """Test draining to a logger sink."""
        sink_stage = self.stage.drain_to(Sinks.logger("prefix: "))

        self.assertIsNotNone(sink_stage)

    def test_stage_drain_to_noop(self):
        """Test draining to a noop sink."""
        sink_stage = self.stage.drain_to(Sinks.noop())

        self.assertIsNotNone(sink_stage)

    def test_stage_chaining(self):
        """Test chaining multiple operations."""
        result = (
            self.stage
            .filter(lambda x: x[1] > 0)
            .map(lambda x: x[1])
            .distinct()
            .sort()
        )

        self.assertIsNotNone(result)

    def test_stage_to_dict(self):
        """Test stage serialization."""
        mapped = self.stage.map(lambda x: x * 2)
        d = mapped.to_dict()

        self.assertIn("name", d)
        self.assertIn("operations", d)
        self.assertEqual(len(d["operations"]), 1)


class TestSources(unittest.TestCase):
    """Tests for Sources factory."""

    def test_map_source(self):
        """Test creating a map source."""
        source = Sources.map("my-map")

        self.assertIsInstance(source, MapSource)
        self.assertEqual(source.map_name, "my-map")
        self.assertTrue(source.is_partitioned)

    def test_list_source(self):
        """Test creating a list source."""
        source = Sources.list("my-list")

        self.assertIsInstance(source, ListSource)
        self.assertEqual(source.list_name, "my-list")
        self.assertFalse(source.is_partitioned)

    def test_items_source(self):
        """Test creating a test items source."""
        source = Sources.items("a", "b", "c")

        self.assertEqual(source.items, ["a", "b", "c"])


class TestSinks(unittest.TestCase):
    """Tests for Sinks factory."""

    def test_map_sink(self):
        """Test creating a map sink."""
        sink = Sinks.map("my-map")

        self.assertIsInstance(sink, MapSink)
        self.assertEqual(sink.map_name, "my-map")
        self.assertTrue(sink.is_partitioned)

    def test_list_sink(self):
        """Test creating a list sink."""
        sink = Sinks.list("my-list")

        self.assertIsInstance(sink, ListSink)
        self.assertEqual(sink.list_name, "my-list")
        self.assertFalse(sink.is_partitioned)

    def test_logger_sink(self):
        """Test creating a logger sink."""
        sink = Sinks.logger("DEBUG: ")

        self.assertIsInstance(sink, LoggerSink)

    def test_noop_sink(self):
        """Test creating a noop sink."""
        sink = Sinks.noop()

        self.assertIsInstance(sink, NoopSink)


class TestJetService(unittest.TestCase):
    """Tests for JetService class."""

    def setUp(self):
        self.mock_client = type("MockClient", (), {
            "_invocation_service": MockInvocationService(),
            "_serialization_service": None,
        })()
        self.jet_service = JetService(self.mock_client)

    def test_new_job_empty_pipeline(self):
        """Test that submitting an empty pipeline raises an error."""
        pipeline = Pipeline.create()

        with self.assertRaises(ValueError):
            self.jet_service.new_job(pipeline)

    def test_new_job(self):
        """Test submitting a new job."""
        pipeline = Pipeline.create()
        pipeline.read_from(Sources.map("test")).drain_to(Sinks.noop())

        job = self.jet_service.new_job(pipeline)

        self.assertIsNotNone(job)
        self.assertIsInstance(job, Job)

    def test_new_job_with_config(self):
        """Test submitting a job with configuration."""
        pipeline = Pipeline.create()
        pipeline.read_from(Sources.map("test")).drain_to(Sinks.noop())
        config = JobConfig(name="my-job", auto_scaling=False)

        job = self.jet_service.new_job(pipeline, config)

        self.assertIsNotNone(job)
        self.assertEqual(job.name, "my-job")

    def test_new_light_job(self):
        """Test submitting a new light job."""
        pipeline = Pipeline.create()
        pipeline.read_from(Sources.map("test")).drain_to(Sinks.noop())

        job = self.jet_service.new_light_job(pipeline)

        self.assertIsNotNone(job)
        self.assertTrue(job.is_light_job)

    def test_generate_job_id(self):
        """Test job ID generation."""
        id1 = self.jet_service._generate_job_id()
        id2 = self.jet_service._generate_job_id()

        self.assertNotEqual(id1, id2)
        self.assertIsInstance(id1, int)

    def test_serialize_pipeline(self):
        """Test pipeline serialization."""
        pipeline = Pipeline.create()
        pipeline.read_from(Sources.map("test"))

        data = self.jet_service._serialize_pipeline(pipeline)

        self.assertIsInstance(data, bytes)
        self.assertIn(b"test", data)

    def test_serialize_config(self):
        """Test config serialization."""
        config = JobConfig(name="test")

        data = self.jet_service._serialize_config(config)

        self.assertIsInstance(data, bytes)
        self.assertIn(b"test", data)


class TestJobMetrics(unittest.TestCase):
    """Tests for JobMetrics class."""

    def test_job_metrics_defaults(self):
        """Test JobMetrics default values."""
        metrics = JobMetrics()

        self.assertEqual(metrics.timestamp, 0)
        self.assertEqual(metrics.metrics, {})

    def test_job_metrics_get(self):
        """Test JobMetrics get method."""
        metrics = JobMetrics(metrics={"count": 100, "rate": 50.5})

        self.assertEqual(metrics.get("count"), 100)
        self.assertEqual(metrics.get("rate"), 50.5)
        self.assertIsNone(metrics.get("missing"))
        self.assertEqual(metrics.get("missing", 0), 0)

    def test_job_metrics_repr(self):
        """Test JobMetrics repr."""
        metrics = JobMetrics(timestamp=12345, metrics={"a": 1, "b": 2})
        repr_str = repr(metrics)

        self.assertIn("12345", repr_str)
        self.assertIn("2", repr_str)


class TestProcessingGuarantee(unittest.TestCase):
    """Tests for ProcessingGuarantee enum."""

    def test_processing_guarantee_values(self):
        """Test ProcessingGuarantee enum values."""
        self.assertEqual(ProcessingGuarantee.NONE.value, "NONE")
        self.assertEqual(ProcessingGuarantee.AT_LEAST_ONCE.value, "AT_LEAST_ONCE")
        self.assertEqual(ProcessingGuarantee.EXACTLY_ONCE.value, "EXACTLY_ONCE")


if __name__ == "__main__":
    unittest.main()
