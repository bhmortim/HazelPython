"""Tests for Hazelcast Jet service and pipeline API."""

import pytest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from hazelcast.jet.pipeline import (
    Pipeline,
    Stage,
    StageWithKey,
    WindowedStage,
    Source,
    BatchSource,
    StreamSource,
    MapSource,
    ListSource,
    FileSource,
    SocketSource,
    JdbcSource,
    KafkaSource,
    TestSource,
    Sink,
    MapSink,
    ListSink,
    LoggerSink,
    FileSink,
    SocketSink,
    JdbcSink,
    KafkaSink,
    NoopSink,
    WindowDefinition,
    WindowType,
    AggregateOperation,
)
from hazelcast.jet.job import (
    Job,
    JobConfig,
    JobStatus,
    JobMetrics,
    ProcessingGuarantee,
)
from hazelcast.jet.service import JetService, JobStateListener
from hazelcast.exceptions import IllegalStateException, IllegalArgumentException


class TestPipelineCreation:
    """Tests for Pipeline creation and basic operations."""

    def test_create_pipeline(self):
        """Test creating a new pipeline."""
        pipeline = Pipeline.create()
        assert pipeline is not None
        assert pipeline.id is not None
        assert pipeline.source is None
        assert pipeline.sink is None
        assert not pipeline.is_complete()

    def test_pipeline_from_list(self):
        """Test creating a batch source from a list."""
        source = Pipeline.from_list("numbers", [1, 2, 3, 4, 5])
        assert isinstance(source, BatchSource)
        assert source.name == "numbers"
        assert source.items == [1, 2, 3, 4, 5]
        assert source.source_type == "batch"

    def test_pipeline_from_map(self):
        """Test creating a source from an IMap."""
        source = Pipeline.from_map("my-map")
        assert isinstance(source, MapSource)
        assert source.name == "my-map"
        assert source.map_name == "my-map"
        assert source.source_type == "map"

    def test_pipeline_to_list(self):
        """Test creating a sink to an IList."""
        sink = Pipeline.to_list("results")
        assert isinstance(sink, ListSink)
        assert sink.name == "results"
        assert sink.list_name == "results"
        assert sink.sink_type == "list"

    def test_pipeline_to_map(self):
        """Test creating a sink to an IMap."""
        sink = Pipeline.to_map("output")
        assert isinstance(sink, MapSink)
        assert sink.name == "output"
        assert sink.map_name == "output"
        assert sink.sink_type == "map"

    def test_pipeline_to_logger(self):
        """Test creating a logger sink."""
        sink = Pipeline.to_logger()
        assert isinstance(sink, LoggerSink)
        assert sink.sink_type == "logger"

    def test_pipeline_noop_sink(self):
        """Test creating a noop sink."""
        sink = Pipeline.noop()
        assert isinstance(sink, NoopSink)
        assert sink.sink_type == "noop"

    def test_complete_pipeline(self):
        """Test building a complete pipeline."""
        pipeline = Pipeline.create()
        source = Pipeline.from_list("input", [1, 2, 3])
        sink = Pipeline.to_list("output")

        pipeline.read_from(source).write_to(sink)

        assert pipeline.is_complete()
        assert pipeline.source == source
        assert pipeline.sink == sink


class TestPipelineTransformations:
    """Tests for pipeline stage transformations."""

    def test_map_transformation(self):
        """Test map transformation."""
        pipeline = Pipeline.create()
        source = Pipeline.from_list("input", [1, 2, 3])

        stage = pipeline.read_from(source)
        mapped = stage.map(lambda x: x * 2)

        assert isinstance(mapped, Stage)
        assert "map" in mapped.name

    def test_filter_transformation(self):
        """Test filter transformation."""
        pipeline = Pipeline.create()
        source = Pipeline.from_list("input", [1, 2, 3, 4, 5])

        stage = pipeline.read_from(source)
        filtered = stage.filter(lambda x: x % 2 == 0)

        assert isinstance(filtered, Stage)
        assert "filter" in filtered.name

    def test_flat_map_transformation(self):
        """Test flatMap transformation."""
        pipeline = Pipeline.create()
        source = Pipeline.from_list("input", ["a b", "c d"])

        stage = pipeline.read_from(source)
        flat_mapped = stage.flat_map(lambda x: x.split())

        assert isinstance(flat_mapped, Stage)
        assert "flatmap" in flat_mapped.name

    def test_peek_transformation(self):
        """Test peek transformation."""
        pipeline = Pipeline.create()
        source = Pipeline.from_list("input", [1, 2, 3])

        items = []
        stage = pipeline.read_from(source)
        peeked = stage.peek(lambda x: items.append(x))

        assert isinstance(peeked, Stage)
        assert "peek" in peeked.name

    def test_group_by_transformation(self):
        """Test groupBy transformation."""
        pipeline = Pipeline.create()
        source = Pipeline.from_list("input", [1, 2, 3, 4, 5])

        stage = pipeline.read_from(source)
        grouped = stage.group_by(lambda x: x % 2)

        assert isinstance(grouped, Stage)
        assert "groupby" in grouped.name

    def test_distinct_transformation(self):
        """Test distinct transformation."""
        pipeline = Pipeline.create()
        source = Pipeline.from_list("input", [1, 1, 2, 2, 3])

        stage = pipeline.read_from(source)
        distinct = stage.distinct()

        assert isinstance(distinct, Stage)
        assert "distinct" in distinct.name

    def test_sort_transformation(self):
        """Test sort transformation."""
        pipeline = Pipeline.create()
        source = Pipeline.from_list("input", [3, 1, 2])

        stage = pipeline.read_from(source)
        sorted_stage = stage.sort()

        assert isinstance(sorted_stage, Stage)
        assert "sort" in sorted_stage.name

    def test_aggregate_transformation(self):
        """Test aggregate transformation."""
        pipeline = Pipeline.create()
        source = Pipeline.from_list("input", [1, 2, 3])

        stage = pipeline.read_from(source)
        aggregated = stage.aggregate(lambda acc, x: acc + x, 0)

        assert isinstance(aggregated, Stage)
        assert "aggregate" in aggregated.name

    def test_chained_transformations(self):
        """Test chaining multiple transformations."""
        pipeline = Pipeline.create()
        source = Pipeline.from_list("input", [1, 2, 3, 4, 5])
        sink = Pipeline.to_list("output")

        pipeline.read_from(source) \
            .filter(lambda x: x > 2) \
            .map(lambda x: x * 2) \
            .write_to(sink)

        assert pipeline.is_complete()
        assert len(pipeline.stages) == 3


class TestGroupingKeyAndWindowing:
    """Tests for grouping key and windowing operations."""

    def test_grouping_key(self):
        """Test groupingKey creates StageWithKey."""
        pipeline = Pipeline.create()
        source = Pipeline.from_list("input", [{"k": 1, "v": "a"}, {"k": 2, "v": "b"}])

        stage = pipeline.read_from(source)
        keyed = stage.grouping_key(lambda x: x["k"])

        assert isinstance(keyed, StageWithKey)

    def test_keyed_aggregate(self):
        """Test aggregation on keyed stage."""
        pipeline = Pipeline.create()
        source = Pipeline.from_list("input", [1, 2, 3, 4, 5])

        stage = pipeline.read_from(source)
        result = stage.grouping_key(lambda x: x % 2).aggregate(
            AggregateOperation.counting()
        )

        assert isinstance(result, Stage)

    def test_tumbling_window(self):
        """Test tumbling window definition."""
        window = WindowDefinition.tumbling(1000)
        assert window.window_type == WindowType.TUMBLING
        assert window.size_millis == 1000
        assert window.slide_millis == 1000

    def test_sliding_window(self):
        """Test sliding window definition."""
        window = WindowDefinition.sliding(1000, 500)
        assert window.window_type == WindowType.SLIDING
        assert window.size_millis == 1000
        assert window.slide_millis == 500

    def test_session_window(self):
        """Test session window definition."""
        window = WindowDefinition.session(5000)
        assert window.window_type == WindowType.SESSION
        assert window.gap_millis == 5000

    def test_windowed_stage(self):
        """Test creating a windowed stage."""
        pipeline = Pipeline.create()
        source = Pipeline.from_list("input", [1, 2, 3])

        stage = pipeline.read_from(source)
        windowed = stage.grouping_key(lambda x: x).window(
            WindowDefinition.tumbling(1000)
        )

        assert isinstance(windowed, WindowedStage)


class TestAggregateOperations:
    """Tests for aggregate operations."""

    def test_counting(self):
        """Test counting aggregate operation."""
        op = AggregateOperation.counting()
        acc = op.create_fn()
        acc = op.accumulate_fn(acc, "item")
        acc = op.accumulate_fn(acc, "item")
        result = op.finish_fn(acc)
        assert result == 2

    def test_summing(self):
        """Test summing aggregate operation."""
        op = AggregateOperation.summing()
        acc = op.create_fn()
        acc = op.accumulate_fn(acc, 10)
        acc = op.accumulate_fn(acc, 20)
        result = op.finish_fn(acc)
        assert result == 30.0

    def test_summing_with_extractor(self):
        """Test summing with value extractor."""
        op = AggregateOperation.summing(lambda x: x["value"])
        acc = op.create_fn()
        acc = op.accumulate_fn(acc, {"value": 10})
        acc = op.accumulate_fn(acc, {"value": 20})
        result = op.finish_fn(acc)
        assert result == 30.0

    def test_averaging(self):
        """Test averaging aggregate operation."""
        op = AggregateOperation.averaging()
        acc = op.create_fn()
        acc = op.accumulate_fn(acc, 10)
        acc = op.accumulate_fn(acc, 20)
        acc = op.accumulate_fn(acc, 30)
        result = op.finish_fn(acc)
        assert result == 20.0

    def test_min_by(self):
        """Test min_by aggregate operation."""
        op = AggregateOperation.min_by()
        acc = op.create_fn()
        acc = op.accumulate_fn(acc, 30)
        acc = op.accumulate_fn(acc, 10)
        acc = op.accumulate_fn(acc, 20)
        result = op.finish_fn(acc)
        assert result == 10

    def test_max_by(self):
        """Test max_by aggregate operation."""
        op = AggregateOperation.max_by()
        acc = op.create_fn()
        acc = op.accumulate_fn(acc, 10)
        acc = op.accumulate_fn(acc, 30)
        acc = op.accumulate_fn(acc, 20)
        result = op.finish_fn(acc)
        assert result == 30

    def test_to_list(self):
        """Test to_list aggregate operation."""
        op = AggregateOperation.to_list()
        acc = op.create_fn()
        acc = op.accumulate_fn(acc, "a")
        acc = op.accumulate_fn(acc, "b")
        result = op.finish_fn(acc)
        assert result == ["a", "b"]

    def test_to_set(self):
        """Test to_set aggregate operation."""
        op = AggregateOperation.to_set()
        acc = op.create_fn()
        acc = op.accumulate_fn(acc, "a")
        acc = op.accumulate_fn(acc, "b")
        acc = op.accumulate_fn(acc, "a")
        result = op.finish_fn(acc)
        assert result == {"a", "b"}


class TestSources:
    """Tests for source connectors."""

    def test_file_source(self):
        """Test file source creation."""
        source = FileSource("files", "/data", "*.txt", True)
        assert source.source_type == "file"
        assert source.directory == "/data"
        assert source.glob == "*.txt"
        assert source.shared_file_system is True

    def test_socket_source(self):
        """Test socket source creation."""
        source = SocketSource("socket", "localhost", 9999)
        assert source.source_type == "socket"
        assert source.host == "localhost"
        assert source.port == 9999

    def test_jdbc_source(self):
        """Test JDBC source creation."""
        source = JdbcSource(
            "db",
            "jdbc:postgresql://localhost/test",
            "SELECT * FROM users",
            "user",
            "pass"
        )
        assert source.source_type == "jdbc"
        assert "postgresql" in source.connection_url
        assert "users" in source.query

    def test_kafka_source(self):
        """Test Kafka source creation."""
        source = KafkaSource(
            "kafka",
            "events",
            "localhost:9092",
            "my-group",
            {"auto.offset.reset": "earliest"}
        )
        assert source.source_type == "kafka"
        assert source.topic == "events"
        assert source.bootstrap_servers == "localhost:9092"
        assert source.group_id == "my-group"

    def test_test_source(self):
        """Test test source creation."""
        source = TestSource("test", 100)
        assert source.source_type == "test"
        assert source.items_per_second == 100


class TestSinks:
    """Tests for sink connectors."""

    def test_file_sink(self):
        """Test file sink creation."""
        sink = FileSink("files", "/output")
        assert sink.sink_type == "file"
        assert sink.directory == "/output"

    def test_socket_sink(self):
        """Test socket sink creation."""
        sink = SocketSink("socket", "localhost", 9999)
        assert sink.sink_type == "socket"
        assert sink.host == "localhost"
        assert sink.port == 9999

    def test_jdbc_sink(self):
        """Test JDBC sink creation."""
        sink = JdbcSink(
            "db",
            "jdbc:postgresql://localhost/test",
            "INSERT INTO users VALUES (?, ?)"
        )
        assert sink.sink_type == "jdbc"
        assert "INSERT" in sink.update_query

    def test_kafka_sink(self):
        """Test Kafka sink creation."""
        sink = KafkaSink("kafka", "output-topic", "localhost:9092")
        assert sink.sink_type == "kafka"
        assert sink.topic == "output-topic"


class TestJobConfig:
    """Tests for JobConfig."""

    def test_default_config(self):
        """Test default job configuration."""
        config = JobConfig()
        assert config.name is None
        assert config.auto_scaling_enabled is True
        assert config.split_brain_protection_enabled is False
        assert config.processing_guarantee == ProcessingGuarantee.NONE
        assert config.snapshot_interval_millis == 10000
        assert config.metrics_enabled is True

    def test_named_config(self):
        """Test named job configuration."""
        config = JobConfig(name="my-job")
        assert config.name == "my-job"

    def test_processing_guarantee(self):
        """Test processing guarantee settings."""
        config = JobConfig(processing_guarantee=ProcessingGuarantee.EXACTLY_ONCE)
        assert config.processing_guarantee == ProcessingGuarantee.EXACTLY_ONCE

    def test_config_arguments(self):
        """Test job config arguments."""
        config = JobConfig()
        config.set_argument("key1", "value1")
        config.set_argument("key2", 42)

        assert config.get_argument("key1") == "value1"
        assert config.get_argument("key2") == 42
        assert config.get_argument("missing", "default") == "default"

    def test_timeout_config(self):
        """Test timeout configuration."""
        config = JobConfig(timeout_millis=60000)
        assert config.timeout_millis == 60000

    def test_suspend_on_failure(self):
        """Test suspend on failure configuration."""
        config = JobConfig(suspend_on_failure=True)
        assert config.suspend_on_failure is True


class TestJobMetrics:
    """Tests for JobMetrics."""

    def test_metrics_creation(self):
        """Test creating job metrics."""
        metrics = JobMetrics()
        assert metrics.items_in == 0
        assert metrics.items_out == 0
        assert metrics.snapshot_count == 0

    def test_metrics_recording(self):
        """Test recording metrics."""
        metrics = JobMetrics()
        metrics.items_in = 100
        metrics.items_out = 50
        metrics.set("custom_metric", 42)

        assert metrics.items_in == 100
        assert metrics.items_out == 50
        assert metrics.get("custom_metric") == 42

    def test_snapshot_recording(self):
        """Test recording snapshots."""
        metrics = JobMetrics()
        metrics.record_snapshot()
        metrics.record_snapshot()

        assert metrics.snapshot_count == 2
        assert metrics.last_snapshot_time is not None

    def test_to_dict(self):
        """Test converting metrics to dict."""
        metrics = JobMetrics()
        metrics.items_in = 100
        metrics.items_out = 50

        result = metrics.to_dict()
        assert result["items_in"] == 100
        assert result["items_out"] == 50
        assert "timestamp" in result


class TestJob:
    """Tests for Job class."""

    def test_job_creation(self):
        """Test job creation."""
        job = Job(1, "test-job")
        assert job.id == 1
        assert job.name == "test-job"
        assert job.status == JobStatus.NOT_RUNNING

    def test_job_start(self):
        """Test starting a job."""
        job = Job(1)
        job._start()

        assert job.status == JobStatus.RUNNING
        assert job.submission_time is not None

    def test_job_complete(self):
        """Test completing a job."""
        job = Job(1)
        job._start()
        job._complete()

        assert job.status == JobStatus.COMPLETED
        assert job.completion_time is not None
        assert job.is_terminal()

    def test_job_fail(self):
        """Test failing a job."""
        job = Job(1)
        job._start()
        job._fail("Test failure")

        assert job.status == JobStatus.FAILED
        assert job.failure_reason == "Test failure"
        assert job.is_terminal()

    def test_job_suspend_resume(self):
        """Test suspending and resuming a job."""
        job = Job(1)
        job._start()

        job.suspend()
        assert job.status == JobStatus.SUSPENDED

        job.resume()
        assert job.status == JobStatus.RUNNING

    def test_job_cancel(self):
        """Test cancelling a job."""
        job = Job(1)
        job._start()

        job.cancel()

        assert job.status == JobStatus.COMPLETED
        assert job.is_user_cancelled

    def test_job_restart(self):
        """Test restarting a job."""
        job = Job(1)
        job._start()

        job.restart()

        assert job.status == JobStatus.RUNNING

    def test_export_snapshot(self):
        """Test exporting a snapshot."""
        job = Job(1)
        job._start()

        future = job.export_snapshot("snapshot-1")
        future.result()

        assert "snapshot-1" in job.snapshots
        assert job.metrics.snapshot_count == 1

    def test_cancel_and_export_snapshot(self):
        """Test cancel and export snapshot."""
        job = Job(1)
        job._start()

        job.cancel_and_export_snapshot("final-snapshot")

        assert job.status == JobStatus.COMPLETED
        assert job.is_user_cancelled
        assert "final-snapshot" in job.snapshots

    def test_get_metrics(self):
        """Test getting job metrics."""
        config = JobConfig(metrics_enabled=True)
        job = Job(1, "test", config)
        job._start()
        job.metrics.items_in = 100

        metrics = job.get_metrics()

        assert metrics["status"] == "RUNNING"
        assert metrics["items_in"] == 100

    def test_get_id_string(self):
        """Test getting job ID as string."""
        job = Job(255)
        assert job.get_id_string() == "00000000000000ff"

    def test_join_completed_job(self):
        """Test joining a completed job."""
        job = Job(1)
        job._start()
        job._complete()

        result = job.join(timeout=1.0)
        assert result == job


class TestJetService:
    """Tests for JetService."""

    def test_service_creation(self):
        """Test service creation."""
        service = JetService()
        assert not service.is_running

    def test_service_start_shutdown(self):
        """Test starting and shutting down service."""
        service = JetService()
        service.start()
        assert service.is_running

        service.shutdown()
        assert not service.is_running

    def test_submit_pipeline(self):
        """Test submitting a pipeline."""
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        source = Pipeline.from_list("input", [1, 2, 3])
        sink = Pipeline.to_list("output")
        pipeline.read_from(source).write_to(sink)

        job = service.submit(pipeline)

        assert job is not None
        assert job.id == 1
        assert job.status == JobStatus.RUNNING

    def test_submit_with_config(self):
        """Test submitting with job config."""
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        pipeline.read_from(Pipeline.from_list("i", [1])).write_to(Pipeline.to_list("o"))

        config = JobConfig(name="named-job")
        job = service.submit(pipeline, config)

        assert job.name == "named-job"

    def test_submit_incomplete_pipeline(self):
        """Test submitting incomplete pipeline fails."""
        service = JetService()
        service.start()

        pipeline = Pipeline.create()

        with pytest.raises(Exception):
            service.submit(pipeline)

    def test_submit_when_not_running(self):
        """Test submitting when service not running fails."""
        service = JetService()
        pipeline = Pipeline.create()
        pipeline.read_from(Pipeline.from_list("i", [1])).write_to(Pipeline.to_list("o"))

        with pytest.raises(IllegalStateException):
            service.submit(pipeline)

    def test_get_job(self):
        """Test getting a job by ID."""
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        pipeline.read_from(Pipeline.from_list("i", [1])).write_to(Pipeline.to_list("o"))

        job = service.submit(pipeline)
        retrieved = service.get_job(job.id)

        assert retrieved == job

    def test_get_job_not_found(self):
        """Test getting non-existent job returns None."""
        service = JetService()
        service.start()

        result = service.get_job(999)
        assert result is None

    def test_get_job_by_name(self):
        """Test getting a job by name."""
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        pipeline.read_from(Pipeline.from_list("i", [1])).write_to(Pipeline.to_list("o"))

        config = JobConfig(name="findable")
        job = service.submit(pipeline, config)

        retrieved = service.get_job_by_name("findable")
        assert retrieved == job

    def test_get_jobs(self):
        """Test getting all jobs."""
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        pipeline.read_from(Pipeline.from_list("i", [1])).write_to(Pipeline.to_list("o"))

        service.submit(pipeline)
        service.submit(pipeline)

        jobs = service.get_jobs()
        assert len(jobs) == 2

    def test_new_job_if_absent(self):
        """Test new_job_if_absent."""
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        pipeline.read_from(Pipeline.from_list("i", [1])).write_to(Pipeline.to_list("o"))

        config = JobConfig(name="unique-job")
        job1 = service.new_job_if_absent(pipeline, config)
        job2 = service.new_job_if_absent(pipeline, config)

        assert job1 == job2

    def test_new_light_job(self):
        """Test submitting a light job."""
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        pipeline.read_from(Pipeline.from_list("i", [1])).write_to(Pipeline.to_list("o"))

        job = service.new_light_job(pipeline)

        assert job.is_light_job
        assert job.config.processing_guarantee == ProcessingGuarantee.NONE

    def test_get_active_jobs(self):
        """Test getting active jobs."""
        service = JetService()
        service.start()

        pipeline = Pipeline.create()
        pipeline.read_from(Pipeline.from_list("i", [1])).write_to(Pipeline.to_list("o"))

        job1 = service.submit(pipeline)
        job2 = service.submit(pipeline)
        job2._complete()

        active = service.get_active_jobs()
        assert len(active) == 1
        assert job1 in active

    def test_job_state_listener(self):
        """Test job state listener."""
        service = JetService()
        service.start()

        events = []

        class TestListener(JobStateListener):
            def on_state_changed(self, job, old_status, new_status):
                events.append(("changed", job.id, new_status))

            def on_job_completed(self, job):
                events.append(("completed", job.id))

        listener_id = service.add_job_state_listener(TestListener())
        assert listener_id is not None

        removed = service.remove_job_state_listener(listener_id)
        assert removed is True

    def test_repr(self):
        """Test service string representation."""
        service = JetService()
        service.start()

        repr_str = repr(service)
        assert "JetService" in repr_str
        assert "running=True" in repr_str
