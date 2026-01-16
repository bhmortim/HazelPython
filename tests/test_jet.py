"""Tests for hazelcast/jet/pipeline.py, job.py, and service.py modules."""

import pytest
import time
from datetime import datetime
from unittest.mock import Mock, MagicMock
from concurrent.futures import Future

from hazelcast.jet.pipeline import (
    WindowType,
    WindowDefinition,
    AggregateOperation,
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
    Stage,
    StageWithKey,
    WindowedStage,
    Pipeline,
)
from hazelcast.jet.job import (
    JobStatus,
    ProcessingGuarantee,
    JobConfig,
    JobMetrics,
    Job,
)
from hazelcast.jet.service import (
    JobStateListener,
    JetService,
)
from hazelcast.exceptions import IllegalStateException, IllegalArgumentException


class TestWindowType:
    """Tests for WindowType enum."""

    def test_all_types_exist(self):
        """Test all window types exist."""
        assert WindowType.TUMBLING.value == "TUMBLING"
        assert WindowType.SLIDING.value == "SLIDING"
        assert WindowType.SESSION.value == "SESSION"


class TestWindowDefinition:
    """Tests for WindowDefinition class."""

    def test_tumbling_window(self):
        """Test tumbling window creation."""
        window = WindowDefinition.tumbling(5000)
        assert window.window_type == WindowType.TUMBLING
        assert window.size_millis == 5000
        assert window.slide_millis == 5000

    def test_sliding_window(self):
        """Test sliding window creation."""
        window = WindowDefinition.sliding(10000, 2000)
        assert window.window_type == WindowType.SLIDING
        assert window.size_millis == 10000
        assert window.slide_millis == 2000

    def test_session_window(self):
        """Test session window creation."""
        window = WindowDefinition.session(30000)
        assert window.window_type == WindowType.SESSION
        assert window.gap_millis == 30000

    def test_repr(self):
        """Test __repr__."""
        window = WindowDefinition.tumbling(5000)
        repr_str = repr(window)
        assert "TUMBLING" in repr_str
        assert "5000" in repr_str


class TestAggregateOperation:
    """Tests for AggregateOperation class."""

    def test_counting(self):
        """Test counting aggregate operation."""
        op = AggregateOperation.counting()
        
        acc = op.create_fn()
        assert acc == 0
        
        acc = op.accumulate_fn(acc, "item1")
        acc = op.accumulate_fn(acc, "item2")
        assert acc == 2
        
        result = op.finish_fn(acc)
        assert result == 2

    def test_counting_combine(self):
        """Test counting combine function."""
        op = AggregateOperation.counting()
        assert op.combine_fn(5, 3) == 8

    def test_counting_deduct(self):
        """Test counting deduct function."""
        op = AggregateOperation.counting()
        assert op.deduct_fn(5, "item") == 4

    def test_summing(self):
        """Test summing aggregate operation."""
        op = AggregateOperation.summing()
        
        acc = op.create_fn()
        acc = op.accumulate_fn(acc, 10)
        acc = op.accumulate_fn(acc, 20)
        acc = op.accumulate_fn(acc, 30)
        
        result = op.finish_fn(acc)
        assert result == 60.0

    def test_summing_with_extractor(self):
        """Test summing with value extractor."""
        op = AggregateOperation.summing(lambda x: x["value"])
        
        acc = op.create_fn()
        acc = op.accumulate_fn(acc, {"value": 10})
        acc = op.accumulate_fn(acc, {"value": 20})
        
        assert op.finish_fn(acc) == 30.0

    def test_averaging(self):
        """Test averaging aggregate operation."""
        op = AggregateOperation.averaging()
        
        acc = op.create_fn()
        acc = op.accumulate_fn(acc, 10)
        acc = op.accumulate_fn(acc, 20)
        acc = op.accumulate_fn(acc, 30)
        
        result = op.finish_fn(acc)
        assert result == 20.0

    def test_averaging_empty(self):
        """Test averaging with no items."""
        op = AggregateOperation.averaging()
        acc = op.create_fn()
        result = op.finish_fn(acc)
        assert result == 0.0

    def test_min_by(self):
        """Test min_by aggregate operation."""
        op = AggregateOperation.min_by()
        
        acc = op.create_fn()
        acc = op.accumulate_fn(acc, 30)
        acc = op.accumulate_fn(acc, 10)
        acc = op.accumulate_fn(acc, 20)
        
        result = op.finish_fn(acc)
        assert result == 10

    def test_min_by_with_key(self):
        """Test min_by with key function."""
        op = AggregateOperation.min_by(lambda x: x["score"])
        
        acc = op.create_fn()
        acc = op.accumulate_fn(acc, {"name": "A", "score": 90})
        acc = op.accumulate_fn(acc, {"name": "B", "score": 60})
        
        result = op.finish_fn(acc)
        assert result["name"] == "B"

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

    def test_repr(self):
        """Test __repr__."""
        op = AggregateOperation.counting()
        repr_str = repr(op)
        assert "AggregateOperation" in repr_str


class TestSources:
    """Tests for Source classes."""

    def test_batch_source(self):
        """Test BatchSource."""
        source = BatchSource("test", [1, 2, 3])
        assert source.name == "test"
        assert source.source_type == "batch"
        assert source.items == [1, 2, 3]

    def test_stream_source(self):
        """Test StreamSource."""
        source = StreamSource("events")
        assert source.name == "events"
        assert source.source_type == "stream"

    def test_map_source(self):
        """Test MapSource."""
        source = MapSource("my-map")
        assert source.source_type == "map"
        assert source.map_name == "my-map"

    def test_list_source(self):
        """Test ListSource."""
        source = ListSource("my-list")
        assert source.source_type == "list"
        assert source.list_name == "my-list"

    def test_file_source(self):
        """Test FileSource."""
        source = FileSource("files", "/data", "*.csv", True)
        assert source.source_type == "file"
        assert source.directory == "/data"
        assert source.glob == "*.csv"
        assert source.shared_file_system is True

    def test_socket_source(self):
        """Test SocketSource."""
        source = SocketSource("socket", "localhost", 9999)
        assert source.source_type == "socket"
        assert source.host == "localhost"
        assert source.port == 9999

    def test_jdbc_source(self):
        """Test JdbcSource."""
        source = JdbcSource(
            "db",
            "jdbc:mysql://localhost/test",
            "SELECT * FROM users",
            "root",
            "password",
        )
        assert source.source_type == "jdbc"
        assert source.connection_url == "jdbc:mysql://localhost/test"
        assert source.query == "SELECT * FROM users"

    def test_kafka_source(self):
        """Test KafkaSource."""
        source = KafkaSource(
            "kafka",
            "my-topic",
            "localhost:9092",
            "my-group",
            {"auto.offset.reset": "earliest"},
        )
        assert source.source_type == "kafka"
        assert source.topic == "my-topic"
        assert source.bootstrap_servers == "localhost:9092"
        assert source.group_id == "my-group"
        assert "auto.offset.reset" in source.properties

    def test_test_source(self):
        """Test TestSource."""
        gen = lambda i: f"item-{i}"
        source = TestSource("test", 100, gen)
        assert source.source_type == "test"
        assert source.items_per_second == 100
        assert source.item_generator(1) == "item-1"

    def test_source_repr(self):
        """Test Source __repr__."""
        source = BatchSource("test", [])
        repr_str = repr(source)
        assert "BatchSource" in repr_str
        assert "test" in repr_str


class TestSinks:
    """Tests for Sink classes."""

    def test_map_sink(self):
        """Test MapSink."""
        sink = MapSink("results")
        assert sink.name == "results"
        assert sink.sink_type == "map"
        assert sink.map_name == "results"

    def test_list_sink(self):
        """Test ListSink."""
        sink = ListSink("output")
        assert sink.sink_type == "list"
        assert sink.list_name == "output"

    def test_logger_sink(self):
        """Test LoggerSink."""
        sink = LoggerSink()
        assert sink.sink_type == "logger"
        assert sink.name == "logger"

    def test_file_sink(self):
        """Test FileSink."""
        sink = FileSink("output", "/results", str)
        assert sink.sink_type == "file"
        assert sink.directory == "/results"
        assert sink.to_string_fn("test") == "test"

    def test_socket_sink(self):
        """Test SocketSink."""
        sink = SocketSink("socket", "localhost", 8080)
        assert sink.sink_type == "socket"
        assert sink.host == "localhost"
        assert sink.port == 8080

    def test_jdbc_sink(self):
        """Test JdbcSink."""
        sink = JdbcSink(
            "db",
            "jdbc:mysql://localhost/test",
            "INSERT INTO results VALUES (?, ?)",
        )
        assert sink.sink_type == "jdbc"
        assert sink.connection_url == "jdbc:mysql://localhost/test"
        assert sink.update_query == "INSERT INTO results VALUES (?, ?)"

    def test_kafka_sink(self):
        """Test KafkaSink."""
        sink = KafkaSink("kafka", "output-topic", "localhost:9092")
        assert sink.sink_type == "kafka"
        assert sink.topic == "output-topic"
        assert sink.bootstrap_servers == "localhost:9092"

    def test_noop_sink(self):
        """Test NoopSink."""
        sink = NoopSink()
        assert sink.sink_type == "noop"

    def test_sink_repr(self):
        """Test Sink __repr__."""
        sink = MapSink("results")
        repr_str = repr(sink)
        assert "MapSink" in repr_str


class TestStage:
    """Tests for Stage class."""

    @pytest.fixture
    def pipeline(self):
        """Create a Pipeline."""
        return Pipeline.create()

    @pytest.fixture
    def stage(self, pipeline):
        """Create a Stage."""
        source = BatchSource("numbers", [1, 2, 3, 4, 5])
        return pipeline.read_from(source)

    def test_name_property(self, stage):
        """Test name property."""
        assert "source" in stage.name

    def test_pipeline_property(self, stage, pipeline):
        """Test pipeline property."""
        assert stage.pipeline is pipeline

    def test_map(self, stage):
        """Test map transformation."""
        mapped = stage.map(lambda x: x * 2)
        assert "map" in mapped.name
        assert len(mapped._transforms) > 0

    def test_filter(self, stage):
        """Test filter transformation."""
        filtered = stage.filter(lambda x: x > 2)
        assert "filter" in filtered.name

    def test_flat_map(self, stage):
        """Test flat_map transformation."""
        flat_mapped = stage.flat_map(lambda x: [x, x])
        assert "flatmap" in flat_mapped.name

    def test_peek(self, stage):
        """Test peek transformation."""
        peeked = stage.peek(lambda x: print(x))
        assert "peek" in peeked.name

    def test_group_by(self, stage):
        """Test group_by transformation."""
        grouped = stage.group_by(lambda x: x % 2)
        assert "groupby" in grouped.name

    def test_distinct(self, stage):
        """Test distinct transformation."""
        distinct = stage.distinct()
        assert "distinct" in distinct.name

    def test_sort(self, stage):
        """Test sort transformation."""
        sorted_stage = stage.sort()
        assert "sort" in sorted_stage.name

    def test_sort_with_key(self, stage):
        """Test sort with key function."""
        sorted_stage = stage.sort(key_fn=lambda x: -x)
        assert "sort" in sorted_stage.name

    def test_aggregate(self, stage):
        """Test aggregate transformation."""
        aggregated = stage.aggregate(AggregateOperation.summing())
        assert "aggregate" in aggregated.name

    def test_aggregate_with_function(self, stage):
        """Test aggregate with accumulator function."""
        aggregated = stage.aggregate(lambda acc, x: acc + x, initial=0)
        assert "aggregate" in aggregated.name

    def test_grouping_key(self, stage):
        """Test grouping_key method."""
        keyed = stage.grouping_key(lambda x: x % 2)
        assert isinstance(keyed, StageWithKey)
        assert keyed.key_fn(5) == 1

    def test_add_timestamps(self, stage):
        """Test add_timestamps transformation."""
        timestamped = stage.add_timestamps(lambda x: x * 1000, 5000)
        assert "timestamps" in timestamped.name

    def test_rebalance(self, stage):
        """Test rebalance transformation."""
        rebalanced = stage.rebalance()
        assert "rebalance" in rebalanced.name

    def test_merge(self, pipeline):
        """Test merge transformation."""
        stage1 = pipeline.read_from(BatchSource("s1", [1, 2]))
        stage2 = pipeline.read_from(BatchSource("s2", [3, 4]))
        merged = stage1.merge(stage2)
        assert "merge" in merged.name

    def test_hash_join(self, pipeline):
        """Test hash_join transformation."""
        stage1 = pipeline.read_from(BatchSource("s1", [1, 2]))
        stage2 = pipeline.read_from(BatchSource("s2", [1, 2]))
        joined = stage1.hash_join(stage2, lambda x: x, lambda x: x)
        assert "hashjoin" in joined.name

    def test_apply(self, stage):
        """Test apply custom transformation."""
        def custom_transform(s):
            return s.map(lambda x: x + 1)
        
        result = stage.apply(custom_transform)
        assert "map" in result.name

    def test_write_to(self, stage, pipeline):
        """Test write_to sink."""
        sink = ListSink("output")
        result = stage.write_to(sink)
        assert result is pipeline
        assert pipeline.sink is sink

    def test_repr(self, stage):
        """Test __repr__."""
        repr_str = repr(stage)
        assert "Stage" in repr_str


class TestStageWithKey:
    """Tests for StageWithKey class."""

    @pytest.fixture
    def keyed_stage(self):
        """Create a StageWithKey."""
        pipeline = Pipeline.create()
        source = BatchSource("items", [{"cat": "A", "val": 1}])
        stage = pipeline.read_from(source)
        return stage.grouping_key(lambda x: x["cat"])

    def test_key_fn_property(self, keyed_stage):
        """Test key_fn property."""
        assert keyed_stage.key_fn({"cat": "B"}) == "B"

    def test_pipeline_property(self, keyed_stage):
        """Test pipeline property."""
        assert keyed_stage.pipeline is not None

    def test_aggregate(self, keyed_stage):
        """Test aggregate on keyed stage."""
        result = keyed_stage.aggregate(AggregateOperation.counting())
        assert isinstance(result, Stage)

    def test_window(self, keyed_stage):
        """Test window method."""
        windowed = keyed_stage.window(WindowDefinition.tumbling(5000))
        assert isinstance(windowed, WindowedStage)

    def test_repr(self, keyed_stage):
        """Test __repr__."""
        repr_str = repr(keyed_stage)
        assert "StageWithKey" in repr_str


class TestWindowedStage:
    """Tests for WindowedStage class."""

    @pytest.fixture
    def windowed_stage(self):
        """Create a WindowedStage."""
        pipeline = Pipeline.create()
        source = BatchSource("items", [1, 2, 3])
        stage = pipeline.read_from(source)
        keyed = stage.grouping_key(lambda x: x % 2)
        return keyed.window(WindowDefinition.tumbling(1000))

    def test_window_definition_property(self, windowed_stage):
        """Test window_definition property."""
        assert windowed_stage.window_definition.size_millis == 1000

    def test_aggregate(self, windowed_stage):
        """Test aggregate on windowed stage."""
        result = windowed_stage.aggregate(AggregateOperation.summing())
        assert isinstance(result, Stage)

    def test_repr(self, windowed_stage):
        """Test __repr__."""
        repr_str = repr(windowed_stage)
        assert "WindowedStage" in repr_str


class TestPipeline:
    """Tests for Pipeline class."""

    @pytest.fixture
    def pipeline(self):
        """Create a Pipeline."""
        return Pipeline.create()

    def test_create(self):
        """Test create factory method."""
        pipeline = Pipeline.create()
        assert isinstance(pipeline, Pipeline)
        assert pipeline.id is not None

    def test_id_property(self, pipeline):
        """Test id property."""
        assert len(pipeline.id) > 0

    def test_source_property_initially_none(self, pipeline):
        """Test source property is None initially."""
        assert pipeline.source is None

    def test_sink_property_initially_none(self, pipeline):
        """Test sink property is None initially."""
        assert pipeline.sink is None

    def test_stages_property(self, pipeline):
        """Test stages property."""
        source = BatchSource("test", [])
        pipeline.read_from(source)
        assert len(pipeline.stages) >= 1

    def test_read_from(self, pipeline):
        """Test read_from method."""
        source = BatchSource("numbers", [1, 2, 3])
        stage = pipeline.read_from(source)
        assert pipeline.source is source
        assert isinstance(stage, Stage)

    def test_from_list(self):
        """Test from_list static method."""
        source = Pipeline.from_list("items", [1, 2, 3])
        assert isinstance(source, BatchSource)
        assert source.items == [1, 2, 3]

    def test_from_map(self):
        """Test from_map static method."""
        source = Pipeline.from_map("my-map")
        assert isinstance(source, MapSource)

    def test_to_list(self):
        """Test to_list static method."""
        sink = Pipeline.to_list("output")
        assert isinstance(sink, ListSink)

    def test_to_map(self):
        """Test to_map static method."""
        sink = Pipeline.to_map("results")
        assert isinstance(sink, MapSink)

    def test_to_logger(self):
        """Test to_logger static method."""
        sink = Pipeline.to_logger()
        assert isinstance(sink, LoggerSink)

    def test_to_file(self):
        """Test to_file static method."""
        sink = Pipeline.to_file("output", "/data")
        assert isinstance(sink, FileSink)

    def test_to_socket(self):
        """Test to_socket static method."""
        sink = Pipeline.to_socket("socket", "localhost", 8080)
        assert isinstance(sink, SocketSink)

    def test_to_kafka(self):
        """Test to_kafka static method."""
        sink = Pipeline.to_kafka("kafka", "topic", "localhost:9092")
        assert isinstance(sink, KafkaSink)

    def test_from_file(self):
        """Test from_file static method."""
        source = Pipeline.from_file("files", "/data", "*.txt")
        assert isinstance(source, FileSource)

    def test_from_socket(self):
        """Test from_socket static method."""
        source = Pipeline.from_socket("socket", "localhost", 9999)
        assert isinstance(source, SocketSource)

    def test_from_kafka(self):
        """Test from_kafka static method."""
        source = Pipeline.from_kafka("kafka", "topic", "localhost:9092")
        assert isinstance(source, KafkaSource)

    def test_noop(self):
        """Test noop static method."""
        sink = Pipeline.noop()
        assert isinstance(sink, NoopSink)

    def test_test_source(self):
        """Test test_source static method."""
        source = Pipeline.test_source("test", 50)
        assert isinstance(source, TestSource)

    def test_is_complete_false(self, pipeline):
        """Test is_complete when incomplete."""
        assert pipeline.is_complete() is False
        pipeline.read_from(BatchSource("test", []))
        assert pipeline.is_complete() is False

    def test_is_complete_true(self, pipeline):
        """Test is_complete when complete."""
        source = BatchSource("test", [1, 2])
        sink = ListSink("output")
        pipeline.read_from(source).write_to(sink)
        assert pipeline.is_complete() is True

    def test_repr(self, pipeline):
        """Test __repr__."""
        repr_str = repr(pipeline)
        assert "Pipeline" in repr_str


class TestJobStatus:
    """Tests for JobStatus enum."""

    def test_all_statuses_exist(self):
        """Test all statuses exist."""
        expected = [
            "NOT_RUNNING", "STARTING", "RUNNING", "SUSPENDED",
            "SUSPENDING", "RESUMING", "COMPLETING", "FAILED", "COMPLETED",
        ]
        for status in expected:
            assert hasattr(JobStatus, status)


class TestProcessingGuarantee:
    """Tests for ProcessingGuarantee enum."""

    def test_all_guarantees_exist(self):
        """Test all guarantees exist."""
        assert ProcessingGuarantee.NONE.value == "NONE"
        assert ProcessingGuarantee.AT_LEAST_ONCE.value == "AT_LEAST_ONCE"
        assert ProcessingGuarantee.EXACTLY_ONCE.value == "EXACTLY_ONCE"


class TestJobConfig:
    """Tests for JobConfig class."""

    @pytest.fixture
    def config(self):
        """Create a JobConfig."""
        return JobConfig(name="test-job")

    def test_init_defaults(self):
        """Test initialization with defaults."""
        config = JobConfig()
        assert config.name is None
        assert config.auto_scaling_enabled is True
        assert config.split_brain_protection_enabled is False
        assert config.processing_guarantee == ProcessingGuarantee.NONE
        assert config.metrics_enabled is True

    def test_name_property(self, config):
        """Test name property."""
        assert config.name == "test-job"
        config.name = "new-name"
        assert config.name == "new-name"

    def test_auto_scaling_property(self, config):
        """Test auto_scaling_enabled property."""
        config.auto_scaling_enabled = False
        assert config.auto_scaling_enabled is False

    def test_snapshot_interval_property(self, config):
        """Test snapshot_interval_millis property."""
        config.snapshot_interval_millis = 5000
        assert config.snapshot_interval_millis == 5000

    def test_processing_guarantee_property(self, config):
        """Test processing_guarantee property."""
        config.processing_guarantee = ProcessingGuarantee.EXACTLY_ONCE
        assert config.processing_guarantee == ProcessingGuarantee.EXACTLY_ONCE

    def test_timeout_property(self, config):
        """Test timeout_millis property."""
        config.timeout_millis = 60000
        assert config.timeout_millis == 60000

    def test_set_argument(self, config):
        """Test set_argument method."""
        result = config.set_argument("key", "value")
        assert result is config
        assert config.arguments["key"] == "value"

    def test_get_argument(self, config):
        """Test get_argument method."""
        config.set_argument("key", "value")
        assert config.get_argument("key") == "value"
        assert config.get_argument("missing", "default") == "default"

    def test_repr(self, config):
        """Test __repr__."""
        repr_str = repr(config)
        assert "JobConfig" in repr_str
        assert "test-job" in repr_str


class TestJobMetrics:
    """Tests for JobMetrics class."""

    @pytest.fixture
    def metrics(self):
        """Create a JobMetrics instance."""
        return JobMetrics()

    def test_timestamp_property(self, metrics):
        """Test timestamp property."""
        assert isinstance(metrics.timestamp, datetime)

    def test_items_in_property(self, metrics):
        """Test items_in property."""
        assert metrics.items_in == 0
        metrics.items_in = 100
        assert metrics.items_in == 100

    def test_items_out_property(self, metrics):
        """Test items_out property."""
        assert metrics.items_out == 0
        metrics.items_out = 50
        assert metrics.items_out == 50

    def test_snapshot_count(self, metrics):
        """Test snapshot_count property."""
        assert metrics.snapshot_count == 0

    def test_record_snapshot(self, metrics):
        """Test record_snapshot method."""
        metrics.record_snapshot()
        assert metrics.snapshot_count == 1
        assert metrics.last_snapshot_time is not None

    def test_get_set(self, metrics):
        """Test get and set methods."""
        metrics.set("custom_metric", 42)
        assert metrics.get("custom_metric") == 42
        assert metrics.get("missing") is None
        assert metrics.get("missing", 0) == 0

    def test_to_dict(self, metrics):
        """Test to_dict method."""
        metrics.items_in = 100
        metrics.items_out = 50
        result = metrics.to_dict()
        assert result["items_in"] == 100
        assert result["items_out"] == 50
        assert "timestamp" in result

    def test_repr(self, metrics):
        """Test __repr__."""
        repr_str = repr(metrics)
        assert "JobMetrics" in repr_str


class TestJob:
    """Tests for Job class."""

    @pytest.fixture
    def job(self):
        """Create a Job instance."""
        config = JobConfig(name="test-job")
        return Job(1, "test-job", config)

    def test_init(self, job):
        """Test initialization."""
        assert job.id == 1
        assert job.name == "test-job"
        assert job.status == JobStatus.NOT_RUNNING

    def test_id_property(self, job):
        """Test id property."""
        assert job.id == 1

    def test_name_property(self, job):
        """Test name property."""
        assert job.name == "test-job"

    def test_config_property(self, job):
        """Test config property."""
        assert job.config.name == "test-job"

    def test_status_property(self, job):
        """Test status property."""
        assert job.status == JobStatus.NOT_RUNNING

    def test_is_terminal_not_terminal(self, job):
        """Test is_terminal for non-terminal status."""
        assert job.is_terminal() is False

    def test_is_terminal_completed(self, job):
        """Test is_terminal for completed job."""
        job._start()
        job._complete()
        assert job.is_terminal() is True

    def test_is_terminal_failed(self, job):
        """Test is_terminal for failed job."""
        job._start()
        job._fail("error")
        assert job.is_terminal() is True

    def test_start(self, job):
        """Test _start method."""
        job._start()
        assert job.status == JobStatus.RUNNING
        assert job.submission_time is not None

    def test_complete(self, job):
        """Test _complete method."""
        job._start()
        job._complete()
        assert job.status == JobStatus.COMPLETED
        assert job.completion_time is not None

    def test_fail(self, job):
        """Test _fail method."""
        job._start()
        job._fail("Something went wrong")
        assert job.status == JobStatus.FAILED
        assert job.failure_reason == "Something went wrong"

    def test_suspend(self, job):
        """Test suspend method."""
        job._start()
        job.suspend()
        assert job.status == JobStatus.SUSPENDED

    def test_suspend_async(self, job):
        """Test suspend_async method."""
        job._start()
        future = job.suspend_async()
        future.result()
        assert job.status == JobStatus.SUSPENDED

    def test_suspend_invalid_state(self, job):
        """Test suspend from invalid state."""
        future = job.suspend_async()
        with pytest.raises(IllegalStateException):
            future.result()

    def test_resume(self, job):
        """Test resume method."""
        job._start()
        job.suspend()
        job.resume()
        assert job.status == JobStatus.RUNNING

    def test_resume_async(self, job):
        """Test resume_async method."""
        job._start()
        job.suspend()
        future = job.resume_async()
        future.result()
        assert job.status == JobStatus.RUNNING

    def test_resume_invalid_state(self, job):
        """Test resume from invalid state."""
        future = job.resume_async()
        with pytest.raises(IllegalStateException):
            future.result()

    def test_restart(self, job):
        """Test restart method."""
        job._start()
        job.restart()
        assert job.status == JobStatus.RUNNING

    def test_cancel(self, job):
        """Test cancel method."""
        job._start()
        job.cancel()
        assert job.status == JobStatus.COMPLETED
        assert job.is_user_cancelled is True

    def test_cancel_async(self, job):
        """Test cancel_async method."""
        job._start()
        future = job.cancel_async()
        future.result()
        assert job.is_user_cancelled is True

    def test_cancel_already_completed(self, job):
        """Test cancel on already completed job."""
        job._start()
        job._complete()
        future = job.cancel_async()
        future.result()

    def test_cancel_and_export_snapshot(self, job):
        """Test cancel_and_export_snapshot method."""
        job._start()
        job.cancel_and_export_snapshot("final-snapshot")
        assert "final-snapshot" in job.snapshots
        assert job.is_user_cancelled is True

    def test_export_snapshot(self, job):
        """Test export_snapshot method."""
        job._start()
        future = job.export_snapshot("checkpoint-1")
        future.result()
        assert "checkpoint-1" in job.snapshots
        assert job.metrics.snapshot_count == 1

    def test_export_snapshot_invalid_state(self, job):
        """Test export_snapshot from invalid state."""
        future = job.export_snapshot("snapshot")
        with pytest.raises(IllegalStateException):
            future.result()

    def test_join(self, job):
        """Test join method."""
        job._start()
        job._complete()
        result = job.join(timeout=1.0)
        assert result is job

    def test_join_async(self, job):
        """Test join_async method."""
        job._start()
        job._complete()
        future = job.join_async()
        result = future.result()
        assert result is job

    def test_get_metrics(self, job):
        """Test get_metrics method."""
        job._start()
        metrics = job.get_metrics()
        assert "status" in metrics
        assert metrics["status"] == "RUNNING"

    def test_get_metrics_async(self, job):
        """Test get_metrics_async method."""
        future = job.get_metrics_async()
        metrics = future.result()
        assert "status" in metrics

    def test_get_id_string(self, job):
        """Test get_id_string method."""
        id_str = job.get_id_string()
        assert len(id_str) == 16

    def test_repr(self, job):
        """Test __repr__."""
        repr_str = repr(job)
        assert "Job" in repr_str
        assert "test-job" in repr_str


class TestJobStateListener:
    """Tests for JobStateListener interface."""

    def test_on_state_changed(self):
        """Test on_state_changed default implementation."""
        listener = JobStateListener()
        job = Job(1, "test")
        listener.on_state_changed(job, JobStatus.NOT_RUNNING, JobStatus.RUNNING)

    def test_on_job_completed(self):
        """Test on_job_completed default implementation."""
        listener = JobStateListener()
        job = Job(1, "test")
        listener.on_job_completed(job)

    def test_on_job_failed(self):
        """Test on_job_failed default implementation."""
        listener = JobStateListener()
        job = Job(1, "test")
        listener.on_job_failed(job, "error")


class TestJetService:
    """Tests for JetService class."""

    @pytest.fixture
    def service(self):
        """Create a JetService."""
        service = JetService()
        service.start()
        return service

    @pytest.fixture
    def complete_pipeline(self):
        """Create a complete pipeline."""
        pipeline = Pipeline.create()
        source = BatchSource("numbers", [1, 2, 3])
        sink = ListSink("results")
        pipeline.read_from(source).write_to(sink)
        return pipeline

    def test_start(self):
        """Test start method."""
        service = JetService()
        assert not service.is_running
        service.start()
        assert service.is_running

    def test_shutdown(self, service):
        """Test shutdown method."""
        service.shutdown()
        assert not service.is_running

    def test_submit(self, service, complete_pipeline):
        """Test submit method."""
        job = service.submit(complete_pipeline)
        assert isinstance(job, Job)
        assert job.status == JobStatus.RUNNING

    def test_submit_async(self, service, complete_pipeline):
        """Test submit_async method."""
        future = service.submit_async(complete_pipeline)
        job = future.result()
        assert job.status == JobStatus.RUNNING

    def test_submit_not_running(self, complete_pipeline):
        """Test submit when service not running."""
        service = JetService()
        with pytest.raises(IllegalStateException):
            service.submit(complete_pipeline)

    def test_submit_none_pipeline(self, service):
        """Test submit with None pipeline."""
        with pytest.raises(IllegalArgumentException):
            service.submit(None)

    def test_submit_incomplete_pipeline(self, service):
        """Test submit with incomplete pipeline."""
        pipeline = Pipeline.create()
        pipeline.read_from(BatchSource("test", []))
        with pytest.raises(IllegalArgumentException):
            service.submit(pipeline)

    def test_new_job(self, service, complete_pipeline):
        """Test new_job method (alias for submit)."""
        job = service.new_job(complete_pipeline)
        assert job.status == JobStatus.RUNNING

    def test_new_job_if_absent(self, service, complete_pipeline):
        """Test new_job_if_absent method."""
        config = JobConfig(name="unique-job")
        job1 = service.new_job_if_absent(complete_pipeline, config)
        job2 = service.new_job_if_absent(complete_pipeline, config)
        assert job1 is job2

    def test_new_job_if_absent_no_name(self, service, complete_pipeline):
        """Test new_job_if_absent without name raises."""
        config = JobConfig()
        with pytest.raises(ValueError):
            service.new_job_if_absent(complete_pipeline, config)

    def test_get_job(self, service, complete_pipeline):
        """Test get_job method."""
        job = service.submit(complete_pipeline)
        retrieved = service.get_job(job.id)
        assert retrieved is job

    def test_get_job_not_found(self, service):
        """Test get_job returns None for unknown ID."""
        assert service.get_job(999) is None

    def test_get_job_async(self, service, complete_pipeline):
        """Test get_job_async method."""
        job = service.submit(complete_pipeline)
        future = service.get_job_async(job.id)
        retrieved = future.result()
        assert retrieved is job

    def test_get_job_by_name(self, service, complete_pipeline):
        """Test get_job_by_name method."""
        config = JobConfig(name="named-job")
        job = service.submit(complete_pipeline, config)
        retrieved = service.get_job_by_name("named-job")
        assert retrieved is job

    def test_get_job_by_name_not_found(self, service):
        """Test get_job_by_name returns None for unknown name."""
        assert service.get_job_by_name("nonexistent") is None

    def test_get_jobs(self, service, complete_pipeline):
        """Test get_jobs method."""
        service.submit(complete_pipeline)
        jobs = service.get_jobs()
        assert len(jobs) >= 1

    def test_get_jobs_async(self, service):
        """Test get_jobs_async method."""
        future = service.get_jobs_async()
        jobs = future.result()
        assert isinstance(jobs, list)

    def test_get_jobs_by_name(self, service, complete_pipeline):
        """Test get_jobs_by_name method."""
        config = JobConfig(name="batch-job")
        service.submit(complete_pipeline, config)
        jobs = service.get_jobs_by_name("batch-job")
        assert len(jobs) == 1

    def test_new_light_job(self, service, complete_pipeline):
        """Test new_light_job method."""
        job = service.new_light_job(complete_pipeline)
        assert job.is_light_job is True
        assert job.config.processing_guarantee == ProcessingGuarantee.NONE

    def test_new_light_job_async(self, service, complete_pipeline):
        """Test new_light_job_async method."""
        future = service.new_light_job_async(complete_pipeline)
        job = future.result()
        assert job.is_light_job is True

    def test_get_active_jobs(self, service, complete_pipeline):
        """Test get_active_jobs method."""
        job = service.submit(complete_pipeline)
        active = service.get_active_jobs()
        assert job in active

    def test_get_active_jobs_excludes_completed(self, service, complete_pipeline):
        """Test get_active_jobs excludes completed jobs."""
        job = service.submit(complete_pipeline)
        job._complete()
        active = service.get_active_jobs()
        assert job not in active

    def test_resume_job(self, service, complete_pipeline):
        """Test resume_job method."""
        job = service.submit(complete_pipeline)
        job.suspend()
        service.resume_job(job.id)
        assert job.status == JobStatus.RUNNING

    def test_resume_job_not_found(self, service):
        """Test resume_job with unknown ID."""
        with pytest.raises(IllegalArgumentException):
            service.resume_job(999)

    def test_add_job_state_listener(self, service):
        """Test add_job_state_listener method."""
        listener = JobStateListener()
        reg_id = service.add_job_state_listener(listener)
        assert reg_id is not None
        assert reg_id.startswith("job-state-")

    def test_remove_job_state_listener(self, service):
        """Test remove_job_state_listener method."""
        listener = JobStateListener()
        reg_id = service.add_job_state_listener(listener)
        assert service.remove_job_state_listener(reg_id) is True
        assert service.remove_job_state_listener(reg_id) is False

    def test_repr(self, service):
        """Test __repr__."""
        repr_str = repr(service)
        assert "JetService" in repr_str
        assert "running=True" in repr_str
