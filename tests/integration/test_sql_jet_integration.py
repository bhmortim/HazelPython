"""Integration tests for SQL and Jet functionality.

These tests require a running Hazelcast cluster and are designed to verify
the end-to-end integration of SQL queries and Jet jobs.

Note: These tests are skipped if no cluster is available.
"""

import os
import unittest
from typing import Optional

try:
    from hazelcast.jet.service import JetService
    from hazelcast.jet.pipeline import Pipeline, Sources, Sinks
    from hazelcast.jet.job import Job, JobConfig, JobStatus

    HAS_JET = True
except ImportError:
    HAS_JET = False


def get_test_client():
    """Get a test client if available."""
    return None


@unittest.skipUnless(HAS_JET, "Jet module not available")
class TestSqlJetIntegration(unittest.TestCase):
    """Integration tests for SQL and Jet functionality."""

    @classmethod
    def setUpClass(cls):
        """Set up test class with client connection."""
        cls.client = get_test_client()
        cls.skip_tests = cls.client is None

    @classmethod
    def tearDownClass(cls):
        """Clean up after tests."""
        if cls.client:
            pass

    def setUp(self):
        """Set up each test."""
        if self.skip_tests:
            self.skipTest("No Hazelcast cluster available")

    def test_jet_service_creation(self):
        """Test creating a JetService."""
        if self.skip_tests:
            self.skipTest("No cluster available")

        jet = JetService(self.client)
        self.assertIsNotNone(jet)

    def test_pipeline_creation(self):
        """Test creating a Pipeline."""
        pipeline = Pipeline.create()
        self.assertIsNotNone(pipeline)
        self.assertTrue(pipeline.is_empty())

    def test_pipeline_with_map_source(self):
        """Test pipeline with map source."""
        pipeline = Pipeline.create()
        stage = pipeline.read_from(Sources.map("test-map"))

        self.assertIsNotNone(stage)
        self.assertFalse(pipeline.is_empty())

    def test_pipeline_with_operations(self):
        """Test pipeline with chained operations."""
        pipeline = Pipeline.create()
        stage = (
            pipeline
            .read_from(Sources.map("input-map"))
            .filter(lambda x: x[1] is not None)
            .map(lambda x: (x[0], x[1] * 2))
            .drain_to(Sinks.map("output-map"))
        )

        dag = pipeline.to_dag()
        self.assertEqual(len(dag["sources"]), 1)
        self.assertEqual(len(dag["sinks"]), 1)

    def test_pipeline_to_json(self):
        """Test pipeline JSON serialization."""
        pipeline = Pipeline.create()
        pipeline.read_from(Sources.list("test-list")).drain_to(Sinks.logger())

        json_str = pipeline.to_json()

        self.assertIn("sources", json_str)
        self.assertIn("sinks", json_str)
        self.assertIn("test-list", json_str)


@unittest.skipUnless(HAS_JET, "Jet module not available")
class TestJetJobLifecycle(unittest.TestCase):
    """Integration tests for Jet job lifecycle."""

    @classmethod
    def setUpClass(cls):
        """Set up test class."""
        cls.client = get_test_client()
        cls.skip_tests = cls.client is None

    def setUp(self):
        """Set up each test."""
        if self.skip_tests:
            self.skipTest("No Hazelcast cluster available")

    def test_job_config_creation(self):
        """Test creating a JobConfig."""
        config = JobConfig(
            name="integration-test-job",
            processing_guarantee="AT_LEAST_ONCE",
            snapshot_interval_millis=5000,
        )

        self.assertEqual(config.name, "integration-test-job")
        self.assertEqual(config.processing_guarantee, "AT_LEAST_ONCE")

    def test_job_status_enum(self):
        """Test JobStatus enum values."""
        self.assertEqual(JobStatus.NOT_RUNNING.value, 0)
        self.assertEqual(JobStatus.RUNNING.value, 2)
        self.assertEqual(JobStatus.COMPLETED.value, 7)

    def test_job_status_from_code(self):
        """Test converting status codes to enum."""
        self.assertEqual(JobStatus.from_code(2), JobStatus.RUNNING)
        self.assertEqual(JobStatus.from_code(7), JobStatus.COMPLETED)
        self.assertEqual(JobStatus.from_code(-1), JobStatus.NOT_RUNNING)


@unittest.skipUnless(HAS_JET, "Jet module not available")
class TestJetSources(unittest.TestCase):
    """Integration tests for Jet sources."""

    def test_map_source(self):
        """Test MapSource creation."""
        source = Sources.map("test-map")

        self.assertEqual(source.map_name, "test-map")
        self.assertTrue(source.is_partitioned)
        self.assertIn("map", source.name)

    def test_list_source(self):
        """Test ListSource creation."""
        source = Sources.list("test-list")

        self.assertEqual(source.list_name, "test-list")
        self.assertFalse(source.is_partitioned)
        self.assertIn("list", source.name)

    def test_items_source(self):
        """Test TestSource creation."""
        source = Sources.items(1, 2, 3)

        self.assertEqual(source.items, [1, 2, 3])
        self.assertEqual(source.name, "test-source")

    def test_source_to_dict(self):
        """Test source serialization."""
        source = Sources.map("my-map")
        d = source.to_dict()

        self.assertEqual(d["type"], "map")
        self.assertEqual(d["name"], "my-map")


@unittest.skipUnless(HAS_JET, "Jet module not available")
class TestJetSinks(unittest.TestCase):
    """Integration tests for Jet sinks."""

    def test_map_sink(self):
        """Test MapSink creation."""
        sink = Sinks.map("output-map")

        self.assertEqual(sink.map_name, "output-map")
        self.assertTrue(sink.is_partitioned)

    def test_list_sink(self):
        """Test ListSink creation."""
        sink = Sinks.list("output-list")

        self.assertEqual(sink.list_name, "output-list")
        self.assertFalse(sink.is_partitioned)

    def test_logger_sink(self):
        """Test LoggerSink creation."""
        sink = Sinks.logger("DEBUG: ")

        self.assertEqual(sink.name, "logger-sink")

    def test_noop_sink(self):
        """Test NoopSink creation."""
        sink = Sinks.noop()

        self.assertEqual(sink.name, "noop-sink")

    def test_sink_to_dict(self):
        """Test sink serialization."""
        sink = Sinks.list("my-list")
        d = sink.to_dict()

        self.assertEqual(d["type"], "list")
        self.assertEqual(d["name"], "my-list")


@unittest.skipUnless(HAS_JET, "Jet module not available")
class TestPipelineOperations(unittest.TestCase):
    """Integration tests for pipeline operations."""

    def setUp(self):
        """Set up each test with a pipeline."""
        self.pipeline = Pipeline.create()
        self.stage = self.pipeline.read_from(Sources.map("input"))

    def test_map_operation(self):
        """Test map operation."""
        result = self.stage.map(lambda x: x * 2)
        self.assertIn("map", result.name)

    def test_filter_operation(self):
        """Test filter operation."""
        result = self.stage.filter(lambda x: x > 0)
        self.assertIn("filter", result.name)

    def test_flat_map_operation(self):
        """Test flatMap operation."""
        result = self.stage.flat_map(lambda x: [x, x])
        self.assertIn("flatmap", result.name)

    def test_group_by_operation(self):
        """Test groupBy operation."""
        result = self.stage.group_by(lambda x: x[0])
        self.assertIn("groupby", result.name)

    def test_aggregate_operation(self):
        """Test aggregate operation."""
        result = self.stage.aggregate(lambda items: sum(items))
        self.assertIn("aggregate", result.name)

    def test_distinct_operation(self):
        """Test distinct operation."""
        result = self.stage.distinct()
        self.assertIn("distinct", result.name)

    def test_sort_operation(self):
        """Test sort operation."""
        result = self.stage.sort()
        self.assertIn("sort", result.name)

    def test_peek_operation(self):
        """Test peek operation."""
        result = self.stage.peek(lambda x: None)
        self.assertIn("peek", result.name)

    def test_operation_chaining(self):
        """Test chaining multiple operations."""
        result = (
            self.stage
            .filter(lambda x: x is not None)
            .map(lambda x: x * 2)
            .distinct()
            .sort()
            .drain_to(Sinks.list("output"))
        )

        dag = self.pipeline.to_dag()
        self.assertEqual(len(dag["sinks"]), 1)


if __name__ == "__main__":
    unittest.main()
