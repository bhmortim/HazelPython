"""Integration tests for SQL and Jet operations against a real Hazelcast cluster."""

import pytest
import time
from typing import List

from tests.integration.conftest import skip_integration, DOCKER_AVAILABLE


@skip_integration
class TestSqlBasicQueries:
    """Test basic SQL query operations."""

    def test_select_from_map(self, connected_client, unique_name):
        """Test SELECT query on a map."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(10):
            test_map.put(i, {"id": i, "name": f"item-{i}"})
        
        sql = connected_client.sql
        
        result = sql.execute(f'SELECT * FROM "{unique_name}"')
        rows = list(result)
        
        assert len(rows) == 10

    def test_select_with_where(self, connected_client, unique_name):
        """Test SELECT with WHERE clause."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(10):
            test_map.put(i, {"id": i, "value": i * 10})
        
        sql = connected_client.sql
        
        result = sql.execute(
            f'SELECT * FROM "{unique_name}" WHERE value > ?',
            50
        )
        rows = list(result)
        
        assert all(row["value"] > 50 for row in rows)

    def test_select_count(self, connected_client, unique_name):
        """Test SELECT COUNT query."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(25):
            test_map.put(i, {"id": i})
        
        sql = connected_client.sql
        
        result = sql.execute(f'SELECT COUNT(*) FROM "{unique_name}"')
        row = next(iter(result))
        
        assert row[0] == 25

    def test_select_order_by(self, connected_client, unique_name):
        """Test SELECT with ORDER BY."""
        test_map = connected_client.get_map(unique_name)
        
        for i in [5, 2, 8, 1, 9]:
            test_map.put(i, {"id": i, "value": i})
        
        sql = connected_client.sql
        
        result = sql.execute(
            f'SELECT * FROM "{unique_name}" ORDER BY value ASC'
        )
        rows = list(result)
        values = [r["value"] for r in rows]
        
        assert values == sorted(values)

    def test_select_limit(self, connected_client, unique_name):
        """Test SELECT with LIMIT."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(100):
            test_map.put(i, {"id": i})
        
        sql = connected_client.sql
        
        result = sql.execute(f'SELECT * FROM "{unique_name}" LIMIT 10')
        rows = list(result)
        
        assert len(rows) == 10


@skip_integration
class TestSqlDmlOperations:
    """Test SQL DML operations."""

    def test_insert(self, connected_client, unique_name):
        """Test INSERT statement."""
        sql = connected_client.sql
        
        sql.execute(f'''
            CREATE MAPPING "{unique_name}" (
                __key INT,
                name VARCHAR,
                value INT
            ) TYPE IMap OPTIONS (
                'keyFormat' = 'int',
                'valueFormat' = 'json-flat'
            )
        ''')
        
        result = sql.execute(
            f'INSERT INTO "{unique_name}" (__key, name, value) VALUES (?, ?, ?)',
            1, "test", 100
        )
        
        assert result.update_count >= 0

    def test_update(self, connected_client, unique_name):
        """Test UPDATE statement."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(5):
            test_map.put(i, {"id": i, "status": "pending"})
        
        sql = connected_client.sql
        
        result = sql.execute(
            f'UPDATE "{unique_name}" SET status = ? WHERE id > ?',
            "completed", 2
        )
        
        assert result.update_count >= 0

    def test_delete(self, connected_client, unique_name):
        """Test DELETE statement."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(10):
            test_map.put(i, {"id": i, "active": i % 2 == 0})
        
        sql = connected_client.sql
        
        result = sql.execute(
            f'DELETE FROM "{unique_name}" WHERE active = ?',
            False
        )
        
        assert result.update_count >= 0


@skip_integration
class TestSqlStreaming:
    """Test SQL streaming result handling."""

    def test_streaming_large_result(self, connected_client, unique_name):
        """Test streaming large result set."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(1000):
            test_map.put(i, {"id": i, "data": f"item-{i}"})
        
        sql = connected_client.sql
        
        result = sql.execute(f'SELECT * FROM "{unique_name}"')
        
        count = 0
        for row in result:
            count += 1
        
        assert count == 1000

    def test_result_iteration_multiple_times(self, connected_client, unique_name):
        """Test that result can only be iterated once."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(10):
            test_map.put(i, {"id": i})
        
        sql = connected_client.sql
        result = sql.execute(f'SELECT * FROM "{unique_name}"')
        
        first_pass = list(result)
        second_pass = list(result)
        
        assert len(first_pass) == 10
        assert len(second_pass) == 0


@skip_integration
class TestSqlParameterizedQueries:
    """Test SQL parameterized queries."""

    def test_single_parameter(self, connected_client, unique_name):
        """Test query with single parameter."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(10):
            test_map.put(i, {"id": i, "category": f"cat-{i % 3}"})
        
        sql = connected_client.sql
        
        result = sql.execute(
            f'SELECT * FROM "{unique_name}" WHERE category = ?',
            "cat-1"
        )
        rows = list(result)
        
        assert all(r["category"] == "cat-1" for r in rows)

    def test_multiple_parameters(self, connected_client, unique_name):
        """Test query with multiple parameters."""
        test_map = connected_client.get_map(unique_name)
        
        for i in range(20):
            test_map.put(i, {"id": i, "value": i * 5, "active": i % 2 == 0})
        
        sql = connected_client.sql
        
        result = sql.execute(
            f'SELECT * FROM "{unique_name}" WHERE value > ? AND active = ?',
            50, True
        )
        rows = list(result)
        
        for row in rows:
            assert row["value"] > 50
            assert row["active"] is True


@skip_integration
class TestJetPipelineExecution:
    """Test Jet pipeline execution."""

    def test_simple_batch_pipeline(self, connected_client, unique_name):
        """Test simple batch pipeline execution."""
        from hazelcast.jet.pipeline import Pipeline, BatchSource
        
        source_list = connected_client.get_list(f"{unique_name}-source")
        sink_list = connected_client.get_list(f"{unique_name}-sink")
        
        for i in range(100):
            source_list.add(i)
        
        jet = connected_client.jet
        
        pipeline = Pipeline.create()
        source = Pipeline.from_list(f"{unique_name}-source", list(range(100)))
        sink = Pipeline.to_list(f"{unique_name}-sink")
        
        pipeline.read_from(source).map(lambda x: x * 2).write_to(sink)
        
        job = jet.submit(pipeline)
        job.join(timeout=30)
        
        assert job.status.value in ["COMPLETED", "RUNNING"]

    def test_pipeline_with_filter(self, connected_client, unique_name):
        """Test pipeline with filter transformation."""
        from hazelcast.jet.pipeline import Pipeline
        
        jet = connected_client.jet
        
        pipeline = Pipeline.create()
        source = Pipeline.from_list(f"{unique_name}-source", list(range(100)))
        sink = Pipeline.to_list(f"{unique_name}-sink")
        
        (pipeline
            .read_from(source)
            .filter(lambda x: x % 2 == 0)
            .write_to(sink))
        
        job = jet.submit(pipeline)
        job.join(timeout=30)

    def test_pipeline_with_aggregation(self, connected_client, unique_name):
        """Test pipeline with aggregation."""
        from hazelcast.jet.pipeline import Pipeline, AggregateOperation
        
        jet = connected_client.jet
        
        pipeline = Pipeline.create()
        source = Pipeline.from_list(f"{unique_name}-source", list(range(10)))
        sink = Pipeline.to_list(f"{unique_name}-sink")
        
        (pipeline
            .read_from(source)
            .aggregate(AggregateOperation.summing())
            .write_to(sink))
        
        job = jet.submit(pipeline)
        job.join(timeout=30)


@skip_integration
class TestJetJobManagement:
    """Test Jet job management operations."""

    def test_get_job_by_id(self, connected_client, unique_name):
        """Test getting a job by ID."""
        from hazelcast.jet.pipeline import Pipeline
        
        jet = connected_client.jet
        
        pipeline = Pipeline.create()
        source = Pipeline.from_list(f"{unique_name}", [1, 2, 3])
        sink = Pipeline.to_list(f"{unique_name}-out")
        pipeline.read_from(source).write_to(sink)
        
        job = jet.submit(pipeline)
        
        retrieved = jet.get_job(job.id)
        assert retrieved is not None
        assert retrieved.id == job.id

    def test_get_jobs(self, connected_client, unique_name):
        """Test listing all jobs."""
        from hazelcast.jet.pipeline import Pipeline
        
        jet = connected_client.jet
        
        pipeline = Pipeline.create()
        source = Pipeline.from_list(f"{unique_name}", [1, 2, 3])
        sink = Pipeline.to_list(f"{unique_name}-out")
        pipeline.read_from(source).write_to(sink)
        
        jet.submit(pipeline)
        
        jobs = jet.get_jobs()
        assert len(jobs) >= 1

    def test_cancel_job(self, connected_client, unique_name):
        """Test cancelling a job."""
        from hazelcast.jet.pipeline import Pipeline
        from hazelcast.jet.job import JobStatus
        
        jet = connected_client.jet
        
        pipeline = Pipeline.create()
        source = Pipeline.test_source(f"{unique_name}", items_per_second=1)
        sink = Pipeline.noop()
        pipeline.read_from(source).write_to(sink)
        
        job = jet.submit(pipeline)
        
        job.cancel()
        
        assert job.status in [JobStatus.COMPLETED, JobStatus.FAILED]

    def test_job_metrics(self, connected_client, unique_name):
        """Test getting job metrics."""
        from hazelcast.jet.pipeline import Pipeline
        
        jet = connected_client.jet
        
        pipeline = Pipeline.create()
        source = Pipeline.from_list(f"{unique_name}", list(range(100)))
        sink = Pipeline.to_list(f"{unique_name}-out")
        pipeline.read_from(source).write_to(sink)
        
        job = jet.submit(pipeline)
        job.join(timeout=10)
        
        metrics = job.get_metrics()
        assert "status" in metrics
