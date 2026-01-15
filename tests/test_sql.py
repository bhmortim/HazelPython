"""Unit tests for SQL service."""

import pytest
from concurrent.futures import Future

from hazelcast.sql import (
    SqlService,
    SqlStatement,
    SqlExpectedResultType,
    SqlResult,
    SqlRow,
    SqlRowMetadata,
    SqlColumnMetadata,
    SqlColumnType,
)
from hazelcast.sql.service import SqlServiceError
from hazelcast.exceptions import IllegalStateException


class TestSqlStatement:
    """Tests for SqlStatement."""

    def test_create_empty(self):
        stmt = SqlStatement()
        assert stmt.sql == ""
        assert stmt.parameters == []
        assert stmt.timeout == SqlStatement.DEFAULT_TIMEOUT
        assert stmt.cursor_buffer_size == SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE

    def test_create_with_sql(self):
        stmt = SqlStatement("SELECT * FROM map")
        assert stmt.sql == "SELECT * FROM map"

    def test_set_sql(self):
        stmt = SqlStatement()
        stmt.sql = "SELECT 1"
        assert stmt.sql == "SELECT 1"

    def test_set_empty_sql_raises(self):
        stmt = SqlStatement()
        with pytest.raises(ValueError):
            stmt.sql = ""

    def test_add_parameter(self):
        stmt = SqlStatement("SELECT * FROM map WHERE id = ?")
        stmt.add_parameter(42)
        assert stmt.parameters == [42]

    def test_add_multiple_parameters(self):
        stmt = SqlStatement("SELECT * FROM map WHERE id = ? AND name = ?")
        stmt.add_parameter(1).add_parameter("test")
        assert stmt.parameters == [1, "test"]

    def test_set_parameters(self):
        stmt = SqlStatement("SELECT * FROM map WHERE a = ? AND b = ?")
        stmt.set_parameters(10, 20)
        assert stmt.parameters == [10, 20]

    def test_clear_parameters(self):
        stmt = SqlStatement("SELECT 1")
        stmt.set_parameters(1, 2, 3)
        stmt.clear_parameters()
        assert stmt.parameters == []

    def test_timeout_setter(self):
        stmt = SqlStatement("SELECT 1")
        stmt.timeout = 30.0
        assert stmt.timeout == 30.0

    def test_timeout_invalid_raises(self):
        stmt = SqlStatement("SELECT 1")
        with pytest.raises(ValueError):
            stmt.timeout = -2

    def test_cursor_buffer_size_setter(self):
        stmt = SqlStatement("SELECT 1")
        stmt.cursor_buffer_size = 8192
        assert stmt.cursor_buffer_size == 8192

    def test_cursor_buffer_size_invalid_raises(self):
        stmt = SqlStatement("SELECT 1")
        with pytest.raises(ValueError):
            stmt.cursor_buffer_size = 0

    def test_schema_setter(self):
        stmt = SqlStatement("SELECT 1")
        stmt.schema = "public"
        assert stmt.schema == "public"

    def test_expected_result_type(self):
        stmt = SqlStatement("SELECT 1")
        stmt.expected_result_type = SqlExpectedResultType.ROWS
        assert stmt.expected_result_type == SqlExpectedResultType.ROWS

    def test_copy(self):
        stmt = SqlStatement("SELECT * FROM map WHERE id = ?")
        stmt.set_parameters(42)
        stmt.timeout = 10.0
        stmt.schema = "test"

        copy = stmt.copy()
        assert copy.sql == stmt.sql
        assert copy.parameters == stmt.parameters
        assert copy.timeout == stmt.timeout
        assert copy.schema == stmt.schema


class TestSqlRowMetadata:
    """Tests for SqlRowMetadata."""

    def test_create(self):
        columns = [
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
        ]
        metadata = SqlRowMetadata(columns)
        assert metadata.column_count == 2

    def test_get_column(self):
        columns = [SqlColumnMetadata("id", SqlColumnType.INTEGER)]
        metadata = SqlRowMetadata(columns)
        col = metadata.get_column(0)
        assert col.name == "id"
        assert col.type == SqlColumnType.INTEGER

    def test_find_column(self):
        columns = [
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
        ]
        metadata = SqlRowMetadata(columns)
        assert metadata.find_column("name") == 1
        assert metadata.find_column("nonexistent") == -1


class TestSqlRow:
    """Tests for SqlRow."""

    def test_create(self):
        columns = [
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
        ]
        metadata = SqlRowMetadata(columns)
        row = SqlRow([1, "test"], metadata)
        assert len(row) == 2

    def test_get_object(self):
        columns = [SqlColumnMetadata("id", SqlColumnType.INTEGER)]
        metadata = SqlRowMetadata(columns)
        row = SqlRow([42], metadata)
        assert row.get_object(0) == 42

    def test_get_object_by_name(self):
        columns = [SqlColumnMetadata("name", SqlColumnType.VARCHAR)]
        metadata = SqlRowMetadata(columns)
        row = SqlRow(["hello"], metadata)
        assert row.get_object_by_name("name") == "hello"

    def test_get_object_by_name_not_found(self):
        columns = [SqlColumnMetadata("name", SqlColumnType.VARCHAR)]
        metadata = SqlRowMetadata(columns)
        row = SqlRow(["hello"], metadata)
        with pytest.raises(KeyError):
            row.get_object_by_name("nonexistent")

    def test_getitem_by_index(self):
        columns = [SqlColumnMetadata("val", SqlColumnType.INTEGER)]
        metadata = SqlRowMetadata(columns)
        row = SqlRow([100], metadata)
        assert row[0] == 100

    def test_getitem_by_name(self):
        columns = [SqlColumnMetadata("val", SqlColumnType.INTEGER)]
        metadata = SqlRowMetadata(columns)
        row = SqlRow([100], metadata)
        assert row["val"] == 100

    def test_to_dict(self):
        columns = [
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
        ]
        metadata = SqlRowMetadata(columns)
        row = SqlRow([1, "test"], metadata)
        assert row.to_dict() == {"id": 1, "name": "test"}

    def test_iter(self):
        columns = [
            SqlColumnMetadata("a", SqlColumnType.INTEGER),
            SqlColumnMetadata("b", SqlColumnType.INTEGER),
        ]
        metadata = SqlRowMetadata(columns)
        row = SqlRow([1, 2], metadata)
        assert list(row) == [1, 2]


class TestSqlResult:
    """Tests for SqlResult."""

    def test_create_row_result(self):
        columns = [SqlColumnMetadata("val", SqlColumnType.INTEGER)]
        metadata = SqlRowMetadata(columns)
        result = SqlResult(query_id="123", metadata=metadata)
        assert result.is_row_set
        assert result.update_count == -1

    def test_create_update_result(self):
        result = SqlResult(query_id="123", update_count=5)
        assert not result.is_row_set
        assert result.update_count == 5

    def test_iteration(self):
        columns = [SqlColumnMetadata("val", SqlColumnType.INTEGER)]
        metadata = SqlRowMetadata(columns)
        result = SqlResult(query_id="123", metadata=metadata)
        result.set_has_more(False)

        row1 = SqlRow([1], metadata)
        row2 = SqlRow([2], metadata)
        result.add_rows([row1, row2])

        rows = list(result)
        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[1][0] == 2

    def test_get_all(self):
        columns = [SqlColumnMetadata("val", SqlColumnType.INTEGER)]
        metadata = SqlRowMetadata(columns)
        result = SqlResult(query_id="123", metadata=metadata)
        result.set_has_more(False)

        row = SqlRow([42], metadata)
        result.add_rows([row])

        all_rows = result.get_all()
        assert len(all_rows) == 1
        assert all_rows[0][0] == 42

    def test_close(self):
        result = SqlResult(query_id="123")
        assert not result.is_closed
        result.close()
        assert result.is_closed

    def test_context_manager(self):
        result = SqlResult(query_id="123")
        with result as r:
            assert not r.is_closed
        assert result.is_closed


class TestSqlService:
    """Tests for SqlService."""

    def test_create(self):
        service = SqlService()
        assert not service.is_running

    def test_start_shutdown(self):
        service = SqlService()
        service.start()
        assert service.is_running
        service.shutdown()
        assert not service.is_running

    def test_execute_not_running_raises(self):
        service = SqlService()
        with pytest.raises(IllegalStateException):
            service.execute("SELECT 1")

    def test_execute_empty_sql_raises(self):
        service = SqlService()
        service.start()
        with pytest.raises(SqlServiceError):
            service.execute("")

    def test_execute_select(self):
        service = SqlService()
        service.start()
        result = service.execute("SELECT * FROM map")
        assert result.is_row_set
        assert result.query_id is not None

    def test_execute_insert(self):
        service = SqlService()
        service.start()
        result = service.execute("INSERT INTO map VALUES (1, 'test')")
        assert not result.is_row_set
        assert result.update_count == 0

    def test_execute_with_parameters(self):
        service = SqlService()
        service.start()
        result = service.execute(
            "SELECT * FROM map WHERE id = ?",
            42,
            timeout=10.0,
        )
        assert result is not None

    def test_execute_statement(self):
        service = SqlService()
        service.start()
        stmt = SqlStatement("SELECT * FROM map")
        stmt.timeout = 5.0
        result = service.execute_statement(stmt)
        assert result.is_row_set

    def test_execute_async(self):
        service = SqlService()
        service.start()
        future = service.execute_async("SELECT 1")
        assert isinstance(future, Future)
        result = future.result()
        assert result is not None

    def test_execute_async_not_running(self):
        service = SqlService()
        future = service.execute_async("SELECT 1")
        with pytest.raises(IllegalStateException):
            future.result()


class TestSqlColumnType:
    """Tests for SqlColumnType enum."""

    def test_values(self):
        assert SqlColumnType.VARCHAR.value == "VARCHAR"
        assert SqlColumnType.INTEGER.value == "INTEGER"
        assert SqlColumnType.BIGINT.value == "BIGINT"
        assert SqlColumnType.BOOLEAN.value == "BOOLEAN"
        assert SqlColumnType.DOUBLE.value == "DOUBLE"
