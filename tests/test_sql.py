"""Tests for the SQL service module."""

import pytest
import threading
import uuid
from concurrent.futures import Future
from unittest.mock import Mock, MagicMock, patch

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
    """Tests for SqlStatement class."""

    def test_init_default(self):
        stmt = SqlStatement()
        assert stmt.sql == ""
        assert stmt.parameters == []
        assert stmt.timeout == SqlStatement.DEFAULT_TIMEOUT
        assert stmt.cursor_buffer_size == SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE
        assert stmt.schema is None
        assert stmt.expected_result_type == SqlExpectedResultType.ANY

    def test_init_with_sql(self):
        stmt = SqlStatement("SELECT * FROM users")
        assert stmt.sql == "SELECT * FROM users"

    def test_sql_setter(self):
        stmt = SqlStatement()
        stmt.sql = "SELECT 1"
        assert stmt.sql == "SELECT 1"

    def test_sql_setter_empty_raises(self):
        stmt = SqlStatement("SELECT 1")
        with pytest.raises(ValueError, match="cannot be empty"):
            stmt.sql = ""

    def test_add_parameter(self):
        stmt = SqlStatement("SELECT * FROM users WHERE id = ?")
        result = stmt.add_parameter(42)
        assert result is stmt
        assert stmt.parameters == [42]

    def test_add_multiple_parameters(self):
        stmt = SqlStatement("SELECT * FROM users WHERE id = ? AND name = ?")
        stmt.add_parameter(42).add_parameter("Alice")
        assert stmt.parameters == [42, "Alice"]

    def test_set_parameters(self):
        stmt = SqlStatement("SELECT * FROM users WHERE id = ? AND name = ?")
        result = stmt.set_parameters(1, "Bob", True)
        assert result is stmt
        assert stmt.parameters == [1, "Bob", True]

    def test_set_parameters_replaces(self):
        stmt = SqlStatement()
        stmt.add_parameter("old")
        stmt.set_parameters("new1", "new2")
        assert stmt.parameters == ["new1", "new2"]

    def test_clear_parameters(self):
        stmt = SqlStatement()
        stmt.set_parameters(1, 2, 3)
        result = stmt.clear_parameters()
        assert result is stmt
        assert stmt.parameters == []

    def test_timeout_setter(self):
        stmt = SqlStatement()
        stmt.timeout = 30.0
        assert stmt.timeout == 30.0

    def test_timeout_infinite(self):
        stmt = SqlStatement()
        stmt.timeout = -1
        assert stmt.timeout == -1

    def test_timeout_invalid_raises(self):
        stmt = SqlStatement()
        with pytest.raises(ValueError, match="must be >= -1"):
            stmt.timeout = -5

    def test_cursor_buffer_size_setter(self):
        stmt = SqlStatement()
        stmt.cursor_buffer_size = 1000
        assert stmt.cursor_buffer_size == 1000

    def test_cursor_buffer_size_invalid_raises(self):
        stmt = SqlStatement()
        with pytest.raises(ValueError, match="must be positive"):
            stmt.cursor_buffer_size = 0

    def test_cursor_buffer_size_negative_raises(self):
        stmt = SqlStatement()
        with pytest.raises(ValueError, match="must be positive"):
            stmt.cursor_buffer_size = -1

    def test_schema_setter(self):
        stmt = SqlStatement()
        stmt.schema = "public"
        assert stmt.schema == "public"

    def test_schema_setter_none(self):
        stmt = SqlStatement()
        stmt.schema = "test"
        stmt.schema = None
        assert stmt.schema is None

    def test_expected_result_type_setter(self):
        stmt = SqlStatement()
        stmt.expected_result_type = SqlExpectedResultType.ROWS
        assert stmt.expected_result_type == SqlExpectedResultType.ROWS

    def test_copy(self):
        stmt = SqlStatement("SELECT * FROM t WHERE x = ?")
        stmt.add_parameter(42)
        stmt.timeout = 10.0
        stmt.cursor_buffer_size = 500
        stmt.schema = "myschema"
        stmt.expected_result_type = SqlExpectedResultType.UPDATE_COUNT

        copy = stmt.copy()
        assert copy is not stmt
        assert copy.sql == stmt.sql
        assert copy.parameters == stmt.parameters
        assert copy.parameters is not stmt._parameters
        assert copy.timeout == stmt.timeout
        assert copy.cursor_buffer_size == stmt.cursor_buffer_size
        assert copy.schema == stmt.schema
        assert copy.expected_result_type == stmt.expected_result_type

    def test_repr(self):
        stmt = SqlStatement("SELECT 1")
        stmt.add_parameter(42)
        repr_str = repr(stmt)
        assert "SqlStatement" in repr_str
        assert "SELECT 1" in repr_str
        assert "42" in repr_str


class TestSqlExpectedResultType:
    """Tests for SqlExpectedResultType enum."""

    def test_values(self):
        assert SqlExpectedResultType.ANY.value == "ANY"
        assert SqlExpectedResultType.ROWS.value == "ROWS"
        assert SqlExpectedResultType.UPDATE_COUNT.value == "UPDATE_COUNT"


class TestSqlColumnMetadata:
    """Tests for SqlColumnMetadata class."""

    def test_init(self):
        col = SqlColumnMetadata("id", SqlColumnType.INTEGER, False)
        assert col.name == "id"
        assert col.type == SqlColumnType.INTEGER
        assert col.nullable is False

    def test_init_default_nullable(self):
        col = SqlColumnMetadata("name", SqlColumnType.VARCHAR)
        assert col.nullable is True

    def test_repr(self):
        col = SqlColumnMetadata("age", SqlColumnType.BIGINT)
        assert "age" in repr(col)
        assert "BIGINT" in repr(col)

    def test_equality(self):
        col1 = SqlColumnMetadata("x", SqlColumnType.DOUBLE)
        col2 = SqlColumnMetadata("x", SqlColumnType.DOUBLE)
        col3 = SqlColumnMetadata("y", SqlColumnType.DOUBLE)
        col4 = SqlColumnMetadata("x", SqlColumnType.REAL)

        assert col1 == col2
        assert col1 != col3
        assert col1 != col4
        assert col1 != "not a column"


class TestSqlRowMetadata:
    """Tests for SqlRowMetadata class."""

    def test_init(self):
        cols = [
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
        ]
        meta = SqlRowMetadata(cols)
        assert meta.column_count == 2

    def test_columns_property(self):
        cols = [SqlColumnMetadata("a", SqlColumnType.BOOLEAN)]
        meta = SqlRowMetadata(cols)
        returned = meta.columns
        assert returned == cols
        assert returned is not cols

    def test_get_column(self):
        cols = [
            SqlColumnMetadata("first", SqlColumnType.DATE),
            SqlColumnMetadata("second", SqlColumnType.TIME),
        ]
        meta = SqlRowMetadata(cols)
        assert meta.get_column(0).name == "first"
        assert meta.get_column(1).name == "second"

    def test_get_column_out_of_range(self):
        meta = SqlRowMetadata([SqlColumnMetadata("x", SqlColumnType.NULL)])
        with pytest.raises(IndexError):
            meta.get_column(5)

    def test_find_column(self):
        cols = [
            SqlColumnMetadata("alpha", SqlColumnType.JSON),
            SqlColumnMetadata("beta", SqlColumnType.OBJECT),
        ]
        meta = SqlRowMetadata(cols)
        assert meta.find_column("alpha") == 0
        assert meta.find_column("beta") == 1

    def test_find_column_not_found(self):
        meta = SqlRowMetadata([SqlColumnMetadata("x", SqlColumnType.DECIMAL)])
        assert meta.find_column("unknown") == -1

    def test_repr(self):
        meta = SqlRowMetadata([SqlColumnMetadata("col", SqlColumnType.TINYINT)])
        assert "SqlRowMetadata" in repr(meta)


class TestSqlRow:
    """Tests for SqlRow class."""

    @pytest.fixture
    def sample_row(self):
        cols = [
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
            SqlColumnMetadata("active", SqlColumnType.BOOLEAN),
        ]
        meta = SqlRowMetadata(cols)
        values = [42, "Alice", True]
        return SqlRow(values, meta)

    def test_metadata_property(self, sample_row):
        assert sample_row.metadata.column_count == 3

    def test_get_object_by_index(self, sample_row):
        assert sample_row.get_object(0) == 42
        assert sample_row.get_object(1) == "Alice"
        assert sample_row.get_object(2) is True

    def test_get_object_out_of_range(self, sample_row):
        with pytest.raises(IndexError):
            sample_row.get_object(10)

    def test_get_object_by_name(self, sample_row):
        assert sample_row.get_object_by_name("id") == 42
        assert sample_row.get_object_by_name("name") == "Alice"
        assert sample_row.get_object_by_name("active") is True

    def test_get_object_by_name_not_found(self, sample_row):
        with pytest.raises(KeyError, match="Column not found"):
            sample_row.get_object_by_name("unknown")

    def test_to_dict(self, sample_row):
        d = sample_row.to_dict()
        assert d == {"id": 42, "name": "Alice", "active": True}

    def test_getitem_int(self, sample_row):
        assert sample_row[0] == 42
        assert sample_row[1] == "Alice"

    def test_getitem_str(self, sample_row):
        assert sample_row["id"] == 42
        assert sample_row["name"] == "Alice"

    def test_len(self, sample_row):
        assert len(sample_row) == 3

    def test_iter(self, sample_row):
        values = list(sample_row)
        assert values == [42, "Alice", True]

    def test_repr(self, sample_row):
        r = repr(sample_row)
        assert "SqlRow" in r
        assert "42" in r
        assert "Alice" in r


class TestSqlColumnType:
    """Tests for SqlColumnType enum."""

    def test_all_types_exist(self):
        types = [
            SqlColumnType.VARCHAR,
            SqlColumnType.BOOLEAN,
            SqlColumnType.TINYINT,
            SqlColumnType.SMALLINT,
            SqlColumnType.INTEGER,
            SqlColumnType.BIGINT,
            SqlColumnType.DECIMAL,
            SqlColumnType.REAL,
            SqlColumnType.DOUBLE,
            SqlColumnType.DATE,
            SqlColumnType.TIME,
            SqlColumnType.TIMESTAMP,
            SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
            SqlColumnType.OBJECT,
            SqlColumnType.NULL,
            SqlColumnType.JSON,
        ]
        assert len(types) == 16


class TestSqlResult:
    """Tests for SqlResult class."""

    def test_init_defaults(self):
        result = SqlResult()
        assert result.query_id is None
        assert result.metadata is None
        assert result.update_count == -1
        assert result.is_row_set is True
        assert result.is_closed is False

    def test_init_with_query_id(self):
        result = SqlResult(query_id="abc-123")
        assert result.query_id == "abc-123"

    def test_init_with_update_count(self):
        result = SqlResult(update_count=5)
        assert result.update_count == 5
        assert result.is_row_set is False

    def test_is_row_set_true(self):
        result = SqlResult(update_count=-1)
        assert result.is_row_set is True

    def test_is_row_set_false(self):
        result = SqlResult(update_count=0)
        assert result.is_row_set is False

    def test_add_rows(self):
        meta = SqlRowMetadata([SqlColumnMetadata("x", SqlColumnType.INTEGER)])
        result = SqlResult(metadata=meta)
        rows = [SqlRow([1], meta), SqlRow([2], meta)]
        result.add_rows(rows)
        assert len(result) == 2

    def test_iteration_empty(self):
        result = SqlResult()
        result.set_has_more(False)
        rows = list(result)
        assert rows == []

    def test_iteration_with_rows(self):
        meta = SqlRowMetadata([SqlColumnMetadata("val", SqlColumnType.VARCHAR)])
        result = SqlResult(metadata=meta)
        result.add_rows([SqlRow(["a"], meta), SqlRow(["b"], meta)])
        result.set_has_more(False)

        collected = []
        for row in result:
            collected.append(row[0])
        assert collected == ["a", "b"]

    def test_get_all(self):
        meta = SqlRowMetadata([SqlColumnMetadata("n", SqlColumnType.BIGINT)])
        result = SqlResult(metadata=meta)
        result.add_rows([SqlRow([10], meta), SqlRow([20], meta)])
        result.set_has_more(False)

        all_rows = result.get_all()
        assert len(all_rows) == 2
        assert all_rows[0][0] == 10
        assert all_rows[1][0] == 20

    def test_rows_as_dicts(self):
        meta = SqlRowMetadata([
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
        ])
        result = SqlResult(metadata=meta)
        result.add_rows([SqlRow([1, "Alice"], meta)])
        result.set_has_more(False)

        dicts = result.rows_as_dicts()
        assert dicts == [{"id": 1, "name": "Alice"}]

    def test_close(self):
        result = SqlResult(query_id="test-id")
        assert not result.is_closed
        result.close()
        assert result.is_closed

    def test_close_idempotent(self):
        result = SqlResult()
        result.close()
        result.close()
        assert result.is_closed

    def test_close_with_callback(self):
        callback_called = []

        def close_cb():
            callback_called.append(True)

        result = SqlResult(close_callback=close_cb)
        result.close()
        assert callback_called == [True]

    def test_iteration_after_close(self):
        result = SqlResult()
        result.close()
        with pytest.raises(StopIteration):
            next(result)

    def test_context_manager(self):
        result = SqlResult(query_id="ctx-test")
        with result as r:
            assert r.query_id == "ctx-test"
        assert result.is_closed

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        result = SqlResult(query_id="async-ctx")
        async with result as r:
            assert r.query_id == "async-ctx"
        assert result.is_closed

    def test_fetch_callback(self):
        meta = SqlRowMetadata([SqlColumnMetadata("v", SqlColumnType.INTEGER)])
        result = SqlResult(metadata=meta)

        fetch_count = [0]

        def fetch_more():
            fetch_count[0] += 1
            if fetch_count[0] == 1:
                return [SqlRow([100], meta)]
            return []

        result.set_fetch_callback(fetch_more)
        result.set_has_more(True)

        rows = list(result)
        assert len(rows) == 1
        assert rows[0][0] == 100

    def test_repr_row_set(self):
        result = SqlResult(query_id="qid", update_count=-1)
        r = repr(result)
        assert "qid" in r
        assert "is_row_set=True" in r

    def test_repr_update_count(self):
        result = SqlResult(update_count=42)
        r = repr(result)
        assert "update_count=42" in r

    def test_iteration_non_row_set(self):
        result = SqlResult(update_count=5)
        rows = list(result)
        assert rows == []


class TestSqlServiceError:
    """Tests for SqlServiceError class."""

    def test_init_default(self):
        err = SqlServiceError()
        assert str(err) == "SQL error"
        assert err.code == -1
        assert err.originating_member_id is None
        assert err.suggestion is None

    def test_init_with_message(self):
        err = SqlServiceError("Custom error")
        assert "Custom error" in str(err)

    def test_init_with_code(self):
        err = SqlServiceError("Error", code=SqlServiceError.SQL_ERROR_CODE_TIMEOUT)
        assert err.code == SqlServiceError.SQL_ERROR_CODE_TIMEOUT

    def test_init_with_member_id(self):
        err = SqlServiceError(originating_member_id="member-1")
        assert err.originating_member_id == "member-1"

    def test_init_with_suggestion(self):
        err = SqlServiceError(suggestion="Try again")
        assert err.suggestion == "Try again"

    def test_str_with_code_and_suggestion(self):
        err = SqlServiceError("Failed", code=1, suggestion="Retry")
        s = str(err)
        assert "code=1" in s
        assert "suggestion=Retry" in s

    def test_error_code_constants(self):
        assert SqlServiceError.SQL_ERROR_CODE_CANCELLED == -1
        assert SqlServiceError.SQL_ERROR_CODE_TIMEOUT == -2
        assert SqlServiceError.SQL_ERROR_CODE_PARSING == -3
        assert SqlServiceError.SQL_ERROR_CODE_GENERIC == -4


class TestSqlService:
    """Tests for SqlService class."""

    def test_init_default(self):
        service = SqlService()
        assert service._invocation_service is None
        assert service._serialization_service is None
        assert not service.is_running

    def test_init_with_services(self):
        inv_svc = Mock()
        ser_svc = Mock()
        service = SqlService(inv_svc, ser_svc)
        assert service._invocation_service is inv_svc
        assert service._serialization_service is ser_svc

    def test_start(self):
        service = SqlService()
        service.start()
        assert service.is_running

    def test_shutdown(self):
        service = SqlService()
        service.start()
        service.shutdown()
        assert not service.is_running

    def test_execute_not_running_raises(self):
        service = SqlService()
        with pytest.raises(IllegalStateException, match="not running"):
            service.execute("SELECT 1")

    def test_execute_empty_sql_raises(self):
        service = SqlService()
        service.start()
        with pytest.raises(SqlServiceError, match="cannot be empty"):
            service.execute("")

    def test_execute_simple(self):
        service = SqlService()
        service.start()
        result = service.execute("SELECT * FROM users")
        assert result is not None
        assert result.query_id is not None

    def test_execute_with_params(self):
        service = SqlService()
        service.start()
        result = service.execute("SELECT * FROM users WHERE id = ?", 42)
        assert result is not None

    def test_execute_with_options(self):
        service = SqlService()
        service.start()
        result = service.execute(
            "SELECT * FROM t",
            timeout=30.0,
            cursor_buffer_size=1000,
            schema="public",
            expected_result_type=SqlExpectedResultType.ROWS,
        )
        assert result is not None

    def test_execute_dml_query(self):
        service = SqlService()
        service.start()
        result = service.execute("INSERT INTO t VALUES (1)")
        assert result.update_count == 0
        assert not result.is_row_set

    def test_execute_update_query(self):
        service = SqlService()
        service.start()
        result = service.execute("UPDATE t SET x = 1")
        assert not result.is_row_set

    def test_execute_delete_query(self):
        service = SqlService()
        service.start()
        result = service.execute("DELETE FROM t")
        assert not result.is_row_set

    def test_execute_statement(self):
        service = SqlService()
        service.start()

        stmt = SqlStatement("SELECT * FROM employees")
        stmt.timeout = 60.0
        stmt.cursor_buffer_size = 2000

        result = service.execute_statement(stmt)
        assert result is not None
        assert result.is_row_set

    def test_execute_statement_not_running_raises(self):
        service = SqlService()
        stmt = SqlStatement("SELECT 1")
        with pytest.raises(IllegalStateException):
            service.execute_statement(stmt)

    def test_execute_statement_empty_sql_raises(self):
        service = SqlService()
        service.start()
        stmt = SqlStatement("")
        with pytest.raises(SqlServiceError):
            service.execute_statement(stmt)

    def test_execute_async(self):
        service = SqlService()
        service.start()
        future = service.execute_async("SELECT * FROM t")
        assert isinstance(future, Future)
        result = future.result()
        assert result is not None

    def test_execute_async_not_running(self):
        service = SqlService()
        future = service.execute_async("SELECT 1")
        with pytest.raises(IllegalStateException):
            future.result()

    def test_execute_async_empty_sql(self):
        service = SqlService()
        service.start()
        future = service.execute_async("")
        with pytest.raises(SqlServiceError):
            future.result()

    def test_execute_statement_async(self):
        service = SqlService()
        service.start()
        stmt = SqlStatement("SELECT 1")
        future = service.execute_statement_async(stmt)
        result = future.result()
        assert result is not None

    def test_repr(self):
        service = SqlService()
        r = repr(service)
        assert "SqlService" in r
        assert "running=False" in r

        service.start()
        r = repr(service)
        assert "running=True" in r

    def test_is_dml_query(self):
        service = SqlService()
        assert service._is_dml_query("INSERT INTO t VALUES (1)")
        assert service._is_dml_query("  UPDATE t SET x = 1")
        assert service._is_dml_query("DELETE FROM t")
        assert service._is_dml_query("MERGE INTO t ...")
        assert service._is_dml_query("CREATE TABLE t ...")
        assert service._is_dml_query("DROP TABLE t")
        assert service._is_dml_query("ALTER TABLE t ...")
        assert not service._is_dml_query("SELECT * FROM t")
        assert not service._is_dml_query("  select 1")

    def test_serialize_parameters(self):
        service = SqlService()
        params = service._serialize_parameters([])
        assert params == []

        params = service._serialize_parameters([None])
        assert params == [b""]

        params = service._serialize_parameters(["hello"])
        assert params == [b"hello"]

        params = service._serialize_parameters([42])
        import struct
        assert params == [struct.pack("<q", 42)]

        params = service._serialize_parameters([True])
        assert params == [b"\x01"]

        params = service._serialize_parameters([False])
        assert params == [b"\x00"]

    def test_convert_timeout(self):
        service = SqlService()
        assert service._convert_timeout(-1) == -1
        assert service._convert_timeout(0) == 0
        assert service._convert_timeout(1.5) == 1500
        assert service._convert_timeout(30) == 30000

    def test_generate_query_id(self):
        service = SqlService()
        qid1 = service._generate_query_id()
        qid2 = service._generate_query_id()
        assert len(qid1) == 16
        assert len(qid2) == 16
        assert qid1 != qid2

    def test_build_row_metadata(self):
        service = SqlService()
        raw = [
            ("id", 4, False),
            ("name", 0, True),
        ]
        meta = service._build_row_metadata(raw)
        assert meta.column_count == 2
        assert meta.get_column(0).name == "id"
        assert meta.get_column(0).type == SqlColumnType.INTEGER
        assert meta.get_column(0).nullable is False
        assert meta.get_column(1).name == "name"
        assert meta.get_column(1).type == SqlColumnType.VARCHAR
        assert meta.get_column(1).nullable is True

    def test_expected_result_type_map(self):
        assert SqlService._EXPECTED_RESULT_TYPE_MAP[SqlExpectedResultType.ANY] == 0
        assert SqlService._EXPECTED_RESULT_TYPE_MAP[SqlExpectedResultType.ROWS] == 1
        assert SqlService._EXPECTED_RESULT_TYPE_MAP[SqlExpectedResultType.UPDATE_COUNT] == 2

    def test_sql_column_type_map(self):
        assert SqlService._SQL_COLUMN_TYPE_MAP[0] == SqlColumnType.VARCHAR
        assert SqlService._SQL_COLUMN_TYPE_MAP[4] == SqlColumnType.INTEGER
        assert SqlService._SQL_COLUMN_TYPE_MAP[5] == SqlColumnType.BIGINT
        assert SqlService._SQL_COLUMN_TYPE_MAP[13] == SqlColumnType.OBJECT


class TestSqlServiceWithMocks:
    """Tests for SqlService with mocked dependencies."""

    def test_execute_with_serialization_service(self):
        ser_svc = Mock()
        ser_svc.to_data.return_value = b"serialized"

        service = SqlService(serialization_service=ser_svc)
        service.start()

        result = service.execute("SELECT * FROM t WHERE x = ?", "test_value")
        assert result is not None

    def test_serialize_parameters_with_service(self):
        ser_svc = Mock()
        ser_svc.to_data.side_effect = lambda x: f"data:{x}".encode()

        service = SqlService(serialization_service=ser_svc)
        params = service._serialize_parameters(["hello", 42])
        assert params == [b"data:hello", b"data:42"]

    def test_serialize_parameters_service_exception(self):
        ser_svc = Mock()
        ser_svc.to_data.side_effect = Exception("Serialization failed")

        service = SqlService(serialization_service=ser_svc)
        params = service._serialize_parameters(["fallback"])
        assert params == [b"fallback"]


class TestSqlIntegration:
    """Integration-style tests for SQL components."""

    def test_full_query_flow(self):
        service = SqlService()
        service.start()

        stmt = SqlStatement("SELECT id, name, score FROM players WHERE team = ?")
        stmt.add_parameter("TeamA")
        stmt.timeout = 30.0
        stmt.cursor_buffer_size = 100
        stmt.expected_result_type = SqlExpectedResultType.ROWS

        result = service.execute_statement(stmt)

        assert result.is_row_set
        assert result.metadata is not None

        result.close()
        assert result.is_closed

    def test_dml_flow(self):
        service = SqlService()
        service.start()

        result = service.execute(
            "INSERT INTO logs (timestamp, message) VALUES (?, ?)",
            "2024-01-01",
            "Test message",
        )

        assert not result.is_row_set
        assert result.update_count >= 0

    def test_result_iteration_with_fetch(self):
        meta = SqlRowMetadata([
            SqlColumnMetadata("seq", SqlColumnType.INTEGER),
        ])

        fetch_calls = [0]
        max_fetches = 3

        def mock_fetch():
            fetch_calls[0] += 1
            if fetch_calls[0] <= max_fetches:
                return [SqlRow([fetch_calls[0] * 10], meta)]
            return []

        result = SqlResult(metadata=meta, query_id="test")
        result.set_fetch_callback(mock_fetch)
        result.set_has_more(True)

        collected = []
        for row in result:
            collected.append(row[0])
            if len(collected) >= 3:
                break

        assert collected == [10, 20, 30]

    def test_concurrent_access(self):
        service = SqlService()
        service.start()

        results = []
        errors = []

        def run_query(query_num):
            try:
                r = service.execute(f"SELECT {query_num}")
                results.append(r.query_id)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=run_query, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 10
        assert len(set(results)) == 10
