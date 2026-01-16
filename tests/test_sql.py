"""Tests for hazelcast/sql/result.py and hazelcast/sql/service.py modules."""

import pytest
import asyncio
from datetime import date, time, datetime, timezone
from decimal import Decimal
from unittest.mock import Mock, MagicMock, patch
from concurrent.futures import Future

from hazelcast.sql.result import (
    SqlColumnType,
    SQL_TYPE_TO_PYTHON,
    convert_sql_value,
    SqlColumnMetadata,
    SqlRowMetadata,
    SqlRow,
    SqlPage,
    SqlResult,
)
from hazelcast.sql.service import (
    SqlExplainResult,
    SqlServiceError,
    SqlService,
)
from hazelcast.sql.statement import SqlStatement, SqlExpectedResultType
from hazelcast.exceptions import IllegalStateException


class TestSqlColumnType:
    """Tests for SqlColumnType enum."""

    def test_all_column_types_exist(self):
        """Test all expected column types exist."""
        expected_types = [
            "VARCHAR", "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT",
            "DECIMAL", "REAL", "DOUBLE", "DATE", "TIME", "TIMESTAMP",
            "TIMESTAMP_WITH_TIME_ZONE", "OBJECT", "NULL", "JSON",
        ]
        for type_name in expected_types:
            assert hasattr(SqlColumnType, type_name)

    @pytest.mark.parametrize("col_type,python_type", [
        (SqlColumnType.VARCHAR, str),
        (SqlColumnType.BOOLEAN, bool),
        (SqlColumnType.TINYINT, int),
        (SqlColumnType.INTEGER, int),
        (SqlColumnType.BIGINT, int),
        (SqlColumnType.DECIMAL, Decimal),
        (SqlColumnType.REAL, float),
        (SqlColumnType.DOUBLE, float),
        (SqlColumnType.DATE, date),
        (SqlColumnType.TIME, time),
        (SqlColumnType.TIMESTAMP, datetime),
        (SqlColumnType.NULL, type(None)),
    ])
    def test_type_mappings(self, col_type, python_type):
        """Test SQL to Python type mappings."""
        assert SQL_TYPE_TO_PYTHON[col_type] == python_type


class TestConvertSqlValue:
    """Tests for convert_sql_value function."""

    def test_convert_none(self):
        """Test converting None returns None."""
        assert convert_sql_value(None, SqlColumnType.VARCHAR) is None
        assert convert_sql_value(None, SqlColumnType.INTEGER) is None

    @pytest.mark.parametrize("value,col_type,expected", [
        (42, SqlColumnType.DECIMAL, Decimal("42")),
        (3.14, SqlColumnType.DECIMAL, Decimal("3.14")),
        ("123.45", SqlColumnType.DECIMAL, Decimal("123.45")),
    ])
    def test_convert_decimal(self, value, col_type, expected):
        """Test converting to DECIMAL."""
        result = convert_sql_value(value, col_type)
        assert result == expected

    def test_convert_date_from_string(self):
        """Test converting DATE from ISO string."""
        result = convert_sql_value("2023-06-15", SqlColumnType.DATE)
        assert result == date(2023, 6, 15)

    def test_convert_date_from_int(self):
        """Test converting DATE from ordinal offset."""
        days_since_epoch = 100
        result = convert_sql_value(days_since_epoch, SqlColumnType.DATE)
        assert isinstance(result, date)

    def test_convert_time_from_string(self):
        """Test converting TIME from ISO string."""
        result = convert_sql_value("14:30:45", SqlColumnType.TIME)
        assert result == time(14, 30, 45)

    def test_convert_time_from_nanos(self):
        """Test converting TIME from nanoseconds."""
        nanos = (3600 + 1800 + 30) * 1_000_000_000 + 500_000_000
        result = convert_sql_value(nanos, SqlColumnType.TIME)
        assert isinstance(result, time)
        assert result.hour == 1
        assert result.minute == 30
        assert result.second == 30

    def test_convert_timestamp_from_string(self):
        """Test converting TIMESTAMP from ISO string."""
        result = convert_sql_value("2023-06-15T14:30:45", SqlColumnType.TIMESTAMP)
        assert isinstance(result, datetime)
        assert result.year == 2023
        assert result.month == 6

    def test_convert_timestamp_from_millis(self):
        """Test converting TIMESTAMP from milliseconds."""
        millis = 1686838245000
        result = convert_sql_value(millis, SqlColumnType.TIMESTAMP)
        assert isinstance(result, datetime)

    def test_convert_timestamp_tz_from_millis(self):
        """Test converting TIMESTAMP_WITH_TIME_ZONE from milliseconds."""
        millis = 1686838245000
        result = convert_sql_value(millis, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE)
        assert isinstance(result, datetime)
        assert result.tzinfo == timezone.utc

    def test_convert_json_from_string(self):
        """Test converting JSON from string."""
        result = convert_sql_value('{"key": "value"}', SqlColumnType.JSON)
        assert result == {"key": "value"}

    def test_convert_json_array(self):
        """Test converting JSON array."""
        result = convert_sql_value('[1, 2, 3]', SqlColumnType.JSON)
        assert result == [1, 2, 3]

    def test_convert_json_invalid(self):
        """Test converting invalid JSON returns original string."""
        result = convert_sql_value("not json", SqlColumnType.JSON)
        assert result == "not json"

    def test_convert_passthrough(self):
        """Test that unsupported types pass through unchanged."""
        obj = {"custom": "object"}
        result = convert_sql_value(obj, SqlColumnType.OBJECT)
        assert result is obj


class TestSqlColumnMetadata:
    """Tests for SqlColumnMetadata class."""

    @pytest.fixture
    def column(self):
        """Create a SqlColumnMetadata instance."""
        return SqlColumnMetadata("id", SqlColumnType.INTEGER, False)

    def test_init(self, column):
        """Test initialization."""
        assert column.name == "id"
        assert column.type == SqlColumnType.INTEGER
        assert column.nullable is False

    def test_init_defaults(self):
        """Test initialization with defaults."""
        col = SqlColumnMetadata("name", SqlColumnType.VARCHAR)
        assert col.nullable is True

    def test_repr(self, column):
        """Test __repr__."""
        repr_str = repr(column)
        assert "id" in repr_str
        assert "INTEGER" in repr_str

    def test_equality(self):
        """Test __eq__."""
        col1 = SqlColumnMetadata("id", SqlColumnType.INTEGER)
        col2 = SqlColumnMetadata("id", SqlColumnType.INTEGER)
        col3 = SqlColumnMetadata("id", SqlColumnType.VARCHAR)
        
        assert col1 == col2
        assert col1 != col3
        assert col1 != "not a column"


class TestSqlRowMetadata:
    """Tests for SqlRowMetadata class."""

    @pytest.fixture
    def columns(self):
        """Create column metadata list."""
        return [
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
            SqlColumnMetadata("active", SqlColumnType.BOOLEAN),
        ]

    @pytest.fixture
    def metadata(self, columns):
        """Create SqlRowMetadata instance."""
        return SqlRowMetadata(columns)

    def test_columns_property(self, metadata, columns):
        """Test columns property returns copy."""
        result = metadata.columns
        assert result == columns
        assert result is not columns

    def test_column_count(self, metadata):
        """Test column_count property."""
        assert metadata.column_count == 3

    def test_get_column(self, metadata):
        """Test get_column method."""
        col = metadata.get_column(1)
        assert col.name == "name"

    def test_get_column_out_of_range(self, metadata):
        """Test get_column with invalid index."""
        with pytest.raises(IndexError):
            metadata.get_column(10)

    def test_find_column_found(self, metadata):
        """Test find_column when column exists."""
        assert metadata.find_column("name") == 1
        assert metadata.find_column("id") == 0

    def test_find_column_not_found(self, metadata):
        """Test find_column when column doesn't exist."""
        assert metadata.find_column("nonexistent") == -1

    def test_get_column_names(self, metadata):
        """Test get_column_names method."""
        names = metadata.get_column_names()
        assert names == ["id", "name", "active"]

    def test_get_column_types(self, metadata):
        """Test get_column_types method."""
        types = metadata.get_column_types()
        assert types == [SqlColumnType.INTEGER, SqlColumnType.VARCHAR, SqlColumnType.BOOLEAN]

    def test_repr(self, metadata):
        """Test __repr__."""
        repr_str = repr(metadata)
        assert "SqlRowMetadata" in repr_str

    def test_equality(self, columns):
        """Test __eq__."""
        meta1 = SqlRowMetadata(columns)
        meta2 = SqlRowMetadata(columns)
        meta3 = SqlRowMetadata([])
        
        assert meta1 == meta2
        assert meta1 != meta3
        assert meta1 != "not metadata"


class TestSqlRow:
    """Tests for SqlRow class."""

    @pytest.fixture
    def metadata(self):
        """Create row metadata."""
        columns = [
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
            SqlColumnMetadata("score", SqlColumnType.DOUBLE),
        ]
        return SqlRowMetadata(columns)

    @pytest.fixture
    def row(self, metadata):
        """Create a SqlRow instance."""
        return SqlRow([1, "Alice", 95.5], metadata)

    def test_get_object(self, row):
        """Test get_object by index."""
        assert row.get_object(0) == 1
        assert row.get_object(1) == "Alice"
        assert row.get_object(2) == 95.5

    def test_get_object_out_of_range(self, row):
        """Test get_object with invalid index."""
        with pytest.raises(IndexError):
            row.get_object(10)

    def test_get_object_by_name(self, row):
        """Test get_object_by_name."""
        assert row.get_object_by_name("id") == 1
        assert row.get_object_by_name("name") == "Alice"

    def test_get_object_by_name_not_found(self, row):
        """Test get_object_by_name with unknown column."""
        with pytest.raises(KeyError, match="Column not found"):
            row.get_object_by_name("unknown")

    def test_to_dict(self, row):
        """Test to_dict method."""
        result = row.to_dict()
        assert result == {"id": 1, "name": "Alice", "score": 95.5}

    def test_to_tuple(self, row):
        """Test to_tuple method."""
        result = row.to_tuple()
        assert result == (1, "Alice", 95.5)

    def test_to_list(self, row):
        """Test to_list method."""
        result = row.to_list()
        assert result == [1, "Alice", 95.5]

    def test_getitem_int(self, row):
        """Test __getitem__ with integer index."""
        assert row[0] == 1
        assert row[2] == 95.5

    def test_getitem_str(self, row):
        """Test __getitem__ with string key."""
        assert row["name"] == "Alice"

    def test_len(self, row):
        """Test __len__."""
        assert len(row) == 3

    def test_iter(self, row):
        """Test __iter__."""
        values = list(row)
        assert values == [1, "Alice", 95.5]

    def test_equality(self, metadata):
        """Test __eq__."""
        row1 = SqlRow([1, "Alice", 95.5], metadata)
        row2 = SqlRow([1, "Alice", 95.5], metadata)
        row3 = SqlRow([2, "Bob", 80.0], metadata)
        
        assert row1 == row2
        assert row1 != row3
        assert row1 != "not a row"

    def test_hash(self, row):
        """Test __hash__."""
        h = hash(row)
        assert isinstance(h, int)

    def test_repr(self, row):
        """Test __repr__."""
        repr_str = repr(row)
        assert "SqlRow" in repr_str

    def test_convert_types(self, metadata):
        """Test row with type conversion."""
        row = SqlRow(["42", "Alice", "95.5"], metadata, convert_types=True)
        assert row[0] == "42"


class TestSqlPage:
    """Tests for SqlPage class."""

    @pytest.fixture
    def metadata(self):
        """Create row metadata."""
        return SqlRowMetadata([
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
        ])

    @pytest.fixture
    def rows(self, metadata):
        """Create a list of rows."""
        return [
            SqlRow([1], metadata),
            SqlRow([2], metadata),
            SqlRow([3], metadata),
        ]

    @pytest.fixture
    def page(self, rows, metadata):
        """Create a SqlPage instance."""
        return SqlPage(rows, metadata, is_last=False, page_number=0)

    def test_rows_property(self, page, rows):
        """Test rows property."""
        assert page.rows == rows

    def test_metadata_property(self, page, metadata):
        """Test metadata property."""
        assert page.metadata is metadata

    def test_is_last_property(self, page):
        """Test is_last property."""
        assert page.is_last is False

    def test_page_number_property(self, page):
        """Test page_number property."""
        assert page.page_number == 0

    def test_row_count_property(self, page):
        """Test row_count property."""
        assert page.row_count == 3

    def test_to_dicts(self, page):
        """Test to_dicts method."""
        dicts = page.to_dicts()
        assert len(dicts) == 3
        assert dicts[0] == {"id": 1}

    def test_to_tuples(self, page):
        """Test to_tuples method."""
        tuples = page.to_tuples()
        assert tuples == [(1,), (2,), (3,)]

    def test_iter(self, page):
        """Test __iter__."""
        rows = list(page)
        assert len(rows) == 3

    def test_len(self, page):
        """Test __len__."""
        assert len(page) == 3

    def test_getitem(self, page):
        """Test __getitem__."""
        row = page[1]
        assert row[0] == 2

    def test_repr(self, page):
        """Test __repr__."""
        repr_str = repr(page)
        assert "SqlPage" in repr_str
        assert "page_number=0" in repr_str


class TestSqlResult:
    """Tests for SqlResult class."""

    @pytest.fixture
    def metadata(self):
        """Create row metadata."""
        return SqlRowMetadata([
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
        ])

    @pytest.fixture
    def result(self, metadata):
        """Create a SqlResult instance."""
        result = SqlResult(
            query_id="test-query-123",
            metadata=metadata,
            update_count=-1,
        )
        rows = [
            SqlRow([1, "Alice"], metadata),
            SqlRow([2, "Bob"], metadata),
            SqlRow([3, "Charlie"], metadata),
        ]
        result.add_rows(rows)
        result.set_has_more(False)
        return result

    @pytest.fixture
    def update_result(self):
        """Create a SqlResult for DML."""
        return SqlResult(update_count=5)

    def test_query_id_property(self, result):
        """Test query_id property."""
        assert result.query_id == "test-query-123"

    def test_metadata_property(self, result, metadata):
        """Test metadata property."""
        assert result.metadata is metadata

    def test_update_count_property(self, update_result):
        """Test update_count property."""
        assert update_result.update_count == 5

    def test_is_row_set_true(self, result):
        """Test is_row_set for SELECT."""
        assert result.is_row_set is True

    def test_is_row_set_false(self, update_result):
        """Test is_row_set for DML."""
        assert update_result.is_row_set is False

    def test_is_closed_initially_false(self, result):
        """Test is_closed is initially False."""
        assert result.is_closed is False

    def test_iteration(self, result):
        """Test iteration over rows."""
        rows = list(result)
        assert len(rows) == 3
        assert rows[0]["id"] == 1
        assert rows[1]["name"] == "Bob"

    def test_iteration_update_result(self, update_result):
        """Test iteration over DML result yields nothing."""
        rows = list(update_result)
        assert rows == []

    def test_get_all(self, result):
        """Test get_all method."""
        result2 = SqlResult(
            query_id="q2",
            metadata=result.metadata,
        )
        result2.add_rows([SqlRow([4, "Dave"], result.metadata)])
        result2.set_has_more(False)
        
        rows = result2.get_all()
        assert len(rows) == 1

    def test_fetch_page(self, result):
        """Test fetch_page method."""
        page = result.fetch_page()
        assert page.row_count == 3
        assert page.page_number == 0

    def test_pages_iteration(self, metadata):
        """Test pages iterator."""
        result = SqlResult(query_id="q", metadata=metadata)
        result.add_rows([SqlRow([1, "A"], metadata), SqlRow([2, "B"], metadata)])
        result.set_has_more(False)
        
        pages = list(result.pages())
        assert len(pages) == 1
        assert pages[0].row_count == 2

    def test_close(self, result):
        """Test close method."""
        result.close()
        assert result.is_closed is True

    def test_close_twice(self, result):
        """Test close is idempotent."""
        result.close()
        result.close()
        assert result.is_closed is True

    def test_context_manager(self, result):
        """Test context manager protocol."""
        with result as r:
            assert r.is_closed is False
        assert result.is_closed is True

    @pytest.mark.asyncio
    async def test_async_context_manager(self, result):
        """Test async context manager protocol."""
        async with result as r:
            assert r.is_closed is False
        assert result.is_closed is True

    def test_rows_as_dicts(self, metadata):
        """Test rows_as_dicts method."""
        result = SqlResult(query_id="q", metadata=metadata)
        result.add_rows([SqlRow([1, "A"], metadata)])
        result.set_has_more(False)
        
        dicts = result.rows_as_dicts()
        assert dicts == [{"id": 1, "name": "A"}]

    def test_rows_as_tuples(self, metadata):
        """Test rows_as_tuples method."""
        result = SqlResult(query_id="q", metadata=metadata)
        result.add_rows([SqlRow([1, "A"], metadata)])
        result.set_has_more(False)
        
        tuples = result.rows_as_tuples()
        assert tuples == [(1, "A")]

    def test_first(self, result):
        """Test first method."""
        row = result.first()
        assert row is not None
        assert row["id"] == 1

    def test_first_empty(self, update_result):
        """Test first returns None for empty result."""
        assert update_result.first() is None

    def test_first_or_raise(self, result):
        """Test first_or_raise method."""
        row = result.first_or_raise()
        assert row["id"] == 1

    def test_first_or_raise_empty(self, update_result):
        """Test first_or_raise raises on empty result."""
        with pytest.raises(ValueError, match="No rows"):
            update_result.first_or_raise()

    def test_scalar(self):
        """Test scalar method."""
        metadata = SqlRowMetadata([SqlColumnMetadata("count", SqlColumnType.INTEGER)])
        result = SqlResult(query_id="q", metadata=metadata)
        result.add_rows([SqlRow([42], metadata)])
        result.set_has_more(False)
        
        assert result.scalar() == 42

    def test_scalar_multiple_columns_raises(self, result):
        """Test scalar raises when multiple columns."""
        with pytest.raises(ValueError, match="Expected 1 column"):
            result.scalar()

    def test_column_values(self, result):
        """Test column_values method."""
        values = result.column_values("id")
        assert values == [1, 2, 3]

    def test_len(self, result):
        """Test __len__."""
        assert len(result) == 3

    def test_bool_row_set(self, result):
        """Test __bool__ for row set."""
        assert bool(result) is True

    def test_bool_update_result(self, update_result):
        """Test __bool__ for update result."""
        assert bool(update_result) is False

    def test_repr_row_set(self, result):
        """Test __repr__ for row set."""
        repr_str = repr(result)
        assert "is_row_set=True" in repr_str

    def test_repr_update(self, update_result):
        """Test __repr__ for update result."""
        repr_str = repr(update_result)
        assert "update_count=5" in repr_str

    def test_set_backpressure_threshold(self, result):
        """Test set_backpressure_threshold method."""
        result.set_backpressure_threshold(500)
        assert result._backpressure_threshold == 500

    def test_set_backpressure_threshold_invalid(self, result):
        """Test set_backpressure_threshold with invalid value."""
        with pytest.raises(ValueError, match="must be positive"):
            result.set_backpressure_threshold(0)

    @pytest.mark.asyncio
    async def test_async_iteration(self, metadata):
        """Test async iteration."""
        result = SqlResult(query_id="q", metadata=metadata)
        result.add_rows([SqlRow([1, "A"], metadata), SqlRow([2, "B"], metadata)])
        result.set_has_more(False)
        
        rows = []
        async for row in result:
            rows.append(row)
        
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_get_all_async(self, metadata):
        """Test get_all_async method."""
        result = SqlResult(query_id="q", metadata=metadata)
        result.add_rows([SqlRow([1, "A"], metadata)])
        result.set_has_more(False)
        
        rows = await result.get_all_async()
        assert len(rows) == 1


class TestSqlExplainResult:
    """Tests for SqlExplainResult class."""

    @pytest.fixture
    def explain_result(self):
        """Create a SqlExplainResult instance."""
        return SqlExplainResult(
            plan="HashJoin\n  TableScan",
            plan_nodes=[{"type": "HashJoin"}, {"type": "TableScan"}],
            query="SELECT * FROM test",
        )

    def test_plan_property(self, explain_result):
        """Test plan property."""
        assert "HashJoin" in explain_result.plan

    def test_plan_nodes_property(self, explain_result):
        """Test plan_nodes property."""
        assert len(explain_result.plan_nodes) == 2

    def test_query_property(self, explain_result):
        """Test query property."""
        assert explain_result.query == "SELECT * FROM test"

    def test_str(self, explain_result):
        """Test __str__."""
        assert str(explain_result) == explain_result.plan

    def test_repr(self, explain_result):
        """Test __repr__."""
        repr_str = repr(explain_result)
        assert "SqlExplainResult" in repr_str


class TestSqlServiceError:
    """Tests for SqlServiceError class."""

    def test_basic_error(self):
        """Test basic error creation."""
        error = SqlServiceError("Query failed")
        assert str(error) == "Query failed"

    def test_error_with_code(self):
        """Test error with code."""
        error = SqlServiceError("Timeout", code=SqlServiceError.SQL_ERROR_CODE_TIMEOUT)
        assert error.code == SqlServiceError.SQL_ERROR_CODE_TIMEOUT

    def test_error_with_suggestion(self):
        """Test error with suggestion."""
        error = SqlServiceError(
            "Syntax error",
            code=SqlServiceError.SQL_ERROR_CODE_PARSING,
            suggestion="Check your SQL syntax",
        )
        assert error.suggestion == "Check your SQL syntax"

    def test_error_codes(self):
        """Test error code constants."""
        assert SqlServiceError.SQL_ERROR_CODE_CANCELLED == -1
        assert SqlServiceError.SQL_ERROR_CODE_TIMEOUT == -2
        assert SqlServiceError.SQL_ERROR_CODE_PARSING == -3
        assert SqlServiceError.SQL_ERROR_CODE_GENERIC == -4

    def test_str_with_all_fields(self):
        """Test __str__ with all fields set."""
        error = SqlServiceError(
            "Error",
            code=-2,
            suggestion="Try again",
        )
        str_repr = str(error)
        assert "code=-2" in str_repr
        assert "suggestion=Try again" in str_repr


class TestSqlService:
    """Tests for SqlService class."""

    @pytest.fixture
    def mock_invocation_service(self):
        """Create a mock InvocationService."""
        return Mock()

    @pytest.fixture
    def mock_serialization_service(self):
        """Create a mock SerializationService."""
        service = Mock()
        service.to_data = Mock(return_value=b"data")
        return service

    @pytest.fixture
    def service(self, mock_invocation_service, mock_serialization_service):
        """Create a SqlService instance."""
        return SqlService(
            invocation_service=mock_invocation_service,
            serialization_service=mock_serialization_service,
        )

    @pytest.fixture
    def service_no_invocation(self):
        """Create SqlService without invocation service."""
        return SqlService()

    def test_start(self, service):
        """Test start method."""
        assert not service.is_running
        service.start()
        assert service.is_running

    def test_shutdown(self, service):
        """Test shutdown method."""
        service.start()
        service.shutdown()
        assert not service.is_running

    def test_execute_not_running(self, service):
        """Test execute raises when not running."""
        with pytest.raises(IllegalStateException, match="not running"):
            service.execute("SELECT * FROM test")

    def test_execute_empty_sql(self, service):
        """Test execute raises for empty SQL."""
        service.start()
        with pytest.raises(SqlServiceError, match="cannot be empty"):
            service.execute("")

    def test_execute_dml_without_invocation(self, service_no_invocation):
        """Test execute DML without invocation service."""
        service_no_invocation.start()
        result = service_no_invocation.execute("INSERT INTO test VALUES (1)")
        assert result.update_count == 0

    def test_execute_select_without_invocation(self, service_no_invocation):
        """Test execute SELECT without invocation service."""
        service_no_invocation.start()
        result = service_no_invocation.execute("SELECT * FROM test")
        assert result.is_row_set

    def test_execute_async(self, service_no_invocation):
        """Test execute_async method."""
        service_no_invocation.start()
        future = service_no_invocation.execute_async("SELECT * FROM test")
        result = future.result()
        assert result.is_row_set

    def test_execute_async_not_running(self, service):
        """Test execute_async when not running."""
        future = service.execute_async("SELECT * FROM test")
        with pytest.raises(IllegalStateException):
            future.result()

    def test_explain_not_running(self, service):
        """Test explain raises when not running."""
        with pytest.raises(IllegalStateException, match="not running"):
            service.explain("SELECT * FROM test")

    def test_explain_empty_sql(self, service):
        """Test explain raises for empty SQL."""
        service.start()
        with pytest.raises(SqlServiceError, match="cannot be empty"):
            service.explain("")

    def test_get_column_type_for_value(self, service):
        """Test get_column_type_for_value method."""
        assert service.get_column_type_for_value(None) == SqlColumnType.NULL
        assert service.get_column_type_for_value("test") == SqlColumnType.VARCHAR
        assert service.get_column_type_for_value(True) == SqlColumnType.BOOLEAN
        assert service.get_column_type_for_value(42) == SqlColumnType.BIGINT
        assert service.get_column_type_for_value(3.14) == SqlColumnType.DOUBLE
        assert service.get_column_type_for_value(b"data") == SqlColumnType.OBJECT

    def test_get_active_query_count(self, service):
        """Test get_active_query_count method."""
        service.start()
        assert service.get_active_query_count() == 0

    def test_cancel_all_queries(self, service):
        """Test cancel_all_queries method."""
        service.start()
        cancelled = service.cancel_all_queries()
        assert cancelled == 0

    def test_repr(self, service):
        """Test __repr__."""
        service.start()
        repr_str = repr(service)
        assert "SqlService" in repr_str
        assert "running=True" in repr_str

    @pytest.mark.parametrize("sql,is_dml", [
        ("SELECT * FROM test", False),
        ("INSERT INTO test VALUES (1)", True),
        ("UPDATE test SET x = 1", True),
        ("DELETE FROM test", True),
        ("CREATE TABLE test (id INT)", True),
        ("DROP TABLE test", True),
        ("ALTER TABLE test ADD col INT", True),
    ])
    def test_is_dml_query(self, service, sql, is_dml):
        """Test _is_dml_query method."""
        assert service._is_dml_query(sql) == is_dml

    def test_convert_timeout(self, service):
        """Test _convert_timeout method."""
        assert service._convert_timeout(-1) == -1
        assert service._convert_timeout(5.0) == 5000
        assert service._convert_timeout(0.5) == 500

    def test_serialize_parameters_empty(self, service):
        """Test _serialize_parameters with empty list."""
        result = service._serialize_parameters([])
        assert result == []

    def test_serialize_parameters_with_values(self, service):
        """Test _serialize_parameters with values."""
        result = service._serialize_parameters([1, "test", None])
        assert len(result) == 3
        assert result[2] == b""

    def test_serialize_primitive_none(self, service):
        """Test _serialize_primitive with None."""
        assert service._serialize_primitive(None) == b""

    def test_serialize_primitive_bool(self, service):
        """Test _serialize_primitive with bool."""
        assert service._serialize_primitive(True) == b"\x01"
        assert service._serialize_primitive(False) == b"\x00"

    def test_serialize_primitive_string(self, service):
        """Test _serialize_primitive with string."""
        assert service._serialize_primitive("hello") == b"hello"
