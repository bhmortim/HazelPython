"""Comprehensive tests for SQL service functionality."""

import asyncio
import pytest
from concurrent.futures import Future
from datetime import date, time, datetime, timezone
from decimal import Decimal
from unittest.mock import Mock, MagicMock, patch

from hazelcast.sql.statement import SqlStatement, SqlExpectedResultType
from hazelcast.sql.result import (
    SqlColumnType,
    SqlColumnMetadata,
    SqlRowMetadata,
    SqlRow,
    SqlResult,
    SqlPage,
    convert_sql_value,
    SQL_TYPE_TO_PYTHON,
)
from hazelcast.sql.service import SqlService, SqlServiceError, SqlExplainResult
from hazelcast.exceptions import IllegalStateException


class TestSqlColumnType:
    """Tests for SqlColumnType enum."""

    def test_all_column_types_defined(self):
        """Verify all expected column types are defined."""
        expected_types = {
            "VARCHAR", "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER",
            "BIGINT", "DECIMAL", "REAL", "DOUBLE", "DATE", "TIME",
            "TIMESTAMP", "TIMESTAMP_WITH_TIME_ZONE", "OBJECT", "NULL", "JSON"
        }
        actual_types = {t.name for t in SqlColumnType}
        assert actual_types == expected_types

    def test_column_type_values(self):
        """Verify column type values match names."""
        for col_type in SqlColumnType:
            assert col_type.value == col_type.name


class TestSqlColumnMetadata:
    """Tests for SqlColumnMetadata class."""

    def test_basic_creation(self):
        """Test creating column metadata."""
        meta = SqlColumnMetadata("id", SqlColumnType.INTEGER, False)
        assert meta.name == "id"
        assert meta.type == SqlColumnType.INTEGER
        assert meta.nullable is False

    def test_nullable_default(self):
        """Test default nullable value."""
        meta = SqlColumnMetadata("name", SqlColumnType.VARCHAR)
        assert meta.nullable is True

    def test_repr(self):
        """Test string representation."""
        meta = SqlColumnMetadata("age", SqlColumnType.INTEGER)
        assert "age" in repr(meta)
        assert "INTEGER" in repr(meta)

    def test_equality(self):
        """Test metadata equality."""
        meta1 = SqlColumnMetadata("id", SqlColumnType.INTEGER)
        meta2 = SqlColumnMetadata("id", SqlColumnType.INTEGER)
        meta3 = SqlColumnMetadata("name", SqlColumnType.VARCHAR)

        assert meta1 == meta2
        assert meta1 != meta3
        assert meta1 != "not a metadata"


class TestSqlRowMetadata:
    """Tests for SqlRowMetadata class."""

    def test_basic_creation(self):
        """Test creating row metadata."""
        columns = [
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
        ]
        meta = SqlRowMetadata(columns)
        assert meta.column_count == 2

    def test_get_column(self):
        """Test getting column by index."""
        columns = [
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
        ]
        meta = SqlRowMetadata(columns)

        col = meta.get_column(0)
        assert col.name == "id"

        col = meta.get_column(1)
        assert col.name == "name"

    def test_get_column_out_of_range(self):
        """Test getting column with invalid index."""
        meta = SqlRowMetadata([SqlColumnMetadata("id", SqlColumnType.INTEGER)])

        with pytest.raises(IndexError):
            meta.get_column(5)

    def test_find_column(self):
        """Test finding column index by name."""
        columns = [
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
        ]
        meta = SqlRowMetadata(columns)

        assert meta.find_column("id") == 0
        assert meta.find_column("name") == 1
        assert meta.find_column("unknown") == -1

    def test_columns_property(self):
        """Test columns property returns a copy."""
        columns = [SqlColumnMetadata("id", SqlColumnType.INTEGER)]
        meta = SqlRowMetadata(columns)

        retrieved = meta.columns
        assert len(retrieved) == 1
        retrieved.append(SqlColumnMetadata("extra", SqlColumnType.VARCHAR))
        assert meta.column_count == 1

    def test_get_column_names(self):
        """Test getting all column names."""
        columns = [
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
        ]
        meta = SqlRowMetadata(columns)
        names = meta.get_column_names()
        assert names == ["id", "name"]

    def test_get_column_types(self):
        """Test getting all column types."""
        columns = [
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
        ]
        meta = SqlRowMetadata(columns)
        types = meta.get_column_types()
        assert types == [SqlColumnType.INTEGER, SqlColumnType.VARCHAR]

    def test_equality(self):
        """Test row metadata equality."""
        columns = [SqlColumnMetadata("id", SqlColumnType.INTEGER)]
        meta1 = SqlRowMetadata(columns)
        meta2 = SqlRowMetadata(columns)
        assert meta1 == meta2


class TestSqlRow:
    """Tests for SqlRow class."""

    @pytest.fixture
    def sample_metadata(self):
        """Create sample row metadata."""
        return SqlRowMetadata([
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
            SqlColumnMetadata("active", SqlColumnType.BOOLEAN),
        ])

    def test_basic_creation(self, sample_metadata):
        """Test creating a row."""
        row = SqlRow([1, "Alice", True], sample_metadata)
        assert len(row) == 3
        assert row.metadata == sample_metadata

    def test_get_object_by_index(self, sample_metadata):
        """Test getting value by index."""
        row = SqlRow([1, "Alice", True], sample_metadata)
        assert row.get_object(0) == 1
        assert row.get_object(1) == "Alice"
        assert row.get_object(2) is True

    def test_get_object_by_name(self, sample_metadata):
        """Test getting value by name."""
        row = SqlRow([1, "Alice", True], sample_metadata)
        assert row.get_object_by_name("id") == 1
        assert row.get_object_by_name("name") == "Alice"

    def test_get_object_by_name_not_found(self, sample_metadata):
        """Test getting value by non-existent name."""
        row = SqlRow([1, "Alice", True], sample_metadata)
        with pytest.raises(KeyError):
            row.get_object_by_name("unknown")

    def test_bracket_access(self, sample_metadata):
        """Test bracket access for values."""
        row = SqlRow([1, "Alice", True], sample_metadata)
        assert row[0] == 1
        assert row["name"] == "Alice"

    def test_to_dict(self, sample_metadata):
        """Test converting row to dictionary."""
        row = SqlRow([1, "Alice", True], sample_metadata)
        d = row.to_dict()
        assert d == {"id": 1, "name": "Alice", "active": True}

    def test_to_dict_with_type_conversion(self, sample_metadata):
        """Test dictionary conversion with type conversion."""
        row = SqlRow([1, "Alice", True], sample_metadata)
        d = row.to_dict(convert_types=True)
        assert d == {"id": 1, "name": "Alice", "active": True}

    def test_to_tuple(self, sample_metadata):
        """Test converting row to tuple."""
        row = SqlRow([1, "Alice", True], sample_metadata)
        t = row.to_tuple()
        assert t == (1, "Alice", True)

    def test_to_list(self, sample_metadata):
        """Test converting row to list."""
        row = SqlRow([1, "Alice", True], sample_metadata)
        lst = row.to_list()
        assert lst == [1, "Alice", True]

    def test_iteration(self, sample_metadata):
        """Test iterating over row values."""
        row = SqlRow([1, "Alice", True], sample_metadata)
        values = list(row)
        assert values == [1, "Alice", True]

    def test_equality(self, sample_metadata):
        """Test row equality."""
        row1 = SqlRow([1, "Alice", True], sample_metadata)
        row2 = SqlRow([1, "Alice", True], sample_metadata)
        row3 = SqlRow([2, "Bob", False], sample_metadata)

        assert row1 == row2
        assert row1 != row3

    def test_hash(self, sample_metadata):
        """Test row hashing."""
        row1 = SqlRow([1, "Alice", True], sample_metadata)
        row2 = SqlRow([1, "Alice", True], sample_metadata)
        assert hash(row1) == hash(row2)

    def test_repr(self, sample_metadata):
        """Test string representation."""
        row = SqlRow([1, "Alice", True], sample_metadata)
        repr_str = repr(row)
        assert "SqlRow" in repr_str
        assert "Alice" in repr_str


class TestConvertSqlValue:
    """Tests for SQL value type conversion."""

    def test_convert_none(self):
        """Test converting None values."""
        assert convert_sql_value(None, SqlColumnType.VARCHAR) is None
        assert convert_sql_value(None, SqlColumnType.INTEGER) is None

    def test_convert_decimal_from_int(self):
        """Test converting integer to Decimal."""
        result = convert_sql_value(42, SqlColumnType.DECIMAL)
        assert isinstance(result, Decimal)
        assert result == Decimal("42")

    def test_convert_decimal_from_float(self):
        """Test converting float to Decimal."""
        result = convert_sql_value(3.14, SqlColumnType.DECIMAL)
        assert isinstance(result, Decimal)

    def test_convert_decimal_from_string(self):
        """Test converting string to Decimal."""
        result = convert_sql_value("123.45", SqlColumnType.DECIMAL)
        assert result == Decimal("123.45")

    def test_convert_date_from_string(self):
        """Test converting string to date."""
        result = convert_sql_value("2024-01-15", SqlColumnType.DATE)
        assert isinstance(result, date)
        assert result == date(2024, 1, 15)

    def test_convert_time_from_string(self):
        """Test converting string to time."""
        result = convert_sql_value("14:30:00", SqlColumnType.TIME)
        assert isinstance(result, time)
        assert result.hour == 14
        assert result.minute == 30

    def test_convert_timestamp_from_string(self):
        """Test converting string to datetime."""
        result = convert_sql_value("2024-01-15T14:30:00", SqlColumnType.TIMESTAMP)
        assert isinstance(result, datetime)
        assert result.year == 2024

    def test_convert_timestamp_with_tz_from_int(self):
        """Test converting timestamp int to timezone-aware datetime."""
        ts_millis = 1705329000000
        result = convert_sql_value(ts_millis, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE)
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_convert_json_from_string(self):
        """Test converting JSON string to object."""
        result = convert_sql_value('{"key": "value"}', SqlColumnType.JSON)
        assert result == {"key": "value"}

    def test_convert_json_invalid_string(self):
        """Test handling invalid JSON string."""
        result = convert_sql_value("not json", SqlColumnType.JSON)
        assert result == "not json"

    def test_convert_passthrough(self):
        """Test values pass through unchanged for unsupported types."""
        obj = object()
        result = convert_sql_value(obj, SqlColumnType.OBJECT)
        assert result is obj


class TestSqlPage:
    """Tests for SqlPage class."""

    @pytest.fixture
    def sample_metadata(self):
        return SqlRowMetadata([
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
        ])

    @pytest.fixture
    def sample_rows(self, sample_metadata):
        return [
            SqlRow([1, "Alice"], sample_metadata),
            SqlRow([2, "Bob"], sample_metadata),
        ]

    def test_basic_creation(self, sample_rows, sample_metadata):
        """Test creating a page."""
        page = SqlPage(sample_rows, sample_metadata, is_last=False, page_number=0)
        assert page.row_count == 2
        assert page.page_number == 0
        assert page.is_last is False

    def test_empty_page(self, sample_metadata):
        """Test empty page."""
        page = SqlPage([], sample_metadata, is_last=True)
        assert page.row_count == 0
        assert page.is_last is True

    def test_iteration(self, sample_rows, sample_metadata):
        """Test iterating over page rows."""
        page = SqlPage(sample_rows, sample_metadata)
        rows = list(page)
        assert len(rows) == 2

    def test_indexing(self, sample_rows, sample_metadata):
        """Test index access."""
        page = SqlPage(sample_rows, sample_metadata)
        assert page[0].get_object_by_name("name") == "Alice"
        assert page[1].get_object_by_name("name") == "Bob"

    def test_len(self, sample_rows, sample_metadata):
        """Test length."""
        page = SqlPage(sample_rows, sample_metadata)
        assert len(page) == 2

    def test_to_dicts(self, sample_rows, sample_metadata):
        """Test converting page to dictionaries."""
        page = SqlPage(sample_rows, sample_metadata)
        dicts = page.to_dicts()
        assert dicts == [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

    def test_to_tuples(self, sample_rows, sample_metadata):
        """Test converting page to tuples."""
        page = SqlPage(sample_rows, sample_metadata)
        tuples = page.to_tuples()
        assert tuples == [(1, "Alice"), (2, "Bob")]

    def test_repr(self, sample_rows, sample_metadata):
        """Test string representation."""
        page = SqlPage(sample_rows, sample_metadata, is_last=True, page_number=5)
        repr_str = repr(page)
        assert "SqlPage" in repr_str
        assert "page_number=5" in repr_str


class TestSqlResult:
    """Tests for SqlResult class."""

    @pytest.fixture
    def sample_metadata(self):
        return SqlRowMetadata([
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
        ])

    def test_row_set_result(self, sample_metadata):
        """Test creating a row set result."""
        result = SqlResult(query_id="test-id", metadata=sample_metadata, update_count=-1)
        assert result.is_row_set is True
        assert result.query_id == "test-id"

    def test_update_count_result(self):
        """Test creating an update count result."""
        result = SqlResult(update_count=5)
        assert result.is_row_set is False
        assert result.update_count == 5

    def test_add_rows_and_iterate(self, sample_metadata):
        """Test adding rows and iterating."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.set_has_more(False)

        rows = [
            SqlRow([1, "Alice"], sample_metadata),
            SqlRow([2, "Bob"], sample_metadata),
        ]
        result.add_rows(rows)

        collected = list(result)
        assert len(collected) == 2
        assert collected[0].get_object_by_name("name") == "Alice"

    def test_get_all(self, sample_metadata):
        """Test getting all rows."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.set_has_more(False)
        result.add_rows([
            SqlRow([1, "Alice"], sample_metadata),
            SqlRow([2, "Bob"], sample_metadata),
        ])

        all_rows = result.get_all()
        assert len(all_rows) == 2

    def test_rows_as_dicts(self, sample_metadata):
        """Test getting rows as dictionaries."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.set_has_more(False)
        result.add_rows([
            SqlRow([1, "Alice"], sample_metadata),
        ])

        dicts = result.rows_as_dicts()
        assert dicts == [{"id": 1, "name": "Alice"}]

    def test_rows_as_tuples(self, sample_metadata):
        """Test getting rows as tuples."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.set_has_more(False)
        result.add_rows([
            SqlRow([1, "Alice"], sample_metadata),
        ])

        tuples = result.rows_as_tuples()
        assert tuples == [(1, "Alice")]

    def test_first(self, sample_metadata):
        """Test getting first row."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.set_has_more(False)
        result.add_rows([
            SqlRow([1, "Alice"], sample_metadata),
            SqlRow([2, "Bob"], sample_metadata),
        ])

        first = result.first()
        assert first is not None
        assert first.get_object_by_name("name") == "Alice"

    def test_first_empty(self, sample_metadata):
        """Test first on empty result."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.set_has_more(False)
        assert result.first() is None

    def test_first_or_raise(self, sample_metadata):
        """Test first_or_raise."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.set_has_more(False)

        with pytest.raises(ValueError):
            result.first_or_raise()

    def test_scalar(self, sample_metadata):
        """Test scalar value extraction."""
        meta = SqlRowMetadata([SqlColumnMetadata("count", SqlColumnType.INTEGER)])
        result = SqlResult(metadata=meta, update_count=-1)
        result.set_has_more(False)
        result.add_rows([SqlRow([42], meta)])

        assert result.scalar() == 42

    def test_scalar_wrong_column_count(self, sample_metadata):
        """Test scalar with multiple columns."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.set_has_more(False)
        result.add_rows([SqlRow([1, "Alice"], sample_metadata)])

        with pytest.raises(ValueError):
            result.scalar()

    def test_column_values(self, sample_metadata):
        """Test getting column values."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.set_has_more(False)
        result.add_rows([
            SqlRow([1, "Alice"], sample_metadata),
            SqlRow([2, "Bob"], sample_metadata),
        ])

        names = result.column_values("name")
        assert names == ["Alice", "Bob"]

    def test_close(self, sample_metadata):
        """Test closing result."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.add_rows([SqlRow([1, "Alice"], sample_metadata)])

        result.close()
        assert result.is_closed is True

    def test_context_manager(self, sample_metadata):
        """Test context manager protocol."""
        with SqlResult(metadata=sample_metadata, update_count=-1) as result:
            result.set_has_more(False)
            result.add_rows([SqlRow([1, "Alice"], sample_metadata)])
            assert result.is_closed is False

        assert result.is_closed is True

    def test_len(self, sample_metadata):
        """Test length of result."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.add_rows([
            SqlRow([1, "Alice"], sample_metadata),
            SqlRow([2, "Bob"], sample_metadata),
        ])
        assert len(result) == 2

    def test_bool_row_set(self, sample_metadata):
        """Test bool on row set result."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        assert bool(result) is True

    def test_bool_update_count(self):
        """Test bool on update count result."""
        result = SqlResult(update_count=5)
        assert bool(result) is False

    def test_fetch_page(self, sample_metadata):
        """Test fetching a page."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.set_has_more(False)
        result.add_rows([
            SqlRow([1, "Alice"], sample_metadata),
            SqlRow([2, "Bob"], sample_metadata),
        ])

        page = result.fetch_page()
        assert page.row_count == 2
        assert page.is_last is True

    def test_pages_iterator(self, sample_metadata):
        """Test pages iterator."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.set_has_more(False)
        result.add_rows([
            SqlRow([1, "Alice"], sample_metadata),
            SqlRow([2, "Bob"], sample_metadata),
        ])

        pages = list(result.pages())
        assert len(pages) == 1
        assert pages[0].row_count == 2

    def test_backpressure_threshold(self, sample_metadata):
        """Test setting backpressure threshold."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.set_backpressure_threshold(500)
        assert result._backpressure_threshold == 500

    def test_backpressure_threshold_invalid(self, sample_metadata):
        """Test invalid backpressure threshold."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        with pytest.raises(ValueError):
            result.set_backpressure_threshold(0)

    def test_repr_row_set(self, sample_metadata):
        """Test repr for row set result."""
        result = SqlResult(query_id="test-123", metadata=sample_metadata, update_count=-1)
        assert "test-123" in repr(result)
        assert "is_row_set=True" in repr(result)

    def test_repr_update_count(self):
        """Test repr for update count result."""
        result = SqlResult(update_count=10)
        assert "update_count=10" in repr(result)


class TestSqlStatement:
    """Tests for SqlStatement class."""

    def test_basic_creation(self):
        """Test creating a statement."""
        stmt = SqlStatement("SELECT * FROM users")
        assert stmt.sql == "SELECT * FROM users"
        assert stmt.timeout == SqlStatement.DEFAULT_TIMEOUT
        assert stmt.cursor_buffer_size == SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE

    def test_empty_sql_validation(self):
        """Test validation of empty SQL."""
        stmt = SqlStatement()
        with pytest.raises(ValueError):
            stmt.sql = ""

    def test_add_parameter(self):
        """Test adding parameters."""
        stmt = SqlStatement("SELECT * FROM users WHERE id = ?")
        stmt.add_parameter(42)
        assert stmt.parameters == [42]

    def test_set_parameters(self):
        """Test setting multiple parameters."""
        stmt = SqlStatement("SELECT * FROM users WHERE id = ? AND name = ?")
        stmt.set_parameters(42, "Alice")
        assert stmt.parameters == [42, "Alice"]

    def test_clear_parameters(self):
        """Test clearing parameters."""
        stmt = SqlStatement()
        stmt.add_parameter(1)
        stmt.add_parameter(2)
        stmt.clear_parameters()
        assert stmt.parameters == []

    def test_timeout_setter(self):
        """Test timeout setter."""
        stmt = SqlStatement()
        stmt.timeout = 30.0
        assert stmt.timeout == 30.0

    def test_timeout_invalid(self):
        """Test invalid timeout."""
        stmt = SqlStatement()
        with pytest.raises(ValueError):
            stmt.timeout = -5

    def test_cursor_buffer_size_setter(self):
        """Test cursor buffer size setter."""
        stmt = SqlStatement()
        stmt.cursor_buffer_size = 1000
        assert stmt.cursor_buffer_size == 1000

    def test_cursor_buffer_size_invalid(self):
        """Test invalid cursor buffer size."""
        stmt = SqlStatement()
        with pytest.raises(ValueError):
            stmt.cursor_buffer_size = 0

    def test_schema_setter(self):
        """Test schema setter."""
        stmt = SqlStatement()
        stmt.schema = "public"
        assert stmt.schema == "public"

    def test_expected_result_type(self):
        """Test expected result type."""
        stmt = SqlStatement()
        stmt.expected_result_type = SqlExpectedResultType.ROWS
        assert stmt.expected_result_type == SqlExpectedResultType.ROWS

    def test_copy(self):
        """Test copying statement."""
        stmt = SqlStatement("SELECT * FROM users")
        stmt.add_parameter(1)
        stmt.timeout = 10.0
        stmt.schema = "test"

        copy = stmt.copy()
        assert copy.sql == stmt.sql
        assert copy.parameters == stmt.parameters
        assert copy.timeout == stmt.timeout
        assert copy.schema == stmt.schema

        copy.add_parameter(2)
        assert len(stmt.parameters) == 1

    def test_method_chaining(self):
        """Test method chaining."""
        stmt = (
            SqlStatement("SELECT * FROM users WHERE id = ?")
            .add_parameter(1)
            .add_parameter(2)
            .clear_parameters()
            .add_parameter(3)
        )
        assert stmt.parameters == [3]

    def test_repr(self):
        """Test string representation."""
        stmt = SqlStatement("SELECT 1")
        assert "SELECT 1" in repr(stmt)


class TestSqlExpectedResultType:
    """Tests for SqlExpectedResultType enum."""

    def test_all_types_defined(self):
        """Verify all expected result types are defined."""
        expected = {"ANY", "ROWS", "UPDATE_COUNT"}
        actual = {t.name for t in SqlExpectedResultType}
        assert actual == expected


class TestSqlExplainResult:
    """Tests for SqlExplainResult class."""

    def test_basic_creation(self):
        """Test creating an explain result."""
        result = SqlExplainResult(
            plan="PhysicalScan\n  -> Filter",
            plan_nodes=[{"type": "scan"}, {"type": "filter"}],
            query="SELECT * FROM users",
        )
        assert result.query == "SELECT * FROM users"
        assert "PhysicalScan" in result.plan
        assert len(result.plan_nodes) == 2

    def test_str(self):
        """Test string conversion."""
        result = SqlExplainResult(plan="Test Plan", query="SELECT 1")
        assert str(result) == "Test Plan"

    def test_repr(self):
        """Test repr."""
        result = SqlExplainResult(plan="Plan", query="SELECT 1")
        assert "SELECT 1" in repr(result)

    def test_default_plan_nodes(self):
        """Test default plan nodes."""
        result = SqlExplainResult(plan="Plan", query="SELECT 1")
        assert result.plan_nodes == []


class TestSqlServiceError:
    """Tests for SqlServiceError class."""

    def test_basic_creation(self):
        """Test creating an error."""
        error = SqlServiceError("Test error", code=-1)
        assert str(error) == "Test error, code=-1"

    def test_with_suggestion(self):
        """Test error with suggestion."""
        error = SqlServiceError(
            "Syntax error",
            code=SqlServiceError.SQL_ERROR_CODE_PARSING,
            suggestion="Check your query syntax",
        )
        assert "suggestion" in str(error)

    def test_error_codes(self):
        """Test error code constants."""
        assert SqlServiceError.SQL_ERROR_CODE_CANCELLED == -1
        assert SqlServiceError.SQL_ERROR_CODE_TIMEOUT == -2
        assert SqlServiceError.SQL_ERROR_CODE_PARSING == -3
        assert SqlServiceError.SQL_ERROR_CODE_GENERIC == -4


class TestSqlService:
    """Tests for SqlService class."""

    @pytest.fixture
    def mock_invocation_service(self):
        return Mock()

    @pytest.fixture
    def mock_serialization_service(self):
        service = Mock()
        service.to_data = Mock(return_value=b"serialized")
        service.to_object = Mock(side_effect=lambda x: x)
        return service

    @pytest.fixture
    def sql_service(self, mock_invocation_service, mock_serialization_service):
        service = SqlService(
            invocation_service=mock_invocation_service,
            serialization_service=mock_serialization_service,
        )
        service.start()
        return service

    def test_start_and_shutdown(self, mock_invocation_service, mock_serialization_service):
        """Test starting and shutting down service."""
        service = SqlService(mock_invocation_service, mock_serialization_service)
        assert service.is_running is False

        service.start()
        assert service.is_running is True

        service.shutdown()
        assert service.is_running is False

    def test_execute_not_running(self, mock_invocation_service, mock_serialization_service):
        """Test execute when service not running."""
        service = SqlService(mock_invocation_service, mock_serialization_service)

        with pytest.raises(IllegalStateException):
            service.execute("SELECT 1")

    def test_execute_empty_query(self, sql_service):
        """Test execute with empty query."""
        with pytest.raises(SqlServiceError):
            sql_service.execute("")

    def test_execute_basic(self, sql_service):
        """Test basic query execution."""
        result = sql_service.execute("SELECT * FROM users")
        assert isinstance(result, SqlResult)

    def test_execute_with_parameters(self, sql_service):
        """Test query with parameters."""
        result = sql_service.execute("SELECT * FROM users WHERE id = ?", 42)
        assert isinstance(result, SqlResult)

    def test_execute_with_options(self, sql_service):
        """Test query with all options."""
        result = sql_service.execute(
            "SELECT * FROM users",
            timeout=30.0,
            cursor_buffer_size=1000,
            schema="public",
            expected_result_type=SqlExpectedResultType.ROWS,
            convert_types=True,
        )
        assert isinstance(result, SqlResult)

    def test_execute_async(self, sql_service):
        """Test async query execution."""
        future = sql_service.execute_async("SELECT 1")
        assert isinstance(future, Future)
        result = future.result()
        assert isinstance(result, SqlResult)

    def test_execute_async_not_running(self, mock_invocation_service, mock_serialization_service):
        """Test async execute when not running."""
        service = SqlService(mock_invocation_service, mock_serialization_service)
        future = service.execute_async("SELECT 1")
        with pytest.raises(IllegalStateException):
            future.result()

    def test_execute_statement(self, sql_service):
        """Test statement execution."""
        stmt = SqlStatement("SELECT * FROM users")
        result = sql_service.execute_statement(stmt)
        assert isinstance(result, SqlResult)

    def test_execute_statement_empty(self, sql_service):
        """Test statement with empty SQL."""
        stmt = SqlStatement()
        with pytest.raises(SqlServiceError):
            sql_service.execute_statement(stmt)

    def test_explain(self, sql_service):
        """Test EXPLAIN query."""
        result = sql_service.explain("SELECT * FROM users")
        assert isinstance(result, SqlExplainResult)

    def test_explain_empty_query(self, sql_service):
        """Test EXPLAIN with empty query."""
        with pytest.raises(SqlServiceError):
            sql_service.explain("")

    def test_explain_not_running(self, mock_invocation_service, mock_serialization_service):
        """Test EXPLAIN when not running."""
        service = SqlService(mock_invocation_service, mock_serialization_service)
        with pytest.raises(IllegalStateException):
            service.explain("SELECT 1")

    def test_is_dml_query(self, sql_service):
        """Test DML query detection."""
        assert sql_service._is_dml_query("INSERT INTO users VALUES (1)") is True
        assert sql_service._is_dml_query("UPDATE users SET name = 'x'") is True
        assert sql_service._is_dml_query("DELETE FROM users") is True
        assert sql_service._is_dml_query("SELECT * FROM users") is False

    def test_is_explain_query(self, sql_service):
        """Test EXPLAIN query detection."""
        assert sql_service._is_explain_query("EXPLAIN SELECT 1") is True
        assert sql_service._is_explain_query("SELECT 1") is False

    def test_get_column_type_for_value(self, sql_service):
        """Test Python to SQL type mapping."""
        assert sql_service.get_column_type_for_value("text") == SqlColumnType.VARCHAR
        assert sql_service.get_column_type_for_value(42) == SqlColumnType.BIGINT
        assert sql_service.get_column_type_for_value(3.14) == SqlColumnType.DOUBLE
        assert sql_service.get_column_type_for_value(True) == SqlColumnType.BOOLEAN
        assert sql_service.get_column_type_for_value(None) == SqlColumnType.NULL

    def test_get_active_query_count(self, sql_service):
        """Test active query count."""
        assert sql_service.get_active_query_count() == 0

    def test_cancel_all_queries(self, sql_service):
        """Test cancelling all queries."""
        cancelled = sql_service.cancel_all_queries()
        assert cancelled == 0

    def test_close_query(self, sql_service):
        """Test closing a query."""
        sql_service.close_query("nonexistent-id")

    def test_serialize_parameters(self, sql_service):
        """Test parameter serialization."""
        params = sql_service._serialize_parameters([1, "test", None])
        assert len(params) == 3

    def test_convert_timeout(self, sql_service):
        """Test timeout conversion."""
        assert sql_service._convert_timeout(-1) == -1
        assert sql_service._convert_timeout(0) == 0
        assert sql_service._convert_timeout(1.5) == 1500

    def test_repr(self, sql_service):
        """Test string representation."""
        repr_str = repr(sql_service)
        assert "SqlService" in repr_str
        assert "running=True" in repr_str


class TestSqlTypeMapping:
    """Tests for SQL type mapping completeness."""

    def test_sql_type_to_python_mapping(self):
        """Verify all SQL types have Python mappings."""
        for col_type in SqlColumnType:
            assert col_type in SQL_TYPE_TO_PYTHON, f"Missing mapping for {col_type}"

    def test_service_column_type_map_complete(self):
        """Verify service type map covers all types."""
        service = SqlService()
        for i in range(16):
            assert i in service._SQL_COLUMN_TYPE_MAP


class TestAsyncOperations:
    """Tests for async SQL operations."""

    @pytest.fixture
    def sample_metadata(self):
        return SqlRowMetadata([
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
        ])

    @pytest.mark.asyncio
    async def test_async_iteration(self, sample_metadata):
        """Test async iteration over result."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.set_has_more(False)
        result.add_rows([
            SqlRow([1, "Alice"], sample_metadata),
            SqlRow([2, "Bob"], sample_metadata),
        ])

        rows = []
        async for row in result:
            rows.append(row)

        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_async_context_manager(self, sample_metadata):
        """Test async context manager."""
        async with SqlResult(metadata=sample_metadata, update_count=-1) as result:
            result.set_has_more(False)
            result.add_rows([SqlRow([1, "Alice"], sample_metadata)])
            assert result.is_closed is False

        assert result.is_closed is True

    @pytest.mark.asyncio
    async def test_get_all_async(self, sample_metadata):
        """Test getting all rows async."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.set_has_more(False)
        result.add_rows([
            SqlRow([1, "Alice"], sample_metadata),
        ])

        rows = await result.get_all_async()
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_fetch_page_async(self, sample_metadata):
        """Test fetching page async."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.set_has_more(False)
        result.add_rows([
            SqlRow([1, "Alice"], sample_metadata),
        ])

        page = await result.fetch_page_async()
        assert page.row_count == 1


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.fixture
    def sample_metadata(self):
        return SqlRowMetadata([
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
        ])

    def test_iterate_closed_result(self, sample_metadata):
        """Test iterating over closed result."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.close()

        with pytest.raises(StopIteration):
            next(iter(result))

    def test_iterate_update_count_result(self):
        """Test iterating over update count result."""
        result = SqlResult(update_count=5)

        with pytest.raises(StopIteration):
            next(iter(result))

    def test_double_close(self, sample_metadata):
        """Test closing result twice."""
        result = SqlResult(metadata=sample_metadata, update_count=-1)
        result.close()
        result.close()
        assert result.is_closed is True

    def test_close_with_callback(self, sample_metadata):
        """Test close with callback."""
        callback_called = [False]

        def close_callback():
            callback_called[0] = True

        result = SqlResult(
            metadata=sample_metadata,
            update_count=-1,
            close_callback=close_callback,
        )
        result.close()

        assert callback_called[0] is True

    def test_row_with_fewer_values_than_columns(self):
        """Test row with mismatched value/column count."""
        metadata = SqlRowMetadata([
            SqlColumnMetadata("a", SqlColumnType.INTEGER),
            SqlColumnMetadata("b", SqlColumnType.INTEGER),
            SqlColumnMetadata("c", SqlColumnType.INTEGER),
        ])
        row = SqlRow([1, 2], metadata)

        d = row.to_dict()
        assert d.get("c") is None

    def test_empty_row_metadata(self):
        """Test empty row metadata."""
        metadata = SqlRowMetadata([])
        assert metadata.column_count == 0
        assert metadata.find_column("any") == -1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
