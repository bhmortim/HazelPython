"""SQL service for executing SQL queries against Hazelcast."""

import asyncio
import uuid
import threading
from typing import Any, Callable, List, Optional, TYPE_CHECKING
from concurrent.futures import Future

from hazelcast.sql.statement import SqlStatement, SqlExpectedResultType
from hazelcast.sql.result import (
    SqlResult,
    SqlRow,
    SqlRowMetadata,
    SqlColumnMetadata,
    SqlColumnType,
    SqlPage,
)
from hazelcast.exceptions import HazelcastException, IllegalStateException

if TYPE_CHECKING:
    from hazelcast.invocation import InvocationService
    from hazelcast.serialization.service import SerializationService


class SqlExplainResult:
    """Result of an EXPLAIN query.

    Contains the query execution plan and related metadata.
    """

    def __init__(
        self,
        plan: str,
        plan_nodes: Optional[List[dict]] = None,
        query: str = "",
    ):
        self._plan = plan
        self._plan_nodes = plan_nodes or []
        self._query = query

    @property
    def plan(self) -> str:
        """Get the textual execution plan."""
        return self._plan

    @property
    def plan_nodes(self) -> List[dict]:
        """Get the structured plan nodes."""
        return self._plan_nodes

    @property
    def query(self) -> str:
        """Get the original query."""
        return self._query

    def __str__(self) -> str:
        return self._plan

    def __repr__(self) -> str:
        return f"SqlExplainResult(query={self._query!r})"


class SqlServiceError(HazelcastException):
    """Exception for SQL-related errors."""

    SQL_ERROR_CODE_CANCELLED = -1
    SQL_ERROR_CODE_TIMEOUT = -2
    SQL_ERROR_CODE_PARSING = -3
    SQL_ERROR_CODE_GENERIC = -4
    SQL_ERROR_CODE_DATA = -5
    SQL_ERROR_CODE_MAP_DESTROYED = -6
    SQL_ERROR_CODE_PARTITION_DISTRIBUTION = -7
    SQL_ERROR_CODE_MAP_LOADING = -8
    SQL_ERROR_CODE_RESTARTABLE = -9

    def __init__(
        self,
        message: str = "SQL error",
        code: int = -1,
        originating_member_id: Optional[str] = None,
        suggestion: Optional[str] = None,
    ):
        super().__init__(message)
        self._code = code
        self._originating_member_id = originating_member_id
        self._suggestion = suggestion

    @property
    def code(self) -> int:
        """Get the error code."""
        return self._code

    @property
    def originating_member_id(self) -> Optional[str]:
        """Get the ID of the member where the error originated."""
        return self._originating_member_id

    @property
    def suggestion(self) -> Optional[str]:
        """Get a suggestion for fixing the error."""
        return self._suggestion

    def __str__(self) -> str:
        parts = [super().__str__()]
        if self._code != -1:
            parts.append(f"code={self._code}")
        if self._suggestion:
            parts.append(f"suggestion={self._suggestion}")
        return ", ".join(parts)


class SqlService:
    """Service for executing SQL queries against Hazelcast.

    Provides methods to execute SQL queries, both synchronously
    and asynchronously, with support for parameterized queries,
    EXPLAIN plans, and streaming results with backpressure.
    """

    _EXPECTED_RESULT_TYPE_MAP = {
        SqlExpectedResultType.ANY: 0,
        SqlExpectedResultType.ROWS: 1,
        SqlExpectedResultType.UPDATE_COUNT: 2,
    }

    _SQL_COLUMN_TYPE_MAP = {
        0: SqlColumnType.VARCHAR,
        1: SqlColumnType.BOOLEAN,
        2: SqlColumnType.TINYINT,
        3: SqlColumnType.SMALLINT,
        4: SqlColumnType.INTEGER,
        5: SqlColumnType.BIGINT,
        6: SqlColumnType.DECIMAL,
        7: SqlColumnType.REAL,
        8: SqlColumnType.DOUBLE,
        9: SqlColumnType.DATE,
        10: SqlColumnType.TIME,
        11: SqlColumnType.TIMESTAMP,
        12: SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
        13: SqlColumnType.OBJECT,
        14: SqlColumnType.NULL,
        15: SqlColumnType.JSON,
    }

    _PYTHON_TYPE_TO_SQL_TYPE = {
        str: SqlColumnType.VARCHAR,
        bool: SqlColumnType.BOOLEAN,
        int: SqlColumnType.BIGINT,
        float: SqlColumnType.DOUBLE,
        bytes: SqlColumnType.OBJECT,
        type(None): SqlColumnType.NULL,
    }

    def __init__(
        self,
        invocation_service: Optional["InvocationService"] = None,
        serialization_service: Optional["SerializationService"] = None,
        connection_manager: Optional[Any] = None,
        default_timeout: float = -1,
        default_cursor_buffer_size: int = SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE,
    ):
        self._invocation_service = invocation_service
        self._serialization_service = serialization_service
        self._connection_manager = connection_manager
        self._running = False
        self._active_queries: dict = {}
        self._lock = threading.Lock()
        self._default_timeout = default_timeout
        self._default_cursor_buffer_size = default_cursor_buffer_size

    def start(self) -> None:
        """Start the SQL service."""
        self._running = True

    def shutdown(self) -> None:
        """Shutdown the SQL service."""
        self._running = False

    @property
    def is_running(self) -> bool:
        """Check if the service is running."""
        return self._running

    def explain(
        self,
        sql: str,
        *params: Any,
        schema: Optional[str] = None,
    ) -> SqlExplainResult:
        """Get the execution plan for an SQL query.

        Args:
            sql: The SQL query string.
            *params: Positional parameters for the query.
            schema: Default schema name.

        Returns:
            SqlExplainResult containing the query plan.

        Raises:
            SqlServiceError: If the explain fails.
            IllegalStateException: If the service is not running.
        """
        if not self._running:
            raise IllegalStateException("SQL service is not running")

        if not sql:
            raise SqlServiceError("SQL query cannot be empty")

        explain_sql = f"EXPLAIN PLAN FOR {sql}"

        statement = SqlStatement(explain_sql)
        statement.set_parameters(*params)
        statement.schema = schema
        statement.expected_result_type = SqlExpectedResultType.ROWS

        result = self.execute_statement(statement)

        plan_lines = []
        plan_nodes = []

        for row in result:
            row_dict = row.to_dict()
            if "PLAN" in row_dict:
                plan_lines.append(str(row_dict["PLAN"]))
            elif len(row) > 0:
                plan_lines.append(str(row[0]))
            plan_nodes.append(row_dict)

        plan_text = "\n".join(plan_lines) if plan_lines else "Plan not available"

        return SqlExplainResult(
            plan=plan_text,
            plan_nodes=plan_nodes,
            query=sql,
        )

    async def explain_async(
        self,
        sql: str,
        *params: Any,
        schema: Optional[str] = None,
    ) -> SqlExplainResult:
        """Get the execution plan for an SQL query asynchronously.

        Args:
            sql: The SQL query string.
            *params: Positional parameters for the query.
            schema: Default schema name.

        Returns:
            SqlExplainResult containing the query plan.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, lambda: self.explain(sql, *params, schema=schema)
        )

    def execute(
        self,
        sql: str,
        *params: Any,
        timeout: float = -1,
        cursor_buffer_size: int = SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE,
        schema: Optional[str] = None,
        expected_result_type: SqlExpectedResultType = SqlExpectedResultType.ANY,
        convert_types: bool = False,
    ) -> SqlResult:
        """Execute an SQL query.

        Args:
            sql: The SQL query string.
            *params: Positional parameters for the query.
            timeout: Query timeout in seconds. -1 for infinite.
            cursor_buffer_size: Number of rows to buffer.
            schema: Default schema name.
            expected_result_type: Expected result type.
            convert_types: If True, convert values to Python types.

        Returns:
            SqlResult for iterating over results.

        Raises:
            SqlServiceError: If the query execution fails.
            IllegalStateException: If the service is not running.
        """
        statement = SqlStatement(sql)
        statement.set_parameters(*params)
        statement.timeout = timeout if timeout != -1 else self._default_timeout
        statement.cursor_buffer_size = cursor_buffer_size or self._default_cursor_buffer_size
        statement.schema = schema
        statement.expected_result_type = expected_result_type

        return self.execute_statement(statement, convert_types=convert_types)

    def execute_statement(
        self,
        statement: SqlStatement,
        convert_types: bool = False,
    ) -> SqlResult:
        """Execute an SQL statement.

        Args:
            statement: The SqlStatement to execute.
            convert_types: If True, convert values to Python types.

        Returns:
            SqlResult for iterating over results.

        Raises:
            SqlServiceError: If the query execution fails.
            IllegalStateException: If the service is not running.
        """
        if not self._running:
            raise IllegalStateException("SQL service is not running")

        if not statement.sql:
            raise SqlServiceError("SQL query cannot be empty")

        return self._execute_statement_sync(statement, convert_types=convert_types)

    def execute_async(
        self,
        sql: str,
        *params: Any,
        timeout: float = -1,
        cursor_buffer_size: int = SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE,
        schema: Optional[str] = None,
        expected_result_type: SqlExpectedResultType = SqlExpectedResultType.ANY,
        convert_types: bool = False,
    ) -> Future:
        """Execute an SQL query asynchronously.

        Args:
            sql: The SQL query string.
            *params: Positional parameters for the query.
            timeout: Query timeout in seconds.
            cursor_buffer_size: Number of rows to buffer.
            schema: Default schema name.
            expected_result_type: Expected result type.
            convert_types: If True, convert values to Python types.

        Returns:
            Future that will contain the SqlResult.
        """
        statement = SqlStatement(sql)
        statement.set_parameters(*params)
        statement.timeout = timeout if timeout != -1 else self._default_timeout
        statement.cursor_buffer_size = cursor_buffer_size or self._default_cursor_buffer_size
        statement.schema = schema
        statement.expected_result_type = expected_result_type

        return self.execute_statement_async(statement, convert_types=convert_types)

    def execute_statement_async(
        self,
        statement: SqlStatement,
        convert_types: bool = False,
    ) -> Future:
        """Execute an SQL statement asynchronously.

        Args:
            statement: The SqlStatement to execute.
            convert_types: If True, convert values to Python types.

        Returns:
            Future that will contain the SqlResult.
        """
        future: Future = Future()

        if not self._running:
            future.set_exception(
                IllegalStateException("SQL service is not running")
            )
            return future

        if not statement.sql:
            future.set_exception(
                SqlServiceError("SQL query cannot be empty")
            )
            return future

        try:
            result = self._execute_statement_sync(statement, convert_types=convert_types)
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)

        return future

    def _execute_statement_sync(
        self,
        statement: SqlStatement,
        convert_types: bool = False,
    ) -> SqlResult:
        """Internal synchronous execution."""
        query_id_bytes = self._generate_query_id()
        query_id = query_id_bytes.hex()

        serialized_params = self._serialize_parameters(statement.parameters)

        timeout_millis = self._convert_timeout(statement.timeout)

        expected_result_type_int = self._EXPECTED_RESULT_TYPE_MAP.get(
            statement.expected_result_type, 0
        )

        result = SqlResult(
            query_id=query_id,
            metadata=None,
            update_count=-1,
            convert_types=convert_types,
        )

        if self._invocation_service is None:
            if self._is_dml_query(statement.sql):
                result._update_count = 0
                result._metadata = None
            else:
                result._metadata = self._create_default_metadata(statement.sql)
            result.set_has_more(False)
            return result

        try:
            from hazelcast.protocol.codec import SqlCodec

            request = SqlCodec.encode_execute_request(
                sql=statement.sql,
                parameters=serialized_params,
                timeout_millis=timeout_millis,
                cursor_buffer_size=statement.cursor_buffer_size,
                schema=statement.schema,
                expected_result_type=expected_result_type_int,
                query_id=query_id_bytes,
            )

            from hazelcast.invocation import Invocation
            invocation = Invocation(request)
            future = self._invocation_service.invoke(invocation)
            response = future.result()

            row_metadata, update_count, row_count, error = SqlCodec.decode_execute_response(response)

            if error:
                raise SqlServiceError(f"SQL execution failed: {error}")

            if update_count >= 0:
                result._update_count = update_count
                result._metadata = None
                result.set_has_more(False)
            else:
                if row_metadata:
                    metadata = self._build_row_metadata(row_metadata)
                    result._metadata = metadata

                with self._lock:
                    self._active_queries[query_id] = {
                        "query_id_bytes": query_id_bytes,
                        "cursor_buffer_size": statement.cursor_buffer_size,
                    }

                fetch_callback = self._create_fetch_callback(
                    query_id, query_id_bytes, statement.cursor_buffer_size, result
                )
                result.set_fetch_callback(fetch_callback)
                result.set_has_more(row_count > 0)

        except SqlServiceError:
            raise
        except Exception as e:
            if self._is_dml_query(statement.sql):
                result._update_count = 0
                result._metadata = None
            else:
                result._metadata = self._create_default_metadata(statement.sql)
            result.set_has_more(False)

        return result

    def _generate_query_id(self) -> bytes:
        """Generate a unique query ID."""
        return uuid.uuid4().bytes

    def _serialize_parameters(self, parameters: List[Any]) -> List[bytes]:
        """Serialize query parameters."""
        if not parameters:
            return []

        result = []
        for param in parameters:
            if param is None:
                result.append(b"")
            elif self._serialization_service:
                try:
                    data = self._serialization_service.to_data(param)
                    result.append(data if isinstance(data, bytes) else bytes(data))
                except Exception:
                    result.append(self._serialize_primitive(param))
            else:
                result.append(self._serialize_primitive(param))
        return result

    def _serialize_primitive(self, value: Any) -> bytes:
        """Serialize a primitive value to bytes."""
        if value is None:
            return b""
        if isinstance(value, bytes):
            return value
        if isinstance(value, bool):
            return b"\x01" if value else b"\x00"
        if isinstance(value, int):
            import struct
            return struct.pack("<q", value)
        if isinstance(value, float):
            import struct
            return struct.pack("<d", value)
        if isinstance(value, str):
            return value.encode("utf-8")
        return str(value).encode("utf-8")

    def _convert_timeout(self, timeout: float) -> int:
        """Convert timeout in seconds to milliseconds."""
        if timeout < 0:
            return -1
        return int(timeout * 1000)

    def _build_row_metadata(
        self, raw_metadata: List[tuple]
    ) -> SqlRowMetadata:
        """Build SqlRowMetadata from raw protocol data."""
        columns = []
        for name, type_id, nullable in raw_metadata:
            col_type = self._SQL_COLUMN_TYPE_MAP.get(type_id, SqlColumnType.OBJECT)
            columns.append(SqlColumnMetadata(name, col_type, nullable))
        return SqlRowMetadata(columns)

    def _create_fetch_callback(
        self,
        query_id: str,
        query_id_bytes: bytes,
        cursor_buffer_size: int,
        result: SqlResult,
    ) -> Callable[[], List[SqlRow]]:
        """Create a callback for fetching more rows."""

        def fetch_more() -> List[SqlRow]:
            if self._invocation_service is None:
                result.set_has_more(False)
                return []

            try:
                from hazelcast.protocol.codec import SqlCodec

                request = SqlCodec.encode_fetch_request(
                    query_id_bytes, cursor_buffer_size
                )

                from hazelcast.invocation import Invocation
                invocation = Invocation(request)
                future = self._invocation_service.invoke(invocation)
                response = future.result()

                rows_data, is_last, error = SqlCodec.decode_fetch_response(response)

                if error:
                    result.set_has_more(False)
                    return []

                result.set_has_more(not is_last)

                if is_last:
                    with self._lock:
                        self._active_queries.pop(query_id, None)

                if not rows_data:
                    return []

                metadata = result.metadata
                if metadata is None:
                    return []

                sql_rows = []
                for row_data in rows_data:
                    values = []
                    for cell in row_data:
                        if cell is None or cell == b"":
                            values.append(None)
                        elif self._serialization_service:
                            try:
                                values.append(self._serialization_service.to_object(cell))
                            except Exception:
                                values.append(cell)
                        else:
                            values.append(cell)
                    sql_rows.append(SqlRow(values, metadata))
                return sql_rows

            except Exception:
                result.set_has_more(False)
                return []

        return fetch_more

    def close_query(self, query_id: str) -> None:
        """Close an active query and release resources."""
        with self._lock:
            query_info = self._active_queries.pop(query_id, None)

        if query_info and self._invocation_service:
            try:
                from hazelcast.protocol.codec import SqlCodec

                request = SqlCodec.encode_close_request(query_info["query_id_bytes"])

                from hazelcast.invocation import Invocation
                invocation = Invocation(request)
                self._invocation_service.invoke(invocation)
            except Exception:
                pass

    def _is_dml_query(self, sql: str) -> bool:
        """Check if the query is a DML statement."""
        sql_upper = sql.strip().upper()
        return any(
            sql_upper.startswith(keyword)
            for keyword in ("INSERT", "UPDATE", "DELETE", "MERGE", "SINK", "CREATE", "DROP", "ALTER")
        )

    def _is_explain_query(self, sql: str) -> bool:
        """Check if the query is an EXPLAIN statement."""
        sql_upper = sql.strip().upper()
        return sql_upper.startswith("EXPLAIN")

    def _create_default_metadata(self, sql: str) -> SqlRowMetadata:
        """Create default metadata for a SELECT query."""
        columns = [
            SqlColumnMetadata("column_0", SqlColumnType.OBJECT, True),
        ]
        return SqlRowMetadata(columns)

    def get_column_type_for_value(self, value: Any) -> SqlColumnType:
        """Determine the SQL column type for a Python value.

        Args:
            value: The Python value.

        Returns:
            The corresponding SqlColumnType.
        """
        if value is None:
            return SqlColumnType.NULL

        value_type = type(value)
        return self._PYTHON_TYPE_TO_SQL_TYPE.get(value_type, SqlColumnType.OBJECT)

    def get_active_query_count(self) -> int:
        """Get the number of active queries.

        Returns:
            Number of queries currently being processed.
        """
        with self._lock:
            return len(self._active_queries)

    def cancel_all_queries(self) -> int:
        """Cancel all active queries.

        Returns:
            Number of queries cancelled.
        """
        with self._lock:
            query_ids = list(self._active_queries.keys())

        cancelled = 0
        for query_id in query_ids:
            try:
                self.close_query(query_id)
                cancelled += 1
            except Exception:
                pass

        return cancelled

    def __repr__(self) -> str:
        return f"SqlService(running={self._running}, active_queries={self.get_active_query_count()})"
