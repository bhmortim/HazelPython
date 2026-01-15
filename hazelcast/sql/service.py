"""SQL service for executing SQL queries against Hazelcast."""

import uuid
from typing import Any, List, Optional, TYPE_CHECKING
from concurrent.futures import Future

from hazelcast.sql.statement import SqlStatement, SqlExpectedResultType
from hazelcast.sql.result import (
    SqlResult,
    SqlRow,
    SqlRowMetadata,
    SqlColumnMetadata,
    SqlColumnType,
)
from hazelcast.exceptions import HazelcastException, IllegalStateException

if TYPE_CHECKING:
    from hazelcast.invocation import InvocationService
    from hazelcast.serialization.service import SerializationService


class SqlServiceError(HazelcastException):
    """Exception for SQL-related errors."""

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


class SqlService:
    """Service for executing SQL queries against Hazelcast.

    Provides methods to execute SQL queries, both synchronously
    and asynchronously, with support for parameterized queries.
    """

    def __init__(
        self,
        invocation_service: Optional["InvocationService"] = None,
        serialization_service: Optional["SerializationService"] = None,
    ):
        self._invocation_service = invocation_service
        self._serialization_service = serialization_service
        self._running = False

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

    def execute(
        self,
        sql: str,
        *params: Any,
        timeout: float = -1,
        cursor_buffer_size: int = SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE,
        schema: Optional[str] = None,
        expected_result_type: SqlExpectedResultType = SqlExpectedResultType.ANY,
    ) -> SqlResult:
        """Execute an SQL query.

        Args:
            sql: The SQL query string.
            *params: Positional parameters for the query.
            timeout: Query timeout in seconds. -1 for infinite.
            cursor_buffer_size: Number of rows to buffer.
            schema: Default schema name.
            expected_result_type: Expected result type.

        Returns:
            SqlResult for iterating over results.

        Raises:
            SqlServiceError: If the query execution fails.
            IllegalStateException: If the service is not running.
        """
        statement = SqlStatement(sql)
        statement.set_parameters(*params)
        statement.timeout = timeout
        statement.cursor_buffer_size = cursor_buffer_size
        statement.schema = schema
        statement.expected_result_type = expected_result_type

        return self.execute_statement(statement)

    def execute_statement(self, statement: SqlStatement) -> SqlResult:
        """Execute an SQL statement.

        Args:
            statement: The SqlStatement to execute.

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

        return self._execute_statement_sync(statement)

    def execute_async(
        self,
        sql: str,
        *params: Any,
        timeout: float = -1,
        cursor_buffer_size: int = SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE,
        schema: Optional[str] = None,
        expected_result_type: SqlExpectedResultType = SqlExpectedResultType.ANY,
    ) -> Future:
        """Execute an SQL query asynchronously.

        Args:
            sql: The SQL query string.
            *params: Positional parameters for the query.
            timeout: Query timeout in seconds.
            cursor_buffer_size: Number of rows to buffer.
            schema: Default schema name.
            expected_result_type: Expected result type.

        Returns:
            Future that will contain the SqlResult.
        """
        statement = SqlStatement(sql)
        statement.set_parameters(*params)
        statement.timeout = timeout
        statement.cursor_buffer_size = cursor_buffer_size
        statement.schema = schema
        statement.expected_result_type = expected_result_type

        return self.execute_statement_async(statement)

    def execute_statement_async(self, statement: SqlStatement) -> Future:
        """Execute an SQL statement asynchronously.

        Args:
            statement: The SqlStatement to execute.

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
            result = self._execute_statement_sync(statement)
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)

        return future

    def _execute_statement_sync(self, statement: SqlStatement) -> SqlResult:
        """Internal synchronous execution.

        This is a stub implementation that returns empty results.
        Real implementation would send protocol messages to the cluster.
        """
        query_id = str(uuid.uuid4())

        if self._is_dml_query(statement.sql):
            return SqlResult(
                query_id=query_id,
                metadata=None,
                update_count=0,
            )

        metadata = self._create_default_metadata(statement.sql)
        result = SqlResult(
            query_id=query_id,
            metadata=metadata,
            update_count=-1,
        )

        return result

    def _is_dml_query(self, sql: str) -> bool:
        """Check if the query is a DML statement."""
        sql_upper = sql.strip().upper()
        return any(
            sql_upper.startswith(keyword)
            for keyword in ("INSERT", "UPDATE", "DELETE", "MERGE")
        )

    def _create_default_metadata(self, sql: str) -> SqlRowMetadata:
        """Create default metadata for a SELECT query."""
        columns = [
            SqlColumnMetadata("column_0", SqlColumnType.OBJECT, True),
        ]
        return SqlRowMetadata(columns)

    def __repr__(self) -> str:
        return f"SqlService(running={self._running})"
