"""SQL statement building for Hazelcast SQL queries."""

from enum import Enum
from typing import Any, Dict, List, Optional


class SqlExpectedResultType(Enum):
    """Expected result type for SQL queries."""

    ANY = "ANY"
    ROWS = "ROWS"
    UPDATE_COUNT = "UPDATE_COUNT"


class SqlStatement:
    """Represents an SQL statement with parameters and options.

    Use this class to build SQL queries with configurable parameters,
    timeouts, and cursor buffer sizes.
    """

    DEFAULT_TIMEOUT = -1
    DEFAULT_CURSOR_BUFFER_SIZE = 4096

    def __init__(self, sql: str = ""):
        """Initialize an SQL statement.

        Args:
            sql: The SQL query string.
        """
        self._sql = sql
        self._parameters: List[Any] = []
        self._timeout: float = self.DEFAULT_TIMEOUT
        self._cursor_buffer_size: int = self.DEFAULT_CURSOR_BUFFER_SIZE
        self._schema: Optional[str] = None
        self._expected_result_type: SqlExpectedResultType = SqlExpectedResultType.ANY

    @property
    def sql(self) -> str:
        """Get the SQL query string."""
        return self._sql

    @sql.setter
    def sql(self, value: str) -> None:
        """Set the SQL query string."""
        if not value:
            raise ValueError("SQL query cannot be empty")
        self._sql = value

    @property
    def parameters(self) -> List[Any]:
        """Get the query parameters."""
        return list(self._parameters)

    @property
    def timeout(self) -> float:
        """Get the query timeout in seconds."""
        return self._timeout

    @timeout.setter
    def timeout(self, value: float) -> None:
        """Set the query timeout in seconds.

        Args:
            value: Timeout in seconds. -1 for infinite.
        """
        if value < -1:
            raise ValueError("Timeout must be >= -1")
        self._timeout = value

    @property
    def cursor_buffer_size(self) -> int:
        """Get the cursor buffer size."""
        return self._cursor_buffer_size

    @cursor_buffer_size.setter
    def cursor_buffer_size(self, value: int) -> None:
        """Set the cursor buffer size.

        Args:
            value: Buffer size in number of rows.
        """
        if value <= 0:
            raise ValueError("Cursor buffer size must be positive")
        self._cursor_buffer_size = value

    @property
    def schema(self) -> Optional[str]:
        """Get the default schema."""
        return self._schema

    @schema.setter
    def schema(self, value: Optional[str]) -> None:
        """Set the default schema."""
        self._schema = value

    @property
    def expected_result_type(self) -> SqlExpectedResultType:
        """Get the expected result type."""
        return self._expected_result_type

    @expected_result_type.setter
    def expected_result_type(self, value: SqlExpectedResultType) -> None:
        """Set the expected result type."""
        self._expected_result_type = value

    def add_parameter(self, value: Any) -> "SqlStatement":
        """Add a positional parameter.

        Args:
            value: The parameter value.

        Returns:
            This statement for chaining.
        """
        self._parameters.append(value)
        return self

    def set_parameters(self, *args: Any) -> "SqlStatement":
        """Set all positional parameters.

        Args:
            *args: Parameter values in order.

        Returns:
            This statement for chaining.
        """
        self._parameters = list(args)
        return self

    def clear_parameters(self) -> "SqlStatement":
        """Clear all parameters.

        Returns:
            This statement for chaining.
        """
        self._parameters.clear()
        return self

    def copy(self) -> "SqlStatement":
        """Create a copy of this statement.

        Returns:
            A new SqlStatement with the same configuration.
        """
        stmt = SqlStatement(self._sql)
        stmt._parameters = list(self._parameters)
        stmt._timeout = self._timeout
        stmt._cursor_buffer_size = self._cursor_buffer_size
        stmt._schema = self._schema
        stmt._expected_result_type = self._expected_result_type
        return stmt

    def __repr__(self) -> str:
        return (
            f"SqlStatement(sql={self._sql!r}, "
            f"parameters={self._parameters!r}, "
            f"timeout={self._timeout})"
        )
