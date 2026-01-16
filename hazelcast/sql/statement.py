"""SQL statement building for Hazelcast SQL queries."""

from enum import Enum
from typing import Any, Dict, List, Optional


class SqlExpectedResultType(Enum):
    """Expected result type for SQL queries.

    Specifies what type of result is expected from an SQL statement.
    This can be used to optimize query execution and validate results.

    Attributes:
        ANY: Accept any result type (rows or update count).
        ROWS: Expect a result set with rows (SELECT queries).
        UPDATE_COUNT: Expect an update count (INSERT, UPDATE, DELETE).

    Example:
        >>> statement = SqlStatement("SELECT * FROM users")
        >>> statement.expected_result_type = SqlExpectedResultType.ROWS
    """

    ANY = "ANY"
    ROWS = "ROWS"
    UPDATE_COUNT = "UPDATE_COUNT"


class SqlStatement:
    """Represents an SQL statement with parameters and options.

    Use this class to build SQL queries with configurable parameters,
    timeouts, and cursor buffer sizes. Supports method chaining for
    fluent configuration.

    Attributes:
        DEFAULT_TIMEOUT: Default query timeout (-1 for infinite).
        DEFAULT_CURSOR_BUFFER_SIZE: Default cursor buffer size (4096 rows).

    Example:
        Basic query::

            statement = SqlStatement("SELECT * FROM users WHERE age > ?")
            statement.add_parameter(25)
            result = sql_service.execute_statement(statement)

        Fluent configuration::

            statement = (
                SqlStatement("SELECT * FROM users WHERE dept = ?")
                .add_parameter("engineering")
                .set_parameters("engineering", "active")
            )
            statement.timeout = 30.0
            statement.cursor_buffer_size = 1000
    """

    DEFAULT_TIMEOUT = -1
    DEFAULT_CURSOR_BUFFER_SIZE = 4096

    def __init__(self, sql: str = ""):
        """Initialize an SQL statement.

        Args:
            sql: The SQL query string. Can contain ``?`` placeholders for
                positional parameters.

        Example:
            >>> stmt = SqlStatement("SELECT * FROM users")
            >>> stmt = SqlStatement("SELECT * FROM users WHERE id = ?")
        """
        self._sql = sql
        self._parameters: List[Any] = []
        self._timeout: float = self.DEFAULT_TIMEOUT
        self._cursor_buffer_size: int = self.DEFAULT_CURSOR_BUFFER_SIZE
        self._schema: Optional[str] = None
        self._expected_result_type: SqlExpectedResultType = SqlExpectedResultType.ANY

    @property
    def sql(self) -> str:
        """Get the SQL query string.

        Returns:
            str: The SQL query string.
        """
        return self._sql

    @sql.setter
    def sql(self, value: str) -> None:
        """Set the SQL query string.

        Args:
            value: The SQL query string. Cannot be empty.

        Raises:
            ValueError: If the query string is empty.
        """
        if not value:
            raise ValueError("SQL query cannot be empty")
        self._sql = value

    @property
    def parameters(self) -> List[Any]:
        """Get the query parameters.

        Returns:
            List[Any]: A copy of the positional parameters list.
        """
        return list(self._parameters)

    @property
    def timeout(self) -> float:
        """Get the query timeout in seconds.

        Returns:
            float: The timeout value. -1 means infinite timeout.
        """
        return self._timeout

    @timeout.setter
    def timeout(self, value: float) -> None:
        """Set the query timeout in seconds.

        Args:
            value: Timeout in seconds. Use -1 for infinite timeout.

        Raises:
            ValueError: If timeout is less than -1.
        """
        if value < -1:
            raise ValueError("Timeout must be >= -1")
        self._timeout = value

    @property
    def cursor_buffer_size(self) -> int:
        """Get the cursor buffer size.

        Returns:
            int: The number of rows to buffer per fetch.
        """
        return self._cursor_buffer_size

    @cursor_buffer_size.setter
    def cursor_buffer_size(self, value: int) -> None:
        """Set the cursor buffer size.

        Controls how many rows are fetched from the cluster at a time.
        Larger values reduce network round-trips but use more memory.

        Args:
            value: Buffer size in number of rows. Must be positive.

        Raises:
            ValueError: If value is not positive.
        """
        if value <= 0:
            raise ValueError("Cursor buffer size must be positive")
        self._cursor_buffer_size = value

    @property
    def schema(self) -> Optional[str]:
        """Get the default schema.

        Returns:
            Optional[str]: The default schema name, or None if not set.
        """
        return self._schema

    @schema.setter
    def schema(self, value: Optional[str]) -> None:
        """Set the default schema.

        The schema is used to resolve unqualified table names.

        Args:
            value: The schema name, or None to use the default.
        """
        self._schema = value

    @property
    def expected_result_type(self) -> SqlExpectedResultType:
        """Get the expected result type.

        Returns:
            SqlExpectedResultType: The expected result type.
        """
        return self._expected_result_type

    @expected_result_type.setter
    def expected_result_type(self, value: SqlExpectedResultType) -> None:
        """Set the expected result type.

        Args:
            value: The expected result type.
        """
        self._expected_result_type = value

    def add_parameter(self, value: Any) -> "SqlStatement":
        """Add a positional parameter.

        Parameters are bound to ``?`` placeholders in the SQL string
        in the order they are added.

        Args:
            value: The parameter value. Can be any serializable type.

        Returns:
            SqlStatement: This statement for method chaining.

        Example:
            >>> stmt = SqlStatement("SELECT * FROM users WHERE age > ? AND status = ?")
            >>> stmt.add_parameter(25).add_parameter("active")
        """
        self._parameters.append(value)
        return self

    def set_parameters(self, *args: Any) -> "SqlStatement":
        """Set all positional parameters, replacing any existing ones.

        Args:
            *args: Parameter values in order, corresponding to ``?``
                placeholders in the SQL string.

        Returns:
            SqlStatement: This statement for method chaining.

        Example:
            >>> stmt = SqlStatement("SELECT * FROM users WHERE age > ? AND status = ?")
            >>> stmt.set_parameters(25, "active")
        """
        self._parameters = list(args)
        return self

    def clear_parameters(self) -> "SqlStatement":
        """Clear all parameters.

        Returns:
            SqlStatement: This statement for method chaining.

        Example:
            >>> stmt.clear_parameters()
            >>> stmt.set_parameters(new_value)
        """
        self._parameters.clear()
        return self

    def copy(self) -> "SqlStatement":
        """Create a copy of this statement.

        Creates a deep copy with the same SQL, parameters, and configuration.
        Modifying the copy does not affect the original.

        Returns:
            SqlStatement: A new SqlStatement with the same configuration.

        Example:
            >>> original = SqlStatement("SELECT * FROM users")
            >>> original.timeout = 30
            >>> copy = original.copy()
            >>> copy.timeout = 60  # Does not affect original
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
