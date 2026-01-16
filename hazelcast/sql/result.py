"""SQL result handling for Hazelcast SQL queries."""

import asyncio
from decimal import Decimal
from enum import Enum
from typing import Any, AsyncIterator, Callable, Dict, Iterator, List, Optional, TYPE_CHECKING
from concurrent.futures import Future
import threading
from datetime import date, time, datetime, timezone, timedelta

if TYPE_CHECKING:
    from hazelcast.sql.service import SqlService


class SqlColumnType(Enum):
    """SQL column data types.

    Represents the data type of a column in an SQL result set.
    Each type corresponds to a standard SQL type and maps to
    a specific Python type.

    Attributes:
        VARCHAR: Variable-length string (Python ``str``).
        BOOLEAN: Boolean value (Python ``bool``).
        TINYINT: 8-bit signed integer (Python ``int``).
        SMALLINT: 16-bit signed integer (Python ``int``).
        INTEGER: 32-bit signed integer (Python ``int``).
        BIGINT: 64-bit signed integer (Python ``int``).
        DECIMAL: Arbitrary precision decimal (Python ``Decimal``).
        REAL: 32-bit floating point (Python ``float``).
        DOUBLE: 64-bit floating point (Python ``float``).
        DATE: Date without time (Python ``date``).
        TIME: Time without date (Python ``time``).
        TIMESTAMP: Timestamp without timezone (Python ``datetime``).
        TIMESTAMP_WITH_TIME_ZONE: Timestamp with timezone (Python ``datetime``).
        OBJECT: Arbitrary serializable object (Python ``object``).
        NULL: Null type (Python ``None``).
        JSON: JSON data (Python ``dict``, ``list``, or ``str``).

    Example:
        >>> for col in result.metadata.columns:
        ...     print(f"{col.name}: {col.type.value}")
    """

    VARCHAR = "VARCHAR"
    BOOLEAN = "BOOLEAN"
    TINYINT = "TINYINT"
    SMALLINT = "SMALLINT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    DECIMAL = "DECIMAL"
    REAL = "REAL"
    DOUBLE = "DOUBLE"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_WITH_TIME_ZONE = "TIMESTAMP_WITH_TIME_ZONE"
    OBJECT = "OBJECT"
    NULL = "NULL"
    JSON = "JSON"


# Python type mappings for SQL column types
SQL_TYPE_TO_PYTHON = {
    SqlColumnType.VARCHAR: str,
    SqlColumnType.BOOLEAN: bool,
    SqlColumnType.TINYINT: int,
    SqlColumnType.SMALLINT: int,
    SqlColumnType.INTEGER: int,
    SqlColumnType.BIGINT: int,
    SqlColumnType.DECIMAL: Decimal,
    SqlColumnType.REAL: float,
    SqlColumnType.DOUBLE: float,
    SqlColumnType.DATE: date,
    SqlColumnType.TIME: time,
    SqlColumnType.TIMESTAMP: datetime,
    SqlColumnType.TIMESTAMP_WITH_TIME_ZONE: datetime,
    SqlColumnType.OBJECT: object,
    SqlColumnType.NULL: type(None),
    SqlColumnType.JSON: (dict, list, str),
}


def convert_sql_value(value: Any, column_type: SqlColumnType) -> Any:
    """Convert a raw SQL value to the appropriate Python type.

    Args:
        value: The raw value from the query result.
        column_type: The SQL column type.

    Returns:
        The converted Python value.
    """
    if value is None:
        return None

    if column_type == SqlColumnType.DECIMAL:
        if isinstance(value, (int, float, str)):
            return Decimal(str(value))
        return value

    if column_type == SqlColumnType.DATE:
        if isinstance(value, str):
            return date.fromisoformat(value)
        if isinstance(value, int):
            return date.fromordinal(value + date(1970, 1, 1).toordinal())
        return value

    if column_type == SqlColumnType.TIME:
        if isinstance(value, str):
            return time.fromisoformat(value)
        if isinstance(value, int):
            seconds = value // 1_000_000_000
            nanos = value % 1_000_000_000
            return time(
                hour=seconds // 3600,
                minute=(seconds % 3600) // 60,
                second=seconds % 60,
                microsecond=nanos // 1000,
            )
        return value

    if column_type == SqlColumnType.TIMESTAMP:
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        if isinstance(value, int):
            return datetime.fromtimestamp(value / 1000.0)
        return value

    if column_type == SqlColumnType.TIMESTAMP_WITH_TIME_ZONE:
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        if isinstance(value, int):
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        return value

    if column_type == SqlColumnType.JSON:
        if isinstance(value, str):
            import json
            try:
                return json.loads(value)
            except (json.JSONDecodeError, ValueError):
                return value
        return value

    return value


class SqlColumnMetadata:
    """Metadata for a single SQL column.

    Contains information about a column's name, type, and nullability.

    Attributes:
        name: The column name.
        type: The SQL data type of the column.
        nullable: Whether the column can contain NULL values.

    Example:
        >>> col = result.metadata.get_column(0)
        >>> print(f"Column: {col.name}, Type: {col.type.value}")
    """

    def __init__(
        self,
        name: str,
        column_type: SqlColumnType,
        nullable: bool = True,
    ):
        """Initialize column metadata.

        Args:
            name: The column name.
            column_type: The SQL data type.
            nullable: Whether the column allows NULL values.
        """
        self._name = name
        self._type = column_type
        self._nullable = nullable

    @property
    def name(self) -> str:
        """Get the column name.

        Returns:
            str: The column name as defined in the query or table schema.
        """
        return self._name

    @property
    def type(self) -> SqlColumnType:
        """Get the column type.

        Returns:
            SqlColumnType: The SQL data type of this column.
        """
        return self._type

    @property
    def nullable(self) -> bool:
        """Check if the column is nullable.

        Returns:
            bool: True if the column can contain NULL values.
        """
        return self._nullable

    def __repr__(self) -> str:
        return f"SqlColumnMetadata(name={self._name!r}, type={self._type.value})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SqlColumnMetadata):
            return False
        return self._name == other._name and self._type == other._type


class SqlRowMetadata:
    """Metadata for SQL result rows.

    Provides access to column information for an SQL result set.

    Example:
        >>> metadata = result.metadata
        >>> print(f"Columns: {metadata.column_count}")
        >>> for name in metadata.get_column_names():
        ...     idx = metadata.find_column(name)
        ...     col = metadata.get_column(idx)
        ...     print(f"  {name}: {col.type.value}")
    """

    def __init__(self, columns: List[SqlColumnMetadata]):
        """Initialize row metadata.

        Args:
            columns: List of column metadata objects.
        """
        self._columns = columns
        self._name_to_index: Dict[str, int] = {
            col.name: i for i, col in enumerate(columns)
        }

    @property
    def columns(self) -> List[SqlColumnMetadata]:
        """Get all column metadata.

        Returns:
            List[SqlColumnMetadata]: A copy of the column metadata list.
        """
        return list(self._columns)

    @property
    def column_count(self) -> int:
        """Get the number of columns.

        Returns:
            int: The number of columns in each row.
        """
        return len(self._columns)

    def get_column(self, index: int) -> SqlColumnMetadata:
        """Get column metadata by index.

        Args:
            index: The column index (0-based).

        Returns:
            SqlColumnMetadata: The metadata for the specified column.

        Raises:
            IndexError: If index is out of range.

        Example:
            >>> col = metadata.get_column(0)
            >>> print(col.name)
        """
        return self._columns[index]

    def find_column(self, name: str) -> int:
        """Find column index by name.

        Args:
            name: The column name (case-sensitive).

        Returns:
            int: Column index (0-based), or -1 if not found.

        Example:
            >>> idx = metadata.find_column("user_id")
            >>> if idx >= 0:
            ...     value = row.get_object(idx)
        """
        return self._name_to_index.get(name, -1)

    def get_column_names(self) -> List[str]:
        """Get all column names in order.

        Returns:
            List[str]: List of column names.

        Example:
            >>> names = metadata.get_column_names()
            >>> print(", ".join(names))
        """
        return [col.name for col in self._columns]

    def get_column_types(self) -> List[SqlColumnType]:
        """Get all column types in order.

        Returns:
            List[SqlColumnType]: List of column types.

        Example:
            >>> types = metadata.get_column_types()
        """
        return [col.type for col in self._columns]

    def __repr__(self) -> str:
        return f"SqlRowMetadata(columns={self._columns!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SqlRowMetadata):
            return False
        return self._columns == other._columns


class SqlRow:
    """A single row in an SQL result set.

    Provides access to column values by index or name. Supports
    iteration, indexing, and conversion to dict/tuple formats.

    Example:
        Accessing values::

            for row in result:
                # By index
                id_val = row[0]
                # By name
                name_val = row["name"]
                # Using methods
                age = row.get_object(2)
                status = row.get_object_by_name("status")

        Converting to other formats::

            row_dict = row.to_dict()
            row_tuple = row.to_tuple()
    """

    def __init__(
        self,
        values: List[Any],
        metadata: SqlRowMetadata,
        convert_types: bool = False,
    ):
        """Initialize an SQL row.

        Args:
            values: List of column values.
            metadata: Row metadata describing the columns.
            convert_types: If True, convert values to Python types based
                on column metadata.
        """
        self._metadata = metadata
        if convert_types:
            self._values = self._convert_values(values)
        else:
            self._values = values

    def _convert_values(self, values: List[Any]) -> List[Any]:
        """Convert raw values to appropriate Python types."""
        result = []
        columns = self._metadata.columns
        for i, val in enumerate(values):
            if i < len(columns):
                result.append(convert_sql_value(val, columns[i].type))
            else:
                result.append(val)
        return result

    @property
    def metadata(self) -> SqlRowMetadata:
        """Get the row metadata.

        Returns:
            SqlRowMetadata: The metadata describing this row's columns.
        """
        return self._metadata

    def get_object(self, index: int) -> Any:
        """Get a value by column index.

        Args:
            index: The column index (0-based).

        Returns:
            Any: The column value, or None if the value is NULL.

        Raises:
            IndexError: If index is out of range.

        Example:
            >>> first_col = row.get_object(0)
        """
        return self._values[index]

    def get_object_by_name(self, name: str) -> Any:
        """Get a value by column name.

        Args:
            name: The column name (case-sensitive).

        Returns:
            Any: The column value, or None if the value is NULL.

        Raises:
            KeyError: If column name is not found.

        Example:
            >>> user_id = row.get_object_by_name("user_id")
        """
        index = self._metadata.find_column(name)
        if index < 0:
            raise KeyError(f"Column not found: {name}")
        return self._values[index]

    def to_dict(self, convert_types: bool = False) -> Dict[str, Any]:
        """Convert the row to a dictionary.

        Args:
            convert_types: If True, convert values to Python types based
                on column metadata.

        Returns:
            Dict[str, Any]: Dictionary mapping column names to values.

        Example:
            >>> row_data = row.to_dict()
            >>> print(row_data["name"])
        """
        columns = self._metadata.columns
        result = {}
        for i, col in enumerate(columns):
            val = self._values[i] if i < len(self._values) else None
            if convert_types:
                val = convert_sql_value(val, col.type)
            result[col.name] = val
        return result

    def to_tuple(self) -> tuple:
        """Convert the row to a tuple of values.

        Returns:
            tuple: Tuple containing all column values in order.

        Example:
            >>> values = row.to_tuple()
            >>> id, name, age = values
        """
        return tuple(self._values)

    def to_list(self) -> List[Any]:
        """Convert the row to a list of values.

        Returns:
            List[Any]: List containing all column values in order.

        Example:
            >>> values = row.to_list()
        """
        return list(self._values)

    def __getitem__(self, key) -> Any:
        if isinstance(key, int):
            return self.get_object(key)
        return self.get_object_by_name(key)

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SqlRow):
            return False
        return self._values == other._values

    def __hash__(self) -> int:
        return hash(tuple(self._values))

    def __repr__(self) -> str:
        return f"SqlRow({self.to_dict()!r})"


class SqlPage:
    """A page of SQL result rows.

    Represents a batch of rows fetched from the server, useful for
    paginated result processing with control over memory usage.

    Attributes:
        rows: The rows in this page.
        metadata: The row metadata.
        is_last: Whether this is the last page.
        page_number: The page number (0-based).
        row_count: The number of rows in this page.

    Example:
        >>> for page in result.pages():
        ...     print(f"Page {page.page_number}: {page.row_count} rows")
        ...     for row in page:
        ...         process(row)
        ...     if page.is_last:
        ...         break
    """

    def __init__(
        self,
        rows: List[SqlRow],
        metadata: Optional[SqlRowMetadata] = None,
        is_last: bool = False,
        page_number: int = 0,
    ):
        """Initialize an SQL page.

        Args:
            rows: List of rows in this page.
            metadata: Row metadata (optional).
            is_last: Whether this is the last page.
            page_number: The page number (0-based).
        """
        self._rows = rows
        self._metadata = metadata
        self._is_last = is_last
        self._page_number = page_number

    @property
    def rows(self) -> List[SqlRow]:
        """Get the rows in this page.

        Returns:
            List[SqlRow]: The list of rows.
        """
        return self._rows

    @property
    def metadata(self) -> Optional[SqlRowMetadata]:
        """Get the row metadata.

        Returns:
            Optional[SqlRowMetadata]: The metadata, or None if not available.
        """
        return self._metadata

    @property
    def is_last(self) -> bool:
        """Check if this is the last page.

        Returns:
            bool: True if no more pages are available.
        """
        return self._is_last

    @property
    def page_number(self) -> int:
        """Get the page number (0-based).

        Returns:
            int: The page number.
        """
        return self._page_number

    @property
    def row_count(self) -> int:
        """Get the number of rows in this page.

        Returns:
            int: The row count.
        """
        return len(self._rows)

    def to_dicts(self, convert_types: bool = False) -> List[Dict[str, Any]]:
        """Convert all rows to dictionaries.

        Args:
            convert_types: If True, convert values to Python types.

        Returns:
            List[Dict[str, Any]]: List of dictionaries, one per row.

        Example:
            >>> records = page.to_dicts()
            >>> for rec in records:
            ...     print(rec["name"])
        """
        return [row.to_dict(convert_types) for row in self._rows]

    def to_tuples(self) -> List[tuple]:
        """Convert all rows to tuples.

        Returns:
            List[tuple]: List of tuples, one per row.

        Example:
            >>> tuples = page.to_tuples()
        """
        return [row.to_tuple() for row in self._rows]

    def __iter__(self) -> Iterator[SqlRow]:
        return iter(self._rows)

    def __len__(self) -> int:
        return len(self._rows)

    def __getitem__(self, index: int) -> SqlRow:
        return self._rows[index]

    def __repr__(self) -> str:
        return (
            f"SqlPage(page_number={self._page_number}, "
            f"row_count={len(self._rows)}, is_last={self._is_last})"
        )


class SqlResult:
    """Result of an SQL query execution.

    Supports iteration over rows for SELECT queries, or provides
    update count for DML queries. Provides both synchronous and
    asynchronous iteration with backpressure support for streaming
    results.

    The result must be closed when done to release server-side resources.
    Use as a context manager for automatic cleanup.

    Attributes:
        query_id: Unique identifier for this query.
        metadata: Row metadata (for SELECT queries).
        update_count: Number of affected rows (for DML queries, -1 for SELECT).
        is_row_set: True if this result contains rows.
        is_closed: True if the result has been closed.

    Example:
        Iterating over rows::

            result = sql.execute("SELECT * FROM users")
            for row in result:
                print(row["name"])
            result.close()

        Context manager (recommended)::

            with sql.execute("SELECT * FROM users") as result:
                for row in result:
                    print(row["name"])

        Async iteration::

            async with sql.execute_async("SELECT * FROM users") as result:
                async for row in result:
                    print(row["name"])

        Getting update count::

            result = sql.execute("DELETE FROM users WHERE status = ?", "inactive")
            print(f"Deleted {result.update_count} rows")
    """

    def __init__(
        self,
        query_id: Optional[str] = None,
        metadata: Optional[SqlRowMetadata] = None,
        update_count: int = -1,
        is_infinite: bool = False,
        close_callback: Optional[Callable[[], None]] = None,
        convert_types: bool = False,
    ):
        self._query_id = query_id
        self._metadata = metadata
        self._update_count = update_count
        self._is_infinite = is_infinite
        self._rows: List[SqlRow] = []
        self._row_index = 0
        self._closed = False
        self._lock = threading.Lock()
        self._fetch_callback: Optional[Callable[[], List[SqlRow]]] = None
        self._async_fetch_callback: Optional[Callable[[], Any]] = None
        self._close_callback: Optional[Callable[[], None]] = close_callback
        self._has_more = True
        self._iteration_started = False
        self._exhausted = False
        self._convert_types = convert_types
        self._page_number = 0
        self._backpressure_threshold = 1000
        self._backpressure_event = threading.Event()
        self._backpressure_event.set()

    @property
    def query_id(self) -> Optional[str]:
        """Get the query ID."""
        return self._query_id

    @property
    def metadata(self) -> Optional[SqlRowMetadata]:
        """Get the row metadata (for SELECT queries)."""
        return self._metadata

    @property
    def update_count(self) -> int:
        """Get the update count (for DML queries).

        Returns:
            Number of rows affected, or -1 for SELECT queries.
        """
        return self._update_count

    @property
    def is_row_set(self) -> bool:
        """Check if this result contains rows (SELECT query)."""
        return self._update_count < 0

    @property
    def is_closed(self) -> bool:
        """Check if this result is closed."""
        return self._closed

    def set_fetch_callback(self, callback: Callable[[], List[SqlRow]]) -> None:
        """Set callback for fetching more rows."""
        self._fetch_callback = callback

    def set_async_fetch_callback(self, callback: Callable[[], Any]) -> None:
        """Set async callback for fetching more rows."""
        self._async_fetch_callback = callback

    def add_rows(self, rows: List[SqlRow]) -> None:
        """Add rows to the result set with backpressure support."""
        with self._lock:
            self._rows.extend(rows)
            if len(self._rows) - self._row_index > self._backpressure_threshold:
                self._backpressure_event.clear()

    def set_has_more(self, has_more: bool) -> None:
        """Set whether more rows are available."""
        self._has_more = has_more

    def set_backpressure_threshold(self, threshold: int) -> None:
        """Set the backpressure threshold.

        Args:
            threshold: Maximum number of buffered rows before applying backpressure.
        """
        if threshold <= 0:
            raise ValueError("Backpressure threshold must be positive")
        self._backpressure_threshold = threshold

    def __iter__(self) -> Iterator[SqlRow]:
        """Iterate over result rows."""
        return self

    def __next__(self) -> SqlRow:
        """Get the next row.

        Returns:
            The next SqlRow.

        Raises:
            StopIteration: If no more rows are available.
        """
        if self._closed:
            raise StopIteration

        if not self.is_row_set:
            raise StopIteration

        self._iteration_started = True

        with self._lock:
            if self._row_index < len(self._rows):
                row = self._rows[self._row_index]
                self._row_index += 1
                if len(self._rows) - self._row_index < self._backpressure_threshold // 2:
                    self._backpressure_event.set()
                return row

            if self._exhausted or not self._has_more:
                raise StopIteration

        if self._fetch_callback and self._has_more:
            try:
                new_rows = self._fetch_callback()
                if new_rows:
                    with self._lock:
                        self._rows.extend(new_rows)
                        if self._row_index < len(self._rows):
                            row = self._rows[self._row_index]
                            self._row_index += 1
                            return row
                else:
                    self._exhausted = True
            except Exception:
                self._exhausted = True
                raise StopIteration

        self._exhausted = True
        raise StopIteration

    def __aiter__(self) -> AsyncIterator[SqlRow]:
        """Async iterate over result rows."""
        return self

    async def __anext__(self) -> SqlRow:
        """Get the next row asynchronously.

        Returns:
            The next SqlRow.

        Raises:
            StopAsyncIteration: If no more rows are available.
        """
        if self._closed:
            raise StopAsyncIteration

        if not self.is_row_set:
            raise StopAsyncIteration

        self._iteration_started = True

        with self._lock:
            if self._row_index < len(self._rows):
                row = self._rows[self._row_index]
                self._row_index += 1
                if len(self._rows) - self._row_index < self._backpressure_threshold // 2:
                    self._backpressure_event.set()
                return row

            if self._exhausted or not self._has_more:
                raise StopAsyncIteration

        if self._async_fetch_callback and self._has_more:
            try:
                new_rows = await self._async_fetch_callback()
                if new_rows:
                    with self._lock:
                        self._rows.extend(new_rows)
                        if self._row_index < len(self._rows):
                            row = self._rows[self._row_index]
                            self._row_index += 1
                            return row
                else:
                    self._exhausted = True
            except Exception:
                self._exhausted = True
                raise StopAsyncIteration
        elif self._fetch_callback and self._has_more:
            try:
                loop = asyncio.get_event_loop()
                new_rows = await loop.run_in_executor(None, self._fetch_callback)
                if new_rows:
                    with self._lock:
                        self._rows.extend(new_rows)
                        if self._row_index < len(self._rows):
                            row = self._rows[self._row_index]
                            self._row_index += 1
                            return row
                else:
                    self._exhausted = True
            except Exception:
                self._exhausted = True
                raise StopAsyncIteration

        self._exhausted = True
        raise StopAsyncIteration

    def get_all(self) -> List[SqlRow]:
        """Get all rows as a list.

        Returns:
            List of all SqlRow objects.
        """
        return list(self)

    async def get_all_async(self) -> List[SqlRow]:
        """Get all rows as a list asynchronously.

        Returns:
            List of all SqlRow objects.
        """
        rows = []
        async for row in self:
            rows.append(row)
        return rows

    def fetch_page(self) -> SqlPage:
        """Fetch the next page of results.

        Returns:
            SqlPage containing the next batch of rows.
        """
        if self._closed or not self.is_row_set:
            return SqlPage([], self._metadata, is_last=True, page_number=self._page_number)

        rows = []
        is_last = False

        with self._lock:
            while self._row_index < len(self._rows):
                rows.append(self._rows[self._row_index])
                self._row_index += 1

        if self._fetch_callback and self._has_more and not rows:
            try:
                new_rows = self._fetch_callback()
                if new_rows:
                    rows.extend(new_rows)
                else:
                    is_last = True
                    self._exhausted = True
            except Exception:
                is_last = True
                self._exhausted = True

        if not self._has_more or self._exhausted:
            is_last = True

        page = SqlPage(rows, self._metadata, is_last=is_last, page_number=self._page_number)
        self._page_number += 1
        return page

    async def fetch_page_async(self) -> SqlPage:
        """Fetch the next page of results asynchronously.

        Returns:
            SqlPage containing the next batch of rows.
        """
        if self._closed or not self.is_row_set:
            return SqlPage([], self._metadata, is_last=True, page_number=self._page_number)

        rows = []
        is_last = False

        with self._lock:
            while self._row_index < len(self._rows):
                rows.append(self._rows[self._row_index])
                self._row_index += 1

        if self._has_more and not rows:
            if self._async_fetch_callback:
                try:
                    new_rows = await self._async_fetch_callback()
                    if new_rows:
                        rows.extend(new_rows)
                    else:
                        is_last = True
                        self._exhausted = True
                except Exception:
                    is_last = True
                    self._exhausted = True
            elif self._fetch_callback:
                try:
                    loop = asyncio.get_event_loop()
                    new_rows = await loop.run_in_executor(None, self._fetch_callback)
                    if new_rows:
                        rows.extend(new_rows)
                    else:
                        is_last = True
                        self._exhausted = True
                except Exception:
                    is_last = True
                    self._exhausted = True

        if not self._has_more or self._exhausted:
            is_last = True

        page = SqlPage(rows, self._metadata, is_last=is_last, page_number=self._page_number)
        self._page_number += 1
        return page

    def pages(self) -> Iterator[SqlPage]:
        """Iterate over result pages.

        Yields:
            SqlPage objects until all results are consumed.
        """
        while True:
            page = self.fetch_page()
            if page.row_count > 0:
                yield page
            if page.is_last:
                break

    async def pages_async(self) -> AsyncIterator[SqlPage]:
        """Iterate over result pages asynchronously.

        Yields:
            SqlPage objects until all results are consumed.
        """
        while True:
            page = await self.fetch_page_async()
            if page.row_count > 0:
                yield page
            if page.is_last:
                break

    def close(self) -> None:
        """Close this result and release resources."""
        if self._closed:
            return
        self._closed = True
        self._exhausted = True
        with self._lock:
            self._rows.clear()
        if self._close_callback:
            try:
                self._close_callback()
            except Exception:
                pass

    async def close_async(self) -> None:
        """Close this result asynchronously."""
        self.close()

    def __enter__(self) -> "SqlResult":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    async def __aenter__(self) -> "SqlResult":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close_async()

    def rows_as_dicts(self, convert_types: bool = False) -> List[Dict[str, Any]]:
        """Get all rows as a list of dictionaries.

        Args:
            convert_types: If True, convert values to Python types.

        Returns:
            List of dictionaries mapping column names to values.
        """
        return [row.to_dict(convert_types) for row in self.get_all()]

    def rows_as_tuples(self) -> List[tuple]:
        """Get all rows as a list of tuples.

        Returns:
            List of tuples containing column values.
        """
        return [row.to_tuple() for row in self.get_all()]

    def first(self) -> Optional[SqlRow]:
        """Get the first row, if available.

        Returns:
            The first SqlRow, or None if no rows exist.
        """
        try:
            return next(iter(self))
        except StopIteration:
            return None

    def first_or_raise(self) -> SqlRow:
        """Get the first row, raising if not available.

        Returns:
            The first SqlRow.

        Raises:
            ValueError: If no rows are available.
        """
        row = self.first()
        if row is None:
            raise ValueError("No rows in result")
        return row

    def scalar(self) -> Any:
        """Get the single value from a single-row, single-column result.

        Returns:
            The scalar value.

        Raises:
            ValueError: If result doesn't contain exactly one row and column.
        """
        row = self.first_or_raise()
        if len(row) != 1:
            raise ValueError(f"Expected 1 column, got {len(row)}")
        return row[0]

    def column_values(self, column: str) -> List[Any]:
        """Get all values for a specific column.

        Args:
            column: The column name.

        Returns:
            List of values from that column across all rows.
        """
        return [row.get_object_by_name(column) for row in self.get_all()]

    def __len__(self) -> int:
        """Return the number of rows fetched so far."""
        with self._lock:
            return len(self._rows)

    def __bool__(self) -> bool:
        """Return True if this is a row set with potential rows."""
        return self.is_row_set

    def __repr__(self) -> str:
        if self.is_row_set:
            return f"SqlResult(query_id={self._query_id!r}, is_row_set=True)"
        return f"SqlResult(update_count={self._update_count})"
