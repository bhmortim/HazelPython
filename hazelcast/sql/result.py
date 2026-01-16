"""SQL result handling for Hazelcast SQL queries."""

from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional, TYPE_CHECKING
from concurrent.futures import Future
import threading

if TYPE_CHECKING:
    from hazelcast.sql.service import SqlService


class SqlColumnType(Enum):
    """SQL column data types."""

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


class SqlColumnMetadata:
    """Metadata for a single SQL column."""

    def __init__(
        self,
        name: str,
        column_type: SqlColumnType,
        nullable: bool = True,
    ):
        self._name = name
        self._type = column_type
        self._nullable = nullable

    @property
    def name(self) -> str:
        """Get the column name."""
        return self._name

    @property
    def type(self) -> SqlColumnType:
        """Get the column type."""
        return self._type

    @property
    def nullable(self) -> bool:
        """Check if the column is nullable."""
        return self._nullable

    def __repr__(self) -> str:
        return f"SqlColumnMetadata(name={self._name!r}, type={self._type.value})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SqlColumnMetadata):
            return False
        return self._name == other._name and self._type == other._type


class SqlRowMetadata:
    """Metadata for SQL result rows."""

    def __init__(self, columns: List[SqlColumnMetadata]):
        self._columns = columns
        self._name_to_index: Dict[str, int] = {
            col.name: i for i, col in enumerate(columns)
        }

    @property
    def columns(self) -> List[SqlColumnMetadata]:
        """Get all column metadata."""
        return list(self._columns)

    @property
    def column_count(self) -> int:
        """Get the number of columns."""
        return len(self._columns)

    def get_column(self, index: int) -> SqlColumnMetadata:
        """Get column metadata by index.

        Args:
            index: The column index (0-based).

        Returns:
            Column metadata.

        Raises:
            IndexError: If index is out of range.
        """
        return self._columns[index]

    def find_column(self, name: str) -> int:
        """Find column index by name.

        Args:
            name: The column name.

        Returns:
            Column index, or -1 if not found.
        """
        return self._name_to_index.get(name, -1)

    def __repr__(self) -> str:
        return f"SqlRowMetadata(columns={self._columns!r})"


class SqlRow:
    """A single row in an SQL result set."""

    def __init__(self, values: List[Any], metadata: SqlRowMetadata):
        self._values = values
        self._metadata = metadata

    @property
    def metadata(self) -> SqlRowMetadata:
        """Get the row metadata."""
        return self._metadata

    def get_object(self, index: int) -> Any:
        """Get a value by column index.

        Args:
            index: The column index (0-based).

        Returns:
            The column value.

        Raises:
            IndexError: If index is out of range.
        """
        return self._values[index]

    def get_object_by_name(self, name: str) -> Any:
        """Get a value by column name.

        Args:
            name: The column name.

        Returns:
            The column value.

        Raises:
            KeyError: If column name is not found.
        """
        index = self._metadata.find_column(name)
        if index < 0:
            raise KeyError(f"Column not found: {name}")
        return self._values[index]

    def to_dict(self) -> Dict[str, Any]:
        """Convert the row to a dictionary.

        Returns:
            Dictionary mapping column names to values.
        """
        return {
            col.name: self._values[i]
            for i, col in enumerate(self._metadata.columns)
        }

    def __getitem__(self, key) -> Any:
        if isinstance(key, int):
            return self.get_object(key)
        return self.get_object_by_name(key)

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._values)

    def __repr__(self) -> str:
        return f"SqlRow({self.to_dict()!r})"


class SqlResult:
    """Result of an SQL query execution.

    Supports iteration over rows for SELECT queries,
    or provides update count for DML queries.
    """

    def __init__(
        self,
        query_id: Optional[str] = None,
        metadata: Optional[SqlRowMetadata] = None,
        update_count: int = -1,
        is_infinite: bool = False,
        close_callback: Optional[Callable[[], None]] = None,
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
        self._close_callback: Optional[Callable[[], None]] = close_callback
        self._has_more = True
        self._iteration_started = False
        self._exhausted = False

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

    def add_rows(self, rows: List[SqlRow]) -> None:
        """Add rows to the result set."""
        with self._lock:
            self._rows.extend(rows)

    def set_has_more(self, has_more: bool) -> None:
        """Set whether more rows are available."""
        self._has_more = has_more

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

    def get_all(self) -> List[SqlRow]:
        """Get all rows as a list.

        Returns:
            List of all SqlRow objects.
        """
        return list(self)

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

    def rows_as_dicts(self) -> List[Dict[str, Any]]:
        """Get all rows as a list of dictionaries.

        Returns:
            List of dictionaries mapping column names to values.
        """
        return [row.to_dict() for row in self.get_all()]

    def __len__(self) -> int:
        """Return the number of rows fetched so far."""
        with self._lock:
            return len(self._rows)

    def __repr__(self) -> str:
        if self.is_row_set:
            return f"SqlResult(query_id={self._query_id!r}, is_row_set=True)"
        return f"SqlResult(update_count={self._update_count})"
