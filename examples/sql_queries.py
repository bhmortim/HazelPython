"""SQL Queries Example.

This example demonstrates how to execute SQL queries against Hazelcast
using the SqlService, including parameterized queries and result iteration.

Prerequisites:
    - A running Hazelcast cluster with SQL enabled
    - A map with data to query (or use the setup code below)

Usage:
    python sql_queries.py
"""

from hazelcast import HazelcastClient, ClientConfig
from hazelcast.sql.service import SqlService, SqlServiceError
from hazelcast.sql.statement import SqlStatement, SqlExpectedResultType
from hazelcast.exceptions import IllegalStateException


def setup_test_data(client: HazelcastClient) -> None:
    """Populate a map with sample employee data."""
    employees = client.get_map("employees")
    employees.clear()

    sample_data = [
        ("emp1", {"id": 1, "name": "Alice", "department": "Engineering", "salary": 75000}),
        ("emp2", {"id": 2, "name": "Bob", "department": "Engineering", "salary": 65000}),
        ("emp3", {"id": 3, "name": "Charlie", "department": "Sales", "salary": 55000}),
        ("emp4", {"id": 4, "name": "Diana", "department": "Engineering", "salary": 85000}),
        ("emp5", {"id": 5, "name": "Eve", "department": "Marketing", "salary": 60000}),
    ]

    for key, value in sample_data:
        employees.put(key, value)

    print(f"Populated {employees.size()} employees")


def basic_query_example(sql_service: SqlService) -> None:
    """Execute a simple SELECT query."""
    print("\n--- Basic SELECT Query ---")

    try:
        result = sql_service.execute("SELECT * FROM employees")

        print(f"Query ID: {result.query_id[:8]}...")
        print(f"Is row set: {result.is_row_set}")

        if result.metadata:
            columns = [col.name for col in result.metadata.columns]
            print(f"Columns: {columns}")

        row_count = 0
        for row in result:
            print(f"  Row: {row.to_dict()}")
            row_count += 1

        print(f"Total rows: {row_count}")

    except SqlServiceError as e:
        print(f"SQL Error: {e.message} (code={e.code})")
        if e.suggestion:
            print(f"Suggestion: {e.suggestion}")


def parameterized_query_example(sql_service: SqlService) -> None:
    """Execute a parameterized query with positional parameters."""
    print("\n--- Parameterized Query ---")

    try:
        # Query with positional parameters
        result = sql_service.execute(
            "SELECT * FROM employees WHERE department = ? AND salary > ?",
            "Engineering",  # First parameter
            70000,          # Second parameter
        )

        print("High-earning engineers:")
        for row in result:
            print(f"  {row.to_dict()}")

    except SqlServiceError as e:
        print(f"SQL Error: {e.message}")


def sql_statement_example(sql_service: SqlService) -> None:
    """Use SqlStatement for fine-grained control over query execution."""
    print("\n--- Using SqlStatement ---")

    try:
        # Create a statement with configuration
        stmt = SqlStatement("SELECT * FROM employees WHERE salary >= ?")
        stmt.add_parameter(60000)
        stmt.timeout = 30.0  # 30 second timeout
        stmt.cursor_buffer_size = 100
        stmt.expected_result_type = SqlExpectedResultType.ROWS

        print(f"Statement SQL: {stmt.sql}")
        print(f"Parameters: {stmt.parameters}")
        print(f"Timeout: {stmt.timeout}s")

        result = sql_service.execute_statement(stmt)

        print("Employees with salary >= 60000:")
        for row in result:
            print(f"  {row.to_dict()}")

    except SqlServiceError as e:
        print(f"SQL Error: {e.message}")


def dml_query_example(sql_service: SqlService) -> None:
    """Execute DML (INSERT, UPDATE, DELETE) queries."""
    print("\n--- DML Query Example ---")

    try:
        # Note: DML queries return update counts instead of row sets
        result = sql_service.execute(
            "UPDATE employees SET salary = salary * 1.1 WHERE department = ?",
            "Engineering",
        )

        if not result.is_row_set:
            print(f"Rows affected: {result.update_count}")

    except SqlServiceError as e:
        print(f"SQL Error: {e.message}")


def async_query_example(sql_service: SqlService) -> None:
    """Execute a query asynchronously."""
    print("\n--- Async Query Example ---")

    try:
        # Execute asynchronously
        future = sql_service.execute_async(
            "SELECT * FROM employees WHERE department = ?",
            "Sales",
        )

        print("Query submitted, waiting for result...")

        # Wait for and process the result
        result = future.result(timeout=10.0)

        print("Sales department employees:")
        for row in result:
            print(f"  {row.to_dict()}")

    except SqlServiceError as e:
        print(f"SQL Error: {e.message}")
    except Exception as e:
        print(f"Error: {e}")


def error_handling_example(sql_service: SqlService) -> None:
    """Demonstrate error handling for SQL operations."""
    print("\n--- Error Handling Example ---")

    # Example 1: Empty query
    try:
        sql_service.execute("")
    except SqlServiceError as e:
        print(f"Empty query error: {e.message}")

    # Example 2: Invalid SQL syntax (would fail on real cluster)
    try:
        result = sql_service.execute("SELCT * FORM employees")  # Typos
        # In a stub implementation, this might not fail
        print("Query executed (stub implementation)")
    except SqlServiceError as e:
        print(f"Syntax error: {e.message}")
        if e.suggestion:
            print(f"  Suggestion: {e.suggestion}")

    # Example 3: Service not running
    stopped_service = SqlService()
    # Don't start it
    try:
        stopped_service.execute("SELECT 1")
    except IllegalStateException as e:
        print(f"Service state error: {e.message}")


def main():
    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    with HazelcastClient(config) as client:
        print(f"Connected to cluster: {config.cluster_name}")

        # Setup test data
        setup_test_data(client)

        # Get SQL service
        sql_service = client.get_sql()
        print(f"\nSQL Service: {sql_service}")

        # Run examples
        basic_query_example(sql_service)
        parameterized_query_example(sql_service)
        sql_statement_example(sql_service)
        dml_query_example(sql_service)
        async_query_example(sql_service)
        error_handling_example(sql_service)

    print("\nClient shutdown complete")


if __name__ == "__main__":
    main()
