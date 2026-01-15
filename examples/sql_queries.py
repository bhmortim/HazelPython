"""SQL queries example for the Hazelcast Python client.

This example demonstrates:
- Executing SQL SELECT queries
- Using parameterized queries
- Processing result sets
- Executing DML statements
"""

from hazelcast import HazelcastClient, ClientConfig
from hazelcast.sql import SqlService, SqlStatement, SqlExpectedResultType


def main():
    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = ["localhost:5701"]

    client = HazelcastClient(config)

    try:
        client.start()

        # Create SQL service
        sql_service = SqlService()
        sql_service.start()

        # Create a mapping (in real usage, this would be against a real cluster)
        print("--- SQL Examples ---\n")

        # Simple SELECT query
        print("1. Simple SELECT:")
        result = sql_service.execute("SELECT * FROM employees")
        print(f"   Query ID: {result.query_id}")
        print(f"   Is row set: {result.is_row_set}")

        # Parameterized query
        print("\n2. Parameterized Query:")
        result = sql_service.execute(
            "SELECT * FROM employees WHERE department = ? AND salary > ?",
            "Engineering",
            50000,
            timeout=30.0
        )
        print(f"   Query executed with parameters")

        # Using SqlStatement for more control
        print("\n3. Using SqlStatement:")
        stmt = SqlStatement("SELECT * FROM employees WHERE active = ?")
        stmt.add_parameter(True)
        stmt.timeout = 60.0
        stmt.cursor_buffer_size = 1000
        stmt.schema = "public"
        stmt.expected_result_type = SqlExpectedResultType.ROWS

        result = sql_service.execute_statement(stmt)
        print(f"   Statement executed: {stmt}")

        # DML statement
        print("\n4. DML Statement:")
        result = sql_service.execute(
            "INSERT INTO employees (id, name, department) VALUES (?, ?, ?)",
            1001,
            "John Doe",
            "Sales"
        )
        print(f"   Update count: {result.update_count}")

        # Async execution
        print("\n5. Async Execution:")
        future = sql_service.execute_async("SELECT COUNT(*) FROM employees")
        result = future.result()
        print(f"   Async query completed")

        # Processing results
        print("\n6. Processing Results:")
        from hazelcast.sql import SqlRowMetadata, SqlColumnMetadata, SqlColumnType, SqlRow

        # Create sample metadata and rows for demonstration
        columns = [
            SqlColumnMetadata("id", SqlColumnType.INTEGER),
            SqlColumnMetadata("name", SqlColumnType.VARCHAR),
            SqlColumnMetadata("salary", SqlColumnType.DOUBLE),
        ]
        metadata = SqlRowMetadata(columns)

        # Demonstrate row access
        sample_row = SqlRow([1, "Alice", 75000.0], metadata)
        print(f"   Row as dict: {sample_row.to_dict()}")
        print(f"   Access by index: {sample_row[0]}")
        print(f"   Access by name: {sample_row['name']}")

        sql_service.shutdown()

    finally:
        client.shutdown()


if __name__ == "__main__":
    main()
