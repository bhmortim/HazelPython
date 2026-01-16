SQL Queries
===========

The Hazelcast Python Client supports SQL queries for querying and
manipulating data stored in IMap and other data structures.

Getting the SQL Service
-----------------------

.. code-block:: python

    sql = client.get_sql()

Basic Queries
-------------

SELECT Queries
~~~~~~~~~~~~~~

.. code-block:: python

    # Query a map
    result = sql.execute("SELECT * FROM my_map")

    # Iterate results
    for row in result:
        print(row.to_dict())

    # With parameters
    result = sql.execute(
        "SELECT * FROM users WHERE age > ? AND status = ?",
        30,
        "active"
    )

INSERT Queries
~~~~~~~~~~~~~~

.. code-block:: python

    sql.execute(
        "INSERT INTO users (__key, name, age) VALUES (?, ?, ?)",
        "user:123",
        "Alice",
        30
    )

UPDATE Queries
~~~~~~~~~~~~~~

.. code-block:: python

    sql.execute(
        "UPDATE users SET age = ? WHERE __key = ?",
        31,
        "user:123"
    )

DELETE Queries
~~~~~~~~~~~~~~

.. code-block:: python

    sql.execute("DELETE FROM users WHERE age < ?", 18)

Working with Results
--------------------

Row Access
~~~~~~~~~~

.. code-block:: python

    result = sql.execute("SELECT name, age, email FROM users")

    for row in result:
        # By column name
        name = row.get("name")

        # By index
        age = row[1]

        # As dictionary
        data = row.to_dict()

Row Metadata
~~~~~~~~~~~~

.. code-block:: python

    result = sql.execute("SELECT * FROM users")

    # Get column information
    for column in result.row_metadata.columns:
        print(f"{column.name}: {column.type}")

Statement Configuration
-----------------------

Configure query execution:

.. code-block:: python

    from hazelcast.sql import SqlStatement

    statement = SqlStatement("SELECT * FROM large_table")
    statement.timeout = 30.0  # 30 second timeout
    statement.cursor_buffer_size = 1000  # Rows per page

    result = sql.execute_statement(statement)

Creating Mappings
-----------------

Before querying, create a mapping:

.. code-block:: python

    sql.execute("""
        CREATE MAPPING users (
            __key VARCHAR,
            name VARCHAR,
            age INT,
            email VARCHAR
        )
        TYPE IMap
        OPTIONS (
            'keyFormat' = 'varchar',
            'valueFormat' = 'json-flat'
        )
    """)

Aggregations
------------

.. code-block:: python

    # Count
    result = sql.execute("SELECT COUNT(*) FROM users")
    count = next(iter(result))[0]

    # Sum, Avg, Min, Max
    result = sql.execute("""
        SELECT
            COUNT(*) as total,
            AVG(age) as avg_age,
            MIN(age) as min_age,
            MAX(age) as max_age
        FROM users
    """)

    row = next(iter(result))
    print(f"Total: {row.get('total')}, Avg Age: {row.get('avg_age')}")

Grouping
--------

.. code-block:: python

    result = sql.execute("""
        SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
        FROM employees
        GROUP BY department
        ORDER BY count DESC
    """)

    for row in result:
        print(f"{row.get('department')}: {row.get('count')} employees")

Joins
-----

.. code-block:: python

    result = sql.execute("""
        SELECT u.name, o.order_id, o.total
        FROM users u
        JOIN orders o ON u.__key = o.user_id
        WHERE o.total > ?
    """, 100.0)

Error Handling
--------------

.. code-block:: python

    from hazelcast.exceptions import HazelcastException

    try:
        result = sql.execute("SELECT * FROM nonexistent_table")
        for row in result:
            print(row)
    except HazelcastException as e:
        print(f"Query failed: {e}")
