SQL Service
===========

Hazelcast SQL enables querying data stored in the cluster using standard SQL.

Basic Queries
-------------

.. code-block:: python

   sql = client.get_sql()

   # Simple query
   result = sql.execute("SELECT * FROM employees")

   for row in result:
       print(row.to_dict())

Parameterized Queries
---------------------

Use positional parameters for safe query execution:

.. code-block:: python

   sql = client.get_sql()

   # Positional parameters
   result = sql.execute(
       "SELECT * FROM employees WHERE department = ? AND salary > ?",
       "Engineering",
       50000,
   )

   for row in result:
       print(f"{row['name']}: ${row['salary']}")


SqlStatement
------------

For fine-grained control, use ``SqlStatement``:

.. code-block:: python

   from hazelcast.sql import SqlStatement, SqlExpectedResultType

   sql = client.get_sql()

   stmt = SqlStatement("SELECT * FROM employees WHERE age >= ?")
   stmt.add_parameter(30)
   stmt.timeout = 30.0  # 30 second timeout
   stmt.cursor_buffer_size = 100
   stmt.expected_result_type = SqlExpectedResultType.ROWS

   result = sql.execute_statement(stmt)

   for row in result:
       print(row.to_dict())


Working with Results
--------------------

SqlResult Properties
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   result = sql.execute("SELECT * FROM employees")

   # Check if result is a row set (vs update count)
   if result.is_row_set:
       # Access metadata
       metadata = result.metadata
       for column in metadata.columns:
           print(f"Column: {column.name}, Type: {column.type}")

       # Iterate rows
       for row in result:
           # Access by column name
           name = row["name"]

           # Access by index
           first_col = row[0]

           # Convert to dict
           data = row.to_dict()

   else:
       # DML query
       print(f"Rows affected: {result.update_count}")

Row Access
~~~~~~~~~~

.. code-block:: python

   result = sql.execute("SELECT name, age, department FROM employees")

   for row in result:
       # By column name
       name = row["name"]
       age = row["age"]

       # By index
       name = row[0]
       age = row[1]

       # As dictionary
       data = row.to_dict()
       # {"name": "Alice", "age": 30, "department": "Engineering"}


DML Queries
-----------

Execute INSERT, UPDATE, DELETE queries:

.. code-block:: python

   sql = client.get_sql()

   # Insert
   result = sql.execute(
       "INSERT INTO employees (id, name, salary) VALUES (?, ?, ?)",
       1, "Alice", 75000,
   )
   print(f"Inserted: {result.update_count} rows")

   # Update
   result = sql.execute(
       "UPDATE employees SET salary = salary * 1.1 WHERE department = ?",
       "Engineering",
   )
   print(f"Updated: {result.update_count} rows")

   # Delete
   result = sql.execute(
       "DELETE FROM employees WHERE status = ?",
       "inactive",
   )
   print(f"Deleted: {result.update_count} rows")


Async Queries
-------------

Execute queries asynchronously:

.. code-block:: python

   sql = client.get_sql()

   # Execute asynchronously
   future = sql.execute_async("SELECT * FROM employees WHERE department = ?", "Sales")

   # Do other work...

   # Wait for result
   result = future.result(timeout=10.0)

   for row in result:
       print(row.to_dict())


Error Handling
--------------

.. code-block:: python

   from hazelcast.sql import SqlServiceError

   sql = client.get_sql()

   try:
       result = sql.execute("SELECT * FROM nonexistent_table")
   except SqlServiceError as e:
       print(f"SQL Error: {e.message}")
       print(f"Error code: {e.code}")
       if e.suggestion:
           print(f"Suggestion: {e.suggestion}")


Mapping Types
-------------

Before querying IMaps with SQL, create a mapping:

.. code-block:: sql

   CREATE MAPPING employees (
       __key VARCHAR,
       id INTEGER,
       name VARCHAR,
       department VARCHAR,
       salary DECIMAL
   )
   TYPE IMap
   OPTIONS (
       'keyFormat' = 'varchar',
       'valueFormat' = 'json-flat'
   )

Execute via Python:

.. code-block:: python

   sql = client.get_sql()

   # Create mapping
   sql.execute("""
       CREATE MAPPING employees (
           __key VARCHAR,
           id INTEGER,
           name VARCHAR,
           department VARCHAR,
           salary DECIMAL
       )
       TYPE IMap
       OPTIONS (
           'keyFormat' = 'varchar',
           'valueFormat' = 'json-flat'
       )
   """)

   # Now query the map
   result = sql.execute("SELECT * FROM employees WHERE salary > 50000")


Supported SQL Features
----------------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Feature
     - Support
   * - SELECT
     - Full support with projections, filters, ordering
   * - INSERT
     - Supported for IMap
   * - UPDATE
     - Supported for IMap
   * - DELETE
     - Supported for IMap
   * - JOIN
     - Limited support
   * - Aggregate functions
     - COUNT, SUM, AVG, MIN, MAX
   * - Window functions
     - Not supported
   * - Subqueries
     - Limited support


Best Practices
--------------

1. **Use parameterized queries**: Prevents SQL injection and improves
   query plan caching.

2. **Create mappings explicitly**: Define column types for better
   performance.

3. **Set appropriate timeouts**: Prevent long-running queries from
   blocking resources.

4. **Handle large results with care**: Use LIMIT or stream results
   instead of loading all into memory.
