SQL and Jet
===========

Hazelcast provides SQL query capabilities and Jet stream processing
for analyzing and transforming distributed data.

SQL Queries
-----------

Execute SQL queries against data stored in Hazelcast maps.

Basic Queries
~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   with HazelcastClient() as client:
       sql = client.get_sql()
       
       # Simple query
       result = sql.execute("SELECT * FROM employees")
       
       for row in result:
           print(row.to_dict())
       
       # Query with parameters
       result = sql.execute(
           "SELECT * FROM employees WHERE age > ? AND department = ?",
           30, "Engineering"
       )
       
       # Access columns by name or index
       for row in result:
           name = row["name"]
           age = row[1]  # By index
           print(f"{name}: {age}")

Query Options
~~~~~~~~~~~~~

Configure query behavior:

.. code-block:: python

   from hazelcast.sql.statement import SqlExpectedResultType

   result = sql.execute(
       "SELECT name, salary FROM employees WHERE department = ?",
       "Sales",
       timeout=30.0,  # Query timeout in seconds
       cursor_buffer_size=100,  # Rows to buffer
       schema="public",  # Default schema
       expected_result_type=SqlExpectedResultType.ROWS,
   )

DML Operations
~~~~~~~~~~~~~~

Execute INSERT, UPDATE, DELETE statements:

.. code-block:: python

   # Insert
   result = sql.execute(
       "INSERT INTO employees (id, name, age) VALUES (?, ?, ?)",
       101, "Alice", 28
   )
   print(f"Rows affected: {result.update_count}")
   
   # Update
   result = sql.execute(
       "UPDATE employees SET salary = salary * 1.1 WHERE department = ?",
       "Engineering"
   )
   
   # Delete
   result = sql.execute(
       "DELETE FROM employees WHERE status = ?",
       "inactive"
   )

Explain Plans
~~~~~~~~~~~~~

Analyze query execution plans:

.. code-block:: python

   explain_result = sql.explain(
       "SELECT * FROM employees WHERE age > ?",
       30
   )
   
   print(explain_result.plan)

Using SqlStatement
~~~~~~~~~~~~~~~~~~

For complex queries, use SqlStatement:

.. code-block:: python

   from hazelcast.sql.statement import SqlStatement, SqlExpectedResultType

   statement = SqlStatement("SELECT * FROM orders WHERE total > ?")
   statement.set_parameters(1000)
   statement.timeout = 60.0
   statement.cursor_buffer_size = 500
   statement.expected_result_type = SqlExpectedResultType.ROWS
   
   result = sql.execute_statement(statement)

Async Execution
~~~~~~~~~~~~~~~

Execute queries asynchronously:

.. code-block:: python

   # Async execute
   future = sql.execute_async("SELECT COUNT(*) FROM orders")
   result = future.result()
   
   # Or with asyncio
   async def query_async():
       result = await sql.execute_async("SELECT * FROM products")
       async for row in result:
           print(row.to_dict())

Column Metadata
~~~~~~~~~~~~~~~

Access result set metadata:

.. code-block:: python

   result = sql.execute("SELECT * FROM employees")
   
   metadata = result.metadata
   for column in metadata.columns:
       print(f"Column: {column.name}, Type: {column.type}")

Creating Mappings
~~~~~~~~~~~~~~~~~

Before querying a map, create a mapping:

.. code-block:: sql

   CREATE MAPPING employees (
       id BIGINT,
       name VARCHAR,
       age INT,
       salary DECIMAL,
       department VARCHAR
   )
   TYPE IMap
   OPTIONS (
       'keyFormat' = 'bigint',
       'valueFormat' = 'json-flat'
   )

Or programmatically:

.. code-block:: python

   sql.execute("""
       CREATE MAPPING IF NOT EXISTS employees (
           id BIGINT,
           name VARCHAR,
           age INT
       )
       TYPE IMap
       OPTIONS (
           'keyFormat' = 'bigint',
           'valueFormat' = 'json-flat'
       )
   """)

Jet Stream Processing
---------------------

Jet enables distributed stream and batch processing.

Creating Pipelines
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.jet.pipeline import Pipeline

   # Create a pipeline
   pipeline = Pipeline.create()
   
   # Add stages
   pipeline.read_from("orders") \
       .filter(lambda order: order["total"] > 100) \
       .map(lambda order: {
           "id": order["id"],
           "customer": order["customer"],
           "total": order["total"]
       }) \
       .write_to("high-value-orders")

Submitting Jobs
~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.jet.job import JobConfig, ProcessingGuarantee

   with HazelcastClient() as client:
       jet = client.get_jet()
       
       # Submit with default config
       job = jet.submit(pipeline)
       
       # Submit with custom config
       config = JobConfig()
       config.name = "order-processor"
       config.processing_guarantee = ProcessingGuarantee.EXACTLY_ONCE
       
       job = jet.submit(pipeline, config)
       
       print(f"Job ID: {job.id}")
       print(f"Status: {job.status}")

Job Management
~~~~~~~~~~~~~~

.. code-block:: python

   jet = client.get_jet()
   
   # Get job by ID
   job = jet.get_job(job_id)
   
   # Get job by name
   job = jet.get_job_by_name("order-processor")
   
   # List all jobs
   jobs = jet.get_jobs()
   
   # List active jobs
   active_jobs = jet.get_active_jobs()
   
   # Control job lifecycle
   job.suspend()
   job.resume()
   job.cancel()
   job.restart()
   
   # Wait for completion
   job.join()
   job.join(timeout=60.0)

Job Status
~~~~~~~~~~

.. code-block:: python

   from hazelcast.jet.job import JobStatus

   job = jet.get_job(job_id)
   
   # Check status
   if job.status == JobStatus.RUNNING:
       print("Job is running")
   elif job.status == JobStatus.COMPLETED:
       print("Job completed successfully")
   elif job.status == JobStatus.FAILED:
       print(f"Job failed: {job.failure_reason}")
   
   # Check if terminal state
   if job.is_terminal():
       print("Job has finished")

Processing Guarantees
~~~~~~~~~~~~~~~~~~~~~

Configure fault tolerance:

.. code-block:: python

   from hazelcast.jet.job import ProcessingGuarantee, JobConfig

   config = JobConfig()
   
   # No guarantee (fastest)
   config.processing_guarantee = ProcessingGuarantee.NONE
   
   # At-least-once (may process duplicates on failure)
   config.processing_guarantee = ProcessingGuarantee.AT_LEAST_ONCE
   
   # Exactly-once (strongest guarantee)
   config.processing_guarantee = ProcessingGuarantee.EXACTLY_ONCE
   
   # Snapshot interval
   config.snapshot_interval_millis = 10000  # 10 seconds

Light Jobs
~~~~~~~~~~

For short-running jobs without fault tolerance:

.. code-block:: python

   # Light jobs have lower overhead
   job = jet.new_light_job(pipeline)
   
   # Wait for result
   job.join()

Job State Listeners
~~~~~~~~~~~~~~~~~~~

Monitor job state changes:

.. code-block:: python

   from hazelcast.jet.service import JobStateListener

   class MyListener(JobStateListener):
       def on_state_changed(self, job, old_status, new_status):
           print(f"Job {job.name}: {old_status} -> {new_status}")
       
       def on_job_completed(self, job):
           print(f"Job {job.name} completed!")
       
       def on_job_failed(self, job, reason):
           print(f"Job {job.name} failed: {reason}")

   jet = client.get_jet()
   reg_id = jet.add_job_state_listener(MyListener())
   
   # Later
   jet.remove_job_state_listener(reg_id)

Best Practices
--------------

SQL Best Practices
~~~~~~~~~~~~~~~~~~

1. **Create Mappings**: Always create explicit mappings before querying
2. **Use Parameters**: Use parameterized queries to prevent SQL injection
3. **Set Timeouts**: Always set query timeouts to prevent runaway queries
4. **Buffer Size**: Adjust cursor_buffer_size based on result set size
5. **Index**: Create indexes on frequently queried columns

Jet Best Practices
~~~~~~~~~~~~~~~~~~

1. **Name Jobs**: Always name jobs for easier identification
2. **Choose Guarantee**: Select appropriate processing guarantee for your use case
3. **Monitor Jobs**: Use listeners and Management Center to monitor job health
4. **Handle Failures**: Implement proper error handling and retry logic
5. **Resource Planning**: Consider cluster capacity when submitting jobs
