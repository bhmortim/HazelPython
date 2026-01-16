Jet Service
===========

Jet is Hazelcast's distributed stream and batch processing engine.

Pipeline Building
-----------------

Create processing pipelines with sources, transformations, and sinks:

.. code-block:: python

   from hazelcast.jet import Pipeline, JobConfig

   jet = client.get_jet()

   # Create pipeline
   pipeline = Pipeline.create()

   # Define source
   source = Pipeline.from_list("numbers", [1, 2, 3, 4, 5])

   # Define sink
   sink = Pipeline.to_list("results")

   # Build pipeline
   pipeline.read_from(source) \
       .map(lambda x: x * 2) \
       .filter(lambda x: x > 4) \
       .write_to(sink)

   # Submit job
   config = JobConfig()
   config.name = "double-and-filter"
   job = jet.submit(pipeline, config)


Sources
-------

Built-in Sources
~~~~~~~~~~~~~~~~

.. code-block:: python

   # From a list (batch)
   source = Pipeline.from_list("my-list", [1, 2, 3, 4, 5])

   # From an IMap
   source = Pipeline.from_map("my-map")


Sinks
-----

Built-in Sinks
~~~~~~~~~~~~~~

.. code-block:: python

   # To a list
   sink = Pipeline.to_list("results")

   # To an IMap
   sink = Pipeline.to_map("output-map")


Transformations
---------------

Map
~~~

Transform each element:

.. code-block:: python

   pipeline.read_from(source) \
       .map(lambda x: x * 2) \
       .write_to(sink)

Filter
~~~~~~

Keep elements matching a predicate:

.. code-block:: python

   pipeline.read_from(source) \
       .filter(lambda x: x > 10) \
       .write_to(sink)

FlatMap
~~~~~~~

Transform each element into zero or more elements:

.. code-block:: python

   pipeline.read_from(source) \
       .flat_map(lambda x: [x, x * 2]) \
       .write_to(sink)


Job Configuration
-----------------

.. code-block:: python

   from hazelcast.jet import JobConfig

   config = JobConfig()
   config.name = "my-job"
   config.auto_scaling_enabled = True
   config.split_brain_protection_enabled = False
   config.processing_guarantee = "EXACTLY_ONCE"  # or "AT_LEAST_ONCE"
   config.snapshot_interval_millis = 10000


Job Management
--------------

Submitting Jobs
~~~~~~~~~~~~~~~

.. code-block:: python

   jet = client.get_jet()

   # Submit and get job handle
   job = jet.submit(pipeline, config)
   print(f"Job ID: {job.id}")
   print(f"Job name: {job.name}")
   print(f"Status: {job.status}")

Job Status
~~~~~~~~~~

.. code-block:: python

   from hazelcast.jet import JobStatus

   job = jet.submit(pipeline, config)

   # Check status
   status = job.status  # JobStatus.RUNNING, COMPLETED, FAILED, etc.

   # Wait for completion
   job.join()

Job Lifecycle
~~~~~~~~~~~~~

.. code-block:: python

   # Suspend a running job
   job.suspend()

   # Resume a suspended job
   job.resume()

   # Restart a job
   job.restart()

   # Cancel a job
   job.cancel()


Retrieving Jobs
---------------

.. code-block:: python

   jet = client.get_jet()

   # Get all jobs
   jobs = jet.get_jobs()
   for job in jobs:
       print(f"{job.name}: {job.status}")

   # Get job by ID
   job = jet.get_job(job_id)

   # Get job by name
   job = jet.get_job_by_name("my-job")


Submit If Absent
----------------

Submit a job only if one with the same name doesn't exist:

.. code-block:: python

   jet = client.get_jet()

   # Returns existing job if name already exists
   job = jet.new_job_if_absent(pipeline, config)


Example: Word Count
-------------------

.. code-block:: python

   from hazelcast.jet import Pipeline, JobConfig

   jet = client.get_jet()

   # Sample text data
   lines = [
       "hello world",
       "hello hazelcast",
       "hazelcast jet",
       "distributed computing",
   ]

   pipeline = Pipeline.create()
   source = Pipeline.from_list("lines", lines)
   sink = Pipeline.to_map("word-counts")

   pipeline.read_from(source) \
       .flat_map(lambda line: line.split()) \
       .map(lambda word: (word, 1)) \
       .group_by(lambda x: x[0]) \
       .aggregate(lambda group: (group[0], sum(x[1] for x in group))) \
       .write_to(sink)

   config = JobConfig()
   config.name = "word-count"
   job = jet.submit(pipeline, config)

   job.join()

   # Read results
   word_counts = client.get_map("word-counts")
   for word, count in word_counts.entry_set():
       print(f"{word}: {count}")


Best Practices
--------------

1. **Name your jobs**: Makes monitoring and management easier.

2. **Use appropriate processing guarantees**: EXACTLY_ONCE for critical
   data, AT_LEAST_ONCE for better performance.

3. **Monitor job status**: Check job health and handle failures.

4. **Use snapshots**: Enable snapshots for fault tolerance in streaming jobs.

5. **Test locally first**: Debug pipelines with small datasets before
   deploying to the cluster.
