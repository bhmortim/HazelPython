Jet Stream Processing
=====================

Hazelcast Jet provides distributed stream and batch processing
capabilities within the Hazelcast cluster.

Getting the Jet Service
-----------------------

.. code-block:: python

    jet = client.get_jet()

Building Pipelines
------------------

Create data processing pipelines using the Pipeline API:

.. code-block:: python

    from hazelcast.jet.pipeline import Pipeline

    # Create pipeline
    pipeline = Pipeline.create()

    # Define source, transformations, and sink
    source = Pipeline.from_list("numbers", [1, 2, 3, 4, 5])
    sink = Pipeline.to_list("results")

    pipeline.read_from(source) \
        .map(lambda x: x * 2) \
        .filter(lambda x: x > 4) \
        .write_to(sink)

Sources
-------

Batch Sources
~~~~~~~~~~~~~

.. code-block:: python

    # From list
    source = Pipeline.from_list("data", items)

    # From IMap
    source = Pipeline.from_map("my-map")

    # From IList
    from hazelcast.jet.pipeline import ListSource
    source = ListSource("my-list")

Stream Sources
~~~~~~~~~~~~~~

.. code-block:: python

    from hazelcast.jet.pipeline import StreamSource

    source = StreamSource("events")

Transformations
---------------

Map
~~~

Transform each element:

.. code-block:: python

    pipeline.read_from(source) \
        .map(lambda x: x.upper()) \
        .write_to(sink)

Filter
~~~~~~

Keep elements matching predicate:

.. code-block:: python

    pipeline.read_from(source) \
        .filter(lambda x: x > 0) \
        .write_to(sink)

FlatMap
~~~~~~~

Transform each element to multiple elements:

.. code-block:: python

    pipeline.read_from(source) \
        .flat_map(lambda x: x.split()) \
        .write_to(sink)

Group By
~~~~~~~~

Group elements by key:

.. code-block:: python

    pipeline.read_from(source) \
        .group_by(lambda x: x["category"]) \
        .write_to(sink)

Aggregate
~~~~~~~~~

Aggregate elements:

.. code-block:: python

    pipeline.read_from(source) \
        .aggregate(
            accumulator=lambda acc, x: acc + x,
            initial=0
        ) \
        .write_to(sink)

Distinct
~~~~~~~~

Remove duplicates:

.. code-block:: python

    pipeline.read_from(source) \
        .distinct() \
        .write_to(sink)

Sort
~~~~

Sort elements:

.. code-block:: python

    pipeline.read_from(source) \
        .sort(key_fn=lambda x: x["timestamp"]) \
        .write_to(sink)

Sinks
-----

.. code-block:: python

    # To IList
    sink = Pipeline.to_list("results")

    # To IMap
    sink = Pipeline.to_map("output-map")

    # To logger
    sink = Pipeline.to_logger()

Job Configuration
-----------------

Configure job execution:

.. code-block:: python

    from hazelcast.jet.job import JobConfig, ProcessingGuarantee

    config = JobConfig(
        name="my-processing-job",
        auto_scaling_enabled=True,
        processing_guarantee=ProcessingGuarantee.EXACTLY_ONCE,
        snapshot_interval_millis=10000,
    )

    job = jet.submit(pipeline, config)

Submitting Jobs
---------------

.. code-block:: python

    # Submit and get job handle
    job = jet.submit(pipeline)
    print(f"Job ID: {job.id}")
    print(f"Status: {job.status}")

    # Submit only if name doesn't exist
    config = JobConfig(name="unique-job")
    job = jet.new_job_if_absent(pipeline, config)

Job Lifecycle
-------------

Monitoring
~~~~~~~~~~

.. code-block:: python

    from hazelcast.jet.job import JobStatus

    job = jet.submit(pipeline)

    # Check status
    print(f"Status: {job.status}")

    # Check if terminal
    if job.is_terminal():
        print("Job finished")

    # Get metrics
    metrics = job.get_metrics()

Controlling
~~~~~~~~~~~

.. code-block:: python

    # Suspend running job
    job.suspend()

    # Resume suspended job
    job.resume()

    # Restart job
    job.restart()

    # Cancel job
    job.cancel()

Waiting for Completion
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Wait indefinitely
    job.join()

    # Wait with timeout
    try:
        job.join(timeout=60.0)
    except TimeoutException:
        print("Job did not complete in time")

Retrieving Jobs
---------------

.. code-block:: python

    # Get by ID
    job = jet.get_job(job_id)

    # Get by name
    job = jet.get_job_by_name("my-job")

    # Get all jobs
    jobs = jet.get_jobs()
    for job in jobs:
        print(f"{job.name}: {job.status}")

Snapshots
---------

Export job state for recovery:

.. code-block:: python

    # Export snapshot
    job.export_snapshot("checkpoint-1")

    # Resume from snapshot
    config = JobConfig(name="recovered-job")
    config.initial_snapshot_name = "checkpoint-1"
    job = jet.submit(pipeline, config)
