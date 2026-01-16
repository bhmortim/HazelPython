hazelcast.jet.job
=================

Jet job management and configuration.

A ``Job`` represents a running or completed pipeline execution. Use the
``Job`` object to monitor status, retrieve metrics, and control job lifecycle.

Usage Examples
--------------

Job Lifecycle
~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.jet.job import JobStatus

   # Submit a job
   job = jet.submit(pipeline)
   print(f"Job ID: {job.id}")
   print(f"Job Name: {job.name}")
   
   # Check status
   status = job.get_status()
   print(f"Status: {status}")
   
   # Wait for completion
   job.join()  # Blocks until job completes or fails
   
   # Or wait with timeout
   try:
       job.join(timeout=60.0)
   except TimeoutError:
       print("Job did not complete in time")

Job Control
~~~~~~~~~~~

.. code-block:: python

   # Suspend a running job
   job.suspend()
   
   # Resume a suspended job
   job.resume()
   
   # Cancel a job
   job.cancel()
   
   # Restart a job
   job.restart()
   
   # Export snapshot
   job.export_snapshot("my-snapshot")

Job Metrics
~~~~~~~~~~~

.. code-block:: python

   # Get job metrics
   metrics = job.get_metrics()
   
   print(f"Received count: {metrics.received_count}")
   print(f"Emitted count: {metrics.emitted_count}")
   print(f"Processing time: {metrics.total_processing_time_ms}ms")
   
   # Get submission time
   submitted = job.get_submission_time()
   print(f"Submitted at: {submitted}")

Job Configuration
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.jet.job import JobConfig, ProcessingGuarantee

   config = JobConfig(
       name="my-streaming-job",
       processing_guarantee=ProcessingGuarantee.EXACTLY_ONCE,
       snapshot_interval_ms=10000,
       auto_scaling=True,
       split_brain_protection=True,
       max_processor_accumulated_records=100000,
   )
   
   job = jet.submit(pipeline, config)

Processing Guarantees
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.jet.job import ProcessingGuarantee

   # No guarantee (fastest, may lose or duplicate data)
   config = JobConfig(
       processing_guarantee=ProcessingGuarantee.NONE
   )
   
   # At-least-once (no data loss, may have duplicates)
   config = JobConfig(
       processing_guarantee=ProcessingGuarantee.AT_LEAST_ONCE,
       snapshot_interval_ms=5000
   )
   
   # Exactly-once (no loss, no duplicates, requires idempotent sinks)
   config = JobConfig(
       processing_guarantee=ProcessingGuarantee.EXACTLY_ONCE,
       snapshot_interval_ms=10000
   )

Job Status Monitoring
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.jet.job import JobStatus

   job = jet.submit(pipeline)
   
   while True:
       status = job.get_status()
       
       if status == JobStatus.RUNNING:
           metrics = job.get_metrics()
           print(f"Processed: {metrics.received_count}")
       elif status == JobStatus.COMPLETED:
           print("Job completed successfully")
           break
       elif status == JobStatus.FAILED:
           print(f"Job failed: {job.get_suspension_cause()}")
           break
       elif status == JobStatus.SUSPENDED:
           print("Job is suspended")
       
       time.sleep(5)

Job Status Values
-----------------

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Status
     - Description
   * - ``NOT_RUNNING``
     - Job is not yet started
   * - ``STARTING``
     - Job is being initialized
   * - ``RUNNING``
     - Job is actively processing data
   * - ``SUSPENDED``
     - Job is paused but retains state
   * - ``COMPLETING``
     - Job is completing (batch jobs)
   * - ``COMPLETED``
     - Job finished successfully
   * - ``FAILED``
     - Job failed with an error
   * - ``SUSPENDED_EXPORTING_SNAPSHOT``
     - Job is suspended while exporting snapshot

Best Practices
--------------

1. **Name Your Jobs**: Always provide a meaningful name for easier
   identification and debugging.

2. **Choose Processing Guarantee**: Select the appropriate guarantee
   based on your data accuracy requirements.

3. **Set Snapshot Interval**: For stateful jobs, configure snapshot
   interval based on acceptable data loss window.

4. **Monitor Metrics**: Regularly check job metrics to identify
   bottlenecks and performance issues.

5. **Handle Failures**: Implement monitoring for job failures and
   have a recovery strategy ready.

API Reference
-------------

.. automodule:: hazelcast.jet.job
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

Job
~~~

.. autoclass:: hazelcast.jet.job.Job
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

JobConfig
~~~~~~~~~

.. autoclass:: hazelcast.jet.job.JobConfig
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

JobMetrics
~~~~~~~~~~

.. autoclass:: hazelcast.jet.job.JobMetrics
   :members:
   :undoc-members:
   :show-inheritance:

Enumerations
------------

JobStatus
~~~~~~~~~

.. autoclass:: hazelcast.jet.job.JobStatus
   :members:
   :undoc-members:

ProcessingGuarantee
~~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.jet.job.ProcessingGuarantee
   :members:
   :undoc-members:
