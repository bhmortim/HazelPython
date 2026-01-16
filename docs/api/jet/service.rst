hazelcast.jet.service
=====================

Jet service for pipeline submission and job management.

The ``JetService`` is the entry point for Hazelcast Jet stream and batch
processing. It provides methods to submit pipelines, manage running jobs,
and query job status.

Usage Examples
--------------

Accessing Jet Service
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Access Jet service
   jet = client.jet
   
   # Or explicitly
   jet = client.get_jet_service()

Submitting a Job
~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.jet.pipeline import Pipeline
   from hazelcast.jet.job import JobConfig

   # Create a pipeline
   pipeline = Pipeline()
   pipeline.read_from(Sources.map("input-map")) \
       .filter(lambda x: x.value > 100) \
       .write_to(Sinks.map("output-map"))
   
   # Submit with default config
   job = jet.submit(pipeline)
   print(f"Job ID: {job.id}")
   
   # Submit with custom config
   config = JobConfig(
       name="my-job",
       auto_scaling=True,
       split_brain_protection=True
   )
   job = jet.submit(pipeline, config)

Listing Jobs
~~~~~~~~~~~~

.. code-block:: python

   # List all jobs
   jobs = jet.get_jobs()
   for job in jobs:
       print(f"Job: {job.name}, Status: {job.status}")
   
   # Get job by ID
   job = jet.get_job(job_id)
   
   # Get job by name
   job = jet.get_job_by_name("my-job")

Job Status Listener
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.jet.service import JobStateListener

   class MyListener(JobStateListener):
       def on_job_status_change(self, job_id, old_status, new_status):
           print(f"Job {job_id}: {old_status} -> {new_status}")
           if new_status == JobStatus.FAILED:
               handle_job_failure(job_id)
   
   # Add listener
   reg_id = jet.add_job_status_listener(MyListener())
   
   # Remove listener
   jet.remove_job_status_listener(reg_id)

Job Summaries
~~~~~~~~~~~~~

.. code-block:: python

   # Get summary of all jobs
   summaries = jet.get_job_summaries()
   for summary in summaries:
       print(f"Job: {summary.name}")
       print(f"  Status: {summary.status}")
       print(f"  Submitted: {summary.submission_time}")
       print(f"  Completion: {summary.completion_time}")

Best Practices
--------------

1. **Name Your Jobs**: Always set a meaningful job name for easier
   monitoring and management.

2. **Monitor Job Status**: Use ``JobStateListener`` to react to
   job state changes in real-time.

3. **Handle Failures**: Implement proper error handling and consider
   retry logic for failed jobs.

4. **Resource Management**: Monitor cluster resources when running
   multiple concurrent jobs.

API Reference
-------------

.. automodule:: hazelcast.jet.service
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

JetService
~~~~~~~~~~

.. autoclass:: hazelcast.jet.service.JetService
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

JobStateListener
~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.jet.service.JobStateListener
   :members:
   :undoc-members:
   :show-inheritance:
