Executor Service
================

.. module:: hazelcast.service.executor
   :synopsis: Executor service for distributed task execution.

This module provides distributed task execution capabilities.

Callback Interfaces
-------------------

.. autoclass:: ExecutorCallback
   :members:

.. autoclass:: FunctionExecutorCallback
   :members:
   :special-members: __init__

Task Class
----------

.. autoclass:: ExecutorTask
   :members:

Executor Service
----------------

.. autoclass:: ExecutorService
   :members:
   :special-members: __init__

Example Usage
-------------

Submitting tasks to the cluster::

    from hazelcast.service.executor import ExecutorService

    executor = ExecutorService("my-executor")

    # Submit to any member
    future = executor.submit(lambda: compute_result())
    result = future.result()

    # Submit to specific member
    future = executor.submit_to_member(task, member_uuid)

    # Submit to key owner (data locality)
    future = executor.submit_to_key_owner(task, "user:123")

Using callbacks::

    from hazelcast.service.executor import FunctionExecutorCallback

    callback = FunctionExecutorCallback(
        on_success=lambda r: print(f"Result: {r}"),
        on_error=lambda e: print(f"Error: {e}")
    )

    executor.submit(task, callback=callback)
