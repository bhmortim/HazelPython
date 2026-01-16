CP Subsystem API
================

The CP Subsystem provides strongly consistent distributed primitives
using the Raft consensus algorithm.

AtomicLong
----------

.. autoclass:: hazelcast.cp.atomic.AtomicLong
   :members:
   :undoc-members:
   :show-inheritance:

AtomicReference
---------------

.. autoclass:: hazelcast.cp.atomic.AtomicReference
   :members:
   :undoc-members:
   :show-inheritance:

FencedLock
----------

.. autoclass:: hazelcast.cp.sync.FencedLock
   :members:
   :undoc-members:
   :show-inheritance:

Semaphore
---------

.. autoclass:: hazelcast.cp.sync.Semaphore
   :members:
   :undoc-members:
   :show-inheritance:

CountDownLatch
--------------

.. autoclass:: hazelcast.cp.sync.CountDownLatch
   :members:
   :undoc-members:
   :show-inheritance:

CPMap
-----

.. autoclass:: hazelcast.cp.cp_map.CPMap
   :members:
   :undoc-members:
   :show-inheritance:

Usage Examples
--------------

AtomicLong Example
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   counter = client.get_atomic_long("my-counter")
   counter.set(0)
   
   # Atomic operations
   counter.increment_and_get()
   counter.add_and_get(10)
   
   # Compare and set
   counter.compare_and_set(11, 100)

FencedLock Example
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   lock = client.get_fenced_lock("my-lock")
   
   # Using context manager
   with lock as fence:
       # Critical section
       print(f"Lock acquired with fence: {fence}")

Semaphore Example
~~~~~~~~~~~~~~~~~

.. code-block:: python

   semaphore = client.get_semaphore("my-semaphore")
   semaphore.init(10)
   
   # Acquire and release
   semaphore.acquire()
   try:
       # Do work
       pass
   finally:
       semaphore.release()
