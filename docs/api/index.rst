API Reference
=============

This section provides detailed API documentation for all public modules
in the HazelPython client library.

.. toctree::
   :maxdepth: 2
   :caption: Modules:

   client
   config
   proxy/index
   cp/index
   sql/index
   jet/index
   transactions
   serialization/index

Module Overview
---------------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Module
     - Description
   * - :doc:`client`
     - Main client entry point and connection management
   * - :doc:`config`
     - Client configuration classes
   * - :doc:`proxy/index`
     - Distributed data structure proxies (Map, Queue, Set, etc.)
   * - :doc:`cp/index`
     - CP Subsystem primitives (AtomicLong, FencedLock, Semaphore)
   * - :doc:`sql/index`
     - SQL query execution and result handling
   * - :doc:`jet/index`
     - Jet stream processing pipelines and jobs
   * - :doc:`transactions`
     - Transaction API and transactional data structures
   * - :doc:`serialization/index`
     - Serialization services and configuration
