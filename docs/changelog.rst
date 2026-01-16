Changelog
=========

Version 0.1.0 (Unreleased)
--------------------------

Initial release of the Hazelcast Python Client.

Features
~~~~~~~~

* Client connection and lifecycle management
* Distributed data structures:

  - IMap with entry listeners, entry processors, and near cache
  - IQueue with blocking operations
  - ISet and IList
  - MultiMap
  - ReplicatedMap
  - Ringbuffer
  - Topic and ReliableTopic
  - PNCounter
  - CardinalityEstimator

* CP Subsystem support:

  - AtomicLong
  - AtomicReference
  - FencedLock
  - Semaphore
  - CountDownLatch

* SQL query support
* Jet stream processing
* Transaction support (ONE_PHASE and TWO_PHASE)
* Distributed executor service
* YAML configuration loading
* Comprehensive event listeners
