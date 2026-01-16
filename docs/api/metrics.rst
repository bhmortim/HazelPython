Metrics
=======

.. module:: hazelcast.metrics
   :synopsis: Client metrics collection and reporting.

This module provides metrics collection infrastructure for monitoring
Hazelcast client performance and health.

Metric Types
------------

.. autoclass:: GaugeMetric
   :members:
   :special-members: __init__

.. autoclass:: CounterMetric
   :members:
   :special-members: __init__

.. autoclass:: TimerMetric
   :members:
   :special-members: __init__

Specialized Metrics
-------------------

.. autoclass:: ConnectionMetrics
   :members:

.. autoclass:: InvocationMetrics
   :members:

.. autoclass:: NearCacheMetrics
   :members:

Metrics Registry
----------------

.. autoclass:: MetricsRegistry
   :members:
   :special-members: __init__

Example Usage
-------------

Basic metrics usage::

    from hazelcast.metrics import MetricsRegistry

    registry = MetricsRegistry()

    # Record connection events
    registry.record_connection_opened()
    registry.record_connection_attempt(success=True)

    # Record invocations
    registry.record_invocation_start()
    registry.record_invocation_end(response_time=15.5, success=True)

    # Get all metrics as dictionary
    metrics = registry.to_dict()
    print(f"Active connections: {metrics['connections']['active']}")

Custom gauges and counters::

    registry = MetricsRegistry()

    # Register a gauge
    gauge = registry.register_gauge(
        "memory.used",
        lambda: get_memory_usage(),
        unit="bytes"
    )

    # Register and use a counter
    counter = registry.register_counter("requests.total")
    counter.increment()
