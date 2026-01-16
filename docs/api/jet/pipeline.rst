hazelcast.jet.pipeline
======================

Pipeline building API for Jet jobs.

A ``Pipeline`` represents a directed acyclic graph (DAG) of data processing
stages. You build pipelines by chaining sources, transforms, and sinks.

Usage Examples
--------------

Simple Pipeline
~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.jet.pipeline import Pipeline, Sources, Sinks

   pipeline = Pipeline()
   
   # Read -> Transform -> Write
   pipeline.read_from(Sources.map("input")) \
       .map(lambda entry: (entry.key, entry.value * 2)) \
       .write_to(Sinks.map("output"))

Stream Processing
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.jet.pipeline import Pipeline, Sources, Sinks, WindowType

   pipeline = Pipeline()
   
   # Stream from Kafka, window, aggregate, sink to Map
   pipeline.read_from(Sources.kafka(
           bootstrap_servers="localhost:9092",
           topic="events"
       )) \
       .with_timestamps(lambda e: e.timestamp) \
       .group_by(lambda e: e.user_id) \
       .window(WindowType.TUMBLING, duration=60000) \
       .aggregate(count()) \
       .write_to(Sinks.map("user-counts"))

Batch Processing
~~~~~~~~~~~~~~~~

.. code-block:: python

   # Read from Map, filter, transform, write to list
   pipeline = Pipeline()
   
   pipeline.read_from(Sources.map("users")) \
       .filter(lambda user: user.value["active"]) \
       .map(lambda user: user.value["email"]) \
       .write_to(Sinks.list("active-emails"))

Joining Streams
~~~~~~~~~~~~~~~

.. code-block:: python

   pipeline = Pipeline()
   
   orders = pipeline.read_from(Sources.map("orders"))
   customers = pipeline.read_from(Sources.map("customers"))
   
   orders.hash_join(
       customers,
       join_key=lambda order: order.value["customer_id"],
       project=lambda order, customer: {
           "order_id": order.key,
           "customer_name": customer.value["name"]
       }
   ).write_to(Sinks.map("enriched-orders"))

Best Practices
--------------

1. **Prefer Map Sources**: For in-cluster data, use ``Sources.map()``
   for optimal performance.

2. **Use Windowing**: For unbounded streams, use windowing to bound
   state and produce periodic results.

3. **Filter Early**: Apply filters as early as possible to reduce
   data flowing through the pipeline.

4. **Use Proper Key Functions**: Choose key functions that distribute
   data evenly across partitions.

API Reference
-------------

.. automodule:: hazelcast.jet.pipeline
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

Pipeline
~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.Pipeline
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

Stage
~~~~~

.. autoclass:: hazelcast.jet.pipeline.Stage
   :members:
   :undoc-members:
   :show-inheritance:

StageWithKey
~~~~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.StageWithKey
   :members:
   :undoc-members:
   :show-inheritance:

WindowedStage
~~~~~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.WindowedStage
   :members:
   :undoc-members:
   :show-inheritance:

Sources
-------

Source
~~~~~~

.. autoclass:: hazelcast.jet.pipeline.Source
   :members:
   :undoc-members:
   :show-inheritance:

BatchSource
~~~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.BatchSource
   :members:
   :undoc-members:
   :show-inheritance:

StreamSource
~~~~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.StreamSource
   :members:
   :undoc-members:
   :show-inheritance:

MapSource
~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.MapSource
   :members:
   :undoc-members:
   :show-inheritance:

ListSource
~~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.ListSource
   :members:
   :undoc-members:
   :show-inheritance:

FileSource
~~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.FileSource
   :members:
   :undoc-members:
   :show-inheritance:

SocketSource
~~~~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.SocketSource
   :members:
   :undoc-members:
   :show-inheritance:

KafkaSource
~~~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.KafkaSource
   :members:
   :undoc-members:
   :show-inheritance:

JdbcSource
~~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.JdbcSource
   :members:
   :undoc-members:
   :show-inheritance:

TestSource
~~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.TestSource
   :members:
   :undoc-members:
   :show-inheritance:

Sinks
-----

Sink
~~~~

.. autoclass:: hazelcast.jet.pipeline.Sink
   :members:
   :undoc-members:
   :show-inheritance:

MapSink
~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.MapSink
   :members:
   :undoc-members:
   :show-inheritance:

ListSink
~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.ListSink
   :members:
   :undoc-members:
   :show-inheritance:

LoggerSink
~~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.LoggerSink
   :members:
   :undoc-members:
   :show-inheritance:

FileSink
~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.FileSink
   :members:
   :undoc-members:
   :show-inheritance:

SocketSink
~~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.SocketSink
   :members:
   :undoc-members:
   :show-inheritance:

KafkaSink
~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.KafkaSink
   :members:
   :undoc-members:
   :show-inheritance:

JdbcSink
~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.JdbcSink
   :members:
   :undoc-members:
   :show-inheritance:

NoopSink
~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.NoopSink
   :members:
   :undoc-members:
   :show-inheritance:

Aggregation
-----------

AggregateOperation
~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.AggregateOperation
   :members:
   :undoc-members:
   :show-inheritance:

WindowDefinition
~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.WindowDefinition
   :members:
   :undoc-members:
   :show-inheritance:

Enumerations
------------

WindowType
~~~~~~~~~~

.. autoclass:: hazelcast.jet.pipeline.WindowType
   :members:
   :undoc-members:
