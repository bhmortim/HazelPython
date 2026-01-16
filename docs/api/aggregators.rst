Aggregators API
===============

.. automodule:: hazelcast.aggregator
   :members:
   :undoc-members:
   :show-inheritance:

Aggregator Classes
------------------

Base Class
~~~~~~~~~~

.. autoclass:: hazelcast.aggregator.Aggregator
   :members:

Built-in Aggregators
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.aggregator.CountAggregator
   :members:

.. autoclass:: hazelcast.aggregator.SumAggregator
   :members:

.. autoclass:: hazelcast.aggregator.AverageAggregator
   :members:

.. autoclass:: hazelcast.aggregator.MinAggregator
   :members:

.. autoclass:: hazelcast.aggregator.MaxAggregator
   :members:

.. autoclass:: hazelcast.aggregator.DistinctValuesAggregator
   :members:

Type-Specific Aggregators
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.aggregator.IntegerSumAggregator
   :members:

.. autoclass:: hazelcast.aggregator.IntegerAverageAggregator
   :members:

.. autoclass:: hazelcast.aggregator.LongSumAggregator
   :members:

.. autoclass:: hazelcast.aggregator.LongAverageAggregator
   :members:

.. autoclass:: hazelcast.aggregator.DoubleSumAggregator
   :members:

.. autoclass:: hazelcast.aggregator.DoubleAverageAggregator
   :members:

Factory Functions
-----------------

.. autofunction:: hazelcast.aggregator.count
.. autofunction:: hazelcast.aggregator.sum_
.. autofunction:: hazelcast.aggregator.average
.. autofunction:: hazelcast.aggregator.min_
.. autofunction:: hazelcast.aggregator.max_
.. autofunction:: hazelcast.aggregator.distinct
.. autofunction:: hazelcast.aggregator.integer_sum
.. autofunction:: hazelcast.aggregator.integer_average
.. autofunction:: hazelcast.aggregator.long_sum
.. autofunction:: hazelcast.aggregator.long_average
.. autofunction:: hazelcast.aggregator.double_sum
.. autofunction:: hazelcast.aggregator.double_average

Projections
-----------

.. automodule:: hazelcast.projection
   :members:
   :undoc-members:

.. autoclass:: hazelcast.projection.Projection
   :members:

.. autoclass:: hazelcast.projection.SingleAttributeProjection
   :members:

.. autoclass:: hazelcast.projection.MultiAttributeProjection
   :members:

.. autoclass:: hazelcast.projection.IdentityProjection
   :members:

.. autofunction:: hazelcast.projection.single_attribute
.. autofunction:: hazelcast.projection.multi_attribute
.. autofunction:: hazelcast.projection.identity
