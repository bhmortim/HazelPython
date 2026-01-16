Predicates API
==============

.. automodule:: hazelcast.predicate
   :members:
   :undoc-members:
   :show-inheritance:

Predicate Classes
-----------------

Base Class
~~~~~~~~~~

.. autoclass:: hazelcast.predicate.Predicate
   :members:
   :undoc-members:

Comparison Predicates
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.predicate.EqualPredicate
   :members:

.. autoclass:: hazelcast.predicate.NotEqualPredicate
   :members:

.. autoclass:: hazelcast.predicate.GreaterThanPredicate
   :members:

.. autoclass:: hazelcast.predicate.GreaterThanOrEqualPredicate
   :members:

.. autoclass:: hazelcast.predicate.LessThanPredicate
   :members:

.. autoclass:: hazelcast.predicate.LessThanOrEqualPredicate
   :members:

.. autoclass:: hazelcast.predicate.BetweenPredicate
   :members:

.. autoclass:: hazelcast.predicate.InPredicate
   :members:

Pattern Predicates
~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.predicate.LikePredicate
   :members:

.. autoclass:: hazelcast.predicate.ILikePredicate
   :members:

.. autoclass:: hazelcast.predicate.RegexPredicate
   :members:

Logical Predicates
~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.predicate.AndPredicate
   :members:

.. autoclass:: hazelcast.predicate.OrPredicate
   :members:

.. autoclass:: hazelcast.predicate.NotPredicate
   :members:

Special Predicates
~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.predicate.SqlPredicate
   :members:

.. autoclass:: hazelcast.predicate.TruePredicate
   :members:

.. autoclass:: hazelcast.predicate.FalsePredicate
   :members:

.. autoclass:: hazelcast.predicate.PagingPredicate
   :members:

Factory Functions
-----------------

.. autofunction:: hazelcast.predicate.attr
.. autofunction:: hazelcast.predicate.sql
.. autofunction:: hazelcast.predicate.and_
.. autofunction:: hazelcast.predicate.or_
.. autofunction:: hazelcast.predicate.not_
.. autofunction:: hazelcast.predicate.true
.. autofunction:: hazelcast.predicate.false
.. autofunction:: hazelcast.predicate.paging
