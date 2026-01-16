hazelcast.proxy.cardinality_estimator
=====================================

Distributed CardinalityEstimator (HyperLogLog) proxy.

A ``CardinalityEstimator`` uses the HyperLogLog algorithm to estimate the
number of distinct elements in a dataset with minimal memory usage. It's
ideal for counting unique visitors, distinct IPs, unique search queries,
and similar use cases where exact counts are not required.

Key characteristics:

- **Memory efficient**: Uses ~12KB regardless of dataset size
- **Approximate**: Typical error rate of ~2%
- **Mergeable**: Can combine estimates from multiple sources
- **No element retrieval**: Only tracks cardinality, not elements

Usage Examples
--------------

Basic Usage
~~~~~~~~~~~

.. code-block:: python

   from hazelcast import HazelcastClient

   client = HazelcastClient()
   
   # Get or create a CardinalityEstimator
   estimator = client.get_cardinality_estimator("unique-visitors")
   
   # Add elements
   estimator.add("user:123")
   estimator.add("user:456")
   estimator.add("user:123")  # Duplicate, won't increase count
   
   # Get estimated cardinality
   count = estimator.estimate()
   print(f"Estimated unique visitors: {count}")  # ~2
   
   client.shutdown()

Unique Visitor Tracking
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Track unique visitors per day
   from datetime import date

   today = date.today().isoformat()
   visitors = client.get_cardinality_estimator(f"visitors:{today}")
   
   def on_page_view(user_id):
       visitors.add(user_id)
   
   def get_unique_visitors():
       return visitors.estimate()
   
   # At end of day
   print(f"Unique visitors today: {get_unique_visitors()}")

Distinct IP Addresses
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Track distinct IPs accessing an API
   distinct_ips = client.get_cardinality_estimator("api:distinct-ips")
   
   def on_api_request(request):
       ip_address = request.remote_addr
       distinct_ips.add(ip_address)
       return handle_request(request)
   
   # Monitor distinct IPs
   print(f"Distinct IPs: {distinct_ips.estimate()}")

Search Query Analysis
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Estimate distinct search queries
   queries = client.get_cardinality_estimator("search:distinct-queries")
   
   def on_search(query):
       # Normalize query before adding
       normalized = query.lower().strip()
       queries.add(normalized)
       return perform_search(query)
   
   # Analytics
   print(f"Distinct search queries: {queries.estimate()}")

Product Views Estimation
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Estimate unique users who viewed each product
   def track_product_view(product_id, user_id):
       estimator = client.get_cardinality_estimator(f"product:{product_id}:viewers")
       estimator.add(user_id)
   
   def get_product_reach(product_id):
       estimator = client.get_cardinality_estimator(f"product:{product_id}:viewers")
       return estimator.estimate()
   
   # Track views
   track_product_view("PROD-123", "user:alice")
   track_product_view("PROD-123", "user:bob")
   track_product_view("PROD-123", "user:alice")  # Duplicate
   
   print(f"Product reach: {get_product_reach('PROD-123')}")  # ~2

Understanding Estimation Error
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # HyperLogLog has approximately 2% error rate
   
   estimator = client.get_cardinality_estimator("test")
   
   # Add 1 million distinct elements
   for i in range(1_000_000):
       estimator.add(f"element-{i}")
   
   estimate = estimator.estimate()
   # estimate will be approximately 1,000,000 +/- 20,000
   print(f"Estimated: {estimate}")
   print(f"Expected error range: {1_000_000 * 0.98} - {1_000_000 * 1.02}")

Configuration Options
---------------------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Option
     - Default
     - Description
   * - ``backup_count``
     - 1
     - Number of synchronous backups
   * - ``async_backup_count``
     - 0
     - Number of asynchronous backups
   * - ``merge_policy``
     - HyperLogLogMergePolicy
     - Policy for merging during split-brain recovery

Best Practices
--------------

1. **Accept Approximation**: CardinalityEstimator gives estimates, not
   exact counts. Design your application to tolerate ~2% error.

2. **Use for Large Datasets**: For small datasets (< 1000 elements),
   exact counting may be more appropriate.

3. **Hash Consistently**: Add the same representation of each element.
   ``"USER:123"`` and ``"user:123"`` are counted separately.

4. **No Element Retrieval**: You cannot list elements that were added.
   If you need element retrieval, use a Set instead.

5. **Combine Per-Period Estimators**: For time-windowed analysis,
   create separate estimators per time period.

When to Use CardinalityEstimator
--------------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Use Case
     - Recommendation
   * - Exact count required
     - Use ``Set`` or ``Counter``
   * - Large dataset (millions+)
     - Use ``CardinalityEstimator``
   * - Memory is limited
     - Use ``CardinalityEstimator``
   * - Small dataset (< 1000)
     - Use ``Set``
   * - Need element retrieval
     - Use ``Set``

API Reference
-------------

.. automodule:: hazelcast.proxy.cardinality_estimator
   :members:
   :undoc-members:
   :show-inheritance:
   :member-order: bysource

Classes
-------

CardinalityEstimator
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.cardinality_estimator.CardinalityEstimator
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

HyperLogLog
~~~~~~~~~~~

.. autoclass:: hazelcast.proxy.cardinality_estimator.HyperLogLog
   :members:
   :undoc-members:
   :show-inheritance:
