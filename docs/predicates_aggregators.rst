Predicates and Aggregators
==========================

Predicates filter distributed data, while aggregators compute values
across filtered entries. Both execute on the cluster for efficiency.

Predicates
----------

Comparison Predicates
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.predicate import attr

   users = client.get_map("users")
   
   # Equal
   admins = users.values(attr("role").equal("admin"))
   
   # Not equal
   non_admins = users.values(attr("role").not_equal("admin"))
   
   # Greater than
   seniors = users.values(attr("age").greater_than(65))
   
   # Greater than or equal
   adults = users.values(attr("age").greater_than_or_equal(18))
   
   # Less than
   young = users.values(attr("age").less_than(30))
   
   # Less than or equal
   under_30 = users.values(attr("age").less_than_or_equal(30))
   
   # Between (inclusive)
   middle_aged = users.values(attr("age").between(30, 50))

Collection Predicates
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # In a set of values
   managers = users.values(
       attr("role").is_in("manager", "director", "vp")
   )

Pattern Matching
~~~~~~~~~~~~~~~~

.. code-block:: python

   # LIKE (SQL wildcards: % and _)
   smiths = users.values(attr("name").like("Smith%"))
   
   # ILIKE (case-insensitive LIKE)
   smiths = users.values(attr("name").ilike("smith%"))
   
   # Regular expression
   valid_emails = users.values(
       attr("email").regex(r"^[\w.]+@[\w.]+\.\w+$")
   )

Logical Predicates
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.predicate import and_, or_, not_, attr

   # AND
   young_admins = users.values(
       and_(
           attr("age").less_than(30),
           attr("role").equal("admin")
       )
   )
   
   # OR
   special_users = users.values(
       or_(
           attr("role").equal("admin"),
           attr("role").equal("moderator")
       )
   )
   
   # NOT
   non_admins = users.values(
       not_(attr("role").equal("admin"))
   )
   
   # Complex combinations
   result = users.values(
       and_(
           attr("status").equal("active"),
           or_(
               attr("role").equal("admin"),
               and_(
                   attr("department").equal("IT"),
                   attr("level").greater_than(3)
               )
           )
       )
   )

SQL Predicate
~~~~~~~~~~~~~

Use SQL-like syntax:

.. code-block:: python

   from hazelcast.predicate import sql

   # Simple SQL predicate
   adults = users.values(sql("age >= 18"))
   
   # Complex SQL predicate
   result = users.values(
       sql("status = 'active' AND (role = 'admin' OR department = 'IT')")
   )

Special Predicates
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.predicate import true, false, InstanceOfPredicate

   # Always true/false
   all_users = users.values(true())
   no_users = users.values(false())
   
   # Instance of type
   employees = users.values(InstanceOfPredicate("com.example.Employee"))

Paging Predicate
~~~~~~~~~~~~~~~~

For large result sets:

.. code-block:: python

   from hazelcast.predicate import paging, attr

   # Create paging predicate
   paging_pred = paging(
       predicate=attr("status").equal("active"),
       page_size=100,
   )
   
   # Get first page
   page1 = users.values(paging_pred)
   
   # Navigate pages
   paging_pred.next_page()
   page2 = users.values(paging_pred)
   
   paging_pred.previous_page()
   back_to_page1 = users.values(paging_pred)
   
   # Jump to specific page
   paging_pred.page = 5
   page5 = users.values(paging_pred)
   
   # Reset to first page
   paging_pred.reset()

Aggregators
-----------

Basic Aggregators
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from hazelcast.aggregator import (
       count, sum_, average, min_, max_, distinct
   )

   employees = client.get_map("employees")
   
   # Count entries
   total = employees.aggregate(count())
   
   # Count with attribute (non-null values)
   with_salary = employees.aggregate(count("salary"))
   
   # Sum
   total_salary = employees.aggregate(sum_("salary"))
   
   # Average
   avg_salary = employees.aggregate(average("salary"))
   
   # Minimum
   min_salary = employees.aggregate(min_("salary"))
   
   # Maximum
   max_salary = employees.aggregate(max_("salary"))
   
   # Distinct values
   departments = employees.aggregate(distinct("department"))

Type-Specific Aggregators
~~~~~~~~~~~~~~~~~~~~~~~~~

For better precision and performance:

.. code-block:: python

   from hazelcast.aggregator import (
       integer_sum, integer_average,
       long_sum, long_average,
       double_sum, double_average,
   )

   # Integer aggregators
   total_items = inventory.aggregate(integer_sum("quantity"))
   avg_items = inventory.aggregate(integer_average("quantity"))
   
   # Long aggregators
   total_bytes = files.aggregate(long_sum("size"))
   
   # Double aggregators
   total_price = orders.aggregate(double_sum("total"))
   avg_price = orders.aggregate(double_average("total"))

Aggregating with Predicates
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Combine aggregation with filtering:

.. code-block:: python

   from hazelcast.aggregator import average
   from hazelcast.predicate import attr

   # Average salary in Engineering department
   eng_avg_salary = employees.aggregate(
       average("salary"),
       predicate=attr("department").equal("Engineering")
   )
   
   # Count active users
   from hazelcast.aggregator import count
   active_count = users.aggregate(
       count(),
       predicate=attr("status").equal("active")
   )

Projections
-----------

Extract specific attributes from entries:

.. code-block:: python

   from hazelcast.projection import (
       single_attribute,
       multi_attribute,
       identity,
   )
   from hazelcast.predicate import attr

   users = client.get_map("users")
   
   # Project single attribute
   names = users.project(single_attribute("name"))
   
   # Project multiple attributes
   contacts = users.project(
       multi_attribute("name", "email", "phone")
   )
   
   # Project with predicate
   admin_emails = users.project(
       single_attribute("email"),
       predicate=attr("role").equal("admin")
   )
   
   # Identity projection (full objects)
   all_users = users.project(identity())

Predicate Builder
-----------------

Fluent API for building predicates:

.. code-block:: python

   from hazelcast.predicate import attr, and_, or_

   # Using PredicateBuilder
   age = attr("age")
   role = attr("role")
   
   # Build complex predicates fluently
   predicate = and_(
       age.between(25, 45),
       or_(
           role.equal("engineer"),
           role.equal("manager")
       )
   )
   
   results = users.values(predicate)

Best Practices
--------------

1. **Use Indexes**
   
   Create indexes on frequently queried attributes for better performance.

2. **Prefer Specific Aggregators**
   
   Use type-specific aggregators (integer_sum, etc.) for better precision.

3. **Limit Result Sets**
   
   Use paging predicates for large result sets to avoid memory issues.

4. **Combine Operations**
   
   Use aggregation with predicates to filter before aggregating.

5. **Project Only Needed Data**
   
   Use projections to retrieve only required attributes.
