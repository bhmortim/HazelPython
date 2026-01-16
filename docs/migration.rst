Migration from Java Client
==========================

This guide helps Java developers transition to the Hazelcast Python client.

Configuration
-------------

**Java:**

.. code-block:: java

   ClientConfig config = new ClientConfig();
   config.setClusterName("production");
   config.getNetworkConfig().addAddress("server1:5701", "server2:5701");
   config.getNetworkConfig().setSmartRouting(true);
   
   HazelcastInstance client = HazelcastClient.newHazelcastClient(config);

**Python:**

.. code-block:: python

   from hazelcast import ClientConfig, HazelcastClient

   config = ClientConfig()
   config.cluster_name = "production"
   config.cluster_members = ["server1:5701", "server2:5701"]
   config.smart_routing = True
   
   client = HazelcastClient(config)
   client.start()

Context Manager (Python-specific)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Python supports context managers for automatic cleanup:

.. code-block:: python

   with HazelcastClient(config) as client:
       # Client automatically shuts down on exit
       my_map = client.get_map("my-map")

Map Operations
--------------

**Java:**

.. code-block:: java

   IMap<String, String> map = client.getMap("my-map");
   
   // Basic operations
   map.put("key", "value");
   String value = map.get("key");
   map.putIfAbsent("key2", "value2");
   map.remove("key");
   
   // With TTL
   map.put("temp", "value", 60, TimeUnit.SECONDS);
   
   // Bulk operations
   map.putAll(Map.of("a", "1", "b", "2"));
   Map<String, String> result = map.getAll(Set.of("a", "b"));

**Python:**

.. code-block:: python

   my_map = client.get_map("my-map")
   
   # Basic operations
   my_map.put("key", "value")
   value = my_map.get("key")
   my_map.put_if_absent("key2", "value2")
   my_map.remove("key")
   
   # With TTL (in seconds)
   my_map.put("temp", "value", ttl=60)
   
   # Bulk operations
   my_map.put_all({"a": "1", "b": "2"})
   result = my_map.get_all(["a", "b"])

Async Operations
----------------

**Java:**

.. code-block:: java

   CompletableFuture<String> future = map.getAsync("key");
   future.thenAccept(value -> System.out.println("Value: " + value));
   
   // Or blocking
   String value = map.getAsync("key").get();

**Python:**

.. code-block:: python

   # Using Future
   future = my_map.get_async("key")
   value = future.result()
   
   # Using asyncio
   async def example():
       value = await my_map.get_async("key")

Predicates
----------

**Java:**

.. code-block:: java

   import com.hazelcast.query.Predicates;
   
   // Equal
   Predicate<String, User> pred = Predicates.equal("age", 30);
   
   // SQL
   Predicate<String, User> sqlPred = Predicates.sql("age > 18 AND status = 'active'");
   
   // Combined
   Predicate<String, User> combined = Predicates.and(
       Predicates.greaterThan("age", 18),
       Predicates.equal("status", "active")
   );
   
   Collection<User> users = map.values(combined);

**Python:**

.. code-block:: python

   from hazelcast.predicate import attr, sql, and_
   
   # Equal
   pred = attr("age").equal(30)
   
   # SQL
   sql_pred = sql("age > 18 AND status = 'active'")
   
   # Combined
   combined = and_(
       attr("age").greater_than(18),
       attr("status").equal("active")
   )
   
   users = my_map.values(combined)

Aggregators
-----------

**Java:**

.. code-block:: java

   import com.hazelcast.aggregation.Aggregators;
   
   Long count = map.aggregate(Aggregators.count());
   Long sum = map.aggregate(Aggregators.longSum("salary"));
   Double avg = map.aggregate(Aggregators.doubleAvg("salary"));

**Python:**

.. code-block:: python

   from hazelcast.aggregator import count, sum_, average
   
   total = my_map.aggregate(count())
   total_salary = my_map.aggregate(sum_("salary"))
   avg_salary = my_map.aggregate(average("salary"))

Entry Processors
----------------

**Java:**

.. code-block:: java

   public class IncrementProcessor implements EntryProcessor<String, Integer, Integer> {
       @Override
       public Integer process(Entry<String, Integer> entry) {
           Integer oldValue = entry.getValue();
           entry.setValue(oldValue + 1);
           return oldValue;
       }
   }
   
   Integer oldValue = map.executeOnKey("counter", new IncrementProcessor());

**Python:**

.. code-block:: python

   from hazelcast.processor import AbstractEntryProcessor, EntryProcessorEntry

   class IncrementProcessor(AbstractEntryProcessor):
       def process(self, entry: EntryProcessorEntry):
           old_value = entry.get_value() or 0
           entry.set_value(old_value + 1)
           return old_value

   old_value = my_map.execute_on_key("counter", IncrementProcessor())

Transactions
------------

**Java:**

.. code-block:: java

   TransactionOptions options = new TransactionOptions()
       .setTransactionType(TransactionType.TWO_PHASE)
       .setTimeout(60, TimeUnit.SECONDS);
   
   TransactionContext ctx = client.newTransactionContext(options);
   try {
       ctx.beginTransaction();
       TransactionalMap<String, String> txnMap = ctx.getMap("my-map");
       txnMap.put("key", "value");
       ctx.commitTransaction();
   } catch (Exception e) {
       ctx.rollbackTransaction();
       throw e;
   }

**Python:**

.. code-block:: python

   from hazelcast.transaction import TransactionOptions, TransactionType

   options = TransactionOptions(
       transaction_type=TransactionType.TWO_PHASE,
       timeout=60.0,
   )
   
   # Using context manager (recommended)
   with client.new_transaction_context(options) as ctx:
       txn_map = ctx.get_map("my-map")
       txn_map.put("key", "value")
       # Auto-commits on success

SQL
---

**Java:**

.. code-block:: java

   try (SqlResult result = client.getSql().execute("SELECT * FROM users WHERE age > ?", 18)) {
       for (SqlRow row : result) {
           String name = row.getObject("name");
           int age = row.getObject("age");
       }
   }

**Python:**

.. code-block:: python

   sql = client.get_sql()
   result = sql.execute("SELECT * FROM users WHERE age > ?", 18)
   
   for row in result:
       name = row["name"]
       age = row["age"]

CP Subsystem
------------

**Java:**

.. code-block:: java

   IAtomicLong counter = client.getCPSubsystem().getAtomicLong("counter");
   counter.set(0);
   long newValue = counter.incrementAndGet();
   
   FencedLock lock = client.getCPSubsystem().getLock("my-lock");
   lock.lock();
   try {
       // Critical section
   } finally {
       lock.unlock();
   }

**Python:**

.. code-block:: python

   counter = client.get_atomic_long("counter")
   counter.set(0)
   new_value = counter.increment_and_get()
   
   lock = client.get_fenced_lock("my-lock")
   with lock as fence:
       # Critical section
       pass

Listeners
---------

**Java:**

.. code-block:: java

   map.addEntryListener(new EntryAddedListener<String, String>() {
       @Override
       public void entryAdded(EntryEvent<String, String> event) {
           System.out.println("Added: " + event.getKey());
       }
   }, true);

**Python:**

.. code-block:: python

   def on_added(event):
       print(f"Added: {event.key}")

   reg_id = my_map.add_entry_listener(
       on_added=on_added,
       include_value=True,
   )

Key Differences
---------------

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Feature
     - Java
     - Python
   * - Method naming
     - camelCase
     - snake_case
   * - Resource cleanup
     - try-finally or try-with-resources
     - Context manager (with)
   * - Async pattern
     - CompletableFuture
     - Future or asyncio
   * - Time units
     - TimeUnit enum
     - Seconds (float)
   * - Collections
     - Java Collections
     - Python lists/dicts
   * - Null handling
     - null
     - None
   * - Package structure
     - com.hazelcast.*
     - hazelcast.*

Type Mapping
------------

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - Java Type
     - Python Type
   * - ``String``
     - ``str``
   * - ``int/Integer``
     - ``int``
   * - ``long/Long``
     - ``int``
   * - ``double/Double``
     - ``float``
   * - ``boolean/Boolean``
     - ``bool``
   * - ``byte[]``
     - ``bytes``
   * - ``List<T>``
     - ``list``
   * - ``Set<T>``
     - ``set``
   * - ``Map<K,V>``
     - ``dict``
   * - ``Optional<T>``
     - ``Optional[T]`` or ``None``
   * - ``CompletableFuture<T>``
     - ``Future``
