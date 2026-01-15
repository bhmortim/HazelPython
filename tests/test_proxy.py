"""Unit tests for distributed data structure proxies."""

import unittest
from concurrent.futures import Future
from unittest.mock import MagicMock, patch

from hazelcast.proxy.base import Proxy, ProxyContext, DistributedObject
from hazelcast.proxy.map import MapProxy, EntryEvent, EntryListener
from hazelcast.proxy.multi_map import MultiMapProxy
from hazelcast.proxy.queue import QueueProxy
from hazelcast.proxy.set import SetProxy
from hazelcast.proxy.list import ListProxy
from hazelcast.proxy.ringbuffer import RingbufferProxy, OverflowPolicy, ReadResultSet
from hazelcast.proxy.topic import TopicProxy, TopicMessage, MessageListener
from hazelcast.proxy.reliable_topic import (
    ReliableTopicProxy,
    ReliableTopicConfig,
    TopicOverloadPolicy,
)
from hazelcast.proxy.pn_counter import PNCounterProxy
from hazelcast.predicate import (
    Predicate,
    SqlPredicate,
    TruePredicate,
    FalsePredicate,
    EqualPredicate,
    NotEqualPredicate,
    GreaterThanPredicate,
    LessThanPredicate,
    BetweenPredicate,
    InPredicate,
    LikePredicate,
    AndPredicate,
    OrPredicate,
    NotPredicate,
    PagingPredicate,
    PredicateBuilder,
    attr,
    sql,
    true,
    false,
    and_,
    or_,
    not_,
    paging,
)
from hazelcast.aggregator import (
    Aggregator,
    CountAggregator,
    SumAggregator,
    AverageAggregator,
    MinAggregator,
    MaxAggregator,
    DistinctValuesAggregator,
    Projection,
    SingleAttributeProjection,
    MultiAttributeProjection,
    IdentityProjection,
    count,
    sum_,
    average,
    min_,
    max_,
    distinct,
    single_attribute,
    multi_attribute,
    identity,
)
from hazelcast.exceptions import IllegalStateException


class TestProxyBase(unittest.TestCase):
    """Tests for base Proxy class."""

    def test_proxy_name(self):
        proxy = MapProxy("test-map")
        self.assertEqual("test-map", proxy.name)

    def test_proxy_service_name(self):
        proxy = MapProxy("test-map")
        self.assertEqual("hz:impl:mapService", proxy.service_name)

    def test_proxy_not_destroyed_initially(self):
        proxy = MapProxy("test-map")
        self.assertFalse(proxy.is_destroyed)

    def test_proxy_destroy(self):
        proxy = MapProxy("test-map")
        proxy.destroy()
        self.assertTrue(proxy.is_destroyed)

    def test_proxy_operation_after_destroy_raises(self):
        proxy = MapProxy("test-map")
        proxy.destroy()
        with self.assertRaises(IllegalStateException):
            proxy.get("key")

    def test_proxy_repr(self):
        proxy = MapProxy("my-map")
        self.assertIn("my-map", repr(proxy))
        self.assertIn("MapProxy", repr(proxy))


class TestMapProxy(unittest.TestCase):
    """Tests for MapProxy."""

    def setUp(self):
        self.map = MapProxy("test-map")

    def test_put_returns_none_by_default(self):
        result = self.map.put("key", "value")
        self.assertIsNone(result)

    def test_put_async_returns_future(self):
        future = self.map.put_async("key", "value")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_get_returns_none_by_default(self):
        result = self.map.get("key")
        self.assertIsNone(result)

    def test_get_async_returns_future(self):
        future = self.map.get_async("key")
        self.assertIsInstance(future, Future)
        self.assertIsNone(future.result())

    def test_remove_returns_none_by_default(self):
        result = self.map.remove("key")
        self.assertIsNone(result)

    def test_remove_async_returns_future(self):
        future = self.map.remove_async("key")
        self.assertIsInstance(future, Future)

    def test_contains_key_returns_false_by_default(self):
        result = self.map.contains_key("key")
        self.assertFalse(result)

    def test_contains_key_async_returns_future(self):
        future = self.map.contains_key_async("key")
        self.assertIsInstance(future, Future)
        self.assertFalse(future.result())

    def test_contains_value_returns_false_by_default(self):
        result = self.map.contains_value("value")
        self.assertFalse(result)

    def test_size_returns_zero_by_default(self):
        result = self.map.size()
        self.assertEqual(0, result)

    def test_is_empty_returns_true_by_default(self):
        result = self.map.is_empty()
        self.assertTrue(result)

    def test_clear_completes(self):
        self.map.clear()

    def test_key_set_returns_empty_by_default(self):
        result = self.map.key_set()
        self.assertEqual(set(), result)

    def test_values_returns_empty_by_default(self):
        result = self.map.values()
        self.assertEqual([], result)

    def test_entry_set_returns_empty_by_default(self):
        result = self.map.entry_set()
        self.assertEqual(set(), result)

    def test_put_if_absent_returns_none(self):
        result = self.map.put_if_absent("key", "value")
        self.assertIsNone(result)

    def test_replace_returns_none(self):
        result = self.map.replace("key", "new_value")
        self.assertIsNone(result)

    def test_replace_if_same_returns_false(self):
        result = self.map.replace_if_same("key", "old", "new")
        self.assertFalse(result)

    def test_get_all_returns_empty_dict(self):
        result = self.map.get_all({"k1", "k2"})
        self.assertEqual({}, result)

    def test_put_all_completes(self):
        self.map.put_all({"k1": "v1", "k2": "v2"})

    def test_add_entry_listener(self):
        listener = EntryListener()
        reg_id = self.map.add_entry_listener(listener)
        self.assertIsNotNone(reg_id)

    def test_remove_entry_listener(self):
        listener = EntryListener()
        reg_id = self.map.add_entry_listener(listener)
        result = self.map.remove_entry_listener(reg_id)
        self.assertTrue(result)

    def test_remove_nonexistent_listener_returns_false(self):
        result = self.map.remove_entry_listener("nonexistent")
        self.assertFalse(result)

    def test_lock_completes(self):
        self.map.lock("key")

    def test_try_lock_returns_true(self):
        result = self.map.try_lock("key")
        self.assertTrue(result)

    def test_unlock_completes(self):
        self.map.unlock("key")

    def test_is_locked_returns_false(self):
        result = self.map.is_locked("key")
        self.assertFalse(result)

    def test_aggregate_returns_none(self):
        result = self.map.aggregate(CountAggregator())
        self.assertIsNone(result)

    def test_project_returns_empty_list(self):
        result = self.map.project(SingleAttributeProjection("name"))
        self.assertEqual([], result)

    def test_execute_on_key_returns_none(self):
        result = self.map.execute_on_key("key", MagicMock())
        self.assertIsNone(result)

    def test_dunder_len(self):
        self.assertEqual(0, len(self.map))

    def test_dunder_contains(self):
        self.assertFalse("key" in self.map)

    def test_dunder_getitem(self):
        result = self.map["key"]
        self.assertIsNone(result)

    def test_dunder_setitem(self):
        self.map["key"] = "value"

    def test_dunder_delitem(self):
        del self.map["key"]

    def test_dunder_iter(self):
        keys = list(self.map)
        self.assertEqual([], keys)


class TestEntryEvent(unittest.TestCase):
    """Tests for EntryEvent."""

    def test_event_properties(self):
        event = EntryEvent(
            event_type=EntryEvent.ADDED,
            key="test_key",
            value="test_value",
            old_value="old_value",
        )
        self.assertEqual(EntryEvent.ADDED, event.event_type)
        self.assertEqual("test_key", event.key)
        self.assertEqual("test_value", event.value)
        self.assertEqual("old_value", event.old_value)

    def test_event_type_constants(self):
        self.assertEqual(1, EntryEvent.ADDED)
        self.assertEqual(2, EntryEvent.REMOVED)
        self.assertEqual(4, EntryEvent.UPDATED)
        self.assertEqual(8, EntryEvent.EVICTED)


class TestMultiMapProxy(unittest.TestCase):
    """Tests for MultiMapProxy."""

    def setUp(self):
        self.mmap = MultiMapProxy("test-multimap")

    def test_put_returns_true(self):
        result = self.mmap.put("key", "value")
        self.assertTrue(result)

    def test_get_returns_empty_collection(self):
        result = self.mmap.get("key")
        self.assertEqual([], result)

    def test_remove_returns_false(self):
        result = self.mmap.remove("key", "value")
        self.assertFalse(result)

    def test_remove_all_returns_empty(self):
        result = self.mmap.remove_all("key")
        self.assertEqual([], result)

    def test_contains_key_returns_false(self):
        result = self.mmap.contains_key("key")
        self.assertFalse(result)

    def test_contains_value_returns_false(self):
        result = self.mmap.contains_value("value")
        self.assertFalse(result)

    def test_contains_entry_returns_false(self):
        result = self.mmap.contains_entry("key", "value")
        self.assertFalse(result)

    def test_size_returns_zero(self):
        result = self.mmap.size()
        self.assertEqual(0, result)

    def test_value_count_returns_zero(self):
        result = self.mmap.value_count("key")
        self.assertEqual(0, result)

    def test_key_set_returns_empty(self):
        result = self.mmap.key_set()
        self.assertEqual(set(), result)

    def test_values_returns_empty(self):
        result = self.mmap.values()
        self.assertEqual([], result)

    def test_entry_set_returns_empty(self):
        result = self.mmap.entry_set()
        self.assertEqual(set(), result)

    def test_dunder_len(self):
        self.assertEqual(0, len(self.mmap))

    def test_dunder_contains(self):
        self.assertFalse("key" in self.mmap)

    def test_dunder_getitem(self):
        result = self.mmap["key"]
        self.assertEqual([], result)


class TestQueueProxy(unittest.TestCase):
    """Tests for QueueProxy."""

    def setUp(self):
        self.queue = QueueProxy("test-queue")

    def test_add_returns_true(self):
        result = self.queue.add("item")
        self.assertTrue(result)

    def test_offer_returns_true(self):
        result = self.queue.offer("item")
        self.assertTrue(result)

    def test_put_completes(self):
        self.queue.put("item")

    def test_poll_returns_none(self):
        result = self.queue.poll()
        self.assertIsNone(result)

    def test_take_returns_none(self):
        result = self.queue.take()
        self.assertIsNone(result)

    def test_peek_returns_none(self):
        result = self.queue.peek()
        self.assertIsNone(result)

    def test_remove_returns_false(self):
        result = self.queue.remove("item")
        self.assertFalse(result)

    def test_contains_returns_false(self):
        result = self.queue.contains("item")
        self.assertFalse(result)

    def test_size_returns_zero(self):
        result = self.queue.size()
        self.assertEqual(0, result)

    def test_is_empty_returns_true(self):
        result = self.queue.is_empty()
        self.assertTrue(result)

    def test_remaining_capacity_returns_zero(self):
        result = self.queue.remaining_capacity()
        self.assertEqual(0, result)

    def test_get_all_returns_empty(self):
        result = self.queue.get_all()
        self.assertEqual([], result)

    def test_dunder_len(self):
        self.assertEqual(0, len(self.queue))

    def test_dunder_contains(self):
        self.assertFalse("item" in self.queue)

    def test_dunder_iter(self):
        items = list(self.queue)
        self.assertEqual([], items)


class TestSetProxy(unittest.TestCase):
    """Tests for SetProxy."""

    def setUp(self):
        self.set = SetProxy("test-set")

    def test_add_returns_true(self):
        result = self.set.add("item")
        self.assertTrue(result)

    def test_add_all_returns_true(self):
        result = self.set.add_all(["a", "b", "c"])
        self.assertTrue(result)

    def test_remove_returns_false(self):
        result = self.set.remove("item")
        self.assertFalse(result)

    def test_contains_returns_false(self):
        result = self.set.contains("item")
        self.assertFalse(result)

    def test_contains_all_returns_false(self):
        result = self.set.contains_all(["a", "b"])
        self.assertFalse(result)

    def test_size_returns_zero(self):
        result = self.set.size()
        self.assertEqual(0, result)

    def test_is_empty_returns_true(self):
        result = self.set.is_empty()
        self.assertTrue(result)

    def test_get_all_returns_empty(self):
        result = self.set.get_all()
        self.assertEqual([], result)

    def test_dunder_len(self):
        self.assertEqual(0, len(self.set))

    def test_dunder_contains(self):
        self.assertFalse("item" in self.set)


class TestListProxy(unittest.TestCase):
    """Tests for ListProxy."""

    def setUp(self):
        self.list = ListProxy("test-list")

    def test_add_returns_true(self):
        result = self.list.add("item")
        self.assertTrue(result)

    def test_add_at_completes(self):
        self.list.add_at(0, "item")

    def test_add_all_returns_true(self):
        result = self.list.add_all(["a", "b", "c"])
        self.assertTrue(result)

    def test_get_returns_none(self):
        result = self.list.get(0)
        self.assertIsNone(result)

    def test_set_returns_none(self):
        result = self.list.set(0, "item")
        self.assertIsNone(result)

    def test_remove_returns_false(self):
        result = self.list.remove("item")
        self.assertFalse(result)

    def test_remove_at_returns_none(self):
        result = self.list.remove_at(0)
        self.assertIsNone(result)

    def test_contains_returns_false(self):
        result = self.list.contains("item")
        self.assertFalse(result)

    def test_index_of_returns_negative_one(self):
        result = self.list.index_of("item")
        self.assertEqual(-1, result)

    def test_last_index_of_returns_negative_one(self):
        result = self.list.last_index_of("item")
        self.assertEqual(-1, result)

    def test_sub_list_returns_empty(self):
        result = self.list.sub_list(0, 10)
        self.assertEqual([], result)

    def test_size_returns_zero(self):
        result = self.list.size()
        self.assertEqual(0, result)

    def test_is_empty_returns_true(self):
        result = self.list.is_empty()
        self.assertTrue(result)

    def test_dunder_len(self):
        self.assertEqual(0, len(self.list))

    def test_dunder_contains(self):
        self.assertFalse("item" in self.list)

    def test_dunder_getitem(self):
        result = self.list[0]
        self.assertIsNone(result)

    def test_dunder_setitem(self):
        self.list[0] = "item"

    def test_dunder_delitem(self):
        del self.list[0]


class TestRingbufferProxy(unittest.TestCase):
    """Tests for RingbufferProxy."""

    def setUp(self):
        self.rb = RingbufferProxy("test-ringbuffer")

    def test_capacity_returns_zero(self):
        result = self.rb.capacity()
        self.assertEqual(0, result)

    def test_size_returns_zero(self):
        result = self.rb.size()
        self.assertEqual(0, result)

    def test_tail_sequence_returns_negative_one(self):
        result = self.rb.tail_sequence()
        self.assertEqual(-1, result)

    def test_head_sequence_returns_zero(self):
        result = self.rb.head_sequence()
        self.assertEqual(0, result)

    def test_remaining_capacity_returns_zero(self):
        result = self.rb.remaining_capacity()
        self.assertEqual(0, result)

    def test_add_returns_sequence(self):
        result = self.rb.add("item")
        self.assertEqual(0, result)

    def test_add_with_overflow_policy(self):
        result = self.rb.add("item", OverflowPolicy.FAIL)
        self.assertEqual(0, result)

    def test_add_all_returns_sequence(self):
        result = self.rb.add_all(["a", "b", "c"])
        self.assertEqual(-1, result)

    def test_read_one_returns_none(self):
        result = self.rb.read_one(0)
        self.assertIsNone(result)

    def test_read_many_returns_result_set(self):
        result = self.rb.read_many(0, 1, 10)
        self.assertIsInstance(result, ReadResultSet)
        self.assertEqual(0, len(result))

    def test_dunder_len(self):
        self.assertEqual(0, len(self.rb))


class TestReadResultSet(unittest.TestCase):
    """Tests for ReadResultSet."""

    def test_empty_result_set(self):
        rs = ReadResultSet([], 0, 0)
        self.assertEqual(0, len(rs))
        self.assertEqual(0, rs.read_count)
        self.assertEqual(0, rs.next_sequence_to_read)

    def test_result_set_with_items(self):
        rs = ReadResultSet(["a", "b", "c"], 3, 103)
        self.assertEqual(3, len(rs))
        self.assertEqual(["a", "b", "c"], rs.items)
        self.assertEqual(3, rs.read_count)
        self.assertEqual(103, rs.next_sequence_to_read)

    def test_get_sequence(self):
        rs = ReadResultSet(["a", "b", "c"], 3, 103)
        self.assertEqual(100, rs.get_sequence(0))
        self.assertEqual(101, rs.get_sequence(1))
        self.assertEqual(102, rs.get_sequence(2))

    def test_getitem(self):
        rs = ReadResultSet(["a", "b", "c"], 3, 103)
        self.assertEqual("a", rs[0])
        self.assertEqual("b", rs[1])
        self.assertEqual("c", rs[2])

    def test_iter(self):
        rs = ReadResultSet(["a", "b", "c"], 3, 103)
        items = list(rs)
        self.assertEqual(["a", "b", "c"], items)


class TestOverflowPolicy(unittest.TestCase):
    """Tests for OverflowPolicy enum."""

    def test_overwrite_value(self):
        self.assertEqual(0, OverflowPolicy.OVERWRITE.value)

    def test_fail_value(self):
        self.assertEqual(1, OverflowPolicy.FAIL.value)


class TestTopicProxy(unittest.TestCase):
    """Tests for TopicProxy."""

    def setUp(self):
        self.topic = TopicProxy("test-topic")

    def test_publish_completes(self):
        self.topic.publish("message")

    def test_publish_async_returns_future(self):
        future = self.topic.publish_async("message")
        self.assertIsInstance(future, Future)

    def test_add_message_listener_with_listener(self):
        listener = MessageListener()
        reg_id = self.topic.add_message_listener(listener)
        self.assertIsNotNone(reg_id)

    def test_add_message_listener_with_callback(self):
        callback = MagicMock()
        reg_id = self.topic.add_message_listener(on_message=callback)
        self.assertIsNotNone(reg_id)

    def test_add_message_listener_raises_without_args(self):
        with self.assertRaises(ValueError):
            self.topic.add_message_listener()

    def test_remove_message_listener(self):
        listener = MessageListener()
        reg_id = self.topic.add_message_listener(listener)
        result = self.topic.remove_message_listener(reg_id)
        self.assertTrue(result)

    def test_remove_nonexistent_listener(self):
        result = self.topic.remove_message_listener("nonexistent")
        self.assertFalse(result)


class TestTopicMessage(unittest.TestCase):
    """Tests for TopicMessage."""

    def test_message_properties(self):
        msg = TopicMessage("hello", 12345)
        self.assertEqual("hello", msg.message)
        self.assertEqual(12345, msg.publish_time)
        self.assertIsNone(msg.publishing_member)


class TestReliableTopicProxy(unittest.TestCase):
    """Tests for ReliableTopicProxy."""

    def setUp(self):
        self.topic = ReliableTopicProxy("test-reliable-topic")

    def test_default_config(self):
        self.assertEqual(10, self.topic.config.read_batch_size)
        self.assertEqual(TopicOverloadPolicy.BLOCK, self.topic.config.overload_policy)

    def test_custom_config(self):
        config = ReliableTopicConfig(
            read_batch_size=50,
            overload_policy=TopicOverloadPolicy.DISCARD_OLDEST,
        )
        topic = ReliableTopicProxy("test", config=config)
        self.assertEqual(50, topic.config.read_batch_size)
        self.assertEqual(TopicOverloadPolicy.DISCARD_OLDEST, topic.config.overload_policy)

    def test_publish_completes(self):
        self.topic.publish("message")

    def test_add_message_listener(self):
        callback = MagicMock()
        reg_id = self.topic.add_message_listener(on_message=callback)
        self.assertIsNotNone(reg_id)

    def test_remove_message_listener(self):
        callback = MagicMock()
        reg_id = self.topic.add_message_listener(on_message=callback)
        result = self.topic.remove_message_listener(reg_id)
        self.assertTrue(result)


class TestReliableTopicConfig(unittest.TestCase):
    """Tests for ReliableTopicConfig."""

    def test_default_values(self):
        config = ReliableTopicConfig()
        self.assertEqual(10, config.read_batch_size)
        self.assertEqual(TopicOverloadPolicy.BLOCK, config.overload_policy)

    def test_custom_values(self):
        config = ReliableTopicConfig(
            read_batch_size=100,
            overload_policy=TopicOverloadPolicy.ERROR,
        )
        self.assertEqual(100, config.read_batch_size)
        self.assertEqual(TopicOverloadPolicy.ERROR, config.overload_policy)

    def test_invalid_read_batch_size(self):
        from hazelcast.exceptions import ConfigurationException
        with self.assertRaises(ConfigurationException):
            ReliableTopicConfig(read_batch_size=0)

    def test_setter_validation(self):
        config = ReliableTopicConfig()
        from hazelcast.exceptions import ConfigurationException
        with self.assertRaises(ConfigurationException):
            config.read_batch_size = -1


class TestPNCounterProxy(unittest.TestCase):
    """Tests for PNCounterProxy."""

    def setUp(self):
        self.counter = PNCounterProxy("test-counter")

    def test_get_returns_zero(self):
        result = self.counter.get()
        self.assertEqual(0, result)

    def test_get_and_add_returns_zero(self):
        result = self.counter.get_and_add(5)
        self.assertEqual(0, result)

    def test_add_and_get_returns_delta(self):
        result = self.counter.add_and_get(5)
        self.assertEqual(5, result)

    def test_get_and_subtract_returns_zero(self):
        result = self.counter.get_and_subtract(5)
        self.assertEqual(0, result)

    def test_subtract_and_get_returns_negative_delta(self):
        result = self.counter.subtract_and_get(5)
        self.assertEqual(-5, result)

    def test_get_and_increment_returns_zero(self):
        result = self.counter.get_and_increment()
        self.assertEqual(0, result)

    def test_increment_and_get_returns_one(self):
        result = self.counter.increment_and_get()
        self.assertEqual(1, result)

    def test_get_and_decrement_returns_zero(self):
        result = self.counter.get_and_decrement()
        self.assertEqual(0, result)

    def test_decrement_and_get_returns_negative_one(self):
        result = self.counter.decrement_and_get()
        self.assertEqual(-1, result)

    def test_reset_clears_state(self):
        self.counter._observed_clock["member1"] = 10
        self.counter._current_target_replica = "member1"
        self.counter.reset()
        self.assertEqual({}, self.counter._observed_clock)
        self.assertIsNone(self.counter._current_target_replica)


class TestPredicates(unittest.TestCase):
    """Tests for predicate classes."""

    def test_sql_predicate(self):
        pred = SqlPredicate("age > 30")
        self.assertEqual("age > 30", pred.sql)
        self.assertEqual({"type": "sql", "sql": "age > 30"}, pred.to_dict())

    def test_true_predicate(self):
        pred = TruePredicate()
        self.assertEqual({"type": "true"}, pred.to_dict())

    def test_false_predicate(self):
        pred = FalsePredicate()
        self.assertEqual({"type": "false"}, pred.to_dict())

    def test_equal_predicate(self):
        pred = EqualPredicate("name", "John")
        self.assertEqual("name", pred.attribute)
        self.assertEqual("John", pred.value)
        self.assertEqual(
            {"type": "equal", "attribute": "name", "value": "John"},
            pred.to_dict(),
        )

    def test_not_equal_predicate(self):
        pred = NotEqualPredicate("status", "inactive")
        self.assertEqual(
            {"type": "not_equal", "attribute": "status", "value": "inactive"},
            pred.to_dict(),
        )

    def test_greater_than_predicate(self):
        pred = GreaterThanPredicate("age", 18)
        self.assertEqual(
            {"type": "greater_than", "attribute": "age", "value": 18},
            pred.to_dict(),
        )

    def test_less_than_predicate(self):
        pred = LessThanPredicate("price", 100)
        self.assertEqual(
            {"type": "less_than", "attribute": "price", "value": 100},
            pred.to_dict(),
        )

    def test_between_predicate(self):
        pred = BetweenPredicate("age", 18, 65)
        self.assertEqual("age", pred.attribute)
        self.assertEqual(18, pred.from_value)
        self.assertEqual(65, pred.to_value)
        self.assertEqual(
            {"type": "between", "attribute": "age", "from": 18, "to": 65},
            pred.to_dict(),
        )

    def test_in_predicate(self):
        pred = InPredicate("status", "active", "pending", "review")
        self.assertEqual("status", pred.attribute)
        self.assertEqual(["active", "pending", "review"], pred.values)
        self.assertEqual(
            {"type": "in", "attribute": "status", "values": ["active", "pending", "review"]},
            pred.to_dict(),
        )

    def test_like_predicate(self):
        pred = LikePredicate("name", "John%")
        self.assertEqual("name", pred.attribute)
        self.assertEqual("John%", pred.pattern)

    def test_and_predicate(self):
        p1 = EqualPredicate("a", 1)
        p2 = EqualPredicate("b", 2)
        pred = AndPredicate(p1, p2)
        result = pred.to_dict()
        self.assertEqual("and", result["type"])
        self.assertEqual(2, len(result["predicates"]))

    def test_or_predicate(self):
        p1 = EqualPredicate("a", 1)
        p2 = EqualPredicate("b", 2)
        pred = OrPredicate(p1, p2)
        result = pred.to_dict()
        self.assertEqual("or", result["type"])
        self.assertEqual(2, len(result["predicates"]))

    def test_not_predicate(self):
        inner = EqualPredicate("active", True)
        pred = NotPredicate(inner)
        result = pred.to_dict()
        self.assertEqual("not", result["type"])
        self.assertIn("predicate", result)

    def test_paging_predicate(self):
        inner = EqualPredicate("active", True)
        pred = PagingPredicate(inner, page_size=20)
        self.assertEqual(0, pred.page)
        self.assertEqual(20, pred.page_size)
        pred.next_page()
        self.assertEqual(1, pred.page)
        pred.previous_page()
        self.assertEqual(0, pred.page)
        pred.reset()
        self.assertEqual(0, pred.page)

    def test_paging_predicate_negative_page_raises(self):
        pred = PagingPredicate(page_size=10)
        with self.assertRaises(ValueError):
            pred.page = -1


class TestPredicateBuilder(unittest.TestCase):
    """Tests for PredicateBuilder."""

    def test_attr_function(self):
        builder = attr("name")
        self.assertIsInstance(builder, PredicateBuilder)

    def test_builder_equal(self):
        pred = attr("name").equal("John")
        self.assertIsInstance(pred, EqualPredicate)
        self.assertEqual("name", pred.attribute)
        self.assertEqual("John", pred.value)

    def test_builder_not_equal(self):
        pred = attr("status").not_equal("deleted")
        self.assertIsInstance(pred, NotEqualPredicate)

    def test_builder_greater_than(self):
        pred = attr("age").greater_than(18)
        self.assertIsInstance(pred, GreaterThanPredicate)

    def test_builder_less_than(self):
        pred = attr("price").less_than(100)
        self.assertIsInstance(pred, LessThanPredicate)

    def test_builder_between(self):
        pred = attr("age").between(18, 65)
        self.assertIsInstance(pred, BetweenPredicate)

    def test_builder_is_in(self):
        pred = attr("color").is_in("red", "green", "blue")
        self.assertIsInstance(pred, InPredicate)

    def test_builder_like(self):
        pred = attr("name").like("J%")
        self.assertIsInstance(pred, LikePredicate)


class TestPredicateFunctions(unittest.TestCase):
    """Tests for predicate factory functions."""

    def test_sql_function(self):
        pred = sql("age > 30")
        self.assertIsInstance(pred, SqlPredicate)

    def test_true_function(self):
        pred = true()
        self.assertIsInstance(pred, TruePredicate)

    def test_false_function(self):
        pred = false()
        self.assertIsInstance(pred, FalsePredicate)

    def test_and_function(self):
        pred = and_(EqualPredicate("a", 1), EqualPredicate("b", 2))
        self.assertIsInstance(pred, AndPredicate)

    def test_or_function(self):
        pred = or_(EqualPredicate("a", 1), EqualPredicate("b", 2))
        self.assertIsInstance(pred, OrPredicate)

    def test_not_function(self):
        pred = not_(EqualPredicate("active", True))
        self.assertIsInstance(pred, NotPredicate)

    def test_paging_function(self):
        pred = paging(page_size=25)
        self.assertIsInstance(pred, PagingPredicate)
        self.assertEqual(25, pred.page_size)


class TestAggregators(unittest.TestCase):
    """Tests for aggregator classes."""

    def test_count_aggregator(self):
        agg = CountAggregator()
        self.assertEqual({"type": "count"}, agg.to_dict())

    def test_count_aggregator_with_attribute(self):
        agg = CountAggregator("name")
        self.assertEqual({"type": "count", "attribute": "name"}, agg.to_dict())

    def test_sum_aggregator(self):
        agg = SumAggregator("price")
        self.assertEqual("price", agg.attribute)
        self.assertEqual({"type": "sum", "attribute": "price"}, agg.to_dict())

    def test_average_aggregator(self):
        agg = AverageAggregator("score")
        self.assertEqual({"type": "average", "attribute": "score"}, agg.to_dict())

    def test_min_aggregator(self):
        agg = MinAggregator("age")
        self.assertEqual({"type": "min", "attribute": "age"}, agg.to_dict())

    def test_max_aggregator(self):
        agg = MaxAggregator("salary")
        self.assertEqual({"type": "max", "attribute": "salary"}, agg.to_dict())

    def test_distinct_aggregator(self):
        agg = DistinctValuesAggregator("category")
        self.assertEqual({"type": "distinct", "attribute": "category"}, agg.to_dict())


class TestAggregatorFunctions(unittest.TestCase):
    """Tests for aggregator factory functions."""

    def test_count_function(self):
        agg = count()
        self.assertIsInstance(agg, CountAggregator)

    def test_count_with_attribute(self):
        agg = count("name")
        self.assertEqual("name", agg.attribute)

    def test_sum_function(self):
        agg = sum_("price")
        self.assertIsInstance(agg, SumAggregator)

    def test_average_function(self):
        agg = average("score")
        self.assertIsInstance(agg, AverageAggregator)

    def test_min_function(self):
        agg = min_("age")
        self.assertIsInstance(agg, MinAggregator)

    def test_max_function(self):
        agg = max_("salary")
        self.assertIsInstance(agg, MaxAggregator)

    def test_distinct_function(self):
        agg = distinct("category")
        self.assertIsInstance(agg, DistinctValuesAggregator)


class TestProjections(unittest.TestCase):
    """Tests for projection classes."""

    def test_single_attribute_projection(self):
        proj = SingleAttributeProjection("name")
        self.assertEqual("name", proj.attribute)
        self.assertEqual(
            {"type": "single_attribute", "attribute": "name"},
            proj.to_dict(),
        )

    def test_multi_attribute_projection(self):
        proj = MultiAttributeProjection("name", "age", "email")
        self.assertEqual(["name", "age", "email"], proj.attributes)
        self.assertEqual(
            {"type": "multi_attribute", "attributes": ["name", "age", "email"]},
            proj.to_dict(),
        )

    def test_identity_projection(self):
        proj = IdentityProjection()
        self.assertEqual({"type": "identity"}, proj.to_dict())


class TestProjectionFunctions(unittest.TestCase):
    """Tests for projection factory functions."""

    def test_single_attribute_function(self):
        proj = single_attribute("name")
        self.assertIsInstance(proj, SingleAttributeProjection)

    def test_multi_attribute_function(self):
        proj = multi_attribute("name", "age")
        self.assertIsInstance(proj, MultiAttributeProjection)
        self.assertEqual(["name", "age"], proj.attributes)

    def test_identity_function(self):
        proj = identity()
        self.assertIsInstance(proj, IdentityProjection)


class TestProxyContext(unittest.TestCase):
    """Tests for ProxyContext."""

    def test_context_properties(self):
        mock_invocation = MagicMock()
        mock_serialization = MagicMock()
        mock_partition = MagicMock()
        mock_listener = MagicMock()

        context = ProxyContext(
            invocation_service=mock_invocation,
            serialization_service=mock_serialization,
            partition_service=mock_partition,
            listener_service=mock_listener,
        )

        self.assertEqual(mock_invocation, context.invocation_service)
        self.assertEqual(mock_serialization, context.serialization_service)
        self.assertEqual(mock_partition, context.partition_service)
        self.assertEqual(mock_listener, context.listener_service)


if __name__ == "__main__":
    unittest.main()
