"""Unit tests for hazelcast/aggregator.py"""

import unittest
from abc import ABC

from hazelcast.aggregator import (
    Aggregator,
    CountAggregator,
    DistinctValuesAggregator,
    SumAggregator,
    AverageAggregator,
    MinAggregator,
    MaxAggregator,
    IntegerSumAggregator,
    IntegerAverageAggregator,
    LongSumAggregator,
    LongAverageAggregator,
    DoubleSumAggregator,
    DoubleAverageAggregator,
    FixedPointSumAggregator,
    FloatingPointSumAggregator,
    Projection,
    SingleAttributeProjection,
    MultiAttributeProjection,
    IdentityProjection,
    count,
    distinct,
    sum_,
    average,
    min_,
    max_,
    integer_sum,
    integer_average,
    long_sum,
    long_average,
    double_sum,
    double_average,
    single_attribute,
    multi_attribute,
    identity,
)


class TestAggregatorABC(unittest.TestCase):
    def test_aggregator_is_abstract(self):
        self.assertTrue(issubclass(Aggregator, ABC))

    def test_aggregator_cannot_be_instantiated(self):
        with self.assertRaises(TypeError):
            Aggregator()

    def test_aggregator_has_to_dict_abstract_method(self):
        self.assertIn("to_dict", Aggregator.__abstractmethods__)


class TestCountAggregator(unittest.TestCase):
    def test_init_without_attribute(self):
        agg = CountAggregator()
        self.assertIsNone(agg.attribute)

    def test_init_with_attribute(self):
        agg = CountAggregator("name")
        self.assertEqual(agg.attribute, "name")

    def test_to_dict_without_attribute(self):
        agg = CountAggregator()
        result = agg.to_dict()
        self.assertEqual(result, {"type": "count"})

    def test_to_dict_with_attribute(self):
        agg = CountAggregator("email")
        result = agg.to_dict()
        self.assertEqual(result, {"type": "count", "attribute": "email"})

    def test_repr_without_attribute(self):
        agg = CountAggregator()
        self.assertEqual(repr(agg), "CountAggregator()")

    def test_repr_with_attribute(self):
        agg = CountAggregator("id")
        self.assertEqual(repr(agg), "CountAggregator('id')")


class TestDistinctValuesAggregator(unittest.TestCase):
    def test_init(self):
        agg = DistinctValuesAggregator("category")
        self.assertEqual(agg.attribute, "category")

    def test_to_dict(self):
        agg = DistinctValuesAggregator("status")
        result = agg.to_dict()
        self.assertEqual(result, {"type": "distinct", "attribute": "status"})

    def test_repr(self):
        agg = DistinctValuesAggregator("type")
        self.assertEqual(repr(agg), "DistinctValuesAggregator('type')")


class TestSumAggregator(unittest.TestCase):
    def test_init(self):
        agg = SumAggregator("amount")
        self.assertEqual(agg.attribute, "amount")

    def test_to_dict(self):
        agg = SumAggregator("price")
        result = agg.to_dict()
        self.assertEqual(result, {"type": "sum", "attribute": "price"})

    def test_repr(self):
        agg = SumAggregator("total")
        self.assertEqual(repr(agg), "SumAggregator('total')")


class TestAverageAggregator(unittest.TestCase):
    def test_init(self):
        agg = AverageAggregator("score")
        self.assertEqual(agg.attribute, "score")

    def test_to_dict(self):
        agg = AverageAggregator("rating")
        result = agg.to_dict()
        self.assertEqual(result, {"type": "average", "attribute": "rating"})

    def test_repr(self):
        agg = AverageAggregator("value")
        self.assertEqual(repr(agg), "AverageAggregator('value')")


class TestMinAggregator(unittest.TestCase):
    def test_init(self):
        agg = MinAggregator("age")
        self.assertEqual(agg.attribute, "age")

    def test_to_dict(self):
        agg = MinAggregator("timestamp")
        result = agg.to_dict()
        self.assertEqual(result, {"type": "min", "attribute": "timestamp"})

    def test_repr(self):
        agg = MinAggregator("price")
        self.assertEqual(repr(agg), "MinAggregator('price')")


class TestMaxAggregator(unittest.TestCase):
    def test_init(self):
        agg = MaxAggregator("salary")
        self.assertEqual(agg.attribute, "salary")

    def test_to_dict(self):
        agg = MaxAggregator("count")
        result = agg.to_dict()
        self.assertEqual(result, {"type": "max", "attribute": "count"})

    def test_repr(self):
        agg = MaxAggregator("level")
        self.assertEqual(repr(agg), "MaxAggregator('level')")


class TestIntegerSumAggregator(unittest.TestCase):
    def test_init(self):
        agg = IntegerSumAggregator("quantity")
        self.assertEqual(agg.attribute, "quantity")

    def test_to_dict(self):
        agg = IntegerSumAggregator("count")
        result = agg.to_dict()
        self.assertEqual(result, {"type": "integer_sum", "attribute": "count"})

    def test_repr(self):
        agg = IntegerSumAggregator("items")
        self.assertEqual(repr(agg), "IntegerSumAggregator('items')")


class TestIntegerAverageAggregator(unittest.TestCase):
    def test_init(self):
        agg = IntegerAverageAggregator("rating")
        self.assertEqual(agg.attribute, "rating")

    def test_to_dict(self):
        agg = IntegerAverageAggregator("score")
        result = agg.to_dict()
        self.assertEqual(result, {"type": "integer_average", "attribute": "score"})

    def test_repr(self):
        agg = IntegerAverageAggregator("level")
        self.assertEqual(repr(agg), "IntegerAverageAggregator('level')")


class TestLongSumAggregator(unittest.TestCase):
    def test_init(self):
        agg = LongSumAggregator("bytes")
        self.assertEqual(agg.attribute, "bytes")

    def test_to_dict(self):
        agg = LongSumAggregator("size")
        result = agg.to_dict()
        self.assertEqual(result, {"type": "long_sum", "attribute": "size"})

    def test_repr(self):
        agg = LongSumAggregator("total")
        self.assertEqual(repr(agg), "LongSumAggregator('total')")


class TestLongAverageAggregator(unittest.TestCase):
    def test_init(self):
        agg = LongAverageAggregator("duration")
        self.assertEqual(agg.attribute, "duration")

    def test_to_dict(self):
        agg = LongAverageAggregator("latency")
        result = agg.to_dict()
        self.assertEqual(result, {"type": "long_average", "attribute": "latency"})

    def test_repr(self):
        agg = LongAverageAggregator("time")
        self.assertEqual(repr(agg), "LongAverageAggregator('time')")


class TestDoubleSumAggregator(unittest.TestCase):
    def test_init(self):
        agg = DoubleSumAggregator("amount")
        self.assertEqual(agg.attribute, "amount")

    def test_to_dict(self):
        agg = DoubleSumAggregator("price")
        result = agg.to_dict()
        self.assertEqual(result, {"type": "double_sum", "attribute": "price"})

    def test_repr(self):
        agg = DoubleSumAggregator("total")
        self.assertEqual(repr(agg), "DoubleSumAggregator('total')")


class TestDoubleAverageAggregator(unittest.TestCase):
    def test_init(self):
        agg = DoubleAverageAggregator("rate")
        self.assertEqual(agg.attribute, "rate")

    def test_to_dict(self):
        agg = DoubleAverageAggregator("percentage")
        result = agg.to_dict()
        self.assertEqual(result, {"type": "double_average", "attribute": "percentage"})

    def test_repr(self):
        agg = DoubleAverageAggregator("ratio")
        self.assertEqual(repr(agg), "DoubleAverageAggregator('ratio')")


class TestFixedPointSumAggregator(unittest.TestCase):
    def test_init(self):
        agg = FixedPointSumAggregator("decimal_value")
        self.assertEqual(agg.attribute, "decimal_value")

    def test_to_dict(self):
        agg = FixedPointSumAggregator("currency")
        result = agg.to_dict()
        self.assertEqual(result, {"type": "fixed_point_sum", "attribute": "currency"})

    def test_repr(self):
        agg = FixedPointSumAggregator("balance")
        self.assertEqual(repr(agg), "FixedPointSumAggregator('balance')")


class TestFloatingPointSumAggregator(unittest.TestCase):
    def test_init(self):
        agg = FloatingPointSumAggregator("measurement")
        self.assertEqual(agg.attribute, "measurement")

    def test_to_dict(self):
        agg = FloatingPointSumAggregator("temperature")
        result = agg.to_dict()
        self.assertEqual(result, {"type": "floating_point_sum", "attribute": "temperature"})

    def test_repr(self):
        agg = FloatingPointSumAggregator("weight")
        self.assertEqual(repr(agg), "FloatingPointSumAggregator('weight')")


class TestAggregatorFactoryFunctions(unittest.TestCase):
    def test_count_without_attribute(self):
        agg = count()
        self.assertIsInstance(agg, CountAggregator)
        self.assertIsNone(agg.attribute)

    def test_count_with_attribute(self):
        agg = count("name")
        self.assertIsInstance(agg, CountAggregator)
        self.assertEqual(agg.attribute, "name")

    def test_distinct(self):
        agg = distinct("category")
        self.assertIsInstance(agg, DistinctValuesAggregator)
        self.assertEqual(agg.attribute, "category")

    def test_sum_(self):
        agg = sum_("amount")
        self.assertIsInstance(agg, SumAggregator)
        self.assertEqual(agg.attribute, "amount")

    def test_average(self):
        agg = average("score")
        self.assertIsInstance(agg, AverageAggregator)
        self.assertEqual(agg.attribute, "score")

    def test_min_(self):
        agg = min_("age")
        self.assertIsInstance(agg, MinAggregator)
        self.assertEqual(agg.attribute, "age")

    def test_max_(self):
        agg = max_("salary")
        self.assertIsInstance(agg, MaxAggregator)
        self.assertEqual(agg.attribute, "salary")

    def test_integer_sum(self):
        agg = integer_sum("count")
        self.assertIsInstance(agg, IntegerSumAggregator)
        self.assertEqual(agg.attribute, "count")

    def test_integer_average(self):
        agg = integer_average("rating")
        self.assertIsInstance(agg, IntegerAverageAggregator)
        self.assertEqual(agg.attribute, "rating")

    def test_long_sum(self):
        agg = long_sum("bytes")
        self.assertIsInstance(agg, LongSumAggregator)
        self.assertEqual(agg.attribute, "bytes")

    def test_long_average(self):
        agg = long_average("duration")
        self.assertIsInstance(agg, LongAverageAggregator)
        self.assertEqual(agg.attribute, "duration")

    def test_double_sum(self):
        agg = double_sum("price")
        self.assertIsInstance(agg, DoubleSumAggregator)
        self.assertEqual(agg.attribute, "price")

    def test_double_average(self):
        agg = double_average("rate")
        self.assertIsInstance(agg, DoubleAverageAggregator)
        self.assertEqual(agg.attribute, "rate")


class TestReExportedProjectionClasses(unittest.TestCase):
    def test_projection_class_exported(self):
        self.assertTrue(issubclass(Projection, ABC))

    def test_single_attribute_projection_exported(self):
        proj = SingleAttributeProjection("test")
        self.assertEqual(proj.attribute, "test")

    def test_multi_attribute_projection_exported(self):
        proj = MultiAttributeProjection("a", "b")
        self.assertEqual(proj.attributes, ["a", "b"])

    def test_identity_projection_exported(self):
        proj = IdentityProjection()
        self.assertEqual(proj.to_dict(), {"type": "identity"})


class TestReExportedProjectionFunctions(unittest.TestCase):
    def test_single_attribute_function(self):
        proj = single_attribute("name")
        self.assertIsInstance(proj, SingleAttributeProjection)

    def test_multi_attribute_function(self):
        proj = multi_attribute("a", "b", "c")
        self.assertIsInstance(proj, MultiAttributeProjection)

    def test_identity_function(self):
        proj = identity()
        self.assertIsInstance(proj, IdentityProjection)


if __name__ == "__main__":
    unittest.main()
