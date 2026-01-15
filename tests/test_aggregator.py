"""Unit tests for the aggregator module."""

import unittest

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


class TestCountAggregator(unittest.TestCase):
    def test_creation_no_attribute(self):
        agg = CountAggregator()
        self.assertIsNone(agg.attribute)

    def test_creation_with_attribute(self):
        agg = CountAggregator("name")
        self.assertEqual(agg.attribute, "name")

    def test_to_dict_no_attribute(self):
        agg = CountAggregator()
        self.assertEqual(agg.to_dict(), {"type": "count"})

    def test_to_dict_with_attribute(self):
        agg = CountAggregator("status")
        self.assertEqual(agg.to_dict(), {"type": "count", "attribute": "status"})

    def test_repr_no_attribute(self):
        agg = CountAggregator()
        self.assertEqual(repr(agg), "CountAggregator()")

    def test_repr_with_attribute(self):
        agg = CountAggregator("id")
        self.assertEqual(repr(agg), "CountAggregator('id')")


class TestDistinctValuesAggregator(unittest.TestCase):
    def test_creation(self):
        agg = DistinctValuesAggregator("category")
        self.assertEqual(agg.attribute, "category")

    def test_to_dict(self):
        agg = DistinctValuesAggregator("type")
        self.assertEqual(agg.to_dict(), {"type": "distinct", "attribute": "type"})

    def test_repr(self):
        agg = DistinctValuesAggregator("status")
        self.assertEqual(repr(agg), "DistinctValuesAggregator('status')")


class TestSumAggregator(unittest.TestCase):
    def test_creation(self):
        agg = SumAggregator("price")
        self.assertEqual(agg.attribute, "price")

    def test_to_dict(self):
        agg = SumAggregator("amount")
        self.assertEqual(agg.to_dict(), {"type": "sum", "attribute": "amount"})

    def test_repr(self):
        agg = SumAggregator("total")
        self.assertEqual(repr(agg), "SumAggregator('total')")


class TestAverageAggregator(unittest.TestCase):
    def test_creation(self):
        agg = AverageAggregator("score")
        self.assertEqual(agg.attribute, "score")

    def test_to_dict(self):
        agg = AverageAggregator("rating")
        self.assertEqual(agg.to_dict(), {"type": "average", "attribute": "rating"})

    def test_repr(self):
        agg = AverageAggregator("grade")
        self.assertEqual(repr(agg), "AverageAggregator('grade')")


class TestMinAggregator(unittest.TestCase):
    def test_creation(self):
        agg = MinAggregator("price")
        self.assertEqual(agg.attribute, "price")

    def test_to_dict(self):
        agg = MinAggregator("timestamp")
        self.assertEqual(agg.to_dict(), {"type": "min", "attribute": "timestamp"})

    def test_repr(self):
        agg = MinAggregator("age")
        self.assertEqual(repr(agg), "MinAggregator('age')")


class TestMaxAggregator(unittest.TestCase):
    def test_creation(self):
        agg = MaxAggregator("salary")
        self.assertEqual(agg.attribute, "salary")

    def test_to_dict(self):
        agg = MaxAggregator("value")
        self.assertEqual(agg.to_dict(), {"type": "max", "attribute": "value"})

    def test_repr(self):
        agg = MaxAggregator("count")
        self.assertEqual(repr(agg), "MaxAggregator('count')")


class TestIntegerSumAggregator(unittest.TestCase):
    def test_creation(self):
        agg = IntegerSumAggregator("quantity")
        self.assertEqual(agg.attribute, "quantity")

    def test_to_dict(self):
        agg = IntegerSumAggregator("items")
        self.assertEqual(agg.to_dict(), {"type": "integer_sum", "attribute": "items"})

    def test_repr(self):
        agg = IntegerSumAggregator("units")
        self.assertEqual(repr(agg), "IntegerSumAggregator('units')")


class TestIntegerAverageAggregator(unittest.TestCase):
    def test_creation(self):
        agg = IntegerAverageAggregator("count")
        self.assertEqual(agg.attribute, "count")

    def test_to_dict(self):
        agg = IntegerAverageAggregator("level")
        self.assertEqual(
            agg.to_dict(), {"type": "integer_average", "attribute": "level"}
        )

    def test_repr(self):
        agg = IntegerAverageAggregator("rank")
        self.assertEqual(repr(agg), "IntegerAverageAggregator('rank')")


class TestLongSumAggregator(unittest.TestCase):
    def test_creation(self):
        agg = LongSumAggregator("bytes")
        self.assertEqual(agg.attribute, "bytes")

    def test_to_dict(self):
        agg = LongSumAggregator("size")
        self.assertEqual(agg.to_dict(), {"type": "long_sum", "attribute": "size"})

    def test_repr(self):
        agg = LongSumAggregator("offset")
        self.assertEqual(repr(agg), "LongSumAggregator('offset')")


class TestLongAverageAggregator(unittest.TestCase):
    def test_creation(self):
        agg = LongAverageAggregator("duration")
        self.assertEqual(agg.attribute, "duration")

    def test_to_dict(self):
        agg = LongAverageAggregator("latency")
        self.assertEqual(agg.to_dict(), {"type": "long_average", "attribute": "latency"})

    def test_repr(self):
        agg = LongAverageAggregator("time")
        self.assertEqual(repr(agg), "LongAverageAggregator('time')")


class TestDoubleSumAggregator(unittest.TestCase):
    def test_creation(self):
        agg = DoubleSumAggregator("price")
        self.assertEqual(agg.attribute, "price")

    def test_to_dict(self):
        agg = DoubleSumAggregator("cost")
        self.assertEqual(agg.to_dict(), {"type": "double_sum", "attribute": "cost"})

    def test_repr(self):
        agg = DoubleSumAggregator("value")
        self.assertEqual(repr(agg), "DoubleSumAggregator('value')")


class TestDoubleAverageAggregator(unittest.TestCase):
    def test_creation(self):
        agg = DoubleAverageAggregator("rate")
        self.assertEqual(agg.attribute, "rate")

    def test_to_dict(self):
        agg = DoubleAverageAggregator("percentage")
        self.assertEqual(
            agg.to_dict(), {"type": "double_average", "attribute": "percentage"}
        )

    def test_repr(self):
        agg = DoubleAverageAggregator("ratio")
        self.assertEqual(repr(agg), "DoubleAverageAggregator('ratio')")


class TestFixedPointSumAggregator(unittest.TestCase):
    def test_creation(self):
        agg = FixedPointSumAggregator("amount")
        self.assertEqual(agg.attribute, "amount")

    def test_to_dict(self):
        agg = FixedPointSumAggregator("balance")
        self.assertEqual(
            agg.to_dict(), {"type": "fixed_point_sum", "attribute": "balance"}
        )

    def test_repr(self):
        agg = FixedPointSumAggregator("total")
        self.assertEqual(repr(agg), "FixedPointSumAggregator('total')")


class TestFloatingPointSumAggregator(unittest.TestCase):
    def test_creation(self):
        agg = FloatingPointSumAggregator("measurement")
        self.assertEqual(agg.attribute, "measurement")

    def test_to_dict(self):
        agg = FloatingPointSumAggregator("reading")
        self.assertEqual(
            agg.to_dict(), {"type": "floating_point_sum", "attribute": "reading"}
        )

    def test_repr(self):
        agg = FloatingPointSumAggregator("metric")
        self.assertEqual(repr(agg), "FloatingPointSumAggregator('metric')")


class TestSingleAttributeProjection(unittest.TestCase):
    def test_creation(self):
        proj = SingleAttributeProjection("name")
        self.assertEqual(proj.attribute, "name")

    def test_to_dict(self):
        proj = SingleAttributeProjection("email")
        self.assertEqual(
            proj.to_dict(), {"type": "single_attribute", "attribute": "email"}
        )

    def test_repr(self):
        proj = SingleAttributeProjection("id")
        self.assertEqual(repr(proj), "SingleAttributeProjection('id')")


class TestMultiAttributeProjection(unittest.TestCase):
    def test_creation(self):
        proj = MultiAttributeProjection("name", "email", "age")
        self.assertEqual(proj.attributes, ["name", "email", "age"])

    def test_to_dict(self):
        proj = MultiAttributeProjection("first_name", "last_name")
        self.assertEqual(
            proj.to_dict(),
            {"type": "multi_attribute", "attributes": ["first_name", "last_name"]},
        )

    def test_repr(self):
        proj = MultiAttributeProjection("a", "b")
        self.assertEqual(repr(proj), "MultiAttributeProjection(['a', 'b'])")


class TestIdentityProjection(unittest.TestCase):
    def test_to_dict(self):
        proj = IdentityProjection()
        self.assertEqual(proj.to_dict(), {"type": "identity"})

    def test_repr(self):
        proj = IdentityProjection()
        self.assertEqual(repr(proj), "IdentityProjection()")


class TestAggregatorFactoryFunctions(unittest.TestCase):
    def test_count_no_attribute(self):
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
        agg = sum_("price")
        self.assertIsInstance(agg, SumAggregator)
        self.assertEqual(agg.attribute, "price")

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

    def test_integer_average(self):
        agg = integer_average("level")
        self.assertIsInstance(agg, IntegerAverageAggregator)

    def test_long_sum(self):
        agg = long_sum("bytes")
        self.assertIsInstance(agg, LongSumAggregator)

    def test_long_average(self):
        agg = long_average("duration")
        self.assertIsInstance(agg, LongAverageAggregator)

    def test_double_sum(self):
        agg = double_sum("price")
        self.assertIsInstance(agg, DoubleSumAggregator)

    def test_double_average(self):
        agg = double_average("rate")
        self.assertIsInstance(agg, DoubleAverageAggregator)


class TestProjectionFactoryFunctions(unittest.TestCase):
    def test_single_attribute(self):
        proj = single_attribute("name")
        self.assertIsInstance(proj, SingleAttributeProjection)
        self.assertEqual(proj.attribute, "name")

    def test_multi_attribute(self):
        proj = multi_attribute("name", "email", "age")
        self.assertIsInstance(proj, MultiAttributeProjection)
        self.assertEqual(proj.attributes, ["name", "email", "age"])

    def test_identity(self):
        proj = identity()
        self.assertIsInstance(proj, IdentityProjection)


if __name__ == "__main__":
    unittest.main()
