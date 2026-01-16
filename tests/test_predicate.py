"""Unit tests for hazelcast/predicate.py"""

import unittest
from abc import ABC

from hazelcast.predicate import (
    Predicate,
    SqlPredicate,
    TruePredicate,
    FalsePredicate,
    EqualPredicate,
    NotEqualPredicate,
    GreaterThanPredicate,
    GreaterThanOrEqualPredicate,
    LessThanPredicate,
    LessThanOrEqualPredicate,
    BetweenPredicate,
    InPredicate,
    LikePredicate,
    ILikePredicate,
    RegexPredicate,
    InstanceOfPredicate,
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


class TestPredicateABC(unittest.TestCase):
    def test_predicate_is_abstract(self):
        self.assertTrue(issubclass(Predicate, ABC))

    def test_predicate_cannot_be_instantiated(self):
        with self.assertRaises(TypeError):
            Predicate()

    def test_predicate_has_to_dict_abstract_method(self):
        self.assertIn("to_dict", Predicate.__abstractmethods__)


class TestSqlPredicate(unittest.TestCase):
    def test_init(self):
        pred = SqlPredicate("name = 'John'")
        self.assertEqual(pred.sql, "name = 'John'")

    def test_sql_property(self):
        pred = SqlPredicate("age > 30")
        self.assertEqual(pred.sql, "age > 30")

    def test_to_dict(self):
        pred = SqlPredicate("active = true")
        result = pred.to_dict()
        self.assertEqual(result, {"type": "sql", "sql": "active = true"})

    def test_repr(self):
        pred = SqlPredicate("id = 1")
        self.assertEqual(repr(pred), "SqlPredicate('id = 1')")


class TestTruePredicate(unittest.TestCase):
    def test_to_dict(self):
        pred = TruePredicate()
        self.assertEqual(pred.to_dict(), {"type": "true"})

    def test_repr(self):
        pred = TruePredicate()
        self.assertEqual(repr(pred), "TruePredicate()")


class TestFalsePredicate(unittest.TestCase):
    def test_to_dict(self):
        pred = FalsePredicate()
        self.assertEqual(pred.to_dict(), {"type": "false"})

    def test_repr(self):
        pred = FalsePredicate()
        self.assertEqual(repr(pred), "FalsePredicate()")


class TestEqualPredicate(unittest.TestCase):
    def test_init(self):
        pred = EqualPredicate("name", "John")
        self.assertEqual(pred.attribute, "name")
        self.assertEqual(pred.value, "John")

    def test_to_dict(self):
        pred = EqualPredicate("age", 25)
        result = pred.to_dict()
        self.assertEqual(result, {"type": "equal", "attribute": "age", "value": 25})

    def test_repr(self):
        pred = EqualPredicate("status", "active")
        self.assertEqual(repr(pred), "EqualPredicate('status', 'active')")


class TestNotEqualPredicate(unittest.TestCase):
    def test_init(self):
        pred = NotEqualPredicate("name", "John")
        self.assertEqual(pred.attribute, "name")
        self.assertEqual(pred.value, "John")

    def test_to_dict(self):
        pred = NotEqualPredicate("status", "inactive")
        result = pred.to_dict()
        self.assertEqual(result, {"type": "not_equal", "attribute": "status", "value": "inactive"})

    def test_repr(self):
        pred = NotEqualPredicate("id", 0)
        self.assertEqual(repr(pred), "NotEqualPredicate('id', 0)")


class TestGreaterThanPredicate(unittest.TestCase):
    def test_init(self):
        pred = GreaterThanPredicate("age", 18)
        self.assertEqual(pred.attribute, "age")
        self.assertEqual(pred.value, 18)

    def test_to_dict(self):
        pred = GreaterThanPredicate("price", 100.0)
        result = pred.to_dict()
        self.assertEqual(result, {"type": "greater_than", "attribute": "price", "value": 100.0})

    def test_repr(self):
        pred = GreaterThanPredicate("count", 5)
        self.assertEqual(repr(pred), "GreaterThanPredicate('count', 5)")


class TestGreaterThanOrEqualPredicate(unittest.TestCase):
    def test_init(self):
        pred = GreaterThanOrEqualPredicate("age", 18)
        self.assertEqual(pred.attribute, "age")
        self.assertEqual(pred.value, 18)

    def test_to_dict(self):
        pred = GreaterThanOrEqualPredicate("score", 60)
        result = pred.to_dict()
        self.assertEqual(result, {"type": "greater_equal", "attribute": "score", "value": 60})

    def test_repr(self):
        pred = GreaterThanOrEqualPredicate("level", 1)
        self.assertEqual(repr(pred), "GreaterThanOrEqualPredicate('level', 1)")


class TestLessThanPredicate(unittest.TestCase):
    def test_init(self):
        pred = LessThanPredicate("age", 65)
        self.assertEqual(pred.attribute, "age")
        self.assertEqual(pred.value, 65)

    def test_to_dict(self):
        pred = LessThanPredicate("price", 50.0)
        result = pred.to_dict()
        self.assertEqual(result, {"type": "less_than", "attribute": "price", "value": 50.0})

    def test_repr(self):
        pred = LessThanPredicate("quantity", 10)
        self.assertEqual(repr(pred), "LessThanPredicate('quantity', 10)")


class TestLessThanOrEqualPredicate(unittest.TestCase):
    def test_init(self):
        pred = LessThanOrEqualPredicate("age", 65)
        self.assertEqual(pred.attribute, "age")
        self.assertEqual(pred.value, 65)

    def test_to_dict(self):
        pred = LessThanOrEqualPredicate("weight", 100)
        result = pred.to_dict()
        self.assertEqual(result, {"type": "less_equal", "attribute": "weight", "value": 100})

    def test_repr(self):
        pred = LessThanOrEqualPredicate("priority", 5)
        self.assertEqual(repr(pred), "LessThanOrEqualPredicate('priority', 5)")


class TestBetweenPredicate(unittest.TestCase):
    def test_init(self):
        pred = BetweenPredicate("age", 18, 65)
        self.assertEqual(pred.attribute, "age")
        self.assertEqual(pred.from_value, 18)
        self.assertEqual(pred.to_value, 65)

    def test_to_dict(self):
        pred = BetweenPredicate("price", 10.0, 100.0)
        result = pred.to_dict()
        self.assertEqual(result, {
            "type": "between",
            "attribute": "price",
            "from": 10.0,
            "to": 100.0,
        })

    def test_repr(self):
        pred = BetweenPredicate("score", 0, 100)
        self.assertEqual(repr(pred), "BetweenPredicate('score', 0, 100)")


class TestInPredicate(unittest.TestCase):
    def test_init_with_values(self):
        pred = InPredicate("status", "active", "pending", "completed")
        self.assertEqual(pred.attribute, "status")
        self.assertEqual(pred.values, ["active", "pending", "completed"])

    def test_init_empty_values(self):
        pred = InPredicate("status")
        self.assertEqual(pred.values, [])

    def test_to_dict(self):
        pred = InPredicate("color", "red", "green", "blue")
        result = pred.to_dict()
        self.assertEqual(result, {
            "type": "in",
            "attribute": "color",
            "values": ["red", "green", "blue"],
        })

    def test_repr(self):
        pred = InPredicate("id", 1, 2, 3)
        self.assertEqual(repr(pred), "InPredicate('id', [1, 2, 3])")


class TestLikePredicate(unittest.TestCase):
    def test_init(self):
        pred = LikePredicate("name", "John%")
        self.assertEqual(pred.attribute, "name")
        self.assertEqual(pred.pattern, "John%")

    def test_to_dict(self):
        pred = LikePredicate("email", "%@example.com")
        result = pred.to_dict()
        self.assertEqual(result, {"type": "like", "attribute": "email", "pattern": "%@example.com"})

    def test_repr(self):
        pred = LikePredicate("title", "%Manager%")
        self.assertEqual(repr(pred), "LikePredicate('title', '%Manager%')")


class TestILikePredicate(unittest.TestCase):
    def test_init(self):
        pred = ILikePredicate("name", "john%")
        self.assertEqual(pred.attribute, "name")
        self.assertEqual(pred.pattern, "john%")

    def test_to_dict(self):
        pred = ILikePredicate("city", "%york%")
        result = pred.to_dict()
        self.assertEqual(result, {"type": "ilike", "attribute": "city", "pattern": "%york%"})

    def test_repr(self):
        pred = ILikePredicate("country", "us%")
        self.assertEqual(repr(pred), "ILikePredicate('country', 'us%')")


class TestRegexPredicate(unittest.TestCase):
    def test_init(self):
        pred = RegexPredicate("email", r"^[\w.]+@[\w.]+$")
        self.assertEqual(pred.attribute, "email")
        self.assertEqual(pred.pattern, r"^[\w.]+@[\w.]+$")

    def test_to_dict(self):
        pred = RegexPredicate("phone", r"\d{3}-\d{4}")
        result = pred.to_dict()
        self.assertEqual(result, {"type": "regex", "attribute": "phone", "pattern": r"\d{3}-\d{4}"})

    def test_repr(self):
        pred = RegexPredicate("code", r"[A-Z]{3}")
        self.assertEqual(repr(pred), "RegexPredicate('code', '[A-Z]{3}')")


class TestInstanceOfPredicate(unittest.TestCase):
    def test_init(self):
        pred = InstanceOfPredicate("com.example.User")
        self.assertEqual(pred.class_name, "com.example.User")

    def test_to_dict(self):
        pred = InstanceOfPredicate("java.lang.String")
        result = pred.to_dict()
        self.assertEqual(result, {"type": "instance_of", "class_name": "java.lang.String"})

    def test_repr(self):
        pred = InstanceOfPredicate("MyClass")
        self.assertEqual(repr(pred), "InstanceOfPredicate('MyClass')")


class TestAndPredicate(unittest.TestCase):
    def test_init_with_predicates(self):
        p1 = EqualPredicate("a", 1)
        p2 = EqualPredicate("b", 2)
        pred = AndPredicate(p1, p2)
        self.assertEqual(len(pred.predicates), 2)

    def test_init_empty(self):
        pred = AndPredicate()
        self.assertEqual(pred.predicates, [])

    def test_to_dict_recursive(self):
        p1 = EqualPredicate("x", 10)
        p2 = GreaterThanPredicate("y", 5)
        pred = AndPredicate(p1, p2)
        result = pred.to_dict()
        self.assertEqual(result["type"], "and")
        self.assertEqual(len(result["predicates"]), 2)
        self.assertEqual(result["predicates"][0], {"type": "equal", "attribute": "x", "value": 10})
        self.assertEqual(result["predicates"][1], {"type": "greater_than", "attribute": "y", "value": 5})

    def test_repr(self):
        p1 = TruePredicate()
        pred = AndPredicate(p1)
        self.assertIn("AndPredicate", repr(pred))


class TestOrPredicate(unittest.TestCase):
    def test_init_with_predicates(self):
        p1 = EqualPredicate("a", 1)
        p2 = EqualPredicate("a", 2)
        pred = OrPredicate(p1, p2)
        self.assertEqual(len(pred.predicates), 2)

    def test_to_dict_recursive(self):
        p1 = LessThanPredicate("x", 0)
        p2 = GreaterThanPredicate("x", 100)
        pred = OrPredicate(p1, p2)
        result = pred.to_dict()
        self.assertEqual(result["type"], "or")
        self.assertEqual(len(result["predicates"]), 2)

    def test_repr(self):
        pred = OrPredicate()
        self.assertIn("OrPredicate", repr(pred))


class TestNotPredicate(unittest.TestCase):
    def test_init(self):
        inner = EqualPredicate("active", False)
        pred = NotPredicate(inner)
        self.assertEqual(pred.predicate, inner)

    def test_to_dict_nested(self):
        inner = TruePredicate()
        pred = NotPredicate(inner)
        result = pred.to_dict()
        self.assertEqual(result, {"type": "not", "predicate": {"type": "true"}})

    def test_repr(self):
        inner = FalsePredicate()
        pred = NotPredicate(inner)
        self.assertIn("NotPredicate", repr(pred))


class TestPagingPredicate(unittest.TestCase):
    def test_init_defaults(self):
        pred = PagingPredicate()
        self.assertIsNone(pred.predicate)
        self.assertEqual(pred.page_size, 10)
        self.assertEqual(pred.page, 0)

    def test_init_with_predicate(self):
        inner = EqualPredicate("active", True)
        pred = PagingPredicate(predicate=inner, page_size=25)
        self.assertEqual(pred.predicate, inner)
        self.assertEqual(pred.page_size, 25)

    def test_page_setter(self):
        pred = PagingPredicate()
        pred.page = 5
        self.assertEqual(pred.page, 5)

    def test_page_setter_negative_raises(self):
        pred = PagingPredicate()
        with self.assertRaises(ValueError) as ctx:
            pred.page = -1
        self.assertIn("negative", str(ctx.exception).lower())

    def test_next_page(self):
        pred = PagingPredicate()
        result = pred.next_page()
        self.assertEqual(pred.page, 1)
        self.assertIs(result, pred)

    def test_previous_page(self):
        pred = PagingPredicate()
        pred.page = 3
        result = pred.previous_page()
        self.assertEqual(pred.page, 2)
        self.assertIs(result, pred)

    def test_previous_page_at_zero(self):
        pred = PagingPredicate()
        pred.previous_page()
        self.assertEqual(pred.page, 0)

    def test_reset(self):
        pred = PagingPredicate()
        pred.page = 10
        result = pred.reset()
        self.assertEqual(pred.page, 0)
        self.assertIs(result, pred)

    def test_to_dict_without_inner_predicate(self):
        pred = PagingPredicate(page_size=20)
        pred.page = 2
        result = pred.to_dict()
        self.assertEqual(result, {"type": "paging", "page_size": 20, "page": 2})

    def test_to_dict_with_inner_predicate(self):
        inner = EqualPredicate("type", "user")
        pred = PagingPredicate(predicate=inner, page_size=15)
        result = pred.to_dict()
        self.assertEqual(result["type"], "paging")
        self.assertEqual(result["page_size"], 15)
        self.assertEqual(result["predicate"], {"type": "equal", "attribute": "type", "value": "user"})

    def test_repr(self):
        pred = PagingPredicate(page_size=50)
        pred.page = 3
        self.assertEqual(repr(pred), "PagingPredicate(page=3, page_size=50)")


class TestPredicateBuilder(unittest.TestCase):
    def test_init(self):
        builder = PredicateBuilder("name")
        self.assertEqual(builder._attribute, "name")

    def test_equal(self):
        builder = PredicateBuilder("age")
        pred = builder.equal(25)
        self.assertIsInstance(pred, EqualPredicate)
        self.assertEqual(pred.attribute, "age")
        self.assertEqual(pred.value, 25)

    def test_not_equal(self):
        builder = PredicateBuilder("status")
        pred = builder.not_equal("deleted")
        self.assertIsInstance(pred, NotEqualPredicate)

    def test_greater_than(self):
        builder = PredicateBuilder("price")
        pred = builder.greater_than(100)
        self.assertIsInstance(pred, GreaterThanPredicate)

    def test_greater_than_or_equal(self):
        builder = PredicateBuilder("score")
        pred = builder.greater_than_or_equal(60)
        self.assertIsInstance(pred, GreaterThanOrEqualPredicate)

    def test_less_than(self):
        builder = PredicateBuilder("count")
        pred = builder.less_than(10)
        self.assertIsInstance(pred, LessThanPredicate)

    def test_less_than_or_equal(self):
        builder = PredicateBuilder("weight")
        pred = builder.less_than_or_equal(100)
        self.assertIsInstance(pred, LessThanOrEqualPredicate)

    def test_between(self):
        builder = PredicateBuilder("age")
        pred = builder.between(18, 65)
        self.assertIsInstance(pred, BetweenPredicate)
        self.assertEqual(pred.from_value, 18)
        self.assertEqual(pred.to_value, 65)

    def test_is_in(self):
        builder = PredicateBuilder("color")
        pred = builder.is_in("red", "green", "blue")
        self.assertIsInstance(pred, InPredicate)
        self.assertEqual(pred.values, ["red", "green", "blue"])

    def test_like(self):
        builder = PredicateBuilder("name")
        pred = builder.like("J%")
        self.assertIsInstance(pred, LikePredicate)

    def test_ilike(self):
        builder = PredicateBuilder("name")
        pred = builder.ilike("j%")
        self.assertIsInstance(pred, ILikePredicate)

    def test_regex(self):
        builder = PredicateBuilder("email")
        pred = builder.regex(r".*@.*")
        self.assertIsInstance(pred, RegexPredicate)


class TestFactoryFunctions(unittest.TestCase):
    def test_attr(self):
        builder = attr("name")
        self.assertIsInstance(builder, PredicateBuilder)
        self.assertEqual(builder._attribute, "name")

    def test_sql_function(self):
        pred = sql("age > 18")
        self.assertIsInstance(pred, SqlPredicate)
        self.assertEqual(pred.sql, "age > 18")

    def test_true_function(self):
        pred = true()
        self.assertIsInstance(pred, TruePredicate)

    def test_false_function(self):
        pred = false()
        self.assertIsInstance(pred, FalsePredicate)

    def test_and_function(self):
        p1 = TruePredicate()
        p2 = FalsePredicate()
        pred = and_(p1, p2)
        self.assertIsInstance(pred, AndPredicate)
        self.assertEqual(len(pred.predicates), 2)

    def test_or_function(self):
        p1 = TruePredicate()
        p2 = FalsePredicate()
        pred = or_(p1, p2)
        self.assertIsInstance(pred, OrPredicate)

    def test_not_function(self):
        inner = TruePredicate()
        pred = not_(inner)
        self.assertIsInstance(pred, NotPredicate)

    def test_paging_function(self):
        pred = paging(page_size=20)
        self.assertIsInstance(pred, PagingPredicate)
        self.assertEqual(pred.page_size, 20)

    def test_paging_function_with_predicate(self):
        inner = EqualPredicate("active", True)
        pred = paging(predicate=inner, page_size=15, comparator="test")
        self.assertIsInstance(pred, PagingPredicate)
        self.assertEqual(pred.predicate, inner)


if __name__ == "__main__":
    unittest.main()
