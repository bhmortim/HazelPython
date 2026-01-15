"""Unit tests for the predicate module."""

import unittest

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


class TestSqlPredicate(unittest.TestCase):
    def test_creation(self):
        p = SqlPredicate("age > 30")
        self.assertEqual(p.sql, "age > 30")

    def test_to_dict(self):
        p = SqlPredicate("name = 'John'")
        self.assertEqual(p.to_dict(), {"type": "sql", "sql": "name = 'John'"})

    def test_repr(self):
        p = SqlPredicate("active = true")
        self.assertEqual(repr(p), "SqlPredicate('active = true')")


class TestTruePredicate(unittest.TestCase):
    def test_to_dict(self):
        p = TruePredicate()
        self.assertEqual(p.to_dict(), {"type": "true"})

    def test_repr(self):
        p = TruePredicate()
        self.assertEqual(repr(p), "TruePredicate()")


class TestFalsePredicate(unittest.TestCase):
    def test_to_dict(self):
        p = FalsePredicate()
        self.assertEqual(p.to_dict(), {"type": "false"})

    def test_repr(self):
        p = FalsePredicate()
        self.assertEqual(repr(p), "FalsePredicate()")


class TestEqualPredicate(unittest.TestCase):
    def test_creation(self):
        p = EqualPredicate("name", "John")
        self.assertEqual(p.attribute, "name")
        self.assertEqual(p.value, "John")

    def test_to_dict(self):
        p = EqualPredicate("age", 30)
        self.assertEqual(p.to_dict(), {"type": "equal", "attribute": "age", "value": 30})

    def test_repr(self):
        p = EqualPredicate("status", "active")
        self.assertEqual(repr(p), "EqualPredicate('status', 'active')")


class TestNotEqualPredicate(unittest.TestCase):
    def test_creation(self):
        p = NotEqualPredicate("name", "John")
        self.assertEqual(p.attribute, "name")
        self.assertEqual(p.value, "John")

    def test_to_dict(self):
        p = NotEqualPredicate("status", "deleted")
        self.assertEqual(
            p.to_dict(), {"type": "not_equal", "attribute": "status", "value": "deleted"}
        )

    def test_repr(self):
        p = NotEqualPredicate("type", "admin")
        self.assertEqual(repr(p), "NotEqualPredicate('type', 'admin')")


class TestGreaterThanPredicate(unittest.TestCase):
    def test_creation(self):
        p = GreaterThanPredicate("age", 18)
        self.assertEqual(p.attribute, "age")
        self.assertEqual(p.value, 18)

    def test_to_dict(self):
        p = GreaterThanPredicate("score", 100)
        self.assertEqual(
            p.to_dict(), {"type": "greater_than", "attribute": "score", "value": 100}
        )

    def test_repr(self):
        p = GreaterThanPredicate("price", 50.0)
        self.assertEqual(repr(p), "GreaterThanPredicate('price', 50.0)")


class TestGreaterThanOrEqualPredicate(unittest.TestCase):
    def test_creation(self):
        p = GreaterThanOrEqualPredicate("age", 21)
        self.assertEqual(p.attribute, "age")
        self.assertEqual(p.value, 21)

    def test_to_dict(self):
        p = GreaterThanOrEqualPredicate("quantity", 10)
        self.assertEqual(
            p.to_dict(), {"type": "greater_equal", "attribute": "quantity", "value": 10}
        )

    def test_repr(self):
        p = GreaterThanOrEqualPredicate("level", 5)
        self.assertEqual(repr(p), "GreaterThanOrEqualPredicate('level', 5)")


class TestLessThanPredicate(unittest.TestCase):
    def test_creation(self):
        p = LessThanPredicate("age", 65)
        self.assertEqual(p.attribute, "age")
        self.assertEqual(p.value, 65)

    def test_to_dict(self):
        p = LessThanPredicate("price", 1000)
        self.assertEqual(
            p.to_dict(), {"type": "less_than", "attribute": "price", "value": 1000}
        )

    def test_repr(self):
        p = LessThanPredicate("weight", 100.5)
        self.assertEqual(repr(p), "LessThanPredicate('weight', 100.5)")


class TestLessThanOrEqualPredicate(unittest.TestCase):
    def test_creation(self):
        p = LessThanOrEqualPredicate("count", 50)
        self.assertEqual(p.attribute, "count")
        self.assertEqual(p.value, 50)

    def test_to_dict(self):
        p = LessThanOrEqualPredicate("rating", 5)
        self.assertEqual(
            p.to_dict(), {"type": "less_equal", "attribute": "rating", "value": 5}
        )

    def test_repr(self):
        p = LessThanOrEqualPredicate("priority", 3)
        self.assertEqual(repr(p), "LessThanOrEqualPredicate('priority', 3)")


class TestBetweenPredicate(unittest.TestCase):
    def test_creation(self):
        p = BetweenPredicate("age", 18, 65)
        self.assertEqual(p.attribute, "age")
        self.assertEqual(p.from_value, 18)
        self.assertEqual(p.to_value, 65)

    def test_to_dict(self):
        p = BetweenPredicate("price", 10.0, 100.0)
        self.assertEqual(
            p.to_dict(),
            {"type": "between", "attribute": "price", "from": 10.0, "to": 100.0},
        )

    def test_repr(self):
        p = BetweenPredicate("year", 2000, 2023)
        self.assertEqual(repr(p), "BetweenPredicate('year', 2000, 2023)")


class TestInPredicate(unittest.TestCase):
    def test_creation(self):
        p = InPredicate("status", "active", "pending", "approved")
        self.assertEqual(p.attribute, "status")
        self.assertEqual(p.values, ["active", "pending", "approved"])

    def test_to_dict(self):
        p = InPredicate("type", "A", "B", "C")
        self.assertEqual(
            p.to_dict(), {"type": "in", "attribute": "type", "values": ["A", "B", "C"]}
        )

    def test_repr(self):
        p = InPredicate("color", "red", "blue")
        self.assertEqual(repr(p), "InPredicate('color', ['red', 'blue'])")


class TestLikePredicate(unittest.TestCase):
    def test_creation(self):
        p = LikePredicate("name", "John%")
        self.assertEqual(p.attribute, "name")
        self.assertEqual(p.pattern, "John%")

    def test_to_dict(self):
        p = LikePredicate("email", "%@example.com")
        self.assertEqual(
            p.to_dict(),
            {"type": "like", "attribute": "email", "pattern": "%@example.com"},
        )

    def test_repr(self):
        p = LikePredicate("title", "%Manager%")
        self.assertEqual(repr(p), "LikePredicate('title', '%Manager%')")


class TestILikePredicate(unittest.TestCase):
    def test_creation(self):
        p = ILikePredicate("name", "john%")
        self.assertEqual(p.attribute, "name")
        self.assertEqual(p.pattern, "john%")

    def test_to_dict(self):
        p = ILikePredicate("city", "%york%")
        self.assertEqual(
            p.to_dict(), {"type": "ilike", "attribute": "city", "pattern": "%york%"}
        )

    def test_repr(self):
        p = ILikePredicate("country", "us%")
        self.assertEqual(repr(p), "ILikePredicate('country', 'us%')")


class TestRegexPredicate(unittest.TestCase):
    def test_creation(self):
        p = RegexPredicate("email", r"^[\w.]+@\w+\.\w+$")
        self.assertEqual(p.attribute, "email")
        self.assertEqual(p.pattern, r"^[\w.]+@\w+\.\w+$")

    def test_to_dict(self):
        p = RegexPredicate("phone", r"^\d{3}-\d{4}$")
        self.assertEqual(
            p.to_dict(),
            {"type": "regex", "attribute": "phone", "pattern": r"^\d{3}-\d{4}$"},
        )

    def test_repr(self):
        p = RegexPredicate("code", r"[A-Z]{2}\d{3}")
        self.assertEqual(repr(p), "RegexPredicate('code', '[A-Z]{2}\\\\d{3}')")


class TestInstanceOfPredicate(unittest.TestCase):
    def test_creation(self):
        p = InstanceOfPredicate("com.example.Person")
        self.assertEqual(p.class_name, "com.example.Person")

    def test_to_dict(self):
        p = InstanceOfPredicate("java.lang.String")
        self.assertEqual(
            p.to_dict(), {"type": "instance_of", "class_name": "java.lang.String"}
        )

    def test_repr(self):
        p = InstanceOfPredicate("com.example.Order")
        self.assertEqual(repr(p), "InstanceOfPredicate('com.example.Order')")


class TestAndPredicate(unittest.TestCase):
    def test_creation(self):
        p1 = EqualPredicate("status", "active")
        p2 = GreaterThanPredicate("age", 18)
        and_pred = AndPredicate(p1, p2)
        self.assertEqual(len(and_pred.predicates), 2)

    def test_to_dict(self):
        p1 = EqualPredicate("type", "user")
        p2 = TruePredicate()
        and_pred = AndPredicate(p1, p2)
        result = and_pred.to_dict()
        self.assertEqual(result["type"], "and")
        self.assertEqual(len(result["predicates"]), 2)

    def test_repr(self):
        p1 = TruePredicate()
        p2 = FalsePredicate()
        and_pred = AndPredicate(p1, p2)
        self.assertIn("AndPredicate", repr(and_pred))


class TestOrPredicate(unittest.TestCase):
    def test_creation(self):
        p1 = EqualPredicate("role", "admin")
        p2 = EqualPredicate("role", "superuser")
        or_pred = OrPredicate(p1, p2)
        self.assertEqual(len(or_pred.predicates), 2)

    def test_to_dict(self):
        p1 = LessThanPredicate("price", 10)
        p2 = GreaterThanPredicate("price", 100)
        or_pred = OrPredicate(p1, p2)
        result = or_pred.to_dict()
        self.assertEqual(result["type"], "or")
        self.assertEqual(len(result["predicates"]), 2)

    def test_repr(self):
        p1 = TruePredicate()
        or_pred = OrPredicate(p1)
        self.assertIn("OrPredicate", repr(or_pred))


class TestNotPredicate(unittest.TestCase):
    def test_creation(self):
        inner = EqualPredicate("deleted", True)
        not_pred = NotPredicate(inner)
        self.assertEqual(not_pred.predicate, inner)

    def test_to_dict(self):
        inner = FalsePredicate()
        not_pred = NotPredicate(inner)
        result = not_pred.to_dict()
        self.assertEqual(result["type"], "not")
        self.assertEqual(result["predicate"], {"type": "false"})

    def test_repr(self):
        inner = TruePredicate()
        not_pred = NotPredicate(inner)
        self.assertIn("NotPredicate", repr(not_pred))


class TestPagingPredicate(unittest.TestCase):
    def test_creation_default(self):
        p = PagingPredicate()
        self.assertIsNone(p.predicate)
        self.assertEqual(p.page_size, 10)
        self.assertEqual(p.page, 0)

    def test_creation_with_predicate(self):
        inner = EqualPredicate("active", True)
        p = PagingPredicate(predicate=inner, page_size=20)
        self.assertEqual(p.predicate, inner)
        self.assertEqual(p.page_size, 20)

    def test_page_navigation(self):
        p = PagingPredicate(page_size=10)
        self.assertEqual(p.page, 0)
        p.next_page()
        self.assertEqual(p.page, 1)
        p.next_page()
        self.assertEqual(p.page, 2)
        p.previous_page()
        self.assertEqual(p.page, 1)
        p.reset()
        self.assertEqual(p.page, 0)

    def test_previous_page_at_zero(self):
        p = PagingPredicate()
        p.previous_page()
        self.assertEqual(p.page, 0)

    def test_set_page(self):
        p = PagingPredicate()
        p.page = 5
        self.assertEqual(p.page, 5)

    def test_set_negative_page_raises(self):
        p = PagingPredicate()
        with self.assertRaises(ValueError):
            p.page = -1

    def test_to_dict_without_inner(self):
        p = PagingPredicate(page_size=25)
        p.page = 3
        result = p.to_dict()
        self.assertEqual(result["type"], "paging")
        self.assertEqual(result["page_size"], 25)
        self.assertEqual(result["page"], 3)
        self.assertNotIn("predicate", result)

    def test_to_dict_with_inner(self):
        inner = TruePredicate()
        p = PagingPredicate(predicate=inner, page_size=15)
        result = p.to_dict()
        self.assertEqual(result["predicate"], {"type": "true"})

    def test_repr(self):
        p = PagingPredicate(page_size=50)
        p.page = 2
        self.assertEqual(repr(p), "PagingPredicate(page=2, page_size=50)")


class TestPredicateBuilder(unittest.TestCase):
    def test_equal(self):
        builder = PredicateBuilder("name")
        p = builder.equal("John")
        self.assertIsInstance(p, EqualPredicate)
        self.assertEqual(p.attribute, "name")
        self.assertEqual(p.value, "John")

    def test_not_equal(self):
        builder = PredicateBuilder("status")
        p = builder.not_equal("deleted")
        self.assertIsInstance(p, NotEqualPredicate)

    def test_greater_than(self):
        builder = PredicateBuilder("age")
        p = builder.greater_than(18)
        self.assertIsInstance(p, GreaterThanPredicate)

    def test_greater_than_or_equal(self):
        builder = PredicateBuilder("score")
        p = builder.greater_than_or_equal(100)
        self.assertIsInstance(p, GreaterThanOrEqualPredicate)

    def test_less_than(self):
        builder = PredicateBuilder("price")
        p = builder.less_than(1000)
        self.assertIsInstance(p, LessThanPredicate)

    def test_less_than_or_equal(self):
        builder = PredicateBuilder("quantity")
        p = builder.less_than_or_equal(50)
        self.assertIsInstance(p, LessThanOrEqualPredicate)

    def test_between(self):
        builder = PredicateBuilder("year")
        p = builder.between(2000, 2023)
        self.assertIsInstance(p, BetweenPredicate)

    def test_is_in(self):
        builder = PredicateBuilder("type")
        p = builder.is_in("A", "B", "C")
        self.assertIsInstance(p, InPredicate)

    def test_like(self):
        builder = PredicateBuilder("name")
        p = builder.like("John%")
        self.assertIsInstance(p, LikePredicate)

    def test_ilike(self):
        builder = PredicateBuilder("email")
        p = builder.ilike("%@example.com")
        self.assertIsInstance(p, ILikePredicate)

    def test_regex(self):
        builder = PredicateBuilder("code")
        p = builder.regex(r"\d+")
        self.assertIsInstance(p, RegexPredicate)


class TestFactoryFunctions(unittest.TestCase):
    def test_attr(self):
        builder = attr("name")
        self.assertIsInstance(builder, PredicateBuilder)

    def test_sql(self):
        p = sql("age > 30 AND active = true")
        self.assertIsInstance(p, SqlPredicate)
        self.assertEqual(p.sql, "age > 30 AND active = true")

    def test_true(self):
        p = true()
        self.assertIsInstance(p, TruePredicate)

    def test_false(self):
        p = false()
        self.assertIsInstance(p, FalsePredicate)

    def test_and_(self):
        p1 = true()
        p2 = false()
        p = and_(p1, p2)
        self.assertIsInstance(p, AndPredicate)

    def test_or_(self):
        p1 = true()
        p2 = false()
        p = or_(p1, p2)
        self.assertIsInstance(p, OrPredicate)

    def test_not_(self):
        p = not_(true())
        self.assertIsInstance(p, NotPredicate)

    def test_paging(self):
        p = paging(page_size=20)
        self.assertIsInstance(p, PagingPredicate)
        self.assertEqual(p.page_size, 20)


class TestComplexPredicates(unittest.TestCase):
    def test_nested_and_or(self):
        p = and_(
            or_(
                attr("status").equal("active"),
                attr("status").equal("pending"),
            ),
            attr("age").greater_than(18),
        )
        result = p.to_dict()
        self.assertEqual(result["type"], "and")
        self.assertEqual(len(result["predicates"]), 2)
        self.assertEqual(result["predicates"][0]["type"], "or")

    def test_double_negation(self):
        p = not_(not_(true()))
        result = p.to_dict()
        self.assertEqual(result["type"], "not")
        self.assertEqual(result["predicate"]["type"], "not")
        self.assertEqual(result["predicate"]["predicate"]["type"], "true")


if __name__ == "__main__":
    unittest.main()
