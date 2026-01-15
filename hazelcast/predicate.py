"""Predicate classes for filtering distributed data structure entries."""

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Pattern, Set
import re


class Predicate(ABC):
    """Base class for all predicates."""

    @abstractmethod
    def to_dict(self) -> dict:
        """Convert the predicate to a dictionary for serialization."""
        pass


class SqlPredicate(Predicate):
    """Predicate using SQL-like syntax."""

    def __init__(self, sql: str):
        self._sql = sql

    @property
    def sql(self) -> str:
        return self._sql

    def to_dict(self) -> dict:
        return {"type": "sql", "sql": self._sql}

    def __repr__(self) -> str:
        return f"SqlPredicate({self._sql!r})"


class TruePredicate(Predicate):
    """Predicate that always evaluates to true."""

    def to_dict(self) -> dict:
        return {"type": "true"}

    def __repr__(self) -> str:
        return "TruePredicate()"


class FalsePredicate(Predicate):
    """Predicate that always evaluates to false."""

    def to_dict(self) -> dict:
        return {"type": "false"}

    def __repr__(self) -> str:
        return "FalsePredicate()"


class EqualPredicate(Predicate):
    """Predicate for equality comparison."""

    def __init__(self, attribute: str, value: Any):
        self._attribute = attribute
        self._value = value

    @property
    def attribute(self) -> str:
        return self._attribute

    @property
    def value(self) -> Any:
        return self._value

    def to_dict(self) -> dict:
        return {"type": "equal", "attribute": self._attribute, "value": self._value}

    def __repr__(self) -> str:
        return f"EqualPredicate({self._attribute!r}, {self._value!r})"


class NotEqualPredicate(Predicate):
    """Predicate for inequality comparison."""

    def __init__(self, attribute: str, value: Any):
        self._attribute = attribute
        self._value = value

    @property
    def attribute(self) -> str:
        return self._attribute

    @property
    def value(self) -> Any:
        return self._value

    def to_dict(self) -> dict:
        return {"type": "not_equal", "attribute": self._attribute, "value": self._value}

    def __repr__(self) -> str:
        return f"NotEqualPredicate({self._attribute!r}, {self._value!r})"


class GreaterThanPredicate(Predicate):
    """Predicate for greater-than comparison."""

    def __init__(self, attribute: str, value: Any):
        self._attribute = attribute
        self._value = value

    @property
    def attribute(self) -> str:
        return self._attribute

    @property
    def value(self) -> Any:
        return self._value

    def to_dict(self) -> dict:
        return {"type": "greater_than", "attribute": self._attribute, "value": self._value}

    def __repr__(self) -> str:
        return f"GreaterThanPredicate({self._attribute!r}, {self._value!r})"


class GreaterThanOrEqualPredicate(Predicate):
    """Predicate for greater-than-or-equal comparison."""

    def __init__(self, attribute: str, value: Any):
        self._attribute = attribute
        self._value = value

    @property
    def attribute(self) -> str:
        return self._attribute

    @property
    def value(self) -> Any:
        return self._value

    def to_dict(self) -> dict:
        return {"type": "greater_equal", "attribute": self._attribute, "value": self._value}

    def __repr__(self) -> str:
        return f"GreaterThanOrEqualPredicate({self._attribute!r}, {self._value!r})"


class LessThanPredicate(Predicate):
    """Predicate for less-than comparison."""

    def __init__(self, attribute: str, value: Any):
        self._attribute = attribute
        self._value = value

    @property
    def attribute(self) -> str:
        return self._attribute

    @property
    def value(self) -> Any:
        return self._value

    def to_dict(self) -> dict:
        return {"type": "less_than", "attribute": self._attribute, "value": self._value}

    def __repr__(self) -> str:
        return f"LessThanPredicate({self._attribute!r}, {self._value!r})"


class LessThanOrEqualPredicate(Predicate):
    """Predicate for less-than-or-equal comparison."""

    def __init__(self, attribute: str, value: Any):
        self._attribute = attribute
        self._value = value

    @property
    def attribute(self) -> str:
        return self._attribute

    @property
    def value(self) -> Any:
        return self._value

    def to_dict(self) -> dict:
        return {"type": "less_equal", "attribute": self._attribute, "value": self._value}

    def __repr__(self) -> str:
        return f"LessThanOrEqualPredicate({self._attribute!r}, {self._value!r})"


class BetweenPredicate(Predicate):
    """Predicate for range comparison (inclusive)."""

    def __init__(self, attribute: str, from_value: Any, to_value: Any):
        self._attribute = attribute
        self._from_value = from_value
        self._to_value = to_value

    @property
    def attribute(self) -> str:
        return self._attribute

    @property
    def from_value(self) -> Any:
        return self._from_value

    @property
    def to_value(self) -> Any:
        return self._to_value

    def to_dict(self) -> dict:
        return {
            "type": "between",
            "attribute": self._attribute,
            "from": self._from_value,
            "to": self._to_value,
        }

    def __repr__(self) -> str:
        return f"BetweenPredicate({self._attribute!r}, {self._from_value!r}, {self._to_value!r})"


class InPredicate(Predicate):
    """Predicate for checking membership in a set of values."""

    def __init__(self, attribute: str, *values: Any):
        self._attribute = attribute
        self._values = list(values)

    @property
    def attribute(self) -> str:
        return self._attribute

    @property
    def values(self) -> List[Any]:
        return self._values

    def to_dict(self) -> dict:
        return {"type": "in", "attribute": self._attribute, "values": self._values}

    def __repr__(self) -> str:
        return f"InPredicate({self._attribute!r}, {self._values!r})"


class LikePredicate(Predicate):
    """Predicate for SQL LIKE pattern matching."""

    def __init__(self, attribute: str, pattern: str):
        self._attribute = attribute
        self._pattern = pattern

    @property
    def attribute(self) -> str:
        return self._attribute

    @property
    def pattern(self) -> str:
        return self._pattern

    def to_dict(self) -> dict:
        return {"type": "like", "attribute": self._attribute, "pattern": self._pattern}

    def __repr__(self) -> str:
        return f"LikePredicate({self._attribute!r}, {self._pattern!r})"


class ILikePredicate(Predicate):
    """Predicate for case-insensitive SQL LIKE pattern matching."""

    def __init__(self, attribute: str, pattern: str):
        self._attribute = attribute
        self._pattern = pattern

    @property
    def attribute(self) -> str:
        return self._attribute

    @property
    def pattern(self) -> str:
        return self._pattern

    def to_dict(self) -> dict:
        return {"type": "ilike", "attribute": self._attribute, "pattern": self._pattern}

    def __repr__(self) -> str:
        return f"ILikePredicate({self._attribute!r}, {self._pattern!r})"


class RegexPredicate(Predicate):
    """Predicate for regular expression matching."""

    def __init__(self, attribute: str, pattern: str):
        self._attribute = attribute
        self._pattern = pattern

    @property
    def attribute(self) -> str:
        return self._attribute

    @property
    def pattern(self) -> str:
        return self._pattern

    def to_dict(self) -> dict:
        return {"type": "regex", "attribute": self._attribute, "pattern": self._pattern}

    def __repr__(self) -> str:
        return f"RegexPredicate({self._attribute!r}, {self._pattern!r})"


class InstanceOfPredicate(Predicate):
    """Predicate for checking instance type."""

    def __init__(self, class_name: str):
        self._class_name = class_name

    @property
    def class_name(self) -> str:
        return self._class_name

    def to_dict(self) -> dict:
        return {"type": "instance_of", "class_name": self._class_name}

    def __repr__(self) -> str:
        return f"InstanceOfPredicate({self._class_name!r})"


class AndPredicate(Predicate):
    """Predicate that combines multiple predicates with AND logic."""

    def __init__(self, *predicates: Predicate):
        self._predicates = list(predicates)

    @property
    def predicates(self) -> List[Predicate]:
        return self._predicates

    def to_dict(self) -> dict:
        return {
            "type": "and",
            "predicates": [p.to_dict() for p in self._predicates],
        }

    def __repr__(self) -> str:
        return f"AndPredicate({self._predicates!r})"


class OrPredicate(Predicate):
    """Predicate that combines multiple predicates with OR logic."""

    def __init__(self, *predicates: Predicate):
        self._predicates = list(predicates)

    @property
    def predicates(self) -> List[Predicate]:
        return self._predicates

    def to_dict(self) -> dict:
        return {
            "type": "or",
            "predicates": [p.to_dict() for p in self._predicates],
        }

    def __repr__(self) -> str:
        return f"OrPredicate({self._predicates!r})"


class NotPredicate(Predicate):
    """Predicate that negates another predicate."""

    def __init__(self, predicate: Predicate):
        self._predicate = predicate

    @property
    def predicate(self) -> Predicate:
        return self._predicate

    def to_dict(self) -> dict:
        return {"type": "not", "predicate": self._predicate.to_dict()}

    def __repr__(self) -> str:
        return f"NotPredicate({self._predicate!r})"


class PagingPredicate(Predicate):
    """Predicate for paginated queries."""

    def __init__(
        self,
        predicate: Optional[Predicate] = None,
        page_size: int = 10,
        comparator: Optional[Any] = None,
    ):
        self._predicate = predicate
        self._page_size = page_size
        self._comparator = comparator
        self._page = 0
        self._iteration_type = "ENTRY"

    @property
    def predicate(self) -> Optional[Predicate]:
        return self._predicate

    @property
    def page_size(self) -> int:
        return self._page_size

    @property
    def page(self) -> int:
        return self._page

    @page.setter
    def page(self, value: int) -> None:
        if value < 0:
            raise ValueError("Page cannot be negative")
        self._page = value

    def next_page(self) -> "PagingPredicate":
        self._page += 1
        return self

    def previous_page(self) -> "PagingPredicate":
        if self._page > 0:
            self._page -= 1
        return self

    def reset(self) -> "PagingPredicate":
        self._page = 0
        return self

    def to_dict(self) -> dict:
        result = {
            "type": "paging",
            "page_size": self._page_size,
            "page": self._page,
        }
        if self._predicate:
            result["predicate"] = self._predicate.to_dict()
        return result

    def __repr__(self) -> str:
        return f"PagingPredicate(page={self._page}, page_size={self._page_size})"


class PredicateBuilder:
    """Builder for creating predicates fluently."""

    def __init__(self, attribute: str):
        self._attribute = attribute

    def equal(self, value: Any) -> EqualPredicate:
        return EqualPredicate(self._attribute, value)

    def not_equal(self, value: Any) -> NotEqualPredicate:
        return NotEqualPredicate(self._attribute, value)

    def greater_than(self, value: Any) -> GreaterThanPredicate:
        return GreaterThanPredicate(self._attribute, value)

    def greater_than_or_equal(self, value: Any) -> GreaterThanOrEqualPredicate:
        return GreaterThanOrEqualPredicate(self._attribute, value)

    def less_than(self, value: Any) -> LessThanPredicate:
        return LessThanPredicate(self._attribute, value)

    def less_than_or_equal(self, value: Any) -> LessThanOrEqualPredicate:
        return LessThanOrEqualPredicate(self._attribute, value)

    def between(self, from_value: Any, to_value: Any) -> BetweenPredicate:
        return BetweenPredicate(self._attribute, from_value, to_value)

    def is_in(self, *values: Any) -> InPredicate:
        return InPredicate(self._attribute, *values)

    def like(self, pattern: str) -> LikePredicate:
        return LikePredicate(self._attribute, pattern)

    def ilike(self, pattern: str) -> ILikePredicate:
        return ILikePredicate(self._attribute, pattern)

    def regex(self, pattern: str) -> RegexPredicate:
        return RegexPredicate(self._attribute, pattern)


def attr(attribute: str) -> PredicateBuilder:
    """Create a predicate builder for an attribute.

    Args:
        attribute: The attribute name to build predicates for.

    Returns:
        A PredicateBuilder instance.
    """
    return PredicateBuilder(attribute)


def sql(expression: str) -> SqlPredicate:
    """Create a SQL predicate.

    Args:
        expression: The SQL expression.

    Returns:
        A SqlPredicate instance.
    """
    return SqlPredicate(expression)


def true() -> TruePredicate:
    """Create a predicate that always returns true."""
    return TruePredicate()


def false() -> FalsePredicate:
    """Create a predicate that always returns false."""
    return FalsePredicate()


def and_(*predicates: Predicate) -> AndPredicate:
    """Create an AND predicate combining multiple predicates."""
    return AndPredicate(*predicates)


def or_(*predicates: Predicate) -> OrPredicate:
    """Create an OR predicate combining multiple predicates."""
    return OrPredicate(*predicates)


def not_(predicate: Predicate) -> NotPredicate:
    """Create a NOT predicate negating another predicate."""
    return NotPredicate(predicate)


def paging(
    predicate: Optional[Predicate] = None,
    page_size: int = 10,
    comparator: Optional[Any] = None,
) -> PagingPredicate:
    """Create a paging predicate for paginated queries."""
    return PagingPredicate(predicate, page_size, comparator)
