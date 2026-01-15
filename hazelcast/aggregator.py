"""Aggregator classes for aggregating distributed data structure entries."""

from abc import ABC, abstractmethod
from typing import Any, Optional


class Aggregator(ABC):
    """Base class for all aggregators."""

    @abstractmethod
    def to_dict(self) -> dict:
        """Convert the aggregator to a dictionary for serialization."""
        pass


class CountAggregator(Aggregator):
    """Aggregator that counts entries."""

    def __init__(self, attribute: Optional[str] = None):
        self._attribute = attribute

    @property
    def attribute(self) -> Optional[str]:
        return self._attribute

    def to_dict(self) -> dict:
        result = {"type": "count"}
        if self._attribute:
            result["attribute"] = self._attribute
        return result

    def __repr__(self) -> str:
        if self._attribute:
            return f"CountAggregator({self._attribute!r})"
        return "CountAggregator()"


class DistinctValuesAggregator(Aggregator):
    """Aggregator that returns distinct values for an attribute."""

    def __init__(self, attribute: str):
        self._attribute = attribute

    @property
    def attribute(self) -> str:
        return self._attribute

    def to_dict(self) -> dict:
        return {"type": "distinct", "attribute": self._attribute}

    def __repr__(self) -> str:
        return f"DistinctValuesAggregator({self._attribute!r})"


class SumAggregator(Aggregator):
    """Aggregator that computes the sum of values."""

    def __init__(self, attribute: str):
        self._attribute = attribute

    @property
    def attribute(self) -> str:
        return self._attribute

    def to_dict(self) -> dict:
        return {"type": "sum", "attribute": self._attribute}

    def __repr__(self) -> str:
        return f"SumAggregator({self._attribute!r})"


class AverageAggregator(Aggregator):
    """Aggregator that computes the average of values."""

    def __init__(self, attribute: str):
        self._attribute = attribute

    @property
    def attribute(self) -> str:
        return self._attribute

    def to_dict(self) -> dict:
        return {"type": "average", "attribute": self._attribute}

    def __repr__(self) -> str:
        return f"AverageAggregator({self._attribute!r})"


class MinAggregator(Aggregator):
    """Aggregator that finds the minimum value."""

    def __init__(self, attribute: str):
        self._attribute = attribute

    @property
    def attribute(self) -> str:
        return self._attribute

    def to_dict(self) -> dict:
        return {"type": "min", "attribute": self._attribute}

    def __repr__(self) -> str:
        return f"MinAggregator({self._attribute!r})"


class MaxAggregator(Aggregator):
    """Aggregator that finds the maximum value."""

    def __init__(self, attribute: str):
        self._attribute = attribute

    @property
    def attribute(self) -> str:
        return self._attribute

    def to_dict(self) -> dict:
        return {"type": "max", "attribute": self._attribute}

    def __repr__(self) -> str:
        return f"MaxAggregator({self._attribute!r})"


class IntegerSumAggregator(Aggregator):
    """Aggregator that computes the sum of integer values."""

    def __init__(self, attribute: str):
        self._attribute = attribute

    @property
    def attribute(self) -> str:
        return self._attribute

    def to_dict(self) -> dict:
        return {"type": "integer_sum", "attribute": self._attribute}

    def __repr__(self) -> str:
        return f"IntegerSumAggregator({self._attribute!r})"


class IntegerAverageAggregator(Aggregator):
    """Aggregator that computes the average of integer values."""

    def __init__(self, attribute: str):
        self._attribute = attribute

    @property
    def attribute(self) -> str:
        return self._attribute

    def to_dict(self) -> dict:
        return {"type": "integer_average", "attribute": self._attribute}

    def __repr__(self) -> str:
        return f"IntegerAverageAggregator({self._attribute!r})"


class LongSumAggregator(Aggregator):
    """Aggregator that computes the sum of long values."""

    def __init__(self, attribute: str):
        self._attribute = attribute

    @property
    def attribute(self) -> str:
        return self._attribute

    def to_dict(self) -> dict:
        return {"type": "long_sum", "attribute": self._attribute}

    def __repr__(self) -> str:
        return f"LongSumAggregator({self._attribute!r})"


class LongAverageAggregator(Aggregator):
    """Aggregator that computes the average of long values."""

    def __init__(self, attribute: str):
        self._attribute = attribute

    @property
    def attribute(self) -> str:
        return self._attribute

    def to_dict(self) -> dict:
        return {"type": "long_average", "attribute": self._attribute}

    def __repr__(self) -> str:
        return f"LongAverageAggregator({self._attribute!r})"


class DoubleSumAggregator(Aggregator):
    """Aggregator that computes the sum of double values."""

    def __init__(self, attribute: str):
        self._attribute = attribute

    @property
    def attribute(self) -> str:
        return self._attribute

    def to_dict(self) -> dict:
        return {"type": "double_sum", "attribute": self._attribute}

    def __repr__(self) -> str:
        return f"DoubleSumAggregator({self._attribute!r})"


class DoubleAverageAggregator(Aggregator):
    """Aggregator that computes the average of double values."""

    def __init__(self, attribute: str):
        self._attribute = attribute

    @property
    def attribute(self) -> str:
        return self._attribute

    def to_dict(self) -> dict:
        return {"type": "double_average", "attribute": self._attribute}

    def __repr__(self) -> str:
        return f"DoubleAverageAggregator({self._attribute!r})"


class FixedPointSumAggregator(Aggregator):
    """Aggregator that computes the sum with fixed-point precision."""

    def __init__(self, attribute: str):
        self._attribute = attribute

    @property
    def attribute(self) -> str:
        return self._attribute

    def to_dict(self) -> dict:
        return {"type": "fixed_point_sum", "attribute": self._attribute}

    def __repr__(self) -> str:
        return f"FixedPointSumAggregator({self._attribute!r})"


class FloatingPointSumAggregator(Aggregator):
    """Aggregator that computes the sum with floating-point precision."""

    def __init__(self, attribute: str):
        self._attribute = attribute

    @property
    def attribute(self) -> str:
        return self._attribute

    def to_dict(self) -> dict:
        return {"type": "floating_point_sum", "attribute": self._attribute}

    def __repr__(self) -> str:
        return f"FloatingPointSumAggregator({self._attribute!r})"


class Projection(ABC):
    """Base class for projections."""

    @abstractmethod
    def to_dict(self) -> dict:
        """Convert the projection to a dictionary for serialization."""
        pass


class SingleAttributeProjection(Projection):
    """Projection that extracts a single attribute."""

    def __init__(self, attribute: str):
        self._attribute = attribute

    @property
    def attribute(self) -> str:
        return self._attribute

    def to_dict(self) -> dict:
        return {"type": "single_attribute", "attribute": self._attribute}

    def __repr__(self) -> str:
        return f"SingleAttributeProjection({self._attribute!r})"


class MultiAttributeProjection(Projection):
    """Projection that extracts multiple attributes."""

    def __init__(self, *attributes: str):
        self._attributes = list(attributes)

    @property
    def attributes(self) -> list:
        return self._attributes

    def to_dict(self) -> dict:
        return {"type": "multi_attribute", "attributes": self._attributes}

    def __repr__(self) -> str:
        return f"MultiAttributeProjection({self._attributes!r})"


class IdentityProjection(Projection):
    """Projection that returns the entry as-is."""

    def to_dict(self) -> dict:
        return {"type": "identity"}

    def __repr__(self) -> str:
        return "IdentityProjection()"


def count(attribute: Optional[str] = None) -> CountAggregator:
    """Create a count aggregator."""
    return CountAggregator(attribute)


def distinct(attribute: str) -> DistinctValuesAggregator:
    """Create a distinct values aggregator."""
    return DistinctValuesAggregator(attribute)


def sum_(attribute: str) -> SumAggregator:
    """Create a sum aggregator."""
    return SumAggregator(attribute)


def average(attribute: str) -> AverageAggregator:
    """Create an average aggregator."""
    return AverageAggregator(attribute)


def min_(attribute: str) -> MinAggregator:
    """Create a min aggregator."""
    return MinAggregator(attribute)


def max_(attribute: str) -> MaxAggregator:
    """Create a max aggregator."""
    return MaxAggregator(attribute)


def integer_sum(attribute: str) -> IntegerSumAggregator:
    """Create an integer sum aggregator."""
    return IntegerSumAggregator(attribute)


def integer_average(attribute: str) -> IntegerAverageAggregator:
    """Create an integer average aggregator."""
    return IntegerAverageAggregator(attribute)


def long_sum(attribute: str) -> LongSumAggregator:
    """Create a long sum aggregator."""
    return LongSumAggregator(attribute)


def long_average(attribute: str) -> LongAverageAggregator:
    """Create a long average aggregator."""
    return LongAverageAggregator(attribute)


def double_sum(attribute: str) -> DoubleSumAggregator:
    """Create a double sum aggregator."""
    return DoubleSumAggregator(attribute)


def double_average(attribute: str) -> DoubleAverageAggregator:
    """Create a double average aggregator."""
    return DoubleAverageAggregator(attribute)


def single_attribute(attribute: str) -> SingleAttributeProjection:
    """Create a single attribute projection."""
    return SingleAttributeProjection(attribute)


def multi_attribute(*attributes: str) -> MultiAttributeProjection:
    """Create a multi-attribute projection."""
    return MultiAttributeProjection(*attributes)


def identity() -> IdentityProjection:
    """Create an identity projection."""
    return IdentityProjection()
