"""Projection classes for transforming distributed data structure entries."""

from abc import ABC, abstractmethod
from typing import List


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
    def attributes(self) -> List[str]:
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


def single_attribute(attribute: str) -> SingleAttributeProjection:
    """Create a single attribute projection.

    Args:
        attribute: The attribute name to project.

    Returns:
        A SingleAttributeProjection instance.
    """
    return SingleAttributeProjection(attribute)


def multi_attribute(*attributes: str) -> MultiAttributeProjection:
    """Create a multi-attribute projection.

    Args:
        attributes: The attribute names to project.

    Returns:
        A MultiAttributeProjection instance.
    """
    return MultiAttributeProjection(*attributes)


def identity() -> IdentityProjection:
    """Create an identity projection.

    Returns:
        An IdentityProjection instance.
    """
    return IdentityProjection()
