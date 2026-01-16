"""Unit tests for hazelcast/projection.py"""

import unittest
from abc import ABC

from hazelcast.projection import (
    Projection,
    SingleAttributeProjection,
    MultiAttributeProjection,
    IdentityProjection,
    single_attribute,
    multi_attribute,
    identity,
)


class TestProjectionABC(unittest.TestCase):
    def test_projection_is_abstract(self):
        self.assertTrue(issubclass(Projection, ABC))

    def test_projection_cannot_be_instantiated(self):
        with self.assertRaises(TypeError):
            Projection()

    def test_projection_has_to_dict_abstract_method(self):
        self.assertIn("to_dict", Projection.__abstractmethods__)


class TestSingleAttributeProjection(unittest.TestCase):
    def test_init(self):
        proj = SingleAttributeProjection("name")
        self.assertEqual(proj.attribute, "name")

    def test_attribute_property(self):
        proj = SingleAttributeProjection("email")
        self.assertEqual(proj.attribute, "email")

    def test_to_dict(self):
        proj = SingleAttributeProjection("age")
        result = proj.to_dict()
        self.assertEqual(result, {"type": "single_attribute", "attribute": "age"})

    def test_repr(self):
        proj = SingleAttributeProjection("status")
        self.assertEqual(repr(proj), "SingleAttributeProjection('status')")


class TestMultiAttributeProjection(unittest.TestCase):
    def test_init_single_attribute(self):
        proj = MultiAttributeProjection("name")
        self.assertEqual(proj.attributes, ["name"])

    def test_init_multiple_attributes(self):
        proj = MultiAttributeProjection("name", "email", "age")
        self.assertEqual(proj.attributes, ["name", "email", "age"])

    def test_init_no_attributes(self):
        proj = MultiAttributeProjection()
        self.assertEqual(proj.attributes, [])

    def test_attributes_property(self):
        proj = MultiAttributeProjection("a", "b", "c")
        self.assertEqual(proj.attributes, ["a", "b", "c"])

    def test_to_dict(self):
        proj = MultiAttributeProjection("first_name", "last_name")
        result = proj.to_dict()
        self.assertEqual(result, {
            "type": "multi_attribute",
            "attributes": ["first_name", "last_name"],
        })

    def test_repr(self):
        proj = MultiAttributeProjection("x", "y")
        self.assertEqual(repr(proj), "MultiAttributeProjection(['x', 'y'])")


class TestIdentityProjection(unittest.TestCase):
    def test_to_dict(self):
        proj = IdentityProjection()
        result = proj.to_dict()
        self.assertEqual(result, {"type": "identity"})

    def test_repr(self):
        proj = IdentityProjection()
        self.assertEqual(repr(proj), "IdentityProjection()")


class TestFactoryFunctions(unittest.TestCase):
    def test_single_attribute_function(self):
        proj = single_attribute("name")
        self.assertIsInstance(proj, SingleAttributeProjection)
        self.assertEqual(proj.attribute, "name")

    def test_multi_attribute_function(self):
        proj = multi_attribute("name", "email", "phone")
        self.assertIsInstance(proj, MultiAttributeProjection)
        self.assertEqual(proj.attributes, ["name", "email", "phone"])

    def test_multi_attribute_function_empty(self):
        proj = multi_attribute()
        self.assertIsInstance(proj, MultiAttributeProjection)
        self.assertEqual(proj.attributes, [])

    def test_identity_function(self):
        proj = identity()
        self.assertIsInstance(proj, IdentityProjection)


if __name__ == "__main__":
    unittest.main()
