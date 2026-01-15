"""Unit tests for the projection module."""

import unittest

from hazelcast.projection import (
    Projection,
    SingleAttributeProjection,
    MultiAttributeProjection,
    IdentityProjection,
    single_attribute,
    multi_attribute,
    identity,
)


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

    def test_is_projection(self):
        proj = SingleAttributeProjection("field")
        self.assertIsInstance(proj, Projection)


class TestMultiAttributeProjection(unittest.TestCase):
    def test_creation_single(self):
        proj = MultiAttributeProjection("name")
        self.assertEqual(proj.attributes, ["name"])

    def test_creation_multiple(self):
        proj = MultiAttributeProjection("name", "email", "age")
        self.assertEqual(proj.attributes, ["name", "email", "age"])

    def test_creation_empty(self):
        proj = MultiAttributeProjection()
        self.assertEqual(proj.attributes, [])

    def test_to_dict(self):
        proj = MultiAttributeProjection("first_name", "last_name")
        self.assertEqual(
            proj.to_dict(),
            {"type": "multi_attribute", "attributes": ["first_name", "last_name"]},
        )

    def test_to_dict_empty(self):
        proj = MultiAttributeProjection()
        self.assertEqual(proj.to_dict(), {"type": "multi_attribute", "attributes": []})

    def test_repr(self):
        proj = MultiAttributeProjection("a", "b")
        self.assertEqual(repr(proj), "MultiAttributeProjection(['a', 'b'])")

    def test_is_projection(self):
        proj = MultiAttributeProjection("x", "y")
        self.assertIsInstance(proj, Projection)


class TestIdentityProjection(unittest.TestCase):
    def test_to_dict(self):
        proj = IdentityProjection()
        self.assertEqual(proj.to_dict(), {"type": "identity"})

    def test_repr(self):
        proj = IdentityProjection()
        self.assertEqual(repr(proj), "IdentityProjection()")

    def test_is_projection(self):
        proj = IdentityProjection()
        self.assertIsInstance(proj, Projection)


class TestProjectionFactoryFunctions(unittest.TestCase):
    def test_single_attribute(self):
        proj = single_attribute("name")
        self.assertIsInstance(proj, SingleAttributeProjection)
        self.assertEqual(proj.attribute, "name")

    def test_multi_attribute_single(self):
        proj = multi_attribute("name")
        self.assertIsInstance(proj, MultiAttributeProjection)
        self.assertEqual(proj.attributes, ["name"])

    def test_multi_attribute_multiple(self):
        proj = multi_attribute("name", "email", "age")
        self.assertIsInstance(proj, MultiAttributeProjection)
        self.assertEqual(proj.attributes, ["name", "email", "age"])

    def test_multi_attribute_empty(self):
        proj = multi_attribute()
        self.assertIsInstance(proj, MultiAttributeProjection)
        self.assertEqual(proj.attributes, [])

    def test_identity(self):
        proj = identity()
        self.assertIsInstance(proj, IdentityProjection)


class TestProjectionSerialization(unittest.TestCase):
    def test_single_attribute_roundtrip(self):
        proj = single_attribute("price")
        data = proj.to_dict()
        self.assertEqual(data["type"], "single_attribute")
        self.assertEqual(data["attribute"], "price")

    def test_multi_attribute_roundtrip(self):
        proj = multi_attribute("name", "price", "quantity")
        data = proj.to_dict()
        self.assertEqual(data["type"], "multi_attribute")
        self.assertEqual(data["attributes"], ["name", "price", "quantity"])

    def test_identity_roundtrip(self):
        proj = identity()
        data = proj.to_dict()
        self.assertEqual(data["type"], "identity")
        self.assertEqual(len(data), 1)


class TestBackwardCompatibility(unittest.TestCase):
    """Test that projections can still be imported from aggregator module."""

    def test_import_from_aggregator(self):
        from hazelcast.aggregator import (
            Projection as AggProjection,
            SingleAttributeProjection as AggSingle,
            MultiAttributeProjection as AggMulti,
            IdentityProjection as AggIdentity,
            single_attribute as agg_single,
            multi_attribute as agg_multi,
            identity as agg_identity,
        )

        self.assertIs(AggProjection, Projection)
        self.assertIs(AggSingle, SingleAttributeProjection)
        self.assertIs(AggMulti, MultiAttributeProjection)
        self.assertIs(AggIdentity, IdentityProjection)

        proj1 = agg_single("test")
        self.assertIsInstance(proj1, SingleAttributeProjection)

        proj2 = agg_multi("a", "b")
        self.assertIsInstance(proj2, MultiAttributeProjection)

        proj3 = agg_identity()
        self.assertIsInstance(proj3, IdentityProjection)


if __name__ == "__main__":
    unittest.main()
