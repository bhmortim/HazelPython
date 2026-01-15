"""Unit tests for hazelcast.cp module."""

import pytest
from concurrent.futures import Future

from hazelcast.cp.base import CPProxy, CPGroupId
from hazelcast.exceptions import IllegalStateException


class TestCPGroupId:
    """Tests for CPGroupId class."""

    def test_init_default(self):
        group_id = CPGroupId("default")
        assert group_id.name == "default"
        assert group_id.seed == 0
        assert group_id.group_id == 0

    def test_init_with_values(self):
        group_id = CPGroupId("custom", seed=123, group_id=456)
        assert group_id.name == "custom"
        assert group_id.seed == 123
        assert group_id.group_id == 456

    def test_equality(self):
        g1 = CPGroupId("test", 1, 2)
        g2 = CPGroupId("test", 1, 2)
        g3 = CPGroupId("test", 1, 3)
        g4 = CPGroupId("other", 1, 2)
        assert g1 == g2
        assert g1 != g3
        assert g1 != g4
        assert g1 != "not a group"

    def test_hash(self):
        g1 = CPGroupId("test", 1, 2)
        g2 = CPGroupId("test", 1, 2)
        assert hash(g1) == hash(g2)
        groups = {g1, g2}
        assert len(groups) == 1

    def test_repr(self):
        group_id = CPGroupId("test", group_id=42)
        r = repr(group_id)
        assert "CPGroupId" in r
        assert "test" in r
        assert "42" in r

    def test_default_group_name(self):
        assert CPGroupId.DEFAULT_GROUP_NAME == "default"


class ConcreteCPProxy(CPProxy):
    """Concrete implementation of CPProxy for testing."""

    def __init__(self, name, group_id=None, invocation_service=None):
        if group_id is None:
            group_id = CPGroupId("default")
        super().__init__("test:cpService", name, group_id, invocation_service)


class TestCPProxy:
    """Tests for CPProxy base class."""

    def test_init(self):
        group_id = CPGroupId("default")
        proxy = ConcreteCPProxy("test-cp", group_id)
        assert proxy.name == "test-cp"
        assert proxy.service_name == "test:cpService"
        assert proxy.group_id is group_id
        assert proxy.is_destroyed is False

    def test_direct_to_leader_default(self):
        proxy = ConcreteCPProxy("test")
        assert proxy.direct_to_leader is True

    def test_direct_to_leader_setter(self):
        proxy = ConcreteCPProxy("test")
        proxy.direct_to_leader = False
        assert proxy.direct_to_leader is False

    def test_destroy(self):
        proxy = ConcreteCPProxy("test")
        proxy.destroy()
        assert proxy.is_destroyed is True

    def test_destroy_twice_raises(self):
        proxy = ConcreteCPProxy("test")
        proxy.destroy()
        with pytest.raises(IllegalStateException):
            proxy.destroy()

    def test_check_not_destroyed(self):
        proxy = ConcreteCPProxy("test")
        proxy._check_not_destroyed()
        proxy.destroy()
        with pytest.raises(IllegalStateException):
            proxy._check_not_destroyed()

    @pytest.mark.asyncio
    async def test_destroy_async(self):
        proxy = ConcreteCPProxy("test")
        await proxy.destroy_async()
        assert proxy.is_destroyed is True

    def test_invoke_no_service(self):
        proxy = ConcreteCPProxy("test")
        future = proxy._invoke(None)
        assert isinstance(future, Future)
        assert future.result() is None

    def test_repr(self):
        group_id = CPGroupId("mygroup")
        proxy = ConcreteCPProxy("test-name", group_id)
        r = repr(proxy)
        assert "ConcreteCPProxy" in r
        assert "test-name" in r
        assert "mygroup" in r
