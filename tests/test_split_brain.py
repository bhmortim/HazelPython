"""Tests for split-brain protection configuration."""

import pytest

from hazelcast.config import (
    ClientConfig,
    SplitBrainProtectionConfig,
    SplitBrainProtectionOn,
    SplitBrainProtectionFunctionType,
    SplitBrainProtectionEvent,
    SplitBrainProtectionListener,
    SplitBrainProtectionFunction,
)
from hazelcast.exceptions import ConfigurationException


class TestSplitBrainProtectionEvent:
    """Tests for SplitBrainProtectionEvent."""

    def test_event_properties(self):
        event = SplitBrainProtectionEvent(
            name="test-quorum",
            threshold=3,
            current_members=["member1:5701", "member2:5701"],
            is_present=False,
        )
        assert event.name == "test-quorum"
        assert event.threshold == 3
        assert event.current_members == ["member1:5701", "member2:5701"]
        assert event.is_present is False

    def test_event_members_is_copy(self):
        members = ["member1:5701"]
        event = SplitBrainProtectionEvent(
            name="test", threshold=2, current_members=members, is_present=True
        )
        members.append("member2:5701")
        assert len(event.current_members) == 1


class TestSplitBrainProtectionListener:
    """Tests for SplitBrainProtectionListener."""

    def test_listener_implementation(self):
        class TestListener(SplitBrainProtectionListener):
            def __init__(self):
                self.present_events = []
                self.absent_events = []

            def on_present(self, event):
                self.present_events.append(event)

            def on_absent(self, event):
                self.absent_events.append(event)

        listener = TestListener()
        present_event = SplitBrainProtectionEvent(
            "q1", 2, ["m1", "m2"], is_present=True
        )
        absent_event = SplitBrainProtectionEvent(
            "q1", 2, ["m1"], is_present=False
        )

        listener.on_present(present_event)
        listener.on_absent(absent_event)

        assert len(listener.present_events) == 1
        assert len(listener.absent_events) == 1
        assert listener.present_events[0].is_present is True
        assert listener.absent_events[0].is_present is False


class TestSplitBrainProtectionFunction:
    """Tests for SplitBrainProtectionFunction."""

    def test_custom_function(self):
        class RequireMinimumMembers(SplitBrainProtectionFunction):
            def __init__(self, minimum: int):
                self._minimum = minimum

            def apply(self, members):
                return len(members) >= self._minimum

        func = RequireMinimumMembers(3)
        assert func.apply(["m1", "m2", "m3"]) is True
        assert func.apply(["m1", "m2"]) is False
        assert func.apply([]) is False

    def test_custom_function_with_specific_members(self):
        class RequireSpecificMember(SplitBrainProtectionFunction):
            def __init__(self, required_member: str):
                self._required = required_member

            def apply(self, members):
                return self._required in members

        func = RequireSpecificMember("primary:5701")
        assert func.apply(["primary:5701", "secondary:5701"]) is True
        assert func.apply(["secondary:5701", "tertiary:5701"]) is False


class TestSplitBrainProtectionConfig:
    """Tests for SplitBrainProtectionConfig."""

    def test_default_values(self):
        config = SplitBrainProtectionConfig()
        assert config.name == "default"
        assert config.enabled is True
        assert config.min_cluster_size == 2
        assert config.protect_on == SplitBrainProtectionOn.READ_WRITE
        assert config.function_type == SplitBrainProtectionFunctionType.MEMBER_COUNT
        assert config.function is None
        assert config.listeners == []

    def test_custom_values(self):
        config = SplitBrainProtectionConfig(
            name="my-quorum",
            enabled=False,
            min_cluster_size=5,
            protect_on=SplitBrainProtectionOn.WRITE,
            function_type=SplitBrainProtectionFunctionType.PROBABILISTIC,
        )
        assert config.name == "my-quorum"
        assert config.enabled is False
        assert config.min_cluster_size == 5
        assert config.protect_on == SplitBrainProtectionOn.WRITE
        assert config.function_type == SplitBrainProtectionFunctionType.PROBABILISTIC

    def test_setters(self):
        config = SplitBrainProtectionConfig()
        config.enabled = False
        config.min_cluster_size = 3
        config.protect_on = SplitBrainProtectionOn.READ
        config.function_type = SplitBrainProtectionFunctionType.RECENTLY_ACTIVE

        assert config.enabled is False
        assert config.min_cluster_size == 3
        assert config.protect_on == SplitBrainProtectionOn.READ
        assert config.function_type == SplitBrainProtectionFunctionType.RECENTLY_ACTIVE

    def test_empty_name_raises(self):
        with pytest.raises(ConfigurationException, match="name cannot be empty"):
            SplitBrainProtectionConfig(name="")

    def test_invalid_min_cluster_size_raises(self):
        with pytest.raises(ConfigurationException, match="min_cluster_size"):
            SplitBrainProtectionConfig(min_cluster_size=0)

        with pytest.raises(ConfigurationException, match="min_cluster_size"):
            SplitBrainProtectionConfig(min_cluster_size=-1)

    def test_add_listener(self):
        class TestListener(SplitBrainProtectionListener):
            def on_present(self, event):
                pass

            def on_absent(self, event):
                pass

        config = SplitBrainProtectionConfig()
        listener1 = TestListener()
        listener2 = TestListener()

        result = config.add_listener(listener1)
        assert result is config
        assert len(config.listeners) == 1

        config.add_listener(listener2)
        assert len(config.listeners) == 2

    def test_custom_function(self):
        class MyFunction(SplitBrainProtectionFunction):
            def apply(self, members):
                return len(members) > 0

        func = MyFunction()
        config = SplitBrainProtectionConfig(function=func)
        assert config.function is func

    def test_probabilistic_settings(self):
        config = SplitBrainProtectionConfig()
        assert config.probabilistic_suspected_epsilon_millis == 10000
        assert config.probabilistic_max_sample_size == 200
        assert config.probabilistic_acceptable_heartbeat_pause_millis == 60000
        assert config.probabilistic_heartbeat_interval_millis == 5000

        config.probabilistic_suspected_epsilon_millis = 5000
        config.probabilistic_max_sample_size = 100
        config.probabilistic_acceptable_heartbeat_pause_millis = 30000
        config.probabilistic_heartbeat_interval_millis = 2500

        assert config.probabilistic_suspected_epsilon_millis == 5000
        assert config.probabilistic_max_sample_size == 100
        assert config.probabilistic_acceptable_heartbeat_pause_millis == 30000
        assert config.probabilistic_heartbeat_interval_millis == 2500

    def test_probabilistic_validation(self):
        config = SplitBrainProtectionConfig()

        with pytest.raises(ConfigurationException):
            config.probabilistic_suspected_epsilon_millis = -1

        with pytest.raises(ConfigurationException):
            config.probabilistic_max_sample_size = 0

        with pytest.raises(ConfigurationException):
            config.probabilistic_acceptable_heartbeat_pause_millis = -1

        with pytest.raises(ConfigurationException):
            config.probabilistic_heartbeat_interval_millis = 0

    def test_recently_active_settings(self):
        config = SplitBrainProtectionConfig()
        assert config.recently_active_heartbeat_tolerance_millis == 60000

        config.recently_active_heartbeat_tolerance_millis = 30000
        assert config.recently_active_heartbeat_tolerance_millis == 30000

        with pytest.raises(ConfigurationException):
            config.recently_active_heartbeat_tolerance_millis = -1

    def test_from_dict_basic(self):
        data = {
            "enabled": True,
            "min_cluster_size": 3,
            "protect_on": "write",
            "function_type": "MEMBER_COUNT",
        }
        config = SplitBrainProtectionConfig.from_dict("my-quorum", data)

        assert config.name == "my-quorum"
        assert config.enabled is True
        assert config.min_cluster_size == 3
        assert config.protect_on == SplitBrainProtectionOn.WRITE
        assert config.function_type == SplitBrainProtectionFunctionType.MEMBER_COUNT

    def test_from_dict_probabilistic(self):
        data = {
            "function_type": "PROBABILISTIC",
            "probabilistic_suspected_epsilon_millis": 5000,
            "probabilistic_max_sample_size": 50,
            "probabilistic_acceptable_heartbeat_pause_millis": 20000,
            "probabilistic_heartbeat_interval_millis": 1000,
        }
        config = SplitBrainProtectionConfig.from_dict("prob-quorum", data)

        assert config.function_type == SplitBrainProtectionFunctionType.PROBABILISTIC
        assert config.probabilistic_suspected_epsilon_millis == 5000
        assert config.probabilistic_max_sample_size == 50
        assert config.probabilistic_acceptable_heartbeat_pause_millis == 20000
        assert config.probabilistic_heartbeat_interval_millis == 1000

    def test_from_dict_recently_active(self):
        data = {
            "function_type": "RECENTLY_ACTIVE",
            "recently_active_heartbeat_tolerance_millis": 45000,
        }
        config = SplitBrainProtectionConfig.from_dict("active-quorum", data)

        assert config.function_type == SplitBrainProtectionFunctionType.RECENTLY_ACTIVE
        assert config.recently_active_heartbeat_tolerance_millis == 45000

    def test_from_dict_invalid_protect_on(self):
        data = {"protect_on": "INVALID"}
        with pytest.raises(ConfigurationException, match="Invalid protect_on"):
            SplitBrainProtectionConfig.from_dict("test", data)

    def test_from_dict_invalid_function_type(self):
        data = {"function_type": "INVALID"}
        with pytest.raises(ConfigurationException, match="Invalid function_type"):
            SplitBrainProtectionConfig.from_dict("test", data)


class TestSplitBrainProtectionEnums:
    """Tests for split-brain protection enums."""

    def test_protect_on_values(self):
        assert SplitBrainProtectionOn.READ.value == "READ"
        assert SplitBrainProtectionOn.WRITE.value == "WRITE"
        assert SplitBrainProtectionOn.READ_WRITE.value == "READ_WRITE"

    def test_function_type_values(self):
        assert SplitBrainProtectionFunctionType.MEMBER_COUNT.value == "MEMBER_COUNT"
        assert SplitBrainProtectionFunctionType.PROBABILISTIC.value == "PROBABILISTIC"
        assert SplitBrainProtectionFunctionType.RECENTLY_ACTIVE.value == "RECENTLY_ACTIVE"


class TestClientConfigSplitBrainProtection:
    """Tests for split-brain protection in ClientConfig."""

    def test_default_empty(self):
        config = ClientConfig()
        assert config.split_brain_protections == {}

    def test_add_split_brain_protection(self):
        config = ClientConfig()
        sbp = SplitBrainProtectionConfig(name="quorum1", min_cluster_size=3)

        result = config.add_split_brain_protection(sbp)
        assert result is config
        assert "quorum1" in config.split_brain_protections
        assert config.split_brain_protections["quorum1"] is sbp

    def test_add_multiple_protections(self):
        config = ClientConfig()
        sbp1 = SplitBrainProtectionConfig(name="read-quorum", protect_on=SplitBrainProtectionOn.READ)
        sbp2 = SplitBrainProtectionConfig(name="write-quorum", protect_on=SplitBrainProtectionOn.WRITE)

        config.add_split_brain_protection(sbp1)
        config.add_split_brain_protection(sbp2)

        assert len(config.split_brain_protections) == 2
        assert config.split_brain_protections["read-quorum"].protect_on == SplitBrainProtectionOn.READ
        assert config.split_brain_protections["write-quorum"].protect_on == SplitBrainProtectionOn.WRITE

    def test_get_split_brain_protection(self):
        config = ClientConfig()
        sbp = SplitBrainProtectionConfig(name="my-quorum")
        config.add_split_brain_protection(sbp)

        assert config.get_split_brain_protection("my-quorum") is sbp
        assert config.get_split_brain_protection("nonexistent") is None

    def test_from_dict(self):
        data = {
            "cluster_name": "test-cluster",
            "split_brain_protections": {
                "quorum1": {
                    "enabled": True,
                    "min_cluster_size": 3,
                    "protect_on": "WRITE",
                },
                "quorum2": {
                    "function_type": "PROBABILISTIC",
                    "min_cluster_size": 5,
                },
            },
        }
        config = ClientConfig.from_dict(data)

        assert len(config.split_brain_protections) == 2

        q1 = config.get_split_brain_protection("quorum1")
        assert q1 is not None
        assert q1.min_cluster_size == 3
        assert q1.protect_on == SplitBrainProtectionOn.WRITE

        q2 = config.get_split_brain_protection("quorum2")
        assert q2 is not None
        assert q2.min_cluster_size == 5
        assert q2.function_type == SplitBrainProtectionFunctionType.PROBABILISTIC
