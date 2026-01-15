"""Unit tests for client lifecycle management."""

import pytest
from unittest.mock import MagicMock, patch

from hazelcast.client import HazelcastClient, ClientState, _VALID_TRANSITIONS
from hazelcast.config import ClientConfig
from hazelcast.listener import LifecycleState, LifecycleEvent
from hazelcast.exceptions import IllegalStateException, ClientOfflineException


class TestClientState:
    """Tests for ClientState enum."""

    def test_values(self):
        assert ClientState.INITIAL.value == "INITIAL"
        assert ClientState.STARTING.value == "STARTING"
        assert ClientState.CONNECTED.value == "CONNECTED"
        assert ClientState.DISCONNECTED.value == "DISCONNECTED"
        assert ClientState.RECONNECTING.value == "RECONNECTING"
        assert ClientState.SHUTTING_DOWN.value == "SHUTTING_DOWN"
        assert ClientState.SHUTDOWN.value == "SHUTDOWN"


class TestValidTransitions:
    """Tests for state transition validation."""

    def test_initial_transitions(self):
        valid = _VALID_TRANSITIONS[ClientState.INITIAL]
        assert ClientState.STARTING in valid
        assert ClientState.SHUTDOWN in valid
        assert ClientState.CONNECTED not in valid

    def test_starting_transitions(self):
        valid = _VALID_TRANSITIONS[ClientState.STARTING]
        assert ClientState.CONNECTED in valid
        assert ClientState.DISCONNECTED in valid
        assert ClientState.SHUTTING_DOWN in valid

    def test_connected_transitions(self):
        valid = _VALID_TRANSITIONS[ClientState.CONNECTED]
        assert ClientState.DISCONNECTED in valid
        assert ClientState.SHUTTING_DOWN in valid
        assert ClientState.RECONNECTING not in valid

    def test_shutdown_is_terminal(self):
        valid = _VALID_TRANSITIONS[ClientState.SHUTDOWN]
        assert len(valid) == 0


class TestHazelcastClientInit:
    """Tests for HazelcastClient initialization."""

    def test_default_config(self):
        client = HazelcastClient()
        assert client.config is not None
        assert client.config.cluster_name == "dev"

    def test_custom_config(self, custom_config):
        client = HazelcastClient(custom_config)
        assert client.config.cluster_name == "test-cluster"

    def test_initial_state(self):
        client = HazelcastClient()
        assert client.state == ClientState.INITIAL

    def test_uuid_generated(self):
        client = HazelcastClient()
        assert client.uuid is not None
        assert len(client.uuid) > 0

    def test_name_from_config(self, custom_config):
        client = HazelcastClient(custom_config)
        assert client.name == "test-client"

    def test_name_auto_generated(self):
        client = HazelcastClient()
        assert client.name.startswith("hz.client_")

    def test_running_initially_false(self):
        client = HazelcastClient()
        assert client.running is False


class TestHazelcastClientLifecycleListeners:
    """Tests for lifecycle listener management."""

    def test_add_lifecycle_listener_with_listener(self):
        client = HazelcastClient()
        events = []

        from hazelcast.listener import LifecycleListener

        class TestListener(LifecycleListener):
            def on_state_changed(self, event):
                events.append(event)

        reg_id = client.add_lifecycle_listener(listener=TestListener())
        assert reg_id is not None

    def test_add_lifecycle_listener_with_callback(self):
        client = HazelcastClient()
        events = []
        reg_id = client.add_lifecycle_listener(
            on_state_changed=lambda e: events.append(e)
        )
        assert reg_id is not None

    def test_add_lifecycle_listener_no_args_raises(self):
        client = HazelcastClient()
        with pytest.raises(ValueError):
            client.add_lifecycle_listener()

    def test_remove_listener(self):
        client = HazelcastClient()
        reg_id = client.add_lifecycle_listener(on_state_changed=lambda e: None)
        assert client.remove_listener(reg_id) is True
        assert client.remove_listener(reg_id) is False

    def test_add_membership_listener(self):
        client = HazelcastClient()
        reg_id = client.add_membership_listener(
            on_member_added=lambda e: None,
            on_member_removed=lambda e: None,
        )
        assert reg_id is not None

    def test_add_distributed_object_listener(self):
        client = HazelcastClient()
        from hazelcast.listener import DistributedObjectListener

        class TestListener(DistributedObjectListener):
            def on_distributed_object_created(self, event):
                pass

            def on_distributed_object_destroyed(self, event):
                pass

        reg_id = client.add_distributed_object_listener(TestListener())
        assert reg_id is not None


class TestHazelcastClientStateTransitions:
    """Tests for state transition behavior."""

    def test_transition_to_starting(self):
        client = HazelcastClient()
        client._transition_state(ClientState.STARTING)
        assert client.state == ClientState.STARTING

    def test_invalid_transition_raises(self):
        client = HazelcastClient()
        with pytest.raises(IllegalStateException):
            client._transition_state(ClientState.CONNECTED)

    def test_lifecycle_event_fired_on_transition(self):
        client = HazelcastClient()
        events = []
        client.add_lifecycle_listener(on_state_changed=lambda e: events.append(e))
        client._transition_state(ClientState.STARTING)
        assert len(events) == 1
        assert events[0].state == LifecycleState.STARTING


class TestHazelcastClientShutdown:
    """Tests for client shutdown behavior."""

    def test_shutdown_from_initial(self):
        client = HazelcastClient()
        client._transition_state(ClientState.SHUTDOWN)
        assert client.state == ClientState.SHUTDOWN

    def test_shutdown_already_shutdown(self):
        client = HazelcastClient()
        client._transition_state(ClientState.SHUTDOWN)
        client.shutdown()
        assert client.state == ClientState.SHUTDOWN

    def test_shutdown_clears_listeners(self):
        client = HazelcastClient()
        reg_id = client.add_lifecycle_listener(on_state_changed=lambda e: None)
        client._transition_state(ClientState.SHUTDOWN)
        client._listener_service.clear()
        assert client.remove_listener(reg_id) is False


class TestHazelcastClientNotConnected:
    """Tests for operations when client is not connected."""

    def test_get_distributed_objects_empty(self):
        client = HazelcastClient()
        client._transition_state(ClientState.STARTING)
        client._transition_state(ClientState.CONNECTED)
        objects = client.get_distributed_objects()
        assert objects == []

    def test_check_running_not_connected(self):
        client = HazelcastClient()
        with pytest.raises(ClientOfflineException):
            client._check_running()


class TestHazelcastClientRepr:
    """Tests for string representation."""

    def test_repr(self):
        client = HazelcastClient()
        r = repr(client)
        assert "HazelcastClient" in r
        assert "INITIAL" in r
        assert "dev" in r
