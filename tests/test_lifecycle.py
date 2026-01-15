"""Unit tests for client lifecycle and listeners."""

import asyncio
import unittest
from unittest.mock import MagicMock, patch

from hazelcast.client import HazelcastClient, ClientState
from hazelcast.config import ClientConfig
from hazelcast.exceptions import IllegalStateException, ClientOfflineException
from hazelcast.listener import (
    LifecycleState,
    LifecycleEvent,
    LifecycleListener,
    FunctionLifecycleListener,
    MemberInfo,
    MembershipEvent,
    MembershipEventType,
    MembershipListener,
    FunctionMembershipListener,
    DistributedObjectEvent,
    DistributedObjectEventType,
    DistributedObjectListener,
    FunctionDistributedObjectListener,
    ListenerService,
)
from hazelcast.auth import (
    Credentials,
    CredentialsType,
    UsernamePasswordCredentials,
    TokenCredentials,
    KerberosCredentials,
    CustomCredentials,
    StaticCredentialsFactory,
    CallableCredentialsFactory,
    AuthenticationService,
)


class TestClientState(unittest.TestCase):
    """Tests for ClientState enum."""

    def test_states_exist(self):
        self.assertEqual(ClientState.INITIAL.value, "INITIAL")
        self.assertEqual(ClientState.STARTING.value, "STARTING")
        self.assertEqual(ClientState.CONNECTED.value, "CONNECTED")
        self.assertEqual(ClientState.DISCONNECTED.value, "DISCONNECTED")
        self.assertEqual(ClientState.RECONNECTING.value, "RECONNECTING")
        self.assertEqual(ClientState.SHUTTING_DOWN.value, "SHUTTING_DOWN")
        self.assertEqual(ClientState.SHUTDOWN.value, "SHUTDOWN")


class TestHazelcastClientLifecycle(unittest.TestCase):
    """Tests for HazelcastClient lifecycle management."""

    def test_initial_state(self):
        client = HazelcastClient()
        self.assertEqual(client.state, ClientState.INITIAL)
        self.assertFalse(client.running)

    def test_client_has_uuid(self):
        client = HazelcastClient()
        self.assertIsNotNone(client.uuid)
        self.assertEqual(len(client.uuid), 36)

    def test_client_has_name(self):
        client = HazelcastClient()
        self.assertIsNotNone(client.name)
        self.assertTrue(client.name.startswith("hz.client_"))

    def test_custom_client_name(self):
        config = ClientConfig()
        config.client_name = "my-client"
        client = HazelcastClient(config)
        self.assertEqual(client.name, "my-client")

    def test_client_labels(self):
        config = ClientConfig()
        config.labels = ["label1", "label2"]
        client = HazelcastClient(config)
        self.assertEqual(client.labels, ["label1", "label2"])

    def test_start_transitions_to_connected(self):
        client = HazelcastClient()
        client.start()
        self.assertEqual(client.state, ClientState.CONNECTED)
        self.assertTrue(client.running)
        client.shutdown()

    def test_connect_is_alias_for_start(self):
        client = HazelcastClient()
        result = client.connect()
        self.assertEqual(result, client)
        self.assertEqual(client.state, ClientState.CONNECTED)
        client.shutdown()

    def test_shutdown_transitions_to_shutdown(self):
        client = HazelcastClient()
        client.start()
        client.shutdown()
        self.assertEqual(client.state, ClientState.SHUTDOWN)
        self.assertFalse(client.running)

    def test_double_shutdown_is_safe(self):
        client = HazelcastClient()
        client.start()
        client.shutdown()
        client.shutdown()
        self.assertEqual(client.state, ClientState.SHUTDOWN)

    def test_shutdown_from_initial_state(self):
        client = HazelcastClient()
        client.shutdown()
        self.assertEqual(client.state, ClientState.SHUTDOWN)

    def test_invalid_transition_raises(self):
        client = HazelcastClient()
        client.start()
        client.shutdown()
        with self.assertRaises(IllegalStateException):
            client._transition_state(ClientState.CONNECTED)

    def test_context_manager(self):
        with HazelcastClient() as client:
            self.assertEqual(client.state, ClientState.CONNECTED)
        self.assertEqual(client.state, ClientState.SHUTDOWN)

    def test_repr(self):
        client = HazelcastClient()
        result = repr(client)
        self.assertIn("HazelcastClient", result)
        self.assertIn("state=INITIAL", result)
        self.assertIn("cluster=", result)


class TestHazelcastClientAsyncLifecycle(unittest.IsolatedAsyncioTestCase):
    """Async tests for HazelcastClient lifecycle."""

    async def test_async_start(self):
        client = HazelcastClient()
        await client.start_async()
        self.assertEqual(client.state, ClientState.CONNECTED)
        await client.shutdown_async()

    async def test_async_shutdown(self):
        client = HazelcastClient()
        await client.start_async()
        await client.shutdown_async()
        self.assertEqual(client.state, ClientState.SHUTDOWN)

    async def test_async_context_manager(self):
        async with HazelcastClient() as client:
            self.assertEqual(client.state, ClientState.CONNECTED)
        self.assertEqual(client.state, ClientState.SHUTDOWN)


class TestLifecycleEvents(unittest.TestCase):
    """Tests for lifecycle events."""

    def test_lifecycle_event_creation(self):
        event = LifecycleEvent(LifecycleState.CONNECTED)
        self.assertEqual(event.state, LifecycleState.CONNECTED)
        self.assertIsNone(event.previous_state)

    def test_lifecycle_event_with_previous(self):
        event = LifecycleEvent(
            LifecycleState.CONNECTED,
            LifecycleState.STARTING,
        )
        self.assertEqual(event.state, LifecycleState.CONNECTED)
        self.assertEqual(event.previous_state, LifecycleState.STARTING)

    def test_lifecycle_event_str(self):
        event = LifecycleEvent(LifecycleState.CONNECTED, LifecycleState.STARTING)
        result = str(event)
        self.assertIn("STARTING", result)
        self.assertIn("CONNECTED", result)


class TestLifecycleListeners(unittest.TestCase):
    """Tests for lifecycle listeners."""

    def test_function_listener(self):
        events = []
        listener = FunctionLifecycleListener(lambda e: events.append(e))
        event = LifecycleEvent(LifecycleState.CONNECTED)
        listener.on_state_changed(event)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].state, LifecycleState.CONNECTED)

    def test_add_lifecycle_listener_with_callback(self):
        client = HazelcastClient()
        events = []

        reg_id = client.add_lifecycle_listener(
            on_state_changed=lambda e: events.append(e)
        )
        self.assertIsNotNone(reg_id)

        client.start()
        self.assertTrue(any(e.state == LifecycleState.CONNECTED for e in events))
        client.shutdown()

    def test_add_lifecycle_listener_with_instance(self):
        client = HazelcastClient()
        events = []
        listener = FunctionLifecycleListener(lambda e: events.append(e))

        reg_id = client.add_lifecycle_listener(listener=listener)
        self.assertIsNotNone(reg_id)
        client.shutdown()

    def test_remove_lifecycle_listener(self):
        client = HazelcastClient()
        events = []

        reg_id = client.add_lifecycle_listener(
            on_state_changed=lambda e: events.append(e)
        )
        client.remove_listener(reg_id)

        client.start()
        self.assertEqual(len(events), 0)
        client.shutdown()


class TestMembershipEvents(unittest.TestCase):
    """Tests for membership events and listeners."""

    def test_member_info(self):
        member = MemberInfo(
            member_uuid="uuid-123",
            address="localhost:5701",
            is_lite_member=False,
        )
        self.assertEqual(member.uuid, "uuid-123")
        self.assertEqual(member.address, "localhost:5701")
        self.assertFalse(member.is_lite_member)

    def test_member_info_equality(self):
        member1 = MemberInfo("uuid-123", "localhost:5701")
        member2 = MemberInfo("uuid-123", "localhost:5702")
        member3 = MemberInfo("uuid-456", "localhost:5701")
        self.assertEqual(member1, member2)
        self.assertNotEqual(member1, member3)

    def test_membership_event(self):
        member = MemberInfo("uuid-123", "localhost:5701")
        event = MembershipEvent(
            MembershipEventType.MEMBER_ADDED,
            member,
            [member],
        )
        self.assertEqual(event.event_type, MembershipEventType.MEMBER_ADDED)
        self.assertEqual(event.member, member)
        self.assertEqual(len(event.members), 1)

    def test_function_membership_listener(self):
        added_events = []
        removed_events = []

        listener = FunctionMembershipListener(
            on_added=lambda e: added_events.append(e),
            on_removed=lambda e: removed_events.append(e),
        )

        member = MemberInfo("uuid-123", "localhost:5701")
        add_event = MembershipEvent(MembershipEventType.MEMBER_ADDED, member, [member])
        remove_event = MembershipEvent(MembershipEventType.MEMBER_REMOVED, member, [])

        listener.on_member_added(add_event)
        listener.on_member_removed(remove_event)

        self.assertEqual(len(added_events), 1)
        self.assertEqual(len(removed_events), 1)


class TestDistributedObjectEvents(unittest.TestCase):
    """Tests for distributed object events and listeners."""

    def test_distributed_object_event(self):
        event = DistributedObjectEvent(
            DistributedObjectEventType.CREATED,
            "hz:impl:mapService",
            "my-map",
        )
        self.assertEqual(event.event_type, DistributedObjectEventType.CREATED)
        self.assertEqual(event.service_name, "hz:impl:mapService")
        self.assertEqual(event.object_name, "my-map")

    def test_function_distributed_object_listener(self):
        created_events = []
        destroyed_events = []

        listener = FunctionDistributedObjectListener(
            on_created=lambda e: created_events.append(e),
            on_destroyed=lambda e: destroyed_events.append(e),
        )

        create_event = DistributedObjectEvent(
            DistributedObjectEventType.CREATED,
            "hz:impl:mapService",
            "my-map",
        )
        destroy_event = DistributedObjectEvent(
            DistributedObjectEventType.DESTROYED,
            "hz:impl:mapService",
            "my-map",
        )

        listener.on_distributed_object_created(create_event)
        listener.on_distributed_object_destroyed(destroy_event)

        self.assertEqual(len(created_events), 1)
        self.assertEqual(len(destroyed_events), 1)


class TestListenerService(unittest.TestCase):
    """Tests for ListenerService."""

    def test_add_and_fire_lifecycle_listener(self):
        service = ListenerService()
        events = []

        reg_id = service.add_lifecycle_listener(
            FunctionLifecycleListener(lambda e: events.append(e))
        )
        self.assertIsNotNone(reg_id)

        event = LifecycleEvent(LifecycleState.CONNECTED)
        service.fire_lifecycle_event(event)

        self.assertEqual(len(events), 1)

    def test_remove_listener(self):
        service = ListenerService()
        events = []

        reg_id = service.add_lifecycle_listener(
            FunctionLifecycleListener(lambda e: events.append(e))
        )

        self.assertTrue(service.remove_listener(reg_id))
        self.assertFalse(service.remove_listener(reg_id))

        service.fire_lifecycle_event(LifecycleEvent(LifecycleState.CONNECTED))
        self.assertEqual(len(events), 0)

    def test_clear_listeners(self):
        service = ListenerService()
        events = []

        service.add_lifecycle_listener(
            FunctionLifecycleListener(lambda e: events.append(e))
        )
        service.clear()

        service.fire_lifecycle_event(LifecycleEvent(LifecycleState.CONNECTED))
        self.assertEqual(len(events), 0)


class TestCredentials(unittest.TestCase):
    """Tests for credential classes."""

    def test_username_password_credentials(self):
        creds = UsernamePasswordCredentials("user", "pass")
        self.assertEqual(creds.credentials_type, CredentialsType.USERNAME_PASSWORD)
        self.assertEqual(creds.username, "user")
        self.assertEqual(creds.password, "pass")

        data = creds.to_dict()
        self.assertEqual(data["type"], "USERNAME_PASSWORD")
        self.assertEqual(data["username"], "user")

    def test_token_credentials(self):
        creds = TokenCredentials("my-token")
        self.assertEqual(creds.credentials_type, CredentialsType.TOKEN)
        self.assertEqual(creds.token, "my-token")

        data = creds.to_dict()
        self.assertEqual(data["type"], "TOKEN")
        self.assertEqual(data["token"], "my-token")

    def test_kerberos_credentials(self):
        creds = KerberosCredentials(
            service_principal="hazelcast/server@REALM",
            spn="hazelcast/server",
        )
        self.assertEqual(creds.credentials_type, CredentialsType.KERBEROS)
        self.assertEqual(creds.service_principal, "hazelcast/server@REALM")
        self.assertEqual(creds.spn, "hazelcast/server")

    def test_custom_credentials(self):
        creds = CustomCredentials({"key": "value"})
        self.assertEqual(creds.credentials_type, CredentialsType.CUSTOM)
        self.assertEqual(creds.data, {"key": "value"})


class TestCredentialsFactory(unittest.TestCase):
    """Tests for credentials factory classes."""

    def test_static_factory(self):
        creds = TokenCredentials("token")
        factory = StaticCredentialsFactory(creds)
        result = factory.create_credentials()
        self.assertEqual(result, creds)

    def test_callable_factory(self):
        call_count = [0]

        def create():
            call_count[0] += 1
            return TokenCredentials(f"token-{call_count[0]}")

        factory = CallableCredentialsFactory(create)
        creds1 = factory.create_credentials()
        creds2 = factory.create_credentials()

        self.assertEqual(creds1.token, "token-1")
        self.assertEqual(creds2.token, "token-2")


class TestAuthenticationService(unittest.TestCase):
    """Tests for AuthenticationService."""

    def test_initial_state(self):
        service = AuthenticationService()
        self.assertFalse(service.is_authenticated)
        self.assertIsNone(service.cluster_uuid)

    def test_with_credentials(self):
        creds = UsernamePasswordCredentials("user", "pass")
        service = AuthenticationService(credentials=creds)
        result = service.get_credentials()
        self.assertEqual(result, creds)

    def test_with_factory(self):
        creds = TokenCredentials("token")
        factory = StaticCredentialsFactory(creds)
        service = AuthenticationService(credentials_factory=factory)
        result = service.get_credentials()
        self.assertEqual(result.token, "token")

    def test_mark_authenticated(self):
        service = AuthenticationService()
        service.mark_authenticated("cluster-uuid", "member-uuid")
        self.assertTrue(service.is_authenticated)
        self.assertEqual(service.cluster_uuid, "cluster-uuid")
        self.assertEqual(service.member_uuid, "member-uuid")

    def test_reset(self):
        service = AuthenticationService()
        service.mark_authenticated("cluster-uuid", "member-uuid")
        service.reset()
        self.assertFalse(service.is_authenticated)
        self.assertIsNone(service.cluster_uuid)

    def test_from_config_with_username(self):
        from hazelcast.config import SecurityConfig
        config = SecurityConfig(username="user", password="pass")
        service = AuthenticationService.from_config(config)
        creds = service.get_credentials()
        self.assertIsInstance(creds, UsernamePasswordCredentials)
        self.assertEqual(creds.username, "user")

    def test_from_config_with_token(self):
        from hazelcast.config import SecurityConfig
        config = SecurityConfig(token="my-token")
        service = AuthenticationService.from_config(config)
        creds = service.get_credentials()
        self.assertIsInstance(creds, TokenCredentials)
        self.assertEqual(creds.token, "my-token")


if __name__ == "__main__":
    unittest.main()
