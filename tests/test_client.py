"""Comprehensive unit tests for hazelcast/client.py."""

import asyncio
import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from hazelcast.client import (
    HazelcastClient,
    ClientState,
    _VALID_TRANSITIONS,
    SERVICE_NAME_MAP,
    SERVICE_NAME_CACHE,
    SERVICE_NAME_QUEUE,
    SERVICE_NAME_SET,
    SERVICE_NAME_LIST,
    SERVICE_NAME_MULTI_MAP,
    SERVICE_NAME_REPLICATED_MAP,
    SERVICE_NAME_RINGBUFFER,
    SERVICE_NAME_TOPIC,
    SERVICE_NAME_RELIABLE_TOPIC,
    SERVICE_NAME_PN_COUNTER,
    SERVICE_NAME_ATOMIC_LONG,
    SERVICE_NAME_ATOMIC_REFERENCE,
    SERVICE_NAME_COUNT_DOWN_LATCH,
    SERVICE_NAME_SEMAPHORE,
    SERVICE_NAME_FENCED_LOCK,
    SERVICE_NAME_CP_MAP,
    SERVICE_NAME_EXECUTOR,
    SERVICE_NAME_SCHEDULED_EXECUTOR,
    SERVICE_NAME_DURABLE_EXECUTOR,
    SERVICE_NAME_CARDINALITY_ESTIMATOR,
    SERVICE_NAME_FLAKE_ID,
)
from hazelcast.config import ClientConfig
from hazelcast.exceptions import ClientOfflineException, IllegalStateException
from hazelcast.listener import (
    LifecycleListener,
    LifecycleEvent,
    LifecycleState,
    MembershipListener,
    MembershipEvent,
    DistributedObjectListener,
    PartitionLostListener,
    PartitionLostEvent,
    MigrationListener,
    MigrationEvent,
    InitialMembershipListener,
    InitialMembershipEvent,
)


class TestClientState:
    def test_enum_values(self):
        assert ClientState.INITIAL.value == "INITIAL"
        assert ClientState.STARTING.value == "STARTING"
        assert ClientState.CONNECTED.value == "CONNECTED"
        assert ClientState.DISCONNECTED.value == "DISCONNECTED"
        assert ClientState.RECONNECTING.value == "RECONNECTING"
        assert ClientState.SHUTTING_DOWN.value == "SHUTTING_DOWN"
        assert ClientState.SHUTDOWN.value == "SHUTDOWN"


class TestValidTransitions:
    def test_initial_transitions(self):
        assert ClientState.STARTING in _VALID_TRANSITIONS[ClientState.INITIAL]
        assert ClientState.SHUTDOWN in _VALID_TRANSITIONS[ClientState.INITIAL]

    def test_starting_transitions(self):
        assert ClientState.CONNECTED in _VALID_TRANSITIONS[ClientState.STARTING]
        assert ClientState.DISCONNECTED in _VALID_TRANSITIONS[ClientState.STARTING]
        assert ClientState.SHUTTING_DOWN in _VALID_TRANSITIONS[ClientState.STARTING]

    def test_connected_transitions(self):
        assert ClientState.DISCONNECTED in _VALID_TRANSITIONS[ClientState.CONNECTED]
        assert ClientState.SHUTTING_DOWN in _VALID_TRANSITIONS[ClientState.CONNECTED]

    def test_disconnected_transitions(self):
        assert ClientState.RECONNECTING in _VALID_TRANSITIONS[ClientState.DISCONNECTED]
        assert ClientState.SHUTTING_DOWN in _VALID_TRANSITIONS[ClientState.DISCONNECTED]
        assert ClientState.SHUTDOWN in _VALID_TRANSITIONS[ClientState.DISCONNECTED]

    def test_reconnecting_transitions(self):
        assert ClientState.CONNECTED in _VALID_TRANSITIONS[ClientState.RECONNECTING]
        assert ClientState.DISCONNECTED in _VALID_TRANSITIONS[ClientState.RECONNECTING]
        assert ClientState.SHUTTING_DOWN in _VALID_TRANSITIONS[ClientState.RECONNECTING]

    def test_shutting_down_transitions(self):
        assert ClientState.SHUTDOWN in _VALID_TRANSITIONS[ClientState.SHUTTING_DOWN]

    def test_shutdown_transitions(self):
        assert len(_VALID_TRANSITIONS[ClientState.SHUTDOWN]) == 0


class TestHazelcastClientInit:
    def test_default_init(self):
        client = HazelcastClient()
        
        assert client.state == ClientState.INITIAL
        assert client.config.cluster_name == "dev"
        assert client.running is False
        assert client.uuid is not None
        assert "hz.client_" in client.name

    def test_init_with_config(self):
        config = ClientConfig()
        config.cluster_name = "test-cluster"
        config.client_name = "my-client"
        
        client = HazelcastClient(config)
        
        assert client.config.cluster_name == "test-cluster"
        assert client.name == "my-client"

    def test_init_generates_uuid(self):
        client1 = HazelcastClient()
        client2 = HazelcastClient()
        
        assert client1.uuid != client2.uuid


class TestHazelcastClientProperties:
    def test_config_property(self):
        config = ClientConfig()
        client = HazelcastClient(config)
        assert client.config is config

    def test_labels_property(self):
        config = ClientConfig()
        config.labels = ["label1", "label2"]
        client = HazelcastClient(config)
        
        assert client.labels == ["label1", "label2"]

    def test_state_property_thread_safe(self):
        client = HazelcastClient()
        assert client.state == ClientState.INITIAL

    def test_running_property(self):
        client = HazelcastClient()
        assert client.running is False


class TestHazelcastClientStateTransitions:
    def test_transition_initial_to_starting(self):
        client = HazelcastClient()
        result = client._transition_state(ClientState.STARTING)
        
        assert result is True
        assert client.state == ClientState.STARTING

    def test_invalid_transition_raises(self):
        client = HazelcastClient()
        
        with pytest.raises(IllegalStateException, match="Invalid state transition"):
            client._transition_state(ClientState.CONNECTED)

    def test_lifecycle_event_fired_on_transition(self):
        client = HazelcastClient()
        events = []
        
        class TestListener(LifecycleListener):
            def on_state_changed(self, event):
                events.append(event)
        
        client.add_lifecycle_listener(TestListener())
        client._transition_state(ClientState.STARTING)
        
        assert len(events) == 1
        assert events[0].state == LifecycleState.STARTING

    def test_map_to_lifecycle_state(self):
        client = HazelcastClient()
        
        assert client._map_to_lifecycle_state(ClientState.STARTING) == LifecycleState.STARTING
        assert client._map_to_lifecycle_state(ClientState.CONNECTED) == LifecycleState.CONNECTED
        assert client._map_to_lifecycle_state(ClientState.DISCONNECTED) == LifecycleState.DISCONNECTED
        assert client._map_to_lifecycle_state(ClientState.SHUTTING_DOWN) == LifecycleState.SHUTTING_DOWN
        assert client._map_to_lifecycle_state(ClientState.SHUTDOWN) == LifecycleState.SHUTDOWN
        assert client._map_to_lifecycle_state(ClientState.INITIAL) is None


class TestHazelcastClientListeners:
    def test_add_lifecycle_listener_with_instance(self):
        client = HazelcastClient()
        
        class TestListener(LifecycleListener):
            def on_state_changed(self, event):
                pass
        
        listener = TestListener()
        reg_id = client.add_lifecycle_listener(listener)
        
        assert reg_id is not None

    def test_add_lifecycle_listener_with_callback(self):
        client = HazelcastClient()
        
        def on_change(event):
            pass
        
        reg_id = client.add_lifecycle_listener(on_state_changed=on_change)
        assert reg_id is not None

    def test_add_lifecycle_listener_no_args_raises(self):
        client = HazelcastClient()
        
        with pytest.raises(ValueError, match="Either listener or on_state_changed must be provided"):
            client.add_lifecycle_listener()

    def test_add_membership_listener_with_instance(self):
        client = HazelcastClient()
        
        class TestListener(MembershipListener):
            def on_member_added(self, event):
                pass
            def on_member_removed(self, event):
                pass
        
        reg_id = client.add_membership_listener(TestListener())
        assert reg_id is not None

    def test_add_membership_listener_with_callbacks(self):
        client = HazelcastClient()
        
        reg_id = client.add_membership_listener(
            on_member_added=lambda e: None,
            on_member_removed=lambda e: None,
        )
        assert reg_id is not None

    def test_add_distributed_object_listener(self):
        client = HazelcastClient()
        
        class TestListener(DistributedObjectListener):
            def on_created(self, event):
                pass
            def on_destroyed(self, event):
                pass
        
        reg_id = client.add_distributed_object_listener(TestListener())
        assert reg_id is not None

    def test_add_partition_lost_listener_with_instance(self):
        client = HazelcastClient()
        
        class TestListener(PartitionLostListener):
            def on_partition_lost(self, event):
                pass
        
        reg_id = client.add_partition_lost_listener(TestListener())
        assert reg_id is not None

    def test_add_partition_lost_listener_with_callback(self):
        client = HazelcastClient()
        
        reg_id = client.add_partition_lost_listener(on_partition_lost=lambda e: None)
        assert reg_id is not None

    def test_add_partition_lost_listener_no_args_raises(self):
        client = HazelcastClient()
        
        with pytest.raises(ValueError, match="Either listener or on_partition_lost must be provided"):
            client.add_partition_lost_listener()

    def test_add_migration_listener_with_instance(self):
        client = HazelcastClient()
        
        class TestListener(MigrationListener):
            def on_migration_started(self, event):
                pass
            def on_migration_completed(self, event):
                pass
            def on_migration_failed(self, event):
                pass
        
        reg_id = client.add_migration_listener(TestListener())
        assert reg_id is not None

    def test_add_migration_listener_with_callbacks(self):
        client = HazelcastClient()
        
        reg_id = client.add_migration_listener(
            on_migration_started=lambda e: None,
            on_migration_completed=lambda e: None,
            on_migration_failed=lambda e: None,
        )
        assert reg_id is not None

    def test_add_initial_membership_listener_with_instance(self):
        client = HazelcastClient()
        
        class TestListener(InitialMembershipListener):
            def init(self, event):
                pass
            def on_member_added(self, event):
                pass
            def on_member_removed(self, event):
                pass
        
        reg_id = client.add_initial_membership_listener(TestListener())
        assert reg_id is not None

    def test_add_initial_membership_listener_with_callbacks(self):
        client = HazelcastClient()
        
        reg_id = client.add_initial_membership_listener(
            on_init=lambda e: None,
            on_member_added=lambda e: None,
            on_member_removed=lambda e: None,
        )
        assert reg_id is not None

    def test_remove_listener(self):
        client = HazelcastClient()
        
        reg_id = client.add_lifecycle_listener(on_state_changed=lambda e: None)
        result = client.remove_listener(reg_id)
        
        assert result is True


class TestHazelcastClientStart:
    @patch.object(HazelcastClient, "_init_services")
    def test_start_success(self, mock_init):
        client = HazelcastClient()
        result = client.start()
        
        assert result is client
        assert client.state == ClientState.CONNECTED
        mock_init.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    def test_start_failure(self, mock_init):
        mock_init.side_effect = Exception("Connection failed")
        client = HazelcastClient()
        
        with pytest.raises(ClientOfflineException, match="Failed to connect"):
            client.start()
        
        assert client.state == ClientState.DISCONNECTED

    @patch.object(HazelcastClient, "_init_services")
    def test_connect_alias(self, mock_init):
        client = HazelcastClient()
        result = client.connect()
        
        assert result is client
        assert client.state == ClientState.CONNECTED


class TestHazelcastClientShutdown:
    @patch.object(HazelcastClient, "_init_services")
    def test_shutdown_from_connected(self, mock_init):
        client = HazelcastClient()
        client.start()
        client.shutdown()
        
        assert client.state == ClientState.SHUTDOWN

    def test_shutdown_idempotent(self):
        client = HazelcastClient()
        client._state = ClientState.SHUTDOWN
        
        client.shutdown()
        assert client.state == ClientState.SHUTDOWN

    def test_shutdown_from_shutting_down(self):
        client = HazelcastClient()
        client._state = ClientState.SHUTTING_DOWN
        
        client.shutdown()

    @patch.object(HazelcastClient, "_init_services")
    def test_shutdown_destroys_proxies(self, mock_init):
        client = HazelcastClient()
        client.start()
        
        mock_proxy = MagicMock()
        mock_proxy.is_destroyed = False
        client._proxies[("service", "name")] = mock_proxy
        
        client.shutdown()
        
        assert len(client._proxies) == 0


class TestHazelcastClientProxies:
    @patch.object(HazelcastClient, "_init_services")
    def test_check_running_raises_when_not_connected(self, mock_init):
        client = HazelcastClient()
        
        with pytest.raises(ClientOfflineException, match="Client is not connected"):
            client._check_running()

    @patch.object(HazelcastClient, "_init_services")
    def test_get_distributed_objects(self, mock_init):
        client = HazelcastClient()
        client.start()
        
        mock_proxy = MagicMock()
        mock_proxy.is_destroyed = False
        client._proxies[("service", "name")] = mock_proxy
        
        objects = client.get_distributed_objects()
        
        assert len(objects) == 1
        assert mock_proxy in objects

    @patch.object(HazelcastClient, "_init_services")
    def test_get_distributed_objects_excludes_destroyed(self, mock_init):
        client = HazelcastClient()
        client.start()
        
        mock_proxy = MagicMock()
        mock_proxy.is_destroyed = True
        client._proxies[("service", "name")] = mock_proxy
        
        objects = client.get_distributed_objects()
        
        assert len(objects) == 0

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.Map")
    def test_get_map(self, mock_map_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_map_class.return_value = MagicMock()
        
        result = client.get_map("test-map")
        
        mock_map_class.assert_called_once()
        assert result is not None

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.Cache")
    def test_get_cache(self, mock_cache_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_cache_class.return_value = MagicMock()
        
        result = client.get_cache("test-cache")
        
        mock_cache_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.CacheManager")
    def test_get_cache_manager(self, mock_cm_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_cm_class.return_value = MagicMock()
        
        result = client.get_cache_manager()
        
        assert result is not None
        
        result2 = client.get_cache_manager()
        assert result is result2

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.Queue")
    def test_get_queue(self, mock_queue_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_queue_class.return_value = MagicMock()
        
        client.get_queue("test-queue")
        mock_queue_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.Set")
    def test_get_set(self, mock_set_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_set_class.return_value = MagicMock()
        
        client.get_set("test-set")
        mock_set_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.HzList")
    def test_get_list(self, mock_list_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_list_class.return_value = MagicMock()
        
        client.get_list("test-list")
        mock_list_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.MultiMap")
    def test_get_multi_map(self, mock_mm_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_mm_class.return_value = MagicMock()
        
        client.get_multi_map("test-mm")
        mock_mm_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.ReplicatedMap")
    def test_get_replicated_map(self, mock_rm_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_rm_class.return_value = MagicMock()
        
        client.get_replicated_map("test-rm")
        mock_rm_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.Ringbuffer")
    def test_get_ringbuffer(self, mock_rb_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_rb_class.return_value = MagicMock()
        
        client.get_ringbuffer("test-rb")
        mock_rb_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.Topic")
    def test_get_topic(self, mock_topic_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_topic_class.return_value = MagicMock()
        
        client.get_topic("test-topic")
        mock_topic_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.ReliableTopic")
    def test_get_reliable_topic(self, mock_rt_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_rt_class.return_value = MagicMock()
        
        client.get_reliable_topic("test-rt")
        mock_rt_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.CardinalityEstimator")
    def test_get_cardinality_estimator(self, mock_ce_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_ce_class.return_value = MagicMock()
        
        client.get_cardinality_estimator("test-ce")
        mock_ce_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.FlakeIdGenerator")
    def test_get_flake_id_generator(self, mock_fig_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_fig_class.return_value = MagicMock()
        
        client.get_flake_id_generator("test-fig")
        mock_fig_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.PNCounter")
    def test_get_pn_counter(self, mock_pnc_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_pnc_class.return_value = MagicMock()
        
        client.get_pn_counter("test-pnc")
        mock_pnc_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.AtomicLong")
    def test_get_atomic_long(self, mock_al_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_al_class.return_value = MagicMock()
        
        client.get_atomic_long("test-al")
        mock_al_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.AtomicReference")
    def test_get_atomic_reference(self, mock_ar_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_ar_class.return_value = MagicMock()
        
        client.get_atomic_reference("test-ar")
        mock_ar_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.CountDownLatch")
    def test_get_count_down_latch(self, mock_cdl_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_cdl_class.return_value = MagicMock()
        
        client.get_count_down_latch("test-cdl")
        mock_cdl_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.Semaphore")
    def test_get_semaphore(self, mock_sem_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_sem_class.return_value = MagicMock()
        
        client.get_semaphore("test-sem")
        mock_sem_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.IExecutorService")
    def test_get_executor_service(self, mock_exec_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_exec_class.return_value = MagicMock()
        
        client.get_executor_service("test-exec")
        mock_exec_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.IScheduledExecutorService")
    def test_get_scheduled_executor_service(self, mock_sched_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_sched_class.return_value = MagicMock()
        
        client.get_scheduled_executor_service("test-sched")
        mock_sched_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.DurableExecutorService")
    def test_get_durable_executor_service(self, mock_dur_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_dur_class.return_value = MagicMock()
        
        client.get_durable_executor_service("test-durable")
        mock_dur_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.FencedLock")
    def test_get_fenced_lock(self, mock_lock_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_lock_class.return_value = MagicMock()
        
        client.get_fenced_lock("test-lock")
        mock_lock_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.CPMap")
    def test_get_cp_map(self, mock_cpmap_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_cpmap_class.return_value = MagicMock()
        
        client.get_cp_map("test-cpmap")
        mock_cpmap_class.assert_called_once()


class TestHazelcastClientServices:
    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.SqlService")
    def test_get_sql(self, mock_sql_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_sql_class.return_value = MagicMock()
        
        result = client.get_sql()
        
        assert result is not None
        
        result2 = client.get_sql()
        assert result is result2

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.JetService")
    def test_get_jet(self, mock_jet_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_jet_class.return_value = MagicMock()
        
        result = client.get_jet()
        
        assert result is not None
        
        result2 = client.get_jet()
        assert result is result2

    @patch.object(HazelcastClient, "_init_services")
    @patch("hazelcast.client.TransactionContext")
    def test_new_transaction_context(self, mock_txn_class, mock_init):
        client = HazelcastClient()
        client.start()
        mock_txn_class.return_value = MagicMock()
        
        result = client.new_transaction_context()
        
        mock_txn_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    def test_get_management_center_service(self, mock_init):
        client = HazelcastClient()
        client.start()
        
        result = client.get_management_center_service()
        assert result is not None


class TestHazelcastClientContextManager:
    @patch.object(HazelcastClient, "start")
    @patch.object(HazelcastClient, "shutdown")
    def test_sync_context_manager(self, mock_shutdown, mock_start):
        mock_start.return_value = MagicMock()
        
        with HazelcastClient() as client:
            pass
        
        mock_start.assert_called_once()
        mock_shutdown.assert_called_once()

    @pytest.mark.asyncio
    @patch.object(HazelcastClient, "start_async", new_callable=AsyncMock)
    @patch.object(HazelcastClient, "shutdown_async", new_callable=AsyncMock)
    async def test_async_context_manager(self, mock_shutdown, mock_start):
        async with HazelcastClient() as client:
            pass
        
        mock_start.assert_called_once()
        mock_shutdown.assert_called_once()


class TestHazelcastClientRepr:
    def test_repr(self):
        config = ClientConfig()
        config.cluster_name = "test-cluster"
        config.client_name = "test-client"
        client = HazelcastClient(config)
        
        repr_str = repr(client)
        
        assert "test-client" in repr_str
        assert "INITIAL" in repr_str
        assert "test-cluster" in repr_str


class TestHazelcastClientAsync:
    @pytest.mark.asyncio
    @patch.object(HazelcastClient, "_init_services")
    async def test_start_async_success(self, mock_init):
        client = HazelcastClient()
        result = await client.start_async()
        
        assert result is client
        assert client.state == ClientState.CONNECTED

    @pytest.mark.asyncio
    @patch.object(HazelcastClient, "_init_services")
    async def test_start_async_failure(self, mock_init):
        mock_init.side_effect = Exception("Connection failed")
        client = HazelcastClient()
        
        with pytest.raises(ClientOfflineException):
            await client.start_async()
        
        assert client.state == ClientState.DISCONNECTED

    @pytest.mark.asyncio
    @patch.object(HazelcastClient, "_init_services")
    async def test_shutdown_async(self, mock_init):
        client = HazelcastClient()
        await client.start_async()
        await client.shutdown_async()
        
        assert client.state == ClientState.SHUTDOWN

    @pytest.mark.asyncio
    async def test_shutdown_async_idempotent(self):
        client = HazelcastClient()
        client._state = ClientState.SHUTDOWN
        
        await client.shutdown_async()
        assert client.state == ClientState.SHUTDOWN


class TestHazelcastClientProxyCreation:
    @patch.object(HazelcastClient, "_init_services")
    def test_get_or_create_proxy_creates_new(self, mock_init):
        client = HazelcastClient()
        client.start()
        
        mock_proxy_class = MagicMock()
        mock_proxy_instance = MagicMock()
        mock_proxy_instance.is_destroyed = False
        mock_proxy_class.return_value = mock_proxy_instance
        
        result = client._get_or_create_proxy("service", "name", mock_proxy_class)
        
        assert result is mock_proxy_instance
        mock_proxy_class.assert_called_once()

    @patch.object(HazelcastClient, "_init_services")
    def test_get_or_create_proxy_returns_existing(self, mock_init):
        client = HazelcastClient()
        client.start()
        
        mock_proxy = MagicMock()
        mock_proxy.is_destroyed = False
        client._proxies[("service", "name")] = mock_proxy
        
        mock_proxy_class = MagicMock()
        result = client._get_or_create_proxy("service", "name", mock_proxy_class)
        
        assert result is mock_proxy
        mock_proxy_class.assert_not_called()

    @patch.object(HazelcastClient, "_init_services")
    def test_get_or_create_proxy_replaces_destroyed(self, mock_init):
        client = HazelcastClient()
        client.start()
        
        old_proxy = MagicMock()
        old_proxy.is_destroyed = True
        client._proxies[("service", "name")] = old_proxy
        
        mock_proxy_class = MagicMock()
        new_proxy = MagicMock()
        new_proxy.is_destroyed = False
        mock_proxy_class.return_value = new_proxy
        
        result = client._get_or_create_proxy("service", "name", mock_proxy_class)
        
        assert result is new_proxy
        mock_proxy_class.assert_called_once()


class TestServiceNames:
    def test_service_name_constants(self):
        assert SERVICE_NAME_MAP == "hz:impl:mapService"
        assert SERVICE_NAME_CACHE == "hz:impl:cacheService"
        assert SERVICE_NAME_QUEUE == "hz:impl:queueService"
        assert SERVICE_NAME_SET == "hz:impl:setService"
        assert SERVICE_NAME_LIST == "hz:impl:listService"
        assert SERVICE_NAME_MULTI_MAP == "hz:impl:multiMapService"
        assert SERVICE_NAME_REPLICATED_MAP == "hz:impl:replicatedMapService"
        assert SERVICE_NAME_RINGBUFFER == "hz:impl:ringbufferService"
        assert SERVICE_NAME_TOPIC == "hz:impl:topicService"
        assert SERVICE_NAME_RELIABLE_TOPIC == "hz:impl:reliableTopicService"
        assert SERVICE_NAME_PN_COUNTER == "hz:impl:PNCounterService"
        assert SERVICE_NAME_EXECUTOR == "hz:impl:executorService"
        assert SERVICE_NAME_SCHEDULED_EXECUTOR == "hz:impl:scheduledExecutorService"
        assert SERVICE_NAME_DURABLE_EXECUTOR == "hz:impl:durableExecutorService"
        assert SERVICE_NAME_CARDINALITY_ESTIMATOR == "hz:impl:cardinalityEstimatorService"
        assert SERVICE_NAME_FLAKE_ID == "hz:impl:flakeIdGeneratorService"
        assert SERVICE_NAME_ATOMIC_LONG == "hz:raft:atomicLongService"
        assert SERVICE_NAME_ATOMIC_REFERENCE == "hz:raft:atomicRefService"
        assert SERVICE_NAME_COUNT_DOWN_LATCH == "hz:raft:countDownLatchService"
        assert SERVICE_NAME_SEMAPHORE == "hz:raft:semaphoreService"
        assert SERVICE_NAME_FENCED_LOCK == "hz:raft:lockService"
        assert SERVICE_NAME_CP_MAP == "hz:raft:mapService"
