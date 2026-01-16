"""Hazelcast client implementation."""

import asyncio
import logging
import threading
import uuid
from enum import Enum
from typing import Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

from hazelcast.config import ClientConfig
from hazelcast.exceptions import (
    ClientOfflineException,
    IllegalStateException,
    HazelcastException,
)
from hazelcast.logging import get_logger

_logger = get_logger("client")
from hazelcast.listener import (
    LifecycleState,
    LifecycleEvent,
    LifecycleListener,
    FunctionLifecycleListener,
    MembershipListener,
    FunctionMembershipListener,
    MembershipEvent,
    DistributedObjectListener,
    DistributedObjectEvent,
    DistributedObjectEventType,
    ListenerService,
)
from hazelcast.auth import AuthenticationService
from hazelcast.proxy.base import DistributedObject, Proxy, ProxyContext

if TYPE_CHECKING:
    from hazelcast.proxy.map import Map
    from hazelcast.proxy.executor import IExecutorService
    from hazelcast.proxy.queue import Queue
    from hazelcast.proxy.collections import Set, List as HzList
    from hazelcast.proxy.multi_map import MultiMap
    from hazelcast.proxy.replicated_map import ReplicatedMap
    from hazelcast.proxy.ringbuffer import Ringbuffer
    from hazelcast.proxy.topic import Topic
    from hazelcast.proxy.reliable_topic import ReliableTopic
    from hazelcast.proxy.pn_counter import PNCounter
    from hazelcast.cp.atomic import AtomicLong, AtomicReference
    from hazelcast.proxy.cardinality_estimator import CardinalityEstimator
    from hazelcast.cp.sync import CountDownLatch, Semaphore, FencedLock
    from hazelcast.sql.service import SqlService
    from hazelcast.jet.service import JetService
    from hazelcast.transaction import TransactionContext, TransactionOptions


SERVICE_NAME_MAP = "hz:impl:mapService"
SERVICE_NAME_EXECUTOR = "hz:impl:executorService"
SERVICE_NAME_CARDINALITY_ESTIMATOR = "hz:impl:cardinalityEstimatorService"
SERVICE_NAME_FLAKE_ID = "hz:impl:flakeIdGeneratorService"
SERVICE_NAME_QUEUE = "hz:impl:queueService"
SERVICE_NAME_SET = "hz:impl:setService"
SERVICE_NAME_LIST = "hz:impl:listService"
SERVICE_NAME_MULTI_MAP = "hz:impl:multiMapService"
SERVICE_NAME_REPLICATED_MAP = "hz:impl:replicatedMapService"
SERVICE_NAME_RINGBUFFER = "hz:impl:ringbufferService"
SERVICE_NAME_TOPIC = "hz:impl:topicService"
SERVICE_NAME_RELIABLE_TOPIC = "hz:impl:reliableTopicService"
SERVICE_NAME_PN_COUNTER = "hz:impl:PNCounterService"
SERVICE_NAME_ATOMIC_LONG = "hz:raft:atomicLongService"
SERVICE_NAME_ATOMIC_REFERENCE = "hz:raft:atomicRefService"
SERVICE_NAME_COUNT_DOWN_LATCH = "hz:raft:countDownLatchService"
SERVICE_NAME_SEMAPHORE = "hz:raft:semaphoreService"
SERVICE_NAME_FENCED_LOCK = "hz:raft:lockService"


class ClientState(Enum):
    """Internal client state for the state machine."""

    INITIAL = "INITIAL"
    STARTING = "STARTING"
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    RECONNECTING = "RECONNECTING"
    SHUTTING_DOWN = "SHUTTING_DOWN"
    SHUTDOWN = "SHUTDOWN"


_VALID_TRANSITIONS = {
    ClientState.INITIAL: {ClientState.STARTING, ClientState.SHUTDOWN},
    ClientState.STARTING: {ClientState.CONNECTED, ClientState.DISCONNECTED, ClientState.SHUTTING_DOWN},
    ClientState.CONNECTED: {ClientState.DISCONNECTED, ClientState.SHUTTING_DOWN},
    ClientState.DISCONNECTED: {ClientState.RECONNECTING, ClientState.SHUTTING_DOWN, ClientState.SHUTDOWN},
    ClientState.RECONNECTING: {ClientState.CONNECTED, ClientState.DISCONNECTED, ClientState.SHUTTING_DOWN},
    ClientState.SHUTTING_DOWN: {ClientState.SHUTDOWN},
    ClientState.SHUTDOWN: set(),
}


class HazelcastClient:
    """Hazelcast Python Client.

    The main entry point for connecting to a Hazelcast cluster. Provides access
    to distributed data structures, SQL queries, CP subsystem, and Jet pipelines.

    The client supports both synchronous and asynchronous operations, with
    automatic connection management, failover, and reconnection.

    Attributes:
        config: The client configuration.
        name: The client instance name.
        uuid: The unique client identifier.
        state: The current client state (INITIAL, CONNECTED, etc.).
        running: Whether the client is connected and operational.

    Example:
        Basic usage with context manager::

            from hazelcast import HazelcastClient, ClientConfig

            config = ClientConfig()
            config.cluster_name = "dev"
            config.cluster_members = ["localhost:5701"]

            with HazelcastClient(config) as client:
                my_map = client.get_map("my-map")
                my_map.put("key", "value")
                value = my_map.get("key")

        Async usage::

            async with HazelcastClient(config) as client:
                my_map = client.get_map("my-map")
                await my_map.put_async("key", "value")

    Note:
        Always call `shutdown()` or use a context manager to properly
        close connections when done with the client.
    """

    def __init__(self, config: ClientConfig = None):
        """Initialize the Hazelcast client.

        Creates a new client instance with the specified configuration.
        The client is not connected until `start()` is called.

        Args:
            config: Client configuration specifying cluster addresses,
                credentials, serialization settings, and more.
                If None, default configuration connecting to
                localhost:5701 is used.

        Example:
            >>> config = ClientConfig()
            >>> config.cluster_name = "production"
            >>> config.cluster_members = ["node1:5701", "node2:5701"]
            >>> client = HazelcastClient(config)
        """
        self._config = config or ClientConfig()
        self._state = ClientState.INITIAL
        self._state_lock = threading.Lock()

        self._client_uuid = str(uuid.uuid4())
        self._client_name = self._config.client_name or f"hz.client_{self._client_uuid[:8]}"

        self._listener_service = ListenerService()
        self._auth_service = AuthenticationService.from_config(self._config.security)

        self._connection_manager = None
        self._invocation_service = None
        self._serialization_service = None
        self._partition_service = None
        self._metrics_registry = None

        self._proxy_context: Optional[ProxyContext] = None
        self._proxies: Dict[Tuple[str, str], Proxy] = {}
        self._proxies_lock = threading.Lock()

        self._sql_service = None
        self._jet_service = None

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None

    @property
    def config(self) -> ClientConfig:
        """Get the client configuration."""
        return self._config

    @property
    def name(self) -> str:
        """Get the client name."""
        return self._client_name

    @property
    def uuid(self) -> str:
        """Get the client UUID."""
        return self._client_uuid

    @property
    def labels(self) -> List[str]:
        """Get the client labels."""
        return self._config.labels

    @property
    def state(self) -> ClientState:
        """Get the current client state."""
        with self._state_lock:
            return self._state

    @property
    def running(self) -> bool:
        """Check if the client is running and connected."""
        return self._state in (ClientState.CONNECTED, ClientState.RECONNECTING)

    @property
    def lifecycle_service(self) -> ListenerService:
        """Get the listener service for registering listeners."""
        return self._listener_service

    @property
    def invocation_service(self):
        """Get the invocation service."""
        return self._invocation_service

    @property
    def serialization_service(self):
        """Get the serialization service."""
        return self._serialization_service

    @property
    def partition_service(self):
        """Get the partition service."""
        return self._partition_service

    def _transition_state(self, new_state: ClientState) -> bool:
        """Transition to a new state if valid.

        Args:
            new_state: The target state.

        Returns:
            True if transition was successful.

        Raises:
            IllegalStateException: If the transition is not valid.
        """
        with self._state_lock:
            current = self._state
            valid_targets = _VALID_TRANSITIONS.get(current, set())

            if new_state not in valid_targets:
                _logger.error(
                    "Invalid state transition attempted: %s -> %s",
                    current.value,
                    new_state.value,
                )
                raise IllegalStateException(
                    f"Invalid state transition: {current.value} -> {new_state.value}"
                )

            self._state = new_state
            _logger.debug("Client state changed: %s -> %s", current.value, new_state.value)

        lifecycle_state = self._map_to_lifecycle_state(new_state)
        if lifecycle_state:
            previous_lifecycle = self._map_to_lifecycle_state(current)
            event = LifecycleEvent(lifecycle_state, previous_lifecycle)
            self._listener_service.fire_lifecycle_event(event)

        return True

    def _map_to_lifecycle_state(self, client_state: ClientState) -> Optional[LifecycleState]:
        """Map internal client state to lifecycle state for events."""
        mapping = {
            ClientState.STARTING: LifecycleState.STARTING,
            ClientState.CONNECTED: LifecycleState.CONNECTED,
            ClientState.DISCONNECTED: LifecycleState.DISCONNECTED,
            ClientState.SHUTTING_DOWN: LifecycleState.SHUTTING_DOWN,
            ClientState.SHUTDOWN: LifecycleState.SHUTDOWN,
        }
        return mapping.get(client_state)

    def add_lifecycle_listener(
        self,
        listener: LifecycleListener = None,
        on_state_changed: Callable[[LifecycleEvent], None] = None,
    ) -> str:
        """Add a lifecycle listener.

        Args:
            listener: A LifecycleListener instance, or
            on_state_changed: A callback function for state changes.

        Returns:
            Registration ID for removing the listener.
        """
        if listener is None and on_state_changed is not None:
            listener = FunctionLifecycleListener(on_state_changed)
        elif listener is None:
            raise ValueError("Either listener or on_state_changed must be provided")

        return self._listener_service.add_lifecycle_listener(listener)

    def add_membership_listener(
        self,
        listener: MembershipListener = None,
        on_member_added: Callable[[MembershipEvent], None] = None,
        on_member_removed: Callable[[MembershipEvent], None] = None,
    ) -> str:
        """Add a membership listener.

        Args:
            listener: A MembershipListener instance, or
            on_member_added: Callback for member added events.
            on_member_removed: Callback for member removed events.

        Returns:
            Registration ID for removing the listener.
        """
        if listener is None:
            listener = FunctionMembershipListener(on_member_added, on_member_removed)

        return self._listener_service.add_membership_listener(listener)

    def add_distributed_object_listener(
        self,
        listener: DistributedObjectListener,
    ) -> str:
        """Add a distributed object listener.

        Args:
            listener: A DistributedObjectListener instance.

        Returns:
            Registration ID for removing the listener.
        """
        return self._listener_service.add_distributed_object_listener(listener)

    def remove_listener(self, registration_id: str) -> bool:
        """Remove a registered listener.

        Args:
            registration_id: The registration ID returned when adding the listener.

        Returns:
            True if the listener was removed.
        """
        return self._listener_service.remove_listener(registration_id)

    def start(self) -> "HazelcastClient":
        """Start the client and connect to the cluster synchronously.

        Establishes connections to the Hazelcast cluster, authenticates,
        and initializes all internal services. After successful connection,
        the client is ready to use distributed data structures.

        Returns:
            This client instance for method chaining.

        Raises:
            IllegalStateException: If the client is not in INITIAL state.
            ClientOfflineException: If connection to the cluster fails.
            AuthenticationException: If authentication with the cluster fails.

        Example:
            >>> client = HazelcastClient(config)
            >>> client.start()
            >>> # Client is now connected
            >>> client.shutdown()
        """
        _logger.info(
            "Starting Hazelcast client %s (cluster=%s)",
            self._client_name,
            self._config.cluster_name,
        )
        self._transition_state(ClientState.STARTING)

        try:
            self._start_internal()
            self._transition_state(ClientState.CONNECTED)
            _logger.info("Hazelcast client %s connected successfully", self._client_name)
        except Exception as e:
            _logger.error("Failed to connect: %s", e)
            self._transition_state(ClientState.DISCONNECTED)
            raise ClientOfflineException(f"Failed to connect: {e}")

        return self

    def _start_internal(self) -> None:
        """Internal startup logic."""
        self._init_services()

    def _init_services(self) -> None:
        """Initialize all client services."""
        from hazelcast.serialization.service import SerializationService
        from hazelcast.service.partition import PartitionService
        from hazelcast.network.connection_manager import ConnectionManager
        from hazelcast.invocation import InvocationService
        from hazelcast.metrics import MetricsRegistry

        self._metrics_registry = MetricsRegistry()
        self._serialization_service = SerializationService(self._config.serialization)
        self._partition_service = PartitionService()
        self._connection_manager = ConnectionManager(
            self._config,
            self._auth_service,
            self._listener_service,
        )
        self._invocation_service = InvocationService(
            self._connection_manager,
            self._config,
        )

        self._proxy_context = ProxyContext(
            invocation_service=self._invocation_service,
            serialization_service=self._serialization_service,
            partition_service=self._partition_service,
            listener_service=self._listener_service,
        )

    def connect(self) -> "HazelcastClient":
        """Alias for start() - connect to the cluster.

        Returns:
            This client instance.
        """
        return self.start()

    async def start_async(self) -> "HazelcastClient":
        """Start the client and connect to the cluster asynchronously.

        Returns:
            This client instance.
        """
        self._transition_state(ClientState.STARTING)

        try:
            await self._start_async_internal()
            self._transition_state(ClientState.CONNECTED)
        except Exception as e:
            self._transition_state(ClientState.DISCONNECTED)
            raise ClientOfflineException(f"Failed to connect: {e}")

        return self

    async def _start_async_internal(self) -> None:
        """Internal async startup logic."""
        self._init_services()

    def shutdown(self) -> None:
        """Shutdown the client and disconnect from the cluster.

        Gracefully closes all connections, destroys proxies, and releases
        resources. After shutdown, the client cannot be restarted.

        This method is idempotent - calling it multiple times has no effect.

        Example:
            >>> client = HazelcastClient(config)
            >>> client.start()
            >>> # ... use the client ...
            >>> client.shutdown()
        """
        current_state = self.state

        if current_state == ClientState.SHUTDOWN:
            return

        if current_state == ClientState.SHUTTING_DOWN:
            return

        _logger.info("Shutting down Hazelcast client %s", self._client_name)

        try:
            self._transition_state(ClientState.SHUTTING_DOWN)
        except IllegalStateException:
            return

        try:
            self._shutdown_internal()
        finally:
            self._transition_state(ClientState.SHUTDOWN)
            self._listener_service.clear()
            _logger.info("Hazelcast client %s shutdown complete", self._client_name)

    def _shutdown_internal(self) -> None:
        """Internal shutdown logic."""
        self._destroy_proxies()
        self._shutdown_services()

    def _destroy_proxies(self) -> None:
        """Destroy all created proxies."""
        with self._proxies_lock:
            for proxy in list(self._proxies.values()):
                try:
                    if not proxy.is_destroyed:
                        proxy._destroyed = True
                except Exception:
                    pass
            self._proxies.clear()

    def _shutdown_services(self) -> None:
        """Shutdown all client services."""
        if self._invocation_service:
            try:
                self._invocation_service.shutdown()
            except Exception:
                pass

        if self._connection_manager:
            try:
                self._connection_manager.shutdown()
            except Exception:
                pass

    async def shutdown_async(self) -> None:
        """Shutdown the client asynchronously."""
        current_state = self.state

        if current_state in (ClientState.SHUTDOWN, ClientState.SHUTTING_DOWN):
            return

        try:
            self._transition_state(ClientState.SHUTTING_DOWN)
        except IllegalStateException:
            return

        try:
            await self._shutdown_async_internal()
        finally:
            self._transition_state(ClientState.SHUTDOWN)
            self._listener_service.clear()

    async def _shutdown_async_internal(self) -> None:
        """Internal async shutdown logic."""
        self._destroy_proxies()
        self._shutdown_services()

    def _get_or_create_proxy(
        self,
        service_name: str,
        name: str,
        proxy_class: type,
    ) -> Proxy:
        """Get an existing proxy or create a new one.

        Args:
            service_name: The service name for the distributed object.
            name: The name of the distributed object.
            proxy_class: The proxy class to instantiate.

        Returns:
            The proxy instance.
        """
        self._check_running()

        key = (service_name, name)
        with self._proxies_lock:
            if key in self._proxies:
                proxy = self._proxies[key]
                if not proxy.is_destroyed:
                    _logger.debug("Returning existing proxy: %s/%s", service_name, name)
                    return proxy

            _logger.debug("Creating new proxy: %s/%s", service_name, name)
            proxy = proxy_class(service_name, name, self._proxy_context)
            self._proxies[key] = proxy

        event = DistributedObjectEvent(
            DistributedObjectEventType.CREATED,
            service_name,
            name,
            proxy,
        )
        self._listener_service.fire_distributed_object_event(event)

        return proxy

    def _check_running(self) -> None:
        """Check if the client is running."""
        if not self.running:
            raise ClientOfflineException("Client is not connected")

    def get_distributed_objects(self) -> List[DistributedObject]:
        """Get all distributed objects created by this client.

        Returns:
            List of all distributed objects.
        """
        with self._proxies_lock:
            return [p for p in self._proxies.values() if not p.is_destroyed]

    def get_map(self, name: str) -> "Map":
        """Get or create a distributed Map.

        Returns a proxy to a distributed map. The map is created on the
        cluster if it doesn't exist. Multiple calls with the same name
        return the same proxy instance.

        Args:
            name: Name of the distributed map. Must be unique within
                the cluster.

        Returns:
            MapProxy instance for performing map operations.

        Raises:
            ClientOfflineException: If the client is not connected.

        Example:
            >>> my_map = client.get_map("users")
            >>> my_map.put("user:1", {"name": "Alice", "age": 30})
            >>> user = my_map.get("user:1")
        """
        from hazelcast.proxy.map import Map
        return self._get_or_create_proxy(SERVICE_NAME_MAP, name, Map)

    def get_queue(self, name: str) -> "Queue":
        """Get or create a distributed Queue.

        Returns a proxy to a distributed blocking queue.

        Args:
            name: Name of the distributed queue.

        Returns:
            QueueProxy instance for queue operations.

        Raises:
            ClientOfflineException: If the client is not connected.

        Example:
            >>> queue = client.get_queue("task-queue")
            >>> queue.offer("task-1")
            >>> task = queue.poll(timeout=5.0)
        """
        from hazelcast.proxy.queue import Queue
        return self._get_or_create_proxy(SERVICE_NAME_QUEUE, name, Queue)

    def get_set(self, name: str) -> "Set":
        """Get or create a distributed Set.

        Returns a proxy to a distributed set that doesn't allow duplicates.

        Args:
            name: Name of the distributed set.

        Returns:
            SetProxy instance for set operations.

        Raises:
            ClientOfflineException: If the client is not connected.

        Example:
            >>> my_set = client.get_set("unique-ids")
            >>> my_set.add("id-1")
            >>> exists = my_set.contains("id-1")
        """
        from hazelcast.proxy.collections import Set
        return self._get_or_create_proxy(SERVICE_NAME_SET, name, Set)

    def get_list(self, name: str) -> "HzList":
        """Get or create a distributed List.

        Returns a proxy to a distributed list that maintains insertion order.

        Args:
            name: Name of the distributed list.

        Returns:
            ListProxy instance for list operations.

        Raises:
            ClientOfflineException: If the client is not connected.

        Example:
            >>> my_list = client.get_list("items")
            >>> my_list.add("item-1")
            >>> item = my_list.get(0)
        """
        from hazelcast.proxy.collections import List as HzList
        return self._get_or_create_proxy(SERVICE_NAME_LIST, name, HzList)

    def get_multi_map(self, name: str) -> "MultiMap":
        """Get or create a distributed MultiMap.

        Returns a proxy to a distributed map that allows multiple values
        per key.

        Args:
            name: Name of the distributed multi-map.

        Returns:
            MultiMapProxy instance for multi-map operations.

        Raises:
            ClientOfflineException: If the client is not connected.

        Example:
            >>> mm = client.get_multi_map("user-roles")
            >>> mm.put("user:1", "admin")
            >>> mm.put("user:1", "editor")
            >>> roles = mm.get("user:1")  # ["admin", "editor"]
        """
        from hazelcast.proxy.multi_map import MultiMap
        return self._get_or_create_proxy(SERVICE_NAME_MULTI_MAP, name, MultiMap)

    def get_replicated_map(self, name: str) -> "ReplicatedMap":
        """Get or create a distributed ReplicatedMap.

        Returns a proxy to a replicated map where data is stored on all
        cluster members. Unlike IMap, ReplicatedMap does not partition
        data - every member holds a complete copy.

        ReplicatedMap is suitable for small, read-heavy datasets where
        eventual consistency is acceptable.

        Args:
            name: Name of the distributed replicated map.

        Returns:
            ReplicatedMapProxy instance for replicated map operations.

        Raises:
            ClientOfflineException: If the client is not connected.

        Example:
            >>> rep_map = client.get_replicated_map("config")
            >>> rep_map.put("timeout", 30, ttl=300)
            >>> timeout = rep_map.get("timeout")
        """
        from hazelcast.proxy.replicated_map import ReplicatedMap
        return self._get_or_create_proxy(SERVICE_NAME_REPLICATED_MAP, name, ReplicatedMap)

    def get_ringbuffer(self, name: str) -> "Ringbuffer":
        """Get or create a distributed Ringbuffer.

        Returns a proxy to a distributed ringbuffer - a bounded, circular
        data structure with sequence-based access.

        Args:
            name: Name of the distributed ringbuffer.

        Returns:
            RingbufferProxy instance for ringbuffer operations.

        Raises:
            ClientOfflineException: If the client is not connected.

        Example:
            >>> rb = client.get_ringbuffer("events")
            >>> seq = rb.add("event-data")
            >>> item = rb.read_one(seq)
        """
        from hazelcast.proxy.ringbuffer import Ringbuffer
        return self._get_or_create_proxy(SERVICE_NAME_RINGBUFFER, name, Ringbuffer)

    def get_topic(self, name: str) -> "Topic":
        """Get or create a distributed Topic.

        Returns a proxy to a distributed publish-subscribe topic.

        Args:
            name: Name of the distributed topic.

        Returns:
            TopicProxy instance for pub-sub operations.

        Raises:
            ClientOfflineException: If the client is not connected.

        Example:
            >>> topic = client.get_topic("notifications")
            >>> topic.add_message_listener(on_message=lambda m: print(m.message))
            >>> topic.publish("Hello, subscribers!")
        """
        from hazelcast.proxy.topic import Topic
        return self._get_or_create_proxy(SERVICE_NAME_TOPIC, name, Topic)

    def get_reliable_topic(self, name: str) -> "ReliableTopic":
        """Get or create a distributed ReliableTopic.

        Args:
            name: Name of the reliable topic.

        Returns:
            The ReliableTopic proxy.
        """
        from hazelcast.proxy.reliable_topic import ReliableTopic
        return self._get_or_create_proxy(SERVICE_NAME_RELIABLE_TOPIC, name, ReliableTopic)

    def get_cardinality_estimator(self, name: str) -> "CardinalityEstimator":
        """Get or create a distributed CardinalityEstimator.

        Returns a proxy to a distributed cardinality estimator that uses
        the HyperLogLog algorithm to estimate the number of distinct
        elements added to it.

        CardinalityEstimator is useful for counting unique visitors,
        unique events, or any scenario requiring approximate distinct
        counts with low memory overhead.

        Args:
            name: Name of the distributed cardinality estimator.

        Returns:
            CardinalityEstimator instance for cardinality estimation.

        Raises:
            ClientOfflineException: If the client is not connected.

        Example:
            >>> estimator = client.get_cardinality_estimator("unique-visitors")
            >>> estimator.add("user-1")
            >>> estimator.add("user-2")
            >>> count = estimator.estimate().result()
        """
        from hazelcast.proxy.cardinality_estimator import CardinalityEstimator
        return self._get_or_create_proxy(
            SERVICE_NAME_CARDINALITY_ESTIMATOR, name, CardinalityEstimator
        )

    def get_flake_id_generator(self, name: str) -> "FlakeIdGenerator":
        """Get or create a distributed FlakeIdGenerator.

        Returns a proxy to a distributed ID generator that produces
        cluster-wide unique, roughly time-ordered 64-bit IDs.

        FlakeIdGenerator is useful for generating unique identifiers
        for entities like orders, users, or events without coordination
        overhead.

        Args:
            name: Name of the distributed FlakeID generator.

        Returns:
            FlakeIdGenerator instance for ID generation.

        Raises:
            ClientOfflineException: If the client is not connected.

        Example:
            >>> id_gen = client.get_flake_id_generator("order-ids")
            >>> order_id = id_gen.new_id()
            >>> print(f"New order ID: {order_id}")
        """
        from hazelcast.proxy.flake_id import FlakeIdGenerator
        return self._get_or_create_proxy(SERVICE_NAME_FLAKE_ID, name, FlakeIdGenerator)

    def get_pn_counter(self, name: str) -> "PNCounter":
        """Get or create a distributed PNCounter.

        Returns a proxy to a CRDT Positive-Negative Counter that supports
        increment and decrement with eventual consistency.

        Args:
            name: Name of the distributed PN counter.

        Returns:
            PNCounterProxy instance for counter operations.

        Raises:
            ClientOfflineException: If the client is not connected.

        Example:
            >>> counter = client.get_pn_counter("page-views")
            >>> counter.increment_and_get()
            >>> count = counter.get()
        """
        from hazelcast.proxy.pn_counter import PNCounter
        return self._get_or_create_proxy(SERVICE_NAME_PN_COUNTER, name, PNCounter)

    def get_atomic_long(self, name: str) -> "AtomicLong":
        """Get or create a CP AtomicLong.

        Returns a proxy to a CP subsystem atomic long counter with
        strong consistency guarantees.

        Args:
            name: Name of the atomic long in the CP subsystem.

        Returns:
            AtomicLong instance for atomic counter operations.

        Raises:
            ClientOfflineException: If the client is not connected.

        Note:
            Requires CP subsystem to be enabled on the cluster
            (minimum 3 members).

        Example:
            >>> counter = client.get_atomic_long("sequence-generator")
            >>> counter.set(0)
            >>> next_id = counter.increment_and_get()
        """
        from hazelcast.cp.atomic import AtomicLong
        return self._get_or_create_proxy(SERVICE_NAME_ATOMIC_LONG, name, AtomicLong)

    def get_atomic_reference(self, name: str) -> "AtomicReference":
        """Get or create a CP AtomicReference.

        Args:
            name: Name of the atomic reference.

        Returns:
            The AtomicReference proxy.
        """
        from hazelcast.cp.atomic import AtomicReference
        return self._get_or_create_proxy(SERVICE_NAME_ATOMIC_REFERENCE, name, AtomicReference)

    def get_count_down_latch(self, name: str) -> "CountDownLatch":
        """Get or create a CP CountDownLatch.

        Args:
            name: Name of the count down latch.

        Returns:
            The CountDownLatch proxy.
        """
        from hazelcast.cp.sync import CountDownLatch
        return self._get_or_create_proxy(SERVICE_NAME_COUNT_DOWN_LATCH, name, CountDownLatch)

    def get_semaphore(self, name: str) -> "Semaphore":
        """Get or create a CP Semaphore.

        Args:
            name: Name of the semaphore.

        Returns:
            The Semaphore proxy.
        """
        from hazelcast.cp.sync import Semaphore
        return self._get_or_create_proxy(SERVICE_NAME_SEMAPHORE, name, Semaphore)

    def get_executor_service(self, name: str) -> "IExecutorService":
        """Get or create a distributed IExecutorService.

        Returns a proxy to a distributed executor service that can execute
        tasks on cluster members. Tasks can be submitted to specific members,
        to the owner of a key (for data locality), or to all members.

        Args:
            name: Name of the distributed executor service.

        Returns:
            IExecutorService instance for executing tasks.

        Raises:
            ClientOfflineException: If the client is not connected.

        Example:
            >>> executor = client.get_executor_service("my-executor")
            >>> future = executor.submit_to_key_owner(my_task, "user:123")
            >>> result = future.result()
        """
        from hazelcast.proxy.executor import IExecutorService
        return self._get_or_create_proxy(SERVICE_NAME_EXECUTOR, name, IExecutorService)

    def get_fenced_lock(self, name: str) -> "FencedLock":
        """Get or create a CP FencedLock.

        Returns a proxy to a CP subsystem distributed mutex with
        fencing token support for safe lock ownership verification.

        Args:
            name: Name of the fenced lock in the CP subsystem.

        Returns:
            FencedLock instance for distributed locking.

        Raises:
            ClientOfflineException: If the client is not connected.

        Note:
            Requires CP subsystem to be enabled on the cluster.

        Example:
            >>> lock = client.get_fenced_lock("resource-lock")
            >>> with lock as fence:
            ...     # Critical section
            ...     print(f"Lock acquired with fence: {fence}")
        """
        from hazelcast.cp.sync import FencedLock
        return self._get_or_create_proxy(SERVICE_NAME_FENCED_LOCK, name, FencedLock)

    def get_sql(self) -> "SqlService":
        """Get the SQL service for executing queries.

        Returns the SQL service for executing SQL queries against
        data stored in the Hazelcast cluster.

        Returns:
            SqlService instance for query execution.

        Raises:
            ClientOfflineException: If the client is not connected.

        Example:
            >>> sql = client.get_sql()
            >>> result = sql.execute("SELECT * FROM employees WHERE age > ?", 30)
            >>> for row in result:
            ...     print(row.to_dict())
        """
        self._check_running()

        if self._sql_service is None:
            from hazelcast.sql.service import SqlService
            self._sql_service = SqlService(
                self._invocation_service,
                self._serialization_service,
                self._connection_manager,
            )
        return self._sql_service

    def new_transaction_context(
        self, options: "TransactionOptions" = None
    ) -> "TransactionContext":
        """Create a new transaction context.

        Creates a new TransactionContext for executing transactional
        operations across multiple distributed data structures with
        ACID guarantees.

        Args:
            options: Optional transaction configuration including timeout,
                durability, and commit type (ONE_PHASE or TWO_PHASE).
                If None, default options are used.

        Returns:
            A new TransactionContext instance.

        Raises:
            ClientOfflineException: If the client is not connected.

        Example:
            Basic usage with context manager::

                with client.new_transaction_context() as ctx:
                    txn_map = ctx.get_map("my-map")
                    txn_map.put("key", "value")
                    # Auto-commits on success

            Two-phase commit::

                from hazelcast import TransactionOptions, TransactionType

                options = TransactionOptions(
                    timeout=60.0,
                    transaction_type=TransactionType.TWO_PHASE,
                )
                with client.new_transaction_context(options) as ctx:
                    txn_map = ctx.get_map("my-map")
                    txn_map.put("key", "value")
        """
        self._check_running()

        from hazelcast.transaction import TransactionContext
        return TransactionContext(self._proxy_context, options)

    def get_jet(self) -> "JetService":
        """Get the Jet service for stream processing.

        Returns the Jet service for submitting and managing
        distributed stream/batch processing pipelines.

        Returns:
            JetService instance for pipeline submission.

        Raises:
            ClientOfflineException: If the client is not connected.

        Example:
            >>> jet = client.get_jet()
            >>> pipeline = Pipeline.create()
            >>> # ... build pipeline ...
            >>> job = jet.submit(pipeline)
            >>> print(f"Job status: {job.status}")
        """
        self._check_running()

        if self._jet_service is None:
            from hazelcast.jet.service import JetService
            self._jet_service = JetService(
                invocation_service=self._invocation_service,
                serialization_service=self._serialization_service,
            )
        return self._jet_service
=======
        return self._jet_service

    def __enter__(self) -> "HazelcastClient":
        """Enter context manager - starts the client."""
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager - shuts down the client."""
        self.shutdown()

    async def __aenter__(self) -> "HazelcastClient":
        """Async context manager entry - starts the client."""
        return await self.start_async()

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit - shuts down the client."""
        await self.shutdown_async()

    def __repr__(self) -> str:
        return (
            f"HazelcastClient(name={self._client_name!r}, "
            f"state={self._state.value}, "
            f"cluster={self._config.cluster_name!r})"
        )
