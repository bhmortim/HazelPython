"""FlakeID Generator distributed data structure proxy.

This module provides the FlakeIdGenerator for generating cluster-wide
unique, roughly time-ordered 64-bit IDs.

Classes:
    FlakeIdGeneratorProxy: Proxy for the distributed FlakeID Generator.
    AutoBatcher: Manages ID batch fetching and local caching.

Example:
    Basic ID generation::

        id_gen = client.get_flake_id_generator("order-ids")

        # Generate a single ID
        order_id = id_gen.new_id()

        # Generate multiple IDs
        ids = [id_gen.new_id() for _ in range(100)]
"""

import threading
import time
from concurrent.futures import Future
from typing import Optional, TYPE_CHECKING

from hazelcast.proxy.base import Proxy, ProxyContext

if TYPE_CHECKING:
    from hazelcast.protocol.codec import IdBatch

SERVICE_NAME_FLAKE_ID = "hz:impl:flakeIdGeneratorService"

DEFAULT_BATCH_SIZE = 100
DEFAULT_PREFETCH_COUNT = 1
DEFAULT_PREFETCH_VALIDITY_MILLIS = 600000


class AutoBatcher:
    """Manages ID batch fetching and local caching.

    AutoBatcher prefetches batches of IDs from the cluster and caches
    them locally. This reduces network round-trips when generating
    many IDs in quick succession.

    Args:
        batch_size: Number of IDs to fetch per batch.
        prefetch_validity_millis: How long prefetched IDs remain valid.
        fetch_batch_fn: Function to fetch a new batch from the cluster.

    Attributes:
        batch_size: The configured batch size.

    Example:
        >>> batcher = AutoBatcher(
        ...     batch_size=100,
        ...     prefetch_validity_millis=600000,
        ...     fetch_batch_fn=lambda: fetch_from_cluster()
        ... )
        >>> next_id = batcher.next_id()
    """

    def __init__(
        self,
        batch_size: int = DEFAULT_BATCH_SIZE,
        prefetch_validity_millis: int = DEFAULT_PREFETCH_VALIDITY_MILLIS,
        fetch_batch_fn=None,
    ):
        """Initialize the AutoBatcher.

        Args:
            batch_size: Number of IDs per batch.
            prefetch_validity_millis: Validity period for prefetched IDs.
            fetch_batch_fn: Callable that returns a Future[IdBatch].
        """
        self._batch_size = batch_size
        self._prefetch_validity_millis = prefetch_validity_millis
        self._fetch_batch_fn = fetch_batch_fn
        self._current_batch: Optional["IdBatch"] = None
        self._batch_fetch_time: int = 0
        self._lock = threading.Lock()

    @property
    def batch_size(self) -> int:
        """Get the batch size."""
        return self._batch_size

    def next_id(self) -> int:
        """Get the next ID from the local cache or fetch a new batch.

        Returns:
            A unique 64-bit ID.

        Raises:
            RuntimeError: If unable to fetch a new batch.
        """
        with self._lock:
            if self._should_fetch_new_batch():
                self._fetch_new_batch()

            if self._current_batch is None:
                raise RuntimeError("Failed to obtain ID batch")

            try:
                return self._current_batch.next_id()
            except StopIteration:
                self._fetch_new_batch()
                if self._current_batch is None:
                    raise RuntimeError("Failed to obtain ID batch")
                return self._current_batch.next_id()

    def next_id_async(self) -> Future:
        """Get the next ID asynchronously.

        Returns:
            A Future containing the next ID.
        """
        future: Future = Future()
        try:
            next_id = self.next_id()
            future.set_result(next_id)
        except Exception as e:
            future.set_exception(e)
        return future

    def _should_fetch_new_batch(self) -> bool:
        """Check if a new batch needs to be fetched."""
        if self._current_batch is None:
            return True

        if self._current_batch.is_exhausted():
            return True

        if self._is_batch_expired():
            return True

        return False

    def _is_batch_expired(self) -> bool:
        """Check if the current batch has expired."""
        if self._prefetch_validity_millis <= 0:
            return False

        current_time = int(time.time() * 1000)
        return (current_time - self._batch_fetch_time) > self._prefetch_validity_millis

    def _fetch_new_batch(self) -> None:
        """Fetch a new batch from the cluster."""
        if self._fetch_batch_fn is None:
            self._create_local_batch()
            return

        try:
            batch_future = self._fetch_batch_fn()
            self._current_batch = batch_future.result()
            self._batch_fetch_time = int(time.time() * 1000)
        except Exception:
            self._create_local_batch()

    def _create_local_batch(self) -> None:
        """Create a local batch for testing/offline use."""
        from hazelcast.protocol.codec import IdBatch

        base = int(time.time() * 1000) << 22
        self._current_batch = IdBatch(base, 1, self._batch_size)
        self._batch_fetch_time = int(time.time() * 1000)

    def reset(self) -> None:
        """Reset the batcher state, clearing cached batches."""
        with self._lock:
            self._current_batch = None
            self._batch_fetch_time = 0


class FlakeIdGeneratorProxy(Proxy):
    """Proxy for Hazelcast FlakeIdGenerator distributed data structure.

    FlakeIdGenerator generates cluster-wide unique, roughly time-ordered
    64-bit IDs. The IDs are composed of:
    - Timestamp component (milliseconds)
    - Node ID component
    - Sequence component

    IDs are generated in batches for efficiency. When a batch is exhausted,
    a new batch is fetched from the cluster. Batches have a validity period
    after which they are discarded to ensure rough time-ordering.

    Attributes:
        name: The name of this FlakeID generator.

    Example:
        Basic ID generation::

            id_gen = client.get_flake_id_generator("orders")

            # Generate unique IDs
            order_id = id_gen.new_id()
            invoice_id = id_gen.new_id()

        Configuring batch size::

            # IDs are fetched in batches for efficiency
            # Default batch size is 100

    Note:
        FlakeIdGenerator IDs are roughly time-ordered but not strictly
        monotonic. Do not rely on strict ordering between IDs generated
        by different cluster members.
    """

    SERVICE_NAME = SERVICE_NAME_FLAKE_ID

    def __init__(
        self,
        service_name: str,
        name: str,
        context: Optional[ProxyContext] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        prefetch_count: int = DEFAULT_PREFETCH_COUNT,
        prefetch_validity_millis: int = DEFAULT_PREFETCH_VALIDITY_MILLIS,
    ):
        """Initialize the FlakeIdGeneratorProxy.

        Args:
            service_name: The service name for this proxy.
            name: The name of the distributed FlakeID generator.
            context: The proxy context for service access.
            batch_size: Number of IDs to fetch per batch.
            prefetch_count: Number of batches to prefetch (reserved).
            prefetch_validity_millis: How long prefetched IDs remain valid.
        """
        super().__init__(service_name, name, context)
        self._batch_size = batch_size
        self._prefetch_count = prefetch_count
        self._prefetch_validity_millis = prefetch_validity_millis
        self._auto_batcher = AutoBatcher(
            batch_size=batch_size,
            prefetch_validity_millis=prefetch_validity_millis,
            fetch_batch_fn=self._fetch_batch,
        )

    @property
    def batch_size(self) -> int:
        """Get the configured batch size."""
        return self._batch_size

    def new_id(self) -> int:
        """Generate a new unique ID.

        Returns a cluster-wide unique 64-bit ID. IDs are fetched in
        batches from the cluster and cached locally for efficiency.

        Returns:
            A unique 64-bit integer ID.

        Raises:
            IllegalStateException: If the generator has been destroyed.
            RuntimeError: If unable to fetch IDs from the cluster.

        Example:
            >>> id_gen = client.get_flake_id_generator("users")
            >>> user_id = id_gen.new_id()
            >>> print(f"New user ID: {user_id}")
        """
        self._check_not_destroyed()
        return self._auto_batcher.next_id()

    def new_id_async(self) -> Future:
        """Generate a new unique ID asynchronously.

        Async version of :meth:`new_id`.

        Returns:
            A Future containing the generated ID.

        Example:
            >>> future = id_gen.new_id_async()
            >>> user_id = future.result()
        """
        self._check_not_destroyed()
        return self._auto_batcher.next_id_async()

    def _fetch_batch(self) -> Future:
        """Fetch a new ID batch from the cluster.

        Returns:
            A Future containing the IdBatch.
        """
        from hazelcast.protocol.codec import FlakeIdGeneratorCodec

        request = FlakeIdGeneratorCodec.encode_new_id_batch_request(
            self._name, self._batch_size
        )
        return self._invoke(request, FlakeIdGeneratorCodec.decode_new_id_batch_response)

    def _on_destroy(self) -> None:
        """Called when the proxy is destroyed."""
        self._auto_batcher.reset()


FlakeIdGenerator = FlakeIdGeneratorProxy
