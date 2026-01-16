"""FlakeIdGenerator proxy for cluster-wide unique ID generation."""

import threading
from concurrent.futures import Future
from typing import List, Optional, TYPE_CHECKING

from hazelcast.proxy.base import Proxy, ProxyContext
from hazelcast.protocol.codec import (
    FlakeIdGeneratorCodec,
    IdBatch,
)
from hazelcast.logging import get_logger

if TYPE_CHECKING:
    from hazelcast.protocol.client_message import ClientMessage

_logger = get_logger("flake_id")

SERVICE_NAME = "hz:impl:flakeIdGeneratorService"
DEFAULT_BATCH_SIZE = 100


class FlakeIdGenerator(Proxy):
    """Proxy for the Hazelcast FlakeIdGenerator distributed data structure.

    FlakeIdGenerator generates cluster-wide unique 64-bit IDs. IDs are
    composed of a timestamp, node ID, and sequence number, ensuring
    uniqueness across the cluster without coordination.

    The generator uses batching to minimize network calls. When `new_id()`
    is called, it returns an ID from a locally cached batch. When the
    batch is exhausted, a new batch is fetched from the cluster.
    """

    def __init__(
        self,
        name: str,
        context: Optional[ProxyContext] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ):
        """Initialize the FlakeIdGenerator proxy.

        Args:
            name: The name of this FlakeIdGenerator.
            context: The proxy context containing required services.
            batch_size: The number of IDs to fetch per batch.
        """
        super().__init__(SERVICE_NAME, name, context)
        self._batch_size = batch_size
        self._batch: Optional[IdBatch] = None
        self._lock = threading.Lock()

    @property
    def batch_size(self) -> int:
        """Get the configured batch size."""
        return self._batch_size

    def new_id(self) -> int:
        """Generate a new cluster-wide unique ID.

        This method returns a locally cached ID if available, otherwise
        fetches a new batch from the cluster synchronously.

        Returns:
            A unique 64-bit integer ID.

        Raises:
            IllegalStateException: If this proxy has been destroyed.
            HazelcastError: If there is a communication error with the cluster.
        """
        self._check_not_destroyed()

        with self._lock:
            if self._batch is None or self._batch.is_exhausted():
                self._batch = self._fetch_new_batch()
            return self._batch.next_id()

    def new_id_async(self) -> Future:
        """Generate a new cluster-wide unique ID asynchronously.

        Returns:
            A Future that will contain the unique ID.
        """
        self._check_not_destroyed()

        with self._lock:
            if self._batch is not None and not self._batch.is_exhausted():
                future: Future = Future()
                future.set_result(self._batch.next_id())
                return future

        return self._fetch_new_batch_async_and_get_id()

    def new_id_batch(self, size: int) -> List[int]:
        """Generate a batch of unique IDs.

        Args:
            size: The number of IDs to generate.

        Returns:
            A list of unique 64-bit integer IDs.

        Raises:
            ValueError: If size is not positive.
            IllegalStateException: If this proxy has been destroyed.
        """
        if size <= 0:
            raise ValueError("Batch size must be positive")

        self._check_not_destroyed()
        ids: List[int] = []

        with self._lock:
            while len(ids) < size:
                if self._batch is None or self._batch.is_exhausted():
                    fetch_size = max(self._batch_size, size - len(ids))
                    self._batch = self._fetch_new_batch(fetch_size)

                while not self._batch.is_exhausted() and len(ids) < size:
                    ids.append(self._batch.next_id())

        return ids

    def new_id_batch_async(self, size: int) -> Future:
        """Generate a batch of unique IDs asynchronously.

        Args:
            size: The number of IDs to generate.

        Returns:
            A Future that will contain a list of unique IDs.
        """
        if size <= 0:
            future: Future = Future()
            future.set_exception(ValueError("Batch size must be positive"))
            return future

        self._check_not_destroyed()

        result_future: Future = Future()
        ids: List[int] = []

        def collect_ids(batch_future: Future) -> None:
            try:
                batch = batch_future.result()
                with self._lock:
                    self._batch = batch
                    while not self._batch.is_exhausted() and len(ids) < size:
                        ids.append(self._batch.next_id())

                if len(ids) < size:
                    fetch_size = max(self._batch_size, size - len(ids))
                    next_future = self._request_new_batch(fetch_size)
                    next_future.add_done_callback(collect_ids)
                else:
                    result_future.set_result(ids)
            except Exception as e:
                result_future.set_exception(e)

        with self._lock:
            while self._batch is not None and not self._batch.is_exhausted() and len(ids) < size:
                ids.append(self._batch.next_id())

        if len(ids) >= size:
            result_future.set_result(ids)
        else:
            fetch_size = max(self._batch_size, size - len(ids))
            batch_future = self._request_new_batch(fetch_size)
            batch_future.add_done_callback(collect_ids)

        return result_future

    def _fetch_new_batch(self, size: Optional[int] = None) -> IdBatch:
        """Fetch a new batch of IDs from the cluster synchronously.

        Args:
            size: The batch size to request, or None for the default.

        Returns:
            A new IdBatch.
        """
        batch_size = size if size is not None else self._batch_size
        future = self._request_new_batch(batch_size)
        return future.result()

    def _fetch_new_batch_async_and_get_id(self) -> Future:
        """Fetch a new batch asynchronously and return the first ID."""
        result_future: Future = Future()

        def on_batch_received(f: Future) -> None:
            try:
                batch = f.result()
                with self._lock:
                    self._batch = batch
                    id_value = self._batch.next_id()
                result_future.set_result(id_value)
            except Exception as e:
                result_future.set_exception(e)

        batch_future = self._request_new_batch(self._batch_size)
        batch_future.add_done_callback(on_batch_received)
        return result_future

    def _request_new_batch(self, batch_size: int) -> Future:
        """Request a new batch of IDs from the cluster.

        Args:
            batch_size: The number of IDs to request.

        Returns:
            A Future that will contain the IdBatch.
        """
        request = FlakeIdGeneratorCodec.encode_new_id_batch_request(
            self._name, batch_size
        )
        return self._invoke(
            request, FlakeIdGeneratorCodec.decode_new_id_batch_response
        )

    def _on_destroy(self) -> None:
        """Clear the cached batch when destroyed."""
        with self._lock:
            self._batch = None

    async def _on_destroy_async(self) -> None:
        """Clear the cached batch when destroyed asynchronously."""
        with self._lock:
            self._batch = None
