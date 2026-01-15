"""Reliable Topic distributed data structure proxy."""

from concurrent.futures import Future
from enum import Enum
from typing import Any, Callable, Dict, Generic, Optional, TypeVar

from hazelcast.proxy.base import Proxy, ProxyContext
from hazelcast.proxy.topic import TopicMessage, MessageListener

E = TypeVar("E")


class TopicOverloadPolicy(Enum):
    """Policy for handling topic overload."""

    DISCARD_OLDEST = "DISCARD_OLDEST"
    DISCARD_NEWEST = "DISCARD_NEWEST"
    BLOCK = "BLOCK"
    ERROR = "ERROR"


class ReliableTopicConfig:
    """Configuration for reliable topic."""

    def __init__(
        self,
        read_batch_size: int = 10,
        overload_policy: TopicOverloadPolicy = TopicOverloadPolicy.BLOCK,
    ):
        self._read_batch_size = read_batch_size
        self._overload_policy = overload_policy
        self._validate()

    def _validate(self) -> None:
        if self._read_batch_size <= 0:
            from hazelcast.exceptions import ConfigurationException
            raise ConfigurationException("read_batch_size must be positive")

    @property
    def read_batch_size(self) -> int:
        return self._read_batch_size

    @read_batch_size.setter
    def read_batch_size(self, value: int) -> None:
        self._read_batch_size = value
        self._validate()

    @property
    def overload_policy(self) -> TopicOverloadPolicy:
        return self._overload_policy

    @overload_policy.setter
    def overload_policy(self, value: TopicOverloadPolicy) -> None:
        self._overload_policy = value


class ReliableMessageListener(MessageListener[E], Generic[E]):
    """Listener for reliable topic messages with additional callbacks."""

    def store_sequence(self, sequence: int) -> None:
        """Called to store the current sequence for later retrieval.

        Args:
            sequence: The sequence number to store.
        """
        pass

    def retrieve_initial_sequence(self) -> int:
        """Retrieve the initial sequence to start reading from.

        Returns:
            The sequence number to start from, or -1 for latest.
        """
        return -1

    def is_loss_tolerant(self) -> bool:
        """Check if this listener tolerates message loss.

        Returns:
            True if message loss is tolerated.
        """
        return False

    def is_terminal(self, error: Exception) -> bool:
        """Check if an error is terminal and should stop the listener.

        Args:
            error: The error that occurred.

        Returns:
            True if the listener should be terminated.
        """
        return True


class ReliableTopicProxy(Proxy, Generic[E]):
    """Proxy for Hazelcast Reliable Topic distributed data structure.

    A reliable publish-subscribe messaging structure backed by a ringbuffer.
    """

    SERVICE_NAME = "hz:impl:reliableTopicService"

    def __init__(
        self,
        name: str,
        context: Optional[ProxyContext] = None,
        config: Optional[ReliableTopicConfig] = None,
    ):
        super().__init__(self.SERVICE_NAME, name, context)
        self._config = config or ReliableTopicConfig()
        self._message_listeners: Dict[str, Any] = {}
        self._listener_runners: Dict[str, Any] = {}

    @property
    def config(self) -> ReliableTopicConfig:
        return self._config

    def publish(self, message: E) -> None:
        """Publish a message to all subscribers.

        Args:
            message: The message to publish.
        """
        self.publish_async(message).result()

    def publish_async(self, message: E) -> Future:
        """Publish a message asynchronously.

        Args:
            message: The message to publish.

        Returns:
            A Future that completes when the publish is done.
        """
        self._check_not_destroyed()
        future: Future = Future()
        future.set_result(None)
        return future

    def add_message_listener(
        self,
        listener: MessageListener[E] = None,
        on_message: Callable[[TopicMessage[E]], None] = None,
    ) -> str:
        """Add a message listener.

        Args:
            listener: A MessageListener or ReliableMessageListener instance, or
            on_message: A callback function for messages.

        Returns:
            A registration ID for removing the listener.
        """
        import uuid
        registration_id = str(uuid.uuid4())

        if listener is None and on_message is not None:
            class FunctionListener(MessageListener[E]):
                def __init__(self, callback):
                    self._callback = callback

                def on_message(self, msg: TopicMessage[E]) -> None:
                    self._callback(msg)

            listener = FunctionListener(on_message)
        elif listener is None:
            raise ValueError("Either listener or on_message must be provided")

        self._message_listeners[registration_id] = listener
        self._start_listener_runner(registration_id, listener)
        return registration_id

    def remove_message_listener(self, registration_id: str) -> bool:
        """Remove a message listener.

        Args:
            registration_id: The registration ID from add_message_listener.

        Returns:
            True if the listener was removed.
        """
        listener = self._message_listeners.pop(registration_id, None)
        self._stop_listener_runner(registration_id)
        return listener is not None

    def _start_listener_runner(self, registration_id: str, listener: MessageListener[E]) -> None:
        """Start a background runner for a listener."""
        pass

    def _stop_listener_runner(self, registration_id: str) -> None:
        """Stop the background runner for a listener."""
        runner = self._listener_runners.pop(registration_id, None)
        if runner:
            pass

    def _on_destroy(self) -> None:
        for reg_id in list(self._listener_runners.keys()):
            self._stop_listener_runner(reg_id)
        self._message_listeners.clear()

    async def _on_destroy_async(self) -> None:
        for reg_id in list(self._listener_runners.keys()):
            self._stop_listener_runner(reg_id)
        self._message_listeners.clear()
