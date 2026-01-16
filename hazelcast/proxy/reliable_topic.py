"""Reliable Topic distributed data structure proxy."""

from concurrent.futures import Future
from enum import Enum
from typing import Any, Callable, Dict, Generic, Optional, TypeVar

from hazelcast.proxy.base import Proxy, ProxyContext
from hazelcast.proxy.topic import TopicMessage, MessageListener

E = TypeVar("E")


class StaleSequenceException(Exception):
    """Raised when the sequence is behind the head of the ringbuffer.

    This exception indicates that the requested sequence number is no
    longer available because newer data has overwritten it.

    Attributes:
        head_sequence: The current head sequence of the ringbuffer.

    Example:
        >>> try:
        ...     topic.read_from(old_sequence)
        ... except StaleSequenceException as e:
        ...     print(f"Data lost, head is now: {e.head_sequence}")
    """

    def __init__(self, message: str, head_sequence: int = -1):
        """Initialize the exception.

        Args:
            message: The error message.
            head_sequence: The current head sequence of the ringbuffer.
        """
        super().__init__(message)
        self._head_sequence = head_sequence

    @property
    def head_sequence(self) -> int:
        """Get the current head sequence of the ringbuffer.

        Returns:
            The sequence number of the oldest available item.
        """
        return self._head_sequence


class TopicOverloadPolicy(Enum):
    """Policy for handling topic overload.

    Determines behavior when the backing ringbuffer is full and a
    new message is published.

    Attributes:
        DISCARD_OLDEST: Discard the oldest message to make room.
        DISCARD_NEWEST: Discard the new message being published.
        BLOCK: Block until space becomes available.
        ERROR: Raise an error when the buffer is full.

    Example:
        >>> config = ReliableTopicConfig(overload_policy=TopicOverloadPolicy.BLOCK)
    """

    DISCARD_OLDEST = "DISCARD_OLDEST"
    DISCARD_NEWEST = "DISCARD_NEWEST"
    BLOCK = "BLOCK"
    ERROR = "ERROR"


class ReliableTopicConfig:
    """Configuration for reliable topic.

    Configures the behavior of a ReliableTopic including batch sizes
    and overload handling.

    Attributes:
        read_batch_size: Number of messages to read in each batch.
        overload_policy: Policy for handling buffer overflow.

    Example:
        >>> config = ReliableTopicConfig(
        ...     read_batch_size=50,
        ...     overload_policy=TopicOverloadPolicy.DISCARD_OLDEST
        ... )
    """

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
            try:
                from hazelcast.exceptions import ConfigurationException
            except ImportError:
                class ConfigurationException(Exception):
                    """Raised when configuration is invalid."""
                    pass
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
    """Listener for reliable topic messages with additional callbacks.

    Extends MessageListener with methods for sequence tracking and
    loss tolerance, enabling reliable message delivery.

    Type Parameters:
        E: The type of messages this listener handles.

    Example:
        >>> class MyListener(ReliableMessageListener[str]):
        ...     def __init__(self):
        ...         self._seq = -1
        ...
        ...     def on_message(self, msg):
        ...         print(f"Received: {msg.message}")
        ...
        ...     def store_sequence(self, seq):
        ...         self._seq = seq
        ...
        ...     def retrieve_initial_sequence(self):
        ...         return self._seq
    """

    def store_sequence(self, sequence: int) -> None:
        """Store the current sequence for later retrieval.

        Called after each message is processed to allow the listener
        to persist its position for recovery.

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

    def on_stale_sequence(self, head_sequence: int) -> int:
        """Called when a stale sequence is detected.

        Args:
            head_sequence: The current head sequence of the ringbuffer.

        Returns:
            The new sequence to continue from.
        """
        return head_sequence


class ReliableTopicProxy(Proxy, Generic[E]):
    """Proxy for Hazelcast Reliable Topic distributed data structure.

    A reliable publish-subscribe messaging structure backed by a ringbuffer.
    Unlike regular Topic, ReliableTopic guarantees message ordering and
    provides at-least-once delivery semantics.

    Type Parameters:
        E: The type of messages in this topic.

    Attributes:
        name: The name of this reliable topic.
        config: The configuration for this topic.

    Example:
        Basic publish-subscribe::

            topic = client.get_reliable_topic("events")

            def handler(msg):
                print(f"Received: {msg.message}")

            topic.add_message_listener(on_message=handler)
            topic.publish("Hello, reliable world!")
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
        import time
        topic_message = TopicMessage(message, int(time.time() * 1000))
        self._deliver_message(topic_message)
        future: Future = Future()
        future.set_result(None)
        return future

    def _deliver_message(self, message: TopicMessage[E]) -> None:
        """Deliver a message to all runners."""
        for runner in self._listener_runners.values():
            if not runner.is_cancelled:
                runner.process_message(message)

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
        initial_sequence = -1
        if isinstance(listener, ReliableMessageListener):
            initial_sequence = listener.retrieve_initial_sequence()

        runner = _MessageRunner(
            registration_id=registration_id,
            listener=listener,
            initial_sequence=initial_sequence,
            config=self._config,
        )
        self._listener_runners[registration_id] = runner

    def _stop_listener_runner(self, registration_id: str) -> None:
        """Stop the background runner for a listener."""
        runner = self._listener_runners.pop(registration_id, None)
        if runner:
            runner.cancel()

    def _handle_stale_sequence(
        self,
        registration_id: str,
        stale_sequence: int,
        head_sequence: int,
    ) -> int:
        """Handle a stale sequence error.

        Args:
            registration_id: The listener registration ID.
            stale_sequence: The stale sequence that was requested.
            head_sequence: The current head sequence.

        Returns:
            The new sequence to continue from.

        Raises:
            StaleSequenceException: If the listener cannot recover.
        """
        listener = self._message_listeners.get(registration_id)
        if listener is None:
            raise StaleSequenceException(
                f"Sequence {stale_sequence} is stale, head is {head_sequence}",
                head_sequence,
            )

        if isinstance(listener, ReliableMessageListener):
            if listener.is_loss_tolerant():
                return listener.on_stale_sequence(head_sequence)
            raise StaleSequenceException(
                f"Sequence {stale_sequence} is stale, head is {head_sequence}. "
                "Listener is not loss tolerant.",
                head_sequence,
            )

        raise StaleSequenceException(
            f"Sequence {stale_sequence} is stale, head is {head_sequence}",
            head_sequence,
        )

    def _on_destroy(self) -> None:
        for reg_id in list(self._listener_runners.keys()):
            self._stop_listener_runner(reg_id)
        self._message_listeners.clear()

    async def _on_destroy_async(self) -> None:
        for reg_id in list(self._listener_runners.keys()):
            self._stop_listener_runner(reg_id)
        self._message_listeners.clear()


class _MessageRunner:
    """Internal runner for processing reliable topic messages."""

    def __init__(
        self,
        registration_id: str,
        listener: MessageListener[E],
        initial_sequence: int,
        config: ReliableTopicConfig,
    ):
        self._registration_id = registration_id
        self._listener = listener
        self._sequence = initial_sequence
        self._config = config
        self._cancelled = False

    @property
    def sequence(self) -> int:
        return self._sequence

    @sequence.setter
    def sequence(self, value: int) -> None:
        self._sequence = value
        if isinstance(self._listener, ReliableMessageListener):
            self._listener.store_sequence(value)

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

    def cancel(self) -> None:
        """Cancel this runner."""
        self._cancelled = True

    def process_message(self, message: TopicMessage[E]) -> None:
        """Process a received message."""
        if self._cancelled:
            return
        try:
            self._listener.on_message(message)
            self._sequence += 1
            if isinstance(self._listener, ReliableMessageListener):
                self._listener.store_sequence(self._sequence)
        except Exception as e:
            if isinstance(self._listener, ReliableMessageListener):
                if self._listener.is_terminal(e):
                    self.cancel()
                    raise
            else:
                self.cancel()
                raise
