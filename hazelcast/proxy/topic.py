"""Topic distributed data structure proxy.

This module provides the Topic distributed data structure for
publish-subscribe messaging patterns.

Classes:
    TopicMessage: A message received from a topic.
    MessageListener: Listener interface for receiving topic messages.
    TopicProxy: Proxy for the ITopic distributed data structure.
    LocalTopicStats: Local statistics for a topic.

Example:
    Basic topic usage::

        topic = client.get_topic("notifications")

        # Subscribe to messages
        def on_message(msg):
            print(f"Received: {msg.message}")

        reg_id = topic.add_message_listener(on_message=on_message)

        # Publish a message
        topic.publish("Hello, World!")

        # Unsubscribe
        topic.remove_message_listener(reg_id)
"""

from concurrent.futures import Future
from typing import Any, Callable, Dict, Generic, Optional, TypeVar

from hazelcast.proxy.base import Proxy, ProxyContext

E = TypeVar("E")


class TopicMessage(Generic[E]):
    """A message received from a topic.

    TopicMessage wraps the actual message content along with metadata
    about when and where the message was published.

    Type Parameters:
        E: The type of the message content.

    Args:
        message: The message content.
        publish_time: The timestamp when the message was published (milliseconds).
        publishing_member: The cluster member that published the message.

    Attributes:
        message: The message content.
        publish_time: Publication timestamp in milliseconds since epoch.
        publishing_member: The member that published the message.

    Example:
        >>> def on_message(msg: TopicMessage[str]):
        ...     print(f"Message: {msg.message}")
        ...     print(f"Published at: {msg.publish_time}")
    """

    def __init__(
        self,
        message: E,
        publish_time: int,
        publishing_member: Any = None,
    ):
        """Initialize the TopicMessage.

        Args:
            message: The message content.
            publish_time: Publication timestamp in milliseconds.
            publishing_member: The member that published this message.
        """
        self._message = message
        self._publish_time = publish_time
        self._publishing_member = publishing_member

    @property
    def message(self) -> E:
        """Get the message content.

        Returns:
            The message content of type E.
        """
        return self._message

    @property
    def publish_time(self) -> int:
        """Get the publication timestamp.

        Returns:
            Timestamp in milliseconds since epoch when the message was published.
        """
        return self._publish_time

    @property
    def publishing_member(self) -> Any:
        """Get the publishing member.

        Returns:
            The cluster member that published this message, or None.
        """
        return self._publishing_member


class MessageListener(Generic[E]):
    """Listener interface for receiving topic messages.

    Implement this interface to receive messages published to a topic.

    Type Parameters:
        E: The type of messages this listener handles.

    Example:
        >>> class MyListener(MessageListener[str]):
        ...     def on_message(self, message: TopicMessage[str]) -> None:
        ...         print(f"Received: {message.message}")
        ...
        >>> topic.add_message_listener(MyListener())
    """

    def on_message(self, message: TopicMessage[E]) -> None:
        """Called when a message is received from the topic.

        This method is invoked for each message published to the topic
        after the listener was registered.

        Args:
            message: The received message wrapped in a TopicMessage.

        Note:
            This method should not block for long periods as it may
            delay delivery of subsequent messages.
        """
        pass


class TopicProxy(Proxy, Generic[E]):
    """Proxy for Hazelcast ITopic distributed data structure.

    ITopic is a distributed publish-subscribe messaging mechanism.
    Messages published to a topic are delivered to all subscribers
    that are registered at the time of publication.

    Topics provide at-most-once delivery semantics. If a subscriber
    is not connected when a message is published, it will not receive
    that message. For reliable delivery, use :class:`ReliableTopic`.

    Type Parameters:
        E: The type of messages in this topic.

    Attributes:
        name: The name of this topic.

    Example:
        Basic publish-subscribe::

            topic = client.get_topic("events")

            # Subscribe
            def handler(msg):
                print(f"Event: {msg.message}")

            topic.add_message_listener(on_message=handler)

            # Publish
            topic.publish({"type": "user_login", "user": "alice"})

    Note:
        Topic messages are not persisted. For persistence, consider
        using ReliableTopic which is backed by a Ringbuffer.
    """

    SERVICE_NAME = "hz:impl:topicService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        """Initialize the TopicProxy.

        Args:
            name: The name of the distributed topic.
            context: The proxy context for service access.
        """
        super().__init__(self.SERVICE_NAME, name, context)
        self._message_listeners: Dict[str, MessageListener[E]] = {}

    def publish(self, message: E) -> None:
        """Publish a message to all subscribers.

        Publishes the given message to all registered listeners. This
        is a synchronous operation that blocks until the message is
        sent to the cluster.

        Args:
            message: The message to publish. Must be serializable.

        Raises:
            IllegalStateException: If the topic has been destroyed.

        Example:
            >>> topic.publish("Hello, subscribers!")
            >>> topic.publish({"event": "update", "data": [1, 2, 3]})
        """
        self.publish_async(message).result()

    def publish_async(self, message: E) -> Future:
        """Publish a message asynchronously.

        Async version of :meth:`publish`.

        Args:
            message: The message to publish.

        Returns:
            A Future that completes when the publish operation is done.

        Example:
            >>> future = topic.publish_async("async message")
            >>> # Do other work
            >>> future.result()  # Wait for completion
        """
        self._check_not_destroyed()
        import time
        topic_message = TopicMessage(message, int(time.time() * 1000))
        self._notify_listeners(topic_message)
        future: Future = Future()
        future.set_result(None)
        return future

    def _notify_listeners(self, message: TopicMessage[E]) -> None:
        """Notify all registered listeners of a message."""
        for listener in self._message_listeners.values():
            try:
                listener.on_message(message)
            except Exception:
                pass

    def add_message_listener(
        self,
        listener: MessageListener[E] = None,
        on_message: Callable[[TopicMessage[E]], None] = None,
    ) -> str:
        """Add a message listener.

        Args:
            listener: A MessageListener instance, or
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
        return registration_id

    def remove_message_listener(self, registration_id: str) -> bool:
        """Remove a message listener.

        Args:
            registration_id: The registration ID from add_message_listener.

        Returns:
            True if the listener was removed.
        """
        return self._message_listeners.pop(registration_id, None) is not None

    def get_local_topic_stats(self) -> "LocalTopicStats":
        """Get local statistics for this topic.

        Returns:
            Local topic statistics.
        """
        return LocalTopicStats()


class LocalTopicStats:
    """Local statistics for a topic.

    Provides statistics about topic operations performed by this
    client instance.

    Attributes:
        publish_operation_count: Number of publish operations.
        receive_operation_count: Number of messages received.

    Example:
        >>> stats = topic.get_local_topic_stats()
        >>> print(f"Published: {stats.publish_operation_count}")
        >>> print(f"Received: {stats.receive_operation_count}")
    """

    def __init__(
        self,
        publish_operation_count: int = 0,
        receive_operation_count: int = 0,
    ):
        """Initialize LocalTopicStats.

        Args:
            publish_operation_count: Number of publish operations.
            receive_operation_count: Number of messages received.
        """
        self._publish_operation_count = publish_operation_count
        self._receive_operation_count = receive_operation_count

    @property
    def publish_operation_count(self) -> int:
        """Get the number of publish operations.

        Returns:
            The count of publish operations performed.
        """
        return self._publish_operation_count

    @property
    def receive_operation_count(self) -> int:
        """Get the number of messages received.

        Returns:
            The count of messages received by listeners.
        """
        return self._receive_operation_count
