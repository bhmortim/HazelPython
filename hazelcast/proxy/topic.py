"""Topic distributed data structure proxy."""

from concurrent.futures import Future
from typing import Any, Callable, Dict, Generic, Optional, TypeVar

from hazelcast.proxy.base import Proxy, ProxyContext

E = TypeVar("E")


class TopicMessage(Generic[E]):
    """Message received from a topic."""

    def __init__(
        self,
        message: E,
        publish_time: int,
        publishing_member: Any = None,
    ):
        self._message = message
        self._publish_time = publish_time
        self._publishing_member = publishing_member

    @property
    def message(self) -> E:
        return self._message

    @property
    def publish_time(self) -> int:
        return self._publish_time

    @property
    def publishing_member(self) -> Any:
        return self._publishing_member


class MessageListener(Generic[E]):
    """Listener for topic messages."""

    def on_message(self, message: TopicMessage[E]) -> None:
        """Called when a message is received.

        Args:
            message: The received message.
        """
        pass


class TopicProxy(Proxy, Generic[E]):
    """Proxy for Hazelcast ITopic distributed data structure.

    A publish-subscribe messaging structure.
    """

    SERVICE_NAME = "hz:impl:topicService"

    def __init__(self, name: str, context: Optional[ProxyContext] = None):
        super().__init__(self.SERVICE_NAME, name, context)
        self._message_listeners: Dict[str, MessageListener[E]] = {}

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
    """Local statistics for a topic."""

    def __init__(
        self,
        publish_operation_count: int = 0,
        receive_operation_count: int = 0,
    ):
        self._publish_operation_count = publish_operation_count
        self._receive_operation_count = receive_operation_count

    @property
    def publish_operation_count(self) -> int:
        return self._publish_operation_count

    @property
    def receive_operation_count(self) -> int:
        return self._receive_operation_count
