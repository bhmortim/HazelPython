"""Hazelcast client exceptions.

This module defines the exception hierarchy for the Hazelcast Python Client.
All exceptions inherit from :class:`HazelcastException`.

Example:
    Handling Hazelcast exceptions::

        from hazelcast.exceptions import (
            HazelcastException,
            ClientOfflineException,
            TimeoutException,
        )

        try:
            result = my_map.get("key")
        except ClientOfflineException:
            print("Client is not connected")
        except TimeoutException:
            print("Operation timed out")
        except HazelcastException as e:
            print(f"Hazelcast error: {e}")
"""


class HazelcastException(Exception):
    """Base class for all Hazelcast exceptions.

    All exceptions raised by the Hazelcast Python Client inherit from
    this class, allowing for broad exception handling when needed.

    Args:
        message: The error message describing the exception.
        cause: The underlying exception that caused this error, if any.

    Attributes:
        cause: The underlying cause of this exception, if any.

    Example:
        >>> try:
        ...     # Hazelcast operation
        ...     pass
        ... except HazelcastException as e:
        ...     print(f"Error: {e}")
    """

    def __init__(self, message: str = "", cause: Exception = None):
        super().__init__(message)
        self.cause = cause


class IllegalStateException(HazelcastException):
    """Raised when an operation is invoked on an illegal state.

    This exception is thrown when an operation cannot be performed
    because the object is in an inappropriate state.

    Example:
        - Calling operations on a destroyed distributed object
        - Invalid state transitions in the client lifecycle
        - Using a shut down executor service
    """
    pass


class IllegalArgumentException(HazelcastException):
    """Raised when an illegal or inappropriate argument is passed.

    This exception indicates that a method has been passed an argument
    that is not valid for that method.

    Example:
        - Passing None when a value is required
        - Providing an out-of-range index
        - Invalid configuration values
    """
    pass


class ConfigurationException(HazelcastException):
    """Raised when there is a configuration error.

    This exception indicates that the client configuration is invalid
    or inconsistent.

    Example:
        - Invalid cluster member addresses
        - Negative timeout values
        - Incompatible configuration options
    """
    pass


class StaleSequenceException(HazelcastException):
    """Raised when a sequence is older than the head sequence of a ringbuffer.

    This exception occurs when attempting to read from a ringbuffer
    sequence that has already been overwritten.

    Args:
        message: The error message.
        head_sequence: The current head sequence of the ringbuffer.

    Attributes:
        head_sequence: The current head sequence that caused this error.

    Example:
        >>> try:
        ...     item = ringbuffer.read_one(old_sequence)
        ... except StaleSequenceException as e:
        ...     print(f"Sequence stale, head is now: {e.head_sequence}")
        ...     # Resume from head
        ...     item = ringbuffer.read_one(e.head_sequence)
    """

    def __init__(self, message: str, head_sequence: int = -1):
        """Initialize the StaleSequenceException.

        Args:
            message: The error message describing the stale sequence.
            head_sequence: The current head sequence of the ringbuffer.
        """
        super().__init__(message)
        self._head_sequence = head_sequence

    @property
    def head_sequence(self) -> int:
        """Get the current head sequence of the ringbuffer.

        Returns:
            The head sequence at the time of the exception.
        """
        return self._head_sequence


class TimeoutException(HazelcastException):
    """Raised when an operation times out.

    This exception indicates that an operation did not complete
    within the specified timeout period.

    Example:
        >>> try:
        ...     result = my_map.get_async("key").result(timeout=5.0)
        ... except TimeoutException:
        ...     print("Operation timed out")
    """
    pass


class OperationTimeoutException(TimeoutException):
    """Raised when an operation times out on the server side.

    This exception is more specific than :class:`TimeoutException`
    and indicates that the timeout occurred during server-side
    processing rather than client-side waiting.
    """
    pass


class AuthenticationException(HazelcastException):
    """Raised when authentication with the cluster fails.

    This exception indicates that the client could not authenticate
    with the Hazelcast cluster due to invalid credentials or
    configuration.

    Example:
        - Invalid username or password
        - Expired authentication token
        - Missing required security credentials
    """
    pass


class TargetDisconnectedException(HazelcastException):
    """Raised when the target cluster member is disconnected.

    This exception indicates that the specific member targeted
    by an operation is no longer connected to the cluster.
    """
    pass


class HazelcastSerializationException(HazelcastException):
    """Raised when serialization or deserialization fails.

    This exception indicates that an object could not be serialized
    for transmission or deserialized upon receipt.

    Example:
        - Missing serializer for a type
        - Incompatible class versions
        - Corrupted serialized data
    """
    pass


class ClientOfflineException(HazelcastException):
    """Raised when the client is not connected to the cluster.

    This exception indicates that an operation was attempted while
    the client is not connected to any cluster member.

    Example:
        >>> try:
        ...     my_map = client.get_map("test")
        ... except ClientOfflineException:
        ...     print("Client is not connected")
        ...     client.start()  # Reconnect
    """
    pass
