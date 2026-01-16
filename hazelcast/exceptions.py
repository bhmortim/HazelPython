"""Hazelcast client exceptions."""


class HazelcastException(Exception):
    """Base class for all Hazelcast exceptions."""
    pass


class IllegalStateException(HazelcastException):
    """Raised when an operation is invoked on an illegal state."""
    pass


class IllegalArgumentException(HazelcastException):
    """Raised when an illegal argument is passed."""
    pass


class ConfigurationException(HazelcastException):
    """Raised when there is a configuration error."""
    pass


class StaleSequenceException(HazelcastException):
    """Raised when a sequence is older than the head sequence of a ringbuffer."""

    def __init__(self, message: str, head_sequence: int = -1):
        super().__init__(message)
        self._head_sequence = head_sequence

    @property
    def head_sequence(self) -> int:
        """Get the current head sequence."""
        return self._head_sequence


class TimeoutException(HazelcastException):
    """Raised when an operation times out."""
    pass


class OperationTimeoutException(TimeoutException):
    """Raised when an operation times out on the server side."""
    pass


class AuthenticationException(HazelcastException):
    """Raised when authentication fails."""
    pass


class TargetDisconnectedException(HazelcastException):
    """Raised when the target member is disconnected."""
    pass


class HazelcastSerializationException(HazelcastException):
    """Raised when serialization fails."""
    pass
