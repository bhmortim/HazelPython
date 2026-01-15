"""Hazelcast exception hierarchy."""


class HazelcastException(Exception):
    """Base exception for all Hazelcast-related errors."""

    def __init__(self, message: str = "", cause: Exception = None):
        super().__init__(message)
        self.message = message
        self.cause = cause

    def __str__(self) -> str:
        if self.cause:
            return f"{self.message} (caused by: {self.cause})"
        return self.message


class ClientOfflineException(HazelcastException):
    """Raised when the client is not connected to the cluster."""

    def __init__(self, message: str = "Client is offline"):
        super().__init__(message)


class AuthenticationException(HazelcastException):
    """Raised when authentication with the cluster fails."""

    def __init__(self, message: str = "Authentication failed"):
        super().__init__(message)


class TimeoutException(HazelcastException):
    """Raised when an operation times out."""

    def __init__(self, message: str = "Operation timed out"):
        super().__init__(message)


class SerializationException(HazelcastException):
    """Raised when serialization or deserialization fails."""

    def __init__(self, message: str = "Serialization error", cause: Exception = None):
        super().__init__(message, cause)


class TargetDisconnectedException(HazelcastException):
    """Raised when the target member disconnects during an operation."""

    def __init__(self, message: str = "Target member disconnected"):
        super().__init__(message)


class HazelcastInstanceNotActiveException(HazelcastException):
    """Raised when the Hazelcast instance is not active."""

    def __init__(self, message: str = "Hazelcast instance is not active"):
        super().__init__(message)


class OperationTimeoutException(TimeoutException):
    """Raised when an operation exceeds its timeout."""

    def __init__(self, message: str = "Operation timeout exceeded"):
        super().__init__(message)


class InvocationMightContainCompactDataException(HazelcastException):
    """Raised when an invocation might contain compact serialized data."""

    def __init__(self, message: str = "Invocation might contain compact data"):
        super().__init__(message)


class IllegalStateException(HazelcastException):
    """Raised when the client is in an illegal state for the requested operation."""

    def __init__(self, message: str = "Illegal state"):
        super().__init__(message)


class IllegalArgumentException(HazelcastException):
    """Raised when an illegal argument is passed to a method."""

    def __init__(self, message: str = "Illegal argument"):
        super().__init__(message)


class ConfigurationException(HazelcastException):
    """Raised when there is a configuration error."""

    def __init__(self, message: str = "Configuration error"):
        super().__init__(message)
