"""Unit tests for hazelcast.exceptions module."""

import pytest

from hazelcast.exceptions import (
    HazelcastException,
    ClientOfflineException,
    AuthenticationException,
    TimeoutException,
    SerializationException,
    TargetDisconnectedException,
    HazelcastInstanceNotActiveException,
    OperationTimeoutException,
    InvocationMightContainCompactDataException,
    IllegalStateException,
    IllegalArgumentException,
    ConfigurationException,
)


class TestHazelcastException:
    """Tests for HazelcastException base class."""

    def test_init_with_message(self):
        exc = HazelcastException("test message")
        assert exc.message == "test message"
        assert exc.cause is None
        assert str(exc) == "test message"

    def test_init_with_cause(self):
        cause = ValueError("original error")
        exc = HazelcastException("wrapped error", cause=cause)
        assert exc.message == "wrapped error"
        assert exc.cause is cause
        assert "caused by" in str(exc)
        assert "original error" in str(exc)

    def test_init_default_message(self):
        exc = HazelcastException()
        assert exc.message == ""
        assert str(exc) == ""

    def test_is_exception_subclass(self):
        exc = HazelcastException("test")
        assert isinstance(exc, Exception)


class TestClientOfflineException:
    """Tests for ClientOfflineException."""

    def test_default_message(self):
        exc = ClientOfflineException()
        assert "offline" in exc.message.lower()

    def test_custom_message(self):
        exc = ClientOfflineException("Custom offline message")
        assert exc.message == "Custom offline message"


class TestAuthenticationException:
    """Tests for AuthenticationException."""

    def test_default_message(self):
        exc = AuthenticationException()
        assert "authentication" in exc.message.lower()

    def test_custom_message(self):
        exc = AuthenticationException("Invalid credentials")
        assert exc.message == "Invalid credentials"


class TestTimeoutException:
    """Tests for TimeoutException."""

    def test_default_message(self):
        exc = TimeoutException()
        assert "timeout" in exc.message.lower()

    def test_custom_message(self):
        exc = TimeoutException("Connection timeout")
        assert exc.message == "Connection timeout"


class TestSerializationException:
    """Tests for SerializationException."""

    def test_default_message(self):
        exc = SerializationException()
        assert "serialization" in exc.message.lower()

    def test_with_cause(self):
        cause = TypeError("Cannot serialize object")
        exc = SerializationException("Failed to serialize", cause=cause)
        assert exc.cause is cause
        assert "caused by" in str(exc)


class TestTargetDisconnectedException:
    """Tests for TargetDisconnectedException."""

    def test_default_message(self):
        exc = TargetDisconnectedException()
        assert "disconnected" in exc.message.lower()


class TestHazelcastInstanceNotActiveException:
    """Tests for HazelcastInstanceNotActiveException."""

    def test_default_message(self):
        exc = HazelcastInstanceNotActiveException()
        assert "not active" in exc.message.lower()


class TestOperationTimeoutException:
    """Tests for OperationTimeoutException."""

    def test_is_timeout_exception_subclass(self):
        exc = OperationTimeoutException()
        assert isinstance(exc, TimeoutException)

    def test_default_message(self):
        exc = OperationTimeoutException()
        assert "timeout" in exc.message.lower()


class TestInvocationMightContainCompactDataException:
    """Tests for InvocationMightContainCompactDataException."""

    def test_default_message(self):
        exc = InvocationMightContainCompactDataException()
        assert "compact" in exc.message.lower()


class TestIllegalStateException:
    """Tests for IllegalStateException."""

    def test_default_message(self):
        exc = IllegalStateException()
        assert "illegal state" in exc.message.lower()

    def test_custom_message(self):
        exc = IllegalStateException("Client is shutting down")
        assert exc.message == "Client is shutting down"


class TestIllegalArgumentException:
    """Tests for IllegalArgumentException."""

    def test_default_message(self):
        exc = IllegalArgumentException()
        assert "illegal argument" in exc.message.lower()

    def test_custom_message(self):
        exc = IllegalArgumentException("Value must be positive")
        assert exc.message == "Value must be positive"


class TestConfigurationException:
    """Tests for ConfigurationException."""

    def test_default_message(self):
        exc = ConfigurationException()
        assert "configuration" in exc.message.lower()

    def test_custom_message(self):
        exc = ConfigurationException("Invalid cluster name")
        assert exc.message == "Invalid cluster name"


class TestExceptionHierarchy:
    """Tests for exception hierarchy and inheritance."""

    def test_all_exceptions_inherit_from_hazelcast(self):
        exceptions = [
            ClientOfflineException(),
            AuthenticationException(),
            TimeoutException(),
            SerializationException(),
            TargetDisconnectedException(),
            HazelcastInstanceNotActiveException(),
            OperationTimeoutException(),
            InvocationMightContainCompactDataException(),
            IllegalStateException(),
            IllegalArgumentException(),
            ConfigurationException(),
        ]
        for exc in exceptions:
            assert isinstance(exc, HazelcastException)

    def test_operation_timeout_inherits_from_timeout(self):
        exc = OperationTimeoutException()
        assert isinstance(exc, TimeoutException)
        assert isinstance(exc, HazelcastException)

    def test_can_catch_all_with_hazelcast_exception(self):
        with pytest.raises(HazelcastException):
            raise ClientOfflineException()

        with pytest.raises(HazelcastException):
            raise SerializationException()

        with pytest.raises(HazelcastException):
            raise ConfigurationException()
