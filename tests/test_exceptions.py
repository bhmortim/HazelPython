"""Unit tests for Hazelcast exceptions."""

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
    IllegalStateException,
    IllegalArgumentException,
    ConfigurationException,
)


class TestHazelcastException:
    def test_default_message(self):
        exc = HazelcastException()
        assert exc.message == ""
        assert exc.cause is None

    def test_custom_message(self):
        exc = HazelcastException("Something went wrong")
        assert exc.message == "Something went wrong"
        assert str(exc) == "Something went wrong"

    def test_with_cause(self):
        cause = ValueError("Original error")
        exc = HazelcastException("Wrapped error", cause=cause)
        assert exc.cause is cause
        assert "caused by" in str(exc)
        assert "Original error" in str(exc)

    def test_is_exception(self):
        exc = HazelcastException("Test")
        assert isinstance(exc, Exception)


class TestClientOfflineException:
    def test_default_message(self):
        exc = ClientOfflineException()
        assert "offline" in exc.message.lower()

    def test_custom_message(self):
        exc = ClientOfflineException("Custom offline message")
        assert exc.message == "Custom offline message"

    def test_inherits_from_hazelcast_exception(self):
        exc = ClientOfflineException()
        assert isinstance(exc, HazelcastException)


class TestAuthenticationException:
    def test_default_message(self):
        exc = AuthenticationException()
        assert "authentication" in exc.message.lower()

    def test_custom_message(self):
        exc = AuthenticationException("Invalid credentials")
        assert exc.message == "Invalid credentials"

    def test_inherits_from_hazelcast_exception(self):
        exc = AuthenticationException()
        assert isinstance(exc, HazelcastException)


class TestTimeoutException:
    def test_default_message(self):
        exc = TimeoutException()
        assert "timeout" in exc.message.lower()

    def test_inherits_from_hazelcast_exception(self):
        exc = TimeoutException()
        assert isinstance(exc, HazelcastException)


class TestOperationTimeoutException:
    def test_inherits_from_timeout_exception(self):
        exc = OperationTimeoutException()
        assert isinstance(exc, TimeoutException)
        assert isinstance(exc, HazelcastException)


class TestSerializationException:
    def test_default_message(self):
        exc = SerializationException()
        assert "serialization" in exc.message.lower()

    def test_with_cause(self):
        cause = TypeError("Cannot serialize object")
        exc = SerializationException("Failed to serialize", cause=cause)
        assert exc.cause is cause

    def test_inherits_from_hazelcast_exception(self):
        exc = SerializationException()
        assert isinstance(exc, HazelcastException)


class TestTargetDisconnectedException:
    def test_default_message(self):
        exc = TargetDisconnectedException()
        assert "disconnect" in exc.message.lower()

    def test_inherits_from_hazelcast_exception(self):
        exc = TargetDisconnectedException()
        assert isinstance(exc, HazelcastException)


class TestHazelcastInstanceNotActiveException:
    def test_default_message(self):
        exc = HazelcastInstanceNotActiveException()
        assert "not active" in exc.message.lower()

    def test_inherits_from_hazelcast_exception(self):
        exc = HazelcastInstanceNotActiveException()
        assert isinstance(exc, HazelcastException)


class TestIllegalStateException:
    def test_default_message(self):
        exc = IllegalStateException()
        assert "illegal state" in exc.message.lower()

    def test_inherits_from_hazelcast_exception(self):
        exc = IllegalStateException()
        assert isinstance(exc, HazelcastException)


class TestIllegalArgumentException:
    def test_default_message(self):
        exc = IllegalArgumentException()
        assert "illegal argument" in exc.message.lower()

    def test_inherits_from_hazelcast_exception(self):
        exc = IllegalArgumentException()
        assert isinstance(exc, HazelcastException)


class TestConfigurationException:
    def test_default_message(self):
        exc = ConfigurationException()
        assert "configuration" in exc.message.lower()

    def test_inherits_from_hazelcast_exception(self):
        exc = ConfigurationException()
        assert isinstance(exc, HazelcastException)


class TestExceptionRaising:
    def test_raise_and_catch_base(self):
        with pytest.raises(HazelcastException):
            raise HazelcastException("Test error")

    def test_catch_specific_as_base(self):
        with pytest.raises(HazelcastException):
            raise ClientOfflineException()

    def test_catch_timeout_hierarchy(self):
        with pytest.raises(TimeoutException):
            raise OperationTimeoutException()
