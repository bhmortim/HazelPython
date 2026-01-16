"""Unit tests for hazelcast.exceptions module."""

import pytest

from hazelcast.exceptions import (
    HazelcastException,
    IllegalStateException,
    IllegalArgumentException,
    ConfigurationException,
    StaleSequenceException,
    TimeoutException,
    OperationTimeoutException,
    AuthenticationException,
    TargetDisconnectedException,
    HazelcastSerializationException,
    SerializationException,
    ClientOfflineException,
)


class TestHazelcastException:
    """Tests for HazelcastException base class."""

    def test_create_with_message(self):
        ex = HazelcastException("test message")
        assert str(ex) == "test message"
        assert ex.cause is None

    def test_create_with_message_and_cause(self):
        cause = ValueError("original error")
        ex = HazelcastException("wrapper message", cause=cause)
        assert str(ex) == "wrapper message"
        assert ex.cause is cause

    def test_create_empty(self):
        ex = HazelcastException()
        assert str(ex) == ""
        assert ex.cause is None

    def test_inheritance(self):
        ex = HazelcastException("test")
        assert isinstance(ex, Exception)


class TestIllegalStateException:
    """Tests for IllegalStateException."""

    def test_inheritance(self):
        ex = IllegalStateException("invalid state")
        assert isinstance(ex, HazelcastException)
        assert isinstance(ex, Exception)

    def test_message(self):
        ex = IllegalStateException("Map is destroyed")
        assert str(ex) == "Map is destroyed"


class TestIllegalArgumentException:
    """Tests for IllegalArgumentException."""

    def test_inheritance(self):
        ex = IllegalArgumentException("invalid argument")
        assert isinstance(ex, HazelcastException)

    def test_with_cause(self):
        cause = TypeError("wrong type")
        ex = IllegalArgumentException("invalid", cause=cause)
        assert ex.cause is cause


class TestConfigurationException:
    """Tests for ConfigurationException."""

    def test_inheritance(self):
        ex = ConfigurationException("bad config")
        assert isinstance(ex, HazelcastException)

    def test_message(self):
        ex = ConfigurationException("Invalid cluster address")
        assert "Invalid cluster address" in str(ex)


class TestStaleSequenceException:
    """Tests for StaleSequenceException."""

    def test_with_head_sequence(self):
        ex = StaleSequenceException("sequence stale", head_sequence=100)
        assert str(ex) == "sequence stale"
        assert ex.head_sequence == 100

    def test_default_head_sequence(self):
        ex = StaleSequenceException("stale")
        assert ex.head_sequence == -1

    def test_inheritance(self):
        ex = StaleSequenceException("test", 50)
        assert isinstance(ex, HazelcastException)

    @pytest.mark.parametrize("head_seq", [0, 1, 100, 999999])
    def test_various_head_sequences(self, head_seq):
        ex = StaleSequenceException("msg", head_sequence=head_seq)
        assert ex.head_sequence == head_seq


class TestTimeoutException:
    """Tests for TimeoutException."""

    def test_inheritance(self):
        ex = TimeoutException("timed out")
        assert isinstance(ex, HazelcastException)

    def test_message(self):
        ex = TimeoutException("Operation timed out after 30s")
        assert "30s" in str(ex)


class TestOperationTimeoutException:
    """Tests for OperationTimeoutException."""

    def test_inheritance(self):
        ex = OperationTimeoutException("server timeout")
        assert isinstance(ex, TimeoutException)
        assert isinstance(ex, HazelcastException)


class TestAuthenticationException:
    """Tests for AuthenticationException."""

    def test_inheritance(self):
        ex = AuthenticationException("auth failed")
        assert isinstance(ex, HazelcastException)

    def test_with_cause(self):
        cause = ValueError("invalid token")
        ex = AuthenticationException("auth failed", cause=cause)
        assert ex.cause is cause


class TestTargetDisconnectedException:
    """Tests for TargetDisconnectedException."""

    def test_inheritance(self):
        ex = TargetDisconnectedException("member disconnected")
        assert isinstance(ex, HazelcastException)


class TestHazelcastSerializationException:
    """Tests for HazelcastSerializationException."""

    def test_inheritance(self):
        ex = HazelcastSerializationException("serialization error")
        assert isinstance(ex, HazelcastException)

    def test_alias(self):
        assert SerializationException is HazelcastSerializationException


class TestClientOfflineException:
    """Tests for ClientOfflineException."""

    def test_inheritance(self):
        ex = ClientOfflineException("client offline")
        assert isinstance(ex, HazelcastException)

    def test_message(self):
        ex = ClientOfflineException("Not connected to cluster")
        assert "Not connected" in str(ex)


class TestExceptionChaining:
    """Tests for exception chaining scenarios."""

    def test_nested_causes(self):
        root = ValueError("root cause")
        middle = HazelcastSerializationException("ser error", cause=root)
        outer = HazelcastException("operation failed", cause=middle)
        
        assert outer.cause is middle
        assert middle.cause is root

    def test_raise_and_catch(self):
        with pytest.raises(HazelcastException) as exc_info:
            raise IllegalStateException("test error")
        assert isinstance(exc_info.value, IllegalStateException)

    @pytest.mark.parametrize("exception_class", [
        HazelcastException,
        IllegalStateException,
        IllegalArgumentException,
        ConfigurationException,
        TimeoutException,
        OperationTimeoutException,
        AuthenticationException,
        TargetDisconnectedException,
        HazelcastSerializationException,
        ClientOfflineException,
    ])
    def test_all_exceptions_catchable_as_base(self, exception_class):
        with pytest.raises(HazelcastException):
            raise exception_class("test")
