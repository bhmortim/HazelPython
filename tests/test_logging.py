"""Unit tests for hazelcast.logging module."""

import pytest
import logging
from unittest.mock import MagicMock, patch

from hazelcast.logging import (
    HazelcastLoggerFactory,
    get_logger,
    configure_logging,
    set_level,
    HAZELCAST_ROOT_LOGGER,
)


class TestHazelcastLoggerFactory:
    """Tests for HazelcastLoggerFactory class."""

    def test_get_logger_root(self):
        logger = HazelcastLoggerFactory.get_logger()
        assert logger.name == HAZELCAST_ROOT_LOGGER

    def test_get_logger_component(self):
        logger = HazelcastLoggerFactory.get_logger("connection")
        assert logger.name == f"{HAZELCAST_ROOT_LOGGER}.connection"

    def test_configure(self):
        logger = HazelcastLoggerFactory.configure(level=logging.DEBUG)
        assert logger.level == logging.DEBUG
        assert HazelcastLoggerFactory.is_configured() is True

    def test_configure_with_handler(self):
        handler = logging.StreamHandler()
        logger = HazelcastLoggerFactory.configure(handler=handler)
        assert handler in logger.handlers or len(logger.handlers) > 0

    def test_set_level(self):
        HazelcastLoggerFactory.configure()
        HazelcastLoggerFactory.set_level(logging.WARNING, "test_component")
        logger = HazelcastLoggerFactory.get_logger("test_component")
        assert logger.level == logging.WARNING

    def test_disable(self):
        HazelcastLoggerFactory.configure()
        HazelcastLoggerFactory.disable()
        logger = logging.getLogger(HAZELCAST_ROOT_LOGGER)
        assert logger.disabled is True

    def test_enable(self):
        HazelcastLoggerFactory.configure()
        HazelcastLoggerFactory.disable()
        HazelcastLoggerFactory.enable()
        logger = logging.getLogger(HAZELCAST_ROOT_LOGGER)
        assert logger.disabled is False


class TestModuleFunctions:
    """Tests for module-level functions."""

    def test_get_logger(self):
        logger = get_logger("invocation")
        assert logger.name == f"{HAZELCAST_ROOT_LOGGER}.invocation"

    def test_get_logger_empty(self):
        logger = get_logger()
        assert logger.name == HAZELCAST_ROOT_LOGGER

    def test_configure_logging(self):
        logger = configure_logging(level=logging.INFO)
        assert logger is not None

    def test_set_level_function(self):
        configure_logging()
        set_level(logging.ERROR, "test")
        logger = get_logger("test")
        assert logger.level == logging.ERROR

    def test_loggers_are_hierarchical(self):
        parent = get_logger()
        child = get_logger("child")
        assert child.parent is parent or child.parent.name == parent.name
