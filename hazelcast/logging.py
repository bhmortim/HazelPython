"""Logging infrastructure for Hazelcast Python Client.

This module provides a centralized logging configuration and factory
for all Hazelcast components.

Example:
    >>> from hazelcast.logging import configure_logging, get_logger
    >>> configure_logging(level=logging.DEBUG)
    >>> logger = get_logger("my_component")
    >>> logger.info("Component initialized")
"""

import logging
from typing import Optional


HAZELCAST_ROOT_LOGGER = "hazelcast"


class HazelcastLoggerFactory:
    """Factory for creating and managing Hazelcast component loggers.

    Provides hierarchical loggers under the 'hazelcast' namespace,
    allowing fine-grained control over logging levels per component.
    """

    _configured: bool = False
    _default_level: int = logging.INFO

    @classmethod
    def get_logger(cls, name: str = "") -> logging.Logger:
        """Get a logger for a Hazelcast component.

        Args:
            name: Component name (e.g., 'connection', 'invocation').
                  If empty, returns the root hazelcast logger.

        Returns:
            A logger instance for the specified component.
        """
        if name:
            logger_name = f"{HAZELCAST_ROOT_LOGGER}.{name}"
        else:
            logger_name = HAZELCAST_ROOT_LOGGER
        return logging.getLogger(logger_name)

    @classmethod
    def configure(
        cls,
        level: int = logging.INFO,
        format_string: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handler: Optional[logging.Handler] = None,
    ) -> logging.Logger:
        """Configure the Hazelcast logging system.

        Args:
            level: Logging level (e.g., logging.DEBUG, logging.INFO).
            format_string: Format string for log messages.
            handler: Optional custom handler. If None, a StreamHandler is used.

        Returns:
            The configured root logger.
        """
        logger = logging.getLogger(HAZELCAST_ROOT_LOGGER)
        logger.setLevel(level)
        cls._default_level = level

        if not logger.handlers:
            if handler is None:
                handler = logging.StreamHandler()
            handler.setLevel(level)
            formatter = logging.Formatter(format_string)
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        cls._configured = True
        return logger

    @classmethod
    def set_level(cls, level: int, component: str = "") -> None:
        """Set logging level for a specific component or the root logger.

        Args:
            level: Logging level (e.g., logging.DEBUG).
            component: Component name, or empty for root logger.
        """
        logger = cls.get_logger(component)
        logger.setLevel(level)

    @classmethod
    def disable(cls) -> None:
        """Disable all Hazelcast logging."""
        logger = logging.getLogger(HAZELCAST_ROOT_LOGGER)
        logger.disabled = True

    @classmethod
    def enable(cls) -> None:
        """Enable Hazelcast logging."""
        logger = logging.getLogger(HAZELCAST_ROOT_LOGGER)
        logger.disabled = False

    @classmethod
    def is_configured(cls) -> bool:
        """Check if logging has been configured."""
        return cls._configured


def get_logger(name: str = "") -> logging.Logger:
    """Get a Hazelcast logger for a component.

    This is a convenience function that delegates to HazelcastLoggerFactory.

    Args:
        name: Component name (e.g., 'connection', 'client', 'proxy').

    Returns:
        Logger instance for the component.
    """
    return HazelcastLoggerFactory.get_logger(name)


def configure_logging(
    level: int = logging.INFO,
    format_string: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handler: Optional[logging.Handler] = None,
) -> logging.Logger:
    """Configure the Hazelcast logging system.

    This is the primary entry point for setting up logging.

    Args:
        level: Logging level (e.g., logging.DEBUG, logging.INFO).
        format_string: Format string for log messages.
        handler: Optional custom handler. If None, a StreamHandler is used.

    Returns:
        The configured root logger.
    """
    return HazelcastLoggerFactory.configure(level, format_string, handler)


def set_level(level: int, component: str = "") -> None:
    """Set logging level for a component.

    Args:
        level: Logging level.
        component: Component name, or empty for root logger.
    """
    HazelcastLoggerFactory.set_level(level, component)
