"""Logging configuration for Hazelcast Python Client."""

import logging
from typing import Optional


HAZELCAST_LOGGER_NAME = "hazelcast"

_logger: Optional[logging.Logger] = None


def get_logger() -> logging.Logger:
    """Get the Hazelcast logger instance."""
    global _logger
    if _logger is None:
        _logger = logging.getLogger(HAZELCAST_LOGGER_NAME)
    return _logger


def configure_logging(
    level: int = logging.INFO,
    format_string: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handler: Optional[logging.Handler] = None,
) -> logging.Logger:
    """Configure the Hazelcast logger.

    Args:
        level: Logging level (e.g., logging.DEBUG, logging.INFO).
        format_string: Format string for log messages.
        handler: Optional custom handler. If None, a StreamHandler is used.

    Returns:
        The configured logger instance.
    """
    logger = get_logger()
    logger.setLevel(level)

    if not logger.handlers:
        if handler is None:
            handler = logging.StreamHandler()
        handler.setLevel(level)
        formatter = logging.Formatter(format_string)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


def set_level(level: int) -> None:
    """Set the logging level for the Hazelcast logger.

    Args:
        level: Logging level (e.g., logging.DEBUG, logging.INFO).
    """
    logger = get_logger()
    logger.setLevel(level)
    for handler in logger.handlers:
        handler.setLevel(level)


def disable_logging() -> None:
    """Disable all Hazelcast logging."""
    logger = get_logger()
    logger.disabled = True


def enable_logging() -> None:
    """Enable Hazelcast logging."""
    logger = get_logger()
    logger.disabled = False
