"""Logging configuration for Hazelcast Python Client.

This module provides utilities for configuring logging in the Hazelcast
Python Client. It supports custom log levels, formats, and handlers.

The module uses a single logger named "hazelcast" for all client logging.
By default, no handlers are configured, allowing users to integrate with
their existing logging setup.

Example:
    Basic logging configuration::

        from hazelcast.logging_config import configure_logging
        import logging

        configure_logging(level=logging.DEBUG)

    Custom handler::

        import logging
        from hazelcast.logging_config import configure_logging

        handler = logging.FileHandler("hazelcast.log")
        configure_logging(level=logging.INFO, handler=handler)

    Disable logging::

        from hazelcast.logging_config import disable_logging

        disable_logging()

Attributes:
    HAZELCAST_LOGGER_NAME: The name of the Hazelcast logger ("hazelcast").
"""

import logging
from typing import Optional


HAZELCAST_LOGGER_NAME = "hazelcast"

_logger: Optional[logging.Logger] = None


def get_logger() -> logging.Logger:
    """Get the Hazelcast logger instance.

    Returns a singleton logger instance for the Hazelcast client. The logger
    is created on first access and reused for subsequent calls.

    Returns:
        The Hazelcast logger instance.

    Example:
        >>> logger = get_logger()
        >>> logger.info("Connected to cluster")
    """
    global _logger
    if _logger is None:
        _logger = logging.getLogger(HAZELCAST_LOGGER_NAME)
    return _logger


def configure_logging(
    level: int = logging.INFO,
    format_string: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handler: Optional[logging.Handler] = None,
) -> logging.Logger:
    """Configure the Hazelcast logger with custom settings.

    Sets up the Hazelcast logger with the specified level, format, and handler.
    If no handler is provided, a StreamHandler (console output) is used.
    This method only adds a handler if no handlers are already configured.

    Args:
        level: Logging level (e.g., ``logging.DEBUG``, ``logging.INFO``,
            ``logging.WARNING``). Defaults to ``logging.INFO``.
        format_string: Format string for log messages following Python's
            logging format specification. Defaults to a format including
            timestamp, logger name, level, and message.
        handler: Optional custom handler. If ``None``, a ``StreamHandler``
            is used for console output.

    Returns:
        The configured logger instance.

    Example:
        Configure with DEBUG level::

            import logging
            from hazelcast.logging_config import configure_logging

            logger = configure_logging(level=logging.DEBUG)

        Configure with file handler::

            import logging
            from hazelcast.logging_config import configure_logging

            file_handler = logging.FileHandler("hazelcast.log")
            logger = configure_logging(
                level=logging.INFO,
                handler=file_handler
            )
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

    Updates the logging level for both the logger and all its handlers.
    This allows runtime adjustment of logging verbosity.

    Args:
        level: Logging level (e.g., ``logging.DEBUG``, ``logging.INFO``,
            ``logging.WARNING``, ``logging.ERROR``, ``logging.CRITICAL``).

    Example:
        >>> from hazelcast.logging_config import set_level
        >>> import logging
        >>> set_level(logging.DEBUG)  # Enable verbose logging
        >>> set_level(logging.ERROR)  # Only show errors
    """
    logger = get_logger()
    logger.setLevel(level)
    for handler in logger.handlers:
        handler.setLevel(level)


def disable_logging() -> None:
    """Disable all Hazelcast logging.

    Completely disables logging output from the Hazelcast client.
    Use :func:`enable_logging` to re-enable logging.

    Example:
        >>> from hazelcast.logging_config import disable_logging
        >>> disable_logging()  # Suppress all Hazelcast log output
    """
    logger = get_logger()
    logger.disabled = True


def enable_logging() -> None:
    """Enable Hazelcast logging.

    Re-enables logging after it was disabled with :func:`disable_logging`.

    Example:
        >>> from hazelcast.logging_config import enable_logging
        >>> enable_logging()  # Resume Hazelcast logging
    """
    logger = get_logger()
    logger.disabled = False
