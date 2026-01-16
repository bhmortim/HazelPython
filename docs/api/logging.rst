Logging Configuration
=====================

.. module:: hazelcast.logging_config
   :synopsis: Logging configuration for Hazelcast Python Client.

This module provides utilities for configuring logging in the Hazelcast
Python Client.

Module Attributes
-----------------

.. data:: HAZELCAST_LOGGER_NAME

   The name of the Hazelcast logger (``"hazelcast"``).

Functions
---------

.. autofunction:: get_logger

.. autofunction:: configure_logging

.. autofunction:: set_level

.. autofunction:: disable_logging

.. autofunction:: enable_logging

Example Usage
-------------

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
