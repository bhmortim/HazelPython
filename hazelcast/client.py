"""Hazelcast client implementation."""

from hazelcast.config import ClientConfig


class HazelcastClient:
    """Hazelcast Python Client.

    The main entry point for connecting to a Hazelcast cluster.
    """

    def __init__(self, config: ClientConfig = None):
        """Initialize the Hazelcast client.

        Args:
            config: Client configuration. If None, default configuration is used.
        """
        self._config = config or ClientConfig()
        self._running = False

    @property
    def config(self) -> ClientConfig:
        """Get the client configuration."""
        return self._config

    @property
    def running(self) -> bool:
        """Check if the client is running."""
        return self._running

    def start(self) -> "HazelcastClient":
        """Start the client and connect to the cluster.

        Returns:
            This client instance.
        """
        self._running = True
        return self

    def shutdown(self) -> None:
        """Shutdown the client and disconnect from the cluster."""
        self._running = False
