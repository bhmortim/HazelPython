"""Pytest configuration for integration tests."""

import os
import pytest
import subprocess
import time
from typing import Generator, Optional

HAZELCAST_IMAGE = "hazelcast/hazelcast:5.3"
HAZELCAST_PORT = 5701
CONTAINER_NAME = "hazelcast-test"


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="Run integration tests (requires Docker)",
    )
    parser.addoption(
        "--hazelcast-host",
        action="store",
        default="localhost",
        help="Hazelcast host for integration tests",
    )
    parser.addoption(
        "--hazelcast-port",
        action="store",
        default="5701",
        help="Hazelcast port for integration tests",
    )


def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration test (requires Hazelcast cluster)",
    )


def pytest_collection_modifyitems(config, items):
    """Skip integration tests unless --integration flag is provided."""
    if config.getoption("--integration"):
        return

    skip_integration = pytest.mark.skip(
        reason="Need --integration option to run integration tests"
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)


class HazelcastContainer:
    """Manages a Hazelcast Docker container for testing."""

    def __init__(
        self,
        image: str = HAZELCAST_IMAGE,
        port: int = HAZELCAST_PORT,
        container_name: str = CONTAINER_NAME,
    ):
        self.image = image
        self.port = port
        self.container_name = container_name
        self._running = False

    def start(self, timeout: float = 60.0) -> bool:
        """Start the Hazelcast container.

        Args:
            timeout: Maximum time to wait for startup.

        Returns:
            True if started successfully.
        """
        if self._running:
            return True

        try:
            subprocess.run(
                ["docker", "rm", "-f", self.container_name],
                capture_output=True,
            )

            result = subprocess.run(
                [
                    "docker", "run", "-d",
                    "--name", self.container_name,
                    "-p", f"{self.port}:5701",
                    "-e", "HZ_CLUSTERNAME=dev",
                    self.image,
                ],
                capture_output=True,
                text=True,
            )

            if result.returncode != 0:
                print(f"Failed to start container: {result.stderr}")
                return False

            if self._wait_for_ready(timeout):
                self._running = True
                return True

            return False

        except FileNotFoundError:
            print("Docker not found. Please install Docker to run integration tests.")
            return False

    def stop(self) -> None:
        """Stop and remove the container."""
        if not self._running:
            return

        subprocess.run(
            ["docker", "rm", "-f", self.container_name],
            capture_output=True,
        )
        self._running = False

    def _wait_for_ready(self, timeout: float) -> bool:
        """Wait for Hazelcast to be ready."""
        import socket

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1.0)
                result = sock.connect_ex(("localhost", self.port))
                sock.close()

                if result == 0:
                    time.sleep(2.0)
                    return True
            except Exception:
                pass

            time.sleep(1.0)

        return False

    @property
    def host(self) -> str:
        """Get the container host."""
        return "localhost"

    @property
    def address(self) -> str:
        """Get the full address string."""
        return f"{self.host}:{self.port}"


@pytest.fixture(scope="session")
def hazelcast_container(request) -> Generator[Optional[HazelcastContainer], None, None]:
    """Provide a Hazelcast container for integration tests.

    This fixture starts a Hazelcast container at the beginning of
    the test session and stops it at the end.
    """
    if not request.config.getoption("--integration"):
        yield None
        return

    container = HazelcastContainer()

    if container.start():
        yield container
        container.stop()
    else:
        yield None


@pytest.fixture
def hazelcast_address(request, hazelcast_container) -> str:
    """Get the Hazelcast address for tests."""
    host = request.config.getoption("--hazelcast-host")
    port = request.config.getoption("--hazelcast-port")
    return f"{host}:{port}"


@pytest.fixture
def client_config(hazelcast_address):
    """Provide a configured ClientConfig for tests."""
    from hazelcast.config import ClientConfig

    config = ClientConfig()
    config.cluster_name = "dev"
    config.cluster_members = [hazelcast_address]
    config.connection_timeout = 10.0

    return config


@pytest.fixture
def hazelcast_client(client_config):
    """Provide a connected HazelcastClient for tests."""
    from hazelcast import HazelcastClient

    client = HazelcastClient(client_config)

    try:
        client.start()
        yield client
    finally:
        client.shutdown()
