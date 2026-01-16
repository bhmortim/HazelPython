"""Integration test fixtures using testcontainers."""

import os
import time
import pytest
from typing import Generator, List, Optional

# Check if testcontainers is available
try:
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs
    TESTCONTAINERS_AVAILABLE = True
except ImportError:
    TESTCONTAINERS_AVAILABLE = False
    DockerContainer = None

# Check if Docker is available
DOCKER_AVAILABLE = False
if TESTCONTAINERS_AVAILABLE:
    try:
        import docker
        client = docker.from_env()
        client.ping()
        DOCKER_AVAILABLE = True
    except Exception:
        pass

# CI environment detection
CI_ENVIRONMENT = os.environ.get("CI", "false").lower() == "true"
SKIP_INTEGRATION = os.environ.get("SKIP_INTEGRATION_TESTS", "false").lower() == "true"

# Hazelcast image configuration
HAZELCAST_IMAGE = os.environ.get("HAZELCAST_IMAGE", "hazelcast/hazelcast:5.3")
HAZELCAST_PORT = 5701


def requires_docker(func):
    """Decorator to skip tests when Docker is not available."""
    return pytest.mark.skipif(
        not DOCKER_AVAILABLE,
        reason="Docker is not available"
    )(func)


def requires_testcontainers(func):
    """Decorator to skip tests when testcontainers is not installed."""
    return pytest.mark.skipif(
        not TESTCONTAINERS_AVAILABLE,
        reason="testcontainers package is not installed"
    )(func)


skip_integration = pytest.mark.skipif(
    SKIP_INTEGRATION or not DOCKER_AVAILABLE,
    reason="Integration tests disabled or Docker unavailable"
)


class HazelcastContainer:
    """Wrapper for Hazelcast Docker container."""

    def __init__(
        self,
        image: str = HAZELCAST_IMAGE,
        cluster_name: str = "dev",
        port: int = HAZELCAST_PORT,
    ):
        self._image = image
        self._cluster_name = cluster_name
        self._port = port
        self._container: Optional[DockerContainer] = None
        self._host: Optional[str] = None
        self._mapped_port: Optional[int] = None

    def start(self) -> "HazelcastContainer":
        """Start the Hazelcast container."""
        if not TESTCONTAINERS_AVAILABLE:
            raise RuntimeError("testcontainers package is not installed")

        self._container = (
            DockerContainer(self._image)
            .with_exposed_ports(self._port)
            .with_env("HZ_CLUSTERNAME", self._cluster_name)
            .with_env("JAVA_OPTS", "-Dhazelcast.phone.home.enabled=false")
        )
        self._container.start()

        wait_for_logs(self._container, "is STARTED", timeout=60)
        time.sleep(2)

        self._host = self._container.get_container_host_ip()
        self._mapped_port = int(self._container.get_exposed_port(self._port))

        return self

    def stop(self) -> None:
        """Stop the Hazelcast container."""
        if self._container:
            self._container.stop()
            self._container = None

    @property
    def host(self) -> str:
        """Get the container host."""
        return self._host or "localhost"

    @property
    def port(self) -> int:
        """Get the mapped port."""
        return self._mapped_port or self._port

    @property
    def address(self) -> str:
        """Get the full address string."""
        return f"{self.host}:{self.port}"

    @property
    def cluster_name(self) -> str:
        """Get the cluster name."""
        return self._cluster_name

    def __enter__(self) -> "HazelcastContainer":
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()


class HazelcastCluster:
    """Multi-node Hazelcast cluster for testing."""

    def __init__(
        self,
        size: int = 3,
        image: str = HAZELCAST_IMAGE,
        cluster_name: str = "test-cluster",
    ):
        self._size = size
        self._image = image
        self._cluster_name = cluster_name
        self._containers: List[HazelcastContainer] = []
        self._network_name: Optional[str] = None

    def start(self) -> "HazelcastCluster":
        """Start the cluster with multiple nodes."""
        if not TESTCONTAINERS_AVAILABLE:
            raise RuntimeError("testcontainers package is not installed")

        for i in range(self._size):
            container = HazelcastContainer(
                image=self._image,
                cluster_name=self._cluster_name,
                port=HAZELCAST_PORT,
            )
            container.start()
            self._containers.append(container)
            time.sleep(1)

        time.sleep(3)
        return self

    def stop(self) -> None:
        """Stop all cluster nodes."""
        for container in self._containers:
            try:
                container.stop()
            except Exception:
                pass
        self._containers.clear()

    @property
    def addresses(self) -> List[str]:
        """Get all cluster member addresses."""
        return [c.address for c in self._containers]

    @property
    def cluster_name(self) -> str:
        """Get the cluster name."""
        return self._cluster_name

    @property
    def size(self) -> int:
        """Get the current cluster size."""
        return len(self._containers)

    def stop_node(self, index: int) -> None:
        """Stop a specific node by index."""
        if 0 <= index < len(self._containers):
            self._containers[index].stop()

    def __enter__(self) -> "HazelcastCluster":
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()


@pytest.fixture(scope="session")
def hazelcast_container() -> Generator[HazelcastContainer, None, None]:
    """Session-scoped fixture for a single Hazelcast container."""
    if not DOCKER_AVAILABLE:
        pytest.skip("Docker is not available")

    container = HazelcastContainer(cluster_name="integration-test")
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="function")
def hazelcast_container_per_test() -> Generator[HazelcastContainer, None, None]:
    """Function-scoped fixture for isolated Hazelcast container per test."""
    if not DOCKER_AVAILABLE:
        pytest.skip("Docker is not available")

    container = HazelcastContainer(cluster_name="test-isolated")
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def hazelcast_cluster() -> Generator[HazelcastCluster, None, None]:
    """Module-scoped fixture for a multi-node Hazelcast cluster."""
    if not DOCKER_AVAILABLE:
        pytest.skip("Docker is not available")

    cluster = HazelcastCluster(size=3, cluster_name="multi-node-test")
    cluster.start()
    yield cluster
    cluster.stop()


@pytest.fixture
def client_config(hazelcast_container: HazelcastContainer):
    """Create a ClientConfig configured for the test container."""
    from hazelcast.config import ClientConfig

    config = ClientConfig()
    config.cluster_name = hazelcast_container.cluster_name
    config.cluster_members = [hazelcast_container.address]
    config.connection_timeout = 10.0
    return config


@pytest.fixture
def cluster_client_config(hazelcast_cluster: HazelcastCluster):
    """Create a ClientConfig configured for the test cluster."""
    from hazelcast.config import ClientConfig

    config = ClientConfig()
    config.cluster_name = hazelcast_cluster.cluster_name
    config.cluster_members = hazelcast_cluster.addresses
    config.connection_timeout = 10.0
    return config


@pytest.fixture
def connected_client(client_config):
    """Fixture providing a connected HazelcastClient."""
    from hazelcast.client import HazelcastClient

    client = HazelcastClient(client_config)
    client.start()
    yield client
    client.shutdown()
