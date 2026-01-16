#!/usr/bin/env python3
"""Advanced networking and socket tuning example.

Demonstrates how to configure network settings for optimal
performance in various deployment scenarios.

Topics covered:
- Socket buffer sizes
- TCP_NODELAY (Nagle's algorithm)
- Keep-alive settings
- Connection timeouts
- Reconnection strategies
- Smart routing
"""

from hazelcast import HazelcastClient, ClientConfig
from hazelcast.config import (
    NetworkConfig,
    ConnectionStrategyConfig,
    RetryConfig,
    ReconnectMode,
)


def low_latency_config() -> ClientConfig:
    """Configuration optimized for low-latency applications.

    Best for:
    - Real-time trading systems
    - Gaming servers
    - Interactive applications
    """
    print("=== Low Latency Configuration ===")

    config = ClientConfig()
    config.cluster_name = "dev"

    # Network settings for minimal latency
    config.network = NetworkConfig(
        addresses=["localhost:5701", "localhost:5702"],
        connection_timeout=3.0,  # Fail fast on connection issues
        smart_routing=True,  # Route to data owners
        tcp_no_delay=True,  # Disable Nagle's algorithm
        socket_keep_alive=True,
        socket_send_buffer_size=64 * 1024,  # 64KB send buffer
        socket_receive_buffer_size=64 * 1024,  # 64KB receive buffer
        socket_linger_seconds=0,  # Don't wait on close
    )

    # Aggressive reconnection for minimal downtime
    config.connection_strategy = ConnectionStrategyConfig(
        async_start=False,
        reconnect_mode=ReconnectMode.ON,
        retry=RetryConfig(
            initial_backoff=0.1,  # 100ms initial retry
            max_backoff=2.0,  # Max 2s between retries
            multiplier=1.5,
            jitter=0.1,
        ),
    )

    print("  TCP_NODELAY: enabled (disable Nagle's algorithm)")
    print("  Buffer sizes: 64KB send/receive")
    print("  Linger: 0 (immediate close)")
    print("  Smart routing: enabled")

    return config


def high_throughput_config() -> ClientConfig:
    """Configuration optimized for high-throughput batch operations.

    Best for:
    - ETL pipelines
    - Batch processing
    - Data migration
    """
    print("\n=== High Throughput Configuration ===")

    config = ClientConfig()
    config.cluster_name = "dev"

    # Network settings for maximum throughput
    config.network = NetworkConfig(
        addresses=["localhost:5701", "localhost:5702", "localhost:5703"],
        connection_timeout=10.0,  # More tolerance for busy servers
        smart_routing=True,
        tcp_no_delay=False,  # Allow Nagle's algorithm for batching
        socket_keep_alive=True,
        socket_send_buffer_size=256 * 1024,  # 256KB send buffer
        socket_receive_buffer_size=256 * 1024,  # 256KB receive buffer
        socket_linger_seconds=5,  # Allow pending data to flush
    )

    # Patient reconnection for batch jobs
    config.connection_strategy = ConnectionStrategyConfig(
        async_start=False,
        reconnect_mode=ReconnectMode.ON,
        retry=RetryConfig(
            initial_backoff=1.0,
            max_backoff=30.0,
            multiplier=2.0,
            jitter=0.2,
        ),
    )

    print("  TCP_NODELAY: disabled (allow batching)")
    print("  Buffer sizes: 256KB send/receive")
    print("  Linger: 5 seconds")
    print("  Connection timeout: 10 seconds")

    return config


def resilient_config() -> ClientConfig:
    """Configuration optimized for resilience in unreliable networks.

    Best for:
    - Cloud deployments with network variability
    - Cross-datacenter connections
    - Unstable network environments
    """
    print("\n=== Resilient Network Configuration ===")

    config = ClientConfig()
    config.cluster_name = "dev"

    # Network settings for resilience
    config.network = NetworkConfig(
        addresses=[
            "node1.cluster.local:5701",
            "node2.cluster.local:5701",
            "node3.cluster.local:5701",
        ],
        connection_timeout=30.0,  # Long timeout for unreliable networks
        smart_routing=True,
        tcp_no_delay=True,
        socket_keep_alive=True,  # Detect dead connections
        socket_send_buffer_size=128 * 1024,
        socket_receive_buffer_size=128 * 1024,
        socket_linger_seconds=10,  # Allow graceful close
    )

    # Robust reconnection strategy
    config.connection_strategy = ConnectionStrategyConfig(
        async_start=True,  # Don't block on initial connection
        reconnect_mode=ReconnectMode.ASYNC,
        retry=RetryConfig(
            initial_backoff=0.5,
            max_backoff=60.0,  # Up to 1 minute between retries
            multiplier=2.0,
            jitter=0.3,  # 30% jitter to prevent thundering herd
        ),
    )

    print("  Async start: enabled")
    print("  Reconnect mode: ASYNC")
    print("  Max backoff: 60 seconds")
    print("  Jitter: 30%")

    return config


def dumb_client_config() -> ClientConfig:
    """Configuration for non-smart (dumb) client mode.

    Best for:
    - Simple deployments behind load balancer
    - When partition awareness is not needed
    - Testing and development
    """
    print("\n=== Dumb Client Configuration ===")

    config = ClientConfig()
    config.cluster_name = "dev"

    config.network = NetworkConfig(
        addresses=["hazelcast-lb.local:5701"],  # Single load balancer endpoint
        connection_timeout=5.0,
        smart_routing=False,  # All requests go through single connection
        tcp_no_delay=True,
        socket_keep_alive=True,
    )

    print("  Smart routing: disabled")
    print("  All requests routed through single connection")

    return config


def socket_options_reference():
    """Reference for socket options and their effects."""
    print("\n=== Socket Options Reference ===")

    options = [
        (
            "tcp_no_delay",
            "bool",
            "True",
            "Disables Nagle's algorithm. Set True for low latency, "
            "False for throughput optimization.",
        ),
        (
            "socket_keep_alive",
            "bool",
            "True",
            "Enables TCP keep-alive probes to detect dead connections. "
            "Always recommended.",
        ),
        (
            "socket_send_buffer_size",
            "int",
            "OS default",
            "TCP send buffer size in bytes. Larger values improve throughput "
            "for bulk transfers.",
        ),
        (
            "socket_receive_buffer_size",
            "int",
            "OS default",
            "TCP receive buffer size in bytes. Match with send buffer "
            "for symmetric performance.",
        ),
        (
            "socket_linger_seconds",
            "int",
            "None",
            "Time to wait for pending data on close. "
            "0 = immediate close, None = OS default.",
        ),
        (
            "connection_timeout",
            "float",
            "5.0",
            "Seconds to wait for connection establishment.",
        ),
        (
            "smart_routing",
            "bool",
            "True",
            "Route operations to partition owners for reduced latency.",
        ),
    ]

    print(f"\n  {'Option':<28} {'Type':<12} {'Default':<12}")
    print("  " + "-" * 80)
    for opt, typ, default, desc in options:
        print(f"  {opt:<28} {typ:<12} {default:<12}")
        # Wrap description
        words = desc.split()
        line = "    "
        for word in words:
            if len(line) + len(word) > 78:
                print(line)
                line = "    "
            line += word + " "
        if line.strip():
            print(line)


def retry_config_examples():
    """Examples of different retry configurations."""
    print("\n=== Retry Configuration Examples ===")

    # Aggressive retry for critical services
    aggressive = RetryConfig(
        initial_backoff=0.1,  # 100ms
        max_backoff=5.0,  # 5 seconds max
        multiplier=1.5,
        jitter=0.1,
    )
    print("\n  Aggressive retry (critical services):")
    print(f"    Initial: {aggressive.initial_backoff}s")
    print(f"    Max: {aggressive.max_backoff}s")
    print(f"    Multiplier: {aggressive.multiplier}x")

    # Conservative retry for batch jobs
    conservative = RetryConfig(
        initial_backoff=2.0,  # 2 seconds
        max_backoff=120.0,  # 2 minutes max
        multiplier=2.0,
        jitter=0.3,
    )
    print("\n  Conservative retry (batch jobs):")
    print(f"    Initial: {conservative.initial_backoff}s")
    print(f"    Max: {conservative.max_backoff}s")
    print(f"    Multiplier: {conservative.multiplier}x")

    # Calculate retry sequence
    print("\n  Retry sequence (aggressive):")
    backoff = aggressive.initial_backoff
    for i in range(6):
        print(f"    Attempt {i+1}: wait {backoff:.2f}s")
        backoff = min(backoff * aggressive.multiplier, aggressive.max_backoff)


def reconnect_mode_reference():
    """Reference for reconnection modes."""
    print("\n=== Reconnection Modes ===")

    modes = [
        (
            "OFF",
            "No automatic reconnection. Client fails permanently on disconnect.",
        ),
        (
            "ON",
            "Synchronous reconnection. Operations block until reconnected.",
        ),
        (
            "ASYNC",
            "Asynchronous reconnection. Operations may fail during reconnection.",
        ),
    ]

    for mode, desc in modes:
        print(f"\n  {mode}:")
        print(f"    {desc}")


def main():
    # Display configuration examples
    low_latency_config()
    high_throughput_config()
    resilient_config()
    dumb_client_config()

    # Reference information
    socket_options_reference()
    retry_config_examples()
    reconnect_mode_reference()

    # Demonstrate actual connection with low-latency config
    print("\n=== Connecting with Low-Latency Config ===")

    config = ClientConfig()
    config.cluster_name = "dev"
    config.network = NetworkConfig(
        addresses=["localhost:5701"],
        connection_timeout=5.0,
        smart_routing=True,
        tcp_no_delay=True,
        socket_keep_alive=True,
        socket_send_buffer_size=64 * 1024,
        socket_receive_buffer_size=64 * 1024,
    )

    client = HazelcastClient(config)

    try:
        client.start()
        print("  Connected successfully!")

        # Verify smart routing is active
        print(f"  Smart routing: {config.network.smart_routing}")
        print(f"  TCP_NODELAY: {config.network.tcp_no_delay}")
        print(f"  Keep-alive: {config.network.socket_keep_alive}")

        # Quick test operation
        test_map = client.get_map("network-test")
        test_map.put("test-key", "test-value")
        value = test_map.get("test-key")
        print(f"  Test operation successful: {value}")
        test_map.delete("test-key")

    finally:
        client.shutdown()
        print("\nClient shutdown complete.")


if __name__ == "__main__":
    main()
