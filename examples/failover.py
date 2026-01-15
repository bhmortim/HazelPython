"""Failover Configuration Example.

This example demonstrates how to configure automatic failover across
multiple Hazelcast clusters for high availability.

Failover enables the client to automatically switch to a backup cluster
when the primary cluster becomes unavailable.

Prerequisites:
    - Multiple Hazelcast clusters (primary and backup)

Usage:
    python failover.py
"""

from hazelcast import HazelcastClient, ClientConfig
from hazelcast.failover import FailoverConfig, ClusterConfig, CNAMEResolver


def basic_failover_example():
    """Configure basic failover between two clusters."""
    print("\n" + "=" * 50)
    print("Basic Failover Configuration")
    print("=" * 50)

    # Create failover configuration
    failover = FailoverConfig(try_count=3)

    # Add primary cluster (highest priority)
    failover.add_cluster(
        cluster_name="production-primary",
        addresses=["primary-node1:5701", "primary-node2:5701"],
        priority=0,  # Lower number = higher priority
    )

    # Add backup cluster
    failover.add_cluster(
        cluster_name="production-backup",
        addresses=["backup-node1:5701", "backup-node2:5701"],
        priority=1,
    )

    print(f"Failover configuration:")
    print(f"  Try count: {failover.try_count}")
    print(f"  Clusters: {failover.cluster_count}")

    for cluster in failover.clusters:
        print(f"    - {cluster.cluster_name} (priority={cluster.priority})")
        print(f"      Addresses: {cluster.addresses}")

    return failover


def cname_discovery_example():
    """Configure CNAME-based dynamic discovery."""
    print("\n" + "=" * 50)
    print("CNAME-Based Discovery")
    print("=" * 50)

    failover = FailoverConfig(try_count=5)

    # Add cluster with static addresses
    failover.add_cluster(
        cluster_name="static-cluster",
        addresses=["node1.example.com:5701"],
        priority=0,
    )

    # Add cluster with CNAME-based discovery
    # DNS will be resolved periodically to discover cluster members
    failover.add_cname_cluster(
        cluster_name="dynamic-cluster",
        dns_name="hazelcast.internal.example.com",
        port=5701,
        priority=1,
        refresh_interval=60.0,  # Re-resolve DNS every 60 seconds
    )

    print("Clusters configured:")
    for cluster in failover.clusters:
        addresses = failover.get_cluster_addresses(cluster.cluster_name)
        print(f"  {cluster.cluster_name}:")
        print(f"    Priority: {cluster.priority}")
        print(f"    Addresses: {addresses or 'dynamic (CNAME)'}")

    return failover


def cname_resolver_example():
    """Demonstrate the CNAMEResolver directly."""
    print("\n" + "=" * 50)
    print("CNAME Resolver Demo")
    print("=" * 50)

    # Create a CNAME resolver
    resolver = CNAMEResolver(
        dns_name="localhost",  # Using localhost for demo
        port=5701,
        refresh_interval=30.0,
    )

    print(f"CNAME Resolver:")
    print(f"  DNS name: {resolver.dns_name}")
    print(f"  Port: {resolver.port}")

    # Resolve addresses
    addresses = resolver.resolve()
    print(f"  Resolved addresses: {addresses}")

    # Force refresh
    addresses = resolver.refresh()
    print(f"  After refresh: {addresses}")


def failover_state_example():
    """Demonstrate failover state management."""
    print("\n" + "=" * 50)
    print("Failover State Management")
    print("=" * 50)

    failover = FailoverConfig(try_count=3)
    failover.add_cluster("cluster-a", ["node-a:5701"], priority=0)
    failover.add_cluster("cluster-b", ["node-b:5701"], priority=1)
    failover.add_cluster("cluster-c", ["node-c:5701"], priority=2)

    print(f"Initial cluster index: {failover.current_cluster_index}")

    current = failover.get_current_cluster()
    print(f"Current cluster: {current.cluster_name if current else 'None'}")

    # Simulate failover
    print("\nSimulating failover sequence:")

    for i in range(4):
        next_cluster = failover.switch_to_next_cluster()
        print(f"  Switch {i + 1}: Now on '{next_cluster.cluster_name}' "
              f"(index={failover.current_cluster_index})")

    # Reset to primary
    print("\nResetting to primary cluster...")
    failover.reset()
    current = failover.get_current_cluster()
    print(f"Current cluster after reset: {current.cluster_name}")


def convert_to_client_config_example():
    """Show how to convert FailoverConfig to ClientConfig."""
    print("\n" + "=" * 50)
    print("Convert to ClientConfig")
    print("=" * 50)

    # Setup failover
    failover = FailoverConfig(try_count=3)
    failover.add_cluster(
        cluster_name="production",
        addresses=["prod-node1:5701", "prod-node2:5701"],
        priority=0,
    )
    failover.add_cluster(
        cluster_name="disaster-recovery",
        addresses=["dr-node1:5701"],
        priority=1,
    )

    # Convert to ClientConfig for a specific cluster
    print("Creating ClientConfig for primary cluster:")
    primary_config = failover.to_client_config(cluster_index=0)
    print(f"  Cluster name: {primary_config.cluster_name}")
    print(f"  Addresses: {primary_config.cluster_members}")

    print("\nCreating ClientConfig for DR cluster:")
    dr_config = failover.to_client_config(cluster_index=1)
    print(f"  Cluster name: {dr_config.cluster_name}")
    print(f"  Addresses: {dr_config.cluster_members}")


def from_configs_example():
    """Create FailoverConfig from multiple ClientConfigs."""
    print("\n" + "=" * 50)
    print("Create from Multiple ClientConfigs")
    print("=" * 50)

    # Create individual client configs
    config1 = ClientConfig()
    config1.cluster_name = "east-coast"
    config1.cluster_members = ["east-node1:5701", "east-node2:5701"]

    config2 = ClientConfig()
    config2.cluster_name = "west-coast"
    config2.cluster_members = ["west-node1:5701"]

    config3 = ClientConfig()
    config3.cluster_name = "europe"
    config3.cluster_members = ["eu-node1:5701"]

    # Create failover from configs
    failover = FailoverConfig.from_configs([config1, config2, config3])

    print("Created failover from 3 ClientConfigs:")
    for cluster in failover.clusters:
        addresses = failover.get_cluster_addresses(cluster.cluster_name)
        print(f"  {cluster.cluster_name} (priority={cluster.priority})")
        print(f"    Addresses: {addresses}")


def failover_with_client_example():
    """Show how failover would be used with HazelcastClient."""
    print("\n" + "=" * 50)
    print("Using Failover with HazelcastClient")
    print("=" * 50)

    print("""
In production, you would use failover like this:

    from hazelcast import HazelcastClient
    from hazelcast.failover import FailoverConfig

    # Configure failover
    failover = FailoverConfig(try_count=3)
    failover.add_cluster("primary", ["primary:5701"], priority=0)
    failover.add_cluster("backup", ["backup:5701"], priority=1)

    # Connect with automatic failover handling
    current_index = failover.current_cluster_index
    connected = False

    while not connected and current_index < failover.cluster_count:
        try:
            config = failover.to_client_config(current_index)
            client = HazelcastClient(config)
            client.start()
            connected = True
            print(f"Connected to {config.cluster_name}")
        except Exception as e:
            print(f"Failed to connect to cluster {current_index}: {e}")
            failover.switch_to_next_cluster()
            current_index = failover.current_cluster_index

    if not connected:
        raise Exception("Could not connect to any cluster")

    # Use the client...
    my_map = client.get_map("my-map")

    # On disconnect, the client could automatically switch
    # to the next cluster in the failover configuration

    client.shutdown()
    """)


def multi_region_example():
    """Configure failover for multi-region deployment."""
    print("\n" + "=" * 50)
    print("Multi-Region Failover Configuration")
    print("=" * 50)

    failover = FailoverConfig(try_count=5)

    # Primary region: US East
    failover.add_cluster(
        cluster_name="us-east-production",
        addresses=[
            "us-east-1a.hazelcast.internal:5701",
            "us-east-1b.hazelcast.internal:5701",
            "us-east-1c.hazelcast.internal:5701",
        ],
        priority=0,
    )

    # Secondary region: US West
    failover.add_cluster(
        cluster_name="us-west-production",
        addresses=[
            "us-west-2a.hazelcast.internal:5701",
            "us-west-2b.hazelcast.internal:5701",
        ],
        priority=1,
    )

    # DR region: Europe
    failover.add_cname_cluster(
        cluster_name="eu-disaster-recovery",
        dns_name="hazelcast-dr.eu.internal",
        port=5701,
        priority=2,
        refresh_interval=120.0,
    )

    print("Multi-region failover configuration:")
    print(f"  Total clusters: {failover.cluster_count}")
    print(f"  Try count per cluster: {failover.try_count}")
    print()

    for cluster in failover.clusters:
        addresses = failover.get_cluster_addresses(cluster.cluster_name)
        print(f"  Region: {cluster.cluster_name}")
        print(f"    Priority: {cluster.priority}")
        if addresses:
            print(f"    Nodes: {len(addresses)}")
            for addr in addresses[:2]:  # Show first 2
                print(f"      - {addr}")
            if len(addresses) > 2:
                print(f"      ... and {len(addresses) - 2} more")
        else:
            print(f"    Discovery: CNAME-based")
        print()


def main():
    print("Hazelcast Failover Configuration Examples")
    print("=" * 50)
    print("""
Failover enables automatic connection to backup clusters when
the primary cluster is unavailable. This provides high availability
across data centers or cloud regions.

Key concepts:
- Clusters are tried in priority order (lower number = higher priority)
- try_count specifies attempts per cluster before moving to next
- CNAME-based discovery enables dynamic cluster membership
    """)

    # Run examples
    basic_failover_example()
    cname_discovery_example()
    cname_resolver_example()
    failover_state_example()
    convert_to_client_config_example()
    from_configs_example()
    failover_with_client_example()
    multi_region_example()

    print("\n" + "=" * 50)
    print("All failover examples completed!")
    print("=" * 50)


if __name__ == "__main__":
    main()
