"""AWS discovery strategy for EC2 and ECS."""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

from hazelcast.discovery.base import DiscoveryStrategy, DiscoveryNode, DiscoveryException


@dataclass
class AwsConfig:
    """Configuration for AWS discovery.

    Attributes:
        access_key: AWS access key ID (optional if using IAM role).
        secret_key: AWS secret access key (optional if using IAM role).
        region: AWS region name.
        security_group_name: Filter by security group name.
        tag_key: Filter by tag key.
        tag_value: Filter by tag value.
        iam_role: IAM role to assume (optional).
        host_header: Custom host header for API calls.
        connection_timeout_seconds: Timeout for AWS API calls.
        read_timeout_seconds: Read timeout for AWS API responses.
        connection_retries: Number of connection retry attempts.
        hz_port: Hazelcast port on discovered instances.
        use_public_ip: Whether to use public IP addresses.
        ecs_cluster: ECS cluster name for ECS discovery.
        ecs_service: ECS service name for filtering.
        use_ecs: Whether to use ECS discovery instead of EC2.
    """

    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    region: str = "us-east-1"
    security_group_name: Optional[str] = None
    tag_key: Optional[str] = None
    tag_value: Optional[str] = None
    iam_role: Optional[str] = None
    host_header: Optional[str] = None
    connection_timeout_seconds: int = 10
    read_timeout_seconds: int = 10
    connection_retries: int = 3
    hz_port: int = 5701
    use_public_ip: bool = False
    ecs_cluster: Optional[str] = None
    ecs_service: Optional[str] = None
    use_ecs: bool = False
    filters: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> "AwsConfig":
        """Create AwsConfig from a dictionary."""
        return cls(
            access_key=data.get("access_key"),
            secret_key=data.get("secret_key"),
            region=data.get("region", "us-east-1"),
            security_group_name=data.get("security_group_name"),
            tag_key=data.get("tag_key"),
            tag_value=data.get("tag_value"),
            iam_role=data.get("iam_role"),
            host_header=data.get("host_header"),
            connection_timeout_seconds=data.get("connection_timeout_seconds", 10),
            read_timeout_seconds=data.get("read_timeout_seconds", 10),
            connection_retries=data.get("connection_retries", 3),
            hz_port=data.get("hz_port", 5701),
            use_public_ip=data.get("use_public_ip", False),
            ecs_cluster=data.get("ecs_cluster"),
            ecs_service=data.get("ecs_service"),
            use_ecs=data.get("use_ecs", False),
            filters=data.get("filters", {}),
        )


class AwsDiscoveryStrategy(DiscoveryStrategy):
    """AWS discovery strategy for EC2 and ECS instances.

    Discovers Hazelcast cluster members running on AWS EC2 instances
    or ECS tasks. Supports filtering by security groups, tags, and
    other EC2/ECS attributes.

    Example:
        EC2 discovery with tags::

            config = AwsConfig(
                region="us-west-2",
                tag_key="hazelcast-cluster",
                tag_value="production",
            )
            strategy = AwsDiscoveryStrategy(config)
            nodes = strategy.discover_nodes()

        ECS discovery::

            config = AwsConfig(
                region="us-west-2",
                use_ecs=True,
                ecs_cluster="my-cluster",
                ecs_service="hazelcast-service",
            )
            strategy = AwsDiscoveryStrategy(config)
            nodes = strategy.discover_nodes()
    """

    def __init__(self, config: Optional[AwsConfig] = None):
        """Initialize AWS discovery strategy.

        Args:
            config: AWS configuration options.
        """
        super().__init__()
        self._config = config or AwsConfig()
        self._ec2_client = None
        self._ecs_client = None

    @property
    def config(self) -> AwsConfig:
        """Get the AWS configuration."""
        return self._config

    def start(self) -> None:
        """Start the AWS discovery strategy and initialize boto3 clients."""
        super().start()
        self._initialize_clients()

    def stop(self) -> None:
        """Stop the AWS discovery strategy."""
        super().stop()
        self._ec2_client = None
        self._ecs_client = None

    def _initialize_clients(self) -> None:
        """Initialize boto3 clients."""
        try:
            import boto3
        except ImportError:
            raise DiscoveryException(
                "boto3 is required for AWS discovery. "
                "Install it with: pip install boto3"
            )

        session_kwargs = {"region_name": self._config.region}

        if self._config.access_key and self._config.secret_key:
            session_kwargs["aws_access_key_id"] = self._config.access_key
            session_kwargs["aws_secret_access_key"] = self._config.secret_key

        try:
            session = boto3.Session(**session_kwargs)

            if self._config.iam_role:
                sts = session.client("sts")
                assumed_role = sts.assume_role(
                    RoleArn=self._config.iam_role,
                    RoleSessionName="HazelcastDiscovery",
                )
                credentials = assumed_role["Credentials"]
                session = boto3.Session(
                    aws_access_key_id=credentials["AccessKeyId"],
                    aws_secret_access_key=credentials["SecretAccessKey"],
                    aws_session_token=credentials["SessionToken"],
                    region_name=self._config.region,
                )

            self._ec2_client = session.client("ec2")
            if self._config.use_ecs:
                self._ecs_client = session.client("ecs")

        except Exception as e:
            raise DiscoveryException(f"Failed to initialize AWS clients: {e}")

    def discover_nodes(self) -> List[DiscoveryNode]:
        """Discover Hazelcast nodes on AWS.

        Returns:
            List of discovered nodes.

        Raises:
            DiscoveryException: If discovery fails.
        """
        if not self._started:
            self.start()

        if self._config.use_ecs:
            return self._discover_ecs_nodes()
        return self._discover_ec2_nodes()

    def _discover_ec2_nodes(self) -> List[DiscoveryNode]:
        """Discover EC2 instances."""
        filters = [{"Name": "instance-state-name", "Values": ["running"]}]

        if self._config.security_group_name:
            filters.append({
                "Name": "group-name",
                "Values": [self._config.security_group_name],
            })

        if self._config.tag_key and self._config.tag_value:
            filters.append({
                "Name": f"tag:{self._config.tag_key}",
                "Values": [self._config.tag_value],
            })

        for key, value in self._config.filters.items():
            filters.append({"Name": key, "Values": [value]})

        try:
            response = self._ec2_client.describe_instances(Filters=filters)
        except Exception as e:
            raise DiscoveryException(f"Failed to describe EC2 instances: {e}")

        nodes = []
        for reservation in response.get("Reservations", []):
            for instance in reservation.get("Instances", []):
                node = self._instance_to_node(instance)
                if node:
                    nodes.append(node)

        return nodes

    def _instance_to_node(self, instance: dict) -> Optional[DiscoveryNode]:
        """Convert an EC2 instance to a DiscoveryNode."""
        private_ip = instance.get("PrivateIpAddress")
        if not private_ip:
            return None

        public_ip = instance.get("PublicIpAddress")

        tags = {tag["Key"]: tag["Value"] for tag in instance.get("Tags", [])}

        return DiscoveryNode(
            private_address=private_ip,
            public_address=public_ip,
            port=self._config.hz_port,
            properties={
                "instance_id": instance.get("InstanceId"),
                "instance_type": instance.get("InstanceType"),
                "availability_zone": instance.get("Placement", {}).get(
                    "AvailabilityZone"
                ),
                "tags": tags,
            },
        )

    def _discover_ecs_nodes(self) -> List[DiscoveryNode]:
        """Discover ECS tasks."""
        if not self._config.ecs_cluster:
            raise DiscoveryException("ecs_cluster is required for ECS discovery")

        try:
            list_kwargs = {"cluster": self._config.ecs_cluster}
            if self._config.ecs_service:
                list_kwargs["serviceName"] = self._config.ecs_service

            task_arns = []
            paginator = self._ecs_client.get_paginator("list_tasks")
            for page in paginator.paginate(**list_kwargs):
                task_arns.extend(page.get("taskArns", []))

            if not task_arns:
                return []

            response = self._ecs_client.describe_tasks(
                cluster=self._config.ecs_cluster,
                tasks=task_arns,
            )

        except Exception as e:
            raise DiscoveryException(f"Failed to describe ECS tasks: {e}")

        nodes = []
        for task in response.get("tasks", []):
            node = self._task_to_node(task)
            if node:
                nodes.append(node)

        return nodes

    def _task_to_node(self, task: dict) -> Optional[DiscoveryNode]:
        """Convert an ECS task to a DiscoveryNode."""
        for attachment in task.get("attachments", []):
            if attachment.get("type") == "ElasticNetworkInterface":
                details = {
                    d["name"]: d["value"] for d in attachment.get("details", [])
                }
                private_ip = details.get("privateIPv4Address")
                if private_ip:
                    return DiscoveryNode(
                        private_address=private_ip,
                        port=self._config.hz_port,
                        properties={
                            "task_arn": task.get("taskArn"),
                            "task_definition_arn": task.get("taskDefinitionArn"),
                            "container_instance_arn": task.get("containerInstanceArn"),
                        },
                    )

        for container in task.get("containers", []):
            for network in container.get("networkInterfaces", []):
                private_ip = network.get("privateIpv4Address")
                if private_ip:
                    return DiscoveryNode(
                        private_address=private_ip,
                        port=self._config.hz_port,
                        properties={
                            "task_arn": task.get("taskArn"),
                            "container_name": container.get("name"),
                        },
                    )

        return None
