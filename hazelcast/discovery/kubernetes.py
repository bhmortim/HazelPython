"""Kubernetes discovery strategy for pods and services."""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any

from hazelcast.discovery.base import DiscoveryStrategy, DiscoveryNode, DiscoveryException


@dataclass
class KubernetesConfig:
    """Configuration for Kubernetes discovery.

    Attributes:
        namespace: Kubernetes namespace to search (default: current namespace).
        service_name: Service name for discovery via endpoints.
        service_label_name: Label key for service-based discovery.
        service_label_value: Label value for service-based discovery.
        pod_label_name: Label key for pod-based discovery.
        pod_label_value: Label value for pod-based discovery.
        service_dns: Service DNS name for DNS-based discovery.
        service_dns_timeout: Timeout for DNS resolution in seconds.
        hz_port: Hazelcast port on discovered pods.
        use_node_name_as_external_address: Use node name as external address.
        kubeconfig_path: Path to kubeconfig file (optional).
        context: Kubernetes context to use (optional).
        api_token: API bearer token for authentication.
        ca_certificate: Path to CA certificate.
        in_cluster: Whether running inside a Kubernetes cluster.
    """

    namespace: Optional[str] = None
    service_name: Optional[str] = None
    service_label_name: Optional[str] = None
    service_label_value: Optional[str] = None
    pod_label_name: Optional[str] = None
    pod_label_value: Optional[str] = None
    service_dns: Optional[str] = None
    service_dns_timeout: float = 5.0
    hz_port: int = 5701
    use_node_name_as_external_address: bool = False
    kubeconfig_path: Optional[str] = None
    context: Optional[str] = None
    api_token: Optional[str] = None
    ca_certificate: Optional[str] = None
    in_cluster: bool = True

    @classmethod
    def from_dict(cls, data: dict) -> "KubernetesConfig":
        """Create KubernetesConfig from a dictionary."""
        return cls(
            namespace=data.get("namespace"),
            service_name=data.get("service_name"),
            service_label_name=data.get("service_label_name"),
            service_label_value=data.get("service_label_value"),
            pod_label_name=data.get("pod_label_name"),
            pod_label_value=data.get("pod_label_value"),
            service_dns=data.get("service_dns"),
            service_dns_timeout=data.get("service_dns_timeout", 5.0),
            hz_port=data.get("hz_port", 5701),
            use_node_name_as_external_address=data.get(
                "use_node_name_as_external_address", False
            ),
            kubeconfig_path=data.get("kubeconfig_path"),
            context=data.get("context"),
            api_token=data.get("api_token"),
            ca_certificate=data.get("ca_certificate"),
            in_cluster=data.get("in_cluster", True),
        )


class KubernetesDiscoveryStrategy(DiscoveryStrategy):
    """Kubernetes discovery strategy for pods and services.

    Discovers Hazelcast cluster members running as Kubernetes pods.
    Supports multiple discovery modes:
    - Service endpoints discovery
    - Pod label-based discovery
    - DNS-based discovery

    Example:
        Service-based discovery::

            config = KubernetesConfig(
                namespace="hazelcast",
                service_name="hazelcast-service",
            )
            strategy = KubernetesDiscoveryStrategy(config)
            nodes = strategy.discover_nodes()

        Pod label discovery::

            config = KubernetesConfig(
                namespace="hazelcast",
                pod_label_name="app",
                pod_label_value="hazelcast",
            )
            strategy = KubernetesDiscoveryStrategy(config)
            nodes = strategy.discover_nodes()

        DNS-based discovery::

            config = KubernetesConfig(
                service_dns="hazelcast.hazelcast.svc.cluster.local",
            )
            strategy = KubernetesDiscoveryStrategy(config)
            nodes = strategy.discover_nodes()
    """

    def __init__(self, config: Optional[KubernetesConfig] = None):
        """Initialize Kubernetes discovery strategy.

        Args:
            config: Kubernetes configuration options.
        """
        super().__init__()
        self._config = config or KubernetesConfig()
        self._v1 = None
        self._namespace = None

    @property
    def config(self) -> KubernetesConfig:
        """Get the Kubernetes configuration."""
        return self._config

    def start(self) -> None:
        """Start the Kubernetes discovery strategy and initialize clients."""
        super().start()
        self._initialize_client()

    def stop(self) -> None:
        """Stop the Kubernetes discovery strategy."""
        super().stop()
        self._v1 = None

    def _initialize_client(self) -> None:
        """Initialize the Kubernetes client."""
        try:
            from kubernetes import client, config
        except ImportError:
            raise DiscoveryException(
                "kubernetes is required for Kubernetes discovery. "
                "Install with: pip install kubernetes"
            )

        try:
            if self._config.in_cluster:
                try:
                    config.load_incluster_config()
                except config.ConfigException:
                    if self._config.kubeconfig_path:
                        config.load_kube_config(
                            config_file=self._config.kubeconfig_path,
                            context=self._config.context,
                        )
                    else:
                        config.load_kube_config(context=self._config.context)
            else:
                if self._config.kubeconfig_path:
                    config.load_kube_config(
                        config_file=self._config.kubeconfig_path,
                        context=self._config.context,
                    )
                else:
                    config.load_kube_config(context=self._config.context)

            self._v1 = client.CoreV1Api()

            self._namespace = self._config.namespace or self._get_current_namespace()

        except Exception as e:
            raise DiscoveryException(f"Failed to initialize Kubernetes client: {e}")

    def _get_current_namespace(self) -> str:
        """Get the current namespace from the service account."""
        try:
            with open(
                "/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r"
            ) as f:
                return f.read().strip()
        except Exception:
            return "default"

    def discover_nodes(self) -> List[DiscoveryNode]:
        """Discover Hazelcast nodes on Kubernetes.

        Returns:
            List of discovered nodes.

        Raises:
            DiscoveryException: If discovery fails.
        """
        if not self._started:
            self.start()

        if self._config.service_dns:
            return self._discover_dns_nodes()
        elif self._config.service_name:
            return self._discover_service_endpoints()
        elif self._config.pod_label_name:
            return self._discover_pods_by_label()
        elif self._config.service_label_name:
            return self._discover_service_by_label()
        else:
            raise DiscoveryException(
                "No discovery method configured. Set service_name, "
                "pod_label_name, service_label_name, or service_dns."
            )

    def _discover_service_endpoints(self) -> List[DiscoveryNode]:
        """Discover pods via service endpoints."""
        try:
            endpoints = self._v1.read_namespaced_endpoints(
                name=self._config.service_name,
                namespace=self._namespace,
            )
        except Exception as e:
            raise DiscoveryException(f"Failed to get service endpoints: {e}")

        nodes = []
        for subset in endpoints.subsets or []:
            port = self._config.hz_port
            if subset.ports:
                for p in subset.ports:
                    if p.name == "hazelcast" or p.port == self._config.hz_port:
                        port = p.port
                        break

            for address in subset.addresses or []:
                node = DiscoveryNode(
                    private_address=address.ip,
                    port=port,
                    properties={
                        "pod_name": address.target_ref.name
                        if address.target_ref
                        else None,
                        "node_name": address.node_name,
                    },
                )
                nodes.append(node)

        return nodes

    def _discover_pods_by_label(self) -> List[DiscoveryNode]:
        """Discover pods by label selector."""
        label_selector = f"{self._config.pod_label_name}={self._config.pod_label_value}"

        try:
            pods = self._v1.list_namespaced_pod(
                namespace=self._namespace,
                label_selector=label_selector,
            )
        except Exception as e:
            raise DiscoveryException(f"Failed to list pods: {e}")

        nodes = []
        for pod in pods.items:
            if pod.status.phase != "Running":
                continue

            pod_ip = pod.status.pod_ip
            if not pod_ip:
                continue

            node = DiscoveryNode(
                private_address=pod_ip,
                port=self._config.hz_port,
                properties={
                    "pod_name": pod.metadata.name,
                    "pod_uid": pod.metadata.uid,
                    "node_name": pod.spec.node_name,
                    "labels": dict(pod.metadata.labels or {}),
                },
            )
            nodes.append(node)

        return nodes

    def _discover_service_by_label(self) -> List[DiscoveryNode]:
        """Discover services by label and get their endpoints."""
        label_selector = (
            f"{self._config.service_label_name}={self._config.service_label_value}"
        )

        try:
            services = self._v1.list_namespaced_service(
                namespace=self._namespace,
                label_selector=label_selector,
            )
        except Exception as e:
            raise DiscoveryException(f"Failed to list services: {e}")

        all_nodes = []
        for service in services.items:
            try:
                endpoints = self._v1.read_namespaced_endpoints(
                    name=service.metadata.name,
                    namespace=self._namespace,
                )

                for subset in endpoints.subsets or []:
                    for address in subset.addresses or []:
                        node = DiscoveryNode(
                            private_address=address.ip,
                            port=self._config.hz_port,
                            properties={
                                "service_name": service.metadata.name,
                                "pod_name": address.target_ref.name
                                if address.target_ref
                                else None,
                            },
                        )
                        all_nodes.append(node)

            except Exception:
                continue

        return all_nodes

    def _discover_dns_nodes(self) -> List[DiscoveryNode]:
        """Discover nodes via DNS SRV or A records."""
        import socket

        try:
            try:
                import dns.resolver

                answers = dns.resolver.resolve(
                    self._config.service_dns, "SRV", lifetime=self._config.service_dns_timeout
                )
                nodes = []
                for rdata in answers:
                    target = str(rdata.target).rstrip(".")
                    try:
                        ip_answers = dns.resolver.resolve(
                            target, "A", lifetime=self._config.service_dns_timeout
                        )
                        for ip_rdata in ip_answers:
                            nodes.append(
                                DiscoveryNode(
                                    private_address=str(ip_rdata),
                                    port=rdata.port,
                                    properties={"dns_target": target},
                                )
                            )
                    except Exception:
                        continue
                return nodes

            except ImportError:
                pass

            socket.setdefaulttimeout(self._config.service_dns_timeout)
            _, _, ips = socket.gethostbyname_ex(self._config.service_dns)

            return [
                DiscoveryNode(
                    private_address=ip,
                    port=self._config.hz_port,
                    properties={"dns_name": self._config.service_dns},
                )
                for ip in ips
            ]

        except socket.gaierror as e:
            raise DiscoveryException(f"DNS resolution failed: {e}")
        except Exception as e:
            raise DiscoveryException(f"DNS discovery failed: {e}")
