Cloud Discovery
===============

Hazelcast supports automatic discovery of cluster members from various
cloud providers and orchestration platforms.

AWS Discovery
-------------

Discover Hazelcast members running on AWS EC2 or ECS:

.. code-block:: python

   from hazelcast import ClientConfig
   from hazelcast.config import DiscoveryConfig, DiscoveryStrategyType

   config = ClientConfig()
   config.discovery.enabled = True
   config.discovery.strategy_type = DiscoveryStrategyType.AWS
   config.discovery.aws = {
       "region": "us-west-2",
       "tag_key": "hazelcast-cluster",
       "tag_value": "production",
       "access_key": "AKIAIOSFODNN7EXAMPLE",  # Or use IAM role
       "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
   }

**AWS Configuration Options:**

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Option
     - Description
   * - ``region``
     - AWS region (e.g., "us-west-2")
   * - ``access_key``
     - AWS access key (optional if using IAM)
   * - ``secret_key``
     - AWS secret key (optional if using IAM)
   * - ``tag_key``
     - EC2 tag key to filter instances
   * - ``tag_value``
     - EC2 tag value to filter instances
   * - ``security_group_name``
     - Security group name to filter
   * - ``iam_role``
     - IAM role for authentication

Azure Discovery
---------------

Discover members running on Azure VMs or Scale Sets:

.. code-block:: python

   config = ClientConfig()
   config.discovery.enabled = True
   config.discovery.strategy_type = DiscoveryStrategyType.AZURE
   config.discovery.azure = {
       "subscription_id": "your-subscription-id",
       "resource_group": "your-resource-group",
       "scale_set": "your-scale-set",  # Optional
       "client_id": "your-client-id",
       "client_secret": "your-client-secret",
       "tenant_id": "your-tenant-id",
   }

**Azure Configuration Options:**

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Option
     - Description
   * - ``subscription_id``
     - Azure subscription ID
   * - ``resource_group``
     - Resource group containing VMs
   * - ``scale_set``
     - Virtual Machine Scale Set name (optional)
   * - ``client_id``
     - Service principal client ID
   * - ``client_secret``
     - Service principal client secret
   * - ``tenant_id``
     - Azure AD tenant ID
   * - ``tag``
     - Tag to filter VMs

GCP Discovery
-------------

Discover members running on Google Compute Engine:

.. code-block:: python

   config = ClientConfig()
   config.discovery.enabled = True
   config.discovery.strategy_type = DiscoveryStrategyType.GCP
   config.discovery.gcp = {
       "project": "your-project-id",
       "zone": "us-central1-a",  # Or "region" for regional
       "label_key": "hazelcast-cluster",
       "label_value": "production",
   }

**GCP Configuration Options:**

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Option
     - Description
   * - ``project``
     - GCP project ID
   * - ``zone``
     - Compute Engine zone (or use ``region``)
   * - ``region``
     - Compute Engine region
   * - ``label_key``
     - Instance label key to filter
   * - ``label_value``
     - Instance label value to filter
   * - ``service_account_key``
     - Path to service account JSON key

Kubernetes Discovery
--------------------

Discover members running in Kubernetes:

.. code-block:: python

   config = ClientConfig()
   config.discovery.enabled = True
   config.discovery.strategy_type = DiscoveryStrategyType.KUBERNETES
   config.discovery.kubernetes = {
       "namespace": "hazelcast",
       "service_name": "hazelcast-service",
       # Or use pod labels
       "pod_label_name": "app",
       "pod_label_value": "hazelcast",
   }

**Kubernetes Configuration Options:**

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Option
     - Description
   * - ``namespace``
     - Kubernetes namespace
   * - ``service_name``
     - Kubernetes service name
   * - ``pod_label_name``
     - Pod label key for filtering
   * - ``pod_label_value``
     - Pod label value for filtering
   * - ``service_dns``
     - DNS name of the service
   * - ``resolve_not_ready_addresses``
     - Include not-ready pods

Hazelcast Cloud (Viridian)
--------------------------

Connect to Hazelcast Cloud managed clusters:

.. code-block:: python

   config = ClientConfig()
   config.discovery.enabled = True
   config.discovery.strategy_type = DiscoveryStrategyType.CLOUD
   config.discovery.cloud = {
       "cluster_name": "my-cluster",
       "token": "your-discovery-token",
   }
   
   # TLS is typically required for cloud
   config.security.tls.enabled = True

**Cloud Configuration Options:**

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Option
     - Description
   * - ``cluster_name``
     - Name of your Viridian cluster
   * - ``token``
     - Discovery token from Viridian console
   * - ``url``
     - Custom coordinator URL (optional)

Multicast Discovery
-------------------

Discover members using multicast (local network):

.. code-block:: python

   config = ClientConfig()
   config.discovery.enabled = True
   config.discovery.strategy_type = DiscoveryStrategyType.MULTICAST
   config.discovery.multicast = {
       "group": "224.2.2.3",
       "port": 54327,
   }

**Multicast Configuration Options:**

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Option
     - Description
   * - ``group``
     - Multicast group address
   * - ``port``
     - Multicast port
   * - ``timeout_seconds``
     - Discovery timeout

YAML Configuration
------------------

Configure discovery in YAML:

.. code-block:: yaml

   hazelcast_client:
     discovery:
       enabled: true
       strategy_type: AWS
       aws:
         region: us-west-2
         tag_key: hazelcast-cluster
         tag_value: production

Or for Kubernetes:

.. code-block:: yaml

   hazelcast_client:
     discovery:
       enabled: true
       strategy_type: KUBERNETES
       kubernetes:
         namespace: hazelcast
         service_name: hazelcast

Best Practices
--------------

1. **Use IAM Roles/Service Accounts**
   
   Prefer IAM roles (AWS), managed identities (Azure), or service accounts
   (GCP/K8s) over explicit credentials.

2. **Filter Specifically**
   
   Use tags/labels to filter only your cluster's instances.

3. **Enable TLS for Cloud**
   
   Always enable TLS when connecting to Hazelcast Cloud.

4. **Handle Discovery Failures**
   
   Implement retry logic for transient discovery failures.

5. **Test Locally First**
   
   Test with explicit addresses before enabling cloud discovery.
