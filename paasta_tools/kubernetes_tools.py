# Copyright 2015-2018 Yelp Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import base64
import hashlib
import itertools
import json
import logging
import math
import os
import re
from datetime import datetime
from enum import Enum
from inspect import currentframe
from pathlib import Path
from typing import Any
from typing import Collection
from typing import Dict
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple
from typing import Union

import a_sync
import requests
import service_configuration_lib
from humanfriendly import parse_size
from kubernetes import client as kube_client
from kubernetes import config as kube_config
from kubernetes.client import models
from kubernetes.client import V1Affinity
from kubernetes.client import V1AWSElasticBlockStoreVolumeSource
from kubernetes.client import V1beta1CustomResourceDefinition
from kubernetes.client import V1beta1PodDisruptionBudget
from kubernetes.client import V1beta1PodDisruptionBudgetSpec
from kubernetes.client import V1Capabilities
from kubernetes.client import V1ConfigMap
from kubernetes.client import V1Container
from kubernetes.client import V1ContainerPort
from kubernetes.client import V1ContainerStatus
from kubernetes.client import V1ControllerRevision
from kubernetes.client import V1CustomResourceDefinitionList
from kubernetes.client import V1DeleteOptions
from kubernetes.client import V1Deployment
from kubernetes.client import V1DeploymentSpec
from kubernetes.client import V1DeploymentStrategy
from kubernetes.client import V1EnvVar
from kubernetes.client import V1EnvVarSource
from kubernetes.client import V1Event
from kubernetes.client import V1ExecAction
from kubernetes.client import V1Handler
from kubernetes.client import V1HostPathVolumeSource
from kubernetes.client import V1HTTPGetAction
from kubernetes.client import V1KeyToPath
from kubernetes.client import V1LabelSelector
from kubernetes.client import V1Lifecycle
from kubernetes.client import V1Namespace
from kubernetes.client import V1Node
from kubernetes.client import V1NodeAffinity
from kubernetes.client import V1NodeSelector
from kubernetes.client import V1NodeSelectorRequirement
from kubernetes.client import V1NodeSelectorTerm
from kubernetes.client import V1ObjectFieldSelector
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1PersistentVolumeClaim
from kubernetes.client import V1PersistentVolumeClaimSpec
from kubernetes.client import V1Pod
from kubernetes.client import V1PodAffinityTerm
from kubernetes.client import V1PodAntiAffinity
from kubernetes.client import V1PodCondition
from kubernetes.client import V1PodSecurityContext
from kubernetes.client import V1PodSpec
from kubernetes.client import V1PodTemplateSpec
from kubernetes.client import V1Probe
from kubernetes.client import V1ReplicaSet
from kubernetes.client import V1ResourceRequirements
from kubernetes.client import V1RoleBinding
from kubernetes.client import V1RoleRef
from kubernetes.client import V1RollingUpdateDeployment
from kubernetes.client import V1Secret
from kubernetes.client import V1SecretKeySelector
from kubernetes.client import V1SecretVolumeSource
from kubernetes.client import V1SecurityContext
from kubernetes.client import V1ServiceAccount
from kubernetes.client import V1StatefulSet
from kubernetes.client import V1StatefulSetSpec
from kubernetes.client import V1Subject
from kubernetes.client import V1TCPSocketAction
from kubernetes.client import V1Volume
from kubernetes.client import V1VolumeMount
from kubernetes.client import V2beta2CrossVersionObjectReference
from kubernetes.client import V2beta2HorizontalPodAutoscaler
from kubernetes.client import V2beta2HorizontalPodAutoscalerCondition
from kubernetes.client import V2beta2HorizontalPodAutoscalerSpec
from kubernetes.client import V2beta2MetricIdentifier
from kubernetes.client import V2beta2MetricSpec
from kubernetes.client import V2beta2MetricTarget
from kubernetes.client import V2beta2ObjectMetricSource
from kubernetes.client import V2beta2ResourceMetricSource
from kubernetes.client.models import V2beta2HorizontalPodAutoscalerStatus
from kubernetes.client.rest import ApiException
from mypy_extensions import TypedDict
from service_configuration_lib import read_soa_metadata

from paasta_tools import __version__
from paasta_tools.async_utils import async_timeout
from paasta_tools.long_running_service_tools import AutoscalingParamsDict
from paasta_tools.long_running_service_tools import host_passes_blacklist
from paasta_tools.long_running_service_tools import host_passes_whitelist
from paasta_tools.long_running_service_tools import InvalidHealthcheckMode
from paasta_tools.long_running_service_tools import load_service_namespace_config
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfigDict
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.secret_providers import BaseSecretProvider
from paasta_tools.secret_tools import get_secret_name_from_ref
from paasta_tools.secret_tools import is_secret_ref
from paasta_tools.secret_tools import is_shared_secret
from paasta_tools.secret_tools import SHARED_SECRET_SERVICE
from paasta_tools.utils import AwsEbsVolume
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import CAPS_DROP
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DeployBlacklist
from paasta_tools.utils import DeployWhitelist
from paasta_tools.utils import DockerVolume
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import get_git_sha_from_dockerurl
from paasta_tools.utils import load_service_instance_config
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import PaastaColors
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import PersistentVolume
from paasta_tools.utils import SecretVolume
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import time_cache
from paasta_tools.utils import VolumeWithMode


log = logging.getLogger(__name__)

KUBE_CONFIG_PATH = "/etc/kubernetes/admin.conf"
YELP_ATTRIBUTE_PREFIX = "yelp.com/"
PAASTA_ATTRIBUTE_PREFIX = "paasta.yelp.com/"
KUBE_DEPLOY_STATEGY_MAP = {
    "crossover": "RollingUpdate",
    "downthenup": "Recreate",
    "brutal": "RollingUpdate",
}
HACHECK_POD_NAME = "hacheck"
UWSGI_EXPORTER_POD_NAME = "uwsgi--exporter"
SIDECAR_CONTAINER_NAMES = [HACHECK_POD_NAME, UWSGI_EXPORTER_POD_NAME]
KUBERNETES_NAMESPACE = "paasta"
MAX_EVENTS_TO_RETRIEVE = 200
DISCOVERY_ATTRIBUTES = {
    "region",
    "superregion",
    "ecosystem",
    "habitat",
    "pool",
    "hostname",
}

GPU_RESOURCE_NAME = "nvidia.com/gpu"
DEFAULT_STORAGE_CLASS_NAME = "ebs"
DEFAULT_PRESTOP_SLEEP_SECONDS = 30
DEFAULT_HADOWN_PRESTOP_SLEEP_SECONDS = DEFAULT_PRESTOP_SLEEP_SECONDS + 1


DEFAULT_USE_PROMETHEUS_CPU = False
DEFAULT_USE_PROMETHEUS_UWSGI = True
DEFAULT_USE_RESOURCE_METRICS_CPU = True


# conditions is None when creating a new HPA, but the client raises an error in that case.
# For detail, https://github.com/kubernetes-client/python/issues/553
# This hack should be removed when the issue got fixed.
# This is no better way to work around rn.
class MonkeyPatchAutoScalingConditions(V2beta2HorizontalPodAutoscalerStatus):
    @property
    def conditions(self) -> Sequence[V2beta2HorizontalPodAutoscalerCondition]:
        return super().conditions()

    @conditions.setter
    def conditions(
        self, conditions: Optional[Sequence[V2beta2HorizontalPodAutoscalerCondition]]
    ) -> None:
        self._conditions = list() if conditions is None else conditions


models.V2beta2HorizontalPodAutoscalerStatus = MonkeyPatchAutoScalingConditions


class KubeKind(NamedTuple):
    singular: str
    plural: str


class KubeDeployment(NamedTuple):
    service: str
    instance: str
    git_sha: str
    image_version: Optional[str]
    config_sha: str
    replicas: Optional[int]


class KubeCustomResource(NamedTuple):
    service: str
    instance: str
    config_sha: str
    git_sha: str
    kind: str
    namespace: str
    name: str


class KubeContainerResources(NamedTuple):
    cpus: float
    mem: float  # mb
    disk: float  # mb


class KubernetesServiceRegistration(NamedTuple):
    name: str
    instance: str
    port: int
    pod_ip: str
    registrations: Sequence[str]


class CustomResourceDefinition(NamedTuple):
    file_prefix: str
    version: str
    kube_kind: KubeKind
    group: str


class KubeLifecycleDict(TypedDict, total=False):
    termination_grace_period_seconds: int
    pre_stop_command: Union[str, List[str]]


class KubeAffinityCondition(TypedDict, total=False):
    service: str
    instance: str


def _set_disrupted_pods(self: Any, disrupted_pods: Mapping[str, datetime]) -> None:
    """Private function used to patch the setter for V1beta1PodDisruptionBudgetStatus.
    Can be removed once https://github.com/kubernetes-client/python/issues/466 is resolved
    """
    self._disrupted_pods = disrupted_pods


KubeContainerResourceRequest = TypedDict(
    "KubeContainerResourceRequest",
    {
        "cpu": float,
        "memory": str,
        "ephemeral-storage": str,
    },
    total=False,
)


SidecarResourceRequirements = TypedDict(
    "SidecarResourceRequirements",
    {
        "requests": KubeContainerResourceRequest,
        "limits": KubeContainerResourceRequest,
    },
    total=False,
)


KubePodAnnotations = TypedDict(
    "KubePodAnnotations",
    {
        "autoscaling": str,
        "iam.amazonaws.com/role": str,
        "paasta.yelp.com/prometheus_path": str,
        "paasta.yelp.com/prometheus_port": str,
        "paasta.yelp.com/routable_ip": str,
        "smartstack_registrations": str,
    },
    total=False,
)

KubePodLabels = TypedDict(
    "KubePodLabels",
    {
        # NOTE: we can't use the paasta_prefixed() helper here
        # since mypy expects TypedDict keys to be string literals
        "paasta.yelp.com/deploy_group": str,
        "paasta.yelp.com/git_sha": str,
        "paasta.yelp.com/image_version": str,
        "paasta.yelp.com/instance": str,
        "paasta.yelp.com/prometheus_shard": str,
        "paasta.yelp.com/scrape_uwsgi_prometheus": str,
        "paasta.yelp.com/scrape_piscina_prometheus": str,
        "paasta.yelp.com/service": str,
        "paasta.yelp.com/autoscaled": str,
        "yelp.com/paasta_git_sha": str,
        "yelp.com/paasta_instance": str,
        "yelp.com/paasta_service": str,
        "sidecar.istio.io/inject": str,
    },
    total=False,
)


class KubernetesDeploymentConfigDict(LongRunningServiceConfigDict, total=False):
    bounce_method: str
    bounce_health_params: Dict[str, Any]
    service_account_name: str
    node_selectors: Dict[str, Union[str, Dict[str, Any]]]
    sidecar_resource_requirements: Dict[str, SidecarResourceRequirements]
    lifecycle: KubeLifecycleDict
    anti_affinity: Union[KubeAffinityCondition, List[KubeAffinityCondition]]
    prometheus_shard: str
    prometheus_path: str
    prometheus_port: int
    routable_ip: bool
    pod_management_policy: str
    is_istio_sidecar_injection_enabled: bool
    boto_keys: List[str]


def load_kubernetes_service_config_no_cache(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> "KubernetesDeploymentConfig":
    """Read a service instance's configuration for kubernetes.

    If a branch isn't specified for a config, the 'branch' key defaults to
    paasta-${cluster}.${instance}.

    :param name: The service name
    :param instance: The instance of the service to retrieve
    :param cluster: The cluster to read the configuration for
    :param load_deployments: A boolean indicating if the corresponding deployments.json for this service
                             should also be loaded
    :param soa_dir: The SOA configuration directory to read from
    :returns: A dictionary of whatever was in the config for the service instance"""
    general_config = service_configuration_lib.read_service_configuration(
        service, soa_dir=soa_dir
    )
    instance_config = load_service_instance_config(
        service, instance, "kubernetes", cluster, soa_dir=soa_dir
    )
    general_config = deep_merge_dictionaries(
        overrides=instance_config, defaults=general_config
    )

    branch_dict: Optional[BranchDictV2] = None
    if load_deployments:
        deployments_json = load_v2_deployments_json(service, soa_dir=soa_dir)
        temp_instance_config = KubernetesDeploymentConfig(
            service=service,
            cluster=cluster,
            instance=instance,
            config_dict=general_config,
            branch_dict=None,
            soa_dir=soa_dir,
        )
        branch = temp_instance_config.get_branch()
        deploy_group = temp_instance_config.get_deploy_group()
        branch_dict = deployments_json.get_branch_dict(service, branch, deploy_group)

    return KubernetesDeploymentConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=general_config,
        branch_dict=branch_dict,
        soa_dir=soa_dir,
    )


@time_cache(ttl=5)
def load_kubernetes_service_config(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> "KubernetesDeploymentConfig":
    """Read a service instance's configuration for kubernetes.

    If a branch isn't specified for a config, the 'branch' key defaults to
    paasta-${cluster}.${instance}.

    :param name: The service name
    :param instance: The instance of the service to retrieve
    :param cluster: The cluster to read the configuration for
    :param load_deployments: A boolean indicating if the corresponding deployments.json for this service
                             should also be loaded
    :param soa_dir: The SOA configuration directory to read from
    :returns: A dictionary of whatever was in the config for the service instance"""
    return load_kubernetes_service_config_no_cache(
        service=service,
        instance=instance,
        cluster=cluster,
        load_deployments=load_deployments,
        soa_dir=soa_dir,
    )


def limit_size_with_hash(name: str, limit: int = 63, suffix: int = 4) -> str:
    """Returns `name` unchanged if it's length does not exceed the `limit`.
    Otherwise, returns truncated `name` with it's hash of size `suffix`
    appended.

    base32 encoding is chosen as it satisfies the common requirement in
    various k8s names to be alphanumeric.
    """
    if len(name) > limit:
        digest = hashlib.md5(name.encode()).digest()
        hash = base64.b32encode(digest).decode().replace("=", "").lower()
        return f"{name[:(limit-suffix-1)]}-{hash[:suffix]}"
    else:
        return name


class InvalidKubernetesConfig(Exception):
    def __init__(self, exception: Exception, service: str, instance: str) -> None:
        super().__init__(
            f"Couldn't generate config for kubernetes service: {service}.{instance}: {exception}"
        )


class KubeClient:
    def __init__(self, component: Optional[str] = None) -> None:
        kube_config.load_kube_config(
            config_file=os.environ.get("KUBECONFIG", KUBE_CONFIG_PATH),
            context=os.environ.get("KUBECONTEXT"),
        )
        models.V1beta1PodDisruptionBudgetStatus.disrupted_pods = property(
            fget=lambda *args, **kwargs: models.V1beta1PodDisruptionBudgetStatus.disrupted_pods(
                *args, **kwargs
            ),
            fset=_set_disrupted_pods,
        )
        if not component:
            # If we don't get an explicit component set via constructor,
            # try to find it by looking back in the stack, and getting `__file__` from
            # the context calling this constructor
            # Normally, `__module__` would make more sense, but since we have a lot of
            # single scripts we directly call, that would be set to `__main__` most of the time.
            current = currentframe()
            parent = current.f_back
            component = parent.f_globals.get("__file__", "unknown")

        self.api_client = kube_client.ApiClient()
        self.api_client.user_agent = f"paasta/{component}/v{__version__}"

        self.deployments = kube_client.AppsV1Api(self.api_client)
        self.core = kube_client.CoreV1Api(self.api_client)
        self.policy = kube_client.PolicyV1beta1Api(self.api_client)
        self.apiextensions = kube_client.ApiextensionsV1beta1Api(self.api_client)
        self.custom = kube_client.CustomObjectsApi(self.api_client)
        self.autoscaling = kube_client.AutoscalingV2beta2Api(self.api_client)
        self.rbac = kube_client.RbacAuthorizationV1Api(self.api_client)

        self.request = self.api_client.request
        # This function is used by the k8s client to serialize OpenAPI objects
        # into JSON before posting to the api. The JSON output can be used
        # in place of OpenAPI objects in client function calls. This allows us
        # to monkey-patch the JSON data with configs the api supports, but the
        # Python client lib may not yet.
        self.jsonify = self.api_client.sanitize_for_serialization


def allowlist_denylist_to_requirements(
    allowlist: DeployWhitelist, denylist: DeployBlacklist
) -> List[Tuple[str, str, List[str]]]:
    """Converts deploy_whitelist and deploy_blacklist to a list of
    requirements, which can be converted to node affinities.
    """
    requirements = []
    # convert whitelist into a node selector req
    if allowlist:
        location_type, alloweds = allowlist
        requirements.append((to_node_label(location_type), "In", alloweds))
    # convert blacklist into multiple node selector reqs
    if denylist:
        # not going to prune for duplicates, or group blacklist items for
        # same location_type. makes testing easier and k8s can handle it.
        for location_type, not_allowed in denylist:
            requirements.append((to_node_label(location_type), "NotIn", [not_allowed]))
    return requirements


def raw_selectors_to_requirements(
    raw_selectors: Mapping[str, Any]
) -> List[Tuple[str, str, List[str]]]:
    """Converts certain node_selectors into requirements, which can be
    converted to node affinities.
    """
    requirements = []

    for label, configs in raw_selectors.items():
        if type(configs) is not list or len(configs) == 0:
            continue
        elif type(configs[0]) is str:
            # specifying an array/list of strings for a label is shorthand
            # for the "In" operator
            configs = [{"operator": "In", "values": configs}]

        label = to_node_label(label)
        for config in configs:
            if config["operator"] in {"In", "NotIn"}:
                values = config["values"]
            elif config["operator"] in {"Exists", "DoesNotExist"}:
                values = []
            elif config["operator"] in {"Gt", "Lt"}:
                # config["value"] is validated by jsonschema to be an int. but,
                # k8s expects singleton list of the int represented as a str
                # for these operators.
                values = [str(config["value"])]
            else:
                raise ValueError(
                    f"Unknown k8s node affinity operator: {config['operator']}"
                )
            requirements.append((label, config["operator"], values))

    return requirements


def registration_label(namespace: str) -> str:
    """Returns namespace prefixed with registrations.{paasta_prefix}/, with
    name part of label key truncated to 63 characters with hash as suffix
    if needed.
    """
    limited_namespace = limit_size_with_hash(namespace, limit=63, suffix=4)
    return f"registrations.{PAASTA_ATTRIBUTE_PREFIX}{limited_namespace}"


class KubernetesDeploymentConfig(LongRunningServiceConfig):
    config_dict: KubernetesDeploymentConfigDict

    config_filename_prefix = "kubernetes"

    def __init__(
        self,
        service: str,
        cluster: str,
        instance: str,
        config_dict: KubernetesDeploymentConfigDict,
        branch_dict: Optional[BranchDictV2],
        soa_dir: str = DEFAULT_SOA_DIR,
    ) -> None:
        super().__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
            soa_dir=soa_dir,
        )

    def copy(self) -> "KubernetesDeploymentConfig":
        return self.__class__(
            service=self.service,
            instance=self.instance,
            cluster=self.cluster,
            config_dict=self.config_dict.copy(),
            branch_dict=self.branch_dict.copy()
            if self.branch_dict is not None
            else None,
            soa_dir=self.soa_dir,
        )

    def get_kubernetes_namespace(self) -> str:
        return KUBERNETES_NAMESPACE

    def get_cmd(self) -> Optional[List[str]]:
        cmd = super(LongRunningServiceConfig, self).get_cmd()
        if cmd:
            if isinstance(cmd, str):
                return ["sh", "-c", cmd]
            elif isinstance(cmd, list):
                return cmd
            else:
                raise ValueError("cmd should be str or list")
        else:
            return None

    def get_bounce_method(self) -> str:
        """Get the bounce method specified in the service's kubernetes configuration."""
        # map existing bounce methods to k8s equivalents.
        # but if there's an EBS volume we must downthenup to free up the volume.
        # in the future we may support stateful sets to dynamically create the volumes
        bounce_method = self.config_dict.get("bounce_method", "crossover")
        if self.get_aws_ebs_volumes() and not bounce_method == "downthenup":
            raise Exception(
                "If service instance defines an EBS volume it must use a downthenup bounce_method"
            )
        return bounce_method

    # TODO: move the default scaling policy to system paasta configs
    def get_autoscaling_scaling_policy(
        self,
        max_replicas: int,
        autoscaling_params: AutoscalingParamsDict,
    ) -> Dict:
        """Returns the k8s HPA scaling policy in raw JSON. Requires k8s v1.18
        to work.
        """
        # The HPA scaling algorithm is as follows. Every sync period (default:
        # 15 seconds), the HPA will:
        #   1. determine what the desired capacity is from metrics
        #   2. apply min/max replica scaling limits
        #   3. rate-limit the scaling magnitude (e.g. scale down by no more than
        #      30% of current replicas)
        #   4. constrain the scaling magnitude by the period seconds (e.g. scale
        #      down by no more than 30% of current replicas per 60 seconds)
        #   5. record the desired capacity, then pick the highest capacity from
        #      the stabilization window (default: last 300 seconds) as the final
        #      desired capacity.
        #      - the idea is to stabilize scaling against (heavily) fluctuating
        #        metrics
        policy = {
            "scaleDown": {
                "stabilizationWindowSeconds": 300,
                # the policy in a human-readable way: scale down every 60s by
                # at most 30% of current replicas.
                "selectPolicy": "Max",
                "policies": [{"type": "Percent", "value": 30, "periodSeconds": 60}],
            }
        }
        policy["scaleDown"].update(autoscaling_params.get("scaledown_policies", {}))
        return policy

    def namespace_external_metric_name(self, metric_name: str) -> str:
        return f"{self.get_sanitised_deployment_name()}-{metric_name}"

    def get_autoscaling_metric_spec(
        self,
        name: str,
        cluster: str,
        kube_client: KubeClient,
        namespace: str = "paasta",
    ) -> Optional[Union[V2beta2HorizontalPodAutoscaler, Dict]]:
        # Returns None if an HPA should not be attached based on the config,
        # or the config is invalid.

        if self.get_desired_state() == "stop":
            return None

        if not self.is_autoscaling_enabled():
            return None

        autoscaling_params = self.get_autoscaling_params()
        if autoscaling_params["decision_policy"] == "bespoke":
            return None

        min_replicas = self.get_min_instances()
        max_replicas = self.get_max_instances()
        if min_replicas == 0 or max_replicas == 0:
            log.error(
                f"Invalid value for min or max_instances: {min_replicas}, {max_replicas}"
            )
            return None

        metrics_provider = autoscaling_params["metrics_provider"]
        metrics = []
        target = autoscaling_params["setpoint"]
        annotations: Dict[str, str] = {}
        # we will have both the SFX adapter and the Prometheus adapter serving metrics for some
        # HPAs and, since they may be looking at the same thing, we need to suffix one of these metric
        # identifiers because metric names inside an HPA must be unique.
        prometheus_hpa_metric_name = (
            f"{self.namespace_external_metric_name(metrics_provider)}-prom"
        )
        # TODO: we should remove mesos_cpu as an option once we've cleaned up our configs
        if metrics_provider in ("mesos_cpu", "cpu"):
            use_prometheus = autoscaling_params.get(
                "use_prometheus", DEFAULT_USE_PROMETHEUS_CPU
            )
            if use_prometheus:
                metrics.append(
                    V2beta2MetricSpec(
                        type="Object",
                        object=V2beta2ObjectMetricSource(
                            metric=V2beta2MetricIdentifier(
                                name=prometheus_hpa_metric_name
                            ),
                            described_object=V2beta2CrossVersionObjectReference(
                                api_version="apps/v1", kind="Deployment", name=name
                            ),
                            target=V2beta2MetricTarget(
                                type="Value",
                                value=int(target * 100),
                            ),
                        ),
                    )
                )

            use_resource_metrics = autoscaling_params.get(
                "use_resource_metrics", DEFAULT_USE_RESOURCE_METRICS_CPU
            )
            if use_resource_metrics:
                metrics.append(
                    V2beta2MetricSpec(
                        type="Resource",
                        resource=V2beta2ResourceMetricSource(
                            name="cpu",
                            target=V2beta2MetricTarget(
                                type="Utilization",
                                average_utilization=int(target * 100),
                            ),
                        ),
                    )
                )
        elif metrics_provider in {"uwsgi", "piscina"}:
            metrics.append(
                V2beta2MetricSpec(
                    type="Object",
                    object=V2beta2ObjectMetricSource(
                        metric=V2beta2MetricIdentifier(name=prometheus_hpa_metric_name),
                        described_object=V2beta2CrossVersionObjectReference(
                            api_version="apps/v1", kind="Deployment", name=name
                        ),
                        target=V2beta2MetricTarget(
                            type="Value",
                            # we average the number of instances needed to handle the current (or
                            # averaged) load instead of the load itself as this leads to more
                            # stable behavior. we return the percentage by which we want to
                            # scale, so the target in the HPA should always be 1.
                            # PAASTA-16756 for details
                            value=1,
                        ),
                    ),
                )
            )
        elif metrics_provider in {"arbitrary_promql"}:
            metrics.append(
                V2beta2MetricSpec(
                    type="Object",
                    object=V2beta2ObjectMetricSource(
                        metric=V2beta2MetricIdentifier(name=prometheus_hpa_metric_name),
                        described_object=V2beta2CrossVersionObjectReference(
                            api_version="apps/v1", kind="Deployment", name=name
                        ),
                        target=V2beta2MetricTarget(
                            # Use the setpoint specified by the user.
                            type="Value",
                            value=target,
                        ),
                    ),
                )
            )
        else:
            log.error(
                f"Unknown metrics_provider specified: {metrics_provider} for\
                {name}/name in namespace{namespace}"
            )
            return None

        hpa = V2beta2HorizontalPodAutoscaler(
            kind="HorizontalPodAutoscaler",
            metadata=V1ObjectMeta(
                name=name, namespace=namespace, annotations=annotations
            ),
            spec=V2beta2HorizontalPodAutoscalerSpec(
                max_replicas=max_replicas,
                min_replicas=min_replicas,
                metrics=metrics,
                scale_target_ref=V2beta2CrossVersionObjectReference(
                    api_version="apps/v1", kind="Deployment", name=name
                ),
            ),
        )

        # In k8s v1.18, HPA scaling policies can be set:
        #   https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#support-for-configurable-scaling-behavior
        # However, the python client library currently only supports v1.17, so
        # we need to monkey-patch scaling policies until the library is updated
        # v1.18.
        scaling_policy = self.get_autoscaling_scaling_policy(
            max_replicas,
            autoscaling_params,
        )
        if scaling_policy:
            hpa = kube_client.jsonify(hpa)  # this is a hack, see KubeClient class
            hpa["spec"]["behavior"] = scaling_policy
        return hpa

    def get_deployment_strategy_config(self) -> V1DeploymentStrategy:
        # get soa defined bounce_method
        bounce_method = self.get_bounce_method()
        # get k8s equivalent
        strategy_type = KUBE_DEPLOY_STATEGY_MAP[bounce_method]

        if strategy_type == "RollingUpdate":
            max_surge = "100%"
            if bounce_method == "crossover":
                max_unavailable = "{}%".format(
                    int((1 - self.get_bounce_margin_factor()) * 100)
                )
            elif bounce_method == "brutal":
                # `brutal` bounce method means a bounce margin factor of 0, do not call get_bounce_margin_factor
                max_unavailable = "100%"
            else:
                raise Exception("Unknown bounce method for RollingUpdate.")
            rolling_update = V1RollingUpdateDeployment

            # this translates bounce_margin to k8s speak maxUnavailable
            # for now we keep max_surge 100% but we could customise later
            rolling_update = V1RollingUpdateDeployment(
                max_surge=max_surge, max_unavailable=max_unavailable
            )
        else:
            rolling_update = None

        return V1DeploymentStrategy(type=strategy_type, rolling_update=rolling_update)

    def get_sanitised_volume_name(self, volume_name: str, length_limit: int = 0) -> str:
        """I know but we really aren't allowed many characters..."""
        volume_name = volume_name.rstrip("/")
        sanitised = volume_name.replace("/", "slash-").replace(".", "dot-")
        sanitised_name = sanitise_kubernetes_name(sanitised)
        if length_limit and len(sanitised_name) > length_limit:
            sanitised_name = (
                sanitised_name[0 : length_limit - 6]
                + "--"
                + hashlib.md5(sanitised_name.encode("ascii")).hexdigest()[:4]
            )
        return sanitised_name

    def get_docker_volume_name(self, docker_volume: DockerVolume) -> str:
        return self.get_sanitised_volume_name(
            "host--{name}".format(name=docker_volume["hostPath"]), length_limit=63
        )

    def get_persistent_volume_name(self, docker_volume: PersistentVolume) -> str:
        return self.get_sanitised_volume_name(
            "pv--{name}".format(name=docker_volume["container_path"]), length_limit=253
        )

    def get_aws_ebs_volume_name(self, aws_ebs_volume: AwsEbsVolume) -> str:
        return self.get_sanitised_volume_name(
            "aws-ebs--{name}{partition}".format(
                name=aws_ebs_volume["volume_id"],
                partition=aws_ebs_volume.get("partition", ""),
            )
        )

    def get_secret_volume_name(self, secret_volume: SecretVolume) -> str:
        return self.get_sanitised_volume_name(
            "secret--{name}".format(name=secret_volume["secret_name"]), length_limit=253
        )

    def get_boto_secret_volume_name(self, service_name: str) -> str:
        return self.get_sanitised_volume_name(
            f"secret-boto-key-{service_name}", length_limit=63
        )

    def read_only_mode(self, d: VolumeWithMode) -> bool:
        return d.get("mode", "RO") == "RO"

    def get_readiness_check_script(
        self, system_paasta_config: SystemPaastaConfig
    ) -> List[str]:
        """Script to check if a service is up in smartstack / envoy"""
        enable_envoy_check = self.get_enable_envoy_readiness_check(system_paasta_config)
        enable_nerve_check = self.get_enable_nerve_readiness_check(system_paasta_config)
        if enable_nerve_check and enable_envoy_check:
            return system_paasta_config.get_envoy_nerve_readiness_check_script()
        elif enable_envoy_check:
            return system_paasta_config.get_envoy_readiness_check_script()
        else:
            return system_paasta_config.get_nerve_readiness_check_script()

    def get_sidecar_containers(
        self,
        system_paasta_config: SystemPaastaConfig,
        service_namespace_config: ServiceNamespaceConfig,
        hacheck_sidecar_volumes: Sequence[DockerVolume],
    ) -> Sequence[V1Container]:
        hacheck_container = self.get_hacheck_sidecar_container(
            system_paasta_config,
            service_namespace_config,
            hacheck_sidecar_volumes,
        )
        uwsgi_exporter_container = self.get_uwsgi_exporter_sidecar_container(
            system_paasta_config
        )

        sidecars = []
        if hacheck_container:
            sidecars.append(hacheck_container)
        if uwsgi_exporter_container:
            sidecars.append(uwsgi_exporter_container)
        return sidecars

    def get_hacheck_sidecar_container(
        self,
        system_paasta_config: SystemPaastaConfig,
        service_namespace_config: ServiceNamespaceConfig,
        hacheck_sidecar_volumes: Sequence[DockerVolume],
    ) -> Optional[V1Container]:
        registrations = " ".join(self.get_registrations())
        # s_m_j currently asserts that services are healthy in smartstack before
        # continuing a bounce. this readiness check lets us achieve the same thing
        readiness_probe: Optional[V1Probe]
        if service_namespace_config.is_in_smartstack() and (
            self.get_enable_nerve_readiness_check(system_paasta_config)
            or self.get_enable_envoy_readiness_check(system_paasta_config)
        ):
            initial_delay = self.get_healthcheck_grace_period_seconds()
            # COMPINFRA-989, this used to be hardcoded to always be 10 seconds
            # and to not cause rolling updates on everything at once this is a config option for now
            if not system_paasta_config.get_hacheck_match_initial_delay():
                initial_delay = 10
            readiness_probe = V1Probe(
                _exec=V1ExecAction(
                    command=self.get_readiness_check_script(system_paasta_config)
                    + [str(self.get_container_port())]
                    + self.get_registrations()
                ),
                initial_delay_seconds=initial_delay,
                period_seconds=10,
            )
        else:
            readiness_probe = None

        if service_namespace_config.is_in_smartstack():
            return V1Container(
                image=system_paasta_config.get_hacheck_sidecar_image_url(),
                lifecycle=V1Lifecycle(
                    pre_stop=V1Handler(
                        _exec=V1ExecAction(
                            command=[
                                "/bin/sh",
                                "-c",
                                f"/usr/bin/hadown {registrations}; sleep {DEFAULT_HADOWN_PRESTOP_SLEEP_SECONDS}",
                            ]
                        )
                    )
                ),
                resources=self.get_sidecar_resource_requirements("hacheck"),
                name=HACHECK_POD_NAME,
                env=self.get_kubernetes_environment(),
                ports=[V1ContainerPort(container_port=6666)],
                readiness_probe=readiness_probe,
                volume_mounts=self.get_volume_mounts(
                    docker_volumes=hacheck_sidecar_volumes,
                    aws_ebs_volumes=[],
                    persistent_volumes=[],
                    secret_volumes=[],
                ),
            )
        return None

    def get_uwsgi_exporter_sidecar_container(
        self,
        system_paasta_config: SystemPaastaConfig,
    ) -> Optional[V1Container]:

        if self.should_run_uwsgi_exporter_sidecar(system_paasta_config):
            stats_port_env = V1EnvVar(
                name="STATS_PORT",
                value=str(self.get_autoscaling_params().get("uwsgi_stats_port", 8889)),
            )

            return V1Container(
                image=system_paasta_config.get_uwsgi_exporter_sidecar_image_url(),
                resources=self.get_sidecar_resource_requirements("uwsgi_exporter"),
                name=UWSGI_EXPORTER_POD_NAME,
                env=self.get_kubernetes_environment() + [stats_port_env],
                ports=[V1ContainerPort(container_port=9117)],
                lifecycle=V1Lifecycle(
                    pre_stop=V1Handler(
                        _exec=V1ExecAction(
                            command=[
                                "/bin/sh",
                                "-c",
                                # we sleep for the same amount of time as we do after an hadown to ensure that we have accurate
                                # metrics up until our Pod dies
                                f"sleep {DEFAULT_HADOWN_PRESTOP_SLEEP_SECONDS}",
                            ]
                        )
                    )
                ),
            )

        return None

    def should_run_uwsgi_exporter_sidecar(
        self,
        system_paasta_config: SystemPaastaConfig,
    ) -> bool:
        if self.is_autoscaling_enabled():
            autoscaling_params = self.get_autoscaling_params()
            if autoscaling_params["metrics_provider"] == "uwsgi":
                if autoscaling_params.get(
                    "use_prometheus",
                    DEFAULT_USE_PROMETHEUS_UWSGI
                    or system_paasta_config.default_should_run_uwsgi_exporter_sidecar(),
                ):
                    return True
        return False

    def should_setup_piscina_prometheus_scraping(
        self,
    ) -> bool:
        if self.is_autoscaling_enabled():
            autoscaling_params = self.get_autoscaling_params()
            return autoscaling_params["metrics_provider"] == "piscina"
        return False

    def get_env(
        self, system_paasta_config: Optional["SystemPaastaConfig"] = None
    ) -> Dict[str, str]:
        env = super().get_env(system_paasta_config=system_paasta_config)
        # see CLIENTOBS-64 and PAASTA-17558
        # this is deliberately set here to make sure it is only available for
        # k8s long-running services. putting this in `InstanceConfig.get_env` will
        # make it available for all workloads, which will cause big bounces and
        # continuous reconfiguring every time soa-configs is updated unless the
        # env var is deliberately excluded from config hashing for those workloads
        # as well.
        env["PAASTA_SOA_CONFIGS_SHA"] = read_soa_metadata(soa_dir=self.soa_dir).get(
            "git_sha", ""
        )
        return env

    def get_container_env(self) -> Sequence[V1EnvVar]:
        secret_env_vars = {}
        shared_secret_env_vars = {}
        for k, v in self.get_env().items():
            if is_secret_ref(v):
                if is_shared_secret(v):
                    shared_secret_env_vars[k] = v
                else:
                    secret_env_vars[k] = v

        user_env = [
            V1EnvVar(name=name, value=value)
            for name, value in self.get_env().items()
            if name
            not in list(secret_env_vars.keys()) + list(shared_secret_env_vars.keys())
        ]
        user_env += self.get_kubernetes_secret_env_vars(
            secret_env_vars=secret_env_vars,
            shared_secret_env_vars=shared_secret_env_vars,
        )
        return user_env + self.get_kubernetes_environment()  # type: ignore

    def get_kubernetes_secret_env_vars(
        self,
        secret_env_vars: Mapping[str, str],
        shared_secret_env_vars: Mapping[str, str],
    ) -> Sequence[V1EnvVar]:
        ret = []
        for k, v in secret_env_vars.items():
            service = self.get_sanitised_service_name()
            secret = get_secret_name_from_ref(v)
            sanitised_secret = sanitise_kubernetes_name(secret)
            ret.append(
                V1EnvVar(
                    name=k,
                    value_from=V1EnvVarSource(
                        secret_key_ref=V1SecretKeySelector(
                            name=f"paasta-secret-{service}-{sanitised_secret}",
                            key=secret,
                            optional=False,
                        )
                    ),
                )
            )
        for k, v in shared_secret_env_vars.items():
            service = sanitise_kubernetes_name(SHARED_SECRET_SERVICE)
            secret = get_secret_name_from_ref(v)
            sanitised_secret = sanitise_kubernetes_name(secret)
            ret.append(
                V1EnvVar(
                    name=k,
                    value_from=V1EnvVarSource(
                        secret_key_ref=V1SecretKeySelector(
                            name=f"paasta-secret-{service}-{sanitised_secret}",
                            key=secret,
                            optional=False,
                        )
                    ),
                )
            )
        return ret

    def get_kubernetes_environment(self) -> List[V1EnvVar]:
        kubernetes_env = [
            V1EnvVar(
                name="PAASTA_POD_IP",
                value_from=V1EnvVarSource(
                    field_ref=V1ObjectFieldSelector(field_path="status.podIP")
                ),
            ),
            V1EnvVar(
                # this is used by some functions of operator-sdk
                # it uses this environment variable to get the pods
                name="POD_NAME",
                value_from=V1EnvVarSource(
                    field_ref=V1ObjectFieldSelector(field_path="metadata.name")
                ),
            ),
            V1EnvVar(
                name="PAASTA_HOST",
                value_from=V1EnvVarSource(
                    field_ref=V1ObjectFieldSelector(field_path="spec.nodeName")
                ),
            ),
        ]
        return kubernetes_env

    def get_resource_requirements(self) -> V1ResourceRequirements:
        limits = {
            "cpu": self.get_cpus() + self.get_cpu_burst_add(),
            "memory": f"{self.get_mem()}Mi",
            "ephemeral-storage": f"{self.get_disk()}Mi",
        }
        requests = {
            "cpu": self.get_cpus(),
            "memory": f"{self.get_mem()}Mi",
            "ephemeral-storage": f"{self.get_disk()}Mi",
        }
        if self.get_gpus():
            limits[GPU_RESOURCE_NAME] = self.get_gpus()
            requests[GPU_RESOURCE_NAME] = self.get_gpus()
        return V1ResourceRequirements(limits=limits, requests=requests)

    def get_sidecar_resource_requirements(
        self, sidecar_name: str
    ) -> V1ResourceRequirements:
        config = self.config_dict.get("sidecar_resource_requirements", {}).get(
            sidecar_name, {}
        )
        requests: KubeContainerResourceRequest = {
            "cpu": 0.1,
            "memory": "1024Mi",
            "ephemeral-storage": "256Mi",
        }
        requests.update(config.get("requests", {}))

        limits: KubeContainerResourceRequest = {
            "cpu": requests["cpu"],
            "memory": requests["memory"],
            "ephemeral-storage": requests["ephemeral-storage"],
        }
        limits.update(config.get("limits", {}))

        return V1ResourceRequirements(
            limits=limits,
            requests=requests,
        )

    def get_liveness_probe(
        self, service_namespace_config: ServiceNamespaceConfig
    ) -> Optional[V1Probe]:
        mode = self.get_healthcheck_mode(service_namespace_config)
        if mode is None:
            return None

        initial_delay_seconds = self.get_healthcheck_grace_period_seconds()
        period_seconds = self.get_healthcheck_interval_seconds()
        timeout_seconds = self.get_healthcheck_timeout_seconds()
        failure_threshold = self.get_healthcheck_max_consecutive_failures()
        probe = V1Probe(
            failure_threshold=failure_threshold,
            initial_delay_seconds=initial_delay_seconds,
            period_seconds=period_seconds,
            timeout_seconds=timeout_seconds,
        )

        if mode == "http" or mode == "https":
            path = self.get_healthcheck_uri(service_namespace_config)
            probe.http_get = V1HTTPGetAction(
                path=path, port=self.get_container_port(), scheme=mode.upper()
            )
        elif mode == "tcp":
            probe.tcp_socket = V1TCPSocketAction(port=self.get_container_port())
        elif mode == "cmd":
            probe._exec = V1ExecAction(
                command=["/bin/sh", "-c", self.get_healthcheck_cmd()]
            )
        else:
            raise InvalidHealthcheckMode(
                "Unknown mode: %s. Only acceptable healthcheck modes are http/https/tcp/cmd"
                % mode
            )

        return probe

    def get_security_context(self) -> Optional[V1SecurityContext]:
        cap_add = self.config_dict.get("cap_add", None)
        if cap_add is None:
            return V1SecurityContext(capabilities=V1Capabilities(drop=CAPS_DROP))
        else:
            return V1SecurityContext(
                capabilities=V1Capabilities(add=cap_add, drop=CAPS_DROP)
            )

    def get_kubernetes_containers(
        self,
        docker_volumes: Sequence[DockerVolume],
        hacheck_sidecar_volumes: Sequence[DockerVolume],
        system_paasta_config: SystemPaastaConfig,
        aws_ebs_volumes: Sequence[AwsEbsVolume],
        secret_volumes: Sequence[SecretVolume],
        service_namespace_config: ServiceNamespaceConfig,
    ) -> Sequence[V1Container]:
        ports = [self.get_container_port()]
        # MONK-1130
        # The prometheus_port is used for scraping metrics from the main
        # container in the pod. Prometheus discovers ports using the kubernetes
        # API and creates scrape targets for all the exported container ports.
        # A better way of doing this would to export the prometheus port as pod
        # annotations but this is not currently supported.
        # https://github.com/prometheus/prometheus/issues/3756
        prometheus_port = self.get_prometheus_port()
        if prometheus_port and prometheus_port not in ports:
            ports.append(prometheus_port)

        service_container = V1Container(
            image=self.get_docker_url(),
            command=self.get_cmd(),
            args=self.get_args(),
            env=self.get_container_env(),
            resources=self.get_resource_requirements(),
            lifecycle=V1Lifecycle(
                pre_stop=self.get_kubernetes_container_termination_action()
            ),
            name=self.get_sanitised_instance_name(),
            liveness_probe=self.get_liveness_probe(service_namespace_config),
            readiness_probe=self.get_readiness_probe(service_namespace_config),
            ports=[V1ContainerPort(container_port=port) for port in ports],
            security_context=self.get_security_context(),
            volume_mounts=self.get_volume_mounts(
                docker_volumes=docker_volumes,
                aws_ebs_volumes=aws_ebs_volumes,
                persistent_volumes=self.get_persistent_volumes(),
                secret_volumes=secret_volumes,
            ),
        )
        containers = [service_container] + self.get_sidecar_containers(  # type: ignore
            system_paasta_config=system_paasta_config,
            service_namespace_config=service_namespace_config,
            hacheck_sidecar_volumes=hacheck_sidecar_volumes,
        )
        return containers

    def get_readiness_probe(
        self, service_namespace_config: ServiceNamespaceConfig
    ) -> Optional[V1Probe]:
        if service_namespace_config.is_in_smartstack():
            return None
        else:
            return self.get_liveness_probe(service_namespace_config)

    def get_kubernetes_container_termination_action(self) -> V1Handler:
        command = self.config_dict.get("lifecycle", KubeLifecycleDict({})).get(
            "pre_stop_command", []
        )
        # default pre stop hook for the container
        if not command:
            return V1Handler(
                _exec=V1ExecAction(
                    command=["/bin/sh", "-c", f"sleep {DEFAULT_PRESTOP_SLEEP_SECONDS}"]
                )
            )
        if isinstance(command, str):
            command = [command]
        return V1Handler(_exec=V1ExecAction(command=command))

    def get_pod_volumes(
        self,
        docker_volumes: Sequence[DockerVolume],
        aws_ebs_volumes: Sequence[AwsEbsVolume],
        secret_volumes: Sequence[SecretVolume],
    ) -> Sequence[V1Volume]:
        pod_volumes = []
        unique_docker_volumes = {
            self.get_docker_volume_name(docker_volume): docker_volume
            for docker_volume in docker_volumes
        }
        for name, docker_volume in unique_docker_volumes.items():
            pod_volumes.append(
                V1Volume(
                    host_path=V1HostPathVolumeSource(path=docker_volume["hostPath"]),
                    name=name,
                )
            )
        unique_aws_ebs_volumes = {
            self.get_aws_ebs_volume_name(aws_ebs_volume): aws_ebs_volume
            for aws_ebs_volume in aws_ebs_volumes
        }
        for name, aws_ebs_volume in unique_aws_ebs_volumes.items():
            pod_volumes.append(
                V1Volume(
                    aws_elastic_block_store=V1AWSElasticBlockStoreVolumeSource(
                        volume_id=aws_ebs_volume["volume_id"],
                        fs_type=aws_ebs_volume.get("fs_type"),
                        partition=aws_ebs_volume.get("partition"),
                        # k8s wants RW volume even if it's later mounted RO
                        read_only=False,
                    ),
                    name=name,
                )
            )
        for secret_volume in secret_volumes:
            service = self.get_sanitised_service_name()
            sanitised_secret = sanitise_kubernetes_name(secret_volume["secret_name"])
            if "items" in secret_volume:
                items = [
                    V1KeyToPath(
                        key=item["key"],
                        mode=mode_to_int(item.get("mode")),
                        path=item["path"],
                    )
                    for item in secret_volume["items"]
                ]
            else:
                items = None
            pod_volumes.append(
                V1Volume(
                    name=self.get_secret_volume_name(secret_volume),
                    secret=V1SecretVolumeSource(
                        secret_name=f"paasta-secret-{service}-{sanitised_secret}",
                        default_mode=mode_to_int(secret_volume.get("default_mode")),
                        items=items,
                    ),
                )
            )
        boto_volume = self.get_boto_volume()
        if boto_volume:
            pod_volumes.append(boto_volume)
        return pod_volumes

    def get_boto_volume(self) -> Optional[V1Volume]:
        required_boto_keys = self.config_dict.get("boto_keys", [])
        service_name = self.get_sanitised_deployment_name()
        if not required_boto_keys:
            return None
        items = []
        for boto_key in required_boto_keys:
            for filetype in ["sh", "yaml", "cfg", "json"]:
                this_key = boto_key + "." + filetype
                secret_name = this_key.replace(".", "-").replace("_", "--")
                item = V1KeyToPath(
                    key=secret_name,
                    mode=mode_to_int("0444"),
                    path=this_key,
                )
                items.append(item)
        # Check that boto keys actually exist as secrets
        secret_hash = self.get_boto_secret_hash()
        if not secret_hash:
            log.warning(f"Expected to find k8s secret {secret_name} for boto_cfg")
            return None
        secret_name = limit_size_with_hash(f"paasta-boto-key-{service_name}")
        volume = V1Volume(
            name=self.get_boto_secret_volume_name(service_name),
            secret=V1SecretVolumeSource(
                secret_name=secret_name,
                default_mode=mode_to_int("0444"),
                items=items,
            ),
        )
        return volume

    def get_volume_mounts(
        self,
        docker_volumes: Sequence[DockerVolume],
        aws_ebs_volumes: Sequence[AwsEbsVolume],
        persistent_volumes: Sequence[PersistentVolume],
        secret_volumes: Sequence[SecretVolume],
    ) -> Sequence[V1VolumeMount]:
        volume_mounts = (
            [
                V1VolumeMount(
                    mount_path=docker_volume["containerPath"],
                    name=self.get_docker_volume_name(docker_volume),
                    read_only=self.read_only_mode(docker_volume),
                )
                for docker_volume in docker_volumes
            ]
            + [
                V1VolumeMount(
                    mount_path=aws_ebs_volume["container_path"],
                    name=self.get_aws_ebs_volume_name(aws_ebs_volume),
                    read_only=self.read_only_mode(aws_ebs_volume),
                )
                for aws_ebs_volume in aws_ebs_volumes
            ]
            + [
                V1VolumeMount(
                    mount_path=volume["container_path"],
                    name=self.get_persistent_volume_name(volume),
                    read_only=self.read_only_mode(volume),
                )
                for volume in persistent_volumes
            ]
            + [
                V1VolumeMount(
                    mount_path=volume["container_path"],
                    name=self.get_secret_volume_name(volume),
                    read_only=True,
                )
                for volume in secret_volumes
            ]
        )
        if self.config_dict.get("boto_keys", []):
            secret_hash = self.get_boto_secret_hash()
            service_name = self.get_sanitised_deployment_name()
            if secret_hash:
                mount = V1VolumeMount(
                    mount_path="/etc/boto_cfg",
                    name=self.get_boto_secret_volume_name(service_name),
                    read_only=True,
                )
                for existing_mount in volume_mounts:
                    if existing_mount.mount_path == "/etc/boto_cfg":
                        volume_mounts.remove(existing_mount)
                        break
                volume_mounts.append(mount)
        return volume_mounts

    def get_boto_secret_hash(self) -> str:
        kube_client = KubeClient()
        deployment_name = self.get_sanitised_deployment_name()
        service_name = self.get_sanitised_service_name()
        secret_name = limit_size_with_hash(f"paasta-boto-key-{deployment_name}")
        return get_kubernetes_secret_signature(
            kube_client=kube_client, secret=secret_name, service=service_name
        )

    def get_sanitised_service_name(self) -> str:
        return sanitise_kubernetes_name(self.get_service())

    def get_sanitised_instance_name(self) -> str:
        return sanitise_kubernetes_name(self.get_instance())

    def get_autoscaled_instances(self) -> Optional[int]:
        try:
            if self.get_persistent_volumes():
                return (
                    KubeClient()
                    .deployments.read_namespaced_stateful_set(
                        name=self.get_sanitised_deployment_name(), namespace="paasta"
                    )
                    .spec.replicas
                )
            else:
                return (
                    KubeClient()
                    .deployments.read_namespaced_deployment(
                        name=self.get_sanitised_deployment_name(), namespace="paasta"
                    )
                    .spec.replicas
                )
        except ApiException as e:
            log.error(e)
            log.debug(
                "Error occured when trying to connect to Kubernetes API, \
                returning max_instances (%d)"
                % self.get_max_instances()
            )
            return None

    def get_min_instances(self) -> Optional[int]:
        return self.config_dict.get(
            "min_instances",
            1,
        )

    def get_max_instances(self) -> Optional[int]:
        return self.config_dict.get(
            "max_instances",
            None,
        )

    def set_autoscaled_instances(
        self, instance_count: int, kube_client: KubeClient
    ) -> None:
        """Set the number of instances in the same way that the autoscaler does."""
        set_instances_for_kubernetes_service(
            kube_client=kube_client, service_config=self, instance_count=instance_count
        )

    def get_desired_instances(self) -> int:
        """For now if we have an EBS instance it means we can only have 1 instance
        since we can't attach to multiple instances. In the future we might support
        statefulsets which are clever enough to manage EBS for you"""
        instances = super().get_desired_instances()
        if self.get_aws_ebs_volumes() and instances not in [1, 0]:
            raise Exception(
                "Number of instances must be 1 or 0 if an EBS volume is defined."
            )
        return instances

    def get_volume_claim_templates(self) -> Sequence[V1PersistentVolumeClaim]:
        return [
            V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(name=self.get_persistent_volume_name(volume)),
                spec=V1PersistentVolumeClaimSpec(
                    # must be ReadWriteOnce for EBS
                    access_modes=["ReadWriteOnce"],
                    storage_class_name=self.get_storage_class_name(volume),
                    resources=V1ResourceRequirements(
                        requests={"storage": f"{volume['size']}Gi"}
                    ),
                ),
            )
            for volume in self.get_persistent_volumes()
        ]

    def get_storage_class_name(self, volume: PersistentVolume) -> str:
        try:
            system_paasta_config = load_system_paasta_config()
            supported_storage_classes = (
                system_paasta_config.get_supported_storage_classes()
            )
        except (PaastaNotConfiguredError):
            log.warning("No PaaSTA configuration was found, returning default value")
            supported_storage_classes = []
        storage_class_name = volume.get("storage_class_name", "ebs")
        if storage_class_name not in supported_storage_classes:
            log.warning(f"storage class {storage_class_name} is not supported")
            storage_class_name = DEFAULT_STORAGE_CLASS_NAME
        return storage_class_name

    def get_kubernetes_metadata(self, git_sha: str) -> V1ObjectMeta:
        return V1ObjectMeta(
            name=self.get_sanitised_deployment_name(),
            labels={
                "yelp.com/paasta_service": self.get_service(),
                "yelp.com/paasta_instance": self.get_instance(),
                "yelp.com/paasta_git_sha": git_sha,
                "paasta.yelp.com/service": self.get_service(),
                "paasta.yelp.com/instance": self.get_instance(),
                "paasta.yelp.com/git_sha": git_sha,
                paasta_prefixed("autoscaled"): str(
                    self.is_autoscaling_enabled()
                ).lower(),
            },
        )

    def get_sanitised_deployment_name(self) -> str:
        return get_kubernetes_app_name(self.get_service(), self.get_instance())

    def get_min_task_uptime(self) -> int:
        return self.config_dict.get("bounce_health_params", {}).get(
            "min_task_uptime", 0
        )

    def get_enable_nerve_readiness_check(
        self, system_paasta_config: SystemPaastaConfig
    ) -> bool:
        """Enables a k8s readiness check on the Pod to ensure that all registrations
        are UP on the local synapse haproxy"""
        return self.config_dict.get("bounce_health_params", {}).get(
            "check_haproxy", system_paasta_config.get_enable_nerve_readiness_check()
        )

    def get_enable_envoy_readiness_check(
        self, system_paasta_config: SystemPaastaConfig
    ) -> bool:
        """Enables a k8s readiness check on the Pod to ensure that all registrations
        are UP on the local Envoy"""
        return self.config_dict.get("bounce_health_params", {}).get(
            "check_envoy", system_paasta_config.get_enable_envoy_readiness_check()
        )

    def get_pod_management_policy(self) -> str:
        """Get sts pod_management_policy from config, default to 'OrderedReady'"""
        return self.config_dict.get("pod_management_policy", "OrderedReady")

    def format_kubernetes_app(self) -> Union[V1Deployment, V1StatefulSet]:
        """Create the configuration that will be passed to the Kubernetes REST API."""

        try:
            system_paasta_config = load_system_paasta_config()
            docker_url = self.get_docker_url()
            git_sha = get_git_sha_from_dockerurl(docker_url, long=True)
            complete_config: Union[V1StatefulSet, V1Deployment]
            if self.get_persistent_volumes():
                complete_config = V1StatefulSet(
                    api_version="apps/v1",
                    kind="StatefulSet",
                    metadata=self.get_kubernetes_metadata(git_sha),
                    spec=V1StatefulSetSpec(
                        service_name=self.get_sanitised_deployment_name(),
                        volume_claim_templates=self.get_volume_claim_templates(),
                        replicas=self.get_desired_instances(),
                        revision_history_limit=0,
                        selector=V1LabelSelector(
                            match_labels={
                                "paasta.yelp.com/service": self.get_service(),
                                "paasta.yelp.com/instance": self.get_instance(),
                            }
                        ),
                        template=self.get_pod_template_spec(
                            git_sha=git_sha, system_paasta_config=system_paasta_config
                        ),
                        pod_management_policy=self.get_pod_management_policy(),
                    ),
                )
            else:
                complete_config = V1Deployment(
                    api_version="apps/v1",
                    kind="Deployment",
                    metadata=self.get_kubernetes_metadata(git_sha),
                    spec=V1DeploymentSpec(
                        replicas=self.get_desired_instances(),
                        min_ready_seconds=self.get_min_task_uptime(),
                        selector=V1LabelSelector(
                            match_labels={
                                "paasta.yelp.com/service": self.get_service(),
                                "paasta.yelp.com/instance": self.get_instance(),
                            }
                        ),
                        revision_history_limit=0,
                        template=self.get_pod_template_spec(
                            git_sha=git_sha, system_paasta_config=system_paasta_config
                        ),
                        strategy=self.get_deployment_strategy_config(),
                    ),
                )

            prometheus_shard = self.get_prometheus_shard()
            if prometheus_shard:
                complete_config.metadata.labels[
                    "paasta.yelp.com/prometheus_shard"
                ] = prometheus_shard

            image_version = self.get_image_version()
            if image_version is not None:
                complete_config.metadata.labels[
                    "paasta.yelp.com/image_version"
                ] = image_version

            # DO NOT ADD LABELS AFTER THIS LINE
            config_hash = get_config_hash(
                self.sanitize_for_config_hash(complete_config),
                force_bounce=self.get_force_bounce(),
            )
            complete_config.metadata.labels["yelp.com/paasta_config_sha"] = config_hash
            complete_config.metadata.labels["paasta.yelp.com/config_sha"] = config_hash

            complete_config.spec.template.metadata.labels[
                "yelp.com/paasta_config_sha"
            ] = config_hash
            complete_config.spec.template.metadata.labels[
                "paasta.yelp.com/config_sha"
            ] = config_hash
        except Exception as e:
            raise InvalidKubernetesConfig(e, self.get_service(), self.get_instance())
        log.debug("Complete configuration for instance is: %s", complete_config)
        return complete_config

    def get_kubernetes_service_account_name(self) -> Optional[str]:
        return self.config_dict.get("service_account_name", None)

    def is_istio_sidecar_injection_enabled(self) -> bool:
        return self.config_dict.get("is_istio_sidecar_injection_enabled", False)

    def has_routable_ip(
        self,
        service_namespace_config: ServiceNamespaceConfig,
        system_paasta_config: SystemPaastaConfig,
    ) -> str:
        """Return whether the routable_ip label should be true or false.

        Services with a `prometheus_port` defined or that use the uwsgi_exporter sidecar must have a routable IP
        address to allow Prometheus shards to scrape metrics.
        """
        if (
            self.config_dict.get("routable_ip", False)
            or service_namespace_config.is_in_smartstack()
            or self.get_prometheus_port() is not None
            or self.should_run_uwsgi_exporter_sidecar(system_paasta_config)
        ):
            return "true"
        return "false"

    def get_pod_template_spec(
        self, git_sha: str, system_paasta_config: SystemPaastaConfig
    ) -> V1PodTemplateSpec:
        service_namespace_config = load_service_namespace_config(
            service=self.service, namespace=self.get_nerve_namespace()
        )
        docker_volumes = self.get_volumes(
            system_volumes=system_paasta_config.get_volumes()
        )
        hacheck_sidecar_volumes = system_paasta_config.get_hacheck_sidecar_volumes()
        has_routable_ip = self.has_routable_ip(
            service_namespace_config, system_paasta_config
        )
        annotations: KubePodAnnotations = {
            "smartstack_registrations": json.dumps(self.get_registrations()),
            "paasta.yelp.com/routable_ip": has_routable_ip,
        }
        metrics_provider = self.get_autoscaling_params()["metrics_provider"]

        # The HPAMetrics collector needs these annotations to tell it to pull
        # metrics from these pods
        # TODO: see if we can remove this as we're no longer using sfx data to scale
        if metrics_provider == "uwsgi":
            annotations["autoscaling"] = metrics_provider

        pod_spec_kwargs = {}
        pod_spec_kwargs.update(system_paasta_config.get_pod_defaults())
        pod_spec_kwargs.update(
            service_account_name=self.get_kubernetes_service_account_name(),
            containers=self.get_kubernetes_containers(
                docker_volumes=docker_volumes,
                hacheck_sidecar_volumes=hacheck_sidecar_volumes,
                aws_ebs_volumes=self.get_aws_ebs_volumes(),
                secret_volumes=self.get_secret_volumes(),
                system_paasta_config=system_paasta_config,
                service_namespace_config=service_namespace_config,
            ),
            share_process_namespace=True,
            node_selector=self.get_node_selector(),
            restart_policy="Always",
            volumes=self.get_pod_volumes(
                docker_volumes=docker_volumes + hacheck_sidecar_volumes,
                aws_ebs_volumes=self.get_aws_ebs_volumes(),
                secret_volumes=self.get_secret_volumes(),
            ),
        )
        # need to check if there are node selectors/affinities. if there are none
        # and we create an empty affinity object, k8s will deselect all nodes.
        node_affinity = self.get_node_affinity()
        if node_affinity is not None:
            pod_spec_kwargs["affinity"] = V1Affinity(node_affinity=node_affinity)

        pod_anti_affinity = self.get_pod_anti_affinity()
        if pod_anti_affinity is not None:
            affinity = pod_spec_kwargs.get("affinity", V1Affinity())
            affinity.pod_anti_affinity = pod_anti_affinity
            pod_spec_kwargs["affinity"] = affinity

        termination_grace_period = self.get_termination_grace_period()
        if termination_grace_period is not None:
            pod_spec_kwargs[
                "termination_grace_period_seconds"
            ] = termination_grace_period

        if self.get_iam_role_provider() == "aws":
            annotations["iam.amazonaws.com/role"] = ""
            iam_role = self.get_iam_role()
            if iam_role:
                pod_spec_kwargs[
                    "service_account_name"
                ] = create_or_find_service_account_name(iam_role)
                # PAASTA-16919: remove everything related to fs_group when
                # https://github.com/aws/amazon-eks-pod-identity-webhook/issues/8
                # will be fixed.
                fs_group = self.get_fs_group()
                if fs_group is None:
                    # We need some reasoable default for group id of a process
                    # running inside the container.  Seems like most of such
                    # programs run as `nobody`, let's use that as a default.
                    fs_group = 65534
                pod_spec_kwargs["security_context"] = V1PodSecurityContext(
                    fs_group=fs_group
                )
        else:
            annotations["iam.amazonaws.com/role"] = self.get_iam_role()

        # prometheus_path is used to override the default scrape path in Prometheus
        prometheus_path = self.get_prometheus_path()
        if prometheus_path:
            annotations["paasta.yelp.com/prometheus_path"] = prometheus_path

        # prometheus_port is used to override the default scrape port in Prometheus
        prometheus_port = self.get_prometheus_port()
        if prometheus_port:
            annotations["paasta.yelp.com/prometheus_port"] = str(prometheus_port)

        # Default Pod labels
        labels: KubePodLabels = {
            "yelp.com/paasta_service": self.get_service(),
            "yelp.com/paasta_instance": self.get_instance(),
            "yelp.com/paasta_git_sha": git_sha,
            # NOTE: we can't use the paasta_prefixed() helper here
            # since mypy expects TypedDict keys to be string literals
            "paasta.yelp.com/service": self.get_service(),
            "paasta.yelp.com/instance": self.get_instance(),
            "paasta.yelp.com/git_sha": git_sha,
            "paasta.yelp.com/autoscaled": str(self.is_autoscaling_enabled()).lower(),
        }

        # Allow the Prometheus Operator's Pod Service Monitor for specified
        # shard to find this pod
        prometheus_shard = self.get_prometheus_shard()
        if prometheus_shard:
            labels["paasta.yelp.com/prometheus_shard"] = prometheus_shard

        image_version = self.get_image_version()
        if image_version is not None:
            labels["paasta.yelp.com/image_version"] = image_version

        if system_paasta_config.get_kubernetes_add_registration_labels():
            # Allow Kubernetes Services to easily find
            # pods belonging to a certain smartstack namespace
            for registration in self.get_registrations():
                labels[registration_label(registration)] = "true"  # type: ignore

        if self.is_istio_sidecar_injection_enabled():
            labels["sidecar.istio.io/inject"] = "true"

        # not all services use uwsgi autoscaling, so we label those that do in order to have
        # prometheus selectively discover/scrape them
        if self.should_run_uwsgi_exporter_sidecar(
            system_paasta_config=system_paasta_config
        ):
            # this is kinda silly, but k8s labels must be strings
            labels["paasta.yelp.com/scrape_uwsgi_prometheus"] = "true"

            # this should probably eventually be made into a default label,
            # but for now we're fine with it being behind this feature toggle.
            # ideally, we'd also have the docker image here for ease-of-use
            # in Prometheus relabeling, but that information is over the
            # character limit for k8s labels (63 chars)
            labels["paasta.yelp.com/deploy_group"] = self.get_deploy_group()

        elif self.should_setup_piscina_prometheus_scraping():
            labels["paasta.yelp.com/deploy_group"] = self.get_deploy_group()
            labels["paasta.yelp.com/scrape_piscina_prometheus"] = "true"

        return V1PodTemplateSpec(
            metadata=V1ObjectMeta(
                labels=labels,
                annotations=annotations,
            ),
            spec=V1PodSpec(**pod_spec_kwargs),
        )

    def get_node_selector(self) -> Mapping[str, str]:
        """Converts simple node restrictions into node selectors. Unlike node
        affinities, selectors will show up in `kubectl describe`.
        """
        raw_selectors: Mapping[str, Any] = self.config_dict.get("node_selectors", {})
        node_selectors = {
            to_node_label(label): value
            for label, value in raw_selectors.items()
            if type(value) is str
        }
        node_selectors["yelp.com/pool"] = self.get_pool()
        return node_selectors

    def get_node_affinity(self) -> Optional[V1NodeAffinity]:
        """Converts deploy_whitelist and deploy_blacklist in node affinities.

        note: At the time of writing, `kubectl describe` does not show affinities,
        only selectors. To see affinities, use `kubectl get pod -o json` instead.
        """
        requirements = allowlist_denylist_to_requirements(
            allowlist=self.get_deploy_whitelist(),
            denylist=self.get_deploy_blacklist(),
        )
        requirements.extend(
            raw_selectors_to_requirements(
                raw_selectors=self.config_dict.get("node_selectors", {}),
            )
        )
        # package everything into a node affinity - lots of layers :P
        if len(requirements) == 0:
            return None
        term = V1NodeSelectorTerm(
            match_expressions=[
                V1NodeSelectorRequirement(
                    key=key,
                    operator=op,
                    values=vs,
                )
                for key, op, vs in requirements
            ]
        )
        selector = V1NodeSelector(node_selector_terms=[term])
        return V1NodeAffinity(
            # this means that the selectors are only used during scheduling.
            # changing it while the pod is running will not cause an eviction.
            # this should be fine since if there are whitelist/blacklist config
            # changes, we will bounce anyway.
            required_during_scheduling_ignored_during_execution=selector,
        )

    def get_pod_anti_affinity(self) -> Optional[V1PodAntiAffinity]:
        """
        Converts the given anti-affinity on service and instance to pod
        affinities with the "paasta.yelp.com" prefixed label selector
        :return:
        """
        conditions = self.config_dict.get("anti_affinity", [])
        if not conditions:
            return None

        if not isinstance(conditions, list):
            conditions = [conditions]

        affinity_terms = []
        for condition in conditions:
            label_selector = self._kube_affinity_condition_to_label_selector(condition)
            if label_selector:
                affinity_terms.append(
                    V1PodAffinityTerm(
                        # Topology of a hostname means the pod of this service
                        # cannot be scheduled on host containing another pod
                        # matching the label_selector
                        topology_key="kubernetes.io/hostname",
                        label_selector=label_selector,
                    )
                )

        return V1PodAntiAffinity(
            required_during_scheduling_ignored_during_execution=affinity_terms
        )

    def _kube_affinity_condition_to_label_selector(
        self, condition: KubeAffinityCondition
    ) -> Optional[V1LabelSelector]:
        """Converts the given condition to label selectors with paasta prefix"""
        labels = {}
        if "service" in condition:
            labels[PAASTA_ATTRIBUTE_PREFIX + "service"] = condition.get("service")
        if "instance" in condition:
            labels[PAASTA_ATTRIBUTE_PREFIX + "instance"] = condition.get("instance")
        return V1LabelSelector(match_labels=labels) if labels else None

    def sanitize_for_config_hash(
        self, config: Union[V1Deployment, V1StatefulSet]
    ) -> Mapping[str, Any]:
        """Removes some data from config to make it suitable for
        calculation of config hash.

        :param config: complete_config hash to sanitise
        :returns: sanitised copy of complete_config hash
        """
        ahash = config.to_dict()  # deep convert to dict
        ahash["paasta_secrets"] = get_kubernetes_secret_hashes(
            service=self.get_service(), environment_variables=self.get_env()
        )

        # remove data we dont want used to hash configs
        # replica count
        if ahash["spec"] is not None:
            del ahash["spec"]["replicas"]
        # soa-configs SHA
        try:
            for container in ahash["spec"]["template"]["spec"]["containers"]:
                container["env"] = [
                    e
                    for e in container["env"]
                    if e.get("name", "") != "PAASTA_SOA_CONFIGS_SHA"
                ]
        except TypeError:  # any of the values can be None
            pass

        return ahash

    def get_termination_grace_period(self) -> Optional[int]:
        return self.config_dict.get("lifecycle", KubeLifecycleDict({})).get(
            "termination_grace_period_seconds"
        )

    def get_prometheus_shard(self) -> Optional[str]:
        return self.config_dict.get("prometheus_shard")

    def get_prometheus_path(self) -> Optional[str]:
        return self.config_dict.get("prometheus_path")

    def get_prometheus_port(self) -> Optional[int]:
        return self.config_dict.get("prometheus_port")


def get_kubernetes_secret_hashes(
    environment_variables: Mapping[str, str], service: str
) -> Mapping[str, str]:
    hashes = {}
    to_get_hash = []
    for v in environment_variables.values():
        if is_secret_ref(v):
            to_get_hash.append(v)
    if to_get_hash:
        kube_client = KubeClient()
        for value in to_get_hash:
            hashes[value] = get_kubernetes_secret_signature(
                kube_client=kube_client,
                secret=get_secret_name_from_ref(value),
                service=SHARED_SECRET_SERVICE if is_shared_secret(value) else service,
            )
    return hashes


def get_k8s_pods() -> Mapping[str, Any]:
    return requests.get("http://127.0.0.1:10255/pods").json()


def get_all_kubernetes_services_running_here() -> List[Tuple[str, str, int]]:
    """Returns all k8s paasta services, even if not in smartstack. Returns a service, instance, port
    tuple to match the return value of other similar functions"""
    services = []
    try:
        pods = get_k8s_pods()
    except requests.exceptions.ConnectionError:
        log.debug("Failed to connect to the kublet when trying to get pods")
        return []
    for pod in pods["items"]:
        try:
            service = pod["metadata"]["labels"]["paasta.yelp.com/service"]
            instance = pod["metadata"]["labels"]["paasta.yelp.com/instance"]
            services.append((service, instance, 0))
        except KeyError:
            log.debug(f"Skipping listing what looks like a non-paasta pod: {pod}")
    return services


def get_kubernetes_services_running_here() -> Sequence[KubernetesServiceRegistration]:
    services = []
    pods = get_k8s_pods()
    for pod in pods["items"]:
        if pod["status"]["phase"] != "Running" or "smartstack_registrations" not in pod[
            "metadata"
        ].get("annotations", {}):
            continue
        try:
            port = None
            for container in pod["spec"]["containers"]:
                if container["name"] != HACHECK_POD_NAME:
                    port = container["ports"][0]["containerPort"]
                    break
            services.append(
                KubernetesServiceRegistration(
                    name=pod["metadata"]["labels"]["paasta.yelp.com/service"],
                    instance=pod["metadata"]["labels"]["paasta.yelp.com/instance"],
                    port=port,
                    pod_ip=pod["status"]["podIP"],
                    registrations=json.loads(
                        pod["metadata"]["annotations"]["smartstack_registrations"]
                    ),
                )
            )
        except KeyError as e:
            log.warning(
                f"Found running paasta pod but missing {e} key so not registering with nerve"
            )
    return services


def get_kubernetes_services_running_here_for_nerve(
    cluster: Optional[str], soa_dir: str
) -> Sequence[Tuple[str, ServiceNamespaceConfig]]:
    try:
        system_paasta_config = load_system_paasta_config()
        if not cluster:
            cluster = system_paasta_config.get_cluster()
        # In the cases where there is *no* cluster or in the case
        # where there isn't a Paasta configuration file at *all*, then
        # there must be no kubernetes services running here, so we catch
        # these custom exceptions and return [].
        if not system_paasta_config.get_register_k8s_pods():
            return []
    except PaastaNotConfiguredError:
        log.warning("No PaaSTA config so skipping registering k8s pods in nerve")
        return []
    kubernetes_services = get_kubernetes_services_running_here()
    nerve_list = []
    for kubernetes_service in kubernetes_services:
        try:
            for registration in kubernetes_service.registrations:
                reg_service, reg_namespace, _, __ = decompose_job_id(registration)
                try:
                    nerve_dict = load_service_namespace_config(
                        service=reg_service, namespace=reg_namespace, soa_dir=soa_dir
                    )
                except Exception as e:
                    log.warning(str(e))
                    log.warning(
                        f"Could not get smartstack config for {reg_service}.{reg_namespace}, skipping"
                    )
                    # but the show must go on!
                    continue
                if not nerve_dict.is_in_smartstack():
                    continue
                nerve_dict["port"] = kubernetes_service.port
                nerve_dict["service_ip"] = kubernetes_service.pod_ip
                if system_paasta_config.get_kubernetes_use_hacheck_sidecar():
                    nerve_dict["hacheck_ip"] = kubernetes_service.pod_ip
                else:
                    nerve_dict["extra_healthcheck_headers"] = {
                        "X-Nerve-Check-IP": kubernetes_service.pod_ip
                    }
                nerve_list.append((registration, nerve_dict))
        except (KeyError):
            continue  # SOA configs got deleted for this app, it'll get cleaned up

    return nerve_list


def force_delete_pods(
    service: str,
    paasta_service: str,
    instance: str,
    namespace: str,
    kube_client: KubeClient,
) -> None:
    # Note that KubeClient.deployments.delete_namespaced_deployment must be called prior to this method.
    pods_to_delete = a_sync.block(
        pods_for_service_instance,
        paasta_service,
        instance,
        kube_client,
    )
    delete_options = V1DeleteOptions()
    for pod in pods_to_delete:
        kube_client.core.delete_namespaced_pod(
            pod.metadata.name, namespace, body=delete_options, grace_period_seconds=0
        )


def get_all_namespaces(kube_client: KubeClient) -> List[str]:
    namespaces = kube_client.core.list_namespace()
    return [item.metadata.name for item in namespaces.items]


def ensure_namespace(kube_client: KubeClient, namespace: str) -> None:
    paasta_namespace = V1Namespace(
        metadata=V1ObjectMeta(name=namespace, labels={"name": namespace})
    )
    namespaces = kube_client.core.list_namespace()
    namespace_names = [item.metadata.name for item in namespaces.items]
    if namespace not in namespace_names:
        log.warning(f"Creating namespace: {namespace} as it does not exist")
        kube_client.core.create_namespace(body=paasta_namespace)


def list_deployments(
    kube_client: KubeClient, label_selector: str = ""
) -> Sequence[KubeDeployment]:
    deployments = kube_client.deployments.list_namespaced_deployment(
        namespace="paasta", label_selector=label_selector
    )
    stateful_sets = kube_client.deployments.list_namespaced_stateful_set(
        namespace="paasta", label_selector=label_selector
    )
    return [
        KubeDeployment(
            service=item.metadata.labels["paasta.yelp.com/service"],
            instance=item.metadata.labels["paasta.yelp.com/instance"],
            git_sha=item.metadata.labels.get("paasta.yelp.com/git_sha", ""),
            image_version=item.metadata.labels.get(
                "paasta.yelp.com/image_version", None
            ),
            config_sha=item.metadata.labels["paasta.yelp.com/config_sha"],
            replicas=item.spec.replicas
            if item.metadata.labels.get(paasta_prefixed("autoscaled"), "false")
            == "false"
            else None,
        )
        for item in deployments.items + stateful_sets.items
    ]


def recent_container_restart(
    restart_count: int,
    last_state: Optional[str],
    last_timestamp: Optional[int],
    time_window_s: int = 900,  # 15 mins
) -> bool:
    min_timestamp = datetime.now().timestamp() - time_window_s
    return (
        restart_count > 0
        and last_state == "terminated"
        and last_timestamp is not None
        and last_timestamp > min_timestamp
    )


@async_timeout()
async def get_tail_lines_for_kubernetes_container(
    kube_client: KubeClient,
    pod: V1Pod,
    container: V1ContainerStatus,
    num_tail_lines: int,
    previous: bool = False,
) -> MutableMapping[str, Any]:
    tail_lines: MutableMapping[str, Any] = {
        "stdout": [],
        "stderr": [],
        "error_message": "",
    }

    if container.name != HACHECK_POD_NAME:
        error = ""
        if container.state.waiting:
            error = container.state.waiting.message or ""
        elif container.state.terminated:
            error = container.state.terminated.message or ""
        tail_lines["error_message"] = error

        try:
            if num_tail_lines > 0:
                log = kube_client.core.read_namespaced_pod_log(
                    name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
                    container=container.name,
                    tail_lines=num_tail_lines,
                    previous=previous,
                )
                tail_lines["stdout"].extend(log.split("\n"))
        except ApiException as e:
            # there is a potential race condition in which a pod's containers
            # have not failed, but have when we get the container's logs. in this
            # case, use the error from the exception, though it is less accurate.
            if error == "":
                body = json.loads(e.body)
                error = body.get("message", "")
            tail_lines["error_message"] = f"couldn't read stdout/stderr: '{error}'"

    return tail_lines


async def get_pod_event_messages(
    kube_client: KubeClient, pod: V1Pod, max_age_in_seconds: Optional[int] = None
) -> List[Dict]:
    pod_events = await get_events_for_object(
        kube_client, pod, "Pod", max_age_in_seconds
    )
    pod_event_messages = []
    if pod_events:
        for event in pod_events:
            message = {
                "message": event.message,
                "timeStamp": str(event.last_timestamp),
            }
            pod_event_messages.append(message)
    return pod_event_messages


def format_pod_event_messages(
    pod_event_messages: List[Dict], pod_name: str
) -> List[str]:
    rows: List[str] = list()
    rows.append(PaastaColors.blue(f"  Pod Events for {pod_name}"))
    for message in pod_event_messages:
        if "error" in message:
            rows.append(PaastaColors.yellow(f'   Error: {message["error"]}'))
        else:
            timestamp = message.get("time_stamp", "unknown time")
            message_text = message.get("message", "")
            rows.append(f"    Event at {timestamp}: {message_text}")
    return rows


def format_tail_lines_for_kubernetes_pod(
    pod_containers: Sequence,
    pod_name: str,
) -> List[str]:
    errors: List[str] = []
    lines: List[str] = []
    tail_line_prefixes = (
        ("tail_lines", "current"),
        ("previous_tail_lines", "previous (pre-restart)"),
    )

    for container in pod_containers:
        for tail_line_key, stream_prefix in tail_line_prefixes:
            tail_lines = getattr(container, tail_line_key, None)
            if tail_lines is None:
                break
            if tail_lines.error_message:
                errors.append(PaastaColors.red(f"    {tail_lines.error_message}"))

            for stream_name in ("stdout", "stderr"):
                stream_lines = getattr(tail_lines, stream_name, [])
                if len(stream_lines) > 0:
                    lines.append(
                        PaastaColors.blue(
                            f"  {stream_prefix} {stream_name} tail for {container.name} "
                            f"in pod {pod_name}"
                        )
                    )
                    lines.extend(f"    {line}" for line in stream_lines)

    rows: List[str] = []
    if errors:
        rows.append(
            PaastaColors.blue(
                f"  errors for container {container.name} in pod {pod_name}"
            )
        )
        rows.extend(errors)
        rows.append("")
    rows.extend(lines)
    return rows


def create_custom_resource(
    kube_client: KubeClient,
    formatted_resource: Mapping[str, Any],
    version: str,
    kind: KubeKind,
    group: str,
) -> None:
    return kube_client.custom.create_namespaced_custom_object(
        group=group,
        version=version,
        namespace=f"paasta-{kind.plural}",
        plural=kind.plural,
        body=formatted_resource,
    )


def update_custom_resource(
    kube_client: KubeClient,
    formatted_resource: Mapping[str, Any],
    version: str,
    name: str,
    kind: KubeKind,
    group: str,
) -> None:
    co = kube_client.custom.get_namespaced_custom_object(
        name=name,
        group=group,
        version=version,
        namespace=f"paasta-{kind.plural}",
        plural=kind.plural,
    )
    formatted_resource["metadata"]["resourceVersion"] = co["metadata"][
        "resourceVersion"
    ]
    return kube_client.custom.replace_namespaced_custom_object(
        name=name,
        group=group,
        version=version,
        namespace=f"paasta-{kind.plural}",
        plural=kind.plural,
        body=formatted_resource,
    )


def list_custom_resources(
    kind: KubeKind,
    version: str,
    kube_client: KubeClient,
    group: str,
    label_selector: str = "",
) -> Sequence[KubeCustomResource]:
    crs = kube_client.custom.list_namespaced_custom_object(
        group=group,
        version=version,
        label_selector=label_selector,
        plural=kind.plural,
        namespace=f"paasta-{kind.plural}",
    )
    kube_custom_resources = []
    for cr in crs["items"]:
        try:
            kube_custom_resources.append(
                KubeCustomResource(
                    service=cr["metadata"]["labels"]["paasta.yelp.com/service"],
                    instance=cr["metadata"]["labels"]["paasta.yelp.com/instance"],
                    config_sha=cr["metadata"]["labels"]["paasta.yelp.com/config_sha"],
                    git_sha=cr["metadata"]["labels"].get("paasta.yelp.com/git_sha", ""),
                    kind=cr["kind"],
                    namespace=cr["metadata"]["namespace"],
                    name=cr["metadata"]["name"],
                )
            )
        except KeyError as e:
            log.debug(
                f"Ignoring custom resource that is missing paasta label {e}: {cr}"
            )
            continue
    return kube_custom_resources


def delete_custom_resource(
    kube_client: KubeClient,
    name: str,
    namespace: str,
    group: str,
    version: str,
    plural: str,
) -> None:
    return kube_client.custom.delete_namespaced_custom_object(
        name=name,
        namespace=namespace,
        group=group,
        version=version,
        plural=plural,
        body=V1DeleteOptions(),
    )


def max_unavailable(instance_count: int, bounce_margin_factor: float) -> int:
    if instance_count == 0:
        return 0
    else:
        return max(
            instance_count - int(math.ceil(instance_count * bounce_margin_factor)), 1
        )


def pod_disruption_budget_for_service_instance(
    service: str,
    instance: str,
    max_unavailable: Union[str, int],
) -> V1beta1PodDisruptionBudget:
    return V1beta1PodDisruptionBudget(
        metadata=V1ObjectMeta(
            name=get_kubernetes_app_name(service, instance),
            namespace="paasta",
        ),
        spec=V1beta1PodDisruptionBudgetSpec(
            max_unavailable=max_unavailable,
            selector=V1LabelSelector(
                match_labels={
                    "paasta.yelp.com/service": service,
                    "paasta.yelp.com/instance": instance,
                }
            ),
        ),
    )


def create_pod_disruption_budget(
    kube_client: KubeClient, pod_disruption_budget: V1beta1PodDisruptionBudget
) -> None:
    return kube_client.policy.create_namespaced_pod_disruption_budget(
        namespace="paasta", body=pod_disruption_budget
    )


def set_instances_for_kubernetes_service(
    kube_client: KubeClient,
    service_config: KubernetesDeploymentConfig,
    instance_count: int,
) -> None:
    name = service_config.get_sanitised_deployment_name()
    formatted_application = service_config.format_kubernetes_app()
    formatted_application.spec.replicas = instance_count
    if service_config.get_persistent_volumes():
        kube_client.deployments.patch_namespaced_stateful_set_scale(
            name=name, namespace="paasta", body=formatted_application
        )
    else:
        kube_client.deployments.patch_namespaced_deployment_scale(
            name=name, namespace="paasta", body=formatted_application
        )


def get_annotations_for_kubernetes_service(
    kube_client: KubeClient, service_config: KubernetesDeploymentConfig
) -> Dict:
    name = service_config.get_sanitised_deployment_name()
    if service_config.get_persistent_volumes():
        k8s_service = kube_client.deployments.read_namespaced_stateful_set(
            name=name, namespace="paasta"
        )
    else:
        k8s_service = kube_client.deployments.read_namespaced_deployment(
            name=name, namespace="paasta"
        )
    return k8s_service.metadata.annotations if k8s_service.metadata.annotations else {}


def write_annotation_for_kubernetes_service(
    kube_client: KubeClient,
    service_config: KubernetesDeploymentConfig,
    formatted_application: Union[V1Deployment, V1StatefulSet],
    annotation: Dict,
) -> None:
    name = formatted_application.metadata.name
    formatted_application.metadata.annotations = annotation
    if service_config.get_persistent_volumes():
        kube_client.deployments.patch_namespaced_stateful_set(
            name=name, namespace="paasta", body=formatted_application
        )
    else:
        kube_client.deployments.patch_namespaced_deployment(
            name=name, namespace="paasta", body=formatted_application
        )


def list_all_deployments(kube_client: KubeClient) -> Sequence[KubeDeployment]:
    return list_deployments(kube_client)


def list_matching_deployments(
    service: str, instance: str, kube_client: KubeClient
) -> Sequence[KubeDeployment]:
    return list_deployments(
        kube_client,
        f"paasta.yelp.com/service={service},paasta.yelp.com/instance={instance}",
    )


@async_timeout()
async def replicasets_for_service_instance(
    service: str, instance: str, kube_client: KubeClient, namespace: str = "paasta"
) -> Sequence[V1ReplicaSet]:
    async_list_replica_set = a_sync.to_async(
        kube_client.deployments.list_namespaced_replica_set
    )
    response = await async_list_replica_set(
        label_selector=f"paasta.yelp.com/service={service},paasta.yelp.com/instance={instance}",
        namespace=namespace,
    )
    return response.items


@async_timeout()
async def controller_revisions_for_service_instance(
    service: str, instance: str, kube_client: KubeClient, namespace: str = "paasta"
) -> Sequence[V1ControllerRevision]:
    async_list_controller_revisions = a_sync.to_async(
        kube_client.deployments.list_namespaced_controller_revision
    )
    response = await async_list_controller_revisions(
        label_selector=f"paasta.yelp.com/service={service},paasta.yelp.com/instance={instance}",
        namespace=namespace,
    )
    return response.items


@async_timeout(15)
async def pods_for_service_instance(
    service: str, instance: str, kube_client: KubeClient, namespace: str = "paasta"
) -> Sequence[V1Pod]:
    async_list_pods = a_sync.to_async(kube_client.core.list_namespaced_pod)
    response = await async_list_pods(
        label_selector=f"paasta.yelp.com/service={service},paasta.yelp.com/instance={instance}",
        namespace=namespace,
    )
    return response.items


def get_pods_by_node(kube_client: KubeClient, node: V1Node) -> Sequence[V1Pod]:
    return kube_client.core.list_pod_for_all_namespaces(
        field_selector=f"spec.nodeName={node.metadata.name}"
    ).items


def get_all_pods(kube_client: KubeClient, namespace: str = "paasta") -> Sequence[V1Pod]:
    return kube_client.core.list_namespaced_pod(namespace=namespace).items


@time_cache(ttl=300)
def get_all_pods_cached(
    kube_client: KubeClient, namespace: str = "paasta"
) -> Sequence[V1Pod]:
    pods: Sequence[V1Pod] = get_all_pods(kube_client, namespace)
    return pods


def filter_pods_by_service_instance(
    pod_list: Sequence[V1Pod], service: str, instance: str
) -> Sequence[V1Pod]:
    return [
        pod
        for pod in pod_list
        if pod.metadata.labels is not None
        and pod.metadata.labels.get("paasta.yelp.com/service", "") == service
        and pod.metadata.labels.get("paasta.yelp.com/instance", "") == instance
    ]


def _is_it_ready(
    it: Union[V1Pod, V1Node],
) -> bool:
    ready_conditions = [
        cond.status == "True"
        for cond in it.status.conditions or []
        if cond.type == "Ready"
    ]
    return all(ready_conditions) if ready_conditions else False


is_pod_ready = _is_it_ready
is_node_ready = _is_it_ready


def is_pod_completed(pod: V1Pod) -> bool:
    condition = get_pod_condition(pod, "ContainersReady")
    return condition.reason == "PodCompleted" if condition else False


def is_pod_scheduled(pod: V1Pod) -> bool:
    scheduled_condition = get_pod_condition(pod, "PodScheduled")
    return scheduled_condition.status == "True" if scheduled_condition else False


def get_pod_condition(pod: V1Pod, condition: str) -> V1PodCondition:
    conditions = [
        cond for cond in pod.status.conditions or [] if cond.type == condition
    ]
    if conditions:
        return conditions[0]
    return None


class PodStatus(Enum):
    PENDING = (1,)
    RUNNING = (2,)
    SUCCEEDED = (3,)
    FAILED = (4,)
    UNKNOWN = (5,)


_POD_STATUS_NAME_TO_STATUS = {s.name.upper(): s for s in PodStatus}


def get_pod_status(
    pod: V1Pod,
) -> PodStatus:
    # TODO: we probably also need to deduce extended statuses here, like
    # `CrashLoopBackOff`, `ContainerCreating` timeout, and etc.
    return _POD_STATUS_NAME_TO_STATUS[pod.status.phase.upper()]


def parse_container_resources(resources: Mapping[str, str]) -> KubeContainerResources:
    cpu_str = resources.get("cpu")
    if not cpu_str:
        cpus = None
    elif cpu_str[-1] == "m":
        cpus = float(cpu_str[:-1]) / 1000
    else:
        cpus = float(cpu_str)

    mem_str = resources.get("memory")
    if not mem_str:
        mem_mb = None
    else:
        mem_mb = parse_size(mem_str) / 1000000

    disk_str = resources.get("ephemeral-storage")
    if not disk_str:
        disk_mb = None
    else:
        disk_mb = parse_size(disk_str) / 1000000

    return KubeContainerResources(cpus=cpus, mem=mem_mb, disk=disk_mb)


def get_active_shas_for_service(
    obj_list: Sequence[Union[V1Pod, V1ReplicaSet, V1Deployment, V1StatefulSet]],
) -> Set[Tuple[str, str]]:
    ret = set()

    for obj in obj_list:
        config_sha = obj.metadata.labels.get("paasta.yelp.com/config_sha")
        if config_sha and config_sha.startswith("config"):
            config_sha = config_sha[len("config") :]

        git_sha = obj.metadata.labels.get("paasta.yelp.com/git_sha")
        if git_sha and git_sha.startswith("git"):
            git_sha = git_sha[len("git") :]

        # Suppress entries where we have no clue what's running.
        if git_sha or config_sha:
            ret.add((git_sha, config_sha))
    return ret


def get_all_nodes(
    kube_client: KubeClient,
) -> Sequence[V1Node]:
    return kube_client.core.list_node().items


@time_cache(ttl=300)
def get_all_nodes_cached(kube_client: KubeClient) -> Sequence[V1Node]:
    nodes: Sequence[V1Node] = get_all_nodes(kube_client)
    return nodes


def filter_nodes_by_blacklist(
    nodes: Sequence[V1Node], blacklist: DeployBlacklist, whitelist: DeployWhitelist
) -> Sequence[V1Node]:
    """Takes an input list of nodes and filters them based on the given blacklist.
    The blacklist is in the form of:

        [["location_type", "location]]

    Where the list inside is something like ["region", "uswest1-prod"]

    :returns: The list of nodes after the filter
    """
    if whitelist:
        whitelist = (paasta_prefixed(whitelist[0]), whitelist[1])
    blacklist = [(paasta_prefixed(entry[0]), entry[1]) for entry in blacklist]
    return [
        node
        for node in nodes
        if host_passes_whitelist(node.metadata.labels, whitelist)
        and host_passes_blacklist(node.metadata.labels, blacklist)
    ]


def paasta_prefixed(
    attribute: str,
) -> str:
    # discovery attributes are exempt for now
    if attribute in DISCOVERY_ATTRIBUTES:
        return YELP_ATTRIBUTE_PREFIX + attribute
    elif "/" in attribute:
        return attribute
    else:
        return PAASTA_ATTRIBUTE_PREFIX + attribute


def get_nodes_grouped_by_attribute(
    nodes: Sequence[V1Node], attribute: str
) -> Mapping[str, Sequence[V1Node]]:
    attribute = paasta_prefixed(attribute)
    sorted_nodes = sorted(
        nodes, key=lambda node: node.metadata.labels.get(attribute, "")
    )
    return {
        key: list(group)
        for key, group in itertools.groupby(
            sorted_nodes, key=lambda node: node.metadata.labels.get(attribute, "")
        )
        if key
    }


def get_kubernetes_app_name(service: str, instance: str) -> str:
    return "{service}-{instance}".format(
        service=sanitise_kubernetes_name(service),
        instance=sanitise_kubernetes_name(instance),
    )


def get_kubernetes_app_by_name(
    name: str, kube_client: KubeClient, namespace: str = "paasta"
) -> Union[V1Deployment, V1StatefulSet]:
    try:
        app = kube_client.deployments.read_namespaced_deployment_status(
            name=name, namespace=namespace
        )
        return app
    except ApiException as e:
        if e.status == 404:
            pass
        else:
            raise
    return kube_client.deployments.read_namespaced_stateful_set_status(
        name=name, namespace=namespace
    )


def create_deployment(
    kube_client: KubeClient, formatted_deployment: V1Deployment
) -> None:
    return kube_client.deployments.create_namespaced_deployment(
        namespace="paasta", body=formatted_deployment
    )


def update_deployment(
    kube_client: KubeClient, formatted_deployment: V1Deployment
) -> None:
    return kube_client.deployments.replace_namespaced_deployment(
        name=formatted_deployment.metadata.name,
        namespace="paasta",
        body=formatted_deployment,
    )


def patch_deployment(
    kube_client: KubeClient, formatted_deployment: V1Deployment
) -> None:
    return kube_client.deployments.patch_namespaced_deployment(
        name=formatted_deployment.metadata.name,
        namespace="paasta",
        body=formatted_deployment,
    )


def delete_deployment(kube_client: KubeClient, deployment_name: str) -> None:
    return kube_client.deployments.delete_namespaced_deployment(
        name=deployment_name,
        namespace="paasta",
    )


def create_stateful_set(
    kube_client: KubeClient, formatted_stateful_set: V1StatefulSet
) -> None:
    return kube_client.deployments.create_namespaced_stateful_set(
        namespace="paasta", body=formatted_stateful_set
    )


def update_stateful_set(
    kube_client: KubeClient, formatted_stateful_set: V1StatefulSet
) -> None:
    return kube_client.deployments.replace_namespaced_stateful_set(
        name=formatted_stateful_set.metadata.name,
        namespace="paasta",
        body=formatted_stateful_set,
    )


def get_event_timestamp(event: V1Event) -> Optional[float]:
    # Cycle through timestamp attributes in order of preference
    for ts_attr in ["last_timestamp", "event_time", "first_timestamp"]:
        ts = getattr(event, ts_attr)
        if ts:
            return ts.timestamp()
    return None


@async_timeout()
async def get_events_for_object(
    kube_client: KubeClient,
    obj: Union[V1Pod, V1Deployment, V1StatefulSet, V1ReplicaSet],
    kind: str,  # for some reason, obj.kind isn't populated when this function is called so we pass it in by hand
    max_age_in_seconds: Optional[int] = None,
) -> List[V1Event]:

    try:
        # this is a blocking call since it does network I/O and can end up significantly blocking the
        # asyncio event loop when doing things like getting events for all the Pods for a service with
        # a large amount of replicas. therefore, we need to wrap the kubernetes client into something
        # that's awaitable so that we can actually do things concurrently and not serially
        events = await a_sync.to_async(kube_client.core.list_namespaced_event)(
            namespace=obj.metadata.namespace,
            field_selector=f"involvedObject.name={obj.metadata.name},involvedObject.kind={kind}",
            limit=MAX_EVENTS_TO_RETRIEVE,
        )
        events = events.items if events else []
        if max_age_in_seconds and max_age_in_seconds > 0:
            min_timestamp = datetime.now().timestamp() - max_age_in_seconds
            events = [
                evt
                for evt in events
                if get_event_timestamp(evt) is None
                or get_event_timestamp(evt) > min_timestamp
            ]
        return events
    except ApiException:
        return []


@async_timeout()
async def get_hpa(
    kube_client: KubeClient,
    name: str,
    namespace: str,
) -> V2beta2HorizontalPodAutoscaler:
    async_get_hpa = a_sync.to_async(
        kube_client.autoscaling.read_namespaced_horizontal_pod_autoscaler
    )
    try:
        return await async_get_hpa(name, namespace)
    except ApiException as e:
        if e.status == 404:
            return None
        else:
            raise


def get_kubernetes_app_deploy_status(
    app: Union[V1Deployment, V1StatefulSet],
    desired_instances: int,
) -> Tuple[int, str]:
    if app.status.ready_replicas is None:
        if desired_instances == 0:
            deploy_status = KubernetesDeployStatus.Stopped
        else:
            deploy_status = KubernetesDeployStatus.Waiting
    elif app.status.ready_replicas != desired_instances:
        deploy_status = KubernetesDeployStatus.Waiting
    # updated_replicas can currently be None for stateful sets so we may not correctly detect status for now
    # when https://github.com/kubernetes/kubernetes/pull/62943 lands in a release this should work for both:
    elif app.status.updated_replicas is not None and (
        app.status.updated_replicas < desired_instances
    ):
        deploy_status = KubernetesDeployStatus.Deploying
    elif app.status.replicas == 0 and desired_instances == 0:
        deploy_status = KubernetesDeployStatus.Stopped
    else:
        deploy_status = KubernetesDeployStatus.Running
    # Temporarily removing the message because the events query it used was overloading etcd
    # TODO: change the implementation or remove the deploy message entirely
    deploy_message = ""
    return deploy_status, deploy_message


class KubernetesDeployStatus:
    """An enum to represent Kubernetes app deploy status.
    Changing name of the keys will affect both the paasta CLI and API.
    """

    Running, Deploying, Waiting, Stopped = range(0, 4)

    @classmethod
    def tostring(cls, val: int) -> str:
        for k, v in vars(cls).items():
            if v == val:
                return k
        raise ValueError("Unknown Kubernetes deploy status %d" % val)

    @classmethod
    def fromstring(cls, _str: str) -> int:
        return getattr(cls, _str, None)


def is_kubernetes_available() -> bool:
    return Path(os.environ.get("KUBECONFIG", KUBE_CONFIG_PATH)).exists()


def create_secret(
    kube_client: KubeClient,
    secret: str,
    service: str,
    secret_provider: BaseSecretProvider,
    namespace: str = "paasta",
) -> None:
    service = sanitise_kubernetes_name(service)
    sanitised_secret = sanitise_kubernetes_name(secret)
    kube_client.core.create_namespaced_secret(
        namespace=namespace,
        body=V1Secret(
            metadata=V1ObjectMeta(
                name=f"{namespace}-secret-{service}-{sanitised_secret}",
                labels={
                    "yelp.com/paasta_service": service,
                    "paasta.yelp.com/service": service,
                },
            ),
            data={
                secret: base64.b64encode(
                    secret_provider.decrypt_secret_raw(secret)
                ).decode("utf-8")
            },
        ),
    )


def create_plaintext_dict_secret(
    kube_client: KubeClient,
    secret_name: str,
    secret_data: dict,
    service: str,
    namespace: str = "paasta",
) -> None:
    service = sanitise_kubernetes_name(service)
    sanitised_secret = sanitise_kubernetes_name(secret_name)
    kube_client.core.create_namespaced_secret(
        namespace=namespace,
        body=V1Secret(
            metadata=V1ObjectMeta(
                name=sanitised_secret,
                labels={
                    "yelp.com/paasta_service": service,
                    "paasta.yelp.com/service": service,
                },
            ),
            data=secret_data,
        ),
    )


def update_secret(
    kube_client: KubeClient,
    secret: str,
    service: str,
    secret_provider: BaseSecretProvider,
    namespace: str = "paasta",
) -> None:
    service = sanitise_kubernetes_name(service)
    sanitised_secret = sanitise_kubernetes_name(secret)
    kube_client.core.replace_namespaced_secret(
        name=f"{namespace}-secret-{service}-{sanitised_secret}",
        namespace=namespace,
        body=V1Secret(
            metadata=V1ObjectMeta(
                name=f"{namespace}-secret-{service}-{sanitised_secret}",
                labels={
                    "yelp.com/paasta_service": service,
                    "paasta.yelp.com/service": service,
                },
            ),
            data={
                secret: base64.b64encode(
                    secret_provider.decrypt_secret_raw(secret)
                ).decode("utf-8")
            },
        ),
    )


def update_plaintext_dict_secret(
    kube_client: KubeClient,
    secret_name: str,
    secret_data: dict,
    service: str,
    namespace: str = "paasta",
) -> None:
    service = sanitise_kubernetes_name(service)
    sanitised_secret = sanitise_kubernetes_name(secret_name)
    kube_client.core.replace_namespaced_secret(
        name=sanitised_secret,
        namespace=namespace,
        body=V1Secret(
            metadata=V1ObjectMeta(
                name=sanitised_secret,
                labels={
                    "yelp.com/paasta_service": service,
                    "paasta.yelp.com/service": service,
                },
            ),
            data=secret_data,
        ),
    )


def get_kubernetes_secret_signature(
    kube_client: KubeClient,
    secret: str,
    service: str,
    namespace: str = "paasta",
) -> Optional[str]:
    service = sanitise_kubernetes_name(service)
    secret = sanitise_kubernetes_name(secret)
    try:
        signature = kube_client.core.read_namespaced_config_map(
            name=f"{namespace}-secret-{service}-{secret}-signature", namespace=namespace
        )
    except ApiException as e:
        if e.status == 404:
            return None
        else:
            raise
    if not signature:
        return None
    else:
        return signature.data["signature"]


def update_kubernetes_secret_signature(
    kube_client: KubeClient,
    secret: str,
    service: str,
    secret_signature: str,
    namespace: str = "paasta",
) -> None:
    service = sanitise_kubernetes_name(service)
    secret = sanitise_kubernetes_name(secret)
    kube_client.core.replace_namespaced_config_map(
        name=f"{namespace}-secret-{service}-{secret}-signature",
        namespace=namespace,
        body=V1ConfigMap(
            metadata=V1ObjectMeta(
                name=f"{namespace}-secret-{service}-{secret}-signature",
                labels={
                    "yelp.com/paasta_service": service,
                    "paasta.yelp.com/service": service,
                },
            ),
            data={"signature": secret_signature},
        ),
    )


def create_kubernetes_secret_signature(
    kube_client: KubeClient,
    secret: str,
    service: str,
    secret_signature: str,
    namespace: str = "paasta",
) -> None:
    service = sanitise_kubernetes_name(service)
    secret = sanitise_kubernetes_name(secret)
    kube_client.core.create_namespaced_config_map(
        namespace=namespace,
        body=V1ConfigMap(
            metadata=V1ObjectMeta(
                name=f"{namespace}-secret-{service}-{secret}-signature",
                labels={
                    "yelp.com/paasta_service": service,
                    "paasta.yelp.com/service": service,
                },
            ),
            data={"signature": secret_signature},
        ),
    )


def sanitise_kubernetes_name(
    service: str,
) -> str:
    name = service.replace("_", "--")
    if name.startswith("--"):
        name = name.replace("--", "underscore-", 1)
    return name.lower()


def load_custom_resource_definitions(
    system_paasta_config: SystemPaastaConfig,
) -> Sequence[CustomResourceDefinition]:
    custom_resources = []
    for custom_resource_dict in system_paasta_config.get_kubernetes_custom_resources():
        kube_kind = KubeKind(**custom_resource_dict.pop("kube_kind"))  # type: ignore
        custom_resources.append(
            CustomResourceDefinition(  # type: ignore
                kube_kind=kube_kind, **custom_resource_dict  # type: ignore
            )
        )
    return custom_resources


def sanitised_cr_name(service: str, instance: str) -> str:
    sanitised_service = sanitise_kubernetes_name(service)
    sanitised_instance = sanitise_kubernetes_name(instance)
    return f"{sanitised_service}-{sanitised_instance}"


def get_cr(
    kube_client: KubeClient, cr_id: Mapping[str, str]
) -> Optional[Mapping[str, Any]]:
    try:
        return kube_client.custom.get_namespaced_custom_object(**cr_id)
    except ApiException as e:
        if e.status == 404:
            return None
        else:
            raise


def set_cr_desired_state(
    kube_client: KubeClient, cr_id: Mapping[str, str], desired_state: str
) -> str:
    cr = kube_client.custom.get_namespaced_custom_object(**cr_id)
    if cr.get("status", {}).get("state") == desired_state:
        return cr["status"]

    if "metadata" not in cr:
        cr["metadata"] = {}
    if "annotations" not in cr["metadata"]:
        cr["metadata"]["annotations"] = {}
    cr["metadata"]["annotations"]["yelp.com/desired_state"] = desired_state
    cr["metadata"]["annotations"]["paasta.yelp.com/desired_state"] = desired_state
    kube_client.custom.replace_namespaced_custom_object(**cr_id, body=cr)
    status = cr.get("status")
    return status


def get_pod_hostname(kube_client: KubeClient, pod: V1Pod) -> str:
    """Gets the hostname of a pod's node from labels"""
    if not pod.spec.node_name:  # can be none, if pod not yet scheduled
        return "NotScheduled"
    try:
        node = kube_client.core.read_node(name=pod.spec.node_name)
    except ApiException:
        # fall back to node name (which has the IP) if node somehow doesnt exist
        return pod.spec.node_name
    # if label has disappeared (say we changed it), default to node name
    return node.metadata.labels.get("yelp.com/hostname", pod.spec.node_name)


def get_pod_node(
    kube_client: KubeClient, pod: V1Pod, cache_nodes: bool = False
) -> Optional[V1Node]:
    if cache_nodes:
        nodes = get_all_nodes_cached(kube_client)
    else:
        nodes = get_all_nodes(kube_client)
    running_node = [node for node in nodes if node.metadata.name == pod.spec.node_name]
    return running_node[0] if running_node else None


def to_node_label(label: str) -> str:
    """k8s-ifies certain special node labels"""
    if label in {"instance_type", "instance-type"}:
        return "node.kubernetes.io/instance-type"
    elif label in {
        "datacenter",
        "ecosystem",
        "habitat",
        "hostname",
        "region",
        "superregion",
    }:
        return f"yelp.com/{label}"
    return label


def get_all_service_accounts(
    kube_client: KubeClient,
    namespace: str,
) -> Sequence[V1ServiceAccount]:
    return kube_client.core.list_namespaced_service_account(namespace=namespace).items


def get_all_role_bindings(
    kube_client: KubeClient,
    namespace: str,
) -> Sequence[V1RoleBinding]:
    return kube_client.rbac.list_namespaced_role_binding(namespace=namespace).items


_RE_NORMALIZE_IAM_ROLE = re.compile(r"[^0-9a-zA-Z]+")


def create_or_find_service_account_name(
    iam_role: str,
    namespace: str = "paasta",
    k8s_role: Optional[str] = None,
    dry_run: bool = False,
) -> str:
    # the service account is expected to always be prefixed with paasta- as using the actual namespace
    # potentially wastes a lot of characters (e.g., paasta-nrtsearchservices) that could be used for
    # the actual name
    if iam_role:  # this is either an empty string or a real role
        # it's possible for an IAM role to be used for multiple purposes. Some usages may require a
        # Kubernetes Role attached to the Service Account (e.g., Spark drivers may access S3 but also
        # need to manage Spark executor Pods), while "normal" services/batches need a Service Account
        # with only an IAM role attached.
        # to support these two usecases, we'll suffix the name of a Service Account with the
        # Kubernetes Role name to disambiguate between the two.
        if k8s_role:
            sa_name = f"paasta--{_RE_NORMALIZE_IAM_ROLE.sub('-', iam_role)}--{k8s_role}"
        else:
            sa_name = f"paasta--{_RE_NORMALIZE_IAM_ROLE.sub('-', iam_role)}"
    # until Core ML migrates Spark to use Pod Identity, we need to support starting Spark drivers with a Service Account
    # that only has k8s access
    elif not iam_role and k8s_role:
        sa_name = f"paasta--{k8s_role}"
    # we should never get here in normal usage, but just in case we make a mistake in the future :)
    else:
        raise ValueError(
            "Expected at least one of iam_role or k8s_role to be passed in!"
        )

    # if someone is dry-running paasta_setup_tron_namespace or some other tool that
    # calls this function, we probably don't want to mutate k8s state :)
    if dry_run:
        return sa_name

    kube_client = KubeClient()
    if not any(
        sa.metadata and sa.metadata.name == sa_name
        for sa in get_all_service_accounts(kube_client, namespace)
    ):
        sa = V1ServiceAccount(
            kind="ServiceAccount",
            metadata=V1ObjectMeta(
                name=sa_name,
                namespace=namespace,
                annotations={"eks.amazonaws.com/role-arn": iam_role},
            ),
        )
        kube_client.core.create_namespaced_service_account(namespace=namespace, body=sa)

    # we're expecting that any Role dynamically associated with a Service Account already exists.
    # at Yelp, this means that we have a version-controlled resource for the Role in Puppet.
    # and since the Role already exists, we just need to associate it with the Service Account through
    # a Role Binding
    if k8s_role:
        # that said, we still check that there's a RoleBinding every time this function is called so that
        # we can self-heal if we somehow create a Service Account and then fail to create a Role Binding
        # due to a transient issue
        if not any(
            rb.metadata and rb.metadata.name == sa_name
            for rb in get_all_role_bindings(kube_client, namespace)
        ):
            role_binding = V1RoleBinding(
                metadata=V1ObjectMeta(
                    name=sa_name,
                    namespace=namespace,
                ),
                role_ref=V1RoleRef(
                    api_group="rbac.authorization.k8s.io",
                    kind="Role",
                    name=k8s_role,
                ),
                subjects=[
                    V1Subject(
                        kind="ServiceAccount",
                        namespace=namespace,
                        name=sa_name,
                    ),
                ],
            )
            kube_client.rbac.create_namespaced_role_binding(
                namespace=namespace, body=role_binding
            )

    return sa_name


def mode_to_int(mode: Optional[Union[str, int]]) -> Optional[int]:
    if mode is not None:
        if isinstance(mode, str):
            if len(mode) < 2 or mode[0] != "0":
                raise ValueError(f"Invalid mode: {mode}")
            mode = int(mode[1:], 8)
    return mode


def update_crds(
    kube_client: KubeClient,
    desired_crds: Collection[V1beta1CustomResourceDefinition],
    existing_crds: V1CustomResourceDefinitionList,
) -> bool:
    success = True
    for desired_crd in desired_crds:
        existing_crd = None
        for crd in existing_crds.items:
            if crd.metadata.name == desired_crd.metadata["name"]:
                existing_crd = crd
                break

        try:
            if existing_crd:
                desired_crd.metadata[
                    "resourceVersion"
                ] = existing_crd.metadata.resource_version
                kube_client.apiextensions.replace_custom_resource_definition(
                    name=desired_crd.metadata["name"], body=desired_crd
                )
            else:
                try:
                    kube_client.apiextensions.create_custom_resource_definition(
                        body=desired_crd
                    )
                except ValueError as err:
                    # TODO: kubernetes server will sometimes reply with conditions:null,
                    # figure out how to deal with this correctly, for more details:
                    # https://github.com/kubernetes/kubernetes/pull/64996
                    if "`conditions`, must not be `None`" in str(err):
                        pass
                    else:
                        raise err
            log.info(f"deployed internal crd {desired_crd.metadata['name']}")
        except ApiException as exc:
            log.error(
                f"error deploying crd {desired_crd.metadata['name']}, "
                f"status: {exc.status}, reason: {exc.reason}"
            )
            log.debug(exc.body)
            success = False

    return success
