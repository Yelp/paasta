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
import copy
import itertools
import json
import logging
import math
import os
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any
from typing import List
from typing import Mapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple
from typing import Union

import requests
import service_configuration_lib
from kubernetes import client as kube_client
from kubernetes import config as kube_config
from kubernetes.client import models
from kubernetes.client import V1AWSElasticBlockStoreVolumeSource
from kubernetes.client import V1beta1PodDisruptionBudget
from kubernetes.client import V1beta1PodDisruptionBudgetSpec
from kubernetes.client import V1Capabilities
from kubernetes.client import V1ConfigMap
from kubernetes.client import V1Container
from kubernetes.client import V1ContainerPort
from kubernetes.client import V1DeleteOptions
from kubernetes.client import V1Deployment
from kubernetes.client import V1DeploymentSpec
from kubernetes.client import V1DeploymentStrategy
from kubernetes.client import V1EnvVar
from kubernetes.client import V1EnvVarSource
from kubernetes.client import V1ExecAction
from kubernetes.client import V1Handler
from kubernetes.client import V1HostPathVolumeSource
from kubernetes.client import V1HTTPGetAction
from kubernetes.client import V1LabelSelector
from kubernetes.client import V1Lifecycle
from kubernetes.client import V1Namespace
from kubernetes.client import V1Node
from kubernetes.client import V1ObjectFieldSelector
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1PersistentVolumeClaim
from kubernetes.client import V1PersistentVolumeClaimSpec
from kubernetes.client import V1Pod
from kubernetes.client import V1PodSpec
from kubernetes.client import V1PodTemplateSpec
from kubernetes.client import V1Probe
from kubernetes.client import V1ResourceRequirements
from kubernetes.client import V1RollingUpdateDeployment
from kubernetes.client import V1Secret
from kubernetes.client import V1SecretKeySelector
from kubernetes.client import V1SecurityContext
from kubernetes.client import V1StatefulSet
from kubernetes.client import V1StatefulSetSpec
from kubernetes.client import V1TCPSocketAction
from kubernetes.client import V1Volume
from kubernetes.client import V1VolumeMount
from kubernetes.client import V2beta1CrossVersionObjectReference
from kubernetes.client import V2beta1HorizontalPodAutoscaler
from kubernetes.client import V2beta1HorizontalPodAutoscalerCondition
from kubernetes.client import V2beta1HorizontalPodAutoscalerSpec
from kubernetes.client import V2beta1MetricSpec
from kubernetes.client import V2beta1PodsMetricSource
from kubernetes.client import V2beta1ResourceMetricSource
from kubernetes.client.models import V2beta1HorizontalPodAutoscalerStatus
from kubernetes.client.rest import ApiException

from paasta_tools.long_running_service_tools import host_passes_blacklist
from paasta_tools.long_running_service_tools import host_passes_whitelist
from paasta_tools.long_running_service_tools import InvalidHealthcheckMode
from paasta_tools.long_running_service_tools import load_service_namespace_config
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfigDict
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.marathon_tools import AutoscalingParamsDict
from paasta_tools.secret_providers import BaseSecretProvider
from paasta_tools.secret_tools import get_secret_name_from_ref
from paasta_tools.secret_tools import is_secret_ref
from paasta_tools.secret_tools import is_shared_secret
from paasta_tools.secret_tools import SHARED_SECRET_SERVICE
from paasta_tools.utils import AwsEbsVolume
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DeployBlacklist
from paasta_tools.utils import DeployWhitelist
from paasta_tools.utils import DockerVolume
from paasta_tools.utils import get_code_sha_from_dockerurl
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import PersistentVolume
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import time_cache
from paasta_tools.utils import VolumeWithMode


log = logging.getLogger(__name__)

KUBE_CONFIG_PATH = "/etc/kubernetes/admin.conf"
YELP_ATTRIBUTE_PREFIX = "yelp.com/"
CONFIG_HASH_BLACKLIST = {"replicas"}
KUBE_DEPLOY_STATEGY_MAP = {"crossover": "RollingUpdate", "downthenup": "Recreate"}
KUBE_DEPLOY_STATEGY_REVMAP = {v: k for k, v in KUBE_DEPLOY_STATEGY_MAP.items()}
HACHECK_POD_NAME = "hacheck"


# For detail, https://github.com/kubernetes-client/python/issues/553
# This hack should be removed when the issue got fixed.
# This is no better way to work around rn.


class MonkeyPatchAutoScalingConditions(V2beta1HorizontalPodAutoscalerStatus):
    @property
    def conditions(self) -> Sequence[V2beta1HorizontalPodAutoscalerCondition]:
        return super().conditions()

    @conditions.setter
    def conditions(
        self, conditions: Optional[Sequence[V2beta1HorizontalPodAutoscalerCondition]]
    ) -> None:
        self._conditions = list() if conditions is None else conditions


models.V2beta1HorizontalPodAutoscalerStatus = MonkeyPatchAutoScalingConditions


class KubeKind(NamedTuple):
    singular: str
    plural: str


class KubeDeployment(NamedTuple):
    service: str
    instance: str
    git_sha: str
    config_sha: str
    replicas: int


class KubeCustomResource(NamedTuple):
    service: str
    instance: str
    config_sha: str
    kind: str
    namespace: str
    name: str


class KubeService(NamedTuple):
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


def _set_disrupted_pods(self: Any, disrupted_pods: Mapping[str, datetime]) -> None:
    """Private function used to patch the setter for V1beta1PodDisruptionBudgetStatus.
    Can be removed once https://github.com/kubernetes-client/python/issues/466 is resolved
    """
    self._disrupted_pods = disrupted_pods


class KubernetesDeploymentConfigDict(LongRunningServiceConfigDict, total=False):
    bounce_method: str
    bounce_margin_factor: float
    service_account_name: str
    autoscaling: AutoscalingParamsDict


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
    kubernetes_conf_file = "kubernetes-%s" % cluster
    instance_configs = service_configuration_lib.read_extra_service_information(
        service, kubernetes_conf_file, soa_dir=soa_dir
    )

    if instance.startswith("_"):
        raise InvalidJobNameError(
            f"Unable to load kubernetes job config for {service}.{instance} as instance name starts with '_'"
        )
    if instance not in instance_configs:
        raise NoConfigurationForServiceError(
            f"{instance} not found in config file {soa_dir}/{service}/{kubernetes_conf_file}.yaml."
        )

    general_config = deep_merge_dictionaries(
        overrides=instance_configs[instance], defaults=general_config
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


class InvalidKubernetesConfig(Exception):
    def __init__(self, exception: Exception, service: str, instance: str) -> None:
        super().__init__(
            f"Couldn't generate config for kubernetes service: {service}.{instance}: {exception}"
        )


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
        return KUBE_DEPLOY_STATEGY_MAP[bounce_method]

    def get_autoscaling_metric_spec(
        self, name: str, namespace: str = "paasta"
    ) -> Optional[V2beta1HorizontalPodAutoscaler]:
        min_replicas = self.get_min_instances()
        max_replicas = self.get_max_instances()
        if not min_replicas or not max_replicas:
            log.error(
                "Please specify min_instances and max_instances for autoscaling to work"
            )
            return None
        metrics_provider = self.config_dict.get("autoscaling", {}).get(
            "metrics_provider", "mesos_cpu"
        )
        # TODO support multiple metrics
        metrics = []
        target = (
            float(self.config_dict.get("autoscaling", {}).get("setpoint", "0.8")) * 100
        )
        # TODO support bespoke PAASTA-15680
        if (
            self.config_dict.get("autoscaling", {}).get("decision_policy", "")
            == "bespoke"
        ):
            log.error(
                f"Sorry, bespoke is not implemented yet. Please use a different decision \
                policy if possible for {name}/name in namespace{namespace}"
            )
            return None
        elif metrics_provider == "mesos_cpu":
            metrics.append(
                V2beta1MetricSpec(
                    type="Resource",
                    resource=V2beta1ResourceMetricSource(
                        name="cpu", target_average_utilization=target
                    ),
                )
            )
        elif metrics_provider == "http":
            metrics.append(
                V2beta1MetricSpec(
                    type="Pods",
                    pods=V2beta1PodsMetricSource(
                        metric_name="http", target_average_value=target
                    ),
                )
            )
        elif metrics_provider == "uwsgi":
            metrics.append(
                V2beta1MetricSpec(
                    type="Pods",
                    pods=V2beta1PodsMetricSource(
                        metric_name="uwsgi", target_average_value=target
                    ),
                )
            )
        else:
            log.error(
                f"Wrong metrics specified: {metrics_provider} for\
                {name}/name in namespace{namespace}"
            )
            return None

        return V2beta1HorizontalPodAutoscaler(
            kind="HorizontalPodAutoscaler",
            metadata=V1ObjectMeta(name=name, namespace=namespace),
            spec=V2beta1HorizontalPodAutoscalerSpec(
                max_replicas=max_replicas,
                min_replicas=min_replicas,
                metrics=metrics,
                scale_target_ref=V2beta1CrossVersionObjectReference(
                    kind="Deployment", name=name
                ),
            ),
        )

    def get_deployment_strategy_config(self) -> V1DeploymentStrategy:
        strategy_type = self.get_bounce_method()
        rolling_update: Optional[V1RollingUpdateDeployment]
        if strategy_type == "RollingUpdate":
            # this translates bounce_margin to k8s speak maxUnavailable
            # for now we keep max_surge 100% but we could customise later
            rolling_update = V1RollingUpdateDeployment(
                max_surge="100%",
                max_unavailable="{}%".format(
                    int((1 - self.get_bounce_margin_factor()) * 100)
                ),
            )
        else:
            rolling_update = None

        strategy = V1DeploymentStrategy(
            type=strategy_type, rolling_update=rolling_update
        )
        return strategy

    def get_sanitised_volume_name(self, volume_name: str) -> str:
        """I know but we really aren't allowed many characters..."""
        volume_name = volume_name.rstrip("/")
        sanitised = volume_name.replace("/", "slash-").replace(".", "dot-")
        return sanitise_kubernetes_name(sanitised)

    def get_docker_volume_name(self, docker_volume: DockerVolume) -> str:
        return self.get_sanitised_volume_name(
            "host--{name}".format(name=docker_volume["hostPath"])
        )

    def get_persistent_volume_name(self, docker_volume: PersistentVolume) -> str:
        return self.get_sanitised_volume_name(
            "pv--{name}".format(name=docker_volume["container_path"])
        )

    def get_aws_ebs_volume_name(self, aws_ebs_volume: AwsEbsVolume) -> str:
        return self.get_sanitised_volume_name(
            "aws-ebs--{name}{partition}".format(
                name=aws_ebs_volume["volume_id"],
                partition=aws_ebs_volume.get("partition", ""),
            )
        )

    def read_only_mode(self, d: VolumeWithMode) -> bool:
        return d.get("mode", "RO") == "RO"

    def get_sidecar_containers(
        self,
        system_paasta_config: SystemPaastaConfig,
        service_namespace_config: ServiceNamespaceConfig,
    ) -> Sequence[V1Container]:
        registrations = " ".join(self.get_registrations())
        # s_m_j currently asserts that services are healthy in smartstack before
        # continuing a bounce. this readiness check lets us achieve the same thing
        readiness_probe: Optional[V1Probe]
        if system_paasta_config.get_enable_nerve_readiness_check():
            readiness_probe = V1Probe(
                _exec=V1ExecAction(
                    command=[
                        system_paasta_config.get_nerve_readiness_check_script(),
                        str(self.get_container_port()),
                    ]
                    + self.get_registrations()
                ),
                initial_delay_seconds=10,
                period_seconds=10,
            )
        else:
            readiness_probe = None

        sidecars = []
        if service_namespace_config.is_in_smartstack():
            sidecars.append(
                V1Container(
                    image=system_paasta_config.get_hacheck_sidecar_image_url(),
                    lifecycle=V1Lifecycle(
                        pre_stop=V1Handler(
                            _exec=V1ExecAction(
                                command=[
                                    "/bin/sh",
                                    "-c",
                                    f"/usr/bin/hadown {registrations}; sleep 31",
                                ]
                            )
                        )
                    ),
                    name=HACHECK_POD_NAME,
                    env=self.get_kubernetes_environment(),
                    ports=[V1ContainerPort(container_port=6666)],
                    readiness_probe=readiness_probe,
                )
            )
        return sidecars

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

    def get_kubernetes_environment(self) -> Sequence[V1EnvVar]:
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
        ]
        return kubernetes_env

    def get_resource_requirements(self) -> V1ResourceRequirements:
        return V1ResourceRequirements(
            limits={
                "cpu": self.get_cpus() + self.get_cpu_burst_add(),
                "memory": f"{self.get_mem()}Mi",
            },
            requests={"cpu": self.get_cpus(), "memory": f"{self.get_mem()}Mi"},
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
                "Unknown mode: %s. Only acceptable healthcheck modes are http/https/tcp"
                % mode
            )

        return probe

    def get_security_context(self) -> Optional[V1SecurityContext]:
        cap_add = self.config_dict.get("cap_add", None)
        if cap_add is None:
            return None
        return V1SecurityContext(capabilities=V1Capabilities(add=cap_add))

    def get_kubernetes_containers(
        self,
        docker_volumes: Sequence[DockerVolume],
        system_paasta_config: SystemPaastaConfig,
        aws_ebs_volumes: Sequence[AwsEbsVolume],
        service_namespace_config: ServiceNamespaceConfig,
    ) -> Sequence[V1Container]:
        service_container = V1Container(
            image=self.get_docker_url(),
            command=self.get_cmd(),
            args=self.get_args(),
            env=self.get_container_env(),
            resources=self.get_resource_requirements(),
            lifecycle=V1Lifecycle(
                pre_stop=V1Handler(
                    _exec=V1ExecAction(command=["/bin/sh", "-c", "sleep 30"])
                )
            ),
            name=self.get_sanitised_deployment_name(),
            liveness_probe=self.get_liveness_probe(service_namespace_config),
            ports=[V1ContainerPort(container_port=self.get_container_port())],
            security_context=self.get_security_context(),
            volume_mounts=self.get_volume_mounts(
                docker_volumes=docker_volumes,
                aws_ebs_volumes=aws_ebs_volumes,
                persistent_volumes=self.get_persistent_volumes(),
            ),
        )
        containers = [service_container] + self.get_sidecar_containers(  # type: ignore
            system_paasta_config=system_paasta_config,
            service_namespace_config=service_namespace_config,
        )
        return containers

    def get_pod_volumes(
        self,
        docker_volumes: Sequence[DockerVolume],
        aws_ebs_volumes: Sequence[AwsEbsVolume],
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
        return pod_volumes

    def get_volume_mounts(
        self,
        docker_volumes: Sequence[DockerVolume],
        aws_ebs_volumes: Sequence[AwsEbsVolume],
        persistent_volumes: Sequence[PersistentVolume],
    ) -> Sequence[V1VolumeMount]:
        return (
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
        )

    def get_sanitised_service_name(self) -> str:
        return sanitise_kubernetes_name(self.get_service())

    def get_sanitised_instance_name(self) -> str:
        return sanitise_kubernetes_name(self.get_instance())

    def get_instances(self, with_limit: bool = True) -> int:
        """
        Return expected number of instances. If the controller is running, return
        desired replicas. Otherwise, return the number of instances in yelpsoa_config
        """
        if self.get_max_instances() is not None:
            try:
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
                return self.get_max_instances()
        else:
            instances = self.config_dict.get("instances", 1)
            log.debug("Autoscaling not enabled, returning %d instances" % instances)
            return instances

    def get_desired_instances(self) -> int:
        """ For now if we have an EBS instance it means we can only have 1 instance
        since we can't attach to multiple instances. In the future we might support
        statefulsets which are clever enough to manage EBS for you"""

        if self.get_desired_state() == "start":
            instances = self.config_dict.get("instances", self.get_min_instances())
        elif self.get_desired_state() == "stop":
            instances = 0
            log.debug("Instance is set to stop. Returning '0' instances")
        else:
            raise Exception(f"The state of {self.service}.{self.instance} is unknown.")

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
        storage_class_name = volume.get("storage_class_name", "ebs")
        if storage_class_name not in ["ebs", "ebs-slow"]:
            log.warning(f"storage class {storage_class_name} is not supported")
            storage_class_name = "ebs"
        return storage_class_name

    def get_kubernetes_metadata(self, code_sha: str) -> V1ObjectMeta:
        return V1ObjectMeta(
            name="{service}-{instance}".format(
                service=self.get_sanitised_service_name(),
                instance=self.get_sanitised_instance_name(),
            ),
            labels={
                "yelp.com/paasta_service": self.get_service(),
                "yelp.com/paasta_instance": self.get_instance(),
                "yelp.com/paasta_git_sha": code_sha,
            },
        )

    def get_sanitised_deployment_name(self) -> str:
        return "{service}-{instance}".format(
            service=self.get_sanitised_service_name(),
            instance=self.get_sanitised_instance_name(),
        )

    def format_kubernetes_app(self) -> Union[V1Deployment, V1StatefulSet]:
        """Create the configuration that will be passed to the Kubernetes REST API."""

        try:
            system_paasta_config = load_system_paasta_config()
            docker_url = self.get_docker_url()
            code_sha = get_code_sha_from_dockerurl(docker_url)
            complete_config: Union[V1StatefulSet, V1Deployment]
            if self.get_persistent_volumes():
                complete_config = V1StatefulSet(
                    api_version="apps/v1",
                    kind="StatefulSet",
                    metadata=self.get_kubernetes_metadata(code_sha),
                    spec=V1StatefulSetSpec(
                        service_name="{service}-{instance}".format(
                            service=self.get_sanitised_service_name(),
                            instance=self.get_sanitised_instance_name(),
                        ),
                        volume_claim_templates=self.get_volume_claim_templates(),
                        replicas=self.get_desired_instances(),
                        revision_history_limit=0,
                        selector=V1LabelSelector(
                            match_labels={
                                "yelp.com/paasta_service": self.get_service(),
                                "yelp.com/paasta_instance": self.get_instance(),
                            }
                        ),
                        template=self.get_pod_template_spec(
                            code_sha=code_sha, system_paasta_config=system_paasta_config
                        ),
                    ),
                )
            else:
                complete_config = V1Deployment(
                    api_version="apps/v1",
                    kind="Deployment",
                    metadata=self.get_kubernetes_metadata(code_sha),
                    spec=V1DeploymentSpec(
                        replicas=self.get_desired_instances(),
                        selector=V1LabelSelector(
                            match_labels={
                                "yelp.com/paasta_service": self.get_service(),
                                "yelp.com/paasta_instance": self.get_instance(),
                            }
                        ),
                        revision_history_limit=0,
                        template=self.get_pod_template_spec(
                            code_sha=code_sha, system_paasta_config=system_paasta_config
                        ),
                        strategy=self.get_deployment_strategy_config(),
                    ),
                )

            config_hash = get_config_hash(
                self.sanitize_for_config_hash(complete_config),
                force_bounce=self.get_force_bounce(),
            )
            complete_config.metadata.labels["yelp.com/paasta_config_sha"] = config_hash
            complete_config.spec.template.metadata.labels[
                "yelp.com/paasta_config_sha"
            ] = config_hash
        except Exception as e:
            raise InvalidKubernetesConfig(e, self.get_service(), self.get_instance())
        log.debug("Complete configuration for instance is: %s", complete_config)
        return complete_config

    def get_kubernetes_service_account_name(self) -> Optional[str]:
        return self.config_dict.get("service_account_name", None)

    def get_pod_template_spec(
        self, code_sha: str, system_paasta_config: SystemPaastaConfig
    ) -> V1PodTemplateSpec:
        service_namespace_config = load_service_namespace_config(
            service=self.service, namespace=self.get_nerve_namespace()
        )
        docker_volumes = self.get_volumes(
            system_volumes=system_paasta_config.get_volumes()
        )
        annotations = {"smartstack_registrations": json.dumps(self.get_registrations())}
        metrics_provider = self.config_dict.get("autoscaling", {}).get(
            "metrics_provider", ""
        )
        if metrics_provider in {"http", "uwsgi"}:
            annotations["autoscaling"] = metrics_provider

        return V1PodTemplateSpec(
            metadata=V1ObjectMeta(
                labels={
                    "yelp.com/paasta_service": self.get_service(),
                    "yelp.com/paasta_instance": self.get_instance(),
                    "yelp.com/paasta_git_sha": code_sha,
                },
                annotations=annotations,
            ),
            spec=V1PodSpec(
                service_account_name=self.get_kubernetes_service_account_name(),
                containers=self.get_kubernetes_containers(
                    docker_volumes=docker_volumes,
                    aws_ebs_volumes=self.get_aws_ebs_volumes(),
                    system_paasta_config=system_paasta_config,
                    service_namespace_config=service_namespace_config,
                ),
                node_selector=self.get_node_selector(),
                restart_policy="Always",
                volumes=self.get_pod_volumes(
                    docker_volumes=docker_volumes,
                    aws_ebs_volumes=self.get_aws_ebs_volumes(),
                ),
            ),
        )

    def get_node_selector(self) -> Mapping[str, str]:
        return {"yelp.com/pool": self.get_pool()}

    def sanitize_for_config_hash(
        self, config: Union[V1Deployment, V1StatefulSet]
    ) -> Mapping[str, Any]:
        """Removes some data from config to make it suitable for
        calculation of config hash.

        :param config: complete_config hash to sanitize
        :returns: sanitized copy of complete_config hash
        """
        ahash = {
            key: copy.deepcopy(value)
            for key, value in config.to_dict().items()
            if key not in CONFIG_HASH_BLACKLIST
        }
        spec = ahash["spec"]
        ahash["spec"] = {
            key: copy.deepcopy(value)
            for key, value in spec.items()
            if key not in CONFIG_HASH_BLACKLIST
        }
        ahash["paasta_secrets"] = get_kubernetes_secret_hashes(
            service=self.get_service(), environment_variables=self.get_env()
        )
        return ahash

    def get_bounce_margin_factor(self) -> float:
        return self.config_dict.get("bounce_margin_factor", 1.0)


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


def get_kubernetes_services_running_here() -> Sequence[KubeService]:
    services = []
    pods = requests.get("http://127.0.0.1:10255/pods").json()
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
                KubeService(
                    name=pod["metadata"]["labels"]["yelp.com/paasta_service"],
                    instance=pod["metadata"]["labels"]["yelp.com/paasta_instance"],
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


class KubeClient:
    def __init__(self) -> None:
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
        self.deployments = kube_client.AppsV1Api()
        self.core = kube_client.CoreV1Api()
        self.policy = kube_client.PolicyV1beta1Api()
        self.apiextensions = kube_client.ApiextensionsV1beta1Api()
        self.custom = kube_client.CustomObjectsApi()
        self.autoscaling = kube_client.AutoscalingV2beta1Api()


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
            service=item.metadata.labels["yelp.com/paasta_service"],
            instance=item.metadata.labels["yelp.com/paasta_instance"],
            git_sha=item.metadata.labels["yelp.com/paasta_git_sha"],
            config_sha=item.metadata.labels["yelp.com/paasta_config_sha"],
            replicas=item.spec.replicas,
        )
        for item in deployments.items + stateful_sets.items
    ]


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
                    service=cr["metadata"]["labels"]["yelp.com/paasta_service"],
                    instance=cr["metadata"]["labels"]["yelp.com/paasta_instance"],
                    config_sha=cr["metadata"]["labels"]["yelp.com/paasta_config_sha"],
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
    service: str, instance: str, max_unavailable: str
) -> V1beta1PodDisruptionBudget:
    return V1beta1PodDisruptionBudget(
        metadata=V1ObjectMeta(name=f"{service}-{instance}", namespace="paasta"),
        spec=V1beta1PodDisruptionBudgetSpec(
            max_unavailable=max_unavailable,
            selector=V1LabelSelector(
                match_labels={
                    "yelp.com/paasta_service": service,
                    "yelp.com/paasta_instance": instance,
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


def list_all_deployments(kube_client: KubeClient) -> Sequence[KubeDeployment]:
    return list_deployments(kube_client)


def list_matching_deployments(
    service: str, instance: str, kube_client: KubeClient
) -> Sequence[KubeDeployment]:
    return list_deployments(
        kube_client,
        f"yelp.com/paasta_instance={instance},yelp.com/paasta_service={service}",
    )


def pods_for_service_instance(
    service: str, instance: str, kube_client: KubeClient
) -> Sequence[V1Pod]:
    return kube_client.core.list_namespaced_pod(
        namespace="paasta",
        label_selector=f"yelp.com/paasta_service={service},yelp.com/paasta_instance={instance}",
    ).items


def get_all_pods(kube_client: KubeClient, namespace: str = "paasta") -> Sequence[V1Pod]:
    return kube_client.core.list_namespaced_pod(namespace=namespace).items


def filter_pods_by_service_instance(
    pod_list: Sequence[V1Pod], service: str, instance: str
) -> Sequence[V1Pod]:
    return [
        pod
        for pod in pod_list
        if pod.metadata.labels is not None
        and pod.metadata.labels["yelp.com/paasta_service"] == service
        and pod.metadata.labels["yelp.com/paasta_instance"] == instance
    ]


def _is_it_ready(it: Union[V1Pod, V1Node],) -> bool:
    ready_conditions = [
        cond.status == "True" for cond in it.status.conditions if cond.type == "Ready"
    ]
    return all(ready_conditions) if ready_conditions else False


is_pod_ready = _is_it_ready

is_node_ready = _is_it_ready


class PodStatus(Enum):
    PENDING = (1,)
    RUNNING = (2,)
    SUCCEEDED = (3,)
    FAILED = (4,)
    UNKNOWN = (5,)


_POD_STATUS_NAME_TO_STATUS = {s.name.upper(): s for s in PodStatus}


def get_pod_status(pod: V1Pod,) -> PodStatus:
    # TODO: we probably also need to deduce extended statuses here, like
    # `CrashLoopBackOff`, `ContainerCreating` timeout, and etc.
    return _POD_STATUS_NAME_TO_STATUS[pod.status.phase.upper()]


def get_active_shas_for_service(pod_list: Sequence[V1Pod],) -> Mapping[str, Set[str]]:
    ret: Mapping[str, Set[str]] = {"config_sha": set(), "git_sha": set()}
    for pod in pod_list:
        ret["config_sha"].add(pod.metadata.labels["yelp.com/paasta_config_sha"])
        ret["git_sha"].add(pod.metadata.labels["yelp.com/paasta_git_sha"])
    return ret


def get_all_nodes(kube_client: KubeClient,) -> Sequence[V1Node]:
    return kube_client.core.list_node().items


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
        whitelist = (maybe_add_yelp_prefix(whitelist[0]), whitelist[1])
    blacklist = [(maybe_add_yelp_prefix(entry[0]), entry[1]) for entry in blacklist]
    return [
        node
        for node in nodes
        if host_passes_whitelist(node.metadata.labels, whitelist)
        and host_passes_blacklist(node.metadata.labels, blacklist)
    ]


def maybe_add_yelp_prefix(attribute: str,) -> str:
    return YELP_ATTRIBUTE_PREFIX + attribute if "/" not in attribute else attribute


def get_nodes_grouped_by_attribute(
    nodes: Sequence[V1Node], attribute: str
) -> Mapping[str, Sequence[V1Node]]:
    attribute = maybe_add_yelp_prefix(attribute)
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


def get_kubernetes_app_by_name(
    name: str, kube_client: KubeClient
) -> Union[V1Deployment, V1StatefulSet]:
    try:
        app = kube_client.deployments.read_namespaced_deployment_status(
            name=name, namespace="paasta"
        )
        return app
    except ApiException as e:
        if e.status == 404:
            pass
        else:
            raise
    return kube_client.deployments.read_namespaced_stateful_set_status(
        name=name, namespace="paasta"
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


def get_kubernetes_app_deploy_status(
    kube_client: KubeClient,
    app: Union[V1Deployment, V1StatefulSet],
    desired_instances: int,
) -> int:
    if (
        app.status.ready_replicas is None
        or app.status.ready_replicas < desired_instances
    ):
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
    return deploy_status


class KubernetesDeployStatus:
    """ An enum to represent Kubernetes app deploy status.
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
    return Path(KUBE_CONFIG_PATH).exists()


def create_secret(
    kube_client: KubeClient,
    secret: str,
    service: str,
    secret_provider: BaseSecretProvider,
) -> None:
    service = sanitise_kubernetes_name(service)
    sanitised_secret = sanitise_kubernetes_name(secret)
    kube_client.core.create_namespaced_secret(
        namespace="paasta",
        body=V1Secret(
            metadata=V1ObjectMeta(
                name=f"paasta-secret-{service}-{sanitised_secret}",
                labels={"yelp.com/paasta_service": service},
            ),
            data={
                secret: base64.b64encode(
                    secret_provider.decrypt_secret_raw(secret)
                ).decode("utf-8")
            },
        ),
    )


def update_secret(
    kube_client: KubeClient,
    secret: str,
    service: str,
    secret_provider: BaseSecretProvider,
) -> None:
    service = sanitise_kubernetes_name(service)
    sanitised_secret = sanitise_kubernetes_name(secret)
    kube_client.core.replace_namespaced_secret(
        name=f"paasta-secret-{service}-{sanitised_secret}",
        namespace="paasta",
        body=V1Secret(
            metadata=V1ObjectMeta(
                name=f"paasta-secret-{service}-{sanitised_secret}",
                labels={"yelp.com/paasta_service": service},
            ),
            data={
                secret: base64.b64encode(
                    secret_provider.decrypt_secret_raw(secret)
                ).decode("utf-8")
            },
        ),
    )


def get_kubernetes_secret_signature(
    kube_client: KubeClient, secret: str, service: str
) -> Optional[str]:
    service = sanitise_kubernetes_name(service)
    secret = sanitise_kubernetes_name(secret)
    try:
        signature = kube_client.core.read_namespaced_config_map(
            name=f"paasta-secret-{service}-{secret}-signature", namespace="paasta"
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
    kube_client: KubeClient, secret: str, service: str, secret_signature: str
) -> None:
    service = sanitise_kubernetes_name(service)
    secret = sanitise_kubernetes_name(secret)
    kube_client.core.replace_namespaced_config_map(
        name=f"paasta-secret-{service}-{secret}-signature",
        namespace="paasta",
        body=V1ConfigMap(
            metadata=V1ObjectMeta(
                name=f"paasta-secret-{service}-{secret}-signature",
                labels={"yelp.com/paasta_service": service},
            ),
            data={"signature": secret_signature},
        ),
    )


def create_kubernetes_secret_signature(
    kube_client: KubeClient, secret: str, service: str, secret_signature: str
) -> None:
    service = sanitise_kubernetes_name(service)
    secret = sanitise_kubernetes_name(secret)
    kube_client.core.create_namespaced_config_map(
        namespace="paasta",
        body=V1ConfigMap(
            metadata=V1ObjectMeta(
                name=f"paasta-secret-{service}-{secret}-signature",
                labels={"yelp.com/paasta_service": service},
            ),
            data={"signature": secret_signature},
        ),
    )


def sanitise_kubernetes_name(service: str,) -> str:
    return service.replace("_", "--")


def load_custom_resource_definitions(
    system_paasta_config: SystemPaastaConfig,
) -> Sequence[CustomResourceDefinition]:
    custom_resources = []
    for custom_resource_dict in system_paasta_config.get_kubernetes_custom_resources():
        kube_kind = KubeKind(**custom_resource_dict.pop("kube_kind"))  # type: ignore
        custom_resources.append(
            CustomResourceDefinition(  # type: ignore
                kube_kind=kube_kind, **custom_resource_dict
            )
        )
    return custom_resources
