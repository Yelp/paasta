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
"""
"""
import copy
import itertools
import logging
import math
from datetime import datetime
from typing import Any
from typing import Dict
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
from kubernetes.client import V1Container
from kubernetes.client import V1ContainerPort
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
from kubernetes.client import V1StatefulSet
from kubernetes.client import V1StatefulSetSpec
from kubernetes.client import V1TCPSocketAction
from kubernetes.client import V1Volume
from kubernetes.client import V1VolumeMount
from kubernetes.client.rest import ApiException

from paasta_tools.long_running_service_tools import host_passes_blacklist
from paasta_tools.long_running_service_tools import host_passes_whitelist
from paasta_tools.long_running_service_tools import InvalidHealthcheckMode
from paasta_tools.long_running_service_tools import load_service_namespace_config
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfigDict
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
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

YELP_ATTRIBUTE_PREFIX = 'yelp.com/'
CONFIG_HASH_BLACKLIST = {'replicas'}
KUBE_DEPLOY_STATEGY_MAP = {'crossover': 'RollingUpdate', 'downthenup': 'Recreate'}
KUBE_DEPLOY_STATEGY_REVMAP = {v: k for k, v in KUBE_DEPLOY_STATEGY_MAP.items()}
KubeDeployment = NamedTuple(
    'KubeDeployment', [
        ('service', str),
        ('instance', str),
        ('git_sha', str),
        ('config_sha', str),
        ('replicas', int),
    ],
)
KubeService = NamedTuple(
    'KubeService', [
        ('name', str),
        ('instance', str),
        ('port', int),
        ('pod_ip', str),
    ],
)


def _set_disrupted_pods(self: Any, disrupted_pods: Mapping[str, datetime]) -> None:
    """Private function used to patch the setter for V1beta1PodDisruptionBudgetStatus.
    Can be removed once https://github.com/kubernetes-client/python/issues/466 is resolved
    """
    self._disrupted_pods = disrupted_pods


class KubernetesDeploymentConfigDict(LongRunningServiceConfigDict, total=False):
    bounce_method: str
    bounce_margin_factor: float


def load_kubernetes_service_config_no_cache(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool=True,
    soa_dir: str=DEFAULT_SOA_DIR,
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
        service,
        soa_dir=soa_dir,
    )
    kubernetes_conf_file = "kubernetes-%s" % cluster
    instance_configs = service_configuration_lib.read_extra_service_information(
        service,
        kubernetes_conf_file,
        soa_dir=soa_dir,
    )

    if instance.startswith('_'):
        raise InvalidJobNameError(
            f"Unable to load kubernetes job config for {service}.{instance} as instance name starts with '_'",
        )
    if instance not in instance_configs:
        raise NoConfigurationForServiceError(
            f"{instance} not found in config file {soa_dir}/{service}/{kubernetes_conf_file}.yaml.",
        )

    general_config = deep_merge_dictionaries(overrides=instance_configs[instance], defaults=general_config)

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
    load_deployments: bool=True,
    soa_dir: str=DEFAULT_SOA_DIR,
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
        super().__init__(f"Couldn't generate config for kubernetes service: {service}.{instance}: {exception}")


class KubernetesDeploymentConfig(LongRunningServiceConfig):
    config_dict: KubernetesDeploymentConfigDict

    config_filename_prefix = 'kubernetes'

    def __init__(
        self,
        service: str,
        cluster: str,
        instance: str,
        config_dict: KubernetesDeploymentConfigDict,
        branch_dict: Optional[BranchDictV2],
        soa_dir: str=DEFAULT_SOA_DIR,
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
            config_dict=dict(self.config_dict),
            branch_dict=dict(self.branch_dict) if self.branch_dict is not None else None,
            soa_dir=self.soa_dir,
        )

    def get_cmd(self) -> Optional[List[str]]:
        cmd = super(LongRunningServiceConfig, self).get_cmd()
        if cmd:
            if isinstance(cmd, str):
                return ['sh', '-c', cmd]
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
        bounce_method = self.config_dict.get('bounce_method', 'crossover')
        if self.get_aws_ebs_volumes() and not bounce_method == 'downthenup':
            raise Exception("If service instance defines an EBS volume it must use a downthenup bounce_method")
        return KUBE_DEPLOY_STATEGY_MAP[bounce_method]

    def get_deployment_strategy_config(self) -> V1DeploymentStrategy:
        strategy_type = self.get_bounce_method()
        rolling_update: Optional[V1RollingUpdateDeployment]
        if strategy_type == 'RollingUpdate':
            # this translates bounce_margin to k8s speak maxUnavailable
            # for now we keep max_surge 100% but we could customise later
            rolling_update = V1RollingUpdateDeployment(
                max_surge='100%',
                max_unavailable="{}%".format(int((1 - self.get_bounce_margin_factor()) * 100)),
            )
        else:
            rolling_update = None

        strategy = V1DeploymentStrategy(
            type=strategy_type,
            rolling_update=rolling_update,
        )
        return strategy

    def get_sanitised_volume_name(self, volume_name: str) -> str:
        """I know but we really aren't allowed many characters..."""
        volume_name = volume_name.rstrip('/')
        sanitised = volume_name.replace('/', 'slash-')
        return sanitised.replace('_', '--')

    def get_docker_volume_name(self, docker_volume: DockerVolume) -> str:
        return self.get_sanitised_volume_name(
            'host--{name}'.format(name=docker_volume['hostPath']),
        )

    def get_persistent_volume_name(self, docker_volume: PersistentVolume) -> str:
        return self.get_sanitised_volume_name(
            'pv--{name}'.format(name=docker_volume['container_path']),
        )

    def get_aws_ebs_volume_name(self, aws_ebs_volume: AwsEbsVolume) -> str:
        return self.get_sanitised_volume_name(
            'aws-ebs--{name}{partition}'.format(
                name=aws_ebs_volume['volume_id'],
                partition=aws_ebs_volume.get('partition', ''),
            ),
        )

    def read_only_mode(self, d: VolumeWithMode) -> bool:
        return d.get('mode', 'RO') == 'RO'

    def get_sidecar_containers(
        self,
        system_paasta_config: SystemPaastaConfig,
        service_namespace_config: ServiceNamespaceConfig,
    ) -> List[V1Container]:
        registrations = " ".join(self.get_registrations())
        # s_m_j currently asserts that services are healthy in smartstack before
        # continuing a bounce. this readiness check lets us achieve the same thing
        readiness_probe: Optional[V1Probe]
        if system_paasta_config.get_enable_nerve_readiness_check():
            readiness_probe = V1Probe(
                _exec=V1ExecAction(
                    command=[
                        system_paasta_config.get_nerve_readiness_check_script(),
                    ] + self.get_registrations(),
                ),
                initial_delay_seconds=10,
                period_seconds=10,
            )
        else:
            readiness_probe = None

        sidecars = []
        if service_namespace_config.is_in_smartstack():
            sidecars.append(V1Container(
                image=system_paasta_config.get_hacheck_sidecar_image_url(),
                lifecycle=V1Lifecycle(
                    pre_stop=V1Handler(
                        _exec=V1ExecAction(
                            command=[
                                "/bin/sh",
                                "-c",
                                f"/usr/bin/hadown {registrations}; sleep 31",
                            ],
                        ),
                    ),
                ),
                name="hacheck",
                env=self.get_kubernetes_environment(),
                ports=[
                    V1ContainerPort(
                        container_port=6666,
                    ),
                ],
                readiness_probe=readiness_probe,
            ))
        return sidecars

    def get_container_env(self) -> Sequence[V1EnvVar]:
        user_env = [V1EnvVar(name=name, value=value) for name, value in self.get_env().items()]
        return user_env + self.get_kubernetes_environment()

    def get_kubernetes_environment(self) -> List[V1EnvVar]:
        kubernetes_env = [
            V1EnvVar(
                name='PAASTA_POD_IP',
                value_from=V1EnvVarSource(
                    field_ref=V1ObjectFieldSelector(
                        field_path='status.podIP',
                    ),
                ),
            ),
        ]
        return kubernetes_env

    def get_resource_requirements(self) -> V1ResourceRequirements:
        return V1ResourceRequirements(
            limits={
                'cpu': self.get_cpus() + self.get_cpu_burst_add(),
                'memory': f'{self.get_mem()}Mi',
            },
            requests={
                'cpu': self.get_cpus(),
                'memory': f'{self.get_mem()}Mi',
            },
        )

    def get_liveness_probe(
        self,
        service_namespace_config: ServiceNamespaceConfig,
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

        if mode == 'http' or mode == 'https':
            path = self.get_healthcheck_uri(service_namespace_config)
            probe.http_get = V1HTTPGetAction(
                path=path,
                port=8888,
                scheme=mode.upper(),
            )
        elif mode == 'tcp':
            probe.tcp_socket = V1TCPSocketAction(
                port=8888,
            )
        elif mode == 'cmd':
            probe._exec = V1ExecAction(
                command=self.get_healthcheck_cmd(),
            )
        else:
            raise InvalidHealthcheckMode(
                "Unknown mode: %s. Only acceptable healthcheck modes are http/https/tcp" % mode,
            )

        return probe

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
                    _exec=V1ExecAction(
                        command=[
                            "/bin/sh",
                            "-c",
                            "sleep 30",
                        ],
                    ),
                ),
            ),
            name=self.get_sanitised_deployment_name(),
            liveness_probe=self.get_liveness_probe(service_namespace_config),
            ports=[
                V1ContainerPort(
                    container_port=8888,
                ),
            ],
            volume_mounts=self.get_volume_mounts(
                docker_volumes=docker_volumes,
                aws_ebs_volumes=aws_ebs_volumes,
                persistent_volumes=self.get_persistent_volumes(),
            ),
        )
        containers = [service_container] + self.get_sidecar_containers(
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
                    host_path=V1HostPathVolumeSource(
                        path=docker_volume['hostPath'],
                    ),
                    name=name,
                ),
            )
        unique_aws_ebs_volumes = {
            self.get_aws_ebs_volume_name(aws_ebs_volume): aws_ebs_volume
            for aws_ebs_volume in aws_ebs_volumes
        }
        for name, aws_ebs_volume in unique_aws_ebs_volumes.items():
            pod_volumes.append(
                V1Volume(
                    aws_elastic_block_store=V1AWSElasticBlockStoreVolumeSource(
                        volume_id=aws_ebs_volume['volume_id'],
                        fs_type=aws_ebs_volume.get('fs_type'),
                        partition=aws_ebs_volume.get('partition'),
                        # k8s wants RW volume even if it's later mounted RO
                        read_only=False,
                    ),
                    name=name,
                ),
            )
        return pod_volumes

    def get_volume_mounts(
        self,
        docker_volumes: Sequence[DockerVolume],
        aws_ebs_volumes: Sequence[AwsEbsVolume],
        persistent_volumes: Sequence[PersistentVolume],
    ) -> Sequence[V1VolumeMount]:
        return [
            V1VolumeMount(
                mount_path=docker_volume['containerPath'],
                name=self.get_docker_volume_name(docker_volume),
                read_only=self.read_only_mode(docker_volume),
            )
            for docker_volume in docker_volumes
        ] + [
            V1VolumeMount(
                mount_path=aws_ebs_volume['container_path'],
                name=self.get_aws_ebs_volume_name(aws_ebs_volume),
                read_only=self.read_only_mode(aws_ebs_volume),
            )
            for aws_ebs_volume in aws_ebs_volumes
        ] + [
            V1VolumeMount(
                mount_path=volume['container_path'],
                name=self.get_persistent_volume_name(volume),
                read_only=self.read_only_mode(volume),
            )
            for volume in persistent_volumes
        ]

    def get_sanitised_service_name(self) -> str:
        return self.get_service().replace('_', '--')

    def get_sanitised_instance_name(self) -> str:
        return self.get_instance().replace('_', '--')

    def get_desired_instances(self) -> int:
        """ For now if we have an EBS instance it means we can only have 1 instance
        since we can't attach to multiple instances. In the future we might support
        statefulsets which are clever enough to manage EBS for you"""
        instances = super().get_desired_instances()
        if self.get_aws_ebs_volumes() and instances not in [1, 0]:
            raise Exception("Number of instances must be 1 or 0 if an EBS volume is defined.")
        return instances

    def get_volume_claim_templates(self) -> Sequence[V1PersistentVolumeClaim]:
        return [
            V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(
                    name=self.get_persistent_volume_name(volume),
                ),
                spec=V1PersistentVolumeClaimSpec(
                    # must be ReadWriteOnce for EBS
                    access_modes=["ReadWriteOnce"],
                    storage_class_name=self.get_storage_class_name(),
                    resources=V1ResourceRequirements(
                        requests={
                            'storage': f"{volume['size']}Gi",
                        },
                    ),
                ),
            ) for volume in self.get_persistent_volumes()
        ]

    def get_storage_class_name(self) -> str:
        # TODO: once we support node affinity this method should return the
        # name of the storage class for a particular AZ. We should also enforce
        # that a storage class exists with the correct name (like we do for the
        # paasta namespace object)
        return "ebs-gp2-us-west-1a"

    def get_kubernetes_metadata(self, code_sha: str) -> V1ObjectMeta:
        return V1ObjectMeta(
            name="{service}-{instance}".format(
                service=self.get_sanitised_service_name(),
                instance=self.get_sanitised_instance_name(),
            ),
            labels={
                "service": self.get_service(),
                "instance": self.get_instance(),
                "git_sha": code_sha,
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
            if self.get_persistent_volumes():
                complete_config = V1StatefulSet(
                    api_version='apps/v1',
                    kind='StatefulSet',
                    metadata=self.get_kubernetes_metadata(code_sha),
                    spec=V1StatefulSetSpec(
                        service_name="{service}-{instance}".format(
                            service=self.get_sanitised_service_name(),
                            instance=self.get_sanitised_instance_name(),
                        ),
                        volume_claim_templates=self.get_volume_claim_templates(),
                        replicas=self.get_desired_instances(),
                        selector=V1LabelSelector(
                            match_labels={
                                "service": self.get_service(),
                                "instance": self.get_instance(),
                            },
                        ),
                        template=self.get_pod_template_spec(
                            code_sha=code_sha,
                            system_paasta_config=system_paasta_config,
                        ),
                    ),
                )
            else:
                complete_config = V1Deployment(
                    api_version='apps/v1',
                    kind='Deployment',
                    metadata=self.get_kubernetes_metadata(code_sha),
                    spec=V1DeploymentSpec(
                        replicas=self.get_desired_instances(),
                        selector=V1LabelSelector(
                            match_labels={
                                "service": self.get_service(),
                                "instance": self.get_instance(),
                            },
                        ),
                        template=self.get_pod_template_spec(
                            code_sha=code_sha,
                            system_paasta_config=system_paasta_config,
                        ),
                        strategy=self.get_deployment_strategy_config(),
                    ),
                )

            config_hash = get_config_hash(
                self.sanitize_for_config_hash(complete_config),
                force_bounce=self.get_force_bounce(),
            )
            complete_config.metadata.labels['config_sha'] = config_hash
            complete_config.spec.template.metadata.labels['config_sha'] = config_hash
        except Exception as e:
            raise InvalidKubernetesConfig(e, self.get_service(), self.get_instance())
        log.debug("Complete configuration for instance is: %s", complete_config)
        return complete_config

    def get_pod_template_spec(
        self,
        code_sha: str,
        system_paasta_config: SystemPaastaConfig,
    ) -> V1PodTemplateSpec:
        service_namespace_config = load_service_namespace_config(
            service=self.service,
            namespace=self.get_nerve_namespace(),
        )
        docker_volumes = self.get_volumes(system_volumes=system_paasta_config.get_volumes())
        return V1PodTemplateSpec(
            metadata=V1ObjectMeta(
                labels={
                    "service": self.get_service(),
                    "instance": self.get_instance(),
                    "git_sha": code_sha,
                },
            ),
            spec=V1PodSpec(
                containers=self.get_kubernetes_containers(
                    docker_volumes=docker_volumes,
                    aws_ebs_volumes=self.get_aws_ebs_volumes(),
                    system_paasta_config=system_paasta_config,
                    service_namespace_config=service_namespace_config,
                ),
                restart_policy="Always",
                volumes=self.get_pod_volumes(
                    docker_volumes=docker_volumes,
                    aws_ebs_volumes=self.get_aws_ebs_volumes(),
                ),
            ),
        )

    def sanitize_for_config_hash(
        self,
        config: V1Deployment,
    ) -> Dict[str, Any]:
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
        spec = ahash['spec']
        ahash['spec'] = {
            key: copy.deepcopy(value)
            for key, value in spec.items()
            if key not in CONFIG_HASH_BLACKLIST
        }
        return ahash

    def get_bounce_margin_factor(self) -> float:
        return self.config_dict.get('bounce_margin_factor', 1.0)


def get_kubernetes_services_running_here() -> Sequence[KubeService]:
    services = []
    pods = requests.get('http://127.0.0.1:10255/pods').json()
    for pod in pods['items']:
        if pod['status']['phase'] != 'Running' or pod['metadata']['namespace'] != 'paasta':
            continue
        try:
            services.append(KubeService(
                name=pod['metadata']['labels']['service'],
                instance=pod['metadata']['labels']['instance'],
                port=8888,
                pod_ip=pod['status']['podIP'],
            ))
        except KeyError as e:
            log.warning(f"Found running paasta pod but missing {e} key so not registering with nerve")
    return services


def get_kubernetes_services_running_here_for_nerve(
    cluster: str,
    soa_dir: str,
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
            kubernetes_service_config = load_kubernetes_service_config(
                service=kubernetes_service.name,
                instance=kubernetes_service.instance,
                cluster=cluster,
                load_deployments=False,
                soa_dir=soa_dir,
            )
            for registration in kubernetes_service_config.get_registrations():
                reg_service, reg_namespace, _, __ = decompose_job_id(registration)
                nerve_dict = load_service_namespace_config(
                    service=reg_service, namespace=reg_namespace, soa_dir=soa_dir,
                )
                if not nerve_dict.is_in_smartstack():
                    continue
                nerve_dict['port'] = kubernetes_service.port
                nerve_dict['service_ip'] = kubernetes_service.pod_ip
                nerve_dict['hacheck_ip'] = kubernetes_service.pod_ip
                nerve_list.append((registration, nerve_dict))
        except (KeyError, NoConfigurationForServiceError):
            continue  # SOA configs got deleted for this app, it'll get cleaned up

    return nerve_list


class KubeClient:
    def __init__(self) -> None:
        kube_config.load_kube_config(config_file='/etc/kubernetes/admin.conf')
        models.V1beta1PodDisruptionBudgetStatus.disrupted_pods = property(
            fget=lambda *args, **kwargs: models.V1beta1PodDisruptionBudgetStatus.disrupted_pods(*args, **kwargs),
            fset=_set_disrupted_pods,
        )
        self.deployments = kube_client.AppsV1Api()
        self.core = kube_client.CoreV1Api()
        self.policy = kube_client.PolicyV1beta1Api()


def ensure_paasta_namespace(kube_client: KubeClient) -> None:
    paasta_namespace = V1Namespace(
        metadata=V1ObjectMeta(
            name="paasta",
            labels={
                "name": "paasta",
            },
        ),
    )
    namespaces = kube_client.core.list_namespace()
    namespace_names = [item.metadata.name for item in namespaces.items]
    if 'paasta' not in namespace_names:
        log.warning("Creating paasta namespace as it does not exist")
        kube_client.core.create_namespace(body=paasta_namespace)


def list_deployments(
    kube_client: KubeClient,
    label_selector: str = '',
) -> Sequence[KubeDeployment]:
    deployments = kube_client.deployments.list_namespaced_deployment(
        namespace='paasta',
        label_selector=label_selector,
    )
    stateful_sets = kube_client.deployments.list_namespaced_stateful_set(
        namespace='paasta',
        label_selector=label_selector,
    )
    return [
        KubeDeployment(
            service=item.metadata.labels['service'],
            instance=item.metadata.labels['instance'],
            git_sha=item.metadata.labels['git_sha'],
            config_sha=item.metadata.labels['config_sha'],
            replicas=item.spec.replicas,
        ) for item in deployments.items + stateful_sets.items
    ]


def max_unavailable(instance_count: int, bounce_margin_factor: float) -> int:
    if instance_count == 0:
        return 0
    else:
        return max(
            instance_count - int(math.ceil(instance_count * bounce_margin_factor)),
            1,
        )


def pod_disruption_budget_for_service_instance(
    service: str,
    instance: str,
    min_instances: int,
) -> V1beta1PodDisruptionBudget:
    return V1beta1PodDisruptionBudget(
        metadata=V1ObjectMeta(
            name=f"{service}-{instance}",
            namespace="paasta",
        ),
        spec=V1beta1PodDisruptionBudgetSpec(
            min_available=min_instances,
            selector=V1LabelSelector(
                match_labels={
                    "service": service,
                    "instance": instance,
                },
            ),
        ),
    )


def create_pod_disruption_budget(kube_client: KubeClient, pod_disruption_budget: V1beta1PodDisruptionBudget) -> None:
    return kube_client.policy.create_namespaced_pod_disruption_budget(
        namespace='paasta',
        body=pod_disruption_budget,
    )


def list_all_deployments(kube_client: KubeClient) -> Sequence[KubeDeployment]:
    return list_deployments(kube_client)


def list_matching_deployments(
    service: str,
    instance: str,
    kube_client: KubeClient,
) -> Sequence[KubeDeployment]:
    return list_deployments(kube_client, f'instance={instance},service={service}')


def pods_for_service_instance(
    service: str,
    instance: str,
    kube_client: KubeClient,
) -> Sequence[V1Pod]:
    return kube_client.core.list_namespaced_pod(
        namespace='paasta',
        label_selector=f'service={service},instance={instance}',
    ).items


def get_all_pods(
    kube_client: KubeClient,
) -> Sequence[V1Pod]:
    return kube_client.core.list_namespaced_pod(
        namespace='paasta',
    ).items


def filter_pods_by_service_instance(
    pod_list: Sequence[V1Pod],
    service: str,
    instance: str,
) -> Sequence[V1Pod]:
    return [
        pod for pod in pod_list
        if pod.metadata.labels['service'] == service and
        pod.metadata.labels['instance'] == instance
    ]


def is_pod_ready(
    pod: V1Pod,
) -> bool:
    ready_conditions = [cond.status for cond in pod.status.conditions if cond.type == 'Ready']
    return all(ready_conditions) if ready_conditions else False


def get_active_shas_for_service(
    pod_list: Sequence[V1Pod],
) -> Mapping[str, Set[str]]:
    ret: Mapping[str, Set[str]] = {'config_sha': set(), 'git_sha': set()}
    for pod in pod_list:
        ret['config_sha'].add(pod.metadata.labels['config_sha'])
        ret['git_sha'].add(pod.metadata.labels['git_sha'])
    return ret


def get_all_nodes(
    kube_client: KubeClient,
) -> Sequence[V1Node]:
    return kube_client.core.list_node().items


def filter_nodes_by_blacklist(
    nodes: Sequence[V1Node],
    blacklist: DeployBlacklist,
    whitelist: DeployWhitelist,
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
        node for node in nodes if host_passes_whitelist(
            node.metadata.labels,
            whitelist,
        ) and host_passes_blacklist(
            node.metadata.labels,
            blacklist,
        )
    ]


def maybe_add_yelp_prefix(
    attribute: str,
) -> str:
    return YELP_ATTRIBUTE_PREFIX + attribute if '/' not in attribute else attribute


def get_nodes_grouped_by_attribute(
    nodes: Sequence[V1Node],
    attribute: str,
) -> Mapping[str, Sequence[V1Node]]:
    attribute = maybe_add_yelp_prefix(attribute)
    sorted_nodes = sorted(
        nodes,
        key=lambda node: node.metadata.labels.get(attribute, ""),
    )
    return {
        key: list(group) for key, group in itertools.groupby(
            sorted_nodes,
            key=lambda node: node.metadata.labels.get(attribute, ""),
        ) if key
    }


def get_kubernetes_app_by_name(
    name: str,
    kube_client: KubeClient,
) -> Union[V1Deployment, V1StatefulSet]:
    try:
        app = kube_client.deployments.read_namespaced_deployment_status(
            name=name,
            namespace='paasta',
        )
        return app
    except ApiException as e:
        if e.status == 404:
            pass
        else:
            raise
    return kube_client.deployments.read_namespaced_stateful_set_status(
        name=name,
        namespace='paasta',
    )


def create_deployment(kube_client: KubeClient, formatted_deployment: V1Deployment) -> None:
    return kube_client.deployments.create_namespaced_deployment(
        namespace='paasta',
        body=formatted_deployment,
    )


def update_deployment(kube_client: KubeClient, formatted_deployment: V1Deployment) -> None:
    return kube_client.deployments.replace_namespaced_deployment(
        name=formatted_deployment.metadata.name,
        namespace='paasta',
        body=formatted_deployment,
    )


def create_stateful_set(kube_client: KubeClient, formatted_stateful_set: V1StatefulSet) -> None:
    return kube_client.deployments.create_namespaced_stateful_set(
        namespace='paasta',
        body=formatted_stateful_set,
    )


def update_stateful_set(kube_client: KubeClient, formatted_stateful_set: V1StatefulSet) -> None:
    return kube_client.deployments.replace_namespaced_stateful_set(
        name=formatted_stateful_set.metadata.name,
        namespace='paasta',
        body=formatted_stateful_set,
    )


def get_kubernetes_app_deploy_status(
    kube_client: KubeClient,
    app: Union[V1Deployment, V1StatefulSet],
    desired_instances: int,
) -> int:
    if app.status.ready_replicas is None or app.status.ready_replicas < desired_instances:
        deploy_status = KubernetesDeployStatus.Waiting
    # updated_replicas can currently be None for stateful sets so we may not correctly detect status for now
    # when https://github.com/kubernetes/kubernetes/pull/62943 lands in a release this should work for both:
    elif app.status.updated_replicas is not None and (app.status.updated_replicas < desired_instances):
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
