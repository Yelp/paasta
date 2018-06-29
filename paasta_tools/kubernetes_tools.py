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
import logging
from collections import namedtuple
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import requests
import service_configuration_lib
from kubernetes import client as kube_client
from kubernetes import config as kube_config

from paasta_tools.long_running_service_tools import load_service_namespace_config
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfigDict
from paasta_tools.long_running_service_tools import ServiceNamespaceConfig
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import decompose_job_id
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import DockerVolume
from paasta_tools.utils import get_code_sha_from_dockerurl
from paasta_tools.utils import get_config_hash
from paasta_tools.utils import InvalidJobNameError
from paasta_tools.utils import load_system_paasta_config
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import PaastaNotConfiguredError
from paasta_tools.utils import SystemPaastaConfig
from paasta_tools.utils import time_cache


log = logging.getLogger(__name__)

CONFIG_HASH_BLACKLIST = {'replicas'}
KUBE_DEPLOY_STATEGY_MAP = {'crossover': 'RollingUpdate', 'downthenup': 'Recreate'}
KubeDeployment = namedtuple('KubeDeployment', ['service', 'instance', 'git_sha', 'config_sha', 'replicas'])
KubeService = namedtuple('KubeService', ['name', 'instance', 'port', 'pod_ip'])


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
    pass


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
        super(KubernetesDeploymentConfig, self).__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            config_dict=config_dict,
            branch_dict=branch_dict,
            soa_dir=soa_dir,
        )

    def __repr__(self) -> str:
        return "KubernetesDeploymentConfig({!r}, {!r}, {!r}, {!r}, {!r}, {!r})".format(
            self.service,
            self.cluster,
            self.instance,
            self.config_dict,
            self.branch_dict,
            self.soa_dir,
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

    def get_bounce_method(self) -> str:
        """Get the bounce method specified in the service's kubernetes configuration."""
        # map existing bounce methods to k8s equivalents.
        return KUBE_DEPLOY_STATEGY_MAP[self.config_dict.get('bounce_method', 'crossover')]

    def get_deployment_strategy_config(self) -> Dict[str, Any]:
        strategy_dict: Dict[str, Any] = {}
        strategy_dict['type'] = self.get_bounce_method()
        if self.get_bounce_method() == 'RollingUpdate':
            # this translates bounce_margin to k8s speak maxUnavailable
            # for now we keep max_surge 100% but we could customise later
            strategy_dict['rollingUpdate'] = {
                "maxSurge": '100%',
                "maxUnavailable": "{}%".format(int((1 - self.get_bounce_margin_factor()) * 100)),
            }
        return strategy_dict

    def get_sanitised_volume_name(self, volume_name: str) -> str:
        """I know but we really aren't allowed many characters..."""
        volume_name = volume_name.rstrip('/')
        sanitised = volume_name.replace('/', 'slash-')
        return sanitised.replace('_', '--')

    def get_sidecar_containers(self, system_paasta_config: SystemPaastaConfig) -> List[Dict[str, Any]]:
        registrations = " ".join(self.get_registrations())
        hacheck_sidecar = {
            "image": system_paasta_config.get_hacheck_sidecar_image_url(),
            "lifecycle": {
                "preStop": {
                    "exec": {
                        "command": [
                            "/bin/sh",
                            "-c",
                            f"/usr/bin/hadown {registrations}; sleep 31",
                        ],
                    },
                },
            },
            "name": "hacheck",
            "env": self.get_kubernetes_environment(),
            "ports": [
                {
                    "containerPort": 6666,
                },
            ],
        }
        # s_m_j currently asserts that services are healthy in smartstack before
        # continuing a bounce. this readinees check lets us acheive the same thing
        if system_paasta_config.get_enable_nerve_readiness_check():
            hacheck_sidecar['readinessProbe'] = {
                'exec': {
                    'initialDelaySeconds': 10,
                    'periodSeconds': 10,
                    'command': [
                        system_paasta_config.get_nerve_readiness_check_script(),
                    ] + self.get_registrations(),
                },
            }
        return [hacheck_sidecar]

    def get_container_env(self) -> List[Dict[str, str]]:
        user_env = [{'name': name, 'value': value} for name, value in self.get_env().items()]
        return user_env + self.get_kubernetes_environment()

    def get_kubernetes_environment(self) -> List[Dict[str, Any]]:
        kubernetes_env = [{
            'name': 'PAASTA_POD_IP',
            'valueFrom': {
                'fieldRef': {
                    'fieldPath': 'status.podIP',
                },
            },
        }]
        return kubernetes_env

    def get_kubernetes_containers(
        self,
        volumes: List[DockerVolume],
        system_paasta_config: SystemPaastaConfig,
    ) -> List[Dict[str, Any]]:
        service_container = {
            "image": self.get_docker_url(),
            "cmd": self.get_cmd(),
            "args": self.get_args(),
            "env": self.get_container_env(),
            "lifecycle": {
                "preStop": {
                    "exec": {
                        "command": [
                            "/bin/sh",
                            "-c",
                            "sleep 30",
                        ],
                    },
                },
            },
            "name": "{service}-{instance}".format(
                service=self.get_sanitised_service_name(),
                instance=self.get_sanitised_instance_name(),
            ),
            "livenessProbe": {
                "failureThreshold": 10,
                "httpGet": {
                    "path": "/status",
                    "port": 8888,
                },
                "initialDelaySeconds": 15,
                "periodSeconds": 10,
                "timeoutSeconds": 5,
            },
            "ports": [
                {
                    "containerPort": 8888,
                },
            ],
            "volumeMounts": self.get_volume_mounts(volumes=volumes),
        }
        containers = [service_container] + self.get_sidecar_containers(system_paasta_config=system_paasta_config)
        return containers

    def get_pod_volumes(self, volumes: List[DockerVolume]) -> List[Dict[str, Any]]:
        pod_volumes = []
        for volume in volumes:
            pod_volumes.append({
                "hostPath": {
                    "path": volume['hostPath'],
                },
                "name": self.get_sanitised_volume_name(volume['containerPath']),
            })
        return pod_volumes

    def get_volume_mounts(self, volumes: List[DockerVolume]) -> List[Dict[str, Any]]:
        volume_mounts = []
        for volume in volumes:
            volume_mounts.append({
                "mountPath": volume['containerPath'],
                "name": self.get_sanitised_volume_name(volume['containerPath']),
                "readOnly": True if volume.get('mode', 'RO') == 'RO' else False,
            })
        return volume_mounts

    def get_sanitised_service_name(self) -> str:
        return self.get_service().replace('_', '--')

    def get_sanitised_instance_name(self) -> str:
        return self.get_instance().replace('_', '--')

    def format_kubernetes_app_dict(self) -> Dict[str, Any]:
        """Create the configuration that will be passed to the Kubernetes REST API."""

        system_paasta_config = load_system_paasta_config()
        docker_url = self.get_docker_url()
        # service_namespace_config = load_service_namespace_config(
        #     service=self.service,
        #     namespace=self.get_nerve_namespace(),
        # )
        docker_volumes = self.get_volumes(system_volumes=system_paasta_config.get_volumes())

        code_sha = get_code_sha_from_dockerurl(docker_url)
        complete_config: Dict[str, Any] = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": "{service}-{instance}".format(
                    service=self.get_sanitised_service_name(),
                    instance=self.get_sanitised_instance_name(),
                ),
                "labels": {
                    "service": self.get_service(),
                    "instance": self.get_instance(),
                },
            },
            "spec": {
                "replicas": self.get_instances(),
                "selector": {
                    "matchLabels": {
                        "service": self.get_service(),
                        "instance": self.get_instance(),
                    },
                },
                "strategy": self.get_deployment_strategy_config(),
                "template": {
                    "metadata": {
                        "labels": {
                            "service": self.get_service(),
                            "instance": self.get_instance(),
                        },
                    },
                    "spec": {
                        "containers": self.get_kubernetes_containers(
                            volumes=docker_volumes,
                            system_paasta_config=system_paasta_config,
                        ),
                        "restartPolicy": "Always",
                        "volumes": self.get_pod_volumes(docker_volumes),
                    },
                },
            },
        }

        config_hash = get_config_hash(
            self.sanitize_for_config_hash(complete_config),
            force_bounce=self.get_force_bounce(),
        )
        complete_config['metadata']['labels']['config_sha'] = config_hash
        complete_config['spec']['template']['metadata']['labels']['config_sha'] = config_hash
        complete_config['metadata']['labels']['git_sha'] = code_sha
        complete_config['spec']['template']['metadata']['labels']['git_sha'] = code_sha

        log.debug("Complete configuration for instance is: %s", complete_config)
        return complete_config

    def sanitize_for_config_hash(
        self,
        config: Dict,
    ) -> Dict[str, Any]:
        """Removes some data from config to make it suitable for
        calculation of config hash.

        :param config: complete_config hash to sanitize
        :returns: sanitized copy of complete_config hash
        """
        ahash = {key: copy.deepcopy(value) for key, value in config.items() if key not in CONFIG_HASH_BLACKLIST}
        spec = ahash['spec']
        ahash['spec'] = {key: copy.deepcopy(value) for key, value in spec.items() if key not in CONFIG_HASH_BLACKLIST}
        return ahash

    def get_bounce_margin_factor(self) -> float:
        return self.config_dict.get('bounce_margin_factor', 1.0)


def read_all_registrations_for_service_instance(
    service: str,
    instance: str,
    cluster: Optional[str]=None,
    soa_dir: str=DEFAULT_SOA_DIR,
) -> List[str]:
    """Retreive all registrations as fully specified name.instance pairs
    for a particular service instance.

    For example, the 'main' paasta instance of the 'test' service may register
    in the 'test.main' namespace as well as the 'other_svc.main' namespace.

    If one is not defined in the config file, returns a list containing
    name.instance instead.
    """
    if not cluster:
        cluster = load_system_paasta_config().get_cluster()

    kubernetes_service_config = load_kubernetes_service_config(
        service=service,
        instance=instance,
        cluster=cluster,
        load_deployments=False,
        soa_dir=soa_dir,
    )
    return kubernetes_service_config.get_registrations()


def get_kubernetes_services_running_here() -> List[KubeService]:
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
) -> List[Tuple[str, ServiceNamespaceConfig]]:
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
            registrations = read_all_registrations_for_service_instance(
                kubernetes_service.name, kubernetes_service.instance, cluster, soa_dir,
            )
            for registration in registrations:
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


class KubeClient():
    def __init__(self) -> None:
        kube_config.load_kube_config(config_file='/etc/kubernetes/admin.conf')
        self.deployments = kube_client.AppsV1Api()
        self.core = kube_client.CoreV1Api()


def ensure_paasta_namespace(kube_client: KubeClient) -> None:
    paasta_namespace = {
        "kind": "Namespace",
        "apiVersion": "v1",
        "metadata": {
            "name": "paasta",
            "labels": {
                "name": "paasta",
            },
        },
    }
    namespaces = kube_client.core.list_namespace()
    namespace_names = [item.metadata.name for item in namespaces.items]
    if 'paasta' not in namespace_names:
        log.warning("Creating paasta namespace as it does not exist")
        kube_client.core.create_namespace(body=paasta_namespace)


def list_all_deployments(kube_client: KubeClient) -> List[KubeDeployment]:
    deployments = kube_client.deployments.list_namespaced_deployment(namespace='paasta')
    return [
        KubeDeployment(
            service=item.metadata.labels['service'],
            instance=item.metadata.labels['instance'],
            git_sha=item.metadata.labels['git_sha'],
            config_sha=item.metadata.labels['config_sha'],
            replicas=item.spec.replicas,
        ) for item in deployments.items
    ]


def create_deployment(kube_client: KubeClient, formatted_deployment_dict: Dict[str, Any]) -> None:
    return kube_client.deployments.create_namespaced_deployment(
        namespace='paasta',
        body=formatted_deployment_dict,
    )


def update_deployment(kube_client: KubeClient, formatted_deployment_dict: Dict[str, Any]) -> None:
    return kube_client.deployments.patch_namespaced_deployment(
        name=formatted_deployment_dict['metadata']['name'],
        namespace='paasta',
        body=formatted_deployment_dict,
    )
