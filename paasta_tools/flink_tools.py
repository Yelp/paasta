# Copyright 2015-2019 Yelp Inc.
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
from typing import Any
from typing import List
from typing import Mapping
from typing import Optional

import service_configuration_lib
from kubernetes.client.rest import ApiException
from mypy_extensions import TypedDict

from paasta_tools.kubernetes_tools import InvalidJobNameError
from paasta_tools.kubernetes_tools import KubeClient
from paasta_tools.kubernetes_tools import NoConfigurationForServiceError
from paasta_tools.kubernetes_tools import sanitise_kubernetes_name
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfigDict
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_v2_deployments_json


FLINK_INGRESS_PORT = 31080


class TaskManagerConfig(TypedDict, total=False):
    instances: int


class FlinkDeploymentConfigDict(LongRunningServiceConfigDict, total=False):
    taskmanager: TaskManagerConfig


class FlinkDeploymentConfig(LongRunningServiceConfig):
    config_dict: FlinkDeploymentConfigDict

    config_filename_prefix = "flink"

    def __init__(
        self,
        service: str,
        cluster: str,
        instance: str,
        config_dict: FlinkDeploymentConfigDict,
        branch_dict: Optional[BranchDictV2],
        soa_dir: str = DEFAULT_SOA_DIR,
    ) -> None:

        super().__init__(
            cluster=cluster,
            instance=instance,
            service=service,
            soa_dir=soa_dir,
            config_dict=config_dict,
            branch_dict=branch_dict,
        )

    def validate(
        self,
        params: List[str] = [
            "cpus",
            "mem",
            "security",
            "dependencies_reference",
            "deploy_group",
        ],
    ) -> List[str]:
        # Use InstanceConfig to validate shared config keys like cpus and mem
        error_msgs = super().validate(params=params)

        if error_msgs:
            name = self.get_instance()
            return [f"{name}: {msg}" for msg in error_msgs]
        else:
            return []


def load_flink_instance_config(
    service: str,
    instance: str,
    cluster: str,
    load_deployments: bool = True,
    soa_dir: str = DEFAULT_SOA_DIR,
) -> FlinkDeploymentConfig:
    """Read a service instance's configuration for Flink.

    If a branch isn't specified for a config, the 'branch' key defaults to
    paasta-${cluster}.${instance}.

    :param service: The service name
    :param instance: The instance of the service to retrieve
    :param cluster: The cluster to read the configuration for
    :param load_deployments: A boolean indicating if the corresponding deployments.json for this service
                             should also be loaded
    :param soa_dir: The SOA configuration directory to read from
    :returns: A dictionary of whatever was in the config for the service instance"""
    general_config = service_configuration_lib.read_service_configuration(
        service, soa_dir=soa_dir
    )
    flink_conf_file = "flink-%s" % cluster
    instance_configs = service_configuration_lib.read_extra_service_information(
        service, flink_conf_file, soa_dir=soa_dir
    )

    if instance.startswith("_"):
        raise InvalidJobNameError(
            f"Unable to load kubernetes job config for {service}.{instance} as instance name starts with '_'"
        )
    if instance not in instance_configs:
        raise NoConfigurationForServiceError(
            f"{instance} not found in config file {soa_dir}/{service}/{flink_conf_file}.yaml."
        )

    general_config = deep_merge_dictionaries(
        overrides=instance_configs[instance], defaults=general_config
    )

    branch_dict: Optional[BranchDictV2] = None
    if load_deployments:
        deployments_json = load_v2_deployments_json(service, soa_dir=soa_dir)
        temp_instance_config = FlinkDeploymentConfig(
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

    return FlinkDeploymentConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=general_config,
        branch_dict=branch_dict,
        soa_dir=soa_dir,
    )


def sanitised_name(service: str, instance: str) -> str:
    sanitised_service = sanitise_kubernetes_name(service)
    sanitised_instance = sanitise_kubernetes_name(instance)
    return f"{sanitised_service}-{sanitised_instance}"


def flink_custom_object_id(service: str, instance: str) -> Mapping[str, str]:
    return dict(
        group="yelp.com",
        version="v1alpha1",
        namespace="paasta-flinks",
        plural="flinks",
        name=sanitised_name(service, instance),
    )


def get_flink_config(
    kube_client: KubeClient, service: str, instance: str
) -> Optional[Mapping[str, Any]]:
    try:
        co = kube_client.custom.get_namespaced_custom_object(
            **flink_custom_object_id(service, instance)
        )
        status = co.get("status")
        return status
    except ApiException as e:
        if e.status == 404:
            return None
        else:
            raise


def set_flink_desired_state(
    kube_client: KubeClient, service: str, instance: str, desired_state: str
) -> str:
    co_id = flink_custom_object_id(service, instance)
    co = kube_client.custom.get_namespaced_custom_object(**co_id)
    if co.get("status", {}).get("state") == desired_state:
        return co["status"]

    if "metadata" not in co:
        co["metadata"] = {}
    if "annotations" not in co["metadata"]:
        co["metadata"]["annotations"] = {}
    co["metadata"]["annotations"]["yelp.com/desired_state"] = desired_state
    kube_client.custom.replace_namespaced_custom_object(**co_id, body=co)
    status = co.get("status")
    return status


def get_dashboard_url(cluster: str, service: str, instance: str) -> str:
    sname = sanitised_name(service, instance)
    return f"http://flink.k8s.paasta-{cluster}.yelp:{FLINK_INGRESS_PORT}/{sname}"
