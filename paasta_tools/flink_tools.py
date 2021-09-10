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
import json
from typing import Any
from typing import List
from typing import Mapping
from typing import Optional

import requests
import service_configuration_lib
from mypy_extensions import TypedDict

from paasta_tools.kubernetes_tools import sanitised_cr_name
from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfigDict
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_service_instance_config
from paasta_tools.utils import load_v2_deployments_json


FLINK_INGRESS_PORT = 31080
FLINK_DASHBOARD_TIMEOUT_SECONDS = 5


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

    # Since Flink services are stateful, losing capacity is not transparent to the users
    def get_replication_crit_percentage(self) -> int:
        return self.config_dict.get("replication_threshold", 100)


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
    instance_config = load_service_instance_config(
        service, instance, "flink", cluster, soa_dir=soa_dir
    )
    general_config = deep_merge_dictionaries(
        overrides=instance_config, defaults=general_config
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


# TODO: read this from CRD in service configs
def cr_id(service: str, instance: str) -> Mapping[str, str]:
    return dict(
        group="yelp.com",
        version="v1alpha1",
        namespace="paasta-flinks",
        plural="flinks",
        name=sanitised_cr_name(service, instance),
    )


def get_flink_ingress_url_root(cluster: str) -> str:
    return f"http://flink.k8s.{cluster}.paasta:{FLINK_INGRESS_PORT}/"


def _dashboard_get(cr_name: str, cluster: str, path: str) -> str:
    root = get_flink_ingress_url_root(cluster)
    url = f"{root}{cr_name}/{path}"
    response = requests.get(url, timeout=FLINK_DASHBOARD_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


def get_flink_jobmanager_overview(cr_name: str, cluster: str) -> Mapping[str, Any]:
    try:
        response = _dashboard_get(cr_name, cluster, "overview")
        return json.loads(response)
    except requests.RequestException as e:
        url = e.request.url
        err = e.response or str(e)
        raise ValueError(f"failed HTTP request to Jobmanager dashboard {url}: {err}")
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON decoding error from Jobmanager dashboard: {e}")
    except ConnectionError as e:
        raise ValueError(f"failed HTTP request to Jobmanager dashboard: {e}")


def get_flink_job_exceptions(
    cr_name: str, cluster: str, job_id: str
) -> Mapping[str, Any]:
    try:
        response = _dashboard_get(cr_name, cluster, f"/jobs/{job_id}/exceptions")
        return json.loads(response)
    except requests.RequestException as e:
        url = e.request.url
        err = e.response or str(e)
        raise ValueError(f"failed HTTP request to job exceptions {url}: {err}")
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON decoding error from job exceptions: {e}")
    except ConnectionError as e:
        raise ValueError(f"failed HTTP request to job exceptions: {e}")
