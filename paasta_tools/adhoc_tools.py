# Copyright 2015-2016 Yelp Inc.
#
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
import logging

import service_configuration_lib

from paasta_tools.long_running_service_tools import LongRunningServiceConfig
from paasta_tools.long_running_service_tools import LongRunningServiceConfigDict
from paasta_tools.utils import BranchDictV2
from paasta_tools.utils import deep_merge_dictionaries
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import load_service_instance_config
from paasta_tools.utils import load_v2_deployments_json
from paasta_tools.utils import NoConfigurationForServiceError
from paasta_tools.utils import NoDeploymentsAvailable
from paasta_tools.utils import prompt_pick_one


log = logging.getLogger(__name__)


def load_adhoc_job_config(
    service, instance, cluster, load_deployments=True, soa_dir=DEFAULT_SOA_DIR
):
    general_config = service_configuration_lib.read_service_configuration(
        service, soa_dir=soa_dir
    )
    instance_config = load_service_instance_config(
        service=service,
        instance=instance,
        instance_type="adhoc",
        cluster=cluster,
        soa_dir=soa_dir,
    )
    general_config = deep_merge_dictionaries(
        overrides=instance_config, defaults=general_config
    )

    branch_dict = None
    if load_deployments:
        deployments_json = load_v2_deployments_json(service, soa_dir=soa_dir)
        temp_instance_config = AdhocJobConfig(
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

    return AdhocJobConfig(
        service=service,
        cluster=cluster,
        instance=instance,
        config_dict=general_config,
        branch_dict=branch_dict,
        soa_dir=soa_dir,
    )


class AdhocJobConfig(LongRunningServiceConfig):
    config_filename_prefix = "adhoc"

    def __init__(
        self,
        service: str,
        instance: str,
        cluster: str,
        config_dict: LongRunningServiceConfigDict,
        branch_dict: BranchDictV2,
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


def get_default_interactive_config(
    service: str, cluster: str, soa_dir: str, load_deployments: bool = False
) -> AdhocJobConfig:
    default_job_config = {"cpus": 4, "mem": 10240, "disk": 1024}

    try:
        job_config = load_adhoc_job_config(
            service=service, instance="interactive", cluster=cluster, soa_dir=soa_dir
        )
    except NoConfigurationForServiceError:
        job_config = AdhocJobConfig(
            service=service,
            instance="interactive",
            cluster=cluster,
            config_dict={},
            branch_dict=None,
            soa_dir=soa_dir,
        )
    except NoDeploymentsAvailable:
        job_config = load_adhoc_job_config(
            service=service,
            instance="interactive",
            cluster=cluster,
            soa_dir=soa_dir,
            load_deployments=False,
        )

    if not job_config.branch_dict and load_deployments:
        deployments_json = load_v2_deployments_json(service, soa_dir=soa_dir)
        deploy_group = prompt_pick_one(
            deployments_json.get_deploy_groups(), choosing="deploy group"
        )
        job_config.config_dict["deploy_group"] = deploy_group
        job_config.branch_dict = {
            "docker_image": deployments_json.get_docker_image_for_deploy_group(
                deploy_group
            ),
            "git_sha": deployments_json.get_git_sha_for_deploy_group(deploy_group),
            "force_bounce": None,
            "desired_state": "start",
        }

    for key, value in default_job_config.items():
        job_config.config_dict.setdefault(key, value)

    return job_config
