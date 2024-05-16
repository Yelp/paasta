#!/usr/bin/env python
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
r"""
Creates a deployments.json file in the specified SOA configuration directory.
This file contains a dictionary of k/v pairs representing a map between remote
deploy groups of a service's Git repository and the current SHA at the tip of that deploy group.
This is done by specifying a 'deploy_group' key in a service instance's configuration,
or if there is no 'docker_image' key in the configuration, a deploy group name
is assumed to be paasta-{cluster}-{instance}, where cluster is the cluster
the configuration is for and instance is the instance name.

For example, if the service paasta_test has an instance called main with no
deploy group in its configuration in the hab cluster, then this script
will create a key/value pair of 'paasta_test:paasta-hab.main': 'services-paasta_test:paasta-SHA',
where SHA is the current SHA at the tip of the branch named hab in
git@github.yelpcorp.com:services/paasta_test.git. If main had a deploy_group key with
a value of 'master', the key would be paasta_test:master instead, and the SHA
would be the SHA at the tip of master.

This is done for all services in the SOA configuration directory, across any
service configuration files (filename is 'kubernetes-\*.yaml').

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -v, --verbose: Verbose output
"""
import argparse
import concurrent.futures
import json
import logging
import os
import re
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from mypy_extensions import TypedDict

from paasta_tools import remote_git
from paasta_tools.cli.utils import get_instance_configs_for_service
from paasta_tools.utils import atomic_file_write
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_git_url
from paasta_tools.utils import get_latest_deployment_tag

log = logging.getLogger(__name__)
TARGET_FILE = "deployments.json"


V1_Mapping = TypedDict(
    "V1_Mapping", {"docker_image": str, "desired_state": str, "force_bounce": str}
)
V2_Deployment = TypedDict(
    "V2_Deployment",
    {"docker_image": str, "git_sha": str, "image_version": Optional[str]},
)
V2_Control = TypedDict("V2_Control", {"desired_state": str, "force_bounce": str})
V2_Mappings = TypedDict(
    "V2_Mappings",
    {"deployments": Dict[str, V2_Deployment], "controls": Dict[str, V2_Control]},
)


DeploymentsDict = TypedDict(
    "DeploymentsDict", {"v1": Dict[str, V1_Mapping], "v2": V2_Mappings}
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Creates deployments.json for paasta services."
    )
    parser.add_argument(
        "-d",
        "--soa-dir",
        dest="soa_dir",
        metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", dest="verbose", default=False
    )
    parser.add_argument(
        "-s",
        "--service",
        required=True,
        help="Service name to make the deployments.json for",
    )
    args = parser.parse_args()
    return args


def get_deploy_group_mappings(
    soa_dir: str, service: str
) -> Tuple[Dict[str, V1_Mapping], V2_Mappings]:
    """Gets mappings from service:deploy_group to services-service:paasta-hash-image_version,
    where hash is the current SHA at the HEAD of branch_name and image_version
    can be used to provide additional version information for the Docker image.
    This is done for all services in soa_dir.

    :param soa_dir: The SOA configuration directory to read from
    :returns: A dictionary mapping service:deploy_group to a dictionary
      containing:

    - 'docker_image': something like "services-service:paasta-hash". This is
      relative to the paasta docker registry.
    - 'desired_state': either 'start' or 'stop'. Says whether this branch
      should be running.
    - 'force_bounce': An arbitrary value, which may be None. A change in this
      value should trigger a bounce, even if the other properties of this app
      have not changed.
    """
    mappings: Dict[str, V1_Mapping] = {}
    v2_mappings: V2_Mappings = {"deployments": {}, "controls": {}}
    git_url = get_git_url(service=service, soa_dir=soa_dir)

    # Most of the time of this function is in two parts:
    # 1. getting remote refs from git. (Mostly IO, just waiting for git to get back to us.)
    # 2. loading instance configs. (Mostly CPU, copy.deepcopying yaml over and over again)
    # Let's do these two things in parallel.

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    remote_refs_future = executor.submit(remote_git.list_remote_refs, git_url)

    service_configs = get_instance_configs_for_service(soa_dir=soa_dir, service=service)

    deploy_group_branch_mappings = {
        config.get_branch(): config.get_deploy_group() for config in service_configs
    }
    if not deploy_group_branch_mappings:
        log.info("Service %s has no valid deploy groups. Skipping.", service)
        return mappings, v2_mappings

    remote_refs = remote_refs_future.result()

    tag_by_deploy_group = {
        dg: get_latest_deployment_tag(remote_refs, dg)
        for dg in set(deploy_group_branch_mappings.values())
    }
    state_by_branch_and_sha = get_desired_state_by_branch_and_sha(remote_refs)

    for control_branch, deploy_group in deploy_group_branch_mappings.items():
        (deploy_ref_name, deploy_ref_sha, image_version) = tag_by_deploy_group[
            deploy_group
        ]
        if deploy_ref_name in remote_refs:
            commit_sha = remote_refs[deploy_ref_name]
            control_branch_alias = f"{service}:paasta-{control_branch}"
            control_branch_alias_v2 = f"{service}:{control_branch}"
            docker_image = build_docker_image_name(service, commit_sha, image_version)
            desired_state, force_bounce = state_by_branch_and_sha.get(
                (control_branch, deploy_ref_sha), ("start", None)
            )
            log.info("Mapping %s to docker image %s", control_branch, docker_image)

            v2_mappings["deployments"][deploy_group] = {
                "docker_image": docker_image,
                "git_sha": commit_sha,
                "image_version": image_version,
            }
            mappings[control_branch_alias] = {
                "docker_image": docker_image,
                "desired_state": desired_state,
                "force_bounce": force_bounce,
            }
            v2_mappings["controls"][control_branch_alias_v2] = {
                "desired_state": desired_state,
                "force_bounce": force_bounce,
            }
    return mappings, v2_mappings


def build_docker_image_name(
    service: str, sha: str, image_version: Optional[str] = None
) -> str:
    image_name = f"services-{service}:paasta-{sha}"
    if image_version is not None:
        image_name += f"-{image_version}"

    return image_name


def get_desired_state_by_branch_and_sha(
    remote_refs: Dict[str, str]
) -> Dict[Tuple[str, str], Tuple[str, Any]]:
    tag_pattern = r"^refs/tags/(?:paasta-){0,2}(?P<branch>[a-zA-Z0-9-_.]+)-(?P<force_bounce>[^-]+)-(?P<state>(start|stop))$"

    states_by_branch_and_sha: Dict[Tuple[str, str], List[Tuple[str, Any]]] = {}

    for ref_name, sha in remote_refs.items():
        match = re.match(tag_pattern, ref_name)
        if match:
            gd = match.groupdict()
            states_by_branch_and_sha.setdefault((gd["branch"], sha), []).append(
                (gd["state"], gd["force_bounce"])
            )

    return {
        (branch, sha): sorted(states, key=lambda x: x[1])[-1]
        for ((branch, sha), states) in states_by_branch_and_sha.items()
    }


def get_deployments_dict_from_deploy_group_mappings(
    deploy_group_mappings: Dict[str, V1_Mapping], v2_deploy_group_mappings: V2_Mappings
) -> DeploymentsDict:
    return {"v1": deploy_group_mappings, "v2": v2_deploy_group_mappings}


def generate_deployments_for_service(service: str, soa_dir: str) -> None:
    try:
        with open(os.path.join(soa_dir, service, TARGET_FILE), "r") as oldf:
            old_deployments_dict = json.load(oldf)
    except (IOError, ValueError):
        old_deployments_dict = {}
    mappings, v2_mappings = get_deploy_group_mappings(soa_dir=soa_dir, service=service)

    deployments_dict = get_deployments_dict_from_deploy_group_mappings(
        mappings, v2_mappings
    )
    if deployments_dict != old_deployments_dict:
        with atomic_file_write(os.path.join(soa_dir, service, TARGET_FILE)) as newf:
            json.dump(deployments_dict, newf)


def main() -> None:
    args = parse_args()
    soa_dir = os.path.abspath(args.soa_dir)
    service = args.service
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)

    generate_deployments_for_service(service=service, soa_dir=soa_dir)


if __name__ == "__main__":
    main()
