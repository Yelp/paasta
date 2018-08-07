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
"""
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
git@git.yelpcorp.com:services/paasta_test.git. If main had a deploy_group key with
a value of 'master', the key would be paasta_test:master instead, and the SHA
would be the SHA at the tip of master.

This is done for all services in the SOA configuration directory, across any
service configuration files (filename is 'marathon-\*.yaml').

Command line options:

- -d <SOA_DIR>, --soa-dir <SOA_DIR>: Specify a SOA config dir to read from
- -v, --verbose: Verbose output
"""
import argparse
import json
import logging
import os
import re
from typing import Any
from typing import Dict
from typing import Tuple

from mypy_extensions import TypedDict

from paasta_tools import remote_git
from paasta_tools.cli.utils import get_instance_configs_for_service
from paasta_tools.utils import atomic_file_write
from paasta_tools.utils import DEFAULT_SOA_DIR
from paasta_tools.utils import get_git_url

log = logging.getLogger(__name__)
TARGET_FILE = 'deployments.json'


V1_Mapping = TypedDict(
    'V1_Mapping',
    {
        "docker_image": str,
        "desired_state": str,
        "force_bounce": str,
    },
)
V2_Deployment = TypedDict(
    'V2_Deployment',
    {
        'docker_image': str,
        'git_sha': str,
    },
)
V2_Control = TypedDict(
    'V2_Control',
    {
        'desired_state': str,
        'force_bounce': str,
    },
)
V2_Mappings = TypedDict(
    'V2_Mappings',
    {
        'deployments': Dict[str, V2_Deployment],
        'controls': Dict[str, V2_Control],
    },
)


DeploymentsDict = TypedDict(
    'DeploymentsDict',
    {
        'v1': Dict[str, V1_Mapping],
        'v2': V2_Mappings,
    },
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Creates marathon jobs.')
    parser.add_argument(
        '-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
        default=DEFAULT_SOA_DIR,
        help="define a different soa config directory",
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        dest="verbose", default=False,
    )
    parser.add_argument(
        '-s', '--service', required=True,
        help="Service name to make the deployments.json for",
    )
    args = parser.parse_args()
    return args


def get_latest_deployment_tag(refs: Dict[str, str], deploy_group: str) -> Tuple[str, str]:
    """Gets the latest deployment tag and sha for the specified deploy_group

    :param refs: A dictionary mapping git refs to shas
    :param deploy_group: The deployment group to return a deploy tag for

    :returns: A tuple of the form (ref, sha) where ref is the actual deployment
              tag (with the most recent timestamp)  and sha is the sha it points at
    """
    most_recent_dtime = None
    most_recent_ref = None
    most_recent_sha = None
    pattern = re.compile('^refs/tags/paasta-%s-(\d{8}T\d{6})-deploy$' % deploy_group)

    for ref_name, sha in refs.items():
        match = pattern.match(ref_name)
        if match:
            dtime = match.groups()[0]
            if most_recent_dtime is None or dtime > most_recent_dtime:
                most_recent_dtime = dtime
                most_recent_ref = ref_name
                most_recent_sha = sha
    return most_recent_ref, most_recent_sha


def get_deploy_group_mappings(
    soa_dir: str,
    service: str,
) -> Tuple[Dict[str, V1_Mapping], V2_Mappings]:
    """Gets mappings from service:deploy_group to services-service:paasta-hash,
    where hash is the current SHA at the HEAD of branch_name.
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
    v2_mappings: V2_Mappings = {'deployments': {}, 'controls': {}}

    service_configs = get_instance_configs_for_service(
        soa_dir=soa_dir,
        service=service,
    )

    deploy_group_branch_mappings = {
        config.get_branch(): config.get_deploy_group()
        for config in service_configs
    }
    if not deploy_group_branch_mappings:
        log.info('Service %s has no valid deploy groups. Skipping.', service)
        return mappings, v2_mappings

    git_url = get_git_url(
        service=service,
        soa_dir=soa_dir,
    )
    remote_refs = remote_git.list_remote_refs(git_url)

    for control_branch, deploy_group in deploy_group_branch_mappings.items():
        (deploy_ref_name, _) = get_latest_deployment_tag(remote_refs, deploy_group)
        if deploy_ref_name in remote_refs:
            commit_sha = remote_refs[deploy_ref_name]
            control_branch_alias = f'{service}:paasta-{control_branch}'
            control_branch_alias_v2 = f'{service}:{control_branch}'
            docker_image = build_docker_image_name(service, commit_sha)
            desired_state, force_bounce = get_desired_state(
                branch=control_branch,
                remote_refs=remote_refs,
                deploy_group=deploy_group,
            )
            log.info('Mapping %s to docker image %s', control_branch, docker_image)

            v2_mappings['deployments'][deploy_group] = {
                'docker_image': docker_image,
                'git_sha': commit_sha,
            }
            mappings[control_branch_alias] = {
                'docker_image': docker_image,
                'desired_state': desired_state,
                'force_bounce': force_bounce,
            }
            v2_mappings['controls'][control_branch_alias_v2] = {
                'desired_state': desired_state,
                'force_bounce': force_bounce,
            }
    return mappings, v2_mappings


def build_docker_image_name(service: str, sha: str) -> str:
    return f'services-{service}:paasta-{sha}'


def get_service_from_docker_image(image_name: str) -> str:
    """Does the opposite of build_docker_image_name and retrieves the
    name of a service our of a provided docker image

    An image name has the full path, including the registry. Like:
    docker-paasta.yelpcorp.com:443/services-example_service:paasta-591ae8a7b3224e3b3322370b858377dd6ef335b6
    """
    matches = re.search('.*/services-(.*?):paasta-.*?', image_name)
    return matches.group(1)


def get_desired_state(branch: str, remote_refs: Dict[str, str], deploy_group: str) -> Tuple[str, Any]:
    """Gets the desired state (start or stop) from the given repo, as well as
    an arbitrary value (which may be None) that will change when a restart is
    desired.
    """
    # (?:paasta-){1,2} supports a previous mistake where some tags would be called
    # paasta-paasta-cluster.instance
    tag_pattern = r'^refs/tags/(?:paasta-){0,2}%s-(?P<force_bounce>[^-]+)-(?P<state>(start|stop))$' % branch

    states = []
    (_, head_sha) = get_latest_deployment_tag(remote_refs, deploy_group)

    for ref_name, sha in remote_refs.items():
        if sha == head_sha:
            match = re.match(tag_pattern, ref_name)
            if match:
                gd = match.groupdict()
                states.append((gd['state'], gd['force_bounce']))

    if states:
        # there may be more than one that matches, so take the one that sorts
        # last by the force_bounce key.
        sorted_states = sorted(states, key=lambda x: x[1])
        return sorted_states[-1]
    else:
        return ('start', None)


def get_deployments_dict_from_deploy_group_mappings(
    deploy_group_mappings: Dict[str, V1_Mapping],
    v2_deploy_group_mappings: V2_Mappings,
) -> DeploymentsDict:
    return {'v1': deploy_group_mappings, 'v2': v2_deploy_group_mappings}


def generate_deployments_for_service(service: str, soa_dir: str) -> None:
    try:
        with open(os.path.join(soa_dir, service, TARGET_FILE), 'r') as oldf:
            old_deployments_dict = json.load(oldf)
    except (IOError, ValueError) as e:
        old_deployments_dict = {}
    mappings, v2_mappings = get_deploy_group_mappings(
        soa_dir=soa_dir,
        service=service,
    )

    deployments_dict = get_deployments_dict_from_deploy_group_mappings(mappings, v2_mappings)
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
