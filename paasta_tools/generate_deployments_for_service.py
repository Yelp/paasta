#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
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
branches of a service's Git repository and the current SHA at the tip of that branch.
This is done by specifing a 'branch' key in a service instance's configuration,
or if there is no 'docker_image' key in the configuration, a branch name
is assumed to be paasta-{cluster}-{instance}, where cluster is the cluster
the configuration is for and instance is the instance name.

For example, if the service paasta_test has an instance called main with no
branch in its configuration in the hab cluster, then this script
will create a key/value pair of 'paasta_test:paasta-hab.main': 'services-paasta_test:paasta-SHA',
where SHA is the current SHA at the tip of the branch named hab in
git@git.yelpcorp.com:services/paasta_test.git. If main had a branch key with
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

import service_configuration_lib

from paasta_tools import remote_git
from paasta_tools.chronos_tools import load_chronos_job_config
from paasta_tools.marathon_tools import load_marathon_service_config
from paasta_tools.utils import atomic_file_write
from paasta_tools.utils import get_git_url
from paasta_tools.utils import get_service_instance_list
from paasta_tools.utils import list_clusters

log = logging.getLogger('__main__')
logging.basicConfig()
TARGET_FILE = 'deployments.json'


def parse_args():
    parser = argparse.ArgumentParser(description='Creates marathon jobs.')
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    parser.add_argument('-s', '--service', required=True,
                        help="Service name to make the deployments.json for")
    args = parser.parse_args()
    return args


def get_instance_config_for_service(soa_dir, service):
    for cluster in list_clusters(
        service=service,
        soa_dir=soa_dir,
    ):
        print cluster
        for _, instance in get_service_instance_list(
            service=service,
            cluster=cluster,
            instance_type='marathon',
            soa_dir=soa_dir,
        ):
            print instance
            yield load_marathon_service_config(
                service=service,
                instance=instance,
                cluster=cluster,
                soa_dir=soa_dir,
                load_deployments=False,
            )
        for _, instance in get_service_instance_list(
            service=service,
            cluster=cluster,
            instance_type='chronos',
            soa_dir=soa_dir,
        ):
            yield load_chronos_job_config(
                service=service,
                instance=instance,
                cluster=cluster,
                soa_dir=soa_dir,
                load_deployments=False,
            )


def get_branches_for_service(soa_dir, service):
    """Get all branches defined in marathon/chronos configuration files for a soa service.

    :param soa_dir: The SOA configuration directory to read from
    :param service: The service name to get branches for
    :returns: A list of branches defined in instances for the service
    """
    valid_branches = set(['paasta-%s' % config.get_deploy_group() for config in get_instance_config_for_service(
        soa_dir=soa_dir,
        service=service,
    )])

    return valid_branches


def get_branch_mappings(soa_dir, service, old_mappings):
    """Gets mappings from service:branch_name to services-service:paasta-hash,
    where hash is the current SHA at the HEAD of branch_name.
    This is done for all services in soa_dir.

    :param soa_dir: The SOA configuration directory to read from
    :param old_mappings: A dictionary like the return dictionary. Used for fallback if there is a problem with a new
                         mapping.
    :returns: A dictionary mapping service:branch_name to a dictionary containing:

    - 'docker_image': something like "services-service:paasta-hash". This is relative to the paasta docker
      registry.
    - 'desired_state': either 'start' or 'stop'. Says whether this branch should be running.
    - 'force_bounce': An arbitrary value, which may be None. A change in this value should trigger a bounce, even if
      the other properties of this app have not changed.
    """
    mappings = {}
    valid_branches = get_branches_for_service(
        service=service,
        soa_dir=soa_dir,
    )
    if not valid_branches:
        log.info('Service %s has no valid branches. Skipping.', service)
        return {}

    git_url = get_git_url(
        service=service,
        soa_dir=soa_dir,
    )
    remote_refs = remote_git.list_remote_refs(git_url)

    for branch in valid_branches:
        ref_name = 'refs/heads/%s' % branch
        if ref_name in remote_refs:
            commit_sha = remote_refs[ref_name]
            branch_alias = '%s:%s' % (service, branch)
            docker_image = build_docker_image_name(service, commit_sha)
            log.info('Mapping branch %s to docker image %s', branch_alias, docker_image)
            mapping = mappings.setdefault(branch_alias, {})
            mapping['docker_image'] = docker_image

            desired_state, force_bounce = get_desired_state(service, branch, remote_refs)
            mapping['desired_state'] = desired_state
            mapping['force_bounce'] = force_bounce
    print mappings
    return mappings


def build_docker_image_name(service, sha):
    return 'services-%s:paasta-%s' % (service, sha)


def get_service_from_docker_image(image_name):
    """Does the opposite of build_docker_image_name and retrieves the
    name of a service our of a provided docker image

    An image name has the full path, including the registry. Like:
    docker-paasta.yelpcorp.com:443/services-example_service:paasta-591ae8a7b3224e3b3322370b858377dd6ef335b6
    """
    matches = re.search('.*/services-(.*?):paasta-.*?', image_name)
    return matches.group(1)


def get_desired_state(service, branch, remote_refs):
    """Gets the desired state (start or stop) from the given repo, as well as
    an arbitrary value (which may be None) that will change when a restart is
    desired.
    """
    tag_pattern = r'^refs/tags/paasta-%s-(?P<force_bounce>[^-]+)-(?P<state>.*)$' % branch

    states = []
    head_sha = remote_refs['refs/heads/%s' % branch]

    for ref_name, sha in remote_refs.iteritems():
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


def get_deployments_dict_from_branch_mappings(branch_mappings):
    return {'v1': branch_mappings}


def get_branch_mappings_from_deployments_dict(deployments_dict):
    try:
        return deployments_dict['v1']
    except KeyError:
        branch_mappings = {}
        for branch, image in deployments_dict.items():
            if isinstance(image, str):
                branch_mappings[branch] = {
                    'docker_image': image,
                    'desired_state': 'start',
                    'force_bounce': None,
                }
        return branch_mappings


def main():
    args = parse_args()
    soa_dir = os.path.abspath(args.soa_dir)
    service = args.service
    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.WARNING)
    try:
        with open(os.path.join(soa_dir, service, TARGET_FILE), 'r') as f:
            old_deployments_dict = json.load(f)
            old_mappings = get_branch_mappings_from_deployments_dict(old_deployments_dict)
    except (IOError, ValueError):
        old_mappings = {}
    mappings = get_branch_mappings(
        soa_dir=soa_dir,
        service=service,
        old_mappings=old_mappings,
    )

    deployments_dict = get_deployments_dict_from_branch_mappings(mappings)

    with atomic_file_write(os.path.join(soa_dir, service, TARGET_FILE)) as f:
        json.dump(deployments_dict, f)


if __name__ == "__main__":
    main()
