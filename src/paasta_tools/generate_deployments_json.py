#!/usr/bin/env python
"""
Usage: ./generate_deployments_json.py [options]

Creates a deployments.json file in the specified SOA configuration directory.
This file contains a dictionary of k/v pairs representing a map between remote
branches of a service's Git repository and the current SHA at the tip of that branch.
This is done by specifing a 'branch' key in a service instance's configuration,
or if there is no 'docker_image' key in the configuration, a branch name
is assumed to be paasta-{cluster}-{instance}, where cluster is the cluster
the configuration is for and instance is the instance name.

For example, if the service paasta_test has an instance called main with no
branch in its configuration in the devc cluster, then this script
will create a key/value pair of 'paasta_test:paasta-devc.main': 'services-paasta_test:paasta-SHA',
where SHA is the current SHA at the tip of the branch named devc in
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
import git
import json
import logging
import os
import re
import service_configuration_lib
from paasta_tools import marathon_tools
from paasta_tools.utils import get_git_url
import sys
import tempfile


log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler(sys.stdout))
TARGET_FILE = 'deployments.json'


def parse_args():
    parser = argparse.ArgumentParser(description='Creates marathon jobs.')
    parser.add_argument('-d', '--soa-dir', dest="soa_dir", metavar="SOA_DIR",
                        default=service_configuration_lib.DEFAULT_SOA_DIR,
                        help="define a different soa config directory")
    parser.add_argument('-v', '--verbose', action='store_true',
                        dest="verbose", default=False)
    args = parser.parse_args()
    return args


def get_branches_from_marathon_file(file_dir, filename):
    """Get all branches defined in a single service configuration file.
    A branch is defined for an instance if it has a 'branch' key, or
    the branch name is paasta-{cluster}.{instance},
    where cluster is the cluster the marathon file is defined for
    (i.e. marathon-devc.yaml is for devc), and instance is the
    instance name.

    :param file_dir: The directory that the filename argument is in
    :param filename: The name of the service configuration file to read from
    :returns: A set of branch names listed in the configuration file"""
    valid_branches = set([])
    config = service_configuration_lib.read_service_information(os.path.join(file_dir, filename))
    for instance in config:
        target_branch = None
        if 'branch' in config[instance]:
            target_branch = config[instance]['branch']
        else:
            try:
                # cluster may contain dashes (and frequently does) so
                # reassemble the cluster after pulling out the marathon bit.
                cluster = '-'.join(filename.split('-')[1:]).split('.')[0]
                target_branch = marathon_tools.get_default_branch(cluster, instance)
            except IndexError:
                pass
        if target_branch:
            valid_branches.add(target_branch)
    return valid_branches


def get_branches_for_service(soa_dir, service):
    """Get all branches defined in marathon configuration files for a soa service.

    :param soa_dir: The SOA configuration directory to read from
    :param service: The service name to get branches for
    :returns: A list of branches defined in instances for the service"""
    valid_branches = set([])
    working_dir = os.path.join(soa_dir, service)
    for fname in os.listdir(working_dir):
        if fname.startswith('marathon-'):
            valid_branches = valid_branches.union(get_branches_from_marathon_file(working_dir, fname))
    return valid_branches


def get_remote_refs_for_service(mygit, service, tags=False):
    """Use a git.Git object from GitPython to retrieve all remote refs of the given type
    that exist on a service's git repository.

    :param mygit: An initialized git.Git object
    :param service: The service name to get branches for
    :returns: A list of tuples of (ref_name, HEAD), where HEAD
              is the complete SHA at the HEAD of the paired ref_name"""

    reftype = 'tags' if tags else 'heads'
    try:
        git_url = get_git_url(service)
        branches = mygit.ls_remote(('--%s' % reftype), git_url).split('\n')
        # Each branch has the form HEAD_HASH\trefs/heads/BRANCH_NAME; we want
        # a tuple of (HEAD_HASH, BRANCH_NAME).
        remote_branches = [(branch.split('\t')[0], branch.split('\t')[1].split('refs/%s/' % reftype)[1])
                           for branch in branches if branch != '']
        return remote_branches
    except git.errors.GitCommandError:
        log.warning('Service %s has branches, but the remote git repo is not named %s', service, service)
        return []


def get_remote_branches_for_service(mygit, service, tags=False):
    """Use a git.Git object from GitPython to retrieve all remote branches
    that exist on a service's git repository.

    :param mygit: An initialized git.Git object
    :param service: The service name to get branches for
    :returns: A list of tuples of (branch_name, HEAD), where HEAD
              is the complete SHA at the HEAD of the paired branch_name"""
    return get_remote_refs_for_service(mygit, service, tags=False)


def get_remote_tags_for_service(mygit, service):
    """Use a git.Git object from GitPython to retrieve all remote tags
    that exist on a service's git repository.

    :param mygit: An initialized git.Git object
    :param service: The service name to get tags for
    :returns: A list of tuples of (tag_name, HEAD), where HEAD
              is the complete SHA at the HEAD of the paired tag_name"""
    return get_remote_refs_for_service(mygit, service, tags=True)


def get_service_directories(soa_dir):
    """Get the service directories for a given soa directory.

    :param soa_dir: The SOA configuration directory to get subdirs from
    :returns: A list of subdirectories in soa_dir"""
    # Uses os.walk to create a generator, then calls .next() to get
    # the first entry of the generator (the entries in soa_dir itself).
    # The generator returns pwd, dirs, files, and we want dirs.
    return sorted(os.walk(soa_dir).next()[1])


def get_branch_mappings(soa_dir, old_mappings):
    """Gets mappings from service_name:branch_name to services-service_name:paasta-hash,
    where hash is the current SHA at the HEAD of branch_name.
    This is done for all services in soa_dir.

    :param soa_dir: The SOA configuration directory to read from
    :param old_mappings: A dictionary like the return dictionary. Used for fallback if there is a problem with a new mapping.
    :returns: A dictionary mapping service_name:branch_name to a dictionary containing:
        - 'docker_image': something like "services-service_name:paasta-hash". This is relative to the paasta docker registry.
        - 'desired_state': either 'start' or 'stop'. Says whether this branch should be running.
        - 'force_bounce': An arbitrary value, which may be None. A change in this value should trigger a bounce, even if the
            other properties of this app have not changed.
    """
    tmp_dir = tempfile.mkdtemp()
    mygit = git.Git(tmp_dir)
    mappings = {}
    docker_registry = marathon_tools.get_docker_registry()
    for service in get_service_directories(soa_dir):
        log.info('Examining service %s', service)
        valid_branches = get_branches_for_service(soa_dir, service)
        if not valid_branches:
            log.info('Service %s has no valid branches. Skipping.', service)
            continue
        remote_branches = get_remote_branches_for_service(mygit, service)
        head_and_branch_from_valid_remote_branches = filter(lambda (head, branch): branch in valid_branches, remote_branches)

        if not head_and_branch_from_valid_remote_branches:
            log.info('Service %s has no remote branches which are valid. Skipping.', service)
            continue

        for head, branch in head_and_branch_from_valid_remote_branches:
            branch_alias = '%s:%s' % (service, branch)
            docker_image = 'services-%s:paasta-%s' % (service, head)
            if marathon_tools.get_docker_url(docker_registry, docker_image, verify=True):
                log.info('Mapping branch %s to docker image %s', branch_alias, docker_image)
                mapping = mappings.setdefault(branch_alias, {})
                mapping['docker_image'] = docker_image

                desired_state, force_bounce = get_desired_state(mygit, service, branch, head)
                mapping['desired_state'] = desired_state
                mapping['force_bounce'] = force_bounce
            else:
                log.error('Branch %s should be mapped to image %s, but that image isn\'t \
                           in the docker_registry %s', branch_alias, docker_image, docker_registry)
                mappings[branch_alias] = old_mappings.get(branch_alias, None)
    try:
        os.rmdir(tmp_dir)
    except OSError:
        log.error("Failed to remove temporary directory %s", tmp_dir)
    return mappings


def get_desired_state(mygit, service, branch, head_sha):
    """Gets the desired state (start or stop) from the given repo, as well as
    an arbitrary value (which may be None) that will change when a restart is
    desired.
    """
    sha_tags = get_remote_tags_for_service(mygit, service)
    tag_pattern = r'^paasta-%s-(?P<force_bounce>[^-]+)-(?P<state>.*)$' % branch

    states = []
    for sha, tag in sha_tags:
        if sha == head_sha:
            match = re.match(tag_pattern, tag)
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
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)
    try:
        with open(os.path.join(soa_dir, TARGET_FILE), 'r') as f:
            old_deployments_dict = json.load(f)
            old_mappings = get_branch_mappings_from_deployments_dict(old_deployments_dict)
    except (IOError, ValueError):
        old_mappings = {}
    mappings = get_branch_mappings(soa_dir, old_mappings)

    deployments_dict = get_deployments_dict_from_branch_mappings(mappings)

    with open(os.path.join(soa_dir, TARGET_FILE), 'w') as f:
        json.dump(deployments_dict, f)


if __name__ == "__main__":
    main()
