#!/usr/share/python/paasta-tools/bin/python
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
import service_configuration_lib
from paasta_tools import marathon_tools
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


def get_git_url(service):
    """Get the git url for a service. Assumes that the service's
    repo matches its name, and that it lives in services- i.e.
    if this is called with the string 'test', the returned
    url will be git@git.yelpcorp.com:services/test.git.

    :param service: The service name to get a URL for
    :returns: A git url to the service's repository"""
    return 'git@git.yelpcorp.com:services/%s.git' % service


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


def get_remote_branches_for_service(mygit, service):
    """Use a git.Git object from GitPython to retrieve all remote branches
    that exist on a service's git repository.

    :param mygit: An initialized git.Git object
    :param service: The service name to get branches for
    :returns: A list of tuples of (branch_name, HEAD), where HEAD
              is the complete SHA at the HEAD of the paired branch_name"""
    try:
        git_url = get_git_url(service)
        branches = mygit.ls_remote('-h', git_url).split('\n')
        # Each branch has the form HEAD_HASH\trefs/heads/BRANCH_NAME; we want
        # a tuple of (HEAD_HASH, BRANCH_NAME).
        remote_branches = [(branch.split('\t')[0], branch.split('\t')[1].split('refs/heads/')[1])
                           for branch in branches]
        return remote_branches
    except git.errors.GitCommandError:
        log.warning('Service %s has branches, but the remote git repo is not named %s', service, service)
        return []


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
    :param old_mapings: A dictionary like the return dictionary. Used for fallback if there is a problem with a new mapping.
    :returns: A dictionary mapping service_name:branch_name to services-service_name:paasta-hash"""
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
                mappings[branch_alias] = docker_image
            else:
                log.error('Branch %s should be mapped to image %s, but that image isn\'t \
                           in the docker_registry %s', branch_alias, docker_image, docker_registry)
                mappings[branch_alias] = old_mappings.get(branch_alias, None)
    try:
        os.rmdir(tmp_dir)
    except OSError:
        log.error("Failed to remove temporary directory %s", tmp_dir)
    return mappings


def main():
    args = parse_args()
    soa_dir = os.path.abspath(args.soa_dir)
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)
    try:
        with open(os.path.join(soa_dir, TARGET_FILE), 'r') as f:
            old_mappings = json.load(f)
    except (IOError, ValueError):
        old_mappings = {}
    mappings = get_branch_mappings(soa_dir, old_mappings)
    with open(os.path.join(soa_dir, TARGET_FILE), 'w') as f:
        json.dump(mappings, f)


if __name__ == "__main__":
    main()
