#!/usr/bin/env python
import argparse
import json
import logging
import os
import tempfile
import service_configuration_lib
import git


log = logging.getLogger(__name__)
TARGET_FILE = 'deployments.json'


def parse_args():
<<<<<<< HEAD
=======
    """Command line arguments:
      -v, --verbose: Verbose output
      -d, --soa-dir: Set a soa directory manually"""
>>>>>>> master
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
    repo matches its name, and that it lives in services/ ."""
    return 'git@git.yelpcorp.com:services/%s.git' % service


def get_branches_from_marathon_file(file_dir, filename):
    """Get all branches defined from a single marathon file.
    A branch is defined for an instance if it has a 'branch' key, or
    does not have any 'docker_image' key. In the case of the latter
    but not the former, the branch name is the cluster the marathon
    file is designated for (i.e. marathon-devc.yaml is devc)."""
    valid_branches = set([])
    config = service_configuration_lib.read_service_information(os.path.join(file_dir, filename))
    for instance in config:
        target_branch = None
        if 'branch' in config[instance]:
            target_branch = config[instance]['branch']
        # Change this to else when we don't care about docker_image anymore
        elif 'docker_image' not in config[instance]:
            try:
                target_branch = filename.split('-')[1].split('.')[0]
            except IndexError:
                pass
        if target_branch:
            valid_branches.add(target_branch)
    return valid_branches


def get_branches_for_service(soa_dir, service):
    """Get all branches defined in marathon configuration files for a soa service."""
    valid_branches = set([])
    working_dir = os.path.join(soa_dir, service)
    for fname in os.listdir(working_dir):
        if 'marathon-' in fname:
            valid_branches = valid_branches.union(get_branches_from_marathon_file(working_dir, fname))
    return valid_branches


def get_remote_branches_for_service(mygit, service):
    """Use a git.Git object from GitPython to retrieve all branches that
    exist for the given service.

    Returns a list of tuples of the form branch_name, HEAD where HEAD
    is the current hash at the HEAD of branch_name."""
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

    Uses os.walk to create a generator, then calls .next() to get
    the first entry of the generator (the entries in soa_dir itself).
    The generator returns pwd, dirs, files, and we want dirs."""
    return os.walk(soa_dir).next()[1]


def get_branch_mappings(soa_dir):
    """Gets mappings from service_name:branch_name to service_name:hash, where
    hash is the first 6 characters of the current hash at the HEAD of branch_name.
    This is done for all services in soa_dir."""
    tmp_dir = tempfile.mkdtemp()
    mygit = git.Git(tmp_dir)
    mappings = {}
    for service in get_service_directories(soa_dir):
        log.info('Examining service %s', service)
        valid_branches = get_branches_for_service(soa_dir, service)
        if not valid_branches:
            log.info('Service %s has no branches.', service)
            continue
        remote_branches = get_remote_branches_for_service(mygit, service)
        for head, branch in filter(lambda (head, branch): branch in valid_branches, remote_branches):
            branch_alias = '%s:%s' % (service, branch)
            docker_image = '%s:%s' % (service, head[0:6])
            log.info('Mapping branch %s to docker image %s', branch_alias, docker_image)
            mappings[branch_alias] = docker_image
    try:
        os.rmdir(tmp_dir)
    except OSError:
<<<<<<< HEAD
        pass
=======
        log.error("Failed to remove temporary directory %s", tmp_dir)
>>>>>>> master
    return mappings


def main():
    """Generates a deployments.json file in a soa directory. This
    file contains a dictionary where the keys are the alias used
    by setup_marathon_job to locate the correct docker image,
    which is the key's value.

    The alias (key) has the form service_name:branch_name, where
    branch_name is a branch on service_name's git repo.
    The docker image (value) has the form service_name:HEAD,
    where HEAD is the first 6 characters of the current hash
<<<<<<< HEAD
    at the HEAD of the branch_name this value is mapped to.

    Command line arguments:
      -v, --verbose: Verbose output
      -d, --soa-dir: Set a soa directory manually"""
=======
    at the HEAD of the branch_name this value is mapped to."""
>>>>>>> master
    args = parse_args()
    soa_dir = os.path.abspath(args.soa_dir)
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)
    mappings = get_branch_mappings(soa_dir)
    with open(os.path.join(soa_dir, TARGET_FILE), 'w') as f:
        json.dump(mappings, f)


if __name__ == "__main__":
    main()