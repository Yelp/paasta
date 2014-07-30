#!/usr/bin/env python
import argparse
import json
import logging
import os
import service_configuration_lib
import git


log = logging.getLogger(__name__)
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
    return 'git@git.yelpcorp.com:services/%s.git' % service


def get_branches_for_service(rootdir, service):
    valid_branches = set([])
    for fname in os.listdir(os.path.join(rootdir, service)):
        if 'marathon-' in fname:
            fpath = os.path.join(rootdir, service, fname)
            config = service_configuration_lib.read_service_information(fpath)
            for instance in config:
                target_branch = None
                if 'branch' in config[instance]:
                    target_branch = config[instance]['branch']
                # Change this to else when we don't care about docker_image anymore
                elif 'docker_image' not in config[instance]:
                    try:
                        target_branch = fname.split('-')[1].split('.')[0]
                    except:
                        pass
                if target_branch:
                    valid_branches.add(target_branch)
    return valid_branches


def get_remote_branches_for_service(mygit, service):
    try:
        remote_branches = [(branch.split('\t')[0], branch.split('\t')[1].split('refs/heads/')[1])
                           for branch in mygit.ls_remote('-h', get_git_url(service)).split('\n')]
        return remote_branches
    except git.errors.GitCommandError:
        log.warning('Service %s has branches, but the remote git repo is not named %s', service, service)
        return []


def main():
    args = parse_args()
    rootdir = os.path.abspath(args.soa_dir)
    if args.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)
    if not os.path.exists('/tmp/tmpgit'):
        os.mkdir('/tmp/tmpgit')
    mygit = git.Git('/tmp/tmpgit')
    mappings = {}
    for service in os.walk(rootdir).next()[1]:
        log.info('Examining service %s', service)
        valid_branches = get_branches_for_service(rootdir, service)
        if not valid_branches:
            log.info('Services %s has no branches. Skipping', service)
            continue
        remote_branches = get_remote_branches_for_service(mygit, service)
        for head, branch in remote_branches:
            if branch not in valid_branches:
                continue
            key = '%s:%s' % (service, branch)
            value = '%s:%s' % (service, head[0:6])
            log.info('Mapping branch %s to docker image %s', key, value)
            mappings[key] = value
    with open(os.path.join(rootdir, TARGET_FILE), 'w') as f:
        json.dump(mappings, f)

if __name__ == "__main__":
    main()