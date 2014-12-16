#!/usr/bin/env python
"""Contains methods used by the paasta client to check whether Yelp service
passes all the markers required to be considered paasta ready."""
import os
import subprocess

from service_configuration_lib import read_extra_service_information
from service_deployment_tools.monitoring_tools import get_team
from service_deployment_tools.paasta_cli.utils import \
    guess_service_name, is_file_in_dir, PaastaCheckMessages, \
    NoSuchService, validate_service_name


def add_subparser(subparsers):
    check_parser = subparsers.add_parser(
        'check',
        description="Execute 'paasta check' from service repo root",
        help="Determine whether service in pwd is paasta ready")
    check_parser.set_defaults(command=paasta_check)


def deploy_check(service_path):
    """Check whether deploy.yaml exists in service directory. Prints success or
    error message.

    :param service_path: path to a directory containing deploy.yaml"""
    if is_file_in_dir('deploy.yaml', service_path):
        print PaastaCheckMessages.DEPLOY_YAML_FOUND
    else:
        print PaastaCheckMessages.DEPLOY_YAML_MISSING


def docker_file_valid(path):
    """Ensure Dockerfile is valid.

    :param path : path to a Dockerfile
    :return : A boolean that is True if the Dockerfile reads from yelpcorp"""
    dockerfile = open(path, 'r')
    first_line = dockerfile.readline()
    if first_line.startswith("FROM docker-dev.yelpcorp.com"):
        return True
    else:
        return False


def docker_check():
    """Check whether Dockerfile exists in service directory, and is valid.
    Prints suitable message depending on outcome"""
    docker_file_path = is_file_in_dir('Dockerfile', os.getcwd())
    if docker_file_path:
        print PaastaCheckMessages.DOCKERFILE_FOUND
        if docker_file_valid(docker_file_path):
            print PaastaCheckMessages.DOCKERFILE_VALID
        else:
            print PaastaCheckMessages.DOCKERFILE_INVALID
    else:
        print PaastaCheckMessages.DOCKERFILE_MISSING


def git_repo_check(service_name):
    cmd = 'git'
    args = [cmd, 'ls-remote', 'git@git.yelpcorp.com:services/%s' % service_name]
    if subprocess.call(args) == 0:
        print PaastaCheckMessages.GIT_REPO_FOUND
    else:
        print PaastaCheckMessages.git_repo_missing(service_name)


def marathon_check(service_path):
    """Check whether a marathon yaml file exists in service directory, and
    print success/failure message.

    :param service_path: path to a directory containing the marathon yaml
    files"""
    if is_file_in_dir('marathon*.yaml', service_path):
        print PaastaCheckMessages.MARATHON_YAML_FOUND
    else:
        print PaastaCheckMessages.MARATHON_YAML_MISSING


def sensu_check(service_name, service_path):
    """Check whether monitoring.yaml exists in service directory,
    and that the team name is declared.

    :param service_name: name of service currently being examined
    :param service_path: path to loction of monitoring.yaml file"""
    if is_file_in_dir('monitoring.yaml', service_path):
        print PaastaCheckMessages.SENSU_MONITORING_FOUND
        team = get_team(None, service_name)
        if team is None:
            print PaastaCheckMessages.SENSU_TEAM_MISSING
        else:
            print PaastaCheckMessages.sensu_team_found(team)
    else:
        print PaastaCheckMessages.SENSU_MONITORING_MISSING


def service_dir_check(service_name):
    """Check whether directory service_name exists in /nail/etc/services
    :param service_name: string of service name we wish to inspect
    """
    try:
        validate_service_name(service_name)
        print PaastaCheckMessages.service_dir_found(service_name)
    except NoSuchService:
        print PaastaCheckMessages.service_dir_missing(service_name)


def smartstack_check(service_name, service_path):
    """Check whether smartstack.yaml exists in service directory, and the proxy
    ports are declared.  Print appropriate message depending on outcome.

    :param service_name: name of service currently being examined
    :param service_path: path to loction of smartstack.yaml file"""
    if is_file_in_dir('smartstack.yaml', service_path):
        print PaastaCheckMessages.SMARTSTACK_YAML_FOUND
        smartstack_dict = read_extra_service_information(
            service_name, 'smartstack')
        instances = smartstack_dict.keys()
        if instances:
            no_ports_found = True
            for instance in instances:
                if 'proxy_port' in smartstack_dict[instance]:
                    no_ports_found = False
                    print PaastaCheckMessages.smartstack_port_found(
                        instance, smartstack_dict[instance]['proxy_port'])
            if no_ports_found:
                print PaastaCheckMessages.SMARTSTACK_PORT_MISSING
        else:
            print PaastaCheckMessages.SMARTSTACK_PORT_MISSING
    else:
        print PaastaCheckMessages.SMARTSTACK_YAML_MISSING


def paasta_check(args):
    """Analyze the service in the PWD to determine if it is paasta ready
    :param args: argparse.Namespace obj created from sys.args by paasta_cli"""
    service_name = guess_service_name()
    service_path = os.path.join('/nail/etc/services', service_name)

    service_dir_check(service_name)
    deploy_check(service_path)
    docker_check()
    git_repo_check(service_name)
    marathon_check(service_path)
    sensu_check(service_name, service_path)
    smartstack_check(service_name, service_path)
