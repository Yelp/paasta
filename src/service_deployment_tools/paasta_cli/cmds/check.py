#!/usr/bin/env python
"""
Contains methods used by the paasta client to check whether Yelp service
passes all the markers required to be considered paasta ready.
"""
import os
import sys

from service_deployment_tools.marathon_tools import get_proxy_port_for_instance
from service_deployment_tools.monitoring_tools import get_team
from service_deployment_tools.paasta_cli.utils import \
    is_file_in_dir, PaastaCheckMessages


class NoSuchService(Exception):
    """
    Exception to be raised in the event that the service name can not be guessed
    """
    pass


def add_subparser(subparsers):
    check_parser = subparsers.add_parser(
        'check',
        description="Execute 'paasta check' from service repo root",
        help="Determine whether service in pwd is paasta ready")
    check_parser.set_defaults(command=paasta_check)


def guess_service_name():
    """
    Deduce the service name from the pwd
    :return : A string representing the service name, or a bool False
    """
    dir_name = os.path.basename(os.getcwd())
    service_path = os.path.join('/nail/etc/services', dir_name)
    if os.path.isdir(service_path):
        return dir_name
    else:
        raise NoSuchService(dir_name)


def deploy_check(service_path):
    """
    Check whether deploy.yaml exists in service directory
    """
    if is_file_in_dir('deploy.yaml', service_path):
        print PaastaCheckMessages.DEPLOY_YAML_FOUND
    else:
        print PaastaCheckMessages.DEPLOY_YAML_MISSING


def docker_file_valid(path):
    dockerfile = open(path, 'r')
    first_line = dockerfile.readline()
    if first_line.startswith("FROM docker-dev.yelpcorp.com"):
        return True
    else:
        return False


def docker_check():
    """
    Check whether Dockerfile exists in service directory
    """
    docker_file_path = is_file_in_dir('Dockerfile', os.getcwd())
    if docker_file_path:
        print PaastaCheckMessages.DOCKERFILE_FOUND
        if docker_file_valid(docker_file_path):
            print PaastaCheckMessages.DOCKERFILE_VALID
        else:
            print PaastaCheckMessages.DOCKERFILE_INVALID
    else:
        print PaastaCheckMessages.DOCKERFILE_MISSING


def marathon_check(service_path):
    """
    Check whether marathon yaml file exists in service directory
    """
    if is_file_in_dir('marathon*.yaml', service_path):
        print PaastaCheckMessages.MARATHON_YAML_FOUND
    else:
        print PaastaCheckMessages.MARATHON_YAML_MISSING


def sensu_check(service_name, service_path):
    """
    Check whether monitoring.yaml exists in service directory
    """
    if is_file_in_dir('monitoring.yaml', service_path):
        print PaastaCheckMessages.SENSU_MONITORING_FOUND
        team = get_team(None, service_name)
        if team is None:
            print PaastaCheckMessages.SENSU_TEAM_MISSING
        else:
            print PaastaCheckMessages.sensu_team_found(team)
    else:
        print PaastaCheckMessages.SENSU_MONITORING_MISSING


def smartstack_check(service_name, service_path):
    """
    Check whether smartstack.yaml exists in service directory
    """

    if is_file_in_dir('smartstack.yaml', service_path):
        print PaastaCheckMessages.SMARTSTACK_YAML_FOUND
        try:
            port = get_proxy_port_for_instance(service_name, 'main')
            print PaastaCheckMessages.smartstack_port_found(port)
        except KeyError:
            print PaastaCheckMessages.SMARTSTACK_PORT_MISSING
    else:
        print PaastaCheckMessages.SMARTSTACK_YAML_MISSING


def paasta_check(args):
    """
    Analyze the service in the PWD to determine if it is paasta ready
    """
    try:
        service_name = guess_service_name()
        service_path = os.path.join('/nail/etc/services', service_name)
        deploy_check(service_path)
        docker_check()
        marathon_check(service_path)
        sensu_check(service_name, service_path)
        smartstack_check(service_name, service_path)
    except NoSuchService:
        print PaastaCheckMessages.SERVICE_NAME_NOT_FOUND
        sys.exit(1)
