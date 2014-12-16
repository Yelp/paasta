#!/usr/bin/env python
"""
Contains methods used by the paasta client to check whether Yelp service
passes all the markers required to be considered paasta ready.
"""
import os
import sys

from service_configuration_lib import read_extra_service_information
from service_deployment_tools.monitoring_tools import get_team
from service_deployment_tools.paasta_cli.utils import \
    guess_service_name, is_file_in_dir, PaastaCheckMessages, \
    NoSuchService, validate_service_name as service_dir_exists_check


def add_subparser(subparsers):
    check_parser = subparsers.add_parser(
        'check',
        description="Execute 'paasta check' from service repo root",
        help="Determine whether service in pwd is paasta ready")
    check_parser.set_defaults(command=paasta_check)


def deploy_check(service_path):
    """
    Check whether deploy.yaml exists in service directory
    """
    if is_file_in_dir('deploy.yaml', service_path):
        print PaastaCheckMessages.DEPLOY_YAML_FOUND
    else:
        print PaastaCheckMessages.DEPLOY_YAML_MISSING


def docker_file_valid(path):
    """
    Ensure Dockerfile is valid
    :param path : path to a Dockerfile
    :return : A boolean that is True if the Dockerfile reads from yelpcorp
    """
    dockerfile = open(path, 'r')
    first_line = dockerfile.readline()
    if first_line.startswith("FROM docker-dev.yelpcorp.com"):
        return True
    else:
        return False


def docker_check():
    """
    Check whether Dockerfile exists in service directory, and is valid
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
    Check whether a marathon yaml file exists in service directory
    """
    if is_file_in_dir('marathon*.yaml', service_path):
        print PaastaCheckMessages.MARATHON_YAML_FOUND
    else:
        print PaastaCheckMessages.MARATHON_YAML_MISSING


def sensu_check(service_name, service_path):
    """
    Check whether monitoring.yaml exists in service directory, and that the team
    name is declared
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
    Check whether smartstack.yaml exists in service directory, and the proxy
    ports are declared
    """
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
    """
    Analyze the service in the PWD to determine if it is paasta ready
    :param args: arguments supplied to the paasta client
    """
    service_name = guess_service_name()
    try:
        service_dir_exists_check(service_name)
    except NoSuchService:
        print PaastaCheckMessages.SERVICE_DIR_MISSING
        sys.exit(1)

    service_path = os.path.join('/nail/etc/services', service_name)
    deploy_check(service_path)
    docker_check()
    marathon_check(service_path)
    sensu_check(service_name, service_path)
    smartstack_check(service_name, service_path)
