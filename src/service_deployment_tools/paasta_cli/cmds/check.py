#!/usr/bin/env python
"""
Contains methods used by the paasta client to check whether Yelp service
passes all the markers required to be considered paasta ready.
"""
import service_deployment_tools.paasta_cli.utils as utils
from service_deployment_tools.paasta_cli.cmds.checks import\
    deploy_check, docker_check, sensu_check, smartstack_check


def add_subparser(subparsers):
    check_parser = subparsers.add_parser(
        'check',
        description="Determine whether service in pwd is paasta ready.")
    check_parser.set_defaults(command=paasta_check)


def file_found_message(filename):
    return "%s %s exists" % (utils.check_mark(), filename)


def file_not_found_message(filename):
    return "%s Cannot find %s. Ensure you are in the service root directory"\
           % (utils.x_mark(), filename)


def paasta_check(args):
    """
    Analyze the service in the PWD to determine if it is paasta ready
    """
    if deploy_check.deploy_yaml_exists():
        print file_found_message('deploy.yaml')
    else:
        print file_not_found_message('deploy.yaml')

    if docker_check.dockerfile_exists():
        print file_found_message('Dockerfile')
    else:
        print file_not_found_message('Dockerfile')

    if sensu_check.monitoring_yaml_exists():
        print file_found_message('monitoring.yaml')
    else:
        print file_not_found_message('monitoring.yaml')

    if smartstack_check.smartstack_yaml_exists():
        print file_found_message('smartstack.yaml')
    else:
        print file_not_found_message('smartstack.yaml')
