#!/usr/bin/env python
"""
Contains methods used by the paasta client to check whether Yelp service
passes all the markers required to be considered paasta ready.
"""
from service_deployment_tools.paasta_cli.cmds.checks import\
    deploy_check, docker_check, sensu_check, smartstack_check


def add_subparser(subparsers):
    check_parser = subparsers.add_parser(
        'check',
        description="Determine whether service in pwd is paasta ready.")
    check_parser.set_defaults(command=paasta_check)


def paasta_check(args):
    """
    Analyze the service in the PWD to determine if it is paasta ready
    """
    deploy_check.deploy_yaml_exists()
    docker_check.dockerfile_exists()
    sensu_check.monitoring_yaml_exists()
    smartstack_check.smartstack_yaml_exists()
