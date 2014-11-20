#!/usr/bin/env python
"""
Contains methods used by the paasta client to list Yelp services
"""

from service_configuration_lib import read_services_configuration


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'list', description="Display a list of Yelp services.")
    list_parser.set_defaults(command=paasta_list)


def paasta_list(args):
    """
    Print a list of Yelp services currently running
    """

    services = sorted(read_services_configuration().keys())

    for service in services:
        print service
