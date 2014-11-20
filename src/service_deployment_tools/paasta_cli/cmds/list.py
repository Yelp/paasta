#!/usr/bin/env python
"""
Contains methods used by the paasta client to list Yelp services
"""

from service_configuration_lib import read_services_configuration


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'list', description="Display a list of Yelp services.")
    list_parser.set_defaults(command=paasta_list)
    list_parser.add_argument(
        '-s', '--sort', action='store_true', default='False',
        help='sort alphabetically')


def get_services():
    """
    :return: a list of marathon services that are currently running
    """
    return read_services_configuration().keys()


def paasta_list(args):
    """
    Print a list of Yelp services currently running
    """
    for service in get_services():
        print service
