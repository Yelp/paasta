#!/usr/bin/env python
"""Contains methods used by the paasta client to list Yelp services"""
from paasta_tools.paasta_cli.utils import list_services
from paasta_tools.paasta_cli.utils import list_service_instances


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'list',
        description="Display a list of Yelp services.",
        help="List Yelp services.")
    list_parser.add_argument(
        '-i', '--print-instances',
        action='store_true',
        help='Display all service.instance values, which only PaaSTA services have.'
    )
    list_parser.set_defaults(command=paasta_list)


def paasta_list(args):
    """Print a list of Yelp services currently running
    :param args: argparse.Namespace obj created from sys.args by paasta_cli"""
    if args.print_instances:
        services = list_service_instances()
    else:
        services = list_services()

    for service in services:
        print service
