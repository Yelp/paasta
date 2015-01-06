#!/usr/bin/env python
"""Contains methods used by the paasta client to list Yelp services"""
from paasta_tools.paasta_cli.utils import list_services


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'list',
        description="Display a list of Yelp services.",
        help="List Yelp services.")
    list_parser.set_defaults(command=paasta_list)


def paasta_list(args):
    """Print a list of Yelp services currently running
    :param args: argparse.Namespace obj created from sys.args by paasta_cli"""
    services = list_services()

    for service in services:
        print service
