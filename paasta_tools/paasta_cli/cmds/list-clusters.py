#!/usr/bin/env python
from paasta_tools.utils import list_clusters


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'list-clusters',
        description="Display a list of PaaSTA clusters.",
        help="List PaaSTA clusters.")
    list_parser.set_defaults(command=paasta_list_clusters)


def paasta_list_clusters(args):
    for cluster in list_clusters():
        print cluster
