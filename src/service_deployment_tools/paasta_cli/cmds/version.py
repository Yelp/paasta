#!/usr/bin/env python
"""
Contains methods used to return the current version of the PaaSTA client
"""
import pkg_resources


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'version',
        description="Print the current version of the PaaSTA client",
        help="Print the current version of the PaaSTA client")
    list_parser.set_defaults(command=paasta_version)


def paasta_version(args):
    """
    Print the current version of the PaaSTA client.  The version number must
    match the latest git tag as returned by 'git describe --tags' otherwise a
    test will fail.
    """

    print pkg_resources.require("service-deployment-tools")[0].version
