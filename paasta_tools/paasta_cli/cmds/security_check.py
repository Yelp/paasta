#!/usr/bin/env python
import sys


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'security-check',
        description='Performs a security check (not implemented)',
        help='Performs a security check (not implemented)')

    list_parser.add_argument('-s', '--service',
                             help='Name of service for which you wish to check. Leading "services-", as included in a '
                                  'Jenkins job name, will be stripped.',
                             required=True,
                             )
    list_parser.add_argument('-c', '--commit',
                             help='Git sha of the image to check',
                             required=True,
                             )

    list_parser.set_defaults(command=perform_security_check)


def perform_security_check(args):
    print 'Not implemented yet'
    sys.exit(0)
