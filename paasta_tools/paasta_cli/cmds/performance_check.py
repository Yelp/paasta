#!/usr/bin/env python
import sys


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'performance-check',
        description='Performs a performance check (not implemented)',
        help='Performs a performance check (not implemented)')

    list_parser.add_argument('-s', '--service',
                             help='Name of service for which you wish to check. Leading "services-", as included in a Jenkins job name, will be stripped.',
                             required=True,
                             )
    list_parser.add_argument('-c', '--commit',
                             help='Git sha of the image to check',
                             required=True,
                             )

    list_parser.set_defaults(command=perform_performance_check)


def perform_performance_check(args):
    print 'Not implemented yet'
    sys.exit(0)
