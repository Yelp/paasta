#!/usr/bin/env python
"""DELETE ME AFTER 2015-07-24!

This is a stub to help people not get lost in the rename to local-run. See
PAASTA-870.
"""

import sys


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'test-run',
        description='DEPRECATED! Use local-run instead!',
        help='DEPRECATED! Use local-run instead!',
    )

    list_parser.set_defaults(command=paasta_test_run)


def paasta_test_run(args):
    sys.stderr.write('DEPRECATED! Use local-run instead!\n')
    sys.exit(1)
