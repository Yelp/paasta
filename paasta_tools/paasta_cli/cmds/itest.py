#!/usr/bin/env python
"""Contains methods used by the paasta client to build and test a docker image."""

import os
import sys

from paasta_tools.paasta_cli.utils import validate_service_name
from paasta_tools.utils import _run
from paasta_tools.utils import build_docker_tag
from paasta_tools.utils import check_docker_image


def add_subparser(subparsers):
    list_parser = subparsers.add_parser(
        'itest',
        description='Builds and tests a docker image',
        help='Builds and tests a docker image')

    list_parser.add_argument('-s', '--service',
                             help='Test and build docker image for this service. Leading '
                                  '"services-", as included in a Jenkins job name, '
                                  'will be stripped.',
                             required=True,
                             )
    list_parser.add_argument('-c', '--commit',
                             help='Git sha used to construct tag for built image',
                             required=True,
                             )

    list_parser.set_defaults(command=paasta_itest)


def paasta_itest(args):
    """Build and test a docker image"""
    service_name = args.service
    if service_name and service_name.startswith('services-'):
        service_name = service_name.split('services-', 1)[1]
    validate_service_name(service_name)

    tag = build_docker_tag(service_name, args.commit)
    run_env = os.environ.copy()
    run_env['DOCKER_TAG'] = tag
    cmd = "make itest"

    print 'INFO: Executing command "%s" with DOCKER_TAG set to %s' % (cmd, tag)
    returncode, output = _run(cmd, env=run_env, timeout=3600, log=True, component='build', service_name=service_name)
    if returncode != 0:
        print 'ERROR: Failed to run itest. Output:\n%sReturn code was: %d' % (output, returncode)
        sys.exit(returncode)
    if not check_docker_image(service_name, args.commit):
        print 'ERROR: itest has not created %s' % tag
        sys.exit(1)
