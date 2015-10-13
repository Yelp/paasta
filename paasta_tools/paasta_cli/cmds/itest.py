#!/usr/bin/env python
"""Contains methods used by the paasta client to build and test a docker image."""

import os
import sys

from paasta_tools.paasta_cli.utils import get_jenkins_build_output_url
from paasta_tools.paasta_cli.utils import validate_service_name
from paasta_tools.utils import _log
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
    service = args.service
    if service and service.startswith('services-'):
        service = service.split('services-', 1)[1]
    validate_service_name(service)

    tag = build_docker_tag(service, args.commit)
    run_env = os.environ.copy()
    run_env['DOCKER_TAG'] = tag
    cmd = "make itest"
    loglines = []

    _log(
        service=service,
        line='starting itest for %s.' % args.commit,
        component='build',
        level='event'
    )
    returncode, output = _run(
        cmd,
        env=run_env,
        timeout=3600,
        log=True,
        component='build',
        service=service,
        loglevel='debug'
    )
    if returncode != 0:
        loglines.append(
            'ERROR: itest failed for %s.' % args.commit
        )
        output = get_jenkins_build_output_url()
        if output:
            loglines.append('See output: %s' % output)
    else:
        loglines.append('itest passed for %s.' % args.commit)
        if not check_docker_image(service, args.commit):
            loglines.append('ERROR: itest has not created %s' % tag)
            returncode = 1
    for logline in loglines:
        _log(
            service=service,
            line=logline,
            component='build',
            level='event',
        )
    sys.exit(returncode)
